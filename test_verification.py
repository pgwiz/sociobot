"""Comprehensive verification and smoke tests for Sociobot."""

import asyncio
import os
import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("test_verification")


async def test_api_integration():
    """Verify connectivity to ytsp-api.pgwiz.cloud endpoints."""
    logger.info("─── 1. Testing Stream Extractor API ───")
    import httpx

    base_url = "https://ytsp-api.pgwiz.cloud"

    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        # Test /health with retry
        h_ok = False
        for attempt in range(1, 4):
            try:
                h_resp = await client.get(f"{base_url}/health")
                if h_resp.status_code == 200:
                    logger.info("✓ /health endpoint active and healthy.")
                    h_ok = True
                    break
            except Exception as e:
                logger.warning(f"Health check attempt {attempt} failed: {e}")
                await asyncio.sleep(1.0)
        if not h_ok:
            logger.error("✗ /health failed after retries")
            return False

        # Test Search
        try:
            s_resp = await client.get(f"{base_url}/api/search/spotify", params={"query": "coldplay", "limit": 3})
            assert s_resp.status_code == 200, f"Search returned {s_resp.status_code}"
            data = s_resp.json()
            results = data if isinstance(data, list) else data.get("results", [])
            assert len(results) > 0, "No search results returned"
            logger.info(f"✓ /api/search/spotify returned {len(results)} items: '{results[0].get('title')}'")
        except Exception as e:
            logger.error(f"✗ Search failed: {e}")
            return False

        # Test Stream Info
        try:
            stream_resp = await client.get(f"{base_url}/stream/dQw4w9WgXcQ", params={"quality": "audio_high"})
            assert stream_resp.status_code == 200, f"Stream info returned {stream_resp.status_code}"
            s_data = stream_resp.json()
            assert "streamUrl" in s_data or "proxy_url" in s_data, "Stream URL missing from response"
            logger.info(f"✓ /stream endpoint returned streamUrl: '{s_data.get('title')}'")
        except Exception as e:
            logger.error(f"✗ Stream info failed: {e}")
            return False

    return True


async def test_database_engine():
    """Verify database migrations, channel storage, replication lookups, and deletion."""
    logger.info("\n─── 2. Testing Database Layer (SQLite mode) ───")
    from bot.database import Database
    from bot.config import settings

    test_db_path = "test_sociobot.db"
    if os.path.exists(test_db_path):
        os.remove(test_db_path)

    # Force SQLite test
    orig_url = settings.DATABASE_URL
    orig_path = settings.DATABASE_PATH
    settings.DATABASE_URL = f"sqlite:///{test_db_path}"
    settings.DATABASE_PATH = test_db_path

    test_db = Database()
    try:
        await test_db.connect()
        logger.info("✓ SQLite database connected and migrated successfully.")

        # 1. User creation and channel linking
        user = await test_db.get_or_create_user(chat_id=123456, username="testuser", first_name="Tester")
        assert user.get("chat_id") == 123456
        logger.info("✓ User registration verified.")

        await test_db.link_user_channel(chat_id=123456, channel_id=-100987654321, channel_title="My Music Vault")
        c_info = await test_db.get_user_channel(chat_id=123456)
        assert c_info.get("channel_id") == -100987654321
        assert c_info.get("channel_title") == "My Music Vault"
        logger.info("✓ Channel linking verified.")

        # 2. Saving media post
        post_url = "https://t.me/c/987654321/42"
        await test_db.save_user_media(
            user_chat_id=123456,
            channel_id=-100987654321,
            channel_msg_id=42,
            channel_post_url=post_url,
            track_id="dQw4w9WgXcQ",
            quality="audio_high",
            telegram_file_id="FILE_123"
        )
        stored = await test_db.get_user_stored_track(123456, "dQw4w9WgXcQ", "audio_high")
        assert stored is not None
        assert stored["channel_msg_id"] == 42
        logger.info("✓ User media record verified.")

        # 3. Peer replication lookup
        peers = await test_db.find_available_peer_sources("dQw4w9WgXcQ", "audio_high")
        assert len(peers) == 1
        assert peers[0]["channel_id"] == -100987654321
        logger.info("✓ Peer candidate lookup verified.")

        # 4. Track deletion
        deleted = await test_db.delete_user_media(123456, "dQw4w9WgXcQ", "audio_high")
        assert len(deleted) == 1
        assert deleted[0]["channel_msg_id"] == 42
        check_after = await test_db.get_user_stored_track(123456, "dQw4w9WgXcQ", "audio_high")
        assert check_after is None
        logger.info("✓ User media deletion verified.")

        # 5. Super Admin user management methods
        users_list, total_u = await test_db.list_users(page=1, page_size=10)
        assert total_u >= 1
        assert any(u["chat_id"] == 123456 for u in users_list)
        logger.info(f"✓ list_users verified: {total_u} users found.")

        user_details = await test_db.get_user_details(123456)
        assert user_details is not None
        assert user_details["chat_id"] == 123456
        assert user_details["is_banned"] is False
        logger.info("✓ get_user_details verified.")

        # Toggle Admin
        new_adm = await test_db.toggle_user_admin(123456)
        assert new_adm is True
        logger.info("✓ toggle_user_admin verified.")

        # Toggle Ban
        new_ban = await test_db.toggle_user_ban(123456)
        assert new_ban is True
        assert await test_db.is_user_banned(123456) is True
        logger.info("✓ toggle_user_ban verified.")

        # Force Unlink Channel
        await test_db.force_unlink_user_channel(123456)
        unlinked_info = await test_db.get_user_channel(123456)
        assert unlinked_info["channel_id"] is None
        logger.info("✓ force_unlink_user_channel verified.")

        # 6. Schema isolation & stats
        cur_schema = await test_db.get_current_schema()
        assert cur_schema == "main"
        assert test_db.schema == settings.DB_SCHEMA
        logger.info(f"✓ Schema properties verified (configured: '{test_db.schema}', active engine: '{cur_schema}').")

        stats = await test_db.get_stats()
        assert stats["users"] >= 1
        assert "schema" in stats
        logger.info(f"✓ Stats aggregation verified: {stats}")

    finally:
        await test_db.disconnect()
        settings.DATABASE_URL = orig_url
        settings.DATABASE_PATH = orig_path
        if os.path.exists(test_db_path):
            try:
                os.remove(test_db_path)
            except Exception:
                pass

    return True


async def test_postgres_schema_isolation():
    """Verify live Neon PostgreSQL schema isolation if PostgreSQL URL is available."""
    from bot.database import Database
    from bot.config import settings

    pg_url = os.environ.get("NEON_DATABASE_URL") or os.environ.get("TEST_PG_URL")
    if not pg_url:
        # Check sibling .env for testing
        sibling_env = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "telegram-ultra-mini", ".env")
        if os.path.exists(sibling_env):
            with open(sibling_env, "r", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("DATABASE_URL=") and ("postgresql://" in line or "postgres://" in line):
                        pg_url = line.strip().split("=", 1)[1]
                        break

    if not pg_url:
        logger.info("\n─── 3. Testing PostgreSQL Schema Isolation (Skipped: No PG URL) ───")
        return True

    logger.info("\n─── 3. Testing PostgreSQL Schema Isolation (Live Neon) ───")
    orig_url = settings.DATABASE_URL
    orig_schema = settings.DB_SCHEMA

    test_schema = "sociobot_test"
    settings.DATABASE_URL = pg_url
    settings.DB_SCHEMA = test_schema

    test_db = Database()
    try:
        await test_db.connect()
        assert test_db.is_postgres is True
        active_schema = await test_db.get_current_schema()
        assert active_schema == test_schema, f"Expected schema '{test_schema}', got '{active_schema}'"
        logger.info(f"✓ Neon PostgreSQL connection isolated in custom schema: '{active_schema}'")

        # Test inserting into custom schema
        u = await test_db.get_or_create_user(chat_id=999999, username="pg_tester", first_name="SchemaTester")
        assert u.get("chat_id") == 999999
        logger.info("✓ User insertion into isolated custom schema succeeded.")

        # Verify that public schema does not have this test user
        async with test_db.pg_pool.acquire() as conn:
            pub_has_user = await conn.fetchval(
                "SELECT COUNT(*) FROM public.users WHERE chat_id = 999999;"
            ) if await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='users');"
            ) else 0
            assert pub_has_user == 0, "Pollution detected! User leaked into public schema!"
            logger.info("✓ Complete isolation verified: public.users is completely unaffected.")

            # Clean up test schema
            await conn.execute(f'DROP SCHEMA IF EXISTS "{test_schema}" CASCADE;')
            logger.info(f"✓ Cleaned up test schema '{test_schema}'.")

    finally:
        await test_db.disconnect()
        settings.DATABASE_URL = orig_url
        settings.DB_SCHEMA = orig_schema

    return True


def test_bot_syntax():
    """Verify that all handlers and dispatchers import cleanly without circular dependencies."""
    logger.info("\n─── 4. Testing Bot Routers & Dispatcher ───")
    from aiogram import Dispatcher
    from bot.handlers import register_all_handlers

    dp = Dispatcher()
    register_all_handlers(dp)
    logger.info(f"✓ Successfully registered {len(dp.sub_routers)} modular routers in Dispatcher.")
    return True


async def main():
    logger.info("=== Starting Sociobot Verification Suite ===\n")
    api_ok = await test_api_integration()
    db_ok = await test_database_engine()
    pg_schema_ok = await test_postgres_schema_isolation()
    syntax_ok = test_bot_syntax()

    if api_ok and db_ok and pg_schema_ok and syntax_ok:
        logger.info("\n🎉 ALL SOCIOBOT VERIFICATION TESTS PASSED SUCCESSFULLY!")
        sys.exit(0)
    else:
        logger.error("\n❌ SOME TESTS FAILED.")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
