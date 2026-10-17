"""Comprehensive automated test suite for MongoDB Atlas backend in Sociobot."""

import asyncio
import os
import sys
import unittest
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from bot.config import settings
from bot.database import Database

TEST_DB_NAME = "test_sociobot_mongo_temp"
TEST_MONGO_URL = f"mongodb+srv://rtwoft_db_user:0sudhUOXWychqplX@clasto.nlhmbxk.mongodb.net/{TEST_DB_NAME}?retryWrites=true&w=majority"


class TestMongoIntegration(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # Override settings for isolated test database
        self.orig_db_url = settings.DATABASE_URL
        settings.DATABASE_URL = TEST_MONGO_URL
        self.db = Database()
        await self.db.connect()

    async def asyncTearDown(self):
        # Drop test database and cleanup
        if self.db.mongo_client:
            await self.db.mongo_client.drop_database(TEST_DB_NAME)
        await self.db.disconnect()
        settings.DATABASE_URL = self.orig_db_url

    def test_database_name_resolution(self):
        r = Database.resolve_mongo_db_name
        # 1. URI path priority
        self.assertEqual(
            r("mongodb+srv://user:pass@cluster.mongodb.net/custom_db?retryWrites=true"),
            "custom_db"
        )
        # 2. MONGO_DB_NAME setting priority
        self.assertEqual(
            r("mongodb+srv://user:pass@cluster.mongodb.net/?retryWrites=true", configured_db_name="env_db"),
            "env_db"
        )
        # 3. Configured schema priority
        self.assertEqual(
            r("mongodb+srv://user:pass@cluster.mongodb.net/", default_schema="my_schema"),
            "my_schema"
        )
        # 4. Bot token ID extraction
        self.assertEqual(
            r("mongodb+srv://user:pass@cluster.mongodb.net/", bot_token="7391859147:AAEWboEBCY0lA7ZOsjRrSMwHrHq7y4-IQAY"),
            "bot_7391859147"
        )
        # 5. Fallback
        self.assertEqual(
            r("mongodb+srv://user:pass@cluster.mongodb.net/"),
            "sociobot"
        )

    async def test_backend_detection(self):
        self.assertTrue(self.db.is_mongo)
        self.assertFalse(self.db.is_postgres)
        self.assertTrue(self.db.is_connected)
        self.assertEqual(await self.db.get_current_schema(), TEST_DB_NAME)

    async def test_user_lifecycle_and_onboarding(self):
        chat_id = 123456789
        user = await self.db.get_or_create_user(chat_id, username="testuser", first_name="Test")
        self.assertEqual(user["chat_id"], chat_id)
        self.assertEqual(user["username"], "testuser")
        self.assertFalse(user["terms_accepted"])

        await self.db.set_terms_accepted(chat_id, True)
        ch_info = await self.db.get_user_channel(chat_id)
        self.assertIsNone(ch_info["channel_id"])  # No channel linked yet

        await self.db.link_user_channel(chat_id, -100111222333, "My Vault")
        ch_info = await self.db.get_user_channel(chat_id)
        self.assertIsNotNone(ch_info)
        self.assertEqual(ch_info["channel_id"], -100111222333)
        self.assertTrue(ch_info["terms_accepted"])
        self.assertTrue(ch_info["is_storage_active"])

    async def test_multi_channel_vaults_and_quota(self):
        chat_id = 987654321
        await self.db.get_or_create_user(chat_id)

        # Link 5 channels (default quota = 5)
        for i in range(1, 6):
            res = await self.db.add_user_channel(chat_id, -100200000 + i, f"Channel {i}")
            self.assertEqual(res["channel_id"], -100200000 + i)

        self.assertEqual(await self.db.get_channel_count(chat_id), 5)

        # 6th channel should raise ValueError due to quota limit
        with self.assertRaises(ValueError):
            await self.db.add_user_channel(chat_id, -100200099, "Channel Overflow")

        # Upgrade quota to 10
        await self.db.upgrade_user_quota(chat_id, 10)
        self.assertEqual(await self.db.get_user_max_channels(chat_id), 10)

        # Now 6th channel succeeds
        res6 = await self.db.add_user_channel(chat_id, -100200099, "Channel Overflow")
        self.assertEqual(res6["channel_id"], -100200099)
        self.assertEqual(await self.db.get_channel_count(chat_id), 6)

    async def test_platform_routing_and_unlinking(self):
        chat_id = 456789123
        await self.db.get_or_create_user(chat_id)

        ch1_id = -1001001
        ch2_id = -1001002
        await self.db.add_user_channel(chat_id, ch1_id, "YouTube Channel", set_primary=True)
        await self.db.add_user_channel(chat_id, ch2_id, "Spotify Channel", set_primary=False)

        # Route youtube to ch1, spotify to ch2
        await self.db.set_platform_route(chat_id, "youtube", ch1_id)
        await self.db.set_platform_route(chat_id, "spotify", ch2_id)

        self.assertEqual(await self.db.get_destination_channel(chat_id, "youtube"), ch1_id)
        self.assertEqual(await self.db.get_destination_channel(chat_id, "spotify"), ch2_id)
        # Unrouted platform falls back to primary channel (ch1)
        self.assertEqual(await self.db.get_destination_channel(chat_id, "tiktok"), ch1_id)

        # Unlink ch2 -> spotify routes should re-route to primary ch1
        res = await self.db.remove_user_channel(chat_id, ch2_id)
        self.assertTrue(res["success"])
        self.assertEqual(res["new_primary_id"], ch1_id)
        self.assertEqual(await self.db.get_destination_channel(chat_id, "spotify"), ch1_id)

    async def test_media_vault_peer_discovery_and_purge(self):
        user_1 = 11111
        user_2 = 22222
        track_id = "trk_rick_astley"
        quality = "audio"

        await self.db.save_track_metadata(
            track_id=track_id,
            quality=quality,
            title="Never Gonna Give You Up",
            artist="Rick Astley",
            duration_secs=213,
            file_size_bytes=5242880
        )

        meta = await self.db.get_track_metadata(track_id, quality)
        self.assertEqual(meta["title"], "Never Gonna Give You Up")

        # Save media for user 1
        await self.db.save_user_media(
            user_chat_id=user_1,
            channel_id=-1005551,
            channel_msg_id=10,
            channel_post_url="https://t.me/c/5551/10",
            track_id=track_id,
            quality=quality,
            telegram_file_id="tg_file_001",
            platform="youtube"
        )

        # Save media for user 2 (peer source)
        await self.db.save_user_media(
            user_chat_id=user_2,
            channel_id=-1005552,
            channel_msg_id=20,
            channel_post_url="https://t.me/c/5552/20",
            track_id=track_id,
            quality=quality,
            telegram_file_id="tg_file_002",
            platform="youtube"
        )

        # User 1 retrieval
        u1_track = await self.db.get_user_stored_track(user_1, track_id, quality)
        self.assertIsNotNone(u1_track)
        self.assertEqual(u1_track["channel_msg_id"], 10)
        self.assertEqual(u1_track["title"], "Never Gonna Give You Up")

        # Peer candidate lookup
        peers = await self.db.find_available_peer_sources(track_id, quality)
        self.assertEqual(len(peers), 2)

        # Count & Purge
        cnt = await self.db.get_channel_media_count(user_1, -1005551)
        self.assertEqual(cnt, 1)

        purged = await self.db.purge_channel_media(user_1, -1005551)
        self.assertEqual(len(purged), 1)
        self.assertEqual(purged[0], (-1005551, 10))

        # After purge, user 1 has none, user 2 still has 1
        self.assertIsNone(await self.db.get_user_stored_track(user_1, track_id, quality))
        peers_after = await self.db.find_available_peer_sources(track_id, quality)
        self.assertEqual(len(peers_after), 1)
        self.assertEqual(peers_after[0]["user_chat_id"], user_2)

    async def test_api_cache_and_rate_limits(self):
        # API Cache
        key = "api:search:query123"
        data = {"results": [{"title": "Song 1"}], "count": 1}
        await self.db.set_api_cache(key, data, ttl_seconds=3600)

        cached = await self.db.get_api_cache(key)
        self.assertEqual(cached, data)

        await self.db.delete_api_cache(key)
        self.assertIsNone(await self.db.get_api_cache(key))

        # Rate limits
        user_id = 77777
        action = "download_press"
        for _ in range(3):
            self.assertTrue(await self.db.check_rate_limit(user_id, action, max_requests=3, window_secs=60))

        # 4th request within window is rate limited
        self.assertFalse(await self.db.check_rate_limit(user_id, action, max_requests=3, window_secs=60))

    async def test_admin_stats_and_user_management(self):
        u1 = 88801
        u2 = 88802
        await self.db.get_or_create_user(u1, username="alice", first_name="Alice")
        await self.db.get_or_create_user(u2, username="bob", first_name="Bob")

        # Stats
        stats = await self.db.get_stats()
        self.assertGreaterEqual(stats["users"], 2)
        self.assertEqual(stats["schema"], TEST_DB_NAME)

        # Toggle admin
        is_admin = await self.db.toggle_user_admin(u1)
        self.assertTrue(is_admin)

        # Toggle ban
        is_banned = await self.db.toggle_user_ban(u2)
        self.assertTrue(is_banned)
        self.assertTrue(await self.db.is_user_banned(u2))

        # Search users
        users, total = await self.db.list_users(page=1, page_size=10, search="alice")
        self.assertEqual(total, 1)
        self.assertEqual(users[0]["username"], "alice")
        self.assertTrue(users[0]["is_admin"])

        # Find by identifier
        found = await self.db.find_user_by_identifier("@alice")
        self.assertIsNotNone(found)
        self.assertEqual(found["chat_id"], u1)


if __name__ == "__main__":
    unittest.main()
