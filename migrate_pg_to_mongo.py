#!/usr/bin/env python3
"""Database Migration Script: Neon PostgreSQL -> MongoDB Atlas for Sociobot.

Migrates all existing tables and records:
- users
- user_channels
- user_platform_routes
- user_media_storage
- tracks (cached track metadata)
- api_cache
- download_history
- rate_limits

Supports:
- Multi-database naming isolation per bot token ID, custom URI path, or schema/dbname.
- Idempotent upserts (safe to run multiple times).
- CLI arguments or automatic .env detection.
- Detailed migration summary and row count verification.
"""

import argparse
import asyncio
import json
import logging
import os
import sys
import urllib.parse
from datetime import datetime
from typing import Optional, Dict, Any, List

import asyncpg
import motor.motor_asyncio
from pymongo import UpdateOne

try:
    import certifi
    HAS_CERTIFI = True
except ImportError:
    HAS_CERTIFI = False

# Add project root to sys.path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from bot.config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("pg_to_mongo_migration")

DEFAULT_PG_URL = (
    "postgresql://neondb_owner:npg_Ots3aN9RMFvb@ep-autumn-silence-b4czkrxz-pooler.c-6.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
)
DEFAULT_MONGO_URL = (
    "mongodb+srv://rtwoft_db_user:0sudhUOXWychqplX@clasto.nlhmbxk.mongodb.net/?retryWrites=true&w=majority"
)


def resolve_mongo_db_name(
    uri: str = "",
    configured_db_name: Optional[str] = None,
    bot_token: Optional[str] = None,
    default_schema: Optional[str] = None
) -> str:
    """Resolve target MongoDB database name following priority hierarchy:
    
    1. Database name explicitly in URI path (e.g. mongodb+srv://.../sociobot?retryWrites=true)
    2. Explicitly configured db_name / MONGO_DB_NAME
    3. Bot token ID (e.g. bot_7391859147)
    4. Configured DB_SCHEMA (e.g. sociobot)
    5. Fallback default: 'sociobot'
    """
    if uri:
        try:
            parsed = urllib.parse.urlparse(uri)
            path_part = parsed.path.lstrip("/").strip()
            if path_part and not path_part.startswith("?"):
                name = path_part.split("/")[0].split("?")[0].strip()
                if name:
                    return name
        except Exception as e:
            logger.warning(f"Failed parsing MongoDB URI path: {e}")

    if configured_db_name and str(configured_db_name).strip():
        return str(configured_db_name).strip()

    if default_schema and str(default_schema).strip() and str(default_schema).strip() != "public":
        return str(default_schema).strip()

    if bot_token and ":" in bot_token:
        bot_id = bot_token.split(":")[0].strip()
        if bot_id and bot_id.isdigit():
            return f"bot_{bot_id}"

    return "sociobot"


async def migrate(
    pg_url: str,
    mongo_url: str,
    schema: str = "sociobot",
    db_name: Optional[str] = None,
    drop_target: bool = False,
    dry_run: bool = False
) -> Dict[str, Any]:
    """Execute migration from PostgreSQL to MongoDB."""
    bot_token = settings.TELEGRAM_BOT_TOKEN
    target_db_name = resolve_mongo_db_name(
        uri=mongo_url,
        configured_db_name=db_name or getattr(settings, "MONGO_DB_NAME", None),
        bot_token=bot_token,
        default_schema=schema
    )

    logger.info("=" * 65)
    logger.info("MIGRATION: Neon PostgreSQL -> MongoDB Atlas")
    logger.info(f"Source PG Schema: '{schema}'")
    logger.info(f"Target Database:  '{target_db_name}'")
    logger.info(f"Dry Run Mode:     {dry_run}")
    logger.info(f"Drop Target:      {drop_target}")
    logger.info("=" * 65)

    # 1. Connect to PostgreSQL
    logger.info("Connecting to Neon PostgreSQL...")
    clean_pg_url = pg_url.replace("&channel_binding=require", "")
    pg_conn = await asyncpg.connect(clean_pg_url, statement_cache_size=0, timeout=30.0)
    logger.info("Connected to Neon PostgreSQL.")

    # 2. Connect to MongoDB
    mongo_kwargs: Dict[str, Any] = {
        "serverSelectionTimeoutMS": 15000,
        "connectTimeoutMS": 15000,
    }
    if HAS_CERTIFI:
        mongo_kwargs["tlsCAFile"] = certifi.where()

    mongo_client = motor.motor_asyncio.AsyncIOMotorClient(
        mongo_url,
        **mongo_kwargs
    )
    await mongo_client.admin.command("ping")
    mongo_db = mongo_client[target_db_name]
    logger.info(f"Connected to MongoDB cluster. Database: '{target_db_name}'")

    results: Dict[str, Any] = {}

    try:
        if drop_target and not dry_run:
            logger.warning(f"Dropping target collections in '{target_db_name}'...")
            await mongo_db.users.drop()
            await mongo_db.user_channels.drop()
            await mongo_db.user_platform_routes.drop()
            await mongo_db.user_media_storage.drop()
            await mongo_db.tracks.drop()
            await mongo_db.api_cache.drop()
            await mongo_db.download_history.drop()
            await mongo_db.rate_limits.drop()

        # ── Table 1: USERS ───────────────────────────────────────────────
        logger.info("\n--- Migrating 'users' ---")
        user_rows = await pg_conn.fetch(f"""
            SELECT chat_id, username, first_name, channel_id, channel_title,
                   is_storage_active, terms_accepted, is_admin, is_banned, max_channels,
                   created_at, last_active
            FROM "{schema}".users;
        """)
        results["users_pg"] = len(user_rows)
        logger.info(f"Found {len(user_rows)} users in PostgreSQL.")

        if not dry_run and user_rows:
            ops = []
            for r in user_rows:
                doc = {
                    "chat_id": int(r["chat_id"]),
                    "username": r["username"],
                    "first_name": r["first_name"],
                    "channel_id": int(r["channel_id"]) if r["channel_id"] is not None else None,
                    "channel_title": r["channel_title"],
                    "is_storage_active": bool(r["is_storage_active"]),
                    "terms_accepted": bool(r["terms_accepted"]),
                    "is_admin": bool(r["is_admin"]),
                    "is_banned": bool(r["is_banned"]),
                    "max_channels": int(r["max_channels"] or 5),
                    "created_at": r["created_at"],
                    "last_active": r["last_active"]
                }
                ops.append(
                    UpdateOne(
                        {"chat_id": doc["chat_id"]},
                        {"$set": doc},
                        upsert=True
                    )
                )
            if ops:
                res = await mongo_db.users.bulk_write(ops)
                logger.info(f"MongoDB users: upserted={len(res.upserted_ids)}, modified={res.modified_count}, matched={res.matched_count}")

        # ── Table 2: USER_CHANNELS ───────────────────────────────────────
        logger.info("\n--- Migrating 'user_channels' ---")
        uc_rows = await pg_conn.fetch(f"""
            SELECT user_chat_id, channel_id, channel_title, is_primary, is_active, created_at
            FROM "{schema}".user_channels;
        """)
        results["user_channels_pg"] = len(uc_rows)
        logger.info(f"Found {len(uc_rows)} linked user channels in PostgreSQL.")

        if not dry_run and uc_rows:
            ops = []
            for r in uc_rows:
                doc = {
                    "user_chat_id": int(r["user_chat_id"]),
                    "channel_id": int(r["channel_id"]),
                    "channel_title": r["channel_title"],
                    "is_primary": bool(r["is_primary"]),
                    "is_active": bool(r["is_active"]),
                    "created_at": r["created_at"]
                }
                ops.append(
                    UpdateOne(
                        {"user_chat_id": doc["user_chat_id"], "channel_id": doc["channel_id"]},
                        {"$set": doc},
                        upsert=True
                    )
                )
            if ops:
                res = await mongo_db.user_channels.bulk_write(ops)
                logger.info(f"MongoDB user_channels: upserted={len(res.upserted_ids)}, modified={res.modified_count}, matched={res.matched_count}")

        # ── Table 3: USER_PLATFORM_ROUTES ────────────────────────────────
        logger.info("\n--- Migrating 'user_platform_routes' ---")
        route_rows = await pg_conn.fetch(f"""
            SELECT user_chat_id, platform, channel_id, updated_at
            FROM "{schema}".user_platform_routes;
        """)
        results["user_platform_routes_pg"] = len(route_rows)
        logger.info(f"Found {len(route_rows)} platform routes in PostgreSQL.")

        if not dry_run and route_rows:
            ops = []
            for r in route_rows:
                doc = {
                    "user_chat_id": int(r["user_chat_id"]),
                    "platform": r["platform"],
                    "channel_id": int(r["channel_id"]),
                    "updated_at": r["updated_at"]
                }
                ops.append(
                    UpdateOne(
                        {"user_chat_id": doc["user_chat_id"], "platform": doc["platform"]},
                        {"$set": doc},
                        upsert=True
                    )
                )
            if ops:
                res = await mongo_db.user_platform_routes.bulk_write(ops)
                logger.info(f"MongoDB user_platform_routes: upserted={len(res.upserted_ids)}, modified={res.modified_count}, matched={res.matched_count}")

        # ── Table 4: USER_MEDIA_STORAGE ──────────────────────────────────
        logger.info("\n--- Migrating 'user_media_storage' ---")
        ums_rows = await pg_conn.fetch(f"""
            SELECT user_chat_id, channel_id, channel_msg_id, channel_post_url,
                   track_id, quality, telegram_file_id, is_available, platform,
                   destination_channel_id, created_at
            FROM "{schema}".user_media_storage;
        """)
        results["user_media_storage_pg"] = len(ums_rows)
        logger.info(f"Found {len(ums_rows)} user media storage records in PostgreSQL.")

        if not dry_run and ums_rows:
            ops = []
            for r in ums_rows:
                doc = {
                    "user_chat_id": int(r["user_chat_id"]),
                    "channel_id": int(r["channel_id"]),
                    "channel_msg_id": int(r["channel_msg_id"]),
                    "channel_post_url": r["channel_post_url"],
                    "track_id": r["track_id"],
                    "quality": r["quality"],
                    "telegram_file_id": r["telegram_file_id"],
                    "is_available": bool(r["is_available"]),
                    "platform": r["platform"] or "youtube",
                    "destination_channel_id": int(r["destination_channel_id"]) if r["destination_channel_id"] is not None else int(r["channel_id"]),
                    "created_at": r["created_at"]
                }
                ops.append(
                    UpdateOne(
                        {
                            "user_chat_id": doc["user_chat_id"],
                            "track_id": doc["track_id"],
                            "quality": doc["quality"]
                        },
                        {"$set": doc},
                        upsert=True
                    )
                )
            if ops:
                res = await mongo_db.user_media_storage.bulk_write(ops)
                logger.info(f"MongoDB user_media_storage: upserted={len(res.upserted_ids)}, modified={res.modified_count}, matched={res.matched_count}")

        # ── Table 5: TRACKS ──────────────────────────────────────────────
        logger.info("\n--- Migrating 'tracks' ---")
        track_rows = await pg_conn.fetch(f"""
            SELECT track_id, quality, title, artist, duration_secs, file_size_bytes,
                   thumbnail_url, source, created_at
            FROM "{schema}".tracks;
        """)
        results["tracks_pg"] = len(track_rows)
        logger.info(f"Found {len(track_rows)} tracks in PostgreSQL.")

        if not dry_run and track_rows:
            ops = []
            for r in track_rows:
                doc = {
                    "track_id": r["track_id"],
                    "quality": r["quality"],
                    "title": r["title"],
                    "artist": r["artist"],
                    "duration_secs": int(r["duration_secs"] or 0),
                    "file_size_bytes": int(r["file_size_bytes"] or 0),
                    "thumbnail_url": r["thumbnail_url"],
                    "source": r["source"] or "youtube",
                    "created_at": r["created_at"]
                }
                ops.append(
                    UpdateOne(
                        {"track_id": doc["track_id"], "quality": doc["quality"]},
                        {"$set": doc},
                        upsert=True
                    )
                )
            if ops:
                res = await mongo_db.tracks.bulk_write(ops)
                logger.info(f"MongoDB tracks: upserted={len(res.upserted_ids)}, modified={res.modified_count}, matched={res.matched_count}")

        # ── Table 6: API_CACHE ───────────────────────────────────────────
        logger.info("\n--- Migrating 'api_cache' ---")
        cache_rows = await pg_conn.fetch(f"""
            SELECT cache_key, response_json, expires_at, created_at
            FROM "{schema}".api_cache;
        """)
        results["api_cache_pg"] = len(cache_rows)
        logger.info(f"Found {len(cache_rows)} api_cache records in PostgreSQL.")

        if not dry_run and cache_rows:
            ops = []
            for r in cache_rows:
                raw_json = r["response_json"]
                if isinstance(raw_json, str):
                    try:
                        parsed_json = json.loads(raw_json)
                    except Exception:
                        parsed_json = raw_json
                else:
                    parsed_json = raw_json

                doc = {
                    "cache_key": r["cache_key"],
                    "response_json": parsed_json,
                    "expires_at": r["expires_at"],
                    "created_at": r["created_at"]
                }
                ops.append(
                    UpdateOne(
                        {"cache_key": doc["cache_key"]},
                        {"$set": doc},
                        upsert=True
                    )
                )
            if ops:
                res = await mongo_db.api_cache.bulk_write(ops)
                logger.info(f"MongoDB api_cache: upserted={len(res.upserted_ids)}, modified={res.modified_count}, matched={res.matched_count}")

        # ── Table 7: DOWNLOAD_HISTORY ────────────────────────────────────
        logger.info("\n--- Migrating 'download_history' ---")
        hist_rows = await pg_conn.fetch(f"""
            SELECT user_chat_id, track_id, title, quality, channel_msg_id, channel_post_url, created_at
            FROM "{schema}".download_history;
        """)
        results["download_history_pg"] = len(hist_rows)
        logger.info(f"Found {len(hist_rows)} download history records in PostgreSQL.")

        if not dry_run and hist_rows:
            ops = []
            for r in hist_rows:
                doc = {
                    "user_chat_id": int(r["user_chat_id"]),
                    "track_id": r["track_id"],
                    "title": r["title"],
                    "quality": r["quality"],
                    "channel_msg_id": int(r["channel_msg_id"]) if r["channel_msg_id"] else None,
                    "channel_post_url": r["channel_post_url"],
                    "created_at": r["created_at"]
                }
                ops.append(
                    UpdateOne(
                        {
                            "user_chat_id": doc["user_chat_id"],
                            "track_id": doc["track_id"],
                            "created_at": doc["created_at"]
                        },
                        {"$set": doc},
                        upsert=True
                    )
                )
            if ops:
                res = await mongo_db.download_history.bulk_write(ops)
                logger.info(f"MongoDB download_history: upserted={len(res.upserted_ids)}, matched={res.matched_count}")

        # ── Table 8: RATE_LIMITS ─────────────────────────────────────────
        logger.info("\n--- Migrating 'rate_limits' ---")
        rl_rows = await pg_conn.fetch(f"""
            SELECT user_chat_id, action, attempt_count, window_start
            FROM "{schema}".rate_limits;
        """)
        results["rate_limits_pg"] = len(rl_rows)
        logger.info(f"Found {len(rl_rows)} rate limit records in PostgreSQL.")

        if not dry_run and rl_rows:
            ops = []
            for r in rl_rows:
                doc = {
                    "user_chat_id": int(r["user_chat_id"]),
                    "action": r["action"],
                    "attempt_count": int(r["attempt_count"] or 1),
                    "window_start": r["window_start"]
                }
                ops.append(
                    UpdateOne(
                        {"user_chat_id": doc["user_chat_id"], "action": doc["action"]},
                        {"$set": doc},
                        upsert=True
                    )
                )
            if ops:
                res = await mongo_db.rate_limits.bulk_write(ops)
                logger.info(f"MongoDB rate_limits: upserted={len(res.upserted_ids)}, matched={res.matched_count}")

        # ── Ensure Indexes ───────────────────────────────────────────────
        if not dry_run:
            logger.info("\nEnsuring MongoDB indexes...")
            await mongo_db.users.create_index("chat_id", unique=True)
            await mongo_db.users.create_index("username")

            await mongo_db.user_channels.create_index([("user_chat_id", 1), ("channel_id", 1)], unique=True)
            await mongo_db.user_channels.create_index("user_chat_id")

            await mongo_db.user_platform_routes.create_index([("user_chat_id", 1), ("platform", 1)], unique=True)
            await mongo_db.user_platform_routes.create_index("user_chat_id")

            await mongo_db.user_media_storage.create_index(
                [("user_chat_id", 1), ("track_id", 1), ("quality", 1)], unique=True
            )
            await mongo_db.user_media_storage.create_index(
                [("track_id", 1), ("quality", 1), ("is_available", 1)]
            )
            await mongo_db.user_media_storage.create_index("user_chat_id")
            await mongo_db.user_media_storage.create_index("channel_id")

            await mongo_db.tracks.create_index([("track_id", 1), ("quality", 1)], unique=True)
            await mongo_db.tracks.create_index("track_id")

            await mongo_db.api_cache.create_index("cache_key", unique=True)
            await mongo_db.api_cache.create_index("expires_at")

            await mongo_db.download_history.create_index("user_chat_id")
            await mongo_db.download_history.create_index([("user_chat_id", 1), ("created_at", -1)])

            await mongo_db.rate_limits.create_index([("user_chat_id", 1), ("action", 1)], unique=True)
            logger.info("Indexes built successfully.")

        # ── Count Verification ───────────────────────────────────────────
        if not dry_run:
            results["users_mongo"] = await mongo_db.users.count_documents({})
            results["user_channels_mongo"] = await mongo_db.user_channels.count_documents({})
            results["user_platform_routes_mongo"] = await mongo_db.user_platform_routes.count_documents({})
            results["user_media_storage_mongo"] = await mongo_db.user_media_storage.count_documents({})
            results["tracks_mongo"] = await mongo_db.tracks.count_documents({})
            results["api_cache_mongo"] = await mongo_db.api_cache.count_documents({})
            results["download_history_mongo"] = await mongo_db.download_history.count_documents({})
            results["rate_limits_mongo"] = await mongo_db.rate_limits.count_documents({})
        else:
            for k in [
                "users", "user_channels", "user_platform_routes", "user_media_storage",
                "tracks", "api_cache", "download_history", "rate_limits"
            ]:
                results[f"{k}_mongo"] = 0

        results["target_database"] = target_db_name

        print("\n" + "=" * 65)
        print(f"MIGRATION SUMMARY (Target DB: {target_db_name})")
        print("=" * 65)
        all_matched = True
        tables = [
            ("users", "users_pg", "users_mongo"),
            ("user_channels", "user_channels_pg", "user_channels_mongo"),
            ("user_platform_routes", "user_platform_routes_pg", "user_platform_routes_mongo"),
            ("user_media_storage", "user_media_storage_pg", "user_media_storage_mongo"),
            ("tracks", "tracks_pg", "tracks_mongo"),
            ("api_cache", "api_cache_pg", "api_cache_mongo"),
            ("download_history", "download_history_pg", "download_history_mongo"),
            ("rate_limits", "rate_limits_pg", "rate_limits_mongo"),
        ]
        for name, pg_k, mongo_k in tables:
            pg_cnt = results.get(pg_k, 0)
            mongo_cnt = results.get(mongo_k, 0)
            status = "MATCH" if (pg_cnt == mongo_cnt or dry_run) else "MISMATCH"
            if not dry_run and pg_cnt != mongo_cnt:
                all_matched = False
            print(f"  {name:<25}: PG={pg_cnt:<5} -> Mongo={mongo_cnt:<5} [{status}]")
        print("=" * 65)
        results["success"] = all_matched
        return results

    finally:
        await pg_conn.close()
        mongo_client.close()


def main():
    parser = argparse.ArgumentParser(description="Migrate Sociobot from Neon PostgreSQL to MongoDB Atlas")
    parser.add_argument("--pg-url", default=None, help="Neon PostgreSQL Connection String")
    parser.add_argument("--mongo-url", default=None, help="MongoDB Atlas Connection String")
    parser.add_argument("--schema", default=None, help="PostgreSQL schema name (default: sociobot)")
    parser.add_argument("--db-name", default=None, help="Target MongoDB database name")
    parser.add_argument("--drop-target", action="store_true", help="Drop target MongoDB collections first")
    parser.add_argument("--dry-run", action="store_true", help="Dry run: inspect PG row counts without writing to MongoDB")

    args = parser.parse_args()

    # Determine PG URL
    pg_url = args.pg_url or settings.DATABASE_URL
    if not pg_url or not (pg_url.startswith("postgresql://") or pg_url.startswith("postgres://")):
        pg_url = DEFAULT_PG_URL

    # Determine Mongo URL
    mongo_url = args.mongo_url or getattr(settings, "MONGO_URL", None)
    if not mongo_url or not (mongo_url.startswith("mongodb://") or mongo_url.startswith("mongodb+srv://")):
        mongo_url = DEFAULT_MONGO_URL

    schema = args.schema or getattr(settings, "DB_SCHEMA", "sociobot") or "sociobot"

    res = asyncio.run(
        migrate(
            pg_url=pg_url,
            mongo_url=mongo_url,
            schema=schema,
            db_name=args.db_name,
            drop_target=args.drop_target,
            dry_run=args.dry_run
        )
    )

    if not res.get("success", False) and not args.dry_run:
        sys.exit(1)


if __name__ == "__main__":
    main()
