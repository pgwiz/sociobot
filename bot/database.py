"""Database Manager with Triple Support: MongoDB Atlas, Neon PostgreSQL & SQLite Fallback.

Decentralized Multi-Node Media Storage Architecture:
- Tracks individual user private storage channels.
- Records media posted across user channels with direct deep-links.
- Facilitates peer-to-peer media replication across channels.
- Handles node failure, soft/hard deletion, and channel unlinking.
- Dynamic multi-tenant database naming per bot token ID, custom URI, or schema.
"""

import asyncio
import json
import logging
import os
import re
import urllib.parse
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List, Tuple

import aiosqlite
import asyncpg

try:
    import motor.motor_asyncio
    import pymongo
    HAS_MOTOR = True
except ImportError:
    HAS_MOTOR = False

try:
    import certifi
    HAS_CERTIFI = True
except ImportError:
    HAS_CERTIFI = False

from bot.config import settings

logger = logging.getLogger(__name__)

NEON_TRANSIENT_ERRORS = (
    asyncpg.PostgresConnectionError,
    asyncpg.CannotConnectNowError,
    asyncpg.AdminShutdownError,
    asyncpg.InterfaceError,
    asyncpg.ConnectionDoesNotExistError,
    ConnectionResetError,
    ConnectionRefusedError,
    asyncio.TimeoutError,
    OSError
)


class Database:
    def __init__(self):
        self.pg_pool: Optional[asyncpg.Pool] = None
        self.sqlite_conn: Optional[aiosqlite.Connection] = None
        self.mongo_client: Optional[Any] = None
        self.mongo_db: Optional[Any] = None
        self._active_db_name: Optional[str] = None
        self._keepalive_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        self._is_shutting_down = False

    @property
    def schema(self) -> str:
        """Sanitized PostgreSQL custom schema name for table isolation."""
        s = (getattr(settings, "DB_SCHEMA", "sociobot") or "sociobot").strip().replace('"', '').replace("'", "").replace(";", "")
        return s or "sociobot"

    @property
    def is_mongo(self) -> bool:
        """Return True if DATABASE_URL or MONGO_URL specifies MongoDB."""
        db_url = settings.DATABASE_URL or getattr(settings, "MONGO_URL", "") or ""
        return db_url.startswith("mongodb://") or db_url.startswith("mongodb+srv://")

    @property
    def is_postgres(self) -> bool:
        """Return True if DATABASE_URL specifies PostgreSQL / Neon."""
        if self.is_mongo:
            return False
        db_url = settings.DATABASE_URL or ""
        return db_url.startswith("postgresql://") or db_url.startswith("postgres://")

    @property
    def is_connected(self) -> bool:
        return self.mongo_client is not None or self.pg_pool is not None or self.sqlite_conn is not None

    @staticmethod
    def resolve_mongo_db_name(
        uri: str = "",
        configured_db_name: Optional[str] = None,
        bot_token: Optional[str] = None,
        default_schema: Optional[str] = None
    ) -> str:
        """Resolve MongoDB database name.
        
        Resolution priority:
        1. Database name explicitly in URI path (e.g. mongodb+srv://.../dbname?retryWrites=true)
        2. Configured MONGO_DB_NAME env / setting
        3. Configured DB_SCHEMA (if set and not 'public')
        4. Bot token ID (e.g. bot_7391859147)
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

    async def connect(self) -> None:
        """Connect to MongoDB Atlas, Neon PostgreSQL, or SQLite based on configuration."""
        async with self._lock:
            if self.is_connected:
                return

            db_url = settings.DATABASE_URL or getattr(settings, "MONGO_URL", "") or ""

            # ── 1. Connect to MongoDB Atlas ──
            if self.is_mongo:
                if not HAS_MOTOR:
                    raise ImportError("MongoDB driver not installed. Run 'pip install motor pymongo dnspython'.")

                bot_tok = settings.TELEGRAM_BOT_TOKEN
                db_name = self.resolve_mongo_db_name(
                    uri=db_url,
                    configured_db_name=getattr(settings, "MONGO_DB_NAME", None),
                    bot_token=bot_tok,
                    default_schema=self.schema
                )
                self._active_db_name = db_name

                logger.info(f"Connecting to MongoDB cluster (target database: '{db_name}')...")
                max_retries = 3
                for attempt in range(1, max_retries + 1):
                    try:
                        client_kwargs: Dict[str, Any] = {
                            "serverSelectionTimeoutMS": 10000,
                            "connectTimeoutMS": 10000,
                            "socketTimeoutMS": 30000,
                            "maxPoolSize": getattr(settings, "DB_POOL_MAX_SIZE", 10),
                            "minPoolSize": getattr(settings, "DB_POOL_MIN_SIZE", 2),
                        }
                        if HAS_CERTIFI:
                            client_kwargs["tlsCAFile"] = certifi.where()

                        if getattr(settings, "MONGO_TLS_INSECURE", False) or "tlsallowinvalidcertificates=true" in db_url.lower():
                            client_kwargs["tlsAllowInvalidCertificates"] = True

                        self.mongo_client = motor.motor_asyncio.AsyncIOMotorClient(
                            db_url,
                            **client_kwargs
                        )
                        # Verify connectivity
                        await self.mongo_client.admin.command("ping")
                        self.mongo_db = self.mongo_client[db_name]
                        logger.info(f"MongoDB connected successfully to database: '{db_name}'.")
                        await self._migrate()
                        return
                    except Exception as e:
                        err_msg = str(e)
                        logger.warning(f"MongoDB connection attempt {attempt} failed: {err_msg}")
                        if "tlsv1 alert internal error" in err_msg.lower() or "ssl handshake failed" in err_msg.lower():
                            logger.error(
                                "MongoDB SSL/TLS Alert: This almost always means MongoDB Atlas Network Access (IP Access List) "
                                "is blocking this server's dynamic IP address. If deploying on Render, Heroku, or cloud containers, "
                                "add '0.0.0.0/0' (Allow access from anywhere) in MongoDB Atlas Dashboard -> Security -> Network Access."
                            )
                        if attempt < max_retries:
                            await asyncio.sleep(2.0)
                        else:
                            logger.error(f"Failed to connect to MongoDB after {max_retries} attempts.")
                            raise

            # ── 2. Connect to Neon PostgreSQL ──
            elif self.is_postgres:
                max_retries = 5
                base_delay = 1.5
                schema = self.schema
                self._is_shutting_down = False

                # Pool setup hook enforces search_path on every checkout across resets
                async def _setup_connection(conn):
                    await conn.execute(f'SET search_path TO "{schema}", public;')

                clean_pg_url = db_url.replace("&channel_binding=require", "")

                for attempt in range(1, max_retries + 1):
                    try:
                        logger.info(f"Connecting to Neon PostgreSQL (schema: '{schema}', attempt {attempt}/{max_retries})...")
                        self.pg_pool = await asyncpg.create_pool(
                            dsn=clean_pg_url,
                            min_size=settings.DB_POOL_MIN_SIZE,
                            max_size=settings.DB_POOL_MAX_SIZE,
                            timeout=25.0,
                            command_timeout=25.0,
                            statement_cache_size=0,  # Required for PgBouncer / Neon connection pooling
                            max_inactive_connection_lifetime=300.0,
                            setup=_setup_connection,
                            server_settings={"search_path": f"{schema},public"}
                        )

                        # Test connectivity and pre-create custom schema
                        async with self.pg_pool.acquire() as conn:
                            await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')
                            await conn.fetchval("SELECT 1;")

                        logger.info(f"Neon PostgreSQL connected and active (schema: '{schema}').")
                        await self._migrate()

                        if getattr(settings, "ENABLE_NEON_KEEPALIVE", False) and not self._keepalive_task:
                            self._keepalive_task = asyncio.create_task(self._keepalive_loop())
                        return

                    except NEON_TRANSIENT_ERRORS as e:
                        err_str = str(e).strip() or repr(e)
                        logger.warning(
                            f"Neon cold-start delay on attempt {attempt}/{max_retries}: {type(e).__name__}: {err_str}"
                        )
                        if attempt < max_retries:
                            sleep_time = base_delay * (2 ** (attempt - 1))
                            logger.info(f"Waiting {sleep_time:.1f}s for Neon compute to spin up...")
                            await asyncio.sleep(sleep_time)
                        else:
                            logger.error("Failed to connect to Neon PostgreSQL after retries.")
                            raise

            # ── 3. Connect to SQLite (Fallback) ──
            else:
                db_path = settings.DATABASE_PATH or "sociobot.db"
                if db_url.startswith("sqlite:///"):
                    db_path = db_url.replace("sqlite:///", "")
                elif db_url.startswith("sqlite://"):
                    db_path = db_url.replace("sqlite://", "")

                logger.info(f"Connecting to SQLite database at {db_path}...")
                dir_name = os.path.dirname(os.path.abspath(db_path))
                if dir_name:
                    os.makedirs(dir_name, exist_ok=True)

                self.sqlite_conn = await aiosqlite.connect(db_path)
                self.sqlite_conn.row_factory = aiosqlite.Row
                await self.sqlite_conn.execute("PRAGMA journal_mode=WAL;")
                await self.sqlite_conn.execute("PRAGMA synchronous=NORMAL;")
                logger.info("SQLite database connected in WAL mode.")
                await self._migrate()

    async def disconnect(self) -> None:
        """Gracefully release database resources."""
        self._is_shutting_down = True
        if self._keepalive_task and not self._keepalive_task.done():
            self._keepalive_task.cancel()
            try:
                await self._keepalive_task
            except asyncio.CancelledError:
                pass
            self._keepalive_task = None

        if self.mongo_client:
            self.mongo_client.close()
            self.mongo_client = None
            self.mongo_db = None
            logger.info("MongoDB client connection closed.")

        if self.pg_pool:
            try:
                await asyncio.wait_for(self.pg_pool.close(), timeout=3.0)
            except (asyncio.TimeoutError, Exception):
                self.pg_pool.terminate()
            self.pg_pool = None
            logger.info("Neon PostgreSQL pool closed.")

        if self.sqlite_conn:
            await self.sqlite_conn.close()
            self.sqlite_conn = None
            logger.info("SQLite connection closed.")

    async def _keepalive_loop(self) -> None:
        """Lightweight background heartbeat every 4m to prevent Neon cold start during active use."""
        while not self._is_shutting_down:
            try:
                await asyncio.sleep(240)
                if self.pg_pool and not self._is_shutting_down:
                    async with self.pg_pool.acquire() as conn:
                        await conn.fetchval("SELECT 1;")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"Neon keepalive ping failed (sleeping): {e}")

    async def _execute_pg_with_retry(self, callback, retries: int = 3):
        """Execute a PostgreSQL query with automatic retry on transient disconnect."""
        if self._is_shutting_down or not self.pg_pool:
            raise RuntimeError("Database connection pool is shutting down or closed.")

        for attempt in range(1, retries + 1):
            if self._is_shutting_down:
                raise RuntimeError("Database connection pool is shutting down.")
            try:
                async with self.pg_pool.acquire() as conn:
                    await conn.execute(f'SET search_path TO "{self.schema}", public;')
                    return await callback(conn)
            except NEON_TRANSIENT_ERRORS as e:
                err_repr = str(e).strip() or repr(e)
                logger.warning(
                    f"Transient DB error during query (attempt {attempt}/{retries}): {type(e).__name__}: {err_repr}"
                )
                if self._is_shutting_down:
                    raise
                if attempt < retries:
                    if self.pg_pool:
                        try:
                            self.pg_pool.terminate()
                        except Exception:
                            pass
                        self.pg_pool = None
                    await asyncio.sleep(1.0)
                    await self.connect()
                else:
                    raise

    async def _migrate(self) -> None:
        """Run schema migrations for MongoDB, PostgreSQL, or SQLite."""
        # ── MongoDB Indexes ──
        if self.is_mongo:
            logger.info(f"Ensuring MongoDB indexes on database '{self.mongo_db.name}'...")
            await self.mongo_db.users.create_index("chat_id", unique=True)
            await self.mongo_db.users.create_index("username")

            await self.mongo_db.user_channels.create_index([("user_chat_id", 1), ("channel_id", 1)], unique=True)
            await self.mongo_db.user_channels.create_index("user_chat_id")

            await self.mongo_db.user_platform_routes.create_index([("user_chat_id", 1), ("platform", 1)], unique=True)
            await self.mongo_db.user_platform_routes.create_index("user_chat_id")

            await self.mongo_db.user_media_storage.create_index(
                [("user_chat_id", 1), ("track_id", 1), ("quality", 1)], unique=True
            )
            await self.mongo_db.user_media_storage.create_index(
                [("track_id", 1), ("quality", 1), ("is_available", 1)]
            )
            await self.mongo_db.user_media_storage.create_index("user_chat_id")
            await self.mongo_db.user_media_storage.create_index("channel_id")

            await self.mongo_db.tracks.create_index([("track_id", 1), ("quality", 1)], unique=True)
            await self.mongo_db.tracks.create_index("track_id")

            await self.mongo_db.api_cache.create_index("cache_key", unique=True)
            await self.mongo_db.api_cache.create_index("expires_at")

            await self.mongo_db.download_history.create_index("user_chat_id")
            await self.mongo_db.download_history.create_index([("user_chat_id", 1), ("created_at", -1)])

            await self.mongo_db.rate_limits.create_index([("user_chat_id", 1), ("action", 1)], unique=True)
            logger.info(f"MongoDB indexes verified successfully on '{self.mongo_db.name}'.")

        # ── PostgreSQL Migrations ──
        elif self.is_postgres:
            schema = self.schema

            async def _run(conn):
                await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')
                await conn.execute(f'SET search_path TO "{schema}", public;')
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        chat_id BIGINT PRIMARY KEY,
                        username TEXT,
                        first_name TEXT,
                        channel_id BIGINT,
                        channel_title TEXT,
                        is_storage_active BOOLEAN DEFAULT TRUE,
                        terms_accepted BOOLEAN DEFAULT FALSE,
                        is_admin BOOLEAN DEFAULT FALSE,
                        is_banned BOOLEAN DEFAULT FALSE,
                        max_channels INT DEFAULT 5,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        last_active TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    );
                    CREATE TABLE IF NOT EXISTS user_channels (
                        id BIGSERIAL PRIMARY KEY,
                        user_chat_id BIGINT NOT NULL,
                        channel_id BIGINT NOT NULL,
                        channel_title TEXT NOT NULL,
                        is_primary BOOLEAN DEFAULT FALSE,
                        is_active BOOLEAN DEFAULT TRUE,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        CONSTRAINT unique_user_channel UNIQUE (user_chat_id, channel_id)
                    );

                    CREATE TABLE IF NOT EXISTS user_platform_routes (
                        user_chat_id BIGINT NOT NULL,
                        platform TEXT NOT NULL,
                        channel_id BIGINT NOT NULL,
                        updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        PRIMARY KEY (user_chat_id, platform)
                    );

                    ALTER TABLE users ADD COLUMN IF NOT EXISTS is_banned BOOLEAN DEFAULT FALSE;
                    ALTER TABLE users ADD COLUMN IF NOT EXISTS max_channels INT DEFAULT 5;

                    CREATE TABLE IF NOT EXISTS user_media_storage (
                        id BIGSERIAL PRIMARY KEY,
                        user_chat_id BIGINT NOT NULL,
                        channel_id BIGINT NOT NULL,
                        channel_msg_id BIGINT NOT NULL,
                        channel_post_url TEXT NOT NULL,
                        track_id TEXT NOT NULL,
                        quality TEXT NOT NULL,
                        telegram_file_id TEXT,
                        is_available BOOLEAN DEFAULT TRUE,
                        platform TEXT DEFAULT 'youtube',
                        destination_channel_id BIGINT,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        CONSTRAINT unique_user_track_quality UNIQUE(user_chat_id, track_id, quality)
                    );
                    ALTER TABLE user_media_storage ADD COLUMN IF NOT EXISTS platform TEXT DEFAULT 'youtube';
                    ALTER TABLE user_media_storage ADD COLUMN IF NOT EXISTS destination_channel_id BIGINT;

                    CREATE TABLE IF NOT EXISTS tracks (
                        track_id TEXT NOT NULL,
                        quality TEXT NOT NULL,
                        title TEXT,
                        artist TEXT,
                        duration_secs INTEGER DEFAULT 0,
                        file_size_bytes BIGINT DEFAULT 0,
                        thumbnail_url TEXT,
                        source TEXT DEFAULT 'youtube',
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        PRIMARY KEY (track_id, quality)
                    );

                    CREATE TABLE IF NOT EXISTS api_cache (
                        cache_key TEXT PRIMARY KEY,
                        response_json JSONB,
                        expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    );

                    CREATE TABLE IF NOT EXISTS download_history (
                        id BIGSERIAL PRIMARY KEY,
                        user_chat_id BIGINT NOT NULL,
                        track_id TEXT NOT NULL,
                        title TEXT,
                        quality TEXT,
                        channel_msg_id BIGINT,
                        channel_post_url TEXT,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    );

                    CREATE TABLE IF NOT EXISTS rate_limits (
                        user_chat_id BIGINT NOT NULL,
                        action TEXT NOT NULL,
                        attempt_count INTEGER DEFAULT 1,
                        window_start TIMESTAMP WITH TIME ZONE NOT NULL,
                        PRIMARY KEY (user_chat_id, action)
                    );

                    CREATE INDEX IF NOT EXISTS idx_storage_lookup ON user_media_storage(track_id, quality, is_available);
                    CREATE INDEX IF NOT EXISTS idx_storage_user ON user_media_storage(user_chat_id);
                    CREATE INDEX IF NOT EXISTS idx_storage_channel ON user_media_storage(channel_id);
                    CREATE INDEX IF NOT EXISTS idx_api_cache_expires ON api_cache(expires_at);
                    CREATE INDEX IF NOT EXISTS idx_history_user ON download_history(user_chat_id);
                    CREATE INDEX IF NOT EXISTS idx_user_channels_user ON user_channels(user_chat_id);
                    CREATE INDEX IF NOT EXISTS idx_platform_routes_user ON user_platform_routes(user_chat_id);

                    INSERT INTO user_channels (user_chat_id, channel_id, channel_title, is_primary, is_active)
                    SELECT chat_id, channel_id, COALESCE(channel_title, 'My Storage Channel'), TRUE, TRUE
                    FROM users
                    WHERE channel_id IS NOT NULL
                    ON CONFLICT (user_chat_id, channel_id) DO NOTHING;
                """)
            await self._execute_pg_with_retry(_run)
            logger.info(f"Neon PostgreSQL schema migrations applied successfully to schema '{schema}'.")

        # ── SQLite Migrations ──
        else:
            await self.sqlite_conn.executescript("""
                CREATE TABLE IF NOT EXISTS users (
                    chat_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    channel_id INTEGER,
                    channel_title TEXT,
                    is_storage_active INTEGER DEFAULT 1,
                    terms_accepted INTEGER DEFAULT 0,
                    is_admin INTEGER DEFAULT 0,
                    is_banned INTEGER DEFAULT 0,
                    max_channels INTEGER DEFAULT 5,
                    created_at TEXT DEFAULT (datetime('now')),
                    last_active TEXT DEFAULT (datetime('now'))
                );

                CREATE TABLE IF NOT EXISTS user_channels (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_chat_id INTEGER NOT NULL,
                    channel_id INTEGER NOT NULL,
                    channel_title TEXT NOT NULL,
                    is_primary INTEGER DEFAULT 0,
                    is_active INTEGER DEFAULT 1,
                    created_at TEXT DEFAULT (datetime('now')),
                    UNIQUE(user_chat_id, channel_id)
                );

                CREATE TABLE IF NOT EXISTS user_platform_routes (
                    user_chat_id INTEGER NOT NULL,
                    platform TEXT NOT NULL,
                    channel_id INTEGER NOT NULL,
                    updated_at TEXT DEFAULT (datetime('now')),
                    PRIMARY KEY (user_chat_id, platform)
                );

                CREATE TABLE IF NOT EXISTS user_media_storage (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_chat_id INTEGER NOT NULL,
                    channel_id INTEGER NOT NULL,
                    channel_msg_id INTEGER NOT NULL,
                    channel_post_url TEXT NOT NULL,
                    track_id TEXT NOT NULL,
                    quality TEXT NOT NULL,
                    telegram_file_id TEXT,
                    is_available INTEGER DEFAULT 1,
                    platform TEXT DEFAULT 'youtube',
                    destination_channel_id INTEGER,
                    created_at TEXT DEFAULT (datetime('now')),
                    UNIQUE(user_chat_id, track_id, quality)
                );

                CREATE TABLE IF NOT EXISTS tracks (
                    track_id TEXT NOT NULL,
                    quality TEXT NOT NULL,
                    title TEXT,
                    artist TEXT,
                    duration_secs INTEGER DEFAULT 0,
                    file_size_bytes INTEGER DEFAULT 0,
                    thumbnail_url TEXT,
                    source TEXT DEFAULT 'youtube',
                    created_at TEXT DEFAULT (datetime('now')),
                    PRIMARY KEY (track_id, quality)
                );

                CREATE TABLE IF NOT EXISTS api_cache (
                    cache_key TEXT PRIMARY KEY,
                    response_json TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    created_at TEXT DEFAULT (datetime('now'))
                );

                CREATE TABLE IF NOT EXISTS download_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_chat_id INTEGER NOT NULL,
                    track_id TEXT NOT NULL,
                    title TEXT,
                    quality TEXT,
                    channel_msg_id INTEGER,
                    channel_post_url TEXT,
                    created_at TEXT DEFAULT (datetime('now'))
                );

                CREATE TABLE IF NOT EXISTS rate_limits (
                    user_chat_id INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    attempt_count INTEGER DEFAULT 1,
                    window_start TEXT NOT NULL,
                    PRIMARY KEY (user_chat_id, action)
                );

                CREATE INDEX IF NOT EXISTS idx_storage_lookup ON user_media_storage(track_id, quality, is_available);
                CREATE INDEX IF NOT EXISTS idx_storage_user ON user_media_storage(user_chat_id);
                CREATE INDEX IF NOT EXISTS idx_storage_channel ON user_media_storage(channel_id);
                CREATE INDEX IF NOT EXISTS idx_api_cache_expires ON api_cache(expires_at);
                CREATE INDEX IF NOT EXISTS idx_history_user ON download_history(user_chat_id);
                CREATE INDEX IF NOT EXISTS idx_user_channels_user ON user_channels(user_chat_id);
                CREATE INDEX IF NOT EXISTS idx_platform_routes_user ON user_platform_routes(user_chat_id);

                INSERT OR IGNORE INTO user_channels (user_chat_id, channel_id, channel_title, is_primary, is_active)
                SELECT chat_id, channel_id, COALESCE(channel_title, 'My Storage Channel'), 1, 1
                FROM users
                WHERE channel_id IS NOT NULL;
            """)
            await self.sqlite_conn.commit()
            logger.info("SQLite schema migrations applied successfully.")

    # ── User Onboarding & Channel Configuration ───────────────────────────

    async def get_or_create_user(
        self,
        chat_id: int,
        username: Optional[str] = None,
        first_name: Optional[str] = None,
        is_admin: bool = False
    ) -> Dict[str, Any]:
        """Fetch or insert a user record, updating activity timestamp."""
        if not self.is_connected:
            return {"chat_id": chat_id, "username": username, "first_name": first_name, "is_admin": is_admin, "is_banned": False, "max_channels": 5}

        if self.is_mongo:
            now = datetime.now(timezone.utc)
            set_doc = {
                "username": username,
                "first_name": first_name,
                "last_active": now
            }
            if is_admin:
                set_doc["is_admin"] = True

            res = await self.mongo_db.users.find_one_and_update(
                {"chat_id": chat_id},
                {
                    "$set": set_doc,
                    "$setOnInsert": {
                        "chat_id": chat_id,
                        "channel_id": None,
                        "channel_title": None,
                        "is_storage_active": True,
                        "terms_accepted": False,
                        "is_admin": is_admin,
                        "is_banned": False,
                        "max_channels": 5,
                        "created_at": now
                    }
                },
                upsert=True,
                return_document=pymongo.ReturnDocument.AFTER
            )
            if res:
                d = dict(res)
                d.pop("_id", None)
                d["is_storage_active"] = bool(d.get("is_storage_active", True))
                d["terms_accepted"] = bool(d.get("terms_accepted", False))
                d["is_admin"] = bool(d.get("is_admin", False))
                d["is_banned"] = bool(d.get("is_banned", False))
                d["max_channels"] = int(d.get("max_channels") or 5)
                return d
            return {}
        elif self.is_postgres:
            async def _run(conn):
                row = await conn.fetchrow(
                    """
                    INSERT INTO users (chat_id, username, first_name, is_admin, last_active)
                    VALUES ($1, $2, $3, $4, NOW())
                    ON CONFLICT (chat_id) DO UPDATE SET
                        username = EXCLUDED.username,
                        first_name = EXCLUDED.first_name,
                        last_active = NOW(),
                        is_admin = CASE WHEN users.is_admin THEN TRUE ELSE EXCLUDED.is_admin END
                    RETURNING chat_id, username, first_name, channel_id, channel_title, is_storage_active, terms_accepted, is_admin, is_banned, max_channels;
                    """,
                    chat_id, username, first_name, is_admin
                )
                return dict(row) if row else {}
            return await self._execute_pg_with_retry(_run)
        else:
            await self.sqlite_conn.execute(
                """
                INSERT INTO users (chat_id, username, first_name, is_admin, last_active)
                VALUES (?, ?, ?, ?, datetime('now'))
                ON CONFLICT (chat_id) DO UPDATE SET
                    username = excluded.username,
                    first_name = excluded.first_name,
                    last_active = datetime('now');
                """,
                (chat_id, username, first_name, 1 if is_admin else 0)
            )
            await self.sqlite_conn.commit()
            cursor = await self.sqlite_conn.execute(
                "SELECT chat_id, username, first_name, channel_id, channel_title, is_storage_active, terms_accepted, is_admin, is_banned, max_channels FROM users WHERE chat_id = ?;",
                (chat_id,)
            )
            row = await cursor.fetchone()
            if row:
                d = dict(row)
                d["is_storage_active"] = bool(d.get("is_storage_active", 1))
                d["terms_accepted"] = bool(d.get("terms_accepted", 0))
                d["is_admin"] = bool(d.get("is_admin", 0))
                d["is_banned"] = bool(d.get("is_banned", 0))
                d["max_channels"] = int(d.get("max_channels") or 5)
                return d
            return {}

    async def link_user_channel(self, chat_id: int, channel_id: int, channel_title: str) -> None:
        """Register or update a user's private storage channel (syncs user_channels and users)."""
        if not self.is_connected:
            return

        try:
            await self.add_user_channel(chat_id, channel_id, channel_title, set_primary=True)
        except Exception as e:
            logger.warning(f"Error in link_user_channel adding to user_channels: {e}")

        if self.is_mongo:
            await self.mongo_db.users.update_one(
                {"chat_id": chat_id},
                {"$set": {"channel_id": channel_id, "channel_title": channel_title, "is_storage_active": True}}
            )
        elif self.is_postgres:
            async def _run(conn):
                await conn.execute(
                    """
                    UPDATE users
                    SET channel_id = $1, channel_title = $2, is_storage_active = TRUE
                    WHERE chat_id = $3;
                    """,
                    channel_id, channel_title, chat_id
                )
            await self._execute_pg_with_retry(_run)
        else:
            await self.sqlite_conn.execute(
                """
                UPDATE users
                SET channel_id = ?, channel_title = ?, is_storage_active = 1
                WHERE chat_id = ?;
                """,
                (channel_id, channel_title, chat_id)
            )
            await self.sqlite_conn.commit()

    async def unlink_user_channel(self, chat_id: int) -> None:
        """Unlink channel and pause personal storage."""
        if not self.is_connected:
            return

        if self.is_mongo:
            await self.mongo_db.users.update_one(
                {"chat_id": chat_id},
                {"$set": {"is_storage_active": False}}
            )
            await self.mongo_db.user_channels.update_many(
                {"user_chat_id": chat_id},
                {"$set": {"is_active": False}}
            )
        elif self.is_postgres:
            async def _run(conn):
                await conn.execute(
                    """
                    UPDATE users
                    SET is_storage_active = FALSE
                    WHERE chat_id = $1;
                    """,
                    chat_id
                )
                await conn.execute("UPDATE user_channels SET is_active = FALSE WHERE user_chat_id = $1;", chat_id)
            await self._execute_pg_with_retry(_run)
        else:
            await self.sqlite_conn.execute(
                """
                UPDATE users
                SET is_storage_active = 0
                WHERE chat_id = ?;
                """,
                (chat_id,)
            )
            await self.sqlite_conn.execute("UPDATE user_channels SET is_active = 0 WHERE user_chat_id = ?;", (chat_id,))
            await self.sqlite_conn.commit()

    async def set_terms_accepted(self, chat_id: int, accepted: bool = True) -> None:
        """Update polite onboarding terms acknowledgment."""
        if not self.is_connected:
            return

        if self.is_mongo:
            await self.mongo_db.users.update_one(
                {"chat_id": chat_id},
                {"$set": {"terms_accepted": accepted}}
            )
        elif self.is_postgres:
            async def _run(conn):
                await conn.execute(
                    "UPDATE users SET terms_accepted = $1 WHERE chat_id = $2;",
                    accepted, chat_id
                )
            await self._execute_pg_with_retry(_run)
        else:
            await self.sqlite_conn.execute(
                "UPDATE users SET terms_accepted = ? WHERE chat_id = ?;",
                (1 if accepted else 0, chat_id)
            )
            await self.sqlite_conn.commit()

    async def get_user_channel(self, chat_id: int) -> Optional[Dict[str, Any]]:
        """Retrieve primary user channel info and storage readiness (backward-compatible)."""
        if not self.is_connected:
            return None

        # 1. Prefer active primary from user_channels
        channels = await self.get_user_channels(chat_id)
        if channels:
            primary = next((c for c in channels if c.get("is_primary")), channels[0])
            user_terms = False
            if self.is_mongo:
                u = await self.mongo_db.users.find_one({"chat_id": chat_id}, {"terms_accepted": 1})
                user_terms = bool(u.get("terms_accepted")) if u else False
            elif self.is_postgres:
                async def _get_terms(conn):
                    return await conn.fetchval("SELECT terms_accepted FROM users WHERE chat_id = $1;", chat_id)
                user_terms = bool(await self._execute_pg_with_retry(_get_terms) or False)
            else:
                cur = await self.sqlite_conn.execute("SELECT terms_accepted FROM users WHERE chat_id = ?;", (chat_id,))
                r = await cur.fetchone()
                user_terms = bool(r[0]) if r else False

            return {
                "channel_id": primary["channel_id"],
                "channel_title": primary["channel_title"],
                "is_storage_active": primary.get("is_active", True),
                "terms_accepted": user_terms,
                "is_primary": True
            }

        # 2. Fallback to legacy users table
        if self.is_mongo:
            row = await self.mongo_db.users.find_one(
                {"chat_id": chat_id},
                {"channel_id": 1, "channel_title": 1, "is_storage_active": 1, "terms_accepted": 1}
            )
            if row:
                d = dict(row)
                d.pop("_id", None)
                d["is_storage_active"] = bool(d.get("is_storage_active", False))
                d["terms_accepted"] = bool(d.get("terms_accepted", False))
                return d
            return None
        elif self.is_postgres:
            async def _run(conn):
                row = await conn.fetchrow(
                    "SELECT channel_id, channel_title, is_storage_active, terms_accepted FROM users WHERE chat_id = $1;",
                    chat_id
                )
                return dict(row) if row else None
            return await self._execute_pg_with_retry(_run)
        else:
            cursor = await self.sqlite_conn.execute(
                "SELECT channel_id, channel_title, is_storage_active, terms_accepted FROM users WHERE chat_id = ?;",
                (chat_id,)
            )
            row = await cursor.fetchone()
            if row:
                d = dict(row)
                d["is_storage_active"] = bool(d.get("is_storage_active", 0))
                d["terms_accepted"] = bool(d.get("terms_accepted", 0))
                return d
            return None

    # ── Multi-Channel & Platform Routing (v2.0) ──────────────────────────

    async def get_user_max_channels(self, user_chat_id: int) -> int:
        """Return the maximum number of storage channels allowed for a user."""
        if not self.is_connected:
            return getattr(settings, "DEFAULT_MAX_CHANNELS", 5)

        default_max = getattr(settings, "DEFAULT_MAX_CHANNELS", 5)
        if self.is_mongo:
            u = await self.mongo_db.users.find_one({"chat_id": user_chat_id}, {"max_channels": 1})
            return int(u["max_channels"]) if (u and u.get("max_channels") is not None) else default_max
        elif self.is_postgres:
            async def _run(conn):
                val = await conn.fetchval("SELECT max_channels FROM users WHERE chat_id = $1;", user_chat_id)
                return int(val) if val is not None else default_max
            return await self._execute_pg_with_retry(_run)
        else:
            cur = await self.sqlite_conn.execute("SELECT max_channels FROM users WHERE chat_id = ?;", (user_chat_id,))
            row = await cur.fetchone()
            return int(row[0]) if (row and row[0] is not None) else default_max

    async def upgrade_user_quota(self, user_chat_id: int, max_channels: int) -> bool:
        """Upgrade or update a user's channel quota."""
        if not self.is_connected:
            return False

        if self.is_mongo:
            await self.mongo_db.users.update_one({"chat_id": user_chat_id}, {"$set": {"max_channels": max_channels}})
            return True
        elif self.is_postgres:
            async def _run(conn):
                await conn.execute("UPDATE users SET max_channels = $1 WHERE chat_id = $2;", max_channels, user_chat_id)
                return True
            return await self._execute_pg_with_retry(_run)
        else:
            await self.sqlite_conn.execute("UPDATE users SET max_channels = ? WHERE chat_id = ?;", (max_channels, user_chat_id))
            await self.sqlite_conn.commit()
            return True

    async def get_channel_count(self, user_chat_id: int) -> int:
        """Count active channels linked to a user."""
        if not self.is_connected:
            return 0

        if self.is_mongo:
            return await self.mongo_db.user_channels.count_documents({"user_chat_id": user_chat_id, "is_active": True})
        elif self.is_postgres:
            async def _run(conn):
                val = await conn.fetchval(
                    "SELECT COUNT(*) FROM user_channels WHERE user_chat_id = $1 AND is_active = TRUE;",
                    user_chat_id
                )
                return int(val or 0)
            return await self._execute_pg_with_retry(_run)
        else:
            cur = await self.sqlite_conn.execute(
                "SELECT COUNT(*) FROM user_channels WHERE user_chat_id = ? AND is_active = 1;",
                (user_chat_id,)
            )
            row = await cur.fetchone()
            return int(row[0] or 0) if row else 0

    async def get_user_channels(self, user_chat_id: int) -> List[Dict[str, Any]]:
        """Return list of all active storage channels for a user, primary first."""
        if not self.is_connected:
            return []

        if self.is_mongo:
            cursor = self.mongo_db.user_channels.find(
                {"user_chat_id": user_chat_id, "is_active": True}
            ).sort([("is_primary", -1), ("_id", 1)])
            docs = await cursor.to_list(length=100)
            res = []
            for idx, d in enumerate(docs, 1):
                item = dict(d)
                item["id"] = idx
                item.pop("_id", None)
                item["is_primary"] = bool(item.get("is_primary", False))
                item["is_active"] = bool(item.get("is_active", True))
                res.append(item)
            return res
        elif self.is_postgres:
            async def _run(conn):
                rows = await conn.fetch(
                    """
                    SELECT id, user_chat_id, channel_id, channel_title, is_primary, is_active, created_at
                    FROM user_channels
                    WHERE user_chat_id = $1 AND is_active = TRUE
                    ORDER BY is_primary DESC, id ASC;
                    """,
                    user_chat_id
                )
                return [dict(r) for r in rows]
            return await self._execute_pg_with_retry(_run)
        else:
            cur = await self.sqlite_conn.execute(
                """
                SELECT id, user_chat_id, channel_id, channel_title, is_primary, is_active, created_at
                FROM user_channels
                WHERE user_chat_id = ? AND is_active = 1
                ORDER BY is_primary DESC, id ASC;
                """,
                (user_chat_id,)
            )
            rows = await cur.fetchall()
            result = []
            for r in rows:
                d = dict(r)
                d["is_primary"] = bool(d.get("is_primary", 0))
                d["is_active"] = bool(d.get("is_active", 1))
                result.append(d)
            return result

    async def add_user_channel(
        self,
        user_chat_id: int,
        channel_id: int,
        channel_title: str,
        set_primary: bool = False
    ) -> Dict[str, Any]:
        """
        Link a channel to a user's vault network.
        Enforces channel quota. Automatically sets primary if first channel or set_primary=True.
        """
        if not self.is_connected:
            raise RuntimeError("Database disconnected.")

        existing_channels = await self.get_user_channels(user_chat_id)
        already_linked = any(c["channel_id"] == channel_id for c in existing_channels)

        if not already_linked:
            max_ch = await self.get_user_max_channels(user_chat_id)
            if len(existing_channels) >= max_ch:
                raise ValueError(f"Channel quota exceeded (max {max_ch}). Upgrade quota with admin.")

        # If it's the user's first channel, always make it primary
        if not existing_channels:
            set_primary = True

        if self.is_mongo:
            now = datetime.now(timezone.utc)
            if set_primary:
                await self.mongo_db.user_channels.update_many(
                    {"user_chat_id": user_chat_id},
                    {"$set": {"is_primary": False}}
                )
            await self.mongo_db.user_channels.update_one(
                {"user_chat_id": user_chat_id, "channel_id": channel_id},
                {
                    "$set": {
                        "channel_title": channel_title,
                        "is_primary": set_primary,
                        "is_active": True
                    },
                    "$setOnInsert": {
                        "user_chat_id": user_chat_id,
                        "channel_id": channel_id,
                        "created_at": now
                    }
                },
                upsert=True
            )
            if set_primary:
                await self.mongo_db.users.update_one(
                    {"chat_id": user_chat_id},
                    {"$set": {"channel_id": channel_id, "channel_title": channel_title, "is_storage_active": True}}
                )
            return {
                "user_chat_id": user_chat_id,
                "channel_id": channel_id,
                "channel_title": channel_title,
                "is_primary": set_primary,
                "is_active": True
            }
        elif self.is_postgres:
            async def _run(conn):
                if set_primary:
                    await conn.execute(
                        "UPDATE user_channels SET is_primary = FALSE WHERE user_chat_id = $1;",
                        user_chat_id
                    )
                row = await conn.fetchrow(
                    """
                    INSERT INTO user_channels (user_chat_id, channel_id, channel_title, is_primary, is_active, created_at)
                    VALUES ($1, $2, $3, $4, TRUE, NOW())
                    ON CONFLICT (user_chat_id, channel_id) DO UPDATE SET
                        channel_title = EXCLUDED.channel_title,
                        is_primary = CASE WHEN $4 THEN TRUE ELSE user_channels.is_primary END,
                        is_active = TRUE
                    RETURNING id, user_chat_id, channel_id, channel_title, is_primary, is_active;
                    """,
                    user_chat_id, channel_id, channel_title, set_primary
                )
                if set_primary:
                    await conn.execute(
                        "UPDATE users SET channel_id = $1, channel_title = $2, is_storage_active = TRUE WHERE chat_id = $3;",
                        channel_id, channel_title, user_chat_id
                    )
                return dict(row) if row else {}
            return await self._execute_pg_with_retry(_run)
        else:
            if set_primary:
                await self.sqlite_conn.execute(
                    "UPDATE user_channels SET is_primary = 0 WHERE user_chat_id = ?;",
                    (user_chat_id,)
                )
            await self.sqlite_conn.execute(
                """
                INSERT INTO user_channels (user_chat_id, channel_id, channel_title, is_primary, is_active, created_at)
                VALUES (?, ?, ?, ?, 1, datetime('now'))
                ON CONFLICT (user_chat_id, channel_id) DO UPDATE SET
                    channel_title = excluded.channel_title,
                    is_primary = CASE WHEN ? = 1 THEN 1 ELSE user_channels.is_primary END,
                    is_active = 1;
                """,
                (user_chat_id, channel_id, channel_title, 1 if set_primary else 0, 1 if set_primary else 0)
            )
            if set_primary:
                await self.sqlite_conn.execute(
                    "UPDATE users SET channel_id = ?, channel_title = ?, is_storage_active = 1 WHERE chat_id = ?;",
                    (channel_id, channel_title, user_chat_id)
                )
            await self.sqlite_conn.commit()
            return {
                "user_chat_id": user_chat_id,
                "channel_id": channel_id,
                "channel_title": channel_title,
                "is_primary": set_primary,
                "is_active": True
            }

    async def set_primary_channel(self, user_chat_id: int, channel_id: int) -> bool:
        """Mark a specific channel as primary and sync users table."""
        if not self.is_connected:
            return False

        if self.is_mongo:
            await self.mongo_db.user_channels.update_many(
                {"user_chat_id": user_chat_id},
                {"$set": {"is_primary": False}}
            )
            res = await self.mongo_db.user_channels.find_one_and_update(
                {"user_chat_id": user_chat_id, "channel_id": channel_id, "is_active": True},
                {"$set": {"is_primary": True}},
                return_document=pymongo.ReturnDocument.AFTER
            )
            if res:
                title = res.get("channel_title")
                await self.mongo_db.users.update_one(
                    {"chat_id": user_chat_id},
                    {"$set": {"channel_id": channel_id, "channel_title": title, "is_storage_active": True}}
                )
            return True
        elif self.is_postgres:
            async def _run(conn):
                await conn.execute(
                    """
                    UPDATE user_channels
                    SET is_primary = (channel_id = $2)
                    WHERE user_chat_id = $1 AND is_active = TRUE;
                    """,
                    user_chat_id, channel_id
                )
                title = await conn.fetchval(
                    "SELECT channel_title FROM user_channels WHERE user_chat_id = $1 AND channel_id = $2;",
                    user_chat_id, channel_id
                )
                if title:
                    await conn.execute(
                        "UPDATE users SET channel_id = $1, channel_title = $2, is_storage_active = TRUE WHERE chat_id = $3;",
                        channel_id, title, user_chat_id
                    )
                return True
            return await self._execute_pg_with_retry(_run)
        else:
            await self.sqlite_conn.execute(
                """
                UPDATE user_channels
                SET is_primary = CASE WHEN channel_id = ? THEN 1 ELSE 0 END
                WHERE user_chat_id = ? AND is_active = 1;
                """,
                (channel_id, user_chat_id)
            )
            cur = await self.sqlite_conn.execute(
                "SELECT channel_title FROM user_channels WHERE user_chat_id = ? AND channel_id = ?;",
                (user_chat_id, channel_id)
            )
            row = await cur.fetchone()
            if row:
                title = row[0]
                await self.sqlite_conn.execute(
                    "UPDATE users SET channel_id = ?, channel_title = ?, is_storage_active = 1 WHERE chat_id = ?;",
                    (channel_id, title, user_chat_id)
                )
            await self.sqlite_conn.commit()
            return True

    async def remove_user_channel(self, user_chat_id: int, channel_id: int) -> Dict[str, Any]:
        """
        Unlink/delete a specific channel for a user.
        Re-routes any platform routes pointing to this channel to the user's primary channel.
        Promotes next channel to primary if the unlinked channel was primary.
        """
        if not self.is_connected:
            return {"success": False, "re_routed": 0, "new_primary_id": None}

        if self.is_mongo:
            ch = await self.mongo_db.user_channels.find_one({"user_chat_id": user_chat_id, "channel_id": channel_id})
            was_primary = bool(ch.get("is_primary")) if ch else False
            await self.mongo_db.user_channels.delete_one({"user_chat_id": user_chat_id, "channel_id": channel_id})

            cursor = self.mongo_db.user_channels.find(
                {"user_chat_id": user_chat_id, "is_active": True}
            ).sort([("is_primary", -1), ("_id", 1)])
            remaining = await cursor.to_list(length=100)

            new_primary_id = None
            if remaining:
                if was_primary:
                    new_primary_id = remaining[0]["channel_id"]
                    await self.mongo_db.user_channels.update_one(
                        {"user_chat_id": user_chat_id, "channel_id": new_primary_id},
                        {"$set": {"is_primary": True}}
                    )
                    await self.mongo_db.users.update_one(
                        {"chat_id": user_chat_id},
                        {"$set": {"channel_id": new_primary_id, "channel_title": remaining[0]["channel_title"], "is_storage_active": True}}
                    )
                else:
                    prim = next((c for c in remaining if c.get("is_primary")), remaining[0])
                    new_primary_id = prim["channel_id"]
            else:
                await self.mongo_db.users.update_one(
                    {"chat_id": user_chat_id},
                    {"$set": {"channel_id": None, "channel_title": None, "is_storage_active": False}}
                )

            re_routed = 0
            if new_primary_id:
                up_res = await self.mongo_db.user_platform_routes.update_many(
                    {"user_chat_id": user_chat_id, "channel_id": channel_id},
                    {"$set": {"channel_id": new_primary_id}}
                )
                re_routed = up_res.modified_count
            else:
                await self.mongo_db.user_platform_routes.delete_many({"user_chat_id": user_chat_id})

            return {"success": True, "re_routed": re_routed, "new_primary_id": new_primary_id}

        elif self.is_postgres:
            async def _run(conn):
                # 1. Delete from user_channels
                was_primary = await conn.fetchval(
                    "SELECT is_primary FROM user_channels WHERE user_chat_id = $1 AND channel_id = $2;",
                    user_chat_id, channel_id
                )
                await conn.execute(
                    "DELETE FROM user_channels WHERE user_chat_id = $1 AND channel_id = $2;",
                    user_chat_id, channel_id
                )

                # 2. Check remaining channels
                remaining = await conn.fetch(
                    "SELECT channel_id, channel_title FROM user_channels WHERE user_chat_id = $1 AND is_active = TRUE ORDER BY is_primary DESC, id ASC;",
                    user_chat_id
                )

                new_primary_id = None
                if remaining:
                    if was_primary:
                        new_primary_id = remaining[0]["channel_id"]
                        await conn.execute(
                            "UPDATE user_channels SET is_primary = (channel_id = $2) WHERE user_chat_id = $1;",
                            user_chat_id, new_primary_id
                        )
                        await conn.execute(
                            "UPDATE users SET channel_id = $1, channel_title = $2, is_storage_active = TRUE WHERE chat_id = $3;",
                            new_primary_id, remaining[0]["channel_title"], user_chat_id
                        )
                    else:
                        new_primary_id = await conn.fetchval(
                            "SELECT channel_id FROM user_channels WHERE user_chat_id = $1 AND is_primary = TRUE LIMIT 1;",
                            user_chat_id
                        )
                else:
                    await conn.execute(
                        "UPDATE users SET channel_id = NULL, channel_title = NULL, is_storage_active = FALSE WHERE chat_id = $1;",
                        user_chat_id
                    )

                # 3. Re-route platform routes that were pointing to the removed channel
                re_routed = 0
                if new_primary_id:
                    res = await conn.execute(
                        "UPDATE user_platform_routes SET channel_id = $1 WHERE user_chat_id = $2 AND channel_id = $3;",
                        new_primary_id, user_chat_id, channel_id
                    )
                    re_routed = int(res.split(" ")[-1]) if " " in res else 0
                else:
                    await conn.execute("DELETE FROM user_platform_routes WHERE user_chat_id = $1;", user_chat_id)

                return {"success": True, "re_routed": re_routed, "new_primary_id": new_primary_id}
            return await self._execute_pg_with_retry(_run)
        else:
            cur_p = await self.sqlite_conn.execute(
                "SELECT is_primary FROM user_channels WHERE user_chat_id = ? AND channel_id = ?;",
                (user_chat_id, channel_id)
            )
            row_p = await cur_p.fetchone()
            was_primary = bool(row_p[0]) if row_p else False

            await self.sqlite_conn.execute(
                "DELETE FROM user_channels WHERE user_chat_id = ? AND channel_id = ?;",
                (user_chat_id, channel_id)
            )

            cur_rem = await self.sqlite_conn.execute(
                "SELECT channel_id, channel_title FROM user_channels WHERE user_chat_id = ? AND is_active = 1 ORDER BY is_primary DESC, id ASC;",
                (user_chat_id,)
            )
            remaining = await cur_rem.fetchall()

            new_primary_id = None
            if remaining:
                if was_primary:
                    new_primary_id = remaining[0][0]
                    await self.sqlite_conn.execute(
                        "UPDATE user_channels SET is_primary = CASE WHEN channel_id = ? THEN 1 ELSE 0 END WHERE user_chat_id = ?;",
                        (new_primary_id, user_chat_id)
                    )
                    await self.sqlite_conn.execute(
                        "UPDATE users SET channel_id = ?, channel_title = ?, is_storage_active = 1 WHERE chat_id = ?;",
                        (new_primary_id, remaining[0][1], user_chat_id)
                    )
                else:
                    cur_prim = await self.sqlite_conn.execute(
                        "SELECT channel_id FROM user_channels WHERE user_chat_id = ? AND is_primary = 1 LIMIT 1;",
                        (user_chat_id,)
                    )
                    r_prim = await cur_prim.fetchone()
                    new_primary_id = r_prim[0] if r_prim else remaining[0][0]
            else:
                await self.sqlite_conn.execute(
                    "UPDATE users SET channel_id = NULL, channel_title = NULL, is_storage_active = 0 WHERE chat_id = ?;",
                    (user_chat_id,)
                )

            re_routed = 0
            if new_primary_id:
                cur_up = await self.sqlite_conn.execute(
                    "UPDATE user_platform_routes SET channel_id = ? WHERE user_chat_id = ? AND channel_id = ?;",
                    (new_primary_id, user_chat_id, channel_id)
                )
                re_routed = cur_up.rowcount
            else:
                await self.sqlite_conn.execute("DELETE FROM user_platform_routes WHERE user_chat_id = ?;", (user_chat_id,))

            await self.sqlite_conn.commit()
            return {"success": True, "re_routed": re_routed, "new_primary_id": new_primary_id}

    async def set_platform_route(self, user_chat_id: int, platform: str, channel_id: int) -> bool:
        """Route all downloads from a specific platform to the given channel."""
        if not self.is_connected:
            return False

        if self.is_mongo:
            now = datetime.now(timezone.utc)
            await self.mongo_db.user_platform_routes.update_one(
                {"user_chat_id": user_chat_id, "platform": platform},
                {
                    "$set": {"channel_id": channel_id, "updated_at": now},
                    "$setOnInsert": {"user_chat_id": user_chat_id, "platform": platform}
                },
                upsert=True
            )
            return True
        elif self.is_postgres:
            async def _run(conn):
                await conn.execute(
                    """
                    INSERT INTO user_platform_routes (user_chat_id, platform, channel_id, updated_at)
                    VALUES ($1, $2, $3, NOW())
                    ON CONFLICT (user_chat_id, platform) DO UPDATE SET
                        channel_id = EXCLUDED.channel_id,
                        updated_at = NOW();
                    """,
                    user_chat_id, platform, channel_id
                )
                return True
            return await self._execute_pg_with_retry(_run)
        else:
            await self.sqlite_conn.execute(
                """
                INSERT INTO user_platform_routes (user_chat_id, platform, channel_id, updated_at)
                VALUES (?, ?, ?, datetime('now'))
                ON CONFLICT (user_chat_id, platform) DO UPDATE SET
                    channel_id = excluded.channel_id,
                    updated_at = datetime('now');
                """,
                (user_chat_id, platform, channel_id)
            )
            await self.sqlite_conn.commit()
            return True

    async def remove_platform_route(self, user_chat_id: int, platform: str) -> bool:
        """Remove explicit platform route so it falls back to primary channel."""
        if not self.is_connected:
            return False

        if self.is_mongo:
            await self.mongo_db.user_platform_routes.delete_one({"user_chat_id": user_chat_id, "platform": platform})
            return True
        elif self.is_postgres:
            async def _run(conn):
                await conn.execute("DELETE FROM user_platform_routes WHERE user_chat_id = $1 AND platform = $2;", user_chat_id, platform)
                return True
            return await self._execute_pg_with_retry(_run)
        else:
            await self.sqlite_conn.execute("DELETE FROM user_platform_routes WHERE user_chat_id = ? AND platform = ?;", (user_chat_id, platform))
            await self.sqlite_conn.commit()
            return True

    async def get_platform_route(self, user_chat_id: int, platform: str) -> Optional[int]:
        """Get explicitly routed channel_id for a platform."""
        if not self.is_connected:
            return None

        if self.is_mongo:
            doc = await self.mongo_db.user_platform_routes.find_one({"user_chat_id": user_chat_id, "platform": platform})
            return int(doc["channel_id"]) if doc else None
        elif self.is_postgres:
            async def _run(conn):
                val = await conn.fetchval(
                    "SELECT channel_id FROM user_platform_routes WHERE user_chat_id = $1 AND platform = $2;",
                    user_chat_id, platform
                )
                return int(val) if val is not None else None
            return await self._execute_pg_with_retry(_run)
        else:
            cur = await self.sqlite_conn.execute(
                "SELECT channel_id FROM user_platform_routes WHERE user_chat_id = ? AND platform = ?;",
                (user_chat_id, platform)
            )
            row = await cur.fetchone()
            return int(row[0]) if (row and row[0] is not None) else None

    async def get_all_platform_routes(self, user_chat_id: int) -> Dict[str, int]:
        """Return dict of {platform: channel_id} for a user."""
        if not self.is_connected:
            return {}

        if self.is_mongo:
            cursor = self.mongo_db.user_platform_routes.find({"user_chat_id": user_chat_id})
            docs = await cursor.to_list(length=100)
            return {d["platform"]: int(d["channel_id"]) for d in docs}
        elif self.is_postgres:
            async def _run(conn):
                rows = await conn.fetch(
                    "SELECT platform, channel_id FROM user_platform_routes WHERE user_chat_id = $1;",
                    user_chat_id
                )
                return {r["platform"]: int(r["channel_id"]) for r in rows}
            return await self._execute_pg_with_retry(_run)
        else:
            cur = await self.sqlite_conn.execute(
                "SELECT platform, channel_id FROM user_platform_routes WHERE user_chat_id = ?;",
                (user_chat_id,)
            )
            rows = await cur.fetchall()
            return {r[0]: int(r[1]) for r in rows}

    async def get_destination_channel(self, user_chat_id: int, platform: str) -> Optional[int]:
        """
        Resolve destination channel ID:
        1. Check explicit platform route (if channel still active).
        2. Primary channel from user_channels.
        3. Any active channel from user_channels.
        4. Fallback to users.channel_id.
        """
        # 1. Check explicit platform route
        routed_cid = await self.get_platform_route(user_chat_id, platform)
        if routed_cid:
            channels = await self.get_user_channels(user_chat_id)
            if any(c["channel_id"] == routed_cid for c in channels):
                return routed_cid

        # 2. Check primary channel in user_channels
        channels = await self.get_user_channels(user_chat_id)
        if channels:
            primary = next((c for c in channels if c.get("is_primary")), channels[0])
            return primary["channel_id"]

        # 3. Fallback to users.channel_id
        user_info = await self.get_user_channel(user_chat_id)
        if user_info and user_info.get("channel_id") and user_info.get("is_storage_active"):
            return user_info["channel_id"]

        return None

    # ── Decentralized Multi-Node Media Storage ────────────────────────────

    async def get_user_stored_track(self, user_chat_id: int, track_id: str, quality: str = "audio") -> Optional[Dict[str, Any]]:
        """Check if this user already has this track stored in their personal channel."""
        if not self.is_connected:
            return None

        if self.is_mongo:
            doc = await self.mongo_db.user_media_storage.find_one(
                {"user_chat_id": user_chat_id, "track_id": track_id, "quality": quality, "is_available": True}
            )
            if not doc:
                return None
            res = {
                "channel_id": doc["channel_id"],
                "channel_msg_id": doc["channel_msg_id"],
                "channel_post_url": doc.get("channel_post_url", ""),
                "telegram_file_id": doc.get("telegram_file_id"),
                "title": None,
                "artist": None,
                "duration_secs": 0,
                "file_size_bytes": 0,
                "thumbnail_url": None
            }
            track_meta = await self.mongo_db.tracks.find_one({"track_id": track_id, "quality": quality})
            if track_meta:
                res["title"] = track_meta.get("title")
                res["artist"] = track_meta.get("artist")
                res["duration_secs"] = track_meta.get("duration_secs", 0)
                res["file_size_bytes"] = track_meta.get("file_size_bytes", 0)
                res["thumbnail_url"] = track_meta.get("thumbnail_url")
            return res
        elif self.is_postgres:
            async def _run(conn):
                row = await conn.fetchrow(
                    """
                    SELECT s.channel_id, s.channel_msg_id, s.channel_post_url, s.telegram_file_id,
                           t.title, t.artist, t.duration_secs, t.file_size_bytes, t.thumbnail_url
                    FROM user_media_storage s
                    LEFT JOIN tracks t ON (s.track_id = t.track_id AND s.quality = t.quality)
                    WHERE s.user_chat_id = $1 AND s.track_id = $2 AND s.quality = $3 AND s.is_available = TRUE
                    LIMIT 1;
                    """,
                    user_chat_id, track_id, quality
                )
                return dict(row) if row else None
            return await self._execute_pg_with_retry(_run)
        else:
            cursor = await self.sqlite_conn.execute(
                """
                SELECT s.channel_id, s.channel_msg_id, s.channel_post_url, s.telegram_file_id,
                       t.title, t.artist, t.duration_secs, t.file_size_bytes, t.thumbnail_url
                FROM user_media_storage s
                LEFT JOIN tracks t ON (s.track_id = t.track_id AND s.quality = t.quality)
                WHERE s.user_chat_id = ? AND s.track_id = ? AND s.quality = ? AND s.is_available = 1
                LIMIT 1;
                """,
                (user_chat_id, track_id, quality)
            )
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def find_available_peer_sources(self, track_id: str, quality: str = "audio") -> List[Dict[str, Any]]:
        """Find active storage channel nodes where this track is cached for peer replication."""
        if not self.is_connected:
            return []

        if self.is_mongo:
            cursor = self.mongo_db.user_media_storage.find(
                {"track_id": track_id, "quality": quality, "is_available": True}
            ).sort("created_at", 1).limit(5)
            docs = await cursor.to_list(length=5)
            if not docs:
                return []
            track_meta = await self.mongo_db.tracks.find_one({"track_id": track_id, "quality": quality})
            results = []
            for doc in docs:
                item = {
                    "channel_id": doc["channel_id"],
                    "channel_msg_id": doc["channel_msg_id"],
                    "channel_post_url": doc.get("channel_post_url", ""),
                    "user_chat_id": doc["user_chat_id"],
                    "telegram_file_id": doc.get("telegram_file_id"),
                    "title": track_meta.get("title") if track_meta else None,
                    "artist": track_meta.get("artist") if track_meta else None,
                    "duration_secs": track_meta.get("duration_secs", 0) if track_meta else 0,
                    "file_size_bytes": track_meta.get("file_size_bytes", 0) if track_meta else 0,
                    "thumbnail_url": track_meta.get("thumbnail_url") if track_meta else None,
                }
                results.append(item)
            return results
        elif self.is_postgres:
            async def _run(conn):
                rows = await conn.fetch(
                    """
                    SELECT s.channel_id, s.channel_msg_id, s.channel_post_url, s.user_chat_id, s.telegram_file_id,
                           t.title, t.artist, t.duration_secs, t.file_size_bytes, t.thumbnail_url
                    FROM user_media_storage s
                    LEFT JOIN tracks t ON (s.track_id = t.track_id AND s.quality = t.quality)
                    WHERE s.track_id = $1 AND s.quality = $2 AND s.is_available = TRUE
                    ORDER BY s.created_at ASC
                    LIMIT 5;
                    """,
                    track_id, quality
                )
                return [dict(r) for r in rows]
            return await self._execute_pg_with_retry(_run)
        else:
            cursor = await self.sqlite_conn.execute(
                """
                SELECT s.channel_id, s.channel_msg_id, s.channel_post_url, s.user_chat_id, s.telegram_file_id,
                       t.title, t.artist, t.duration_secs, t.file_size_bytes, t.thumbnail_url
                FROM user_media_storage s
                LEFT JOIN tracks t ON (s.track_id = t.track_id AND s.quality = t.quality)
                WHERE s.track_id = ? AND s.quality = ? AND s.is_available = 1
                ORDER BY s.created_at ASC
                LIMIT 5;
                """,
                (track_id, quality)
            )
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]

    async def save_user_media(
        self,
        user_chat_id: int,
        channel_id: int,
        channel_msg_id: int,
        channel_post_url: str,
        track_id: str,
        quality: str,
        telegram_file_id: Optional[str] = None,
        platform: str = "youtube",
        destination_channel_id: Optional[int] = None
    ) -> None:
        """Register newly posted or replicated media in a user's channel."""
        if not self.is_connected:
            return

        dest_id = destination_channel_id or channel_id

        if self.is_mongo:
            now = datetime.now(timezone.utc)
            await self.mongo_db.user_media_storage.update_one(
                {"user_chat_id": user_chat_id, "track_id": track_id, "quality": quality},
                {
                    "$set": {
                        "channel_id": channel_id,
                        "channel_msg_id": channel_msg_id,
                        "channel_post_url": channel_post_url,
                        "telegram_file_id": telegram_file_id,
                        "platform": platform,
                        "destination_channel_id": dest_id,
                        "is_available": True,
                        "created_at": now
                    },
                    "$setOnInsert": {
                        "user_chat_id": user_chat_id,
                        "track_id": track_id,
                        "quality": quality
                    }
                },
                upsert=True
            )
        elif self.is_postgres:
            async def _run(conn):
                await conn.execute(
                    """
                    INSERT INTO user_media_storage (
                        user_chat_id, channel_id, channel_msg_id, channel_post_url,
                        track_id, quality, telegram_file_id, is_available, platform, destination_channel_id, created_at
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, TRUE, $8, $9, NOW())
                    ON CONFLICT (user_chat_id, track_id, quality) DO UPDATE SET
                        channel_id = EXCLUDED.channel_id,
                        channel_msg_id = EXCLUDED.channel_msg_id,
                        channel_post_url = EXCLUDED.channel_post_url,
                        telegram_file_id = COALESCE(EXCLUDED.telegram_file_id, user_media_storage.telegram_file_id),
                        platform = EXCLUDED.platform,
                        destination_channel_id = EXCLUDED.destination_channel_id,
                        is_available = TRUE,
                        created_at = NOW();
                    """,
                    user_chat_id, channel_id, channel_msg_id, channel_post_url, track_id, quality, telegram_file_id, platform, dest_id
                )
            await self._execute_pg_with_retry(_run)
        else:
            await self.sqlite_conn.execute(
                """
                INSERT INTO user_media_storage (
                    user_chat_id, channel_id, channel_msg_id, channel_post_url,
                    track_id, quality, telegram_file_id, is_available, platform, destination_channel_id, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, datetime('now'))
                ON CONFLICT (user_chat_id, track_id, quality) DO UPDATE SET
                    channel_id = excluded.channel_id,
                    channel_msg_id = excluded.channel_msg_id,
                    channel_post_url = excluded.channel_post_url,
                    telegram_file_id = coalesce(excluded.telegram_file_id, user_media_storage.telegram_file_id),
                    platform = excluded.platform,
                    destination_channel_id = excluded.destination_channel_id,
                    is_available = 1,
                    created_at = datetime('now');
                """,
                (user_chat_id, channel_id, channel_msg_id, channel_post_url, track_id, quality, telegram_file_id, platform, dest_id)
            )
            await self.sqlite_conn.commit()

    async def mark_node_unavailable(self, channel_id: int, channel_msg_id: int) -> None:
        """Mark a specific post as unavailable if copying or accessing it fails."""
        if not self.is_connected:
            return

        if self.is_mongo:
            await self.mongo_db.user_media_storage.update_many(
                {"channel_id": channel_id, "channel_msg_id": channel_msg_id},
                {"$set": {"is_available": False}}
            )
        elif self.is_postgres:
            async def _run(conn):
                await conn.execute(
                    "UPDATE user_media_storage SET is_available = FALSE WHERE channel_id = $1 AND channel_msg_id = $2;",
                    channel_id, channel_msg_id
                )
            await self._execute_pg_with_retry(_run)
        else:
            await self.sqlite_conn.execute(
                "UPDATE user_media_storage SET is_available = 0 WHERE channel_id = ? AND channel_msg_id = ?;",
                (channel_id, channel_msg_id)
            )
            await self.sqlite_conn.commit()

    async def delete_user_media(self, user_chat_id: int, track_id: str, quality: Optional[str] = None) -> List[Dict[str, Any]]:
        """Remove a track from a user's library and return channel message info for physical Telegram deletion."""
        if not self.is_connected:
            return []

        if self.is_mongo:
            flt = {"user_chat_id": user_chat_id, "track_id": track_id}
            if quality:
                flt["quality"] = quality
            cursor = self.mongo_db.user_media_storage.find(flt, {"channel_id": 1, "channel_msg_id": 1, "track_id": 1, "quality": 1})
            docs = await cursor.to_list(length=100)
            await self.mongo_db.user_media_storage.delete_many(flt)
            return [
                {
                    "channel_id": d["channel_id"],
                    "channel_msg_id": d["channel_msg_id"],
                    "track_id": d["track_id"],
                    "quality": d["quality"]
                }
                for d in docs
            ]
        elif self.is_postgres:
            async def _run(conn):
                if quality:
                    rows = await conn.fetch(
                        """
                        DELETE FROM user_media_storage
                        WHERE user_chat_id = $1 AND track_id = $2 AND quality = $3
                        RETURNING channel_id, channel_msg_id, track_id, quality;
                        """,
                        user_chat_id, track_id, quality
                    )
                else:
                    rows = await conn.fetch(
                        """
                        DELETE FROM user_media_storage
                        WHERE user_chat_id = $1 AND track_id = $2
                        RETURNING channel_id, channel_msg_id, track_id, quality;
                        """,
                        user_chat_id, track_id
                    )
                return [dict(r) for r in rows]
            return await self._execute_pg_with_retry(_run)
        else:
            if quality:
                cursor = await self.sqlite_conn.execute(
                    "SELECT channel_id, channel_msg_id, track_id, quality FROM user_media_storage WHERE user_chat_id = ? AND track_id = ? AND quality = ?;",
                    (user_chat_id, track_id, quality)
                )
            else:
                cursor = await self.sqlite_conn.execute(
                    "SELECT channel_id, channel_msg_id, track_id, quality FROM user_media_storage WHERE user_chat_id = ? AND track_id = ?;",
                    (user_chat_id, track_id)
                )
            rows = await cursor.fetchall()
            result = [dict(r) for r in rows]

            if quality:
                await self.sqlite_conn.execute(
                    "DELETE FROM user_media_storage WHERE user_chat_id = ? AND track_id = ? AND quality = ?;",
                    (user_chat_id, track_id, quality)
                )
            else:
                await self.sqlite_conn.execute(
                    "DELETE FROM user_media_storage WHERE user_chat_id = ? AND track_id = ?;",
                    (user_chat_id, track_id)
                )
            await self.sqlite_conn.commit()
            return result

    async def get_channel_media_count(self, user_chat_id: int, channel_id: int) -> int:
        """Return count of active media records stored for a specific user and channel."""
        if not self.is_connected:
            return 0

        if self.is_mongo:
            return await self.mongo_db.user_media_storage.count_documents(
                {"user_chat_id": user_chat_id, "channel_id": channel_id, "is_available": True}
            )
        elif self.is_postgres:
            async def _run(conn):
                val = await conn.fetchval(
                    "SELECT COUNT(*) FROM user_media_storage WHERE user_chat_id = $1 AND channel_id = $2 AND is_available = TRUE;",
                    user_chat_id, channel_id
                )
                return int(val or 0)
            return await self._execute_pg_with_retry(_run)
        else:
            cur = await self.sqlite_conn.execute(
                "SELECT COUNT(*) FROM user_media_storage WHERE user_chat_id = ? AND channel_id = ? AND is_available = 1;",
                (user_chat_id, channel_id)
            )
            row = await cur.fetchone()
            return int(row[0] or 0) if row else 0

    async def purge_channel_media(self, user_chat_id: int, channel_id: int) -> List[Tuple[int, int]]:
        """
        Delete all media records for a specific channel and user from user_media_storage.
        Returns list of (channel_id, channel_msg_id) tuples for physical Telegram deletion.
        """
        if not self.is_connected:
            return []

        if self.is_mongo:
            flt = {"user_chat_id": user_chat_id, "channel_id": channel_id}
            cursor = self.mongo_db.user_media_storage.find(flt, {"channel_id": 1, "channel_msg_id": 1})
            docs = await cursor.to_list(length=10000)
            await self.mongo_db.user_media_storage.delete_many(flt)
            return [
                (int(d["channel_id"]), int(d["channel_msg_id"]))
                for d in docs
                if d.get("channel_msg_id") is not None and int(d["channel_msg_id"]) > 0
            ]
        elif self.is_postgres:
            async def _run(conn):
                rows = await conn.fetch(
                    """
                    DELETE FROM user_media_storage
                    WHERE user_chat_id = $1 AND channel_id = $2
                    RETURNING channel_id, channel_msg_id;
                    """,
                    user_chat_id, channel_id
                )
                return [
                    (int(r["channel_id"]), int(r["channel_msg_id"]))
                    for r in rows
                    if r["channel_msg_id"] is not None and int(r["channel_msg_id"]) > 0
                ]
            return await self._execute_pg_with_retry(_run)
        else:
            cur = await self.sqlite_conn.execute(
                "SELECT channel_id, channel_msg_id FROM user_media_storage WHERE user_chat_id = ? AND channel_id = ?;",
                (user_chat_id, channel_id)
            )
            rows = await cur.fetchall()
            result = [
                (int(r[0]), int(r[1]))
                for r in rows
                if r[1] is not None and int(r[1]) > 0
            ]
            await self.sqlite_conn.execute(
                "DELETE FROM user_media_storage WHERE user_chat_id = ? AND channel_id = ?;",
                (user_chat_id, channel_id)
            )
            await self.sqlite_conn.commit()
            return result

    # ── Track Metadata Cache ──────────────────────────────────────────────

    async def save_track_metadata(
        self,
        track_id: str,
        quality: str,
        title: str,
        artist: str,
        duration_secs: int = 0,
        file_size_bytes: int = 0,
        thumbnail_url: Optional[str] = None,
        source: str = "youtube"
    ) -> None:
        """Cache global track metadata."""
        if not self.is_connected:
            return

        if self.is_mongo:
            now = datetime.now(timezone.utc)
            await self.mongo_db.tracks.update_one(
                {"track_id": track_id, "quality": quality},
                {
                    "$set": {
                        "title": title,
                        "artist": artist,
                        "duration_secs": duration_secs,
                        "file_size_bytes": file_size_bytes,
                        "thumbnail_url": thumbnail_url,
                        "source": source
                    },
                    "$setOnInsert": {
                        "track_id": track_id,
                        "quality": quality,
                        "created_at": now
                    }
                },
                upsert=True
            )
        elif self.is_postgres:
            async def _run(conn):
                await conn.execute(
                    """
                    INSERT INTO tracks (
                        track_id, quality, title, artist, duration_secs, file_size_bytes, thumbnail_url, source, created_at
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
                    ON CONFLICT (track_id, quality) DO UPDATE SET
                        title = EXCLUDED.title,
                        artist = EXCLUDED.artist,
                        duration_secs = EXCLUDED.duration_secs,
                        file_size_bytes = EXCLUDED.file_size_bytes,
                        thumbnail_url = EXCLUDED.thumbnail_url;
                    """,
                    track_id, quality, title, artist, duration_secs, file_size_bytes, thumbnail_url, source
                )
            await self._execute_pg_with_retry(_run)
        else:
            await self.sqlite_conn.execute(
                """
                INSERT INTO tracks (
                    track_id, quality, title, artist, duration_secs, file_size_bytes, thumbnail_url, source, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT (track_id, quality) DO UPDATE SET
                    title = excluded.title,
                    artist = excluded.artist,
                    duration_secs = excluded.duration_secs,
                    file_size_bytes = excluded.file_size_bytes,
                    thumbnail_url = excluded.thumbnail_url;
                """,
                (track_id, quality, title, artist, duration_secs, file_size_bytes, thumbnail_url, source)
            )
            await self.sqlite_conn.commit()

    async def get_track_metadata(self, track_id: str, quality: str = "audio") -> Optional[Dict[str, Any]]:
        """Retrieve cached metadata for a track."""
        if not self.is_connected:
            return None

        if self.is_mongo:
            doc = await self.mongo_db.tracks.find_one({"track_id": track_id, "quality": quality}, {"_id": 0})
            return dict(doc) if doc else None
        elif self.is_postgres:
            async def _run(conn):
                row = await conn.fetchrow(
                    "SELECT track_id, quality, title, artist, duration_secs, file_size_bytes, thumbnail_url, source FROM tracks WHERE track_id = $1 AND quality = $2;",
                    track_id, quality
                )
                return dict(row) if row else None
            return await self._execute_pg_with_retry(_run)
        else:
            cursor = await self.sqlite_conn.execute(
                "SELECT track_id, quality, title, artist, duration_secs, file_size_bytes, thumbnail_url, source FROM tracks WHERE track_id = ? AND quality = ?;",
                (track_id, quality)
            )
            row = await cursor.fetchone()
            return dict(row) if row else None

    # ── High-Performance API Cache ────────────────────────────────────────

    async def get_api_cache(self, cache_key: str) -> Optional[Any]:
        """Fetch unexpired cached API response."""
        if not self.is_connected:
            return None

        if self.is_mongo:
            now = datetime.now(timezone.utc)
            doc = await self.mongo_db.api_cache.find_one({"cache_key": cache_key, "expires_at": {"$gt": now}})
            if doc:
                val = doc.get("response_json")
                if isinstance(val, str):
                    try:
                        return json.loads(val)
                    except Exception:
                        return val
                return val
            return None
        elif self.is_postgres:
            async def _run(conn):
                row = await conn.fetchrow(
                    "SELECT response_json FROM api_cache WHERE cache_key = $1 AND expires_at > NOW();",
                    cache_key
                )
                if row:
                    val = row["response_json"]
                    return json.loads(val) if isinstance(val, str) else val
                return None
            return await self._execute_pg_with_retry(_run)
        else:
            cursor = await self.sqlite_conn.execute(
                "SELECT response_json FROM api_cache WHERE cache_key = ? AND expires_at > datetime('now');",
                (cache_key,)
            )
            row = await cursor.fetchone()
            if row:
                try:
                    return json.loads(row["response_json"])
                except Exception:
                    return row["response_json"]
            return None

    async def set_api_cache(self, cache_key: str, data: Any, ttl_seconds: int = 86400) -> None:
        """Store API response with expiry."""
        if not self.is_connected:
            return

        expires_at_dt = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)

        if self.is_mongo:
            now = datetime.now(timezone.utc)
            await self.mongo_db.api_cache.update_one(
                {"cache_key": cache_key},
                {
                    "$set": {
                        "response_json": data,
                        "expires_at": expires_at_dt
                    },
                    "$setOnInsert": {
                        "cache_key": cache_key,
                        "created_at": now
                    }
                },
                upsert=True
            )
        elif self.is_postgres:
            data_json = json.dumps(data)
            async def _run(conn):
                await conn.execute(
                    """
                    INSERT INTO api_cache (cache_key, response_json, expires_at, created_at)
                    VALUES ($1, $2::jsonb, $3, NOW())
                    ON CONFLICT (cache_key) DO UPDATE SET
                        response_json = EXCLUDED.response_json,
                        expires_at = EXCLUDED.expires_at;
                    """,
                    cache_key, data_json, expires_at_dt
                )
            await self._execute_pg_with_retry(_run)
        else:
            data_json = json.dumps(data)
            expires_at_str = expires_at_dt.strftime("%Y-%m-%d %H:%M:%S")
            await self.sqlite_conn.execute(
                """
                INSERT INTO api_cache (cache_key, response_json, expires_at, created_at)
                VALUES (?, ?, ?, datetime('now'))
                ON CONFLICT (cache_key) DO UPDATE SET
                    response_json = excluded.response_json,
                    expires_at = excluded.expires_at;
                """,
                (cache_key, data_json, expires_at_str)
            )
            await self.sqlite_conn.commit()

    async def delete_api_cache(self, cache_key: str) -> None:
        """Remove a cached key from database."""
        if not self.is_connected:
            return

        if self.is_mongo:
            await self.mongo_db.api_cache.delete_one({"cache_key": cache_key})
        elif self.is_postgres:
            async def _run(conn):
                await conn.execute("DELETE FROM api_cache WHERE cache_key = $1;", cache_key)
            await self._execute_pg_with_retry(_run)
        else:
            await self.sqlite_conn.execute("DELETE FROM api_cache WHERE cache_key = ?;", (cache_key,))
            await self.sqlite_conn.commit()

    # ── Download History & Rate Limiting ──────────────────────────────────

    async def log_download(
        self,
        user_chat_id: int,
        track_id: str,
        title: str,
        quality: str,
        channel_msg_id: int,
        channel_post_url: str
    ) -> None:
        """Record user download activity."""
        if not self.is_connected:
            return

        if self.is_mongo:
            now = datetime.now(timezone.utc)
            await self.mongo_db.download_history.insert_one({
                "user_chat_id": user_chat_id,
                "track_id": track_id,
                "title": title,
                "quality": quality,
                "channel_msg_id": channel_msg_id,
                "channel_post_url": channel_post_url,
                "created_at": now
            })
        elif self.is_postgres:
            async def _run(conn):
                await conn.execute(
                    """
                    INSERT INTO download_history (user_chat_id, track_id, title, quality, channel_msg_id, channel_post_url, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, NOW());
                    """,
                    user_chat_id, track_id, title, quality, channel_msg_id, channel_post_url
                )
            await self._execute_pg_with_retry(_run)
        else:
            await self.sqlite_conn.execute(
                """
                INSERT INTO download_history (user_chat_id, track_id, title, quality, channel_msg_id, channel_post_url, created_at)
                VALUES (?, ?, ?, ?, ?, ?, datetime('now'));
                """,
                (user_chat_id, track_id, title, quality, channel_msg_id, channel_post_url)
            )
            await self.sqlite_conn.commit()

    async def get_user_history(self, user_chat_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        """Retrieve recent downloads for a user."""
        if not self.is_connected:
            return []

        if self.is_mongo:
            cursor = self.mongo_db.download_history.find(
                {"user_chat_id": user_chat_id},
                {"_id": 0, "track_id": 1, "title": 1, "quality": 1, "channel_msg_id": 1, "channel_post_url": 1, "created_at": 1}
            ).sort("created_at", -1).limit(limit)
            docs = await cursor.to_list(length=limit)
            return [dict(d) for d in docs]
        elif self.is_postgres:
            async def _run(conn):
                rows = await conn.fetch(
                    """
                    SELECT track_id, title, quality, channel_msg_id, channel_post_url, created_at
                    FROM download_history
                    WHERE user_chat_id = $1
                    ORDER BY created_at DESC
                    LIMIT $2;
                    """,
                    user_chat_id, limit
                )
                return [dict(r) for r in rows]
            return await self._execute_pg_with_retry(_run)
        else:
            cursor = await self.sqlite_conn.execute(
                """
                SELECT track_id, title, quality, channel_msg_id, channel_post_url, created_at
                FROM download_history
                WHERE user_chat_id = ?
                ORDER BY created_at DESC
                LIMIT ?;
                """,
                (user_chat_id, limit)
            )
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]

    async def check_rate_limit(self, user_chat_id: int, action: str, max_requests: int = 10, window_secs: int = 60) -> bool:
        """Return True if under rate limit, False if rate-limited."""
        if not self.is_connected:
            return True

        now_dt = datetime.now(timezone.utc)
        window_start_dt = now_dt - timedelta(seconds=window_secs)

        if self.is_mongo:
            row = await self.mongo_db.rate_limits.find_one({"user_chat_id": user_chat_id, "action": action})
            if not row:
                await self.mongo_db.rate_limits.update_one(
                    {"user_chat_id": user_chat_id, "action": action},
                    {"$set": {"attempt_count": 1, "window_start": now_dt}},
                    upsert=True
                )
                return True

            w_start = row.get("window_start")
            if isinstance(w_start, str):
                try:
                    w_start = datetime.fromisoformat(w_start)
                except Exception:
                    w_start = window_start_dt - timedelta(seconds=1)
            if w_start.tzinfo is None:
                w_start = w_start.replace(tzinfo=timezone.utc)

            if w_start < window_start_dt:
                await self.mongo_db.rate_limits.update_one(
                    {"user_chat_id": user_chat_id, "action": action},
                    {"$set": {"attempt_count": 1, "window_start": now_dt}},
                    upsert=True
                )
                return True

            if row.get("attempt_count", 1) >= max_requests:
                return False

            await self.mongo_db.rate_limits.update_one(
                {"user_chat_id": user_chat_id, "action": action},
                {"$inc": {"attempt_count": 1}}
            )
            return True

        elif self.is_postgres:
            async def _run(conn):
                row = await conn.fetchrow(
                    "SELECT attempt_count, window_start FROM rate_limits WHERE user_chat_id = $1 AND action = $2;",
                    user_chat_id, action
                )
                if not row or row["window_start"] < window_start_dt:
                    await conn.execute(
                        """
                        INSERT INTO rate_limits (user_chat_id, action, attempt_count, window_start)
                        VALUES ($1, $2, 1, NOW())
                        ON CONFLICT (user_chat_id, action) DO UPDATE SET attempt_count = 1, window_start = NOW();
                        """,
                        user_chat_id, action
                    )
                    return True

                if row["attempt_count"] >= max_requests:
                    return False

                await conn.execute(
                    "UPDATE rate_limits SET attempt_count = attempt_count + 1 WHERE user_chat_id = $1 AND action = $2;",
                    user_chat_id, action
                )
                return True
            return await self._execute_pg_with_retry(_run)
        else:
            cursor = await self.sqlite_conn.execute(
                "SELECT attempt_count, window_start FROM rate_limits WHERE user_chat_id = ? AND action = ?;",
                (user_chat_id, action)
            )
            row = await cursor.fetchone()
            window_start_str = window_start_dt.strftime("%Y-%m-%d %H:%M:%S")

            if not row or row["window_start"] < window_start_str:
                await self.sqlite_conn.execute(
                    """
                    INSERT INTO rate_limits (user_chat_id, action, attempt_count, window_start)
                    VALUES (?, ?, 1, datetime('now'))
                    ON CONFLICT (user_chat_id, action) DO UPDATE SET attempt_count = 1, window_start = datetime('now');
                    """,
                    (user_chat_id, action)
                )
                await self.sqlite_conn.commit()
                return True

            if row["attempt_count"] >= max_requests:
                return False

            await self.sqlite_conn.execute(
                "UPDATE rate_limits SET attempt_count = attempt_count + 1 WHERE user_chat_id = ? AND action = ?;",
                (user_chat_id, action)
            )
            await self.sqlite_conn.commit()
            return True

    async def get_current_schema(self) -> Optional[str]:
        """Return the active PostgreSQL schema, MongoDB db name, or 'main' for SQLite."""
        if not self.is_connected:
            return None
        if self.is_mongo:
            return self._active_db_name or (self.mongo_db.name if self.mongo_db else "mongodb")
        elif self.is_postgres:
            async def _run(conn):
                return await conn.fetchval("SELECT current_schema();")
            return await self._execute_pg_with_retry(_run)
        elif self.sqlite_conn:
            return "main"
        return None

    # ── Admin Dashboard Statistics ────────────────────────────────────────

    async def get_stats(self) -> Dict[str, Any]:
        """Aggregate system metrics for the administrative dashboard."""
        if not self.is_connected:
            return {"users": 0, "channels": 0, "media_nodes": 0, "unique_tracks": 0, "schema": "disconnected"}

        if self.is_mongo:
            total_users = await self.mongo_db.users.count_documents({})
            channels = await self.mongo_db.user_channels.distinct("channel_id", {"is_active": True})
            media_nodes = await self.mongo_db.user_media_storage.count_documents({"is_available": True})
            unique_tracks = await self.mongo_db.tracks.count_documents({})
            return {
                "users": total_users,
                "channels": len(channels),
                "media_nodes": media_nodes,
                "unique_tracks": unique_tracks,
                "schema": self._active_db_name or self.mongo_db.name
            }
        elif self.is_postgres:
            schema = self.schema
            async def _run(conn):
                total_users = await conn.fetchval("SELECT COUNT(*) FROM users;")
                active_channels = await conn.fetchval("SELECT COUNT(DISTINCT channel_id) FROM users WHERE is_storage_active = TRUE AND channel_id IS NOT NULL;")
                media_nodes = await conn.fetchval("SELECT COUNT(*) FROM user_media_storage WHERE is_available = TRUE;")
                unique_tracks = await conn.fetchval("SELECT COUNT(*) FROM tracks;")
                return {
                    "users": total_users or 0,
                    "channels": active_channels or 0,
                    "media_nodes": media_nodes or 0,
                    "unique_tracks": unique_tracks or 0,
                    "schema": schema
                }
            return await self._execute_pg_with_retry(_run)
        else:
            async def _fetch(sql):
                cur = await self.sqlite_conn.execute(sql)
                r = await cur.fetchone()
                return r[0] if r else 0

            return {
                "users": await _fetch("SELECT COUNT(*) FROM users;"),
                "channels": await _fetch("SELECT COUNT(DISTINCT channel_id) FROM users WHERE is_storage_active = 1 AND channel_id IS NOT NULL;"),
                "media_nodes": await _fetch("SELECT COUNT(*) FROM user_media_storage WHERE is_available = 1;"),
                "unique_tracks": await _fetch("SELECT COUNT(*) FROM tracks;"),
                "schema": "sqlite"
            }

    async def cleanup_expired_cache(self) -> int:
        """Purge expired cache rows to maintain low storage footprint."""
        if not self.is_connected:
            return 0

        if self.is_mongo:
            now = datetime.now(timezone.utc)
            res = await self.mongo_db.api_cache.delete_many({"expires_at": {"$lt": now}})
            return res.deleted_count
        elif self.is_postgres:
            async def _run(conn):
                res = await conn.execute("DELETE FROM api_cache WHERE expires_at < NOW();")
                return int(res.split(" ")[-1]) if " " in res else 0
            return await self._execute_pg_with_retry(_run)
        else:
            cursor = await self.sqlite_conn.execute("DELETE FROM api_cache WHERE expires_at < datetime('now');")
            await self.sqlite_conn.commit()
            return cursor.rowcount

    # ── Super Admin User Management ───────────────────────────────────────

    async def is_user_banned(self, chat_id: int) -> bool:
        """Check if user is currently banned."""
        if not self.is_connected:
            return False

        if self.is_mongo:
            u = await self.mongo_db.users.find_one({"chat_id": chat_id}, {"is_banned": 1})
            return bool(u.get("is_banned")) if u else False
        elif self.is_postgres:
            async def _run(conn):
                val = await conn.fetchval("SELECT is_banned FROM users WHERE chat_id = $1;", chat_id)
                return bool(val) if val is not None else False
            return await self._execute_pg_with_retry(_run)
        else:
            cursor = await self.sqlite_conn.execute("SELECT is_banned FROM users WHERE chat_id = ?;", (chat_id,))
            row = await cursor.fetchone()
            return bool(row[0]) if (row and row[0] is not None) else False

    async def list_users(
        self,
        page: int = 1,
        page_size: int = 8,
        search: Optional[str] = None
    ) -> Tuple[List[Dict[str, Any]], int]:
        """List registered users with storage stats and pagination."""
        if not self.is_connected:
            return [], 0

        offset = max(0, (page - 1) * page_size)
        search_pattern = f"%{search.strip()}%" if search and search.strip() else None
        exact_id = search.strip() if search and search.strip().isdigit() else None

        if self.is_mongo:
            flt: Dict[str, Any] = {}
            if search and search.strip():
                clean = search.strip()
                if clean.isdigit():
                    flt = {"$or": [
                        {"chat_id": int(clean)},
                        {"username": {"$regex": re.escape(clean), "$options": "i"}},
                        {"first_name": {"$regex": re.escape(clean), "$options": "i"}}
                    ]}
                else:
                    flt = {"$or": [
                        {"username": {"$regex": re.escape(clean), "$options": "i"}},
                        {"first_name": {"$regex": re.escape(clean), "$options": "i"}}
                    ]}
            total = await self.mongo_db.users.count_documents(flt)
            cursor = self.mongo_db.users.find(flt).sort("last_active", -1).skip(offset).limit(page_size)
            user_docs = await cursor.to_list(length=page_size)
            results = []
            for u in user_docs:
                d = dict(u)
                d.pop("_id", None)
                d["is_storage_active"] = bool(d.get("is_storage_active", True))
                d["is_admin"] = bool(d.get("is_admin", False))
                d["is_banned"] = bool(d.get("is_banned", False))
                d["max_channels"] = int(d.get("max_channels") or 5)
                v_cnt = await self.mongo_db.user_media_storage.count_documents(
                    {"user_chat_id": d["chat_id"], "is_available": True}
                )
                d["vault_count"] = v_cnt
                results.append(d)
            return results, total

        elif self.is_postgres:
            async def _run(conn):
                if search_pattern:
                    total = await conn.fetchval(
                        """
                        SELECT COUNT(*) FROM users
                        WHERE username ILIKE $1 OR first_name ILIKE $1 OR CAST(chat_id AS TEXT) = $2;
                        """,
                        search_pattern, exact_id or "-1"
                    )
                    rows = await conn.fetch(
                        """
                        SELECT u.chat_id, u.username, u.first_name, u.channel_id, u.channel_title,
                               u.is_storage_active, u.is_admin, u.is_banned, u.created_at, u.last_active,
                               COUNT(s.id) AS vault_count
                        FROM users u
                        LEFT JOIN user_media_storage s ON (u.chat_id = s.user_chat_id AND s.is_available = TRUE)
                        WHERE u.username ILIKE $1 OR u.first_name ILIKE $1 OR CAST(u.chat_id AS TEXT) = $2
                        GROUP BY u.chat_id, u.username, u.first_name, u.channel_id, u.channel_title,
                                 u.is_storage_active, u.is_admin, u.is_banned, u.created_at, u.last_active
                        ORDER BY u.last_active DESC
                        LIMIT $3 OFFSET $4;
                        """,
                        search_pattern, exact_id or "-1", page_size, offset
                    )
                else:
                    total = await conn.fetchval("SELECT COUNT(*) FROM users;")
                    rows = await conn.fetch(
                        """
                        SELECT u.chat_id, u.username, u.first_name, u.channel_id, u.channel_title,
                               u.is_storage_active, u.is_admin, u.is_banned, u.created_at, u.last_active,
                               COUNT(s.id) AS vault_count
                        FROM users u
                        LEFT JOIN user_media_storage s ON (u.chat_id = s.user_chat_id AND s.is_available = TRUE)
                        GROUP BY u.chat_id, u.username, u.first_name, u.channel_id, u.channel_title,
                                 u.is_storage_active, u.is_admin, u.is_banned, u.created_at, u.last_active
                        ORDER BY u.last_active DESC
                        LIMIT $1 OFFSET $2;
                        """,
                        page_size, offset
                    )
                return [dict(r) for r in rows], (total or 0)
            return await self._execute_pg_with_retry(_run)
        else:
            if search_pattern:
                cur_total = await self.sqlite_conn.execute(
                    """
                    SELECT COUNT(*) FROM users
                    WHERE username LIKE ? OR first_name LIKE ? OR CAST(chat_id AS TEXT) = ?;
                    """,
                    (search_pattern, search_pattern, exact_id or "-1")
                )
                r_total = await cur_total.fetchone()
                total = r_total[0] if r_total else 0

                cur_rows = await self.sqlite_conn.execute(
                    """
                    SELECT u.chat_id, u.username, u.first_name, u.channel_id, u.channel_title,
                           u.is_storage_active, u.is_admin, u.is_banned, u.created_at, u.last_active,
                           COUNT(s.id) AS vault_count
                    FROM users u
                    LEFT JOIN user_media_storage s ON (u.chat_id = s.user_chat_id AND s.is_available = 1)
                    WHERE u.username LIKE ? OR u.first_name LIKE ? OR CAST(u.chat_id AS TEXT) = ?
                    GROUP BY u.chat_id
                    ORDER BY u.last_active DESC
                    LIMIT ? OFFSET ?;
                    """,
                    (search_pattern, search_pattern, exact_id or "-1", page_size, offset)
                )
            else:
                cur_total = await self.sqlite_conn.execute("SELECT COUNT(*) FROM users;")
                r_total = await cur_total.fetchone()
                total = r_total[0] if r_total else 0

                cur_rows = await self.sqlite_conn.execute(
                    """
                    SELECT u.chat_id, u.username, u.first_name, u.channel_id, u.channel_title,
                           u.is_storage_active, u.is_admin, u.is_banned, u.created_at, u.last_active,
                           COUNT(s.id) AS vault_count
                    FROM users u
                    LEFT JOIN user_media_storage s ON (u.chat_id = s.user_chat_id AND s.is_available = 1)
                    GROUP BY u.chat_id
                    ORDER BY u.last_active DESC
                    LIMIT ? OFFSET ?;
                    """,
                    (page_size, offset)
                )

            rows = await cur_rows.fetchall()
            user_list = []
            for r in rows:
                d = dict(r)
                d["is_storage_active"] = bool(d.get("is_storage_active", 0))
                d["is_admin"] = bool(d.get("is_admin", 0))
                d["is_banned"] = bool(d.get("is_banned", 0))
                d["max_channels"] = int(d.get("max_channels") or 5)
                user_list.append(d)
            return user_list, total

    async def get_user_details(self, chat_id: int) -> Optional[Dict[str, Any]]:
        """Retrieve complete profile, storage channels, and download statistics for a user."""
        if not self.is_connected:
            return None

        user_dict = None
        if self.is_mongo:
            u = await self.mongo_db.users.find_one({"chat_id": chat_id})
            if u:
                d = dict(u)
                d.pop("_id", None)
                d["is_storage_active"] = bool(d.get("is_storage_active", False))
                d["terms_accepted"] = bool(d.get("terms_accepted", False))
                d["is_admin"] = bool(d.get("is_admin", False))
                d["is_banned"] = bool(d.get("is_banned", False))
                d["max_channels"] = int(d.get("max_channels") or 5)
                d["vault_count"] = await self.mongo_db.user_media_storage.count_documents(
                    {"user_chat_id": chat_id, "is_available": True}
                )
                d["download_count"] = await self.mongo_db.download_history.count_documents(
                    {"user_chat_id": chat_id}
                )
                user_dict = d
        elif self.is_postgres:
            async def _run(conn):
                row = await conn.fetchrow(
                    """
                    SELECT u.chat_id, u.username, u.first_name, u.channel_id, u.channel_title,
                           u.is_storage_active, u.terms_accepted, u.is_admin, u.is_banned, u.max_channels, u.created_at, u.last_active,
                           (SELECT COUNT(*) FROM user_media_storage s WHERE s.user_chat_id = u.chat_id AND s.is_available = TRUE) AS vault_count,
                           (SELECT COUNT(*) FROM download_history h WHERE h.user_chat_id = u.chat_id) AS download_count
                    FROM users u
                    WHERE u.chat_id = $1;
                    """,
                    chat_id
                )
                return dict(row) if row else None
            user_dict = await self._execute_pg_with_retry(_run)
        else:
            cur = await self.sqlite_conn.execute(
                """
                SELECT u.chat_id, u.username, u.first_name, u.channel_id, u.channel_title,
                       u.is_storage_active, u.terms_accepted, u.is_admin, u.is_banned, u.max_channels, u.created_at, u.last_active,
                       (SELECT COUNT(*) FROM user_media_storage s WHERE s.user_chat_id = u.chat_id AND s.is_available = 1) AS vault_count,
                       (SELECT COUNT(*) FROM download_history h WHERE h.user_chat_id = u.chat_id) AS download_count
                FROM users u
                WHERE u.chat_id = ?;
                """,
                (chat_id,)
            )
            row = await cur.fetchone()
            if row:
                d = dict(row)
                d["is_storage_active"] = bool(d.get("is_storage_active", 0))
                d["terms_accepted"] = bool(d.get("terms_accepted", 0))
                d["is_admin"] = bool(d.get("is_admin", 0))
                d["is_banned"] = bool(d.get("is_banned", 0))
                user_dict = d

        if user_dict:
            user_dict["max_channels"] = int(user_dict.get("max_channels") or 5)
            user_dict["channels"] = await self.get_user_channels(chat_id)
            user_dict["routes"] = await self.get_all_platform_routes(chat_id)
        return user_dict

    async def get_user_vault_tracks(
        self,
        user_chat_id: int,
        page: int = 1,
        page_size: int = 6
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Retrieve paginated list of media tracks stored in a user's channel."""
        if not self.is_connected:
            return [], 0

        offset = max(0, (page - 1) * page_size)

        if self.is_mongo:
            flt = {"user_chat_id": user_chat_id, "is_available": True}
            total = await self.mongo_db.user_media_storage.count_documents(flt)
            cursor = self.mongo_db.user_media_storage.find(flt).sort("created_at", -1).skip(offset).limit(page_size)
            docs = await cursor.to_list(length=page_size)
            results = []
            for idx, s in enumerate(docs, 1):
                t_meta = await self.mongo_db.tracks.find_one({"track_id": s["track_id"], "quality": s["quality"]})
                item = {
                    "id": idx + offset,
                    "track_id": s["track_id"],
                    "quality": s["quality"],
                    "channel_msg_id": s["channel_msg_id"],
                    "channel_post_url": s.get("channel_post_url", ""),
                    "telegram_file_id": s.get("telegram_file_id"),
                    "created_at": s.get("created_at"),
                    "title": t_meta.get("title") if t_meta else None,
                    "artist": t_meta.get("artist") if t_meta else None,
                    "duration_secs": t_meta.get("duration_secs", 0) if t_meta else 0,
                    "thumbnail_url": t_meta.get("thumbnail_url") if t_meta else None,
                }
                results.append(item)
            return results, total

        elif self.is_postgres:
            async def _run(conn):
                total = await conn.fetchval(
                    "SELECT COUNT(*) FROM user_media_storage WHERE user_chat_id = $1 AND is_available = TRUE;",
                    user_chat_id
                )
                rows = await conn.fetch(
                    """
                    SELECT s.id, s.track_id, s.quality, s.channel_msg_id, s.channel_post_url, s.telegram_file_id, s.created_at,
                           t.title, t.artist, t.duration_secs, t.thumbnail_url
                    FROM user_media_storage s
                    LEFT JOIN tracks t ON (s.track_id = t.track_id AND s.quality = t.quality)
                    WHERE s.user_chat_id = $1 AND s.is_available = TRUE
                    ORDER BY s.created_at DESC
                    LIMIT $2 OFFSET $3;
                    """,
                    user_chat_id, page_size, offset
                )
                return [dict(r) for r in rows], (total or 0)
            return await self._execute_pg_with_retry(_run)
        else:
            cur_tot = await self.sqlite_conn.execute(
                "SELECT COUNT(*) FROM user_media_storage WHERE user_chat_id = ? AND is_available = 1;",
                (user_chat_id,)
            )
            r_tot = await cur_tot.fetchone()
            total = r_tot[0] if r_tot else 0

            cur = await self.sqlite_conn.execute(
                """
                SELECT s.id, s.track_id, s.quality, s.channel_msg_id, s.channel_post_url, s.telegram_file_id, s.created_at,
                       t.title, t.artist, t.duration_secs, t.thumbnail_url
                FROM user_media_storage s
                LEFT JOIN tracks t ON (s.track_id = t.track_id AND s.quality = t.quality)
                WHERE s.user_chat_id = ? AND s.is_available = 1
                ORDER BY s.created_at DESC
                LIMIT ? OFFSET ?;
                """,
                (user_chat_id, page_size, offset)
            )
            rows = await cur.fetchall()
            return [dict(r) for r in rows], total

    async def toggle_user_admin(self, chat_id: int) -> bool:
        """Toggle is_admin status for a user and return the new value."""
        if not self.is_connected:
            return False

        if self.is_mongo:
            u = await self.mongo_db.users.find_one({"chat_id": chat_id}, {"is_admin": 1})
            new_val = not bool(u.get("is_admin")) if u else True
            await self.mongo_db.users.update_one({"chat_id": chat_id}, {"$set": {"is_admin": new_val}})
            return new_val
        elif self.is_postgres:
            async def _run(conn):
                row = await conn.fetchrow(
                    "UPDATE users SET is_admin = NOT is_admin WHERE chat_id = $1 RETURNING is_admin;",
                    chat_id
                )
                return bool(row["is_admin"]) if row else False
            return await self._execute_pg_with_retry(_run)
        else:
            cur = await self.sqlite_conn.execute("SELECT is_admin FROM users WHERE chat_id = ?;", (chat_id,))
            row = await cur.fetchone()
            new_val = 0 if (row and row[0]) else 1
            await self.sqlite_conn.execute("UPDATE users SET is_admin = ? WHERE chat_id = ?;", (new_val, chat_id))
            await self.sqlite_conn.commit()
            return bool(new_val)

    async def toggle_user_ban(self, chat_id: int) -> bool:
        """Toggle is_banned status for a user and return the new value."""
        if not self.is_connected:
            return False

        if self.is_mongo:
            u = await self.mongo_db.users.find_one({"chat_id": chat_id}, {"is_banned": 1})
            new_val = not bool(u.get("is_banned")) if u else True
            await self.mongo_db.users.update_one({"chat_id": chat_id}, {"$set": {"is_banned": new_val}})
            return new_val
        elif self.is_postgres:
            async def _run(conn):
                row = await conn.fetchrow(
                    "UPDATE users SET is_banned = NOT is_banned WHERE chat_id = $1 RETURNING is_banned;",
                    chat_id
                )
                return bool(row["is_banned"]) if row else False
            return await self._execute_pg_with_retry(_run)
        else:
            cur = await self.sqlite_conn.execute("SELECT is_banned FROM users WHERE chat_id = ?;", (chat_id,))
            row = await cur.fetchone()
            new_val = 0 if (row and row[0]) else 1
            await self.sqlite_conn.execute("UPDATE users SET is_banned = ? WHERE chat_id = ?;", (new_val, chat_id))
            await self.sqlite_conn.commit()
            return bool(new_val)

    async def force_unlink_user_channel(self, chat_id: int, channel_id: Optional[int] = None) -> None:
        """Forcefully disconnect a user's storage channel(s) and clean up routes."""
        if not self.is_connected:
            return

        if channel_id:
            await self.remove_user_channel(chat_id, channel_id)
            return

        if self.is_mongo:
            await self.mongo_db.users.update_one(
                {"chat_id": chat_id},
                {"$set": {"channel_id": None, "channel_title": None, "is_storage_active": False}}
            )
            await self.mongo_db.user_channels.delete_many({"user_chat_id": chat_id})
            await self.mongo_db.user_platform_routes.delete_many({"user_chat_id": chat_id})
            await self.mongo_db.user_media_storage.update_many(
                {"user_chat_id": chat_id},
                {"$set": {"is_available": False}}
            )
        elif self.is_postgres:
            async def _run(conn):
                await conn.execute(
                    "UPDATE users SET channel_id = NULL, channel_title = NULL, is_storage_active = FALSE WHERE chat_id = $1;",
                    chat_id
                )
                await conn.execute("DELETE FROM user_channels WHERE user_chat_id = $1;", chat_id)
                await conn.execute("DELETE FROM user_platform_routes WHERE user_chat_id = $1;", chat_id)
                await conn.execute(
                    "UPDATE user_media_storage SET is_available = FALSE WHERE user_chat_id = $1;",
                    chat_id
                )
            await self._execute_pg_with_retry(_run)
        else:
            await self.sqlite_conn.execute(
                "UPDATE users SET channel_id = NULL, channel_title = NULL, is_storage_active = 0 WHERE chat_id = ?;",
                (chat_id,)
            )
            await self.sqlite_conn.execute("DELETE FROM user_channels WHERE user_chat_id = ?;", (chat_id,))
            await self.sqlite_conn.execute("DELETE FROM user_platform_routes WHERE user_chat_id = ?;", (chat_id,))
            await self.sqlite_conn.execute(
                "UPDATE user_media_storage SET is_available = 0 WHERE user_chat_id = ?;",
                (chat_id,)
            )
            await self.sqlite_conn.commit()

    async def find_user_by_identifier(self, query: str) -> Optional[Dict[str, Any]]:
        """Look up user by numeric ID or username."""
        clean = query.strip()
        if not clean:
            return None

        # Check numeric ID
        if clean.isdigit() or (clean.startswith("-") and clean[1:].isdigit()):
            return await self.get_user_details(int(clean))

        clean_user = clean.lstrip("@")

        if self.is_mongo:
            doc = await self.mongo_db.users.find_one(
                {"username": {"$regex": f"^{re.escape(clean_user)}$", "$options": "i"}},
                {"chat_id": 1}
            )
            return await self.get_user_details(doc["chat_id"]) if doc else None
        elif self.is_postgres:
            async def _run(conn):
                row = await conn.fetchrow(
                    "SELECT chat_id FROM users WHERE username ILIKE $1 LIMIT 1;",
                    clean_user
                )
                return row["chat_id"] if row else None
            cid = await self._execute_pg_with_retry(_run)
            return await self.get_user_details(cid) if cid else None
        else:
            cur = await self.sqlite_conn.execute(
                "SELECT chat_id FROM users WHERE username LIKE ? LIMIT 1;",
                (clean_user,)
            )
            row = await cur.fetchone()
            return await self.get_user_details(row[0]) if row else None


# Singleton instance
db = Database()
