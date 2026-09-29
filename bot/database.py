"""Database Manager with Dual Support: Neon PostgreSQL & SQLite Fallback.

Decentralized Multi-Node Media Storage Architecture:
- Tracks individual user private storage channels.
- Records media posted across user channels with direct deep-links.
- Facilitates peer-to-peer media replication across channels.
- Handles node failure, soft/hard deletion, and channel unlinking.
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List, Tuple
import aiosqlite
import asyncpg
from bot.config import settings

logger = logging.getLogger(__name__)

NEON_TRANSIENT_ERRORS = (
    asyncpg.PostgresConnectionError,
    asyncpg.CannotConnectNowError,
    asyncpg.AdminShutdownError,
    asyncpg.InterfaceError,
    ConnectionResetError,
    ConnectionRefusedError,
    asyncio.TimeoutError,
    OSError
)


class Database:
    def __init__(self):
        self.pg_pool: Optional[asyncpg.Pool] = None
        self.sqlite_conn: Optional[aiosqlite.Connection] = None
        self._keepalive_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

    @property
    def is_postgres(self) -> bool:
        """Return True if DATABASE_URL specifies PostgreSQL / Neon."""
        db_url = settings.DATABASE_URL or ""
        return db_url.startswith("postgresql://") or db_url.startswith("postgres://")

    @property
    def is_connected(self) -> bool:
        return self.pg_pool is not None or self.sqlite_conn is not None

    async def connect(self) -> None:
        """Connect to either Neon PostgreSQL or SQLite based on configuration."""
        async with self._lock:
            if self.is_connected:
                return

            db_url = settings.DATABASE_URL or ""

            if self.is_postgres:
                max_retries = 5
                base_delay = 1.5

                for attempt in range(1, max_retries + 1):
                    try:
                        logger.info(f"Connecting to Neon PostgreSQL (attempt {attempt}/{max_retries})...")
                        self.pg_pool = await asyncpg.create_pool(
                            dsn=db_url,
                            min_size=settings.DB_POOL_MIN_SIZE,
                            max_size=settings.DB_POOL_MAX_SIZE,
                            timeout=30.0,
                            command_timeout=30.0,
                            statement_cache_size=0,  # Required for PgBouncer / Neon connection pooling
                            max_inactive_connection_lifetime=300.0
                        )

                        async with self.pg_pool.acquire() as conn:
                            await conn.fetchval("SELECT 1;")

                        logger.info("Neon PostgreSQL connected and active.")
                        await self._migrate()

                        if getattr(settings, "ENABLE_NEON_KEEPALIVE", False) and not self._keepalive_task:
                            self._keepalive_task = asyncio.create_task(self._keepalive_loop())
                        return

                    except NEON_TRANSIENT_ERRORS as e:
                        logger.warning(f"Neon cold-start delay on attempt {attempt}: {e}")
                        if attempt < max_retries:
                            sleep_time = base_delay * (2 ** (attempt - 1))
                            logger.info(f"Waiting {sleep_time:.1f}s for Neon compute to spin up...")
                            await asyncio.sleep(sleep_time)
                        else:
                            logger.error("Failed to connect to Neon PostgreSQL after retries.")
                            raise
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
        if self._keepalive_task and not self._keepalive_task.done():
            self._keepalive_task.cancel()
            try:
                await self._keepalive_task
            except asyncio.CancelledError:
                pass
            self._keepalive_task = None

        if self.pg_pool:
            await self.pg_pool.close()
            self.pg_pool = None
            logger.info("Neon PostgreSQL pool closed.")

        if self.sqlite_conn:
            await self.sqlite_conn.close()
            self.sqlite_conn = None
            logger.info("SQLite connection closed.")

    async def _keepalive_loop(self) -> None:
        """Lightweight background heartbeat every 4m to prevent Neon cold start during active use."""
        while True:
            try:
                await asyncio.sleep(240)
                if self.pg_pool:
                    async with self.pg_pool.acquire() as conn:
                        await conn.fetchval("SELECT 1;")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"Neon keepalive ping failed (sleeping): {e}")

    async def _execute_pg_with_retry(self, callback, retries: int = 3):
        """Execute a PostgreSQL query with automatic retry on transient disconnect."""
        for attempt in range(1, retries + 1):
            try:
                async with self.pg_pool.acquire() as conn:
                    return await callback(conn)
            except NEON_TRANSIENT_ERRORS as e:
                logger.warning(f"Transient DB error during query (attempt {attempt}): {e}")
                if attempt < retries:
                    if self.pg_pool:
                        try:
                            await self.pg_pool.close()
                        except Exception:
                            pass
                        self.pg_pool = None
                    await asyncio.sleep(1.0)
                    await self.connect()
                else:
                    raise

    async def _migrate(self) -> None:
        """Run schema migrations for PostgreSQL or SQLite."""
        if self.is_postgres:
            async def _run(conn):
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
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        last_active TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    );
                    ALTER TABLE users ADD COLUMN IF NOT EXISTS is_banned BOOLEAN DEFAULT FALSE;

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
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        CONSTRAINT unique_user_track_quality UNIQUE(user_chat_id, track_id, quality)
                    );

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
                """)
            await self._execute_pg_with_retry(_run)
            logger.info("Neon PostgreSQL schema migrations applied successfully.")
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
                    created_at TEXT DEFAULT (datetime('now')),
                    last_active TEXT DEFAULT (datetime('now'))
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
            """)
            await self.sqlite_conn.commit()
            try:
                await self.sqlite_conn.execute("ALTER TABLE users ADD COLUMN is_banned INTEGER DEFAULT 0;")
                await self.sqlite_conn.commit()
            except Exception:
                pass
            logger.info("SQLite schema migrations applied successfully.")

    # ── User & Channel Management ─────────────────────────────────────────

    async def get_or_create_user(
        self,
        chat_id: int,
        username: Optional[str] = None,
        first_name: Optional[str] = None,
        is_admin: bool = False
    ) -> Dict[str, Any]:
        """Fetch or insert a user record, updating activity timestamp."""
        if not self.is_connected:
            return {"chat_id": chat_id, "username": username, "first_name": first_name, "is_admin": is_admin, "is_banned": False}

        if self.is_postgres:
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
                    RETURNING chat_id, username, first_name, channel_id, channel_title, is_storage_active, terms_accepted, is_admin, is_banned;
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
                "SELECT chat_id, username, first_name, channel_id, channel_title, is_storage_active, terms_accepted, is_admin, is_banned FROM users WHERE chat_id = ?;",
                (chat_id,)
            )
            row = await cursor.fetchone()
            if row:
                d = dict(row)
                d["is_storage_active"] = bool(d.get("is_storage_active", 1))
                d["terms_accepted"] = bool(d.get("terms_accepted", 0))
                d["is_admin"] = bool(d.get("is_admin", 0))
                d["is_banned"] = bool(d.get("is_banned", 0))
                return d
            return {}

    async def link_user_channel(self, chat_id: int, channel_id: int, channel_title: str) -> None:
        """Register or update a user's private storage channel."""
        if not self.is_connected:
            return

        if self.is_postgres:
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

        if self.is_postgres:
            async def _run(conn):
                await conn.execute(
                    """
                    UPDATE users
                    SET is_storage_active = FALSE
                    WHERE chat_id = $1;
                    """,
                    chat_id
                )
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
            await self.sqlite_conn.commit()

    async def set_terms_accepted(self, chat_id: int, accepted: bool = True) -> None:
        """Update polite onboarding terms acknowledgment."""
        if not self.is_connected:
            return

        if self.is_postgres:
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
        """Retrieve user channel info and storage readiness."""
        if not self.is_connected:
            return None

        if self.is_postgres:
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

    # ── Decentralized Multi-Node Media Storage ────────────────────────────

    async def get_user_stored_track(self, user_chat_id: int, track_id: str, quality: str = "audio") -> Optional[Dict[str, Any]]:
        """Check if this user already has this track stored in their personal channel."""
        if not self.is_connected:
            return None

        if self.is_postgres:
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

        if self.is_postgres:
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
        telegram_file_id: Optional[str] = None
    ) -> None:
        """Register newly posted or replicated media in a user's channel."""
        if not self.is_connected:
            return

        if self.is_postgres:
            async def _run(conn):
                await conn.execute(
                    """
                    INSERT INTO user_media_storage (
                        user_chat_id, channel_id, channel_msg_id, channel_post_url,
                        track_id, quality, telegram_file_id, is_available, created_at
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, TRUE, NOW())
                    ON CONFLICT (user_chat_id, track_id, quality) DO UPDATE SET
                        channel_id = EXCLUDED.channel_id,
                        channel_msg_id = EXCLUDED.channel_msg_id,
                        channel_post_url = EXCLUDED.channel_post_url,
                        telegram_file_id = COALESCE(EXCLUDED.telegram_file_id, user_media_storage.telegram_file_id),
                        is_available = TRUE,
                        created_at = NOW();
                    """,
                    user_chat_id, channel_id, channel_msg_id, channel_post_url, track_id, quality, telegram_file_id
                )
            await self._execute_pg_with_retry(_run)
        else:
            await self.sqlite_conn.execute(
                """
                INSERT INTO user_media_storage (
                    user_chat_id, channel_id, channel_msg_id, channel_post_url,
                    track_id, quality, telegram_file_id, is_available, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, 1, datetime('now'))
                ON CONFLICT (user_chat_id, track_id, quality) DO UPDATE SET
                    channel_id = excluded.channel_id,
                    channel_msg_id = excluded.channel_msg_id,
                    channel_post_url = excluded.channel_post_url,
                    telegram_file_id = coalesce(excluded.telegram_file_id, user_media_storage.telegram_file_id),
                    is_available = 1,
                    created_at = datetime('now');
                """,
                (user_chat_id, channel_id, channel_msg_id, channel_post_url, track_id, quality, telegram_file_id)
            )
            await self.sqlite_conn.commit()

    async def mark_node_unavailable(self, channel_id: int, channel_msg_id: int) -> None:
        """Mark a specific post as unavailable if copying or accessing it fails."""
        if not self.is_connected:
            return

        if self.is_postgres:
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

        if self.is_postgres:
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

        if self.is_postgres:
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

        if self.is_postgres:
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

        if self.is_postgres:
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
        data_json = json.dumps(data)

        if self.is_postgres:
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

        if self.is_postgres:
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

        if self.is_postgres:
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

        if self.is_postgres:
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

    # ── Admin Dashboard Statistics ────────────────────────────────────────

    async def get_stats(self) -> Dict[str, Any]:
        """Aggregate system metrics for the administrative dashboard."""
        if not self.is_connected:
            return {"users": 0, "channels": 0, "media_nodes": 0, "unique_tracks": 0}

        if self.is_postgres:
            async def _run(conn):
                total_users = await conn.fetchval("SELECT COUNT(*) FROM users;")
                active_channels = await conn.fetchval("SELECT COUNT(DISTINCT channel_id) FROM users WHERE is_storage_active = TRUE AND channel_id IS NOT NULL;")
                media_nodes = await conn.fetchval("SELECT COUNT(*) FROM user_media_storage WHERE is_available = TRUE;")
                unique_tracks = await conn.fetchval("SELECT COUNT(*) FROM tracks;")
                return {
                    "users": total_users or 0,
                    "channels": active_channels or 0,
                    "media_nodes": media_nodes or 0,
                    "unique_tracks": unique_tracks or 0
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
                "unique_tracks": await _fetch("SELECT COUNT(*) FROM tracks;")
            }

    async def cleanup_expired_cache(self) -> int:
        """Purge expired cache rows to maintain low storage footprint."""
        if not self.is_connected:
            return 0

        if self.is_postgres:
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

        if self.is_postgres:
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

        if self.is_postgres:
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
                user_list.append(d)
            return user_list, total

    async def get_user_details(self, chat_id: int) -> Optional[Dict[str, Any]]:
        """Retrieve complete profile, storage channels, and download statistics for a user."""
        if not self.is_connected:
            return None

        if self.is_postgres:
            async def _run(conn):
                row = await conn.fetchrow(
                    """
                    SELECT u.chat_id, u.username, u.first_name, u.channel_id, u.channel_title,
                           u.is_storage_active, u.terms_accepted, u.is_admin, u.is_banned, u.created_at, u.last_active,
                           (SELECT COUNT(*) FROM user_media_storage s WHERE s.user_chat_id = u.chat_id AND s.is_available = TRUE) AS vault_count,
                           (SELECT COUNT(*) FROM download_history h WHERE h.user_chat_id = u.chat_id) AS download_count
                    FROM users u
                    WHERE u.chat_id = $1;
                    """,
                    chat_id
                )
                return dict(row) if row else None
            return await self._execute_pg_with_retry(_run)
        else:
            cur = await self.sqlite_conn.execute(
                """
                SELECT u.chat_id, u.username, u.first_name, u.channel_id, u.channel_title,
                       u.is_storage_active, u.terms_accepted, u.is_admin, u.is_banned, u.created_at, u.last_active,
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
                return d
            return None

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

        if self.is_postgres:
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

        if self.is_postgres:
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

        if self.is_postgres:
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

    async def force_unlink_user_channel(self, chat_id: int) -> None:
        """Forcefully disconnect a user's storage channel and mark their stored media inactive."""
        if not self.is_connected:
            return

        if self.is_postgres:
            async def _run(conn):
                await conn.execute(
                    "UPDATE users SET channel_id = NULL, channel_title = NULL, is_storage_active = FALSE WHERE chat_id = $1;",
                    chat_id
                )
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

        if self.is_postgres:
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

