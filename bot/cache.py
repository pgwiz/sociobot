"""Two-tier caching system for Sociobot.

Tier 1: High-speed in-memory TTLCache (RAM)
Tier 2: Persistent Neon PostgreSQL / SQLite (api_cache table)
"""

import hashlib
import logging
from typing import Optional, Dict, Any
from cachetools import TTLCache
from bot.config import settings
from bot.database import db

logger = logging.getLogger(__name__)


class TwoTierCache:
    def __init__(self):
        self.enabled = settings.ENABLE_MEMORY_CACHE
        self._ram_cache = TTLCache(
            maxsize=settings.MEMORY_CACHE_MAXSIZE,
            ttl=settings.MEMORY_CACHE_TTL_SECS
        )

    @staticmethod
    def hash_key(prefix: str, identifier: str) -> str:
        """Generate a consistent cache key with SHA256 for long query strings."""
        if len(identifier) > 64:
            ident_hash = hashlib.sha256(identifier.encode('utf-8')).hexdigest()[:32]
            return f"{prefix}:{ident_hash}"
        return f"{prefix}:{identifier.strip().lower()}"

    async def get(self, key: str) -> Optional[Any]:
        """Lookup key in RAM first, then fallback to database."""
        if self.enabled and key in self._ram_cache:
            logger.debug(f"[CACHE HIT - RAM] {key}")
            return self._ram_cache[key]

        if db.is_connected:
            try:
                db_data = await db.get_api_cache(key)
                if db_data:
                    logger.debug(f"[CACHE HIT - DB] {key}")
                    if self.enabled:
                        self._ram_cache[key] = db_data
                    return db_data
            except Exception as e:
                logger.warning(f"Error reading cache from DB for {key}: {e}")

        logger.debug(f"[CACHE MISS] {key}")
        return None

    async def set(self, key: str, data: Any, ttl_seconds: int) -> None:
        """Store key in both RAM and database."""
        if self.enabled:
            self._ram_cache[key] = data

        if db.is_connected:
            try:
                await db.set_api_cache(key, data, ttl_seconds)
                logger.debug(f"[CACHE SET] {key} (TTL: {ttl_seconds}s)")
            except Exception as e:
                logger.warning(f"Error persisting cache to DB for {key}: {e}")

    def clear_ram(self) -> None:
        """Clear the in-memory cache."""
        self._ram_cache.clear()
        logger.info("Cleared RAM cache.")


cache = TwoTierCache()
