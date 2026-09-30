"""Track Reference and URL Tokenization Utilities for Sociobot.

Ensures all callback_data payloads stay strictly within Telegram's 64-byte limit
and prevents colon-splitting errors when handling raw URLs in inline keyboards.
"""

import hashlib
import logging
from typing import Optional
from bot.cache import cache

logger = logging.getLogger(__name__)


def create_track_ref(track_id: str) -> str:
    """
    Return a safe, compact identifier for inline button callback_data.
    Standard video IDs (e.g. 11-char YouTube IDs) without colons or slashes are preserved.
    URLs and long identifiers are mapped to a 16-hex deterministic token.
    """
    if not track_id:
        return ""
    clean_id = track_id.strip()
    # If short and contains no colons, slashes, or whitespace, it's safe as-is
    if len(clean_id) <= 24 and ":" not in clean_id and "/" not in clean_id and " " not in clean_id:
        return clean_id

    # Deterministic 16-character hex token from SHA-256
    return hashlib.sha256(clean_id.encode("utf-8")).hexdigest()[:16]


async def register_track_ref(track_id: str) -> str:
    """
    Generate and persist a track reference in cache/database.
    Returns the compact ref token (or original ID if already compact).
    """
    if not track_id:
        return ""
    clean_id = track_id.strip()
    ref = create_track_ref(clean_id)
    if ref != clean_id:
        # Store mapping with 90-day TTL (persists in Neon Postgres / SQLite api_cache table)
        await cache.set(f"trk_ref:{ref}", clean_id, ttl=86400 * 90)
    return ref


async def resolve_track_ref(ref: str) -> str:
    """
    Resolve a compact token back to the original full track_id / URL.
    Falls back to ref if no mapped track is found.
    """
    if not ref:
        return ""
    clean_ref = ref.strip()
    cached = await cache.get(f"trk_ref:{clean_ref}")
    if cached:
        return str(cached)
    return clean_ref
