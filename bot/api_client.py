"""Stream Extractor API Client for ytsp-api.pgwiz.cloud with smart caching and retries."""

import asyncio
import logging
from typing import Optional, Dict, Any, List
import httpx
from bot.config import settings
from bot.cache import cache

logger = logging.getLogger(__name__)


class StreamApiClient:
    def __init__(self):
        self.base_url = settings.YTSP_API_BASE_URL.rstrip('/')
        self.timeout = settings.YTSP_API_TIMEOUT
        self._client: Optional[httpx.AsyncClient] = None

    async def get_client(self) -> httpx.AsyncClient:
        """Get or initialize persistent async HTTP client tied to current event loop."""
        loop = asyncio.get_running_loop()
        if self._client is None or self._client.is_closed or getattr(self, "_loop", None) != loop:
            if self._client and not self._client.is_closed:
                try:
                    await self._client.aclose()
                except Exception:
                    pass
            self._loop = loop
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=httpx.Timeout(self.timeout, connect=10.0),
                limits=httpx.Limits(max_keepalive_connections=20, max_connections=50),
                follow_redirects=True,
                headers={"User-Agent": "Sociobot/1.0"}
            )
        return self._client

    async def close(self) -> None:
        """Close HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def _request_with_retry(self, method: str, path: str, **kwargs) -> Optional[Dict[str, Any]]:
        """Execute HTTP request with 3 retries and exponential backoff."""
        client = await self.get_client()
        retries = 3
        delay = 1.0

        for attempt in range(1, retries + 1):
            try:
                response = await client.request(method, path, **kwargs)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code in (404, 400):
                    logger.warning(f"API request {path} returned client error: {response.status_code}")
                    return None
                else:
                    logger.warning(f"API {path} status {response.status_code}, attempt {attempt}/{retries}")
            except (httpx.TimeoutException, httpx.NetworkError) as e:
                logger.warning(f"API network error for {path}: {e}, attempt {attempt}/{retries}")

            if attempt < retries:
                await asyncio.sleep(delay)
                delay *= 2

        logger.error(f"API request failed after {retries} attempts: {path}")
        return None

    # ── 1. Video / Track Stream Extraction ─────────────────────────────────

    async def get_stream_info(self, video_id_or_url: str, quality: str = "audio") -> Optional[Dict[str, Any]]:
        """
        Extract stream metadata and playable stream URL for a YouTube video or Spotify track.
        Uses /stream/:videoId or /get.
        """
        cache_key = cache.hash_key("stream", f"{video_id_or_url}:{quality}")
        cached_result = await cache.get(cache_key)
        if cached_result:
            return cached_result

        is_spotify = "spotify" in video_id_or_url or (len(video_id_or_url) == 22 and not video_id_or_url.isalnum())
        if is_spotify:
            sp_url = video_id_or_url if video_id_or_url.startswith("http") else f"https://open.spotify.com/track/{video_id_or_url}"
            data = await self._request_with_retry("GET", "/get", params={"ytl": sp_url, "quality": quality}, timeout=45.0)
        else:
            endpoint = f"/stream/{video_id_or_url}"
            params = {"quality": quality}
            data = await self._request_with_retry("GET", endpoint, params=params)

            if not data or not data.get("streamUrl"):
                data = await self._request_with_retry("GET", "/get", params={"ytl": video_id_or_url, "quality": quality})

        if data:
            ttl = settings.DB_METADATA_TTL_DAYS * 86400
            await cache.set(cache_key, data, ttl)

        return data

    # ── 2. Search Music Catalog (Spotify / YouTube matched) ────────────────

    async def search_tracks(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Search tracks using /api/search/spotify.
        Returns array of results with videoId, title, artist, duration, thumbnail.
        """
        cache_key = cache.hash_key("search", f"{query}:{limit}")
        cached_result = await cache.get(cache_key)
        if cached_result and isinstance(cached_result, dict) and "results" in cached_result:
            return cached_result["results"]

        params = {"query": query, "type": "track", "limit": limit}
        data = await self._request_with_retry("GET", "/api/search/spotify", params=params)

        if data:
            results = data if isinstance(data, list) else data.get("results", [])
            ttl = settings.DB_SEARCH_TTL_HOURS * 3600
            await cache.set(cache_key, {"results": results}, ttl)
            return results

        return []

    # ── 3. Extract Spotify Playlist / Album Tracks ─────────────────────────

    async def get_spotify_playlist(self, playlist_id: str, limit: int = 25) -> List[Dict[str, Any]]:
        """
        Extract Spotify playlist or album tracks (keyless).
        Automatically resolves each track to a matched YouTube videoId.
        """
        cache_key = cache.hash_key("sp_pl", f"{playlist_id}:{limit}")
        cached_result = await cache.get(cache_key)
        if cached_result and isinstance(cached_result, dict) and "tracks" in cached_result:
            return cached_result["tracks"]

        endpoint = f"/api/spotify/playlist/{playlist_id}"
        params = {"limit": limit}
        data = await self._request_with_retry("GET", endpoint, params=params, timeout=60.0)

        if data:
            tracks = data.get("tracks") or data.get("results") or (data if isinstance(data, list) else [])
            await cache.set(cache_key, {"tracks": tracks}, settings.DB_METADATA_TTL_DAYS * 86400)
            return tracks

        return []

    # ── 4. Extract YouTube Playlist Tracks ─────────────────────────────────

    async def get_youtube_playlist(self, playlist_id_or_url: str, limit: int = 25) -> List[Dict[str, Any]]:
        """Extract YouTube playlist / album / mix tracks."""
        cache_key = cache.hash_key("yt_pl", f"{playlist_id_or_url}:{limit}")
        cached_result = await cache.get(cache_key)
        if cached_result and isinstance(cached_result, dict) and "tracks" in cached_result:
            return cached_result["tracks"]

        endpoint = "/api/youtube/playlist"
        params = {"id": playlist_id_or_url, "limit": limit}
        data = await self._request_with_retry("GET", endpoint, params=params)

        if data:
            tracks = data.get("tracks") or data.get("items") or (data if isinstance(data, list) else [])
            await cache.set(cache_key, {"tracks": tracks}, settings.DB_METADATA_TTL_DAYS * 86400)
            return tracks

        return []

    # ── 5. Download & Package Audio Track via API ──────────────────────────

    async def request_download(self, url: str) -> Optional[Dict[str, Any]]:
        """Request download package using /download endpoint."""
        endpoint = "/download"
        json_body = {"url": url}
        return await self._request_with_retry("POST", endpoint, json=json_body)


api_client = StreamApiClient()
