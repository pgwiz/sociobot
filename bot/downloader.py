"""Media Downloader Engine for Sociobot.

Primary: ytsp-api.pgwiz.cloud stream proxy and packaging endpoints
Fallback: yt-dlp (emergency fallback if API is unreachable)
"""

import os
import uuid
import logging
import asyncio
from typing import Optional, Tuple, Dict, Any
from pathlib import Path
import httpx
import shutil
from bot.config import settings
from bot.api_client import api_client

logger = logging.getLogger(__name__)


def parse_duration_seconds(duration_val: Any) -> int:
    """Convert duration string (e.g., '3:33' or '01:15:30') or number to integer seconds."""
    if isinstance(duration_val, (int, float)):
        return int(duration_val)
    if not isinstance(duration_val, str) or not duration_val:
        return 0

    parts = duration_val.strip().split(':')
    try:
        if len(parts) == 1:
            return int(parts[0])
        elif len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
        elif len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    except ValueError:
        return 0
    return 0


class Downloader:
    def __init__(self):
        self.download_dir = Path(settings.DOWNLOAD_DIR)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.semaphore = asyncio.Semaphore(settings.MAX_CONCURRENT_DL)

    async def download_track(
        self,
        identifier: str,
        quality: str = "audio_high",
        force_fallback: bool = False
    ) -> Tuple[Optional[str], Optional[str], Dict[str, Any]]:
        """
        Download track using ytsp-api (Primary) or yt-dlp (Fallback).
        Returns: (file_path, thumbnail_path, metadata)
        """
        async with self.semaphore:
            if not force_fallback:
                try:
                    logger.info(f"Attempting download via ytsp-api for {identifier} (quality: {quality})")
                    result = await self._download_via_api(identifier, quality)
                    if result[0]:
                        return result
                    logger.warning("API download returned empty, evaluating fallback...")
                except Exception as e:
                    logger.warning(f"Error during API download for {identifier}: {e}")

            if settings.ENABLE_API_FALLBACK or force_fallback:
                logger.info(f"Attempting emergency fallback download via yt-dlp for {identifier}")
                try:
                    return await self._download_via_ytdlp(identifier, quality)
                except Exception as e:
                    logger.error(f"Fallback yt-dlp failed for {identifier}: {e}")

            return None, None, {}

    # ── Primary: API Stream Download ───────────────────────────────────────

    async def _download_via_api(
        self,
        identifier: str,
        quality: str
    ) -> Tuple[Optional[str], Optional[str], Dict[str, Any]]:
        """Download track directly from stream extractor API with audio/video format integrity."""
        is_video_quality = quality in ("720p", "360p", "best", "video")

        # 1. For Audio: Prioritize /download endpoint (pre-packaged pure MP3 with ID3 tags)
        if not is_video_quality:
            try:
                target_url_or_id = identifier if "http" in identifier else f"https://www.youtube.com/watch?v={identifier}"
                logger.info(f"Requesting pre-packaged MP3 from /download for {target_url_or_id}")
                dl_res = await api_client.request_download(target_url_or_id)
                if dl_res and dl_res.get("success") and dl_res.get("files"):
                    first_file = dl_res["files"][0]
                    dl_path = first_file.get("download_url")
                    if dl_path:
                        full_dl_url = f"{settings.YTSP_API_BASE_URL.rstrip('/')}{dl_path}" if dl_path.startswith('/') else dl_path
                        task_id = uuid.uuid4().hex[:8]
                        file_path = str(self.download_dir / f"{task_id}.mp3")

                        logger.info(f"Downloading pre-packaged MP3 from API: {full_dl_url}")
                        timeout = httpx.Timeout(connect=15.0, read=90.0, write=30.0, pool=30.0)
                        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as dl_client:
                            resp = await dl_client.get(full_dl_url)
                            if resp.status_code == 200 and len(resp.content) > 1000:
                                if resp.content[:3] == b"ID3" or resp.content[:2] in (b"\xff\xfb", b"\xff\xf3", b"\xff\xf2"):
                                    with open(file_path, "wb") as f:
                                        f.write(resp.content)
                                    raw_title = first_file.get("name", "Audio Track").replace(".mp3", "")
                                    parts = raw_title.split(" - ", 1)
                                    artist = parts[0].strip() if len(parts) > 1 else "Unknown Artist"
                                    title = parts[1].strip() if len(parts) > 1 else raw_title
                                    metadata = {
                                        "title": title,
                                        "artist": artist,
                                        "duration": 0,
                                        "videoId": identifier,
                                        "source": "api_download",
                                        "is_video": False,
                                        "quality": quality
                                    }
                                    return file_path, None, metadata
            except Exception as dl_err:
                logger.warning(f"Pre-packaged /download attempt skipped/failed: {dl_err}")

        # 2. Stream proxy from /stream/:videoId or /get
        meta = await api_client.get_stream_info(identifier, quality=quality)
        if meta:
            proxy_url = meta.get("proxy_url")
            stream_url = meta.get("streamUrl")
            target_url = None

            if proxy_url:
                target_url = f"{settings.YTSP_API_BASE_URL.rstrip('/')}{proxy_url}" if proxy_url.startswith('/') else proxy_url
            elif stream_url:
                target_url = stream_url

            if target_url:
                title = meta.get("title") or "Unknown Track"
                artist = meta.get("uploader") or meta.get("artist") or "Unknown Artist"
                duration_secs = parse_duration_seconds(meta.get("duration"))
                thumbnail_url = meta.get("thumbnail")

                task_id = uuid.uuid4().hex[:8]
                thumb_path = str(self.download_dir / f"{task_id}.jpg") if thumbnail_url else None
                temp_raw_path = str(self.download_dir / f"{task_id}.raw")

                try:
                    logger.info(f"Streaming {'video' if is_video_quality else 'audio'} from API: {target_url}")
                    stream_timeout = httpx.Timeout(connect=15.0, read=120.0, write=30.0, pool=30.0)
                    async with httpx.AsyncClient(timeout=stream_timeout, follow_redirects=True) as stream_client:
                        async with stream_client.stream("GET", target_url) as response:
                            if response.status_code in (200, 206):
                                with open(temp_raw_path, "wb") as f:
                                    async for chunk in response.aiter_bytes(chunk_size=65536):
                                        f.write(chunk)

                                if thumb_path and thumbnail_url:
                                    try:
                                        t_resp = await stream_client.get(thumbnail_url, timeout=10.0)
                                        if t_resp.status_code == 200:
                                            with open(thumb_path, "wb") as tf:
                                                tf.write(t_resp.content)
                                    except Exception:
                                        thumb_path = None

                    if os.path.exists(temp_raw_path) and os.path.getsize(temp_raw_path) > 1000:
                        with open(temp_raw_path, "rb") as rf:
                            head = rf.read(16)

                        if is_video_quality:
                            final_video_path = str(self.download_dir / f"{task_id}.mp4")
                            os.replace(temp_raw_path, final_video_path)
                            metadata = {
                                "title": title,
                                "artist": artist,
                                "duration": duration_secs,
                                "videoId": meta.get("videoId") or identifier,
                                "source": "api_stream",
                                "is_video": True,
                                "quality": quality
                            }
                            return final_video_path, thumb_path, metadata
                        else:
                            is_mp3_header = head[:3] == b"ID3" or head[:2] in (b"\xff\xfb", b"\xff\xf3", b"\xff\xf2")
                            if is_mp3_header:
                                final_audio_path = str(self.download_dir / f"{task_id}.mp3")
                                os.replace(temp_raw_path, final_audio_path)
                                metadata = {
                                    "title": title,
                                    "artist": artist,
                                    "duration": duration_secs,
                                    "videoId": meta.get("videoId") or identifier,
                                    "source": "api_stream",
                                    "is_video": False,
                                    "quality": quality
                                }
                                return final_audio_path, thumb_path, metadata

                            final_audio_path = str(self.download_dir / f"{task_id}.mp3")
                            bitrate = "320k" if quality == "audio_high" else ("64k" if quality == "saver" else "192k")

                            if shutil.which("ffmpeg"):
                                logger.info(f"Transcoding stream to pure MP3 ({bitrate}) using ffmpeg...")
                                proc = await asyncio.create_subprocess_exec(
                                    "ffmpeg", "-y", "-i", temp_raw_path,
                                    "-vn", "-c:a", "libmp3lame", "-b:a", bitrate,
                                    final_audio_path,
                                    stdout=asyncio.subprocess.DEVNULL,
                                    stderr=asyncio.subprocess.DEVNULL
                                )
                                await proc.wait()
                                if os.path.exists(temp_raw_path):
                                    try:
                                        os.remove(temp_raw_path)
                                    except Exception:
                                        pass

                                if os.path.exists(final_audio_path) and os.path.getsize(final_audio_path) > 1000:
                                    metadata = {
                                        "title": title,
                                        "artist": artist,
                                        "duration": duration_secs,
                                        "videoId": meta.get("videoId") or identifier,
                                        "source": "api_stream_ffmpeg",
                                        "is_video": False,
                                        "quality": quality
                                    }
                                    return final_audio_path, thumb_path, metadata

                            final_m4a_path = str(self.download_dir / f"{task_id}.m4a")
                            os.replace(temp_raw_path, final_m4a_path)
                            metadata = {
                                "title": title,
                                "artist": artist,
                                "duration": duration_secs,
                                "videoId": meta.get("videoId") or identifier,
                                "source": "api_stream_m4a",
                                "is_video": False,
                                "quality": quality,
                                "ext": "m4a"
                            }
                            return final_m4a_path, thumb_path, metadata

                except Exception as stream_err:
                    logger.warning(f"Stream proxy download failed: {stream_err}")
                    if os.path.exists(temp_raw_path):
                        try:
                            os.remove(temp_raw_path)
                        except Exception:
                            pass

        return None, None, {}

    # ── Fallback: Local yt-dlp ─────────────────────────────────────────────

    async def _download_via_ytdlp(
        self,
        identifier: str,
        quality: str
    ) -> Tuple[Optional[str], Optional[str], Dict[str, Any]]:
        """Fallback download using yt-dlp."""
        import yt_dlp

        task_id = uuid.uuid4().hex[:8]
        is_video_quality = quality in ("720p", "360p", "best", "video")
        outtmpl = str(self.download_dir / f"{task_id}.%(ext)s")

        ydl_opts = {
            "format": "bestvideo[height<=720]+bestaudio/best[height<=720]" if is_video_quality else "bestaudio/best",
            "outtmpl": outtmpl,
            "quiet": True,
            "no_warnings": True,
            "writethumbnail": True,
        }

        if not is_video_quality and shutil.which("ffmpeg"):
            ydl_opts["postprocessors"] = [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "320" if quality == "audio_high" else "192",
            }]

        target = identifier if "http" in identifier else f"https://www.youtube.com/watch?v={identifier}"

        def _extract():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(target, download=True)
                return info

        loop = asyncio.get_running_loop()
        info = await loop.run_in_executor(None, _extract)

        if not info:
            return None, None, {}

        ext = "mp4" if is_video_quality else ("mp3" if shutil.which("ffmpeg") else info.get("ext", "m4a"))
        file_path = str(self.download_dir / f"{task_id}.{ext}")

        thumb_path = None
        for cand_ext in ("jpg", "webp", "png"):
            cand = str(self.download_dir / f"{task_id}.{cand_ext}")
            if os.path.exists(cand):
                thumb_path = cand
                break

        metadata = {
            "title": info.get("title", "Unknown Title"),
            "artist": info.get("uploader", "Unknown Artist"),
            "duration": info.get("duration", 0),
            "videoId": info.get("id", identifier),
            "source": "yt-dlp",
            "is_video": is_video_quality,
            "quality": quality
        }

        return file_path, thumb_path, metadata

    @staticmethod
    def cleanup_files(*paths: Optional[str]) -> None:
        """Safely delete temporary files after upload."""
        if not settings.AUTO_CLEANUP_TEMP:
            return
        for p in paths:
            if p and os.path.exists(p):
                try:
                    os.remove(p)
                except Exception as e:
                    logger.debug(f"Could not remove temp file {p}: {e}")


downloader = Downloader()
