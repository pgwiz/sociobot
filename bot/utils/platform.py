"""Platform Detector and Media Routing Utilities for Sociobot."""

import re
from urllib.parse import urlparse
from typing import Optional, Tuple, Dict

SOCIAL_VIDEO_PLATFORMS = {"tiktok", "instagram", "twitter", "reddit"}
MUSIC_PLATFORMS        = {"spotify", "soundcloud", "bandcamp"}
VIDEO_PLATFORMS        = {"youtube", "vimeo"}

URL_RE = re.compile(
    r"https?://[^\s<>\"']+",
    re.I
)

# Standard list of platforms configurable for channel routing
SUPPORTED_PLATFORMS = [
    ("youtube", "YouTube", "📺"),
    ("spotify", "Spotify", "🎧"),
    ("tiktok", "TikTok", "📱"),
    ("instagram", "Instagram", "📸"),
    ("twitter", "Twitter/X", "🐦"),
    ("reddit", "Reddit", "🤖"),
    ("soundcloud", "SoundCloud", "☁️"),
    ("bandcamp", "Bandcamp", "⛺"),
    ("vimeo", "Vimeo", "🎞️"),
    ("other", "Other Links", "🌐"),
]

PLATFORM_META: Dict[str, Dict[str, str]] = {
    p[0]: {"name": p[1], "icon": p[2]} for p in SUPPORTED_PLATFORMS
}


def detect_platform_and_url(text: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract first URL and identify its platform using exact domain/host parsing.
    Returns (platform, url) or (None, None) if no URL is present.
    """
    match = URL_RE.search(text)
    if not match:
        return None, None
    url = match.group(0)

    try:
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()
    except Exception:
        return "other", url

    if host in ("youtube.com", "youtu.be") or host.endswith(".youtube.com"):
        return "youtube", url
    if host in ("spotify.com", "spotify.link") or host.endswith((".spotify.com", ".spotify.link")):
        return "spotify", url
    if host in ("tiktok.com",) or host.endswith(".tiktok.com"):
        return "tiktok", url
    if host in ("instagram.com", "instagr.am") or host.endswith((".instagram.com", ".instagr.am")):
        return "instagram", url
    if host in ("twitter.com", "x.com", "t.co") or host.endswith((".twitter.com", ".x.com")):
        return "twitter", url
    if host in ("reddit.com", "redd.it") or host.endswith((".reddit.com", ".redd.it")):
        return "reddit", url
    if host in ("soundcloud.com",) or host.endswith(".soundcloud.com"):
        return "soundcloud", url
    if host in ("bandcamp.com",) or host.endswith(".bandcamp.com"):
        return "bandcamp", url
    if host in ("vimeo.com",) or host.endswith(".vimeo.com"):
        return "vimeo", url

    return "other", url


def get_quality_for_platform(platform: str) -> str:
    """Return appropriate quality preset for the platform."""
    if platform in SOCIAL_VIDEO_PLATFORMS:
        return "360p"       # 360p ensures both video and audio streams
    if platform in MUSIC_PLATFORMS:
        return "audio_high"  # 320k MP3
    if platform == "youtube":
        return "audio_high"  # default for YouTube audio
    return "360p"


def is_social_video(platform: str) -> bool:
    """Return True if platform is social short-form video (saver quality + extract audio)."""
    return platform in SOCIAL_VIDEO_PLATFORMS


def get_platform_display_name(platform: str) -> str:
    """Return human-readable display name for platform."""
    return PLATFORM_META.get(platform, {}).get("name", platform.title())


def get_platform_icon(platform: str) -> str:
    """Return icon/emoji for platform."""
    return PLATFORM_META.get(platform, {}).get("icon", "📁")
