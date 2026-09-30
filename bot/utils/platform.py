"""Platform Detector and Media Routing Utilities for Sociobot."""

import re
from typing import Optional, Tuple, Dict

PLATFORM_PATTERNS = [
    ("youtube",    re.compile(r"(?:youtube\.com|youtu\.be)", re.I)),
    ("spotify",    re.compile(r"open\.spotify\.com", re.I)),
    ("tiktok",     re.compile(r"(?:tiktok\.com|vm\.tiktok\.com)", re.I)),
    ("instagram",  re.compile(r"(?:instagram\.com|instagr\.am)", re.I)),
    ("twitter",    re.compile(r"(?:(?:^|[\b/])(?:www\.)?(?:twitter\.com|x\.com)|(?:^|[\b/])t\.co)(?:/|$|\?)", re.I)),
    ("reddit",     re.compile(r"(?:reddit\.com|redd\.it)", re.I)),
    ("soundcloud", re.compile(r"soundcloud\.com", re.I)),
    ("bandcamp",   re.compile(r"bandcamp\.com", re.I)),
    ("vimeo",      re.compile(r"vimeo\.com", re.I)),
]

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
    """Extract first URL and identify its platform. Returns (platform, url)."""
    match = URL_RE.search(text)
    if not match:
        return None, None
    url = match.group(0)
    for platform, pattern in PLATFORM_PATTERNS:
        if pattern.search(url):
            return platform, url
    return "other", url


def get_quality_for_platform(platform: str) -> str:
    """Return appropriate quality preset for the platform."""
    if platform in SOCIAL_VIDEO_PLATFORMS:
        return "saver"       # lowest video quality
    if platform in MUSIC_PLATFORMS:
        return "audio_high"  # 320k MP3
    if platform == "youtube":
        return "audio_high"  # default for YouTube audio
    return "saver"


def is_social_video(platform: str) -> bool:
    """Return True if platform is social short-form video (saver quality + extract audio)."""
    return platform in SOCIAL_VIDEO_PLATFORMS


def get_platform_display_name(platform: str) -> str:
    """Return human-readable display name for platform."""
    return PLATFORM_META.get(platform, {}).get("name", platform.title())


def get_platform_icon(platform: str) -> str:
    """Return icon/emoji for platform."""
    return PLATFORM_META.get(platform, {}).get("icon", "📁")
