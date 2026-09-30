"""Utility modules for Sociobot."""

from bot.utils.platform import (
    detect_platform_and_url,
    get_quality_for_platform,
    is_social_video,
    get_platform_display_name,
    get_platform_icon,
    SUPPORTED_PLATFORMS,
    PLATFORM_META
)
from bot.utils.track_ref import (
    create_track_ref,
    register_track_ref,
    resolve_track_ref
)

__all__ = [
    "detect_platform_and_url",
    "get_quality_for_platform",
    "is_social_video",
    "get_platform_display_name",
    "get_platform_icon",
    "SUPPORTED_PLATFORMS",
    "PLATFORM_META",
    "create_track_ref",
    "register_track_ref",
    "resolve_track_ref",
]
