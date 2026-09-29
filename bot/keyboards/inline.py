"""Inline Keyboards for Sociobot."""

from typing import List, Dict, Any, Optional
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


def format_channel_post_url(channel_id: int, message_id: int) -> str:
    """Format Telegram private or public channel post deep-link."""
    # Telegram private channel IDs start with -100 followed by the clean ID
    cid_str = str(channel_id)
    if cid_str.startswith("-100"):
        clean_cid = cid_str[4:]
    elif cid_str.startswith("-"):
        clean_cid = cid_str[1:]
    else:
        clean_cid = cid_str
    return f"https://t.me/c/{clean_cid}/{message_id}"


def get_onboarding_keyboard() -> InlineKeyboardMarkup:
    """Polite onboarding terms confirmation."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Let's Go 🚀", callback_data="cb:terms_ok"),
            InlineKeyboardButton(text="ℹ️ How It Works", callback_data="cb:terms_info")
        ]
    ])


def get_channel_setup_keyboard(bot_username: Optional[str] = None) -> InlineKeyboardMarkup:
    """Keyboard to prompt adding the bot to a private channel."""
    rows = []
    if bot_username:
        rows.append([
            InlineKeyboardButton(
                text="➕ Add Bot to Your Channel",
                url=f"https://t.me/{bot_username}?startchannel=true"
            )
        ])
    rows.append([
        InlineKeyboardButton(text="🔄 Check Channel Status", callback_data="cb:check_channel")
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def get_search_results_keyboard(results: List[Dict[str, Any]], query: str) -> InlineKeyboardMarkup:
    """Numbered track buttons for search results."""
    buttons = []
    for idx, item in enumerate(results[:8], 1):
        title = item.get("title") or "Unknown Title"
        artist = item.get("artist") or item.get("uploader") or ""
        duration = item.get("duration") or ""
        label = f"{idx}. {title}"
        if artist:
            label += f" - {artist}"
        if duration:
            label += f" [{duration}]"

        # Truncate label to fit Telegram 64-char button display cleanly
        if len(label) > 55:
            label = label[:52] + "..."

        vid = item.get("videoId") or item.get("id") or ""
        buttons.append([InlineKeyboardButton(text=label, callback_data=f"cb:trk:{vid}")])

    buttons.append([InlineKeyboardButton(text="❌ Close", callback_data="cb:close")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_format_picker_keyboard(track_id: str, query: Optional[str] = None) -> InlineKeyboardMarkup:
    """2-Step format selector: Audio vs Video."""
    back_data = f"cb:back_search:{query[:20]}" if query else "cb:close"
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🎵 Audio (MP3 320k)", callback_data=f"cb:dl:{track_id}:audio_high"),
            InlineKeyboardButton(text="🎬 Video (720p HD)", callback_data=f"cb:dl:{track_id}:720p")
        ],
        [
            InlineKeyboardButton(text="⬅️ Back", callback_data=back_data),
            InlineKeyboardButton(text="❌ Cancel", callback_data="cb:close")
        ]
    ])


def get_media_delivery_keyboard(
    channel_post_url: str,
    track_id: str,
    quality: str = "audio_high"
) -> InlineKeyboardMarkup:
    """
    Action buttons attached to the delivered media in user PM:
    1. Direct deep-link to post in user's channel.
    2. Interactive delete button to purge from user's channel and DB.
    3. Force re-download button.
    """
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📂 Open in Channel", url=channel_post_url)
        ],
        [
            InlineKeyboardButton(text="🗑️ Delete", callback_data=f"cb:del:{track_id}:{quality}"),
            InlineKeyboardButton(text="⚡ Force Re-download", callback_data=f"cb:force:{track_id}:{quality}")
        ]
    ])


def get_admin_dashboard_keyboard() -> InlineKeyboardMarkup:
    """Interactive control panel for admins."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🧹 Run Cleanup", callback_data="cb:adm_cleanup"),
            InlineKeyboardButton(text="📊 Refresh Stats", callback_data="cb:adm_refresh")
        ],
        [
            InlineKeyboardButton(text="❌ Close Panel", callback_data="cb:close")
        ]
    ])
