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


def get_admin_dashboard_keyboard(is_super_admin: bool = False) -> InlineKeyboardMarkup:
    """Interactive control panel for admins."""
    rows = [
        [
            InlineKeyboardButton(text="🧹 Run Cleanup", callback_data="cb:adm_cleanup"),
            InlineKeyboardButton(text="📊 Refresh Stats", callback_data="cb:adm_refresh")
        ]
    ]
    if is_super_admin:
        rows.append([
            InlineKeyboardButton(text="👥 Manage & Browse Users", callback_data="cb:usr_page:1")
        ])
    rows.append([
        InlineKeyboardButton(text="❌ Close Panel", callback_data="cb:close")
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def get_users_browser_keyboard(
    users: List[Dict[str, Any]],
    page: int,
    total_pages: int,
    search: Optional[str] = None
) -> InlineKeyboardMarkup:
    """Paginated list of users for Super Admin browsing."""
    buttons = []
    for u in users:
        chat_id = u.get("chat_id")
        fname = u.get("first_name") or "User"
        uname = f"@{u.get('username')}" if u.get("username") else str(chat_id)
        vault_count = u.get("vault_count", 0)
        has_ch = "📁" if u.get("channel_id") else "⚪"
        badge = "👑 " if u.get("is_admin") else ("🚫 " if u.get("is_banned") else "")

        label = f"{badge}{fname} ({uname}) | {has_ch} {vault_count}"
        if len(label) > 55:
            label = label[:52] + "..."
        buttons.append([InlineKeyboardButton(text=label, callback_data=f"cb:usr_view:{chat_id}")])

    # Navigation row
    nav_row = []
    if page > 1:
        nav_row.append(InlineKeyboardButton(text="⬅️ Prev", callback_data=f"cb:usr_page:{page - 1}"))
    nav_row.append(InlineKeyboardButton(text=f"📄 {page}/{max(1, total_pages)}", callback_data="cb:noop"))
    if page < total_pages:
        nav_row.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"cb:usr_page:{page + 1}"))
    buttons.append(nav_row)

    # Action row
    action_row = [
        InlineKeyboardButton(text="🔄 Refresh", callback_data=f"cb:usr_page:{page}"),
        InlineKeyboardButton(text="❌ Close", callback_data="cb:close")
    ]
    buttons.append(action_row)
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_user_detail_keyboard(
    user_id: int,
    is_admin: bool,
    is_banned: bool,
    has_channel: bool,
    vault_count: int = 0
) -> InlineKeyboardMarkup:
    """Action buttons for inspecting and managing a specific user."""
    rows = []
    if vault_count > 0:
        rows.append([
            InlineKeyboardButton(text=f"📁 View Stored Vault ({vault_count})", callback_data=f"cb:usr_vault:{user_id}:1")
        ])

    action_row = []
    if has_channel:
        action_row.append(InlineKeyboardButton(text="🔗 Unlink Channel", callback_data=f"cb:usr_unlink:{user_id}"))
    action_row.append(InlineKeyboardButton(text="💬 Message User", callback_data=f"cb:usr_dm:{user_id}"))
    rows.append(action_row)

    perm_row = [
        InlineKeyboardButton(
            text="👑 Demote Admin" if is_admin else "👑 Make Admin",
            callback_data=f"cb:usr_toggle_adm:{user_id}"
        ),
        InlineKeyboardButton(
            text="🟢 Unban User" if is_banned else "🚫 Ban User",
            callback_data=f"cb:usr_toggle_ban:{user_id}"
        )
    ]
    rows.append(perm_row)

    rows.append([
        InlineKeyboardButton(text="⬅️ Back to Users", callback_data="cb:usr_page:1")
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def get_user_vault_keyboard(
    user_id: int,
    tracks: List[Dict[str, Any]],
    page: int,
    total_pages: int
) -> InlineKeyboardMarkup:
    """Paginated track browser for a user's stored vault."""
    buttons = []
    for t in tracks:
        title = t.get("title") or "Unknown Track"
        quality = t.get("quality") or "audio"
        post_url = t.get("channel_post_url")
        label = f"🎵 {title} ({quality})"
        if len(label) > 55:
            label = label[:52] + "..."

        if post_url:
            buttons.append([InlineKeyboardButton(text=label, url=post_url)])
        else:
            buttons.append([InlineKeyboardButton(text=label, callback_data="cb:noop")])

    nav_row = []
    if page > 1:
        nav_row.append(InlineKeyboardButton(text="⬅️ Prev", callback_data=f"cb:usr_vault:{user_id}:{page - 1}"))
    nav_row.append(InlineKeyboardButton(text=f"📄 {page}/{max(1, total_pages)}", callback_data="cb:noop"))
    if page < total_pages:
        nav_row.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"cb:usr_vault:{user_id}:{page + 1}"))
    buttons.append(nav_row)

    buttons.append([
        InlineKeyboardButton(text="⬅️ Back to User Profile", callback_data=f"cb:usr_view:{user_id}")
    ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

