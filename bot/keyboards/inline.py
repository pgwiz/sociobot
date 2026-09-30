"""Inline Keyboards for Sociobot."""

from typing import List, Dict, Any, Optional
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from bot.utils.platform import SUPPORTED_PLATFORMS, get_platform_display_name, get_platform_icon
from bot.utils.track_ref import create_track_ref


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
    """Keyboard to prompt adding the bot to a private channel or group."""
    rows = []
    if bot_username:
        rows.append([
            InlineKeyboardButton(
                text="➕ Add Bot to Your Channel",
                url=f"https://t.me/{bot_username}?startchannel=sociobot&admin=post_messages+edit_messages+delete_messages"
            )
        ])
        rows.append([
            InlineKeyboardButton(
                text="👥 Add Bot to a Group / Supergroup",
                url=f"https://t.me/{bot_username}?startgroup=sociobot&admin=post_messages+delete_messages"
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
    ref = create_track_ref(track_id)
    back_data = f"cb:back_search:{query[:20]}" if query else "cb:close"
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🎵 Audio (MP3 320k)", callback_data=f"cb:dl:{ref}:audio_high"),
            InlineKeyboardButton(text="🎬 Video (720p HD)", callback_data=f"cb:dl:{ref}:720p")
        ],
        [
            InlineKeyboardButton(text="⬅️ Back", callback_data=back_data),
            InlineKeyboardButton(text="❌ Cancel", callback_data="cb:close")
        ]
    ])


def get_media_delivery_keyboard(
    channel_post_url: str,
    track_id: str,
    quality: str = "audio_high",
    show_extract_audio: bool = False
) -> InlineKeyboardMarkup:
    """
    Action buttons attached to the delivered media in user PM:
    1. Direct deep-link to post in user's channel.
    2. Optional audio extraction button for social video clips.
    3. Interactive delete button to purge from user's channel and DB.
    4. Force re-download button.
    """
    ref = create_track_ref(track_id)
    rows = [
        [
            InlineKeyboardButton(text="📂 Open in Channel", url=channel_post_url)
        ]
    ]
    if show_extract_audio:
        rows.append([
            InlineKeyboardButton(text="🎵 Extract Audio", callback_data=f"cb:extract_audio:{ref}")
        ])
    rows.append([
        InlineKeyboardButton(text="🗑️ Delete", callback_data=f"cb:del:{ref}:{quality}"),
        InlineKeyboardButton(text="⚡ Force Re-download", callback_data=f"cb:force:{ref}:{quality}")
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


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
    vault_count: int = 0,
    channels: Optional[List[Dict[str, Any]]] = None,
    max_channels: int = 5
) -> InlineKeyboardMarkup:
    """Action buttons for inspecting and managing a specific user."""
    rows = []
    if vault_count > 0:
        rows.append([
            InlineKeyboardButton(text=f"📁 View Stored Vault ({vault_count})", callback_data=f"cb:usr_vault:{user_id}:1")
        ])

    # Per-channel unlink buttons
    if channels:
        for ch in channels:
            cid = ch["channel_id"]
            title = ch.get("channel_title") or f"Channel {cid}"
            star = "🌟 " if ch.get("is_primary") else ""
            rows.append([
                InlineKeyboardButton(
                    text=f"🗑️ Unlink: {star}{title[:18]}",
                    callback_data=f"cb:adm_unlink_ch:{user_id}:{cid}"
                )
            ])
    elif has_channel:
        rows.append([
            InlineKeyboardButton(text="🔗 Unlink All Channels", callback_data=f"cb:usr_unlink:{user_id}")
        ])

    # Quota upgrade button
    rows.append([
        InlineKeyboardButton(text=f"💎 Upgrade Quota (Max: {max_channels})", callback_data=f"cb:adm_quota_menu:{user_id}")
    ])

    rows.append([
        InlineKeyboardButton(text="💬 Message User", callback_data=f"cb:usr_dm:{user_id}")
    ])

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


def get_channels_dashboard_keyboard(
    channels: List[Dict[str, Any]],
    routes_map: Dict[str, int],
    max_channels: int = 5,
    bot_username: Optional[str] = None
) -> InlineKeyboardMarkup:
    """Main interactive keyboard for /channels vault dashboard."""
    rows = []
    for ch in channels:
        cid = ch["channel_id"]
        title = ch.get("channel_title") or "Vault Channel"
        is_primary = ch.get("is_primary", False)

        assigned_count = sum(1 for target_cid in routes_map.values() if target_cid == cid)
        route_label = f"⚙️ Route ({assigned_count} tags)"

        rows.append([
            InlineKeyboardButton(text=f"📁 {title[:18]}", callback_data="cb:noop"),
            InlineKeyboardButton(text=route_label, callback_data=f"cb:ch_route:{cid}")
        ])

        ch_actions = []
        if not is_primary:
            ch_actions.append(InlineKeyboardButton(text="🌟 Make Primary", callback_data=f"cb:ch_set_primary:{cid}"))
        else:
            ch_actions.append(InlineKeyboardButton(text="🌟 Primary", callback_data="cb:noop"))

        ch_actions.append(InlineKeyboardButton(text="🗑️ Unlink", callback_data=f"cb:ch_unlink:{cid}"))
        rows.append(ch_actions)

    if len(channels) < max_channels and bot_username:
        rows.append([
            InlineKeyboardButton(
                text="➕ Link New Channel",
                url=f"https://t.me/{bot_username}?startchannel=sociobot&admin=post_messages+edit_messages+delete_messages"
            )
        ])
    elif len(channels) >= max_channels:
        rows.append([
            InlineKeyboardButton(text=f"🔒 Limit Reached ({len(channels)}/{max_channels})", callback_data="cb:noop")
        ])

    rows.append([
        InlineKeyboardButton(text="🔄 Refresh", callback_data="cb:ch_refresh"),
        InlineKeyboardButton(text="❌ Close", callback_data="cb:close")
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def get_platform_route_keyboard(
    channel_id: int,
    user_id: int,
    current_routes: Dict[str, int]
) -> InlineKeyboardMarkup:
    """Platform routing toggle matrix for a specific channel."""
    rows = []
    row = []
    for platform_key, name, icon in SUPPORTED_PLATFORMS:
        is_routed = (current_routes.get(platform_key) == channel_id)
        status_box = "✅" if is_routed else "⬜"
        btn_text = f"{status_box} {icon} {name}"
        row.append(InlineKeyboardButton(text=btn_text, callback_data=f"cb:ch_toggle:{channel_id}:{platform_key}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    rows.append([
        InlineKeyboardButton(text="⬅️ Back to Channels", callback_data="cb:ch_back"),
        InlineKeyboardButton(text="❌ Close", callback_data="cb:close")
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def get_channel_picker_keyboard(
    channels: List[Dict[str, Any]],
    platform: str,
    track_ref: Optional[str] = None
) -> InlineKeyboardMarkup:
    """Channel selection menu when an unrouted media link is detected."""
    rows = []
    clean_ref = create_track_ref(track_ref) if track_ref else ""
    for ch in channels:
        cid = ch["channel_id"]
        title = ch.get("channel_title") or f"Channel {cid}"
        star = "🌟 " if ch.get("is_primary") else ""
        btn_text = f"{star}{title[:24]}"
        cb_data = f"cb:route_pick:{platform}:{cid}:{clean_ref}" if clean_ref else f"cb:route_pick:{platform}:{cid}"
        rows.append([
            InlineKeyboardButton(
                text=btn_text,
                callback_data=cb_data
            )
        ])
    rows.append([
        InlineKeyboardButton(text="❌ Cancel", callback_data="cb:close")
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def get_quota_upgrade_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """Admin selection menu for upgrading a user's channel quota."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="5 Channels (Default)", callback_data=f"cb:adm_set_quota:{user_id}:5"),
            InlineKeyboardButton(text="10 Channels", callback_data=f"cb:adm_set_quota:{user_id}:10")
        ],
        [
            InlineKeyboardButton(text="20 Channels", callback_data=f"cb:adm_set_quota:{user_id}:20"),
            InlineKeyboardButton(text="100 Channels (Unlimited)", callback_data=f"cb:adm_set_quota:{user_id}:100")
        ],
        [
            InlineKeyboardButton(text="⬅️ Back to User Profile", callback_data=f"cb:usr_view:{user_id}")
        ]
    ])


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


def format_channel_link(channel_id: int, username: Optional[str] = None) -> str:
    """Format deep link to open channel in Telegram."""
    if username:
        return f"https://t.me/{username.lstrip('@')}"
    cid_str = str(channel_id)
    if cid_str.startswith("-100"):
        clean_cid = cid_str[4:]
    elif cid_str.startswith("-"):
        clean_cid = cid_str[1:]
    else:
        clean_cid = cid_str
    return f"https://t.me/c/{clean_cid}/1"


def get_mychannels_keyboard(
    channels: List[Dict[str, Any]],
    max_channels: int = 5,
    bot_username: Optional[str] = None
) -> InlineKeyboardMarkup:
    """Channel selection keyboard for /mychannels interactive list."""
    rows = []
    for ch in channels:
        cid = ch["channel_id"]
        title = ch.get("channel_title") or f"Channel {cid}"
        star = "🌟 " if ch.get("is_primary") else "📁 "
        btn_text = f"{star}{title[:28]}"
        rows.append([
            InlineKeyboardButton(text=btn_text, callback_data=f"cb:mych_view:{cid}")
        ])

    if len(channels) < max_channels and bot_username:
        rows.append([
            InlineKeyboardButton(
                text="➕ Link New Channel",
                url=f"https://t.me/{bot_username}?startchannel=sociobot&admin=post_messages+edit_messages+delete_messages"
            )
        ])

    rows.append([
        InlineKeyboardButton(text="🔄 Refresh", callback_data="cb:mych_refresh"),
        InlineKeyboardButton(text="❌ Close", callback_data="cb:close")
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def get_channel_detail_keyboard(
    channel_id: int,
    channel_url: str
) -> InlineKeyboardMarkup:
    """Action buttons for an individual storage channel summary."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔗 Visit Channel", url=channel_url)
        ],
        [
            InlineKeyboardButton(text="🗑️ Unlink Channel", callback_data=f"cb:mych_unlink:{channel_id}"),
            InlineKeyboardButton(text="💥 Delete All Channel Media", callback_data=f"cb:mych_delmedia_1:{channel_id}")
        ],
        [
            InlineKeyboardButton(text="⬅️ Back to Channels", callback_data="cb:mych_list")
        ]
    ])


def get_channel_delmedia_confirm_keyboard(
    channel_id: int,
    step: int
) -> InlineKeyboardMarkup:
    """3-step confirmation keyboard for purging channel media."""
    if step == 1:
        proceed_btn = InlineKeyboardButton(text="⚠️ Proceed (1/3)", callback_data=f"cb:mych_delmedia_2:{channel_id}")
    elif step == 2:
        proceed_btn = InlineKeyboardButton(text="🚨 Yes, Continue (2/3)", callback_data=f"cb:mych_delmedia_3:{channel_id}")
    else:
        proceed_btn = InlineKeyboardButton(text="🔥 PURGE ALL MEDIA NOW (3/3)", callback_data=f"cb:mych_delmedia_confirm:{channel_id}")

    return InlineKeyboardMarkup(inline_keyboard=[
        [
            proceed_btn,
            InlineKeyboardButton(text="❌ Cancel", callback_data=f"cb:mych_view:{channel_id}")
        ]
    ])


