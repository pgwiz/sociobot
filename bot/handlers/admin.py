"""Admin Control Panel, System Maintenance & Super Admin User Management for Sociobot.

Features:
- /admin: Overview of registered users, active storage channels, and community media nodes.
- /users [search]: Super Admin interactive paginated user directory.
- /user <id_or_username>: Super Admin user deep inspection and controls.
- /dm <user_id> <msg>: Super Admin direct message to user.
- /cleanup: Temp directory file cleanup, expired API cache purge, RAM flush.
- /broadcast: System announcements to registered users.
- Role-based /help command.
"""

import os
import glob
import math
import logging
from typing import Optional, List, Dict, Any
from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command
import httpx
from bot.config import settings
from bot.database import db
from bot.cache import cache
from bot.keyboards.inline import (
    get_admin_dashboard_keyboard,
    get_users_browser_keyboard,
    get_user_detail_keyboard,
    get_user_vault_keyboard,
    get_quota_upgrade_keyboard
)

logger = logging.getLogger(__name__)
router = Router(name="admin")


def is_super_admin(user_id: int) -> bool:
    """Check if user has Super Admin authority."""
    return user_id in settings.get_super_admin_ids()


async def is_admin_user(user_id: int) -> bool:
    """Check if user has general administrative privileges (including Super Admins)."""
    if is_super_admin(user_id):
        return True
    user = await db.get_or_create_user(user_id)
    return bool(user.get("is_admin", False))


@router.message(Command("help"))
async def cmd_help(message: Message):
    """Provide role-based command reference."""
    user_id = message.from_user.id
    is_adm = await is_admin_user(user_id)
    is_super = is_super_admin(user_id)

    help_text = (
        "🤖 <b>Sociobot Commands</b>\n\n"
        "<b>Search & Download:</b>\n"
        "• Send any <b>song name</b> to search catalog\n"
        "• Paste a <b>Spotify or YouTube link</b> to download\n"
        "• <code>/search &lt;query&gt;</code> - Search music\n"
        "• <code>/download &lt;link&gt; [force]</code> - Download audio\n"
        "• <code>/video &lt;link&gt; [force]</code> - Download video\n\n"
        "<b>Storage Channel Management:</b>\n"
        "• <code>/mychannels</code> - View channels as buttons, unlink & purge media\n"
        "• <code>/channels</code> - Platform routing matrix dashboard\n"
        "• <code>/mychannel</code> - Check primary channel\n"
        "• <code>/setchannel &lt;id&gt;</code> - Link channel manually\n"
        "• <code>/history</code> - Recent vault downloads\n"
        "• <code>/delete &lt;track_id&gt;</code> - Delete from your channel"
    )

    if is_adm:
        help_text += (
            "\n\n<b>Admin Management:</b>\n"
            "• <code>/admin</code> - Open Control Panel\n"
            "• <code>/cleanup</code> - Purge temp files & cache\n"
            "• <code>/broadcast &lt;msg&gt;</code> - Send announcement"
        )

    if is_super:
        help_text += (
            "\n\n<b>👑 Super Admin Tools:</b>\n"
            "• <code>/users [search]</code> - Interactive user browser\n"
            "• <code>/user &lt;id or @username&gt;</code> - Inspect user profile & vault\n"
            "• <code>/dm &lt;user_id&gt; &lt;msg&gt;</code> - Send direct message to user"
        )

    await message.answer(help_text, parse_mode="HTML")


@router.message(Command("admin"))
async def cmd_admin(message: Message):
    """Display Administrative Dashboard."""
    user_id = message.from_user.id
    if not await is_admin_user(user_id):
        return

    text, kb = await render_admin_dashboard(is_super=is_super_admin(user_id))
    await message.answer(text, parse_mode="HTML", reply_markup=kb)


@router.callback_query(F.data == "cb:adm_refresh")
async def cb_admin_refresh(callback: CallbackQuery):
    """Refresh admin stats."""
    user_id = callback.from_user.id
    if not await is_admin_user(user_id):
        await callback.answer("Unauthorized", show_alert=True)
        return

    text, kb = await render_admin_dashboard(is_super=is_super_admin(user_id))
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        pass
    await callback.answer("Stats updated.")


# ── Super Admin: User Browser & Directory ───────────────────────────────

@router.message(Command("users"))
async def cmd_users(message: Message):
    """Open interactive user browser for Super Admin."""
    if not is_super_admin(message.from_user.id):
        return

    args = message.text.strip().split(maxsplit=1)
    search = args[1].strip() if len(args) > 1 else None

    text, kb = await render_users_browser(page=1, search=search)
    await message.answer(text, parse_mode="HTML", reply_markup=kb)


@router.callback_query(F.data.startswith("cb:usr_page:"))
async def cb_users_page(callback: CallbackQuery):
    """Switch page in user directory."""
    if not is_super_admin(callback.from_user.id):
        await callback.answer("Unauthorized", show_alert=True)
        return

    page = int(callback.data.split(":")[2])
    text, kb = await render_users_browser(page=page)
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        pass
    await callback.answer()


@router.message(Command("user"))
async def cmd_user(message: Message):
    """Directly inspect a specific user by ID or @username."""
    if not is_super_admin(message.from_user.id):
        return

    args = message.text.strip().split(maxsplit=1)
    if len(args) < 2:
        await message.answer("Usage: <code>/user &lt;chat_id or @username&gt;</code>", parse_mode="HTML")
        return

    target = args[1].strip()
    user_data = await db.find_user_by_identifier(target)
    if not user_data:
        await message.answer(f"❌ User <code>{target}</code> not found in database.", parse_mode="HTML")
        return

    text, kb = render_user_profile(user_data)
    await message.answer(text, parse_mode="HTML", reply_markup=kb)


@router.callback_query(F.data.startswith("cb:usr_view:"))
async def cb_user_view(callback: CallbackQuery):
    """View deep profile for a specific user from list."""
    if not is_super_admin(callback.from_user.id):
        await callback.answer("Unauthorized", show_alert=True)
        return

    target_id = int(callback.data.split(":")[2])
    user_data = await db.get_user_details(target_id)
    if not user_data:
        await callback.answer("User record not found.", show_alert=True)
        return

    text, kb = render_user_profile(user_data)
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        pass
    await callback.answer()


@router.callback_query(F.data.startswith("cb:usr_vault:"))
async def cb_user_vault(callback: CallbackQuery):
    """Browse stored vault tracks for a user."""
    if not is_super_admin(callback.from_user.id):
        await callback.answer("Unauthorized", show_alert=True)
        return

    parts = callback.data.split(":")
    target_id = int(parts[2])
    page = int(parts[3]) if len(parts) > 3 else 1

    tracks, total = await db.get_user_vault_tracks(target_id, page=page, page_size=6)
    total_pages = max(1, math.ceil(total / 6))

    user_data = await db.get_user_details(target_id)
    uname = f"@{user_data['username']}" if user_data and user_data.get("username") else str(target_id)

    text = (
        f"📁 <b>Stored Vault:</b> <code>{uname}</code>\n"
        f"• Total Saved Tracks: <b>{total}</b>\n"
        f"• Channel ID: <code>{user_data.get('channel_id', 'None')}</code>\n\n"
        f"Tap any track link to open post in their channel:"
    )

    kb = get_user_vault_keyboard(target_id, tracks, page, total_pages)
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        pass
    await callback.answer()


@router.callback_query(F.data.startswith("cb:usr_unlink:"))
async def cb_user_unlink(callback: CallbackQuery):
    """Forcefully disconnect a user's storage channel."""
    if not is_super_admin(callback.from_user.id):
        await callback.answer("Unauthorized", show_alert=True)
        return

    target_id = int(callback.data.split(":")[2])
    await db.force_unlink_user_channel(target_id)
    await callback.answer("Channel disconnected for this user.", show_alert=True)

    # Refresh profile card
    user_data = await db.get_user_details(target_id)
    if user_data:
        text, kb = render_user_profile(user_data)
        try:
            await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
        except Exception:
            pass


@router.callback_query(F.data.startswith("cb:adm_unlink_ch:"))
async def cb_admin_unlink_channel(callback: CallbackQuery):
    """Super Admin forcefully disconnects a specific storage channel for a user."""
    if not is_super_admin(callback.from_user.id):
        await callback.answer("Unauthorized", show_alert=True)
        return

    parts = callback.data.split(":")
    target_id = int(parts[2])
    channel_id = int(parts[3])

    await db.force_unlink_user_channel(target_id, channel_id=channel_id)
    await callback.answer(f"Channel {channel_id} unlinked.", show_alert=True)

    user_data = await db.get_user_details(target_id)
    if user_data:
        text, kb = render_user_profile(user_data)
        try:
            await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
        except Exception:
            pass


@router.callback_query(F.data.startswith("cb:adm_quota_menu:"))
async def cb_admin_quota_menu(callback: CallbackQuery):
    """Show quota upgrade options for a user."""
    if not is_super_admin(callback.from_user.id):
        await callback.answer("Unauthorized", show_alert=True)
        return

    target_id = int(callback.data.split(":")[2])
    user_data = await db.get_user_details(target_id)
    if not user_data:
        await callback.answer("User not found.", show_alert=True)
        return

    current_max = user_data.get("max_channels", 5)
    fname = user_data.get("first_name", "User")

    kb = get_quota_upgrade_keyboard(target_id)
    text = (
        f"💎 <b>Upgrade Channel Quota for {fname}</b>\n\n"
        f"• Current Quota: <b>{current_max} channels</b>\n"
        f"• Active Channels: <b>{len(user_data.get('channels', []))} channels</b>\n\n"
        "Select new maximum channel limit:"
    )
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        pass
    await callback.answer()


@router.callback_query(F.data.startswith("cb:adm_set_quota:"))
async def cb_admin_set_quota(callback: CallbackQuery):
    """Set channel quota for a user."""
    if not is_super_admin(callback.from_user.id):
        await callback.answer("Unauthorized", show_alert=True)
        return

    parts = callback.data.split(":")
    target_id = int(parts[2])
    new_quota = int(parts[3])

    await db.upgrade_user_quota(target_id, new_quota)
    await callback.answer(f"Channel quota set to {new_quota}!", show_alert=True)

    user_data = await db.get_user_details(target_id)
    if user_data:
        text, kb = render_user_profile(user_data)
        try:
            await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
        except Exception:
            pass


@router.callback_query(F.data.startswith("cb:usr_toggle_adm:"))
async def cb_user_toggle_adm(callback: CallbackQuery):
    """Toggle admin privileges for a user."""
    if not is_super_admin(callback.from_user.id):
        await callback.answer("Unauthorized", show_alert=True)
        return

    target_id = int(callback.data.split(":")[2])
    new_status = await db.toggle_user_admin(target_id)
    status_str = "Promoted to Admin" if new_status else "Demoted from Admin"
    await callback.answer(f"User {status_str}.", show_alert=True)

    user_data = await db.get_user_details(target_id)
    if user_data:
        text, kb = render_user_profile(user_data)
        try:
            await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
        except Exception:
            pass


@router.callback_query(F.data.startswith("cb:usr_toggle_ban:"))
async def cb_user_toggle_ban(callback: CallbackQuery):
    """Toggle ban status for a user."""
    if not is_super_admin(callback.from_user.id):
        await callback.answer("Unauthorized", show_alert=True)
        return

    target_id = int(callback.data.split(":")[2])
    new_banned = await db.toggle_user_ban(target_id)
    status_str = "Banned" if new_banned else "Unbanned"
    await callback.answer(f"User {status_str}.", show_alert=True)

    user_data = await db.get_user_details(target_id)
    if user_data:
        text, kb = render_user_profile(user_data)
        try:
            await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
        except Exception:
            pass


@router.message(Command("dm"))
async def cmd_dm(message: Message, bot: Bot):
    """Send official direct message to a user: /dm <user_id> <message>."""
    if not is_super_admin(message.from_user.id):
        return

    parts = message.text.strip().split(maxsplit=2)
    if len(parts) < 3:
        await message.answer("Usage: <code>/dm &lt;user_id&gt; &lt;message&gt;</code>", parse_mode="HTML")
        return

    target_id_str, text_to_send = parts[1].strip(), parts[2].strip()
    if not target_id_str.isdigit():
        await message.answer("User ID must be numeric.", parse_mode="HTML")
        return

    target_id = int(target_id_str)
    try:
        await bot.send_message(
            chat_id=target_id,
            text=f"📩 <b>Message from Sociobot Administration:</b>\n\n{text_to_send}",
            parse_mode="HTML"
        )
        await message.answer(f"✅ Message successfully sent to user <code>{target_id}</code>.", parse_mode="HTML")
    except Exception as e:
        await message.answer(f"❌ Failed to deliver message to <code>{target_id}</code>: {e}", parse_mode="HTML")


@router.callback_query(F.data.startswith("cb:usr_dm:"))
async def cb_usr_dm_prompt(callback: CallbackQuery):
    """Provide quick hint on sending a DM."""
    target_id = callback.data.split(":")[2]
    await callback.answer(f"To message this user, type: /dm {target_id} <your message>", show_alert=True)


@router.callback_query(F.data == "cb:noop")
async def cb_noop(callback: CallbackQuery):
    """Acknowledge non-interactive status buttons."""
    await callback.answer()


# ── System Maintenance & Broadcast ──────────────────────────────────────

@router.message(Command("cleanup"))
@router.callback_query(F.data == "cb:adm_cleanup")
async def handle_cleanup(event: Message | CallbackQuery):
    """Purge temporary files and expired database cache."""
    user_id = event.from_user.id
    if not await is_admin_user(user_id):
        return

    cleaned_files = 0
    temp_dir = settings.DOWNLOAD_DIR
    for f in glob.glob(os.path.join(temp_dir, "*")):
        try:
            if os.path.isfile(f):
                os.remove(f)
                cleaned_files += 1
        except Exception:
            pass

    cleaned_db = await db.cleanup_expired_cache()
    cache.clear_ram()

    report = (
        f"🧹 <b>Cleanup Complete!</b>\n\n"
        f"• Local temp files removed: <b>{cleaned_files}</b>\n"
        f"• Expired DB cache rows purged: <b>{cleaned_db}</b>\n"
        f"• RAM Cache: <b>Flushed</b>"
    )

    if isinstance(event, CallbackQuery):
        await event.answer("Cleanup completed!", show_alert=False)
        await event.message.answer(report, parse_mode="HTML")
    else:
        await event.answer(report, parse_mode="HTML")


@router.message(Command("broadcast"))
async def cmd_broadcast(message: Message, bot: Bot):
    """Send broadcast message to registered users."""
    if not await is_admin_user(message.from_user.id):
        return

    parts = message.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Usage: <code>/broadcast &lt;announcement text&gt;</code>", parse_mode="HTML")
        return

    announcement = parts[1].strip()
    status_msg = await message.answer("📢 Sending broadcast...", parse_mode="HTML")

    sent = 0
    failed = 0

    if db.is_postgres:
        async def _run(conn):
            return await conn.fetch("SELECT chat_id FROM users WHERE is_banned = FALSE;")
        rows = await db._execute_pg_with_retry(_run)
        chat_ids = [r["chat_id"] for r in rows]
    else:
        cur = await db.sqlite_conn.execute("SELECT chat_id FROM users WHERE is_banned = 0;")
        rows = await cur.fetchall()
        chat_ids = [r[0] for r in rows]

    for cid in chat_ids:
        try:
            await bot.send_message(
                chat_id=cid,
                text=f"📢 <b>Announcement:</b>\n\n{announcement}",
                parse_mode="HTML"
            )
            sent += 1
        except Exception:
            failed += 1

    await status_msg.edit_text(
        f"📢 <b>Broadcast Results:</b>\n\n"
        f"• Delivered: <b>{sent}</b>\n"
        f"• Unreachable/Blocked: <b>{failed}</b>",
        parse_mode="HTML"
    )


# ── Render Helpers ───────────────────────────────────────────────────────

async def render_admin_dashboard(is_super: bool = False):
    """Format dashboard message and buttons."""
    stats = await db.get_stats()

    api_status = "🔴 Unreachable"
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(f"{settings.YTSP_API_BASE_URL.rstrip('/')}/health")
            if resp.status_code == 200:
                api_status = "🟢 Healthy"
    except Exception:
        pass

    text = (
        "⚡ <b>Sociobot Admin Dashboard</b>\n\n"
        f"• <b>Total Users:</b> {stats.get('users', 0)}\n"
        f"• <b>Active Storage Channels:</b> {stats.get('channels', 0)}\n"
        f"• <b>Community Media Nodes:</b> {stats.get('media_nodes', 0)}\n"
        f"• <b>Cached Tracks:</b> {stats.get('unique_tracks', 0)}\n"
        f"• <b>Stream Extractor API:</b> {api_status}\n"
        f"• <b>Database Engine:</b> {f'Neon PostgreSQL (schema: <code>{db.schema}</code>)' if db.is_postgres else 'SQLite (Local)'}\n"
    )
    if is_super:
        text += "👑 <b>Role:</b> Super Administrator\n"
    return text, get_admin_dashboard_keyboard(is_super_admin=is_super)


async def render_users_browser(page: int = 1, search: Optional[str] = None):
    """Format paginated user list."""
    page_size = 8
    users, total = await db.list_users(page=page, page_size=page_size, search=search)
    total_pages = max(1, math.ceil(total / page_size))

    header = f"👥 <b>Registered Users ({total} total)</b>"
    if search:
        header += f" — <i>Filter: '{search}'</i>"
    header += f"\n📄 Page {page} of {total_pages}\n\nTap a user to manage profile, permissions, and storage channel:"

    kb = get_users_browser_keyboard(users, page, total_pages, search=search)
    return header, kb


from bot.utils.platform import get_platform_display_name, get_platform_icon


def render_user_profile(u: dict):
    """Format single user inspection card."""
    chat_id = u.get("chat_id")
    fname = u.get("first_name") or "User"
    uname = f"@{u.get('username')}" if u.get("username") else "None"
    is_admin = u.get("is_admin", False)
    is_banned = u.get("is_banned", False)
    channel_id = u.get("channel_id")
    channel_title = u.get("channel_title") or "None"
    channels = u.get("channels") or []
    routes = u.get("routes") or {}
    max_channels = int(u.get("max_channels") or 5)
    vault_count = u.get("vault_count", 0)
    dl_count = u.get("download_count", 0)
    created_at = str(u.get("created_at", ""))[:19]
    last_active = str(u.get("last_active", ""))[:19]

    role_str = "👑 Super Admin" if is_super_admin(chat_id) else ("⭐ Admin" if is_admin else "👤 Standard User")
    status_str = "🔴 BANNED" if is_banned else "🟢 Active"

    channels_text = []
    if channels:
        for ch in channels:
            cid = ch["channel_id"]
            title = ch.get("channel_title") or f"Vault {cid}"
            star = "🌟 (Primary) " if ch.get("is_primary") else ""
            assigned = [
                f"{get_platform_icon(p)} {get_platform_display_name(p)}"
                for p, t_cid in routes.items()
                if t_cid == cid
            ]
            route_str = f"\n   ↳ <i>Routes:</i> {', '.join(assigned)}" if assigned else ""
            channels_text.append(f"• {star}<b>{title}</b> (<code>{cid}</code>){route_str}")
    elif channel_id:
        channels_text.append(f"• <b>{channel_title}</b> (<code>{channel_id}</code>)")
    else:
        channels_text.append("• <i>No channels connected</i>")

    channels_block = "\n".join(channels_text)

    text = (
        f"👤 <b>User Profile: {fname}</b>\n\n"
        f"• <b>Chat ID:</b> <code>{chat_id}</code>\n"
        f"• <b>Username:</b> {uname}\n"
        f"• <b>Role:</b> {role_str}\n"
        f"• <b>Account Status:</b> {status_str}\n\n"
        f"📁 <b>Connected Vaults ({len(channels)}/{max_channels}):</b>\n"
        f"{channels_block}\n\n"
        f"📊 <b>Activity & Vault:</b>\n"
        f"• <b>Stored in Vault:</b> <b>{vault_count}</b> tracks\n"
        f"• <b>Total Downloads:</b> <b>{dl_count}</b>\n"
        f"• <b>Registered:</b> {created_at}\n"
        f"• <b>Last Active:</b> {last_active}\n"
    )

    kb = get_user_detail_keyboard(
        user_id=chat_id,
        is_admin=is_admin,
        is_banned=is_banned,
        has_channel=bool(channel_id or channels),
        vault_count=vault_count,
        channels=channels,
        max_channels=max_channels
    )
    return text, kb
