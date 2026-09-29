"""Admin Control Panel & System Maintenance for Sociobot.

Features:
- /admin: Overview of registered users, active storage channels, and community media nodes.
- /cleanup: Temp directory file cleanup, expired API cache purge, RAM flush.
- /broadcast: System announcements to registered users.
- Role-based /help command.
"""

import os
import glob
import logging
from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command
import httpx
from bot.config import settings
from bot.database import db
from bot.cache import cache
from bot.keyboards.inline import get_admin_dashboard_keyboard

logger = logging.getLogger(__name__)
router = Router(name="admin")


async def is_admin_user(user_id: int) -> bool:
    """Check if user has administrative privileges."""
    if user_id == settings.ADMIN_CHAT_ID:
        return True
    user = await db.get_or_create_user(user_id)
    return bool(user.get("is_admin", False))


@router.message(Command("help"))
async def cmd_help(message: Message):
    """Provide role-based command reference."""
    is_adm = await is_admin_user(message.from_user.id)

    help_text = (
        "🤖 <b>Sociobot Commands</b>\n\n"
        "<b>Search & Download:</b>\n"
        "• Send any <b>song name</b> to search catalog\n"
        "• Paste a <b>Spotify or YouTube link</b> to download\n"
        "• <code>/search &lt;query&gt;</code> - Search music\n"
        "• <code>/download &lt;link&gt; [force]</code> - Download audio\n"
        "• <code>/video &lt;link&gt; [force]</code> - Download video\n\n"
        "<b>Storage Channel Management:</b>\n"
        "• <code>/mychannel</code> - Check connected channel\n"
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

    await message.answer(help_text, parse_mode="HTML")


@router.message(Command("admin"))
async def cmd_admin(message: Message):
    """Display Administrative Dashboard."""
    if not await is_admin_user(message.from_user.id):
        return

    text, kb = await render_admin_dashboard()
    await message.answer(text, parse_mode="HTML", reply_markup=kb)


@router.callback_query(F.data == "cb:adm_refresh")
async def cb_admin_refresh(callback: CallbackQuery):
    """Refresh admin stats."""
    if not await is_admin_user(callback.from_user.id):
        await callback.answer("Unauthorized", show_alert=True)
        return

    text, kb = await render_admin_dashboard()
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        pass
    await callback.answer("Stats updated.")


@router.message(Command("cleanup"))
@router.callback_query(F.data == "cb:adm_cleanup")
async def handle_cleanup(event: Message | CallbackQuery):
    """Purge temporary files and expired database cache."""
    user_id = event.from_user.id
    if not await is_admin_user(user_id):
        return

    # 1. Clean temp download directory
    cleaned_files = 0
    temp_dir = settings.DOWNLOAD_DIR
    for f in glob.glob(os.path.join(temp_dir, "*")):
        try:
            if os.path.isfile(f):
                os.remove(f)
                cleaned_files += 1
        except Exception:
            pass

    # 2. Purge expired DB cache
    cleaned_db = await db.cleanup_expired_cache()

    # 3. Flush in-memory RAM cache
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

    # Fetch users from DB
    sent = 0
    failed = 0

    if db.is_postgres:
        async def _run(conn):
            return await conn.fetch("SELECT chat_id FROM users;")
        rows = await db._execute_pg_with_retry(_run)
        chat_ids = [r["chat_id"] for r in rows]
    else:
        cur = await db.sqlite_conn.execute("SELECT chat_id FROM users;")
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


async def render_admin_dashboard():
    """Format dashboard message and buttons."""
    stats = await db.get_stats()

    # Test stream API health
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
        f"• <b>Database Engine:</b> {'Neon PostgreSQL' if db.is_postgres else 'SQLite (Local)'}\n"
    )
    return text, get_admin_dashboard_keyboard()
