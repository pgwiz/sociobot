"""User Deletion & Library Management Handler for Sociobot.

Gives users total control over their personal storage:
- Instant interactive deletion via [ 🗑️ Delete ] button on delivered media.
- Deletion command: /delete <track_id> or /del <track_id>.
- /history command showing recent channel posts with direct links and deletion hints.
"""

import logging
from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command
from aiogram.exceptions import TelegramBadRequest
from bot.database import db
from bot.utils.track_ref import resolve_track_ref

logger = logging.getLogger(__name__)
router = Router(name="delete")


@router.callback_query(F.data.startswith("cb:del:"))
async def cb_delete_media(callback: CallbackQuery, bot: Bot):
    """Handle instant interactive deletion from user's channel and database."""
    user_id = callback.from_user.id
    parts = callback.data.split(":")
    raw_track_id = parts[2]
    quality = parts[3] if len(parts) > 3 else None
    track_id = await resolve_track_ref(raw_track_id)

    # Delete records from DB and get channel message reference
    deleted_items = await db.delete_user_media(user_id, track_id, quality)
    if not deleted_items and track_id != raw_track_id:
        deleted_items = await db.delete_user_media(user_id, raw_track_id, quality)

    # Physically delete message from user's channel
    deleted_channel_posts = 0
    for item in deleted_items:
        cid = item.get("channel_id")
        mid = item.get("channel_msg_id")
        if cid and mid:
            try:
                await bot.delete_message(chat_id=cid, message_id=mid)
                deleted_channel_posts += 1
            except TelegramBadRequest as e:
                logger.warning(f"Could not delete message {mid} in channel {cid}: {e}")

    # Remove the message in PM or edit caption
    try:
        await callback.message.delete()
    except Exception:
        try:
            await callback.message.edit_caption(
                caption="🗑️ <i>Media deleted from your channel and library.</i>",
                parse_mode="HTML"
            )
        except Exception:
            pass

    await callback.answer("🗑️ Removed from your channel and personal vault.", show_alert=False)


@router.message(Command("delete"))
@router.message(Command("del"))
async def cmd_delete(message: Message, bot: Bot):
    """Purge track from user's channel: /delete <#track_id>."""
    user_id = message.from_user.id
    parts = message.text.strip().split()
    if len(parts) < 2:
        await message.answer("Usage: <code>/delete &lt;track_id&gt;</code> (or check /history for IDs)", parse_mode="HTML")
        return

    raw_track_id = parts[1].strip().lstrip("#")
    track_id = await resolve_track_ref(raw_track_id)
    deleted_items = await db.delete_user_media(user_id, track_id)
    if not deleted_items and track_id != raw_track_id:
        deleted_items = await db.delete_user_media(user_id, raw_track_id)

    if not deleted_items:
        await message.answer(f"⚠️ Track <code>{track_id}</code> was not found in your library.", parse_mode="HTML")
        return

    for item in deleted_items:
        cid = item.get("channel_id")
        mid = item.get("channel_msg_id")
        if cid and mid:
            try:
                await bot.delete_message(chat_id=cid, message_id=mid)
            except Exception as e:
                logger.debug(f"Could not delete msg {mid} in {cid}: {e}")

    await message.answer(
        f"🗑️ Successfully deleted <b>{len(deleted_items)}</b> copy(s) of <code>{track_id}</code> from your channel.",
        parse_mode="HTML"
    )


@router.message(Command("history"))
async def cmd_history(message: Message):
    """Display user's recent downloads with direct channel links."""
    user_id = message.from_user.id
    history = await db.get_user_history(user_id, limit=10)

    if not history:
        await message.answer("📂 Your download history is currently empty.", parse_mode="HTML")
        return

    lines = ["📂 <b>Your Recent Media Vault:</b>\n"]
    for idx, item in enumerate(history, 1):
        title = item.get("title") or "Unknown Track"
        quality = item.get("quality") or "audio"
        post_url = item.get("channel_post_url")
        track_id = item.get("track_id")

        link_part = f"<a href='{post_url}'>Channel Post ↗</a>" if post_url else ""
        lines.append(f"{idx}. <b>{title}</b> ({quality})\n   ID: <code>{track_id}</code> | {link_part}")

    lines.append("\n💡 <i>To delete any track, use <code>/delete &lt;ID&gt;</code></i>")
    await message.answer("\n".join(lines), parse_mode="HTML", disable_web_page_preview=True)
