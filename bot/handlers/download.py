"""Decentralized Media Download & Peer Replication Engine for Sociobot.

Flow:
1. Verify user's personal channel is active.
2. Check if user already owns this track in their channel (instant PM delivery).
3. If not, check if any peer community channel has it cached.
   - If yes: Replicate directly to user's channel via copy_message.
4. If fresh or forced:
   - Download & package via ytsp-api (or yt-dlp fallback).
   - Post to user's channel.
   - Record in database with direct channel deep-link.
   - Deliver playable copy to user's PM with [ 📂 Open in Channel ], [ 🗑️ Delete ], and [ ⚡ Force ].
"""

import os
import logging
from typing import Optional
from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery, FSInputFile
from aiogram.filters import Command
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from bot.config import settings
from bot.database import db
from bot.downloader import downloader
from bot.keyboards.inline import (
    format_channel_post_url,
    get_media_delivery_keyboard,
    get_channel_setup_keyboard,
    get_format_picker_keyboard
)

logger = logging.getLogger(__name__)
router = Router(name="download")


async def handle_direct_url_request(message: Message, url: str):
    """Invoked when user pastes a Spotify or YouTube URL in chat."""
    # Present format selector for the URL
    kb = get_format_picker_keyboard(url)
    await message.answer(
        f"🔗 <b>Detected Media Link:</b>\n<code>{url}</code>\n\n"
        f"Choose download format:",
        parse_mode="HTML",
        reply_markup=kb
    )


@router.message(Command("download"))
@router.message(Command("song"))
async def cmd_download(message: Message, bot: Bot):
    """Download audio track: /download <url_or_id> [force]."""
    parts = message.text.strip().split()
    if len(parts) < 2:
        await message.answer("Usage: <code>/download <link_or_song_id> [force]</code>", parse_mode="HTML")
        return

    target = parts[1].strip()
    force = len(parts) > 2 and parts[2].lower() == "force"
    await process_media_request(
        bot=bot,
        user_id=message.from_user.id,
        reply_to_chat_id=message.chat.id,
        track_id=target,
        quality="audio_high",
        force=force
    )


@router.message(Command("video"))
async def cmd_video(message: Message, bot: Bot):
    """Download video: /video <url_or_id> [force]."""
    parts = message.text.strip().split()
    if len(parts) < 2:
        await message.answer("Usage: <code>/video <link_or_song_id> [force]</code>", parse_mode="HTML")
        return

    target = parts[1].strip()
    force = len(parts) > 2 and parts[2].lower() == "force"
    await process_media_request(
        bot=bot,
        user_id=message.from_user.id,
        reply_to_chat_id=message.chat.id,
        track_id=target,
        quality="720p",
        force=force
    )


@router.callback_query(F.data.startswith("cb:dl:"))
async def cb_download_format(callback: CallbackQuery, bot: Bot):
    """Handle download from inline format selector: cb:dl:<track_id>:<quality>."""
    parts = callback.data.split(":")
    track_id = parts[2]
    quality = parts[3] if len(parts) > 3 else "audio_high"

    await callback.answer("Starting download...")
    status_msg = await callback.message.edit_text("⏳ Processing request...", parse_mode="HTML")

    await process_media_request(
        bot=bot,
        user_id=callback.from_user.id,
        reply_to_chat_id=callback.message.chat.id,
        track_id=track_id,
        quality=quality,
        force=False,
        status_message=status_msg
    )


@router.callback_query(F.data.startswith("cb:force:"))
async def cb_force_download(callback: CallbackQuery, bot: Bot):
    """Handle force re-download: cb:force:<track_id>:<quality>."""
    parts = callback.data.split(":")
    track_id = parts[2]
    quality = parts[3] if len(parts) > 3 else "audio_high"

    await callback.answer("⚡ Force re-downloading fresh media...")
    status_msg = await callback.message.answer("⚡ Bypassing cached copies and fetching fresh...", parse_mode="HTML")

    await process_media_request(
        bot=bot,
        user_id=callback.from_user.id,
        reply_to_chat_id=callback.message.chat.id,
        track_id=track_id,
        quality=quality,
        force=True,
        status_message=status_msg
    )


async def process_media_request(
    bot: Bot,
    user_id: int,
    reply_to_chat_id: int,
    track_id: str,
    quality: str,
    force: bool = False,
    status_message: Optional[Message] = None
):
    """Core orchestration for decentralized peer delivery and media archiving."""
    if await db.is_user_banned(user_id):
        banned_msg = "🚫 <b>Your access to Sociobot has been restricted by an administrator.</b>"
        if status_message:
            await status_message.edit_text(banned_msg, parse_mode="HTML")
        else:
            await bot.send_message(chat_id=reply_to_chat_id, text=banned_msg, parse_mode="HTML")
        return

    bot_info = await bot.get_me()

    # 1. Verify user's connected private channel
    channel_info = await db.get_user_channel(user_id)
    if not channel_info or not channel_info.get("channel_id") or not channel_info.get("is_storage_active"):
        warn_text = (
            "⚠️ <b>Storage Channel Required</b>\n\n"
            "To download and maintain total control over your media, you need to connect your personal private channel.\n\n"
            f"Please add @{bot_info.username} as an <b>Administrator</b> to your channel, and it will be linked instantly!"
        )
        kb = get_channel_setup_keyboard(bot_info.username)
        if status_message:
            await status_message.edit_text(warn_text, parse_mode="HTML", reply_markup=kb)
        else:
            await bot.send_message(chat_id=reply_to_chat_id, text=warn_text, parse_mode="HTML", reply_markup=kb)
        return

    user_channel_id = channel_info["channel_id"]

    # 2. Check if user already owns this track in their own channel (unless forced)
    if not force:
        existing_copy = await db.get_user_stored_track(user_id, track_id, quality)
        if existing_copy and existing_copy.get("channel_msg_id"):
            try:
                # Fast delivery directly from user's channel to PM
                post_url = existing_copy.get("channel_post_url") or format_channel_post_url(user_channel_id, existing_copy["channel_msg_id"])
                delivery_kb = get_media_delivery_keyboard(post_url, track_id, quality)
                await bot.copy_message(
                    chat_id=reply_to_chat_id,
                    from_chat_id=user_channel_id,
                    message_id=existing_copy["channel_msg_id"],
                    reply_markup=delivery_kb
                )
                if status_message:
                    try:
                        await status_message.delete()
                    except Exception:
                        pass
                return
            except Exception as e:
                logger.warning(f"Could not copy from user's existing copy (msg {existing_copy['channel_msg_id']}): {e}")

    # 3. Check for Peer-to-Peer Replication across community channels (unless forced)
    if not force:
        peer_sources = await db.find_available_peer_sources(track_id, quality)
        for peer in peer_sources:
            peer_channel_id = peer.get("channel_id")
            peer_msg_id = peer.get("channel_msg_id")

            # Skip if peer is the same channel
            if peer_channel_id == user_channel_id:
                continue

            try:
                logger.info(f"Replicating track {track_id} from peer channel {peer_channel_id} to user channel {user_channel_id}")
                # Replicate from peer channel into user's personal channel
                replicated_msg = await bot.copy_message(
                    chat_id=user_channel_id,
                    from_chat_id=peer_channel_id,
                    message_id=peer_msg_id
                )

                if replicated_msg:
                    post_url = format_channel_post_url(user_channel_id, replicated_msg.message_id)
                    # Register user's new peer node in database
                    await db.save_user_media(
                        user_chat_id=user_id,
                        channel_id=user_channel_id,
                        channel_msg_id=replicated_msg.message_id,
                        channel_post_url=post_url,
                        track_id=track_id,
                        quality=quality,
                        telegram_file_id=peer.get("telegram_file_id")
                    )

                    await db.log_download(
                        user_chat_id=user_id,
                        track_id=track_id,
                        title=peer.get("title") or "Audio Track",
                        quality=quality,
                        channel_msg_id=replicated_msg.message_id,
                        channel_post_url=post_url
                    )

                    # Deliver to user PM
                    delivery_kb = get_media_delivery_keyboard(post_url, track_id, quality)
                    await bot.copy_message(
                        chat_id=reply_to_chat_id,
                        from_chat_id=user_channel_id,
                        message_id=replicated_msg.message_id,
                        reply_markup=delivery_kb
                    )

                    if status_message:
                        try:
                            await status_message.delete()
                        except Exception:
                            pass
                    return

            except (TelegramBadRequest, TelegramForbiddenError) as e:
                logger.warning(f"Peer copy failed from {peer_channel_id} ({e}), marking peer node unavailable")
                await db.mark_node_unavailable(peer_channel_id, peer_msg_id)
                continue
            except Exception as e:
                logger.warning(f"Unexpected error during peer replication: {e}")
                continue

    # 4. Fresh Download & Packaging from Stream Extractor API
    if status_message:
        try:
            await status_message.edit_text("⏳ <i>Downloading and packaging media...</i>", parse_mode="HTML")
        except Exception:
            pass

    file_path, thumb_path, metadata = await downloader.download_track(track_id, quality=quality, force_fallback=force)

    if not file_path or not os.path.exists(file_path):
        err_text = "❌ Failed to download or convert media. The source might be restricted or unavailable."
        if status_message:
            await status_message.edit_text(err_text)
        else:
            await bot.send_message(chat_id=reply_to_chat_id, text=err_text)
        return

    # 5. Upload media directly to user's personal channel
    title = metadata.get("title") or "Audio Track"
    artist = metadata.get("artist") or "Unknown Artist"
    duration = metadata.get("duration") or 0
    is_video = metadata.get("is_video", False)

    caption = (
        f"{'🎬' if is_video else '🎵'} <b>{title}</b>\n"
        f"👤 {artist}\n"
        f"⚡ <i>Sociobot Vault</i>"
    )

    try:
        if is_video:
            channel_msg = await bot.send_video(
                chat_id=user_channel_id,
                video=FSInputFile(file_path),
                thumbnail=FSInputFile(thumb_path) if thumb_path and os.path.exists(thumb_path) else None,
                caption=caption,
                parse_mode="HTML",
                supports_streaming=True
            )
            file_id = channel_msg.video.file_id if channel_msg.video else None
        else:
            channel_msg = await bot.send_audio(
                chat_id=user_channel_id,
                audio=FSInputFile(file_path),
                thumbnail=FSInputFile(thumb_path) if thumb_path and os.path.exists(thumb_path) else None,
                title=title,
                performer=artist,
                duration=duration,
                caption=caption,
                parse_mode="HTML"
            )
            file_id = channel_msg.audio.file_id if channel_msg.audio else None

        post_url = format_channel_post_url(user_channel_id, channel_msg.message_id)

        # Save to database
        await db.save_user_media(
            user_chat_id=user_id,
            channel_id=user_channel_id,
            channel_msg_id=channel_msg.message_id,
            channel_post_url=post_url,
            track_id=track_id,
            quality=quality,
            telegram_file_id=file_id
        )

        await db.save_track_metadata(
            track_id=track_id,
            quality=quality,
            title=title,
            artist=artist,
            duration_secs=duration,
            file_size_bytes=os.path.getsize(file_path),
            source=metadata.get("source", "api_stream")
        )

        await db.log_download(
            user_chat_id=user_id,
            track_id=track_id,
            title=title,
            quality=quality,
            channel_msg_id=channel_msg.message_id,
            channel_post_url=post_url
        )

        # Deliver to user PM with interactive action buttons
        delivery_kb = get_media_delivery_keyboard(post_url, track_id, quality)
        await bot.copy_message(
            chat_id=reply_to_chat_id,
            from_chat_id=user_channel_id,
            message_id=channel_msg.message_id,
            reply_markup=delivery_kb
        )

        if status_message:
            try:
                await status_message.delete()
            except Exception:
                pass

    except (TelegramForbiddenError, TelegramBadRequest) as e:
        logger.error(f"Cannot upload to user channel {user_channel_id}: {e}")
        err_msg = (
            "⚠️ <b>Channel Upload Error</b>\n\n"
            f"The bot could not post to your storage channel: <code>{e}</code>\n\n"
            "Please make sure the bot has permission to post messages in your channel!"
        )
        if status_message:
            await status_message.edit_text(err_msg, parse_mode="HTML")
        else:
            await bot.send_message(chat_id=reply_to_chat_id, text=err_msg, parse_mode="HTML")

    finally:
        # Cleanup temp local files
        downloader.cleanup_files(file_path, thumb_path)
