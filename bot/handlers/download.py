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
from bot.downloader import downloader, has_video_stream, is_video_quality
from bot.cache import cache
from bot.utils.track_ref import create_track_ref, register_track_ref, resolve_track_ref
from bot.utils.platform import (
    detect_platform_and_url,
    get_quality_for_platform,
    is_social_video,
    get_platform_display_name,
    get_platform_icon
)
from bot.keyboards.inline import (
    format_channel_post_url,
    get_media_delivery_keyboard,
    get_channel_setup_keyboard,
    get_format_picker_keyboard,
    get_channel_picker_keyboard
)

logger = logging.getLogger(__name__)
router = Router(name="download")


async def handle_direct_url_request(message: Message, url: str):
    """Invoked when user pastes any supported media URL in chat."""
    user_id = message.from_user.id
    platform, clean_url = detect_platform_and_url(url)
    target_url = clean_url or url
    platform_name = platform or "other"

    channels = await db.get_user_channels(user_id)
    if not channels:
        bot_info = await message.bot.get_me()
        warn_text = (
            "⚠️ <b>Storage Channel Required</b>\n\n"
            "To download and maintain total control over your media, you need to connect your personal private channel.\n\n"
            f"Please add @{bot_info.username} as an <b>Administrator</b> to your channel, and it will be linked instantly!\n\n"
            "Use /channels to view and configure your storage vaults."
        )
        kb = get_channel_setup_keyboard(bot_info.username)
        await message.answer(warn_text, parse_mode="HTML", reply_markup=kb)
        return

    # Check if this platform has an explicit route
    existing_route = await db.get_platform_route(user_id, platform_name)

    # If unrouted and user has multiple channels -> prompt dynamic channel picker once
    if not existing_route and len(channels) > 1:
        quality = get_quality_for_platform(platform_name)
        display_name = get_platform_display_name(platform_name)
        icon = get_platform_icon(platform_name)
        ref = await register_track_ref(target_url)
        await cache.set(f"pending_route_dl:{ref}", {"url": target_url, "platform": platform_name, "quality": quality}, ttl=300)
        await cache.set(f"pending_route_dl:{user_id}", {"url": target_url, "platform": platform_name, "quality": quality}, ttl=300)
        kb = get_channel_picker_keyboard(channels, platform_name, track_ref=ref)
        await message.answer(
            f"🔗 <b>Detected {icon} {display_name} Link:</b>\n"
            f"<code>{target_url}</code>\n\n"
            f"Where should <b>{display_name}</b> media be saved?\n"
            f"<i>(We'll remember your choice for future links!)</i>",
            parse_mode="HTML",
            reply_markup=kb
        )
        return

    # If YouTube, offer format selection (Audio vs Video)
    if platform_name == "youtube":
        ref = await register_track_ref(target_url)
        kb = get_format_picker_keyboard(ref)
        await message.answer(
            f"🔗 <b>Detected Media Link:</b>\n<code>{target_url}</code>\n\n"
            f"Choose download format:",
            parse_mode="HTML",
            reply_markup=kb
        )
        return

    # For social videos and music platforms: proceed directly
    quality = get_quality_for_platform(platform_name)
    display_name = get_platform_display_name(platform_name)
    icon = get_platform_icon(platform_name)
    status_msg = await message.answer(
        f"⏳ <b>Processing {icon} {display_name} download...</b>",
        parse_mode="HTML"
    )
    await process_media_request(
        bot=message.bot,
        user_id=user_id,
        reply_to_chat_id=message.chat.id,
        track_id=target_url,
        quality=quality,
        force=False,
        status_message=status_msg,
        platform=platform_name
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
    raw_track_id = parts[2]
    quality = parts[3] if len(parts) > 3 else "audio_high"
    track_id = await resolve_track_ref(raw_track_id)

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
    raw_track_id = parts[2]
    quality = parts[3] if len(parts) > 3 else "audio_high"
    track_id = await resolve_track_ref(raw_track_id)

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
    status_message: Optional[Message] = None,
    platform: Optional[str] = None
):
    """Core orchestration for decentralized peer delivery and media archiving."""
    # Ensure track_id is resolved and registered for compact button callbacks
    track_id = await resolve_track_ref(track_id)
    await register_track_ref(track_id)

    if await db.is_user_banned(user_id):
        banned_msg = "🚫 <b>Your access to Sociobot has been restricted by an administrator.</b>"
        if status_message:
            await status_message.edit_text(banned_msg, parse_mode="HTML")
        else:
            await bot.send_message(chat_id=reply_to_chat_id, text=banned_msg, parse_mode="HTML")
        return

    bot_info = await bot.get_me()

    if not platform:
        p, _ = detect_platform_and_url(track_id)
        platform = p if p else "youtube"

    if is_social_video(platform) and quality == "saver":
        quality = "360p"

    # 1. Verify user's destination private channel
    user_channel_id = await db.get_destination_channel(user_id, platform)
    if not user_channel_id:
        warn_text = (
            "⚠️ <b>Storage Channel Required</b>\n\n"
            "To download and maintain total control over your media, you need to connect your personal private channel.\n\n"
            f"Please add @{bot_info.username} as an <b>Administrator</b> to your channel, and it will be linked instantly!\n\n"
            "Use /channels to view and configure your storage vaults."
        )
        kb = get_channel_setup_keyboard(bot_info.username)
        if status_message:
            await status_message.edit_text(warn_text, parse_mode="HTML", reply_markup=kb)
        else:
            await bot.send_message(chat_id=reply_to_chat_id, text=warn_text, parse_mode="HTML", reply_markup=kb)
        return

    show_extract = is_social_video(platform) and (quality in ("saver", "720p", "360p", "best", "video"))

    # 2. Check if user already owns this track in their own channel (unless forced)
    if not force:
        existing_copy = await db.get_user_stored_track(user_id, track_id, quality)
        if existing_copy and existing_copy.get("channel_msg_id"):
            try:
                # Fast delivery directly from user's channel to PM
                post_url = existing_copy.get("channel_post_url") or format_channel_post_url(user_channel_id, existing_copy["channel_msg_id"])
                delivery_kb = get_media_delivery_keyboard(post_url, track_id, quality, show_extract_audio=show_extract)
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
                        telegram_file_id=peer.get("telegram_file_id"),
                        platform=platform,
                        destination_channel_id=user_channel_id
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
                    delivery_kb = get_media_delivery_keyboard(post_url, track_id, quality, show_extract_audio=show_extract)
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
        if platform in ("tiktok", "instagram"):
            err_text = (
                f"❌ Could not download from <b>{get_platform_display_name(platform)}</b>.\n\n"
                "The platform may be blocking access or the media is private/restricted. "
                "Please try another link or verify the post is public."
            )
        else:
            err_text = "❌ Failed to download or convert media. The source might be restricted or unavailable."
        if status_message:
            await status_message.edit_text(err_text, parse_mode="HTML")
        else:
            await bot.send_message(chat_id=reply_to_chat_id, text=err_text, parse_mode="HTML")
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
            if not await has_video_stream(file_path):
                logger.error(f"Cannot upload to user channel {user_channel_id}: {file_path} has no valid video stream!")
                err_text = "❌ Downloaded video is corrupted or missing a video stream. Please try again."
                if status_message:
                    await status_message.edit_text(err_text, parse_mode="HTML")
                else:
                    await bot.send_message(chat_id=reply_to_chat_id, text=err_text, parse_mode="HTML")
                return

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
            telegram_file_id=file_id,
            platform=platform,
            destination_channel_id=user_channel_id
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
        delivery_kb = get_media_delivery_keyboard(post_url, track_id, quality, show_extract_audio=show_extract)
        try:
            await bot.copy_message(
                chat_id=reply_to_chat_id,
                from_chat_id=user_channel_id,
                message_id=channel_msg.message_id,
                reply_markup=delivery_kb
            )
        except (TelegramForbiddenError, TelegramBadRequest) as e:
            logger.error(f"Cannot deliver message to user PM {reply_to_chat_id}: {e}")

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


@router.callback_query(F.data.startswith("cb:extract_audio:"))
async def cb_extract_audio(callback: CallbackQuery, bot: Bot):
    """Handle audio extraction request from a social video post."""
    raw_track_id = callback.data.split("cb:extract_audio:")[1].strip()
    track_id = await resolve_track_ref(raw_track_id)
    await callback.answer("🎵 Extracting audio track as MP3...")
    status_msg = await callback.message.answer("⏳ <i>Extracting audio track as MP3...</i>", parse_mode="HTML")

    p, _ = detect_platform_and_url(track_id)
    platform = p if p else "youtube"

    await process_media_request(
        bot=bot,
        user_id=callback.from_user.id,
        reply_to_chat_id=callback.message.chat.id,
        track_id=track_id,
        quality="audio_high",
        force=False,
        status_message=status_msg,
        platform=platform
    )
