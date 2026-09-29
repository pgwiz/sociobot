"""Channel Onboarding & Lifecycle Handler for Sociobot.

Handles:
- Auto-detection when bot is added as administrator to user channels.
- Polite onboarding privacy notice and terms acknowledgment.
- Manual /setchannel fallback and /mychannel inspection.
"""

import logging
from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery, ChatMemberUpdated
from aiogram.filters import CommandStart, Command
from aiogram.enums import ChatMemberStatus, ChatType
from bot.config import settings
from bot.database import db
from bot.keyboards.inline import (
    get_onboarding_keyboard,
    get_channel_setup_keyboard
)

logger = logging.getLogger(__name__)
router = Router(name="onboarding")


@router.my_chat_member()
async def on_channel_admin_update(event: ChatMemberUpdated, bot: Bot):
    """Auto-detect when the bot is promoted to administrator in a channel."""
    chat = event.chat
    new_status = event.new_chat_member.status
    old_status = event.old_chat_member.status
    from_user = event.from_user

    logger.info(f"Bot chat member update in {chat.id} ({chat.type}): {old_status} -> {new_status} by user {from_user.id}")

    # Check if promoted to administrator in a channel or supergroup
    if chat.type in (ChatType.CHANNEL, ChatType.SUPERGROUP) and new_status == ChatMemberStatus.ADMINISTRATOR:
        channel_id = chat.id
        channel_title = chat.title or "Private Storage Channel"
        user_id = from_user.id

        # Register or update user record and link channel
        await db.get_or_create_user(
            chat_id=user_id,
            username=from_user.username,
            first_name=from_user.first_name,
            is_admin=(user_id == settings.ADMIN_CHAT_ID)
        )
        await db.link_user_channel(user_id, channel_id, channel_title)

        logger.info(f"Successfully linked channel {channel_id} ('{channel_title}') to user {user_id}")

        # Send polite onboarding confirmation to the user in their PM
        polite_text = (
            f"🎉 <b>Storage Channel Linked!</b>\n\n"
            f"Connected: <b>{channel_title}</b> (<code>{channel_id}</code>)\n\n"
            f"✅ <i>Your media is safely archived here for your full control. "
            f"To keep downloads lightning-fast, audio may also be shared anonymously across the community network.</i>"
        )
        try:
            await bot.send_message(
                chat_id=user_id,
                text=polite_text,
                parse_mode="HTML",
                reply_markup=get_onboarding_keyboard()
            )
        except Exception as e:
            logger.warning(f"Could not send channel link confirmation PM to {user_id}: {e}")

    # Check if bot was kicked or demoted
    elif new_status in (ChatMemberStatus.KICKED, ChatMemberStatus.LEFT, ChatMemberStatus.MEMBER):
        logger.info(f"Bot access removed from channel {chat.id}")
        # Note: Any user mapped to this channel will be prompted on next request


@router.message(CommandStart())
async def cmd_start(message: Message, bot: Bot):
    """Handle /start command with user registration and channel status check."""
    user = message.from_user
    is_admin = (user.id == settings.ADMIN_CHAT_ID)
    await db.get_or_create_user(user.id, user.username, user.first_name, is_admin=is_admin)

    channel_info = await db.get_user_channel(user.id)
    bot_info = await bot.get_me()

    if channel_info and channel_info.get("channel_id") and channel_info.get("is_storage_active"):
        welcome_text = (
            f"👋 Hello, <b>{user.first_name}</b>!\n\n"
            f"Your personal storage channel is connected: <b>{channel_info.get('channel_title', 'Private Channel')}</b>.\n\n"
            f"🎧 <b>How to use:</b>\n"
            f"• Send any <b>song name</b> or artist to search.\n"
            f"• Paste a <b>Spotify</b> or <b>YouTube</b> link to download directly.\n"
            f"• Use /mychannel to manage your connected channel.\n\n"
            f"⚡ <i>All media is stored in your private channel with full delete control.</i>"
        )
        await message.answer(welcome_text, parse_mode="HTML")
    else:
        welcome_text = (
            f"👋 Welcome to <b>Sociobot</b>, <b>{user.first_name}</b>!\n\n"
            f"Sociobot is a decentralized media player where <b>you</b> own your library. "
            f"Media is stored directly in your own private Telegram channel.\n\n"
            f"📌 <b>Quick Setup (takes 10 seconds):</b>\n"
            f"1. Create a private channel in Telegram (e.g. <i>My Music Vault</i>).\n"
            f"2. Add @{bot_info.username} as an <b>Administrator</b> with post permissions.\n"
            f"3. Sociobot will automatically detect and link your channel!"
        )
        await message.answer(
            welcome_text,
            parse_mode="HTML",
            reply_markup=get_channel_setup_keyboard(bot_info.username)
        )


@router.message(Command("mychannel"))
async def cmd_mychannel(message: Message, bot: Bot):
    """Inspect currently linked storage channel."""
    user_id = message.from_user.id
    channel_info = await db.get_user_channel(user_id)
    bot_info = await bot.get_me()

    if channel_info and channel_info.get("channel_id"):
        cid = channel_info["channel_id"]
        title = channel_info.get("channel_title", "Private Channel")
        active = channel_info.get("is_storage_active", True)
        status_text = "🟢 Active" if active else "🔴 Inactive / Disconnected"

        text = (
            f"📁 <b>Your Connected Storage Channel</b>\n\n"
            f"• <b>Title:</b> {title}\n"
            f"• <b>Channel ID:</b> <code>{cid}</code>\n"
            f"• <b>Status:</b> {status_text}\n\n"
            f"💡 To switch to another channel, simply add @{bot_info.username} as admin to the new channel, "
            f"or forward any post from it here with <code>/setchannel</code>."
        )
        await message.answer(text, parse_mode="HTML")
    else:
        text = (
            f"⚠️ <b>No channel connected yet.</b>\n\n"
            f"Add @{bot_info.username} as an administrator to your private channel to link it."
        )
        await message.answer(text, parse_mode="HTML", reply_markup=get_channel_setup_keyboard(bot_info.username))


@router.message(Command("setchannel"))
async def cmd_setchannel(message: Message, bot: Bot):
    """Manual fallback to link channel by ID or username."""
    user_id = message.from_user.id
    args = message.text.strip().split(maxsplit=1)

    target_id_str = None
    if len(args) > 1:
        target_id_str = args[1].strip()
    elif message.forward_from_chat:
        target_id_str = str(message.forward_from_chat.id)

    if not target_id_str:
        await message.answer(
            "📌 <b>How to link manually:</b>\n"
            "Run <code>/setchannel -100xxxxxxxxxx</code> (channel ID)\n"
            "or forward any message from your channel to this chat!",
            parse_mode="HTML"
        )
        return

    try:
        target_id = int(target_id_str)
        chat = await bot.get_chat(target_id)
        member = await bot.get_chat_member(target_id, (await bot.get_me()).id)

        if member.status != ChatMemberStatus.ADMINISTRATOR:
            await message.answer(
                f"⚠️ Bot is in <b>{chat.title}</b>, but is not an administrator. Please grant post permissions.",
                parse_mode="HTML"
            )
            return

        channel_title = chat.title or "Private Channel"
        await db.link_user_channel(user_id, target_id, channel_title)
        await message.answer(
            f"✅ Successfully linked to <b>{channel_title}</b> (<code>{target_id}</code>)!\n\n"
            f"You are all set to search and download music.",
            parse_mode="HTML"
        )
    except Exception as e:
        logger.warning(f"Failed to manually set channel for {user_id}: {e}")
        await message.answer(
            f"❌ Could not access channel <code>{target_id_str}</code>. Make sure the bot is already added as admin.",
            parse_mode="HTML"
        )


@router.callback_query(F.data == "cb:terms_ok")
async def cb_terms_ok(callback: CallbackQuery):
    """User acknowledged polite onboarding notice."""
    await db.set_terms_accepted(callback.from_user.id, True)
    await callback.answer("Awesome! Ready to search and play.", show_alert=False)
    await callback.message.edit_text(
        "🚀 <b>You're all set!</b>\n\n"
        "Send me any song name, artist, or YouTube/Spotify link to get started.",
        parse_mode="HTML"
    )


@router.callback_query(F.data == "cb:terms_info")
async def cb_terms_info(callback: CallbackQuery):
    """Informational modal on how decentralized storage functions."""
    info_text = (
        "ℹ️ <b>How Decentralized Storage Works:</b>\n\n"
        "• <b>Full Ownership:</b> Your downloaded tracks live directly in your personal Telegram channel. You can view, forward, or delete them anytime.\n"
        "• <b>Zero Server Bloat:</b> Audio is cross-copied directly between Telegram channels without consuming server bandwidth or storing copies on external hard drives.\n"
        "• <b>Anonymity:</b> Other users only receive the audio track via Telegram copy. No personal details, usernames, or channel links are exposed."
    )
    await callback.answer()
    await callback.message.answer(info_text, parse_mode="HTML")


@router.callback_query(F.data == "cb:check_channel")
async def cb_check_channel(callback: CallbackQuery, bot: Bot):
    """Verify channel status on demand."""
    user_id = callback.from_user.id
    channel_info = await db.get_user_channel(user_id)
    bot_info = await bot.get_me()

    if channel_info and channel_info.get("channel_id"):
        try:
            cid = channel_info["channel_id"]
            member = await bot.get_chat_member(cid, bot_info.id)
            if member.status == ChatMemberStatus.ADMINISTRATOR:
                await callback.answer("✅ Channel is connected and active!", show_alert=True)
                await callback.message.edit_text(
                    f"✅ <b>Storage Channel Active:</b> {channel_info.get('channel_title', 'Channel')}\n\n"
                    f"Send any song title or link to download!",
                    parse_mode="HTML"
                )
                return
        except Exception:
            pass

    await callback.answer(
        "⚠️ No active admin channel found. Add the bot to your channel first!",
        show_alert=True
    )
