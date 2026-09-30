"""Channel Onboarding & Lifecycle Handler for Sociobot.

Handles:
- Auto-detection when bot is added as administrator to user channels.
- Polite onboarding privacy notice and terms acknowledgment.
- Manual /setchannel fallback and /mychannel inspection.
"""

import logging
from aiogram import Router, F, Bot
from aiogram.types import (
    Message,
    CallbackQuery,
    ChatMemberUpdated,
    InlineKeyboardMarkup,
    InlineKeyboardButton
)
from aiogram.filters import CommandStart, Command
from aiogram.enums import ChatMemberStatus, ChatType
from bot.config import settings
from bot.database import db
from bot.keyboards.inline import (
    get_onboarding_keyboard,
    get_channel_setup_keyboard,
    get_platform_route_keyboard
)

logger = logging.getLogger(__name__)
router = Router(name="onboarding")


@router.my_chat_member()
async def on_channel_admin_update(event: ChatMemberUpdated, bot: Bot):
    """Auto-detect when the bot is promoted to administrator in a channel or group."""
    chat = event.chat
    new_status = event.new_chat_member.status
    old_status = event.old_chat_member.status
    from_user = event.from_user

    logger.info(
        f"Bot chat member update in {chat.id} ({chat.type}): {old_status} -> {new_status} "
        f"by user {from_user.id if from_user else 'None'}"
    )

    if not from_user or from_user.is_bot:
        return

    user_id = from_user.id
    channel_id = chat.id
    channel_title = chat.title or "Private Storage Channel"

    # Check if promoted to administrator in a channel, supergroup, or group
    if chat.type in (ChatType.CHANNEL, ChatType.SUPERGROUP, ChatType.GROUP) and new_status == ChatMemberStatus.ADMINISTRATOR:
        # Check quota
        channels = await db.get_user_channels(user_id)
        max_channels = await db.get_user_max_channels(user_id)
        already_linked = any(c["channel_id"] == channel_id for c in channels)

        if not already_linked and len(channels) >= max_channels:
            try:
                await bot.send_message(
                    chat_id=user_id,
                    text=(
                        f"⚠️ <b>Channel Quota Reached ({len(channels)}/{max_channels})</b>\n\n"
                        f"You have reached your limit of {max_channels} connected storage channels.\n\n"
                        f"To link more channels, please ask a Super Admin to upgrade your quota, "
                        f"or unlink an existing channel with /channels."
                    ),
                    parse_mode="HTML"
                )
            except Exception:
                pass
            return

        # Register or update user record and link channel
        await db.get_or_create_user(
            chat_id=user_id,
            username=from_user.username,
            first_name=from_user.first_name,
            is_admin=(user_id == settings.ADMIN_CHAT_ID)
        )
        await db.add_user_channel(user_id, channel_id, channel_title)
        logger.info(f"Successfully linked {chat.type} {channel_id} ('{channel_title}') to user {user_id}")

        bot_info = await bot.get_me()

        # 1. Post confirmation message directly in the channel so user gets immediate visual feedback!
        channel_post_text = (
            f"🎉 <b>Sociobot Storage Vault Connected!</b>\n\n"
            f"✅ This channel is now linked to <b>{from_user.first_name}</b> (<code>{user_id}</code>).\n\n"
            f"🎵 All music & videos downloaded will be automatically archived in this vault with instant delete control."
        )
        try:
            channel_kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text="🎧 Open Sociobot in PM",
                    url=f"https://t.me/{bot_info.username}?start=vault_linked"
                )]
            ])
            await bot.send_message(
                chat_id=channel_id,
                text=channel_post_text,
                parse_mode="HTML",
                reply_markup=channel_kb
            )
        except Exception as e:
            logger.warning(f"Could not post confirmation to channel {channel_id}: {e}")

        # 2. Also send onboarding confirmation to the user in their PM
        try:
            if len(channels) >= 1 and not already_linked:
                # User now has multiple channels -> offer platform routing
                routes = await db.get_all_platform_routes(user_id)
                await bot.send_message(
                    chat_id=user_id,
                    text=(
                        f"🎉 <b>New Vault Connected: {channel_title}</b>\n\n"
                        f"You now have <b>{len(channels) + 1}</b> storage vaults.\n"
                        f"Which platforms should be saved to this channel?"
                    ),
                    parse_mode="HTML",
                    reply_markup=get_platform_route_keyboard(channel_id, user_id, routes)
                )
            else:
                polite_text = (
                    f"🎉 <b>Storage Channel Linked!</b>\n\n"
                    f"Connected: <b>{channel_title}</b> (<code>{channel_id}</code>)\n\n"
                    f"✅ <i>Your media is safely archived here for your full control. "
                    f"To keep downloads lightning-fast, audio may also be shared anonymously across the community network.</i>"
                )
                await bot.send_message(
                    chat_id=user_id,
                    text=polite_text,
                    parse_mode="HTML",
                    reply_markup=get_onboarding_keyboard()
                )
        except Exception as e:
            logger.info(f"User {user_id} notification failed: {e}")

    # Check if bot was added as ordinary member in a group, guide user to promote it
    elif chat.type in (ChatType.SUPERGROUP, ChatType.GROUP) and new_status == ChatMemberStatus.MEMBER:
        bot_info = await bot.get_me()
        try:
            await bot.send_message(
                chat_id=chat.id,
                text=(
                    f"👋 <b>Thanks for adding Sociobot!</b>\n\n"
                    f"To use this group as your storage vault, please promote me to <b>Administrator</b> "
                    f"with <i>Post Messages</i> and <i>Delete Messages</i> permissions."
                ),
                parse_mode="HTML"
            )
        except Exception:
            pass

    # Check if bot was kicked or demoted
    elif new_status in (ChatMemberStatus.KICKED, ChatMemberStatus.LEFT):
        logger.info(f"Bot access removed from channel {chat.id}")
        # Note: Any user mapped to this channel will be prompted on next request


@router.message(CommandStart())
async def cmd_start(message: Message, bot: Bot):
    """Handle /start command with user registration and channel status check."""
    user = message.from_user
    is_admin = (user.id == settings.ADMIN_CHAT_ID)
    await db.get_or_create_user(user.id, user.username, user.first_name, is_admin=is_admin)

    channel_info = await db.get_user_channel(user.id)
    channels = await db.get_user_channels(user.id)
    bot_info = await bot.get_me()

    if channel_info and channel_info.get("channel_id") and channel_info.get("is_storage_active"):
        ch_count = len(channels)
        welcome_text = (
            f"👋 Hello, <b>{user.first_name}</b>!\n\n"
            f"Your personal storage vault is connected: <b>{channel_info.get('channel_title', 'Private Channel')}</b>.\n"
            f"Total vaults linked: <b>{ch_count}</b>.\n\n"
            f"🎧 <b>How to use:</b>\n"
            f"• Send any <b>song name</b> or artist to search.\n"
            f"• Paste a <b>Spotify</b>, <b>YouTube</b>, <b>TikTok</b>, <b>Instagram</b>, or other media link.\n"
            f"• Use /channels to manage storage channels & customize platform routes.\n\n"
            f"⚡ <i>All media is stored in your private channels with full delete control.</i>"
        )
        await message.answer(welcome_text, parse_mode="HTML")
    else:
        welcome_text = (
            f"👋 Welcome to <b>Sociobot</b>, <b>{user.first_name}</b>!\n\n"
            f"Sociobot is a decentralized multi-platform media player where <b>you</b> own your library. "
            f"Media is stored directly in your own private Telegram channel(s).\n\n"
            f"📌 <b>Quick Setup (takes 10 seconds):</b>\n"
            f"1. Create a private channel in Telegram (e.g. <i>My Music Vault</i>).\n"
            f"2. Add @{bot_info.username} as an <b>Administrator</b> with post permissions.\n"
            f"3. Sociobot will automatically detect and link your channel!\n\n"
            f"💡 You can connect up to 5 storage channels and route different platforms to different channels!"
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
    channels = await db.get_user_channels(user_id)
    bot_info = await bot.get_me()

    if channels:
        primary = next((c for c in channels if c.get("is_primary")), channels[0])
        cid = primary["channel_id"]
        title = primary.get("channel_title", "Private Channel")
        active = primary.get("is_active", True)
        status_text = "🟢 Active" if active else "🔴 Inactive / Disconnected"

        text = (
            f"📁 <b>Your Primary Storage Channel</b>\n\n"
            f"• <b>Title:</b> {title}\n"
            f"• <b>Channel ID:</b> <code>{cid}</code>\n"
            f"• <b>Status:</b> {status_text}\n"
            f"• <b>Total Channels Linked:</b> <b>{len(channels)}</b>\n\n"
            f"💡 <i>Tip: Use <code>/channels</code> to view all your storage vaults, route platforms, "
            f"or unlink channels.</i>"
        )
        await message.answer(text, parse_mode="HTML")
    else:
        text = (
            f"⚠️ <b>No channel connected yet.</b>\n\n"
            f"Add @{bot_info.username} as an administrator to your private channel to link it."
        )
        await message.answer(text, parse_mode="HTML", reply_markup=get_channel_setup_keyboard(bot_info.username))


# ── Forwarded Channel Message Auto-Link ─────────────────────────────────

@router.message(F.forward_origin | F.forward_from_chat)
async def on_forwarded_channel_message(message: Message, bot: Bot):
    """Auto-link storage channel when user forwards any post from their channel to the bot in PM."""
    if message.chat.type != ChatType.PRIVATE:
        return

    user_id = message.from_user.id
    target_chat = None

    # Check modern Telegram Bot API forward_origin (aiogram 3.4+)
    if message.forward_origin:
        origin = message.forward_origin
        if getattr(origin, "type", None) == "channel" and hasattr(origin, "chat"):
            target_chat = origin.chat
        elif hasattr(origin, "chat") and getattr(origin.chat, "type", None) in (ChatType.CHANNEL, ChatType.SUPERGROUP, ChatType.GROUP):
            target_chat = origin.chat

    # Fallback to legacy forward_from_chat
    if not target_chat and message.forward_from_chat:
        target_chat = message.forward_from_chat

    if not target_chat:
        # Forwarded from a private user profile, ignore
        return

    bot_info = await bot.get_me()
    channel_id = target_chat.id
    channel_title = target_chat.title or "Private Storage Channel"

    try:
        member = await bot.get_chat_member(channel_id, bot_info.id)
        if member.status != ChatMemberStatus.ADMINISTRATOR:
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text="➕ Add Bot as Admin",
                    url=f"https://t.me/{bot_info.username}?startchannel=sociobot&admin=post_messages+edit_messages+delete_messages"
                )]
            ])
            await message.answer(
                f"⚠️ I detected your channel <b>{channel_title}</b> (<code>{channel_id}</code>), "
                f"but I am not an <b>Administrator</b> in it yet!\n\n"
                f"Please grant me admin rights with <i>Post Messages</i> permissions, then forward a message again.",
                parse_mode="HTML",
                reply_markup=kb
            )
            return

        # Bot is confirmed administrator in this channel
        channels = await db.get_user_channels(user_id)
        max_channels = await db.get_user_max_channels(user_id)
        already_linked = any(c["channel_id"] == channel_id for c in channels)

        if not already_linked and len(channels) >= max_channels:
            await message.answer(
                f"⚠️ <b>Channel Quota Reached ({len(channels)}/{max_channels})</b>\n\n"
                f"You have reached your limit of {max_channels} connected storage channels. "
                f"Manage your channels with /channels.",
                parse_mode="HTML"
            )
            return

        await db.get_or_create_user(
            chat_id=user_id,
            username=message.from_user.username,
            first_name=message.from_user.first_name,
            is_admin=(user_id == settings.ADMIN_CHAT_ID)
        )
        await db.add_user_channel(user_id, channel_id, channel_title)

        if len(channels) >= 1 and not already_linked:
            routes = await db.get_all_platform_routes(user_id)
            await message.answer(
                f"🎉 <b>New Vault Connected: {channel_title}</b>\n\n"
                f"You now have <b>{len(channels) + 1}</b> storage vaults.\n"
                f"Which platforms should be saved to this channel?",
                parse_mode="HTML",
                reply_markup=get_platform_route_keyboard(channel_id, user_id, routes)
            )
        else:
            await message.answer(
                f"🎉 <b>Storage Vault Connected!</b>\n\n"
                f"Connected: <b>{channel_title}</b> (<code>{channel_id}</code>)\n\n"
                f"✅ All music & videos you download will be automatically archived here with instant delete control.",
                parse_mode="HTML",
                reply_markup=get_onboarding_keyboard()
            )
    except Exception as e:
        logger.warning(f"Could not verify channel {channel_id} from forwarded message: {e}")
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="➕ Add Bot as Admin",
                url=f"https://t.me/{bot_info.username}?startchannel=sociobot&admin=post_messages+edit_messages+delete_messages"
            )]
        ])
        await message.answer(
            f"⚠️ Detected channel <b>{channel_title}</b> (<code>{channel_id}</code>), but I cannot access it yet.\n\n"
            f"Make sure the bot has been added as an <b>Administrator</b> with post permissions first!",
            parse_mode="HTML",
            reply_markup=kb
        )


# ── Manual Channel Setup Command ────────────────────────────────────────

@router.message(Command("setchannel"))
async def cmd_setchannel(message: Message, bot: Bot):
    """Manual fallback to link channel by ID, @username, or t.me link."""
    user_id = message.from_user.id
    args = message.text.strip().split(maxsplit=1)

    target_id_str = None
    if len(args) > 1:
        target_id_str = args[1].strip()
    elif message.forward_origin and getattr(message.forward_origin, "chat", None):
        target_id_str = str(message.forward_origin.chat.id)
    elif message.forward_from_chat:
        target_id_str = str(message.forward_from_chat.id)
    elif message.reply_to_message:
        reply = message.reply_to_message
        if reply.forward_origin and getattr(reply.forward_origin, "chat", None):
            target_id_str = str(reply.forward_origin.chat.id)
        elif reply.forward_from_chat:
            target_id_str = str(reply.forward_from_chat.id)

    if not target_id_str:
        bot_info = await bot.get_me()
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="➕ Add Bot to Your Channel",
                url=f"https://t.me/{bot_info.username}?startchannel=sociobot&admin=post_messages+edit_messages+delete_messages"
            )],
            [InlineKeyboardButton(
                text="👥 Add Bot to a Group",
                url=f"https://t.me/{bot_info.username}?startgroup=sociobot&admin=post_messages+delete_messages"
            )]
        ])
        await message.answer(
            "📌 <b>How to link your storage channel:</b>\n\n"
            "<b>Option 1 (Easiest):</b> Tap the button below to add the bot to your channel with admin rights.\n\n"
            "<b>Option 2:</b> Forward any post from your channel directly to this chat!\n\n"
            "<b>Option 3:</b> Type <code>/setchannel -100xxxxxxxxxx</code> (or <code>/setchannel @yourchannel</code>)",
            parse_mode="HTML",
            reply_markup=kb
        )
        return

    # Clean input: handle links, @usernames, and raw IDs
    clean_target = (
        target_id_str.replace("https://t.me/", "")
        .replace("http://t.me/", "")
        .replace("t.me/", "")
        .strip()
    )
    if clean_target.startswith("@"):
        lookup_target = clean_target
    elif clean_target.startswith("-100") or clean_target.startswith("-"):
        try:
            lookup_target = int(clean_target)
        except ValueError:
            lookup_target = clean_target
    elif clean_target.isdigit():
        lookup_target = int(f"-100{clean_target}")
    else:
        lookup_target = f"@{clean_target}"

    try:
        bot_info = await bot.get_me()
        chat = await bot.get_chat(lookup_target)
        member = await bot.get_chat_member(chat.id, bot_info.id)

        if member.status != ChatMemberStatus.ADMINISTRATOR:
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text="➕ Promote Bot to Admin",
                    url=f"https://t.me/{bot_info.username}?startchannel=sociobot&admin=post_messages+edit_messages+delete_messages"
                )]
            ])
            await message.answer(
                f"⚠️ Bot is in <b>{chat.title}</b>, but is not an <b>Administrator</b>. Please grant post permissions.",
                parse_mode="HTML",
                reply_markup=kb
            )
            return

        channel_title = chat.title or "Private Storage Channel"
        channel_id = chat.id

        channels = await db.get_user_channels(user_id)
        max_channels = await db.get_user_max_channels(user_id)
        already_linked = any(c["channel_id"] == channel_id for c in channels)

        if not already_linked and len(channels) >= max_channels:
            await message.answer(
                f"⚠️ <b>Channel Quota Reached ({len(channels)}/{max_channels})</b>\n\n"
                f"You have reached your limit of {max_channels} connected storage channels. "
                f"Manage your channels with /channels.",
                parse_mode="HTML"
            )
            return

        await db.get_or_create_user(
            chat_id=user_id,
            username=message.from_user.username,
            first_name=message.from_user.first_name,
            is_admin=(user_id == settings.ADMIN_CHAT_ID)
        )
        await db.add_user_channel(user_id, channel_id, channel_title)

        if len(channels) >= 1 and not already_linked:
            routes = await db.get_all_platform_routes(user_id)
            await message.answer(
                f"✅ Successfully linked to <b>{channel_title}</b> (<code>{channel_id}</code>)!\n\n"
                f"You now have <b>{len(channels) + 1}</b> storage vaults.\n"
                f"Which platforms should be saved to this channel?",
                parse_mode="HTML",
                reply_markup=get_platform_route_keyboard(channel_id, user_id, routes)
            )
        else:
            await message.answer(
                f"✅ Successfully linked to <b>{channel_title}</b> (<code>{channel_id}</code>)!\n\n"
                f"You are all set to search and download media.",
                parse_mode="HTML",
                reply_markup=get_onboarding_keyboard()
            )
    except Exception as e:
        logger.warning(f"Failed to manually set channel '{target_id_str}' for {user_id}: {e}")
        bot_info = await bot.get_me()
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="➕ Add Bot to Channel",
                url=f"https://t.me/{bot_info.username}?startchannel=sociobot&admin=post_messages+edit_messages+delete_messages"
            )]
        ])
        await message.answer(
            f"❌ Could not access channel <code>{target_id_str}</code>.\n\n"
            f"Make sure the bot has already been added as an <b>Administrator</b> with post permissions.",
            parse_mode="HTML",
            reply_markup=kb
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
