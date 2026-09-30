"""Channel Management and Platform Routing Handler for Sociobot (v2.0).

Handles:
- /channels interactive dashboard
- Primary channel designation
- Channel unlinking with automatic platform route fallback
- Platform routing matrix
- Dynamic channel picker on unrouted media links
"""

import logging
from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from bot.database import db
from bot.keyboards.inline import (
    get_channels_dashboard_keyboard,
    get_platform_route_keyboard,
    get_channel_setup_keyboard,
    get_mychannels_keyboard,
    get_channel_detail_keyboard,
    get_channel_delmedia_confirm_keyboard,
    format_channel_link
)
from bot.utils.platform import (
    SUPPORTED_PLATFORMS,
    get_platform_display_name,
    get_platform_icon
)

logger = logging.getLogger(__name__)
router = Router(name="channels")


async def render_channels_dashboard(user_id: int, bot: Bot):
    """Build text and keyboard for the /channels dashboard."""
    channels = await db.get_user_channels(user_id)
    routes = await db.get_all_platform_routes(user_id)
    max_channels = await db.get_user_max_channels(user_id)
    bot_info = await bot.get_me()

    if not channels:
        text = (
            "📁 <b>Storage Vaults</b>\n\n"
            "⚠️ You haven't connected any private storage channels yet!\n\n"
            "To get started, create a private channel in Telegram and add "
            f"@{bot_info.username} as an <b>Administrator</b> with post permissions."
        )
        kb = get_channel_setup_keyboard(bot_info.username)
        return text, kb

    # Format dashboard
    text_lines = [
        f"📁 <b>Your Storage Vaults ({len(channels)}/{max_channels})</b>\n",
        "Manage where your media from different platforms gets saved:\n"
    ]

    for ch in channels:
        cid = ch["channel_id"]
        title = ch.get("channel_title") or "Storage Channel"
        is_primary = ch.get("is_primary", False)
        star = "🌟 <b>[Primary Vault]</b> " if is_primary else "📁 "

        text_lines.append(f"{star}<b>{title}</b> (<code>{cid}</code>)")

        # List routed platforms for this channel
        assigned = [
            f"{get_platform_icon(p)} {get_platform_display_name(p)}"
            for p, target_cid in routes.items()
            if target_cid == cid
        ]
        if assigned:
            text_lines.append(f"   ↳ <i>Routes:</i> {', '.join(assigned)}")
        elif is_primary:
            text_lines.append("   ↳ <i>Default fallback for unrouted platforms</i>")
        else:
            text_lines.append("   ↳ <i>No platforms routed here yet</i>")
        text_lines.append("")

    text_lines.append("💡 <i>Tap <b>Route</b> on any channel to customize platform destinations.</i>")
    dashboard_text = "\n".join(text_lines)

    kb = get_channels_dashboard_keyboard(
        channels=channels,
        routes_map=routes,
        max_channels=max_channels,
        bot_username=bot_info.username
    )
    return dashboard_text, kb


@router.message(Command("channels"))
async def cmd_channels(message: Message, bot: Bot):
    """Display user's multi-channel storage dashboard."""
    user_id = message.from_user.id
    if await db.is_user_banned(user_id):
        await message.answer("🚫 Your access to Sociobot has been restricted by an administrator.")
        return

    text, kb = await render_channels_dashboard(user_id, bot)
    await message.answer(text, parse_mode="HTML", reply_markup=kb)


@router.callback_query(F.data == "cb:ch_refresh")
@router.callback_query(F.data == "cb:ch_back")
async def cb_channels_refresh(callback: CallbackQuery, bot: Bot):
    """Refresh or return to /channels dashboard."""
    user_id = callback.from_user.id
    text, kb = await render_channels_dashboard(user_id, bot)
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        pass
    await callback.answer()


@router.callback_query(F.data.startswith("cb:ch_set_primary:"))
async def cb_channel_set_primary(callback: CallbackQuery, bot: Bot):
    """Set a channel as the user's primary/default storage channel."""
    user_id = callback.from_user.id
    channel_id = int(callback.data.split(":")[2])

    success = await db.set_primary_channel(user_id, channel_id)
    if success:
        await callback.answer("🌟 Channel set as your primary vault!", show_alert=False)
    else:
        await callback.answer("Could not set primary channel.", show_alert=True)

    text, kb = await render_channels_dashboard(user_id, bot)
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        pass


@router.callback_query(F.data.startswith("cb:ch_unlink:"))
async def cb_channel_unlink(callback: CallbackQuery, bot: Bot):
    """Unlink a channel and reroute its platforms to primary."""
    user_id = callback.from_user.id
    channel_id = int(callback.data.split(":")[2])

    res = await db.remove_user_channel(user_id, channel_id)
    re_routed = res.get("re_routed", 0)
    new_primary = res.get("new_primary_id")

    if new_primary:
        msg = f"🗑️ Channel unlinked. {re_routed} platform route(s) reassigned to primary vault."
    else:
        msg = "🗑️ Channel unlinked. No remaining channels."

    await callback.answer(msg, show_alert=True)

    text, kb = await render_channels_dashboard(user_id, bot)
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        pass


@router.callback_query(F.data.startswith("cb:ch_route:"))
async def cb_channel_route_menu(callback: CallbackQuery):
    """Open platform routing matrix for a channel."""
    user_id = callback.from_user.id
    channel_id = int(callback.data.split(":")[2])

    channels = await db.get_user_channels(user_id)
    target_ch = next((c for c in channels if c["channel_id"] == channel_id), None)
    if not target_ch:
        await callback.answer("Channel not found.", show_alert=True)
        return

    routes = await db.get_all_platform_routes(user_id)
    title = target_ch.get("channel_title") or f"Channel {channel_id}"

    text = (
        f"⚙️ <b>Route Platforms to:</b>\n"
        f"<b>{title}</b> (<code>{channel_id}</code>)\n\n"
        f"Tap any platform to toggle its destination to this channel.\n"
        f"• ✅ = Routed to this channel\n"
        f"• ⬜ = Routed elsewhere or defaulting to primary"
    )

    kb = get_platform_route_keyboard(channel_id, user_id, routes)
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        pass
    await callback.answer()


@router.callback_query(F.data.startswith("cb:ch_toggle:"))
async def cb_channel_toggle_platform(callback: CallbackQuery):
    """Toggle a platform's assignment to/from a specific channel."""
    user_id = callback.from_user.id
    parts = callback.data.split(":")
    channel_id = int(parts[2])
    platform = parts[3]

    current_routes = await db.get_all_platform_routes(user_id)
    currently_here = (current_routes.get(platform) == channel_id)

    if currently_here:
        # Toggle off -> remove route so it falls back to primary
        await db.remove_platform_route(user_id, platform)
        await callback.answer(f"Removed {get_platform_display_name(platform)} route (falls back to primary)")
    else:
        # Toggle on -> set route to this channel
        await db.set_platform_route(user_id, platform, channel_id)
        await callback.answer(f"Routed {get_platform_display_name(platform)} to this channel!")

    # Update keyboard with fresh routes
    updated_routes = await db.get_all_platform_routes(user_id)
    kb = get_platform_route_keyboard(channel_id, user_id, updated_routes)
    try:
        await callback.message.edit_reply_markup(reply_markup=kb)
    except Exception:
        pass


from bot.cache import cache
from bot.utils.track_ref import register_track_ref, resolve_track_ref


@router.callback_query(F.data.startswith("cb:route_pick:"))
async def cb_channel_route_pick(callback: CallbackQuery, bot: Bot):
    """User selected a destination channel from the interactive link picker."""
    user_id = callback.from_user.id
    parts = callback.data.split(":")
    platform = parts[2]
    channel_id = int(parts[3])
    token = parts[4] if len(parts) > 4 else None

    # Save platform preference
    await db.set_platform_route(user_id, platform, channel_id)
    await callback.answer(f"Saved! {get_platform_display_name(platform)} routed to selected channel.")

    pending = None
    if token:
        pending = await cache.get(f"pending_route_dl:{token}")
        if pending:
            await cache.delete(f"pending_route_dl:{token}")

    if not pending:
        # Fallback for backward compatibility if key was stored per user
        pending = await cache.get(f"pending_route_dl:{user_id}")
        if pending and pending.get("platform") == platform:
            await cache.delete(f"pending_route_dl:{user_id}")

    track_id = pending.get("url") if pending else None
    quality = pending.get("quality", "360p") if pending else "360p"
    if quality == "saver":
        quality = "360p"

    # If token was passed directly without cache hit, resolve via resolve_track_ref
    if not track_id and token:
        resolved = await resolve_track_ref(token)
        if resolved != token:
            track_id = resolved

    if track_id:
        if platform == "youtube":
            from bot.keyboards.inline import get_format_picker_keyboard
            ref = await register_track_ref(track_id)
            kb = get_format_picker_keyboard(ref)
            await callback.message.edit_text(
                f"✅ <b>Routed YouTube to selected vault!</b>\n\n"
                f"🔗 <code>{track_id}</code>\n\n"
                f"Choose download format:",
                parse_mode="HTML",
                reply_markup=kb
            )
            return

        status_msg = await callback.message.edit_text(
            f"⏳ <b>Starting {get_platform_display_name(platform)} download...</b>",
            parse_mode="HTML"
        )

        from bot.handlers.download import process_media_request
        await process_media_request(
            bot=bot,
            user_id=user_id,
            reply_to_chat_id=callback.message.chat.id,
            track_id=track_id,
            quality=quality,
            force=False,
            status_message=status_msg,
            platform=platform
        )
    else:
        await callback.message.edit_text(
            f"✅ <b>{get_platform_icon(platform)} {get_platform_display_name(platform)} routed to this vault!</b>\n\n"
            "Paste your media link anytime to download.",
            parse_mode="HTML"
        )


# ── /mychannels Interactive Channel Management & Media Purge ──────────────

async def render_mychannels_list(user_id: int, bot: Bot):
    """Build text and keyboard for /mychannels interactive channel list."""
    channels = await db.get_user_channels(user_id)
    max_channels = await db.get_user_max_channels(user_id)
    bot_info = await bot.get_me()

    if not channels:
        text = (
            "📁 <b>Storage Vaults</b>\n\n"
            "⚠️ You haven't connected any private storage channels yet!\n\n"
            "To get started, create a private channel in Telegram and add "
            f"@{bot_info.username} as an <b>Administrator</b> with post permissions."
        )
        kb = get_channel_setup_keyboard(bot_info.username)
        return text, kb

    text = (
        f"📁 <b>Your Connected Channels ({len(channels)}/{max_channels})</b>\n\n"
        "Tap a channel below to view details, visit it, unlink it, or delete its media:"
    )
    kb = get_mychannels_keyboard(
        channels=channels,
        max_channels=max_channels,
        bot_username=bot_info.username
    )
    return text, kb


@router.message(Command("mychannels", "mychanels"))
async def cmd_mychannels(message: Message, bot: Bot):
    """Display user's channels as interactive buttons."""
    user_id = message.from_user.id
    if await db.is_user_banned(user_id):
        await message.answer("🚫 Your access to Sociobot has been restricted by an administrator.")
        return

    text, kb = await render_mychannels_list(user_id, bot)
    await message.answer(text, parse_mode="HTML", reply_markup=kb)


@router.callback_query(F.data == "cb:mych_list")
@router.callback_query(F.data == "cb:mych_refresh")
async def cb_mychannels_list(callback: CallbackQuery, bot: Bot):
    """Refresh or return to /mychannels list."""
    user_id = callback.from_user.id
    text, kb = await render_mychannels_list(user_id, bot)
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        pass
    await callback.answer()


@router.callback_query(F.data.startswith("cb:mych_view:"))
async def cb_mychannel_view(callback: CallbackQuery, bot: Bot):
    """Display detailed summary and actions for a specific channel."""
    user_id = callback.from_user.id
    channel_id = int(callback.data.split(":")[2])

    channels = await db.get_user_channels(user_id)
    target_ch = next((c for c in channels if c["channel_id"] == channel_id), None)
    if not target_ch:
        await callback.answer("Channel not found or already unlinked.", show_alert=True)
        text, kb = await render_mychannels_list(user_id, bot)
        try:
            await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
        except Exception:
            pass
        return

    title = target_ch.get("channel_title") or f"Channel {channel_id}"
    is_primary = target_ch.get("is_primary", False)
    routes = await db.get_all_platform_routes(user_id)

    assigned = [
        f"{get_platform_icon(p)} {get_platform_display_name(p)}"
        for p, target_cid in routes.items()
        if target_cid == channel_id
    ]
    if assigned:
        routed_text = ", ".join(assigned)
    elif is_primary:
        routed_text = "Default fallback for all unrouted platforms"
    else:
        routed_text = "None (unrouted)"

    media_count = await db.get_channel_media_count(user_id, channel_id)
    role_text = "🌟 Primary Storage Vault" if is_primary else "📁 Secondary Vault"

    channel_url = format_channel_link(channel_id)
    try:
        chat = await bot.get_chat(channel_id)
        if chat and chat.username:
            channel_url = f"https://t.me/{chat.username}"
    except Exception:
        pass

    text = (
        f"📁 <b>Channel Summary</b>\n\n"
        f"• <b>Title:</b> {title}\n"
        f"• <b>Channel ID:</b> <code>{channel_id}</code>\n"
        f"• <b>Role:</b> {role_text}\n"
        f"• <b>Routed Platforms:</b> {routed_text}\n"
        f"• <b>Stored Media:</b> <b>{media_count}</b> active item(s)\n\n"
        f"Choose an action below:"
    )

    kb = get_channel_detail_keyboard(channel_id=channel_id, channel_url=channel_url)
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        pass
    await callback.answer()


@router.callback_query(F.data.startswith("cb:mych_unlink:"))
async def cb_mychannel_unlink(callback: CallbackQuery, bot: Bot):
    """Unlink channel from /mychannels view, reroute platforms to primary, and return to list."""
    user_id = callback.from_user.id
    channel_id = int(callback.data.split(":")[2])

    res = await db.remove_user_channel(user_id, channel_id)
    re_routed = res.get("re_routed", 0)
    new_primary = res.get("new_primary_id")

    if new_primary:
        msg = f"🗑️ Channel unlinked. {re_routed} platform route(s) reassigned to primary vault."
    else:
        msg = "🗑️ Channel unlinked. No remaining channels."

    await callback.answer(msg, show_alert=True)

    text, kb = await render_mychannels_list(user_id, bot)
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        pass


@router.callback_query(F.data.startswith("cb:mych_delmedia_1:"))
async def cb_mychannel_delmedia_step1(callback: CallbackQuery):
    """Step 1 of 3 confirmations for purging all media from a channel."""
    user_id = callback.from_user.id
    channel_id = int(callback.data.split(":")[2])

    channels = await db.get_user_channels(user_id)
    target_ch = next((c for c in channels if c["channel_id"] == channel_id), None)
    title = target_ch.get("channel_title") if target_ch else f"Channel {channel_id}"

    text = (
        f"⚠️ <b>Delete Channel Media (Confirmation 1/3)</b>\n\n"
        f"Warning: This will delete ALL media stored in <b>{title}</b> from both your Telegram channel and your database library.\n\n"
        f"Proceed to confirmation 2/3?"
    )
    kb = get_channel_delmedia_confirm_keyboard(channel_id=channel_id, step=1)
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        pass
    await callback.answer()


@router.callback_query(F.data.startswith("cb:mych_delmedia_2:"))
async def cb_mychannel_delmedia_step2(callback: CallbackQuery):
    """Step 2 of 3 confirmations for purging all media from a channel."""
    user_id = callback.from_user.id
    channel_id = int(callback.data.split(":")[2])

    channels = await db.get_user_channels(user_id)
    target_ch = next((c for c in channels if c["channel_id"] == channel_id), None)
    title = target_ch.get("channel_title") if target_ch else f"Channel {channel_id}"

    text = (
        f"🚨 <b>Are you really sure? (Confirmation 2/3)</b>\n\n"
        f"This action is permanent and cannot be undone. All files stored in <b>{title}</b> will be deleted from Telegram.\n\n"
        f"Proceed to final confirmation (3/3)?"
    )
    kb = get_channel_delmedia_confirm_keyboard(channel_id=channel_id, step=2)
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        pass
    await callback.answer()


@router.callback_query(F.data.startswith("cb:mych_delmedia_3:"))
async def cb_mychannel_delmedia_step3(callback: CallbackQuery):
    """Step 3 of 3 confirmations for purging all media from a channel."""
    user_id = callback.from_user.id
    channel_id = int(callback.data.split(":")[2])

    channels = await db.get_user_channels(user_id)
    target_ch = next((c for c in channels if c["channel_id"] == channel_id), None)
    title = target_ch.get("channel_title") if target_ch else f"Channel {channel_id}"

    text = (
        f"🔥 <b>FINAL CONFIRMATION: Tap below to permanently purge all media in this channel. (Confirmation 3/3)</b>\n\n"
        f"Target Channel: <b>{title}</b> (<code>{channel_id}</code>)\n\n"
        f"⚠️ <i>All channel messages and library records will be permanently erased now.</i>"
    )
    kb = get_channel_delmedia_confirm_keyboard(channel_id=channel_id, step=3)
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        pass
    await callback.answer()


@router.callback_query(F.data.startswith("cb:mych_delmedia_confirm:") | F.data.startswith("cb:mych_delmedia_purge:"))
async def cb_mychannel_delmedia_purge(callback: CallbackQuery, bot: Bot):
    """Execute physical Telegram message deletion and database purge on 3rd confirmation."""
    user_id = callback.from_user.id
    channel_id = int(callback.data.split(":")[2])

    channels = await db.get_user_channels(user_id)
    target_ch = next((c for c in channels if c["channel_id"] == channel_id), None)
    title = target_ch.get("channel_title") if target_ch else f"Channel {channel_id}"

    try:
        await callback.message.edit_text(
            f"⏳ <b>Purging all media in {title}...</b>\nPlease wait.",
            parse_mode="HTML"
        )
    except Exception:
        pass

    # 1. Purge records from database and get channel message IDs
    purged_items = await db.purge_channel_media(user_id, channel_id)

    # 2. Physically delete messages from Telegram channel
    deleted_count = 0
    for cid, mid in purged_items:
        try:
            await bot.delete_message(chat_id=cid, message_id=mid)
            deleted_count += 1
        except Exception as e:
            logger.warning(f"Could not delete message {mid} in channel {cid}: {e}")

    # 3. Update message with summary
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="⬅️ Back to Channel", callback_data=f"cb:mych_view:{channel_id}"),
            InlineKeyboardButton(text="📁 All Channels", callback_data="cb:mych_list")
        ]
    ])

    summary_text = (
        f"✅ <b>Successfully purged {len(purged_items)} tracks/videos from {title}.</b>\n\n"
        f"• Telegram posts deleted: <b>{deleted_count}/{len(purged_items)}</b>\n"
        f"• Library records cleared: <b>{len(purged_items)}</b>"
    )
    try:
        await callback.message.edit_text(summary_text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        pass
    await callback.answer("Purge complete!", show_alert=False)
