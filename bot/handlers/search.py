"""Search and Catalog Browsing Handler for Sociobot.

Handles:
- Plain text song searching (text-as-search) and /search command.
- Two-step interactive UI: song selection -> format picker (Audio vs Video).
- Spotify & YouTube URL routing.
"""

import logging
from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command
from bot.api_client import api_client
from bot.database import db
from bot.keyboards.inline import (
    get_search_results_keyboard,
    get_format_picker_keyboard
)

logger = logging.getLogger(__name__)
router = Router(name="search")


def is_direct_media_url(text: str) -> bool:
    """Return True if the text is a direct Spotify or YouTube URL."""
    text_lower = text.strip().lower()
    return any(domain in text_lower for domain in (
        "youtube.com", "youtu.be", "spotify.com", "open.spotify.com"
    ))


@router.message(Command("search"))
async def cmd_search(message: Message):
    """Execute search via /search <query>."""
    args = message.text.strip().split(maxsplit=1)
    if len(args) < 2:
        await message.answer("🔍 Please specify a song name: <code>/search Blinding Lights</code>", parse_mode="HTML")
        return
    await execute_search(message, args[1].strip())


@router.message(F.text & ~F.text.startswith("/"))
async def on_plain_text_search(message: Message):
    """Treat any non-command plain text message as a song search (or URL if link)."""
    text = message.text.strip()
    if is_direct_media_url(text):
        # Pass to download handler logic or route to format picker
        from bot.handlers.download import handle_direct_url_request
        await handle_direct_url_request(message, text)
        return

    await execute_search(message, text)


async def execute_search(message: Message, query: str):
    """Perform music catalog search and present interactive track list."""
    user_id = message.from_user.id

    if await db.is_user_banned(user_id):
        await message.answer("🚫 Your access to Sociobot has been restricted by an administrator.")
        return

    if not await db.check_rate_limit(user_id, "search", max_requests=20, window_secs=60):
        await message.answer("⏳ Please slow down a moment before searching again.")
        return

    status_msg = await message.answer(f"🔍 Searching for <i>{query}</i>...", parse_mode="HTML")

    try:
        results = await api_client.search_tracks(query, limit=8)
        if not results:
            await status_msg.edit_text(f"❌ No results found for <i>{query}</i>. Try a different title or artist.", parse_mode="HTML")
            return

        kb = get_search_results_keyboard(results, query)
        await status_msg.edit_text(
            f"🎵 <b>Search Results for:</b> <i>{query}</i>\n\n"
            f"Tap a song to choose Audio or Video:",
            parse_mode="HTML",
            reply_markup=kb
        )
    except Exception as e:
        logger.error(f"Search failed for '{query}': {e}")
        await status_msg.edit_text("❌ An error occurred while searching. Please try again.")


@router.callback_query(F.data.startswith("cb:trk:"))
async def cb_track_select(callback: CallbackQuery):
    """Step 2 of Search: Present Audio vs Video format picker for chosen track."""
    track_id = callback.data.split("cb:trk:")[1].strip()

    # Retrieve cached track metadata if available
    meta = await api_client.get_stream_info(track_id, quality="audio_high")
    title = meta.get("title") if meta else "Selected Track"
    artist = meta.get("uploader") or meta.get("artist", "") if meta else ""
    duration = meta.get("duration", "") if meta else ""

    info_str = f"🎵 <b>{title}</b>"
    if artist:
        info_str += f"\n👤 {artist}"
    if duration:
        info_str += f"\n⏱️ {duration}"

    kb = get_format_picker_keyboard(track_id)
    await callback.message.edit_text(
        f"{info_str}\n\n<b>Choose download format:</b>",
        parse_mode="HTML",
        reply_markup=kb
    )
    await callback.answer()


@router.callback_query(F.data.startswith("cb:back_search:"))
async def cb_back_search(callback: CallbackQuery):
    """Return to search results from the format selector."""
    query = callback.data.split("cb:back_search:")[1].strip()
    results = await api_client.search_tracks(query, limit=8)
    if results:
        kb = get_search_results_keyboard(results, query)
        await callback.message.edit_text(
            f"🎵 <b>Search Results for:</b> <i>{query}</i>\n\n"
            f"Tap a song to choose Audio or Video:",
            parse_mode="HTML",
            reply_markup=kb
        )
    await callback.answer()


@router.callback_query(F.data == "cb:close")
async def cb_close_menu(callback: CallbackQuery):
    """Dismiss menu."""
    try:
        await callback.message.delete()
    except Exception:
        pass
    await callback.answer()
