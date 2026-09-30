# Project Memory: Sociobot

## Overview
- **Project Name:** Sociobot
- **Architecture:** Pure-Python (Async) Telegram Bot powered by `aiogram 3.x`, FastAPI, and Neon Serverless PostgreSQL with SQLite fallback.
- **Core Philosophy:** Decentralized user-owned media warehousing. Rather than relying on a centralized storage channel, each user links their own private channel where media is posted. The network cross-replicates media between peer channels (`copy_message`), giving users complete control over their library while achieving zero duplicate downloads and sub-200ms delivery across the community.

## External Services & Endpoints
1. **Stream Extractor API:**
   - Base URL: `https://ytsp-api.pgwiz.cloud` (configurable via `YTSP_API_BASE_URL` env variable, never hardcoded).
   - Endpoints:
     - `/health`: Health status and active services check.
     - `/stream/:videoId?quality=...`: Metadata & playable stream URL.
     - `/get?ytl=...&quality=...`: Direct stream extraction for YouTube and Spotify URLs.
     - `/api/search/spotify?query=...`: Fast music catalog search with matched YouTube videoId.
     - `/api/spotify/playlist/:id`: Keyless Spotify playlist extraction.
     - `/api/youtube/playlist`: YouTube playlist extraction.
     - `/stream/play`: Audio proxy stream.
     - `/download`: Pre-packaged ID3-tagged MP3 audio packages.

2. **Neon Serverless PostgreSQL & Schema Isolation:**
   - URI: Configured via `DATABASE_URL`.
   - Custom Schema Isolation: Configured via `DB_SCHEMA` (defaults to `sociobot`). Allows `sociobot` to share the same Neon PostgreSQL instance as other bots (like `telegram-ultra-mini` using `public`) without table collisions or data leakage.
   - Driver: `asyncpg` with connection pooling.
   - Scale-to-Zero & Cold-Start Handling:
     - PgBouncer compatibility: `statement_cache_size=0` on `create_pool` to avoid prepared statement conflicts on Neon `-pooler` endpoints.
     - Pool Checkout Setup Hook: `asyncpg.create_pool(..., setup=_setup_connection)` executes `SET search_path TO "<schema>", public;` on every connection checkout, ensuring session state persists across PgBouncer transaction-mode connection recycling and `RESET ALL`.
     - Cold-start wakeup backoff: 5 retries with exponential backoff (1.5s, 3s, 6s, 12s, 24s).
     - Query-level retry: `_execute_pg_with_retry` automatically catches `ConnectionResetError`, `CannotConnectNowError`, or `ConnectionDoesNotExistError`, re-establishes the pool, and re-executes.
     - Keep-alive ping loop: Background task executes `SELECT 1;` every 240 seconds when `ENABLE_NEON_KEEPALIVE=true`.
     - Graceful Shutdown Latch: `self._is_shutting_down` latch prevents keepalive pings or closing queries from touching a closing pool; pool disconnect is bounded with a 3.0s timeout falling back to `terminate()`.
   - SQLite Fallback: Activated when `DATABASE_URL` is unconfigured or starts with `sqlite:///`, using `aiosqlite` with WAL mode.

3. **Render Deployment & WSGI Compatibility:**
   - Entrypoints: `wsgi.py` and `your_application/wsgi.py` wrap the FastAPI app in `a2wsgi.ASGIMiddleware(app)`.
   - Runs cleanly under Render's default command: `gunicorn your_application.wsgi` or standard `uvicorn bot.main:app`.
   - Aiogram polling runs as a background task within FastAPI lifespan with `/` and `/health` HTTP endpoints responding for Render health checks.
   - Lifecycle: Lifespan manager triggers `dp.stop_polling()` followed by bot session, API client, and DB pool termination for 100% clean shutdown on container restarts without orphaned connections or unclosed session warnings.

## Decentralized Storage & Multi-Node Schema
1. **Multi-Channel Onboarding & Vault Flow:**
   - Auto-detection: Intercepts `my_chat_member` (`ChatMemberUpdated`) with `allowed_updates` when the bot is promoted to administrator in a user's channel, supergroup, or group.
   - Enforces channel quota (`max_channels`, default 5). First channel automatically becomes Primary (`is_primary = TRUE`).
   - If user connects a 2nd+ channel, prompts with the platform routing matrix to assign platforms immediately.
   - Forwarding fallback: Forwarding ANY post from a channel to the bot in PM automatically verifies and links the vault.
   - Interactive dashboard: `/channels` allows designating primary vault, routing platforms, and safe unlinking.
   - Safe unlinking: Removing a channel automatically reassigns all its platform routes to the primary vault.

2. **Multi-Platform Extraction & Platform Routing:**
   - Supports 9 platforms: YouTube, Spotify, TikTok, Instagram, Twitter/X, Reddit, SoundCloud, Bandcamp, Vimeo.
   - `bot.utils.platform`: Strict domain-delimited regex pattern detector avoiding false positives (e.g., `reddit.com` vs `t.co`).
   - Quality presets:
     - Social videos (`tiktok`, `instagram`, `twitter`, `reddit`): Download in lowest quality (`saver` MP4) to preserve bandwidth, with an attached `[ 🎵 Extract Audio ]` button.
     - Music platforms (`spotify`, `soundcloud`, `bandcamp`): Default to ID3-tagged 320k MP3 (`audio_high`).
     - Videos (`youtube`, `vimeo`): Present format selector or default to user preference.
   - Dynamic Link Picker: If an unrouted link arrives for a user with >1 vaults, Sociobot displays an inline channel picker, saves the destination route, and downloads the media.

3. **Multi-Node Database Tables:**
   - `users`: User metadata, `channel_id` (legacy pointer), `channel_title`, `max_channels` (default 5), `is_storage_active`, `terms_accepted`, and admin status.
   - `user_channels`: Keyed by `(user_chat_id, channel_id)`, stores `channel_title`, `is_primary`, `is_active`, `created_at`.
   - `user_platform_routes`: Keyed by `(user_chat_id, platform)`, stores `channel_id`, `updated_at`.
   - `user_media_storage`: Keyed by `(user_chat_id, track_id, quality)`, stores `channel_id`, `channel_msg_id`, `channel_post_url`, `telegram_file_id`, `platform`, `destination_channel_id`, and `is_available`.
   - `tracks`: Global metadata registry (`title`, `artist`, `duration_secs`, `thumbnail_url`, `source`).
   - `api_cache`: Persistent JSON cache for metadata (7-day TTL) and search results (24-hour TTL).
   - `download_history`: User download logs.
   - `rate_limits`: Per-user rate limiting.

4. **Replication & Delivery Engine:**
   - When User B requests a track already cached in User A's channel:
     - Bot executes `bot.copy_message(user_b_channel, user_a_channel, msg_id)`.
     - User B receives their own permanent copy in their channel.
     - Bot saves record in `user_media_storage` under User B.
     - If User A ever removes the bot or deletes their copy, User B's copy remains alive, and the network routes through surviving nodes.
   - Attached action buttons on delivered media:
     - `[ 📂 Open in Channel ]` — Direct link (`https://t.me/c/<clean_id>/<msg_id>`).
     - `[ 🎵 Extract Audio ]` — 1-tap conversion of social video to MP3 (only shown for video formats).
     - `[ 🗑️ Delete ]` — Deletes post from user's channel and soft-deletes DB record.
     - `[ ⚡ Force Re-download ]` — Bypasses cache and extracts fresh.

5. **Audio & Video Packaging:**
   - Audio (`audio_high` 320k, `audio` 192k, `saver` 64k): Checks MP3 header; if MP4/AAC stream, transcodes to pure MP3 via FFmpeg or delivers clean `.m4a`.
   - Video (`720p` HD, `360p` SD, `saver` lowest): Streams genuine MP4 video with `supports_streaming=True`.

6. **Super Admin User Management Suite:**
   - Authorization: `SUPER_ADMIN_IDS` in `.env` (comma-separated or single numeric ID, falling back to `ADMIN_CHAT_ID`).
   - Paginated user directory via `/users` with search (`/users <filter>`).
   - Deep inspection card via `/user <id_or_username>` showing all connected channels, platform routes, and quota.
   - In-bot management actions:
     - `[ 📁 View Stored Vault ]`: View all tracks archived in that user's channel.
     - `[ 🗑️ Unlink: <channel> ]`: Forcefully disconnect a specific channel and mark its media unavailable.
     - `[ 💎 Upgrade Quota ]`: Set user maximum channels (5, 10, 20, or 100/Unlimited).
     - `[ 👑 Make/Demote Admin ]`: Promote/demote regular admins.
     - `[ 🚫 Ban/Unban User ]`: Ban abusive users from using search, download, or bot services.
     - `/dm <user_id> <msg>`: Send direct administrative communications to a user.

## Repositories
- Public: `https://github.com/pgwiz/sociobot.git`
- Private: `https://github.com/WiPTech/sociobot.git`
- Push URLs configured on `origin` to push to both targets simultaneously.
