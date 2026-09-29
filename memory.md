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

2. **Neon Serverless PostgreSQL:**
   - URI: Configured via `DATABASE_URL`.
   - Driver: `asyncpg` with connection pooling.
   - Scale-to-Zero & Cold-Start Handling:
     - PgBouncer compatibility: `statement_cache_size=0` on `create_pool` to avoid prepared statement conflicts on Neon `-pooler` endpoints.
     - Cold-start wakeup backoff: 5 retries with exponential backoff (1.5s, 3s, 6s, 12s, 24s).
     - Query-level retry: `_execute_pg_with_retry` automatically catches `ConnectionResetError` or `CannotConnectNowError`, re-establishes the pool, and re-executes.
     - Keep-alive ping loop: Background task executes `SELECT 1;` every 240 seconds when `ENABLE_NEON_KEEPALIVE=true`.
   - SQLite Fallback: Activated when `DATABASE_URL` is unconfigured or starts with `sqlite:///`, using `aiosqlite` with WAL mode.

3. **Render Deployment & WSGI Compatibility:**
   - Entrypoints: `wsgi.py` and `your_application/wsgi.py` wrap the FastAPI app in `a2wsgi.ASGIMiddleware(app)`.
   - Runs cleanly under Render's default command: `gunicorn your_application.wsgi` or standard `uvicorn bot.main:app`.
   - Aiogram polling runs as a background task within FastAPI lifespan with `/` and `/health` HTTP endpoints responding for Render health checks.

## Decentralized Storage & Multi-Node Schema
1. **Channel Onboarding Flow:**
   - Auto-detection: Intercepts `ChatMemberUpdated` when the bot is promoted to administrator in a user's private channel.
   - Saves channel ID and title to `users` table.
   - Sends polite onboarding notice:
     > *"✅ Channel linked! Your media is safely archived here for your full control. To keep downloads lightning-fast, audio may also be shared anonymously across the community network."*
     with a `[ Let's Go 🚀 ]` confirmation button.
   - Manual fallback: `/setchannel <channel_id>` and `/mychannel`.

2. **Multi-Node Database Tables:**
   - `users`: User metadata, connected `channel_id`, `channel_title`, `is_storage_active`, `terms_accepted`, and admin status.
   - `user_media_storage`: Keyed by `(user_chat_id, track_id, quality)`, stores `channel_id`, `channel_msg_id`, `channel_post_url`, `telegram_file_id`, and `is_available`.
   - `tracks`: Global metadata registry (`title`, `artist`, `duration_secs`, `thumbnail_url`, `source`).
   - `api_cache`: Persistent JSON cache for metadata (7-day TTL) and search results (24-hour TTL).
   - `download_history`: User download logs.
   - `rate_limits`: Per-user rate limiting.

3. **Replication & Delivery Engine:**
   - When User B requests a track already cached in User A's channel:
     - Bot executes `bot.copy_message(user_b_channel, user_a_channel, msg_id)`.
     - User B receives their own permanent copy in their channel.
     - Bot saves record in `user_media_storage` under User B.
     - If User A ever removes the bot or deletes their copy, User B's copy remains alive, and the network routes through surviving nodes.
   - Attached action buttons on delivered media:
     - `[ 📂 Open in Channel ]` — Direct link (`https://t.me/c/<clean_id>/<msg_id>`).
     - `[ 🗑️ Delete ]` — Deletes post from user's channel and soft-deletes DB record.
     - `[ ⚡ Force Re-download ]` — Bypasses cache and extracts fresh.

4. **Audio & Video Packaging:**
   - Audio (`audio_high` 320k, `audio` 192k, `saver` 64k): Checks MP3 header; if MP4/AAC stream, transcodes to pure MP3 via FFmpeg or delivers clean `.m4a`.
   - Video (`720p` HD, `360p` SD): Streams genuine MP4 video with `supports_streaming=True`.

5. **Super Admin User Management Suite:**
   - Authorization: `SUPER_ADMIN_IDS` in `.env` (comma-separated or single numeric ID, falling back to `ADMIN_CHAT_ID`).
   - Paginated user directory via `/users` with search (`/users <filter>`).
   - Deep inspection card via `/user <id_or_username>`.
   - In-bot management actions:
     - `[ 📁 View Stored Vault ]`: View all tracks archived in that user's channel.
     - `[ 🔗 Unlink Channel ]`: Forcefully disconnect a channel and mark stored media unavailable.
     - `[ 👑 Make/Demote Admin ]`: Promote/demote regular admins.
     - `[ 🚫 Ban/Unban User ]`: Ban abusive users from using search, download, or bot services.
     - `/dm <user_id> <msg>`: Send direct administrative communications to a user.

## Repositories
- Public: `https://github.com/pgwiz/sociobot.git`
- Private: `https://github.com/WiPTech/sociobot.git`
- Push URLs configured on `origin` to push to both targets simultaneously.
