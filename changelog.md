# Changelog: Sociobot

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.2] - 2026-09-29

### Fixed & Enhanced
- **Graceful Shutdown & Pool Lifecycle Cleanup:**
  - Resolved transient DB error warnings on application exit by introducing `_is_shutting_down` latch in `Database` connection pool.
  - Eliminated shutdown race where keepalive pings or closing queries attempted to acquire connections on a terminating pool.
  - Added bounded 3.0s graceful pool shutdown (`asyncio.wait_for`) with immediate `terminate()` fallback, ensuring instantaneous process termination on signals.
  - Integrated `dp.stop_polling()` prior to task cancellation and bot session closure, eliminating unclosed client session warnings and `TelegramNetworkError` disconnect alerts on restart or container redeploy.
  - Streamlined startup by moving schema verification directly into connection checkout, reducing cold-start round trips.

## [1.2.1] - 2026-09-29

### Fixed & Enhanced
- **Channel Onboarding & Auto-Link Resilience:**
  - Configured `allowed_updates=["message", "callback_query", "my_chat_member", "chat_member"]` in polling loop and set `drop_pending_updates=False` so channel promotion events are not discarded on bot startup.
  - Bot now sends a confirmation message directly into the connected channel with a 1-tap `[ 🎧 Open Sociobot in PM ]` button, providing instant visual feedback even if the user hasn't messaged the bot in PM yet.
  - Added forwarded channel post auto-linking: forwarding any post from a private channel or group into Sociobot's private chat automatically verifies and links the vault.
  - Enhanced `/setchannel` to support raw channel IDs, `@usernames`, and `https://t.me/...` links.
  - Added pre-authorized admin permission query parameters (`admin=post_messages+edit_messages+delete_messages`) to the "Add Bot to Channel" button for 1-tap permission assignment.
  - Extended support to groups and supergroups.

## [1.2.0] - 2026-09-29

### Added
- **PostgreSQL Custom Schema Isolation:**
  - Configurable `DB_SCHEMA` environment setting (defaults to `sociobot`).
  - Pre-creates custom schema (`CREATE SCHEMA IF NOT EXISTS "<schema>"`) during database initialization.
  - Configured `asyncpg.create_pool` with `setup` hook executing `SET search_path TO "<schema>", public;` on every connection checkout, ensuring session state survives PgBouncer transaction-mode connection recycling.
  - Configured `server_settings={"search_path": f"{schema},public"}` in startup packet.
  - Complete collision-free coexistence: multiple bots (such as `sociobot` and `telegram-ultra-mini`) can safely share the same Neon PostgreSQL database instance without data pollution.
  - Added active schema indicator in `/admin` Control Panel and `/stats` API response.
  - Added live PostgreSQL schema isolation smoke test in `test_verification.py`.

## [1.1.0] - 2026-09-29

### Added
- **Super Admin User Management Suite:**
  - Configurable `SUPER_ADMIN_IDS` in `.env` with fallback to `ADMIN_CHAT_ID`.
  - `/users [search]` paginated interactive user directory with channel status and vault counts.
  - `/user <id_or_username>` deep inspection profile card.
  - Interactive management actions:
    - `[ 📁 View Stored Vault ]`: Paginated browser of all tracks stored in a user's channel.
    - `[ 🔗 Unlink Channel ]`: Forcefully disconnect a user's storage channel and mark their files inactive.
    - `[ 👑 Make Admin / Demote Admin ]`: Promote or demote admins.
    - `[ 🚫 Ban User / Unban User ]`: Restrict abusive users from search and downloads.
    - `/dm <user_id> <message>`: Official administrative direct messaging.
  - Anti-abuse ban checks in search and download pipelines.
  - Comprehensive Render Web Service deployment guide in `readme.md`.

## [1.0.0] - 2026-09-29

### Added
- **Decentralized User-Owned Storage:**
  - Replaced centralized storage channel with distributed user-owned private Telegram channels.
  - Auto-detection of channel administrator status via `ChatMemberUpdated` event.
  - Polite onboarding notice with one-tap confirmation button explaining the shared peer network.
  - Manual channel linking via `/setchannel <id>` and `/mychannel` status inspection.
- **Dual Delivery & Interactive Deep-Links:**
  - Posts media directly to user's private channel.
  - Delivers playable audio or video file to user's PM with interactive buttons:
    - `[ 📂 Open in Channel ]` — Direct deep-link (`https://t.me/c/...`).
    - `[ 🗑️ Delete ]` — Instant callback button to delete from channel and database.
    - `[ ⚡ Force Re-download ]` — Bypass cached copies to fetch fresh media.
- **Peer-to-Peer Replication Engine:**
  - Automated cross-user replication via `bot.copy_message` from surviving peer channel nodes.
  - Automatic failure detection: marks inaccessible or revoked nodes as unavailable and routes to next candidate node.
- **Multi-Node Database Schema:**
  - Dual support for Neon Serverless PostgreSQL (`asyncpg`) and SQLite (`aiosqlite`) fallback.
  - Created `user_media_storage`, `users`, `tracks`, `api_cache`, `download_history`, and `rate_limits` tables.
  - PgBouncer compatibility (`statement_cache_size=0`) and Neon cold-start exponential backoff.
- **Stream Extractor API Integration:**
  - Connected to `https://ytsp-api.pgwiz.cloud` with Spotify & YouTube search and stream resolution.
  - Pre-packaged ID3-tagged MP3 audio downloads.
  - FFmpeg container transcoding to prevent malformed playback errors on mobile clients.
  - Genuine MP4 streaming video (`720p HD`).
  - Emergency fallback to local `yt-dlp`.
- **Administrative Suite:**
  - `/admin` Control Panel with real-time user, channel, and media node statistics.
  - `/cleanup` for purging leftover temp files, expired DB cache rows, and flushing RAM.
  - `/broadcast` for sending announcements to registered users.
- **Cloud Hosting & WSGI Entrypoint:**
  - FastAPI server with lifespan manager handling background `aiogram` polling.
  - `GET /` and `GET /health` endpoints for Render 24/7 uptime monitoring.
  - `wsgi.py` and `your_application/wsgi.py` wrapped with `a2wsgi` for Render deployment.
- **Dual Remote Git Synchronization:**
  - Configured Git `origin` to push simultaneously to `pgwiz/sociobot` and private `WiPTech/sociobot`.
