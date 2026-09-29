# Changelog: Sociobot

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
