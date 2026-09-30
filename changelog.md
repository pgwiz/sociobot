# Changelog: Sociobot

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.3] - 2026-09-30

### Added
- **Environment Example File (`.env.example`):**
  - Added clean, copy-pasteable `.env.example` with complete configuration settings for Telegram tokens, Super Admin IDs, Neon PostgreSQL, isolated schema, connection pool parameters, quotas, and cloud hosting ports.
- **Render & Uvicorn Cloud Deployment Guide:**
  - Documented the `python -m bot.main` startup procedure and dynamic `$PORT` binding for Render and cloud platforms.
  - Detailed the FastAPI Lifespan architecture running Uvicorn HTTP endpoints (`/`, `/health`) and Aiogram 3 background Telegram polling concurrently with zero connection leaks.

### Changed
- **Repository Cleanliness & Privacy:**
  - Removed `agent.md` from git tracking and history, added `agent.md` to `.gitignore`.
  - Sanitized public endpoints and personal admin credentials in `.env.example` and `readme.md`, replacing them with generic domain and placeholder values (`https://api.yourdomain.com`, `123456789`).


## [2.0.2] - 2026-09-30

### Added
- **/mychannels Interactive Channel Hub & 3-Step Media Purge:**
  - Added `/mychannels` and `/mychanels` (alias) commands rendering connected storage channels as interactive inline buttons.
  - Channel summary cards displaying Title, ID, Role (Primary vs Secondary), routed platforms, and total stored media item count.
  - Channel action buttons in full-width mobile-safe layout:
    - `[ 🔗 Visit Channel ]`: Direct URL deep-link to channel (`https://t.me/<username>` or `https://t.me/c/<clean_id>/`).
    - `[ 🗑️ Unlink Channel ]`: Detaches channel and automatically falls back routed platforms to primary channel.
    - `[ 💥 Delete All Channel Media ]`: Initiates strict 3-step confirmation purge.
    - `[ ⬅️ Back to Channels ]`: Instant navigation back to channel selection list.
  - **3-Step Confirmation Purge Flow:**
    - Step 1 (Confirmation 1/3): Explicit warning regarding deletion from both Telegram and database library.
    - Step 2 (Confirmation 2/3): Permanent action irreversibility verification.
    - Step 3 (Confirmation 3/3): Final purge execution authorization.
    - Purges messages from Telegram channel (`bot.delete_message`) and database (`user_media_storage`), providing final count breakdown.
    - Added concurrent purge mutex guards (`_purging_channels`), 50ms rate-limit pacing, `TelegramRetryAfter` exponential wait/retry, and `TelegramBadRequest` graceful handling.
    - Added comprehensive HTML entity escaping across all channel titles and cards to prevent parsing failures.
  - **Database Helpers:**
    - `get_channel_media_count(user_chat_id, channel_id)`: Accurate active media count per channel across PostgreSQL and SQLite.
    - `purge_channel_media(user_chat_id, channel_id)`: Deletes channel media records and safely returns positive `(channel_id, channel_msg_id)` tuples for physical Telegram message deletion.

## [2.0.1] - 2026-09-30

### Fixed
- **Telegram 64-byte Callback Length Limit (`BUTTON_DATA_INVALID`):**
  - Implemented URL tokenization engine (`bot.utils.track_ref`) mapping long URLs to deterministic 16-hex references with multi-tier cache persistence.
  - All callback data payloads (`cb:del:...`, `cb:force:...`, `cb:extract_audio:...`, `cb:dl:...`) now remain strictly <= 36 bytes (well under the 64-byte limit).
  - Added seamless bidirectional resolution (`resolve_track_ref`) across delete, force re-download, format selection, and audio extraction handlers.
- **Social Video Blank Screen & Missing Audio Streams:**
  - Updated `get_quality_for_platform` for social video platforms (Instagram, TikTok, Twitter/X, Reddit) to `"360p"` (instead of `"saver"`), ensuring both video and audio streams are downloaded.
  - Added `has_video_stream` stream integrity check via `ffprobe` and container validation in `bot/downloader.py` and before `bot.send_video()` in `bot/handlers/download.py`.
  - Discard corrupt/audio-only containers early so fallback mechanisms can fetch valid video streams.
  - Updated `GENERIC_QUALITY_MAP['saver']` in `org-yt-dl` to `'b[height<=480]/b[height<=360]/worst[ext=mp4]/worst'`.

## [2.0.0] - 2026-09-30

### Added
- **Multi-Platform Media Extraction:**
  - Expanded beyond YouTube and Spotify to support 9 media platforms: YouTube, Spotify, TikTok, Instagram, Twitter/X, Reddit, SoundCloud, Bandcamp, and Vimeo.
  - Regex-based URL platform detector (`bot.utils.platform`) with strict domain delimiters.
  - Social videos (TikTok, Instagram, Twitter/X, Reddit) automatically download in lowest quality (`saver` MP4) to preserve storage and bandwidth.
  - Interactive `[ 🎵 Extract Audio ]` button attached to social video deliveries for 1-tap conversion to ID3-tagged 320k MP3 audio.
  - Music platforms (Spotify, SoundCloud, Bandcamp, YouTube audio) default to high-bitrate ID3-tagged MP3 (`audio_high`).
- **Multi-Channel Storage Vaults & Quotas:**
  - Users can now connect up to 5 private storage channels on the free tier (`DEFAULT_MAX_CHANNELS = 5`).
  - Quota is fully upgradeable by Super Admins via the `/user <id>` panel (5, 10, 20, or Unlimited).
  - New `/channels` interactive management dashboard showing all linked vaults, active routes, and primary vault designations.
  - Dedicated primary vault system: the first connected channel becomes primary automatically; users can switch primary vault with 1 tap.
  - Safe channel unlinking with automatic platform route fallback: when a channel is unlinked, all assigned platforms automatically re-route to the primary vault.
- **Platform Routing Matrix:**
  - Platform-to-vault mapping matrix: route specific platforms to dedicated channels (e.g., Spotify/SoundCloud to *Music Vault*, TikTok/Reels to *Clips Archive*).
  - Dynamic channel picker: when an unrouted media link is sent by a user with multiple channels, an inline prompt asks where that platform's media should be saved, remembering the selection for all future links.
- **Super Admin Multi-Channel & Quota Controls:**
  - `/user <id>` inspection card now renders all linked channels, active platform routes, and channel quota badges.
  - Added per-channel unlinking action buttons (`cb:adm_unlink_ch:<user_id>:<channel_id>`).
  - Added interactive quota upgrade menu (`cb:adm_quota_menu:<user_id>`).
- **Database Schema Upgrades:**
  - Added `user_channels` and `user_platform_routes` tables with zero data loss.
  - Added `max_channels` column to `users`.
  - Added `platform` and `destination_channel_id` columns to `user_media_storage`.
  - Seamless backward compatibility: legacy single-channel users automatically retain their channel as primary without disruption.

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
