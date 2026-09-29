# Sociobot 🎧⚡

**Sociobot** is a high-performance, asynchronous Telegram bot powered by `aiogram 3.x`, FastAPI, and Neon Serverless PostgreSQL. It transforms personal private Telegram channels into a distributed, peer-to-peer media storage network.

Instead of relying on a centralized bot-owned dump channel, users link their own private Telegram channels as their personal music vaults. The bot uploads media to the user's channel, delivers playable copies to their direct chat with interactive deep-links and delete controls, and seamlessly cross-replicates media between peer channels to eliminate redundant downloads and bandwidth consumption.

---

## 🌟 Key Features

1. **Decentralized User-Owned Channel Warehousing:**
   - Every user connects their own private Telegram channel.
   - Media downloaded by a user is permanently saved to their private channel.
   - The user has complete control to inspect, forward, or delete any media.

2. **Peer-to-Peer Cross-User Replication:**
   - When User B requests a track already cached in User A's channel, the bot replicates it via `copy_message` directly into User B's channel in `<200ms`.
   - User B gets their own independent copy with their own delete button.
   - Replicating creates distributed multi-node redundancy across channels.

3. **Dual Delivery & Interactive Controls:**
   - Uploads to the user's private channel and delivers a playable audio or video file directly to their private chat (PM).
   - Attached action buttons:
     - `[ 📂 Open in Channel ]` — Direct deep-link (`https://t.me/c/<clean_id>/<msg_id>`) to the channel post.
     - `[ 🗑️ Delete ]` — One-tap deletion that removes the message from the user's channel and updates database records.
     - `[ ⚡ Force Re-download ]` — Bypasses cached copies and fetches fresh media from the extractor.

4. **Polite Onboarding & Privacy Notice:**
   - Auto-detects when the bot is promoted to administrator in a private channel via Telegram's `ChatMemberUpdated` event.
   - Delivers a polite, concise notice:
     > *"✅ Channel linked! Your media is safely archived here for your full control. To keep downloads lightning-fast, audio may also be shared anonymously across the community network."*
     with a one-tap `[ Let's Go 🚀 ]` confirmation button.

5. **Keyless Music Extraction:**
   - Powered by `https://ytsp-api.pgwiz.cloud` with native Spotify and YouTube resolution.
   - Pre-packaged ID3-tagged MP3 audio files with high bitrate (`320k`).
   - Genuine MP4 streaming video (`720p HD`).
   - Local `yt-dlp` emergency fallback.

6. **Dual Database Engine:**
   - **Neon Serverless PostgreSQL (Primary):** Connection pooling with PgBouncer compatibility (`statement_cache_size=0`), cold-start exponential backoff, automatic query reconnection, and optional keepalive ping loop.
   - **SQLite (Local Fallback):** Asynchronous `aiosqlite` with WAL mode enabled.

7. **Production Ready & Cloud Deployable:**
   - FastAPI health check and metrics endpoints (`/`, `/health`, `/stats`).
   - WSGI & ASGI compatible via `a2wsgi` for seamless 24/7 Render deployment under `gunicorn your_application.wsgi`.

---

## 📋 Commands Reference

### User Commands
| Command | Description |
| :--- | :--- |
| `/start` | Welcome greeting, channel status check, and setup guide. |
| `/help` | Complete command usage reference. |
| `/mychannel` | Inspect currently linked channel status, ID, and connection state. |
| `/setchannel <id>` | Manually link a channel by ID (e.g. `-100...`) or by forwarding a message. |
| `/search <query>` | Search Spotify/YouTube catalog with 1-click download buttons. |
| `/download <url> [force]` | Download audio (MP3 320k) from Spotify or YouTube. |
| `/video <url> [force]` | Download video (MP4 720p) from YouTube. |
| `/history` | View recent vault downloads with direct channel links. |
| `/delete <track_id>` | Delete track copies from your personal channel and library. |

### Administrative Commands
| Command | Description |
| :--- | :--- |
| `/admin` | Interactive dashboard with real-time user, channel, and node statistics. |
| `/cleanup` | Purges leftover temp files, removes expired API cache, and flushes RAM. |
| `/broadcast <text>` | Sends system notifications to all registered users. |

---

## 🚀 Quick Setup & Installation

### 1. Prerequisites
- Python 3.10+
- FFmpeg (recommended for audio transcoding)
- A Telegram Bot token from [@BotFather](https://t.me/BotFather)

### 2. Clone & Install
```bash
git clone https://github.com/pgwiz/sociobot.git
cd sociobot

python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

pip install -r requirements.txt
```

### 3. Environment Configuration
Copy `.env.example` to `.env` and fill in your credentials:
```bash
cp .env.example .env
```
Key variables:
```ini
TELEGRAM_BOT_TOKEN=123456789:ABCDEF...
ADMIN_CHAT_ID=12345678
DATABASE_URL=postgresql://user:pass@ep-xyz-pooler.neon.tech/dbname?sslmode=require
# Or leave DATABASE_URL blank to default to local SQLite (sociobot.db)
```

### 4. Run the Verification Suite
```bash
python test_verification.py
```

### 5. Start the Bot
```bash
# Direct startup
python -m bot.main

# Or via Gunicorn / Uvicorn (Render style)
uvicorn bot.main:app --host 0.0.0.0 --port 8080
```

---

## 🔄 Dual Git Remote Synchronization

This repository is configured with dual push URLs under `origin`:
1. `https://github.com/pgwiz/sociobot.git` (Public)
2. `https://github.com/WiPTech/sociobot.git` (Private)

A single push command synchronizes both remotes:
```bash
git push origin main
```

---

## 📄 License
MIT License.
