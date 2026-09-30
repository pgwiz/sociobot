# Sociobot 🎧⚡

**Sociobot** is a high-performance, asynchronous Telegram bot powered by `aiogram 3.x`, FastAPI, and Neon Serverless PostgreSQL. It transforms personal private Telegram channels into a distributed, peer-to-peer media storage network.

Instead of relying on a centralized bot-owned dump channel, users link their own private Telegram channels as their personal music vaults. The bot uploads media to the user's channel, delivers playable copies to their direct chat with interactive deep-links and delete controls, and seamlessly cross-replicates media between peer channels to eliminate redundant downloads and bandwidth consumption.

---

## 🌟 Key Features

1. **Decentralized Multi-Channel Storage Vaults:**
   - Connect up to 5 private Telegram storage channels on the free tier (expandable up to 100/unlimited by admins).
   - Dedicated `/channels` interactive dashboard for viewing vaults, toggling platform routes, designating primary vaults, and unlinking.
   - Primary channel fallback: unlinking a channel automatically falls back any routed platforms to your primary vault without media loss.

2. **Multi-Platform Media Extraction:**
   - Native support for 9 platforms: **YouTube, Spotify, TikTok, Instagram, Twitter/X, Reddit, SoundCloud, Bandcamp, and Vimeo**.
   - Social videos (TikTok, Instagram, Twitter/X, Reddit) automatically download in bandwidth-efficient lowest quality (`saver` MP4) with an attached `[ 🎵 Extract Audio ]` button.
   - Music platforms (Spotify, SoundCloud, Bandcamp, YouTube audio) default to high-bitrate ID3-tagged MP3 (`audio_high` 320k).
   - Emergency fallback to local `yt-dlp`.

3. **Platform Routing Matrix & Dynamic Prompting:**
   - Map specific platforms to dedicated storage vaults (e.g. Spotify to *Music Vault*, TikTok/Instagram to *Clips Archive*).
   - When pasting an unrouted link with multiple connected vaults, Sociobot prompts once with an inline picker and remembers your destination vault for all future links.

4. **Peer-to-Peer Cross-User Replication:**
   - When User B requests a track already cached in User A's channel, the bot replicates it via `copy_message` directly into User B's channel in `<200ms`.
   - User B gets their own independent copy with their own delete button.
   - Replicating creates distributed multi-node redundancy across channels.

5. **Dual Delivery & Interactive Controls:**
   - Uploads to the user's private channel and delivers a playable audio or video file directly to their private chat (PM).
   - Attached action buttons:
     - `[ 📂 Open in Channel ]` — Direct deep-link (`https://t.me/c/<clean_id>/<msg_id>`) to the channel post.
     - `[ 🎵 Extract Audio ]` — 1-tap conversion of social videos to pure ID3-tagged MP3.
     - `[ 🗑️ Delete ]` — One-tap deletion that removes the message from the user's channel and updates database records.
     - `[ ⚡ Force Re-download ]` — Bypasses cached copies and fetches fresh media from the extractor.

6. **Polite Onboarding & Privacy Notice:**
   - Auto-detects when the bot is promoted to administrator in a private channel via Telegram's `ChatMemberUpdated` event.
   - When connecting a 2nd+ channel, prompts with the platform routing matrix to assign platforms right away.

7. **Dual Database Engine & Schema Isolation:**
   - **Neon Serverless PostgreSQL (Primary):** Isolated custom schema (`DB_SCHEMA=sociobot`) preventing any collision with other bots sharing the same database. Connection pooling with PgBouncer compatibility (`statement_cache_size=0`), cold-start exponential backoff, automatic query reconnection, and optional keepalive ping loop.
   - **SQLite (Local Fallback):** Asynchronous `aiosqlite` with WAL mode enabled.

8. **Production Ready & Cloud Deployable:**
   - FastAPI health check and metrics endpoints (`/`, `/health`, `/stats`).
   - WSGI & ASGI compatible via `a2wsgi` for seamless 24/7 Render deployment under `gunicorn your_application.wsgi`.

---

## 📋 Commands Reference

### User Commands
| Command | Description |
| :--- | :--- |
| `/start` | Welcome greeting, channel status check, and setup guide. |
| `/help` | Complete command usage reference. |
| `/channels` | Interactive multi-channel storage vault and platform routing dashboard. |
| `/mychannels` | Interactive channels button list with channel summary, direct visit link, unlinking, and 3-step media purge. |
| `/mychannel` | Inspect currently linked primary channel status and ID. |
| `/setchannel <id>` | Manually link a channel by ID (e.g. `-100...`) or by forwarding a message. |
| `/search <query>` | Search Spotify/YouTube catalog with 1-click download buttons. |
| `/download <url> [force]` | Download audio (MP3 320k) from supported platforms. |
| `/video <url> [force]` | Download video (MP4) from supported platforms. |
| `/history` | View recent vault downloads with direct channel links. |
| `/delete <track_id>` | Delete track copies from your personal channel and library. |

### Administrative Commands
| Command | Description |
| :--- | :--- |
| `/admin` | Interactive dashboard with real-time user, channel, and node statistics. |
| `/cleanup` | Purges leftover temp files, removes expired API cache, and flushes RAM. |
| `/broadcast <text>` | Sends system notifications to all registered users. |

### 👑 Super Admin User Management
| Command | Description |
| :--- | :--- |
| `/users [search]` | Interactive paginated browser to view all registered users, channel status, and vault counts. |
| `/user <id or @username>` | Deep inspection card for a specific user with multi-channel and quota controls. |
| `/dm <user_id> <message>` | Send an official direct message to a user. |

**Super Admin In-Bot Actions:**
- `[ 📁 View Stored Vault ]`: Browse the tracks currently archived in that user's channel.
- `[ 🗑️ Unlink: <channel> ]`: Forcefully disconnect a specific storage channel for that user.
- `[ 💎 Upgrade Quota ]`: Set user maximum channel quota (5, 10, 20, or 100/Unlimited).
- `[ 👑 Make Admin / Demote Admin ]`: Promote or demote admins.
- `[ 🚫 Ban User / Unban User ]`: Block abusive users from using the bot.

---

## 🚀 Deployment Guide (Render Web Service)

Sociobot is pre-configured for seamless 24/7 cloud hosting on platforms like [Render](https://render.com), Railway, or Fly.io as a Web Service.

### 1. Create a Web Service on Render
- Connect your GitHub repository (`https://github.com/pgwiz/sociobot` or private `WiPTech/sociobot`).
- **Runtime:** `Python 3`
- **Build Command:** `pip install -r requirements.txt`
- **Start Command:** `python -m bot.main`
- **Health Check Path:** `/health`

### 2. How the Uvicorn & Platform Startup Procedure Works
Sociobot combines an HTTP web server and an asynchronous Telegram bot into a single unified process via **FastAPI Lifespan** managed by **Uvicorn**:
1. **Dynamic Port Binding:** When starting with `python -m bot.main`, Sociobot automatically inspects the environment for `PORT` (which Render assigns dynamically, e.g. `10000` or `8080`) and binds Uvicorn to `0.0.0.0:$PORT`.
2. **Environment Variable Loading:** Config values are read directly from Render's Environment settings (or `.env` file locally).
3. **Lifespan Startup Sequence:**
   - Connects to Neon PostgreSQL (or SQLite) and runs automatic schema migrations on the isolated schema (default `sociobot`).
   - Initializes the Aiogram 3 bot and syncs the Telegram command menu.
   - Launches Telegram bot polling (`dp.start_polling`) concurrently as a background task inside the async event loop.
4. **Health Verification:** Uvicorn immediately begins serving `GET /` and `GET /health` with HTTP 200 OK responses, allowing Render's load balancer to verify the service is healthy and mark the deployment live.
5. **Graceful Shutdown:** When Render restarts or redeploys the container, the FastAPI lifespan executes a clean shutdown: it stops Telegram polling, cancels background tasks, and drains database connection pools without connection leaks.

> [!NOTE]
> You can also start the server using Uvicorn directly if preferred:
> ```bash
> uvicorn bot.main:app --host 0.0.0.0 --port $PORT
> ```

### 3. Required Environment Variables on Render
Add the following in the Render **Environment** dashboard (reference `.env.example` for details):

| Variable | Description | Example |
| :--- | :--- | :--- |
| `TELEGRAM_BOT_TOKEN` | Bot token from @BotFather *(Required)* | `123456789:ABCDefGhIJKlmNoPQRsTUVwxyZ` |
| `ADMIN_CHAT_ID` | Primary admin Telegram numeric ID | `123456789` |
| `SUPER_ADMIN_IDS` | Comma-separated Super Admin IDs | `123456789` |
| `DATABASE_URL` | Neon Serverless PostgreSQL connection string | `postgresql://user:pass@ep-xyz-pooler.tech/neondb?sslmode=require` |
| `DB_SCHEMA` | Custom schema name for table isolation (default: `sociobot`) | `sociobot` |
| `ENABLE_NEON_KEEPALIVE` | Ping Neon every 4 min to keep serverless compute awake | `true` |
| `YTSP_API_BASE_URL` | Stream Extractor API base URL | `https://api.yourdomain.com` |
| `PORT` | Dynamic port provided automatically by Render | `8080` (or `10000`) |

> [!TIP]
> **Why Neon PostgreSQL is recommended for Render:**
> Render container disks are ephemeral on free instances. Using a remote PostgreSQL database (like Neon) ensures user channel registrations, media mapping links, and download history persist across deploys and container restarts.

---

## 💻 Local Setup & Development

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
SUPER_ADMIN_IDS=12345678
ADMIN_CHAT_ID=12345678
DATABASE_URL=sqlite:///sociobot.db
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
