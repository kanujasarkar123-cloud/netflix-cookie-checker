# Netflix Cookie Checker — Telegram Bot

A high-speed Telegram bot that validates Netflix cookies in bulk, extracts full account details, and saves hits to MongoDB.

![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/GHCR-kanujasarkar123--cloud%2Fnetflix--cookie--checker-2496ED?logo=docker&logoColor=white)
![UI](https://img.shields.io/badge/UI-100%25_Inline_Buttons-26A5E4?logo=telegram&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-22c55e)

---

## Quick Deploy (Docker)

```bash
docker run -d --name nf-checker \
  -e TELEGRAM_BOT_TOKEN=123456:ABC-your-token \
  -p 5000:5000 \
  --restart unless-stopped \
  ghcr.io/kanujasarkar123-cloud/netflix-cookie-checker:latest
```

Or with docker-compose:

```bash
export TELEGRAM_BOT_TOKEN=123456:ABC-your-token
docker compose up -d
```

---

## Features

- **Bulk cookie checking** — paste text or upload `.txt` / `.zip` / `.rar` file
- **Fully button-based UI** — main menu, help, settings, live stats and proxy panel all reachable through inline buttons
- **Live 1-by-1 progress** — the status message ticks after *every* completed account (`3/50`, `4/50`, …), never in batch jumps
- **Proxy race engine v2** — races N proxies per check, IP-pool rotation across the whole pool, direct only as an emergency fallback
- **Self-healing proxy pool** — 9 best-in-class free sources seeded automatically, refreshed in parallel every cycle, instant-use rotation (no slow pre-validation stage), EMA latency scoring, one-strike removal for dead IPs
- **Consistency safety net** — if the pool is momentarily empty the check falls back to direct instead of returning an error storm
- **MongoDB integration** — hits, free accounts, and on-hold accounts saved automatically
- **Multiple output modes** — Basic (compact) and Full Info
- **Delivery modes** — individual messages or ZIP archive
- **Multi-user ready** — concurrent sessions with per-user and global capacity guards
- **Change password** — [BETA] change a Netflix account password directly from Telegram

---

## Setup

### 1. Clone the repo

```bash
git clone https://github.com/kanujasarkar123-cloud/netflix-cookie-checker.git
cd netflix-cookie-checker
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Set environment variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `TELEGRAM_BOT_TOKEN` | ✅ | — | Telegram bot token from @BotFather |
| `MONGODB_URL` | ❌ | — | MongoDB connection string (hits saved when set) |
| `ADMIN_ID` | ❌ | first `/setadmin` user | Telegram user ID of the admin |
| `RACE_N` | ❌ | `4` | How many proxies to race simultaneously per check |
| `BULK_CONCURRENCY` | ❌ | `16` | How many cookies to check per batch window |
| `MAX_GLOBAL_CHECKS` | ❌ | `6` | Max simultaneous user sessions before a friendly busy note |
| `PROXY_REFRESH_INTERVAL` | ❌ | `300` | Seconds between source re-fetch cycles |
| `PROXY_POOL_MIN` | ❌ | `40` | Alive proxies below this triggers an urgent refill |
| `PROXY_MAX_POOL` | ❌ | `8000` | Hard cap on pool size |
| `PROXY_SCREEN` | ❌ | `1` | Background TCP screening of new IPs (`0` = off) |
| `PROXY_SCREEN_TIMEOUT` | ❌ | `2.0` | TCP connect timeout for screening (seconds) |
| `PROXY_PRELOAD` | ❌ | `1` | Seed the 9 built-in free proxy sources on first run |

> **MongoDB URL tip:** If your password contains `@`, it is automatically percent-encoded — no manual fix needed.

> **First admin:** set the `ADMIN_ID` env var, or simply send `/setadmin` once from your Telegram account — the first user to do it becomes admin.

### 4. Run

```bash
python bot.py
```

---

## Bot Commands

| Command | Description |
|---|---|
| `/start` | Main menu — everything reachable through buttons |
| `/help` | Supported formats & bulk mode guide (with buttons) |
| `/info` | Live stats & bot info (with 🔄 refresh button) |
| `/settings` | Output format & delivery mode |
| `/mode` | Toggle Basic ↔ Full Info |
| `/basic` | Switch to Basic (compact) mode |
| `/fullinfo` | Switch to Full Info mode |
| `/changepw` | [BETA] Change a Netflix account password |
| `/cancel` | Cancel any active flow |

> Admin-only commands (`/proxy`, `/setadmin`) are intentionally hidden from the public menu but still work. The 🛡 Proxy Pool button appears in the main menu for admins only.

---

## Supported Cookie Formats

| Format | Example |
|--------|---------|
| Netscape `.txt` | `.netflix.com TRUE / TRUE … NetflixId ct%3D…` |
| CookieCheckerPro | `[user]-[IN]-[Premium]-[4K+HDR]-[21 Apr 2026]-[VISA]` |
| Pipe-combo | `email:pass \| Country=IN \| NetflixId=ct%3D…` |
| JSON array | `[{"name":"NetflixId","value":"ct%3D…"}]` |
| ZIP bundle | Each `.txt` / `.json` inside = one account |

---

## Proxy Pool (v2 engine)

The pool is **self-healing and self-seeding** — zero manual setup required:

- **9 built-in sources** (TheSpeedX, monosans, shiftytr, clarketm, mmpx12, sunny9577, roosterkid, ProxyScrape API, Geonode API) are seeded on first run — usually 4,000–7,000 proxies within the first minute
- **Instant use** — fresh proxies enter the pool as *new* and are eligible for real checks immediately; no slow pre-validation stage (free proxies die fast, so the real check *is* the screening)
- **Background screening** — light 2s TCP-connect passes continuously promote alive IPs and drop dead ones (never blocks live checks)
- **In-line screening** — one hard failure (timeout, 429, login-page) removes an IP from the pool instantly
- **EMA latency scoring** — fastest IPs are raced first, but a rotating window keeps the *entire* pool in circulation (true IP rotation, not one hosted IP)
- **Auto-refill** — all sources re-fetched in parallel every cycle; if alive count drops below `PROXY_POOL_MIN`, an urgent refill fires on the spot
- **Consistency net** — pool momentarily empty → the check falls back to direct instead of erroring out, and counts the fallback in the admin panel

Admin panel (`/proxy` or 🛡 Proxy Pool button):

- Turn the pool ON/OFF, ♻️ Refill Pool Now, 🧪 Screen New IPs
- Add individual proxies (any common format) or whole source URLs
- Live pool stats: total / alive / new / scored, checks served, direct fallbacks
- Download the full list or clear the pool anytime

---

## Performance Tuning

| Env Var | Default | Suggested Range |
|---|---|---|
| `RACE_N` | `4` | `4`–`8` (more = faster verdicts on a large pool, more bandwidth) |
| `BULK_CONCURRENCY` | `16` | `16`–`32` (per-user parallel window) |
| `MAX_GLOBAL_CHECKS` | `6` | `4`–`10` (simultaneous users) |
| `PROXY_REFRESH_INTERVAL` | `300` | `180`–`600` (lower = fresher pool, more source traffic) |

---

## Status Dashboard

Runs automatically on port `5000` — the admin status site:

| Endpoint | Purpose |
|---|---|
| `/` | Live status page — users, checks, hits, invalid, free, on-hold, checks/min, hit-rate, uptime + recent activity (auto-refresh every 10s) |
| `/api/stats` | JSON stats feed |
| `/health` | Health endpoint for uptime monitors / Docker HEALTHCHECK |

Aggregate counters only — cookies and account data are never exposed here.

---

## File Structure

| File | Purpose |
|------|---------|
| `bot.py` | Telegram handlers, button UI, formatters, streaming bulk processing, ZIP export |
| `checker.py` | Cookie parsing, Netflix HTTP validation, proxy race + direct fallback |
| `proxy_manager.py` | Proxy pool engine v2 — self-seeding sources, instant use, EMA scoring |
| `dashboard.py` | Flask status dashboard (port 5000) |
| `mongodb_store.py` | MongoDB persistence for hits (auto-connects on startup) |
| `password_changer.py` | [BETA] Netflix password changer |
| `stats.py` | Thread-safe in-memory stats tracker |
| `requirements.txt` | Python dependencies |
| `Dockerfile` / `docker-compose.yml` | Container deploy |
| `.github/workflows/docker-publish.yml` | Auto-builds the GHCR image on every push to `main` |

---

## Deploy Anywhere

| Platform | How |
|---|---|
| **VPS** (Ubuntu / Debian / CentOS) | Install Docker → `docker compose up -d` — done |
| **Railway / Render / Koyeb** | Deploy the GHCR Docker image, set `TELEGRAM_BOT_TOKEN` env var (port 5000 optional) |
| **Synology / unRAID / QNAP NAS** | Docker GUI → image below + env vars |
| **Replit** | `replit.nix` included — set secret `TELEGRAM_BOT_TOKEN` and run |

> A Telegram bot is an always-on process — a tiny 1 vCPU / 1 GB VPS is enough. Runtime files (`proxy_pool.db`, `admin_id.txt`) are created automatically in the working directory; mount a volume if you want the proxy-score cache to survive redeploys.

---

## GHCR Image

The image is **public** and rebuilt automatically on every push to `main` (see `.github/workflows/docker-publish.yml`) — so the image always matches the repo. Pull it from anywhere, no login required:

```bash
docker pull ghcr.io/kanujasarkar123-cloud/netflix-cookie-checker:latest
```

<details>
<summary>If you make the repo private later</summary>

The package becomes private too — pull with a GitHub token that has `read:packages`:

```bash
echo YOUR_GITHUB_TOKEN | docker login ghcr.io -u kanujasarkar123-cloud --password-stdin
```
</details>

### Build the image yourself

```bash
git clone https://github.com/kanujasarkar123-cloud/netflix-cookie-checker.git
cd netflix-cookie-checker
docker build -t nf-checker .
```

---

## Requirements

- Python 3.10+ (or just Docker)
- See `requirements.txt`

---

## License

[MIT](LICENSE) © 2026 kanujasarkar123-cloud

---

## Disclaimer

This tool is for **educational purposes** and for checking cookies/accounts **you own or have explicit permission to test**. Do not use it against accounts you have no right to access.
