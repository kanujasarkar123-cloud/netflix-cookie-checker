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
- **Hydra v2.0 proxy engine** — 108 verified-live sources (HTTP + SOCKS4 + SOCKS5, extracted from the Hydra Proxy Scraper bytecode), Hydra's exact IP:PORT regex parser, and real protocol-level handshakes for every proxy. Instant-use rotation, EMA latency scoring, one-strike removal
- **Consistency safety net** — if the pool is momentarily empty the check falls back to direct instead of returning an error storm
- **MongoDB integration** — hits, free accounts, and on-hold accounts saved automatically
- **Multiple output modes** — Basic (compact) and Full Info
- **Delivery modes** — individual messages or ZIP archive
- **Multi-user ready** — every user checks in their own parallel session; while your check runs, your other commands pause with a friendly "/cancel first" note — other users are never affected
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
| `RACE_N` | ❌ | `6` | How many proxies to race simultaneously per check |
| `BULK_CONCURRENCY` | ❌ | `16` | How many cookies to check per batch window |
| `MAX_GLOBAL_CHECKS` | ❌ | `8` | Max simultaneous user sessions before a friendly busy note |
| `PROXY_REFRESH_INTERVAL` | ❌ | `120` | Seconds between source re-fetch cycles |
| `PROXY_POOL_MIN` | ❌ | `80` | Alive proxies below this triggers an urgent refill |
| `PROXY_MAX_POOL` | ❌ | `8000` | Hard cap on pool size |
| `PROXY_SCREEN` | ❌ | `1` | Background TCP screening of new IPs (`0` = off) |
| `PROXY_SCREEN_TIMEOUT` | ❌ | `2.0` | TCP connect timeout for screening (seconds) |
| `PROXY_PRELOAD` | ❌ | `1` | Seed the built-in Hydra source set on first run |
| `PROXY_PER_SOURCE_CAP` | ❌ | `4000` | Max proxies kept per source per refresh cycle |
| `PROXY_MERGE_HARD_CAP` | ❌ | `32000` | Absolute ceiling on in-memory pool entries |
| `TELEGRAM_PROXY` | ❌ | — | Route all Telegram API traffic via a proxy (`http://…` or `socks5://…`) — escape hatch for hosts with unreliable egress to Telegram (e.g. some Render regions) |

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

- **108 built-in sources** (the Hydra v2.0 list: gfpcom, dinoz0rg, MuRongPIG, ErcinDedeoglu, zevtyardt, proxifly, TheSpeedX, monosans, OpenProxyList, ProxyScrape v2/v4 APIs and 90+ more — each live-verified before inclusion) are seeded on first run — tens of thousands of proxies within the first minutes
- **SOCKS4 + SOCKS5 support** — socks lists are stored with their real protocol scheme and validated with the actual SOCKS handshake (greeting + CONNECT), not just an open TCP port
- **Protocol-level screening** — HTTP proxies must serve a real request through the proxy (Hydra's Smart-mode engine); a port being open alone is never enough
- **Instant use** — fresh proxies enter the pool as *new* and are eligible for real checks immediately; no slow pre-validation stage (free proxies die fast, so the real check *is* the screening)
- **Background screening** — full protocol-handshake passes (HTTP / SOCKS4 / SOCKS5) continuously promote alive IPs and drop dead ones (never blocks live checks)
- **In-line screening** — one hard failure (timeout, 429, login-page) removes an IP from the pool instantly
- **EMA latency scoring** — fastest IPs are raced first, but a rotating window keeps the *entire* pool in circulation (true IP rotation, not one hosted IP)
- **Auto-refill** — all sources re-fetched in parallel every 2-minute cycle; if alive count drops below `PROXY_POOL_MIN`, an urgent refill fires on the spot
- **Retry rounds + verdict guarantee** — every cookie is re-raced across fresh proxy rounds (bulk ≈ 24 parallel attempts) until a definitive valid/invalid verdict; only if *every* round fails does the check fall back to one direct attempt — a user-facing ERROR is effectively impossible
- **Consistency net** — pool momentarily empty → the check falls back to direct instead of erroring out, and counts the fallback in the admin panel
- **Admin-controlled route** — the 🛡 Proxy Pool ON/OFF switch decides everything: **OFF = checks go straight through the hosting server's own IP** (direct), **ON = pool-only rotation** with the server IP never touched

Admin panel (`/proxy` or 🛡 Proxy Pool button):

- Turn the pool ON/OFF, ♻️ Refill Pool Now, 🧪 Screen New IPs
- Add individual proxies (any common format) or whole source URLs
- Live pool stats: total / alive / new / scored, checks served, direct fallbacks
- Download the full list or clear the pool anytime

---

## Performance Tuning

| Env Var | Default | Suggested Range |
|---|---|---|
| `RACE_N` | `6` | `4`–`10` (more = faster verdicts on a large pool, more bandwidth) |
| `BULK_CONCURRENCY` | `16` | `16`–`32` (per-user parallel window) |
| `MAX_GLOBAL_CHECKS` | `8` | `4`–`12` (simultaneous users) |
| `PROXY_REFRESH_INTERVAL` | `120` | `90`–`600` (lower = fresher pool, more source traffic) |

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
| `proxy_manager.py` | Proxy pool engine v3 (Hydra) — 108 sources, protocol handshakes, EMA scoring |
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

### Render notes (free tier)

- The dashboard auto-binds `$PORT`, so the Web Service health check passes out of the box.
- **Free web services sleep after ~15 min without inbound web traffic.** A polling bot generates none — keep it awake by pinging your `*.onrender.com` dashboard URL every 10 minutes with [UptimeRobot](https://uptimerobot.com) (free).
- Seeing `Telegram network error: Timed out` in the logs? Switch the service region (e.g. Oregon / Singapore), or set the `TELEGRAM_PROXY` env var to route Telegram traffic through a working proxy.

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
