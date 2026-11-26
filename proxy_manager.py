"""
Proxy Manager v2 — Self-healing IP pool engine.

Design goals (v2):
  • Best-of-the-best free proxy sources seeded automatically — zero manual setup.
  • USE IMMEDIATELY: fresh proxies enter the pool as NEW and are instantly
    eligible for real checks. No long pre-validation stage (free proxies die
    fast, so we screen in the background while the checker is already running).
  • Fast background screening: parallel TCP-connect with a 2s timeout at high
    concurrency — promotes ALIVE, demotes DEAD. Never blocks live checks.
  • EMA latency scoring + rotating selection: every check gets a different
    slice of the pool (true rotation across ALL IPs, not one hosted IP).
  • In-line screening during real checks: one hard failure (timeout / 429 /
    login-page / connection error) removes the proxy from the pool instantly.
  • Auto-refill: all sources re-fetched in parallel every cycle; if the alive
    count drops below POOL_MIN, an extra refresh is triggered on the spot.
  • State persisted in SQLite (proxy_pool.db) — warm restarts keep scores.

Backward compatible public API (checker.py / bot.py / password_changer.py):
  normalize_proxy_url, proxy_manager.{count, available_count, enabled, toggle,
  set_enabled, add_proxy_raw, remove_proxy, clear_proxies, list_proxies,
  proxies_as_text, add_source, remove_source, list_sources, fetch_from_url,
  refresh_all_sources, get_proxies_dict, get_top_proxies_dicts,
  changepw_proxy_enabled, toggle_changepw_proxy, set_changepw_proxy_enabled,
  get_changepw_proxy_dict, status_text, mark_failure, mark_success}

New in v2:
  pool_stats(), ensure_pool(), note_direct_fallback(), screen_now(),
  refresh_now(), refresh_state
"""

import json
import os
import random
import socket
import sqlite3
import threading
import time
import logging
import requests
from pathlib import Path
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

_DB_FILE = Path("proxy_pool.db")

# ── Tunables (env-overridable) ────────────────────────────────────────────────
REFRESH_INTERVAL   = int(os.environ.get("PROXY_REFRESH_INTERVAL", "300"))   # source re-fetch cycle
SCREEN_TIMEOUT     = float(os.environ.get("PROXY_SCREEN_TIMEOUT", "2.0"))   # TCP connect timeout
SCREEN_WORKERS     = int(os.environ.get("PROXY_SCREEN_WORKERS", "400"))     # parallel TCP screens
SCREEN_BATCH       = int(os.environ.get("PROXY_SCREEN_BATCH", "600"))       # proxies per screening pass
MAX_POOL           = int(os.environ.get("PROXY_MAX_POOL", "8000"))          # hard cap on pool size
POOL_MIN           = int(os.environ.get("PROXY_POOL_MIN", "40"))            # alive proxies below this → urgent refill
SCREEN_ENABLED     = os.environ.get("PROXY_SCREEN", "1") not in ("0", "false", "False")
PRELOAD_SOURCES    = os.environ.get("PROXY_PRELOAD", "1") not in ("0", "false", "False")
_FETCH_TIMEOUT     = 15              # seconds per source HTTP fetch
_MAX_FAILURES      = 2               # soft failures before removal (hard failures remove instantly)
_FLUSH_INTERVAL    = 60              # persistence debounce
_PERSIST_LIMIT     = 12000           # max proxies written back to SQLite

_FETCH_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

# ── Best-of-the-best free sources (all verified reachable) ───────────────────
# Plain-text lists (ip:port per line) + two API sources handled by the parser.
DEFAULT_SOURCES: list[str] = [
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
    "https://raw.githubusercontent.com/shiftytr/proxy-list/master/proxy.txt",
    "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
    "https://raw.githubusercontent.com/mmpx12/proxy-list/master/http.txt",
    "https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/generated/http_proxies.txt",
    "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt",
    "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all",
    "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=http%2Chttps",
]

# Proxy entry states
ST_NEW, ST_ALIVE, ST_DEAD = 0, 1, 2

_EMA_ALPHA = 0.3  # weight of the latest latency sample


# ---------------------------------------------------------------------------
# Format normalizer (unchanged behaviour, used everywhere)
# ---------------------------------------------------------------------------

def normalize_proxy_url(raw: str) -> str | None:
    raw = raw.strip()
    if not raw or raw.startswith("#"):
        return None
    if "://" in raw:
        if raw.startswith(("http://", "https://", "socks5://", "socks4://")):
            return raw
        return None
    if "@" in raw:
        return "http://" + raw
    parts = raw.split(":")
    if len(parts) == 2:
        host, port = parts
        if port.isdigit() and 1 <= int(port) <= 65535:
            return f"http://{host}:{port}"
    elif len(parts) == 4:
        if parts[1].isdigit() and 1 <= int(parts[1]) <= 65535:
            host, port, user, pwd = parts
            return f"http://{user}:{pwd}@{host}:{port}"
        elif parts[3].isdigit() and 1 <= int(parts[3]) <= 65535:
            user, pwd, host, port = parts
            return f"http://{user}:{pwd}@{host}:{port}"
    return None


def _parse_host_port(proxy_url: str) -> tuple[str, int] | None:
    try:
        parsed = urlparse(proxy_url)
        host = parsed.hostname
        port = parsed.port
        if host and port:
            return host, port
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# SQLite persistence (config + sources + scored proxies)
# ---------------------------------------------------------------------------

def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(_DB_FILE), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _init_db() -> None:
    with _get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS proxies (
                url         TEXT PRIMARY KEY,
                added_at    REAL NOT NULL DEFAULT 0,
                fail_count  INTEGER NOT NULL DEFAULT 0,
                last_fail   REAL NOT NULL DEFAULT 0,
                last_ok     REAL NOT NULL DEFAULT 0,
                avg_latency REAL,
                state       INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS sources (
                url      TEXT PRIMARY KEY,
                added_at REAL NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS config (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT ''
            );
        """)
        cols = [r[1] for r in conn.execute("PRAGMA table_info(proxies)").fetchall()]
        if "state" not in cols:
            conn.execute("ALTER TABLE proxies ADD COLUMN state INTEGER NOT NULL DEFAULT 0")
        conn.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('enabled', '0')")
        conn.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('changepw_proxy', '0')")


class ProxyEntry:
    __slots__ = ("url", "state", "fail_count", "last_fail", "last_ok", "avg_latency", "added_at")

    def __init__(self, url: str, state: int = ST_NEW, fail_count: int = 0,
                 last_fail: float = 0.0, last_ok: float = 0.0,
                 avg_latency: float | None = None, added_at: float | None = None):
        self.url = url
        self.state = state
        self.fail_count = fail_count
        self.last_fail = last_fail
        self.last_ok = last_ok
        self.avg_latency = avg_latency
        self.added_at = added_at if added_at is not None else time.time()


# ---------------------------------------------------------------------------
# ProxyManager v2
# ---------------------------------------------------------------------------

class ProxyManager:
    def __init__(self):
        _init_db()
        self._lock = threading.RLock()
        self._entries: dict[str, ProxyEntry] = {}
        self._rot_idx: int = 0
        self._dirty = True
        self._stop_event = threading.Event()

        # Refresh bookkeeping (for the admin panel)
        self._last_refresh_ts: float = 0.0
        self._last_refresh_added: int = 0
        self._last_refresh_dead: int = 0
        self._last_refresh_sources_ok: int = 0
        self._last_refresh_sources_total: int = 0
        self._refreshing = False
        self._refresh_cooldown_until = 0.0

        # Counters
        self._checks_served = 0
        self._direct_fallbacks = 0

        self._load_from_db()
        self._seed_default_sources()
        self._kick_refresh(initial=True)

        self._bg_thread = threading.Thread(
            target=self._background_loop, daemon=True, name="proxy-bg"
        )
        self._bg_thread.start()

    # ── Startup helpers ───────────────────────────────────────────────────

    def _load_from_db(self) -> None:
        try:
            with _get_conn() as conn:
                rows = conn.execute(
                    "SELECT url, added_at, fail_count, last_fail, last_ok, avg_latency, state "
                    "FROM proxies"
                ).fetchall()
        except Exception as exc:
            logger.warning(f"proxy_manager: DB load failed: {exc}")
            return
        now = time.time()
        for url, added_at, fail_count, last_fail, last_ok, avg_latency, state in rows:
            if now - (added_at or 0) > 6 * 3600:
                continue  # stale entries older than 6h are dropped
            self._entries[url] = ProxyEntry(
                url,
                state=state if state in (ST_NEW, ST_ALIVE) else ST_NEW,
                fail_count=fail_count,
                last_fail=last_fail,
                last_ok=last_ok,
                avg_latency=avg_latency,
                added_at=added_at,
            )
        if self._entries:
            logger.info(f"proxy_manager: loaded {len(self._entries)} proxies from DB")

    def _seed_default_sources(self) -> None:
        """Insert the built-in best-of-the-best sources once."""
        if not PRELOAD_SOURCES:
            return
        try:
            with _get_conn() as conn:
                row = conn.execute(
                    "SELECT value FROM config WHERE key='defaults_seeded'"
                ).fetchone()
                if row and row[0] == "1":
                    return
                now = time.time()
                for url in DEFAULT_SOURCES:
                    conn.execute(
                        "INSERT OR IGNORE INTO sources (url, added_at) VALUES (?, ?)",
                        (url, now),
                    )
                conn.execute(
                    "INSERT OR REPLACE INTO config (key, value) VALUES ('defaults_seeded', '1')"
                )
        except Exception as exc:
            logger.warning(f"proxy_manager: seeding sources failed: {exc}")

    # ── Background loop ───────────────────────────────────────────────────

    def _kick_refresh(self, initial: bool = False) -> None:
        """Start a background source refresh immediately (non-blocking)."""
        threading.Thread(
            target=self._refresh_from_all_sources, daemon=True, name="prefresh"
        ).start()

    def _background_loop(self) -> None:
        last_refresh = 0.0
        last_flush = 0.0
        while not self._stop_event.is_set():
            now = time.time()
            try:
                if now - last_refresh >= REFRESH_INTERVAL:
                    last_refresh = now
                    self._refresh_from_all_sources()
                self._screen_unscreened()          # continuous, small batches
                self._trim_pool()
                if self._dirty and now - last_flush >= _FLUSH_INTERVAL:
                    last_flush = now
                    self._flush()
            except Exception as exc:
                logger.warning(f"proxy_manager: background error: {exc}")
            self._stop_event.wait(timeout=5)

    # ── Screening (fast TCP connect, background only) ─────────────────────

    def _screen_unscreened(self) -> None:
        """Screen a batch of NEW / stale-ALIVE entries. Never blocks checks."""
        if not SCREEN_ENABLED:
            return
        with self._lock:
            now = time.time()
            batch: list[ProxyEntry] = []
            for e in self._entries.values():
                if e.state == ST_NEW:
                    batch.append(e)
                elif e.state == ST_ALIVE and e.last_ok and now - e.last_ok > 900:
                    batch.append(e)   # re-verify stale alive entries
                if len(batch) >= SCREEN_BATCH:
                    break
        if not batch:
            return

        def _probe(entry: ProxyEntry) -> None:
            hp = _parse_host_port(entry.url)
            ok = False
            if hp:
                try:
                    with socket.create_connection(hp, timeout=SCREEN_TIMEOUT):
                        ok = True
                except (OSError, socket.timeout):
                    ok = False
            with self._lock:
                if ok:
                    if entry.state != ST_ALIVE:
                        entry.state = ST_ALIVE
                    entry.last_ok = time.time()
                    entry.fail_count = 0
                    self._dirty = True
                else:
                    entry.fail_count += 1
                    if entry.state == ST_NEW and entry.fail_count >= 2:
                        entry.state = ST_DEAD          # drop silently
                        self._entries.pop(entry.url, None)
                    elif entry.state == ST_ALIVE and entry.fail_count >= 3:
                        entry.state = ST_DEAD
                        self._entries.pop(entry.url, None)
                    self._dirty = True

        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=min(SCREEN_WORKERS, len(batch)),
                                thread_name_prefix="pscreen") as pool:
            list(pool.map(_probe, batch))

    def screen_now(self, count: int = 400) -> int:
        """Admin action — screen up to *count* unscreened proxies immediately."""
        with self._lock:
            batch = [e for e in self._entries.values() if e.state == ST_NEW][:count]
        if not batch:
            return 0

        def _probe(entry: ProxyEntry) -> None:
            hp = _parse_host_port(entry.url)
            ok = False
            if hp:
                try:
                    with socket.create_connection(hp, timeout=SCREEN_TIMEOUT):
                        ok = True
                except (OSError, socket.timeout):
                    ok = False
            with self._lock:
                if ok:
                    entry.state = ST_ALIVE
                    entry.last_ok = time.time()
                    entry.fail_count = 0
                else:
                    entry.fail_count += 1
                    if entry.fail_count >= 2:
                        self._entries.pop(entry.url, None)
                self._dirty = True

        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=min(SCREEN_WORKERS, len(batch)),
                                thread_name_prefix="pscreen") as pool:
            list(pool.map(_probe, batch))
        return len(batch)

    # ── Pool maintenance ──────────────────────────────────────────────────

    def _trim_pool(self) -> None:
        """Keep the pool under MAX_POOL — newest and best-scored survive."""
        with self._lock:
            if len(self._entries) <= MAX_POOL:
                return
            entries = sorted(
                self._entries.values(),
                key=lambda e: (
                    e.state != ST_ALIVE,                    # alive first
                    e.avg_latency if e.avg_latency else 9e9,
                    -(e.last_ok or e.added_at),             # freshest first
                ),
            )
            keep = entries[:MAX_POOL]
            removed = len(entries) - len(keep)
            self._entries = {e.url: e for e in keep}
            self._dirty = True
        if removed:
            logger.info(f"proxy_manager: trimmed {removed} proxies (pool cap {MAX_POOL})")

    def alive_count(self) -> int:
        with self._lock:
            return sum(1 for e in self._entries.values() if e.state == ST_ALIVE)

    def usable_count(self) -> int:
        """ALIVE + NEW — everything a check may pick right now."""
        with self._lock:
            return sum(1 for e in self._entries.values() if e.state != ST_DEAD)

    # ── Source fetching ───────────────────────────────────────────────────

    def _parse_source_text(self, text: str) -> list[str]:
        """Parse a source payload — plain text lines or Geonode-style JSON."""
        urls: list[str] = []
        stripped = text.lstrip()
        if stripped.startswith("{"):
            try:
                data = json.loads(stripped)
                for item in data.get("data", []):
                    ip, port = item.get("ip"), item.get("port")
                    if ip and port:
                        u = normalize_proxy_url(f"{ip}:{port}")
                        if u:
                            urls.append(u)
                return urls
            except Exception:
                pass
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "://" in line and not line.startswith(("http://", "https://", "socks5://", "socks4://")):
                continue
            # strip inline junk ("1.2.3.4:8080 junk", "http://1.2.3.4:8080,")
            token = line.split()[0].rstrip(",;")
            u = normalize_proxy_url(token)
            if u:
                urls.append(u)
        return urls

    def _fetch_source(self, url: str) -> tuple[list[str], str]:
        try:
            resp = requests.get(url, timeout=_FETCH_TIMEOUT, headers=_FETCH_HEADERS)
            resp.raise_for_status()
            return self._parse_source_text(resp.text), ""
        except Exception as exc:
            return [], str(exc)

    def fetch_from_url(self, url: str) -> tuple[int, int, str]:
        """Fetch one source, merge into pool. Returns (added, skipped, error)."""
        parsed, err = self._fetch_source(url)
        if err:
            return 0, 0, err
        if not parsed:
            return 0, len(parsed), ""
        added = 0
        skipped = 0
        now = time.time()
        with self._lock:
            for u in parsed:
                if u in self._entries:
                    skipped += 1
                    continue
                self._entries[u] = ProxyEntry(u, state=ST_NEW, added_at=now)
                added += 1
            self._dirty = True
        if added:
            logger.info(f"proxy_manager: +{added} new proxies from {url[:60]}")
        return added, skipped, ""

    def _refresh_from_all_sources(self) -> None:
        """Fetch ALL sources in parallel, merge, then trim. Runs in bg thread."""
        if self._refreshing:
            return
        self._refreshing = True
        try:
            sources = self.list_sources()
            if not sources:
                return
            from concurrent.futures import ThreadPoolExecutor
            added_total = 0
            skipped_total = 0
            errors = 0
            with ThreadPoolExecutor(max_workers=min(9, len(sources)),
                                    thread_name_prefix="pfetch") as pool:
                results = list(pool.map(self.fetch_from_url, sources))
            for added, skipped, err in results:
                added_total += added
                skipped_total += skipped
                if err:
                    errors += 1
            self._last_refresh_ts = time.time()
            self._last_refresh_added = added_total
            self._last_refresh_sources_ok = len(sources) - errors
            self._last_refresh_sources_total = len(sources)
            with self._lock:
                dead = sum(1 for e in self._entries.values() if e.state == ST_DEAD)
            self._last_refresh_dead = dead
            if added_total:
                logger.info(
                    f"proxy_manager: refresh +{added_total} proxies "
                    f"({self._last_refresh_sources_ok}/{len(sources)} sources OK)"
                )
        finally:
            self._refreshing = False

    def refresh_all_sources(self) -> tuple[int, int, list[str]]:
        """Synchronous refresh (admin button). Returns (added, skipped, errors)."""
        sources = self.list_sources()
        total_added = 0
        total_skipped = 0
        errors: list[str] = []
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=min(9, max(1, len(sources))),
                                thread_name_prefix="pfetch") as pool:
            results = list(pool.map(self.fetch_from_url, sources))
        for url, (added, skipped, err) in zip(sources, results):
            total_added += added
            total_skipped += skipped
            if err:
                errors.append(f"{url[:40]}… → {err}")
        self._last_refresh_ts = time.time()
        self._last_refresh_added = total_added
        self._last_refresh_sources_ok = len(sources) - len(errors)
        self._last_refresh_sources_total = len(sources)
        return total_added, total_skipped, errors

    def refresh_now(self) -> None:
        """Non-blocking immediate refresh kick (bypasses cooldown)."""
        self._refresh_cooldown_until = 0.0
        threading.Thread(target=self._refresh_from_all_sources,
                         daemon=True, name="prefresh").start()

    def ensure_pool(self) -> None:
        """
        Called by the checker when the pool looks empty — triggers an urgent
        refill in the background (rate-limited by a 60s cooldown).
        """
        if not self.enabled:
            return
        now = time.time()
        if now < self._refresh_cooldown_until or self._refreshing:
            return
        if self.usable_count() >= POOL_MIN:
            return
        self._refresh_cooldown_until = now + 60
        logger.warning("proxy_manager: pool low — urgent source refresh started")
        self.refresh_now()

    # ── In-check feedback (the real screening) ────────────────────────────

    def mark_failure(self, url: str, hard: bool = False) -> None:
        """
        Record a failure. hard=True → removed instantly (429 / timeout /
        login-page / connection error). hard=False → after _MAX_FAILURES.
        """
        if not url:
            return
        with self._lock:
            e = self._entries.get(url)
            if not e:
                return
            e.fail_count += 1
            e.last_fail = time.time()
            if hard or e.fail_count >= _MAX_FAILURES:
                self._entries.pop(url, None)   # dead → gone immediately
            self._dirty = True

    def mark_success(self, url: str, latency: float | None = None) -> None:
        """Alive + EMA latency update — this proxy just proved itself on a real check."""
        if not url:
            return
        with self._lock:
            e = self._entries.get(url)
            if not e:
                # Proxy was added by another worker meanwhile — track it anyway
                e = ProxyEntry(url, state=ST_ALIVE)
                self._entries[url] = e
            e.state = ST_ALIVE
            e.fail_count = 0
            e.last_ok = time.time()
            if latency is not None and latency > 0:
                if e.avg_latency:
                    e.avg_latency = _EMA_ALPHA * latency + (1 - _EMA_ALPHA) * e.avg_latency
                else:
                    e.avg_latency = latency
            self._checks_served += 1
            self._dirty = True

    def note_direct_fallback(self) -> None:
        self._direct_fallbacks += 1

    # ── Persistence ───────────────────────────────────────────────────────

    def _flush(self) -> None:
        with self._lock:
            self._dirty = False
            snapshot = list(self._entries.values())[:_PERSIST_LIMIT]
        try:
            now = time.time()
            with _get_conn() as conn:
                conn.execute("DELETE FROM proxies")
                conn.executemany(
                    "INSERT OR REPLACE INTO proxies "
                    "(url, added_at, fail_count, last_fail, last_ok, avg_latency, state) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    [
                        (e.url, e.added_at, e.fail_count, e.last_fail, e.last_ok,
                         e.avg_latency, e.state)
                        for e in snapshot
                    ],
                )
            del now
        except Exception as exc:
            logger.warning(f"proxy_manager: flush failed: {exc}")

    def flush(self) -> None:
        self._flush()

    # ── Properties / toggles ──────────────────────────────────────────────

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._entries)

    @property
    def available_count(self) -> int:
        return self.usable_count()

    @property
    def enabled(self) -> bool:
        try:
            with _get_conn() as conn:
                row = conn.execute(
                    "SELECT value FROM config WHERE key='enabled'"
                ).fetchone()
            return bool(int(row[0])) if row else False
        except Exception:
            return False

    def _set_config(self, key: str, val: str) -> None:
        with _get_conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (key, val)
            )

    def toggle(self) -> bool:
        new_val = not self.enabled
        self._set_config("enabled", "1" if new_val else "0")
        if new_val and self.usable_count() < POOL_MIN:
            self.ensure_pool()
        return new_val

    def set_enabled(self, val: bool) -> None:
        self._set_config("enabled", "1" if val else "0")

    # ── Manual proxy add / remove ─────────────────────────────────────────

    def add_proxy_raw(self, raw: str) -> tuple[bool, str]:
        normalized = normalize_proxy_url(raw)
        if not normalized:
            return False, (
                "Could not parse that as a proxy. Accepted formats:\n"
                "  • <code>host:port</code>\n"
                "  • <code>host:port:user:pass</code>\n"
                "  • <code>user:pass@host:port</code>\n"
                "  • <code>http://user:pass@host:port</code>\n"
                "  • <code>socks5://host:port</code>"
            )
        with self._lock:
            if normalized in self._entries:
                return False, f"Already in list: <code>{normalized}</code>"
            self._entries[normalized] = ProxyEntry(normalized, state=ST_ALIVE)
            self._dirty = True
        return True, normalized

    def remove_proxy(self, index: int) -> str | None:
        with self._lock:
            urls = list(self._entries.keys())
            if 0 <= index < len(urls):
                url = urls[index]
                self._entries.pop(url, None)
                self._dirty = True
                return url
        return None

    def clear_proxies(self) -> int:
        with self._lock:
            n = len(self._entries)
            self._entries.clear()
            self._dirty = True
        try:
            with _get_conn() as conn:
                conn.execute("DELETE FROM proxies")
        except Exception:
            pass
        return n

    def list_proxies(self) -> list[str]:
        with self._lock:
            entries = sorted(
                self._entries.values(),
                key=lambda e: (e.state != ST_ALIVE,
                               e.avg_latency if e.avg_latency else 9e9),
            )
        return [e.url for e in entries]

    def proxies_as_text(self) -> str:
        return "\n".join(self.list_proxies())

    # ── Source URL management ─────────────────────────────────────────────

    def add_source(self, url: str) -> tuple[bool, str]:
        url = url.strip()
        if not url.startswith(("http://", "https://")):
            return False, "Source URL must start with http:// or https://"
        with _get_conn() as conn:
            existing = conn.execute(
                "SELECT url FROM sources WHERE url=?", (url,)
            ).fetchone()
            if existing:
                return False, "Source URL already saved."
            conn.execute(
                "INSERT INTO sources (url, added_at) VALUES (?, ?)",
                (url, time.time()),
            )
        return True, url

    def remove_source(self, index: int) -> str | None:
        with _get_conn() as conn:
            rows = conn.execute(
                "SELECT url FROM sources ORDER BY added_at ASC"
            ).fetchall()
        if 0 <= index < len(rows):
            url = rows[index][0]
            with _get_conn() as conn:
                conn.execute("DELETE FROM sources WHERE url=?", (url,))
            return url
        return None

    def list_sources(self) -> list[str]:
        try:
            with _get_conn() as conn:
                rows = conn.execute(
                    "SELECT url FROM sources ORDER BY added_at ASC"
                ).fetchall()
            return [r[0] for r in rows]
        except Exception:
            return []

    # ── Runtime selection (used by checker.py) ────────────────────────────

    def _pick(self, n: int) -> list[str]:
        """
        Rotating selection over the usable pool:
          1. ALIVE proxies ordered by EMA latency (fastest first), rotating window
          2. Exploration slots from NEW (unscreened) proxies — random
          3. Random fill from anything usable
        """
        with self._lock:
            alive: list[ProxyEntry] = []
            fresh: list[ProxyEntry] = []
            for e in self._entries.values():
                if e.state == ST_ALIVE:
                    alive.append(e)
                elif e.state == ST_NEW:
                    fresh.append(e)

            result: list[str] = []
            seen: set[str] = set()

            # 1. rotating window over latency-sorted alive proxies
            if alive:
                alive.sort(key=lambda e: (e.avg_latency if e.avg_latency else 9e9))
                exploit_n = max(1, n - 1) if fresh else n
                pool_size = len(alive)
                offset = self._rot_idx % pool_size
                self._rot_idx = (self._rot_idx + max(1, exploit_n)) % pool_size
                for i in range(min(exploit_n, pool_size)):
                    u = alive[(offset + i) % pool_size].url
                    if u not in seen:
                        result.append(u)
                        seen.add(u)
                        if len(result) >= exploit_n:
                            break

            # 2. exploration — random NEW proxies (used immediately, no wait)
            random.shuffle(fresh)
            for e in fresh:
                if len(result) >= n:
                    break
                if e.url not in seen:
                    result.append(e.url)
                    seen.add(e.url)

            # 3. random fill from anything usable
            if len(result) < n:
                others = [e.url for e in alive + fresh if e.url not in seen]
                random.shuffle(others)
                result.extend(others[: n - len(result)])

            return result

    def get_proxies_dict(self) -> dict | None:
        """Single rotating proxy dict (round-robin within the pool)."""
        if not self.enabled:
            return None
        urls = self._pick(1)
        if not urls:
            self.ensure_pool()
            return None
        return {"http": urls[0], "https": urls[0]}

    def get_top_proxies_dicts(self, n: int) -> list[dict]:
        """Up to n rotating proxy dicts for racing (true pool rotation)."""
        if not self.enabled or n <= 0:
            return []
        urls = self._pick(n)
        if not urls:
            self.ensure_pool()
        return [{"http": u, "https": u} for u in urls]

    # ── Password-change proxy setting ─────────────────────────────────────

    @property
    def changepw_proxy_enabled(self) -> bool:
        try:
            with _get_conn() as conn:
                row = conn.execute(
                    "SELECT value FROM config WHERE key='changepw_proxy'"
                ).fetchone()
            return bool(int(row[0])) if row else False
        except Exception:
            return False

    def toggle_changepw_proxy(self) -> bool:
        new_val = not self.changepw_proxy_enabled
        self._set_config("changepw_proxy", "1" if new_val else "0")
        return new_val

    def set_changepw_proxy_enabled(self, val: bool) -> None:
        self._set_config("changepw_proxy", "1" if val else "0")

    def get_changepw_proxy_dict(self) -> dict | None:
        if not self.changepw_proxy_enabled:
            return None
        return self.get_proxies_dict()

    # ── Stats / display ───────────────────────────────────────────────────

    def pool_stats(self) -> dict:
        with self._lock:
            total = len(self._entries)
            alive = sum(1 for e in self._entries.values() if e.state == ST_ALIVE)
            fresh = sum(1 for e in self._entries.values() if e.state == ST_NEW)
            scored = sum(1 for e in self._entries.values() if e.avg_latency)
        return {
            "total": total,
            "alive": alive,
            "new": fresh,
            "scored": scored,
            "sources": len(self.list_sources()),
            "last_refresh": self._last_refresh_ts,
            "last_refresh_added": self._last_refresh_added,
            "sources_ok": self._last_refresh_sources_ok,
            "sources_total": self._last_refresh_sources_total,
            "checks_served": self._checks_served,
            "direct_fallbacks": self._direct_fallbacks,
            "refreshing": self._refreshing,
        }

    def status_text(self) -> str:
        st = self.pool_stats()
        src_note = f" · {st['sources']} sources" if st["sources"] else ""
        if not self.enabled:
            if st["total"] == 0:
                return f"🔴 <b>OFF</b> — direct connection  (no proxies stored{src_note})"
            return f"🔴 <b>OFF</b> — direct connection  ({st['total']} proxies stored{src_note})"
        if st["total"] == 0:
            return f"🟡 <b>ON</b> — pool empty, refilling from sources{src_note}"
        return (
            f"🟢 <b>ON</b> — {st['alive']} alive · {st['new']} new · "
            f"{st['total']} total{src_note}"
        )


proxy_manager = ProxyManager()
