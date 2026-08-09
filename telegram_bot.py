"""Optional Telegram Bot API 9.x dashboard for :mod:`itza`.

This adapter deliberately uses the HTTPS Bot API directly (no outdated
framework assumptions), HTML formatting, and inline keyboards.  It is an
operator UI for the queue runner; the runner remains usable without Telegram.

Environment variables:
    ITZA_BOT_TOKEN   Telegram bot token (required)
    ITZA_ADMIN_IDS   comma-separated Telegram user IDs (optional)
    ITZA_EMAIL / ITZA_PASSWORD
    ITZA_STYLE        modern, neon, compact, plain
    ITZA_WORKERS      active queue workers (1-32)

Run with ``python telegram_bot.py``.
"""
from __future__ import annotations

import html
import json
import logging
import os
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from http.server import HTTPServer, BaseHTTPRequestHandler

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from itza import ITZAQuizClient, THEMES
from itza_loop import ITZAQuizClient as LoopClient


LOG = logging.getLogger("itza.telegram")


# ── Health Check Server ──────────────────────────────────────────────

class HealthCheckHandler(BaseHTTPRequestHandler):
    """Simple HTTP handler for Render.com health checks and ping endpoints."""
    def do_GET(self):
        if self.path in ("/", "/health", "/healthz", "/ping"):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status":"ok","message":"Bot is running"}')
        else:
            self.send_response(404)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Not Found")

    def log_message(self, format, *args):
        # Override to prevent excessive access logs from health pings
        pass


def start_health_server(port: int | None = None) -> HTTPServer | None:
    """Start standard HTTP server for Render health check in a background daemon thread."""
    if port is None:
        port_str = os.environ.get("PORT", "8080").strip()
        port = int(port_str) if port_str.isdigit() else 8080
    try:
        server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True, name="render-health")
        thread.start()
        LOG.info("Render health check server running on http://0.0.0.0:%d/health", port)
        return server
    except Exception as exc:
        LOG.warning("Failed to start health check server on port %d: %s", port, exc)
        return None


def start_keep_alive():
    """Background thread to ping external URL periodically (keeps Render free plan awake)."""
    url = os.environ.get("RENDER_EXTERNAL_URL") or os.environ.get("PING_URL")
    if not url:
        return
    if not url.endswith("/health"):
        url = url.rstrip("/") + "/health"

    def pinger():
        LOG.info("Render keep-alive ping loop active for %s", url)
        while True:
            time.sleep(600)  # Ping every 10 minutes
            try:
                resp = requests.get(url, timeout=10)
                LOG.debug("Keep-alive ping response: %s", resp.status_code)
            except Exception as exc:
                LOG.warning("Keep-alive ping failed: %s", exc)

    thread = threading.Thread(target=pinger, daemon=True, name="render-pinger")
    thread.start()


# ── Styles ───────────────────────────────────────────────────────────

@dataclass(frozen=True)
class TelegramStyle:
    name: str
    icon: str
    accent: str
    parse_mode: str = "HTML"


BOT_STYLES = {
    "modern": TelegramStyle("modern", "✨", "<b>"),
    "neon": TelegramStyle("neon", "⚡", "<b>"),
    "compact": TelegramStyle("compact", "▸", "<b>"),
    "plain": TelegramStyle("plain", "•", ""),
}


# ── Account Storage ──────────────────────────────────────────────────

@dataclass(frozen=True)
class AccountSpec:
    key: str
    email: str
    password: str


def load_accounts(path="", email="", password=""):
    """Load account records without printing or exposing passwords.

    The JSON format is ``[{"key":"main", "email":"...", "password":"..."}]``.
    An environment-variable account is used when no file is configured or missing.
    """
    if path and os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as fh:
                records = json.load(fh)
            if isinstance(records, list):
                result = []
                for i, item in enumerate(records, 1):
                    if isinstance(item, dict) and item.get("email") and item.get("password"):
                        result.append(AccountSpec(str(item.get("key") or i),
                                                  str(item["email"]), str(item["password"])))
                if result:
                    return result
        except Exception as exc:
            LOG.warning("Failed to load accounts from %s: %s", path, exc)
    return [AccountSpec("default", email, password)] if email and password else []


def save_accounts(path, accounts):
    """Save account list to JSON file."""
    if not path:
        path = "accounts.json"
    data = [{"key": a.key, "email": a.email, "password": a.password} for a in accounts]
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2)


def esc(value) -> str:
    """Escape arbitrary account/quiz text for Telegram HTML parse mode."""
    return html.escape(str(value or ""), quote=False)


# ── Run State Tracking ───────────────────────────────────────────────

@dataclass
class RunState:
    """Tracks the live state of a running queue for real-time status queries."""
    status: str = "idle"            # idle | running | finished | failed
    current_quiz: str = ""          # Name of quiz currently being processed
    completed: int = 0
    failed: int = 0
    total: int = 0
    started_at: float = 0.0
    yakka_start: int | None = None
    yakka_current: int | None = None
    yakka_earned: int = 0
    errors: list = field(default_factory=list)
    account_key: str = ""


@dataclass
class LastRun:
    """Stores the result of the most recent completed run."""
    timestamp: float = 0.0
    account_key: str = ""
    completed: int = 0
    failed: int = 0
    total: int = 0
    yakka_start: int | None = None
    yakka_end: int | None = None
    yakka_earned: int = 0
    elapsed: float = 0.0
    scope: str = ""


@dataclass
class LoopState:
    """Tracks the state of the continuous mining loop."""
    running: bool = False
    loop_count: int = 0
    total_earned: int = 0
    started_at: float = 0.0
    current_loop_ok: int = 0
    current_loop_fail: int = 0
    current_quiz: str = ""
    total_quizzes: int = 0
    account_key: str = ""
    last_balance: int | None = None


# ── Telegram API ─────────────────────────────────────────────────────

class TelegramAPI:
    """Small, thread-safe Bot API client with retry/backoff."""
    def __init__(self, token: str, timeout: int = 35):
        self.base = f"https://api.telegram.org/bot{token}"
        self.timeout = timeout
        self._local = threading.local()

    def _session(self):
        session = getattr(self._local, "session", None)
        if session is None:
            retry = Retry(total=4, connect=4, read=4, backoff_factor=.5,
                          status_forcelist=(429, 500, 502, 503, 504),
                          allowed_methods=frozenset({"GET", "POST"}),
                          respect_retry_after_header=True)
            session = requests.Session()
            adapter = HTTPAdapter(max_retries=retry, pool_connections=16, pool_maxsize=16)
            session.mount("https://", adapter)
            self._local.session = session
        return session

    def call(self, method: str, **payload):
        payload = {k: v for k, v in payload.items() if v is not None}
        response = self._session().post(f"{self.base}/{method}", json=payload,
                                        timeout=self.timeout)
        response.raise_for_status()
        data = response.json()
        return data.get("result")


# ── Progress Bar Helper ──────────────────────────────────────────────

def progress_bar(done: int, total: int, width: int = 20) -> str:
    """Render a text progress bar like [████████░░░░░░░░░░░░] 40%"""
    if total <= 0:
        return "[" + "░" * width + "] 0%"
    ratio = min(done / total, 1.0)
    filled = int(width * ratio)
    bar = "█" * filled + "░" * (width - filled)
    pct = int(ratio * 100)
    return f"[{bar}] {pct}%"


def fmt_elapsed(seconds: float) -> str:
    """Format elapsed seconds as Xm Ys or Xs."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    m, s = divmod(int(seconds), 60)
    return f"{m}m {s}s"


# ── Main Bot ─────────────────────────────────────────────────────────

class TelegramBot:
    def __init__(self, token: str, *, style="modern", email="", password="",
                 workers=1, admin_ids=(), accounts=None):
        if style not in BOT_STYLES:
            raise ValueError(f"unknown style: {style}")
        self.api = TelegramAPI(token)
        self.style = style
        self.accounts_file = os.environ.get("ITZA_ACCOUNTS_FILE", "").strip() or "accounts.json"
        self.accounts = list(accounts or load_accounts(self.accounts_file, email=email, password=password))
        self.selected_account = self.accounts[0].key if self.accounts else ""
        self.workers = max(1, min(int(workers), 32))
        self.admin_ids = {int(x) for x in admin_ids if str(x).strip().lstrip("-").isdigit()}
        self.offset = 0
        self.started = time.time()
        self.job_lock = threading.Lock()
        self.job = None
        self.pending_states = {}

        # Live run tracking
        self.run_state = RunState()
        self.last_run = None  # LastRun or None
        self._active_client = None  # ITZAQuizClient used by current/last run
        self._active_client_lock = threading.Lock()

        # Continuous mining loop
        self.loop_state = LoopState()
        self.loop_thread = None  # Thread for the continuous loop
        self._loop_stop_event = threading.Event()  # Signal to stop the loop

    @property
    def theme(self):
        return BOT_STYLES[self.style]

    def add_account(self, email: str, password: str, key: str | None = None) -> AccountSpec:
        """Add or update an account specification and save to disk."""
        email = email.strip()
        password = password.strip()
        key = (key or email).strip()
        existing = next((a for a in self.accounts if a.key == key or a.email == email), None)
        if existing:
            self.accounts.remove(existing)
        spec = AccountSpec(key=key, email=email, password=password)
        self.accounts.append(spec)
        self.selected_account = spec.key
        save_accounts(self.accounts_file, self.accounts)
        return spec

    def delete_account(self, key: str) -> bool:
        """Delete an account specification and save to disk."""
        target = next((a for a in self.accounts if a.key == key), None)
        if target:
            self.accounts.remove(target)
            if self.selected_account == key:
                self.selected_account = self.accounts[0].key if self.accounts else ""
            save_accounts(self.accounts_file, self.accounts)
            return True
        return False

    # ── Keyboards ────────────────────────────────────────────────────

    def markup(self):
        return {"inline_keyboard": [
            [{"text": "🔄 Start Loop", "callback_data": "startloop"},
             {"text": "⏹ Stop Loop", "callback_data": "stoploop"}],
            [{"text": "▶ Run queue", "callback_data": "run"},
             {"text": "▶ Run all", "callback_data": "runall"}],
            [{"text": "📊 Status", "callback_data": "status"},
             {"text": "💰 Balance", "callback_data": "balance"}],
            [{"text": "👤 Accounts", "callback_data": "accounts"},
             {"text": "🔍 Check", "callback_data": "check"}],
            [{"text": "➕ Add Account", "callback_data": "add_account"},
             {"text": "🎨 Style", "callback_data": "styles"}],
            [{"text": "❔ Help", "callback_data": "help"}],
        ]}

    def styles_markup(self):
        return {"inline_keyboard": [[
            {"text": ("✅ " if name == self.style else "") + name.title(),
             "callback_data": f"style:{name}"}
            for name in BOT_STYLES
        ], [{"text": "‹ Back", "callback_data": "home"}]]}

    def accounts_markup(self):
        rows = [[{"text": "➕ Add New Account", "callback_data": "add_account"}]]
        for account in self.accounts[:100]:
            rows.append([{"text": ("✅ " if account.key == self.selected_account else "") + account.key,
                          "callback_data": f"account:{account.key}"}])
        rows.append([{"text": "‹ Back", "callback_data": "home"}])
        return {"inline_keyboard": rows}

    # ── Dashboard ────────────────────────────────────────────────────

    def dashboard(self):
        t = self.theme
        rs = self.run_state

        # Job status
        with self.job_lock:
            job = self.job
        if job and job.is_alive():
            state = "🟢 <b>RUNNING</b>"
        elif rs.status == "finished":
            state = "✅ <b>finished</b>"
        elif rs.status == "failed":
            state = "❌ <b>failed</b>"
        else:
            state = "⚪ <b>idle</b>"

        # Check loop state
        if self.loop_state.running:
            state = "🔄 <b>LOOP MINING</b>"

        lines = [
            f"{t.icon} <b>ITZA Queue Runner</b>",
            "",
            f"Status: {state}",
            f"Account: <code>{esc(self.selected_account or 'none')}</code> "
            f"({len(self.accounts):,} configured)",
            f"Style: <code>{esc(self.style)}</code> · Workers: <code>{self.workers}</code>",
            f"Uptime: <code>{fmt_elapsed(time.time() - self.started)}</code>",
        ]

        # Show live balance if we have a client
        balance = self._get_cached_balance()
        if balance is not None:
            lines.append(f"💰 Balance: <code>{balance:,}</code> Yakka")

        # Show live run progress if running
        if job and job.is_alive() and rs.status == "running":
            lines.append("")
            lines.append(f"<b>⏳ Running: {rs.account_key}</b>")
            lines.append(f"<code>{progress_bar(rs.completed + rs.failed, rs.total)}</code>")
            lines.append(f"Progress: <code>{rs.completed + rs.failed}/{rs.total}</code> "
                         f"(✅ {rs.completed} · ❌ {rs.failed})")
            if rs.current_quiz:
                lines.append(f"Current: <code>{esc(rs.current_quiz[:50])}</code>")
            if rs.yakka_earned:
                lines.append(f"Points earned: <code>+{rs.yakka_earned:,}</code> Yakka")
            elapsed = time.time() - rs.started_at if rs.started_at else 0
            if elapsed > 0:
                lines.append(f"Elapsed: <code>{fmt_elapsed(elapsed)}</code>")

        # Show loop progress if loop is running
        if self.loop_state.running:
            ls = self.loop_state
            lines.append("")
            lines.append(f"<b>🔄 Mining Loop Active</b>")
            lines.append(f"Account: <code>{esc(ls.account_key)}</code>")
            lines.append(f"Loop: <code>#{ls.loop_count}</code>")
            if ls.total_quizzes > 0:
                done = ls.current_loop_ok + ls.current_loop_fail
                lines.append(f"<code>{progress_bar(done, ls.total_quizzes)}</code>")
                lines.append(f"Progress: <code>{done}/{ls.total_quizzes}</code> "
                             f"(✅ {ls.current_loop_ok} · ❌ {ls.current_loop_fail})")
            if ls.current_quiz:
                lines.append(f"Current: <code>{esc(ls.current_quiz[:50])}</code>")
            if ls.last_balance is not None:
                lines.append(f"💰 Balance: <code>{ls.last_balance:,}</code>")
            if ls.total_earned > 0:
                lines.append(f"📈 Total earned: <code>+{ls.total_earned:,}</code> Yakka")
            elapsed = time.time() - ls.started_at if ls.started_at else 0
            if elapsed > 0:
                lines.append(f"⏱ Uptime: <code>{fmt_elapsed(elapsed)}</code>")

        # Show last run info
        if self.last_run and not (job and job.is_alive()):
            lr = self.last_run
            lines.append("")
            lines.append("<b>📋 Last Run</b>")
            ago = time.time() - lr.timestamp
            lines.append(f"When: <code>{fmt_elapsed(ago)} ago</code> · Scope: <code>{esc(lr.scope)}</code>")
            lines.append(f"Quizzes: <code>{lr.total}</code> (✅ {lr.completed} · ❌ {lr.failed})")
            if lr.yakka_start is not None and lr.yakka_end is not None:
                delta = lr.yakka_end - lr.yakka_start
                sign = "+" if delta >= 0 else ""
                lines.append(f"Balance: <code>{lr.yakka_start:,}</code> → <code>{lr.yakka_end:,}</code> "
                             f"(<code>{sign}{delta:,}</code>)")
            if lr.elapsed > 0:
                rate = (lr.completed + lr.failed) / lr.elapsed
                lines.append(f"Time: <code>{fmt_elapsed(lr.elapsed)}</code> · "
                             f"Rate: <code>{rate:.1f}</code> q/s")

        return "\n".join(lines)

    def _get_cached_balance(self) -> int | None:
        """Try to get balance from active client without blocking."""
        with self._active_client_lock:
            client = self._active_client
        if client and client.access_token:
            try:
                return client.get_yakka()
            except Exception:
                pass
        return None

    # ── Send/Edit Helpers ────────────────────────────────────────────

    def send(self, chat_id, text, *, markup=None):
        return self.api.call("sendMessage", chat_id=chat_id, text=text,
                             parse_mode=self.theme.parse_mode,
                             link_preview_options={"is_disabled": True},
                             reply_markup=markup)

    def edit(self, chat_id, message_id, text, *, markup=None):
        try:
            return self.api.call("editMessageText", chat_id=chat_id,
                                 message_id=message_id, text=text,
                                 parse_mode=self.theme.parse_mode,
                                 link_preview_options={"is_disabled": True},
                                 reply_markup=markup)
        except Exception:
            # Message might not have changed or was deleted
            return None

    def allowed(self, update):
        if not self.admin_ids:
            return True
        user = (update.get("message") or update.get("callback_query") or {}).get("from") or {}
        return int(user.get("id", 0)) in self.admin_ids

    # ── Balance Command ──────────────────────────────────────────────

    def do_balance(self, chat_id, message_id=None):
        """Fetch and display current Yakka balance for the selected account."""
        selected = next((a for a in self.accounts if a.key == self.selected_account), None)
        if not selected:
            text = "⚠️ <b>No account selected.</b>\nUse /add to configure an account."
            if message_id:
                self.edit(chat_id, message_id, text, markup=self.markup())
            else:
                self.send(chat_id, text, markup=self.markup())
            return

        text = f"⏳ Fetching balance for <code>{esc(selected.key)}</code>..."
        if message_id:
            self.edit(chat_id, message_id, text)
        else:
            msg = self.send(chat_id, text)
            message_id = msg.get("message_id") if msg else None

        try:
            client = ITZAQuizClient(selected.email, selected.password, style=self.style, quiet=True)
            client.login()
            balance = client.get_yakka()
            user_info = client.get_user_info()

            with self._active_client_lock:
                self._active_client = client

            lines = ["💰 <b>ITZA Balance</b>", ""]
            lines.append(f"Account: <code>{esc(selected.key)}</code>")
            if user_info:
                name = user_info.get("name") or user_info.get("username") or ""
                if name:
                    lines.append(f"Name: <code>{esc(name)}</code>")
            if balance is not None:
                lines.append(f"Balance: <code>{balance:,}</code> Yakka 💎")
            else:
                lines.append("Balance: <code>unavailable</code>")
            lines.append(f"\nEmail: <code>{esc(selected.email)}</code>")

            text = "\n".join(lines)
        except Exception as exc:
            text = f"❌ <b>Balance check failed</b>\n<code>{esc(str(exc)[:200])}</code>"

        if message_id:
            self.edit(chat_id, message_id, text, markup=self.markup())
        else:
            self.send(chat_id, text, markup=self.markup())

    # ── Check Command ────────────────────────────────────────────────

    def do_check(self, chat_id, message_id=None):
        """Verify account credentials, show balance and quiz count."""
        selected = next((a for a in self.accounts if a.key == self.selected_account), None)
        if not selected:
            text = "⚠️ <b>No account selected.</b>\nUse /add to configure an account."
            if message_id:
                self.edit(chat_id, message_id, text, markup=self.markup())
            else:
                self.send(chat_id, text, markup=self.markup())
            return

        text = f"🔍 Checking account <code>{esc(selected.key)}</code>...\n(login + quiz discovery, this may take 30-60s)"
        if message_id:
            self.edit(chat_id, message_id, text)
        else:
            msg = self.send(chat_id, text)
            message_id = msg.get("message_id") if msg else None

        def work():
            try:
                client = ITZAQuizClient(selected.email, selected.password,
                                        style=self.style, quiet=True)
                client.login()
                balance = client.get_yakka()
                user_info = client.get_user_info()
                quizzes = client.get_quizzes()

                with self._active_client_lock:
                    self._active_client = client

                lines = ["🔍 <b>Account Check — PASSED ✅</b>", ""]
                lines.append(f"Account: <code>{esc(selected.key)}</code>")
                lines.append(f"Email: <code>{esc(selected.email)}</code>")
                if user_info:
                    name = user_info.get("name") or user_info.get("username") or ""
                    if name:
                        lines.append(f"Name: <code>{esc(name)}</code>")
                lines.append(f"\n✅ Login: <b>SUCCESS</b>")
                if balance is not None:
                    lines.append(f"💰 Balance: <code>{balance:,}</code> Yakka")
                else:
                    lines.append(f"💰 Balance: <code>unavailable</code>")
                lines.append(f"📚 Available quizzes: <code>{len(quizzes):,}</code>")
                lines.append(f"\n<i>Everything is working! Use ▶ Run queue to start.</i>")

                text = "\n".join(lines)
            except Exception as exc:
                text = (f"🔍 <b>Account Check — FAILED ❌</b>\n\n"
                        f"Account: <code>{esc(selected.key)}</code>\n"
                        f"Error: <code>{esc(str(exc)[:300])}</code>\n\n"
                        f"<i>Check your email/password and try again.</i>")

            if message_id:
                self.edit(chat_id, message_id, text, markup=self.markup())
            else:
                self.send(chat_id, text, markup=self.markup())

        # Run in background thread so it doesn't block the polling loop
        threading.Thread(target=work, name="telegram-check", daemon=True).start()

    # ── Queue Runner ─────────────────────────────────────────────────

    def run_queue(self, chat_id, message_id=None, *, all_accounts=False):
        if self.loop_state.running:
            text = "⚠️ <b>A mining loop is running!</b>\nStop it first with ⏹ Stop Loop before running a one-time queue."
            if message_id:
                self.edit(chat_id, message_id, text, markup=self.markup())
            else:
                self.send(chat_id, text, markup=self.markup())
            return False

        with self.job_lock:
            if self.job and self.job.is_alive():
                text = ("⚠️ <b>A job is already running!</b>\n\n"
                        f"<code>{progress_bar(self.run_state.completed + self.run_state.failed, self.run_state.total)}</code>\n"
                        f"Progress: {self.run_state.completed + self.run_state.failed}/{self.run_state.total}\n"
                        f"Use 📊 Status to check progress.")
                if message_id:
                    self.edit(chat_id, message_id, text, markup=self.markup())
                else:
                    self.send(chat_id, text, markup=self.markup())
                return False

            selected = next((a for a in self.accounts if a.key == self.selected_account), None)
            targets = self.accounts if all_accounts else ([selected] if selected else [])
            if not targets:
                no_acc_markup = {"inline_keyboard": [
                    [{"text": "➕ Add Account", "callback_data": "add_account"}],
                    [{"text": "‹ Back", "callback_data": "home"}]
                ]}
                self.send(chat_id, "⚠️ <b>No account configured.</b>\n\n"
                          "Use <b>/add</b> or click below to enter your ITZA email & password.",
                          markup=no_acc_markup)
                return False

            # Initialize run state
            self.run_state = RunState(
                status="running",
                started_at=time.time(),
                account_key=", ".join(t.key for t in targets),
            )

            def work():
                total_ok = 0
                total_fail = 0
                total_quizzes = 0
                account_errors = []
                all_yakka_start = None
                all_yakka_end = None

                for acc_idx, account in enumerate(targets):
                    try:
                        # Login
                        self.run_state.current_quiz = f"Logging in ({account.key})..."
                        client = ITZAQuizClient(account.email, account.password,
                                                style=self.style, quiet=True)
                        client.login()

                        with self._active_client_lock:
                            self._active_client = client

                        # Get starting balance
                        yakka_before = client.get_yakka()
                        if all_yakka_start is None and yakka_before is not None:
                            all_yakka_start = yakka_before
                        self.run_state.yakka_start = yakka_before

                        # Discover quizzes
                        self.run_state.current_quiz = f"Discovering quizzes ({account.key})..."
                        quizzes = client.get_quizzes()

                        if not quizzes:
                            self.send(chat_id,
                                      f"⚠️ No quizzes found for <code>{esc(account.key)}</code>.")
                            continue

                        self.run_state.total += len(quizzes)
                        total_quizzes += len(quizzes)

                        # Send initial progress message
                        progress_msg = self.send(chat_id,
                            f"▶ <b>Started: {esc(account.key)}</b>\n"
                            f"Found <code>{len(quizzes):,}</code> quizzes\n"
                            f"💰 Starting balance: <code>{yakka_before if yakka_before is not None else '?':,}</code> Yakka\n"
                            f"<code>{progress_bar(0, len(quizzes))}</code>\n"
                            f"Processing...")
                        prog_mid = progress_msg.get("message_id") if progress_msg else None

                        # Process quizzes one by one
                        last_update_time = time.time()
                        acc_ok = 0
                        acc_fail = 0
                        yakka_earned_this_acc = 0

                        for i, lo in enumerate(quizzes, 1):
                            quiz_name = (lo.get("lo_name") or lo.get("quiz_id") or "?")[:50]
                            self.run_state.current_quiz = quiz_name

                            # Get balance before quiz
                            yakka_pre = None
                            if i % 5 == 1 or i <= 3:  # Check every 5th quiz or first 3
                                try:
                                    yakka_pre = client.get_yakka()
                                except Exception:
                                    pass

                            # Complete the quiz
                            try:
                                success, detail, stats = client.complete_quiz(lo)
                            except Exception as exc:
                                success, detail, stats = False, f"error: {exc}", {}

                            if success:
                                acc_ok += 1
                                self.run_state.completed += 1
                            else:
                                acc_fail += 1
                                self.run_state.failed += 1
                                if len(self.run_state.errors) < 20:
                                    self.run_state.errors.append(f"{quiz_name}: {detail}")

                            # Check balance after quiz for delta
                            yakka_post = None
                            if yakka_pre is not None:
                                try:
                                    yakka_post = client.get_yakka()
                                    if yakka_post is not None and yakka_pre is not None:
                                        delta = yakka_post - yakka_pre
                                        if delta > 0:
                                            yakka_earned_this_acc += delta
                                            self.run_state.yakka_earned += delta
                                except Exception:
                                    pass

                            self.run_state.yakka_current = yakka_post or yakka_pre

                            # Update progress message (throttled: every 5 quizzes or 10 seconds)
                            now = time.time()
                            should_update = (
                                i == len(quizzes) or  # Last quiz
                                i % 5 == 0 or         # Every 5th
                                i <= 3 or              # First 3
                                (now - last_update_time) > 10  # Every 10 seconds
                            )

                            if should_update and prog_mid:
                                last_update_time = now
                                done = acc_ok + acc_fail
                                elapsed = now - self.run_state.started_at
                                result_icon = "✅" if success else "❌"

                                update_lines = [
                                    f"▶ <b>Running: {esc(account.key)}</b>",
                                    f"<code>{progress_bar(done, len(quizzes))}</code>",
                                    f"Progress: <code>{done}/{len(quizzes)}</code> "
                                    f"(✅ {acc_ok} · ❌ {acc_fail})",
                                    "",
                                    f"{result_icon} <code>{esc(quiz_name)}</code>",
                                ]
                                q_count = stats.get("questions", 0)
                                correct = stats.get("correct", 0)
                                if q_count:
                                    update_lines.append(
                                        f"   Questions: {q_count} · Correct: {correct}")

                                if yakka_earned_this_acc > 0:
                                    update_lines.append(
                                        f"\n💰 Points earned: <code>+{yakka_earned_this_acc:,}</code> Yakka")
                                if self.run_state.yakka_current is not None:
                                    update_lines.append(
                                        f"💎 Current balance: <code>{self.run_state.yakka_current:,}</code>")

                                update_lines.append(f"\n⏱ Elapsed: <code>{fmt_elapsed(elapsed)}</code>")

                                try:
                                    self.edit(chat_id, prog_mid,
                                              "\n".join(update_lines))
                                except Exception:
                                    pass

                            # Small delay between quizzes
                            if i < len(quizzes):
                                time.sleep(random.uniform(0.2, 1.0))

                        total_ok += acc_ok
                        total_fail += acc_fail

                        # Get final balance for this account
                        try:
                            yakka_after = client.get_yakka()
                            all_yakka_end = yakka_after
                        except Exception:
                            yakka_after = None

                    except Exception as exc:
                        account_errors.append(f"{account.key}: {exc}")
                        LOG.exception("Account %s failed", account.key)

                # ── Final Summary ────────────────────────────────────
                self.run_state.status = "finished"
                self.run_state.current_quiz = ""
                elapsed = time.time() - self.run_state.started_at

                # Store last run
                self.last_run = LastRun(
                    timestamp=time.time(),
                    account_key=", ".join(t.key for t in targets),
                    completed=total_ok,
                    failed=total_fail,
                    total=total_quizzes,
                    yakka_start=all_yakka_start,
                    yakka_end=all_yakka_end,
                    yakka_earned=self.run_state.yakka_earned,
                    elapsed=elapsed,
                    scope="all accounts" if all_accounts else targets[0].key,
                )

                # Build summary message
                summary = [
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                    "✅ <b>QUEUE COMPLETE</b>",
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                    "",
                    f"📊 <b>Results</b>",
                    f"Accounts: <code>{len(targets):,}</code>",
                    f"Total quizzes: <code>{total_quizzes:,}</code>",
                    f"Completed: <code>{total_ok:,}</code> ✅",
                    f"Failed: <code>{total_fail:,}</code> ❌",
                ]

                if all_yakka_start is not None and all_yakka_end is not None:
                    delta = all_yakka_end - all_yakka_start
                    sign = "+" if delta >= 0 else ""
                    summary.append("")
                    summary.append(f"💰 <b>Points</b>")
                    summary.append(f"Before: <code>{all_yakka_start:,}</code> Yakka")
                    summary.append(f"After: <code>{all_yakka_end:,}</code> Yakka")
                    summary.append(f"Earned: <code>{sign}{delta:,}</code> Yakka {'🎉' if delta > 0 else ''}")

                if elapsed > 0:
                    rate = total_quizzes / elapsed if elapsed else 0
                    summary.append("")
                    summary.append(f"⏱ <b>Performance</b>")
                    summary.append(f"Time: <code>{fmt_elapsed(elapsed)}</code>")
                    summary.append(f"Speed: <code>{rate:.2f}</code> quizzes/sec")

                if account_errors:
                    summary.append("")
                    summary.append(f"⚠️ <b>Account Errors ({len(account_errors)})</b>")
                    for err in account_errors[:5]:
                        summary.append(f"  • <code>{esc(str(err)[:100])}</code>")

                if self.run_state.errors:
                    summary.append("")
                    summary.append(f"❌ <b>Quiz Errors (showing {min(len(self.run_state.errors), 5)}/{len(self.run_state.errors)})</b>")
                    for err in self.run_state.errors[:5]:
                        summary.append(f"  • <code>{esc(str(err)[:80])}</code>")

                summary.append("")
                summary.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

                self.send(chat_id, "\n".join(summary), markup=self.markup())

            self.job = threading.Thread(target=work, name="telegram-queue", daemon=True)
            self.job.start()

        scope = f"all {len(targets):,} accounts" if all_accounts else esc(targets[0].key)
        text = (f"▶ <b>Queue started</b>\n"
                f"Scope: <code>{scope}</code>\n"
                f"Workers: <code>{self.workers}</code>\n\n"
                f"<i>You'll see live progress updates below.\n"
                f"Use 📊 Status to check anytime.</i>")
        if message_id:
            self.edit(chat_id, message_id, text, markup=self.markup())
        else:
            self.send(chat_id, text, markup=self.markup())
        return True

    # ── Continuous Mining Loop ────────────────────────────────────────

    def start_loop(self, chat_id, message_id=None):
        """Start the continuous mining loop."""
        if self.loop_state.running:
            text = "⚠️ <b>Mining loop is already running!</b>\nUse ⏹ Stop Loop or /stoploop to stop it first."
            if message_id:
                self.edit(chat_id, message_id, text, markup=self.markup())
            else:
                self.send(chat_id, text, markup=self.markup())
            return

        with self.job_lock:
            if self.job and self.job.is_alive():
                text = "⚠️ <b>A one-time queue job is running!</b>\nWait for it to finish or restart the bot first."
                if message_id:
                    self.edit(chat_id, message_id, text, markup=self.markup())
                else:
                    self.send(chat_id, text, markup=self.markup())
                return

        selected = next((a for a in self.accounts if a.key == self.selected_account), None)
        if not selected:
            text = "⚠️ <b>No account selected.</b>\nUse /add to configure an account."
            if message_id:
                self.edit(chat_id, message_id, text, markup=self.markup())
            else:
                self.send(chat_id, text, markup=self.markup())
            return

        self._loop_stop_event.clear()
        self.loop_state = LoopState(
            running=True,
            started_at=time.time(),
            account_key=selected.key,
        )

        self.loop_thread = threading.Thread(
            target=self._loop_worker, args=(chat_id, selected),
            name="telegram-loop", daemon=True)
        self.loop_thread.start()

        text = (f"🔄 <b>Mining loop starting...</b>\n"
                f"Account: <code>{esc(selected.key)}</code>\n\n"
                f"<i>Use ⏹ Stop Loop or /stoploop to stop.</i>")
        if message_id:
            self.edit(chat_id, message_id, text, markup=self.markup())
        else:
            self.send(chat_id, text, markup=self.markup())

    def _loop_worker(self, chat_id, account):
        """Background worker that continuously mines quizzes until stopped."""
        try:
            # Login using itza_loop's client
            client = LoopClient(account.email, account.password)
            client.login()

            yakka_start = client.get_yakka()
            self.loop_state.last_balance = yakka_start

            # Send initial message
            self.send(chat_id, f"🔄 <b>Mining Loop Active</b>\n\n"
                      f"Account: <code>{esc(account.key)}</code>\n"
                      f"💰 Starting balance: <code>{yakka_start if yakka_start is not None else '?'}</code> Yakka\n\n"
                      f"<i>Mining will continue until you stop it.\nUse ⏹ Stop Loop or /stoploop</i>")

            loop_num = 0
            while not self._loop_stop_event.is_set():
                loop_num += 1
                self.loop_state.loop_count = loop_num
                self.loop_state.current_loop_ok = 0
                self.loop_state.current_loop_fail = 0

                # Refresh token if needed
                try:
                    client._ensure_token()
                except Exception:
                    # Re-login if token refresh fails
                    try:
                        client.login()
                    except Exception as e:
                        self.send(chat_id, f"❌ <b>Loop Error: Re-login failed</b>\n<code>{esc(str(e)[:200])}</code>")
                        break

                # Discover quizzes
                self.loop_state.current_quiz = "Discovering quizzes..."
                try:
                    quizzes = client.get_quizzes()
                except Exception as e:
                    self.send(chat_id, f"⚠️ Quiz discovery failed, retrying in 30s...\n<code>{esc(str(e)[:100])}</code>")
                    if self._loop_stop_event.wait(30):
                        break
                    continue

                if not quizzes:
                    self.send(chat_id, "⚠️ No quizzes found. Retrying in 60s...")
                    if self._loop_stop_event.wait(60):
                        break
                    continue

                self.loop_state.total_quizzes = len(quizzes)

                # Send loop start notification
                before_balance = client.get_yakka()
                self.loop_state.last_balance = before_balance

                self.send(chat_id, f"🔄 <b>Loop #{loop_num}</b> starting\n"
                          f"📚 Quizzes: <code>{len(quizzes)}</code>\n"
                          f"💰 Balance: <code>{before_balance if before_balance is not None else '?'}</code>")

                # Process all quizzes in this loop pass
                loop_ok = 0
                loop_fail = 0
                loop_earned = 0

                for i, lo in enumerate(quizzes):
                    if self._loop_stop_event.is_set():
                        break

                    quiz_name = (lo.get("lo_name") or lo.get("quiz_id") or "?")[:50]
                    self.loop_state.current_quiz = quiz_name

                    # Get pre-quiz balance every 5th quiz
                    yakka_pre = None
                    if i % 5 == 0:
                        try:
                            yakka_pre = client.get_yakka()
                        except Exception:
                            pass

                    try:
                        success, detail, stats = client.complete_quiz(lo)
                    except Exception as e:
                        success, detail, stats = False, f"error: {e}", {}

                    if success:
                        loop_ok += 1
                    else:
                        loop_fail += 1

                    self.loop_state.current_loop_ok = loop_ok
                    self.loop_state.current_loop_fail = loop_fail

                    # Check balance delta
                    if yakka_pre is not None:
                        try:
                            yakka_post = client.get_yakka()
                            if yakka_post is not None:
                                delta = yakka_post - yakka_pre
                                if delta > 0:
                                    loop_earned += delta
                                self.loop_state.last_balance = yakka_post
                        except Exception:
                            pass

                    # Small delay between quizzes
                    if i < len(quizzes) - 1:
                        delay = 2.0 + random.uniform(0, 1.5)
                        if self._loop_stop_event.wait(delay):
                            break

                self.loop_state.total_earned += loop_earned

                # Loop pass complete summary
                after_balance = client.get_yakka()
                self.loop_state.last_balance = after_balance

                elapsed = time.time() - self.loop_state.started_at
                self.send(chat_id,
                    f"✅ <b>Loop #{loop_num} Complete</b>\n\n"
                    f"Results: ✅ {loop_ok} · ❌ {loop_fail}\n"
                    f"💰 Balance: <code>{after_balance if after_balance is not None else '?'}</code>\n"
                    f"📈 This loop: <code>+{loop_earned}</code> · Total: <code>+{self.loop_state.total_earned}</code>\n"
                    f"⏱ Total uptime: <code>{fmt_elapsed(elapsed)}</code>")

                if self._loop_stop_event.is_set():
                    break

                # Cooldown before next loop
                cooldown = 5
                self.loop_state.current_quiz = f"Cooldown {cooldown}s before loop #{loop_num + 1}..."
                if self._loop_stop_event.wait(cooldown):
                    break

        except Exception as e:
            self.send(chat_id, f"❌ <b>Loop crashed</b>\n<code>{esc(str(e)[:300])}</code>\n\n<i>Use ▶ Start Loop to restart.</i>",
                      markup=self.markup())
        finally:
            final_balance = None
            try:
                final_balance = client.get_yakka()
            except Exception:
                pass

            self.loop_state.running = False
            self.loop_state.current_quiz = ""
            elapsed = time.time() - self.loop_state.started_at

            self.send(chat_id,
                f"⏹ <b>Mining Loop Stopped</b>\n\n"
                f"🔄 Loops completed: <code>{self.loop_state.loop_count}</code>\n"
                f"📈 Total earned: <code>+{self.loop_state.total_earned}</code> Yakka\n"
                f"💰 Final balance: <code>{final_balance if final_balance is not None else '?'}</code>\n"
                f"⏱ Total runtime: <code>{fmt_elapsed(elapsed)}</code>",
                markup=self.markup())

    def stop_loop(self, chat_id, message_id=None):
        """Stop the continuous mining loop."""
        if not self.loop_state.running:
            text = "ℹ️ <b>No mining loop is running.</b>\nUse 🔄 Start Loop or /startloop to begin."
            if message_id:
                self.edit(chat_id, message_id, text, markup=self.markup())
            else:
                self.send(chat_id, text, markup=self.markup())
            return

        self._loop_stop_event.set()
        text = "⏳ <b>Stopping mining loop...</b>\n<i>Finishing current quiz, please wait.</i>"
        if message_id:
            self.edit(chat_id, message_id, text, markup=self.markup())
        else:
            self.send(chat_id, text, markup=self.markup())

    # ── Update Handler ───────────────────────────────────────────────

    def handle(self, update):
        if not self.allowed(update):
            return
        callback = update.get("callback_query")
        if callback:
            data = callback.get("data", "")
            chat = callback.get("message", {}).get("chat", {}).get("id")
            mid = callback.get("message", {}).get("message_id")
            self.api.call("answerCallbackQuery", callback_query_id=callback.get("id"))
            if data == "run":
                self.run_queue(chat, mid)
            elif data == "runall":
                self.run_queue(chat, mid, all_accounts=True)
            elif data == "startloop":
                self.start_loop(chat, mid)
            elif data == "stoploop":
                self.stop_loop(chat, mid)
            elif data == "status":
                self.edit(chat, mid, self.dashboard(), markup=self.markup())
            elif data == "balance":
                self.do_balance(chat, mid)
            elif data == "check":
                self.do_check(chat, mid)
            elif data == "styles":
                self.edit(chat, mid, "🎨 <b>Choose a dashboard style</b>",
                          markup=self.styles_markup())
            elif data == "accounts":
                self.edit(chat, mid, "👤 <b>Choose an account</b>",
                          markup=self.accounts_markup())
            elif data == "add_account":
                self.pending_states[chat] = "add_account"
                self.edit(chat, mid, "➕ <b>Add ITZA Account</b>\n\n"
                          "Please send your email and password separated by a space:\n"
                          "<code>email@example.com mypassword</code>\n\n"
                          "Or use command:\n"
                          "<code>/add email@example.com mypassword</code>")
            elif data.startswith("style:"):
                self.style = data.split(":", 1)[1]
                self.edit(chat, mid, self.dashboard(), markup=self.markup())
            elif data.startswith("account:"):
                key = data.split(":", 1)[1]
                if any(a.key == key for a in self.accounts):
                    self.selected_account = key
                self.edit(chat, mid, self.dashboard(), markup=self.markup())
            elif data == "help":
                self.edit(chat, mid,
                          "❔ <b>Help — ITZA Bot Commands</b>\n\n"
                          "<b>Mining Loop:</b>\n"
                          "• <b>🔄 Start Loop</b> — Start continuous mining\n"
                          "• <b>⏹ Stop Loop</b> — Stop the mining loop\n"
                          "• <code>/startloop</code> — Start loop via command\n"
                          "• <code>/stoploop</code> — Stop loop via command\n\n"
                          "<b>Queue Control:</b>\n"
                          "• <b>▶ Run queue</b> — Process quizzes for current account\n"
                          "• <b>▶ Run all</b> — Process all accounts\n\n"
                          "<b>Information:</b>\n"
                          "• <b>📊 Status</b> — Live dashboard with progress\n"
                          "• <b>💰 Balance</b> — Check your Yakka points\n"
                          "• <b>🔍 Check</b> — Verify login + show quiz count\n\n"
                          "<b>Account Management:</b>\n"
                          "• <b>➕ Add Account</b> — Save new credentials\n"
                          "• <b>👤 Accounts</b> — Select/manage accounts\n"
                          "• <code>/add email password</code> — Quick add\n"
                          "• <code>/del account_key</code> — Delete account\n\n"
                          "<b>Other:</b>\n"
                          "• <code>/balance</code> — Check Yakka balance\n"
                          "• <code>/check</code> — Verify everything works\n"
                          "• <code>/style</code> — Change theme",
                          markup=self.markup())
            elif data == "home":
                self.edit(chat, mid, self.dashboard(), markup=self.markup())
            return

        message = update.get("message") or {}
        chat = message.get("chat", {}).get("id")
        text = (message.get("text") or "").strip()
        if not chat or not text:
            return

        # Check if chat is in pending state (e.g. waiting for email password input)
        if chat in self.pending_states and not text.startswith("/"):
            state = self.pending_states.pop(chat, None)
            if state == "add_account":
                parts = text.split(maxsplit=1)
                if len(parts) == 2 and "@" in parts[0]:
                    email, pwd = parts[0], parts[1]
                    acc = self.add_account(email, pwd)
                    self.send(chat, f"✅ <b>Account saved successfully!</b>\n\n"
                              f"Email: <code>{esc(acc.email)}</code>\n"
                              f"Total Accounts: <code>{len(self.accounts)}</code>",
                              markup=self.markup())
                    return
                else:
                    self.send(chat, "⚠️ <b>Invalid format.</b> Send email and password separated by space:\n"
                              "<code>email@example.com mypassword</code>",
                              markup=self.markup())
                    return

        command_parts = text.split(maxsplit=2)
        command = command_parts[0].split("@", 1)[0].lower()

        if command in ("/start", "/help"):
            self.send(chat, self.dashboard(), markup=self.markup())
        elif command == "/status":
            self.send(chat, self.dashboard(), markup=self.markup())
        elif command == "/run":
            self.run_queue(chat)
        elif command == "/runall":
            self.run_queue(chat, all_accounts=True)
        elif command == "/startloop":
            self.start_loop(chat)
        elif command == "/stoploop":
            self.stop_loop(chat)
        elif command == "/balance":
            self.do_balance(chat)
        elif command == "/check":
            self.do_check(chat)
        elif command == "/style":
            self.send(chat, "🎨 <b>Choose a dashboard style</b>", markup=self.styles_markup())
        elif command == "/accounts":
            self.send(chat, "👤 <b>Choose an account</b>", markup=self.accounts_markup())
        elif command in ("/add", "/addaccount"):
            if len(command_parts) >= 3:
                email = command_parts[1]
                pwd = command_parts[2]
                acc = self.add_account(email, pwd)
                self.send(chat, f"✅ <b>Account saved successfully!</b>\n\n"
                          f"Email: <code>{esc(acc.email)}</code>\n"
                          f"Total Accounts: <code>{len(self.accounts)}</code>",
                          markup=self.markup())
            else:
                self.pending_states[chat] = "add_account"
                self.send(chat, "➕ <b>Add ITZA Account</b>\n\n"
                          "Please reply to this message with your email and password separated by a space:\n"
                          "<code>your_email@example.com your_password</code>\n\n"
                          "Or send:\n<code>/add email@example.com password</code>",
                          markup=self.markup())
        elif command in ("/del", "/delaccount", "/deleteaccount"):
            if len(command_parts) >= 2:
                key = command_parts[1]
                if self.delete_account(key):
                    self.send(chat, f"🗑️ <b>Account <code>{esc(key)}</code> deleted.</b>",
                              markup=self.markup())
                else:
                    self.send(chat, f"⚠️ <b>Account <code>{esc(key)}</code> not found.</b>",
                              markup=self.markup())
            else:
                self.send(chat, "⚠️ <b>Usage:</b> <code>/del account_key</code>",
                          markup=self.markup())

    def poll(self):
        self.api.call("setMyCommands", commands=[
            {"command": "start", "description": "Open dashboard"},
            {"command": "run", "description": "Run quiz queue"},
            {"command": "runall", "description": "Run all accounts"},
            {"command": "startloop", "description": "Start continuous mining loop"},
            {"command": "stoploop", "description": "Stop the mining loop"},
            {"command": "balance", "description": "Check Yakka balance"},
            {"command": "check", "description": "Verify login & quiz count"},
            {"command": "add", "description": "Add ITZA email & password"},
            {"command": "accounts", "description": "Manage saved accounts"},
            {"command": "status", "description": "Show worker status"},
            {"command": "style", "description": "Choose UI style"},
        ])
        self.api.call("setChatMenuButton", menu_button={"type": "commands"})
        self.api.call("deleteWebhook", drop_pending_updates=False)
        while True:
            try:
                updates = self.api.call("getUpdates", offset=self.offset,
                                        timeout=25, allowed_updates=["message", "callback_query"])
                for update in updates or []:
                    self.offset = update["update_id"] + 1
                    try:
                        self.handle(update)
                    except Exception:
                        LOG.exception("update handling failed")
            except Exception:
                LOG.exception("polling error, retrying in 5s...")
                time.sleep(5)


def load_dotenv():
    """Load variables from local .env file if present."""
    env_file = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.isfile(env_file):
        with open(env_file, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    k, v = k.strip(), v.strip().strip("'\"")
                    if k and k not in os.environ:
                        os.environ[k] = v


def main():
    load_dotenv()
    token = os.environ.get("ITZA_BOT_TOKEN", "").strip()
    if not token:
        raise SystemExit("Set ITZA_BOT_TOKEN before starting the Telegram dashboard.")
    admins = os.environ.get("ITZA_ADMIN_IDS", "").split(",")
    bot = TelegramBot(token, style=os.environ.get("ITZA_STYLE", "modern"),
                      email=os.environ.get("ITZA_EMAIL", ""),
                      password=os.environ.get("ITZA_PASSWORD", ""),
                      workers=int(os.environ.get("ITZA_WORKERS", "1")),
                      accounts=load_accounts(os.environ.get("ITZA_ACCOUNTS_FILE", ""),
                                             os.environ.get("ITZA_EMAIL", ""),
                                             os.environ.get("ITZA_PASSWORD", "")),
                      admin_ids=admins)
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(message)s")
    start_health_server()
    start_keep_alive()
    LOG.info("Telegram dashboard started with style=%s workers=%s", bot.style, bot.workers)
    bot.poll()


if __name__ == "__main__":
    main()
