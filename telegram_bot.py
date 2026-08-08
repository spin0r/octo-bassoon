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
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from http.server import HTTPServer, BaseHTTPRequestHandler

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from itza import ITZAQuizClient, THEMES


LOG = logging.getLogger("itza.telegram")


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


@dataclass(frozen=True)
class AccountSpec:
    key: str
    email: str
    password: str


def load_accounts(path="", email="", password=""):
    """Load account records without printing or exposing passwords.

    The JSON format is ``[{"key":"main", "email":"...", "password":"..."}]``.
    An environment-variable account is used when no file is configured.
    """
    if path:
        with open(path, encoding="utf-8") as fh:
            records = json.load(fh)
        if not isinstance(records, list):
            raise ValueError("accounts file must contain a JSON list")
        result = []
        for i, item in enumerate(records, 1):
            if not isinstance(item, dict) or not item.get("email") or not item.get("password"):
                raise ValueError(f"invalid account record at index {i - 1}")
            result.append(AccountSpec(str(item.get("key") or i),
                                      str(item["email"]), str(item["password"])))
        return result
    return [AccountSpec("default", email, password)] if email and password else []


def esc(value) -> str:
    """Escape arbitrary account/quiz text for Telegram HTML parse mode."""
    return html.escape(str(value or ""), quote=False)


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
        if not data.get("ok"):
            raise RuntimeError(data.get("description", f"Telegram {method} failed"))
        return data.get("result")


class TelegramBot:
    def __init__(self, token: str, *, style="modern", email="", password="",
                 workers=1, admin_ids=(), accounts=None):
        if style not in BOT_STYLES:
            raise ValueError(f"unknown style: {style}")
        self.api = TelegramAPI(token)
        self.style = style
        self.accounts = list(accounts or load_accounts(email=email, password=password))
        self.selected_account = self.accounts[0].key if self.accounts else ""
        self.workers = max(1, min(int(workers), 32))
        self.admin_ids = {int(x) for x in admin_ids if str(x).strip().lstrip("-").isdigit()}
        self.offset = 0
        self.started = time.time()
        self.job_lock = threading.Lock()
        self.job = None

    @property
    def theme(self):
        return BOT_STYLES[self.style]

    def markup(self):
        return {"inline_keyboard": [
            [{"text": "▶ Run queue", "callback_data": "run"},
             {"text": "▶ Run all", "callback_data": "runall"}],
            [{"text": "📊 Status", "callback_data": "status"},
             {"text": "👤 Account", "callback_data": "accounts"}],
            [{"text": "🎨 Style", "callback_data": "styles"},
             {"text": "❔ Help", "callback_data": "help"}],
        ]}

    def styles_markup(self):
        return {"inline_keyboard": [[
            {"text": ("✅ " if name == self.style else "") + name.title(),
             "callback_data": f"style:{name}"}
            for name in BOT_STYLES
        ], [{"text": "‹ Back", "callback_data": "home"}]]}

    def accounts_markup(self):
        rows = []
        for account in self.accounts[:100]:
            rows.append([{"text": ("✅ " if account.key == self.selected_account else "") + account.key,
                          "callback_data": f"account:{account.key}"}])
        rows.append([{"text": "‹ Back", "callback_data": "home"}])
        return {"inline_keyboard": rows}

    def dashboard(self):
        t = self.theme
        with self.job_lock:
            job = self.job
        if job and job.is_alive():
            state = "<b>running</b>"
        elif job:
            state = "<b>finished</b>"
        else:
            state = "<b>idle</b>"
        return (f"{t.icon} <b>ITZA Queue Runner</b>\n\n"
                f"Status: {state}\n"
                f"Account: <code>{esc(self.selected_account or 'none')}</code> "
                f"({len(self.accounts):,} configured)\n"
                f"Style: <code>{esc(self.style)}</code>\n"
                f"Workers: <code>{self.workers}</code>\n"
                f"Uptime: <code>{int(time.time() - self.started)}s</code>")

    def send(self, chat_id, text, *, markup=None):
        return self.api.call("sendMessage", chat_id=chat_id, text=text,
                             parse_mode=self.theme.parse_mode,
                             link_preview_options={"is_disabled": True},
                             reply_markup=markup)

    def edit(self, chat_id, message_id, text, *, markup=None):
        return self.api.call("editMessageText", chat_id=chat_id,
                             message_id=message_id, text=text,
                             parse_mode=self.theme.parse_mode,
                             link_preview_options={"is_disabled": True},
                             reply_markup=markup)

    def allowed(self, update):
        if not self.admin_ids:
            return True
        user = (update.get("message") or update.get("callback_query") or {}).get("from") or {}
        return int(user.get("id", 0)) in self.admin_ids

    def run_queue(self, chat_id, message_id=None, *, all_accounts=False):
        with self.job_lock:
            if self.job and self.job.is_alive():
                return False

            selected = next((a for a in self.accounts if a.key == self.selected_account), None)
            targets = self.accounts if all_accounts else ([selected] if selected else [])
            if not targets:
                self.send(chat_id, "⚠️ <b>No account configured.</b> Set ITZA_EMAIL/ITZA_PASSWORD "
                          "or provide ITZA_ACCOUNTS_FILE.", markup=self.markup())
                return False

            def work():
                try:
                    def process(account):
                        client = ITZAQuizClient(account.email, account.password,
                                                style=self.style, quiet=True)
                        client.login()
                        quizzes = client.get_quizzes()
                        ok, fail = client.run(quizzes, delay=0.2, workers=1,
                                              style=self.style)
                        return account.key, ok, fail

                    completed = failed = 0
                    account_errors = []
                    # A bounded account pool handles 1,000+ configured records
                    # without spawning one thread for each account.
                    with ThreadPoolExecutor(max_workers=min(self.workers, len(targets)),
                                            thread_name_prefix="itza-account") as pool:
                        futures = {pool.submit(process, account): account for account in targets}
                        for future in as_completed(futures):
                            account = futures[future]
                            try:
                                _, ok, fail = future.result()
                                completed += ok
                                failed += fail
                            except Exception as exc:
                                account_errors.append(f"{account.key}: {exc}")
                    self.send(chat_id, f"✅ <b>Queue complete</b>\n\n"
                              f"Accounts: <code>{len(targets):,}</code>\n"
                              f"Completed: <code>{completed:,}</code>\n"
                              f"Failed: <code>{failed:,}</code>\n"
                              f"Account errors: <code>{len(account_errors):,}</code>",
                              markup=self.markup())
                except Exception as exc:
                    LOG.exception("queue failed")
                    self.send(chat_id, f"⚠️ <b>Queue failed</b>\n<code>{esc(exc)}</code>",
                              markup=self.markup())

            self.job = threading.Thread(target=work, name="telegram-queue", daemon=True)
            self.job.start()
        scope = f"all {len(targets):,} accounts" if all_accounts else esc(targets[0].key)
        text = (f"▶ <b>Queue started</b>\nScope: <code>{scope}</code>\n"
                "Workers are processing jobs in the background.")
        if message_id:
            self.edit(chat_id, message_id, text, markup=self.markup())
        else:
            self.send(chat_id, text, markup=self.markup())
        return True

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
            elif data == "status":
                self.edit(chat, mid, self.dashboard(), markup=self.markup())
            elif data == "styles":
                self.edit(chat, mid, "🎨 <b>Choose a dashboard style</b>",
                          markup=self.styles_markup())
            elif data == "accounts":
                self.edit(chat, mid, "👤 <b>Choose an account</b>",
                          markup=self.accounts_markup())
            elif data.startswith("style:"):
                self.style = data.split(":", 1)[1]
                self.edit(chat, mid, self.dashboard(), markup=self.markup())
            elif data.startswith("account:"):
                key = data.split(":", 1)[1]
                if any(a.key == key for a in self.accounts):
                    self.selected_account = key
                self.edit(chat, mid, self.dashboard(), markup=self.markup())
            elif data == "help":
                self.edit(chat, mid, "❔ <b>Help</b>\nUse Run queue to start processing, "
                          "Status to inspect the worker pool, and Style to change formatting.",
                          markup=self.markup())
            elif data == "home":
                self.edit(chat, mid, self.dashboard(), markup=self.markup())
            return
        message = update.get("message") or {}
        chat = message.get("chat", {}).get("id")
        command = (message.get("text") or "").split()[0].split("@", 1)[0].lower()
        if command in ("/start", "/help"):
            self.send(chat, self.dashboard(), markup=self.markup())
        elif command == "/status":
            self.send(chat, self.dashboard(), markup=self.markup())
        elif command == "/run":
            self.run_queue(chat)
        elif command == "/runall":
            self.run_queue(chat, all_accounts=True)
        elif command == "/style":
            self.send(chat, "🎨 <b>Choose a dashboard style</b>", markup=self.styles_markup())
        elif command == "/accounts":
            self.send(chat, "👤 <b>Choose an account</b>", markup=self.accounts_markup())

    def poll(self):
        self.api.call("setMyCommands", commands=[
            {"command": "start", "description": "Open the dashboard"},
            {"command": "run", "description": "Run the quiz queue"},
            {"command": "runall", "description": "Run all configured accounts"},
            {"command": "status", "description": "Show worker status"},
            {"command": "style", "description": "Choose UI style"},
            {"command": "accounts", "description": "Choose an account"},
        ])
        self.api.call("setChatMenuButton", menu_button={"type": "commands"})
        self.api.call("deleteWebhook", drop_pending_updates=False)
        while True:
            updates = self.api.call("getUpdates", offset=self.offset,
                                    timeout=25, allowed_updates=["message", "callback_query"])
            for update in updates or []:
                self.offset = update["update_id"] + 1
                try:
                    self.handle(update)
                except Exception:
                    LOG.exception("update handling failed")


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
