"""ITZA Queue Runner 2.0.

A resilient command-line client with:

* one consistent ``style`` system for every dashboard message;
* bounded concurrent execution for memory-stable 1,000+ item queues;
* connection pooling, exponential retry, and Retry-After support;
* structured per-job timings and end-of-run throughput summaries;
* environment-variable and command-line configuration.

Run ``python itza.py --help`` for examples.  Credentials are read from
``ITZA_EMAIL`` / ``ITZA_PASSWORD`` when those variables are present.
"""
import argparse
import json
import os
import random
import sys
import time
import hashlib
import re
import logging
import threading
from dataclasses import dataclass
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    sys.stdin.reconfigure(encoding="utf-8", errors="replace")
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# ── COLORS ───────────────────────────────────────────
R = "\033[91m"      # red
G = "\033[92m"      # green  
Y = "\033[93m"      # yellow
B = "\033[94m"      # blue
M = "\033[95m"      # magenta
C = "\033[96m"      # cyan
W = "\033[97m"      # white
D = "\033[0m"       # reset
BOLD = "\033[1m"


@dataclass(frozen=True)
class Theme:
    """Terminal theme.  Keeping all presentation in one place makes the
    script usable from a terminal, CI, or a Telegram/GUI adapter without
    sprinkling escape codes through business logic."""
    name: str
    accent: str
    ok: str
    warn: str
    error: str
    muted: str
    reset: str = D
    bold: str = BOLD


THEMES = {
    "modern": Theme("modern", C, G, Y, R, "\033[90m"),
    "neon": Theme("neon", M, "\033[38;5;82m", "\033[38;5;214m", "\033[38;5;203m", "\033[38;5;245m"),
    "compact": Theme("compact", "", "", "", "", ""),
    "plain": Theme("plain", "", "", "", "", ""),
}


class UI:
    """Consistent, style-aware output layer.

    ``style`` is accepted by every public reporting method and can be
    switched at runtime by integrations.  This is intentionally dependency
    free; if ``rich`` is installed, callers can still wrap this output.
    """
    def __init__(self, style="modern", quiet=False):
        self.style = style if style in THEMES else "modern"
        self.quiet = quiet

    @property
    def theme(self):
        return THEMES[self.style]

    def set_style(self, style):
        if style not in THEMES:
            raise ValueError(f"unknown style: {style}")
        self.style = style

    def _write(self, message, color="", prefix=""):
        if self.quiet:
            return
        t = self.theme
        if self.style in ("plain", "compact"):
            clean = re.sub(r"\x1b\[[0-9;]*m", "", f"{prefix}{message}")
            print(clean)
        else:
            print(f"{color}{prefix}{message}{t.reset}")

    def title(self, message):
        self._write(message, self.theme.accent, f"{self.theme.bold}◆ ")

    def info(self, message):
        self._write(message, self.theme.accent, "· ")

    def ok(self, message):
        self._write(message, self.theme.ok, "✓ ")

    def warn(self, message):
        self._write(message, self.theme.warn, "! ")

    def error(self, message):
        self._write(message, self.theme.error, "✗ ")

    def line(self, char="─", width=68):
        if not self.quiet:
            self._write(char * width, self.theme.muted)

BASE = "https://www.itza.io"
SANITY_URL = "https://6l0cbkca.api.sanity.io/v2022-03-07/data/query"
DATASETS = ["production", "development"]
API = "https://api.itza.world"
DDP_URL = "wss://auth.itza.world/websocket"

TYPEFORM_DEF = "https://form.typeform.com"
TYPEFORM_FORM = "https://graspandrecall.typeform.com"

ACTION_ACCESS_TOKEN = "7ee46e44955e58bbb5b1cf122da103ef5f611c91"
ACTION_EXCHANGE = "96b559b882bbca0faa3afa79ac561475d9ca90bb"

EMAIL = os.environ.get("ITZA_EMAIL", "")
PASSWORD = os.environ.get("ITZA_PASSWORD", "")


class ITZAQuizClient:
    def __init__(self, email=EMAIL, password=PASSWORD, *, style="modern", quiet=False,
                 request_timeout=30, max_retries=4):
        self.email, self.password = email, password
        self.s = requests.Session()
        retry = Retry(
            total=max_retries,
            connect=max_retries,
            read=max_retries,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET", "POST"}),
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=32, pool_maxsize=32)
        self.s.mount("https://", adapter)
        self.s.mount("http://", adapter)
        self.s.headers.update({
            "User-Agent": "ITZA-Automation/2.0 (+https://www.itza.io)",
            "Accept": "application/json",
        })
        self.public_s = requests.Session()
        public_adapter = HTTPAdapter(max_retries=retry, pool_connections=32, pool_maxsize=32)
        self.public_s.mount("https://", public_adapter)
        self.public_s.mount("http://", public_adapter)
        self.public_s.headers.update({"User-Agent": self.s.headers["User-Agent"]})
        self.ui = UI(style, quiet)
        self.request_timeout = request_timeout
        self._quiz_lock = threading.Lock()
        self._token_lock = threading.RLock()
        self.user = None
        self.user_id = None
        self.access_token = None
        self.refresh_token = None
        self.token_expires = 0

    def login(self):
        self._login_nextauth()
        self._get_access_token()
        return True

    def _login_nextauth(self):
        r = self.s.get(f"{BASE}/api/auth/csrf", timeout=15)
        csrf = r.json()["csrfToken"]
        r = self.s.post(f"{BASE}/api/auth/callback/unify",
            data={"csrfToken": csrf, "email": self.email, "password": self.password, "json": "true"},
            timeout=20)
        session = self.s.get(f"{BASE}/api/auth/session", timeout=15).json()
        if not session.get("user"):
            raise Exception("Login failed - check credentials")
        self.user = session["user"]
        real_email = self.user.get("email", "")
        if real_email and "@" in real_email:
            self.email = real_email
            self.ui.ok(f"Login OK: {self.email} (resolved from session)")
        else:
            self.ui.ok(f"Login OK: {self.email}")

    def _get_access_token(self):
        try:
            self._ddp_login()
        except Exception as e:
            raise Exception(f"DDP login failed: {e}")

        if not self.user_id:
            raise Exception("No DDP user ID")

        page_url = f"{BASE}/space/meet-the-planets"
        self.s.get(page_url, timeout=15)

        r = self.s.post(page_url,
            headers={"Next-Action": ACTION_ACCESS_TOKEN, "Content-Type": "text/plain;charset=UTF-8"},
            data=json.dumps([self.user_id]), timeout=20)
        for line in r.text.strip().split("\n"):
            if line.startswith("1:"):
                data = json.loads(line[2:])
                self.refresh_token = data.get("refreshToken")
                break

        if not self.refresh_token:
            raise Exception("No refreshToken from Server Action")

        r = self.s.post(page_url,
            headers={"Next-Action": ACTION_EXCHANGE, "Content-Type": "text/plain;charset=UTF-8"},
            data=json.dumps([self.refresh_token]), timeout=20)
        for line in r.text.strip().split("\n"):
            if line.startswith("1:"):
                data = json.loads(line[2:])
                self.access_token = data.get("access_token")
                self.token_expires = time.time() + data.get("expires_in", 3600)
                self.refresh_token = data.get("refresh_token", self.refresh_token)
                break

        if not self.access_token:
            raise Exception("No access_token from exchange")

        self.s.headers.update({"Authorization": f"Bearer {self.access_token}"})
        self.ui.ok(f"Access token acquired (expires in {int(self.token_expires - time.time())}s)")

    def _ddp_login(self):
        import websocket
        WebSocketTimeout = websocket.WebSocketTimeoutException

        is_email = "@" in self.email
        login_methods = []
        if is_email:
            login_methods.append({"user": {"email": self.email}})
        else:
            login_methods.append({"user": {"username": self.email}})
            login_methods.append({"user": {"email": self.email}})

        for method in login_methods:
            for attempt in range(3):
                ws = None
                try:
                    ws = websocket.create_connection(DDP_URL, timeout=30)
                    ws.send(json.dumps({"msg": "connect", "version": "1", "support": ["1", "pre2", "pre1"]}))
                    deadline = time.time() + 30
                    while time.time() < deadline:
                        ws.settimeout(10)
                        msg = json.loads(ws.recv())
                        if msg.get("msg") == "connected":
                            break
                        elif msg.get("msg") == "ping":
                            ws.send(json.dumps({"msg": "pong", "id": msg.get("id")}))

                    pw_hash = hashlib.sha256(self.password.encode()).hexdigest()
                    ws.send(json.dumps({"msg": "method", "method": "login",
                        "params": [{**method,
                                    "password": {"digest": pw_hash, "algorithm": "sha-256"}}],
                        "id": "1"}))

                    deadline = time.time() + 60
                    while time.time() < deadline:
                        ws.settimeout(10)
                        try:
                            msg = json.loads(ws.recv())
                        except WebSocketTimeout:
                            ws.send(json.dumps({"msg": "ping"}))
                            continue
                        if msg.get("msg") == "ping":
                            ws.send(json.dumps({"msg": "pong", "id": msg.get("id")}))
                        elif msg.get("msg") == "result" and msg.get("id") == "1":
                            if "error" not in msg:
                                self.user_id = msg["result"].get("id")
                            break

                    if self.user_id:
                        ws.close()
                        self.ui.ok(f"DDP login: user {self.user_id[:20]}...")
                        return
                    ws.close()
                except Exception as e:
                    if ws:
                        try:
                            ws.close()
                        except Exception:
                            pass
                    if attempt < 2:
                        time.sleep(2)
                        continue
                    break

        raise Exception("DDP login did not return user ID")

    def _ensure_token(self):
        with self._token_lock:
            if self.access_token and time.time() < self.token_expires - 60:
                return True
            if not self.refresh_token:
                return False
            try:
                r = self.s.post(f"{BASE}/space/meet-the-planets",
                    headers={"Next-Action": ACTION_EXCHANGE, "Content-Type": "text/plain;charset=UTF-8"},
                    data=json.dumps([self.refresh_token]), timeout=20)
                for line in r.text.strip().split("\n"):
                    if line.startswith("1:"):
                        data = json.loads(line[2:])
                        self.access_token = data.get("access_token")
                        self.token_expires = time.time() + data.get("expires_in", 3600)
                        self.refresh_token = data.get("refresh_token", self.refresh_token)
                        break
                if self.access_token:
                    self.s.headers.update({"Authorization": f"Bearer {self.access_token}"})
                    self.ui.ok("Access token refreshed")
                    return True
            except Exception as exc:
                logging.debug("token refresh failed", exc_info=exc)
            return False

    def get_yakka(self):
        if not self.access_token:
            return None
        try:
            r = self.public_s.get(f"{API}/v1/yakka/balance",
                headers={"Authorization": f"Bearer {self.access_token}", "User-Agent": "Mozilla/5.0"},
                timeout=10)
            if r.status_code == 200:
                return r.json().get("balance", 0)
            if r.status_code == 401 and self._ensure_token():
                r = self.public_s.get(f"{API}/v1/yakka/balance",
                    headers={"Authorization": f"Bearer {self.access_token}", "User-Agent": "Mozilla/5.0"},
                    timeout=10)
                if r.status_code == 200:
                    return r.json().get("balance", 0)
        except Exception:
            pass
        return None

    def get_user_info(self):
        if not self.access_token:
            return None
        try:
            r = self.public_s.get(f"{API}/v1/users/me",
                headers={"Authorization": f"Bearer {self.access_token}", "User-Agent": "Mozilla/5.0"},
                timeout=10)
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
        return None

    def query(self, q):
        seen_ids = set()
        results = []
        for ds in DATASETS:
            url = f"{SANITY_URL}/{ds}"
            try:
                r = self.public_s.post(url, json={"query": q},
                    headers={"User-Agent": "Mozilla/5.0"}, timeout=90)
                if r.status_code == 200:
                    for item in r.json().get("result", []):
                        iid = item.get("_id")
                        if iid and iid not in seen_ids:
                            seen_ids.add(iid)
                            results.append(item)
            except Exception:
                continue
        return results

    def get_quizzes(self):
        quizzes = []
        seen = set()

        def add(q):
            qid = q.get("quiz_id")
            key = qid or q.get("quiz_ref")
            if not key or key in seen:
                return
            seen.add(key)
            quizzes.append(q)

        for qz in self.query("*[_type == 'quiz']{_id,title,'slug':slug.current,quizId}"):
            if not qz or not qz.get("_id"):
                continue
            add({
                "kind": "quiz",
                "lo_id": qz["_id"],
                "lo_name": qz.get("title") or qz.get("slug") or qz["_id"],
                "quiz_id": qz.get("quizId") or "",
                "quiz_slug": qz.get("slug") or "",
                "challenge_id": "", "channel_id": "",
                "content_id": qz["_id"], "unit_id": qz["_id"],
                "slug": qz.get("slug") or "", "page_path": "",
                "unit_type": "", "section_id": "", "section_index": "", "pathway_id": "",
            })

        for lo in self.query("""
        *[_type == 'learningObject' && defined(quiz)]{
            _id, name,
            'quiz': quiz->{_id, quizId}
        }"""):
            if not lo or not lo.get("_id"):
                continue
            quiz = lo.get("quiz") or {}
            if not quiz.get("quizId"):
                continue
            add({
                "kind": "quiz",
                "lo_id": lo["_id"],
                "lo_name": lo.get("name") or lo["_id"],
                "quiz_id": quiz.get("quizId") or "",
                "quiz_slug": "",
                "challenge_id": "", "channel_id": "",
                "content_id": lo["_id"], "unit_id": lo["_id"],
                "slug": "", "page_path": "",
                "unit_type": "", "section_id": "", "section_index": "", "pathway_id": "",
            })

        return quizzes

    def _typeform_definition(self, quiz_id):
        for attempt in range(4):
            try:
                r = self.public_s.get(f"{TYPEFORM_DEF}/forms/{quiz_id}",
                    headers={"User-Agent": "Mozilla/5.0"}, timeout=25)
                if r.status_code == 200:
                    return r.json()
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep(3 + attempt * 4)
                    continue
                return {"__status": r.status_code}
            except Exception:
                if attempt < 3:
                    time.sleep(3 + attempt * 4)
                    continue
                return {"__status": -1}
        return {"__status": -1}

    def _parse_correct_answers(self, definition):
        import re as _re
        ty_titles = {}
        for t in definition.get("thankyou_screens") or []:
            ty_titles[t.get("ref")] = (t.get("title") or "")

        def is_correct_screen(title):
            if not title:
                return None
            if _re.search(r"not quite|incorrect|wrong|unlucky|try again|almost|oops|better luck|not right", title, _re.I):
                return False
            return bool(_re.search(r"correct|great job|well done|nice job|amazing|excellent|fantastic|wonderful|brilliant|perfect|good work|right\b|🎉|✅|✔|✨|💯|🔟", title, _re.I))

        def sub_refs(cond):
            op = cond.get("op")
            v = cond.get("vars") or []
            refs = []
            if op in ("is", "is_not"):
                for vv in v:
                    if isinstance(vv, dict) and vv.get("type") == "choice":
                        refs.append(vv.get("value"))
            elif op == "and":
                for sub in v:
                    if isinstance(sub, dict) and sub.get("op") == "is":
                        for vv in (sub.get("vars") or []):
                            if isinstance(vv, dict) and vv.get("type") == "choice":
                                refs.append(vv.get("value"))
            return refs, (op == "is_not")

        info = {}
        for entry in definition.get("logic") or []:
            if entry.get("type") != "field":
                continue
            fref = entry.get("ref")
            for act in entry.get("actions") or []:
                cond = act.get("condition") or {}
                det = act.get("details") or {}
                actype = act.get("action")
                to = (det.get("to") or {}).get("value")
                refs, neg = sub_refs(cond)
                if not refs:
                    continue
                inf = info.setdefault(fref, {"add": [], "and": [], "jump": [], "not": []})
                if neg:
                    inf["not"].append("NOT:" + refs[0])
                elif actype == "add":
                    inf["add"].extend(refs)
                elif actype == "jump" and cond.get("op") == "and" and len(refs) >= 2:
                    if not inf["and"]:
                        inf["and"].extend(refs)
                elif actype == "jump" and len(refs) == 1:
                    inf["jump"].append((refs[0], to))

        correct = {}
        thankyou = {}
        for fref, inf in info.items():
            sel = []
            ty = "default_tys"
            if inf["add"]:
                sel = inf["add"]
            elif inf["and"]:
                sel = inf["and"]
            elif inf["jump"]:
                good = [r for r, to in inf["jump"]
                        if to in ty_titles and is_correct_screen(ty_titles[to]) is True]
                if good:
                    sel = good
                    ty = next(to for r, to in inf["jump"] if r == good[0])
                else:
                    sel = [inf["jump"][0][0]]
            elif inf["not"]:
                sel = inf["not"]
            if sel:
                seen = []
                for r in sel:
                    if r not in seen:
                        seen.append(r)
                correct[fref] = seen
                thankyou[fref] = ty or "default_tys"
        return correct, thankyou

    def _choice_by_ref(self, field, ref):
        for c in (field.get("properties") or {}).get("choices") or []:
            if c.get("ref") == ref:
                return c
        return None

    def _file_upload_answer(self, field, fref, correct, stats, quiz_id):
        fid = field.get("id")
        qid = quiz_id
        TH = {"User-Agent": "Mozilla/5.0", "Origin": TYPEFORM_FORM,
              "Referer": f"{TYPEFORM_FORM}/to/{qid}"}
        try:
            r = self.public_s.get(
                f"{TYPEFORM_FORM}/forms/{qid}/fields/{fid}/upload-credentials",
                headers={**TH, "Content-Type": "application/json"}, timeout=25, verify=False)
            if r.status_code != 200:
                return None
            creds = r.json()
            s3url = creds.get("endpoint") or f"https://{creds['bucket']}.s3.us-east-1.amazonaws.com"
            token = creds.get("token") or ""
            folder = creds.get("folder") or ""
            fn = "itza_canvas.png"
            ext = "png"
            tagging = (f"<Tagging><TagSet><Tag><Key>fileExtension</Key><Value>{ext}"
                       f"</Value></Tag></TagSet></Tagging>")
            fdata = [
                ("key", folder + token),
                ("tagging", tagging),
                ("acl", creds.get("acl", "private")),
            ]
            if creds.get("security_token"):
                fdata.append(("x-amz-security-token", creds["security_token"]))
            fdata += [
                ("x-amz-algorithm", creds.get("algorithm", "AWS4-HMAC-SHA256")),
                ("x-amz-date", creds.get("date")),
                ("x-amz-credential", creds.get("credential")),
                ("policy", creds.get("policy")),
                ("x-amz-signature", creds.get("signature")),
            ]
            with self.public_s.post(s3url, data=fdata,
                               files=[("file", (fn, b"\x89PNG\r\n\x1a\nfake-png", "image/png"))],
                               timeout=30, verify=False) as r2:
                if r2.status_code not in (200, 201, 204):
                    return None
            stats["correct"] += 1
            return {"field": {"id": fid, "type": "file_upload"},
                    "type": "file_name", "file_name": f"{token}-{fn}"}
        except Exception:
            return None

    def _build_answers(self, definition, correct, stats, quiz_id):
        answers = []
        fref_to_id = {}
        for f in definition.get("fields") or []:
            fref_to_id[f.get("ref")] = f.get("id")

        skip_types = {"statement", "group", "hidden", "faq", "deep_dive"}
        for f in definition.get("fields") or []:
            ftype = f.get("type")
            fid = f.get("id")
            fref = f.get("ref")
            props = f.get("properties") or {}
            if not fid or ftype in skip_types:
                continue
            submit_type = "multiple_choice" if ftype == "dropdown" else ftype
            base = {"field": {"id": fid, "type": submit_type}}

            if ftype == "file_upload":
                ans = self._file_upload_answer(f, fref, correct, stats, quiz_id)
                if ans:
                    answers.append(ans)
                continue

            if ftype in ("multiple_choice", "dropdown", "picture_choice", "checkbox", "ranking"):
                choices = props.get("choices") or []
                correct_refs = correct.get(fref) or []
                chosen = []
                multisel = bool(props.get("allow_multiple_selection")) or ftype == "checkbox"
                if correct_refs:
                    if any(str(r).startswith("NOT:") for r in correct_refs):
                        ex = str(correct_refs[0])[4:]
                        cand = [c for c in choices if c.get("ref") != ex]
                        if cand:
                            cobj = cand[0]
                            chosen.append({"id": cobj.get("id"), "label": cobj.get("label")})
                            stats["correct"] += 1
                    else:
                        for r in correct_refs:
                            cobj = self._choice_by_ref(f, r)
                            if cobj:
                                chosen.append({"id": cobj.get("id"), "label": cobj.get("label")})
                        stats["correct"] += len(chosen)
                seen_ids = set()
                uniq = []
                for cobj in chosen:
                    if cobj["id"] not in seen_ids:
                        seen_ids.add(cobj["id"])
                        uniq.append(cobj)
                chosen = uniq
                if not multisel and len(chosen) > 1:
                    chosen = chosen[:1]
                if multisel:
                    min_sel = (props.get("validations") or {}) or (f.get("validations") or {})
                    try:
                        min_n = int(min_sel.get("min_selection") or 1)
                        max_n = int(min_sel.get("max_selection") or min_n)
                    except Exception:
                        min_n, max_n = 1, 1
                    if len(chosen) > max_n:
                        chosen = chosen[:max_n]
                    if len(chosen) < min_n:
                        have = {c.get("id") for c in chosen}
                        for c in choices:
                            if len(chosen) >= min_n:
                                break
                            if c.get("id") not in have:
                                chosen.append({"id": c.get("id"), "label": c.get("label")})
                    while len(chosen) < min_n and choices:
                        c = choices[len(chosen) % len(choices)]
                        if c.get("id") not in {x.get("id") for x in chosen}:
                            chosen.append({"id": c.get("id"), "label": c.get("label")})
                        else:
                            break
                    if len(chosen) < min_n:
                        stats["fallback"] += 1
                        continue
                elif not chosen:
                    stats["fallback"] += 1
                    c = random.choice(choices) if choices else None
                    chosen = [{"id": c.get("id"), "label": c.get("label")}] if c else []
                if chosen:
                    if ftype == "dropdown":
                        b = {"field": {"id": fid, "type": "dropdown"}}
                        b["type"] = "text"
                        b["text"] = chosen[0].get("label", "")
                        answers.append(b)
                    else:
                        b = dict(base)
                        b["type"] = "choices"
                        b["choices"] = chosen
                        answers.append(b)
                elif ftype == "multiple_choice" and not chosen:
                    b = dict(base)
                    b["type"] = "text"
                    b["text"] = "ITZA"
                    answers.append(b)
                continue

            if ftype in ("opinion_scale", "rating", "number"):
                steps = props.get("steps") or 5
                val = random.randint(1, steps if steps else 5)
                b = dict(base); b["type"] = "number"; b["number"] = val
                answers.append(b); stats["fallback"] += 1
                continue

            if ftype in ("boolean", "yes_no", "legal"):
                b = dict(base); b["type"] = "boolean"; b["boolean"] = True
                answers.append(b); stats["fallback"] += 1
                continue

            if ftype in ("short_text", "long_text"):
                b = dict(base); b["type"] = "text"; b["text"] = "ITZA"
                answers.append(b); stats["fallback"] += 1
                continue

            if ftype == "email":
                b = dict(base); b["type"] = "email"; b["email"] = self.email
                answers.append(b); continue

            if ftype in ("url", "website"):
                b = dict(base); b["type"] = "url"; b["url"] = "https://www.itza.io"
                answers.append(b); continue

            if ftype == "date":
                b = dict(base); b["type"] = "date"; b["date"] = "2026-01-01T00:00:00.000Z"
                answers.append(b); continue

            if ftype == "phone_number":
                b = dict(base); b["type"] = "phone_number"; b["phone_number"] = "+441234567890"
                answers.append(b); continue

        return answers, fref_to_id

    def _typeform_submit(self, quiz_id, lo, answers, thankyou_ref):
        TH = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
              "Content-Type": "application/json",
              "Origin": TYPEFORM_FORM,
              "Referer": f"{TYPEFORM_FORM}/to/{quiz_id}"}

        r = self.public_s.post(f"{TYPEFORM_FORM}/forms/{quiz_id}/start-submission",
            headers=TH, json={}, timeout=25)
        if r.status_code != 200:
            return False, "start-submission " + str(r.status_code)
        data = r.json()
        signature = data.get("signature")
        sub = data.get("submission") or {}
        if not signature:
            return False, "no signature"

        hidden = [
            {"key": "user_id", "value": self.user_id or ""},
            {"key": "challenge_id", "value": lo.get("challenge_id") or ""},
            {"key": "unit_id", "value": lo.get("unit_id") or ""},
            {"key": "channel_id", "value": lo.get("channel_id") or ""},
            {"key": "unit_type", "value": lo.get("unit_type") or ""},
            {"key": "section_id", "value": lo.get("section_id") or ""},
            {"key": "section_index", "value": lo.get("section_index") or ""},
            {"key": "hostname", "value": "www.itza.io"},
            {"key": "pathway_id", "value": lo.get("pathway_id") or ""},
            {"key": "content_id", "value": lo.get("content_id") or ""},
        ]

        payload = {
            "signature": signature,
            "form_id": quiz_id,
            "landed_at": sub.get("landed_at", int(time.time())),
            "hidden": hidden,
            "thankyou_screen_ref": thankyou_ref or "default_tys",
            "submission_id": sub.get("submission_id"),
        }
        if answers:
            payload["answers"] = answers

        r = self.public_s.post(f"{TYPEFORM_FORM}/forms/{quiz_id}/complete-submission",
            headers=TH, json=payload, timeout=30)
        if r.status_code == 200:
            try:
                j = r.json()
                if j.get("type") == "completed":
                    return True, j.get("response_id")
            except Exception:
                pass
            return r.status_code == 200, "complete-submission " + str(r.status_code)
        return False, "complete-submission " + str(r.status_code) + " " + r.text

    def complete_quiz(self, lo, *, style=None):
        if style is not None:
            self.ui.set_style(style)
        quiz_id = lo.get("quiz_id")
        if not quiz_id:
            return False, "no quiz_id", {}
        definition = self._typeform_definition(quiz_id)
        if not definition or "__status" in definition:
            return False, "typeform 404/error", {}

        correct, thankyou = self._parse_correct_answers(definition)
        stats = {"correct": 0, "fallback": 0, "questions": 0}
        stats["questions"] = sum(1 for f in definition.get("fields") or []
                                 if f.get("type") not in ("statement", "group", "hidden"))
        answers, _ = self._build_answers(definition, correct, stats, quiz_id)

        ty_ref = next(iter(thankyou.values()), None) or "default_tys"

        ok, detail = self._typeform_submit(quiz_id, lo, answers, ty_ref)
        return ok, detail, stats

    def _complete_job(self, index, total, lo, delay=0.0):
        """Execute one queued quiz job and return a structured result."""
        if delay:
            # Spread starts instead of opening a connection burst.
            time.sleep(random.uniform(0, min(delay, 2.0)))
        started = time.monotonic()
        try:
            success, detail, stats = self.complete_quiz(lo)
        except Exception as exc:
            success, detail, stats = False, f"error: {exc}", {}
        return {
            "index": index,
            "total": total,
            "title": (lo.get("lo_name") or lo.get("quiz_id") or "?")[:60],
            "success": success,
            "detail": detail,
            "stats": stats,
            "elapsed": time.monotonic() - started,
        }

    def _report_job(self, result):
        stats = result["stats"]
        progress = f"{result['index']:>4}/{result['total']:<4}"
        detail = (f"{result['title']} · {stats.get('questions', 0)} questions · "
                  f"{result['elapsed']:.1f}s")
        if result["success"]:
            self.ui.ok(f"[{progress}] {detail}")
        else:
            self.ui.error(f"[{progress}] {detail} · {result['detail']}")

    def run(self, quizzes, delay=2.0, workers=1, *, style=None):
        """Run any size queue through a bounded pool.

        A bounded pool is deliberately used instead of creating one thread per
        item, so queues containing 1,000+ jobs remain memory-stable.  Remote
        endpoints normally rate-limit heavily; 32 live requests is therefore
        the hard ceiling even if a larger number is supplied.
        """
        if style is not None:
            self.ui.set_style(style)
        workers = max(1, min(int(workers or 1), 32))
        yakka_start = self.get_yakka()
        self.ui.line()
        self.ui.title("Execution dashboard")
        self.ui.info(f"Queue: {len(quizzes):,} · Workers: {workers} · Start balance: {yakka_start}")
        self.ui.line()
        ok = fail = 0
        started = time.monotonic()

        if workers == 1:
            for i, lo in enumerate(quizzes, 1):
                result = self._complete_job(i, len(quizzes), lo)
                ok += int(result["success"])
                fail += int(not result["success"])
                self._report_job(result)
                if i < len(quizzes) and delay:
                    time.sleep(delay + random.uniform(0, min(1.5, delay)))
        else:
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="itza-worker") as pool:
                # Keep only a small window of futures in memory. This makes a
                # 100,000-item queue behave like a 1,000-item queue.
                source = iter(enumerate(quizzes, 1))
                pending = set()
                for _ in range(workers * 2):
                    try:
                        i, lo = next(source)
                    except StopIteration:
                        break
                    pending.add(pool.submit(self._complete_job, i, len(quizzes), lo, delay))
                while pending:
                    done, pending = wait(pending, return_when=FIRST_COMPLETED)
                    for future in done:
                        result = future.result()
                        ok += int(result["success"])
                        fail += int(not result["success"])
                        self._report_job(result)
                        try:
                            i, lo = next(source)
                        except StopIteration:
                            continue
                        pending.add(pool.submit(self._complete_job, i, len(quizzes), lo, delay))

        yakka_end = self.get_yakka()
        elapsed = time.monotonic() - started
        self.ui.line("═")
        self.ui.title("Run summary")
        self.ui.ok(f"Completed: {ok:,}")
        if fail:
            self.ui.error(f"Failed: {fail:,}")
        else:
            self.ui.info("Failed: 0")
        if yakka_start is not None and yakka_end is not None:
            self.ui.info(f"Balance: {yakka_start} → {yakka_end} ({yakka_end - yakka_start:+})")
        rate = (ok + fail) / elapsed if elapsed else 0
        self.ui.info(f"Elapsed: {elapsed:.1f}s · Throughput: {rate:.2f} jobs/s")
        self.ui.line("═")
        return ok, fail


def main():
    ap = argparse.ArgumentParser(
        description="ITZA queue runner with resilient networking and a styled terminal dashboard.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""examples:
  python itza.py --email user@example.com --limit 10
  python itza.py --style neon --workers 4 --loops 2
  ITZA_EMAIL=user@example.com ITZA_PASSWORD=secret python itza.py --quiet

styles: modern (default), neon, compact, plain
note: the queue accepts thousands of items while --workers bounds live network traffic.
"""
    )
    ap.add_argument("email_pos", nargs="?", default=None, help="ITZA email or username (positional)")
    ap.add_argument("password_pos", nargs="?", default=None, help="ITZA password (positional)")
    ap.add_argument("--email", "-e", dest="email_flag", default=EMAIL, help="ITZA email")
    ap.add_argument("--password", "-p", dest="password_flag", default=PASSWORD, help="ITZA password")
    ap.add_argument("--limit", type=int, default=int(os.environ.get("ITZA_LIMIT", "0")),
                    help="Max quizzes per run (0 = all)")
    ap.add_argument("--delay", type=float, default=float(os.environ.get("ITZA_DELAY", "2.0")),
                    help="Seconds between quizzes")
    ap.add_argument("--loops", type=int, default=int(os.environ.get("ITZA_LOOPS", "1")),
                    help="Number of queue passes (default: 1)")
    ap.add_argument("--workers", type=int, default=int(os.environ.get("ITZA_WORKERS", "1")),
                    help="Concurrent network workers, 1-32 (default: 1)")
    ap.add_argument("--style", choices=sorted(THEMES), default=os.environ.get("ITZA_STYLE", "modern"),
                    help="Dashboard style (default: modern)")
    ap.add_argument("--quiet", action="store_true", help="Only return process status; suppress dashboard")
    ap.add_argument("--list-limit", type=int, default=20,
                    help="Maximum queue items shown before execution (default: 20)")
    ap.add_argument("--log-level", choices=("DEBUG", "INFO", "WARNING", "ERROR"), default="WARNING")
    ap.add_argument("--quiz", default="", help="Only run this quiz id (skip others)")
    args = ap.parse_args()

    if args.workers < 1 or args.workers > 32:
        ap.error("--workers must be between 1 and 32")
    if args.loops < 1:
        ap.error("--loops must be at least 1")
    if args.delay < 0:
        ap.error("--delay cannot be negative")
    if args.list_limit < 0:
        ap.error("--list-limit cannot be negative")

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(threadName)s %(message)s",
    )
    ui = UI(args.style, args.quiet)

    email = args.email_pos or args.email_flag or EMAIL or ""
    password = args.password_pos or args.password_flag or PASSWORD or ""

    if not email.strip():
        email = input("ITZA email/username: ").strip()
    if not password:
        try:
            import getpass
            password = getpass.getpass("ITZA password: ")
        except Exception:
            password = input("ITZA password: ")

    ui.line("═")
    ui.title("ITZA Queue Runner 2.0")
    ui.info(f"Account: {email}")
    ui.info(f"Style: {args.style} · Workers: {args.workers} · Loops: {args.loops}")
    ui.line("═")

    c = ITZAQuizClient(email, password, style=args.style, quiet=args.quiet)
    ui.info("Authenticating…")
    try:
        c.login()
    except Exception as e:
        ui.error(f"Login failed: {e}")
        return 1

    info = c.get_user_info()
    if info:
        ui.ok(f"User: {info.get('name', '?')} · Points: {info.get('points', '?')}")

    ui.info("Discovering available quizzes…")
    quizzes = c.get_quizzes()
    if not quizzes:
        ui.warn("No quizzes were found.")
        return 0
    if args.quiz:
        quizzes = [q for q in quizzes if q.get("quiz_id") == args.quiz]
        if not quizzes:
            ui.error(f"Quiz '{args.quiz}' was not found.")
            return 2
    elif args.limit and args.limit > 0:
        quizzes = quizzes[:args.limit]
    ui.ok(f"Queued {len(quizzes):,} quizzes")

    for q in quizzes[:args.list_limit]:
        ui.info(f"[{q.get('quiz_id')}] {q.get('lo_name')}")
    if len(quizzes) > args.list_limit:
        ui.info(f"… and {len(quizzes) - args.list_limit:,} more")

    total_failures = 0
    for loop in range(args.loops):
        ui.line()
        ui.title(f"Pass {loop + 1} of {args.loops}")
        _, failures = c.run(quizzes, delay=args.delay, workers=args.workers, style=args.style)
        total_failures += failures
        if loop < args.loops - 1:
            ui.info("Cooling down for 5 seconds before the next pass…")
            time.sleep(5)
    return 1 if total_failures else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)
