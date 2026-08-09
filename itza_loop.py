"""
ITZA Quiz Auto-Complete — REAL submission + correct answers + auto-loop (3x) + colors
"""
import argparse
import json
import os
import random
import sys
import time
import hashlib
import re

import requests

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
    def __init__(self, email=EMAIL, password=PASSWORD):
        self.email, self.password = email, password
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})
        self.user = None
        self.user_id = None
        self.access_token = None
        self.refresh_token = None
        self.token_expires = 0
        self._current_form_id = None

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
            print(f"{G}[+] Login OK: {self.email} (resolved from session){D}")
        else:
            print(f"{G}[+] Login OK: {self.email}{D}")

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
        print(f"{G}[+] Access token acquired (expires in {int(self.token_expires - time.time())}s){D}")

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
                        print(f"{G}[+] DDP login: user {self.user_id[:20]}...{D}")
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
                print(f"{G}[+] Access token refreshed{D}")
                return True
        except Exception:
            pass
        return False

    def get_yakka(self):
        if not self.access_token:
            return None
        try:
            r = requests.get(f"{API}/v1/yakka/balance",
                headers={"Authorization": f"Bearer {self.access_token}", "User-Agent": "Mozilla/5.0"},
                timeout=10)
            if r.status_code == 200:
                return r.json().get("balance", 0)
            if r.status_code == 401 and self._ensure_token():
                r = requests.get(f"{API}/v1/yakka/balance",
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
            r = requests.get(f"{API}/v1/users/me",
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
                r = requests.post(url, json={"query": q},
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
                r = requests.get(f"{TYPEFORM_DEF}/forms/{quiz_id}",
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

    def _file_upload_answer(self, field, fref, correct, stats):
        fid = field.get("id")
        qid = self._current_form_id
        TH = {"User-Agent": "Mozilla/5.0", "Origin": TYPEFORM_FORM,
              "Referer": f"{TYPEFORM_FORM}/to/{qid}"}
        try:
            r = requests.get(
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
            with requests.post(s3url, data=fdata,
                               files=[("file", (fn, b"\x89PNG\r\n\x1a\nfake-png", "image/png"))],
                               timeout=30, verify=False) as r2:
                if r2.status_code not in (200, 201, 204):
                    return None
            stats["correct"] += 1
            return {"field": {"id": fid, "type": "file_upload"},
                    "type": "file_name", "file_name": f"{token}-{fn}"}
        except Exception:
            return None

    def _build_answers(self, definition, correct, stats):
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
                ans = self._file_upload_answer(f, fref, correct, stats)
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

        r = requests.post(f"{TYPEFORM_FORM}/forms/{quiz_id}/start-submission",
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

        r = requests.post(f"{TYPEFORM_FORM}/forms/{quiz_id}/complete-submission",
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

    def complete_quiz(self, lo):
        quiz_id = lo.get("quiz_id")
        if not quiz_id:
            return False, "no quiz_id", {}
        self._current_form_id = quiz_id
        definition = self._typeform_definition(quiz_id)
        if not definition or "__status" in definition:
            return False, "typeform 404/error", {}

        correct, thankyou = self._parse_correct_answers(definition)
        stats = {"correct": 0, "fallback": 0, "questions": 0}
        stats["questions"] = sum(1 for f in definition.get("fields") or []
                                 if f.get("type") not in ("statement", "group", "hidden"))
        answers, _ = self._build_answers(definition, correct, stats)

        ty_ref = next(iter(thankyou.values()), None) or "default_tys"

        ok, detail = self._typeform_submit(quiz_id, lo, answers, ty_ref)
        return ok, detail, stats

    def run(self, quizzes, delay=2.0):
        yakka_start = self.get_yakka()
        print(f"\n{C}[*] Yakka at start: {yakka_start}{D}\n")
        ok = fail = 0
        earned_total = 0
        for i, lo in enumerate(quizzes):
            title = (lo.get("lo_name") or lo.get("quiz_id") or "?")[:60]
            before = self.get_yakka()
            try:
                success, detail, stats = self.complete_quiz(lo)
            except Exception as e:
                success, detail, stats = False, f"error: {e}", {}
            after = self.get_yakka()
            delta = None
            if before is not None and after is not None:
                delta = after - before
                if delta and delta > 0:
                    earned_total += delta
            qs = stats.get("questions", 0)
            cc = stats.get("correct", 0)
            fb = stats.get("fallback", 0)
            if success:
                ok += 1
                delta_s = f"+{delta}" if delta is not None else "?"
                print(f"{G}[OK {i+1}/{len(quizzes)}] {title}  |  Qs={qs}(correct {cc}, fb {fb})  |  "
                      f"Yakka {before}->{after} ({delta_s})  [RUN +{earned_total}]{D}")
            else:
                fail += 1
                print(f"{R}[FAIL {i+1}/{len(quizzes)}] {title}  |  {detail}{D}")
                print(f"{Y}      Qs={qs}(correct {cc}, fb {fb})  |  Yakka {before}->{after}{D}")
            if i < len(quizzes) - 1 and delay:
                time.sleep(delay + random.uniform(0, 1.5))
        yakka_end = self.get_yakka()
        print(f"\n{M}{'=' * 60}{D}")
        print(f"{BOLD}{G}Result: {ok} completed, {fail} failed{D}")
        if yakka_start is not None and yakka_end is not None:
            print(f"{C}Yakka: {yakka_start} -> {yakka_end} (delta: +{yakka_end - yakka_start}){D}")
        print(f"{Y}Total earned this run: +{earned_total} pts{D}")
        print(f"{M}{'=' * 60}{D}")
        return ok, fail


def main():
    ap = argparse.ArgumentParser(
        description="ITZA quiz auto-complete - REAL completion + correct answers",
        usage="python itza_quiz_complete.py [email/username] [password] [options]\n"
              "       python itza_quiz_complete.py --email <email> --password <pass> [options]"
    )
    ap.add_argument("email_pos", nargs="?", default=None, help="ITZA email or username (positional)")
    ap.add_argument("password_pos", nargs="?", default=None, help="ITZA password (positional)")
    ap.add_argument("--email", "-e", dest="email_flag", default=EMAIL, help="ITZA email")
    ap.add_argument("--password", "-p", dest="password_flag", default=PASSWORD, help="ITZA password")
    ap.add_argument("--limit", type=int, default=int(os.environ.get("ITZA_LIMIT", "0")),
                    help="Max quizzes per run (0 = all)")
    ap.add_argument("--delay", type=float, default=float(os.environ.get("ITZA_DELAY", "2.0")),
                    help="Seconds between quizzes")
    ap.add_argument("--loops", type=int, default=int(os.environ.get("ITZA_LOOPS", "1")))
    ap.add_argument("--quiz", default="", help="Only run this quiz id (skip others)")
    args = ap.parse_args()

    email = args.email_pos or args.email_flag or EMAIL or ""
    password = args.password_pos or args.password_flag or PASSWORD or ""

    if not email.strip():
        email = input(f"{C}Enter your ITZA email/username: {D}").strip()
    if not password:
        try:
            import getpass
            password = getpass.getpass(f"{C}Enter your ITZA password: {D}")
        except Exception:
            password = input(f"{C}Enter your ITZA password: {D}")

    print(f"\n{BOLD}{M}[*] Starting ITZA Quiz Auto-Complete{D}")
    print(f"{C}[*] Account: {email}{D}")
    print(f"{C}[*] Target: ALL available quizzes{D}\n")

    c = ITZAQuizClient(email, password)
    print(f"{Y}[*] Logging in...{D}")
    try:
        c.login()
    except Exception as e:
        print(f"{R}[!] Login failed: {e}{D}")
        sys.exit(1)

    info = c.get_user_info()
    if info:
        print(f"{G}[+] User: {info.get('name', '?')} | Points: {info.get('points', '?')}{D}")

    print(f"{Y}[*] Fetching quizzes from Sanity...{D}")
    quizzes = c.get_quizzes()
    if not quizzes:
        print(f"{R}[!] Koi quiz nahi mili.{D}")
        return
    if args.quiz:
        quizzes = [q for q in quizzes if q.get("quiz_id") == args.quiz]
        if not quizzes:
            print(f"{R}[!] Quiz '{args.quiz}' nahi mili.{D}")
            return
    elif args.limit and args.limit > 0:
        quizzes = quizzes[:args.limit]
    print(f"{G}[+] {len(quizzes)} quizzes mili:{D}")

    for q in quizzes[:80]:
        print(f"    {C}- [{q.get('quiz_id')}] {q.get('lo_name')}{D}")

    # ── AUTO LOOP (3x) ── dont change ───────────────────
    AUTO_LOOPS = 3
    for loop in range(AUTO_LOOPS):
        print(f"\n{BOLD}{M}{'=' * 60}{D}")
        print(f"{BOLD}{Y}  LOOP {loop + 1} / {AUTO_LOOPS}{D}")
        print(f"{BOLD}{M}{'=' * 60}{D}")
        c.run(quizzes, delay=args.delay)
        if loop < AUTO_LOOPS - 1:
            print(f"\n{Y}[*] Cooldown 5s before next loop...{D}")
            time.sleep(5)
    # ── END AUTO LOOP ───────────────────────────────────


if __name__ == "__main__":
    main()
