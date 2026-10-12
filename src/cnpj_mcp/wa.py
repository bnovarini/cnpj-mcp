"""Lazy WhatsApp Business lookup for company phones, via the public wa.me page.

What it does: when a company with a phone is returned by a query, the phone is queued. One background thread opens
https://wa.me/<number> (one request every 2-3 seconds, never more) and reads the page's og:title / description meta tags.
For a WhatsApp Business account the page shows the business display name and "Business Account" (or "Official Business Account").
For every other number the page is generic and says nothing either way.

What it stores and shows: only positive evidence (a Business account was found). A number with no signal is NOT reported as
invalid or as "not on WhatsApp"; the only record is a checked_at timestamp so it is not asked again until the freshness window ends.

The lookup never runs inside a query: queries only read the local SQLite cache and add numbers to an in-memory queue.
"""
from __future__ import annotations

import html
import os
import random
import re
import sqlite3
import threading
import time
import unicodedata
import urllib.request
from collections import deque
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher

POSITIVE_TTL_DAYS = 90      # a Business account found: re-check after this long
NO_SIGNAL_TTL_DAYS = 30     # nothing found: ask again only after this long (a business may register later)
SPACING = (2.0, 3.0)        # seconds between two requests, always
QUEUE_MAX = 500
BREAKER_FAILS = 5           # consecutive failed or odd responses before pausing
BREAKER_PAUSE = 1800        # seconds
MAX_PER_CALL = 25           # numbers queued from one tool call

_lock = threading.Lock()
_queue: deque = deque()
_queued: set = set()
_thread: threading.Thread | None = None
_paused_until = 0.0


def enabled() -> bool:
    return os.environ.get("CNPJ_WA", "1") != "0"


def _path() -> str:
    return os.environ.get("CNPJ_WA_DB", "/data/wa.db")


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(_path(), timeout=5)
    c.execute("CREATE TABLE IF NOT EXISTS wa_check(number TEXT PRIMARY KEY, checked_at TEXT NOT NULL, wa_business_name TEXT, wa_account_type TEXT, has_photo INTEGER)")
    return c


def _now() -> datetime:
    return datetime.now(timezone.utc)


def variants(ddd, tel) -> list[str]:
    """E.164 digits to try for one registry phone. Receita often drops the leading 9 of mobiles: DDD + 8 digits starting 6-9 is tried with the 9 first."""
    d, t = re.sub(r"\D", "", str(ddd or "")), re.sub(r"\D", "", str(tel or ""))
    if len(d) != 2 or not t:
        return []
    if len(t) == 8 and t[0] in "6789":
        return ["55" + d + "9" + t, "55" + d + t]
    if len(t) in (8, 9):
        return ["55" + d + t]
    return []


def _fresh(checked_at: str, positive: bool) -> bool:
    try:
        t = datetime.fromisoformat(checked_at)
    except ValueError:
        return False
    return _now() - t < timedelta(days=POSITIVE_TTL_DAYS if positive else NO_SIGNAL_TTL_DAYS)


_STOP = {"ltda", "me", "epp", "eireli", "sa", "s", "a", "mei", "ss", "e", "de", "da", "do", "das", "dos", "cia", "comercio", "servicos", "servico", "e"}


def _norm(s: str) -> list[str]:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().lower()
    return sorted(w for w in re.findall(r"[a-z0-9]+", s) if w not in _STOP)


def name_match(wa_name: str | None, razao: str | None, fantasia: str | None) -> dict | None:
    """0-100 similarity between the WhatsApp display name and the company names (order-free, legal suffixes ignored). A hint, not proof."""
    a = " ".join(_norm(wa_name or ""))
    if not a:
        return None
    best, against = -1, None
    for label, n in (("razao_social", razao), ("nome_fantasia", fantasia)):
        b = " ".join(_norm(n or ""))
        if not b:
            continue
        r = SequenceMatcher(None, a, b).ratio()
        ta, tb = set(a.split()), set(b.split())
        if ta and tb and (ta <= tb or tb <= ta):
            r = max(r, 0.85)
        sc = int(round(r * 100))
        if sc > best:
            best, against = sc, label
    return None if against is None else {"score": best, "against": against}


def lookup(numbers: list[str]) -> dict[str, dict]:
    """Cached positives for these numbers (read only, fast)."""
    if not enabled() or not numbers:
        return {}
    try:
        c = _conn()
        rows = c.execute("SELECT number, checked_at, wa_business_name, wa_account_type, has_photo FROM wa_check WHERE wa_account_type IS NOT NULL AND number IN (%s)" % ",".join("?" * len(numbers)), numbers).fetchall()
        c.close()
    except Exception:
        return {}
    return {n: {"checked_at": t, "wa_business_name": nm, "wa_account_type": ty, "has_photo": bool(ph)} for n, t, nm, ty, ph in rows}


def block_for(company: dict) -> dict | None:
    """The whatsapp_business block for one company row (needs ddd1/telefone1/ddd2/telefone2), or None. Also queues unchecked numbers."""
    nums = []
    for d, t in ((company.get("ddd1"), company.get("telefone1")), (company.get("ddd2"), company.get("telefone2"))):
        for v in variants(d, t):
            if v not in nums:
                nums.append(v)
    if not nums:
        return None
    hits = lookup(nums)
    for n in nums:
        if n in hits:
            h = hits[n]
            out = {"number": "+" + n, "wa_business_name": h["wa_business_name"], "wa_account_type": h["wa_account_type"], "checked_at": h["checked_at"],
                   "source": "public wa.me page (WhatsApp Business account display name); not Receita data",
                   "note": "A WhatsApp Business account exists on this number. The display name is chosen by the account owner and does not by itself prove it belongs to this company; see name_match."}
            nm = name_match(h["wa_business_name"], company.get("razao_social"), company.get("nome_fantasia"))
            if nm:
                out["name_match"] = nm
            return out
    enqueue_groups([nums for nums in _groups(company)])
    return None


def _groups(company: dict) -> list[list[str]]:
    gs = []
    for d, t in ((company.get("ddd1"), company.get("telefone1")), (company.get("ddd2"), company.get("telefone2"))):
        v = variants(d, t)
        if v and v not in gs:
            gs.append(v)
    return gs


def enqueue_groups(groups: list[list[str]]) -> None:
    if not enabled():
        return
    with _lock:
        for g in groups:
            key = tuple(g)
            if key in _queued or len(_queue) >= QUEUE_MAX:
                continue
            _queued.add(key)
            _queue.append(g)
    _ensure_thread()


def enrich_rows(rows: list[dict], key: str = "whatsapp_business") -> None:
    """Attach cached positives to company rows and queue the rest. Never waits on the network."""
    if not enabled():
        return
    n = 0
    for r in rows:
        if r.get("cnpj") is None or "telefone1" not in r and "telefone2" not in r:
            continue
        n += 1
        if n > MAX_PER_CALL:
            break
        b = block_for(r)
        if b:
            r[key] = b


def _ensure_thread() -> None:
    global _thread
    with _lock:
        if _thread and _thread.is_alive():
            return
        _thread = threading.Thread(target=_worker, name="wa-lookup", daemon=True)
        _thread.start()


def fetch_page(number: str) -> tuple[int, str]:
    req = urllib.request.Request("https://wa.me/" + number, headers={"User-Agent": "Mozilla/5.0 (compatible; cnpj-mcp)", "Accept-Language": "en"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return r.status, r.read(600000).decode("utf-8", "replace")


def parse(body: str) -> tuple[str | None, str | None, bool]:
    """(og:title, account type or None, has_photo). Account type is only set for Business accounts."""
    t = re.search(r'property="og:title" content="([^"]*)"', body)
    d = re.search(r'name="description" content="([^"]*)"', body)
    if not t or not d:
        return None, None, False
    desc = html.unescape(d.group(1))
    typ = None
    if "Official Business Account" in desc:
        typ = "official_business"
    elif "Business Account" in desc:
        typ = "business"
    return html.unescape(t.group(1)), typ, bool(typ and "pps.whatsapp.net" in body)


def _record(c: sqlite3.Connection, number: str, name, typ, photo) -> None:
    c.execute("INSERT OR REPLACE INTO wa_check VALUES(?,?,?,?,?)", (number, _now().isoformat(timespec="seconds"), name if typ else None, typ, int(photo) if typ else None))
    c.commit()


def _worker() -> None:
    global _paused_until
    fails = 0
    idle = 0
    while True:
        with _lock:
            g = _queue.popleft() if _queue else None
        if g is None:
            idle += 1
            if idle > 30:   # stop when idle for a minute; restarted by the next enqueue
                return
            time.sleep(2)
            continue
        idle = 0
        if time.time() < _paused_until:
            time.sleep(max(0, _paused_until - time.time()))
        try:
            c = _conn()
            for n in g:
                row = c.execute("SELECT checked_at, wa_account_type FROM wa_check WHERE number=?", (n,)).fetchone()
                if row and _fresh(row[0], row[1] is not None):
                    if row[1] is not None:
                        break
                    continue
                time.sleep(random.uniform(*SPACING))
                try:
                    code, body = fetch_page(n)
                except Exception:
                    code, body = 0, ""
                name, typ, photo = parse(body) if code == 200 else (None, None, False)
                if code != 200 or name is None:
                    fails += 1      # no cache write: a failed fetch says nothing about the number
                    if fails >= BREAKER_FAILS:
                        _paused_until = time.time() + BREAKER_PAUSE
                        fails = 0
                        break
                    continue
                fails = 0
                _record(c, n, name, typ, photo)
                if typ:
                    break
            c.close()
        except Exception:
            time.sleep(5)
        finally:
            with _lock:
                _queued.discard(tuple(g))
