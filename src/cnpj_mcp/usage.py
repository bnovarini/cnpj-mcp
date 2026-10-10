"""Privacy-first usage logging: one row per tool call or client handshake, kept in a local SQLite file.

Stored: UTC timestamp, event ("initialize" or "tool_call"), tool name, client name and version from the MCP initialize handshake,
and the product token of the User-Agent header. Not stored: arguments, query text, results, IP addresses or any identifier.
Tool calls are tied to a client name through a short-lived in-memory map (never written to disk) because the server is stateless.

Read the stats on the machine:  python -m cnpj_mcp.usage [days]
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import sys
import threading
import time

TTL = 3600
_lock = threading.Lock()


def _path() -> str:
    return os.environ.get("CNPJ_USAGE_DB", "/data/usage.db")


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(_path(), timeout=5)
    c.execute("CREATE TABLE IF NOT EXISTS usage(ts TEXT, event TEXT, tool TEXT, client_name TEXT, client_version TEXT, user_agent TEXT)")
    return c


def _clean(s, n=64) -> str:
    return re.sub(r"[^A-Za-z0-9 ._\-/+]", "", str(s or ""))[:n]


class UsageLog:
    def __init__(self, app, tool_names):
        self.app, self.tools, self.seen = app, set(tool_names), {}
        self.salt = os.urandom(16)  # in-memory only, so the map cannot be reversed or persisted

    def _key(self, scope) -> str:
        h = dict(scope["headers"])
        ip = (h.get(b"fly-client-ip") or h.get(b"x-forwarded-for", b"").split(b",")[0] or b"?")
        return hashlib.sha256(self.salt + ip + h.get(b"user-agent", b"")).hexdigest()

    def _record(self, rows):
        try:
            with _lock:
                c = _conn()
                c.executemany("INSERT INTO usage VALUES(?,?,?,?,?,?)", rows)
                c.commit(); c.close()
        except Exception:
            pass  # logging must never break a request

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope["method"] != "POST" or scope["path"] == "/healthz":
            return await self.app(scope, receive, send)
        chunks, more = [], True
        while more:
            m = await receive()
            chunks.append(m.get("body", b"")); more = m.get("more_body", False)
        body = b"".join(chunks)
        try:
            msgs = json.loads(body)
            msgs = msgs if isinstance(msgs, list) else [msgs]
            ua = _clean(dict(scope["headers"]).get(b"user-agent", b"").decode("latin-1").split(" ")[0], 40)
            key, now, rows = self._key(scope), time.time(), []
            ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now))
            for m in msgs:
                meth = m.get("method") if isinstance(m, dict) else None
                if meth == "initialize":
                    ci = (m.get("params") or {}).get("clientInfo") or {}
                    cn, cv = _clean(ci.get("name")), _clean(ci.get("version"), 32)
                    self.seen[key] = (cn, cv, now)
                    rows.append((ts, "initialize", None, cn, cv, ua))
                elif meth == "tools/call":
                    name = (m.get("params") or {}).get("name")
                    s = self.seen.get(key)
                    cn, cv = (s[0], s[1]) if s and now - s[2] < TTL else (None, None)
                    rows.append((ts, "tool_call", name if name in self.tools else "(unknown)", cn, cv, ua))
            if len(self.seen) > 5000:
                self.seen = {k: v for k, v in self.seen.items() if now - v[2] < TTL}
            if rows:
                self._record(rows)
        except Exception:
            pass
        sent = False

        async def replay():
            nonlocal sent
            if not sent:
                sent = True
                return {"type": "http.request", "body": body, "more_body": False}
            return await receive()
        await self.app(scope, replay, send)


def stats(days: int = 30) -> dict:
    c = _conn()
    since = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - days * 86400))
    q = lambda sql: [list(r) for r in c.execute(sql, (since,)).fetchall()]
    return {
        "since": since,
        "tool_calls": c.execute("SELECT count(*) FROM usage WHERE event='tool_call' AND ts>=?", (since,)).fetchone()[0],
        "handshakes": c.execute("SELECT count(*) FROM usage WHERE event='initialize' AND ts>=?", (since,)).fetchone()[0],
        "by_tool": q("SELECT tool, count(*) FROM usage WHERE event='tool_call' AND ts>=? GROUP BY 1 ORDER BY 2 DESC"),
        "by_client": q("SELECT coalesce(client_name,'(unknown)'), coalesce(client_version,''), count(*) FROM usage WHERE event='tool_call' AND ts>=? GROUP BY 1,2 ORDER BY 3 DESC LIMIT 20"),
        "by_day": q("SELECT substr(ts,1,10), count(*) FROM usage WHERE event='tool_call' AND ts>=? GROUP BY 1 ORDER BY 1"),
    }


if __name__ == "__main__":
    print(json.dumps(stats(int(sys.argv[1]) if len(sys.argv) > 1 else 30), indent=2))
