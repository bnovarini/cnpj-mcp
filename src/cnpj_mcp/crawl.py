"""Polite crawler for company websites found through registered corporate email domains.

Reads a TSV of `domain<TAB>cnpj` (extra columns ignored) and writes one JSON line per domain with
what the company's own site publishes: WhatsApp links, phones, emails, social profiles, and the
page each item came from. Results are labelled as coming from the company's website, never mixed
into Receita fields.

Politeness: identifies itself, obeys robots.txt, at most 1 request per second per domain, at most
3 pages per domain (home, robots.txt, one contact page), small page size cap, no retries on 4xx.

Run: python -m cnpj_mcp.crawl domains.tsv out.jsonl [concurrency]
"""
import asyncio, json, re, sys, time
from html import unescape
from urllib import robotparser
from urllib.parse import urljoin, urlparse, parse_qs

import httpx

UA = "cnpj-mcp-crawler/0.1 (+https://github.com/bnovarini/cnpj-mcp)"
MAX_BYTES = 400_000
PHONE = re.compile(r"(?<!\d)(?:\+?55\s?)?\(?(\d{2})\)?[\s.-]?(9?\d{4})[\s.-]?(\d{4})(?!\d)")
EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
SOCIAL = {
    "instagram": re.compile(r"https?://(?:www\.)?instagram\.com/([A-Za-z0-9._]{2,30})/?"),
    "facebook": re.compile(r"https?://(?:www\.|pt-br\.|m\.)?facebook\.com/([A-Za-z0-9.\-_/]{2,80})"),
    "linkedin": re.compile(r"https?://(?:[a-z]{2,3}\.)?linkedin\.com/(company|in)/([A-Za-z0-9\-_%]{2,80})"),
    "youtube": re.compile(r"https?://(?:www\.)?youtube\.com/(@[A-Za-z0-9._\-]{2,50}|channel/[A-Za-z0-9_\-]+|c/[A-Za-z0-9_\-]+)"),
    "tiktok": re.compile(r"https?://(?:www\.)?tiktok\.com/(@[A-Za-z0-9._]{2,40})"),
    "x": re.compile(r"https?://(?:www\.)?(?:twitter|x)\.com/([A-Za-z0-9_]{2,30})"),
}
SOCIAL_SKIP = {"sharer", "share", "tr", "plugins", "dialog", "intent", "home", "login"}
WA = re.compile(r"(?:wa\.me/|(?:api|web)\.whatsapp\.com/send/?\?[^\"' <>]*phone=|whatsapp://send\?[^\"' <>]*phone=)\+?(\d{10,13})")
CONTACT_HINT = re.compile(r"contato|contact|fale[-_ ]?conosco|atendimento|onde[-_ ]?estamos|localiza", re.I)
HREF = re.compile(r"""<a\s[^>]*?href=["']([^"'#][^"']*)["'][^>]*>(.*?)</a>""", re.I | re.S)


class Pacer:
    """At most one request per second per domain."""
    def __init__(self):
        self.last = {}

    async def wait(self, host):
        now = time.monotonic()
        nxt = self.last.get(host, 0) + 1.0
        if nxt > now:
            self.last[host] = nxt
            await asyncio.sleep(nxt - now)
        else:
            self.last[host] = now


def extract(html, url):
    found = {"whatsapp": {}, "phones": {}, "emails": {}, "social": {}}
    text = unescape(re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html, flags=re.S | re.I))
    for m in WA.finditer(html):
        found["whatsapp"].setdefault(m.group(1), url)
    for m in re.finditer(r"""href=["']tel:([^"']+)["']""", html, re.I):
        d = re.sub(r"\D", "", m.group(1))
        if 10 <= len(d) <= 13:
            found["phones"].setdefault(d, url)
    plain = re.sub(r"<[^>]+>", " ", text)
    for m in PHONE.finditer(plain):
        ddd, a, b = m.groups()
        if ddd == "00" or ddd[0] == "0":
            continue
        d = ddd + a + b
        if len(set(d)) > 2:
            found["phones"].setdefault(d, url)
    for m in EMAIL.finditer(html):
        e = m.group(0).lower().rstrip(".")
        if not e.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".css", ".js")):
            found["emails"].setdefault(e, url)
    for name, rx in SOCIAL.items():
        for m in rx.finditer(html):
            handle = "/".join(g for g in m.groups() if g).strip("/")
            if handle.split("/")[0].lower() in SOCIAL_SKIP:
                continue
            found["social"].setdefault(f"{name}:{handle}", url)
            break
    return found


def merge(a, b):
    for k in a:
        for item, src in b[k].items():
            a[k].setdefault(item, src)


async def get(client, pacer, url, rp):
    host = urlparse(url).netloc
    if rp is not None and not rp.can_fetch(UA, url):
        return None, "robots_blocked"
    await pacer.wait(host)
    try:
        async with client.stream("GET", url) as r:
            if r.status_code >= 400:
                return None, f"http_{r.status_code}"
            ctype = r.headers.get("content-type", "")
            if "html" not in ctype and "text" not in ctype:
                return None, "not_html"
            buf = b""
            async for chunk in r.aiter_bytes():
                buf += chunk
                if len(buf) > MAX_BYTES:
                    break
            enc = r.encoding or "utf-8"
            return (str(r.url), buf.decode(enc, errors="replace")), "ok"
    except httpx.ConnectError as e:
        return None, "dns_or_connect_fail"
    except httpx.TimeoutException:
        return None, "timeout"
    except Exception as e:
        return None, "error_" + type(e).__name__


async def crawl_domain(client, pacer, dom, cnpj, sem):
    async with sem:
        rec = {"domain": dom, "cnpj": cnpj, "status": None, "pages": [], "crawled_at": int(time.time())}
        found = {"whatsapp": {}, "phones": {}, "emails": {}, "social": {}}
        base, rp = None, None
        for scheme_host in (f"https://{dom}", f"https://www.{dom}", f"http://{dom}"):
            try:
                await pacer.wait(urlparse(scheme_host).netloc)
                r = await client.get(scheme_host + "/robots.txt")
                rp = robotparser.RobotFileParser()
                if r.status_code == 200 and "html" not in r.headers.get("content-type", "").lower()[:9]:
                    rp.parse(r.text.splitlines())
                elif r.status_code in (401, 403):
                    rp.disallow_all = True
                else:
                    rp.allow_all = True
                base = scheme_host
                break
            except httpx.ConnectError:
                continue
            except httpx.TimeoutException:
                rec["status"] = "timeout"
                continue
            except Exception:
                continue
        if base is None:
            rec["status"] = rec["status"] or "dns_or_connect_fail"
            return rec
        res, st = await get(client, pacer, base + "/", rp)
        if res is None:
            rec["status"] = st
            return rec
        final, html = res
        rec["final_url"] = final
        rec["pages"].append(final)
        merge(found, extract(html, final))
        contact = None
        for href, label in HREF.findall(html):
            if CONTACT_HINT.search(href) or CONTACT_HINT.search(re.sub(r"<[^>]+>", " ", label)):
                u = urljoin(final, href)
                p = urlparse(u)
                if p.netloc.replace("www.", "") == urlparse(final).netloc.replace("www.", "") and u.split("#")[0] != final:
                    contact = u.split("#")[0]
                    break
        if contact:
            res2, st2 = await get(client, pacer, contact, rp)
            if res2:
                rec["pages"].append(res2[0])
                merge(found, extract(res2[1], res2[0]))
        rec["status"] = "ok"
        rec.update({k: [{"value": v, "source_url": s} for v, s in d.items()] for k, d in found.items()})
        return rec


async def main(inp, outp, conc=40):
    """Resumable: domains already present in the output file are skipped. A worker pool keeps memory flat for large inputs."""
    import os
    done = set()
    if os.path.exists(outp):
        for line in open(outp):
            try:
                done.add(json.loads(line)["domain"])
            except Exception:
                pass
    rows = [l.rstrip("\n").split("\t") for l in open(inp) if l.strip()]
    rows = [r for r in rows if r[0] not in done]
    deadline = time.time() + float(os.environ.get("CRAWL_MAX_SECONDS", "0") or 1e12)
    print(f"{len(done)} already done, {len(rows)} to crawl, {conc} workers", flush=True)
    pacer = Pacer()
    limits = httpx.Limits(max_connections=conc * 2, max_keepalive_connections=20)
    q = asyncio.Queue()
    for r in rows:
        q.put_nowait(r)
    count = 0
    async with httpx.AsyncClient(headers={"User-Agent": UA, "Accept-Language": "pt-BR,pt;q=0.9"}, timeout=httpx.Timeout(10, connect=6),
                                 follow_redirects=True, limits=limits, max_redirects=4, verify=False) as client:
        with open(outp, "a") as out:
            async def worker():
                nonlocal count
                one = asyncio.Semaphore(1)
                while time.time() < deadline:
                    try:
                        r = q.get_nowait()
                    except asyncio.QueueEmpty:
                        return
                    rec = await crawl_domain(client, pacer, r[0], r[1] if len(r) > 1 else "", one)
                    pacer.last.pop(r[0], None)
                    pacer.last.pop("www." + r[0], None)
                    out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    count += 1
                    if count % 1000 == 0:
                        out.flush()
                        print(count, "done", flush=True)
            await asyncio.gather(*[worker() for _ in range(conc)])
            out.flush()
    print("finished", count, flush=True)


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1], sys.argv[2], int(sys.argv[3]) if len(sys.argv) > 3 else 40))
