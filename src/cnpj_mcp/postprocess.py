"""Clean a finished crawl: validate phones and WhatsApp numbers, drop generic or vendor social links and shared contacts, flag parked landing pages.
Usage: python -m cnpj_mcp.postprocess merged.jsonl out.parquet"""
import collections, json, sys
import duckdb
from .clean import br_number, wa_number, social_ok, landing_problem
from .crawl import JUNK_EMAIL

SHARED_MIN = {"whatsapp": 25, "phones": 25, "emails": 25, "social": 10}  # distinct domains; beyond this a value belongs to a vendor, host or platform


def _clean_row(r, stats):
    if r.get("status") == "ok":
        prob = landing_problem(r["domain"], r.get("final_url"))
        if prob:
            r["status"] = prob
            stats["landing_" + prob] += 1
            r["whatsapp"] = r["phones"] = r["emails"] = r["social"] = []
        else:
            stats["ok_raw"] += 1
            for k in KEYS:
                stats[k + "_raw"] += len(r.get(k) or [])
            wa, ph = [], []
            for it in r.get("whatsapp") or []:
                n, kind = wa_number(it["value"])
                if n:
                    wa.append({"value": "55" + n, "source_url": it["source_url"]})
                else:
                    stats["whatsapp_drop_" + kind] += 1
            for it in r.get("phones") or []:
                n, kind = br_number(it["value"])
                if n:
                    ph.append({"value": n, "source_url": it["source_url"]})
                else:
                    stats["phones_drop_" + kind] += 1
            r["whatsapp"], r["phones"] = wa, ph
            r["emails"] = [e for e in (r.get("emails") or []) if not JUNK_EMAIL.search(e["value"])]
            soc = r.get("social") or []
            r["social"] = [s for s in soc if social_ok(s["value"])]
            stats["social_generic_drop"] += len(soc) - len(r["social"])
    return {k: r.get(k) or ([] if k in KEYS else None) for k in COLS}


KEYS = ("whatsapp", "phones", "emails", "social")
COLS = ("domain", "status", "final_url", "pages") + KEYS


def run(src, dest):
    stats, cnt = collections.Counter(), {k: collections.Counter() for k in KEYS}
    p1 = dest + ".p1.jsonl"
    with open(src) as f, open(p1, "w") as o:
        for line in f:
            r = _clean_row(json.loads(line), stats)
            for k in KEYS:
                for v in {i["value"] for i in r[k]}:
                    cnt[k][v] += 1
            o.write(json.dumps(r) + "\n")
    shared = {k: {v for v, c in cnt[k].items() if c >= SHARED_MIN[k]} for k in KEYS}
    del cnt
    tmp = dest + ".jsonl"
    with open(p1) as f, open(tmp, "w") as o:
        for line in f:
            r = json.loads(line)
            for k in KEYS:
                if r[k]:
                    keep = [i for i in r[k] if i["value"] not in shared[k]]
                    stats[k + "_shared_drop"] += len(r[k]) - len(keep)
                    r[k] = keep
            if r["status"] == "ok":
                stats["ok"] += 1
                for k in KEYS:
                    stats[k + "_after"] += len(r[k])
                    stats[k + "_sites"] += 1 if r[k] else 0
                stats["any_contact_sites"] += 1 if (r["whatsapp"] or r["phones"] or r["emails"]) else 0
            o.write(json.dumps(r) + "\n")
    c = duckdb.connect()
    c.execute(f"""COPY (SELECT domain, status, final_url, CAST(crawled_at AS VARCHAR) crawled_at, pages, whatsapp, phones, emails, social
        FROM read_json_auto('{tmp}', format='newline_delimited', maximum_object_size=33554432, columns={{domain:'VARCHAR',status:'VARCHAR',final_url:'VARCHAR',pages:'VARCHAR[]',
        whatsapp:'STRUCT(value VARCHAR, source_url VARCHAR)[]',phones:'STRUCT(value VARCHAR, source_url VARCHAR)[]',emails:'STRUCT(value VARCHAR, source_url VARCHAR)[]',
        social:'STRUCT(value VARCHAR, source_url VARCHAR)[]'}})) TO '{dest}' (FORMAT parquet, COMPRESSION zstd)""".replace("CAST(crawled_at AS VARCHAR) crawled_at", "'2026-10-08' crawled_at"))
    return stats


if __name__ == "__main__":
    for k, v in sorted(run(sys.argv[1], sys.argv[2]).items()):
        print(k, v)
