"""Contact-quality score, built as a side table keyed by CNPJ.

Run after build: python -m cnpj_mcp.scores <data_dir>
Output: <data_dir>/contact_score/part0..9.parquet (sorted by cnpj) plus contact_score_meta.json.

The score rates how likely a registered email or phone is a real, direct contact for the company.
Inputs are only fields already in the Receita dump. No external calls.
"""
import json, os, sys, time
import duckdb

GENERIC = ("gmail.com hotmail.com outlook.com yahoo.com yahoo.com.br uol.com.br bol.com.br terra.com.br ig.com.br globo.com live.com "
           "icloud.com msn.com r7.com zipmail.com.br oi.com.br hotmail.com.br outlook.com.br globomail.com me.com gmx.com protonmail.com "
           "yahoo.com.ar aol.com bol.com brturbo.com.br superig.com.br click21.com.br veloxmail.com.br pop.com.br ymail.com mail.com email.com "
           "uai.com.br ibest.com.br itelefonica.com.br netsite.com.br").split()
# common typos of the free providers
TYPOS = "gmail.com.br gmai.com gamil.com gmal.com gmail.con hotmai.com hotmal.com gmail.co gmial.com hotmail.co outlook.com.com".split()
ACCOUNTANT = ("contab", "contad", "assessoria", "escritorio", "contas", "fiscal", "despachante", "consultoria")


def sql_list(xs):
    return "(" + ",".join("'%s'" % x for x in xs) + ")"


def build_scores(data, memory="1200MB", threads=2):
    t0 = time.time()
    c = duckdb.connect()
    tmp = os.path.join(data, "_tmp")
    os.makedirs(tmp, exist_ok=True)
    c.execute(f"SET memory_limit='{memory}'; SET temp_directory='{tmp}'; SET threads={threads}; SET preserve_insertion_order=false")
    src = f"read_parquet('{data}/est_cnpj/*.parquet')"
    # share counts: distinct companies (cnpj_basico) that use the same email, phone, or email domain
    NB = 24  # hash buckets keep each distinct-count inside the memory limit
    def bucketed(name, expr, where, key):
        os.makedirs(f"{tmp}/{name}", exist_ok=True)
        for i in range(NB):
            f = f"{tmp}/{name}/b{i:02d}.parquet"
            if os.path.exists(f):
                continue
            c.execute(f"""COPY (SELECT {key}, count(*) n FROM (SELECT DISTINCT {expr}, cnpj_basico FROM {src} WHERE {where} AND hash({key.split(' ')[0]}) % {NB} = {i}) GROUP BY 1)
              TO '{f}.tmp' (FORMAT parquet)""")
            os.replace(f + ".tmp", f)
        print(name, round(time.time() - t0), flush=True)
    c.execute(f"CREATE VIEW email_src AS SELECT lower(trim(email)) em, cnpj_basico FROM {src} WHERE email IS NOT NULL")
    bucketed("email_n", "lower(trim(email)) AS em", "email IS NOT NULL", "em")
    bucketed("domain_n", "lower(split_part(email,'@',2)) AS dom", "email LIKE '%@%.%'", "dom")
    for i in range(NB):
        f = f"{tmp}/phone_n/b{i:02d}.parquet"
        os.makedirs(f"{tmp}/phone_n", exist_ok=True)
        if os.path.exists(f):
            continue
        c.execute(f"""COPY (SELECT ph, count(*) n FROM (SELECT DISTINCT ph, cnpj_basico FROM (
            SELECT ddd1||telefone1 ph, cnpj_basico FROM {src} WHERE telefone1 IS NOT NULL
            UNION ALL SELECT ddd2||telefone2, cnpj_basico FROM {src} WHERE telefone2 IS NOT NULL) WHERE hash(ph) % {NB} = {i}) GROUP BY 1)
          TO '{f}.tmp' (FORMAT parquet)""")
        os.replace(f + ".tmp", f)
    print("phone_n", round(time.time() - t0), flush=True)
    for name in ("email_n", "phone_n", "domain_n"):
        c.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{tmp}/{name}/*.parquet')")

    gen, typ = sql_list(GENERIC), sql_list(TYPOS)
    acc = " OR ".join(f"dom LIKE '%{a}%'" for a in ACCOUNTANT)
    out = f"{data}/contact_score.new"
    os.makedirs(out, exist_ok=True)

    def phone_kind(ddd, tel):
        # Brazilian numbers: mobile = 9 digits starting with 9, landline = 8 digits starting 2-5, 0800/4004-style = other
        return f"""CASE WHEN {tel} IS NULL THEN NULL
          WHEN replace({tel}, substr({tel},1,1), '') = '' OR {tel} IN ('12345678','123456789') OR {ddd} IS NULL OR length({ddd}) <> 2 OR {ddd} = '00' THEN 'invalid'
          WHEN length({tel}) = 9 AND {tel} LIKE '9%' THEN 'mobile'
          WHEN length({tel}) = 8 AND substr({tel},1,1) IN ('2','3','4','5') THEN 'landline'
          WHEN length({tel}) = 8 AND substr({tel},1,1) IN ('6','7','8','9') THEN 'mobile_old'
          ELSE 'invalid' END"""

    def share_pts(n):
        return f"CASE WHEN {n} IS NULL THEN 0 WHEN {n} = 1 THEN 25 WHEN {n} <= 3 THEN 10 WHEN {n} <= 20 THEN -15 ELSE -35 END"

    for dgt in "0123456789":
        c.execute(f"""COPY (
          WITH b AS (
            SELECT cnpj, cnpj_basico, nome_fantasia, razao_social, lower(trim(email)) em, ddd1, telefone1, ddd2, telefone2
            FROM {src} WHERE cnpj LIKE '{dgt}%' AND (email IS NOT NULL OR telefone1 IS NOT NULL OR telefone2 IS NOT NULL)),
          e AS (
            SELECT b.*, split_part(em,'@',2) dom, split_part(em,'@',1) loc,
              CASE WHEN em IS NULL THEN NULL WHEN em NOT LIKE '%@%.%' THEN 'invalid' END bad
            FROM b),
          j AS (
            SELECT e.*, en.n email_n, dn.n domain_n, p1.n p1_n, p2.n p2_n,
              {phone_kind('ddd1','telefone1')} p1_kind, {phone_kind('ddd2','telefone2')} p2_kind,
              regexp_split_to_array(regexp_replace(strip_accents(lower(coalesce(nome_fantasia, razao_social, ''))), '[^a-z0-9 ]', ' ', 'g'), ' +') toks
            FROM e LEFT JOIN email_n en ON en.em = e.em LEFT JOIN domain_n dn ON dn.dom = e.dom
              LEFT JOIN phone_n p1 ON p1.ph = e.ddd1||e.telefone1 LEFT JOIN phone_n p2 ON p2.ph = e.ddd2||e.telefone2),
          k AS (
            SELECT j.*,
              CASE WHEN em IS NULL THEN NULL WHEN bad IS NOT NULL THEN 'invalid' WHEN dom IN {typ} THEN 'typo'
                   WHEN dom IN {gen} THEN 'generic' WHEN ({acc}) OR ({acc.replace('dom','loc')}) THEN 'accountant' ELSE 'corporate' END email_kind,
              list_bool_or(list_transform(list_filter(toks, x -> length(x) >= 4), x -> contains(dom, x) OR contains(loc, x))) AS name_match
            FROM j),
          s AS (
            SELECT k.*,
              CASE WHEN email_kind IS NULL THEN NULL ELSE greatest(0, least(100,
                CASE email_kind WHEN 'corporate' THEN 55 WHEN 'generic' THEN 30 WHEN 'accountant' THEN 15 ELSE 0 END
                + {share_pts('email_n')} + CASE WHEN coalesce(name_match,false) THEN 20 ELSE 0 END
                - CASE WHEN email_kind = 'corporate' AND domain_n > 20 THEN 25 ELSE 0 END)) END email_score,
              greatest(
                CASE WHEN p1_kind IS NULL THEN NULL ELSE greatest(0, least(100, CASE p1_kind WHEN 'mobile' THEN 65 WHEN 'landline' THEN 55 WHEN 'mobile_old' THEN 40 ELSE -1000 END + {share_pts('p1_n')})) END,
                CASE WHEN p2_kind IS NULL THEN NULL ELSE greatest(0, least(100, CASE p2_kind WHEN 'mobile' THEN 65 WHEN 'landline' THEN 55 WHEN 'mobile_old' THEN 40 ELSE -1000 END + {share_pts('p2_n')})) END) phone_score
            FROM k)
          SELECT cnpj, email_kind, email_n AS email_shared_companies, domain_n AS email_domain_companies, coalesce(name_match,false) AS email_matches_name,
            email_score, p1_kind AS phone1_kind, p1_n AS phone1_shared_companies, p2_kind AS phone2_kind, p2_n AS phone2_shared_companies, phone_score,
            greatest(coalesce(email_score,0), coalesce(phone_score,0)) AS contact_score,
            CASE WHEN greatest(coalesce(email_score,0), coalesce(phone_score,0)) >= 70 THEN 'high'
                 WHEN greatest(coalesce(email_score,0), coalesce(phone_score,0)) >= 40 THEN 'medium' ELSE 'low' END AS contact_tier
          FROM s ORDER BY cnpj) TO '{out}/part{dgt}.parquet' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 50000)""")
        print("part", dgt, round(time.time() - t0), flush=True)
    dest = f"{data}/contact_score"
    if os.path.exists(dest):
        import shutil; shutil.rmtree(dest)
    os.rename(out, dest)
    n = c.execute(f"SELECT count(*), count(*) FILTER (WHERE contact_tier='high'), count(*) FILTER (WHERE contact_tier='medium'), count(*) FILTER (WHERE contact_tier='low') FROM read_parquet('{dest}/*.parquet')").fetchone()
    meta = {"rows": n[0], "high": n[1], "medium": n[2], "low": n[3], "seconds": round(time.time() - t0)}
    json.dump(meta, open(f"{data}/contact_score_meta.json", "w"))
    return meta


if __name__ == "__main__":
    print(build_scores(sys.argv[1]))
