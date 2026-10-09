"""MCP server for analytical queries over Brazil's open CNPJ registry (Receita Federal).

Data: Receita Federal "Dados Abertos do CNPJ" monthly dump (every establishment, company, partner and Simples/MEI record).
DuckDB queries Parquet files directly. Values are kept in Portuguese exactly as Receita publishes them.
"""
from __future__ import annotations

import json
import os
import re
import sys
import threading
import time
from datetime import date
from pathlib import Path
from typing import Any, Optional

import duckdb
from mcp.server.fastmcp import FastMCP

MAX_ROWS = 100
STREET_TYPES = {"AV": "AVENIDA", "AVENIDA": "AVENIDA", "R": "RUA", "RUA": "RUA", "AL": "ALAMEDA", "ALAMEDA": "ALAMEDA", "TV": "TRAVESSA", "TRAV": "TRAVESSA",
                "TRAVESSA": "TRAVESSA", "ROD": "RODOVIA", "RODOVIA": "RODOVIA", "EST": "ESTRADA", "ESTR": "ESTRADA", "ESTRADA": "ESTRADA", "PC": "PRACA",
                "PCA": "PRACA", "PRACA": "PRACA", "PRAÇA": "PRACA", "LG": "LARGO", "LARGO": "LARGO", "PQ": "PARQUE", "PARQUE": "PARQUE", "VL": "VILA"}
QUERY_TIMEOUT_S = float(os.environ.get("CNPJ_QUERY_TIMEOUT", "40"))

NOTE = (
    "Data: Receita Federal open CNPJ dump (dados abertos), one monthly snapshot of every company and establishment registered in Brazil, "
    "including closed ones (situacao 'baixada' is about half of all records). Names, codes and descriptions are in Portuguese as Receita publishes them. "
    "Counts are establishments (one row per CNPJ, matriz and filiais) unless unit='empresas', which counts companies (matriz only). "
    "The registry shows the current state only: a company's earlier addresses, activities or capital are not kept. Capital social is the declared figure in BRL. "
    "Partner CPFs are masked by Receita (only 6 middle digits are published), so people with the same name cannot be told apart for certain. "
    "Partner data is personal data under LGPD even though Receita publishes it: use it for legitimate purposes only."
)
mcp = FastMCP("cnpj-mcp", instructions=NOTE)
_con: Optional[duckdb.DuckDBPyConnection] = None


def _has_key() -> bool:
    con()
    return (data_dir() / "est_cnpj").is_dir()


def _has_score() -> bool:
    return (data_dir() / "contact_score").is_dir()


_SCORE_COLS = ("contact_score", "contact_tier", "email_kind", "email_shared_companies", "email_domain_companies", "email_matches_name", "email_score",
               "phone1_kind", "phone1_shared_companies", "phone2_kind", "phone2_shared_companies", "phone_score")


def _scores(cnpjs: list[str]) -> dict[str, dict]:
    """Contact-quality score and the signals behind it, keyed by CNPJ. Empty if the score table is not built."""
    if not cnpjs or not _has_score():
        return {}
    rows = run("SELECT cnpj, " + ", ".join(_SCORE_COLS) + " FROM cs WHERE cnpj IN (" + ",".join("?" * len(cnpjs)) + ")", list(cnpjs))
    return {r["cnpj"]: r for r in rows}


def _contact_block(r: Optional[dict]) -> Optional[dict]:
    if not r:
        return None
    return {"contact_score": r["contact_score"], "contact_tier": r["contact_tier"],
            "signals": {"email": {"kind": r["email_kind"], "score": r["email_score"], "companies_sharing_this_email": r["email_shared_companies"],
                                  "companies_using_this_email_domain": r["email_domain_companies"], "email_matches_company_name": r["email_matches_name"]},
                        "phone": {"score": r["phone_score"], "phone1_kind": r["phone1_kind"], "companies_sharing_phone1": r["phone1_shared_companies"],
                                  "phone2_kind": r["phone2_kind"], "companies_sharing_phone2": r["phone2_shared_companies"]}}}


def _has_web() -> bool:
    return (data_dir() / "website_contacts.parquet").exists()


def _web_blocks(emails: list) -> dict[str, dict]:
    """Contacts found on each company's own website, keyed by email domain. Separate from Receita data and from the score."""
    doms = sorted({e.split("@")[-1].strip().lower() for e in emails if e and "@" in e})
    if not doms or not _has_web():
        return {}
    rows = run("SELECT domain, final_url, crawled_at, pages, whatsapp, phones, emails, social FROM wc WHERE status = 'ok' AND domain IN (" + ",".join("?" * len(doms)) + ")", doms)
    out = {}
    for r in rows:
        out[r["domain"]] = {
            "source": "from the company's own website (public pages, robots.txt respected); not Receita data and not part of contact_quality",
            "domain": r["domain"], "site": r["final_url"], "crawled_at": r["crawled_at"], "pages_read": r["pages"],
            "whatsapp": r["whatsapp"], "phones": r["phones"], "emails": r["emails"], "social_profiles": r["social"],
            "note": "Each value carries the page it was found on (source_url). The site is matched to the company through the domain of its registered email; a few domains serve up to 3 companies."}
    return out


def data_dir() -> Path:
    return Path(os.environ.get("CNPJ_DATA_DIR", str(Path.home() / ".cache" / "cnpj-mcp")))


def meta() -> dict:
    p = data_dir() / "meta.json"
    return json.loads(p.read_text()) if p.exists() else {}


def con() -> duckdb.DuckDBPyConnection:
    global _con
    if _con is None:
        d = data_dir()
        if not (d / "meta.json").exists():
            raise RuntimeError("Dataset is not loaded yet. Build it with `cnpj-mcp-ingest` then `cnpj-mcp-build`, see the README.")
        c = duckdb.connect()
        c.execute(f"SET memory_limit='{os.environ.get('CNPJ_MEMORY_LIMIT', '1500MB')}'")
        c.execute(f"SET threads={int(os.environ.get('CNPJ_THREADS', '2'))}")
        c.execute(f"SET temp_directory='{d}/.tmp'")
        c.execute(f"CREATE VIEW e AS SELECT * FROM read_parquet('{d}/estabelecimentos/*/*.parquet', hive_partitioning=true)")
        c.execute(f"CREATE VIEW s AS SELECT * FROM read_parquet('{d}/socios.parquet')")
        if (d / "est_cnpj").is_dir():
            c.execute(f"CREATE VIEW k AS SELECT * FROM read_parquet('{d}/est_cnpj/*.parquet')")
        if (d / "contact_score").is_dir():
            c.execute(f"CREATE VIEW cs AS SELECT * FROM read_parquet('{d}/contact_score/*.parquet')")
        if (d / "website_contacts.parquet").exists():
            c.execute(f"CREATE VIEW wc AS SELECT * FROM read_parquet('{d}/website_contacts.parquet')")
        if (d / "socios_nome.parquet").exists():
            c.execute(f"CREATE VIEW sn AS SELECT * FROM read_parquet('{d}/socios_nome.parquet')")
        for t in ("cnaes", "municipios", "naturezas", "motivos", "paises", "qualificacoes"):
            c.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{d}/{t}.parquet')")
        _con = c
    return _con


def run(sql: str, params: list[Any] | None = None) -> list[dict]:
    cur = con().cursor()
    timer = threading.Timer(QUERY_TIMEOUT_S, cur.interrupt)
    timer.start()
    try:
        cur.execute(sql, params or [])
        rows = cur.fetchall()
    except duckdb.InterruptException:
        raise RuntimeError(f"Query took longer than {QUERY_TIMEOUT_S:.0f}s and was stopped. Add a state (uf), cnae or date filter.")
    finally:
        timer.cancel()
    cols = [x[0] for x in cur.description]
    return [{k: (round(v, 2) if isinstance(v, float) else (v.isoformat() if isinstance(v, date) else v)) for k, v in zip(cols, r)} for r in rows]


# ---------------- codes and filters ----------------
UFS = set("AC AL AM AP BA CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO EX".split())
SITUACAO = {"nula": "01", "ativa": "02", "suspensa": "03", "inapta": "04", "baixada": "08"}
SITUACAO_NOME = {v: k for k, v in SITUACAO.items()}
PORTE = {"nao_informado": "00", "micro": "01", "pequeno": "03", "demais": "05"}
PORTE_NOME = {v: k for k, v in PORTE.items()}
SECOES = {"A": "Agricultura, pecuaria, producao florestal, pesca e aquicultura", "B": "Industrias extrativas", "C": "Industrias de transformacao",
          "D": "Eletricidade e gas", "E": "Agua, esgoto, residuos e descontaminacao", "F": "Construcao", "G": "Comercio; reparacao de veiculos",
          "H": "Transporte, armazenagem e correio", "I": "Alojamento e alimentacao", "J": "Informacao e comunicacao", "K": "Atividades financeiras, de seguros",
          "L": "Atividades imobiliarias", "M": "Atividades profissionais, cientificas e tecnicas", "N": "Atividades administrativas e servicos complementares",
          "O": "Administracao publica, defesa e seguridade social", "P": "Educacao", "Q": "Saude humana e servicos sociais", "R": "Artes, cultura, esporte e recreacao",
          "S": "Outras atividades de servicos", "T": "Servicos domesticos", "U": "Organismos internacionais"}
GROUPS = {
    "uf": "e.uf", "municipio": "e.municipio", "cnae_principal": "e.cnae_principal", "cnae_divisao": "e.cnae_divisao", "cnae_secao": "e.cnae_secao",
    "natureza_juridica": "e.natureza_juridica", "porte": "e.porte", "situacao": "e.situacao_cadastral", "ano_abertura": "year(e.data_inicio_atividade)",
    "mes_abertura": "strftime(e.data_inicio_atividade, '%Y-%m')", "ano_baixa": "CASE WHEN e.situacao_cadastral = '08' THEN year(e.data_situacao_cadastral) END",
    "mes_baixa": "CASE WHEN e.situacao_cadastral = '08' THEN strftime(e.data_situacao_cadastral, '%Y-%m') END",
    "mei": "e.opcao_mei", "simples": "e.opcao_simples", "matriz_filial": "e.matriz_filial", "motivo_situacao": "e.motivo_situacao_descricao",
}
GROUP_LABEL = {"municipio": "e.municipio", "cnae_principal": "e.cnae_principal || ' ' || coalesce(any_value(e.cnae_principal_descricao),'')"}


def _d(v: Optional[str], name: str, end: bool = False) -> Optional[str]:
    if v is None or v == "":
        return None
    if not re.fullmatch(r"\d{4}-\d{2}(-\d{2})?", v):
        raise ValueError(f"{name} must be YYYY-MM-DD or YYYY-MM, got {v!r}")
    try:
        if len(v) == 7:
            import calendar
            y, m = int(v[:4]), int(v[5:])
            if end:
                return f"{v}-{calendar.monthrange(y, m)[1]:02d}"
            date(y, m, 1)
            return v + "-01"
        date.fromisoformat(v)
    except ValueError:
        raise ValueError(f"{name} is not a real date: {v!r}")
    return v


def _digits(v: str, name: str, lo: int, hi: int) -> str:
    x = re.sub(r"\D", "", v or "")
    if not (lo <= len(x) <= hi):
        raise ValueError(f"{name} must have {lo}-{hi} digits, got {v!r}")
    return x


def _like(s: str) -> str:
    return "%" + s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"


def _norm(s: str) -> str:
    return s.strip()


def _filters(cnae=None, cnae_secao=None, include_secondary=False, uf=None, municipio=None, municipio_codigo=None, situacao=None, matriz_only=None,
             porte=None, natureza_juridica=None, capital_min=None, capital_max=None, opened_from=None, opened_to=None,
             closed_from=None, closed_to=None, mei=None, simples=None, name_contains=None, cep=None, partner_name=None, unit=None):
    w: list[str] = []
    p: list[Any] = []
    if cnae:
        items = [cnae] if isinstance(cnae, str) else list(cnae)
        if len(items) > 20:
            raise ValueError("cnae takes at most 20 codes")
        parts = []
        for it in items:
            x = _digits(str(it), "cnae", 2, 7)
            parts.append("e.cnae_principal LIKE ?"); p.append(x + "%")
            if include_secondary:
                parts.append("e.cnae_secundaria LIKE ?"); p.append(f"%{x}%")
        w.append("(" + " OR ".join(parts) + ")")
    if cnae_secao:
        sx = cnae_secao.upper().strip()
        if sx not in SECOES:
            raise ValueError("cnae_secao must be a CNAE section letter A-U")
        w.append("e.cnae_secao = ?"); p.append(sx)
    if uf:
        u = uf.upper().strip()
        if u not in UFS:
            raise ValueError(f"uf must be a two-letter state code (or EX for abroad), got {uf!r}")
        w.append("e.uf = ?"); p.append(u)
    if municipio_codigo:
        w.append("e.municipio_codigo = ?"); p.append(_digits(municipio_codigo, "municipio_codigo", 1, 4).zfill(4))
    if municipio:
        w.append("strip_accents(e.municipio) = strip_accents(upper(?))"); p.append(municipio.strip())
    if situacao:
        k = situacao.lower().strip()
        if k in SITUACAO:
            code = SITUACAO[k]
        elif k.zfill(2) in SITUACAO_NOME:
            code = k.zfill(2)
        else:
            raise ValueError("situacao must be one of: ativa, baixada, suspensa, inapta, nula")
        w.append("e.situacao_cadastral = ?"); p.append(code)
    if matriz_only or unit == "empresas":
        w.append("e.matriz_filial = 1")
    if porte:
        k = porte.lower().strip()
        k = {"me": "micro", "microempresa": "micro", "epp": "pequeno", "pequeno porte": "pequeno", "empresa de pequeno porte": "pequeno"}.get(k, k)
        code = PORTE.get(k) or (k if k in PORTE_NOME else None)
        if not code:
            raise ValueError("porte must be one of: micro (ME), pequeno (EPP), demais, nao_informado")
        w.append("e.porte = ?"); p.append(code)
    if natureza_juridica:
        x = natureza_juridica.strip()
        if re.fullmatch(r"\d{1,4}", x):
            w.append("e.natureza_juridica LIKE ?"); p.append(x + "%")
        else:
            w.append("strip_accents(e.natureza_juridica_descricao) ILIKE strip_accents(?) ESCAPE '\\'"); p.append(_like(x))
    if capital_min is not None:
        w.append("e.capital_social >= ?"); p.append(float(capital_min))
    if capital_max is not None:
        w.append("e.capital_social <= ?"); p.append(float(capital_max))
    for col, a, b in (("data_inicio_atividade", opened_from, opened_to), ("data_situacao_cadastral", closed_from, closed_to)):
        f, t = _d(a, "from"), _d(b, "to", end=True)
        if f and t and f > t:
            raise ValueError("date range start is after its end")
        if f:
            w.append(f"e.{col} >= CAST(? AS DATE)"); p.append(f)
        if t:
            w.append(f"e.{col} <= CAST(? AS DATE)"); p.append(t)
    if closed_from or closed_to:
        w.append("e.situacao_cadastral = '08'")
    if mei is not None:
        w.append("e.opcao_mei = 'S'" if mei else "(e.opcao_mei IS DISTINCT FROM 'S')")
    if simples is not None:
        w.append("e.opcao_simples = 'S'" if simples else "(e.opcao_simples IS DISTINCT FROM 'S')")
    if name_contains:
        w.append("(strip_accents(e.razao_social) ILIKE strip_accents(?) ESCAPE '\\' OR strip_accents(e.nome_fantasia) ILIKE strip_accents(?) ESCAPE '\\')")
        p += [_like(name_contains), _like(name_contains)]
    if cep:
        w.append("e.cep = ?"); p.append(_digits(cep, "cep", 8, 8))
    if partner_name:
        w.append("e.cnpj_basico IN (SELECT cnpj_basico FROM s WHERE strip_accents(nome_socio) ILIKE strip_accents(?) ESCAPE '\\')"); p.append(_like(partner_name))
    return (" AND ".join(w) or "TRUE"), p


def _fmt_cnpj(c: str) -> str:
    return f"{c[:2]}.{c[2:5]}.{c[5:8]}/{c[8:12]}-{c[12:]}" if c and len(c) == 14 else c


def _page(limit: int, offset: int = 0) -> tuple[int, int]:
    if int(limit) < 1:
        raise ValueError("limit must be at least 1")
    if int(offset) < 0:
        raise ValueError("offset cannot be negative")
    return min(int(limit), MAX_ROWS), int(offset)


FILTER_DOC = (
    "Filters (all optional, combined with AND): cnae (CNAE code or prefix, e.g. '4711' or '47', list ok; add include_secondary to also match secondary activities), "
    "cnae_secao (letter A-U), uf, municipio (name as in Receita, accents ignored), municipio_codigo, situacao (ativa, baixada, suspensa, inapta, nula), matriz_only, "
    "porte (micro = ME, pequeno = EPP, demais, nao_informado), natureza_juridica (code prefix like '2062' or text like 'sociedade limitada'), capital_min/capital_max (BRL), "
    "opened_from/opened_to (data_inicio_atividade, YYYY-MM or YYYY-MM-DD), closed_from/closed_to (data da baixa), mei, simples (true/false), "
    "name_contains (razao social or nome fantasia), cep, partner_name (a partner's name contains this text)."
)


@mcp.tool(description="What data is loaded: source, snapshot month, row counts, field meanings and caveats. Call first if unsure what the tools cover.")
def dataset_info() -> dict:
    m = meta()
    out_extra = {}
    if m.get("latest_opening_date"):
        out_extra["snapshot_boundary"] = (f"Receita publishes the dump mid-month: the latest opening date in {m.get('month')} is {m['latest_opening_date']}, "
                                          "so that month is partial for openings and closures. Compare full months only.")
    return {**out_extra, "note": NOTE, "snapshot_month": m.get("month"), "establishments": m.get("estabelecimentos"), "companies": m.get("empresas"), "partner_links": m.get("socios"),
            "source": "Receita Federal, Dados Abertos CNPJ (https://www.gov.br/receitafederal/pt-br/acesso-a-informacao/dados-abertos/cadastros)",
            "codes": {"situacao": SITUACAO, "porte": PORTE, "cnae_secoes": SECOES},
            "group_by_options": sorted(GROUPS)}


@mcp.tool(description="Find companies/establishments matching filters. " + FILTER_DOC + " Returns up to 100 rows per page, ordered by opening date (newest first) unless order_by is "
                      "capital_social or razao_social. Use count_companies for 'how many' questions. Set include_contacts to add email, phones, full address and a contact_quality block (0-100 score of how likely the registered email/phone is a real direct contact, with the signals behind it) and, when the company's own website was crawled, a separate website_contacts block (WhatsApp, phones, emails, social profiles found on its public pages, each with source_url). "
                      "min_contact_score keeps only companies at or above that score; it is applied after the page is read, so a call scans at most 2000 candidates and returns next_offset to continue.")
def search_companies(cnae: Optional[list[str]] = None, cnae_secao: Optional[str] = None, include_secondary: bool = False, uf: Optional[str] = None,
                     municipio: Optional[str] = None, municipio_codigo: Optional[str] = None, situacao: Optional[str] = None, matriz_only: bool = False,
                     porte: Optional[str] = None, natureza_juridica: Optional[str] = None, capital_min: Optional[float] = None, capital_max: Optional[float] = None,
                     opened_from: Optional[str] = None, opened_to: Optional[str] = None, closed_from: Optional[str] = None, closed_to: Optional[str] = None,
                     mei: Optional[bool] = None, simples: Optional[bool] = None, name_contains: Optional[str] = None, cep: Optional[str] = None,
                     partner_name: Optional[str] = None, order_by: str = "data_inicio_atividade", include_contacts: bool = False,
                     min_contact_score: Optional[int] = None, limit: int = 25, offset: int = 0) -> list[dict]:
    where, p = _filters(cnae, cnae_secao, include_secondary, uf, municipio, municipio_codigo, situacao, matriz_only, porte, natureza_juridica, capital_min,
                        capital_max, opened_from, opened_to, closed_from, closed_to, mei, simples, name_contains, cep, partner_name)
    orders = {"data_inicio_atividade": "e.data_inicio_atividade DESC NULLS LAST", "capital_social": "e.capital_social DESC NULLS LAST", "razao_social": "e.razao_social"}
    if order_by not in orders:
        raise ValueError("order_by must be data_inicio_atividade, capital_social or razao_social")
    limit, offset = _page(limit, offset)
    if min_contact_score is not None:
        if not isinstance(min_contact_score, int) or not 0 <= min_contact_score <= 100:
            raise ValueError("min_contact_score must be an integer from 0 to 100")
        if not _has_score():
            raise ValueError("The contact score table is not built on this server.")
    want_score = include_contacts or min_contact_score is not None
    extra = (", e.email, e.ddd1, e.telefone1, e.ddd2, e.telefone2, e.tipo_logradouro, e.logradouro, e.numero, e.complemento, e.bairro, e.cep") if include_contacts else ""
    sel = f"""SELECT e.cnpj, e.razao_social, e.nome_fantasia, e.situacao_cadastral, e.data_situacao_cadastral, e.data_inicio_atividade, e.matriz_filial,
        e.cnae_principal, e.cnae_principal_descricao, e.natureza_juridica, e.natureza_juridica_descricao, e.porte, e.capital_social,
        e.opcao_mei AS mei, e.opcao_simples AS simples, e.municipio, e.uf{extra}
        FROM e WHERE {where} ORDER BY {orders[order_by]}, e.cnpj"""
    if min_contact_score is None:
        rows = run(f"{sel} LIMIT {limit + 1} OFFSET {offset}", p)
        sc = _scores([r["cnpj"] for r in rows[:limit]]) if want_score else {}
    else:
        # the score lives in a side table, so filter after the page is read; scan at most 2000 candidates per call
        rows, sc, scanned, pos, more_scan, done = [], {}, 0, offset, False, False
        while not done and scanned < 2000:
            batch = run(f"{sel} LIMIT 250 OFFSET {pos}", p)
            if not batch:
                break
            got = _scores([r["cnpj"] for r in batch])
            for r in batch:
                g = got.get(r["cnpj"])
                if g and g["contact_score"] >= min_contact_score:
                    if len(rows) == limit:
                        rows.append(r)  # one extra row marks that more matches exist
                        done = True
                        break
                    sc[r["cnpj"]] = g
                    rows.append(r)
                pos += 1
                scanned += 1
            if len(batch) < 250:
                break
        else:
            more_scan = not done
    more = len(rows) > limit or (min_contact_score is not None and more_scan)
    rows = rows[:limit]
    for r in rows:
        r["cnpj_formatado"] = _fmt_cnpj(r["cnpj"])
        r["situacao"] = SITUACAO_NOME.get(r.pop("situacao_cadastral"))
        r["porte"] = PORTE_NOME.get(r["porte"], r["porte"])
        if want_score:
            r["contact_quality"] = _contact_block(sc.get(r["cnpj"]))
    if include_contacts:
        wb = _web_blocks([r.get("email") for r in rows])
        for r in rows:
            b = wb.get((r.get("email") or "").split("@")[-1].strip().lower())
            if b:
                r["website_contacts"] = b
    if more:
        rows.append({"truncated": True, "next_offset": (pos if min_contact_score is not None else offset + limit), "message": "More matches exist; raise offset or narrow filters."})
    if not rows:
        rows.append({"message": "No companies matched. Check spelling of municipio (Receita writes names in capitals, e.g. 'SAO PAULO'), and that situacao is not excluding closed companies."})
    return rows


@mcp.tool(description="Count companies or establishments, optionally grouped by one or two fields. " + FILTER_DOC +
                      " group_by options: uf, municipio, cnae_principal, cnae_divisao, cnae_secao, natureza_juridica, porte, situacao, ano_abertura, mes_abertura, ano_baixa, "
                      "mes_baixa, mei, simples, matriz_filial, motivo_situacao. unit='estabelecimentos' (default) counts every CNPJ incl. filiais; unit='empresas' counts "
                      "companies (matriz only). metric='sum_capital' sums declared capital social (BRL, companies only). Examples: MEIs opened per month in a state, "
                      "active agro companies per municipality, closures by year.")
def count_companies(group_by: Optional[list[str]] = None, unit: str = "estabelecimentos", metric: str = "count",
                    cnae: Optional[list[str]] = None, cnae_secao: Optional[str] = None, include_secondary: bool = False, uf: Optional[str] = None,
                    municipio: Optional[str] = None, municipio_codigo: Optional[str] = None, situacao: Optional[str] = None, matriz_only: bool = False,
                    porte: Optional[str] = None, natureza_juridica: Optional[str] = None, capital_min: Optional[float] = None, capital_max: Optional[float] = None,
                    opened_from: Optional[str] = None, opened_to: Optional[str] = None, closed_from: Optional[str] = None, closed_to: Optional[str] = None,
                    mei: Optional[bool] = None, simples: Optional[bool] = None, name_contains: Optional[str] = None, cep: Optional[str] = None,
                    partner_name: Optional[str] = None, order: str = "count_desc", limit: int = 25) -> list[dict]:
    if unit not in ("estabelecimentos", "empresas"):
        raise ValueError("unit must be estabelecimentos or empresas")
    if metric not in ("count", "sum_capital"):
        raise ValueError("metric must be count or sum_capital")
    gb = group_by or []
    if isinstance(gb, str):
        gb = [gb]
    if len(gb) > 2:
        raise ValueError("group_by takes at most two fields")
    for g in gb:
        if g not in GROUPS:
            raise ValueError(f"group_by {g!r} not supported; options: {', '.join(sorted(GROUPS))}")
    if order not in ("count_desc", "key_asc", "key_desc"):
        raise ValueError("order must be count_desc, key_asc or key_desc")
    where, p = _filters(cnae, cnae_secao, include_secondary, uf, municipio, municipio_codigo, situacao, matriz_only, porte, natureza_juridica, capital_min,
                        capital_max, opened_from, opened_to, closed_from, closed_to, mei, simples, name_contains, cep, partner_name, unit)
    if metric == "sum_capital":
        where += " AND e.matriz_filial = 1"
    limit, _ = _page(limit, 0)
    val = "count(*)" if metric == "count" else "sum(e.capital_social)"
    alias = "total" if metric == "count" else "capital_social_total_brl"
    sel = ", ".join(f"{GROUPS[g]} AS {g}" for g in gb)
    if "municipio" in gb:
        sel += ", any_value(e.uf) AS uf_do_municipio" if "uf" not in gb else ""
    grp = " GROUP BY " + ", ".join(str(i + 1) for i in range(len(gb))) if gb else ""
    ordr = {"count_desc": f" ORDER BY {alias} DESC", "key_asc": " ORDER BY 1", "key_desc": " ORDER BY 1 DESC"}[order] if gb else ""
    if "municipio" in gb:
        grp = " GROUP BY " + ", ".join(GROUPS[g] for g in gb)
    rows = run(f"SELECT {sel + ', ' if sel else ''}{val} AS {alias} FROM e WHERE {where}{grp}{ordr} LIMIT {limit}", p)
    for r in rows:
        if "situacao" in r:
            r["situacao"] = SITUACAO_NOME.get(r["situacao"], r["situacao"])
        if "porte" in r:
            r["porte"] = PORTE_NOME.get(r["porte"], r["porte"])
    return rows


@mcp.tool(description="One company by CNPJ (14 digits, with or without punctuation; or the 8-digit root to get the matriz): registration, company data, Simples/MEI, "
                      "address and contacts, secondary CNAEs, partners (with names), and how many establishments the company has.")
def get_company(cnpj: str) -> dict:
    x = _digits(cnpj, "cnpj", 8, 14)
    if len(x) not in (8, 14):
        raise ValueError("cnpj must be 14 digits (or the 8-digit root)")
    ix = _has_key()
    if ix:
        where, p = ("e.cnpj >= ? AND e.cnpj <= ? AND e.matriz_filial = 1", [x + "000000", x + "999999"]) if len(x) == 8 else ("e.cnpj = ?", [x])
        rows = run(f"SELECT e.* EXCLUDE (uf) , e.uf FROM k AS e WHERE {where} ORDER BY e.cnpj LIMIT 1", p)
    else:
        where, p = ("e.cnpj_basico = ? AND e.matriz_filial = 1", [x]) if len(x) == 8 else ("e.cnpj = ?", [x])
        rows = run(f"SELECT e.* EXCLUDE (uf) , e.uf FROM e WHERE {where} LIMIT 1", p)
    if not rows:
        return {"error": f"CNPJ {cnpj} is not in this snapshot. It may be invalid, newer than the snapshot, or the number was never issued."}
    r = rows[0]
    base = r["cnpj_basico"]
    r["cnpj_formatado"] = _fmt_cnpj(r["cnpj"])
    r["situacao"] = SITUACAO_NOME.get(r["situacao_cadastral"])
    r["porte_nome"] = PORTE_NOME.get(r["porte"])
    r["cnae_secundaria_descricoes"] = []
    if r.get("cnae_secundaria"):
        codes = r["cnae_secundaria"].split(",")[:50]
        r["cnae_secundaria_descricoes"] = run("SELECT codigo, descricao FROM cnaes WHERE codigo IN (" + ",".join("?" * len(codes)) + ")", codes)
    if ix:
        r["estabelecimentos_total"] = run("SELECT count(*) AS n FROM k WHERE cnpj >= ? AND cnpj <= ?", [base + "000000", base + "999999"])[0]["n"]
    else:
        r["estabelecimentos_total"] = run("SELECT count(*) AS n FROM e WHERE cnpj_basico = ?", [base])[0]["n"]
    sc = _scores([r["cnpj"]]).get(r["cnpj"])
    if sc:
        r["contact_quality"] = _contact_block(sc)
    wb = _web_blocks([r.get("email")])
    if wb:
        r["website_contacts"] = next(iter(wb.values()))
    r["socios"] = run("SELECT nome_socio, CASE identificador_socio WHEN 1 THEN 'pessoa juridica' WHEN 2 THEN 'pessoa fisica' ELSE 'estrangeiro' END AS tipo, "
                      "cnpj_cpf_socio, qualificacao_socio_descricao AS qualificacao, data_entrada, faixa_etaria, nome_representante FROM s WHERE cnpj_basico = ? "
                      "ORDER BY data_entrada LIMIT 100", [base])
    return r


@mcp.tool(description="Partners (socios) of one company by CNPJ root or full CNPJ: names, type, role (qualificacao), entry date, age band. "
                      "CPFs are masked by Receita. faixa_etaria: 1 = 0-12, 2 = 13-20, 3 = 21-30, 4 = 31-40, 5 = 41-50, 6 = 51-60, 7 = 61-70, 8 = 71-80, 9 = over 80, 0 = not applicable.")
def list_partners(cnpj: str) -> list[dict]:
    x = _digits(cnpj, "cnpj", 8, 14)[:8]
    rows = run("SELECT nome_socio, identificador_socio, cnpj_cpf_socio, qualificacao_socio_descricao AS qualificacao, data_entrada, faixa_etaria, "
               "nome_representante FROM s WHERE cnpj_basico = ? ORDER BY data_entrada LIMIT 200", [x])
    return rows or [{"message": "No partner records for this CNPJ (common for MEI and sole proprietors: they have no socios)."}]


@mcp.tool(description="Find the companies a person or entity is a partner of, by name. Exact (accent-insensitive, case-insensitive) match on the partner name by default; "
                      "set contains=true for a substring match. Optionally narrow with cpf_middle (the 6 digits Receita publishes, e.g. '240659') because the same name "
                      "can belong to different people. Returns company name, status and role.")
def search_partners(name: str, contains: bool = False, cpf_middle: Optional[str] = None, uf: Optional[str] = None, limit: int = 25, offset: int = 0) -> list[dict]:
    if not name or len(name.strip()) < 4:
        raise ValueError("name must have at least 4 characters")
    w = ["strip_accents(s.nome_socio) ILIKE strip_accents(?) ESCAPE '\\'" if contains else "strip_accents(upper(s.nome_socio)) = strip_accents(upper(?))"]
    p: list[Any] = [_like(name.strip()) if contains else name.strip()]
    if cpf_middle:
        w.append("s.cnpj_cpf_socio = ?"); p.append("***" + _digits(cpf_middle, "cpf_middle", 6, 6) + "**")
    ew = ""
    if uf:
        if uf.upper() not in UFS:
            raise ValueError("uf must be a two-letter state code")
        ew = " AND e.uf = ?"; p.append(uf.upper())
    if not contains and (data_dir() / "socios_nome.parquet").exists():
        src = "sn"; w[0] = "nome_key = strip_accents(upper(?))"
    else:
        src = "s"
    limit, offset = _page(limit, offset)
    if src == "sn" and _has_key():
        # fast path: exact name via the name index, then primary-key lookups of the matriz rows
        np = p[: len(p) - (1 if uf else 0)]
        cand = run(f"SELECT nome_socio, cnpj_cpf_socio, qualificacao_socio_descricao AS qualificacao, data_entrada, cnpj_basico FROM sn AS s WHERE {' AND '.join(w)} LIMIT 5000", np)
        out = []
        bases = sorted({c["cnpj_basico"] for c in cand})
        if len(bases) > 300:
            cand = None  # very common name: use the join path below, which can filter by state first
        if cand is not None and cand:
            info = {}
            for i in range(0, len(bases), 100):
                part = bases[i:i + 100]
                q = ("SELECT cnpj, razao_social, situacao_cadastral, data_inicio_atividade, cnae_principal_descricao, municipio, uf FROM k WHERE matriz_filial = 1 AND ("
                     + " OR ".join("(cnpj >= ? AND cnpj <= ?)" for _ in part) + ")" + (" AND uf = ?" if uf else ""))
                prm = [v for b in part for v in (b + "000000", b + "999999")] + ([uf.upper()] if uf else [])
                for r in run(q, prm):
                    info.setdefault(r["cnpj"][:8], r)
            for c in cand:
                r = info.get(c["cnpj_basico"])
                if r:
                    out.append({"nome_socio": c["nome_socio"], "cnpj_cpf_socio": c["cnpj_cpf_socio"], "qualificacao": c["qualificacao"], "data_entrada": c["data_entrada"], **r})
            out.sort(key=lambda r: str(r["data_entrada"] or ""), reverse=True)
        if cand is not None:
            more = len(out) > offset + limit
            rows = out[offset: offset + limit]
            for r in rows:
                r["situacao"] = SITUACAO_NOME.get(r.pop("situacao_cadastral"))
                r["cnpj_formatado"] = _fmt_cnpj(r["cnpj"])
            if more:
                rows.append({"truncated": True, "next_offset": offset + limit})
            return rows or [{"message": "No partner matched. Names are stored in capitals as Receita publishes them; try contains=true."}]
    rows = run(f"""WITH s AS MATERIALIZED (SELECT * FROM {src} AS s WHERE {' AND '.join(w)})
        SELECT s.nome_socio, s.cnpj_cpf_socio, s.qualificacao_socio_descricao AS qualificacao, s.data_entrada, e.cnpj, e.razao_social, e.situacao_cadastral,
        e.data_inicio_atividade, e.cnae_principal_descricao, e.municipio, e.uf
        FROM s JOIN e ON e.cnpj_basico = s.cnpj_basico AND e.matriz_filial = 1
        WHERE TRUE{ew} ORDER BY s.data_entrada DESC NULLS LAST LIMIT {limit + 1} OFFSET {offset}""", p)
    more = len(rows) > limit
    rows = rows[:limit]
    for r in rows:
        r["situacao"] = SITUACAO_NOME.get(r.pop("situacao_cadastral"))
        r["cnpj_formatado"] = _fmt_cnpj(r["cnpj"])
    if more:
        rows.append({"truncated": True, "next_offset": offset + limit})
    return rows or [{"message": "No partner matched. Names are stored in capitals as Receita publishes them; try contains=true."}]


@mcp.tool(description="Establishments registered at one address, by CEP (8 digits), optionally narrowed by street text, number, municipio or uf. Use it to find companies sharing an address.")
def companies_at_address(cep: Optional[str] = None, logradouro_contains: Optional[str] = None, numero: Optional[str] = None, municipio: Optional[str] = None,
                         uf: Optional[str] = None, situacao: Optional[str] = "ativa", limit: int = 50, offset: int = 0) -> list[dict]:
    if not cep and not logradouro_contains:
        raise ValueError("give a cep or logradouro_contains")
    w, p = [], []
    if cep:
        w.append("e.cep = ?"); p.append(_digits(cep, "cep", 8, 8))
    if logradouro_contains:
        text = logradouro_contains.strip()
        first, _, rest = text.partition(" ")
        tipo = STREET_TYPES.get(first.upper().rstrip(".")) if rest.strip() else None
        if tipo:
            w.append("e.tipo_logradouro = ?"); p.append(tipo)
            text = rest.strip()
        w.append("strip_accents(e.logradouro) ILIKE strip_accents(?) ESCAPE '\\'"); p.append(_like(text))
    if numero:
        w.append("e.numero = ?"); p.append(numero.strip())
    if municipio:
        w.append("strip_accents(e.municipio) = strip_accents(upper(?))"); p.append(municipio.strip())
    if uf:
        if uf.upper() not in UFS:
            raise ValueError("uf must be a two-letter state code")
        w.append("e.uf = ?"); p.append(uf.upper())
    if situacao:
        k = situacao.lower()
        if k not in SITUACAO:
            raise ValueError("situacao must be ativa, baixada, suspensa, inapta or nula (or omit for all)")
        w.append("e.situacao_cadastral = ?"); p.append(SITUACAO[k])
    limit, offset = _page(limit, offset)
    where = " AND ".join(w)
    total = run(f"SELECT count(*) AS n FROM e WHERE {where}", p)[0]["n"]
    rows = run(f"""SELECT e.cnpj, e.razao_social, e.nome_fantasia, e.situacao_cadastral, e.data_inicio_atividade, e.cnae_principal_descricao,
        e.tipo_logradouro, e.logradouro, e.numero, e.complemento, e.bairro, e.cep, e.municipio, e.uf
        FROM e WHERE {where} ORDER BY e.complemento, e.cnpj LIMIT {limit} OFFSET {offset}""", p)
    for r in rows:
        r["situacao"] = SITUACAO_NOME.get(r.pop("situacao_cadastral"))
        r["cnpj_formatado"] = _fmt_cnpj(r["cnpj"])
    return [{"total_matching": total, "returned": len(rows), "next_offset": offset + limit if offset + limit < total else None}] + rows


@mcp.tool(description="Look up Receita's code tables by text or code. kind: cnae (activity), natureza_juridica (legal form), municipio, motivo (reason for status), "
                      "qualificacao (partner role), pais. Use it to find the CNAE code for a sector, or the municipio name Receita uses, before filtering.")
def lookup_codes(kind: str, text: str, limit: int = 25) -> list[dict]:
    tabs = {"cnae": "cnaes", "natureza_juridica": "naturezas", "municipio": "municipios", "motivo": "motivos", "qualificacao": "qualificacoes", "pais": "paises"}
    if kind not in tabs:
        raise ValueError(f"kind must be one of: {', '.join(tabs)}")
    if not text or not text.strip():
        raise ValueError("text is required")
    limit, _ = _page(limit, 0)
    t = text.strip()
    if re.fullmatch(r"\d+", t):
        return run(f"SELECT codigo, descricao FROM {tabs[kind]} WHERE codigo LIKE ? ORDER BY codigo LIMIT {limit}", [t + "%"])
    words = [w for w in re.split(r"\s+", t) if w]
    cond = " AND ".join("strip_accents(descricao) ILIKE strip_accents(?) ESCAPE '\\'" for _ in words)
    rows = run(f"SELECT codigo, descricao FROM {tabs[kind]} WHERE {cond} ORDER BY codigo LIMIT {limit}", [_like(w) for w in words])
    if kind == "municipio":
        for r in rows:
            r["uf"] = (run("SELECT any_value(uf) AS uf FROM e WHERE municipio_codigo = ? AND uf <> 'EX'", [r["codigo"]]) or [{}])[0].get("uf") if len(rows) <= 10 else None
    return rows or [{"message": "No match. Receita descriptions are in Portuguese; try a shorter word."}]


# ---------------- hosting ----------------
class RateLimit:
    def __init__(self, app, per_minute: int):
        self.app, self.per_minute, self.hits = app, per_minute, {}

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and scope["path"] != "/healthz":
            h = dict(scope["headers"])
            ip = (h.get(b"fly-client-ip") or h.get(b"x-forwarded-for", b"").split(b",")[0] or b"?").decode().strip()
            now = time.time()
            q_ = [t for t in self.hits.get(ip, []) if now - t < 60]
            if len(q_) >= self.per_minute:
                await send({"type": "http.response.start", "status": 429, "headers": [(b"content-type", b"application/json"), (b"retry-after", b"60")]})
                await send({"type": "http.response.body", "body": b'{"error":"rate limit exceeded, try again in a minute"}'})
                return
            q_.append(now)
            self.hits[ip] = q_
            if len(self.hits) > 5000:
                self.hits = {k: v for k, v in self.hits.items() if v and now - v[-1] < 60}
        await self.app(scope, receive, send)


def _forbid_extra_arguments() -> None:
    for t in mcp._tool_manager.list_tools():
        model = t.fn_metadata.arg_model
        model.model_config["extra"] = "forbid"
        model.model_rebuild(force=True)


_forbid_extra_arguments()


def http_app():
    from mcp.server.transport_security import TransportSecuritySettings
    from starlette.responses import JSONResponse
    hosts = [h for h in os.environ.get("CNPJ_ALLOWED_HOSTS", "").split(",") if h]
    mcp.settings.stateless_http = True
    mcp.settings.json_response = True
    mcp.settings.transport_security = TransportSecuritySettings(
        enable_dns_rebinding_protection=bool(hosts), allowed_hosts=hosts, allowed_origins=["*"] if hosts else [])

    @mcp.custom_route("/healthz", methods=["GET"])
    async def healthz(request):
        return JSONResponse({"ok": True, "snapshot": meta().get("month")})

    return RateLimit(mcp.streamable_http_app(), int(os.environ.get("CNPJ_RATE_PER_MIN", "60")))


def main() -> None:
    argv = sys.argv[1:]
    if "--check" in argv:
        con()
        print(dataset_info())
        return
    if "--http" in argv:
        import uvicorn
        uvicorn.run(http_app(), host=os.environ.get("HOST", "0.0.0.0"), port=int(os.environ.get("PORT", "8080")), log_level="warning", timeout_keep_alive=5)
        return
    mcp.run()


if __name__ == "__main__":
    main()
