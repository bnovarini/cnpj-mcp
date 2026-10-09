"""Decide whether a crawled website belongs to a company, from what the site itself says.

Levels, per (domain, cnpj):
  confirmed  the company's CNPJ (or its 8-digit root) is printed on the site, or the company name is on the site
             together with its registered CEP or phone
  likely     the company name appears on the site (title, heading, copyright line or body text) with no second signal,
             or a single distinctive word of the name appears together with the registered city, CEP or phone
  candidate  nothing on the site ties it to the company; the only link is the domain of the registered email
"""
import re, unicodedata

LEGAL = {"ltda", "ltd", "me", "epp", "eireli", "sa", "s", "a", "mei", "ss", "cia", "slu", "ei", "e", "de", "da", "do", "das", "dos", "d"}
GENERIC = {"comercio", "servicos", "servico", "brasil", "brasileira", "industria", "empresa", "grupo", "transportes", "transporte", "construcoes",
           "construtora", "distribuidora", "representacoes", "importacao", "exportacao", "participacoes", "consultoria", "assessoria", "engenharia",
           "tecnologia", "solucoes", "produtos", "materiais", "equipamentos", "comercial", "industrial", "associacao", "sociedade", "clinica", "loja",
           "restaurante", "academia", "escola", "centro", "nacional", "internacional", "geral", "express", "brasileiro", "digital", "studio", "estudio"}
CNPJ_RE = re.compile(r"(?<!\d)(\d{2})\.?(\d{3})\.?(\d{3})\s?/\s?(\d{4})\s?-?\s?(\d{2})(?!\d)")
CEP_RE = re.compile(r"(?<!\d)(\d{5})-?(\d{3})(?!\d)")
PHONE_RE = re.compile(r"(?<!\d)(?:\+?55\s?)?\(?0?(\d{2})\)?[\s.-]?(9?\d{4})[\s.-]?(\d{4})(?!\d)")
TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.I | re.S)
H_RE = re.compile(r"<h[12][^>]*>(.*?)</h[12]>", re.I | re.S)
META_RE = re.compile(r"""<meta[^>]+(?:property|name)=["'](?:og:site_name|og:title|application-name|twitter:title)["'][^>]*content=["']([^"']{1,200})["']""", re.I)
META2_RE = re.compile(r"""<meta[^>]+content=["']([^"']{1,200})["'][^>]+(?:property|name)=["'](?:og:site_name|og:title|application-name)["']""", re.I)
COPY_RE = re.compile(r"(?:©|&copy;|&#169;|copyright|todos os direitos|direitos reservados)[^<]{0,160}", re.I)
SCRIPT_RE = re.compile(r"<(script|style|noscript|svg)\b.*?</\1>", re.I | re.S)
TAG_RE = re.compile(r"<[^>]+>")


def norm(s):
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    return re.sub(r"[^a-z0-9]+", " ", s).strip()


def name_tokens(name):
    t = [w for w in norm(name).split() if w not in LEGAL]
    while t and re.fullmatch(r"\d{11,14}", t[-1]):  # MEI trade names end with the owner's CPF digits
        t.pop()
    return t


def phrases(razao, fantasia):
    out = []
    for n in (fantasia, razao):
        t = name_tokens(n)
        if len(t) >= 2 and sum(map(len, t)) >= 10:
            out.append(("multi", " ".join(t)))
        elif len(t) == 1 and len(t[0]) >= 7 and t[0] not in GENERIC:
            out.append(("single", t[0]))
    return out


def page_signals(html):
    """Reduce one page to what ident matching needs."""
    html = re.sub(r"[A-Za-z0-9+/=_.%-]{150,}", " ", html[:400_000])
    prom = [m for m in TITLE_RE.findall(html)[:1]] + H_RE.findall(html)[:6] + META_RE.findall(html)[:4] + META2_RE.findall(html)[:4] + [m for m in COPY_RE.findall(html)[:6]]
    from html import unescape
    prom_t = " | ".join(norm(unescape(TAG_RE.sub(" ", p))) for p in prom)
    body = unescape(TAG_RE.sub(" ", SCRIPT_RE.sub(" ", html)))
    return {"prom": " " + prom_t + " ", "body_n": " " + norm(body)[:150_000] + " ", "raw": body[:150_000]}


def combine(sigs):
    return {"prom": " ".join(s["prom"] for s in sigs), "body_n": " ".join(s["body_n"] for s in sigs), "raw": " ".join(s["raw"] for s in sigs)}


def evaluate(sig, comp):
    """comp = dict(cnpj, razao, fantasia, municipio, cep, tels). Returns (level, evidence list)."""
    ev = []
    cn = {"".join(m) for m in CNPJ_RE.findall(sig["raw"])}
    root = comp["cnpj"][:8]
    if comp["cnpj"] in cn:
        return "confirmed", ["cnpj_on_page"]
    if any(c[:8] == root for c in cn):
        return "confirmed", ["cnpj_root_on_page"]
    geo = []
    cep = (comp.get("cep") or "").replace("-", "")
    if cep and any("".join(m) == cep for m in CEP_RE.findall(sig["raw"])):
        geo.append("cep")
    tels = set(comp.get("tels") or [])
    if tels:
        pg = {a + b + c for a, b, c in PHONE_RE.findall(sig["raw"])}
        if tels & pg:
            geo.append("phone")
    mun = norm(comp.get("municipio"))
    city = bool(mun) and (" " + mun + " ") in sig["body_n"]
    best = None
    for kind, ph in phrases(comp.get("razao"), comp.get("fantasia")):
        key = " " + ph + " "
        where = "title/heading/copyright" if key in sig["prom"] else ("body" if key in sig["body_n"] else None)
        if where:
            ev.append(f"name_{kind}_in_{where}")
            if best is None or (kind == "multi" and best[0] == "single"):
                best = (kind, where, len(ph))
    if best is None:
        return "candidate", []
    kind, where, plen = best
    if geo:
        ev += geo
        return ("confirmed" if kind == "multi" else "likely"), ev
    if city:
        ev.append("city")
        return ("confirmed" if kind == "multi" and where != "body" else "likely"), ev
    if kind == "multi" or (where != "body" and plen >= 8):
        return "likely", ev
    return "candidate", ev
