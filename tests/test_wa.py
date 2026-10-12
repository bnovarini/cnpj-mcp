import os, tempfile, time
from cnpj_mcp import wa


def setup_module(m):
    os.environ["CNPJ_WA_DB"] = tempfile.mktemp(suffix=".db")
    wa.SPACING = (0.0, 0.0)


BIZ = '<meta name="description" content="Business Account" /><meta property="og:title" content="Inova Contabil" /><img src="https://pps.whatsapp.net/x.jpg">'
OFF = '<meta name="description" content="Official Business Account" /><meta property="og:title" content="Acme Arquitetura" />'
GEN = '<meta property="og:title" content="Share on WhatsApp" /><meta name="description" content="WhatsApp Messenger: More than 2 billion" />'


def test_variants():
    assert wa.variants("11", "88525431") == ["5511988525431", "551188525431"]
    assert wa.variants("11", "40769694") == ["551140769694"]
    assert wa.variants("11", "988525431") == ["5511988525431"]
    assert wa.variants("", "123") == [] and wa.variants("11", "123") == []


def test_parse():
    assert wa.parse(BIZ) == ("Inova Contabil", "business", True)
    assert wa.parse(OFF)[1] == "official_business"
    assert wa.parse(GEN)[1] is None
    assert wa.parse("<html></html>") == (None, None, False)


def test_name_match():
    assert wa.name_match("Inova Contabil", "INOVA CONTABIL LTDA", None)["score"] >= 85
    assert wa.name_match("Padaria Sol", "INOVA CONTABIL LTDA", None)["score"] < 40
    assert wa.name_match(None, "X", None) is None


def test_flow_positive_only(monkeypatch=None):
    pages = {"5511988525431": GEN, "551188525431": BIZ, "551140769694": GEN}
    calls = []

    def fake(n):
        calls.append(n)
        return 200, pages.get(n, GEN)
    wa.fetch_page = fake
    co = {"cnpj": "1", "razao_social": "INOVA CONTABIL LTDA", "nome_fantasia": None, "ddd1": "11", "telefone1": "88525431", "ddd2": "11", "telefone2": "40769694"}
    rows = [dict(co)]
    wa.enrich_rows(rows)
    assert "whatsapp_business" not in rows[0]          # first call never waits on the network
    for _ in range(50):
        time.sleep(0.1)
        if "551188525431" in calls:
            break
    time.sleep(0.5)
    rows = [dict(co)]
    wa.enrich_rows(rows)
    b = rows[0]["whatsapp_business"]
    assert b["number"] == "+551188525431" and b["wa_account_type"] == "business" and b["name_match"]["score"] >= 85
    c = wa._conn()
    neg = c.execute("SELECT wa_business_name, wa_account_type FROM wa_check WHERE number='5511988525431'").fetchone()
    assert neg == (None, None)                          # no-signal: timestamp only, never a verdict
    n = len(calls)
    wa.enrich_rows([dict(co)]); time.sleep(0.5)
    assert len(calls) == n                              # cached: nothing re-fetched


def test_failed_fetch_not_cached():
    wa.fetch_page = lambda n: (429, "")
    co = {"cnpj": "2", "ddd1": "21", "telefone1": "22223333"}
    wa.enrich_rows([co]); time.sleep(1)
    c = wa._conn()
    assert c.execute("SELECT count(*) FROM wa_check WHERE number='552122223333'").fetchone()[0] == 0
