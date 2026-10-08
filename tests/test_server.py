"""Builds a tiny synthetic dataset with the real build step and exercises the tools."""
import importlib
import os

import duckdb
import pytest


@pytest.fixture(scope="module")
def srv(tmp_path_factory):
    root = tmp_path_factory.mktemp("cnpj")
    raw, out = root / "raw", root / "out"
    c = duckdb.connect()
    def w(table, rows, cols):
        d = raw / table
        d.mkdir(parents=True)
        vals = ", ".join("(" + ", ".join("'" + str(x).replace("'", "''") + "'" for x in r) + ")" for r in rows)
        c.execute(f"COPY (SELECT * FROM (VALUES {vals}) t({', '.join(cols)})) TO '{d}/x.parquet' (FORMAT parquet)")
    from cnpj_mcp.ingest import COLS
    w("cnaes", [("0115600", "Cultivo de soja"), ("4711302", "Comercio varejista de mercadorias em geral")], COLS["cnaes"])
    w("municipios", [("7107", "SAO PAULO"), ("5107", "SINOP")], COLS["municipios"])
    w("naturezas", [("2062", "Sociedade Empresaria Limitada"), ("2135", "Empresario (Individual)")], COLS["naturezas"])
    w("motivos", [("00", "SEM MOTIVO")], COLS["motivos"])
    w("paises", [("105", "BRASIL")], COLS["paises"])
    w("qualificacoes", [("49", "Socio-Administrador")], COLS["qualificacoes"])
    w("empresas", [("11111111", "SOJA SINOP LTDA", "2062", "49", "500000,00", "05", ""),
                   ("22222222", "MERCADINHO DO ZE ME", "2135", "50", "5000,00", "01", "")], COLS["empresas"])
    e = lambda b, o, dv, mf, fant, sit, dsit, ini, cnae, uf, mun, cep: (b, o, dv, mf, fant, sit, dsit, "00", "", "", ini, cnae, "", "RUA", "A", "10", "", "CENTRO", cep, uf, mun, "", "", "", "", "", "", "", "", "")
    w("estabelecimentos", [e("11111111", "0001", "91", "1", "SOJA", "02", "20200101", "20200101", "0115600", "MT", "5107", "78550000"),
                           e("22222222", "0001", "55", "1", "ZE", "08", "20240601", "20230315", "4711302", "SP", "7107", "01000000"),
                           e("22222222", "0002", "36", "2", "ZE FILIAL", "02", "20230401", "20230401", "4711302", "SP", "7107", "01000000")], COLS["estabelecimentos"])
    w("simples", [("11111111", "N", "0", "0", "N", "0", "0"), ("22222222", "S", "20230315", "0", "S", "20230315", "0")], COLS["simples"])
    w("socios", [("11111111", "2", "MARIA DA SILVA", "***123456**", "49", "20200101", "", "***000000**", "", "00", "5")], COLS["socios"])
    from cnpj_mcp import build
    build.build(str(raw), str(out), "2026-09", memory="300MB", threads=1)
    os.environ["CNPJ_DATA_DIR"] = str(out)
    import cnpj_mcp.mcp_server as m
    importlib.reload(m)
    return m


def test_counts_and_units(srv):
    assert srv.count_companies(group_by=["situacao"], order="key_asc") == [{"situacao": "ativa", "total": 2}, {"situacao": "baixada", "total": 1}]
    assert srv.count_companies(unit="empresas", situacao="ativa") == [{"total": 1}]
    assert srv.count_companies(mei=True, group_by=["uf"]) == [{"uf": "SP", "total": 2}]


def test_filters(srv):
    r = srv.search_companies(municipio="sinop", cnae=["01"], opened_from="2020-01")
    assert [x["cnpj"] for x in r] == ["11111111000191"]
    assert srv.search_companies(partner_name="maria silva")[0].get("message")  # words must be contiguous
    assert srv.search_companies(partner_name="SILVA")[0]["razao_social"] == "SOJA SINOP LTDA"
    assert srv.search_companies(natureza_juridica="limitada", capital_min=100000)[0]["porte"] == "demais"


def test_company_partners_address(srv):
    c = srv.get_company("11.111.111/0001-91")
    assert c["estabelecimentos_total"] == 1 and c["socios"][0]["nome_socio"] == "MARIA DA SILVA"
    assert srv.get_company("00000000000000")["error"]
    assert srv.search_partners("Maria da Silva", cpf_middle="123456")[0]["cnpj"] == "11111111000191"
    a = srv.companies_at_address(cep="01000-000")
    assert a[0]["total_matching"] == 1


def test_validation(srv):
    for bad in (dict(uf="ZZ"), dict(situacao="x"), dict(opened_from="2020-13"), dict(opened_from="2024-02", opened_to="2023-01")):
        with pytest.raises(ValueError):
            srv.count_companies(**bad)
    with pytest.raises(ValueError):
        srv.count_companies(group_by=["uf", "porte", "mei"])
    assert srv.lookup_codes("cnae", "soja")[0]["codigo"] == "0115600"
