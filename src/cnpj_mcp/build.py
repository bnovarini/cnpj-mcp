"""Build typed, query-ready Parquet from the raw per-file Parquet written by ingest.py.

Outputs (under <out>):
  estabelecimentos/uf=XX/*.parquet  one row per establishment, joined with its company, Simples/MEI flags and code descriptions
  socios.parquet                    one row per partner link, sorted by cnpj_basico
  cnaes.parquet, municipios.parquet, naturezas.parquet, motivos.parquet, paises.parquet, qualificacoes.parquet
  meta.json                         month, row counts
"""
from __future__ import annotations
import json, os, sys, time
import duckdb

DATE = "try_strptime(NULLIF(NULLIF({c}, '0'), '00000000'), '%Y%m%d')::DATE"

SECAO = """CASE
 WHEN _d BETWEEN 1 AND 3 THEN 'A' WHEN _d BETWEEN 5 AND 9 THEN 'B' WHEN _d BETWEEN 10 AND 33 THEN 'C' WHEN _d = 35 THEN 'D'
 WHEN _d BETWEEN 36 AND 39 THEN 'E' WHEN _d BETWEEN 41 AND 43 THEN 'F' WHEN _d BETWEEN 45 AND 47 THEN 'G' WHEN _d BETWEEN 49 AND 53 THEN 'H'
 WHEN _d BETWEEN 55 AND 56 THEN 'I' WHEN _d BETWEEN 58 AND 63 THEN 'J' WHEN _d BETWEEN 64 AND 66 THEN 'K' WHEN _d = 68 THEN 'L'
 WHEN _d BETWEEN 69 AND 75 THEN 'M' WHEN _d BETWEEN 77 AND 82 THEN 'N' WHEN _d = 84 THEN 'O' WHEN _d = 85 THEN 'P'
 WHEN _d BETWEEN 86 AND 88 THEN 'Q' WHEN _d BETWEEN 90 AND 93 THEN 'R' WHEN _d BETWEEN 94 AND 96 THEN 'S' WHEN _d = 97 THEN 'T'
 WHEN _d = 99 THEN 'U' END"""


def build(raw: str, out: str, month: str, memory: str = "1200MB", threads: int = 2, tmp: str | None = None) -> dict:
    os.makedirs(out, exist_ok=True)
    c = duckdb.connect()
    c.execute(f"SET memory_limit='{memory}'")
    c.execute(f"SET threads={threads}")
    c.execute(f"SET temp_directory='{tmp or os.path.join(out, '.tmp')}'")
    c.execute("SET preserve_insertion_order=false")
    meta: dict = {"month": month}
    t0 = time.time()
    for name in ("cnaes", "municipios", "naturezas", "motivos", "paises", "qualificacoes"):
        c.execute(f"COPY (SELECT * FROM read_parquet('{raw}/{name}/*.parquet') ORDER BY codigo) TO '{out}/{name}.parquet' (FORMAT parquet, COMPRESSION zstd)")
    print("lookups", round(time.time() - t0), flush=True)

    c.execute(f"""CREATE VIEW emp_base AS SELECT cnpj_basico, razao_social, natureza_juridica,
        qualificacao_responsavel, TRY_CAST(replace(capital_social, ',', '.') AS DOUBLE) AS capital_social, porte, NULLIF(ente_federativo, '') AS ente_federativo
        FROM read_parquet('{raw}/empresas/*.parquet')
        QUALIFY row_number() OVER (PARTITION BY cnpj_basico ORDER BY (razao_social = ''), length(razao_social) DESC) = 1""")
    c.execute(f"""CREATE VIEW sim_base AS SELECT cnpj_basico, NULLIF(opcao_simples,'') AS opcao_simples, {DATE.format(c='data_opcao_simples')} AS data_opcao_simples,
        {DATE.format(c='data_exclusao_simples')} AS data_exclusao_simples, NULLIF(opcao_mei,'') AS opcao_mei,
        {DATE.format(c='data_opcao_mei')} AS data_opcao_mei, {DATE.format(c='data_exclusao_mei')} AS data_exclusao_mei
        FROM read_parquet('{raw}/simples/*.parquet')""")
    c.execute(f"CREATE VIEW est_base AS SELECT * FROM read_parquet('{raw}/estabelecimentos/*.parquet')")
    BUCKETS = [(i, i + 5) for i in range(0, 100, 5)]

    def set_bucket(lo: int, hi: int) -> None:
        # every table is keyed by cnpj_basico, so one slice of the key space joins on its own in bounded memory
        w = f"cnpj_basico >= '{lo:02d}'" + (f" AND cnpj_basico < '{hi:02d}'" if hi < 100 else "")
        for v in ("emp", "sim", "est"):
            c.execute(f"CREATE OR REPLACE VIEW {v} AS SELECT * FROM {v}_base WHERE {w}")

    sql = f"""
      SELECT e.cnpj_basico || e.cnpj_ordem || e.cnpj_dv AS cnpj, e.cnpj_basico, e.cnpj_ordem,
        e.matriz_filial::TINYINT AS matriz_filial,
        NULLIF(e.nome_fantasia,'') AS nome_fantasia,
        p.razao_social, p.natureza_juridica, nat.descricao AS natureza_juridica_descricao,
        p.capital_social, p.porte,
        e.situacao_cadastral, {DATE.format(c='e.data_situacao_cadastral')} AS data_situacao_cadastral,
        e.motivo_situacao_cadastral, mot.descricao AS motivo_situacao_descricao,
        {DATE.format(c='e.data_inicio_atividade')} AS data_inicio_atividade,
        e.cnae_principal, cn.descricao AS cnae_principal_descricao,
        substr(e.cnae_principal,1,2) AS cnae_divisao,
        (SELECT d FROM (SELECT TRY_CAST(substr(e.cnae_principal,1,2) AS INTEGER) AS d)) AS _d,
        NULLIF(e.cnae_secundaria,'') AS cnae_secundaria,
        NULLIF(e.tipo_logradouro,'') AS tipo_logradouro, NULLIF(e.logradouro,'') AS logradouro, NULLIF(e.numero,'') AS numero,
        NULLIF(e.complemento,'') AS complemento, NULLIF(e.bairro,'') AS bairro, NULLIF(e.cep,'') AS cep,
        e.municipio AS municipio_codigo, mu.descricao AS municipio,
        NULLIF(e.cidade_exterior,'') AS cidade_exterior, NULLIF(e.pais,'') AS pais,
        NULLIF(e.ddd1,'') AS ddd1, NULLIF(e.telefone1,'') AS telefone1, NULLIF(e.ddd2,'') AS ddd2, NULLIF(e.telefone2,'') AS telefone2,
        NULLIF(e.email,'') AS email, NULLIF(e.situacao_especial,'') AS situacao_especial,
        s.opcao_simples, s.data_opcao_simples, s.data_exclusao_simples, s.opcao_mei, s.data_opcao_mei, s.data_exclusao_mei,
        e.uf
      FROM est e
      LEFT JOIN emp p USING (cnpj_basico)
      LEFT JOIN sim s USING (cnpj_basico)
      LEFT JOIN read_parquet('{out}/naturezas.parquet') nat ON nat.codigo = p.natureza_juridica
      LEFT JOIN read_parquet('{out}/motivos.parquet') mot ON mot.codigo = e.motivo_situacao_cadastral
      LEFT JOIN read_parquet('{out}/cnaes.parquet') cn ON cn.codigo = e.cnae_principal
      LEFT JOIN read_parquet('{out}/municipios.parquet') mu ON mu.codigo = e.municipio
    """
    stage = f"{out}/_stage"
    dest = f"{out}/estabelecimentos"
    done_dir = f"{out}/_done"
    import shutil, glob
    # resumable: a stale stage from another month is discarded, otherwise finished buckets and states are kept
    mark = f"{out}/_stage_month"
    if not (os.path.exists(mark) and open(mark).read() == month):
        for d in (stage, dest + ".new", done_dir):
            shutil.rmtree(d, ignore_errors=True)
        open(mark, "w").write(month)
    os.makedirs(done_dir, exist_ok=True)
    final = f"SELECT * EXCLUDE (_d), ({SECAO}) AS cnae_secao FROM ({sql})"
    for i, (lo, hi) in enumerate(BUCKETS):
        if os.path.exists(f"{done_dir}/b{i:02d}"):
            continue
        for f in glob.glob(f"{stage}/uf=*/b{i:02d}_*"):
            os.remove(f)
        set_bucket(lo, hi)
        c.execute(f"COPY ({final}) TO '{stage}' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 3, PARTITION_BY (uf), ROW_GROUP_SIZE 100000, "
                  f"FILENAME_PATTERN 'b{i:02d}_{{i}}', OVERWRITE_OR_IGNORE)")
        open(f"{done_dir}/b{i:02d}", "w").close()
        print("bucket", i, round(time.time() - t0), flush=True)
    print("join done", round(time.time() - t0), flush=True)
    # phase 2: sort per state and per CNAE division range, so row-group statistics prune CNAE and date filters
    # and no single sort has to spill more than the volume can hold
    RANGES = [("00", "35"), ("35", "47"), ("47", "48"), ("48", "60"), ("60", "80"), ("80", "ZZ")]
    for ufdir in sorted(os.listdir(stage)):
        uf = ufdir.split("=", 1)[1]
        ud = f"{dest}.new/uf={uf}"
        if os.path.exists(f"{ud}/_complete"):
            continue
        shutil.rmtree(ud, ignore_errors=True)
        os.makedirs(ud)
        for k, (lo, hi) in enumerate(RANGES):
            c.execute(f"COPY (SELECT * FROM read_parquet('{stage}/{ufdir}/*.parquet') WHERE cnae_divisao >= '{lo}' AND cnae_divisao < '{hi}' "
                      f"ORDER BY cnae_principal, data_inicio_atividade) TO '{ud}/part{k}.parquet' "
                      f"(FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 6, ROW_GROUP_SIZE 100000)")
        open(f"{ud}/_complete", "w").close()
        shutil.rmtree(f"{stage}/{ufdir}")
        print("sorted", uf, round(time.time() - t0), flush=True)
    shutil.rmtree(stage, ignore_errors=True)
    print("estabelecimentos", round(time.time() - t0), flush=True)

    c.execute(f"""COPY (SELECT cnpj_basico, identificador_socio::TINYINT AS identificador_socio, nome_socio, NULLIF(cnpj_cpf_socio,'') AS cnpj_cpf_socio,
        qualificacao_socio, q.descricao AS qualificacao_socio_descricao, {DATE.format(c='data_entrada')} AS data_entrada, NULLIF(s.pais,'') AS pais,
        NULLIF(cpf_representante,'') AS cpf_representante, NULLIF(nome_representante,'') AS nome_representante,
        NULLIF(qualificacao_representante,'') AS qualificacao_representante, NULLIF(faixa_etaria,'') AS faixa_etaria
      FROM read_parquet('{raw}/socios/*.parquet') s LEFT JOIN read_parquet('{out}/qualificacoes.parquet') q ON q.codigo = s.qualificacao_socio
      ORDER BY cnpj_basico) TO '{out}/socios.new.parquet' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000)""")
    print("socios", round(time.time() - t0), flush=True)

    # atomic swap
    if os.path.exists(dest):
        shutil.rmtree(dest)
    os.rename(dest + ".new", dest)
    os.replace(f"{out}/socios.new.parquet", f"{out}/socios.parquet")
    # name-sorted copy so exact partner-name search can skip most row groups
    c.execute(f"""COPY (SELECT strip_accents(upper(nome_socio)) AS nome_key, * FROM read_parquet('{out}/socios.parquet') ORDER BY nome_key)
      TO '{out}/socios_nome.tmp.parquet' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000)""")
    os.replace(f"{out}/socios_nome.tmp.parquet", f"{out}/socios_nome.parquet")
    # primary-key copy (sorted by cnpj, built in ten first-digit slices to keep spill small) for fast single-company and partner lookups
    kdir = f"{out}/est_cnpj.new"
    os.makedirs(kdir, exist_ok=True)
    for dgt in "0123456789":
        c.execute(f"""COPY (SELECT * FROM read_parquet('{dest}/*/*.parquet', hive_partitioning=true) WHERE cnpj LIKE '{dgt}%' ORDER BY cnpj)
          TO '{kdir}/part{dgt}.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 50000)""")
    if os.path.exists(f"{out}/est_cnpj"):
        shutil.rmtree(f"{out}/est_cnpj")
    os.rename(kdir, f"{out}/est_cnpj")
    meta["latest_opening_date"] = str(c.execute(f"SELECT max(data_inicio_atividade) FROM read_parquet('{out}/est_cnpj/*.parquet')").fetchone()[0])
    meta["estabelecimentos"] = c.execute(f"SELECT count(*) FROM read_parquet('{dest}/*/*.parquet')").fetchone()[0]
    meta["empresas"] = c.execute(f"SELECT count(*) FROM read_parquet('{dest}/*/*.parquet') WHERE matriz_filial = 1").fetchone()[0]
    meta["socios"] = c.execute(f"SELECT count(*) FROM read_parquet('{out}/socios.parquet')").fetchone()[0]
    shutil.rmtree(done_dir, ignore_errors=True)
    shutil.rmtree(stage, ignore_errors=True)
    os.remove(mark)
    meta["built_seconds"] = round(time.time() - t0)
    json.dump(meta, open(f"{out}/meta.json", "w"))
    return meta


if __name__ == "__main__":
    print(build(sys.argv[1], sys.argv[2], sys.argv[3]))
