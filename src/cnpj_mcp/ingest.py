"""Streaming ingest of Receita Federal CNPJ open data into Parquet.

Each monthly zip is streamed (curl | funzip | pyarrow) straight to Parquet, so
neither the zip nor the CSV lands on disk. Zips over 1 GiB (Estabelecimentos0) are fetched with parallel ranged
requests to a temporary file and read with unzip, because funzip fails on zip64 files. Resumable per file: a finished
file is skipped on rerun. Values are kept as Receita publishes them (strings),
except dates and capital which are typed in the build step.
"""
from __future__ import annotations
import os, subprocess, sys, time
import pyarrow as pa, pyarrow.csv as pcsv, pyarrow.parquet as pq

BASE = "https://arquivos.receitafederal.gov.br/public.php/webdav"
TOKEN = "YggdBLfdninEJX9"  # public share token published by Receita (no secret)

COLS = {
 "empresas": ["cnpj_basico","razao_social","natureza_juridica","qualificacao_responsavel","capital_social","porte","ente_federativo"],
 "estabelecimentos": ["cnpj_basico","cnpj_ordem","cnpj_dv","matriz_filial","nome_fantasia","situacao_cadastral","data_situacao_cadastral","motivo_situacao_cadastral","cidade_exterior","pais","data_inicio_atividade","cnae_principal","cnae_secundaria","tipo_logradouro","logradouro","numero","complemento","bairro","cep","uf","municipio","ddd1","telefone1","ddd2","telefone2","ddd_fax","fax","email","situacao_especial","data_situacao_especial"],
 "simples": ["cnpj_basico","opcao_simples","data_opcao_simples","data_exclusao_simples","opcao_mei","data_opcao_mei","data_exclusao_mei"],
 "socios": ["cnpj_basico","identificador_socio","nome_socio","cnpj_cpf_socio","qualificacao_socio","data_entrada","pais","cpf_representante","nome_representante","qualificacao_representante","faixa_etaria"],
 "cnaes": ["codigo","descricao"], "motivos": ["codigo","descricao"], "municipios": ["codigo","descricao"],
 "naturezas": ["codigo","descricao"], "paises": ["codigo","descricao"], "qualificacoes": ["codigo","descricao"],
}

def files(month: str):
    out = []
    for t in ("Empresas","Estabelecimentos","Socios"):
        out += [(t.lower().replace("socios","socios"), f"{t}{i}") for i in range(10)]
    out += [("simples","Simples")]
    out += [(k.lower(), k) for k in ("Cnaes","Motivos","Municipios","Naturezas","Paises","Qualificacoes")]
    return out

BIG = 1 << 30  # zips above this are fetched with parallel ranged requests; Receita serves single streams at about 1 MB/s


def _size(url: str) -> int:
    out = subprocess.run(["curl", "-sfI", "-u", f"{TOKEN}:", url], capture_output=True, text=True).stdout
    for line in out.splitlines():
        if line.lower().startswith("content-length:"):
            return int(line.split(":", 1)[1])
    return 0


def _ranged_download(url: str, path: str, size: int, parts: int = 10) -> None:
    from concurrent.futures import ThreadPoolExecutor
    chunk = -(-size // parts)
    def get(i: int) -> str:
        lo, hi = i * chunk, min(size, (i + 1) * chunk) - 1
        p = f"{path}.part{i}"
        for _ in range(8):
            if subprocess.run(["curl", "-sf", "-r", f"{lo}-{hi}", "-u", f"{TOKEN}:", "-o", p, url]).returncode == 0 and os.path.getsize(p) == hi - lo + 1:
                return p
            time.sleep(3)
        raise RuntimeError(f"range {i} of {url} failed")
    with ThreadPoolExecutor(parts) as ex:
        names = list(ex.map(get, range(parts)))
    with open(path, "wb") as out:
        for n in names:
            with open(n, "rb") as f:
                while chunk_ := f.read(1 << 24):
                    out.write(chunk_)
            os.remove(n)


def ingest_file(month: str, table: str, name: str, outdir: str, retries: int = 6) -> int:
    dest = os.path.join(outdir, table, f"{name}.parquet")
    if os.path.exists(dest):
        return -1
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    cols = COLS[table]
    url = f"{BASE}/{month}/{name}.zip"
    size = _size(url)
    for attempt in range(retries):
        tmp = dest + ".tmp"
        local = None
        if size > BIG:
            local = os.path.join(outdir, f"{name}.zip.tmp")
            _ranged_download(url, local, size)
            # unzip (not funzip): zips with more than 4 GiB inside need zip64 handling, and unzip checks the CRC
            src = subprocess.Popen(["unzip", "-p", local], stdout=subprocess.PIPE)
            procs = [src]
        else:
            curl = subprocess.Popen(["curl", "-sf", "--retry", "3", "-u", f"{TOKEN}:", url], stdout=subprocess.PIPE)
            src = subprocess.Popen(["funzip"], stdin=curl.stdout, stdout=subprocess.PIPE)
            curl.stdout.close()
            procs = [curl, src]
        n = 0
        try:
            reader = pcsv.open_csv(src.stdout,
                read_options=pcsv.ReadOptions(column_names=cols, encoding="latin1", block_size=32 << 20),
                parse_options=pcsv.ParseOptions(delimiter=";", quote_char='"', double_quote=True),
                convert_options=pcsv.ConvertOptions(column_types={c: pa.string() for c in cols}, strings_can_be_null=False))
            with pq.ParquetWriter(tmp, reader.schema, compression="zstd", compression_level=3) as w:
                for batch in reader:
                    w.write_batch(batch); n += batch.num_rows
            for p in procs:
                p.wait()
            if any(p.returncode != 0 for p in procs):
                raise RuntimeError("download or unzip failed: " + ",".join(str(p.returncode) for p in procs))
            os.replace(tmp, dest)
            return n
        except Exception as e:
            print(f"retry {name} ({attempt+1}): {e}", file=sys.stderr, flush=True)
            for p in procs:
                try: p.kill()
                except Exception: pass
            if os.path.exists(tmp): os.remove(tmp)
            time.sleep(10)
        finally:
            if local and os.path.exists(local):
                os.remove(local)
    raise RuntimeError(f"failed {name}")


def main():
    from concurrent.futures import ThreadPoolExecutor
    month, outdir = sys.argv[1], sys.argv[2]
    only = sys.argv[3:]
    workers = int(os.environ.get("CNPJ_WORKERS", "3"))
    todo = [(t, n) for t, n in files(month) if not only or n in only]
    big = {"Estabelecimentos0", "Empresas0", "Simples", "Socios0"}
    todo.sort(key=lambda x: (x[1] not in big, x[1]))
    def run(x):
        t0 = time.time(); n = ingest_file(month, x[0], x[1], outdir)
        print(f"{x[1]}: {'skip' if n < 0 else n} rows {time.time()-t0:.0f}s", flush=True)
    with ThreadPoolExecutor(workers) as ex:
        list(ex.map(run, todo))
    print("INGEST DONE", month, flush=True)

if __name__ == "__main__":
    main()
