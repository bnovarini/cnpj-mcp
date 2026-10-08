# cnpj-mcp

An MCP server for analytical questions over Brazil's open company registry (CNPJ), built from Receita Federal's monthly open-data dump. It is made for questions about many companies at once, not for looking up a single number:

- How many MEIs were opened per month in Minas Gerais since 2023?
- Which municipalities in Mato Grosso have the most active soy and grain companies?
- Which companies share one address, and who are their partners?
- How many companies closed each year, and for what reason?

Single-CNPJ lookup is included, but free APIs already do that well.

## What is in it

Every establishment, company, partner and Simples/MEI record in Receita's dump, joined and typed for fast queries. Values stay in Portuguese, exactly as Receita publishes them. See `dataset_info` for the loaded snapshot month and row counts.

| Receita file | Rows become |
|---|---|
| Estabelecimentos + Empresas + Simples | one row per CNPJ (matriz and filiais) with company, size, capital, legal form, MEI/Simples flags, CNAE, address and contacts |
| Socios | one row per partner link, with names (CPFs are masked by Receita) |
| Cnaes, Municipios, Naturezas, Motivos, Paises, Qualificacoes | code tables |

Source: Receita Federal, Dados Abertos CNPJ, https://www.gov.br/receitafederal/pt-br/acesso-a-informacao/dados-abertos/cadastros

## Use it

Hosted endpoint (streamable HTTP): listed in the MCP registry as `io.github.bnovarini/cnpj-mcp`.

Or run it yourself:

```
pip install "cnpj-mcp[ingest]"
cnpj-mcp ingest 2026-09 ./raw          # streams Receita's zips straight to Parquet, about 7 GB downloaded
cnpj-mcp build 2026-09 ./raw ./data    # joins and types them
CNPJ_DATA_DIR=./data cnpj-mcp          # MCP server over stdio
```

`ingest` needs `curl` and `unzip` on the PATH. It never writes the zip or the CSV to disk, so disk use is only the Parquet output (about 8 GB). It skips files that are already done, so you can re-run it after an interruption. The build step needs about 20 GB of free space for sorting and the swap.

Tools: `dataset_info`, `search_companies`, `count_companies`, `get_company`, `list_partners`, `search_partners`, `companies_at_address`, `lookup_codes`.

## Read this before quoting numbers

- The registry shows the current state only. A company's earlier addresses, activities and capital are not kept.
- About half of all records are closed (situacao baixada). Filter by `situacao` when you mean active companies.
- Counts are establishments (one per CNPJ) unless you pass `unit='empresas'`, which counts companies (matriz only).
- Capital social is what the company declared, in BRL. It is not a measure of real assets.
- Receita's data is only as good as what companies and registries report. Opening dates, CNAE codes and addresses can be wrong or stale.
- MEI and Simples flags show the current option, with dates when it started or ended.

## Personal data and LGPD

Receita publishes partner names, and this project includes them. Under LGPD that is still personal data, even though it is public:

- Receita masks CPFs before publishing (only the 6 middle digits are shown), and this project keeps them masked. Do not try to re-identify people from them.
- Names alone can match different people. Do not treat a name match as proof that two records are the same person.
- Use partner search for legitimate purposes, such as due diligence, journalism or research. Do not use it for profiling people, harassment or unsolicited marketing.
- Contact details (email, phone) are published by Receita too. They are only returned when you ask for them with `include_contacts`.

## Checks

Counts are reconciled to the source and to the government's own figures. See [docs/AUDIT.md](docs/AUDIT.md), including numbers that do not match and why.

## License

MIT for the code. The data comes from Receita Federal's open-data program; check Receita's current terms for reuse.
