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

## Contact quality score

`get_company` and `search_companies` (with `include_contacts`) return a `registry_contact_quality` block: a 0-100 `contact_score`, a tier (high 85+, medium 50-84, low below 50) and the signals behind it. It estimates how likely the email or phone registered with Receita is a real, direct contact. It does not check that anyone answers.

Signals, all computed from the Receita dump itself:

- Email kind: corporate domain, free provider (gmail, hotmail, uol...), accountant-looking address, mistyped free provider (gmial.com), or invalid.
- How many companies share the same email, and how many use the same email domain. One company is a good sign; dozens usually means an accountant or a shared mailbox.
- Whether the email matches the company name.
- Phone type (mobile or landline; Receita often stores mobiles without the ninth digit) and how many companies share the phone.

`contact_score` is the better of the email score and the phone score. `min_contact_score` on `search_companies` takes an integer 0-100 or a tier name (`low` = 0, `medium` = 50, `high` = 85) and keeps only companies whose registered contact scores at or above it. It says nothing about the website. It filters after reading each page, so one call looks at most 2000 candidates and returns `pagination.next_offset` to continue. Rebuild with `python -m cnpj_mcp.scores <data_dir>` after each monthly build.

## Company website contacts (served as a separate block)

`python -m cnpj_mcp.crawl domains.tsv out.jsonl` visits the websites behind corporate email domains and records what the company itself publishes: WhatsApp links, phones, emails and social profiles, each with the page it came from. It obeys robots.txt, identifies itself (`cnpj-mcp-crawler/0.1`), makes at most one request per second per domain and reads at most the home page plus one contact page. Cookies are not stored.

The server reads the result from `website_contacts.parquet` (one row per domain) and adds a separate `website_contacts` block, labelled "from the company's own website", to `get_company` and to `search_companies` with `include_contacts`. It is matched to a company through the domain of its registered email, kept apart from Receita fields, and not part of `contact_score`.

First sweep, October 2026: 724,996 corporate domains used by one to three companies. 420,058 sites loaded (57.9%). After cleaning, 414,185 show contacts that can be used and 4,004 turned out to be parked or "site unavailable" pages (for example `static.uni5.net/indisponivel.php`) and 1,869 redirect to a social or login page; those return no contacts. Of the 414,185 usable sites, 40% show a WhatsApp link, 55% a phone, 54% an email and 51% a social profile; 70% show at least one of WhatsApp, phone or email. The rest were unreachable (28% no DNS or connection), returned an error page, or were skipped because robots.txt asked bots to stay away (3.9%).

Cleaning (`python -m cnpj_mcp.postprocess merged.jsonl out.parquet`, also applied by the crawler): phones and WhatsApp numbers must be valid Brazilian numbers (real area code, 9-digit mobile or 8-digit landline, no repeated or sequential digits); stray leading zeros and the 55 prefix are normalised, old 8-digit mobiles get the ninth digit for WhatsApp, and foreign numbers are dropped. Placeholder and error-tracker emails are removed. Generic social paths (`profile.php`, `sharer.php`, `reel`, ...) and anything found on 10 or more different domains (social) or 25 or more (phones, WhatsApp, emails) are dropped, because they belong to a website builder, a host or a platform. The site is a candidate match only: it is found through the domain of the registered email, so the block carries `"match": "candidate"`. Refresh by rerunning the crawl on the new domain list.

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

## Response shape

`search_companies` and `search_partners` return `{"companies" | "partners": [...], "pagination": {"has_more", "next_offset", "returned"}, "message"?}`. The page is always only data rows; use `pagination.next_offset` as `offset` for the next call.
