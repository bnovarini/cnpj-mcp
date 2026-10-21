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

The server reads the result from `website_contacts.parquet` (one row per domain) and adds a separate `website_contacts` block, labelled "from the company's own website", to `get_company` and to `search_companies` with `include_contacts`. The site is found through the domain of the company's registered email, and each block carries a per-company `match` level with `match_basis` and a `note`: `confirmed` (the page shows the CNPJ or its 8-digit root, or the company name together with its registered CEP, phone or city), `likely` (the company name is in the page title, heading or copyright line only) or `candidate` (nothing on the page ties the site to the company). The levels come from `python -m cnpj_mcp.crawl` (see `ident.py`) and are served from `website_ident.parquet`. The block is kept apart from Receita fields, and not part of `contact_score`.

Site-ownership pass, October 2026: 306,683 domains with contacts were re-crawled (301,602 read successfully). Of 357,489 company-site pairs, 89,171 (25%) are confirmed, 34,421 (10%) likely and 233,897 (65%) candidate.

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

## Usage logging

The hosted server keeps a small usage log, with no personal data and no query contents. For each initialize and tool call it stores: UTC timestamp, event, tool name, and the client name and version the client sends in the MCP initialize handshake. No IP addresses, arguments or results are stored. It is a SQLite table (`/data/usage.db`) on the same machine. Set `CNPJ_USAGE_LOG=0` to turn it off.

Read the stats from the machine (the number is the window in days, default 30):

```
fly ssh console -a cnpj-mcp -C "python -m cnpj_mcp.usage 30"
```

## Query safety

Hosted queries use one bounded worker and a 10-second request query budget by default (`CNPJ_QUERY_TIMEOUT`). Concurrent expensive requests get a retry message instead of an unbounded queue; health and dataset metadata stay responsive. Interrupted queries return an error, not partial results.

Company-name substring filters require `uf` and 3-256 characters. Partner substring searches also require `uf` and retain their existing minimum name length. Nationwide substring name scans are not supported. Narrow by state, activity or date range after a timeout. There is no new index, sidecar or hidden scan cap.

### Optional PostHog observability

HTTP serving can export request-level tool spans and scrubbed error events with
`CNPJ_POSTHOG_TOKEN` set in the server's secret store. No token means no export.
`CNPJ_POSTHOG_HOST` defaults to `https://us.i.posthog.com`; EU projects can use
`https://eu.i.posthog.com`. Keep project tokens out of source and config files.

Only tool name, status, duration and known client name/version are exported.
Error events retain the SDK-generated anonymous ID but do not create person
profiles. No arguments, filter values, query text, results, caller IP/User-Agent,
sessions, raw exception messages, stack traces or local variables are exported.
Errors use a built-in type or `ToolError` and a fixed message. Handshakes and
health checks are not exported. Batch calls share their request duration/status.
Client name/version are sanitized client-supplied labels, not verified identities.
PostHog still sees the server's egress IP and SDK User-Agent at transport level.

The exporter uses bounded asynchronous queues. Graceful ASGI shutdown attempts a
final flush; abrupt termination or network failures can lose telemetry. Existing
SQLite usage logging remains independent. Export failures do not change tool
responses. No broad auto-instrumentation or exception autocapture is enabled.

Set and verify product billing limits before relying on a zero-cost cap. HTTP
200 acceptance from ingestion does not prove an event appears in the project UI.
Use synthetic safe requests to check both tracing and error tracking after setup.

Run `pytest tests/test_observability.py` for offline SDK wire-privacy and response
transparency checks. `tests/compare_http_responses.py CHECKOUT [enabled]`, with
`CNPJ_DATA_DIR` pointing to the same test dataset, snapshots all eight HTTP tool
responses. The optional `enabled` argument uses dummy credentials and intercepted
network transport; it does not send telemetry.

Optional structured Logs export is separately gated by `CNPJ_POSTHOG_LOGS=1`.
It uses the same secret and region, sending OTLP/protobuf to `/i/v1/logs`.
Only fixed tool-completion/failure messages, severity, timestamp and the same
allowlisted tool metadata are emitted. No root handler is installed: uvicorn
access logs, DuckDB logs and all arbitrary Python log messages are excluded.
No ambient trace/person/session context or host/process resource discovery is
used. Logs queues are bounded and flushed on graceful shutdown. Logs export
failure does not disable the existing tracing/error export.

Logs/Tracing share the free ingestion allowance; verify the Logs billing limit
before enabling. Keep default retention unless intentionally accepting retention
charges, which are not covered by ingestion billing limits.

Tool telemetry also carries fixed diagnostic context: exception-class error codes,
schema-checked argument names (no values), result-size buckets, measured local DB
time and remaining request time, UTC hour, app version and an optional validated
`CNPJ_DEPLOY_VERSION` build ID. CNPJ reads local DuckDB/Parquet: no live upstream
requests (`upstream_ms=0`) and no application result cache (`cache=not_applicable`).
`upstream=DB` means DB work was used, not proof the DB caused a failure. Batch
calls share request timing/outcome. Unknown or incomplete results stay `unknown`.
These are properties on existing spans/errors/logs, not extra analytics events.

Aggregate Product Analytics is separately opt-in with `CNPJ_POSTHOG_ANALYTICS=1`.
It adds one `tool_called` event per completed HTTP tool call: tool, status,
duration bucket, measured result bucket, schema-checked argument names and known
client name/version. It retains a fresh SDK-generated anonymous ID per event,
with person profiles and GeoIP disabled. No values, query text, raw results,
errors, sessions, client IP/User-Agent or inherited identity are exported.
This supports aggregate tool/client mix, empty-result rate, filter-name adoption
and latency trends, not user retention, sessions, sequences or funnels.
It adds event volume; verify the Product Analytics billing cap before enabling.
Without the flag, existing diagnostic telemetry behavior is unchanged.
