# Audit of the 2026-09 snapshot

Checks run on the built dataset against the raw Receita Federal files. Numbers are for the 2026-09 dump.

## Internal reconciliation

| Check | Result |
|---|---|
| Raw ingest rows, all tables | 222,197,007 |
| Estabelecimentos0 rows vs source | 30,585,232 = 30,585,236 source lines minus 4 records with newlines inside quoted fields; byte count of the zip member matched exactly |
| Establishments, built vs raw | 73,366,147 = 73,366,147 |
| Distinct CNPJs, built | 73,366,147 |
| Sócios, built vs raw | 28,341,092 = 28,341,092 |
| Establishments with no company name after the join | 0 |
| Establishments by situação (01 nula, 02 ativa, 03 suspensa, 04 inapta, 08 baixada) | 110,199 / 28,263,121 / 321,187 / 10,084,178 / 34,587,462 |

## Defects found by the audit

1. Receita's Empresas2 file has two rows for CNPJ base 08314885: the real company and a blank row. A plain join duplicated that company's 51 establishments (73,366,198 rows instead of 73,366,147). The build now keeps the row with a name. Fixed and rebuilt.
2. One CNPJ base has two matriz rows in the dump (70,085,592 matriz rows for 70,085,591 distinct bases). This comes from the source and is left as published.

## External anchors (Mapa de Empresas, gov.br)

The Mapa de Empresas publishes rounded figures with its own definitions. The comparison below is indicative, not a reconciliation.

| Figure | Mapa de Empresas | This dataset |
|---|---|---|
| Active companies | 25.4 million | 26,937,244 (matriz rows with situação ativa) |
| Companies opened in July 2026 | 485 thousand | 498,621 (matriz rows with data de início in July 2026) |

Both are higher here, by about 6% and 3%. The likely cause is different inclusion rules or reference dates on the Mapa side. This has not been confirmed with the publisher.

## Known limits

- The registry is a snapshot of the current state, not history.
- Receita masks the CPF of every sócio. Partner names are published as is.
- Exact partner-name search uses a name-sorted copy of the sócios table. Substring search scans the whole table and is slow on small machines; add a state filter.
