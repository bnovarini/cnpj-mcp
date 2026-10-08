"""cnpj-mcp command line: serve the MCP server, or build the dataset yourself."""
from __future__ import annotations
import sys

USAGE = """usage:
  cnpj-mcp                        run the MCP server over stdio (needs a built dataset, see CNPJ_DATA_DIR)
  cnpj-mcp --http                 run the MCP server over streamable HTTP
  cnpj-mcp ingest YYYY-MM RAW     download one monthly Receita dump into RAW as Parquet (needs curl, unzip, pyarrow)
  cnpj-mcp build YYYY-MM RAW OUT  build the query-ready dataset in OUT from RAW (set CNPJ_DATA_DIR=OUT to serve it)
"""


def main() -> None:
    a = sys.argv[1:]
    if a and a[0] in ("-h", "--help", "help"):
        print(USAGE); return
    if a and a[0] == "ingest":
        if len(a) != 3:
            print(USAGE); sys.exit(2)
        from . import ingest
        sys.argv = ["ingest", a[1], a[2]]
        ingest.main(); return
    if a and a[0] == "build":
        if len(a) != 4:
            print(USAGE); sys.exit(2)
        from . import build
        print(build.build(a[2], a[3], a[1])); return
    from . import mcp_server
    mcp_server.main()


if __name__ == "__main__":
    main()
