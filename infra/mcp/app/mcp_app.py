'''
FastMCP server exposing Trino as a tool for AI agents.

Connection details are read from env vars at startup:

    TRINO_HOST          required — Trino coordinator hostname
    TRINO_PORT          default 8080
    TRINO_USER          default 'mcp'
    TRINO_PASSWORD      optional — enables BasicAuthentication when set
    TRINO_HTTP_SCHEME   default 'http' (use 'https' with TRINO_PASSWORD)
    TRINO_CATALOG       optional default catalog
    TRINO_SCHEMA        optional default schema

    MCP_HOST            default '0.0.0.0'
    MCP_PORT            default 8000
    MCP_TRANSPORT       default 'http' ('http' | 'sse' | 'stdio')

Run:
    python mcp_app.py
'''

import os
from typing import Any, Optional

import trino
from fastmcp import FastMCP


def _connect(catalog: Optional[str] = None, schema: Optional[str] = None) -> trino.dbapi.Connection:
    auth = None
    if os.getenv('TRINO_PASSWORD'):
        auth = trino.auth.BasicAuthentication(
            os.environ.get('TRINO_USER', 'mcp'),
            os.environ['TRINO_PASSWORD'],
        )

    return trino.dbapi.connect(
        host         = os.environ['TRINO_HOST'],
        port         = int(os.getenv('TRINO_PORT', '8080')),
        user         = os.getenv('TRINO_USER', 'mcp'),
        catalog      = catalog or os.getenv('TRINO_CATALOG') or None,
        schema       = schema  or os.getenv('TRINO_SCHEMA')  or None,
        http_scheme  = os.getenv('TRINO_HTTP_SCHEME', 'http'),
        auth         = auth,
    )


mcp = FastMCP('trino-mcp')


@mcp.tool()
def query(
    sql: str,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    max_rows: int = 1000,
) -> dict[str, Any]:
    '''
    Execute a SQL statement against Trino and return the result set.

    Args:
        sql:      SQL statement to run.
        catalog:  Override the default catalog for this query.
        schema:   Override the default schema for this query.
        max_rows: Cap the number of rows returned (default 1000).

    Returns:
        {'columns': [...], 'rows': [[...], ...], 'row_count': N, 'truncated': bool}
        For statements without a result set (DDL, INSERT, etc.) `rows` is empty
        and `row_count` reflects the number of affected rows when reported.
    '''
    conn = _connect(catalog=catalog, schema=schema)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        columns = [c.name for c in (cur.description or [])]
        rows    = cur.fetchmany(max_rows) if columns else []
        more    = bool(columns) and cur.fetchone() is not None
        return {
            'columns':   columns,
            'rows':      [list(r) for r in rows],
            'row_count': len(rows) if columns else (cur.rowcount or 0),
            'truncated': more,
        }
    finally:
        conn.close()


# ASGI app for `uvicorn mcp_app:app --host 0.0.0.0 --port 8000`.
# Streamable HTTP transport — the modern MCP-over-HTTP wire format.
app = mcp.http_app()


if __name__ == '__main__':
    mcp.run(
        transport = os.getenv('MCP_TRANSPORT', 'http'),
        host      = os.getenv('MCP_HOST', '0.0.0.0'),
        port      = int(os.getenv('MCP_PORT', '8000')),
    )
