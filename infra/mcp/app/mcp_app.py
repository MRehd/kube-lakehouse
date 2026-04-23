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
import requests as r
from typing import Any, Optional

import trino
from fastmcp import FastMCP

CURRENT_BALANCE = {
    'USD': 1000.0,
    'BTC': 0.0,
    'ETH': 0.0,
    'LAST_RATE_BTC': None,
    'LAST_RATE_ETH': None
}

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


@mcp.tool()
def get_crypto_price_history(
    start_time: str,
    end_time: str,
    crypto: str = 'BTC',
    granularity: int = 60
) -> dict[str, Any]:
    '''
    Fetch historical OHLC candles for a crypto product from the Coinbase Exchange API. Maximum of 300 candles per call.

    Args:
        start_time:    ISO 8601 start timestamp (e.g. '2026-04-23T00:00:00Z'), inclusive.
        end_time:      ISO 8601 end timestamp, inclusive.
        crypto:         Base currency symbol (default 'BTC').
        granularity:   Candle width in seconds. Coinbase accepts 60, 300, 900, 3600, 21600, 86400.

    Returns:
        A list of candles sorted ascending by time, each a dict with keys
        'Timestamp' (unix seconds), 'Low', 'High', 'Open', 'Close', 'Volume'.
    '''

    if crypto not in ['BTC', 'ETH']:
        raise ValueError("Invalid crypto. Must be 'BTC' or 'ETH'.")

    schema = ['Timestamp', 'Low', 'High', 'Open', 'Close', 'Volume']
    url = f'https://api.exchange.coinbase.com/products/{crypto}-USD/candles?granularity={granularity}&start={start_time}&end={end_time}'
    data = r.get(url).json()
    data = sorted(data, key=lambda x: x[0])

    return {'candles': [dict(zip(schema, v)) for v in data]}

@mcp.tool()
def get_crypto_price_now(
    mode: str,
    crypto: str = 'BTC'
) -> float:
    '''
    Fetch the current spot price for a crypto product from the Coinbase public price API.

    Args:
        mode:          'buy' for the ask price (what you'd pay to buy 1 unit of base),
                       'sell' for the bid price (what you'd receive for selling 1 unit).
        crypto:         Base currency symbol (default 'BTC').

    Returns:
        The price as a float, denominated in the quote currency per 1 unit of base
        (e.g. for 'BTC-USD', USD per 1 BTC).
    '''

    if mode not in ['buy', 'sell']:
        raise ValueError("Invalid mode. Must be 'buy' or 'sell'.")
    
    if crypto not in ['BTC', 'ETH']:
        raise ValueError("Invalid crypto. Must be 'BTC' or 'ETH'.")

    return float(r.get(f'https://api.coinbase.com/v2/prices/{crypto}-USD/{mode}').json()['data']['amount'])

@mcp.tool()
def place_trade(
    mode: str,
    amount: float,
    crypto: str = 'BTC'
) -> dict[str, Any]:
    '''
    Simulate a crypto trade at the current Coinbase spot price and mutate the
    in-memory CURRENT_BALANCE (holdings + LAST_RATE) to reflect the fill. No
    real order is sent — this is a paper-trading helper for the agent.

    Supported products are the USD-quoted pairs whose base currency is tracked
    in CURRENT_BALANCE: 'BTC-USD' and 'ETH-USD'. USD is always the quote side.

    The meaning of `amount` depends on `mode`:
        - mode='buy':  amount is the USD notional to spend;
                       base received = amount / price.
        - mode='sell': amount is the base-currency quantity to sell;
                       USD received = amount * price.

    Args:
        mode:          'buy' or 'sell'.
        amount:        USD to spend when buying, base-currency quantity to sell when selling.
        crypto:         Base currency symbol (default 'BTC').
                       'ETH' is also supported.

    Returns:
        The updated CURRENT_BALANCE dict with keys 'USD', 'BTC', 'ETH', and
        'LAST_RATE_BTC', 'LAST_RATE_ETH' (the BASE-USD price used to fill this trade).
    '''

    if mode not in ['buy', 'sell']:
        raise ValueError("Invalid mode. Must be 'buy' or 'sell'.")
    
    if crypto not in ['BTC', 'ETH']:
        raise ValueError("Invalid crypto. Must be 'BTC' or 'ETH'.")

    cur_price = float(r.get(f'https://api.coinbase.com/v2/prices/{crypto}-USD/{mode}').json()['data']['amount'])

    if mode == 'buy':
        CURRENT_BALANCE[crypto] += amount / cur_price
        CURRENT_BALANCE['USD'] -= amount
        CURRENT_BALANCE[f'LAST_RATE_{crypto}'] = cur_price
    else:
        CURRENT_BALANCE['USD'] += amount * cur_price
        CURRENT_BALANCE[crypto] -= amount
        CURRENT_BALANCE[f'LAST_RATE_{crypto}'] = cur_price

    return CURRENT_BALANCE

@mcp.tool()
def get_current_balance() -> dict[str, Any]:
    '''
    Gets the current balance information.

    Returns:
        The updated CURRENT_BALANCE dict with keys 'USD', 'BTC', 'ETH', and
        'LAST_RATE_BTC', 'LAST_RATE_ETH' (the BASE-USD price used to fill this trade).
    '''

    return CURRENT_BALANCE

# ASGI app for `uvicorn mcp_app:app --host 0.0.0.0 --port 8000`.
# Streamable HTTP transport — the modern MCP-over-HTTP wire format.
app = mcp.http_app()

if __name__ == '__main__':
    transport = os.getenv('MCP_TRANSPORT', 'http')
    if transport == 'stdio':
        mcp.run(transport=transport)
    else:
        mcp.run(
            transport = transport,
            host      = os.getenv('MCP_HOST', '0.0.0.0'),
            port      = int(os.getenv('MCP_PORT', '8000')),
        )
