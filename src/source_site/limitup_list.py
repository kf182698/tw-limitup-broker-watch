"""Fetch and parse the limit‑up (漲停) list from the configured source."""

from typing import Optional
import pandas as pd
from ..app.utils_http import get_session


def fetch_limitup_html(url: str) -> str:
    """Fetch the HTML from the given limit‑up list URL.

    The caller must provide a fully qualified URL. The response encoding
    will be set based on the HTTP headers or apparent encoding. If the
    encoding cannot be determined, UTF‑8 is used.
    """
    sess = get_session()
    resp = sess.get(url, timeout=20)
    # Try to use encoding provided by the server; fall back to apparent_encoding
    # or UTF‑8 if unknown.
    try:
        resp.encoding = resp.encoding or resp.apparent_encoding or "utf-8"
    except Exception:
        resp.encoding = "utf-8"
    return resp.text


def parse_limitup_table(html: str, trade_date: str) -> pd.DataFrame:
    """Parse the first table in the HTML as a limit‑up list.

    This function attempts to map generic column headers found in a typical
    漲跌幅排行表到 standardized column names used by this project. If
    certain columns cannot be found, they will be omitted. The resulting
    DataFrame will always contain these columns: trade_date, code,
    stock_name, market, close, volume, pct_change.
    """
    # Read all tables; if none, return empty frame with expected columns
    try:
        tables = pd.read_html(html)
    except Exception:
        tables = []
    if not tables:
        return pd.DataFrame(
            columns=["trade_date", "code", "stock_name", "market", "close", "volume", "pct_change"]
        )
    df = tables[0].copy()
    # Map likely headers to our standardized names
    rename_map = {}
    for col in df.columns:
        col_str = str(col)
        if "股票" in col_str or "名稱" in col_str or "證券" in col_str:
            rename_map[col] = "stock_name"
        elif "代號" in col_str or "股票代號" in col_str:
            rename_map[col] = "code"
        elif "收盤" in col_str:
            rename_map[col] = "close"
        elif "成交" in col_str and ("量" in col_str or "股" in col_str):
            rename_map[col] = "volume"
        elif "漲跌" in col_str or "%" in col_str:
            rename_map[col] = "pct_change"
    df = df.rename(columns=rename_map)
    # Convert numeric fields where possible
    if "pct_change" in df.columns:
        df["pct_change"] = pd.to_numeric(df["pct_change"], errors="coerce")
    if "close" in df.columns:
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
    if "volume" in df.columns:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    # Assemble full DataFrame with missing columns filled
    result = pd.DataFrame()
    result["trade_date"] = df.get("trade_date", pd.Series([trade_date] * len(df)))
    result["code"] = df.get("code")
    result["stock_name"] = df.get("stock_name")
    # market may not be provided; leave as None
    result["market"] = None
    result["close"] = df.get("close")
    result["volume"] = df.get("volume")
    result["pct_change"] = df.get("pct_change")
    return result