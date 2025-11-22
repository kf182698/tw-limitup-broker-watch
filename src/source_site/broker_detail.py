"""Fetch and parse broker detail tables for individual stocks."""

import pandas as pd
from app.utils_http import get_session


def build_broker_url(template: str, code: str) -> str:
    """Substitute the stock code into the broker detail URL template."""
    return template.format(code=code)


def fetch_broker_html(url: str) -> str:
    """Fetch the HTML for a broker detail page."""
    sess = get_session()
    resp = sess.get(url, timeout=20)
    try:
        resp.encoding = resp.encoding or resp.apparent_encoding or "utf-8"
    except Exception:
        resp.encoding = "utf-8"
    return resp.text


def parse_broker_table(html: str) -> pd.DataFrame:
    """Parse the first table in the broker detail HTML.

    Attempts to map typical broker detail columns to standardized field names:
    broker_name, buy_volume, sell_volume, net_volume, buy_ratio.
    """
    try:
        tables = pd.read_html(html)
    except Exception:
        tables = []
    if not tables:
        return pd.DataFrame(
            columns=["broker_name", "buy_volume", "sell_volume", "net_volume", "buy_ratio"]
        )
    df = tables[0].copy()
    rename_map = {}
    for col in df.columns:
        col_str = str(col)
        if any(key in col_str for key in ["券商", "分點", "營業部"]):
            rename_map[col] = "broker_name"
        elif "買進" in col_str or "買入" in col_str:
            rename_map[col] = "buy_volume"
        elif "賣出" in col_str or "賣出" in col_str:
            rename_map[col] = "sell_volume"
        elif "買超" in col_str and ("張" in col_str or "股" in col_str):
            rename_map[col] = "net_volume"
        elif "比率" in col_str or "%" in col_str:
            rename_map[col] = "buy_ratio"
    df = df.rename(columns=rename_map)
    # Convert numeric columns
    for col in ["buy_volume", "sell_volume", "net_volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    if "buy_ratio" in df.columns:
        # Convert percentage strings to floats between 0 and 1
        df["buy_ratio"] = (
            df["buy_ratio"].astype(str).str.replace("%", "", regex=False).str.strip()
        )
        df["buy_ratio"] = pd.to_numeric(df["buy_ratio"], errors="coerce") / 100.0
    # Ensure all expected columns exist
    for col in ["broker_name", "buy_volume", "sell_volume", "net_volume", "buy_ratio"]:
        if col not in df.columns:
            df[col] = None
    return df[["broker_name", "buy_volume", "sell_volume", "net_volume", "buy_ratio"]]
