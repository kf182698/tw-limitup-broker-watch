"""
Module to fetch and parse daily limit‑up lists for both TWSE (上市) and TPEX (上櫃).

This module defines two helper functions to retrieve raw HTML from the Fubon eBroker
ranking pages and parse them into DataFrames of limit‑up stocks.

The TWSE (listed) limit‑up list is fetched from the URL specified in
``settings.source.twse_limitup_url`` (usually something like
``https://fubon-ebrokerdj.fbs.com.tw/z/zg/zg_A_0_0.djhtm``). The TPEX (OTC) limit‑up
list is fetched from ``settings.source.tpex_limitup_url`` (for example
``https://fubon-ebrokerdj.fbs.com.tw/z/zg/zg_A_1_1.djhtm``). Both pages contain
ranking tables of price changes; we extract rows where the daily percent change
meets or exceeds the configured ``min_pct_change`` threshold (typically 9.8 or
10.0) to identify stocks that closed at their limit price.

The returned DataFrames have the following columns:

    [trade_date, code, stock_name, market, close, volume, pct_change]

Where ``market`` is either "TWSE" or "TPEX".

Note: Fubon's pages are encoded in Big5. This module sets the response
encoding explicitly and falls back to ``apparent_encoding``.
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
from typing import List


def _get_html(url: str) -> str:
    """Fetch HTML content from the given URL using requests with Big5 decoding."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/117.0 Safari/537.36"
        )
    }
    resp = requests.get(url, headers=headers, timeout=30)
    # Attempt to decode using Big5; fallback to apparent_encoding
    try:
        resp.encoding = "big5"
        html = resp.text
    except Exception:
        resp.encoding = resp.apparent_encoding or "utf-8"
        html = resp.text
    return html


def _parse_limitup_table(html: str, trade_date: str, market: str, min_pct_change: float) -> pd.DataFrame:
    """
    Parse an HTML document containing a price‑change ranking table.

    Returns a DataFrame of rows where the daily percent change (pct_change)
    meets or exceeds ``min_pct_change``.

    :param html: Raw HTML content from Fubon ranking page
    :param trade_date: ISO date string (YYYY-MM-DD)
    :param market: "TWSE" or "TPEX" to tag the output
    :param min_pct_change: Minimum daily change (%) to consider as limit‑up
    :return: DataFrame with columns [trade_date, code, stock_name, market, close, volume, pct_change]
    """
    soup = BeautifulSoup(html, "lxml")
    # Find the first table with the ranking data
    tables = pd.read_html(str(soup))
    if not tables:
        return pd.DataFrame(columns=["trade_date", "code", "stock_name", "market", "close", "volume", "pct_change"])
    df = tables[0]
    # The ranking table should have at least these columns
    # Columns may be unnamed or shifted depending on the page; standardise them
    # Expected column order: [Rank, Code, Name, Price, Change, Pct_change, Volume]
    # Some pages merge Code and Name into one column; handle that case too.
    cols = [c for c in df.columns]
    # Attempt to identify code and name columns
    # If the first column contains codes mixed with names, split them
    if any(df.iloc[:, 0].astype(str).str.isnumeric()):
        # Assume first column is code, second is name
        code_col = df.columns[0]
        name_col = df.columns[1] if len(df.columns) > 1 else df.columns[0]
    else:
        # Try splitting the second column
        code_col = df.columns[1] if len(df.columns) > 1 else df.columns[0]
        name_col = df.columns[2] if len(df.columns) > 2 else df.columns[1]
    # Identify numeric columns for price, pct_change, volume
    # Try to find a column with % sign or containing '漲跌幅'
    pct_col_candidates = [c for c in df.columns if '幅' in str(c) or '%' in str(c)]
    pct_col = pct_col_candidates[0] if pct_col_candidates else df.columns[5]
    price_col_candidates = [c for c in df.columns if '收盤' in str(c) or '價格' in str(c)]
    price_col = price_col_candidates[0] if price_col_candidates else df.columns[3]
    vol_col_candidates = [c for c in df.columns if '成交量' in str(c) or '量' in str(c)]
    vol_col = vol_col_candidates[0] if vol_col_candidates else df.columns[-1]
    # Build new DataFrame
    out_rows = []
    for _, row in df.iterrows():
        code = str(row[code_col]).strip()
        name = str(row[name_col]).strip()
        # Remove any non‑numeric prefix in code
        if not code.isdigit():
            # Some pages have code like '4939亞電' concatenated; split digits
            digits = ''.join(ch for ch in code if ch.isdigit())
            if digits:
                code = digits
            else:
                continue
        try:
            pct_str = str(row[pct_col]).strip()
            # Remove plus/minus signs and percent symbol
            pct_val = float(pct_str.replace('%', '').replace('+', '').replace(',', '').strip())
        except Exception:
            continue
        if pct_val < min_pct_change:
            continue
        # Parse price
        try:
            price = float(str(row[price_col]).replace(',', '').strip())
        except Exception:
            price = None
        # Parse volume
        try:
            volume = int(str(row[vol_col]).replace(',', '').strip())
        except Exception:
            volume = None
        out_rows.append({
            "trade_date": trade_date,
            "code": code,
            "stock_name": name,
            "market": market,
            "close": price,
            "volume": volume,
            "pct_change": pct_val,
        })
    return pd.DataFrame(out_rows)


def fetch_twse_limitup_list(date_str: str, url: str, min_pct_change: float) -> pd.DataFrame:
    """
    Fetch and parse the TWSE (上市) limit‑up list for a given date.

    :param date_str: ISO date string (YYYY-MM-DD)
    :param url: Fubon URL configured for TWSE daily price change ranking
    :param min_pct_change: Minimum percent change to consider as limit‑up
    :return: DataFrame of TWSE limit‑up stocks
    """
    html = _get_html(url)
    return _parse_limitup_table(html, date_str, market="TWSE", min_pct_change=min_pct_change)


def fetch_tpex_limitup_list(date_str: str, url: str, min_pct_change: float) -> pd.DataFrame:
    """
    Fetch and parse the TPEX (上櫃) limit‑up list for a given date.

    :param date_str: ISO date string (YYYY-MM-DD)
    :param url: Fubon URL configured for TPEX daily price change ranking
    :param min_pct_change: Minimum percent change to consider as limit‑up
    :return: DataFrame of TPEX limit‑up stocks
    """
    html = _get_html(url)
    return _parse_limitup_table(html, date_str, market="TPEX", min_pct_change=min_pct_change)


def fetch_limitup_lists(date_str: str, twse_url: str, tpex_url: str, min_pct_change: float) -> pd.DataFrame:
    """
    Fetch both TWSE and TPEX limit‑up lists and return a combined DataFrame.

    :param date_str: ISO date string (YYYY-MM-DD)
    :param twse_url: Fubon URL for TWSE limit‑up ranking page
    :param tpex_url: Fubon URL for TPEX limit‑up ranking page
    :param min_pct_change: Threshold for percent change (e.g. 9.8)
    :return: Combined DataFrame of limit‑up stocks
    """
    twse_df = fetch_twse_limitup_list(date_str, twse_url, min_pct_change)
    tpex_df = fetch_tpex_limitup_list(date_str, tpex_url, min_pct_change)
    return pd.concat([twse_df, tpex_df], ignore_index=True)