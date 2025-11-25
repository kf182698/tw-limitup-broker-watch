from __future__ import annotations

from datetime import date
from io import StringIO
from typing import List, Tuple

import pandas as pd
from bs4 import BeautifulSoup

from app.utils_http import get_session


def _fetch_html(url: str) -> str:
    """使用共用 session 取得 HTML 文字內容。"""
    session = get_session()
    resp = session.get(url, timeout=10)
    resp.raise_for_status()
    resp.encoding = resp.encoding or "utf-8"
    return resp.text


def _extract_tables(html: str) -> List[pd.DataFrame]:
    """
    從 HTML 中提取所有可能的表格。

    不直接對整個 HTML 調用 read_html，而是逐個 table 處理，
    避免把頁面上的說明文字或奇怪結構一起吃掉。
    """
    soup = BeautifulSoup(html, "lxml")
    tables: List[pd.DataFrame] = []

    for tbl in soup.find_all("table"):
        try:
            dfs = pd.read_html(StringIO(str(tbl)))
        except ValueError:
            continue
        for df in dfs:
            if not df.empty:
                tables.append(df)

    return tables


def _pick_stock_table(tables: List[pd.DataFrame]) -> pd.DataFrame:
    """
    從多個表格中挑出「包含股票代號與名稱」的那一個。

    判斷條件：
    - 欄名中要同時包含「名稱」與「代號 / 股票」其中之一。
    """
    for t in tables:
        cols_str = "".join(str(c) for c in t.columns)
        if "名稱" in cols_str and ("代號" in cols_str or "股票" in cols_str):
            df = t.copy()
            df.columns = [str(c).strip() for c in df.columns]
            return df

    raise ValueError("找不到同時包含名稱與代號的股票表格")


def _find_column(df: pd.DataFrame, keywords: List[str]) -> str:
    """
    在欄名中尋找第一個包含任一關鍵字的欄位名稱。
    找不到時丟出錯誤，避免使用硬編 index 導致爆炸。
    """
    for col in df.columns:
        for kw in keywords:
            if kw in str(col):
                return col
    raise ValueError(f"找不到欄位，關鍵字: {keywords}，現有欄名: {list(df.columns)}")


def _parse_limitup_table(html: str, market: str, trade_date: date, min_pct_change: float) -> pd.DataFrame:
    """
    從單一市場的漲幅排行頁面 HTML 中，解析出「已達漲停門檻」的股票清單。

    會回傳至少包含：
    - symbol: 股票代碼
    - name: 股票名稱
    - pct_change: 漲跌幅（數值，不含 %）
    - market: 市場別 (TWSE / TPEX)
    - date: 交易日期 (YYYY-MM-DD)
    以及原始表格的其他欄位。
    """
    tables = _extract_tables(html)
    if not tables:
        raise ValueError(f"[{market}] 找不到任何表格，可能來源頁面變更或被擋")

    df = _pick_stock_table(tables)

    # 嘗試找代號 / 名稱 / 漲幅三種欄位
    symbol_col = _find_column(df, ["代號", "股票代號", "證券代號", "股票"])
    name_col = _find_column(df, ["名稱", "證券名稱"])
    pct_col = _find_column(df, ["漲幅", "漲跌幅", "幅度"])

    # 處理漲幅欄位：移除 %，轉成 float
    pct_series = (
        df[pct_col]
        .astype(str)
        .str.replace("%", "", regex=False)
        .str.replace(",", "", regex=False)
        .str.strip()
    )
    df[pct_col] = pd.to_numeric(pct_series, errors="coerce")

    # 篩選達到漲停門檻的列
    df = df[df[pct_col] >= min_pct_change].copy()

    # 建立標準欄位
    df["symbol"] = df[symbol_col].astype(str).str.strip()
    df["name"] = df[name_col].astype(str).str.strip()
    df["pct_change"] = df[pct_col]
    df["market"] = market
    df["date"] = trade_date.isoformat()

    # 避免有奇怪的 NaN 或空代碼
    df = df[df["symbol"] != ""]
    df = df.dropna(subset=["symbol"])

    return df.reset_index(drop=True)


def fetch_twse_limitup_list(trade_date: date, url: str, min_pct_change: float) -> pd.DataFrame:
    """
    抓取「上市」漲停股清單。
    """
    html = _fetch_html(url)
    return _parse_limitup_table(html, market="TWSE", trade_date=trade_date, min_pct_change=min_pct_change)


def fetch_tpex_limitup_list(trade_date: date, url: str, min_pct_change: float) -> pd.DataFrame:
    """
    抓取「上櫃」漲停股清單。
    """
    html = _fetch_html(url)
    return _parse_limitup_table(html, market="TPEX", trade_date=trade_date, min_pct_change=min_pct_change)


def fetch_limitup_lists(
    trade_date: date,
    twse_url: str,
    tpex_url: str,
    min_pct_change: float,
) -> pd.DataFrame:
    """
    同時抓取「上市 + 上櫃」的漲停股清單並合併。
    """
    frames: List[pd.DataFrame] = []

    if twse_url:
        try:
            twse_df = fetch_twse_limitup_list(trade_date, twse_url, min_pct_change)
            frames.append(twse_df)
        except Exception as e:
            # 保留 log 訊息給呼叫端決定如何處理
            print(f"[WARN] TWSE 漲停清單抓取失敗：{e}")

    if tpex_url:
        try:
            tpex_df = fetch_tpex_limitup_list(trade_date, tpex_url, min_pct_change)
            frames.append(tpex_df)
        except Exception as e:
            print(f"[WARN] TPEX 漲停清單抓取失敗：{e}")

    if not frames:
        # 讓上層知道真的完全抓不到任何資料
        raise ValueError("上市與上櫃漲停清單皆無法取得")

    return pd.concat(frames, ignore_index=True)
