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

    判斷優先順序：
    1. 欄名中同時包含「名稱」以及「代號 / 股票」其一
    2. 若找不到，退而求其次：只要有「代號 / 股票」欄就先當成股票表格
    """
    # 優先：名稱 + 代號 / 股票
    for t in tables:
        cols_str = "".join(str(c) for c in t.columns)
        has_name = "名稱" in cols_str or "證券名稱" in cols_str
        has_code = (
            "代號" in cols_str
            or "股票代號" in cols_str
            or "證券代號" in cols_str
            or "股票" in cols_str
        )
        if has_name and has_code:
            df = t.copy()
            df.columns = [str(c).strip() for c in df.columns]
            return df

    # 次優先：只有代號 / 股票欄
    for t in tables:
        cols_str = "".join(str(c) for c in t.columns)
        has_code_only = (
            "代號" in cols_str
            or "股票代號" in cols_str
            or "證券代號" in cols_str
            or "股票" in cols_str
        )
        if has_code_only:
            df = t.copy()
            df.columns = [str(c).strip() for c in df.columns]
            return df

    raise ValueError("找不到包含股票代號的表格，請檢查來源頁面是否改版")


def _find_column(df: pd.DataFrame, keywords: List[str]) -> str:
    """
    在欄名中尋找第一個包含任一關鍵字的欄位名稱。

    找不到時丟出錯誤，避免使用硬編 index 導致爆炸。
    """
    for col in df.columns:
        for kw in keywords:
            if kw in str(col):
                return col

    raise ValueError(
        f"找不到欄位，關鍵字: {keywords}，現有欄名: {list(df.columns)}"
    )


def _parse_limitup_table(
    html: str,
    market: str,
    trade_date: date,
    min_pct_change: float,
) -> pd.DataFrame:
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
    symbol_col = _find_column(
        df,
        ["代號", "股票代號", "證券代號", "股票"],
    )
    name_col = _find_column(
        df,
        ["名稱", "證券名稱"],
    )
    pct_col = _find_column(
        df,
        ["漲幅", "漲跌幅", "幅度"],
    )

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
def fetch_limitup_lists(
    trade_date: date,
    twse_url: str,
    tpex_url: str,
    min_pct_change: float,
) -> pd.DataFrame:
    """
    取得指定日期的「上市 + 上櫃」漲停股清單。

    參數：
        trade_date: 交易日期
        twse_url:   上市漲幅排行網址
        tpex_url:   上櫃漲幅排行網址
        min_pct_change: 視為「漲停」的最低漲跌幅門檻（例如 9.5）
    回傳：
        合併後的 DataFrame，至少包含：
        - symbol
        - name
        - pct_change
        - market (TWSE / TPEX)
        - date (YYYY-MM-DD)
    """
    # TWSE
    twse_html = _fetch_html(twse_url)
    twse_df = _parse_limitup_table(
        html=twse_html,
        market="TWSE",
        trade_date=trade_date,
        min_pct_change=min_pct_change,
    )

    # TPEX
    tpex_html = _fetch_html(tpex_url)
    tpex_df = _parse_limitup_table(
        html=tpex_html,
        market="TPEX",
        trade_date=trade_date,
        min_pct_change=min_pct_change,
    )

    # 合併兩個市場
    combined = pd.concat([twse_df, tpex_df], ignore_index=True)

    # 標準化欄位命名（若 _parse_limitup_table 已處理，就不會多做事）
    combined = combined.copy()
    if "市場" in combined.columns and "market" not in combined.columns:
        combined = combined.rename(columns={"市場": "market"})
    if "日期" in combined.columns and "date" not in combined.columns:
        combined = combined.rename(columns={"日期": "date"})

    # 確保 date 欄位存在且為字串格式 YYYY-MM-DD
    if "date" not in combined.columns:
        combined["date"] = trade_date.isoformat()
    else:
        combined["date"] = pd.to_datetime(combined["date"], errors="coerce").dt.date
        combined["date"] = combined["date"].fillna(trade_date).astype(str)

    return combined

