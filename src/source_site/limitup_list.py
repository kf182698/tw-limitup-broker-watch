from __future__ import annotations

from datetime import date
from io import StringIO
from typing import List

import pandas as pd

from app.utils_http import get_session


# 官方開放資料 URL（固定使用，不再吃 settings 裡的 Fubon 連結）
TWSE_STOCK_DAY_ALL_URL = "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=open_data"
TPEX_DAILY_CLOSE_URL = (
    "https://www.tpex.org.tw/web/stock/aftertrading/DAILY_CLOSE_quotes/"
    "stk_quote_result.php?l=zh-tw&o=data"
)


def _fetch_csv(url: str) -> pd.DataFrame:
    """
    用共用 session 下載 CSV 並讀成 DataFrame。
    """
    session = get_session()
    resp = session.get(url, timeout=15)
    resp.raise_for_status()

    # 有些 CSV 會帶 BOM，用 utf-8-sig 比較保險
    text = resp.content.decode("utf-8-sig")
    df = pd.read_csv(StringIO(text))
    # 把欄名做 strip，避免前後有空白
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _to_float(series: pd.Series) -> pd.Series:
    """
    將包含逗號、空白或其他符號的數值欄位轉成 float。
    """
    cleaned = (
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("＋", "+", regex=False)
        .str.replace("－", "-", regex=False)
        .str.replace(" ", "", regex=False)
        .str.strip()
    )
    return pd.to_numeric(cleaned, errors="coerce")


def _parse_twse_limitup(trade_date: date, min_pct_change: float) -> pd.DataFrame:
    """
    從 TWSE 開放資料（STOCK_DAY_ALL）抓「上市」當日漲停股。
    """
    df = _fetch_csv(TWSE_STOCK_DAY_ALL_URL)

    # 依欄名關鍵字找出需要的欄位
    def find_col(keywords: List[str]) -> str:
        for col in df.columns:
            for kw in keywords:
                if kw in str(col):
                    return col
        raise ValueError(f"TWSE CSV 找不到欄位，關鍵字: {keywords}，現有欄名: {list(df.columns)}")

    date_col = find_col(["日期"])
    symbol_col = find_col(["證券代號", "代號"])
    name_col = find_col(["證券名稱", "名稱"])
    close_col = find_col(["收盤價", "收盤"])
    change_col = find_col(["漲跌價差", "漲跌"])
    volume_col = find_col(["成交股數"])
    turnover_col = find_col(["成交金額"])

    # 轉數值
    close = _to_float(df[close_col])
    change = _to_float(df[change_col])

    # 前一日收盤 = 收盤價 - 漲跌價差
    prev_close = close - change
    # 漲跌幅 % = 漲跌價差 / 前一日收盤
    pct_change = (change / prev_close) * 100.0
    df["pct_change"] = pct_change

    # 基本欄位整理
    df["symbol"] = df[symbol_col].astype(str).str.strip()
    df["name"] = df[name_col].astype(str).str.strip()
    df["close"] = close
    df["change"] = change
    df["volume"] = _to_float(df[volume_col])
    df["turnover"] = _to_float(df[turnover_col])
    df["market"] = "TWSE"
    df["date"] = trade_date.isoformat()

    # 移除無效 symbol（例如「合計」等）
    df = df[df["symbol"].str.fullmatch(r"\d+")]
    df = df.dropna(subset=["close", "pct_change"])

    # 只保留達到漲停門檻的
    df = df[df["pct_change"] >= min_pct_change].copy()

    return df.reset_index(drop=True)


def _parse_tpex_limitup(trade_date: date, min_pct_change: float) -> pd.DataFrame:
    """
    從 TPEX 開放資料（DAILY_CLOSE_quotes）抓「上櫃」當日漲停股。
    """
    df = _fetch_csv(TPEX_DAILY_CLOSE_URL)

    # 依欄名關鍵字找出需要的欄位
    def find_col(keywords: List[str]) -> str:
        for col in df.columns:
            for kw in keywords:
                if kw in str(col):
                    return col
        raise ValueError(f"TPEX CSV 找不到欄位，關鍵字: {keywords}，現有欄名: {list(df.columns)}")

    # 參考資料集欄位說明：
    # 資料日期;代號;名稱;收盤;漲跌;開盤;最高;最低;均價;成交股數;成交金額;成交筆數;... 
    symbol_col = find_col(["代號"])
    name_col = find_col(["名稱"])
    close_col = find_col(["收盤"])
    change_col = find_col(["漲跌"])
    volume_col = find_col(["成交股數"])
    turnover_col = find_col(["成交金額"])

    close = _to_float(df[close_col])
    change = _to_float(df[change_col])

    # 前一日收盤 = 收盤價 - 漲跌
    prev_close = close - change
    pct_change = (change / prev_close) * 100.0
    df["pct_change"] = pct_change

    df["symbol"] = df[symbol_col].astype(str).str.strip()
    df["name"] = df[name_col].astype(str).str.strip()
    df["close"] = close
    df["change"] = change
    df["volume"] = _to_float(df[volume_col])
    df["turnover"] = _to_float(df[turnover_col])
    df["market"] = "TPEX"
    df["date"] = trade_date.isoformat()

    # 只保留正常股票代碼
    df = df[df["symbol"].str.fullmatch(r"\d+")]
    df = df.dropna(subset=["close", "pct_change"])

    df = df[df["pct_change"] >= min_pct_change].copy()

    return df.reset_index(drop=True)


def fetch_twse_limitup_list(trade_date: date, url: str, min_pct_change: float) -> pd.DataFrame:
    """
    抓取「上市」漲停股清單。

    注意：為了穩定性，**不再使用傳入的 url 參數**，統一改用官方 open data。
    url 參數只保留介面相容性，實際會被忽略。
    """
    return _parse_twse_limitup(trade_date, min_pct_change)


def fetch_tpex_limitup_list(trade_date: date, url: str, min_pct_change: float) -> pd.DataFrame:
    """
    抓取「上櫃」漲停股清單。

    同樣忽略傳入的 url，直接用官方 open data。
    """
    return _parse_tpex_limitup(trade_date, min_pct_change)


def fetch_limitup_lists(
    trade_date: date,
    twse_url: str,
    tpex_url: str,
    min_pct_change: float,
) -> pd.DataFrame:
    """
    同時抓取「上市 + 上櫃」漲停股清單並合併。

    這裡的 twse_url / tpex_url 僅為保留舊介面，不再實際使用。
    """
    frames: List[pd.DataFrame] = []

    try:
        twse_df = fetch_twse_limitup_list(trade_date, twse_url, min_pct_change)
        frames.append(twse_df)
    except Exception as e:
        print(f"[WARN] TWSE 漲停清單抓取失敗：{e}")

    try:
        tpex_df = fetch_tpex_limitup_list(trade_date, tpex_url, min_pct_change)
        frames.append(tpex_df)
    except Exception as e:
        print(f"[WARN] TPEX 漲停清單抓取失敗：{e}")

    if not frames:
        raise ValueError("上市與上櫃漲停清單皆無法取得（官方 open data 來源均失敗）")

    return pd.concat(frames, ignore_index=True)
