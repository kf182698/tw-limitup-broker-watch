"""Fetch and parse the limit-up (漲停) list from the configured source."""

from typing import Optional, Any
import pandas as pd
from io import StringIO
from datetime import date
# from ..app.utils_http import get_session # <-- 移除有問題的相對導入
import requests # <-- 確保 requests 模組可用 (原程式碼有用到 requests.exceptions)


class LimitUpListError(Exception):
    """Base exception for limit-up list fetching and parsing errors."""
    pass


def fetch_limitup_html(url: str) -> str:
    """Fetch the HTML from the given limit-up list URL.

    **已修正：直接使用 requests.get 替換 get_session()，以避免 NameError。**
    Handles response encoding. Raises LimitUpListError on network failure.
    """
    try:
        # sess = get_session() # <-- 移除原始程式碼
        # resp = sess.get(url, timeout=20) # <-- 移除原始程式碼
        
        # 修正：直接使用 requests.get 
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()  # 檢查 HTTP 錯誤
        
        # 設置編碼：使用伺服器/Apparent，最終回退到 UTF-8
        resp.encoding = resp.encoding or resp.apparent_encoding or "utf-8"
        
        return resp.text
    except requests.exceptions.RequestException as e:
        raise LimitUpListError(f"Network error fetching limit-up list from {url}: {e}")


def parse_limitup_table(html: str, trade_date: str) -> pd.DataFrame:
    """Parse the first table in the HTML as a limit-up list.

    **已修正：解決 Pandas FutureWarning 和 AttributeError。**
    The resulting DataFrame will always contain these columns: trade_date, 
    code, stock_name, market, close, volume, pct_change.
    Raises LimitUpListError if no tables can be parsed.
    """
    # -------------------------------------------------------------
    # 🎯 修復 Pandas FutureWarning：使用 StringIO
    # -------------------------------------------------------------
    try:
        tables = pd.read_html(StringIO(html)) 
    except Exception as e:
        raise LimitUpListError(f"Failed to parse HTML tables: {e}")
        
    if not tables:
        raise LimitUpListError("No tables found in the HTML content.")
        
    df = tables[0].copy()
    
    # 欄位映射
    rename_map = {}
    standard_columns = {
        "stock_name": ["股票", "名稱", "證券"],
        "code": ["代號", "股票代號"],
        "close": ["收盤", "價格"],
        "volume": ["成交", "量", "股"],
        "pct_change": ["漲跌", "%", "幅度"],
    }

    for col in df.columns:
        col_str = str(col).strip()
        for std_name, keywords in standard_columns.items():
            if any(k in col_str for k in keywords):
                rename_map[col] = std_name
                break
                
    df = df.rename(columns=rename_map)

    # 數據清洗與類型轉換
    numeric_cols = ["pct_change", "close", "volume"]
    for col in numeric_cols:
        if col in df.columns:
            # 移除逗號和百分號，然後轉換為數字
            df[col] = df[col].astype(str).str.replace(r'[^\d\.\-]', '', regex=True) 
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 確保最終 DataFrame 結構完整
    result = pd.DataFrame()
    
    # 🎯 修正：AttributeError: 'str' object has no attribute 'strftime'
    result["trade_date"] = trade_date # 直接使用傳入的字串日期
    
    result["code"] = df.get("code", pd.Series(dtype=str)).astype(str).str.strip()
    result["stock_name"] = df.get("stock_name", pd.Series(dtype=str)).astype(str).str.strip()
    result["market"] = None # 保持為 None，等待後續判斷 (如 TPEX, TAI)
    result["close"] = df.get("close")
    result["volume"] = df.get("volume")
    result["pct_change"] = df.get("pct_change")
    
    # 刪除 code 或 pct_change 為空的行
    result = result.dropna(subset=["code", "pct_change"])

    return result


def build_limitup_list(trade_date: date, limitup_url: str, min_pct: float) -> Optional[pd.DataFrame]:
    """
    Main function to execute the fetching, parsing, and filtering pipeline.
    
    **注意：這裡假設 trade_date 是一個 datetime.date 物件，用於格式化。**
    """
    try:
        # 將 date 物件格式化為字串，傳遞給 parse_limitup_table
        trade_date_str = trade_date.strftime("%Y-%m-%d")
        
        html = fetch_limitup_html(limitup_url)
        df = parse_limitup_table(html, trade_date_str)
        
        # 篩選出漲停股票 (確保 pct_change 存在且大於等於 min_pct)
        if df.empty or "pct_change" not in df.columns:
            return None
            
        limitup_df = df[df["pct_change"] >= min_pct]
        
        return limitup_df
        
    except LimitUpListError as e:
        print(f"Error in limit-up list pipeline: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None
