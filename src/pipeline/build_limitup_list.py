"""
Pipeline step for building the daily limit-up list.

這個模組會：
1. 呼叫 source_site.limitup_list.fetch_limitup_lists 取得「上市 + 上櫃」漲停股清單
2. 將結果寫入 data_clean/limitup_YYYY-MM-DD.csv
3. 回傳 DataFrame，給後續主力分點篩選使用
"""

from pathlib import Path
from datetime import date
import pandas as pd

# ⚠ 一律使用「絕對匯入」，避免 python -m app.main 時出現
# ImportError: attempted relative import beyond top-level package
from source_site.limitup_list import fetch_limitup_lists
from app.utils_io import write_csv


def build_limitup_list(
    trade_date: date,
    twse_url: str,
    tpex_url: str,
    min_pct: float,
) -> pd.DataFrame:
    """
    建立指定交易日的漲停股清單（上市 + 上櫃）。

    Parameters
    ----------
    trade_date : datetime.date
        交易日期
    twse_url : str
        富邦（或其他來源）的「上市股價漲幅排行」網址
    tpex_url : str
        富邦（或其他來源）的「上櫃股價漲幅排行」網址
    min_pct : float
        判定漲停的最小漲幅門檻（例如 9.8）

    Returns
    -------
    df : pandas.DataFrame
        合併後的漲停股清單（至少包含：代號、名稱、收盤價、成交量、漲跌幅、market 等欄位）
    """
    # 1. 取得「上市 + 上櫃」漲停股列表
    df = fetch_limitup_lists(trade_date, twse_url, tpex_url, min_pct)

    # 2. 寫出到 data_clean/limitup_YYYY-MM-DD.csv
    root = Path(__file__).resolve().parents[2]  # repo root
    out_dir = root / "data_clean"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"limitup_{trade_date.isoformat()}.csv"
    write_csv(df, str(out_path))

    return df
