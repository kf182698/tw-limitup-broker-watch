"""
Pipeline step for building the daily limit-up list.

這個模組會：
1. 呼叫 source_site.limitup_list.fetch_limitup_lists 取得「上市 + 上櫃」漲停股清單
2. 將結果寫入 data_clean/limitup_YYYY-MM-DD.csv
3. 回傳 DataFrame，給後續主力分點篩選使用
"""

from pathlib import Path
from datetime import date as DateType
from typing import Union

import pandas as pd

from source_site.limitup_list import fetch_limitup_lists
from app.utils_io import write_csv


def _get_date_str(trade_date: Union[DateType, str]) -> str:
    """
    trade_date 可能是 datetime.date 或 str，這裡統一轉成 YYYY-MM-DD 字串。
    """
    if isinstance(trade_date, DateType):
        return trade_date.isoformat()
    return str(trade_date)


def build_limitup_list(
    trade_date: Union[DateType, str],
    twse_url: str,
    tpex_url: str,
    min_pct: float,
) -> pd.DataFrame:
    """
    建立指定交易日的漲停股清單（上市 + 上櫃）。

    Parameters
    ----------
    trade_date : datetime.date or str
        交易日期，可以是 date 或 'YYYY-MM-DD' 字串
    twse_url : str
        （目前為相容接口，實際已由 source_site.limitup_list 內部改用官方 open data）
    tpex_url : str
        同上
    min_pct : float
        判定漲停的最小漲幅門檻（例如 9.8）

    Returns
    -------
    df : pandas.DataFrame
        合併後的漲停股清單
    """
    # 1. 取得「上市 + 上櫃」漲停股列表
    df = fetch_limitup_lists(trade_date, twse_url, tpex_url, min_pct)

    # 2. 決定輸出路徑
    root = Path(__file__).resolve().parents[2]  # repo root
    out_dir = root / "data_clean"
    out_dir.mkdir(parents=True, exist_ok=True)

    date_str = _get_date_str(trade_date)
    out_path = out_dir / f"limitup_{date_str}.csv"

    # 3. 寫出 CSV
    write_csv(df, str(out_path))

    return df