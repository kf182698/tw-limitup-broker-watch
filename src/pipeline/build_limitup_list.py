"""
Pipeline step for building the daily limit‑up list.

This module uses the ``fetch_limitup_lists`` function from ``source_site.limitup_list``
to retrieve both TWSE and TPEX limit‑up stocks for a given date. It then writes
the combined result to ``data_clean/limitup_YYYY-MM-DD.csv`` and returns the
DataFrame for downstream processing.
"""

from pathlib import Path
from typing import Optional

import pandas as pd

from ..source_site.limitup_list import fetch_limitup_lists
from ..app.utils_io import write_csv


def build_limitup_list(
    trade_date: str,
    twse_url: str,
    tpex_url: str,
    min_pct_change: float
) -> pd.DataFrame:
    """
    Build the combined limit‑up list for TWSE and TPEX for the given date.

    :param trade_date: ISO date string (YYYY-MM-DD)
    :param twse_url: URL for TWSE limit‑up ranking page
    :param tpex_url: URL for TPEX limit‑up ranking page
    :param min_pct_change: Threshold for limit‑up (e.g. 9.8)
    :return: DataFrame of limit‑up stocks from both markets
    """
    df = fetch_limitup_lists(trade_date, twse_url, tpex_url, min_pct_change)
    # Write to data_clean folder
    Path("data_clean").mkdir(parents=True, exist_ok=True)
    out_path = f"data_clean/limitup_{trade_date}.csv"
    write_csv(df, out_path)
    return df