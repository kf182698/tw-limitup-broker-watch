"""Build the daily limit‑up list and save it to disk."""

from typing import Optional
import pandas as pd

from source_site.limitup_list import fetch_limitup_html, parse_limitup_table
from app.utils_io import write_csv


def build_limitup_list(trade_date: str, limitup_url: str, min_pct_change: Optional[float]) -> pd.DataFrame:
    """Fetch, parse and filter the daily limit‑up list.

    Args:
        trade_date: Date string (YYYY‑MM‑DD) for which to build the list.
        limitup_url: Fully qualified URL of the limit‑up ranking page.
        min_pct_change: Minimum percent change threshold for filtering. If
            None, no filtering is applied.

    Returns:
        A DataFrame containing the filtered list of limit‑up stocks.
    """
    if not limitup_url:
        raise ValueError("limitup_url must be provided in settings")
    html = fetch_limitup_html(limitup_url)
    raw_df = parse_limitup_table(html, trade_date)
    if min_pct_change is not None and "pct_change" in raw_df.columns:
        try:
            threshold = float(min_pct_change)
            filtered = raw_df[raw_df["pct_change"] >= threshold].copy()
        except Exception:
            filtered = raw_df.copy()
    else:
        filtered = raw_df.copy()
    write_csv(filtered, f"data_clean/limitup_{trade_date}.csv")
    return filtered
