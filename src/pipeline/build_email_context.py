"""Prepare email rows for notification."""

from typing import List, Dict, Any
import pandas as pd


def build_email_rows(hits_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Convert the broker hits DataFrame into a list of dictionaries for email."""
    rows: List[Dict[str, Any]] = []
    for _, r in hits_df.iterrows():
        rows.append(
            {
                "股票": r.get("stock_name"),
                "代號": r.get("code"),
                "收盤價": r.get("close"),
                "成交量": r.get("volume"),
                "漲跌幅": r.get("pct_change"),
                "買超券商": r.get("broker_name"),
                "券商代號": r.get("broker_code", ""),
                "買超張": r.get("buy_volume"),
            }
        )
    return rows