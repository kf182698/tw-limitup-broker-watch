"""Build the list of stocks whose top buyer broker matches user targets."""

from typing import Dict, Any
import pandas as pd

from source_site.broker_detail import build_broker_url, fetch_broker_html, parse_broker_table
from app.broker_matcher import match_target_broker
from app.utils_io import write_csv


def build_broker_hits(
    trade_date: str,
    limitup_df: pd.DataFrame,
    broker_url_template: str,
    broker_config: Dict[str, Any],
) -> pd.DataFrame:
    """For each stock in the limit‑up list, fetch the broker detail and check
    if the top buyer broker matches the user's target list.

    Returns a DataFrame with columns:
        trade_date, stock_name, code, close, volume, pct_change,
        broker_name, broker_code, buy_volume
    """
    hits = []
    if not broker_url_template:
        raise ValueError("broker_detail_url_template must be provided in settings")
    for _, row in limitup_df.iterrows():
        code = str(row.get("code"))
        stock_name = row.get("stock_name")
        close = row.get("close")
        volume = row.get("volume")
        pct_change = row.get("pct_change")
        # Build URL and fetch broker detail
        url = build_broker_url(broker_url_template, code)
        try:
            html = fetch_broker_html(url)
        except Exception:
            continue
        bdf = parse_broker_table(html)
        if bdf.empty:
            continue
        # Determine top buyer by buy_volume descending
        top = bdf.sort_values("buy_volume", ascending=False).iloc[0]
        ok, meta = match_target_broker(
            top.get("broker_name", ""), broker_config, top.get("buy_ratio")
        )
        if ok:
            hits.append(
                {
                    "trade_date": trade_date,
                    "stock_name": stock_name,
                    "code": code,
                    "close": close,
                    "volume": volume,
                    "pct_change": pct_change,
                    "broker_name": top.get("broker_name"),
                    # If broker_config includes code, it would be in meta
                    "broker_code": meta.get("code", ""),
                    "buy_volume": int(top.get("buy_volume", 0)),
                }
            )
    out_df = pd.DataFrame(hits)
    if not out_df.empty:
        write_csv(out_df, f"data_clean/broker_hits_{trade_date}.csv")
    return out_df
