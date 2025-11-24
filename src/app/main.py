"""
Main entry point for the Limit-Up Broker Watch project (multi-market version).

This script orchestrates the daily pipeline:

1. Fetch the limit‑up lists for both TWSE (上市) and TPEX (上櫃) using the URLs
   specified in config/settings.yaml.
2. Save the combined list to ``data_clean/limitup_YYYY-MM-DD.csv``.
3. Fetch the detailed broker buy/sell reports for each limit‑up stock and
   evaluate whether the top buy broker matches a target list defined in
   ``config/brokers.yaml``.
4. Save the hits to ``data_clean/broker_hits_YYYY-MM-DD.csv``.
5. Compose and send an email summarising the results. A notification is sent
   even when no hits are found.
"""

import argparse
import os
import sys
from pathlib import Path
import yaml

from app.utils_dates import parse_date
from app.mailer import render_html_table, send_email
from pipeline.build_limitup_list import build_limitup_list
from pipeline.build_broker_hits import build_broker_hits
from pipeline.build_email_context import build_email_rows


def load_settings():
    """Load project settings and broker configuration from YAML files."""
    root = Path(__file__).parents[2]
    settings_path = root / "config" / "settings.yaml"
    brokers_path = root / "config" / "brokers.yaml"
    with open(settings_path, "r", encoding="utf-8") as f:
        settings = yaml.safe_load(f)
    with open(brokers_path, "r", encoding="utf-8") as f:
        brokers = yaml.safe_load(f)
    return settings, brokers


def get_email_credentials():
    """Retrieve email credentials from environment variables (GitHub Secrets)."""
    username = os.getenv("EMAIL_USERNAME")
    password = os.getenv("EMAIL_PASSWORD")
    to_env = os.getenv("EMAIL_TO", "").strip()
    to_list = [addr.strip() for addr in to_env.split(",") if addr.strip()]
    return username, password, to_list


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default="today", help="Date to run the pipeline for (YYYY-MM-DD or 'today')")
    args = parser.parse_args()

    settings, brokers_config = load_settings()
    trade_date = parse_date(args.date, settings["timezone"])
    print(f"[INFO] Processing date: {trade_date}")

    # Fetch limit‑up lists for TWSE and TPEX
    twse_url = settings["source"]["twse_limitup_url"]
    tpex_url = settings["source"]["tpex_limitup_url"]
    min_pct = float(settings["limitup"]["min_pct_change"])
    limitup_df = build_limitup_list(trade_date, twse_url, tpex_url, min_pct)
    print(f"[INFO] Total limit‑up stocks (TWSE + TPEX): {len(limitup_df)}")

    # Build broker hits
    hits_df = build_broker_hits(
        trade_date,
        limitup_df,
        settings["source"]["broker_detail_url_template"],
        brokers_config,
    )
    rows = build_email_rows(hits_df)
    total_hits = len(rows)
    print(f"[INFO] Stocks meeting broker criteria: {total_hits}")

    # Compose email content
    subject_prefix = settings["email"]["subject_prefix"]
    subject = f"{subject_prefix} {trade_date}"
    if total_hits > 0:
        html_body = "<p>今日符合條件的標的如下：</p>" + render_html_table(rows)
    else:
        html_body = (
            f"<p>{trade_date} 無任何漲停股的買超第一名券商符合設定的主力分點清單。</p>"
            f"<p>漲停檔數：{len(limitup_df)}，命中標的數：{total_hits}</p>"
        )

    # Send email
    username, password, to_list = get_email_credentials()
    if not username or not password or not to_list:
        print("[ERROR] Email credentials missing (EMAIL_USERNAME/EMAIL_PASSWORD/EMAIL_TO)")
        sys.exit(1)
    try:
        ok = send_email(subject, html_body, to_list, username, password)
        if ok:
            print(f"[INFO] Email sent to: {', '.join(to_list)}")
        else:
            print("[ERROR] send_email returned False (email might not have been sent)")
            sys.exit(1)
    except Exception as e:
        print(f"[ERROR] Exception while sending email: {e}")
        sys.exit(1)

    print("[INFO] Pipeline completed successfully")


if __name__ == "__main__":
    main()