"""Command line interface for the LimitUp Broker Watch pipeline."""

import argparse
import yaml
from pathlib import Path

from .utils_dates import parse_date
from .mailer import render_html_table, send_email
from ..pipeline.build_limitup_list import build_limitup_list
from ..pipeline.build_broker_hits import build_broker_hits
from ..pipeline.build_email_context import build_email_rows


def load_yaml(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def run_for_date(date_str: str) -> None:
    """Execute the pipeline for a single trading date."""
    settings = load_yaml(Path(__file__).parents[2] / "config" / "settings.yaml")
    brokers_conf = load_yaml(Path(__file__).parents[2] / "config" / "brokers.yaml")
    # Resolve date
    tz = settings.get("timezone", "Asia/Taipei")
    trade_date = parse_date(date_str, tz)
    # Build limit up list
    limitup_url = settings.get("source", {}).get("limitup_url")
    min_pct = settings.get("limitup", {}).get("min_pct_change")
    limitup_df = build_limitup_list(trade_date, limitup_url, min_pct)
    # Build broker hits
    broker_template = settings.get("source", {}).get("broker_detail_url_template")
    hits_df = build_broker_hits(trade_date, limitup_df, broker_template, brokers_conf)
    if hits_df is not None and not hits_df.empty:
        # Prepare email
        email_rows = build_email_rows(hits_df)
        subject_prefix = settings.get("email", {}).get("subject_prefix", "隔沖主力鎖漲停標的")
        subject = f"{subject_prefix} {trade_date}"
        to_list = settings.get("email", {}).get("to", [])
        html_body = render_html_table(email_rows)
        send_email(subject, html_body, to_list)


def main() -> None:
    parser = argparse.ArgumentParser(description="LimitUp Broker Watch CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)
    run_cmd = sub.add_parser("run", help="Run pipeline for a specific date")
    run_cmd.add_argument("--date", required=True, help="Date in YYYY-MM-DD format or 'today'")
    args = parser.parse_args()
    if args.cmd == "run":
        run_for_date(args.date)


if __name__ == "__main__":
    main()