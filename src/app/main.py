"""
Main entry point for the Limit-Up Broker Watch project.

功能：
1. 抓取今日漲停股票
2. 抓取每檔買超券商資料並篩選主力分點
3. 產生 email 報表（不論有無命中標的，都會寄信）
"""

import argparse
import yaml
from pathlib import Path
import os
import sys
from datetime import datetime

from .utils_dates import parse_date
from .mailer import render_html_table, send_email
from ..pipeline.build_limitup_list import build_limitup_list
from ..pipeline.build_broker_hits import build_broker_hits
from ..pipeline.build_email_context import build_email_rows


def load_settings():
    """讀取 config/settings.yaml 與 config/brokers.yaml"""
    root = Path(__file__).parents[2]

    settings_path = root / "config" / "settings.yaml"
    brokers_path = root / "config" / "brokers.yaml"

    with open(settings_path, "r", encoding="utf-8") as f:
        settings = yaml.safe_load(f)

    with open(brokers_path, "r", encoding="utf-8") as f:
        brokers = yaml.safe_load(f)

    return settings, brokers


def get_email_credentials():
    """讀取 GitHub Secrets 或本地環境變數"""
    username = os.getenv("EMAIL_USERNAME")
    password = os.getenv("EMAIL_PASSWORD")
    to_env = os.getenv("EMAIL_TO", "").strip()

    to_list = [addr.strip() for addr in to_env.split(",") if addr.strip()]

    return username, password, to_list


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default="today")
    args = parser.parse_args()

    # 讀設定
    settings, brokers_config = load_settings()

    # 解析日期
    trade_date = parse_date(args.date, settings["timezone"])

    print(f"[INFO] 開始執行：{trade_date}")

    # 1. 取得漲停清單
    limitup_df = build_limitup_list(
        trade_date,
        settings["source"]["limitup_url"],
        settings["limitup"]["min_pct_change"],
    )

    total_limitup = len(limitup_df)
    print(f"[INFO] 今日漲停檔數：{total_limitup}")

    # 2. 主力分點篩選
    hits_df = build_broker_hits(
        trade_date,
        limitup_df,
        settings["source"]["broker_detail_url_template"],
        brokers_config,
    )

    # 3. 準備 email rows
    rows = build_email_rows(hits_df)
    total_hits = len(rows)

    print(f"[INFO] 今日命中券商標的數：{total_hits}")

    # 4. 設定信件標題與內容
    subject_prefix = settings["email"]["subject_prefix"]
    subject = f"{subject_prefix} {trade_date}"

    if total_hits > 0:
        # 有標的 → 正常表格
        html_body = "<p>今日符合條件的標的如下：</p>"
        html_body += render_html_table(rows)
    else:
        # 無標的 → 明確說明
        html_body = (
            f"<p>{trade_date} 無任何漲停股的買超第一名券商 "
            f"符合設定的主力分點清單。</p>"
            f"<p>漲停檔數：{total_limitup}，命中標的數：{total_hits}</p>"
        )

    # 5. 讀取 Email 憑證
    username, password, to_list = get_email_credentials()

    if not username or not password or not to_list:
        print("[ERROR] Email 憑證未設定完整（EMAIL_USERNAME / EMAIL_PASSWORD / EMAIL_TO）")
        sys.exit(1)

    # 6. 嘗試寄信並顯示成功/失敗訊息
    try:
        ok = send_email(subject, html_body, to_list, username, password)
        if ok:
            print(f"[INFO] 郵件成功寄出 → {', '.join(to_list)}")
        else:
            print("[ERROR] send_email 回傳 False（寄信可能失敗）")
            sys.exit(1)
    except Exception as e:
        print(f"[ERROR] 寄信過程發生例外：{e}")
        sys.exit(1)

    print("[INFO] 程式執行完成")
    return 0


if __name__ == "__main__":
    main()
