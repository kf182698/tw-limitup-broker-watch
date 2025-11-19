"""Command line interface for the LimitUp Broker Watch pipeline."""

import argparse
import yaml
import os # <-- 新增：引入 os 模組來讀取環境變數
from pathlib import Path
from typing import Optional # 確保 Optional 可以使用

from .utils_dates import parse_date
from .mailer import render_html_table, send_email
from ..pipeline.build_limitup_list import build_limitup_list
from ..pipeline.build_broker_hits import build_broker_hits
from ..pipeline.build_email_context import build_email_rows


def load_yaml(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_email_credentials():
    """Reads email credentials and recipient from GitHub Secrets (Environment Variables)."""
    # 讀取您在 Actions 中設定的環境變數
    username = os.getenv('EMAIL_USERNAME')
    password = os.getenv('EMAIL_PASSWORD')
    # 假設 EMAIL_TO 可以有多個收件人，用逗號分隔
    to_list = os.getenv('EMAIL_TO', '').split(',') 

    # 檢查必要的憑證是否齊全
    if not username or not password or not to_list or not to_list[0].strip():
        print("DEBUG-ACTION: ❌ Email 憑證不完整。請檢查 Secrets 中的 EMAIL_USERNAME, EMAIL_PASSWORD, EMAIL_TO。")
        return None, None, None, None

    return username, password, [addr.strip() for addr in to_list if addr.strip()], username # username is also the from_addr


def run_for_date(date_str: str) -> None:
    """Execute the pipeline for a single trading date."""
    
    # 步驟 1: 載入配置與憑證
    settings = load_yaml(Path(__file__).parents[2] / "config" / "settings.yaml")
    brokers_conf = load_yaml(Path(__file__).parents[2] / "config" / "brokers.yaml")
    
    # 從環境變數中獲取憑證
    email_user, email_pass, email_to, email_from = get_email_credentials()
    if not email_user:
        return # 如果憑證不完整，直接退出

    # 處理日期
    tz = settings.get("timezone", "Asia/Taipei")
    trade_date = parse_date(date_str, tz)
    
    # 建立漲停清單
    limitup_url = settings.get("source", {}).get("limitup_url")
    min_pct = settings.get("limitup", {}).get("min_pct_change")
    print(f"DEBUG-ACTION: 1/4 正在建構漲停清單 ({trade_date})...")
    limitup_df = build_limitup_list(trade_date, limitup_url, min_pct)
    
    # 建立主力分點命中清單
    broker_template = settings.get("source", {}).get("broker_detail_url_template")
    print("DEBUG-ACTION: 2/4 正在比對主力分點買賣超資料...")
    hits_df = build_broker_hits(trade_date, limitup_df, broker_template, brokers_conf)
    
    
    # 步驟 2: 檢查最終結果
    if hits_df is not None and not hits_df.empty:
        
        # -------------------------------------------------------------------
        # 偵錯檢查點 A：成功找到符合條件個股
        print(f"DEBUG-ACTION: 🚨 3/4 成功找到符合條件個股 {len(hits_df)} 檔，準備寄信！")
        # -------------------------------------------------------------------

        # 準備 Email 內容
        email_rows = build_email_rows(hits_df)
        subject_prefix = settings.get("email", {}).get("subject_prefix", "隔沖主力鎖漲停標的")
        subject = f"{subject_prefix} {trade_date}"
        html_body = render_html_table(email_rows)
        
        # 發送 Email - 傳入憑證
        try:
             send_email(subject, html_body, email_to, email_user, email_pass, email_from)
             print("DEBUG-ACTION: 4/4 Email 發送完成。")
        except Exception as e:
             # 如果寄信失敗，印出完整的錯誤訊息
             print(f"DEBUG-ACTION: ❌ Email 寄送失敗！錯誤訊息: {e}")
             
    else:
        # -------------------------------------------------------------------
        # 偵錯檢查點 B：沒有找到資料
        print("DEBUG-ACTION: 🟢 3/4 今日沒有符合條件的個股，跳過寫入檔案和 Email 寄送。")
        # -------------------------------------------------------------------

        # 建議：即使沒有標的，也發送一封簡短通知信，以驗證 Email 設置是否正常
        subject = f"隔沖主力監控報告 {trade_date} - (無符合標的)"
        html_body = "<p>今日市場上無符合您設定條件的主力鎖漲停標的，無需操作。</p>"
        
        try:
             send_email(subject, html_body, email_to, email_user, email_pass, email_from)
             print("DEBUG-ACTION: 4/4 發送『無標的通知』Email 完成。")
        except Exception as e:
             print(f"DEBUG-ACTION: ❌ 『無標的通知』Email 寄送失敗！錯誤訊息: {e}")


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
