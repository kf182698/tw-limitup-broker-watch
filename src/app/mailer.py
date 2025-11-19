"""Simple email utility using SendGrid or Gmail SMTP."""

from typing import Iterable, Dict, Any, List
import os
import json
import requests
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr

# --- 保持不變 ---
def render_html_table(rows: List[Dict[str, Any]]) -> str:
    """Render a list of dicts into a basic HTML table.

    Rows should all have the same keys. Keys will be used as table headers.
    """
    if not rows:
        return "<p>No data.</p>"
    headers = list(rows[0].keys())
    thead = "".join(f"<th>{h}</th>" for h in headers)
    body_rows = []
    for row in rows:
        tds = "".join(f"<td>{row.get(h, '')}</td>" for h in headers)
        body_rows.append(f"<tr>{tds}</tr>")
    tbody = "".join(body_rows)
    return (
        "<table border='1' cellspacing='0' cellpadding='4'>"
        f"<thead><tr>{thead}</tr></thead>"
        f"<tbody>{tbody}</tbody>"
        "</table>"
    )
# --- 保持不變 ---


def send_email(subject: str, html_body: str, to_addrs: Iterable[str], username: str, password: str) -> bool:
    """Send an email using SendGrid if available, otherwise Gmail SMTP.
    
    Args:
        subject: Subject line.
        html_body: HTML content.
        to_addrs: Iterable of recipient email addresses.
        username: Sender email address (from EMAIL_USERNAME secret).
        password: Sender password/API Key (from EMAIL_PASSWORD secret).

    Returns:
        True if the email was sent successfully.
        Raises an Exception otherwise, to avoid silent failure.
    """
    to_list = list(to_addrs)
    if not to_list:
        raise ValueError("Recipient address list is empty.")
    
    # 判斷是否使用 SendGrid (假設 API Key 傳入在 password 欄位，且長度足夠)
    # 這是為了兼容您原本的 SendGrid/Gmail 雙重邏輯
    is_sendgrid = len(password) > 30 and 'SG.' in password
    
    if is_sendgrid:
        # === SendGrid 邏輯 ===
        # 假設 EMAIL_PASSWORD 包含了 SendGrid API Key
        sg_key = password 
        from_addr = username
        
        payload = {
            "personalizations": [
                {
                    "to": [{"email": addr} for addr in to_list],
                    "subject": subject,
                }
            ],
            "from": {"email": from_addr},
            "content": [
                {
                    "type": "text/html",
                    "value": html_body,
                }
            ],
        }
        
        try:
            response = requests.post(
                "https://api.sendgrid.com/v3/mail/send",
                headers={
                    "Authorization": f"Bearer {sg_key}",
                    "Content-Type": "application/json",
                },
                data=json.dumps(payload),
                timeout=10,
            )
            # 檢查狀態碼，非 2xx 則視為失敗
            if response.status_code >= 400:
                 raise requests.exceptions.HTTPError(f"SendGrid API responded with status code {response.status_code}: {response.text}")

            return True
        except requests.exceptions.RequestException as e:
            # 捕獲網路或 API 錯誤
            raise RuntimeError(f"SendGrid 發送失敗 (API Error): {e}")

    else:
        # === Gmail SMTP 邏輯 (Fallback) ===
        # 假設 EMAIL_USERNAME/EMAIL_PASSWORD 是 Gmail 帳號和應用程式密碼
        
        # 建立郵件內容
        msg = MIMEText(html_body, "html", "utf-8")
        msg["Subject"] = subject
        msg["From"] = formataddr(('Broker Watch Report', username)) # 使用 formataddr 設定寄件人名稱
        msg["To"] = ", ".join(to_list)

        try:
            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
                server.login(username, password)
                server.sendmail(username, to_list, msg.as_string())
            return True
        except smtplib.SMTPAuthenticationError:
             raise smtplib.SMTPAuthenticationError("Gmail 登入失敗：帳號或應用程式密碼錯誤。")
        except smtplib.SMTPException as e:
             # 捕獲其他 SMTP 相關錯誤
             raise RuntimeError(f"Gmail SMTP 發送失敗: {e}")
        except Exception as e:
             # 捕獲其他非 SMTP 錯誤
             raise RuntimeError(f"郵件發送發生未預期錯誤: {e}")
