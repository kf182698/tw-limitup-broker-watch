"""Simple email utility using SendGrid or Gmail SMTP."""

from typing import Iterable, Dict, Any, List
import os
import json
import requests
import smtplib
from email.mime.text import MIMEText


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


def send_email(subject: str, html_body: str, to_addrs: Iterable[str]) -> bool:
    """Send an email using SendGrid if available, otherwise Gmail SMTP.

    Args:
        subject: Subject line.
        html_body: HTML content.
        to_addrs: Iterable of recipient email addresses.

    Returns:
        True if the email was sent, False otherwise.
    """
    to_list = list(to_addrs)
    # Try SendGrid first
    sg_key = os.getenv("SENDGRID_API_KEY")
    if sg_key:
        from_addr = os.getenv("SENDGRID_FROM_EMAIL") or os.getenv("GMAIL_USER") or (to_list[0] if to_list else None)
        if not from_addr:
            return False
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
            return response.status_code < 400
        except Exception:
            pass
    # Fallback to Gmail SMTP
    gmail_user = os.getenv("GMAIL_USER")
    gmail_pass = os.getenv("GMAIL_PASS")
    if gmail_user and gmail_pass:
        msg = MIMEText(html_body, "html", "utf-8")
        msg["Subject"] = subject
        msg["From"] = gmail_user
        msg["To"] = ", ".join(to_list)
        try:
            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
                server.login(gmail_user, gmail_pass)
                server.sendmail(gmail_user, to_list, msg.as_string())
            return True
        except Exception:
            return False
    # No provider configured
    return False