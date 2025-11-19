"""HTTP helpers for the LimitUp Broker Watch project."""

from typing import Optional
import requests
from requests.adapters import HTTPAdapter, Retry


def get_session() -> requests.Session:
    """Return a requests.Session configured with retry and a custom User-Agent."""
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (LimitUpBrokerWatch/1.0; +https://example.com)",
        }
    )
    return session