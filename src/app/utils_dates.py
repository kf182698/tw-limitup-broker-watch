"""Date helpers for parsing user input dates."""

from datetime import datetime
from zoneinfo import ZoneInfo


def parse_date(date_str: str, timezone_str: str) -> str:
    """Parse a date string or keyword into an ISO date string.

    If `date_str` is one of "today" or "今日", the current date in the given
    timezone will be returned. Otherwise the input is returned unchanged.
    """
    if not date_str:
        raise ValueError("date_str must be provided")
    lowered = date_str.lower()
    if lowered in {"today", "今日"}:
        tz = ZoneInfo(timezone_str)
        return datetime.now(tz).date().isoformat()
    # simple validation: ensure format is YYYY-MM-DD
    try:
        datetime.fromisoformat(date_str)
    except ValueError as exc:
        raise ValueError(f"Invalid date format: {date_str}") from exc
    return date_str