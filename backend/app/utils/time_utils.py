"""
Time utilities — IST (Asia/Kolkata, UTC+05:30).

All timestamps stored in the database use IST with the +05:30 offset so
they can be displayed correctly to Indian users without frontend conversion.

Usage:
    from app.utils.time_utils import now_ist, to_ist, fmt_ist

    ts   = now_ist()          # datetime  (tz-aware, IST)
    iso  = now_ist().isoformat()  # e.g. "2025-01-15T14:32:00+05:30"
"""

from datetime import datetime

import pytz

IST = pytz.timezone("Asia/Kolkata")


def now_ist() -> datetime:
    """Return the current datetime in IST (Asia/Kolkata, UTC+05:30)."""
    return datetime.now(IST)


def to_ist(dt: datetime) -> datetime:
    """Convert a tz-aware datetime to IST.  Naive datetimes are assumed UTC."""
    if dt.tzinfo is None:
        dt = pytz.utc.localize(dt)
    return dt.astimezone(IST)


def fmt_ist(dt: datetime) -> str:
    """Return a human-friendly IST string, e.g. '15 Jan 2025, 14:32 IST'."""
    return to_ist(dt).strftime("%d %b %Y, %H:%M IST")
