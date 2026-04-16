from typing import Any, Optional

from app.utils.time_utils import now_ist


def success_response(data: Any, message: str = "Success") -> dict:
    return {
        "status": "success",
        "message": message,
        "data": data,
        "timestamp": now_ist().isoformat(),
    }


def error_response(message: str, detail: Optional[str] = None) -> dict:
    return {
        "status": "error",
        "message": message,
        "detail": detail,
        "timestamp": now_ist().isoformat(),
    }
