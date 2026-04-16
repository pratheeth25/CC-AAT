"""
Signed URL utility.

Generates HMAC-SHA256 time-limited download tokens for dataset files.
The signature covers: dataset_id, version_number, expires_at timestamp.

Usage:
    token = create_signed_token(dataset_id="abc", version=2)
    # embed token in download URL: GET /dataset/abc/download?token=<token>

    payload = verify_signed_token(token)  # raises ValueError if invalid/expired
"""

import hashlib
import hmac
import json
import time
from typing import Any, Dict

from app.config import settings


def create_signed_token(dataset_id: str, version: int) -> str:
    """Return a URL-safe signed token valid for ``SIGNED_URL_TTL_SECONDS`` seconds."""
    expires_at = int(time.time()) + settings.SIGNED_URL_TTL_SECONDS
    payload = json.dumps({"dataset_id": dataset_id, "version": version, "exp": expires_at})
    payload_b64 = payload.encode().hex()
    sig = _sign(payload_b64)
    return f"{payload_b64}.{sig}"


def verify_signed_token(token: str) -> Dict[str, Any]:
    """
    Verify *token* and return the decoded payload dict.

    Raises
    ------
    ValueError  if the token is malformed, the signature is wrong, or it has expired.
    """
    try:
        payload_hex, sig = token.rsplit(".", 1)
    except ValueError:
        raise ValueError("Malformed token")

    expected_sig = _sign(payload_hex)
    if not hmac.compare_digest(expected_sig, sig):
        raise ValueError("Invalid token signature")

    try:
        payload = json.loads(bytes.fromhex(payload_hex).decode())
    except Exception:
        raise ValueError("Malformed token payload")

    if int(time.time()) > payload.get("exp", 0):
        raise ValueError("Token has expired")

    return payload


def _sign(data: str) -> str:
    return hmac.new(
        settings.SIGNED_URL_SECRET.encode(),
        data.encode(),
        hashlib.sha256,
    ).hexdigest()
