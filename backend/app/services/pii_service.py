"""
PII (Personally Identifiable Information) Detection Service.

Scans each column of a DataFrame for common PII patterns:
  - Email addresses
  - Phone numbers (international / US / Indian)
  - Credit-card numbers (Luhn-validated)
  - Aadhaar numbers (Indian 12-digit)
  - Passport numbers
  - IP addresses (v4)
  - SSN (US)

Returns per-column results with type, confidence, and sample matches.
"""

import logging
import re
from typing import Any, Dict, List

import pandas as pd

logger = logging.getLogger(__name__)

# ── Patterns ─────────────────────────────────────────────────────────────────

_PATTERNS: Dict[str, re.Pattern] = {
    "email": re.compile(
        r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z]{2,}", re.IGNORECASE
    ),
    "phone": re.compile(
        r"(?:\+?\d{1,3}[-.\s]?)?\(?\d{2,4}\)?[-.\s]?\d{3,4}[-.\s]?\d{4}"
    ),
    "credit_card": re.compile(
        r"\b(?:\d[ -]*?){13,19}\b"
    ),
    "aadhaar": re.compile(
        r"\b\d{4}[\s-]?\d{4}[\s-]?\d{4}\b"
    ),
    "passport": re.compile(
        r"\b[A-Z]{1,2}\d{6,9}\b", re.IGNORECASE
    ),
    "ipv4": re.compile(
        r"\b(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\b"
    ),
    "ssn": re.compile(
        r"\b\d{3}-\d{2}-\d{4}\b"
    ),
}

# Column-name hints → likely PII type (boosts confidence)
_NAME_HINTS: Dict[str, str] = {
    "email": "email", "e-mail": "email", "mail": "email",
    "phone": "phone", "mobile": "phone", "telephone": "phone", "tel": "phone",
    "credit_card": "credit_card", "card_number": "credit_card", "cc": "credit_card",
    "aadhaar": "aadhaar", "aadhar": "aadhaar",
    "passport": "passport",
    "ip": "ipv4", "ip_address": "ipv4",
    "ssn": "ssn", "social_security": "ssn",
}


def _luhn_check(num_str: str) -> bool:
    """Validate a number string using the Luhn algorithm."""
    digits = [int(d) for d in num_str if d.isdigit()]
    if len(digits) < 13 or len(digits) > 19:
        return False
    checksum = 0
    reverse = digits[::-1]
    for i, d in enumerate(reverse):
        if i % 2 == 1:
            d *= 2
            if d > 9:
                d -= 9
        checksum += d
    return checksum % 10 == 0


def detect_pii(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Scan every column of *df* for PII patterns.

    Returns
    -------
    {
      "columns": [
        {
          "column_name": "Email",
          "pii_type": "email",
          "confidence": "high",
          "match_count": 42,
          "total_rows": 100,
          "match_ratio": 0.42,
          "sample_matches": ["alice@example.com", ...]
        },
        ...
      ],
      "total_pii_columns": 3,
      "risk_level": "high" | "medium" | "low" | "none"
    }
    """
    results: List[Dict[str, Any]] = []

    for col in df.columns:
        series = df[col].dropna().astype(str)
        if series.empty:
            continue

        col_lower = str(col).lower().strip().replace(" ", "_")
        hint_type = _NAME_HINTS.get(col_lower)

        best: Dict[str, Any] | None = None

        for pii_type, pattern in _PATTERNS.items():
            matches = series[series.str.contains(pattern, na=False)]

            # For credit cards, apply Luhn validation to reduce false positives
            if pii_type == "credit_card" and len(matches) > 0:
                matches = matches[matches.apply(
                    lambda v: _luhn_check(re.sub(r"[^\d]", "", str(v)))
                )]

            if len(matches) == 0:
                continue

            ratio = len(matches) / len(series)

            # Confidence scoring — ratio-based
            pii_ratio = ratio * 100  # percentage
            if hint_type == pii_type:
                confidence = "high"
            elif pii_ratio > 80 and ratio >= 0.5:
                confidence = "high"
            elif pii_ratio > 30:
                confidence = "medium"
            else:
                confidence = "low"

            # Keep the best (highest ratio) match for this column
            if best is None or ratio > best["match_ratio"]:
                best = {
                    "column_name": col,
                    "pii_type": pii_type,
                    "confidence": confidence,
                    "match_count": int(len(matches)),
                    "total_rows": int(len(series)),
                    "match_ratio": round(ratio, 4),
                    "sample_matches": matches.head(5).tolist(),
                }

        if best is not None:
            results.append(best)

    # Overall risk level — dataset-level PII assessment
    high_count = sum(1 for r in results if r["confidence"] == "high")
    medium_count = sum(1 for r in results if r["confidence"] == "medium")

    if high_count >= 1:
        risk_level = "high"
    elif medium_count >= 2:
        risk_level = "medium"
    elif results:
        risk_level = "low"
    else:
        risk_level = "none"

    return {
        "columns": results,
        "total_pii_columns": len(results),
        "risk_level": risk_level,
    }
