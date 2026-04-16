"""
Security Scanner Service.

Scans a DataFrame for security threats:
  - XSS payloads        (<script>, javascript:, on*= handlers)
  - SQL injection        (SELECT, DROP, UNION, --, ;)
  - Path traversal       (../ or ..\)
  - Command injection    (&, |, `, $ followed by shell commands)
  - Null byte injection  (\x00)

Returns per-cell findings with row index, column, matched pattern, and severity.
Results feed into the quality scorer as hard deductions.
"""

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List

import pandas as pd

logger = logging.getLogger(__name__)

# ── Threat patterns ────────────────────────────────────────────────────────────

PATTERNS: Dict[str, re.Pattern] = {
    "xss": re.compile(
        r"<script[\s\S]*?>[\s\S]*?</script>"
        r"|javascript\s*:"
        r"|on\w+\s*=",
        re.IGNORECASE,
    ),
    "sql_injection": re.compile(
        r"\b(SELECT|INSERT|UPDATE|DELETE|DROP|UNION|ALTER|EXEC)\b"
        r"|--\s"
        r"|;\s*(SELECT|DROP|DELETE|INSERT|UPDATE)",
        re.IGNORECASE,
    ),
    "path_traversal": re.compile(r"\.\./|\.\.\\"),
    "command_injection": re.compile(
        r"[;&|`$]\s*(ls|cat|rm|curl|wget|bash|sh|chmod|chown|kill|nc)\b",
        re.IGNORECASE,
    ),
    "null_byte": re.compile(r"\x00"),
}

SEVERITY_MAP: Dict[str, str] = {
    "xss": "critical",
    "sql_injection": "critical",
    "path_traversal": "high",
    "command_injection": "critical",
    "null_byte": "high",
}

DEDUCTION_MAP: Dict[str, int] = {
    "xss": 25,
    "sql_injection": 25,
    "path_traversal": 15,
    "command_injection": 20,
    "null_byte": 10,
}


@dataclass
class SecurityFinding:
    row: int
    column: str
    value: str
    threat_type: str
    severity: str
    matched_pattern: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "row": self.row,
            "column": self.column,
            "value": self.value[:200],  # truncate long payloads for safety
            "threat_type": self.threat_type,
            "severity": self.severity,
            "matched_pattern": self.matched_pattern,
        }


@dataclass
class SecurityScanResult:
    findings: List[SecurityFinding] = field(default_factory=list)
    threat_summary: Dict[str, int] = field(default_factory=dict)
    columns_affected: List[str] = field(default_factory=list)
    total_threats: int = 0
    has_critical: bool = False
    score_deduction: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "findings": [f.to_dict() for f in self.findings[:100]],
            "threat_summary": self.threat_summary,
            "columns_affected": self.columns_affected,
            "total_threats": self.total_threats,
            "has_critical": self.has_critical,
            "score_deduction": self.score_deduction,
        }


def scan_dataframe(df: pd.DataFrame) -> SecurityScanResult:
    """
    Scan every string cell in *df* against known threat patterns.

    Returns a SecurityScanResult with per-cell findings and aggregate stats.
    """
    findings: List[SecurityFinding] = []
    threat_counts: Dict[str, int] = {}
    affected_cols: set = set()

    # Only scan object/string columns
    str_cols = df.select_dtypes(include=["object", "string", "category"]).columns

    for col in str_cols:
        for idx, val in df[col].dropna().items():
            cell = str(val)
            for threat_type, pattern in PATTERNS.items():
                match = pattern.search(cell)
                if match:
                    findings.append(
                        SecurityFinding(
                            row=int(idx),
                            column=col,
                            value=cell,
                            threat_type=threat_type,
                            severity=SEVERITY_MAP[threat_type],
                            matched_pattern=match.group()[:80],
                        )
                    )
                    threat_counts[threat_type] = threat_counts.get(threat_type, 0) + 1
                    affected_cols.add(col)

    # Calculate total score deduction (uncapped — security threats are hard penalties)
    # Deduplicate by threat_type per column (one deduction per type per column)
    deduction_keys: set = set()
    total_deduction = 0
    for f in findings:
        key = (f.column, f.threat_type)
        if key not in deduction_keys:
            deduction_keys.add(key)
            total_deduction += DEDUCTION_MAP.get(f.threat_type, 10)

    has_critical = any(f.severity == "critical" for f in findings)

    result = SecurityScanResult(
        findings=findings,
        threat_summary=threat_counts,
        columns_affected=sorted(affected_cols),
        total_threats=len(findings),
        has_critical=has_critical,
        score_deduction=total_deduction,
    )

    if findings:
        logger.warning(
            "Security scan found %d threat(s) across %d column(s) — deduction: -%d",
            len(findings),
            len(affected_cols),
            total_deduction,
        )

    return result
