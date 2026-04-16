"""
Issue Taxonomy — canonical definitions for the root-cause scoring system.

This module is pure *data* — no logic lives here.
Detection, grouping, and scoring logic lives in root_cause_service.py.

Public surface
--------------
IssueGroup          — canonical root-cause categories (str Enum)
DetectionResult     — single detected issue with confidence + explanation
RootCause           — aggregated root cause for one column
FixStep             — actionable, code-level cleaning step
PENALTY_TO_GROUP    — mapping: (dimension, keyword) → IssueGroup
GROUP_DEDUCTION_CAPS — max penalty per group on a 100-pt scale
IMPACT_MAP          — business + technical impact per group
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Tuple


# ── Issue groups (root causes) ────────────────────────────────────────────────

class IssueGroup(str, Enum):
    DATE_QUALITY       = "DATE_QUALITY"
    EMAIL_QUALITY      = "EMAIL_QUALITY"
    DUPLICATES         = "DUPLICATES"
    FORMAT_CONSISTENCY = "FORMAT_CONSISTENCY"
    PII_RISK           = "PII_RISK"
    GARBAGE_VALUES     = "GARBAGE_VALUES"
    ANOMALY            = "ANOMALY"
    COMPLETENESS       = "COMPLETENESS"


# ── Mapping: (dimension, keyword_in_reason) → IssueGroup ─────────────────────
#
# Rules are evaluated top-to-bottom; the first match wins.
# An empty string for keyword means "any reason in this dimension".
#
PENALTY_TO_GROUP: List[Tuple[str, str, IssueGroup]] = [
    # date issues beat generic validity / consistency rules
    ("validity",     "date",         IssueGroup.DATE_QUALITY),
    ("consistency",  "date format",  IssueGroup.DATE_QUALITY),
    ("consistency",  "date",         IssueGroup.DATE_QUALITY),
    # email
    ("validity",     "email",        IssueGroup.EMAIL_QUALITY),
    # uniqueness → duplicates
    ("uniqueness",   "",             IssueGroup.DUPLICATES),
    # PII is always security-scoped
    ("security",     "pii",          IssueGroup.PII_RISK),
    # remaining security = generic security (keep separate from PII)
    ("security",     "",             IssueGroup.PII_RISK),
    # garbage / junk
    ("garbage",      "",             IssueGroup.GARBAGE_VALUES),
    # remaining consistency = format
    ("consistency",  "",             IssueGroup.FORMAT_CONSISTENCY),
    # anomalies
    ("anomalies",    "",             IssueGroup.ANOMALY),
    # everything else = completeness
    ("completeness", "",             IssueGroup.COMPLETENESS),
    ("validity",     "",             IssueGroup.FORMAT_CONSISTENCY),
]


# ── Maximum deduction per group (points on a 100-pt scale) ───────────────────
#
# When multiple raw penalties map to the same group, their deductions are summed
# then capped at this value, preventing double-penalising the same root cause.
#
GROUP_DEDUCTION_CAPS: Dict[IssueGroup, float] = {
    IssueGroup.DATE_QUALITY:       12.0,
    IssueGroup.EMAIL_QUALITY:      10.0,
    IssueGroup.DUPLICATES:         10.0,
    IssueGroup.FORMAT_CONSISTENCY:  8.0,
    IssueGroup.PII_RISK:           20.0,
    IssueGroup.GARBAGE_VALUES:      8.0,
    IssueGroup.ANOMALY:            12.0,
    IssueGroup.COMPLETENESS:       30.0,
}


# ── Business + technical impact per group ─────────────────────────────────────

IMPACT_MAP: Dict[IssueGroup, Dict[str, List[str]]] = {
    IssueGroup.DATE_QUALITY: {
        "business": [
            "Time-series analysis unreliable — reports may contain incorrect trends.",
            "Date-range filters will silently exclude or include the wrong records.",
            "SLA / KPI calculations relying on this column will be inaccurate.",
        ],
        "technical": [
            "Temporal joins and window functions may produce NaT / NaN rows.",
            "pd.to_datetime will raise or coerce on parse failures, losing data.",
            "ORDER BY / GROUP BY on this column produces undefined ordering.",
        ],
    },
    IssueGroup.EMAIL_QUALITY: {
        "business": [
            "Email campaigns will have elevated bounce and undeliverable rates.",
            "User communication channels are unreliable.",
            "Deduplication by email will miss near-duplicate accounts.",
        ],
        "technical": [
            "Email regex validation failures in downstream services.",
            "Lookup joins on email field will silently miss rows.",
        ],
    },
    IssueGroup.DUPLICATES: {
        "business": [
            "Aggregation and reporting metrics will be inflated.",
            "Customer deduplication and CRM sync will be corrupted.",
            "Revenue and count metrics become unreliable.",
        ],
        "technical": [
            "ML model training overfits to repeated samples.",
            "Primary-key uniqueness constraints fail on database load.",
            "GROUP BY aggregations produce incorrect totals.",
        ],
    },
    IssueGroup.FORMAT_CONSISTENCY: {
        "business": [
            "Data cannot be reliably sorted or filtered.",
            "Downstream dashboards display mixed formatting to end users.",
        ],
        "technical": [
            "Schema inference assigns wrong dtypes to affected columns.",
            "String matching and joins fail due to casing or delimiter mismatch.",
        ],
    },
    IssueGroup.PII_RISK: {
        "business": [
            "GDPR / DPDPA / HIPAA compliance violations are possible.",
            "Unauthorised exposure of personal data creates legal liability.",
            "Data sharing with third parties is prohibited without masking.",
        ],
        "technical": [
            "Column-level masking or pseudonymisation must be applied before export.",
            "Access-control policies are required on PII-bearing columns.",
        ],
    },
    IssueGroup.GARBAGE_VALUES: {
        "business": [
            "Analysts cannot trust raw values — manual review required.",
            "KPI dashboards display sentinel / placeholder values.",
        ],
        "technical": [
            "Downstream pipelines may error on placeholder strings.",
            "Aggregation functions (SUM, AVG) skewed by sentinel numerics.",
        ],
    },
    IssueGroup.ANOMALY: {
        "business": [
            "Outliers will skew statistical summaries and averages.",
            "Anomalous records may indicate data pipeline errors or fraud.",
        ],
        "technical": [
            "ML algorithms sensitive to outliers (linear models, k-means) degrade.",
            "Feature scaling / normalisation produces extreme values.",
        ],
    },
    IssueGroup.COMPLETENESS: {
        "business": [
            "Missing values reduce the effective dataset size for analysis.",
            "Imputed values introduce bias if the imputation strategy is wrong.",
        ],
        "technical": [
            "Algorithms that do not handle NaN natively (SVMs, etc.) will error.",
            "Joins on columns with NaN silently drop rows.",
        ],
    },
}


# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class DetectionResult:
    """
    A single detected issue, enriched with confidence and explainability.

    Attributes
    ----------
    type        : Short machine-readable label, e.g. "invalid_email"
    confidence  : Certainty of the detection, 0–1
    explanation : Human-readable sentence describing the finding
    evidence    : Supporting facts (list of strings)
    column      : Column where this was detected (empty = dataset-level)
    severity    : "high" | "medium" | "low"

    Example
    -------
    >>> DetectionResult(
    ...     type="invalid_email",
    ...     confidence=0.84,
    ...     explanation="3 of 25 emails fail RFC pattern validation",
    ...     evidence=["rbrown@work", "mj@nike", "t.mueller@de"],
    ...     column="Email",
    ...     severity="medium",
    ... )
    """
    type:        str
    confidence:  float
    explanation: str
    evidence:    List[str]      = field(default_factory=list)
    column:      str            = ""
    severity:    str            = "medium"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type":        self.type,
            "confidence":  round(self.confidence, 3),
            "explanation": self.explanation,
            "evidence":    self.evidence,
            "column":      self.column,
            "severity":    self.severity,
        }


@dataclass
class RootCause:
    """
    Aggregated root cause for one column.

    Attributes
    ----------
    group      : Canonical IssueGroup this belongs to
    label      : Human-friendly label shown in reports
    severity   : Overall severity for this cause ("high" | "medium" | "low")
    detections : Individual DetectionResult items under this cause
    examples   : Sample bad values from the column
    formats    : Detected format patterns (for date / format issues)
    """
    group:      IssueGroup
    label:      str
    severity:   str
    detections: List[DetectionResult] = field(default_factory=list)
    examples:   List[Any]             = field(default_factory=list)
    formats:    List[str]             = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "group":      self.group.value,
            "label":      self.label,
            "severity":   self.severity,
            "detections": [d.to_dict() for d in self.detections],
            "examples":   [str(e) for e in self.examples[:10]],
            "formats":    self.formats,
        }


@dataclass
class FixStep:
    """
    An actionable, code-level cleaning step for a column.

    Attributes
    ----------
    column    : Column to fix, or "__all__" for dataset-level steps
    action    : Short machine-readable key (e.g. "normalize_casing")
    code_hint : Python one-liner / snippet that performs the fix
    priority  : "high" | "medium" | "low"
    details   : Extra context (e.g. {"strategy": "median", "fill_value": 4.2})

    Example
    -------
    >>> FixStep(
    ...     column="Join_Date",
    ...     action="parse_dates",
    ...     code_hint="df['Join_Date'] = pd.to_datetime(df['Join_Date'], errors='coerce')",
    ...     priority="high",
    ...     details={"detected_formats": ["YYYY-MM-DD", "DD/MM/YYYY"]},
    ... )
    """
    column:    str
    action:    str
    code_hint: str
    priority:  str            = "medium"
    details:   Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "column":    self.column,
            "action":    self.action,
            "code_hint": self.code_hint,
            "priority":  self.priority,
            "details":   self.details,
        }
