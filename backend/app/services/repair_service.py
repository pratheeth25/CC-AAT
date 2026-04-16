"""
Smart Data Repair Suggestions Service.

Analyses a profiled DataFrame and produces actionable suggestions:
  - Fill missing numerics  (mean / median strategy)
  - Fill missing categoricals (mode)
  - Standardise date formats
  - Normalise categorical casing  (USA / usa / Usa → unified)
  - Normalise country synonyms    (USA → United States)
  - Remove duplicates
  - Flag & suggest correction for invalid e-mail addresses

v2 additions
------------
  suggest_fix_steps(df, profile, root_cause_analysis)
      Returns a list of FixStep objects — actionable, code-level cleaning steps
      with an exact code_hint, priority, and structured details.
      Delegates to root_cause_service which holds all the detection logic.
"""

import logging
import re
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Known categorical synonym maps
# ---------------------------------------------------------------------------
_COUNTRY_MAP: Dict[str, str] = {
    "usa": "United States",
    "us": "United States",
    "u.s.": "United States",
    "u.s.a.": "United States",
    "united states of america": "United States",
    "america": "United States",
    "uk": "United Kingdom",
    "u.k.": "United Kingdom",
    "great britain": "United Kingdom",
    "britain": "United Kingdom",
    "england": "United Kingdom",   # approximate – adjust per domain
}

_EMAIL_REGEX = re.compile(
    r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
)

_DATE_INFERENCE_FORMATS = [
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%d-%m-%Y",
    "%m-%d-%Y",
    "%Y/%m/%d",
    "%d %b %Y",
    "%b %d %Y",
]


def suggest_repairs(df: pd.DataFrame, profile: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Return a list of repair suggestion dicts, one per discovered issue.

    Each dict contains:
      column, issue_type, suggestion, action, details
    """
    suggestions: List[Dict[str, Any]] = []

    rows = profile["shape"]["rows"]

    # ---------------------------------------------------------------
    # 1. Global: remove exact duplicates
    # ---------------------------------------------------------------
    exact_dups = profile["duplicates"]["exact_duplicates"]
    if exact_dups > 0:
        suggestions.append(
            {
                "column": "__all__",
                "issue_type": "duplicates",
                "suggestion": f"Remove {exact_dups} exact duplicate rows.",
                "action": "remove_duplicates",
                "details": {"duplicate_count": exact_dups},
            }
        )

    for col, col_stats in profile["columns"].items():
        series = df[col]
        dtype = col_stats["dtype"]
        missing_pct = col_stats["missing_percentage"]

        # -------------------------------------------------------------
        # 2. Missing value fill strategy
        # -------------------------------------------------------------
        if col_stats["missing_count"] > 0:
            if pd.api.types.is_numeric_dtype(series):
                non_null = series.dropna()
                skew = float(non_null.skew()) if len(non_null) > 1 else 0.0
                strategy = "median" if abs(skew) > 1.0 else "mean"
                fill_val = round(float(non_null.median() if strategy == "median" else non_null.mean()), 4)
                suggestions.append(
                    {
                        "column": col,
                        "issue_type": "missing_values",
                        "suggestion": f"Fill {col_stats['missing_count']} missing values with {strategy} ({fill_val}).",
                        "action": f"fill_{strategy}",
                        "details": {
                            "missing_count": col_stats["missing_count"],
                            "fill_value": fill_val,
                            "strategy": strategy,
                        },
                    }
                )
            elif dtype in ("object", "category", "string"):
                non_null = series.dropna()
                mode_vals = non_null.mode()
                if len(mode_vals) > 0:
                    mode_val = str(mode_vals.iloc[0])
                    suggestions.append(
                        {
                            "column": col,
                            "issue_type": "missing_values",
                            "suggestion": f"Fill {col_stats['missing_count']} missing values with mode ('{mode_val}').",
                            "action": "fill_mode",
                            "details": {
                                "missing_count": col_stats["missing_count"],
                                "fill_value": mode_val,
                            },
                        }
                    )

        # -------------------------------------------------------------
        # 3. Date standardisation
        # -------------------------------------------------------------
        if dtype in ("object", "string"):
            date_fmt = _detect_date_format(series)
            if date_fmt and date_fmt != "ISO8601":
                suggestions.append(
                    {
                        "column": col,
                        "issue_type": "inconsistent_dates",
                        "suggestion": f"Standardise dates to ISO 8601 (YYYY-MM-DD). Detected format: {date_fmt}.",
                        "action": "standardize_dates",
                        "details": {"detected_format": date_fmt, "target_format": "%Y-%m-%d"},
                    }
                )

        # -------------------------------------------------------------
        # 4. Country / categorical synonym normalisation
        # -------------------------------------------------------------
        if dtype in ("object", "string"):
            synonyms = _find_country_synonyms(series)
            if synonyms:
                suggestions.append(
                    {
                        "column": col,
                        "issue_type": "categorical_synonyms",
                        "suggestion": f"Normalise {len(synonyms)} country/region synonyms.",
                        "action": "normalize_categories",
                        "details": {"synonym_map": synonyms},
                    }
                )

        # -------------------------------------------------------------
        # 5. Mixed casing fix
        # -------------------------------------------------------------
        if dtype in ("object", "string"):
            casing_issue = _detect_casing_inconsistency(series)
            if casing_issue:
                suggestions.append(
                    {
                        "column": col,
                        "issue_type": "inconsistent_casing",
                        "suggestion": f"Normalise casing (e.g., apply '{casing_issue}').",
                        "action": "normalize_case",
                        "details": {"suggested_case": casing_issue},
                    }
                )

        # -------------------------------------------------------------
        # 6. Email validation
        # -------------------------------------------------------------
        if dtype in ("object", "string") and _looks_like_email_column(col, series):
            invalid_emails = _find_invalid_emails(series)
            if invalid_emails:
                suggestions.append(
                    {
                        "column": col,
                        "issue_type": "invalid_emails",
                        "suggestion": f"Found {len(invalid_emails)} invalid email address(es). Review or remove.",
                        "action": "flag_emails",
                        "details": {"invalid_values": invalid_emails[:20]},
                    }
                )

    return suggestions


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _detect_date_format(series: pd.Series) -> str | None:
    """
    Try to infer a consistent date format for a string column.
    Returns the format string, "ISO8601", or None if not a date column.
    """
    sample = series.dropna().astype(str).head(30)
    if len(sample) == 0:
        return None

    for fmt in _DATE_INFERENCE_FORMATS:
        match_count = 0
        for val in sample:
            try:
                pd.to_datetime(val, format=fmt)
                match_count += 1
            except Exception:
                pass
        if match_count / len(sample) >= 0.7:
            # Check if it's already ISO
            if fmt == "%Y-%m-%d":
                return "ISO8601"
            return fmt

    return None


def _find_country_synonyms(series: pd.Series) -> Dict[str, str]:
    """Return a mapping of {found_value: canonical_value} for country synonyms."""
    synonyms: Dict[str, str] = {}
    non_null = series.dropna().astype(str).unique()
    for val in non_null:
        canonical = _COUNTRY_MAP.get(val.strip().lower())
        if canonical and val != canonical:
            synonyms[val] = canonical
    return synonyms


def _detect_casing_inconsistency(series: pd.Series) -> str | None:
    """
    Returns a suggested casing strategy if the column has casing inconsistency.
    """
    non_null = series.dropna().astype(str)
    if len(non_null) < 5:
        return None

    upper_count = sum(1 for v in non_null if v == v.upper() and v.isalpha())
    lower_count = sum(1 for v in non_null if v == v.lower() and v.isalpha())
    title_count = sum(1 for v in non_null if v == v.title() and v.isalpha())
    total = len(non_null)

    # If any one casing dominates but others also exist, suggest normalisation
    max_casing = max(upper_count, lower_count, title_count)
    if max_casing > 0 and max_casing < total * 0.9:
        if upper_count == max_casing:
            return "upper"
        if lower_count == max_casing:
            return "lower"
        return "title"
    return None


def _looks_like_email_column(col: str, series: pd.Series) -> bool:
    """Heuristic: column name or >30 % of values contain '@' symbol."""
    if "email" in col.lower() or "e-mail" in col.lower() or "mail" in col.lower():
        return True
    sample = series.dropna().astype(str).head(50)
    if len(sample) == 0:
        return False
    at_count = sum(1 for v in sample if "@" in v)
    return at_count / len(sample) >= 0.3


def _find_invalid_emails(series: pd.Series) -> List[str]:
    invalid: List[str] = []
    for val in series.dropna().astype(str):
        if "@" in val and not _EMAIL_REGEX.match(val.strip()):
            invalid.append(val)
    return invalid[:50]


# ---------------------------------------------------------------------------
# v2 — FixStep pipeline (delegates to root_cause_service)
# ---------------------------------------------------------------------------

def suggest_fix_steps(
    df: pd.DataFrame,
    profile: Dict[str, Any],
    root_cause_analysis: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Return a prioritised list of actionable, code-level cleaning steps.

    Each step is a FixStep serialised as a dict with keys:
      column    — column to fix (or "__all__" for dataset-level)
      action    — short machine-readable key
      code_hint — exact Python/pandas snippet that applies the fix
      priority  — "high" | "medium" | "low"
      details   — extra context (fill values, examples, etc.)

    If ``root_cause_analysis`` is not provided, a lightweight on-the-fly
    analysis is run from the profile only (missing values and duplicates).

    Parameters
    ----------
    df                  : Raw DataFrame
    profile             : Output of profiling_service.profile_dataframe()
    root_cause_analysis : Output of root_cause_service.analyze_root_causes()
                          (the "columns" sub-dict from quality output), or the
                          full root-cause analysis dict. Optional.
    """
    from app.services.root_cause_service import suggest_fix_steps as _rcs_steps

    # Accept either the top-level root_cause_analysis dict (with "columns" key)
    # or the raw quality["root_cause_analysis"] dict (already the columns sub-dict)
    if root_cause_analysis is None:
        rca = {"columns": {}, "dedup_penalties": [], "fair_score": 100.0, "group_impacts": {}}
    elif "columns" in root_cause_analysis:
        rca = root_cause_analysis
    else:
        # Wrap columns dict into expected shape
        rca = {
            "columns": root_cause_analysis,
            "dedup_penalties": [],
            "fair_score": 100.0,
            "group_impacts": {},
        }

    steps = _rcs_steps(df=df, profile=profile, root_cause_analysis=rca)
    return [s.to_dict() for s in steps]
