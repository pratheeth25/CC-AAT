"""
Anomaly Detection Service — v2 (Enhanced).

Detects anomalies across several dimensions:
  - Numerical outliers : IQR, Z-score, and (optional) Isolation Forest
  - Domain-specific    : impossible ages, sentinel values, non-numeric in numeric cols
  - Date columns       : invalid dates, future dates, impossible years
  - Categorical columns: values with very low frequency (rare anomalies)
  - Text length        : abnormally long values in short-text fields
  - Generic patterns   : known junk / placeholder values

Return format per column:
  {
    "column": "Phone",
    "anomaly_count": 3,
    "anomalies": [0, "000-000", "?"],
    "methods_used": ["iqr", "pattern"]
  }
"""

import logging
import re
import warnings
from datetime import datetime
from typing import Any, Dict, List, Set

from app.utils.time_utils import now_ist

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Known junk / placeholder strings
_JUNK_PATTERNS: Set[str] = {
    "?", "??", "???", "-", "--", "---",
    "n/a", "na", "null", "none", "nan", "undefined",
    "000-000-0000", "000-000", "0000000000", "1234567890",
    "error", "#ref!", "#value!", "#n/a", "unknown",
    "test", "todo", "tbd", "xxx", "aaa", "abc",
}

_PLACEHOLDER_EMAILS = {
    "test@test.com", "test@example.com", "a@a.com",
    "user@example.com", "admin@admin.com",
}

_AGE_COLUMNS = {"age", "years", "edad"}
_DATE_COLUMNS = {"date", "joindate", "join_date", "created_at", "created",
                 "updated_at", "dob", "birth_date", "birthdate", "signup_date",
                 "start_date", "end_date", "order_date"}
_EMAIL_COLUMNS = {"email", "e-mail", "email_address", "mail"}
_NAME_COLUMNS = {"name", "fullname", "full_name", "first_name", "last_name",
                 "firstname", "lastname", "city", "country", "status", "state"}
_ID_COLUMNS = {"id", "_id", "uuid", "index", "row_id", "record_id", "pk", "key"}

_SENTINEL_NUMERIC = {-1, 0, 999, 9999, -999, 99999}

# Maximum reasonable length for short-text fields (name, city, status)
_SHORT_TEXT_MAX_LEN = 100

# Minimum observations required to run statistical tests
_MIN_OBSERVATIONS = 10
# IQR fence multiplier
_IQR_FACTOR = 1.5
# Z-score threshold
_Z_THRESHOLD = 3.0
# Isolation Forest contamination ratio
_IF_CONTAMINATION = 0.05
# Categorical anomaly: value frequency below this fraction of the most common value
_CATEGORICAL_RARE_RATIO = 0.01
# If a string column has more than this fraction of unique values, skip categorical
# rarity entirely — it is almost certainly an identity/free-text column (name, email…)
_HIGH_CARDINALITY_RATIO = 0.3
# Columns that are always unique by design — never flag with categorical rarity
_IDENTITY_COLUMNS = {"name", "fullname", "full_name", "first_name", "last_name",
                     "firstname", "lastname", "email", "e-mail", "email_address",
                     "mail", "phone", "phone_number", "mobile", "address", "street",
                     "url", "website", "ip", "ip_address", "uuid", "guid", "token"}

# Regex: all three date segments are exactly 2 digits → ambiguous (year order unclear)
# Examples: "95/06/01", "24-01-24", "24.01.24"
_AMBIGUOUS_DATE_RE = re.compile(r'^\d{2}[/\-\.]\d{2}[/\-\.]\d{2}$')


def detect_anomalies(df: pd.DataFrame, method: str = "all") -> List[Dict[str, Any]]:
    """
    Analyse every column in *df* for anomalies.

    Parameters
    ----------
    df     : input DataFrame (raw, before null replacement)
    method : "iqr" | "zscore" | "isolation_forest" | "all"

    Returns
    -------
    List of per-column anomaly dicts.  Only columns with ≥1 anomaly are included.
    """
    method = method.lower()
    results: List[Dict[str, Any]] = []

    for col in df.columns:
        series = df[col]
        col_lower = str(col).lower().strip()

        # ── RULE: Skip ID / key columns entirely ──
        if col_lower in _ID_COLUMNS:
            continue

        if pd.api.types.is_numeric_dtype(series):
            col_result = _numeric_anomalies(col, col_lower, series, method)
        else:
            col_result = _non_numeric_anomalies(col, col_lower, series)

        if col_result and col_result["anomaly_count"] > 0:
            results.append(col_result)

    return results


# ---------------------------------------------------------------------------
# Numeric anomaly detection
# ---------------------------------------------------------------------------

def _numeric_anomalies(
    col: str, col_lower: str, series: pd.Series, method: str
) -> Dict[str, Any]:
    clean = series.dropna()
    anomaly_values: List[Any] = []
    methods_used: List[str] = []
    severity = "low"

    # ── PRIORITY 1: Domain rules (highest priority) ──

    # Age column: strict domain range 15–90
    if col_lower in _AGE_COLUMNS:
        impossible = clean[(clean < 15) | (clean > 90)]
        for val in impossible.tolist():
            v = _safe_native(val)
            if v not in anomaly_values:
                anomaly_values.append(v)
        if len(impossible) > 0:
            methods_used.append("impossible_age")
            severity = "high"
        # Do NOT run statistical outliers on age — domain rule takes precedence
        return {
            "column": col,
            "anomaly_count": len(anomaly_values),
            "anomalies": anomaly_values[:50],
            "methods_used": methods_used,
            "severity": severity,
        }

    # ── PRIORITY 2: Sentinel values (for quantity-like columns) ──
    if col_lower in {"quantity", "count", "amount"}:
        sentinel = clean[clean.isin(list(_SENTINEL_NUMERIC))]
        for val in sentinel.tolist():
            v = _safe_native(val)
            if v not in anomaly_values:
                anomaly_values.append(v)
        if len(sentinel) > 0 and "sentinel_value" not in methods_used:
            methods_used.append("sentinel_value")
            severity = "medium"

    # ── PRIORITY 3: Statistical outlier detection (lowest priority) ──
    if len(clean) >= _MIN_OBSERVATIONS:
        outlier_indices: Set[Any] = set()

        if method in ("iqr", "all"):
            idx = _iqr_outliers(clean)
            outlier_indices.update(idx)
            methods_used.append("iqr")

        if method in ("zscore", "all"):
            idx = _zscore_outliers(clean)
            outlier_indices.update(idx)
            methods_used.append("zscore")

        if method == "isolation_forest" or (method == "all" and len(clean) >= 50):
            idx = _isolation_forest_outliers(clean)
            if idx:
                outlier_indices.update(idx)
                methods_used.append("isolation_forest")

        stat_values = _resolve_values(series, outlier_indices)
        for v in stat_values:
            if v not in anomaly_values:
                anomaly_values.append(v)
        if stat_values and severity == "low":
            severity = "medium"

    return {
        "column": col,
        "anomaly_count": len(anomaly_values),
        "anomalies": anomaly_values[:50],
        "methods_used": methods_used,
        "severity": severity,
    }


def _iqr_outliers(clean: pd.Series) -> List[Any]:
    q1 = clean.quantile(0.25)
    q3 = clean.quantile(0.75)
    iqr = q3 - q1
    lower = q1 - _IQR_FACTOR * iqr
    upper = q3 + _IQR_FACTOR * iqr
    return clean[(clean < lower) | (clean > upper)].index.tolist()


def _zscore_outliers(clean: pd.Series) -> List[Any]:
    std = clean.std()
    if std == 0:
        return []
    z = ((clean - clean.mean()) / std).abs()
    return clean[z > _Z_THRESHOLD].index.tolist()


def _isolation_forest_outliers(clean: pd.Series) -> List[Any]:
    try:
        from sklearn.ensemble import IsolationForest

        model = IsolationForest(
            contamination=_IF_CONTAMINATION, random_state=42, n_jobs=-1
        )
        preds = model.fit_predict(clean.values.reshape(-1, 1))
        return clean[preds == -1].index.tolist()
    except Exception as exc:
        logger.debug("IsolationForest skipped for column: %s", exc)
        return []


# ---------------------------------------------------------------------------
# Non-numeric anomaly detection
# ---------------------------------------------------------------------------

def _non_numeric_anomalies(col: str, col_lower: str, series: pd.Series) -> Dict[str, Any]:
    """
    Enhanced non-numeric anomaly detection with domain-specific checks.
    """
    anomaly_values: List[Any] = []
    methods_used: List[str] = []

    non_null = series.dropna().astype(str)

    # 1. Date anomalies — invalid (HIGH) and ambiguous (MEDIUM) are separated
    if col_lower in _DATE_COLUMNS:
        date_result = _enhanced_date_anomalies(non_null)
        if date_result["invalid"]:
            anomaly_values.extend(date_result["invalid"])
            methods_used.append("date_validation")
        if date_result["ambiguous"]:
            anomaly_values.extend(date_result["ambiguous"])
            methods_used.append("ambiguous_date")
    else:
        # Try auto-detecting date column
        date_result = _date_anomalies(non_null)
        if date_result is not None:
            if date_result["invalid"]:
                anomaly_values.extend(date_result["invalid"])
                methods_used.append("date_validation")
            if date_result["ambiguous"]:
                anomaly_values.extend(date_result["ambiguous"])
                methods_used.append("ambiguous_date")
        else:
            # 2. Categorical rarity — skip for identity/high-cardinality columns
            if col_lower not in _IDENTITY_COLUMNS:
                total_rows = len(series.dropna())
                unique_count = series.dropna().nunique()
                cardinality_ratio = unique_count / total_rows if total_rows > 0 else 0
                if cardinality_ratio <= _HIGH_CARDINALITY_RATIO:
                    cat_anomalies = _categorical_anomalies(series)
                    anomaly_values.extend(cat_anomalies)
                    if cat_anomalies:
                        methods_used.append("categorical_rarity")

    # 3. Non-numeric values in expected-numeric columns
    if col_lower in _AGE_COLUMNS | {"salary", "amount", "price", "quantity", "count"}:
        non_numeric = non_null[pd.to_numeric(non_null, errors="coerce").isna()]
        junk_from_numeric = [v for v in non_numeric if v.strip().lower() not in {"", "nan"}]
        for v in junk_from_numeric:
            if v not in anomaly_values:
                anomaly_values.append(v)
        if junk_from_numeric:
            methods_used.append("non_numeric_in_numeric")

    # 4. Junk / placeholder patterns (always applied to string columns)
    junk = _junk_pattern_anomalies(series)
    for v in junk:
        if v not in anomaly_values:
            anomaly_values.append(v)
    if junk:
        methods_used.append("pattern")

    # 5. Placeholder emails
    if col_lower in _EMAIL_COLUMNS:
        placeholders = non_null[non_null.str.strip().str.lower().isin(_PLACEHOLDER_EMAILS)]
        for v in placeholders:
            if v not in anomaly_values:
                anomaly_values.append(v)
        if len(placeholders) > 0:
            methods_used.append("placeholder_email")

    # 6. Text length check for short-text fields
    if col_lower in _NAME_COLUMNS:
        too_long = non_null[non_null.str.len() > _SHORT_TEXT_MAX_LEN]
        for v in too_long:
            if v not in anomaly_values:
                anomaly_values.append(v)
        if len(too_long) > 0:
            methods_used.append("text_length")

    # Determine severity from methods used
    methods_set = set(methods_used)
    if methods_set & {"date_validation", "non_numeric_in_numeric", "placeholder_email"}:
        severity = "high"
    elif methods_set & {"ambiguous_date"}:
        severity = "medium"
    elif methods_set & {"categorical_rarity", "pattern", "text_length"}:
        severity = "low"
    else:
        severity = "medium"

    return {
        "column": col,
        "anomaly_count": len(anomaly_values),
        "anomalies": anomaly_values[:50],
        "methods_used": methods_used,
        "severity": severity,
    }


def _is_ambiguous_date(val_str: str) -> bool:
    """
    True when all three date segments are two digits, making year/month/day
    order impossible to determine reliably.
    Examples: "95/06/01", "24-01-24", "24.01.24"
    """
    return bool(_AMBIGUOUS_DATE_RE.match(val_str.strip()))


def _enhanced_date_anomalies(non_null: pd.Series) -> Dict[str, List[Any]]:
    """
    Returns {"invalid": [...], "ambiguous": [...]}.

    - invalid  : calendar-impossible dates and completely unparseable values
                 (e.g. "2024-02-30", "00-00-0000", "yesterday") → HIGH severity
    - ambiguous: two-digit year / day-month order unclear
                 (e.g. "95/06/01", "24/01/24") → MEDIUM severity

    Valid dates in any recognisable format are NOT flagged as anomalies.
    Format inconsistency is a consistency issue, not an anomaly.
    """
    invalid: List[Any] = []
    ambiguous: List[Any] = []

    for val in non_null:
        val_str = str(val).strip()

        # Ambiguous pattern takes priority — do not try to parse further
        if _is_ambiguous_date(val_str):
            if val_str not in ambiguous:
                ambiguous.append(val_str)
            continue

        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                pd.to_datetime(val_str, dayfirst=False)
            # Parsed successfully and unambiguous → not an anomaly
            # (different formats like "Jan 16 2023" or "2023/05/12" are valid)
        except Exception:
            # Cannot be parsed at all (includes impossible calendar dates
            # like 2024-02-30 and non-date strings like "yesterday") → invalid
            if val_str not in invalid:
                invalid.append(val_str)

    return {"invalid": invalid[:50], "ambiguous": ambiguous[:50]}


def _date_anomalies(non_null: pd.Series) -> Dict[str, List[Any]] | None:
    """
    Auto-detects whether a column looks like it contains dates.
    Returns {"invalid": [...], "ambiguous": [...]} if yes, otherwise None.
    """
    if len(non_null) == 0:
        return None

    sample = non_null.head(50)
    parsed_count = 0
    for val in sample:
        val_str = str(val).strip()
        # Ambiguous patterns still count as date-like
        if _is_ambiguous_date(val_str):
            parsed_count += 1
            continue
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                pd.to_datetime(val_str, dayfirst=False)
            parsed_count += 1
        except Exception:
            pass

    # Consider it a date column if >60 % of the sample parses
    if parsed_count / len(sample) < 0.6:
        return None

    return _enhanced_date_anomalies(non_null)


def _categorical_anomalies(series: pd.Series) -> List[Any]:
    """
    Values that appear far less often than the dominant value.
    Useful for catching typos like "Unitd States" alongside "United States".
    """
    non_null = series.dropna()
    if len(non_null) < _MIN_OBSERVATIONS:
        return []

    counts = non_null.value_counts()
    if counts.empty:
        return []

    max_freq = counts.iloc[0]
    threshold = max(_CATEGORICAL_RARE_RATIO * max_freq, 1)
    rare = counts[counts <= threshold].index.tolist()
    return [str(v) for v in rare[:50]]


def _junk_pattern_anomalies(series: pd.Series) -> List[Any]:
    non_null = series.dropna().astype(str)
    return [v for v in non_null if v.strip().lower() in _JUNK_PATTERNS][:50]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_values(series: pd.Series, indices: Set[Any]) -> List[Any]:
    """Retrieve actual values from the series for a set of index labels."""
    values: List[Any] = []
    for idx in indices:
        try:
            val = series.loc[idx]
        except KeyError:
            continue
        values.append(_safe_native(val))
    return values[:50]


def _safe_native(val: Any) -> Any:
    """Convert numpy scalar to Python native type."""
    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.floating,)):
        return round(float(val), 6) if not np.isnan(val) else None
    return val
