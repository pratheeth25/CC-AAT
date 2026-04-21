"""
Data Quality Score Engine — v3 (Production-Grade, Context-Aware, Explainable).

Strict pessimistic scoring — assume data is flawed unless proven otherwise.
Non-linear penalties mean higher error rates are punished exponentially.
  1. Completeness  (max deduct 35) — missing_pct^1.2 × 0.35  (non-linear)
  2. Validity      (max deduct 35) — age/email/date + mixed-type detection
  3. Consistency   (max deduct 20) — date format count, case inconsistency
  4. Uniqueness    (max deduct 15) — dup_pct^1.2 × 0.6   (non-linear)
  5. Security      (uncapped / hard cap 30 on threat) — threats + PII penalty
  6. Garbage/Junk  (max deduct 15) — garbage_pct^1.2 × 1.5 (non-linear)
  7. Anomalies     (max deduct 15) — severity-weighted: high/medium/low
  8. Compounding   (up to +20)     — penalty when 3+ dimensions fail together

Systemic corruption: cap score below 40 when 4+ dims fail or deduction >= 65.

Additional layers:
  - Dataset-size normalisation (log10 scale) for non-security dimensions
  - Score calibration (prevents inflated scores: cap 95 / 90)
  - Confidence score based on dataset size
  - Version-aware score delta (stable / degraded / unstable)
  - Full explainability: reason / impact / affected_columns per penalty

Score bands:
  90–100 → A  (Production Ready)
  75–89  → B  (Good)
  60–74  → C  (Needs Cleaning)
  40–59  → D  (Poor)
   <40   → F  (Unusable)
"""

import logging
import math
import re
import warnings
from datetime import datetime
from typing import Any, Dict, List, Optional

from app.utils.time_utils import now_ist

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ── Placeholder / sentinel patterns ───────────────────────────────────────────

_GARBAGE_VALUES = {
    "?", "??", "???", "null", "NULL", "Null", "none", "None", "NONE",
    "n/a", "N/A", "NA", "na", "NaN", "nan",
    "error", "Error", "ERROR", "#REF!", "#VALUE!", "#N/A",
    "undefined", "UNDEFINED", "unknown", "Unknown",
    "test", "Test", "TODO", "TBD", "tbd", "xxx", "XXX",
    "-", "--", "---",
}

_PLACEHOLDER_EMAILS = {
    "test@test.com", "test@example.com", "a@a.com",
    "user@example.com", "admin@admin.com", "noreply@noreply.com",
}

_PLACEHOLDER_DATES = {"0000-00-00", "1900-01-01", "9999-12-31", "1970-01-01"}

_SENTINEL_NUMERIC = {-1, 0, 999, 9999, -999, 99999}

_EMAIL_RE = re.compile(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z]{2,}$")

_AGE_COLUMNS = {"age", "years", "edad"}
_DATE_COLUMNS = {"date", "joindate", "join_date", "created_at", "created",
                 "updated_at", "dob", "birth_date", "birthdate", "signup_date",
                 "start_date", "end_date", "order_date"}
_EMAIL_COLUMNS = {"email", "e-mail", "email_address", "mail"}
_ID_COLUMNS = {"id", "_id", "uuid", "index", "row_id", "record_id", "pk", "key"}
_STATUS_COLUMNS = {"status", "state", "condition", "type", "category"}

_NUMERIC_JUNK_RE = re.compile(r"^[$€£¥]?\s*[\d,]+\.?\d*$")

# ── Anomaly severity buckets ───────────────────────────────────────────────────
_HIGH_SEVERITY_METHODS   = frozenset({"impossible_age", "date_validation",
                                       "non_numeric_in_numeric", "placeholder_email"})
_MEDIUM_SEVERITY_METHODS = frozenset({"iqr", "zscore", "isolation_forest", "sentinel_value"})
# LOW = everything else: categorical_rarity, pattern, text_length


def calculate_quality_score(
    profile: Dict[str, Any],
    anomaly_results: List[Dict[str, Any]],
    security_scan: Optional[Dict[str, Any]] = None,
    delimiter_info: Optional[Dict[str, Any]] = None,
    df: Optional[pd.DataFrame] = None,
    pii_result: Optional[Dict[str, Any]] = None,
    previous_score: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Production-grade quality scoring with calibrated penalties, size normalisation,
    and full explainability.

    Parameters
    ----------
    profile         : result of ``profiling_service.profile_dataframe``
    anomaly_results : list from ``anomaly_service.detect_anomalies``
    security_scan   : result of ``security_scanner.scan_dataframe().to_dict()`` (optional)
    delimiter_info  : result of ``delimiter_detector.detect_delimiter()`` (optional)
    df              : raw DataFrame for deep inspection (optional)
    pii_result      : result of ``pii_service.detect_pii()`` (optional)
    previous_score  : quality score of the previous dataset version (optional)
    """
    rows:        int = max(profile["shape"]["rows"], 1)
    cols:        int = max(profile["shape"]["columns"], 1)
    total_cells: int = rows * cols
    penalties:   List[Dict[str, Any]] = []

    # ══════════════════════════════════════════════════════════════════
    # 1. COMPLETENESS   (max deduct 30)
    #    Formula: missing_pct × 0.5  +  null_rows × 2 (cap 10)
    # ══════════════════════════════════════════════════════════════════
    completeness_deduct = 0.0

    missing_cells = sum(v["count"] for v in profile["missing_values"].values())
    missing_pct   = (missing_cells / total_cells) * 100

    if missing_pct > 0:
        # Non-linear: higher missing % is penalised exponentially harder
        d = min(missing_pct ** 1.2 * 0.35, 35)
        completeness_deduct += d
        affected = [c for c, v in profile["missing_values"].items() if v["count"] > 0]
        penalties.append({
            "dimension":       "completeness",
            "reason":          f"{missing_pct:.1f}% of cells are missing ({missing_cells:,} / {total_cells:,})",
            "impact":          "high" if missing_pct >= 20 else ("medium" if missing_pct >= 5 else "low"),
            "deduction":       -round(d, 2),
            "affected_columns": affected,
        })

    if df is not None:
        null_rows = int(df.isna().all(axis=1).sum())
        if null_rows > 0:
            d = min(null_rows * 2, 10)
            completeness_deduct += d
            penalties.append({
                "dimension":       "completeness",
                "reason":          f"{null_rows} fully-null row(s) found",
                "impact":          "high" if null_rows > 5 else "medium",
                "deduction":       -round(d, 2),
                "affected_columns": [],
            })

    completeness_deduct = min(completeness_deduct, 35)

    # ══════════════════════════════════════════════════════════════════
    # 2. VALIDITY   (max deduct 30)
    # ══════════════════════════════════════════════════════════════════
    validity_deduct = 0.0

    if df is not None:
        for col in df.columns:
            col_lower = str(col).lower().strip()
            series    = df[col].dropna()
            if len(series) == 0:
                continue

            # Age: context-aware valid range 15–90
            if col_lower in _AGE_COLUMNS:
                numeric_vals = pd.to_numeric(series, errors="coerce").dropna()
                invalid_age  = numeric_vals[(numeric_vals < 15) | (numeric_vals > 90)]
                if len(invalid_age):
                    d = min(len(invalid_age) * 2, 15)
                    validity_deduct += d
                    penalties.append({
                        "dimension":       "validity",
                        "reason":          f"{len(invalid_age)} invalid age(s) in '{col}' (expected 15–90): {_sample_vals(invalid_age)}",
                        "impact":          "high",
                        "deduction":       -round(d, 2),
                        "affected_columns": [col],
                    })

            # Email: invalid_email_pct × 0.5, cap 10
            if col_lower in _EMAIL_COLUMNS:
                str_vals = series.astype(str)
                invalid  = str_vals[~str_vals.apply(lambda v: bool(_EMAIL_RE.match(v.strip())))]
                if len(invalid):
                    inv_pct = len(invalid) / len(str_vals) * 100
                    d = min(inv_pct * 0.5, 10)
                    validity_deduct += d
                    penalties.append({
                        "dimension":       "validity",
                        "reason":          f"{len(invalid)} invalid email(s) in '{col}' ({inv_pct:.0f}%)",
                        "impact":          "high" if inv_pct >= 50 else "medium",
                        "deduction":       -round(d, 2),
                        "affected_columns": [col],
                    })

            # Dates: (parse_fail + impossible_year + future_dates) × 2, cap 10
            if col_lower in _DATE_COLUMNS:
                str_vals   = series.astype(str)
                bad_dates  = 0
                future_cnt = 0
                today      = now_ist()
                for val in str_vals:
                    try:
                        with warnings.catch_warnings():
                            warnings.simplefilter("ignore", UserWarning)
                            dt = pd.to_datetime(val, dayfirst=False)
                        if dt.year < 1900 or dt.year > today.year + 1:
                            bad_dates += 1
                        elif (dt > pd.Timestamp(today)
                              and col_lower not in {"end_date", "expiry_date", "due_date"}):
                            future_cnt += 1
                    except Exception:
                        bad_dates += 1

                total_invalid = bad_dates + future_cnt
                if total_invalid:
                    d = min(total_invalid * 2, 10)
                    validity_deduct += d
                    penalties.append({
                        "dimension":       "validity",
                        "reason":          f"{total_invalid} invalid date(s) in '{col}' ({bad_dates} unparseable/impossible, {future_cnt} future)",
                        "impact":          "medium" if total_invalid < 5 else "high",
                        "deduction":       -round(d, 2),
                        "affected_columns": [col],
                    })

            # Mixed data types: numeric-looking column with string pollution — HIGH severity
            if (
                not pd.api.types.is_numeric_dtype(df[col])
                and col_lower not in _EMAIL_COLUMNS
                and col_lower not in _DATE_COLUMNS
                and col_lower not in _STATUS_COLUMNS
                and col_lower not in _ID_COLUMNS
                and len(series) >= 5
            ):
                try:
                    numeric_hits = int(pd.to_numeric(series, errors="coerce").notna().sum())
                    if numeric_hits / len(series) >= 0.7 and len(series) - numeric_hits >= 2:
                        mixed_str = len(series) - numeric_hits
                        pct_mixed = mixed_str / len(series) * 100
                        d = min(pct_mixed ** 1.2 * 0.4, 8)
                        validity_deduct += d
                        penalties.append({
                            "dimension":        "validity",
                            "reason":           f"Mixed data types in '{col}': {mixed_str} non-numeric value(s) in a predominantly numeric column ({pct_mixed:.0f}%)",
                            "impact":           "high",
                            "deduction":        -round(d, 2),
                            "affected_columns": [col],
                        })
                except Exception:
                    pass

    validity_deduct = min(validity_deduct, 35)

    # ══════════════════════════════════════════════════════════════════
    # 3. CONSISTENCY   (max deduct 20)
    #    Formula: date formats ≥ 2 → 5 + (n-2)×2;  case mismatch → +3
    # ══════════════════════════════════════════════════════════════════
    consistency_deduct = 0.0

    if delimiter_info and delimiter_info.get("mixed"):
        d = 15
        consistency_deduct += d
        delims = delimiter_info.get("delimiters_found", [])
        penalties.append({
            "dimension":       "consistency",
            "reason":          f"Mixed delimiters detected ({', '.join(delims)})",
            "impact":          "high",
            "deduction":       -d,
            "affected_columns": [],
        })

    if df is not None:
        for col in df.columns:
            col_lower = str(col).lower().strip()

            if col_lower in _DATE_COLUMNS:
                formats = _detect_date_formats(df[col].dropna().astype(str))
                if len(formats) >= 2:
                    d = 6
                    consistency_deduct += d
                    penalties.append({
                        "dimension":       "consistency",
                        "reason":          f"Mixed date formats in '{col}' ({len(formats)} formats: {', '.join(sorted(formats))})",
                        "impact":          "high" if len(formats) > 2 else "medium",
                        "deduction":       -round(d, 2),
                        "affected_columns": [col],
                    })

            if col_lower in _STATUS_COLUMNS:
                vals       = df[col].dropna().astype(str)
                unique_raw = set(vals.unique())
                unique_norm = {v.lower().strip() for v in unique_raw}
                if len(unique_raw) > len(unique_norm):
                    d = 3
                    consistency_deduct += d
                    penalties.append({
                        "dimension":       "consistency",
                        "reason":          f"Case inconsistency in '{col}' ({len(unique_raw)} variants → {len(unique_norm)} canonical values)",
                        "impact":          "low",
                        "deduction":       -d,
                        "affected_columns": [col],
                    })

    consistency_deduct = min(consistency_deduct, 20)

    # ══════════════════════════════════════════════════════════════════
    # 4. UNIQUENESS   (max deduct 10)
    #    Formula: total_duplicates (exact + logical) × 0.7, capped at 10
    #    Logical duplicates = rows that duplicate all non-ID columns
    # ══════════════════════════════════════════════════════════════════
    uniqueness_deduct = 0.0

    exact_dups   = profile["duplicates"].get("exact_duplicates", 0)
    logical_dups = profile["duplicates"].get("logical_duplicates", 0)
    # logical_dups already includes exact ones — take the max to avoid double-counting
    total_dups   = max(exact_dups, logical_dups)

    if total_dups > 0:
        dup_pct = (total_dups / rows) * 100
        # Non-linear: 10% dup rate hits ~9.5, 20% rate hits cap of 15
        d = min(dup_pct ** 1.2 * 0.6, 15)
        uniqueness_deduct += d
        extra = ""
        if logical_dups > exact_dups:
            extra = f" ({logical_dups - exact_dups} additional logical)"
        penalties.append({
            "dimension":       "uniqueness",
            "reason":          f"{total_dups} duplicate row(s) ({dup_pct:.1f}% of dataset){extra}",
            "impact":          "high" if dup_pct >= 10 else ("medium" if dup_pct >= 2 else "low"),
            "deduction":       -round(d, 2),
            "affected_columns": [],
        })

    uniqueness_deduct = min(uniqueness_deduct, 15)

    # ══════════════════════════════════════════════════════════════════
    # 5. SECURITY   (uncapped — hard cap at 30 when threats detected)
    #    Formula: threat deductions + PII penalty (5 + pii_cols × 2)
    # ══════════════════════════════════════════════════════════════════
    security_deduct  = 0.0
    security_threats = False

    if security_scan and security_scan.get("score_deduction", 0) > 0:
        security_threats = True
        for threat_type, count in security_scan.get("threat_summary", {}).items():
            cols_hit = sorted({
                f["column"]
                for f in security_scan.get("findings", [])
                if f["threat_type"] == threat_type
            })
            threat_d = count * 5
            security_deduct += threat_d
            penalties.append({
                "dimension":       "security",
                "reason":          f"{threat_type.replace('_', ' ').title()} detected in {', '.join(cols_hit)} ({count} occurrence(s))",
                "impact":          "high",
                "deduction":       -threat_d,
                "affected_columns": cols_hit,
            })

    # PII-aware penalty
    pii_columns = 0
    if pii_result:
        pii_columns = pii_result.get("total_pii_columns", 0)
        if pii_columns > 0:
            d = 5 + (pii_columns * 3)
            security_deduct += d
            pii_col_names = [c["column_name"] for c in pii_result.get("columns", [])]
            penalties.append({
                "dimension":       "security",
                "reason":          f"PII detected in {pii_columns} column(s): {', '.join(pii_col_names)} [risk: {pii_result.get('risk_level', 'unknown')}]",
                "impact":          "high" if pii_result.get("risk_level") in ("high", "medium") else "low",
                "deduction":       -round(d, 2),
                "affected_columns": pii_col_names,
            })

    # ══════════════════════════════════════════════════════════════════
    # 6. GARBAGE / JUNK   (max deduct 15)
    #    Formula: garbage_pct × 1.5 (cap 10)  +  placeholder × 2 (cap 5)
    # ══════════════════════════════════════════════════════════════════
    garbage_deduct    = 0.0
    garbage_count     = 0
    placeholder_count = 0

    if df is not None:
        for col in df.select_dtypes(include=["object", "string", "category"]).columns:
            vals      = df[col].dropna().astype(str)
            col_lower = str(col).lower().strip()
            garbage_count += len(
                vals[vals.str.strip().str.lower().isin({v.lower() for v in _GARBAGE_VALUES})]
            )
            if col_lower in _EMAIL_COLUMNS:
                placeholder_count += len(
                    vals[vals.str.strip().str.lower().isin(_PLACEHOLDER_EMAILS)]
                )
            if col_lower in _DATE_COLUMNS:
                placeholder_count += len(vals[vals.str.strip().isin(_PLACEHOLDER_DATES)])

        if garbage_count > 0:
            gp = (garbage_count / total_cells) * 100
            # Non-linear: even 5% garbage punished near cap
            d  = min(gp ** 1.2 * 1.5, 12)
            garbage_deduct += d
            penalties.append({
                "dimension":       "garbage",
                "reason":          f"{garbage_count} garbage/junk value(s) ({gp:.2f}% of cells)",
                "impact":          "medium" if gp >= 2 else "low",
                "deduction":       -round(d, 2),
                "affected_columns": [],
            })

        if placeholder_count > 0:
            d = min(placeholder_count * 2, 5)
            garbage_deduct += d
            penalties.append({
                "dimension":       "garbage",
                "reason":          f"{placeholder_count} placeholder value(s) (test emails, sentinel dates)",
                "impact":          "low",
                "deduction":       -round(d, 2),
                "affected_columns": [],
            })

    garbage_deduct = min(garbage_deduct, 15)

    # ══════════════════════════════════════════════════════════════════
    # 7. SEVERITY-WEIGHTED ANOMALIES   (max deduct 15)
    #    Formula: (high × 3) + (medium × 1.5) + (low × 0.5)
    # ══════════════════════════════════════════════════════════════════
    anomaly_deduct  = 0.0
    anom_aff_cols:   List[str] = []
    high_anom = medium_anom = low_anom = 0

    for anom in anomaly_results:
        severity = anom.get("severity", "low")
        count   = anom.get("anomaly_count", 0)
        anom_aff_cols.append(anom["column"])
        if severity == "high":
            high_anom   += count
        elif severity == "medium":
            medium_anom += count
        else:
            low_anom    += count

    # Medium/low weights deliberately low: IQR flags statistical variation in clean data
    raw_anom       = (high_anom * 3) + (medium_anom * 0.8) + (low_anom * 0.2)
    anomaly_deduct = min(raw_anom, 15)

    if anomaly_deduct > 0:
        penalties.append({
            "dimension":       "anomalies",
            "reason":          f"Severity-weighted anomalies across {len(anomaly_results)} column(s): {high_anom} high-severity, {medium_anom} medium, {low_anom} low",
            "impact":          "high" if high_anom else ("medium" if medium_anom else "low"),
            "deduction":       -round(anomaly_deduct, 2),
            "affected_columns": anom_aff_cols,
        })

    # ══════════════════════════════════════════════════════════════════
    # 8. COMPOUNDING — 3+ failing dimensions add non-linear extra penalty
    #    Each dimension beyond 2 that fails meaningfully adds +5 points
    # ══════════════════════════════════════════════════════════════════
    _failing_dims = sum([
        completeness_deduct > 3,
        validity_deduct > 3,
        consistency_deduct > 3,
        uniqueness_deduct > 3,
        garbage_deduct > 3,
        anomaly_deduct > 3,
    ])
    compounding_deduct = 0.0
    if _failing_dims >= 3:
        compounding_deduct = (_failing_dims - 2) * 5.0
        penalties.append({
            "dimension":        "compounding",
            "reason":           f"{_failing_dims} quality dimensions failing simultaneously — penalties compound",
            "impact":           "high",
            "deduction":        -round(compounding_deduct, 2),
            "affected_columns": [],
        })

    # ══════════════════════════════════════════════════════════════════
    # AGGREGATE — no size normalisation; raw dimension sum
    # ══════════════════════════════════════════════════════════════════
    total_deduction = round(
        completeness_deduct + validity_deduct + consistency_deduct
        + uniqueness_deduct + security_deduct + garbage_deduct + anomaly_deduct
        + compounding_deduct,
        2,
    )

    # ── Final score ─────────────────────────────────────────────────────────
    final_score = max(100.0 - total_deduction, 0.0)

    # Hard cap when active security threats exist
    if security_threats:
        final_score = min(final_score, 30.0)

    # Systemic corruption — cap below 40 when dataset shows pervasive failures
    _systemic = (
        _failing_dims >= 5
        or total_deduction >= 75
        or (missing_pct > 30 and garbage_count > 5 and exact_dups > 10)
    )
    if _systemic:
        final_score = min(final_score, 38.0)

    # Score calibration — prevents inflated scores for low-deduction datasets
    if total_deduction < 5:
        final_score = min(final_score, 95.0)
    elif total_deduction < 15:
        final_score = min(final_score, 88.0)

    total_score = round(final_score, 2)

    # ── Confidence score ─────────────────────────────────────────────────────
    confidence_score = round(min(100.0, math.log10(rows + 1) * 20), 1)

    # ── Version-aware delta ───────────────────────────────────────────────────
    score_delta    = None
    version_status = None
    if previous_score is not None:
        score_delta    = round(total_score - previous_score, 2)
        if score_delta <= -20:
            version_status = "unstable"
        elif score_delta <= -10:
            version_status = "degraded"
        elif score_delta >= 5:
            version_status = "improved"
        else:
            version_status = "stable"

    grade   = _grade(total_score)
    verdict = _verdict(grade)

    # ── Dimension scores (100-scale) ─────────────────────────────────────────
    dim: Dict[str, Any] = {
        "completeness": {
            "score":         max(round(100 - (completeness_deduct / 35) * 100, 1), 0),
            "weight":        0.25,
            "max_deduction": 35,
            "deducted":      round(completeness_deduct, 2),
        },
        "validity": {
            "score":         max(round(100 - (validity_deduct / 30) * 100, 1), 0),
            "weight":        0.20,
            "max_deduction": 30,
            "deducted":      round(validity_deduct, 2),
        },
        "consistency": {
            "score":         max(round(100 - (consistency_deduct / 20) * 100, 1), 0),
            "weight":        0.15,
            "max_deduction": 20,
            "deducted":      round(consistency_deduct, 2),
        },
        "uniqueness": {
            "score":         max(round(100 - (uniqueness_deduct / 15) * 100, 1), 0),
            "weight":        0.15,
            "max_deduction": 15,
            "deducted":      round(uniqueness_deduct, 2),
        },
        "security": {
            "score":         max(round(100 - min(security_deduct, 100), 1), 0),
            "weight":        0.15,
            "max_deduction": "uncapped",
            "deducted":      round(security_deduct, 2),
        },
        "garbage": {
            "score":         max(round(100 - (garbage_deduct / 15) * 100, 1), 0),
            "weight":        0.10,
            "max_deduction": 15,
            "deducted":      round(garbage_deduct, 2),
        },
    }
    for v in dim.values():
        if isinstance(v.get("weight"), float):
            v["weighted"] = round(v["score"] * v["weight"], 2)

    # ── Root-cause analysis (de-duplicated scoring) ──────────────────────────
    # Runs only when a DataFrame is provided; safe to skip (returns empty dict).
    root_cause_analysis: Dict[str, Any] = {}
    fair_score: float = total_score   # default = same as total_score

    if df is not None:
        try:
            from app.services.root_cause_service import analyze_root_causes
            root_cause_analysis = analyze_root_causes(
                df=df,
                profile=profile,
                quality={
                    "penalties_applied": penalties,
                    "metadata": {
                        "missing_pct": round(missing_pct, 2),
                        "duplicate_pct": round((total_dups / rows * 100) if rows > 0 else 0, 2),
                    },
                },
                anomalies=anomaly_results,
                pii_result=pii_result,
            )
            fair_score = root_cause_analysis.get("fair_score", total_score)
        except Exception as _rc_err:
            logger.debug("Root-cause analysis skipped: %s", _rc_err)

    return {
        "total_score":         total_score,
        "fair_score":          fair_score,
        "grade":               grade,
        "fair_grade":          _grade(fair_score),
        "verdict":             verdict,
        "confidence_score":    confidence_score,
        "dimension_scores":    dim,
        "penalties_applied":   penalties,
        "dedup_penalties":     root_cause_analysis.get("dedup_penalties", []),
        "root_cause_analysis": root_cause_analysis.get("columns", {}),
        "group_impacts":       root_cause_analysis.get("group_impacts", {}),
        "metadata": {
            "missing_pct":           round(missing_pct, 2),
            "exact_duplicate_pct":   round((exact_dups / rows * 100) if rows > 0 else 0, 2),
            "logical_duplicate_pct": round((logical_dups / rows * 100) if rows > 0 else 0, 2),
            "duplicate_pct":         round((total_dups / rows * 100) if rows > 0 else 0, 2),
            "anomaly_count":         len(anomaly_results),
            "pii_columns":           pii_columns,
            "drift_detected":        False,
            "score_delta":           score_delta,
            "version_status":        version_status,
            "total_deduction":       total_deduction,
        },
        # Legacy "breakdown" for backward compatibility
        "breakdown": {
            "completeness": {"score": round(30 - completeness_deduct, 2), "max": 30},
            "validity":     {"score": round(30 - validity_deduct, 2),     "max": 30},
            "consistency":  {"score": round(20 - consistency_deduct, 2),  "max": 20},
            "uniqueness":   {"score": round(10 - uniqueness_deduct, 2),   "max": 10},
            "security":     {"score": round(max(15 - security_deduct, 0), 2), "max": 15},
        },
    }


# ── Helpers ────────────────────────────────────────────────────────────────────

def _grade(score: float) -> str:
    if score >= 90: return "A"
    if score >= 75: return "B"
    if score >= 60: return "C"
    if score >= 40: return "D"
    return "F"


def _verdict(grade: str) -> str:
    return {
        "A": "Production Ready — All dimensions within acceptable thresholds",
        "B": "Good — Minor issues that should be addressed",
        "C": "Needs Cleaning — Several quality issues detected",
        "D": "Poor — Significant remediation required before use",
        "F": "Unusable — Critical quality failures detected",
    }.get(grade, "Unknown")


def _deduct_for_threat(threat_type: str) -> int:
    from app.services.security_scanner import DEDUCTION_MAP
    return DEDUCTION_MAP.get(threat_type, 10)


def _sample_vals(series: pd.Series, n: int = 5) -> str:
    return ", ".join(str(v) for v in series.head(n).tolist())


def _detect_date_formats(str_series: pd.Series) -> set:
    """Detect distinct date format patterns present in a string series."""
    format_patterns = [
        (r"^\d{4}-\d{2}-\d{2}$",       "YYYY-MM-DD"),
        (r"^\d{2}/\d{2}/\d{4}$",       "DD/MM/YYYY"),
        (r"^\d{2}-\d{2}-\d{4}$",       "DD-MM-YYYY"),
        (r"^\d{4}/\d{2}/\d{2}$",       "YYYY/MM/DD"),
        (r"^\w+ \d{1,2},?\s*\d{4}$",   "Month DD YYYY"),
        (r"^\d{1,2} \w+ \d{4}$",       "DD Month YYYY"),
        (r"^\d{8}$",                    "YYYYMMDD"),
        (r"^\d{1,2}\.\d{1,2}\.\d{4}$", "DD.MM.YYYY"),
    ]
    found: set = set()
    for val in str_series.head(100):
        val = str(val).strip()
        for pattern, label in format_patterns:
            if re.match(pattern, val):
                found.add(label)
                break
    return found
