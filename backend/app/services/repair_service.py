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

_JUNK_VALUES = {
    "?", "??", "???", "-", "--", "---",
    "n/a", "na", "null", "none", "nan", "undefined", "unknown",
    "error", "#ref!", "#value!", "#n/a",
    "test", "todo", "tbd", "xxx", "aaa", "abc",
    "0000000000", "1234567890", "000-000-0000",
}
_JUNK_LOWER = {v.lower() for v in _JUNK_VALUES}

# NULL-pattern strings pandas does NOT auto-convert to NaN
_EXTRA_NULL_PATTERNS = {"?", "??", "???", "undefined", "UNDEFINED", "n/a", "N/A"}

# Domain rules: (col_keywords, min_valid, max_valid or None)
_REPAIR_DOMAIN_RULES = [
    (("age", "years", "edad"),               0,   120),
    (("score", "rating"),                     0,   100),
    (("percentage", "pct", "percent"),        0,   100),
    (("price", "cost", "amount", "salary",
      "revenue", "fee"),                      0, None),  # non-negative
]

_REPAIR_ID_COLS    = {"id", "_id", "uuid", "index", "row_id", "record_id", "pk", "key"}
_REPAIR_EMAIL_COLS = {"email", "e-mail", "email_address", "mail"}
_REPAIR_DATE_COLS  = {
    "date", "joindate", "join_date", "created_at", "created",
    "updated_at", "dob", "birth_date", "birthdate", "signup_date",
    "start_date", "end_date", "order_date",
}
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def suggest_repairs(df: pd.DataFrame, profile: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Directly analyse *df* and return actionable repair suggestions.
    Works from the raw DataFrame — never relies on profile dtype for
    numeric/non-numeric classification (avoids false-zero when a numeric
    column is stored as object because of a few bad string values).
    """
    suggestions: List[Dict[str, Any]] = []
    rows = len(df)
    if rows == 0:
        return suggestions

    # ── Pre-process: normalise known null strings so missing counts are accurate
    df_norm = df.copy()
    for col in df_norm.select_dtypes(include=["object"]).columns:
        df_norm[col] = df_norm[col].apply(
            lambda v: np.nan
            if isinstance(v, str) and v.strip().lower() in _JUNK_LOWER
            else v
        )

    # ── 1. Logical duplicate rows (excluding ID columns) ─────────────────────
    id_cols  = {c for c in df.columns if str(c).lower().strip() in _REPAIR_ID_COLS}
    non_id   = [c for c in df.columns if c not in id_cols]
    dup_count = int(df.duplicated(subset=non_id).sum()) if non_id else int(df.duplicated().sum())
    if dup_count > 0:
        suggestions.append({
            "column":     "__all__",
            "issue_type": "duplicates",
            "suggestion": f"Remove {dup_count} duplicate row(s) (identical values across non-ID columns).",
            "action":     "remove_duplicates",
            "details":    {"duplicate_count": dup_count},
        })

    for col in df.columns:
        col_lower = str(col).lower().strip()
        if col_lower in _REPAIR_ID_COLS:
            continue

        raw   = df[col]
        normd = df_norm[col]
        missing_count = int(normd.isna().sum())
        missing_pct   = round(missing_count / rows * 100, 1)
        non_null_raw  = raw.dropna()
        non_null_norm = normd.dropna()

        # ── Determine if this column is *numeric in intent* ──────────────────
        is_pure_numeric = pd.api.types.is_numeric_dtype(raw)
        if is_pure_numeric:
            numeric_series = raw.dropna()
            numeric_ratio  = 1.0
            invalid_text   = []
        else:
            str_vals           = non_null_raw.astype(str)
            parsed             = pd.to_numeric(str_vals, errors="coerce")
            numeric_ratio      = parsed.notna().sum() / max(len(str_vals), 1)
            numeric_series     = parsed.dropna()
            # non-junk strings that can't be parsed as numbers = invalid type values
            invalid_text = str_vals[
                parsed.isna() & ~str_vals.str.strip().str.lower().isin(_JUNK_LOWER)
            ].tolist()

        is_numeric_col = is_pure_numeric or numeric_ratio >= 0.6

        # ── 2. Garbage / placeholder values ───────────────────────────────────
        if not is_pure_numeric:
            junk_mask  = non_null_raw.astype(str).str.strip().str.lower().isin(_JUNK_LOWER)
            junk_count = int(junk_mask.sum())
            if junk_count > 0:
                examples = non_null_raw.astype(str)[junk_mask].head(3).tolist()
                suggestions.append({
                    "column":     col,
                    "issue_type": "garbage_values",
                    "suggestion": (
                        f"'{col}' contains {junk_count} garbage/placeholder value(s) "
                        f"(e.g. {', '.join(repr(e) for e in examples)}). "
                        "The cleaning pipeline replaces these with NaN before imputing."
                    ),
                    "action":  "fix_junk",
                    "details": {"garbage_count": junk_count, "examples": examples},
                })

        # ── 3. Invalid text in a numeric-intent column ────────────────────────
        if not is_pure_numeric and is_numeric_col and invalid_text:
            suggestions.append({
                "column":     col,
                "issue_type": "invalid_type",
                "suggestion": (
                    f"'{col}' is a numeric column but has {len(invalid_text)} "
                    f"non-numeric value(s): {', '.join(repr(v) for v in invalid_text[:4])}. "
                    "These will be removed and the column converted to numeric."
                ),
                "action":  "fix_mixed_types",
                "details": {"invalid_count": len(invalid_text), "examples": invalid_text[:5]},
            })

        # ── 4. Out-of-range values (domain rules) ─────────────────────────────
        if is_numeric_col and len(numeric_series) > 0:
            for keywords, lo, hi in _REPAIR_DOMAIN_RULES:
                if any(kw in col_lower for kw in keywords):
                    if lo is not None and hi is not None:
                        out_mask = (numeric_series < lo) | (numeric_series > hi)
                    elif lo is not None:
                        out_mask = numeric_series < lo
                    else:
                        out_mask = numeric_series > hi
                    out_count = int(out_mask.sum())
                    if out_count > 0:
                        rng      = f"{lo}–{hi}" if hi is not None else f"≥ {lo}"
                        examples = numeric_series[out_mask].head(3).tolist()
                        suggestions.append({
                            "column":     col,
                            "issue_type": "out_of_range",
                            "suggestion": (
                                f"{out_count} value(s) in '{col}' fall outside the valid "
                                f"range ({rng}), e.g. {examples}. "
                                "These will be nulled and re-imputed."
                            ),
                            "action":  "fix_range",
                            "details": {"out_of_range_count": out_count, "valid_range": rng,
                                        "examples": [float(v) for v in examples]},
                        })
                    break

        # ── 5. Missing values ─────────────────────────────────────────────────
        if missing_count > 0:
            if is_numeric_col and len(numeric_series) > 1:
                skew     = float(numeric_series.skew())
                strategy = "median" if abs(skew) > 1.0 else "mean"
                fill_val = round(float(
                    numeric_series.median() if strategy == "median" else numeric_series.mean()
                ), 2)
                suggestions.append({
                    "column":     col,
                    "issue_type": "missing_values",
                    "suggestion": (
                        f"Fill {missing_count} missing value(s) in '{col}' "
                        f"({missing_pct}%) with {strategy} = {fill_val}."
                    ),
                    "action":  f"fill_{strategy}",
                    "details": {"missing_count": missing_count, "missing_pct": missing_pct,
                                "fill_value": fill_val, "strategy": strategy},
                })
            elif not is_numeric_col and len(non_null_norm) > 0:
                mode_vals = non_null_norm.mode()
                if len(mode_vals) > 0:
                    mode_val = str(mode_vals.iloc[0])
                    suggestions.append({
                        "column":     col,
                        "issue_type": "missing_values",
                        "suggestion": (
                            f"Fill {missing_count} missing value(s) in '{col}' "
                            f"({missing_pct}%) with mode ('{mode_val}')."
                        ),
                        "action":  "fill_mode",
                        "details": {"missing_count": missing_count, "missing_pct": missing_pct,
                                    "fill_value": mode_val},
                    })

        # ── 6. Invalid email addresses ────────────────────────────────────────
        if col_lower in _REPAIR_EMAIL_COLS:
            email_str   = non_null_raw.astype(str)
            invalid_em  = email_str[~email_str.str.strip().apply(
                lambda v: bool(_EMAIL_REGEX.match(v))
            )]
            if len(invalid_em) > 0:
                suggestions.append({
                    "column":     col,
                    "issue_type": "invalid_emails",
                    "suggestion": (
                        f"Found {len(invalid_em)} invalid email address(es) in '{col}' "
                        f"(e.g. {', '.join(repr(v) for v in invalid_em.head(3).tolist())}). "
                        "These will be removed."
                    ),
                    "action":  "flag_emails",
                    "details": {"invalid_count": len(invalid_em),
                                "examples": invalid_em.head(5).tolist()},
                })

        # ── 7. Date issues (invalid / non-ISO) ───────────────────────────────
        if col_lower in _REPAIR_DATE_COLS:
            date_str   = non_null_raw.astype(str)
            bad_dates, non_iso = [], []
            for v in date_str:
                v_s = v.strip()
                if _ISO_DATE_RE.match(v_s):
                    continue
                try:
                    pd.to_datetime(v_s, dayfirst=False)
                    non_iso.append(v_s)
                except Exception:
                    bad_dates.append(v_s)
            if bad_dates:
                suggestions.append({
                    "column":     col,
                    "issue_type": "invalid_dates",
                    "suggestion": (
                        f"{len(bad_dates)} unparseable date value(s) in '{col}' "
                        f"(e.g. {', '.join(repr(v) for v in bad_dates[:3])}). "
                        "These will be removed."
                    ),
                    "action":  "fix_dates",
                    "details": {"invalid_count": len(bad_dates), "examples": bad_dates[:5]},
                })
            if non_iso:
                suggestions.append({
                    "column":     col,
                    "issue_type": "inconsistent_dates",
                    "suggestion": (
                        f"{len(non_iso)} date(s) in '{col}' are not ISO 8601 "
                        f"(e.g. {', '.join(repr(v) for v in non_iso[:3])}). "
                        "These will be standardised to YYYY-MM-DD."
                    ),
                    "action":  "standardize_dates",
                    "details": {"count": len(non_iso), "examples": non_iso[:5]},
                })

        # ── 8. Country / categorical synonyms ────────────────────────────────
        if not is_numeric_col and col_lower not in _REPAIR_DATE_COLS | _REPAIR_EMAIL_COLS:
            synonyms = _find_country_synonyms(raw)
            if synonyms:
                suggestions.append({
                    "column":     col,
                    "issue_type": "categorical_synonyms",
                    "suggestion": f"Normalise {len(synonyms)} country/region synonym(s) in '{col}'.",
                    "action":     "normalize_categories",
                    "details":    {"synonym_map": synonyms},
                })

        # ── 9. Extremely high missing rate ────────────────────────────────────
        if missing_pct >= 50:
            suggestions.append({
                "column":     col,
                "issue_type": "high_missing_rate",
                "suggestion": (
                    f"'{col}' is {missing_pct}% empty. "
                    "Consider dropping the column or investigating the source."
                ),
                "action":  "review_column",
                "details": {"missing_pct": missing_pct},
            })

    return suggestions

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
        col_lower = str(col).lower().strip()
        missing_pct = col_stats["missing_percentage"]

        # -------------------------------------------------------------
        # 2. Garbage / placeholder values
        # -------------------------------------------------------------
        if dtype in ("object", "category", "string"):
            vals = series.dropna().astype(str)
            junk_mask = vals.str.strip().str.lower().isin(_JUNK_VALUES)
            junk_count = int(junk_mask.sum())
            if junk_count > 0:
                examples = vals[junk_mask].head(3).tolist()
                suggestions.append(
                    {
                        "column": col,
                        "issue_type": "garbage_values",
                        "suggestion": (
                            f"Found {junk_count} garbage/placeholder value(s) in '{col}' "
                            f"(e.g. {', '.join(repr(e) for e in examples)}). "
                            "The cleaning pipeline will replace these with NaN."
                        ),
                        "action": "fix_junk",
                        "details": {"garbage_count": junk_count, "examples": examples},
                    }
                )

        # -------------------------------------------------------------
        # 3. Missing value fill strategy
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
        # 4. Date standardisation
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
        # 5. Country / categorical synonym normalisation
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
        # 6. Mixed casing fix
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
        # 7. Email validation
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

        # -------------------------------------------------------------
        # 8. Mixed-type column (object column that is mostly numeric)
        # -------------------------------------------------------------
        if dtype in ("object", "category", "string"):
            non_null = series.dropna()
            total_non_null = len(non_null)
            if total_non_null >= 5:
                numeric_hits = int(pd.to_numeric(non_null, errors="coerce").notna().sum())
                ratio = numeric_hits / total_non_null
                non_numeric_count = total_non_null - numeric_hits
                if ratio >= 0.7 and non_numeric_count >= 2:
                    suggestions.append(
                        {
                            "column": col,
                            "issue_type": "mixed_types",
                            "suggestion": (
                                f"'{col}' is {round(ratio*100)}% numeric but contains "
                                f"{non_numeric_count} non-numeric value(s) — likely data entry errors."
                            ),
                            "action": "fix_mixed_types",
                            "details": {"non_numeric_count": non_numeric_count, "numeric_ratio": round(ratio, 3)},
                        }
                    )

        # -------------------------------------------------------------
        # 9. Out-of-range numeric values
        # -------------------------------------------------------------
        if pd.api.types.is_numeric_dtype(series):
            s = series.dropna()
            if len(s) > 0 and any(kw in col_lower for kw in _POSITIVE_NUMERIC_KEYWORDS):
                neg_count = int((s < 0).sum())
                if neg_count > 0:
                    suggestions.append(
                        {
                            "column": col,
                            "issue_type": "out_of_range",
                            "suggestion": (
                                f"{neg_count} negative value(s) in '{col}' which should be non-negative."
                            ),
                            "action": "fix_range",
                            "details": {"negative_count": neg_count, "min_value": float(s.min())},
                        }
                    )

        # -------------------------------------------------------------
        # 10. Extremely high missing rate — suggest review / drop
        # -------------------------------------------------------------
        if missing_pct >= 50 and col_stats["missing_count"] > 0:
            suggestions.append(
                {
                    "column": col,
                    "issue_type": "high_missing_rate",
                    "suggestion": (
                        f"'{col}' is {missing_pct}% empty. Consider dropping the column "
                        "or investigating the data source."
                    ),
                    "action": "review_column",
                    "details": {"missing_pct": missing_pct},
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
    if "email" in str(col).lower() or "e-mail" in str(col).lower() or "mail" in str(col).lower():
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
