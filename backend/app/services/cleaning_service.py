"""
Professional Data Cleaning / Repair Engine â€” v2.

CleaningEngine applies a structured multi-step pipeline to a DataFrame,
recalculates the real quality score after every step, and returns a rich
before-vs-after report with per-step repair impact tracking.

Repair operations
-----------------
1. Junk/placeholder normalisation  (NULL, ???, N/A â†’ NaN then impute)
2. Missing numeric fill            (mean / median auto-selected)
3. Missing categorical fill        (mode)
4. Duplicate removal               (exact)
5. Date standardisation            (â†’ ISO 8601, drops impossible dates)
6. Email validation                (flag / remove invalids)
7. Country synonym normalisation   (USA / US / â€¦ â†’ United States)
8. Whitespace & casing             (trim + optional lower/upper/title)
9. Outlier removal                 (IQR, conservative, opt-in)

Score recalculation after EVERY step uses the real quality_service engine â€”
no artificial inflation.
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from app.models.dataset import CleaningRequest
from app.services import dataset_service
from app.services.anomaly_service import detect_anomalies
from app.services.profiling_service import profile_dataframe, _NULL_PATTERNS
from app.services.quality_service import calculate_quality_score
from app.utils.file_utils import build_versioned_path, load_dataframe, save_dataframe
from app.utils.time_utils import now_ist

logger = logging.getLogger(__name__)

# â”€â”€ Country / region synonym map â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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
    "england": "United Kingdom",
    "ind": "India",
    "uae": "United Arab Emirates",
    "can": "Canada",
    "aus": "Australia",
}

_EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")

# Domain rules: (col_name_keywords, min_valid, max_valid)
_DOMAIN_RULES = [
    (("age", "years", "edad"),               0,   120),
    (("score", "rating", "grade"),            0,   100),
    (("percentage", "pct", "percent"),        0,   100),
    (("price", "cost", "amount", "salary",
      "revenue", "fee", "rate"),              0, None),  # non-negative only
]

_DATE_FORMATS = [
    "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y",
    "%d-%m-%Y", "%m-%d-%Y", "%Y/%m/%d",
    "%d %b %Y", "%b %d %Y", "%d %B %Y",
]

_EMAIL_COLUMNS = {"email", "e-mail", "email_address", "mail"}
_DATE_COLUMNS = {
    "date", "joindate", "join_date", "created_at", "created",
    "updated_at", "dob", "birth_date", "birthdate", "signup_date",
    "start_date", "end_date", "order_date",
}
_COUNTRY_COLUMNS = {"country", "country_name", "nation", "region"}


# â”€â”€ Dataclasses â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@dataclass
class RepairStep:
    name: str                    # human label
    dimension: str               # completeness / validity / consistency / uniqueness
    rows_affected: int
    score_before: float
    score_after: float
    score_gain: float
    confidence: str              # high / medium / low
    description: str
    changes: List[str] = field(default_factory=list)


@dataclass
class CleaningReport:
    dataset_id: str
    original_version: int
    new_version: int
    rows_before: int
    rows_after: int
    score_before: float
    score_after: float
    score_delta: float
    improvement_pct: float
    grade_before: str
    grade_after: str
    steps: List[RepairStep]
    remaining_issues: List[str]
    generated_at: str


# â”€â”€ Quick score helper â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _quick_score(df: pd.DataFrame) -> Tuple[float, str]:
    """Run profile + anomaly + quality pipeline and return (score, grade)."""
    profile = profile_dataframe(df)
    anomalies = detect_anomalies(df)
    result = calculate_quality_score(profile=profile, anomaly_results=anomalies, df=df)
    return float(result["total_score"]), result["grade"]


# â”€â”€ Repair steps â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _step_fix_mixed_types(df: pd.DataFrame) -> Tuple[pd.DataFrame, int, List[str]]:
    """
    Convert object columns that are predominantly numeric.
    'twenty', 'abc', 'hello' etc. → NaN, then the column becomes float64.
    This must run BEFORE domain-fix and fill-numeric so those steps can see
    the now-numeric column.
    """
    total_fixed = 0
    changes: List[str] = []
    for col in df.select_dtypes(include=["object"]).columns:
        non_null = df[col].dropna()
        if len(non_null) < 3:
            continue
        parsed        = pd.to_numeric(non_null.astype(str), errors="coerce")
        numeric_ratio = parsed.notna().sum() / len(non_null)
        if numeric_ratio < 0.6:
            continue  # Not a numeric-intent column
        # Convert the whole column, coercing bad strings to NaN
        orig_non_null_count = int(df[col].notna().sum())
        new_col = pd.to_numeric(df[col], errors="coerce")
        became_nan = int(df[col].notna().sum()) - int(new_col.notna().sum())
        if became_nan > 0:
            examples = df.loc[df[col].notna() & new_col.isna(), col].head(3).tolist()
            df[col]      = new_col
            total_fixed += became_nan
            changes.append(
                f"'{col}': converted {became_nan} non-numeric value(s) to NaN "
                f"(e.g. {examples}) — column is predominantly numeric"
            )
        else:
            df[col] = new_col  # still coerce dtype to float for domain-fix
    return df, total_fixed, changes


def _step_fix_domain_values(df: pd.DataFrame) -> Tuple[pd.DataFrame, int, List[str]]:
    """Null out numeric values that violate known domain constraints (age, score, %)."""
    total_fixed = 0
    changes: List[str] = []
    for col in df.select_dtypes(include=[np.number]).columns:
        col_lower = str(col).lower().strip()
        for keywords, lo, hi in _DOMAIN_RULES:
            if not any(kw in col_lower for kw in keywords):
                continue
            mask = pd.Series([False] * len(df), index=df.index)
            if lo is not None:
                mask = mask | (df[col] < lo)
            if hi is not None:
                mask = mask | (df[col] > hi)
            mask = mask & df[col].notna()
            count = int(mask.sum())
            if count > 0:
                sample = df.loc[mask, col].head(3).tolist()
                df.loc[mask, col] = np.nan
                total_fixed += count
                rng = f"{lo}–{hi}" if hi is not None else f">= {lo}"
                changes.append(
                    f"'{col}': removed {count} out-of-range value(s) "
                    f"(valid range {rng}; e.g. {sample})"
                )
            break  # one rule per column is enough
    return df, total_fixed, changes


def _step_junk_normalise(df: pd.DataFrame) -> Tuple[pd.DataFrame, int, List[str]]:
    """Replace junk sentinel strings with NaN (prerequisite for imputation)."""
    before_nulls = int(df.isna().sum().sum())
    df = df.replace(list(_NULL_PATTERNS), np.nan)
    after_nulls = int(df.isna().sum().sum())
    delta = after_nulls - before_nulls
    changes = []
    if delta > 0:
        changes.append(f"Converted {delta} junk/placeholder values (NULL, ???, N/A, â€¦) to proper missing values.")
    return df, delta, changes


def _step_fill_numeric(df: pd.DataFrame) -> Tuple[pd.DataFrame, int, List[str]]:
    total_filled = 0
    changes: List[str] = []
    for col in df.select_dtypes(include=[np.number]).columns:
        missing = int(df[col].isna().sum())
        if missing == 0:
            continue
        non_null = df[col].dropna()
        skew = float(non_null.skew()) if len(non_null) > 1 else 0.0
        if abs(skew) > 1.0:
            fill_val = float(non_null.median())
            strategy = "median"
        else:
            fill_val = float(non_null.mean())
            strategy = "mean"
        df[col] = df[col].fillna(fill_val)
        total_filled += missing
        changes.append(f"'{col}': filled {missing} missing with {strategy} ({round(fill_val, 4)})")
    return df, total_filled, changes


def _step_fill_categorical(df: pd.DataFrame) -> Tuple[pd.DataFrame, int, List[str]]:
    total_filled = 0
    changes: List[str] = []
    for col in df.select_dtypes(include=["object", "category"]).columns:
        missing = int(df[col].isna().sum())
        if missing == 0:
            continue
        mode_vals = df[col].dropna().mode()
        if len(mode_vals) == 0:
            continue
        mode_val = mode_vals.iloc[0]
        df[col] = df[col].fillna(mode_val)
        total_filled += missing
        changes.append(f"'{col}': filled {missing} missing with mode ('{mode_val}')")
    return df, total_filled, changes


def _step_remove_duplicates(df: pd.DataFrame) -> Tuple[pd.DataFrame, int, List[str]]:
    before = len(df)
    df = df.drop_duplicates()
    removed = before - len(df)
    changes = [f"Removed {removed} exact duplicate rows."] if removed > 0 else []
    return df, removed, changes


def _step_standardise_dates(df: pd.DataFrame) -> Tuple[pd.DataFrame, int, List[str]]:
    total_converted = 0
    changes: List[str] = []
    for col in df.select_dtypes(include=["object"]).columns:
        if str(col).lower().strip() not in _DATE_COLUMNS:
            continue
        converted, fmt = _try_parse_dates(df[col])
        if converted is None or fmt is None:
            continue
        # Drop impossible dates (year < 1900 or > 2100)
        impossible = converted[(converted.dt.year < 1900) | (converted.dt.year > 2100)]
        if len(impossible) > 0:
            df.loc[impossible.index, col] = np.nan
            changes.append(f"'{col}': dropped {len(impossible)} impossible dates.")
        if fmt != "%Y-%m-%d":
            mask = converted.notna()
            df.loc[mask, col] = converted[mask].dt.strftime("%Y-%m-%d")
            cnt = int(mask.sum())
            total_converted += cnt
            changes.append(f"'{col}': standardised {cnt} dates from '{fmt}' â†’ ISO 8601.")
    return df, total_converted, changes


def _step_fix_emails(df: pd.DataFrame) -> Tuple[pd.DataFrame, int, List[str]]:
    total_fixed = 0
    changes: List[str] = []
    for col in df.columns:
        if str(col).lower().strip() not in _EMAIL_COLUMNS:
            continue
        series = df[col].dropna().astype(str)
        invalid_mask = ~series.str.strip().apply(lambda v: bool(_EMAIL_RE.match(v)))
        invalid_idx = series[invalid_mask].index
        if len(invalid_idx) == 0:
            continue
        df.loc[invalid_idx, col] = np.nan
        total_fixed += len(invalid_idx)
        sample = series[invalid_idx].head(3).tolist()
        changes.append(
            f"'{col}': removed {len(invalid_idx)} invalid email(s) "
            f"(e.g. {', '.join(sample)}). Set to missing."
        )
    return df, total_fixed, changes


def _step_normalise_countries(df: pd.DataFrame) -> Tuple[pd.DataFrame, int, List[str]]:
    total_fixed = 0
    changes: List[str] = []
    for col in df.select_dtypes(include=["object"]).columns:
        if str(col).lower().strip() not in _COUNTRY_COLUMNS:
            continue
        before = df[col].copy()
        df[col] = df[col].apply(
            lambda v: _COUNTRY_MAP.get(str(v).lower().strip(), v) if pd.notna(v) else v
        )
        fixed = int((before != df[col]).sum())
        if fixed > 0:
            total_fixed += fixed
            changes.append(f"'{col}': normalised {fixed} country synonyms (e.g. USAâ†’United States).")
    return df, total_fixed, changes


def _step_normalise_text(
    df: pd.DataFrame, case: Optional[str], trim: bool = True
) -> Tuple[pd.DataFrame, int, List[str]]:
    total_affected = 0
    changes: List[str] = []
    case_fn = {"lower": str.lower, "upper": str.upper, "title": str.title}.get(
        (case or "").lower()
    )
    for col in df.select_dtypes(include=["object"]).columns:
        affected = 0
        if trim:
            trimmed = df[col].apply(lambda v: v.strip() if isinstance(v, str) else v)
            delta = int((trimmed != df[col]).sum())
            if delta > 0:
                df[col] = trimmed
                affected += delta
        if case_fn:
            cased = df[col].apply(lambda v: case_fn(v) if isinstance(v, str) else v)
            delta = int((cased != df[col]).sum())
            if delta > 0:
                df[col] = cased
                affected += delta
        total_affected += affected
    if total_affected > 0:
        parts = []
        if trim:
            parts.append("trimmed whitespace")
        if case_fn:
            parts.append(f"applied '{case}' casing")
        changes.append(f"Text normalisation ({', '.join(parts)}): {total_affected} values updated.")
    return df, total_affected, changes


def _step_remove_outliers(df: pd.DataFrame) -> Tuple[pd.DataFrame, int, List[str]]:
    changes: List[str] = []
    total_removed = 0
    for col in df.select_dtypes(include=[np.number]).columns:
        q1 = df[col].quantile(0.25)
        q3 = df[col].quantile(0.75)
        iqr = q3 - q1
        if iqr == 0:
            continue
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        mask = df[col].between(lower, upper, inclusive="both") | df[col].isna()
        removed = int((~mask).sum())
        if removed > 0:
            df = df[mask].reset_index(drop=True)
            total_removed += removed
            changes.append(f"'{col}': removed {removed} IQR outlier rows.")
    return df, total_removed, changes


# â”€â”€ Confidence heuristics â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _confidence(step_name: str, rows_affected: int, total_rows: int) -> str:
    pct = rows_affected / max(total_rows, 1) * 100
    if step_name in ("remove_duplicates", "country_normalise", "text_normalise",
                     "domain_fix", "junk_normalise"):
        return "high"
    if step_name in ("fill_numeric", "fill_categorical") and pct < 10:
        return "high"
    if step_name in ("fill_numeric", "fill_categorical") and pct < 30:
        return "medium"
    if step_name == "date_standardise":
        return "high" if rows_affected > 0 else "high"
    if step_name == "email_fix":
        return "medium"
    if step_name == "remove_outliers":
        return "low" if pct > 5 else "medium"
    return "medium"


# â”€â”€ Remaining issues scanner â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _remaining_issues(df: pd.DataFrame) -> List[str]:
    issues: List[str] = []
    profile = profile_dataframe(df)
    # Missing values
    still_missing = [
        f"'{c}': {v['count']} missing ({v['percentage']}%)"
        for c, v in profile["missing_values"].items()
        if v["count"] > 0
    ]
    if still_missing:
        issues.append(f"Missing values remain in: {', '.join(still_missing[:5])}")
    # Duplicates
    exact = profile["duplicates"].get("exact_duplicates", 0)
    if exact > 0:
        issues.append(f"{exact} duplicate rows still present (not selected for removal).")
    return issues


# â”€â”€ Date helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _try_parse_dates(series: pd.Series) -> Tuple[Optional[pd.Series], Optional[str]]:
    sample = series.dropna().astype(str).head(30)
    if len(sample) == 0:
        return None, None
    for fmt in _DATE_FORMATS:
        hits = sum(1 for v in sample if _safe_strptime(v, fmt))
        if hits / len(sample) >= 0.7:
            try:
                parsed = pd.to_datetime(series, format=fmt, errors="coerce")
                if parsed.notna().sum() / max(len(series), 1) >= 0.7:
                    return parsed, fmt
            except Exception:
                pass
    # Try ISO already
    try:
        parsed = pd.to_datetime(series, errors="coerce")
        if parsed.notna().sum() / max(len(series), 1) >= 0.7:
            return parsed, "%Y-%m-%d"
    except Exception:
        pass
    return None, None


def _safe_strptime(val: str, fmt: str) -> bool:
    try:
        datetime.strptime(val.strip(), fmt)
        return True
    except (ValueError, TypeError):
        return False


# â”€â”€ Main pipeline â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

async def apply_cleaning(
    dataset_id: str,
    request: CleaningRequest,
) -> Dict[str, Any]:
    """
    Load the requested version, run the step-by-step cleaning pipeline,
    recalculate quality score after each step, persist the cleaned DataFrame
    as a new version, and return a comprehensive CleaningReport.
    """
    file_path, file_type, version = await dataset_service.resolve_version_path(
        dataset_id, request.version
    )

    df = load_dataframe(file_path, file_type)
    # Ensure all column names are strings — prevents AttributeError on .lower() / .strip()
    df.columns = df.columns.map(str)
    rows_before = len(df)
    total_rows = rows_before

    # ── Baseline score
    score_before, grade_before = _quick_score(df)
    current_score = score_before
    steps: List[RepairStep] = []

    # ── Early exit: already high quality ────────────────────────────────────
    if score_before > 80:
        return {
            "dataset_id":       dataset_id,
            "original_version": version,
            "new_version":      version,
            "rows_before":      rows_before,
            "rows_after":       rows_before,
            "score_before":     round(score_before, 2),
            "score_after":      round(score_before, 2),
            "score_delta":      0.0,
            "improvement_pct":  0.0,
            "grade_before":     grade_before,
            "grade_after":      grade_before,
            "fixes_applied":    ["Dataset quality is already above 80 — no cleaning required."],
            "steps":            [],
            "remaining_issues": [],
            "generated_at":     now_ist().isoformat(),
            "skipped":          True,
        }

    def _record_step(
        name: str,
        label: str,
        dimension: str,
        df_new: pd.DataFrame,
        rows_affected: int,
        changes: List[str],
        confidence_key: str,
    ) -> pd.DataFrame:
        nonlocal current_score
        if rows_affected == 0 and not changes:
            return df_new
        score_new, _ = _quick_score(df_new)
        gain = round(score_new - current_score, 2)
        conf = _confidence(confidence_key, rows_affected, total_rows)
        steps.append(RepairStep(
            name=label,
            dimension=dimension,
            rows_affected=rows_affected,
            score_before=round(current_score, 2),
            score_after=round(score_new, 2),
            score_gain=gain,
            confidence=conf,
            description="; ".join(changes) if changes else label,
            changes=changes,
        ))
        current_score = score_new
        return df_new

    # ── Step 0a: Junk normalisation (always runs)
    df_new, affected, changes = _step_junk_normalise(df)
    df = _record_step("junk_normalise", "Junk Value Normalisation", "garbage",
                      df_new, affected, changes, "junk_normalise")

    # ── Step 0b: Mixed-type repair (always runs)
    # Converts "twenty", "abc", "hello" etc. → NaN in numeric-intent columns.
    # Must run BEFORE domain_fix so domain_fix can see the float column.
    df_new, affected, changes = _step_fix_mixed_types(df)
    df = _record_step("mixed_type_fix", "Mixed-Type Value Removal", "validity",
                      df_new, affected, changes, "domain_fix")

    # ── Step 0c: Domain-invalid value removal (always runs)
    df_new, affected, changes = _step_fix_domain_values(df)
    df = _record_step("domain_fix", "Domain Constraint Enforcement", "validity",
                      df_new, affected, changes, "domain_fix")

    # ── Step 0d: Re-fill NaNs created by steps 0b and 0c (always runs)
    df_new, filled, fill_changes = _step_fill_numeric(df)
    if filled > 0:
        df = _record_step("initial_refill", "Re-impute Cleaned-Value NaNs", "completeness",
                          df_new, filled, fill_changes, "fill_numeric")
    else:
        df = df_new

    # â”€â”€ Step 1: Fill missing numeric â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if request.fix_missing_numeric:
        df_new, affected, changes = _step_fill_numeric(df)
        df = _record_step("fill_numeric", "Missing Numeric Imputation", "completeness",
                          df_new, affected, changes, "fill_numeric")

    # â”€â”€ Step 2: Fill missing categorical â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if request.fix_missing_categorical:
        df_new, affected, changes = _step_fill_categorical(df)
        df = _record_step("fill_categorical", "Missing Categorical Imputation", "completeness",
                          df_new, affected, changes, "fill_categorical")

    # â”€â”€ Step 3: Remove duplicates â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if request.fix_duplicates:
        df_new, affected, changes = _step_remove_duplicates(df)
        df = _record_step("remove_duplicates", "Duplicate Removal", "uniqueness",
                          df_new, affected, changes, "remove_duplicates")

    # â”€â”€ Step 4: Standardise dates â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if request.standardize_dates:
        df_new, affected, changes = _step_standardise_dates(df)
        df = _record_step("date_standardise", "Date Standardisation", "consistency",
                          df_new, affected, changes, "date_standardise")

    # â”€â”€ Step 5: Fix invalid emails â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if getattr(request, "fix_emails", True):
        df_new, affected, changes = _step_fix_emails(df)
        df = _record_step("email_fix", "Email Validation & Repair", "validity",
                          df_new, affected, changes, "email_fix")

    # â”€â”€ Step 6: Country synonym normalisation â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if getattr(request, "normalize_countries", True):
        df_new, affected, changes = _step_normalise_countries(df)
        df = _record_step("country_normalise", "Country Synonym Normalisation", "consistency",
                          df_new, affected, changes, "country_normalise")

    # â”€â”€ Step 7: Text whitespace + casing â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    case = request.normalize_case or None
    if case or True:   # always trim; casing only if specified
        df_new, affected, changes = _step_normalise_text(df, case, trim=True)
        if affected > 0:
            df = _record_step("text_normalise", "Text Normalisation", "consistency",
                              df_new, affected, changes, "text_normalise")
        else:
            df = df_new

    # â”€â”€ Step 8: Remove outliers (opt-in) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if request.remove_outliers:
        df_new, affected, changes = _step_remove_outliers(df)
        df = _record_step("remove_outliers", "Outlier Removal (IQR)", "validity",
                          df_new, affected, changes, "remove_outliers")

    # â”€â”€ Final score â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    score_after, grade_after = _quick_score(df)
    score_delta = round(score_after - score_before, 2)
    improvement_pct = round((score_delta / max(score_before, 1)) * 100, 1)

    # â”€â”€ Persist new version â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    rows_after, cols_after = df.shape
    new_file_path = build_versioned_path(file_path, version + 1)
    save_dataframe(df, new_file_path, file_type)

    all_changes: List[str] = []
    for s in steps:
        all_changes.extend(s.changes)
    if not all_changes:
        all_changes = ["No transformations were necessary."]

    new_version = await dataset_service.add_dataset_version(
        dataset_id=dataset_id,
        new_file_path=new_file_path,
        rows=rows_after,
        cols=cols_after,
        changes_applied=all_changes,
    )

    remaining = _remaining_issues(df)

    report = CleaningReport(
        dataset_id=dataset_id,
        original_version=version,
        new_version=new_version,
        rows_before=rows_before,
        rows_after=rows_after,
        score_before=round(score_before, 2),
        score_after=round(score_after, 2),
        score_delta=score_delta,
        improvement_pct=improvement_pct,
        grade_before=grade_before,
        grade_after=grade_after,
        steps=steps,
        remaining_issues=remaining,
        generated_at=now_ist().isoformat(),
    )

    logger.info(
        "Cleaning complete: dataset=%s v%sâ†’v%s score %.1fâ†’%.1f (+%.1f)",
        dataset_id, version, new_version,
        score_before, score_after, score_delta,
    )

    return _report_to_dict(report)


# ── Serialiser ────────────────────────────────────────────────────────────────

def _report_to_dict(r: CleaningReport) -> Dict[str, Any]:
    return {
        "dataset_id": r.dataset_id,
        "original_version": r.original_version,
        "new_version": r.new_version,
        "rows_before": r.rows_before,
        "rows_after": r.rows_after,
        "score_before": r.score_before,
        "score_after": r.score_after,
        "score_delta": r.score_delta,
        "improvement_pct": r.improvement_pct,
        "grade_before": r.grade_before,
        "grade_after": r.grade_after,
        "fixes_applied": [c for s in r.steps for c in s.changes] or ["No transformations were necessary."],
        "steps": [
            {
                "name": s.name,
                "dimension": s.dimension,
                "rows_affected": s.rows_affected,
                "score_before": s.score_before,
                "score_after": s.score_after,
                "score_gain": s.score_gain,
                "confidence": s.confidence,
                "description": s.description,
                "changes": s.changes,
            }
            for s in r.steps
        ],
        "remaining_issues": r.remaining_issues,
        "generated_at": r.generated_at,
    }
