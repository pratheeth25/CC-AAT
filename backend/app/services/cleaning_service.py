"""
Automated Data Cleaning Pipeline.

Applies user-requested fixes to a dataset version, persists the cleaned file,
registers a new version in MongoDB, and returns a cleaning summary.

Supported transformations
-------------------------
- Fill missing numerics   (mean / median)
- Fill missing categoricals (mode)
- Remove exact duplicate rows
- Standardise date columns to ISO 8601
- Normalise categorical casing  (lower / upper / title)
- Remove IQR-detected outliers  (optional, conservative)
"""

import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from app.utils.time_utils import now_ist

import numpy as np
import pandas as pd

from app.models.dataset import CleaningRequest
from app.services import dataset_service
from app.services.profiling_service import _NULL_PATTERNS
from app.utils.file_utils import build_versioned_path, load_dataframe, save_dataframe

logger = logging.getLogger(__name__)


async def apply_cleaning(
    dataset_id: str,
    request: CleaningRequest,
) -> Dict[str, Any]:
    """
    Load the requested version, apply fixes, store as a new version, and
    update MongoDB.

    Returns a cleaning summary dict.
    """
    file_path, file_type, version = await dataset_service.resolve_version_path(
        dataset_id, request.version
    )

    df = load_dataframe(file_path, file_type)
    rows_before = len(df)

    # Replace smart-null strings with NaN before any fix
    df = df.replace(list(_NULL_PATTERNS), np.nan)

    fixes_applied: List[str] = []

    # ------------------------------------------------------------------
    # 1. Remove exact duplicate rows
    # ------------------------------------------------------------------
    if request.fix_duplicates:
        before = len(df)
        df = df.drop_duplicates()
        removed = before - len(df)
        if removed > 0:
            fixes_applied.append(f"Removed {removed} exact duplicate rows.")

    # ------------------------------------------------------------------
    # 2. Fill missing numeric values
    # ------------------------------------------------------------------
    if request.fix_missing_numeric:
        for col in df.select_dtypes(include=[np.number]).columns:
            missing = df[col].isna().sum()
            if missing > 0:
                skew = df[col].dropna().skew()
                if abs(skew) > 1.0:
                    fill_val = df[col].median()
                    strategy = "median"
                else:
                    fill_val = df[col].mean()
                    strategy = "mean"
                df[col] = df[col].fillna(fill_val)
                fixes_applied.append(
                    f"Filled {missing} missing values in '{col}' with {strategy} ({round(float(fill_val), 4)})."
                )

    # ------------------------------------------------------------------
    # 3. Fill missing categorical values
    # ------------------------------------------------------------------
    if request.fix_missing_categorical:
        for col in df.select_dtypes(include=["object", "category"]).columns:
            missing = df[col].isna().sum()
            if missing > 0:
                mode_vals = df[col].dropna().mode()
                if len(mode_vals) > 0:
                    mode_val = mode_vals.iloc[0]
                    df[col] = df[col].fillna(mode_val)
                    fixes_applied.append(
                        f"Filled {missing} missing values in '{col}' with mode ('{mode_val}')."
                    )

    # ------------------------------------------------------------------
    # 4. Standardise date columns to ISO 8601
    # ------------------------------------------------------------------
    if request.standardize_dates:
        for col in df.select_dtypes(include=["object"]).columns:
            converted, fmt = _try_parse_dates(df[col])
            if converted is not None and fmt is not None and fmt != "%Y-%m-%d":
                df[col] = converted.dt.strftime("%Y-%m-%d")
                fixes_applied.append(
                    f"Standardised date column '{col}' from '{fmt}' to ISO 8601."
                )

    # ------------------------------------------------------------------
    # 5. Normalise casing for string columns
    # ------------------------------------------------------------------
    if request.normalize_case:
        case_fn = {
            "lower": str.lower,
            "upper": str.upper,
            "title": str.title,
        }.get(request.normalize_case.lower())

        if case_fn:
            for col in df.select_dtypes(include=["object"]).columns:
                df[col] = df[col].apply(
                    lambda v: case_fn(v) if isinstance(v, str) else v
                )
            fixes_applied.append(
                f"Applied '{request.normalize_case}' casing to all string columns."
            )

    # ------------------------------------------------------------------
    # 6. Remove numeric outliers (IQR) — conservative, optional
    # ------------------------------------------------------------------
    if request.remove_outliers:
        for col in df.select_dtypes(include=[np.number]).columns:
            q1 = df[col].quantile(0.25)
            q3 = df[col].quantile(0.75)
            iqr = q3 - q1
            lower = q1 - 1.5 * iqr
            upper = q3 + 1.5 * iqr
            mask = df[col].between(lower, upper, inclusive="both") | df[col].isna()
            removed = (~mask).sum()
            if removed > 0:
                df = df[mask]
                fixes_applied.append(
                    f"Removed {removed} IQR outlier rows in column '{col}'."
                )

    if not fixes_applied:
        fixes_applied.append("No transformations were necessary.")

    # ------------------------------------------------------------------
    # Persist new version
    # ------------------------------------------------------------------
    rows_after, cols_after = df.shape
    new_file_path = build_versioned_path(file_path, version + 1)
    save_dataframe(df, new_file_path, file_type)

    new_version = await dataset_service.add_dataset_version(
        dataset_id=dataset_id,
        new_file_path=new_file_path,
        rows=rows_after,
        cols=cols_after,
        changes_applied=fixes_applied,
    )

    logger.info(
        "Cleaning complete: dataset=%s old_version=%s new_version=%s fixes=%d",
        dataset_id,
        version,
        new_version,
        len(fixes_applied),
    )

    return {
        "dataset_id": dataset_id,
        "original_version": version,
        "new_version": new_version,
        "fixes_applied": fixes_applied,
        "rows_before": rows_before,
        "rows_after": rows_after,
        "generated_at": now_ist(),
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DATE_FORMATS = [
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%d-%m-%Y",
    "%m-%d-%Y",
    "%Y/%m/%d",
]


def _try_parse_dates(series: pd.Series) -> Tuple[Optional[pd.Series], Optional[str]]:
    """
    Attempt to parse a string Series as dates.
    Returns (parsed_series, detected_format) or (None, None).
    """
    sample = series.dropna().astype(str).head(30)
    if len(sample) == 0:
        return None, None

    for fmt in _DATE_FORMATS:
        match_count = sum(
            1
            for v in sample
            if _safe_strptime(v, fmt)
        )
        if match_count / len(sample) >= 0.7:
            try:
                parsed = pd.to_datetime(series, format=fmt, errors="coerce")
                if parsed.notna().sum() / max(len(series), 1) >= 0.7:
                    return parsed, fmt
            except Exception:
                pass

    return None, None


def _safe_strptime(val: str, fmt: str) -> bool:
    try:
        datetime.strptime(val.strip(), fmt)
        return True
    except (ValueError, TypeError):
        return False

