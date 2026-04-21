"""
Data profiling service.

Produces a rich profile of a DataFrame including:
  - Shape
  - Smart missing-value detection (catches "", NULL, N/A, ?, etc.)
  - Exact and logical duplicate counting
  - Per-column statistics (dtype, cardinality, numeric quartiles, top-n categoricals)
"""

import logging
import warnings
from typing import Any, Dict, List

import numpy as np
import pandas as pd

# Opt-in to the pandas 3.x downcasting behaviour now to avoid FutureWarnings.
pd.set_option("future.no_silent_downcasting", True)

logger = logging.getLogger(__name__)

# Patterns treated as "missing" in addition to NaN / None
_NULL_PATTERNS: set = {
    "",
    "null",
    "NULL",
    "Null",
    "none",
    "None",
    "NONE",
    "n/a",
    "N/A",
    "NA",
    "na",
    "NaN",
    "nan",
    "NAN",
    "?",
    "-",
    "--",
    "undefined",
    "UNDEFINED",
}

# Columns whose names suggest they are ID / surrogate key columns
_ID_COLUMN_HINTS = {"id", "_id", "uuid", "index", "row_id", "record_id", "pk", "key"}


def profile_dataframe(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Return a comprehensive profile dictionary for *df*.

    Keys
    ----
    shape          : {rows, columns}
    missing_values : per-column {count, percentage}
    duplicates     : exact and logical duplicate counts
    columns        : per-column stats dict
    """
    rows, cols = df.shape

    # Replace smart-null strings with NaN for analysis
    df_clean = df.replace(list(_NULL_PATTERNS), np.nan).infer_objects(copy=False)

    missing_values = _analyse_missing(df_clean, rows)
    duplicates = _analyse_duplicates(df, df_clean, rows)
    column_stats = _analyse_columns(df_clean, rows)

    return {
        "shape": {"rows": rows, "columns": cols},
        "missing_values": missing_values,
        "duplicates": duplicates,
        "columns": column_stats,
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _analyse_missing(df: pd.DataFrame, rows: int) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for col in df.columns:
        count = int(df[col].isna().sum())
        pct = round(count / rows * 100, 2) if rows > 0 else 0.0
        result[col] = {"count": count, "percentage": pct}
    return result


def _analyse_duplicates(
    df_raw: pd.DataFrame, df_clean: pd.DataFrame, rows: int
) -> Dict[str, Any]:
    exact = int(df_raw.duplicated().sum())

    # Logical duplicates ignore probable ID columns
    id_cols = [c for c in df_raw.columns if str(c).lower() in _ID_COLUMN_HINTS]
    non_id = [c for c in df_raw.columns if c not in id_cols]
    logical = int(df_raw.duplicated(subset=non_id).sum()) if non_id else exact

    return {
        "exact_duplicates": exact,
        "exact_duplicate_percentage": round(exact / rows * 100, 2) if rows > 0 else 0.0,
        "logical_duplicates": logical,
        "logical_duplicate_percentage": round(logical / rows * 100, 2) if rows > 0 else 0.0,
        "id_columns_excluded": id_cols,
    }


def _analyse_columns(df: pd.DataFrame, rows: int) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for col in df.columns:
        series = df[col]
        non_null = series.dropna()
        dtype_str = str(series.dtype)
        missing_count = int(series.isna().sum())

        info: Dict[str, Any] = {
            "dtype": dtype_str,
            "unique_count": int(series.nunique(dropna=True)),
            "missing_count": missing_count,
            "missing_percentage": round(missing_count / rows * 100, 2) if rows > 0 else 0.0,
            "sample_values": [_safe_val(v) for v in non_null.head(5).tolist()],
        }

        if pd.api.types.is_numeric_dtype(series) and len(non_null) > 0:
            info.update(
                {
                    "min": _safe_val(non_null.min()),
                    "max": _safe_val(non_null.max()),
                    "mean": _safe_val(non_null.mean()),
                    "median": _safe_val(non_null.median()),
                    "std": _safe_val(non_null.std()),
                    "q1": _safe_val(non_null.quantile(0.25)),
                    "q3": _safe_val(non_null.quantile(0.75)),
                    "skewness": _safe_val(non_null.skew()),
                    "kurtosis": _safe_val(non_null.kurt()),
                }
            )
        elif dtype_str in ("object", "category", "string") and len(non_null) > 0:
            top = non_null.value_counts().head(10).to_dict()
            info["top_values"] = {str(k): int(v) for k, v in top.items()}
            info["is_potentially_categorical"] = info["unique_count"] < max(10, rows * 0.05)

        result[col] = info
    return result


def _safe_val(v: Any) -> Any:
    """Convert NumPy scalars to native Python types; map NaN/Inf to None."""
    if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
        return None
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return round(float(v), 6)
    if isinstance(v, np.bool_):
        return bool(v)
    return v
