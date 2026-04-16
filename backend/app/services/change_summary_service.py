"""
Data Change Summary Service.

Compares two dataset versions and produces a deterministic, human-readable
change summary without statistical tests (no KS-test, no chi-squared).

Output format:
  {
    "columns": { "added": [...], "removed": [...], "type_changes": [...] },
    "rows": { "change": int, "percent_change": float },
    "missing_changes": { "<col>": { "before": int, "after": int, "delta": int } },
    "distribution_shift": { "<col>": { "mean_before": ..., "mean_after": ..., ... } },
    "categorical_changes": [ { "column": ..., "added_values": [...], "removed_values": [...] } ]
  }
"""

import logging
from typing import Any, Dict, List

from app.utils.time_utils import now_ist
import numpy as np
import pandas as pd

from app.services.profiling_service import _NULL_PATTERNS
from app.utils.file_utils import load_dataframe

logger = logging.getLogger(__name__)


def compute_change_summary(
    file_path_a: str,
    file_type_a: str,
    version_a: int,
    file_path_b: str,
    file_type_b: str,
    version_b: int,
    dataset_id: str,
) -> Dict[str, Any]:
    """
    Compare two dataset versions and return a structured change summary.
    """
    df_a = _load_and_clean(file_path_a, file_type_a)
    df_b = _load_and_clean(file_path_b, file_type_b)

    cols_a = set(df_a.columns)
    cols_b = set(df_b.columns)

    # ── Column changes ──
    added_cols = sorted(cols_b - cols_a)
    removed_cols = sorted(cols_a - cols_b)
    common_cols = sorted(cols_a & cols_b)

    type_changes: List[Dict[str, Any]] = []
    for col in common_cols:
        dtype_a = str(df_a[col].dtype)
        dtype_b = str(df_b[col].dtype)
        if dtype_a != dtype_b:
            type_changes.append({
                "column": col,
                "before": dtype_a,
                "after": dtype_b,
            })

    # ── Row changes ──
    rows_a = len(df_a)
    rows_b = len(df_b)
    row_change = rows_b - rows_a
    percent_change = round((row_change / rows_a * 100) if rows_a > 0 else 0.0, 2)

    # ── Missing value changes ──
    missing_changes: Dict[str, Any] = {}
    for col in common_cols:
        miss_a = int(df_a[col].isna().sum())
        miss_b = int(df_b[col].isna().sum())
        if miss_a != miss_b:
            missing_changes[col] = {
                "before": miss_a,
                "after": miss_b,
                "delta": miss_b - miss_a,
            }

    # ── Distribution shift (numerical columns) ──
    distribution_shift: Dict[str, Any] = {}
    for col in common_cols:
        if pd.api.types.is_numeric_dtype(df_a[col]) and pd.api.types.is_numeric_dtype(df_b[col]):
            clean_a = df_a[col].dropna()
            clean_b = df_b[col].dropna()
            if len(clean_a) < 2 or len(clean_b) < 2:
                continue
            distribution_shift[col] = {
                "mean_before": round(float(clean_a.mean()), 4),
                "mean_after": round(float(clean_b.mean()), 4),
                "mean_delta": round(float(clean_b.mean() - clean_a.mean()), 4),
                "std_before": round(float(clean_a.std()), 4),
                "std_after": round(float(clean_b.std()), 4),
                "min_before": round(float(clean_a.min()), 4),
                "min_after": round(float(clean_b.min()), 4),
                "max_before": round(float(clean_a.max()), 4),
                "max_after": round(float(clean_b.max()), 4),
            }

    # ── Categorical changes ──
    categorical_changes: List[Dict[str, Any]] = []
    for col in common_cols:
        if not pd.api.types.is_numeric_dtype(df_a[col]) and not pd.api.types.is_numeric_dtype(df_b[col]):
            vals_a = set(df_a[col].dropna().astype(str).unique())
            vals_b = set(df_b[col].dropna().astype(str).unique())
            added_vals = sorted(vals_b - vals_a)
            removed_vals = sorted(vals_a - vals_b)
            if added_vals or removed_vals:
                categorical_changes.append({
                    "column": col,
                    "added_values": added_vals[:20],
                    "removed_values": removed_vals[:20],
                })

    return {
        "dataset_id": dataset_id,
        "version_a": version_a,
        "version_b": version_b,
        "columns": {
            "added": added_cols,
            "removed": removed_cols,
            "type_changes": type_changes,
        },
        "rows": {
            "before": rows_a,
            "after": rows_b,
            "change": row_change,
            "percent_change": percent_change,
        },
        "missing_changes": missing_changes,
        "distribution_shift": distribution_shift,
        "categorical_changes": categorical_changes,
        "generated_at": now_ist().isoformat(),
    }


def _load_and_clean(file_path: str, file_type: str) -> pd.DataFrame:
    df = load_dataframe(file_path, file_type)
    return df.replace(list(_NULL_PATTERNS), np.nan)

