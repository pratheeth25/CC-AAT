"""
Data Drift Detection Service.

Compares two dataset versions column-by-column and flags statistical drift.

Methods
-------
- Numerical columns : Kolmogorov-Smirnov two-sample test + mean shift
- Categorical columns: Chi-squared test comparing frequency distributions
- Distribution summary: min, max, mean changes (numerical)

A column is classified as *drifted* when its test p-value < 0.05.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from scipy import stats

from app.utils.time_utils import now_ist

from app.services.profiling_service import _NULL_PATTERNS
from app.utils.file_utils import load_dataframe

logger = logging.getLogger(__name__)

_P_VALUE_THRESHOLD = 0.05


def detect_drift(
    file_path_a: str,
    file_type_a: str,
    version_a: int,
    file_path_b: str,
    file_type_b: str,
    version_b: int,
    dataset_id: str,
) -> Dict[str, Any]:
    """
    Compare two dataset versions and return a drift report.

    Returns
    -------
    dict matching DriftResponse model.
    """
    df_a = _load_and_clean(file_path_a, file_type_a)
    df_b = _load_and_clean(file_path_b, file_type_b)

    common_cols = [c for c in df_a.columns if c in df_b.columns]

    drifted: List[Dict[str, Any]] = []
    stable: List[str] = []

    for col in common_cols:
        series_a = df_a[col].dropna()
        series_b = df_b[col].dropna()

        if len(series_a) < 5 or len(series_b) < 5:
            # Not enough data to test
            continue

        if pd.api.types.is_numeric_dtype(series_a) and pd.api.types.is_numeric_dtype(series_b):
            result = _test_numerical_drift(col, series_a, series_b)
        else:
            result = _test_categorical_drift(col, series_a.astype(str), series_b.astype(str))

        if result["is_drifted"]:
            drifted.append(result)
        else:
            stable.append(result)

    return {
        "dataset_id": dataset_id,
        "version_a": version_a,
        "version_b": version_b,
        "drifted_columns": [_strip_internal(d) for d in drifted],
        "stable_columns": [_strip_internal(s) for s in stable],
        "total_columns_compared": len(common_cols),
        "generated_at": now_ist(),
    }


# ---------------------------------------------------------------------------
# Statistical tests
# ---------------------------------------------------------------------------

def _test_numerical_drift(
    col: str, a: pd.Series, b: pd.Series
) -> Dict[str, Any]:
    ks_stat, p_value = stats.ks_2samp(a.values, b.values)
    mean_shift = float(b.mean() - a.mean())
    std_shift = float(b.std() - a.std())
    is_drifted = bool(p_value < _P_VALUE_THRESHOLD)

    return {
        "column": col,
        "drift_type": "numerical",
        "test": "ks_test",
        "ks_statistic": round(float(ks_stat), 6),
        "p_value": round(float(p_value), 6),
        "mean_a": round(float(a.mean()), 4),
        "mean_b": round(float(b.mean()), 4),
        "mean_shift": round(mean_shift, 4),
        "std_shift": round(std_shift, 4),
        "interpretation": (
            f"Significant distribution change detected (KS={ks_stat:.4f}, p={p_value:.4f}). "
            f"Mean shifted by {mean_shift:+.4f}."
        )
        if is_drifted
        else f"Stable (p={p_value:.4f}).",
        "is_drifted": is_drifted,
    }


def _test_categorical_drift(
    col: str, a: pd.Series, b: pd.Series
) -> Dict[str, Any]:
    """
    Align frequency distributions of both series and run chi-squared test.
    """
    cats = list(set(a.unique()) | set(b.unique()))

    freq_a = _category_frequencies(a, cats)
    freq_b = _category_frequencies(b, cats)

    # chi2 requires non-zero expected; add small epsilon
    expected = np.array(freq_b, dtype=float) + 1e-9
    observed = np.array(freq_a, dtype=float) + 1e-9

    chi2, p_value = stats.chisquare(observed, f_exp=expected)
    is_drifted = bool(p_value < _P_VALUE_THRESHOLD)

    return {
        "column": col,
        "drift_type": "categorical",
        "test": "chi2_test",
        "chi2_statistic": round(float(chi2), 6),
        "p_value": round(float(p_value), 6),
        "mean_shift": None,
        "interpretation": (
            f"Significant category distribution shift detected (χ²={chi2:.4f}, p={p_value:.4f})."
        )
        if is_drifted
        else f"Stable (p={p_value:.4f}).",
        "is_drifted": is_drifted,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_and_clean(file_path: str, file_type: str) -> pd.DataFrame:
    df = load_dataframe(file_path, file_type)
    return df.replace(list(_NULL_PATTERNS), np.nan)


def _category_frequencies(series: pd.Series, cats: List[str]) -> List[int]:
    counts = series.value_counts().to_dict()
    return [int(counts.get(c, 0)) for c in cats]


def _strip_internal(d: Dict[str, Any]) -> Dict[str, Any]:
    """Remove internal keys not intended for API consumers."""
    return {k: v for k, v in d.items() if k != "is_drifted"}

