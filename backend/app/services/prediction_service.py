"""
Predictive Quality Degradation Service.

Uses historical quality scores from dataset versions to predict future
quality trends via simple linear regression.

Returns past scores, predicted future scores, breach risk, and estimated
point at which score will drop below the SLA threshold.
"""

import logging
from typing import Any, Dict, List, Optional

import numpy as np

logger = logging.getLogger(__name__)

_DEFAULT_SLA_THRESHOLD = 70  # minimum acceptable quality score
_FORECAST_POINTS = 3  # how many future versions to predict


def predict_degradation(
    version_scores: List[Dict[str, Any]],
    sla_threshold: int = _DEFAULT_SLA_THRESHOLD,
    forecast_points: int = _FORECAST_POINTS,
) -> Dict[str, Any]:
    """
    Predict quality score trajectory for a dataset.

    Parameters
    ----------
    version_scores : list of {"version": int, "score": float}
        Historical quality scores ordered by version number.
    sla_threshold  : minimum acceptable quality score.
    forecast_points: number of future versions to predict.

    Returns
    -------
    {
      "historical": [{"version": 1, "score": 85.0}, ...],
      "predicted":  [{"version": 4, "score": 78.2}, ...],
      "trend": "declining" | "improving" | "stable",
      "slope": -2.3,
      "sla_threshold": 70,
      "breach_risk": "high" | "medium" | "low" | "none",
      "estimated_breach_version": 7 | null,
      "current_score": 80.0,
      "message": "..."
    }
    """
    if not version_scores:
        return _empty_result(sla_threshold)

    versions = np.array([s["version"] for s in version_scores], dtype=float)
    scores = np.array([s["score"] for s in version_scores], dtype=float)

    current_score = float(scores[-1])

    # Need at least 2 points for a trend
    if len(versions) < 2:
        return {
            "historical": version_scores,
            "predicted": [],
            "trend": "stable",
            "slope": 0.0,
            "sla_threshold": sla_threshold,
            "breach_risk": "none" if current_score >= sla_threshold else "high",
            "estimated_breach_version": None,
            "current_score": current_score,
            "message": "Need at least 2 versions for trend prediction.",
        }

    # Simple linear regression: score = slope * version + intercept
    slope, intercept = np.polyfit(versions, scores, 1)
    slope = float(slope)
    intercept = float(intercept)

    # Trend classification
    if slope < -1.0:
        trend = "declining"
    elif slope > 1.0:
        trend = "improving"
    else:
        trend = "stable"

    # Forecast
    last_version = int(versions[-1])
    predicted: List[Dict[str, Any]] = []
    for i in range(1, forecast_points + 1):
        v = last_version + i
        predicted_score = slope * v + intercept
        predicted_score = max(0.0, min(100.0, predicted_score))
        predicted.append({
            "version": v,
            "score": round(predicted_score, 1),
        })

    # Breach estimation
    estimated_breach_version: Optional[int] = None
    if slope < 0 and current_score > sla_threshold:
        # score = slope * v + intercept = sla_threshold
        breach_v = (sla_threshold - intercept) / slope
        if breach_v > last_version:
            estimated_breach_version = int(np.ceil(breach_v))

    # Breach risk
    if current_score < sla_threshold:
        breach_risk = "high"
    elif estimated_breach_version is not None:
        gap = estimated_breach_version - last_version
        if gap <= 2:
            breach_risk = "high"
        elif gap <= 5:
            breach_risk = "medium"
        else:
            breach_risk = "low"
    elif trend == "declining":
        breach_risk = "medium"
    else:
        breach_risk = "none"

    # Message
    if breach_risk == "high" and estimated_breach_version:
        message = (
            f"Quality is declining at {abs(slope):.1f} pts/version. "
            f"Predicted to breach SLA ({sla_threshold}) by version {estimated_breach_version}."
        )
    elif breach_risk == "high":
        message = f"Quality score ({current_score:.0f}) is already below the SLA threshold ({sla_threshold})."
    elif breach_risk == "medium":
        message = f"Quality is trending downward ({slope:+.1f} pts/version). Monitor closely."
    elif trend == "improving":
        message = f"Quality is improving at {slope:+.1f} pts/version."
    else:
        message = "Quality is stable. No degradation risk detected."

    return {
        "historical": version_scores,
        "predicted": predicted,
        "trend": trend,
        "slope": round(slope, 2),
        "sla_threshold": sla_threshold,
        "breach_risk": breach_risk,
        "estimated_breach_version": estimated_breach_version,
        "current_score": round(current_score, 1),
        "message": message,
    }


def _empty_result(sla_threshold: int) -> Dict[str, Any]:
    return {
        "historical": [],
        "predicted": [],
        "trend": "stable",
        "slope": 0.0,
        "sla_threshold": sla_threshold,
        "breach_risk": "none",
        "estimated_breach_version": None,
        "current_score": None,
        "message": "No version history available for prediction.",
    }
