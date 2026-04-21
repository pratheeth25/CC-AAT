"""
Alert Service.

Stores and retrieves quality alerts in MongoDB.
Alerts are triggered by:
  - High missing-value percentage (>20 % in any column)
  - Anomalies detected
  - Quality score degradation (score < 60)
  - Data drift detected in any column
"""

import logging
from typing import Any, Dict, List

from app.utils.time_utils import now_ist

from bson import ObjectId

from app.database import get_database

logger = logging.getLogger(__name__)

COLLECTION = "alerts"

# Thresholds
_HIGH_MISSING_THRESHOLD_PCT = 20.0
_QUALITY_DEGRADED_THRESHOLD = 60.0


# ---------------------------------------------------------------------------
# Trigger helpers (called from route handlers after analysis)
# ---------------------------------------------------------------------------

async def trigger_missing_value_alerts(
    dataset_id: str, dataset_name: str, profile: Dict[str, Any]
) -> List[str]:
    """Create an alert for each column exceeding the missing-value threshold."""
    created_ids: List[str] = []
    for col, stats in profile["missing_values"].items():
        if stats["percentage"] >= _HIGH_MISSING_THRESHOLD_PCT:
            severity = "critical" if stats["percentage"] >= 50 else "high"
            alert_id = await _create_alert(
                dataset_id=dataset_id,
                dataset_name=dataset_name,
                alert_type="high_missing",
                severity=severity,
                message=(
                    f"Column '{col}' has {stats['percentage']}% missing values "
                    f"({stats['count']} cells)."
                ),
                details={"column": col, **stats},
            )
            created_ids.append(alert_id)
    return created_ids


async def trigger_anomaly_alerts(
    dataset_id: str, dataset_name: str, anomaly_results: List[Dict[str, Any]]
) -> List[str]:
    """Create a single summarised alert when anomalies are found."""
    if not anomaly_results:
        return []

    total = sum(r.get("anomaly_count", 0) for r in anomaly_results)
    affected_cols = [r["column"] for r in anomaly_results]
    severity = "critical" if total > 100 else ("high" if total > 20 else "medium")

    alert_id = await _create_alert(
        dataset_id=dataset_id,
        dataset_name=dataset_name,
        alert_type="anomaly_detected",
        severity=severity,
        message=(
            f"Detected {total} anomalous value(s) across "
            f"{len(affected_cols)} column(s): {', '.join(affected_cols[:5])}."
        ),
        details={
            "total_anomalies": total,
            "affected_columns": affected_cols,
        },
    )
    return [alert_id]


async def trigger_quality_alert(
    dataset_id: str, dataset_name: str, quality: Dict[str, Any]
) -> List[str]:
    """Create an alert when overall quality score is below threshold."""
    score = quality.get("total_score", 100)
    if score >= _QUALITY_DEGRADED_THRESHOLD:
        return []

    severity = "critical" if score < 40 else "high"
    alert_id = await _create_alert(
        dataset_id=dataset_id,
        dataset_name=dataset_name,
        alert_type="quality_degraded",
        severity=severity,
        message=f"Dataset quality score is {score}/100 (grade: {quality.get('grade', 'F')}).",
        details={"total_score": score, "grade": quality.get("grade")},
    )
    return [alert_id]


async def trigger_drift_alert(
    dataset_id: str,
    dataset_name: str,
    drift: Dict[str, Any],
) -> List[str]:
    """Create an alert when data drift is detected."""
    drifted = drift.get("drifted_columns", [])
    if not drifted:
        return []

    alert_id = await _create_alert(
        dataset_id=dataset_id,
        dataset_name=dataset_name,
        alert_type="drift_detected",
        severity="high",
        message=(
            f"Data drift detected in {len(drifted)} column(s) between "
            f"v{drift['version_a']} and v{drift['version_b']}."
        ),
        details={
            "version_a": drift["version_a"],
            "version_b": drift["version_b"],
            "drifted_columns": [d["column"] for d in drifted],
        },
    )
    return [alert_id]


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------

async def get_all_alerts(dataset_id: str | None = None) -> List[Dict[str, Any]]:
    db = get_database()
    query: Dict[str, Any] = {}
    if dataset_id:
        query["dataset_id"] = dataset_id
    else:
        # Filter out orphaned alerts whose dataset no longer exists
        active_oids = await db["datasets"].distinct("_id")
        active_ids = [str(oid) for oid in active_oids]
        query["dataset_id"] = {"$in": active_ids}
    cursor = db[COLLECTION].find(query).sort("triggered_at", -1)
    return [_serialise(doc) async for doc in cursor]


async def mark_alert_read(alert_id: str) -> bool:
    db = get_database()
    result = await db[COLLECTION].update_one(
        {"_id": ObjectId(alert_id)}, {"$set": {"is_read": True}}
    )
    return result.modified_count > 0


async def delete_dataset_alerts(dataset_id: str) -> int:
    """Delete all alerts belonging to a dataset. Returns the count removed."""
    db = get_database()
    result = await db[COLLECTION].delete_many({"dataset_id": dataset_id})
    logger.info("Deleted %d alert(s) for dataset %s", result.deleted_count, dataset_id)
    return result.deleted_count


# ---------------------------------------------------------------------------
# Internal
# ---------------------------------------------------------------------------

async def _create_alert(
    dataset_id: str,
    dataset_name: str,
    alert_type: str,
    severity: str,
    message: str,
    details: Dict[str, Any],
) -> str:
    doc = {
        "dataset_id": dataset_id,
        "dataset_name": dataset_name,
        "alert_type": alert_type,
        "severity": severity,
        "message": message,
        "details": details,
        "triggered_at": now_ist(),
        "is_read": False,
    }
    db = get_database()
    result = await db[COLLECTION].insert_one(doc)
    logger.info("Alert created: type=%s dataset=%s id=%s", alert_type, dataset_id, result.inserted_id)
    return str(result.inserted_id)


def _serialise(doc: Dict[str, Any]) -> Dict[str, Any]:
    doc = dict(doc)
    if "_id" in doc and isinstance(doc["_id"], ObjectId):
        doc["_id"] = str(doc["_id"])
    return doc
