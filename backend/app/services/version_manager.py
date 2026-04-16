"""
Version Manager Service.

Provides enhanced version management:
  - List all versions for a dataset
  - Get specific version metadata
  - Restore a previous version as the active/latest
  - Compute diff between two versions
"""

import hashlib
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from bson import ObjectId
from fastapi import HTTPException

from app.database import get_database
from app.services import dataset_service

logger = logging.getLogger(__name__)

COLLECTION = "datasets"


def compute_checksum(file_path: str) -> str:
    """Compute SHA-256 checksum of a file."""
    sha = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha.update(chunk)
        return sha.hexdigest()
    except Exception:
        return ""


async def list_versions(dataset_id: str) -> List[Dict[str, Any]]:
    """Return all versions for a dataset, newest first."""
    doc = await dataset_service.get_dataset_by_id(dataset_id)
    versions = doc.get("versions", [])

    result = []
    for v in sorted(versions, key=lambda x: x["version_number"], reverse=True):
        checksum = ""
        if os.path.exists(v.get("file_path", "")):
            checksum = compute_checksum(v["file_path"])

        result.append({
            "dataset_id": dataset_id,
            "version_number": v["version_number"],
            "created_at": v["created_at"],
            "source": _infer_source(v),
            "parent_version": v["version_number"] - 1 if v["version_number"] > 1 else None,
            "file_path": v["file_path"],
            "row_count": v.get("row_count", 0),
            "col_count": v.get("col_count", 0),
            "checksum": checksum,
            "changes_applied": v.get("changes_applied", []),
            "is_latest": v["version_number"] == doc["current_version"],
        })

    return result


async def get_version(dataset_id: str, version_number: int) -> Dict[str, Any]:
    """Get metadata for a specific version."""
    doc = await dataset_service.get_dataset_by_id(dataset_id)
    ver = next(
        (v for v in doc.get("versions", []) if v["version_number"] == version_number),
        None,
    )
    if ver is None:
        raise HTTPException(
            status_code=404,
            detail=f"Version {version_number} not found for dataset '{dataset_id}'.",
        )

    checksum = ""
    if os.path.exists(ver.get("file_path", "")):
        checksum = compute_checksum(ver["file_path"])

    return {
        "dataset_id": dataset_id,
        "version_number": ver["version_number"],
        "created_at": ver["created_at"],
        "source": _infer_source(ver),
        "parent_version": ver["version_number"] - 1 if ver["version_number"] > 1 else None,
        "file_path": ver["file_path"],
        "row_count": ver.get("row_count", 0),
        "col_count": ver.get("col_count", 0),
        "checksum": checksum,
        "changes_applied": ver.get("changes_applied", []),
        "is_latest": ver["version_number"] == doc["current_version"],
    }


async def restore_version(dataset_id: str, version_number: int) -> Dict[str, Any]:
    """
    Set a previous version as the current active version.
    Does NOT create a new version — just points current_version to the restored one.
    """
    doc = await dataset_service.get_dataset_by_id(dataset_id)
    ver = next(
        (v for v in doc.get("versions", []) if v["version_number"] == version_number),
        None,
    )
    if ver is None:
        raise HTTPException(
            status_code=404,
            detail=f"Version {version_number} not found for dataset '{dataset_id}'.",
        )

    if not os.path.exists(ver.get("file_path", "")):
        raise HTTPException(
            status_code=410,
            detail=f"File for version {version_number} no longer exists on disk.",
        )

    db = get_database()
    await db[COLLECTION].update_one(
        {"_id": ObjectId(dataset_id)},
        {
            "$set": {
                "current_version": version_number,
                "row_count": ver.get("row_count", doc.get("row_count")),
                "col_count": ver.get("col_count", doc.get("col_count")),
            },
        },
    )

    logger.info("Restored dataset %s to version %s", dataset_id, version_number)

    return {
        "dataset_id": dataset_id,
        "restored_version": version_number,
        "row_count": ver.get("row_count", 0),
        "col_count": ver.get("col_count", 0),
        "message": f"Version {version_number} is now the active version.",
    }


async def diff_versions(
    dataset_id: str, version_a: int, version_b: int
) -> Dict[str, Any]:
    """
    Compute a lightweight diff between two versions:
      - Row count delta
      - Column count delta
      - Columns added / removed
      - Changes applied in version_b
    """
    doc = await dataset_service.get_dataset_by_id(dataset_id)
    versions_map = {v["version_number"]: v for v in doc.get("versions", [])}

    va = versions_map.get(version_a)
    vb = versions_map.get(version_b)

    if va is None:
        raise HTTPException(status_code=404, detail=f"Version {version_a} not found.")
    if vb is None:
        raise HTTPException(status_code=404, detail=f"Version {version_b} not found.")

    # Try to load column lists for diff
    cols_a = _get_columns(va)
    cols_b = _get_columns(vb)

    added_cols = sorted(set(cols_b) - set(cols_a)) if cols_a and cols_b else []
    removed_cols = sorted(set(cols_a) - set(cols_b)) if cols_a and cols_b else []

    return {
        "dataset_id": dataset_id,
        "version_a": version_a,
        "version_b": version_b,
        "row_count_a": va.get("row_count", 0),
        "row_count_b": vb.get("row_count", 0),
        "row_delta": vb.get("row_count", 0) - va.get("row_count", 0),
        "col_count_a": va.get("col_count", 0),
        "col_count_b": vb.get("col_count", 0),
        "col_delta": vb.get("col_count", 0) - va.get("col_count", 0),
        "columns_added": added_cols,
        "columns_removed": removed_cols,
        "changes_applied_in_b": vb.get("changes_applied", []),
    }


async def delete_version(dataset_id: str, version_number: int) -> Dict[str, Any]:
    """
    Delete a specific version of a dataset.

    Rules:
    - Cannot delete the only remaining version.
    - Cannot delete the current/latest active version (block deletion).
    - Removes the file from disk and the version entry from the DB.
    """
    doc = await dataset_service.get_dataset_by_id(dataset_id)
    versions: List[Dict[str, Any]] = doc.get("versions", [])
    total_versions = len(versions)

    if total_versions <= 1:
        raise HTTPException(
            status_code=400,
            detail="Cannot delete the only version of a dataset.",
        )

    ver = next((v for v in versions if v["version_number"] == version_number), None)
    if ver is None:
        raise HTTPException(
            status_code=404,
            detail=f"Version {version_number} not found for dataset '{dataset_id}'.",
        )

    current_version = doc.get("current_version")
    if version_number == current_version:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Cannot delete the active/latest version ({version_number}). "
                "Restore a different version first, then delete this one."
            ),
        )

    # Remove file from disk
    file_path = ver.get("file_path", "")
    if file_path and os.path.exists(file_path):
        try:
            os.remove(file_path)
            logger.info("Deleted file for dataset %s version %s: %s", dataset_id, version_number, file_path)
        except OSError as exc:
            logger.warning("Could not delete file %s: %s", file_path, exc)

    # Remove version entry from DB
    db = get_database()
    await db[COLLECTION].update_one(
        {"_id": ObjectId(dataset_id)},
        {"$pull": {"versions": {"version_number": version_number}}},
    )

    remaining_versions = total_versions - 1
    logger.info("Deleted version %s from dataset %s. Remaining: %s", version_number, dataset_id, remaining_versions)

    return {
        "status": "success",
        "message": f"Version {version_number} deleted successfully.",
        "remaining_versions": remaining_versions,
    }


# ── Helpers ────────────────────────────────────────────────────────────────────

def _infer_source(version: Dict[str, Any]) -> str:
    """Infer version source from changes_applied."""
    changes = version.get("changes_applied", [])
    if not changes:
        return "upload"
    return "cleaning"


def _get_columns(version: Dict[str, Any]) -> List[str]:
    """Try to read column names from the version's file."""
    file_path = version.get("file_path", "")
    if not file_path or not os.path.exists(file_path):
        return []
    try:
        import pandas as pd
        if file_path.endswith(".csv"):
            df = pd.read_csv(file_path, nrows=0)
        elif file_path.endswith(".json"):
            df = pd.read_json(file_path).head(0)
        else:
            return []
        return list(df.columns)
    except Exception:
        return []
