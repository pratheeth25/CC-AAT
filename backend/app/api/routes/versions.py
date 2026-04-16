"""
Versioning routes.

GET    /dataset/{id}/versions                      — list all versions
GET    /dataset/{id}/versions/diff                 — compare two versions
GET    /dataset/{id}/versions/{v}                  — get metadata for a specific version
GET    /dataset/{id}/versions/{v}/download         — stream-download a version file
POST   /dataset/{id}/versions/{v}/restore          — restore a previous version as active
DELETE /datasets/{id}/versions/{v}                 — delete a specific version
"""

import os
from typing import Any, Dict

from fastapi import APIRouter, Query
from fastapi.responses import FileResponse, JSONResponse

from app.services import version_manager
from app.services.cache_service import analysis_cache
from app.utils.response_utils import success_response

router = APIRouter(tags=["Versioning"])


@router.get("/dataset/{dataset_id}/versions", summary="List all versions")
async def list_versions(dataset_id: str) -> Dict[str, Any]:
    versions = await version_manager.list_versions(dataset_id)
    return success_response(
        {"dataset_id": dataset_id, "versions": versions},
        message=f"Found {len(versions)} version(s).",
    )


# /diff must be registered BEFORE /{version_number} — FastAPI matches in order and
# {version_number: int} would fail on the literal string "diff" and return 422.
@router.get("/dataset/{dataset_id}/versions/diff", summary="Diff two versions")
async def diff_versions(
    dataset_id: str,
    version_a: int = Query(..., description="First version"),
    version_b: int = Query(..., description="Second version"),
) -> Dict[str, Any]:
    diff = await version_manager.diff_versions(dataset_id, version_a, version_b)
    return success_response(diff)


@router.get("/dataset/{dataset_id}/versions/{version_number}", summary="Get version details")
async def get_version(dataset_id: str, version_number: int) -> Dict[str, Any]:
    version = await version_manager.get_version(dataset_id, version_number)
    return success_response(version)


@router.get(
    "/dataset/{dataset_id}/versions/{version_number}/download",
    summary="Download a specific dataset version",
    response_class=FileResponse,
)
async def download_version(dataset_id: str, version_number: int):
    """
    Stream the raw file for a specific dataset version as an attachment.

    Supports CSV and JSON files. Returns 404 if the file has been deleted from disk.
    """
    version = await version_manager.get_version(dataset_id, version_number)
    file_path = version.get("file_path", "")

    if not file_path or not os.path.isfile(file_path):
        return JSONResponse(
            status_code=404,
            content={"status": "error", "message": f"File for version {version_number} not found on disk."},
        )

    filename = os.path.basename(file_path)
    media_type = "text/csv" if file_path.endswith(".csv") else "application/json"

    return FileResponse(
        path=file_path,
        filename=filename,
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/dataset/{dataset_id}/versions/{version_number}/restore", summary="Restore version")
async def restore_version(dataset_id: str, version_number: int) -> Dict[str, Any]:
    result = await version_manager.restore_version(dataset_id, version_number)
    analysis_cache.invalidate(dataset_id)
    return success_response(result, f"Version {version_number} restored.")


@router.delete("/datasets/{dataset_id}/versions/{version_number}", summary="Delete a dataset version")
async def delete_version(dataset_id: str, version_number: int) -> Dict[str, Any]:
    """
    Permanently delete a specific version of a dataset.

    Restrictions:
    - Cannot delete the only remaining version.
    - Cannot delete the currently active/latest version.
    """
    result = await version_manager.delete_version(dataset_id, version_number)
    analysis_cache.invalidate(dataset_id)
    return success_response(result, message=result["message"])
