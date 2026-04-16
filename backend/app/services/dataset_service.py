import logging
import os
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from app.utils.time_utils import now_ist

import aiofiles
from bson import ObjectId
from fastapi import HTTPException, UploadFile

from app.config import settings
from app.database import get_database
from app.utils.file_utils import build_versioned_path, ensure_upload_dir, load_dataframe

logger = logging.getLogger(__name__)
COLLECTION = "datasets"

ALLOWED_EXTENSIONS = {".csv", ".json"}


async def upload_dataset(file: UploadFile) -> Dict[str, Any]:
    """
    Persist an uploaded CSV/JSON to disk and record its metadata in MongoDB.
    Returns the serialised dataset document.
    """
    filename = file.filename or "unknown_file"
    ext = os.path.splitext(filename)[1].lower()

    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Only CSV and JSON files are supported. Received: '{ext}'",
        )

    file_type = ext[1:]  # "csv" or "json"
    ensure_upload_dir()

    # Read bytes up-front so we can check the size before writing
    content = await file.read()
    size_bytes = len(content)
    max_bytes = settings.MAX_UPLOAD_SIZE_MB * 1024 * 1024
    if size_bytes > max_bytes:
        raise HTTPException(
            status_code=413,
            detail=f"File exceeds maximum allowed size of {settings.MAX_UPLOAD_SIZE_MB} MB.",
        )

    unique_name = f"{uuid.uuid4().hex}_{filename}"
    file_path = os.path.join(settings.UPLOAD_DIR, unique_name)

    async with aiofiles.open(file_path, "wb") as f:
        await f.write(content)

    # Validate the file is parseable and capture its shape
    try:
        df = load_dataframe(file_path, file_type)
        rows, cols = df.shape
    except Exception as exc:
        os.remove(file_path)
        raise HTTPException(
            status_code=422,
            detail=f"Could not parse the uploaded file: {exc}",
        ) from exc

    now = now_ist()
    dataset_doc: Dict[str, Any] = {
        "name": os.path.splitext(filename)[0],
        "original_filename": filename,
        "file_type": file_type,
        "size_bytes": size_bytes,
        "row_count": rows,
        "col_count": cols,
        "uploaded_at": now,
        "current_version": 1,
        "versions": [
            {
                "version_number": 1,
                "file_path": file_path,
                "created_at": now,
                "changes_applied": [],
                "row_count": rows,
                "col_count": cols,
            }
        ],
    }

    db = get_database()
    result = await db[COLLECTION].insert_one(dataset_doc)
    dataset_doc["_id"] = str(result.inserted_id)

    logger.info("Dataset uploaded: id=%s name=%s", dataset_doc["_id"], dataset_doc["name"])
    return _serialise(dataset_doc)


async def upload_new_version(dataset_id: str, file: UploadFile) -> Dict[str, Any]:
    """
    Accept a new file upload and register it as a new version of an existing dataset.
    The uploaded file type must match the existing dataset's file type.
    Returns the updated serialised dataset document.
    """
    doc = await get_dataset_by_id(dataset_id)

    filename = file.filename or "unknown_file"
    ext = os.path.splitext(filename)[1].lower()

    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Only CSV and JSON files are supported. Received: '{ext}'",
        )

    file_type = ext[1:]
    if file_type != doc["file_type"]:
        raise HTTPException(
            status_code=400,
            detail=f"File type must match the existing dataset ({doc['file_type'].upper()}).",
        )

    content = await file.read()
    size_bytes = len(content)
    max_bytes = settings.MAX_UPLOAD_SIZE_MB * 1024 * 1024
    if size_bytes > max_bytes:
        raise HTTPException(
            status_code=413,
            detail=f"File exceeds the maximum allowed size of {settings.MAX_UPLOAD_SIZE_MB} MB.",
        )

    ensure_upload_dir()

    # Derive the new versioned file path from the current version's path
    current_ver = next(
        v for v in doc["versions"] if v["version_number"] == doc["current_version"]
    )
    new_version_num = doc["current_version"] + 1
    new_file_path = build_versioned_path(current_ver["file_path"], new_version_num)

    async with aiofiles.open(new_file_path, "wb") as f:
        await f.write(content)

    try:
        df = load_dataframe(new_file_path, file_type)
        rows, cols = df.shape
    except Exception as exc:
        os.remove(new_file_path)
        raise HTTPException(
            status_code=422,
            detail=f"Could not parse the uploaded file: {exc}",
        ) from exc

    await add_dataset_version(
        dataset_id=dataset_id,
        new_file_path=new_file_path,
        rows=rows,
        cols=cols,
        changes_applied=[f"Manual upload: {filename}"],
    )

    logger.info(
        "New version %s uploaded for dataset %s from file %s",
        new_version_num, dataset_id, filename,
    )
    return await get_dataset_by_id(dataset_id)


async def get_all_datasets() -> List[Dict[str, Any]]:
    db = get_database()
    cursor = db[COLLECTION].find({}).sort("uploaded_at", -1)
    return [_serialise(doc) async for doc in cursor]


async def get_dataset_by_id(dataset_id: str) -> Dict[str, Any]:
    _validate_object_id(dataset_id)
    db = get_database()
    doc = await db[COLLECTION].find_one({"_id": ObjectId(dataset_id)})
    if doc is None:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found.")
    return _serialise(doc)


async def resolve_version_path(
    dataset_id: str, version: Optional[int] = None
) -> Tuple[str, str, int]:
    """
    Return (file_path, file_type, resolved_version_number) for the requested version.
    If *version* is None the current version is used.
    """
    doc = await get_dataset_by_id(dataset_id)
    resolved = version if version is not None else doc["current_version"]

    ver = next((v for v in doc["versions"] if v["version_number"] == resolved), None)
    if ver is None:
        raise HTTPException(
            status_code=404,
            detail=f"Version {resolved} does not exist for dataset '{dataset_id}'.",
        )

    return ver["file_path"], doc["file_type"], resolved


async def add_dataset_version(
    dataset_id: str,
    new_file_path: str,
    rows: int,
    cols: int,
    changes_applied: List[str],
) -> int:
    """
    Append a new version entry to the dataset document in MongoDB.
    Returns the new version number.
    """
    doc = await get_dataset_by_id(dataset_id)
    new_version_number = doc["current_version"] + 1

    version_entry = {
        "version_number": new_version_number,
        "file_path": new_file_path,
        "created_at": now_ist(),
        "changes_applied": changes_applied,
        "row_count": rows,
        "col_count": cols,
    }

    db = get_database()
    await db[COLLECTION].update_one(
        {"_id": ObjectId(dataset_id)},
        {
            "$push": {"versions": version_entry},
            "$set": {
                "current_version": new_version_number,
                "row_count": rows,
                "col_count": cols,
            },
        },
    )
    logger.info(
        "New version %s created for dataset %s", new_version_number, dataset_id
    )
    return new_version_number


# ---------------------------------------------------------------------------
# Delete dataset
# ---------------------------------------------------------------------------

async def delete_dataset(dataset_id: str) -> Dict[str, Any]:
    """
    Delete a dataset record from MongoDB and remove all associated files on disk.
    Returns a summary of what was deleted.
    """
    doc = await get_dataset_by_id(dataset_id)  # raises 404 if not found

    # Collect all version file paths before deletion
    file_paths = [v["file_path"] for v in doc.get("versions", [])]

    db = get_database()
    await db[COLLECTION].delete_one({"_id": ObjectId(dataset_id)})

    # Remove files from disk (best effort — don't raise if missing)
    deleted_files = 0
    for path in file_paths:
        try:
            if os.path.exists(path):
                os.remove(path)
                deleted_files += 1
        except OSError as exc:
            logger.warning("Could not delete file %s: %s", path, exc)

    logger.info(
        "Dataset deleted: id=%s name=%s files_removed=%s",
        dataset_id, doc["name"], deleted_files,
    )
    return {"deleted_id": dataset_id, "name": doc["name"], "files_removed": deleted_files}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _validate_object_id(oid: str) -> None:
    if not ObjectId.is_valid(oid):
        raise HTTPException(status_code=400, detail=f"'{oid}' is not a valid dataset ID.")


def _serialise(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Convert MongoDB document to a JSON-serialisable dict."""
    doc = dict(doc)
    if "_id" in doc and isinstance(doc["_id"], ObjectId):
        doc["_id"] = str(doc["_id"])
    return doc

