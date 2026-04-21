"""
Dataset upload and retrieval routes.

POST /upload
GET  /datasets
GET  /dataset/{id}
DELETE /dataset/{id}
"""

import logging
from typing import Any, Dict, List

from fastapi import APIRouter, File, UploadFile

from app.services import dataset_service
from app.services import alert_service
from app.services.cache_service import analysis_cache
from app.utils.response_utils import success_response

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Datasets"])


@router.post("/upload", summary="Upload a CSV or JSON dataset")
async def upload_dataset(file: UploadFile = File(...)) -> Dict[str, Any]:
    """
    Upload a CSV or JSON file.  The file is stored locally and metadata is
    persisted in MongoDB.  Returns the full dataset document including its
    automatically assigned version 1.
    """
    dataset = await dataset_service.upload_dataset(file)
    return success_response(dataset, "Dataset uploaded successfully.")


@router.get("/datasets", summary="List all uploaded datasets")
async def list_datasets() -> Dict[str, Any]:
    datasets = await dataset_service.get_all_datasets()
    return success_response(datasets, f"{len(datasets)} dataset(s) found.")


@router.post("/dataset/{dataset_id}/upload-version", summary="Upload a new version of a dataset")
async def upload_new_version(
    dataset_id: str, file: UploadFile = File(...)
) -> Dict[str, Any]:
    """
    Upload a CSV or JSON file as a new version of an existing dataset.
    The file type must match the original dataset's type.
    Returns the updated dataset document with the new version appended.
    """
    dataset = await dataset_service.upload_new_version(dataset_id, file)
    analysis_cache.invalidate(dataset_id)
    return success_response(dataset, "New version uploaded successfully.")


@router.get("/dataset/{dataset_id}", summary="Get a single dataset by ID")
async def get_dataset(dataset_id: str) -> Dict[str, Any]:
    dataset = await dataset_service.get_dataset_by_id(dataset_id)
    return success_response(dataset)


@router.delete("/dataset/{dataset_id}", summary="Delete a dataset and all its versions")
async def delete_dataset(dataset_id: str) -> Dict[str, Any]:
    """
    Permanently removes the dataset record from MongoDB and deletes all
    associated version files from disk.  This action is irreversible.
    """
    result = await dataset_service.delete_dataset(dataset_id)
    analysis_cache.invalidate(dataset_id)
    await alert_service.delete_dataset_alerts(dataset_id)
    return success_response(result, f"Dataset '{result['name']}' deleted successfully.")
