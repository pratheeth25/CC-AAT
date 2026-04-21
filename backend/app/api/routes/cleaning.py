"""
Cleaning pipeline route.

POST /dataset/{id}/clean
"""

import logging
from typing import Any, Dict

from fastapi import APIRouter

from app.models.dataset import CleaningRequest
from app.services import cleaning_service
from app.services.cache_service import analysis_cache
from app.utils.response_utils import success_response

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Cleaning"])


@router.post("/dataset/{dataset_id}/clean", summary="Apply automated data cleaning")
async def clean_dataset(
    dataset_id: str,
    request: CleaningRequest = CleaningRequest(),
) -> Dict[str, Any]:
    """
    Applies the requested transformations to the specified dataset version,
    saves the result as a new version, and returns a cleaning summary.

    Request body (all fields optional):
    ```json
    {
      "fix_missing_numeric": true,
      "fix_missing_categorical": true,
      "fix_duplicates": true,
      "standardize_dates": true,
      "fix_emails": true,
      "normalize_countries": true,
      "normalize_case": "lower",
      "remove_outliers": false,
      "version": null
    }
    ```
    """
    result = await cleaning_service.apply_cleaning(dataset_id, request)
    analysis_cache.invalidate(dataset_id)
    return success_response(result, "Cleaning pipeline completed.")
