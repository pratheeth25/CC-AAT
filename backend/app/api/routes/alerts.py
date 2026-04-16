"""
Alerts routes.

GET  /alerts
GET  /alerts/{alert_id}/read   (mark as read)
"""

import logging
from typing import Any, Dict, Optional

from bson import ObjectId
from fastapi import APIRouter, HTTPException, Query

from app.services import alert_service
from app.utils.response_utils import success_response

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Alerts"])


@router.get("/alerts", summary="List all alerts")
async def list_alerts(
    dataset_id: Optional[str] = Query(None, description="Filter by dataset ID"),
) -> Dict[str, Any]:
    """
    Returns all stored alerts, optionally filtered to a single dataset.
    Alerts are sorted newest-first.
    """
    alerts = await alert_service.get_all_alerts(dataset_id=dataset_id)
    return success_response(alerts, f"{len(alerts)} alert(s) found.")


@router.patch("/alerts/{alert_id}/read", summary="Mark an alert as read")
async def mark_read(alert_id: str) -> Dict[str, Any]:
    if not ObjectId.is_valid(alert_id):
        raise HTTPException(status_code=400, detail="Invalid alert ID.")
    updated = await alert_service.mark_alert_read(alert_id)
    if not updated:
        raise HTTPException(status_code=404, detail="Alert not found.")
    return success_response({"alert_id": alert_id, "is_read": True})
