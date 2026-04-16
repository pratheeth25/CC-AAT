from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field

from app.utils.time_utils import now_ist


class AlertDocument(BaseModel):
    id: Optional[str] = Field(None, alias="_id")
    dataset_id: str
    dataset_name: str
    alert_type: str   # "high_missing" | "anomaly_detected" | "quality_degraded" | "drift_detected"
    severity: str     # "low" | "medium" | "high" | "critical"
    message: str
    details: Dict[str, Any] = Field(default_factory=dict)
    triggered_at: datetime = Field(default_factory=now_ist)
    is_read: bool = False

    model_config = {"populate_by_name": True}


class AlertResponse(BaseModel):
    id: str
    dataset_id: str
    dataset_name: str
    alert_type: str
    severity: str
    message: str
    details: Dict[str, Any]
    triggered_at: datetime
    is_read: bool
