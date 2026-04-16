from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from app.utils.time_utils import now_ist


# ---------------------------------------------------------------------------
# Version sub-document
# ---------------------------------------------------------------------------

class DatasetVersion(BaseModel):
    version_number: int
    file_path: str
    created_at: datetime = Field(default_factory=now_ist)
    changes_applied: List[str] = Field(default_factory=list)
    row_count: int = 0
    col_count: int = 0


# ---------------------------------------------------------------------------
# Top-level dataset document (matches MongoDB shape)
# ---------------------------------------------------------------------------

class DatasetDocument(BaseModel):
    id: Optional[str] = Field(None, alias="_id")
    name: str
    original_filename: str
    file_type: str  # "csv" | "json"
    size_bytes: int
    row_count: int
    col_count: int
    uploaded_at: datetime = Field(default_factory=now_ist)
    current_version: int = 1
    versions: List[DatasetVersion] = Field(default_factory=list)

    model_config = {"populate_by_name": True, "arbitrary_types_allowed": True}


# ---------------------------------------------------------------------------
# API response models
# ---------------------------------------------------------------------------

class DatasetResponse(BaseModel):
    id: str
    name: str
    original_filename: str
    file_type: str
    size_bytes: int
    row_count: int
    col_count: int
    uploaded_at: datetime
    current_version: int
    versions: List[DatasetVersion]


class ProfileResponse(BaseModel):
    dataset_id: str
    version: int
    shape: Dict[str, int]
    missing_values: Dict[str, Any]
    duplicates: Dict[str, Any]
    columns: Dict[str, Any]
    generated_at: datetime = Field(default_factory=now_ist)


class QualityResponse(BaseModel):
    dataset_id: str
    version: int
    total_score: float
    grade: str
    verdict: str = ""
    penalties_applied: List[Dict[str, Any]] = Field(default_factory=list)
    dimension_scores: Dict[str, Any] = Field(default_factory=dict)
    breakdown: Dict[str, Any] = Field(default_factory=dict)
    generated_at: datetime = Field(default_factory=now_ist)


class SecurityFindingResponse(BaseModel):
    row: int
    column: str
    value: str
    threat_type: str
    severity: str
    matched_pattern: str


class SecurityScanResponse(BaseModel):
    dataset_id: str
    version: int
    findings: List[SecurityFindingResponse] = Field(default_factory=list)
    threat_summary: Dict[str, int] = Field(default_factory=dict)
    columns_affected: List[str] = Field(default_factory=list)
    total_threats: int = 0
    has_critical: bool = False
    score_deduction: int = 0
    generated_at: datetime = Field(default_factory=now_ist)


class VersionResponse(BaseModel):
    dataset_id: str
    version_number: int
    created_at: datetime
    source: str = "upload"
    parent_version: Optional[int] = None
    file_path: str = ""
    row_count: int = 0
    col_count: int = 0
    checksum: str = ""
    changes_applied: List[str] = Field(default_factory=list)
    is_latest: bool = False


class VersionDiffResponse(BaseModel):
    dataset_id: str
    version_a: int
    version_b: int
    row_count_a: int = 0
    row_count_b: int = 0
    row_delta: int = 0
    col_count_a: int = 0
    col_count_b: int = 0
    col_delta: int = 0
    columns_added: List[str] = Field(default_factory=list)
    columns_removed: List[str] = Field(default_factory=list)
    changes_applied_in_b: List[str] = Field(default_factory=list)


class DelimiterInfoResponse(BaseModel):
    primary: str = ","
    mixed: bool = False
    delimiters_found: List[str] = Field(default_factory=list)
    rows_affected: List[int] = Field(default_factory=list)
    line_count_sampled: int = 0


class AnomalyItem(BaseModel):
    column: str
    anomaly_count: int
    anomalies: List[Any]
    methods_used: List[str]


class AnomalyResponse(BaseModel):
    dataset_id: str
    version: int
    method: str
    results: List[AnomalyItem]
    total_anomalous_columns: int
    generated_at: datetime = Field(default_factory=now_ist)


class RepairSuggestion(BaseModel):
    column: str
    issue_type: str
    suggestion: str
    action: str
    details: Optional[Dict[str, Any]] = None


class RepairResponse(BaseModel):
    dataset_id: str
    version: int
    suggestions: List[RepairSuggestion]
    generated_at: datetime = Field(default_factory=now_ist)


class CleaningRequest(BaseModel):
    fix_missing_numeric: bool = True
    fix_missing_categorical: bool = True
    fix_duplicates: bool = True
    standardize_dates: bool = True
    normalize_case: Optional[str] = None     # "lower" | "upper" | "title" | None
    remove_outliers: bool = False
    version: Optional[int] = None            # source version; None = current


class CleaningResponse(BaseModel):
    dataset_id: str
    original_version: int
    new_version: int
    fixes_applied: List[str]
    rows_before: int
    rows_after: int
    generated_at: datetime = Field(default_factory=now_ist)


class DriftColumn(BaseModel):
    column: str
    drift_type: str       # "numerical" | "categorical"
    test: str             # "ks_test" | "chi2_test"
    p_value: Optional[float]
    mean_shift: Optional[float]
    interpretation: str


class DriftResponse(BaseModel):
    dataset_id: str
    version_a: int
    version_b: int
    drifted_columns: List[DriftColumn]
    stable_columns: List[Any]
    total_columns_compared: int
    generated_at: datetime = Field(default_factory=now_ist)
