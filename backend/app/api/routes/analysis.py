"""
Analysis routes.

GET  /dataset/{id}/profile
GET  /dataset/{id}/quality
GET  /dataset/{id}/anomalies
GET  /dataset/{id}/repairs
GET  /dataset/{id}/drift
GET  /dataset/{id}/security-scan
GET  /dataset/{id}/delimiter-check
GET  /dataset/{id}/pii
GET  /dataset/{id}/prediction
GET  /dataset/{id}/report
"""

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, BackgroundTasks, Query

from app.config import settings
from app.services import (
    alert_service,
    anomaly_service,
    dataset_service,
    delimiter_detector,
    drift_service,
    profiling_service,
    quality_service,
    repair_service,
    security_scanner,
)
from app.services import pii_service, prediction_service, report_service
from app.services import change_summary_service
from app.services.cache_service import analysis_cache
from app.services.job_service import job_manager
from app.utils.file_utils import load_dataframe
from app.utils.response_utils import success_response

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Analysis"])


# ---------------------------------------------------------------------------
# Profile
# ---------------------------------------------------------------------------

@router.get("/dataset/{dataset_id}/profile", summary="Data profiling report")
async def get_profile(
    dataset_id: str,
    version: Optional[int] = Query(None, description="Dataset version to analyse (default: current)"),
) -> Dict[str, Any]:
    """
    Returns shape, missing-value counts, duplicate statistics, and per-column
    statistics for the requested dataset version.
    """
    file_path, file_type, resolved_version = await dataset_service.resolve_version_path(
        dataset_id, version
    )

    # Check cache
    cached = analysis_cache.get(dataset_id, resolved_version, "profile")
    if cached is not None:
        return success_response(cached)

    df = load_dataframe(file_path, file_type, sample=True)
    profile = profiling_service.profile_dataframe(df)

    # Trigger missing-value alerts asynchronously (fire-and-forget style via await)
    doc = await dataset_service.get_dataset_by_id(dataset_id)
    await alert_service.trigger_missing_value_alerts(dataset_id, doc["name"], profile)

    payload = {
        "dataset_id": dataset_id,
        "version": resolved_version,
        **profile,
    }
    analysis_cache.set(dataset_id, resolved_version, "profile", payload)
    return success_response(payload)


# ---------------------------------------------------------------------------
# Quality score
# ---------------------------------------------------------------------------

@router.get("/dataset/{dataset_id}/quality", summary="Data quality score")
async def get_quality(
    dataset_id: str,
    version: Optional[int] = Query(None),
    method: str = Query("all", description="Anomaly method: iqr | zscore | isolation_forest | all"),
) -> Dict[str, Any]:
    """
    Returns the composite quality score (0-100) with penalty-based scoring,
    security scanning, delimiter checking, and per-dimension breakdown.
    """
    file_path, file_type, resolved_version = await dataset_service.resolve_version_path(
        dataset_id, version
    )
    df = load_dataframe(file_path, file_type)

    profile = profiling_service.profile_dataframe(df)
    anomaly_results = anomaly_service.detect_anomalies(df, method=method)

    # Security scan
    sec_result = security_scanner.scan_dataframe(df)
    sec_dict = sec_result.to_dict()

    # PII scan (feeds into security dimension of quality score)
    pii_result = pii_service.detect_pii(df)

    # Delimiter check (only for CSV files)
    delim_info = None
    if file_type == "csv":
        delim_info = delimiter_detector.detect_delimiter(file_path)

    # Previous version score for delta computation
    doc = await dataset_service.get_dataset_by_id(dataset_id)
    previous_score = None
    versions = doc.get("versions", [])
    if len(versions) >= 2:
        sorted_vers = sorted(versions, key=lambda v: v["version_number"])
        prev_ver = sorted_vers[-2]
        if resolved_version == sorted_vers[-1]["version_number"]:
            try:
                prev_path, prev_type, _ = await dataset_service.resolve_version_path(
                    dataset_id, prev_ver["version_number"]
                )
                prev_df      = load_dataframe(prev_path, prev_type)
                prev_profile = profiling_service.profile_dataframe(prev_df)
                prev_anom    = anomaly_service.detect_anomalies(prev_df, method="iqr")
                prev_sec     = security_scanner.scan_dataframe(prev_df).to_dict()
                prev_quality = quality_service.calculate_quality_score(
                    profile=prev_profile, anomaly_results=prev_anom,
                    security_scan=prev_sec, df=prev_df,
                )
                previous_score = prev_quality["total_score"]
            except Exception as exc:
                logger.debug("Could not compute previous version score: %s", exc)

    quality = quality_service.calculate_quality_score(
        profile=profile,
        anomaly_results=anomaly_results,
        security_scan=sec_dict,
        delimiter_info=delim_info,
        df=df,
        pii_result=pii_result,
        previous_score=previous_score,
    )

    await alert_service.trigger_quality_alert(dataset_id, doc["name"], quality)

    return success_response(
        {
            "dataset_id": dataset_id,
            "version": resolved_version,
            **quality,
        }
    )


# ---------------------------------------------------------------------------
# Anomalies
# ---------------------------------------------------------------------------

@router.get("/dataset/{dataset_id}/anomalies", summary="Anomaly detection results")
async def get_anomalies(
    dataset_id: str,
    version: Optional[int] = Query(None),
    method: str = Query(
        "all",
        description="Detection method: iqr | zscore | isolation_forest | all",
    ),
    page: int = Query(1, ge=1, description="Page number (1-based)"),
    limit: int = Query(50, ge=1, le=200, description="Max results per page"),
) -> Dict[str, Any]:
    """
    Detects outliers, invalid dates, rare categoricals, and junk values.

    Response includes per-column anomaly list in the format:
      { "column": "Phone", "anomaly_count": 3, "anomalies": ["?", "000-000"] }
    """
    file_path, file_type, resolved_version = await dataset_service.resolve_version_path(
        dataset_id, version
    )

    # Cache key includes method to allow per-method caching
    cache_key = f"anomalies_{method}"
    cached = analysis_cache.get(dataset_id, resolved_version, cache_key)
    if cached is None:
        df = load_dataframe(file_path, file_type)
        results = anomaly_service.detect_anomalies(df, method=method)
        doc = await dataset_service.get_dataset_by_id(dataset_id)
        await alert_service.trigger_anomaly_alerts(dataset_id, doc["name"], results)
        analysis_cache.set(dataset_id, resolved_version, cache_key, results)
    else:
        results = cached

    # Pagination
    total = len(results)
    start = (page - 1) * limit
    end = start + limit
    page_results = results[start:end]

    return success_response(
        {
            "dataset_id": dataset_id,
            "version": resolved_version,
            "method": method,
            "results": page_results,
            "total_anomalous_columns": total,
            "page": page,
            "limit": limit,
            "total_pages": max(1, (total + limit - 1) // limit),
        }
    )


# ---------------------------------------------------------------------------
# Repair suggestions
# ---------------------------------------------------------------------------

@router.get("/dataset/{dataset_id}/repairs", summary="Smart repair suggestions")
async def get_repairs(
    dataset_id: str,
    version: Optional[int] = Query(None),
) -> Dict[str, Any]:
    """
    Analyses the dataset and returns actionable repair suggestions such as
    filling missing values, standardising dates, normalising casing, etc.
    """
    file_path, file_type, resolved_version = await dataset_service.resolve_version_path(
        dataset_id, version
    )
    df = load_dataframe(file_path, file_type)
    profile = profiling_service.profile_dataframe(df)
    suggestions = repair_service.suggest_repairs(df, profile)

    return success_response(
        {
            "dataset_id": dataset_id,
            "version": resolved_version,
            "suggestions": suggestions,
        }
    )


# ---------------------------------------------------------------------------
# Drift detection
# ---------------------------------------------------------------------------

@router.get("/dataset/{dataset_id}/drift", summary="Data drift between two versions")
async def get_drift(
    dataset_id: str,
    version_a: int = Query(..., description="Older (baseline) version number"),
    version_b: int = Query(..., description="Newer version number"),
) -> Dict[str, Any]:
    """
    Compares two versions of the same dataset and flags columns where
    statistical drift has been detected (KS test for numerical, χ² for categorical).
    """
    if version_a == version_b:
        return success_response(
            {
                "dataset_id": dataset_id,
                "version_a": version_a,
                "version_b": version_b,
                "drifted_columns": [],
                "stable_columns": [],
                "total_columns_compared": 0,
                "note": "Both versions are identical.",
            }
        )

    path_a, type_a, _ = await dataset_service.resolve_version_path(dataset_id, version_a)
    path_b, type_b, _ = await dataset_service.resolve_version_path(dataset_id, version_b)

    drift_report = drift_service.detect_drift(
        file_path_a=path_a,
        file_type_a=type_a,
        version_a=version_a,
        file_path_b=path_b,
        file_type_b=type_b,
        version_b=version_b,
        dataset_id=dataset_id,
    )

    doc = await dataset_service.get_dataset_by_id(dataset_id)
    await alert_service.trigger_drift_alert(dataset_id, doc["name"], drift_report)

    # Return summarized drift: drifted columns in full, stable columns as count only
    summarized = {
        **drift_report,
        "stable_column_count": len(drift_report.get("stable_columns", [])),
        "stable_columns": [c["column"] for c in drift_report.get("stable_columns", [])],
    }
    return success_response(summarized)


# ---------------------------------------------------------------------------
# Data Change Summary (deterministic, replaces drift for simple comparisons)
# ---------------------------------------------------------------------------

@router.get("/dataset/{dataset_id}/change-summary", summary="Data change summary between two versions")
async def get_change_summary(
    dataset_id: str,
    version_a: int = Query(..., description="Older (baseline) version number"),
    version_b: int = Query(..., description="Newer version number"),
) -> Dict[str, Any]:
    """
    Deterministic comparison of two dataset versions: column/row changes,
    missing-value deltas, distribution shifts, and categorical mutations.
    No statistical tests used.
    """
    if version_a == version_b:
        return success_response({
            "dataset_id": dataset_id,
            "version_a": version_a,
            "version_b": version_b,
            "columns": {"added": [], "removed": [], "type_changes": []},
            "rows": {"before": 0, "after": 0, "change": 0, "percent_change": 0.0},
            "missing_changes": {},
            "distribution_shift": {},
            "categorical_changes": [],
            "note": "Both versions are identical.",
        })

    path_a, type_a, _ = await dataset_service.resolve_version_path(dataset_id, version_a)
    path_b, type_b, _ = await dataset_service.resolve_version_path(dataset_id, version_b)

    summary = change_summary_service.compute_change_summary(
        file_path_a=path_a,
        file_type_a=type_a,
        version_a=version_a,
        file_path_b=path_b,
        file_type_b=type_b,
        version_b=version_b,
        dataset_id=dataset_id,
    )

    return success_response(summary)


# ---------------------------------------------------------------------------
# Security scan
# ---------------------------------------------------------------------------

@router.get("/dataset/{dataset_id}/security-scan", summary="Scan dataset for security threats")
async def get_security_scan(
    dataset_id: str,
    version: Optional[int] = Query(None),
) -> Dict[str, Any]:
    """
    Scans dataset for XSS, SQL injection, path traversal, command injection, and null bytes.
    """
    file_path, file_type, resolved_version = await dataset_service.resolve_version_path(
        dataset_id, version
    )
    df = load_dataframe(file_path, file_type)
    result = security_scanner.scan_dataframe(df)

    return success_response(
        {
            "dataset_id": dataset_id,
            "version": resolved_version,
            **result.to_dict(),
        }
    )


# ---------------------------------------------------------------------------
# Delimiter check
# ---------------------------------------------------------------------------

@router.get("/dataset/{dataset_id}/delimiter-check", summary="Check for mixed delimiters")
async def get_delimiter_check(
    dataset_id: str,
    version: Optional[int] = Query(None),
) -> Dict[str, Any]:
    """
    Checks CSV files for mixed or inconsistent delimiters.
    """
    file_path, file_type, resolved_version = await dataset_service.resolve_version_path(
        dataset_id, version
    )

    if file_type != "csv":
        return success_response(
            {
                "dataset_id": dataset_id,
                "version": resolved_version,
                "primary": None,
                "mixed": False,
                "delimiters_found": [],
                "rows_affected": [],
                "note": "Delimiter check only applies to CSV files.",
            }
        )

    info = delimiter_detector.detect_delimiter(file_path)

    return success_response(
        {
            "dataset_id": dataset_id,
            "version": resolved_version,
            **info,
        }
    )


# ---------------------------------------------------------------------------
# PII Detection
# ---------------------------------------------------------------------------

@router.get("/dataset/{dataset_id}/pii", summary="Detect PII in dataset columns")
async def get_pii(
    dataset_id: str,
    version: Optional[int] = Query(None),
) -> Dict[str, Any]:
    """
    Scans every column for personally identifiable information (email, phone,
    credit-card, Aadhaar, passport, IP, SSN).
    """
    file_path, file_type, resolved_version = await dataset_service.resolve_version_path(
        dataset_id, version
    )
    df = load_dataframe(file_path, file_type)
    result = pii_service.detect_pii(df)

    return success_response(
        {
            "dataset_id": dataset_id,
            "version": resolved_version,
            **result,
        }
    )


# ---------------------------------------------------------------------------
# Predictive Quality Degradation
# ---------------------------------------------------------------------------

@router.get("/dataset/{dataset_id}/prediction", summary="Predict quality degradation")
async def get_prediction(
    dataset_id: str,
    sla_threshold: int = Query(70, description="Minimum acceptable quality score"),
) -> Dict[str, Any]:
    """
    Analyses quality scores across dataset versions and predicts future
    quality trajectory using linear regression.
    """
    doc = await dataset_service.get_dataset_by_id(dataset_id)
    versions = doc.get("versions", [])

    # Compute quality score for each version
    version_scores = []
    for v in versions:
        try:
            file_path, file_type, _ = await dataset_service.resolve_version_path(
                dataset_id, v["version_number"]
            )
            df = load_dataframe(file_path, file_type)
            profile = profiling_service.profile_dataframe(df)
            anomaly_results = anomaly_service.detect_anomalies(df, method="all")
            sec_result = security_scanner.scan_dataframe(df)
            sec_dict = sec_result.to_dict()
            delim_info = None
            if file_type == "csv":
                delim_info = delimiter_detector.detect_delimiter(file_path)
            quality = quality_service.calculate_quality_score(
                profile=profile,
                anomaly_results=anomaly_results,
                security_scan=sec_dict,
                delimiter_info=delim_info,
                df=df,
            )
            version_scores.append({
                "version": v["version_number"],
                "score": quality["total_score"],
            })
        except Exception as exc:
            logger.warning("Could not compute quality for version %s: %s", v["version_number"], exc)

    result = prediction_service.predict_degradation(version_scores, sla_threshold=sla_threshold)

    return success_response(
        {
            "dataset_id": dataset_id,
            **result,
        }
    )


# ---------------------------------------------------------------------------
# Comprehensive Report
# ---------------------------------------------------------------------------

@router.get("/dataset/{dataset_id}/report", summary="Generate comprehensive data quality report")
async def get_report(
    dataset_id: str,
    version: Optional[int] = Query(None),
    method: str = Query("all", description="Anomaly method: iqr | zscore | isolation_forest | all"),
) -> Dict[str, Any]:
    """
    Aggregates all analysis results and produces a comprehensive report in
    three formats: structured JSON, human-readable text, and executive summary.
    """
    doc = await dataset_service.get_dataset_by_id(dataset_id)

    file_path, file_type, resolved_version = await dataset_service.resolve_version_path(
        dataset_id, version
    )
    df = load_dataframe(file_path, file_type)

    # Run all analyses
    profile         = profiling_service.profile_dataframe(df)
    anomaly_results = anomaly_service.detect_anomalies(df, method=method)
    sec_result      = security_scanner.scan_dataframe(df)
    sec_dict        = sec_result.to_dict()
    pii_result      = pii_service.detect_pii(df)
    delim_info      = delimiter_detector.detect_delimiter(file_path) if file_type == "csv" else None
    quality         = quality_service.calculate_quality_score(
        profile=profile,
        anomaly_results=anomaly_results,
        security_scan=sec_dict,
        delimiter_info=delim_info,
        df=df,
    )

    # Drift: compare current with previous version if available
    drift_result = None
    versions = doc.get("versions", [])
    if len(versions) >= 2:
        sorted_vers = sorted(versions, key=lambda v: v["version_number"])
        prev_ver = sorted_vers[-2]
        curr_ver = sorted_vers[-1]
        try:
            drift_result = drift_service.detect_drift(
                file_path_a=prev_ver["file_path"],
                file_type_a=doc["file_type"],
                version_a=prev_ver["version_number"],
                file_path_b=curr_ver["file_path"],
                file_type_b=doc["file_type"],
                version_b=curr_ver["version_number"],
                dataset_id=dataset_id,
            )
        except Exception as exc:
            logger.warning("Drift computation skipped in report: %s", exc)

    report = report_service.generate_report(
        dataset_id=dataset_id,
        dataset_name=doc["name"],
        file_type=file_type,
        row_count=doc["row_count"],
        col_count=doc["col_count"],
        profile=profile,
        quality=quality,
        anomalies=anomaly_results,
        security=sec_dict,
        pii=pii_result,
        drift=drift_result,
        versions=versions,
    )

    return success_response(report)


# ---------------------------------------------------------------------------
# Async full analysis job  (POST /dataset/{id}/analyze)
# ---------------------------------------------------------------------------

def _run_full_analysis(job_id: str, dataset_id: str, version: int, file_path: str, file_type: str) -> None:
    """
    Blocking worker: runs all analysis steps and stores result in job_manager.
    Called from a FastAPI BackgroundTask thread.
    """
    import asyncio
    job_manager.start(job_id)
    try:
        df = load_dataframe(file_path, file_type)
        profile         = profiling_service.profile_dataframe(df)
        anomaly_results = anomaly_service.detect_anomalies(df, method="iqr")
        sec_dict        = security_scanner.scan_dataframe(df).to_dict()
        pii_result      = pii_service.detect_pii(df)
        quality         = quality_service.calculate_quality_score(
            profile=profile, anomaly_results=anomaly_results,
            security_scan=sec_dict, df=df, pii_result=pii_result,
        )
        result = {
            "dataset_id": dataset_id,
            "version": version,
            "total_score": quality.get("total_score"),
            "dimensions": quality.get("dimensions"),
            "anomalous_columns": len(anomaly_results),
            "pii_risk": pii_result.get("dataset_risk"),
            "threats": sec_dict.get("total_threats"),
        }
        analysis_cache.set(dataset_id, version, "profile", {"dataset_id": dataset_id, "version": version, **profile})
        job_manager.finish(job_id, result)
    except Exception as exc:
        logger.exception("Async analysis job %s failed", job_id)
        job_manager.fail(job_id, str(exc))


@router.post("/dataset/{dataset_id}/analyze", summary="Trigger async full analysis")
async def trigger_async_analysis(
    dataset_id: str,
    background_tasks: BackgroundTasks,
    version: Optional[int] = Query(None),
) -> Dict[str, Any]:
    """
    Enqueues a full analysis job and returns a job_id immediately.
    Poll GET /jobs/{job_id} to check progress and retrieve results.
    """
    if not settings.ASYNC_ANALYSIS_ENABLED:
        return success_response(
            {"message": "Async analysis is disabled. Use GET /dataset/{id}/quality directly."}
        )

    file_path, file_type, resolved_version = await dataset_service.resolve_version_path(
        dataset_id, version
    )
    job_id = job_manager.create(
        "full_analysis",
        dataset_id=dataset_id,
        version=resolved_version,
    )
    background_tasks.add_task(
        _run_full_analysis, job_id, dataset_id, resolved_version, file_path, file_type
    )
    return success_response(
        {"job_id": job_id, "dataset_id": dataset_id, "version": resolved_version},
        message="Analysis job enqueued. Poll GET /jobs/{job_id} for status.",
    )
