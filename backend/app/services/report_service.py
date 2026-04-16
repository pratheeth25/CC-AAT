"""
Data Quality Report Service.

Aggregates outputs from all analysis services and produces three formats:
  1. Structured JSON report
  2. Human-friendly plain-text report
  3. Executive summary (3–5 lines, non-technical)
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from app.utils.time_utils import now_ist

logger = logging.getLogger(__name__)


# ── Severity helpers ──────────────────────────────────────────────────────────

def _anomaly_severity(count: int) -> str:
    if count >= 20:
        return "high"
    if count >= 5:
        return "medium"
    return "low"


def _drift_severity(p_value: float | None) -> str:
    if p_value is None:
        return "low"
    if p_value < 0.01:
        return "high"
    if p_value < 0.05:
        return "medium"
    return "low"


def _score_label(score: float) -> str:
    if score >= 90:
        return "Excellent"
    if score >= 75:
        return "Good"
    if score >= 60:
        return "Fair"
    if score >= 40:
        return "Poor"
    return "Critical"


def _risk_emoji(risk: str) -> str:
    return {"high": "🔴", "medium": "🟠", "low": "🟡", "none": "🟢"}.get(risk, "⚪")


# ── Main entry point ──────────────────────────────────────────────────────────

def generate_report(
    *,
    dataset_id: str,
    dataset_name: str,
    file_type: str,
    row_count: int,
    col_count: int,
    profile: Dict[str, Any],
    quality: Dict[str, Any],
    anomalies: List[Dict[str, Any]],
    security: Dict[str, Any],
    pii: Dict[str, Any],
    drift: Optional[Dict[str, Any]] = None,
    versions: Optional[List[Dict[str, Any]]] = None,
    alerts: Optional[List[Dict[str, Any]]] = None,
    df: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    Return { "json_report": {...}, "text_report": str, "executive_summary": str }
    """
    json_report = _build_json(
        dataset_id=dataset_id,
        dataset_name=dataset_name,
        file_type=file_type,
        row_count=row_count,
        col_count=col_count,
        quality=quality,
        anomalies=anomalies,
        security=security,
        pii=pii,
        drift=drift,
        df=df,
        profile=profile,
    )

    text_report = _build_text(
        dataset_name=dataset_name,
        json_report=json_report,
        profile=profile,
        versions=versions,
    )

    executive_summary = _build_executive(
        dataset_name=dataset_name,
        json_report=json_report,
    )

    return {
        "dataset_id": dataset_id,
        "generated_at": now_ist().isoformat(),
        "json_report": json_report,
        "text_report": text_report,
        "executive_summary": executive_summary,
    }


# ── JSON format ───────────────────────────────────────────────────────────────

def _build_json(
    *,
    dataset_id: str,
    dataset_name: str,
    file_type: str,
    row_count: int,
    col_count: int,
    quality: Dict[str, Any],
    anomalies: List[Dict[str, Any]],
    security: Dict[str, Any],
    pii: Dict[str, Any],
    drift: Optional[Dict[str, Any]],
    profile: Optional[Dict[str, Any]] = None,
    df: Optional[Any] = None,
) -> Dict[str, Any]:
    overall_score = quality.get("total_score", 0)
    dims = quality.get("dimension_scores", {})

    # Key issues derivation
    key_issues: List[str] = []

    # --- missing values
    missing_pct = _get_missing_pct(quality)
    if missing_pct > 20:
        key_issues.append(f"{missing_pct:.1f}% of values are missing — significant data gaps detected.")
    elif missing_pct > 5:
        key_issues.append(f"{missing_pct:.1f}% of values are missing — consider imputation.")

    # --- duplicates (from metadata)
    metadata = quality.get("metadata", {})
    dup_pct = metadata.get("duplicate_pct", 0)
    if isinstance(dup_pct, (int, float)) and dup_pct > 0:
        key_issues.append(f"{dup_pct:.1f}% duplicate rows detected — deduplicate before analysis.")

    # --- anomalies
    total_anom = sum(r["anomaly_count"] for r in anomalies)
    if total_anom > 0:
        top_col = max(anomalies, key=lambda r: r["anomaly_count"])
        key_issues.append(
            f"{total_anom} anomalies found across {len(anomalies)} column(s); "
            f"worst: '{top_col['column']}' ({top_col['anomaly_count']} values)."
        )

    # --- invalid values (from validity penalties)
    validity_penalties = [p for p in quality.get("penalties_applied", []) if p.get("dimension") == "validity"]
    if validity_penalties:
        affected = sorted({c for p in validity_penalties for c in p.get("affected_columns", [])})
        total_invalid = sum(1 for p in validity_penalties)
        key_issues.append(
            f"{total_invalid} validity issue(s) detected in column(s): {', '.join(affected[:5])}."
        )

    # --- date format inconsistencies (from consistency penalties)
    date_fmt_penalties = [
        p for p in quality.get("penalties_applied", [])
        if p.get("dimension") == "consistency" and "date format" in p.get("reason", "").lower()
    ]
    if date_fmt_penalties:
        affected_cols = sorted({c for p in date_fmt_penalties for c in p.get("affected_columns", [])})
        key_issues.append(
            f"Mixed date formats in column(s): {', '.join(affected_cols)} — standardise before use."
        )

    # --- security
    sec_threats = security.get("threats_found", 0)
    if sec_threats > 0:
        key_issues.append(
            f"{sec_threats} security threat(s) detected "
            f"(XSS/SQL injection patterns) — sanitise before use."
        )

    # --- pii
    pii_cols = pii.get("columns", [])
    if pii_cols:
        names = ", ".join(f"'{c['column_name']}'" for c in pii_cols[:3])
        key_issues.append(
            f"PII detected in {len(pii_cols)} column(s): {names} — apply masking/pseudonymisation."
        )

    # --- low score
    if overall_score < 40:
        key_issues.append(
            f"Overall quality score is critically low ({overall_score:.0f}/100) — "
            "immediate remediation required."
        )

    # Cap at 5
    key_issues = key_issues[:5]
    if not key_issues:
        key_issues = ["No critical issues detected. Dataset appears healthy."]

    # Anomaly rows
    anomaly_rows = [
        {
            "column": r["column"],
            "type": ", ".join(r.get("methods_used", [])) or "unknown",
            "affected_rows": r["anomaly_count"],
            "severity": _anomaly_severity(r["anomaly_count"]),
        }
        for r in sorted(anomalies, key=lambda x: -x["anomaly_count"])
    ]

    # Drift rows
    drift_rows: List[Dict[str, Any]] = []
    if drift:
        for col in drift.get("drifted_columns", []):
            drift_rows.append({
                "column": col["column"],
                "drift_detected": True,
                "p_value": col.get("p_value"),
                "severity": _drift_severity(col.get("p_value")),
            })
        for col in drift.get("stable_columns", []):
            drift_rows.append({
                "column": col["column"],
                "drift_detected": False,
                "p_value": col.get("p_value"),
                "severity": "low",
            })

    # PII rows
    pii_rows = [
        {
            "column": c["column_name"],
            "pii_type": c["pii_type"],
            "risk_level": c["confidence"],
        }
        for c in pii_cols
    ]

    # Security issues list
    sec_issues: List[str] = []
    for t in security.get("threats", []):
        if isinstance(t, dict):
            sec_issues.append(
                f"{t.get('threat_type', 'Unknown')} in column '{t.get('column', '?')}' "
                f"({t.get('count', 0)} row(s))"
            )
    if not sec_issues and sec_threats == 0:
        sec_issues = ["No security threats detected."]

    # Recommendations
    recommendations = _build_recommendations(
        overall_score=overall_score,
        missing_pct=missing_pct,
        anomalies=anomalies,
        security=security,
        pii=pii_cols,
        drift=drift,
        dims=dims,
    )

    return {
        "dataset_summary": {
            "id": dataset_id,
            "name": dataset_name,
            "rows": row_count,
            "columns": col_count,
            "file_type": file_type,
        },
        "quality_score": {
            "overall": round(overall_score, 1),
            "grade": quality.get("grade", "—"),
            "verdict": quality.get("verdict", ""),
            "dimensions": {
                k: round(v.get("score", 0), 1) if isinstance(v, dict) else round(v, 1)
                for k, v in dims.items()
            },
        },
        "key_issues": key_issues,
        "anomalies": anomaly_rows,
        "drift": drift_rows,
        "pii_risks": pii_rows,
        "security_issues": sec_issues,
        "recommendations": recommendations,
        # ── v2 explainability additions ──────────────────────────────────
        "fair_score":          round(quality.get("fair_score", overall_score), 1),
        "fair_grade":          quality.get("fair_grade", quality.get("grade", "—")),
        "dedup_penalties":     quality.get("dedup_penalties", []),
        "root_causes":         quality.get("root_cause_analysis", {}),
        "group_impacts":       quality.get("group_impacts", {}),
        "fix_pipeline":        _build_fix_pipeline(quality=quality, profile=profile, df=df),
    }


def _build_fix_pipeline(
    quality: Dict[str, Any],
    profile: Optional[Dict[str, Any]],
    df: Optional[Any],
) -> List[Dict[str, Any]]:
    """Build a code-level fix pipeline if a DataFrame + profile are available."""
    if df is None or profile is None:
        return []
    try:
        from app.services.repair_service import suggest_fix_steps
        root_cause_analysis = quality.get("root_cause_analysis", {})
        return suggest_fix_steps(df=df, profile=profile, root_cause_analysis=root_cause_analysis)
    except Exception:
        return []


def _get_missing_pct(quality: Dict[str, Any]) -> float:
    """Extract overall missing value % from quality metadata."""
    return float(quality.get("metadata", {}).get("missing_pct", 0.0))


def _build_recommendations(
    *,
    overall_score: float,
    missing_pct: float,
    anomalies: List[Dict[str, Any]],
    security: Dict[str, Any],
    pii: List[Dict[str, Any]],
    drift: Optional[Dict[str, Any]],
    dims: Dict[str, Any],
) -> List[str]:
    recs: List[str] = []

    if missing_pct > 5:
        recs.append(
            "Fill or impute missing values — consider median/mode fill for numeric columns "
            "and 'Unknown' fill for categorical."
        )

    high_anomaly_cols = [r for r in anomalies if r["anomaly_count"] >= 5]
    if high_anomaly_cols:
        cols = ", ".join(f"'{r['column']}'" for r in high_anomaly_cols[:3])
        recs.append(
            f"Investigate and clean outliers in: {cols}. "
            "Use IQR clipping or domain-expert review."
        )

    if security.get("threats_found", 0) > 0:
        recs.append(
            "Strip or escape security payloads (XSS, SQL injection strings) "
            "before storing or serving this data."
        )

    if pii:
        high_pii = [c for c in pii if c.get("confidence") == "high"]
        if high_pii:
            cols = ", ".join(f"'{c['column_name']}'" for c in high_pii[:3])
            recs.append(
                f"Mask or tokenise PII in: {cols}. "
                "Apply pseudonymisation or hashing before sharing externally."
            )

    if drift and len(drift.get("drifted_columns", [])) > 0:
        recs.append(
            f"{len(drift['drifted_columns'])} column(s) show statistical drift. "
            "Investigate upstream data source changes and re-validate pipelines."
        )

    completeness_score = dims.get("completeness", {})
    if isinstance(completeness_score, dict):
        if completeness_score.get("score", 100) < 70:
            recs.append(
                "Completeness score is low — run the Cleaning pipeline to "
                "standardise null representations and remove empty rows."
            )

    uniqueness_score = dims.get("uniqueness", {})
    if isinstance(uniqueness_score, dict):
        if uniqueness_score.get("score", 100) < 70:
            recs.append(
                "Duplicate rows detected — deduplicate using a primary key or hash-based dedup before analysis."
            )

    if overall_score < 60:
        recs.append(
            "Run the automated Cleaning pipeline to apply all suggested repairs in one step."
        )

    return recs[:6] if recs else ["Dataset quality is acceptable. Continue monitoring with each new version."]


# ── Text format ───────────────────────────────────────────────────────────────

def _build_text(
    *,
    dataset_name: str,
    json_report: Dict[str, Any],
    profile: Dict[str, Any],
    versions: Optional[List[Dict[str, Any]]],
) -> str:
    now_str = now_ist().strftime("%d %b %Y, %H:%M IST")
    q = json_report["quality_score"]
    s = json_report["dataset_summary"]
    dims = q["dimensions"]
    issues = json_report["key_issues"]
    anomalies = json_report["anomalies"]
    drift_rows = json_report["drift"]
    pii_rows = json_report["pii_risks"]
    sec = json_report["security_issues"]
    recs = json_report["recommendations"]

    def bar(score: float, width: int = 20) -> str:
        filled = int((score / 100) * width)
        return "█" * filled + "░" * (width - filled)

    def severity_icon(sev: str) -> str:
        return {"high": "◉ HIGH", "medium": "◎ MEDIUM", "low": "○ LOW"}.get(sev, "○")

    lines: List[str] = []

    # ── Title ──
    lines += [
        "=" * 64,
        "  DATA QUALITY REPORT",
        f"  {dataset_name}",
        f"  Generated: {now_str}",
        "=" * 64,
        "",
    ]

    # ── Dataset Summary ──
    lines += [
        "┌─ DATASET SUMMARY ──────────────────────────────────────┐",
        f"│  Rows       : {s['rows']:,}",
        f"│  Columns    : {s['columns']}",
        f"│  File Type  : {s['file_type'].upper()}",
        f"│  Versions   : {len(versions) if versions else 1}",
        "└────────────────────────────────────────────────────────┘",
        "",
    ]

    # ── Quality Score ──
    overall = q["overall"]
    grade = q.get("grade", "")
    score_color = _score_label(overall)
    lines += [
        "┌─ QUALITY SCORE ────────────────────────────────────────┐",
        f"│  Overall Score : {overall:.1f} / 100  [{grade}]  — {score_color}",
        f"│  {bar(overall)}  {overall:.0f}%",
        "│",
        "│  Dimension Breakdown:",
    ]
    for dim, score in dims.items():
        label = dim.replace("_", " ").title().ljust(14)
        lines.append(f"│    {label}  {bar(score, 16)}  {score:.1f} pts")
    lines += ["└────────────────────────────────────────────────────────┘", ""]

    # ── Key Issues ──
    lines += ["┌─ KEY ISSUES ───────────────────────────────────────────┐"]
    for i, issue in enumerate(issues, 1):
        lines.append(f"│  {i}. {issue}")
    lines += ["└────────────────────────────────────────────────────────┘", ""]

    # ── Anomalies ──
    lines += ["┌─ ANOMALY SUMMARY ──────────────────────────────────────┐"]
    if anomalies:
        lines.append(f"│  {len(anomalies)} column(s) with anomalies:")
        for a in anomalies[:8]:
            lines.append(
                f"│  • {a['column']:<20} {a['affected_rows']:>4} rows  "
                f"{severity_icon(a['severity']):<12}  [{a['type']}]"
            )
        if len(anomalies) > 8:
            lines.append(f"│  ... and {len(anomalies) - 8} more columns")
    else:
        lines.append("│  ✓ No anomalies detected.")
    lines += ["└────────────────────────────────────────────────────────┘", ""]

    # ── Drift ──
    drifted = [d for d in drift_rows if d["drift_detected"]]
    lines += ["┌─ DRIFT DETECTION ──────────────────────────────────────┐"]
    if not drift_rows:
        lines.append("│  — Only one version available; no drift comparison possible.")
    elif drifted:
        lines.append(f"│  {len(drifted)} column(s) show significant drift:")
        for d in drifted[:6]:
            pv = f"p={d['p_value']:.4f}" if d["p_value"] is not None else ""
            lines.append(f"│  • {d['column']:<22} {pv:<12} {severity_icon(d['severity'])}")
    else:
        lines.append("│  ✓ No significant drift detected between versions.")
    lines += ["└────────────────────────────────────────────────────────┘", ""]

    # ── Security & PII ──
    lines += ["┌─ SECURITY & PII RISKS ─────────────────────────────────┐"]
    for s_issue in sec:
        icon = "⚠" if "No security" not in s_issue else "✓"
        lines.append(f"│  {icon}  {s_issue}")
    if pii_rows:
        lines.append("│")
        lines.append(f"│  PII Detected ({len(pii_rows)} column(s)):")
        for p in pii_rows:
            lines.append(f"│  • {p['column']:<20} {p['pii_type']:<14} risk={p['risk_level']}")
    else:
        lines.append("│  ✓ No PII detected.")
    lines += ["└────────────────────────────────────────────────────────┘", ""]

    # ── Recommendations ──
    lines += ["┌─ RECOMMENDATIONS ──────────────────────────────────────┐"]
    for i, rec in enumerate(recs, 1):
        # Word-wrap at 56 chars
        words = rec.split()
        current = f"│  {i}. "
        for word in words:
            if len(current) + len(word) + 1 > 62:
                lines.append(current)
                current = "│     " + word
            else:
                current += (" " if len(current) > 5 else "") + word
        lines.append(current)
    lines += ["└────────────────────────────────────────────────────────┘", ""]

    # ── Root-Cause Analysis (v2) ──────────────────────────────────────────
    root_causes = json_report.get("root_causes", {})
    dedup       = json_report.get("dedup_penalties", [])
    fair_score  = json_report.get("fair_score")
    if root_causes or dedup:
        lines += ["┌─ ROOT-CAUSE ANALYSIS (FAIR SCORING) ───────────────────┐"]
        if fair_score is not None:
            orig = json_report["quality_score"]["overall"]
            saved = round(orig - fair_score, 1)
            indicator = f"  (saved {saved:.1f} pts by eliminating double-counting)" if saved > 0 else ""
            lines.append(f"│  Fair Score : {fair_score}/100  (Raw: {orig}/100){indicator}")
        if dedup:
            lines.append("│")
            lines.append("│  De-duplicated penalties:")
            for p in dedup[:8]:
                grp = p.get("root_cause_group", "?")
                ded = p.get("deduction", 0)
                saved = p.get("_saved", 0)
                reason = p.get("reason", "")[:48]
                line = f"│    [{grp}]  {ded:+.1f}  — {reason}"
                if saved > 0:
                    line += f"  (saved {saved:.1f})"
                lines.append(line)
        if root_causes:
            lines.append("│")
            lines.append("│  Per-column root causes:")
            for col, analysis in list(root_causes.items())[:8]:
                status = analysis.get("status", "?")
                rcs    = analysis.get("root_causes", [])
                groups = ", ".join(rc["group"] for rc in rcs)
                lines.append(f"│    [{status}] {col}: {groups}")
        lines += ["└────────────────────────────────────────────────────────┘", ""]

    lines.append("End of Report")

    return "\n".join(lines)


# ── Executive Summary ─────────────────────────────────────────────────────────

def _build_executive(
    *,
    dataset_name: str,
    json_report: Dict[str, Any],
) -> str:
    q = json_report["quality_score"]
    overall = q["overall"]
    grade = q.get("grade", "")
    issues = json_report["key_issues"]
    pii = json_report["pii_risks"]
    sec = json_report["security_issues"]
    drifted = [d for d in json_report["drift"] if d.get("drift_detected")]
    recs = json_report["recommendations"]

    usable = overall >= 60
    critical = overall < 40
    has_pii = any(p["risk_level"] == "high" for p in pii)
    has_security = any("No security" not in s for s in sec)

    status = "✅ USABLE" if usable and not critical else "⚠️ NEEDS ATTENTION" if usable else "🔴 NOT RECOMMENDED"

    lines: List[str] = [
        f"EXECUTIVE SUMMARY — {dataset_name}",
        "─" * 48,
        f"Status: {status}  |  Score: {overall:.0f}/100 (Grade {grade})  |  {_score_label(overall)}",
        "",
    ]

    # Sentence 1: Overall health
    if critical:
        lines.append(
            f"⚠  This dataset has a critically low quality score ({overall:.0f}/100) "
            "and requires immediate remediation before it can be used reliably."
        )
    elif not usable:
        lines.append(
            f"The dataset quality ({overall:.0f}/100) is below the acceptable threshold. "
            "Several issues need to be addressed before production use."
        )
    else:
        lines.append(
            f"The dataset scores {overall:.0f}/100 and is generally usable, "
            f"though {len(issues)} issue(s) were identified that should be reviewed."
        )

    # Sentence 2: Biggest risks
    risks: List[str] = []
    if has_security:
        risks.append("security payloads (potential XSS/SQL injection)")
    if has_pii:
        risks.append(f"high-confidence PII in {len(pii)} column(s)")
    if drifted:
        risks.append(f"data drift in {len(drifted)} column(s)")
    if risks:
        lines.append(f"🔴 Major risks detected: {'; '.join(risks)}.")

    # Sentence 3: Top action
    if recs:
        lines.append(f"▶  Immediate action: {recs[0]}")

    # Sentence 4: Drift / versioning note
    if drifted:
        lines.append(
            f"📊 {len(drifted)} column(s) show statistical distribution changes between "
            "versions — validate upstream data pipelines."
        )

    return "\n".join(lines)

