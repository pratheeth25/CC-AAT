"""
Root-Cause Analysis Service.

Takes the raw penalties and detections from all analysis services and:

  1. Maps each raw penalty to a canonical IssueGroup (via issue_taxonomy)
  2. Merges penalties that share the same root cause for the same column set
     → eliminates double-counting (e.g. date validity + date format consistency
       both map to DATE_QUALITY and are combined into ONE penalty)
  3. Computes a ``fair_score`` based on the de-duplicated penalties
  4. Produces per-column ``RootCause`` summaries with confidence-annotated
     ``DetectionResult`` objects for explainability
  5. Returns ``FixStep`` objects — actionable, code-level cleaning steps

Public API
----------
  analyze_root_causes(df, profile, quality, anomalies, pii_result)
      → Dict with "columns", "dedup_penalties", "fair_score", "group_impacts"

  suggest_fix_steps(df, profile, root_cause_analysis)
      → List[FixStep]

  map_penalty_to_group(penalty) → IssueGroup
  deduplicate_penalties(penalties) → List[Dict]
"""

from __future__ import annotations

import re
import warnings
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from app.services.issue_taxonomy import (
    DetectionResult,
    FixStep,
    GROUP_DEDUCTION_CAPS,
    IMPACT_MAP,
    IssueGroup,
    PENALTY_TO_GROUP,
    RootCause,
)

# ── Column-name keyword sets ──────────────────────────────────────────────────

_DATE_COL_KEYWORDS = frozenset({
    "date", "joindate", "join_date", "created_at", "created", "updated_at",
    "dob", "birth_date", "birthdate", "signup_date", "start_date", "end_date",
    "order_date", "timestamp",
})
_EMAIL_COL_KEYWORDS = frozenset({"email", "e-mail", "email_address", "mail"})
_NAME_COL_KEYWORDS  = frozenset({
    "name", "fullname", "full_name", "firstname", "lastname",
    "first_name", "last_name",
})
_COUNTRY_COL_KEYWORDS = frozenset({"country", "nation", "region", "location"})
_PHONE_COL_KEYWORDS   = frozenset({"phone", "mobile", "cell", "telephone", "contact"})

# ── Known garbage / sentinel values ──────────────────────────────────────────

_GARBAGE_SENTINEL: frozenset = frozenset({
    "null", "NULL", "none", "None", "NONE", "n/a", "N/A", "na", "nan",
    "error", "undefined", "unknown", "test", "todo", "tbd", "xxx", "XXX",
    "?", "??", "???", "-", "--", "---",
})

# ── Date format patterns ──────────────────────────────────────────────────────

_DATE_PATTERNS: List[Tuple[str, str]] = [
    (r"^\d{4}-\d{2}-\d{2}$",           "YYYY-MM-DD"),
    (r"^\d{2}/\d{2}/\d{4}$",           "DD/MM/YYYY"),
    (r"^\d{2}-\d{2}-\d{4}$",           "DD-MM-YYYY"),
    (r"^\d{4}/\d{2}/\d{2}$",           "YYYY/MM/DD"),
    (r"^\d{2}\.\d{2}\.\d{4}$",         "DD.MM.YYYY"),
    (r"^\w{3}\s+\d{1,2},?\s+\d{4}$",   "Mon DD YYYY"),
    (r"^\d{1,2}\s+\w{3}\s+\d{4}$",     "DD Mon YYYY"),
    (r"^\d{2}/\d{2}/\d{2}$",           "MM/DD/YY"),
    (r"^\d{8}$",                        "YYYYMMDD"),
]

_EMAIL_RE = re.compile(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z]{2,}$")

# ── Country synonym groups ────────────────────────────────────────────────────

_COUNTRY_SYNONYMS: Dict[str, frozenset] = {
    "United States": frozenset({"usa", "us", "u.s.", "u.s.a.", "united states", "america"}),
    "United Kingdom": frozenset({"uk", "u.k.", "great britain", "britain", "england"}),
    "Canada": frozenset({"can", "canada"}),
    "Germany": frozenset({"de", "germany", "deutschland"}),
    "Australia": frozenset({"au", "aus", "australia"}),
}


# ─────────────────────────────────────────────────────────────────────────────
# Part 1 — Penalty grouping + de-duplication
# ─────────────────────────────────────────────────────────────────────────────

def map_penalty_to_group(penalty: Dict[str, Any]) -> IssueGroup:
    """Map a single raw penalty dict → canonical IssueGroup."""
    dimension = penalty.get("dimension", "")
    reason    = penalty.get("reason", "").lower()
    for dim, keyword, group in PENALTY_TO_GROUP:
        if dim == dimension and (not keyword or keyword in reason):
            return group
    return IssueGroup.COMPLETENESS


def deduplicate_penalties(penalties: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Merge penalties that share the same (root-cause group, affected-column set).

    The combined deduction for each group is capped at GROUP_DEDUCTION_CAPS,
    eliminating double-counting of the same root cause across multiple scoring
    dimensions (e.g. date validity + date format consistency both → DATE_QUALITY).

    Returns a new list ordered highest-deduction first.
    """
    buckets: Dict[Tuple[frozenset, IssueGroup], List[Dict]] = defaultdict(list)

    for p in penalties:
        group   = map_penalty_to_group(p)
        col_key = frozenset(p.get("affected_columns") or [])
        buckets[(col_key, group)].append(p)

    merged: List[Dict[str, Any]] = []
    for (col_key, group), items in buckets.items():
        raw_total   = sum(abs(i.get("deduction", 0)) for i in items)
        cap         = GROUP_DEDUCTION_CAPS.get(group, 30.0)
        final_deduct = min(raw_total, cap)

        # Collect distinct reasons
        reasons = "; ".join(dict.fromkeys(i["reason"] for i in items))

        # Worst impact wins
        impact_order = {"high": 0, "medium": 1, "low": 2}
        worst_impact = min(
            (i.get("impact", "low") for i in items),
            key=lambda x: impact_order.get(x, 3),
        )

        merged.append({
            "dimension":            items[0].get("dimension", ""),
            "root_cause_group":     group.value,
            "reason":               reasons,
            "impact":               worst_impact,
            "deduction":            -round(final_deduct, 2),
            "affected_columns":     sorted(col_key),
            "_original_count":      len(items),
            "_original_deductions": round(raw_total, 2),
            "_saved":               round(raw_total - final_deduct, 2),
        })

    merged.sort(key=lambda p: abs(p.get("deduction", 0)), reverse=True)
    return merged


def compute_fair_score(dedup_penalties: List[Dict[str, Any]]) -> float:
    """
    Compute a fair total quality score from de-duplicated penalties.

    This score is distinct from the existing total_score:
    - total_score  = 100 − Σ(raw penalties)   [may double-penalise]
    - fair_score   = 100 − Σ(dedup penalties)  [one penalty per root cause]
    """
    total_deduct = sum(abs(p.get("deduction", 0)) for p in dedup_penalties)
    return round(max(100.0 - total_deduct, 0.0), 2)


# ─────────────────────────────────────────────────────────────────────────────
# Part 2 — Per-column root-cause analysis
# ─────────────────────────────────────────────────────────────────────────────

def analyze_root_causes(
    df: pd.DataFrame,
    profile: Dict[str, Any],
    quality: Dict[str, Any],
    anomalies: List[Dict[str, Any]],
    pii_result: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build a per-column root-cause analysis.

    Parameters
    ----------
    df           : Raw DataFrame (needed for deep inspection)
    profile      : Output of profiling_service.profile_dataframe()
    quality      : Output of quality_service.calculate_quality_score()
    anomalies    : Output of anomaly_service.detect_anomalies()
    pii_result   : Output of pii_service.detect_pii() (optional)

    Returns
    -------
    {
      "columns": {
        "Join_Date": {
          "status": "HIGH",
          "root_causes": [
            {
              "group":      "DATE_QUALITY",
              "label":      "Date quality issues",
              "severity":   "high",
              "detections": [
                {
                  "type":        "mixed_date_formats",
                  "confidence":  0.95,
                  "explanation": "Found 3 distinct date formats ...",
                  "evidence":    [...],
                  "column":      "Join_Date",
                  "severity":    "high"
                },
                ...
              ],
              "examples": ["yesterday", "00-00-0000", ...],
              "formats":  ["YYYY-MM-DD", "DD/MM/YYYY", ...]
            }
          ]
        },
        ...
      },
      "dedup_penalties": [...],
      "fair_score":      float,
      "group_impacts":   { "DATE_QUALITY": { "business": [...], "technical": [...] }, ... }
    }
    """
    penalties       = quality.get("penalties_applied", [])
    col_root_causes: Dict[str, Any] = {}

    # Build per-column anomaly index
    anomaly_idx: Dict[str, List[Dict]] = defaultdict(list)
    for a in anomalies:
        anomaly_idx[a["column"]].append(a)

    # Build PII column index
    pii_idx: Dict[str, Dict] = {}
    if pii_result:
        for c in pii_result.get("columns", []):
            pii_idx[c["column_name"]] = c

    for col in df.columns:
        col_lower = col.lower().strip()
        series    = df[col].dropna()
        causes: List[RootCause] = []

        # 1 — DATE QUALITY
        if col_lower in _DATE_COL_KEYWORDS:
            rc = _detect_date_root_cause(col, series)
            if rc:
                causes.append(rc)

        # 2 — EMAIL QUALITY
        if col_lower in _EMAIL_COL_KEYWORDS:
            rc = _detect_email_root_cause(col, series)
            if rc:
                causes.append(rc)

        # 3 — FORMAT CONSISTENCY: name casing
        if col_lower in _NAME_COL_KEYWORDS:
            rc = _detect_casing_root_cause(col, series)
            if rc:
                causes.append(rc)

        # 4 — FORMAT CONSISTENCY: country synonyms
        if col_lower in _COUNTRY_COL_KEYWORDS:
            rc = _detect_category_root_cause(col, series)
            if rc:
                causes.append(rc)

        # 5 — GARBAGE / JUNK values
        rc = _detect_garbage_root_cause(col, series)
        if rc:
            causes.append(rc)

        # 6 — ANOMALIES for this column
        if col in anomaly_idx:
            rc = _detect_anomaly_root_cause(col, anomaly_idx[col])
            if rc:
                causes.append(rc)

        # 7 — PII for this column
        if col in pii_idx:
            rc = _detect_pii_root_cause(col, pii_idx[col], series)
            if rc:
                causes.append(rc)

        if causes:
            overall_sev = _worst_severity([c.severity for c in causes])
            col_root_causes[col] = {
                "status":      overall_sev.upper(),
                "root_causes": [c.to_dict() for c in causes],
            }

    # De-duplicate penalties and compute fair score
    dedup    = deduplicate_penalties(penalties)
    fair_scr = compute_fair_score(dedup)

    # Build impact lookup for active groups only
    active_groups = {p["root_cause_group"] for p in dedup}
    group_impacts: Dict[str, Any] = {}
    for g in active_groups:
        try:
            grp = IssueGroup(g)
            group_impacts[g] = IMPACT_MAP.get(grp, {})
        except ValueError:
            pass

    return {
        "columns":         col_root_causes,
        "dedup_penalties": dedup,
        "fair_score":      fair_scr,
        "group_impacts":   group_impacts,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Part 3 — FixStep pipeline
# ─────────────────────────────────────────────────────────────────────────────

def suggest_fix_steps(
    df: pd.DataFrame,
    profile: Dict[str, Any],
    root_cause_analysis: Dict[str, Any],
) -> List[FixStep]:
    """
    Return a prioritised list of FixStep objects — actionable, code-level
    cleaning steps derived from the root-cause analysis.

    Steps are de-duplicated (same column + action appears only once) and
    ordered: high → medium → low priority.
    """
    steps: List[FixStep] = []
    seen:  set            = set()

    def _add(step: FixStep) -> None:
        key = (step.column, step.action)
        if key not in seen:
            seen.add(key)
            steps.append(step)

    col_analyses = root_cause_analysis.get("columns", {})

    for col, analysis in col_analyses.items():
        col_lower = col.lower().strip()
        for rc in analysis.get("root_causes", []):
            group = rc.get("group", "")
            sev   = rc.get("severity", "low")
            fmts  = rc.get("formats", [])
            exs   = rc.get("examples", [])

            if group == IssueGroup.DATE_QUALITY.value:
                # Step 1: coerce to datetime
                _add(FixStep(
                    column=col,
                    action="parse_dates",
                    code_hint=(
                        f"df['{col}'] = pd.to_datetime(\n"
                        f"    df['{col}'], infer_datetime_format=True, errors='coerce'\n"
                        f")"
                    ),
                    priority="high" if sev == "high" else "medium",
                    details={"detected_formats": fmts, "invalid_examples": exs[:5]},
                ))
                # Step 2: normalise to ISO 8601
                _add(FixStep(
                    column=col,
                    action="standardize_date_format",
                    code_hint=f"df['{col}'] = df['{col}'].dt.strftime('%Y-%m-%d')",
                    priority="medium",
                    details={"target_format": "YYYY-MM-DD"},
                ))
                # Step 3: drop rows where date is still NaT after coerce
                _add(FixStep(
                    column=col,
                    action="drop_unparseable_dates",
                    code_hint=f"df = df.dropna(subset=['{col}'])  # remove rows with bad dates",
                    priority="low",
                    details={"note": "Only apply if dropping rows is acceptable"},
                ))

            elif group == IssueGroup.EMAIL_QUALITY.value:
                _add(FixStep(
                    column=col,
                    action="validate_drop_invalid_emails",
                    code_hint=(
                        f"import re\n"
                        f"_re = re.compile(r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z]{{2,}}$')\n"
                        f"mask = df['{col}'].astype(str).str.strip().apply(lambda v: bool(_re.match(v)))\n"
                        f"df = df[mask]  # drop rows with invalid email"
                    ),
                    priority="high" if sev == "high" else "medium",
                    details={"invalid_examples": exs[:5]},
                ))
                _add(FixStep(
                    column=col,
                    action="normalize_email_casing",
                    code_hint=f"df['{col}'] = df['{col}'].astype(str).str.strip().str.lower()",
                    priority="low",
                    details={},
                ))

            elif group == IssueGroup.FORMAT_CONSISTENCY.value:
                if col_lower in _NAME_COL_KEYWORDS:
                    _add(FixStep(
                        column=col,
                        action="normalize_name_casing",
                        code_hint=f"df['{col}'] = df['{col}'].astype(str).str.strip().str.title()",
                        priority="medium",
                        details={},
                    ))
                elif col_lower in _COUNTRY_COL_KEYWORDS:
                    _add(FixStep(
                        column=col,
                        action="normalize_country_synonyms",
                        code_hint=(
                            f"_cmap = {{\n"
                            f"    'usa': 'United States', 'us': 'United States',\n"
                            f"    'uk': 'United Kingdom', 'can': 'Canada', 'de': 'Germany',\n"
                            f"}}\n"
                            f"df['{col}'] = (\n"
                            f"    df['{col}'].astype(str).str.strip().str.lower()\n"
                            f"    .map(_cmap).fillna(df['{col}'])\n"
                            f")"
                        ),
                        priority="low",
                        details={},
                    ))
                else:
                    _add(FixStep(
                        column=col,
                        action="normalize_casing",
                        code_hint=f"df['{col}'] = df['{col}'].astype(str).str.strip().str.lower()",
                        priority="low",
                        details={},
                    ))

            elif group == IssueGroup.DUPLICATES.value:
                _add(FixStep(
                    column="__all__",
                    action="drop_exact_duplicates",
                    code_hint="df = df.drop_duplicates(keep='first').reset_index(drop=True)",
                    priority="high",
                    details={},
                ))

            elif group == IssueGroup.GARBAGE_VALUES.value:
                sentinel_repr = (
                    "{'null', 'NULL', 'none', 'None', 'n/a', 'N/A', "
                    "'na', '?', '??', '???', '-', '--'}"
                )
                _add(FixStep(
                    column=col,
                    action="replace_garbage_with_nan",
                    code_hint=(
                        f"_sentinel = {sentinel_repr}\n"
                        f"df['{col}'] = df['{col}'].apply(\n"
                        f"    lambda v: None if str(v).strip() in _sentinel else v\n"
                        f")"
                    ),
                    priority="medium" if sev == "medium" else "low",
                    details={"sentinel_examples": exs[:5]},
                ))

    # ── Missing-value imputation (from profile) ───────────────────────────────
    for col, mv in profile.get("missing_values", {}).items():
        if mv.get("count", 0) == 0:
            continue
        series = df[col]
        if pd.api.types.is_numeric_dtype(series):
            non_null = series.dropna()
            skew     = float(non_null.skew()) if len(non_null) > 1 else 0.0
            strategy = "median" if abs(skew) > 1.0 else "mean"
            fill     = round(float(non_null.median() if strategy == "median" else non_null.mean()), 4)
            _add(FixStep(
                column=col,
                action=f"fill_{strategy}",
                code_hint=f"df['{col}'] = df['{col}'].fillna({fill})  # {strategy} imputation",
                priority="medium",
                details={"strategy": strategy, "fill_value": fill, "missing_count": mv["count"]},
            ))
        else:
            non_null = series.dropna().astype(str)
            if len(non_null) > 0 and not non_null.mode().empty:
                mode_val = non_null.mode().iloc[0]
                _add(FixStep(
                    column=col,
                    action="fill_mode",
                    code_hint=f"df['{col}'] = df['{col}'].fillna('{mode_val}')  # mode imputation",
                    priority="low",
                    details={"fill_value": mode_val, "missing_count": mv["count"]},
                ))

    # Sort: high first, then medium, then low
    order = {"high": 0, "medium": 1, "low": 2}
    steps.sort(key=lambda s: order.get(s.priority, 3))
    return steps


# ─────────────────────────────────────────────────────────────────────────────
# Internal per-column detectors
# ─────────────────────────────────────────────────────────────────────────────

def _detect_date_root_cause(col: str, series: pd.Series) -> Optional[RootCause]:
    """Detect all date quality issues and group them under DATE_QUALITY."""
    if len(series) == 0:
        return None

    str_vals = series.astype(str)

    # 1. Identify distinct formats
    formats_found: set = set()
    for val in str_vals.head(200):
        v = val.strip()
        for pattern, label in _DATE_PATTERNS:
            if re.match(pattern, v):
                formats_found.add(label)
                break

    # 2. Parse failures via pandas
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        parsed = pd.to_datetime(str_vals, infer_datetime_format=True, errors="coerce")
    parse_fail_vals = str_vals[parsed.isna()].head(10).tolist()

    # 3. Semantic non-date strings (not captured by format patterns or parse)
    semantic_errors = [
        v.strip() for v in str_vals.head(200)
        if v.strip() and not any(re.match(p, v.strip()) for p, _ in _DATE_PATTERNS)
        and v.strip() not in parse_fail_vals
    ][:5]

    if not formats_found and not parse_fail_vals and not semantic_errors:
        return None

    detections: List[DetectionResult] = []

    if len(formats_found) >= 2:
        detections.append(DetectionResult(
            type="mixed_date_formats",
            confidence=min(0.95, 0.5 + len(formats_found) * 0.1),
            explanation=f"Found {len(formats_found)} distinct date formats: {', '.join(sorted(formats_found))}",
            evidence=[f"Format detected: '{f}'" for f in sorted(formats_found)],
            column=col,
            severity="high" if len(formats_found) > 2 else "medium",
        ))

    if parse_fail_vals:
        detections.append(DetectionResult(
            type="unparseable_dates",
            confidence=0.99,
            explanation=f"{len(parse_fail_vals)} value(s) could not be parsed as valid dates",
            evidence=[f"Unparseable: '{v}'" for v in parse_fail_vals[:5]],
            column=col,
            severity="high",
        ))

    if semantic_errors:
        detections.append(DetectionResult(
            type="semantic_date_errors",
            confidence=0.80,
            explanation="Values use non-standard or ambiguous date representations",
            evidence=[f"Example: '{v}'" for v in semantic_errors[:5]],
            column=col,
            severity="medium",
        ))

    if not detections:
        return None

    severity = _worst_severity([d.severity for d in detections])
    return RootCause(
        group=IssueGroup.DATE_QUALITY,
        label="Date quality issues",
        severity=severity,
        detections=detections,
        examples=(parse_fail_vals + semantic_errors)[:8],
        formats=sorted(formats_found),
    )


def _detect_email_root_cause(col: str, series: pd.Series) -> Optional[RootCause]:
    if len(series) == 0:
        return None

    str_vals = series.astype(str).str.strip()
    invalid  = str_vals[~str_vals.apply(lambda v: bool(_EMAIL_RE.match(v)))]

    if len(invalid) == 0:
        return None

    inv_pct    = len(invalid) / max(len(str_vals), 1)
    confidence = min(0.99, 0.6 + inv_pct * 0.4)

    detection = DetectionResult(
        type="invalid_email",
        confidence=confidence,
        explanation=f"{len(invalid)} of {len(str_vals)} values ({inv_pct * 100:.1f}%) fail email pattern validation",
        evidence=[
            f"{len(invalid)} values fail RFC-style email regex",
            *[f"Invalid: '{v}'" for v in invalid.head(3).tolist()],
        ],
        column=col,
        severity="high" if inv_pct >= 0.5 else "medium",
    )
    return RootCause(
        group=IssueGroup.EMAIL_QUALITY,
        label="Invalid email addresses",
        severity=detection.severity,
        detections=[detection],
        examples=invalid.head(5).tolist(),
    )


def _detect_casing_root_cause(col: str, series: pd.Series) -> Optional[RootCause]:
    if len(series) == 0:
        return None

    str_vals    = series.astype(str).str.strip()
    unique_raw  = set(str_vals.unique())
    unique_norm = {v.lower() for v in unique_raw}

    if len(unique_raw) <= len(unique_norm):
        return None

    variants   = len(unique_raw) - len(unique_norm)
    confidence = min(0.90, 0.55 + variants / max(len(unique_raw), 1))

    detection = DetectionResult(
        type="inconsistent_casing",
        confidence=confidence,
        explanation=(
            f"'{col}' has {len(unique_raw)} raw variants that collapse to "
            f"{len(unique_norm)} canonical values after lower-casing"
        ),
        evidence=[f"Sample variants: {sorted(list(unique_raw))[:5]}"],
        column=col,
        severity="low",
    )
    return RootCause(
        group=IssueGroup.FORMAT_CONSISTENCY,
        label="Inconsistent text casing",
        severity="low",
        detections=[detection],
        examples=sorted(list(unique_raw))[:5],
    )


def _detect_category_root_cause(col: str, series: pd.Series) -> Optional[RootCause]:
    """Detect synonym / abbreviation inconsistency (e.g. USA / US / United States)."""
    if len(series) == 0:
        return None

    str_vals    = series.astype(str).str.lower().str.strip()
    found_groups: List[Tuple[str, List[str]]] = []

    for canonical, syn_set in _COUNTRY_SYNONYMS.items():
        hits = str_vals[str_vals.isin(syn_set)].unique().tolist()
        if len(hits) > 1:
            found_groups.append((canonical, hits))

    if not found_groups:
        return None

    evidence = [
        f"'{canonical}' has variants: {variants}"
        for canonical, variants in found_groups
    ]
    detection = DetectionResult(
        type="category_synonyms",
        confidence=0.90,
        explanation=f"'{col}' contains synonymous variants for the same category",
        evidence=evidence,
        column=col,
        severity="low",
    )
    return RootCause(
        group=IssueGroup.FORMAT_CONSISTENCY,
        label="Categorical synonym inconsistency",
        severity="low",
        detections=[detection],
        examples=[v for _, variants in found_groups for v in variants[:3]],
    )


def _detect_garbage_root_cause(col: str, series: pd.Series) -> Optional[RootCause]:
    if len(series) == 0:
        return None

    str_vals = series.astype(str).str.strip().str.lower()
    garbage  = str_vals[str_vals.isin(_GARBAGE_SENTINEL)]

    if len(garbage) == 0:
        return None

    pct        = len(garbage) / max(len(series), 1)
    confidence = min(0.99, 0.7 + pct * 0.3)

    detection = DetectionResult(
        type="garbage_values",
        confidence=confidence,
        explanation=(
            f"{len(garbage)} garbage/sentinel value(s) in '{col}' "
            f"({pct * 100:.1f}% of non-null values)"
        ),
        evidence=[f"Example: '{v}'" for v in garbage.head(3).tolist()],
        column=col,
        severity="medium" if pct > 0.10 else "low",
    )
    return RootCause(
        group=IssueGroup.GARBAGE_VALUES,
        label="Garbage / placeholder values",
        severity=detection.severity,
        detections=[detection],
        examples=garbage.head(5).tolist(),
    )


def _detect_anomaly_root_cause(
    col: str,
    col_anomalies: List[Dict[str, Any]],
) -> Optional[RootCause]:
    detections: List[DetectionResult] = []

    for a in col_anomalies:
        methods   = a.get("methods_used", ["unknown"])
        sev       = a.get("severity", "medium")
        anom_vals = a.get("anomalies", [])[:5]
        count     = a.get("anomaly_count", 0)

        conf = 0.85 if sev == "high" else 0.70
        detections.append(DetectionResult(
            type="statistical_anomaly",
            confidence=conf,
            explanation=f"{count} anomalous value(s) detected via {', '.join(methods)}",
            evidence=[f"Anomalous value: '{v}'" for v in anom_vals],
            column=col,
            severity=sev,
        ))

    if not detections:
        return None

    severity = _worst_severity([d.severity for d in detections])
    all_examples = [v for a in col_anomalies for v in a.get("anomalies", [])[:3]]
    return RootCause(
        group=IssueGroup.ANOMALY,
        label="Statistical anomalies",
        severity=severity,
        detections=detections,
        examples=all_examples[:10],
    )


def _detect_pii_root_cause(
    col: str,
    pii_col: Dict[str, Any],
    series: pd.Series,
) -> Optional[RootCause]:
    pii_type   = pii_col.get("pii_type", "Unknown")
    raw_conf   = pii_col.get("confidence", 0.5)
    # PII service may return "high"/"medium"/"low" strings or a float
    if isinstance(raw_conf, str):
        confidence = {"high": 0.92, "medium": 0.65, "low": 0.35}.get(raw_conf.lower(), 0.5)
    else:
        try:
            confidence = float(raw_conf)
        except (TypeError, ValueError):
            confidence = 0.5

    detection = DetectionResult(
        type=f"pii_{pii_type.lower().replace(' ', '_')}",
        confidence=confidence,
        explanation=f"'{col}' contains {pii_type} — personally identifiable information",
        evidence=[
            f"PII type: {pii_type}",
            f"Detection confidence: {confidence:.0%}",
            f"Pattern matched across {min(5, len(series))} sampled values",
        ],
        column=col,
        severity="high" if confidence >= 0.8 else "medium",
    )
    return RootCause(
        group=IssueGroup.PII_RISK,
        label=f"PII — {pii_type}",
        severity=detection.severity,
        detections=[detection],
    )


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _worst_severity(severities: List[str]) -> str:
    for s in ("high", "medium", "low"):
        if s in severities:
            return s
    return "low"
