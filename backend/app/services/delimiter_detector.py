"""
Delimiter Detection Pre-Parser.

Scans the first N lines of a CSV file to detect mixed or inconsistent
delimiters. Mixed delimiters cause column shifts that silently corrupt data.

Returns:
  {
    "primary": ",",
    "mixed": True,
    "delimiters_found": [",", ";", "|"],
    "rows_affected": [2, 5, 17],
    "line_count_sampled": 20
  }
"""

import logging
from collections import Counter
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

_DELIMITERS = [",", ";", "|", "\t"]
_SAMPLE_LINES = 30


def detect_delimiter(file_path: str) -> Dict[str, Any]:
    """
    Sample the first *_SAMPLE_LINES* lines and check for mixed delimiters.

    Parameters
    ----------
    file_path : path to the raw CSV file on disk

    Returns
    -------
    dict with:
      primary          – most common delimiter
      mixed            – True if >1 delimiter type dominates any row
      delimiters_found – list of distinct delimiters seen
      rows_affected    – row indices where a non-primary delimiter appears
      line_count_sampled – how many lines were checked
    """
    try:
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            lines = []
            for i, line in enumerate(f):
                if i >= _SAMPLE_LINES + 1:  # +1 for header
                    break
                lines.append(line)
    except Exception as exc:
        logger.warning("Could not open file for delimiter check: %s", exc)
        return _ok_result()

    if len(lines) < 2:
        return _ok_result()

    # Count delimiter occurrences per line (skip header on row 0)
    per_line_counts: List[Dict[str, int]] = []
    for line in lines:
        counts = {d: line.count(d) for d in _DELIMITERS if line.count(d) > 0}
        per_line_counts.append(counts)

    # Determine the primary delimiter by total count across all lines
    totals: Counter = Counter()
    for counts in per_line_counts:
        for d, c in counts.items():
            totals[d] += c

    if not totals:
        return _ok_result()

    primary = totals.most_common(1)[0][0]

    # A line is "affected" if it contains a non-primary delimiter with >= 2 occurrences
    # (a single occurrence might just be inside a value)
    mixed = False
    rows_affected: List[int] = []
    delimiters_found = set()
    delimiters_found.add(primary)

    for row_idx, counts in enumerate(per_line_counts):
        for d, c in counts.items():
            if d != primary and c >= 2:
                mixed = True
                rows_affected.append(row_idx)
                delimiters_found.add(d)

    # Pretty-print delimiter name
    def _label(d: str) -> str:
        return {"," : "comma", ";" : "semicolon", "|" : "pipe", "\t": "tab"}.get(d, repr(d))

    return {
        "primary": primary,
        "mixed": mixed,
        "delimiters_found": [_label(d) for d in sorted(delimiters_found)],
        "rows_affected": rows_affected[:20],
        "line_count_sampled": len(lines),
    }


def _ok_result() -> Dict[str, Any]:
    return {
        "primary": ",",
        "mixed": False,
        "delimiters_found": ["comma"],
        "rows_affected": [],
        "line_count_sampled": 0,
    }
