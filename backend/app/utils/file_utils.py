import os
from typing import Tuple

import pandas as pd

from app.config import settings


def ensure_upload_dir() -> None:
    """Create the upload directory if it does not exist."""
    os.makedirs(settings.UPLOAD_DIR, exist_ok=True)


def load_dataframe(file_path: str, file_type: str, sample: bool = False) -> pd.DataFrame:
    """Load a CSV or JSON file into a pandas DataFrame.

    If *sample* is True and ``settings.MAX_PROFILE_ROWS > 0``, very large files
    are down-sampled to at most ``MAX_PROFILE_ROWS`` rows using a deterministic
    random seed, so results are reproducible between requests.
    """
    if file_type == "csv":
        df = pd.read_csv(file_path, low_memory=False)
    elif file_type == "json":
        df = pd.read_json(file_path)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")

    max_rows = settings.MAX_PROFILE_ROWS
    if sample and max_rows > 0 and len(df) > max_rows:
        df = df.sample(n=max_rows, random_state=42).reset_index(drop=True)

    return df


def save_dataframe(df: pd.DataFrame, file_path: str, file_type: str) -> None:
    """Persist a DataFrame back to disk in the original format."""
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    if file_type == "csv":
        df.to_csv(file_path, index=False)
    elif file_type == "json":
        df.to_json(file_path, orient="records", indent=2)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")


def build_versioned_path(original_path: str, version: int) -> str:
    """Derive a file path for a new dataset version."""
    base, ext = os.path.splitext(original_path)
    # Strip any previous _vN suffix
    if base.endswith(f"_v{version - 1}"):
        base = base[: -(len(str(version - 1)) + 2)]
    return f"{base}_v{version}{ext}"
