"""
Storage abstraction layer — local disk or AWS S3.

All file I/O in the application goes through this module so that switching
from local storage to S3 requires changing only one environment variable.

Configuration
-------------
S3_ENABLED=false (default) → files are read/written to the local filesystem
S3_ENABLED=true            → files are stored in S3; download uses presigned URLs

Environment variables (only required when S3_ENABLED=true):
    S3_BUCKET          — S3 bucket name
    S3_REGION          — AWS region (e.g. ap-south-1)
    AWS_ACCESS_KEY_ID  — IAM access key with s3:GetObject / s3:PutObject / s3:DeleteObject
    AWS_SECRET_ACCESS_KEY

Usage
-----
    from app.services.storage_service import storage

    key = await storage.save(file_bytes, "uploads/abc_dataset.csv")
    data = await storage.load(key)
    url  = storage.presigned_url(key, ttl=3600)   # signed download URL
    await storage.delete(key)
"""

import logging
import os
from io import BytesIO
from typing import Optional

from app.config import settings

logger = logging.getLogger(__name__)


# ── Local (disk) backend ──────────────────────────────────────────────────────

class _LocalStorage:
    """Stores files on the local filesystem under UPLOAD_DIR."""

    def __init__(self, base_dir: str) -> None:
        self._base = base_dir
        os.makedirs(base_dir, exist_ok=True)

    def _abs(self, key: str) -> str:
        # key may already be absolute (legacy paths)
        return key if os.path.isabs(key) else os.path.join(self._base, key)

    async def save(self, data: bytes, key: str) -> str:
        """Persist *data* at *key*; returns the storage key."""
        path = self._abs(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as fh:
            fh.write(data)
        logger.debug("LocalStorage saved %d bytes → %s", len(data), path)
        return key

    async def load(self, key: str) -> bytes:
        path = self._abs(key)
        if not os.path.isfile(path):
            raise FileNotFoundError(f"Storage key not found on disk: {key}")
        with open(path, "rb") as fh:
            return fh.read()

    def presigned_url(self, key: str, ttl: int = 3600) -> Optional[str]:
        """Local storage doesn't support presigned URLs — return None."""
        return None

    def exists(self, key: str) -> bool:
        return os.path.isfile(self._abs(key))

    async def delete(self, key: str) -> None:
        path = self._abs(key)
        if os.path.isfile(path):
            os.remove(path)
            logger.debug("LocalStorage deleted %s", path)

    def backend_name(self) -> str:
        return "local"


# ── AWS S3 backend ────────────────────────────────────────────────────────────

class _S3Storage:
    """
    Stores files in an S3 bucket.

    Requires: boto3>=1.34 installed and valid AWS credentials.
    """

    def __init__(self, bucket: str, region: str) -> None:
        import boto3  # import here so boto3 is optional

        self._bucket = bucket
        self._region = region
        self._client = boto3.client(
            "s3",
            region_name=region,
            aws_access_key_id=getattr(settings, "AWS_ACCESS_KEY_ID", None),
            aws_secret_access_key=getattr(settings, "AWS_SECRET_ACCESS_KEY", None),
        )
        logger.info("S3Storage initialised: bucket=%s region=%s", bucket, region)

    async def save(self, data: bytes, key: str) -> str:
        self._client.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=BytesIO(data),
        )
        logger.debug("S3Storage uploaded %d bytes → s3://%s/%s", len(data), self._bucket, key)
        return key

    async def load(self, key: str) -> bytes:
        response = self._client.get_object(Bucket=self._bucket, Key=key)
        return response["Body"].read()

    def presigned_url(self, key: str, ttl: int = 3600) -> str:
        """Generate a presigned GET URL valid for *ttl* seconds."""
        return self._client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self._bucket, "Key": key},
            ExpiresIn=ttl,
        )

    def exists(self, key: str) -> bool:
        try:
            self._client.head_object(Bucket=self._bucket, Key=key)
            return True
        except self._client.exceptions.ClientError:
            return False

    async def delete(self, key: str) -> None:
        self._client.delete_object(Bucket=self._bucket, Key=key)
        logger.debug("S3Storage deleted s3://%s/%s", self._bucket, key)

    def backend_name(self) -> str:
        return "s3"


# ── Factory ────────────────────────────────────────────────────────────────────

def _build_storage():
    if getattr(settings, "S3_ENABLED", False):
        bucket = getattr(settings, "S3_BUCKET", "")
        region = getattr(settings, "S3_REGION", "ap-south-1")
        if not bucket:
            logger.warning("S3_ENABLED=true but S3_BUCKET not set — falling back to local storage")
        else:
            try:
                return _S3Storage(bucket=bucket, region=region)
            except Exception as exc:
                logger.warning("S3 initialisation failed (%s) — falling back to local storage", exc)

    return _LocalStorage(base_dir=settings.UPLOAD_DIR)


# Singleton used throughout the application
storage = _build_storage()
