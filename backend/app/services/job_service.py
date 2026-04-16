"""
Background job manager for long-running analysis tasks.

Jobs are stored in-memory (dict) and run via FastAPI BackgroundTasks.
For production use, replace with Celery/Redis or a database-backed queue.

Usage:
    from app.services.job_service import job_manager, JobStatus

    job_id = job_manager.create("profile", dataset_id="abc")
    # hand job_id back to the client immediately

    # In the background task:
    job_manager.start(job_id)
    try:
        result = do_work()
        job_manager.finish(job_id, result)
    except Exception as e:
        job_manager.fail(job_id, str(e))
"""

import logging
import uuid
from datetime import datetime
from enum import Enum
from threading import Lock
from typing import Any, Dict, Optional

from app.utils.time_utils import now_ist

logger = logging.getLogger(__name__)

_MAX_JOBS = 500  # keep at most this many completed/failed jobs


class JobStatus(str, Enum):
    PENDING   = "pending"
    RUNNING   = "running"
    COMPLETED = "completed"
    FAILED    = "failed"


class JobManager:
    """In-memory job registry."""

    def __init__(self) -> None:
        self._jobs: Dict[str, Dict[str, Any]] = {}
        self._lock = Lock()

    def create(self, job_type: str, **meta) -> str:
        """Register a new job and return its ID."""
        job_id = str(uuid.uuid4())
        with self._lock:
            self._jobs[job_id] = {
                "id": job_id,
                "type": job_type,
                "status": JobStatus.PENDING,
                "created_at": now_ist().isoformat(),
                "started_at": None,
                "finished_at": None,
                "result": None,
                "error": None,
                **meta,
            }
            self._prune()
        logger.info("Job CREATED  %s (%s)", job_id, job_type)
        return job_id

    def start(self, job_id: str) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job:
                job["status"] = JobStatus.RUNNING
                job["started_at"] = now_ist().isoformat()

    def finish(self, job_id: str, result: Any) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job:
                job["status"] = JobStatus.COMPLETED
                job["result"] = result
                job["finished_at"] = now_ist().isoformat()
        logger.info("Job COMPLETED %s", job_id)

    def fail(self, job_id: str, error: str) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job:
                job["status"] = JobStatus.FAILED
                job["error"] = error
                job["finished_at"] = now_ist().isoformat()
        logger.warning("Job FAILED    %s : %s", job_id, error)

    def get(self, job_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            return dict(self._jobs[job_id]) if job_id in self._jobs else None

    def _prune(self) -> None:
        """Evict oldest completed/failed jobs when the registry is too large."""
        if len(self._jobs) <= _MAX_JOBS:
            return
        terminal = [
            jid for jid, j in self._jobs.items()
            if j["status"] in (JobStatus.COMPLETED, JobStatus.FAILED)
        ]
        # remove oldest first (dict preserves insertion order in Python 3.7+)
        for jid in terminal[: len(self._jobs) - _MAX_JOBS]:
            del self._jobs[jid]


# Singleton used by all routes
job_manager = JobManager()

