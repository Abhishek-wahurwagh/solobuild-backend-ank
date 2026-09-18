"""
ARQ worker entry point.

Start with::

    arq app.domains.campaigns.worker_entry.WorkerSettings

Or during development::

    python -m arq app.domains.campaigns.worker_entry.WorkerSettings
"""

from __future__ import annotations

import logging

from arq.connections import RedisSettings

from app.core.config import settings
from app.domains.campaigns.worker import process_document_upload_batch, screen_campaign_candidates

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(name)-30s  %(levelname)-8s  %(message)s",
)


def _parse_redis_settings() -> RedisSettings:
    """Convert the application ``REDIS_URL`` into arq ``RedisSettings``."""
    return RedisSettings.from_dsn(settings.REDIS_URL)


class WorkerSettings:
    """Configuration object consumed by ``arq``."""

    functions = [process_document_upload_batch, screen_campaign_candidates]
    redis_settings = _parse_redis_settings()

    # Concurrency: how many jobs run simultaneously
    max_jobs = 5

    # Batch timeout is separate from the per-request LLM timeout.
    job_timeout = settings.ARQ_JOB_TIMEOUT_SECONDS

    # ARQ retries unexpected whole-worker failures; per-file retries are separate.
    max_tries = settings.WORKER_MAX_TRIES

    # Health-check key prefix
    health_check_key = "arq:solobuildai:health"
