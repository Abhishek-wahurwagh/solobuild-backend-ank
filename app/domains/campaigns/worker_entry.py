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
from arq.cron import cron

from app.core.config import settings
from app.domains.campaigns.worker import (
    # ── Dispatchers ────────────────────────────────────────────────────────
    dispatch_ingestion_batch,
    dispatch_campaign_screening,
    dispatch_campaign_calling,
    # ── Atomic workers ─────────────────────────────────────────────────────
    ingest_single_file,
    process_csv_chunk,
    screen_single_candidate,
    call_single_candidate,
    process_call_completion_job,
    # ── Maintenance ────────────────────────────────────────────────────────
    reconcile_zombie_tasks,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(name)-30s  %(levelname)-8s  %(message)s",
)


def _parse_redis_settings() -> RedisSettings:
    """Convert the application ``REDIS_URL`` into arq ``RedisSettings``."""
    return RedisSettings.from_dsn(settings.REDIS_URL)


class WorkerSettings:
    """Configuration object consumed by ``arq``."""

    functions = [
        # Dispatchers — fan out atomic tasks; complete in < 1 s
        dispatch_ingestion_batch,
        dispatch_campaign_screening,
        dispatch_campaign_calling,
        # Atomic workers — each processes exactly 1 item
        ingest_single_file,
        process_csv_chunk,
        screen_single_candidate,
        call_single_candidate,
        process_call_completion_job,
    ]

    cron_jobs = [
        # Re-queue candidates stuck IN_PROGRESS for > 2 h (zombie recovery)
        cron(reconcile_zombie_tasks, minute={0, 10, 20, 30, 40, 50}),
    ]

    redis_settings = _parse_redis_settings()

    # Raised from 5 → 20: atomic jobs are short-lived (seconds each),
    # so many more can be held concurrently inside a single event loop.
    max_jobs = 20

    # Dispatcher jobs complete in < 1 s; atomic jobs are bounded by a single
    # LLM/telephony call — 30 min is a safe upper limit.
    job_timeout = settings.ARQ_JOB_TIMEOUT_SECONDS

    # ARQ retries unexpected whole-worker failures; per-candidate retries fire
    # automatically because each atomic task raises on failure.
    max_tries = settings.WORKER_MAX_TRIES

    # Health-check key prefix
    health_check_key = "arq:solobuildai:health"
