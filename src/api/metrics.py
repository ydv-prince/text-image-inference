"""
Prometheus Metrics Exposition Endpoint
Location: src/api/metrics.py

Guarantees:
1. Standard Prometheus v0.0.4 text exposition format.
2. Cached DB Sampling (5s TTL): Avoids expensive database load on rapid Prometheus scrapes.
3. Resilient Fallback: Database disconnects or query errors do not fail the scrape.
4. Optional Access Control: Supports METRICS_REQUIRE_AUTH for secure production deployments.
"""

import asyncio
import secrets
import time
from typing import Optional
from fastapi import APIRouter, Header, Response, status
from sqlalchemy import func, select

from src.core.config import settings
from src.core.errors import AuthenticationException
from src.core.logging import logger
from src.core.metrics import (
    deletion_outbox_queue_depth,
    metrics_registry,
    queue_depth_gauge,
)
from src.infrastructure.database import async_session_factory
from src.infrastructure.models import GenerationJobModel, StorageDeletionOutboxModel

router = APIRouter(tags=["Observability"])

# Lock and timestamp for throttling database sampling across concurrent scrape requests
_sample_lock = asyncio.Lock()
_last_sample_timestamp: float = 0.0

ALL_JOB_STATUSES = ("PENDING", "PROCESSING", "COMPLETED", "FAILED", "CANCELLED", "TIMED_OUT")
ALL_OUTBOX_STATUSES = ("PENDING", "PROCESSING", "COMPLETED", "FAILED", "EXPIRED")


async def _refresh_database_metrics_if_expired(force: bool = False) -> None:
    """
    Refreshes queue depth and deletion outbox depth metrics from the database
    if the cache TTL has elapsed. Throttled via _sample_lock to prevent thundering herds.
    Guarantees:
    - Bounded execution (timeout <= cache_ttl).
    - Preserves existing telemetry on timeout or query failure.
    - Re-entrant and thread-safe.
    """
    global _last_sample_timestamp

    now = time.time()
    cache_ttl = getattr(settings, "METRICS_CACHE_TTL_SECONDS", 5.0)

    if not force and (now - _last_sample_timestamp) < cache_ttl:
        return

    async def _execute_sampling() -> None:
        global _last_sample_timestamp
        async with _sample_lock:
            # Double-check inside lock
            inner_now = time.time()
            if not force and (inner_now - _last_sample_timestamp) < cache_ttl:
                return

            try:
                async with async_session_factory() as session:
                    # 1. Sample Generation Queue Depth by status
                    job_stmt = select(
                        GenerationJobModel.status,
                        func.count(GenerationJobModel.id),
                    ).group_by(GenerationJobModel.status)
                    job_res = await session.execute(job_stmt)
                    counts_by_status = {s: 0 for s in ALL_JOB_STATUSES}
                    for s_val, count in job_res.all():
                        counts_by_status[str(s_val)] = count

                    for s_name, count in counts_by_status.items():
                        queue_depth_gauge.set(count, labels={"status": s_name})

                    # 2. Sample Deletion Outbox Queue Depth by status
                    outbox_stmt = select(
                        StorageDeletionOutboxModel.status,
                        func.count(StorageDeletionOutboxModel.id),
                    ).group_by(StorageDeletionOutboxModel.status)
                    outbox_res = await session.execute(outbox_stmt)
                    outbox_counts = {s: 0 for s in ALL_OUTBOX_STATUSES}
                    for s_val, count in outbox_res.all():
                        outbox_counts[str(s_val)] = count

                    for s_name, count in outbox_counts.items():
                        deletion_outbox_queue_depth.set(count, labels={"status": s_name})

                _last_sample_timestamp = time.time()

            except Exception as e:
                # Observability sampling failure must never take down the scrape endpoint
                # or destroy previously cached telemetry.
                logger.warning(f"Failed to sample database depth metrics for Prometheus scrape: {e}")

    try:
        await asyncio.wait_for(_execute_sampling(), timeout=max(1.0, cache_ttl))
    except (asyncio.TimeoutError, TimeoutError):
        logger.warning("Database metrics sampling timed out; retaining previously cached metrics.")


@router.get(
    "/metrics",
    summary="Prometheus metrics exposition endpoint",
    response_class=Response,
    status_code=status.HTTP_200_OK,
)
async def get_prometheus_metrics(
    authorization: Optional[str] = Header(None),
    x_admin_api_key: Optional[str] = Header(None, alias="X-Admin-API-Key"),
):
    """
    Exposes process-level, queue, worker, deletion, and reconciliation telemetry
    in standard Prometheus v0.0.4 text format.
    """
    if not getattr(settings, "METRICS_ENABLED", True):
        return Response(
            content="Metrics exposition is disabled.\n",
            status_code=status.HTTP_404_NOT_FOUND,
            media_type="text/plain",
        )

    # Optional Admin Authorization Check
    if getattr(settings, "METRICS_REQUIRE_AUTH", False):
        token = x_admin_api_key
        if not token and authorization:
            if authorization.startswith("Bearer "):
                token = authorization[7:].strip()
            else:
                token = authorization.strip()

        expected = settings.ADMIN_API_KEY.get_secret_value() if settings.ADMIN_API_KEY else None
        if not token or not expected or not secrets.compare_digest(token, expected):
            raise AuthenticationException(
                "Administrative authorization required to access metrics endpoint."
            )

    # Refresh throttled DB metrics
    await _refresh_database_metrics_if_expired()

    # Export Prometheus formatted text
    body = metrics_registry.export_prometheus_text()
    return Response(
        content=body,
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )
