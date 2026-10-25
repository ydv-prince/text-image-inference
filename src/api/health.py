"""
Health, Liveness and Readiness Endpoints
Location: src/api/health.py
"""

from datetime import datetime, timezone
import os
from fastapi import APIRouter, Response, status
from pydantic import BaseModel

from src.core.config import settings
from src.infrastructure.database import check_database_health

router = APIRouter(prefix="/health", tags=["Health"])


class LivenessResponse(BaseModel):
    status: str
    timestamp: str
    version: str
    environment: str


class ReadinessResponse(BaseModel):
    status: str
    database: str
    storage: str
    timestamp: str


@router.get("/liveness", response_model=LivenessResponse, status_code=status.HTTP_200_OK)
async def get_liveness():
    """
    Liveness probe.
    Confirms that the ASGI web process is running and responding to HTTP requests.
    """
    return LivenessResponse(
        status="alive",
        timestamp=datetime.now(timezone.utc).isoformat(),
        version=settings.APP_VERSION,
        environment=settings.ENVIRONMENT,
    )


@router.get("/readiness", response_model=ReadinessResponse)
async def get_readiness(response: Response):
    """
    Readiness probe.
    Verifies that all essential dependencies (database connectivity and storage access)
    are operational before routing traffic.
    """
    # 1. Check database connectivity
    db_healthy = await check_database_health()
    db_status = "connected" if db_healthy else "disconnected"

    # 2. Check storage directory accessibility and writability
    storage_status = "accessible"
    try:
        storage_path = settings.STORAGE_BASE_PATH
        storage_path.mkdir(parents=True, exist_ok=True)
        # Test write and delete permissions
        test_file = storage_path / ".healthcheck_tmp"
        test_file.write_text("ok", encoding="utf-8")
        test_file.unlink(missing_ok=True)
    except Exception:
        storage_status = "unwritable"

    overall_ready = db_healthy and (storage_status == "accessible")

    if not overall_ready:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        system_status = "not_ready"
    else:
        response.status_code = status.HTTP_200_OK
        system_status = "ready"

    return ReadinessResponse(
        status=system_status,
        database=db_status,
        storage=storage_status,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )
