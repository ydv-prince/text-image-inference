"""
Worker Ingress API Endpoints (Mode B)
Location: src/api/v1/worker.py

Security & Operational Guarantees:
1. Authenticated strictly via Authorization: Bearer <WORKER_SHARED_SECRET>.
2. Lease ownership enforced: Only the worker holding the lease can heartbeat, complete, or fail the job.
3. Idempotent completion & failure transitions.
4. Non-blocking concurrency via repository leasing.
"""

from datetime import datetime, timezone
from typing import Optional
import uuid
from fastapi import APIRouter, Depends, Response, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.api.v1.worker_schemas import (
    WorkerCompleteRequest,
    WorkerCompleteResponse,
    WorkerFailRequest,
    WorkerFailResponse,
    WorkerHeartbeatRequest,
    WorkerHeartbeatResponse,
    WorkerLeaseRequest,
    WorkerLeaseResponse,
)
from src.api.worker_auth import get_authenticated_worker
from src.core.errors import (
    EntityNotFoundException,
    LeaseExpiredException,
    LeaseOwnershipConflictException,
)
from src.core.logging import logger
from src.domain.interfaces import JobCompletionData, StoredAsset
from src.domain.models import JobStatus
from src.infrastructure.database import get_session_factory
from src.infrastructure.models import GeneratedAssetModel, GenerationJobModel
from src.infrastructure.repositories import get_job_repository

router = APIRouter(prefix="/worker", tags=["Worker Ingress"])


@router.post(
    "/jobs/lease",
    response_model=Optional[WorkerLeaseResponse],
    summary="Lease next eligible pending job",
)
async def lease_job(
    req: WorkerLeaseRequest,
    response: Response,
    _auth: bool = Depends(get_authenticated_worker),
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
):
    """
    Leases the next unreserved pending job for the requesting worker.
    Returns HTTP 200 with job details, or HTTP 204 if queue is empty.
    """
    repo = get_job_repository(session_factory)
    claim = await repo.claim_next_job(
        worker_id=req.worker_id,
        lease_timeout_seconds=req.lease_timeout_seconds,
    )

    if not claim:
        response.status_code = status.HTTP_204_NO_CONTENT
        return None

    return WorkerLeaseResponse(
        job_id=claim.job_id,
        prompt=claim.prompt,
        negative_prompt=claim.negative_prompt,
        width=claim.width,
        height=claim.height,
        steps=claim.steps,
        seed=claim.seed,
        model=claim.model,
        provider_requested=claim.provider_requested,
        retry_count=claim.retry_count,
        max_retries=claim.max_retries,
        locked_at=claim.locked_at,
        lease_timeout_seconds=req.lease_timeout_seconds,
    )


@router.post(
    "/jobs/{job_id}/heartbeat",
    response_model=WorkerHeartbeatResponse,
    summary="Record worker heartbeat and renew lease",
)
async def heartbeat_job(
    job_id: str,
    req: WorkerHeartbeatRequest,
    _auth: bool = Depends(get_authenticated_worker),
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
):
    """
    Refreshes the lease timestamp for an actively held job.
    Rejects foreign or expired workers with HTTP 409.
    """
    repo = get_job_repository(session_factory)
    renewed = await repo.record_heartbeat(job_id=job_id, worker_id=req.worker_id)

    if not renewed:
        # Check why heartbeat failed
        async with session_factory() as session:
            stmt = select(GenerationJobModel).where(GenerationJobModel.id == job_id)
            res = await session.execute(stmt)
            job = res.scalar_one_or_none()
            if not job:
                raise EntityNotFoundException("GenerationJob", job_id)
            if job.locked_by != req.worker_id:
                raise LeaseOwnershipConflictException(
                    job_id=job_id,
                    worker_id=req.worker_id,
                    message=f"Worker '{req.worker_id}' does not own lease on job {job_id}. Current holder: '{job.locked_by}'",
                )
            raise LeaseExpiredException(
                job_id=job_id,
                message=f"Lease on job {job_id} is no longer active (status: {job.status}).",
            )

    return WorkerHeartbeatResponse(
        status="renewed",
        job_id=job_id,
        locked_at=datetime.now(timezone.utc),
    )


@router.post(
    "/jobs/{job_id}/complete",
    response_model=WorkerCompleteResponse,
    summary="Report job completion and attach generated asset",
)
async def complete_job(
    job_id: str,
    req: WorkerCompleteRequest,
    _auth: bool = Depends(get_authenticated_worker),
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
):
    """
    Transitions leased job to COMPLETED and stores generated asset metadata.
    Idempotent: Duplicate calls with matching asset return HTTP 200.
    """
    async with session_factory() as session:
        # Check existing job
        stmt = select(GenerationJobModel).where(GenerationJobModel.id == job_id)
        res = await session.execute(stmt)
        job = res.scalar_one_or_none()

        if not job:
            raise EntityNotFoundException("GenerationJob", job_id)

        # Idempotency check: Already completed?
        if job.status == JobStatus.COMPLETED.value:
            asset_stmt = select(GeneratedAssetModel).where(GeneratedAssetModel.job_id == job_id)
            asset_res = await session.execute(asset_stmt)
            existing_asset = asset_res.scalar_one_or_none()
            if existing_asset and existing_asset.file_hash == req.file_hash:
                return WorkerCompleteResponse(
                    status="completed",
                    job_id=job_id,
                    asset_id=existing_asset.id,
                )

        # Lease ownership check
        if job.locked_by != req.worker_id and job.status != JobStatus.COMPLETED.value:
            raise LeaseOwnershipConflictException(
                job_id=job_id,
                worker_id=req.worker_id,
                message=f"Worker '{req.worker_id}' lost lease on job {job_id}. Current holder: '{job.locked_by}'",
            )

    # Perform repository completion
    stored_asset = StoredAsset(
        asset_id=str(uuid.uuid4()),
        file_hash=req.file_hash,
        storage_path=req.storage_path,
        file_size_bytes=req.file_size_bytes,
        mime_type=req.mime_type,
        width=req.width,
        height=req.height,
        is_synthetic=(req.provider_used == "mock"),
    )

    completion_data = JobCompletionData(
        job_id=job_id,
        worker_id=req.worker_id,
        provider_used=req.provider_used,
        execution_time_ms=req.execution_time_ms,
        peak_vram_mb=req.peak_vram_mb,
        asset=stored_asset,
        public_url=req.public_url,
    )

    repo = get_job_repository(session_factory)
    await repo.complete_job(completion_data)

    return WorkerCompleteResponse(
        status="completed",
        job_id=job_id,
        asset_id=stored_asset.asset_id,
    )


@router.post(
    "/jobs/{job_id}/fail",
    response_model=WorkerFailResponse,
    summary="Report job execution failure",
)
async def fail_job(
    job_id: str,
    req: WorkerFailRequest,
    _auth: bool = Depends(get_authenticated_worker),
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
):
    """
    Transitions job to PENDING with exponential backoff if retries remain,
    or permanently to FAILED if retries are exhausted.
    """
    async with session_factory() as session:
        stmt = select(GenerationJobModel).where(GenerationJobModel.id == job_id)
        res = await session.execute(stmt)
        job = res.scalar_one_or_none()

        if not job:
            raise EntityNotFoundException("GenerationJob", job_id)

        # Verify lease ownership unless already terminal
        if job.status not in (JobStatus.COMPLETED.value, JobStatus.FAILED.value):
            if job.locked_by != req.worker_id:
                raise LeaseOwnershipConflictException(
                    job_id=job_id,
                    worker_id=req.worker_id,
                    message=f"Worker '{req.worker_id}' does not own lease on job {job_id}.",
                )

        current_retry = job.retry_count
        max_retries = job.max_retries

    repo = get_job_repository(session_factory)
    is_retryable = req.error_code not in ("INVALID_PAYLOAD", "PROMPT_BLOCKED")
    await repo.fail_job(
        job_id=job_id,
        worker_id=req.worker_id,
        error_code=req.error_code,
        error_message=req.error_message,
        is_retryable=is_retryable,
        backoff_seconds=float(req.retry_delay_seconds) if req.retry_delay_seconds else None,
    )

    new_status = "retrying" if (is_retryable and (current_retry + 1) < max_retries) else "failed"

    return WorkerFailResponse(
        status=new_status,
        job_id=job_id,
        retry_count=current_retry + 1,
        max_retries=max_retries,
    )
