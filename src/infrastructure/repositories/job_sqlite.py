"""
SQLite Transactional Job Repository (WAL Compatible)
Location: src/infrastructure/repositories/job_sqlite.py

Concurrency & Integrity Protocol:
1. Atomic claim: Queries oldest eligible PENDING job and executes an atomic conditional UPDATE.
2. Isolated transaction: Claim transaction is short and commits immediately upon lease acquisition.
   NO database transaction is held open during provider inference or CAS disk writes.
3. State transitions: Uses JobStateMachine to enforce transition validity before committing.
4. Optimistic Lease Recovery: Reclaim queries check original locked_at timestamp so active
   heartbeat updates cannot be stomped by concurrent sweeper runs.
"""

from datetime import datetime, timedelta, timezone
from typing import List, Optional
from sqlalchemy import func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.core.errors import EntityNotFoundException, LeaseExpiredException
from src.core.logging import logger
from src.domain.interfaces import JobClaim, JobCompletionData
from src.domain.models import JobStatus, StorageType
from src.domain.state_machine import JobStateMachine
from src.infrastructure.models import GeneratedAssetModel, GenerationJobModel
from src.infrastructure.repositories.base import BaseJobRepository


class SqliteJobRepository(BaseJobRepository):
    """
    SQLite-specialized job repository implementing transactional queue semantics.
    """

    async def claim_next_job(
        self,
        worker_id: str,
        lease_timeout_seconds: int = 120,
    ) -> Optional[JobClaim]:
        """
        Atomically leases the oldest pending job whose scheduled_at is ready.
        """
        now_utc = datetime.now(timezone.utc)

        async with self.session_factory() as session:
            async with session.begin():
                # 1. Identify candidate job ID: oldest PENDING job eligible right now
                candidate_stmt = (
                    select(GenerationJobModel.id)
                    .where(
                        GenerationJobModel.status == JobStatus.PENDING.value,
                        or_(
                            GenerationJobModel.scheduled_at.is_(None),
                            GenerationJobModel.scheduled_at <= now_utc,
                        ),
                    )
                    .order_by(GenerationJobModel.created_at.asc())
                    .limit(1)
                )
                res = await session.execute(candidate_stmt)
                candidate_id = res.scalar_one_or_none()

                if not candidate_id:
                    return None

                # 2. Atomic conditional lease update
                update_stmt = (
                    update(GenerationJobModel)
                    .where(
                        GenerationJobModel.id == candidate_id,
                        GenerationJobModel.status == JobStatus.PENDING.value,
                    )
                    .values(
                        status=JobStatus.PROCESSING.value,
                        locked_by=worker_id,
                        locked_at=now_utc,
                        started_at=func.coalesce(GenerationJobModel.started_at, now_utc),
                    )
                    .returning(
                        GenerationJobModel.id,
                        GenerationJobModel.prompt,
                        GenerationJobModel.negative_prompt,
                        GenerationJobModel.width,
                        GenerationJobModel.height,
                        GenerationJobModel.steps,
                        GenerationJobModel.seed,
                        GenerationJobModel.model,
                        GenerationJobModel.provider_requested,
                        GenerationJobModel.retry_count,
                        GenerationJobModel.max_retries,
                        GenerationJobModel.locked_by,
                        GenerationJobModel.locked_at,
                    )
                )
                update_res = await session.execute(update_stmt)
                row = update_res.fetchone()

                if not row:
                    # Raced with another worker; return None for next cycle
                    return None

                return self.map_row_to_claim(row)

    async def record_heartbeat(self, job_id: str, worker_id: str) -> bool:
        """
        Refreshes the lease timestamp for an active job held by worker_id.
        """
        now_utc = datetime.now(timezone.utc)
        async with self.session_factory() as session:
            async with session.begin():
                stmt = (
                    update(GenerationJobModel)
                    .where(
                        GenerationJobModel.id == job_id,
                        GenerationJobModel.status == JobStatus.PROCESSING.value,
                        GenerationJobModel.locked_by == worker_id,
                    )
                    .values(locked_at=now_utc)
                )
                res = await session.execute(stmt)
                return res.rowcount > 0

    async def complete_job(self, completion: JobCompletionData) -> None:
        """
        Marks job COMPLETED, records execution telemetry, and links the CAS asset.
        """
        now_utc = datetime.now(timezone.utc)

        async with self.session_factory() as session:
            async with session.begin():
                # 1. Fetch current job state
                stmt = select(GenerationJobModel).where(GenerationJobModel.id == completion.job_id)
                res = await session.execute(stmt)
                job = res.scalar_one_or_none()

                if not job:
                    raise EntityNotFoundException("GenerationJob", completion.job_id)

                # Check if already completed (Idempotency guarantee)
                if job.status == JobStatus.COMPLETED.value:
                    logger.info(f"Job {completion.job_id} is already completed. Idempotently returning.")
                    return

                # Validate state transition
                JobStateMachine.validate_transition(
                    JobStatus(job.status),
                    JobStatus.COMPLETED,
                    job_id=completion.job_id,
                )

                # Verify worker lease ownership
                if job.locked_by != completion.worker_id:
                    raise LeaseExpiredException(
                        job_id=completion.job_id,
                        message=f"Worker '{completion.worker_id}' lost lease on job {completion.job_id}. Current holder: '{job.locked_by}'",
                    )

                # 2. Attach asset metadata
                # Check if asset already exists for this job
                asset_stmt = select(GeneratedAssetModel).where(GeneratedAssetModel.job_id == completion.job_id)
                asset_res = await session.execute(asset_stmt)
                existing_asset = asset_res.scalar_one_or_none()

                if not existing_asset:
                    new_asset = GeneratedAssetModel(
                        id=completion.asset.asset_id,
                        job_id=completion.job_id,
                        storage_provider=StorageType.LOCAL_CAS.value,
                        file_hash=completion.asset.file_hash,
                        storage_path=completion.asset.storage_path,
                        public_url=completion.public_url,
                        file_size_bytes=completion.asset.file_size_bytes,
                        mime_type=completion.asset.mime_type,
                        width=completion.asset.width,
                        height=completion.asset.height,
                        is_synthetic=completion.asset.is_synthetic,
                        created_at=now_utc,
                    )
                    session.add(new_asset)

                # 3. Update job record
                job.status = JobStatus.COMPLETED.value
                job.provider_used = completion.provider_used
                job.execution_time_ms = completion.execution_time_ms
                job.peak_vram_mb = completion.peak_vram_mb
                job.completed_at = now_utc
                job.locked_by = None
                job.locked_at = None

    async def fail_job(
        self,
        job_id: str,
        worker_id: str,
        error_code: str,
        error_message: str,
        is_retryable: bool,
        backoff_seconds: Optional[float] = None,
    ) -> None:
        """
        Transitions job to PENDING with exponential backoff (if retries remain) or FAILED.
        """
        now_utc = datetime.now(timezone.utc)

        async with self.session_factory() as session:
            async with session.begin():
                stmt = select(GenerationJobModel).where(GenerationJobModel.id == job_id)
                res = await session.execute(stmt)
                job = res.scalar_one_or_none()

                if not job:
                    raise EntityNotFoundException("GenerationJob", job_id)

                if job.status in (JobStatus.COMPLETED.value, JobStatus.FAILED.value):
                    logger.warning(f"Cannot fail job {job_id} in terminal state '{job.status}'.")
                    return

                # Check if retryable and retry quota remains
                if is_retryable and (job.retry_count + 1) < job.max_retries:
                    JobStateMachine.validate_transition(
                        JobStatus(job.status),
                        JobStatus.PENDING,
                        job_id=job_id,
                    )
                    delay = backoff_seconds or 2.0
                    job.status = JobStatus.PENDING.value
                    job.retry_count += 1
                    job.scheduled_at = now_utc + timedelta(seconds=delay)
                    job.error_code = error_code
                    job.error_message = error_message
                    job.locked_by = None
                    job.locked_at = None
                    logger.info(
                        f"Job {job_id} rescheduled for retry {job.retry_count}/{job.max_retries} "
                        f"in {delay:.1f}s. Reason: {error_code} - {error_message}"
                    )
                else:
                    JobStateMachine.validate_transition(
                        JobStatus(job.status),
                        JobStatus.FAILED,
                        job_id=job_id,
                    )
                    job.status = JobStatus.FAILED.value
                    job.retry_count += 1
                    job.completed_at = now_utc
                    job.error_code = error_code
                    job.error_message = error_message
                    job.locked_by = None
                    job.locked_at = None
                    logger.error(
                        f"Job {job_id} permanently FAILED after {job.retry_count} attempts. "
                        f"Reason: {error_code} - {error_message}"
                    )

    async def reclaim_expired_leases(
        self,
        lease_timeout_seconds: int = 120,
        max_retries: int = 3,
        base_backoff_seconds: float = 2.0,
        max_backoff_seconds: float = 60.0,
    ) -> List[str]:
        """
        Reclaims stalled leases where locked_at exceeds lease_timeout_seconds.
        Uses optimistic concurrency control to prevent stomping renewed leases.
        """
        now_utc = datetime.now(timezone.utc)
        threshold = now_utc - timedelta(seconds=lease_timeout_seconds)
        reclaimed_ids: List[str] = []

        async with self.session_factory() as session:
            async with session.begin():
                # Query candidates
                stmt = select(
                    GenerationJobModel.id,
                    GenerationJobModel.retry_count,
                    GenerationJobModel.max_retries,
                    GenerationJobModel.locked_at,
                ).where(
                    GenerationJobModel.status == JobStatus.PROCESSING.value,
                    GenerationJobModel.locked_at < threshold,
                )
                res = await session.execute(stmt)
                stale_jobs = res.fetchall()

                for job_row in stale_jobs:
                    job_id, retry_count, job_max_retries, orig_locked_at = job_row
                    effective_max = job_max_retries or max_retries

                    if (retry_count + 1) < effective_max:
                        # Retryable: compute exponential backoff
                        delay = min(base_backoff_seconds * (2 ** retry_count), max_backoff_seconds)
                        reclaim_stmt = (
                            update(GenerationJobModel)
                            .where(
                                GenerationJobModel.id == job_id,
                                GenerationJobModel.status == JobStatus.PROCESSING.value,
                                GenerationJobModel.locked_at == orig_locked_at,  # Concurrency guard
                            )
                            .values(
                                status=JobStatus.PENDING.value,
                                retry_count=GenerationJobModel.retry_count + 1,
                                scheduled_at=now_utc + timedelta(seconds=delay),
                                error_code="LEASE_TIMEOUT_RETRY",
                                error_message=f"Worker lease expired after {lease_timeout_seconds}s. Rescheduled.",
                                locked_by=None,
                                locked_at=None,
                            )
                        )
                    else:
                        # Retries exhausted: mark terminal FAILED
                        reclaim_stmt = (
                            update(GenerationJobModel)
                            .where(
                                GenerationJobModel.id == job_id,
                                GenerationJobModel.status == JobStatus.PROCESSING.value,
                                GenerationJobModel.locked_at == orig_locked_at,  # Concurrency guard
                            )
                            .values(
                                status=JobStatus.FAILED.value,
                                retry_count=GenerationJobModel.retry_count + 1,
                                completed_at=now_utc,
                                error_code="LEASE_TIMEOUT_EXHAUSTED",
                                error_message=f"Lease timed out and maximum retries ({effective_max}) exhausted.",
                                locked_by=None,
                                locked_at=None,
                            )
                        )

                    rec_res = await session.execute(reclaim_stmt)
                    if rec_res.rowcount > 0:
                        reclaimed_ids.append(job_id)

        if reclaimed_ids:
            logger.info(f"Sweeper reclaimed {len(reclaimed_ids)} expired leases: {reclaimed_ids}")

        return reclaimed_ids

    async def get_job_by_id(self, job_id: str) -> Optional[GenerationJobModel]:
        """Inspection helper returning the full job model."""
        async with self.session_factory() as session:
            stmt = select(GenerationJobModel).where(GenerationJobModel.id == job_id)
            res = await session.execute(stmt)
            return res.scalar_one_or_none()
