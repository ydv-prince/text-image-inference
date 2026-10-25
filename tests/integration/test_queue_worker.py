"""
Integration Tests for Job Queue, Worker Loop & Asset Storage
Location: tests/integration/test_queue_worker.py
"""

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.core.errors import InvalidStateTransitionException
from src.core.security import hash_api_key
from src.domain.interfaces import JobCompletionData, StoredAsset
from src.domain.models import ApiKeyTier, JobStatus, ProviderType, StorageType
from src.infrastructure.models import ApiKeyModel, GeneratedAssetModel, GenerationJobModel
from src.infrastructure.providers.mock import MockProviderAdapter
from src.infrastructure.providers.registry import provider_registry
from src.infrastructure.repositories.job_sqlite import SqliteJobRepository
from src.infrastructure.storage.local_cas import LocalStorageCAS
from src.services.lease_sweeper import LeaseSweeper
from src.services.queue_worker import QueueWorker


async def create_test_api_key(session_factory: async_sessionmaker[AsyncSession]) -> str:
    """Helper creating a test API key returning its ID."""
    async with session_factory() as session:
        async with session.begin():
            key = ApiKeyModel(
                key_hash=hash_api_key("integration_test_key", "test_salt"),
                client_name="Integration-Test-Client",
                tier=ApiKeyTier.FREE.value,
            )
            session.add(key)
        return key.id


async def create_test_job(
    session_factory: async_sessionmaker[AsyncSession],
    api_key_id: str,
    prompt: str = "A peaceful zen garden in spring",
    width: int = 512,
    height: int = 512,
    steps: int = 4,
    seed: int = 42,
    provider_requested: str = "mock",
    status: str = JobStatus.PENDING.value,
    retry_count: int = 0,
    max_retries: int = 3,
    locked_by: str = None,
    locked_at: datetime = None,
    scheduled_at: datetime = None,
) -> str:
    """Helper creating a test generation job returning its ID."""
    async with session_factory() as session:
        async with session.begin():
            job = GenerationJobModel(
                api_key_id=api_key_id,
                payload_hash="integration_test_payload_hash",
                prompt=prompt,
                width=width,
                height=height,
                steps=steps,
                seed=seed,
                model="sd-1.5",
                provider_requested=provider_requested,
                status=status,
                retry_count=retry_count,
                max_retries=max_retries,
                locked_by=locked_by,
                locked_at=locked_at,
                scheduled_at=scheduled_at,
            )
            session.add(job)
        return job.id


# -----------------------------------------------------------------------------
# 1. Pending job can be claimed and completed
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_pending_job_claimed_and_completed(session_factory, tmp_path: Path):
    repo = SqliteJobRepository(session_factory)
    storage = LocalStorageCAS(base_path=tmp_path)
    worker = QueueWorker(repository=repo, storage=storage, worker_id="worker-test-1")

    api_key_id = await create_test_api_key(session_factory)
    job_id = await create_test_job(session_factory, api_key_id)

    # Execute one job
    processed = await worker.process_one_job()
    assert processed is True

    # Inspect persisted job state
    job = await repo.get_job_by_id(job_id)
    assert job.status == JobStatus.COMPLETED.value
    assert job.provider_used == "mock"
    assert job.execution_time_ms >= 0
    assert job.completed_at is not None
    assert job.locked_by is None
    assert job.locked_at is None

    # Inspect persisted asset record
    async with session_factory() as session:
        stmt = select(GeneratedAssetModel).where(GeneratedAssetModel.job_id == job_id)
        res = await session.execute(stmt)
        asset = res.scalar_one_or_none()

    assert asset is not None
    assert asset.mime_type == "image/png"
    assert asset.width == 512
    assert asset.height == 512
    assert asset.is_synthetic is True  # Golden requirement: synthetic labeling

    # Inspect CAS disk file
    assert await storage.verify_asset(asset.file_hash) is True


# -----------------------------------------------------------------------------
# 2. Two concurrent workers cannot claim the same active lease
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_concurrent_worker_leasing_mutual_exclusion(session_factory, tmp_path: Path):
    repo = SqliteJobRepository(session_factory)
    storage = LocalStorageCAS(base_path=tmp_path)

    worker_a = QueueWorker(repository=repo, storage=storage, worker_id="worker-A")
    worker_b = QueueWorker(repository=repo, storage=storage, worker_id="worker-B")

    api_key_id = await create_test_api_key(session_factory)
    job_id = await create_test_job(session_factory, api_key_id)

    # Launch both workers concurrently to claim the single job
    res_a, res_b = await asyncio.gather(
        worker_a.process_one_job(),
        worker_b.process_one_job(),
    )

    # Exactly one worker processed the job, the other got an empty queue
    assert (res_a is True and res_b is False) or (res_a is False and res_b is True)

    job = await repo.get_job_by_id(job_id)
    assert job.status == JobStatus.COMPLETED.value


# -----------------------------------------------------------------------------
# 3. Invalid status transitions are rejected
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_invalid_status_transitions_rejected(session_factory, tmp_path: Path):
    repo = SqliteJobRepository(session_factory)
    storage = LocalStorageCAS(base_path=tmp_path)

    api_key_id = await create_test_api_key(session_factory)
    job_id = await create_test_job(session_factory, api_key_id, status=JobStatus.PENDING.value)

    # Attempting to directly complete a PENDING job without claiming it must fail
    bogus_completion = JobCompletionData(
        job_id=job_id,
        worker_id="rogue-worker",
        provider_used="mock",
        execution_time_ms=10,
        peak_vram_mb=0,
        asset=StoredAsset(
            asset_id="dummy",
            file_hash="dummy_hash",
            storage_path="dummy_path",
            file_size_bytes=100,
            mime_type="image/png",
            width=512,
            height=512,
        ),
    )

    with pytest.raises(InvalidStateTransitionException):
        await repo.complete_job(bogus_completion)


# -----------------------------------------------------------------------------
# 4. Provider failure follows retry policy with exponential backoff
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_provider_failure_follows_retry_policy(session_factory, tmp_path: Path):
    repo = SqliteJobRepository(session_factory)
    storage = LocalStorageCAS(base_path=tmp_path)
    worker = QueueWorker(repository=repo, storage=storage, worker_id="worker-retry-test")

    api_key_id = await create_test_api_key(session_factory)
    job_id = await create_test_job(session_factory, api_key_id)

    # Inject failure into mock provider
    mock_prov = provider_registry.get_provider("mock")
    mock_prov.set_fail_next(True)

    before_time = datetime.now(timezone.utc)
    processed = await worker.process_one_job()
    assert processed is True

    job = await repo.get_job_by_id(job_id)
    assert job.status == JobStatus.PENDING.value  # Re-queued for retry
    assert job.retry_count == 1
    assert job.scheduled_at is not None
    scheduled_utc = job.scheduled_at.replace(tzinfo=timezone.utc) if job.scheduled_at.tzinfo is None else job.scheduled_at
    assert scheduled_utc >= before_time
    assert job.error_code == "INFERENCE_FAILED"
    assert "Injected mock provider failure" in job.error_message
    assert job.locked_by is None
    assert job.locked_at is None


# -----------------------------------------------------------------------------
# 5. Retry exhaustion produces a terminal failure
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_retry_exhaustion_produces_terminal_failure(session_factory, tmp_path: Path):
    repo = SqliteJobRepository(session_factory)
    storage = LocalStorageCAS(base_path=tmp_path)
    worker = QueueWorker(repository=repo, storage=storage, worker_id="worker-exhaustion-test")

    api_key_id = await create_test_api_key(session_factory)
    # Job at attempt 2 of max 3 (next attempt will be 3rd, exhausting retries)
    job_id = await create_test_job(session_factory, api_key_id, retry_count=2, max_retries=3)

    mock_prov = provider_registry.get_provider("mock")
    mock_prov.set_fail_next(True)

    processed = await worker.process_one_job()
    assert processed is True

    job = await repo.get_job_by_id(job_id)
    assert job.status == JobStatus.FAILED.value  # Terminal failure!
    assert job.retry_count == 3
    assert job.completed_at is not None
    assert job.locked_by is None


# -----------------------------------------------------------------------------
# 6. Expired leases can be recovered by sweeper
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_expired_lease_recovered_by_sweeper(session_factory):
    repo = SqliteJobRepository(session_factory)
    sweeper = LeaseSweeper(repository=repo, lease_timeout_seconds=30, max_retries=3)

    api_key_id = await create_test_api_key(session_factory)
    now_utc = datetime.now(timezone.utc)
    stale_time = now_utc - timedelta(seconds=100)  # Expired > 30s

    job_id = await create_test_job(
        session_factory,
        api_key_id,
        status=JobStatus.PROCESSING.value,
        locked_by="crashed-worker",
        locked_at=stale_time,
        retry_count=0,
    )

    reclaimed = await sweeper.sweep_once()
    assert job_id in reclaimed

    job = await repo.get_job_by_id(job_id)
    assert job.status == JobStatus.PENDING.value
    assert job.retry_count == 1
    assert job.scheduled_at is not None
    assert job.locked_by is None
    assert job.error_code == "LEASE_TIMEOUT_RETRY"


# -----------------------------------------------------------------------------
# 7. A renewed/replaced lease is not incorrectly reclaimed
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_renewed_lease_not_incorrectly_reclaimed(session_factory):
    repo = SqliteJobRepository(session_factory)
    sweeper = LeaseSweeper(repository=repo, lease_timeout_seconds=30, max_retries=3)

    api_key_id = await create_test_api_key(session_factory)
    now_utc = datetime.now(timezone.utc)

    # Active job with recent heartbeat (within 30s)
    job_id = await create_test_job(
        session_factory,
        api_key_id,
        status=JobStatus.PROCESSING.value,
        locked_by="healthy-worker",
        locked_at=now_utc - timedelta(seconds=10),
    )

    reclaimed = await sweeper.sweep_once()
    assert job_id not in reclaimed

    job = await repo.get_job_by_id(job_id)
    assert job.status == JobStatus.PROCESSING.value
    assert job.locked_by == "healthy-worker"


# -----------------------------------------------------------------------------
# 8. Identical PNG bytes produce the same digest and stored asset
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_identical_jobs_deduplicate_in_cas(session_factory, tmp_path: Path):
    repo = SqliteJobRepository(session_factory)
    storage = LocalStorageCAS(base_path=tmp_path)
    worker = QueueWorker(repository=repo, storage=storage)

    api_key_id = await create_test_api_key(session_factory)
    # Two separate jobs with identical parameters
    job1_id = await create_test_job(session_factory, api_key_id, prompt="Identical prompt")
    job2_id = await create_test_job(session_factory, api_key_id, prompt="Identical prompt")

    await worker.process_one_job()
    await worker.process_one_job()

    async with session_factory() as session:
        stmt1 = select(GeneratedAssetModel).where(GeneratedAssetModel.job_id == job1_id)
        stmt2 = select(GeneratedAssetModel).where(GeneratedAssetModel.job_id == job2_id)
        asset1 = (await session.execute(stmt1)).scalar_one()
        asset2 = (await session.execute(stmt2)).scalar_one()

    # Content-addressable deduplication: identical file hash and storage path
    assert asset1.file_hash == asset2.file_hash
    assert asset1.storage_path == asset2.storage_path


# -----------------------------------------------------------------------------
# 9. Deterministic invalid request fails immediately without infinite retry
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_deterministic_invalid_request_fails_immediately(session_factory, tmp_path: Path):
    repo = SqliteJobRepository(session_factory)
    storage = LocalStorageCAS(base_path=tmp_path)
    worker = QueueWorker(repository=repo, storage=storage)

    api_key_id = await create_test_api_key(session_factory)
    # Width 500 is not a multiple of 64 (deterministic validation failure)
    job_id = await create_test_job(session_factory, api_key_id, width=500)

    processed = await worker.process_one_job()
    assert processed is True

    job = await repo.get_job_by_id(job_id)
    assert job.status == JobStatus.FAILED.value
    assert job.error_code == "INVALID_PAYLOAD"
    assert "must be multiples of 64" in job.error_message


# -----------------------------------------------------------------------------
# 10. Crash recovery between asset storage and job completion
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_crash_recovery_between_asset_storage_and_db_commit(session_factory, tmp_path: Path):
    repo = SqliteJobRepository(session_factory)
    storage = LocalStorageCAS(base_path=tmp_path)
    sweeper = LeaseSweeper(repository=repo, lease_timeout_seconds=1)

    api_key_id = await create_test_api_key(session_factory)
    job_id = await create_test_job(session_factory, api_key_id)

    # Worker 1 leases job and stores asset into CAS, then "crashes" before completing DB record
    claim = await repo.claim_next_job("worker-crashed", lease_timeout_seconds=1)
    assert claim is not None

    mock_prov = provider_registry.get_provider("mock")
    from src.domain.interfaces import ProviderRequest
    resp = await mock_prov.generate_image(ProviderRequest(prompt=claim.prompt))
    
    # Write asset to disk
    stored_asset = await storage.store_image(resp.image_bytes, is_synthetic=True)
    assert await storage.exists(stored_asset.file_hash) is True

    # "Worker crashed" - wait for lease to expire
    await asyncio.sleep(1.1)

    # Sweeper reclaims stalled job
    reclaimed = await sweeper.sweep_once()
    assert job_id in reclaimed

    # Reset scheduled_at to now for immediate pickup by recovery worker
    async with session_factory() as session:
        async with session.begin():
            job = await session.get(GenerationJobModel, job_id)
            job.scheduled_at = None

    # Worker 2 picks up job, generates, hits CAS cache, and completes job safely!
    worker_2 = QueueWorker(repository=repo, storage=storage, worker_id="worker-recovery")
    processed = await worker_2.process_one_job()
    assert processed is True

    completed_job = await repo.get_job_by_id(job_id)
    assert completed_job.status == JobStatus.COMPLETED.value


# -----------------------------------------------------------------------------
# 11. Worker shutdown/cancellation does not leave corrupted state
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_worker_shutdown_and_cancellation(session_factory, tmp_path: Path):
    repo = SqliteJobRepository(session_factory)
    storage = LocalStorageCAS(base_path=tmp_path)
    worker = QueueWorker(repository=repo, storage=storage, poll_interval=0.1)

    task = asyncio.create_task(worker.run())
    await asyncio.sleep(0.05)

    # Stop via event
    worker.stop()
    await task  # Should complete cleanly without error


@pytest.mark.asyncio
async def test_record_heartbeat_extends_lease(session_factory):
    repo = SqliteJobRepository(session_factory)
    api_key_id = await create_test_api_key(session_factory)
    now_utc = datetime.now(timezone.utc)
    old_time = now_utc - timedelta(seconds=60)

    job_id = await create_test_job(
        session_factory,
        api_key_id,
        status=JobStatus.PROCESSING.value,
        locked_by="worker-heartbeat",
        locked_at=old_time,
    )

    success = await repo.record_heartbeat(job_id, "worker-heartbeat")
    assert success is True

    job = await repo.get_job_by_id(job_id)
    # Check that locked_at was updated to a newer timestamp
    job_locked_utc = job.locked_at.replace(tzinfo=timezone.utc) if job.locked_at.tzinfo is None else job.locked_at
    assert job_locked_utc > old_time

    # Heartbeat from unauthorized worker fails
    failure = await repo.record_heartbeat(job_id, "other-worker")
    assert failure is False


@pytest.mark.asyncio
async def test_sweeper_exhausts_retries_to_failed(session_factory):
    repo = SqliteJobRepository(session_factory)
    sweeper = LeaseSweeper(repository=repo, lease_timeout_seconds=30, max_retries=3)

    api_key_id = await create_test_api_key(session_factory)
    now_utc = datetime.now(timezone.utc)
    stale_time = now_utc - timedelta(seconds=100)

    # Job already at 2 retries (next attempt will hit max_retries=3)
    job_id = await create_test_job(
        session_factory,
        api_key_id,
        status=JobStatus.PROCESSING.value,
        locked_by="dead-worker",
        locked_at=stale_time,
        retry_count=2,
        max_retries=3,
    )

    reclaimed = await sweeper.sweep_once()
    assert job_id in reclaimed

    job = await repo.get_job_by_id(job_id)
    assert job.status == JobStatus.FAILED.value
    assert job.error_code == "LEASE_TIMEOUT_EXHAUSTED"
    assert job.completed_at is not None
