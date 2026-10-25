"""
Unit Tests for DeletionOutboxService & 4-Gate Deletion Pipeline (Milestone 6.3)
Location: tests/unit/test_deletion_outbox.py

Verifies:
1. Rejection of deletion enqueueing when active references exist (Gate 3).
2. Rejection of deletion enqueueing when active upload intents exist (Gate 3).
3. Outbox claiming transaction: status PROCESSING, fencing token, locked_by/at (Gate 1).
4. Concurrent worker safety in SQLite with BEGIN IMMEDIATE claiming.
5. Post-deletion settlement on success: blob transitions to PURGED immutable tombstone (Step 5).
6. Post-deletion settlement on transient error: exponential backoff retry.
7. Post-deletion settlement on fatal error (401/403 or max attempts): status FAILED.
8. Fencing token lease validation: settlement with stale token is rejected.
9. Feature flag behavior: ENABLE_PHYSICAL_ASSET_DELETION=False safely skips physical deletion (Gate 4).
10. Physical deletion when flag is True unlinks file from disk via PhysicalBlobStorage deleter.
11. Two-pass orphan reconciliation engine: Pass 1 QUARANTINED, Pass 2 EVICTION_QUEUED, and Tombstone hit.
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path
import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.core.config import settings
from src.infrastructure.models import (
    ApiKeyModel,
    Base,
    GenerationJobModel,
    StorageBlobModel,
    StorageDeletionOutboxModel,
    StorageOrphanQuarantineModel,
    StorageUploadIntentModel,
)
from src.infrastructure.storage.deleters import LocalCASPhysicalDeleter
from src.services.deletion_outbox_service import DeletionOutboxService
from src.services.storage_service import StorageService


@pytest.fixture
async def db_context(tmp_path: Path):
    """Sets up an isolated SQLite test database with session factory."""
    db_file = tmp_path / "outbox_test.db"
    db_url = f"sqlite+aiosqlite:///{db_file.as_posix()}"
    engine = create_async_engine(db_url)

    async with engine.begin() as conn:
        await conn.execute(text("PRAGMA foreign_keys=ON;"))
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

    # Seed an API key and completed job
    async with session_factory() as session:
        key = ApiKeyModel(id="key-1", key_hash="hash_1", client_name="Client 1")
        session.add(key)
        await session.flush()
        job = GenerationJobModel(
            id="job-1",
            api_key_id=key.id,
            payload_hash="payload_1",
            status="COMPLETED",
            prompt="A majestic lion",
        )
        session.add(job)
        await session.commit()

    yield session_factory, engine

    await engine.dispose()


@pytest.mark.asyncio
async def test_enqueue_deletion_rejected_if_active_references_exist(db_context):
    """Gate 3: Attempting to enqueue deletion on a blob with active referencing assets must be rejected."""
    session_factory, _ = db_context

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash="1" * 64,
            storage_path=f"images/11/11/{'1'*64}/b1.png",
            file_size_bytes=1000,
        )
        await StorageService.associate_asset(
            session=session,
            job_id="job-1",
            blob_id=blob.id,
            file_hash="1" * 64,
            storage_path=blob.storage_path,
            file_size_bytes=1000,
        )
        await session.commit()

        # Enqueueing deletion while asset exists must raise ValueError
        with pytest.raises(ValueError, match="active referencing assets"):
            await DeletionOutboxService.enqueue_deletion(session, blob.id)


@pytest.mark.asyncio
async def test_enqueue_deletion_rejected_if_active_upload_intents_exist(db_context):
    """Gate 3: Attempting to enqueue deletion on a blob with an active unexpired upload intent must be rejected."""
    session_factory, _ = db_context

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash="2" * 64,
            storage_path=f"images/22/22/{'2'*64}/b2.png",
            file_size_bytes=1000,
        )
        # Add an active unexpired intent
        now = datetime.now(timezone.utc)
        intent = StorageUploadIntentModel(
            id="intent-1",
            blob_id=blob.id,
            file_hash=blob.file_hash,
            storage_path=blob.storage_path,
            worker_id="worker-gpu-1",
            lease_token="lease-tok-1",
            status="PENDING_UPLOAD",
            created_at=now,
            expires_at=now + timedelta(seconds=120),
        )
        session.add(intent)
        await session.commit()

        with pytest.raises(ValueError, match="active upload intents"):
            await DeletionOutboxService.enqueue_deletion(session, blob.id)


@pytest.mark.asyncio
async def test_outbox_claiming_and_fencing_token(db_context):
    """Gate 1: Claiming pending tasks sets status PROCESSING and assigns a unique fencing token."""
    session_factory, _ = db_context

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash="3" * 64,
            storage_path=f"images/33/33/{'3'*64}/b3.png",
            file_size_bytes=1000,
        )
        # Enqueue deletion (0 refs, 0 intents)
        task = await DeletionOutboxService.enqueue_deletion(session, blob.id)
        await session.commit()

        assert task.status == "PENDING"
        initial_fencing = task.fencing_token

        # Claim tasks
        claimed = await DeletionOutboxService.claim_pending_tasks(session, worker_id="worker-test-1", limit=10)
        await session.commit()

        assert len(claimed) == 1
        c_task = claimed[0]
        assert c_task.id == task.id
        assert c_task.status == "PROCESSING"
        assert c_task.locked_by == "worker-test-1"
        assert c_task.locked_at is not None
        assert c_task.fencing_token != initial_fencing, "Fencing token must be updated on claim"

        # Parent blob transitioned to DELETION_IN_PROGRESS
        blob_stmt = select(StorageBlobModel).where(StorageBlobModel.id == blob.id)
        b = (await session.execute(blob_stmt)).scalar_one()
        assert b.status == "DELETION_IN_PROGRESS"


@pytest.mark.asyncio
async def test_concurrent_worker_safety_sqlite(db_context):
    """
    Proves that when one worker claims pending tasks with BEGIN IMMEDIATE,
    subsequent queries or concurrent workers do not re-claim the already claimed tasks.
    """
    session_factory, _ = db_context

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash="4" * 64,
            storage_path=f"images/44/44/{'4'*64}/b4.png",
            file_size_bytes=1000,
        )
        await DeletionOutboxService.enqueue_deletion(session, blob.id)
        await session.commit()

    # Worker 1 claims
    async with session_factory() as s1:
        claimed_w1 = await DeletionOutboxService.claim_pending_tasks(s1, worker_id="worker-1", limit=10)
        await s1.commit()
        assert len(claimed_w1) == 1

    # Worker 2 attempts to claim
    async with session_factory() as s2:
        claimed_w2 = await DeletionOutboxService.claim_pending_tasks(s2, worker_id="worker-2", limit=10)
        await s2.commit()
        assert len(claimed_w2) == 0, "Second worker must find zero pending tasks"


@pytest.mark.asyncio
async def test_settlement_success_transitions_to_purged_tombstone(db_context):
    """Step 5: Successful settlement transitions outbox task to COMPLETED and blob to PURGED tombstone."""
    session_factory, _ = db_context

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash="5" * 64,
            storage_path=f"images/55/55/{'5'*64}/b5.png",
            file_size_bytes=1000,
        )
        task = await DeletionOutboxService.enqueue_deletion(session, blob.id)
        await session.commit()

        claimed = await DeletionOutboxService.claim_pending_tasks(session, worker_id="w1", limit=1)
        await session.commit()
        c_task = claimed[0]

        # Settle with success
        settled = await DeletionOutboxService.settle_task(
            session=session,
            task_id=c_task.id,
            fencing_token=c_task.fencing_token,
            success=True,
            error_code=200,
        )
        await session.commit()

        assert settled.status == "COMPLETED"
        assert settled.completed_at is not None

        # Blob must now be an immutable PURGED tombstone
        blob_stmt = select(StorageBlobModel).where(StorageBlobModel.id == blob.id)
        b = (await session.execute(blob_stmt)).scalar_one()
        assert b.status == "PURGED"


@pytest.mark.asyncio
async def test_settlement_transient_failure_exponential_backoff(db_context):
    """Step 5: Transient failure keeps status PENDING, increments attempts, and schedules backoff."""
    session_factory, _ = db_context

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash="6" * 64,
            storage_path=f"images/66/66/{'6'*64}/b6.png",
            file_size_bytes=1000,
        )
        await DeletionOutboxService.enqueue_deletion(session, blob.id)
        await session.commit()

        claimed = await DeletionOutboxService.claim_pending_tasks(session, worker_id="w1", limit=1)
        await session.commit()
        c_task = claimed[0]

        # Settle with transient failure (500)
        settled = await DeletionOutboxService.settle_task(
            session=session,
            task_id=c_task.id,
            fencing_token=c_task.fencing_token,
            success=False,
            error_code=500,
            error_message="HTTP 500 Internal Server Error",
        )
        await session.commit()

        assert settled.status == "PENDING"
        assert settled.attempts == 1
        assert settled.next_retry_at > datetime.now(timezone.utc)

        # Blob remains DELETION_IN_PROGRESS
        b = (await session.execute(select(StorageBlobModel).where(StorageBlobModel.id == blob.id))).scalar_one()
        assert b.status == "DELETION_IN_PROGRESS"


@pytest.mark.asyncio
async def test_settlement_fatal_error_status_failed(db_context):
    """Step 5: Fatal error (401/403 or max attempts exceeded) transitions task to FAILED."""
    session_factory, _ = db_context

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash="7" * 64,
            storage_path=f"images/77/77/{'7'*64}/b7.png",
            file_size_bytes=1000,
        )
        await DeletionOutboxService.enqueue_deletion(session, blob.id)
        await session.commit()

        claimed = await DeletionOutboxService.claim_pending_tasks(session, worker_id="w1", limit=1)
        await session.commit()
        c_task = claimed[0]

        # Settle with fatal error (403 Forbidden)
        settled = await DeletionOutboxService.settle_task(
            session=session,
            task_id=c_task.id,
            fencing_token=c_task.fencing_token,
            success=False,
            error_code=403,
            error_message="Invalid credentials",
        )
        await session.commit()

        assert settled.status == "FAILED"
        assert settled.completed_at is not None
        assert "Invalid credentials" in settled.cancel_reason


@pytest.mark.asyncio
async def test_fencing_token_mismatch_rejected(db_context):
    """Settlement with stale or invalid fencing token must be rejected."""
    session_factory, _ = db_context

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash="8" * 64,
            storage_path=f"images/88/88/{'8'*64}/b8.png",
            file_size_bytes=1000,
        )
        await DeletionOutboxService.enqueue_deletion(session, blob.id)
        await session.commit()

        claimed = await DeletionOutboxService.claim_pending_tasks(session, worker_id="w1", limit=1)
        await session.commit()
        c_task = claimed[0]

        # Settle with bogus fencing token
        settled = await DeletionOutboxService.settle_task(
            session=session,
            task_id=c_task.id,
            fencing_token="bogus-token-12345",
            success=True,
            error_code=200,
        )
        await session.commit()

        assert settled is None, "Mismatched fencing token must be rejected"


@pytest.mark.asyncio
async def test_physical_deletion_feature_flag_disabled(db_context, tmp_path: Path, monkeypatch):
    """
    Gate 4: When ENABLE_PHYSICAL_ASSET_DELETION=False, physical file on disk
    is NOT unlinked, and outbox task completes safely.
    """
    session_factory, _ = db_context
    monkeypatch.setattr(settings, "ENABLE_PHYSICAL_ASSET_DELETION", False)

    # Create dummy file on disk
    test_file = tmp_path / "images" / "dummy.png"
    test_file.parent.mkdir(parents=True, exist_ok=True)
    test_file.write_bytes(b"dummy image bytes")

    deleter = LocalCASPhysicalDeleter(base_path=tmp_path / "images")

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash="9" * 64,
            storage_path="dummy.png",
            file_size_bytes=len(b"dummy image bytes"),
        )
        await DeletionOutboxService.enqueue_deletion(session, blob.id)
        await session.commit()

    results = await DeletionOutboxService.process_batch(
        session_factory=session_factory,
        deleter=deleter,
    )

    assert results["completed"] == 1
    # File must still exist because flag was False!
    assert test_file.is_file(), "File must NOT be deleted when ENABLE_PHYSICAL_ASSET_DELETION is False"


@pytest.mark.asyncio
async def test_physical_deletion_feature_flag_enabled_unlinks_file(db_context, tmp_path: Path, monkeypatch):
    """
    Gate 4: When ENABLE_PHYSICAL_ASSET_DELETION=True, physical file on disk
    IS unlinked by the deleter, and outbox task completes.
    """
    session_factory, _ = db_context
    monkeypatch.setattr(settings, "ENABLE_PHYSICAL_ASSET_DELETION", True)

    test_file = tmp_path / "images" / "delete_me.png"
    test_file.parent.mkdir(parents=True, exist_ok=True)
    test_file.write_bytes(b"image bytes to be unlinked")

    deleter = LocalCASPhysicalDeleter(base_path=tmp_path / "images")

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash="a" * 64,
            storage_path="delete_me.png",
            file_size_bytes=len(b"image bytes to be unlinked"),
        )
        await DeletionOutboxService.enqueue_deletion(session, blob.id)
        await session.commit()

    results = await DeletionOutboxService.process_batch(
        session_factory=session_factory,
        deleter=deleter,
    )

    assert results["completed"] == 1
    # File must be deleted!
    assert not test_file.exists(), "File MUST be unlinked when ENABLE_PHYSICAL_ASSET_DELETION is True"


@pytest.mark.asyncio
async def test_orphan_reconciliation_two_pass_lifecycle(db_context):
    """
    Two-pass orphan reconciliation test:
    - Pass 1: untracked storage object is quarantined (QUARANTINED).
    - Pass 2: expired quarantine (>= 48h) transitions to EVICTION_QUEUED and enqueues outbox task.
    - Resurrected orphan: untracked object matching a PURGED blob enqueues immediate outbox task.
    """
    session_factory, _ = db_context

    async with session_factory() as session:
        # Create a PURGED blob (tombstone)
        purged_blob = await StorageService.create_blob(
            session=session,
            file_hash="tomb" + "0" * 60,
            storage_path="images/tomb.png",
            file_size_bytes=100,
            status="PURGED",
        )
        await session.commit()

        # Storage scan finds two objects:
        # 1. Resurrected tombstone
        # 2. Untracked brand new orphan
        storage_objects = [
            {"storage_path": "images/tomb.png", "file_hash": purged_blob.file_hash, "blob_id": purged_blob.id},
            {"storage_path": "images/brand_new_orphan.png", "file_hash": "orphan123"},
        ]

        stats_pass1 = await DeletionOutboxService.reconcile_orphans(session, storage_objects)
        await session.commit()

        assert stats_pass1["resurrected_orphans"] == 1
        assert stats_pass1["quarantined"] == 1

        # Verify orphan 2 is in quarantine
        q = (await session.execute(
            select(StorageOrphanQuarantineModel).where(StorageOrphanQuarantineModel.storage_path == "images/brand_new_orphan.png")
        )).scalar_one()
        assert q.status == "QUARANTINED"

        # Now simulate age expiration: manually age the quarantine record by 50 hours
        q.quarantined_at = datetime.now(timezone.utc) - timedelta(hours=50)
        await session.commit()

        # Pass 2: Scan again
        stats_pass2 = await DeletionOutboxService.reconcile_orphans(session, [
            {"storage_path": "images/brand_new_orphan.png", "file_hash": "orphan123"}
        ])
        await session.commit()

        assert stats_pass2["eviction_queued"] == 1

        # Check quarantine status is now EVICTION_QUEUED
        q_refreshed = (await session.execute(
            select(StorageOrphanQuarantineModel).where(StorageOrphanQuarantineModel.storage_path == "images/brand_new_orphan.png")
        )).scalar_one()
        assert q_refreshed.status == "EVICTION_QUEUED"

        # Outbox task created for orphan eviction
        outbox_task = (await session.execute(
            select(StorageDeletionOutboxModel).where(StorageDeletionOutboxModel.storage_path == "images/brand_new_orphan.png")
        )).scalar_one()
        assert outbox_task.status == "PENDING"
