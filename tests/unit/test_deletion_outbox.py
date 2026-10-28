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

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
import uuid
import pytest
from pydantic import SecretStr
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
from src.domain.interfaces import PhysicalBlobStorage
from src.infrastructure.storage.deleters import LocalCASPhysicalDeleter
from src.services.deletion_outbox_service import DeletionOutboxService, DeletionOutboxWorker
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
    AUDIT-6.3-01 Regression Test:
    When ENABLE_PHYSICAL_ASSET_DELETION=False:
    1. No physical deleter is called.
    2. The physical object on disk remains untouched.
    3. The outbox task is NOT marked COMPLETED (remains PENDING).
    4. The blob is NOT marked PURGED (remains DELETION_QUEUED).
    5. Repeated disabled runs do not consume retries or corrupt state.
    6. Enabling the flag later permits the pending task to be claimed and purged.
    """
    session_factory, _ = db_context
    monkeypatch.setattr(settings, "ENABLE_PHYSICAL_ASSET_DELETION", False)

    # Create dummy file on disk
    test_file = tmp_path / "images" / "dummy.png"
    test_file.parent.mkdir(parents=True, exist_ok=True)
    test_file.write_bytes(b"dummy image bytes")

    deleter_call_count = 0

    class TrackingDeleter(PhysicalBlobStorage):
        async def delete_physical_object(self, storage_path: str) -> bool:
            nonlocal deleter_call_count
            deleter_call_count += 1
            if test_file.exists():
                test_file.unlink()
            return True

    deleter = TrackingDeleter()

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash="9" * 64,
            storage_path="dummy.png",
            file_size_bytes=len(b"dummy image bytes"),
        )
        task = await DeletionOutboxService.enqueue_deletion(session, blob.id)
        await session.commit()
        blob_id = blob.id
        task_id = task.id

    # 1. Run batch while disabled
    results = await DeletionOutboxService.process_batch(
        session_factory=session_factory,
        deleter=deleter,
    )

    # Must claim 0 and complete 0
    assert results["claimed"] == 0
    assert results["completed"] == 0
    assert deleter_call_count == 0, "Physical deleter must NOT be called when deletion is disabled"
    assert test_file.is_file(), "Physical file on disk must remain untouched"

    # Verify task and blob statuses in database
    async with session_factory() as session:
        t_stmt = select(StorageDeletionOutboxModel).where(StorageDeletionOutboxModel.id == task_id)
        saved_task = (await session.execute(t_stmt)).scalar_one()
        assert saved_task.status == "PENDING", "Task must remain PENDING"
        assert saved_task.attempts == 0, "No attempts should be consumed while disabled"

        b_stmt = select(StorageBlobModel).where(StorageBlobModel.id == blob_id)
        saved_blob = (await session.execute(b_stmt)).scalar_one()
        assert saved_blob.status == "DELETION_QUEUED", "Blob must remain DELETION_QUEUED, NEVER PURGED"

    # 2. Repeated runs while disabled must remain idempotent and harmless
    results2 = await DeletionOutboxService.process_batch(
        session_factory=session_factory,
        deleter=deleter,
    )
    assert results2["claimed"] == 0
    assert results2["completed"] == 0
    assert deleter_call_count == 0
    assert test_file.is_file()

    # 3. Later, enable the feature flag
    monkeypatch.setattr(settings, "ENABLE_PHYSICAL_ASSET_DELETION", True)

    results3 = await DeletionOutboxService.process_batch(
        session_factory=session_factory,
        deleter=deleter,
    )
    assert results3["claimed"] == 1
    assert results3["completed"] == 1
    assert deleter_call_count == 1, "Deleter must be invoked once flag is enabled"
    assert not test_file.exists(), "Physical file must now be deleted"

    # Verify task and blob transitioned to final states
    async with session_factory() as session:
        t_stmt = select(StorageDeletionOutboxModel).where(StorageDeletionOutboxModel.id == task_id)
        final_task = (await session.execute(t_stmt)).scalar_one()
        assert final_task.status == "COMPLETED"

        b_stmt = select(StorageBlobModel).where(StorageBlobModel.id == blob_id)
        final_blob = (await session.execute(b_stmt)).scalar_one()
        assert final_blob.status == "PURGED"


@pytest.mark.asyncio
async def test_advisory_lock_key_derivation():
    """
    AUDIT-6.3-03 Regression Test:
    Verifies that Python hash_to_lock_key matches PostgreSQL fn_hash_to_lock_key derivation
    and produces valid 64-bit signed integers.
    """
    from src.services.deletion_outbox_service import hash_to_lock_key

    # Test boundary values
    min_hash = "00" * 32
    assert hash_to_lock_key(min_hash) == 0

    max_pos_hash = "7f" + "ff" * 31
    assert hash_to_lock_key(max_pos_hash) == 9223372036854775807

    neg_hash = "80" + "00" * 31
    assert hash_to_lock_key(neg_hash) == -9223372036854775808

    minus_one_hash = "ff" * 32
    assert hash_to_lock_key(minus_one_hash) == -1

    # Deterministic consistency
    test_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    key = hash_to_lock_key(test_hash)
    assert isinstance(key, int)
    assert -(2**63) <= key <= (2**63 - 1)



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


@pytest.mark.asyncio
async def test_process_batch_disabled_flag_does_not_increment_retried_metric(db_context, monkeypatch):
    """
    Objective B Regression Test:
    A task released back to PENDING solely because ENABLE_PHYSICAL_ASSET_DELETION
    became False mid-batch must NOT increment results["retried"].
    """
    session_factory, _ = db_context

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash="8" * 64,
            storage_path="images/release_metric.png",
            file_size_bytes=100,
        )
        task = await DeletionOutboxService.enqueue_deletion(session, blob.id)
        await session.commit()
        task_id = task.id
        blob_id = blob.id

    # Initially allow claim, then simulate flag becoming False before physical deletion
    original_claim = DeletionOutboxService.claim_pending_tasks

    async def claim_and_disable(session, worker_id="outbox-worker-1", limit=10):
        tasks = await original_claim(session, worker_id=worker_id, limit=limit)
        monkeypatch.setattr(settings, "ENABLE_PHYSICAL_ASSET_DELETION", False)
        return tasks

    monkeypatch.setattr(settings, "ENABLE_PHYSICAL_ASSET_DELETION", True)
    monkeypatch.setattr(DeletionOutboxService, "claim_pending_tasks", claim_and_disable)

    results = await DeletionOutboxService.process_batch(
        session_factory=session_factory,
    )

    # Claimed was 1, but released cleanly to PENDING without retry penalty
    assert results["claimed"] == 1
    assert results["completed"] == 0
    assert results["failed"] == 0
    assert results["retried"] == 0, "Released task must NOT increment results['retried']"

    # Verify task state in database: status PENDING, attempts 0
    async with session_factory() as session:
        t = (await session.execute(
            select(StorageDeletionOutboxModel).where(StorageDeletionOutboxModel.id == task_id)
        )).scalar_one()
        assert t.status == "PENDING"
        assert t.attempts == 0
        assert t.locked_at is None
        assert t.locked_by is None

        b = (await session.execute(
            select(StorageBlobModel).where(StorageBlobModel.id == blob_id)
        )).scalar_one()
        assert b.status == "DELETION_QUEUED"


@pytest.mark.asyncio
async def test_lock_order_advisory_lock_precedes_row_locks(db_context, monkeypatch):
    """
    Objective A Regression Test:
    Verifies that on PostgreSQL, acquire_hash_advisory_lock is executed BEFORE
    row-level FOR UPDATE locks in enqueue_deletion and settle_task.
    """
    session_factory, _ = db_context

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash="7" * 64,
            storage_path="images/lock_order.png",
            file_size_bytes=100,
        )
        await session.commit()
        blob_id = blob.id

    # 1. Test enqueue_deletion lock ordering on PostgreSQL dialect
    executed_statements = []

    async with session_factory() as session:
        original_execute = session.execute

        async def tracking_execute(statement, params=None, **kwargs):
            sql_str = str(statement)
            executed_statements.append(sql_str)
            if "pg_advisory_xact_lock" in sql_str:
                from unittest.mock import MagicMock
                mock_res = MagicMock()
                mock_res.scalar_one_or_none.return_value = None
                return mock_res
            return await original_execute(statement, params, **kwargs)

        monkeypatch.setattr(type(settings), "is_sqlite", property(lambda self: False))
        monkeypatch.setattr(session, "execute", tracking_execute)

        task = await DeletionOutboxService.enqueue_deletion(session, blob_id)
        await session.commit()
        assert task is not None
        task_id = task.id
        fencing_token = task.fencing_token

        # Verify pg_advisory_xact_lock occurred BEFORE with_for_update
        advisory_indices = [i for i, s in enumerate(executed_statements) if "pg_advisory_xact_lock" in s]
        for_update_indices = [i for i, s in enumerate(executed_statements) if "FOR UPDATE" in s]

        assert len(advisory_indices) >= 1, "pg_advisory_xact_lock must be executed on PostgreSQL"
        assert len(for_update_indices) >= 1, "FOR UPDATE must be executed on PostgreSQL"
        assert advisory_indices[0] < for_update_indices[0], (
            f"Advisory lock (index {advisory_indices[0]}) MUST precede FOR UPDATE (index {for_update_indices[0]})"
        )

    # 2. Test settle_task lock ordering on PostgreSQL dialect
    async with session_factory() as session:
        t = (await session.execute(
            select(StorageDeletionOutboxModel).where(StorageDeletionOutboxModel.id == task_id)
        )).scalar_one()
        t.status = "PROCESSING"
        await session.commit()

    settle_statements = []
    async with session_factory() as session:
        original_execute = session.execute

        async def tracking_settle_execute(statement, params=None, **kwargs):
            sql_str = str(statement)
            settle_statements.append(sql_str)
            if "pg_advisory_xact_lock" in sql_str:
                from unittest.mock import MagicMock
                mock_res = MagicMock()
                mock_res.scalar_one_or_none.return_value = None
                return mock_res
            return await original_execute(statement, params, **kwargs)

        monkeypatch.setattr(type(settings), "is_sqlite", property(lambda self: False))
        monkeypatch.setattr(session, "execute", tracking_settle_execute)

        settled = await DeletionOutboxService.settle_task(
            session=session,
            task_id=task_id,
            fencing_token=fencing_token,
            success=True,
        )
        await session.commit()
        assert settled is not None
        assert settled.status == "COMPLETED"

        advisory_indices = [i for i, s in enumerate(settle_statements) if "pg_advisory_xact_lock" in s]
        for_update_indices = [i for i, s in enumerate(settle_statements) if "FOR UPDATE" in s]

        assert len(advisory_indices) >= 1, "pg_advisory_xact_lock must be executed in settle_task"
        assert len(for_update_indices) >= 1, "FOR UPDATE must be executed in settle_task"
        assert advisory_indices[0] < for_update_indices[0], (
            f"Advisory lock (index {advisory_indices[0]}) MUST precede FOR UPDATE in settle_task (index {for_update_indices[0]})"
        )


@pytest.mark.asyncio
async def test_enqueue_deletion_idempotent_no_outbox_row_lock_or_state_regression(db_context, monkeypatch):
    """
    Step 2E Regression Test:
    Verifies that calling enqueue_deletion on a blob that already has an active
    outbox task (PENDING or PROCESSING):
    1. Returns the existing task idempotently without mutating next_retry_at.
    2. Never emits an UPDATE on storage_deletion_outbox (avoiding Level 5 row lock acquisition).
    3. Never regresses blob status from DELETION_IN_PROGRESS back to DELETION_QUEUED.
    4. Eliminates the circular wait deadlock against claim_pending_tasks.
    """
    session_factory, _ = db_context

    # 1. Seed blob and initial pending task
    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash="9" * 64,
            storage_path="images/lock_inversion_fix.png",
            file_size_bytes=2048,
        )
        task1 = await DeletionOutboxService.enqueue_deletion(session, blob.id)
        await session.commit()
        blob_id = blob.id
        task_id = task1.id
        orig_next_retry = task1.next_retry_at

    # 2. Worker claims task -> status becomes PROCESSING, blob becomes DELETION_IN_PROGRESS
    async with session_factory() as session:
        claimed = await DeletionOutboxService.claim_pending_tasks(
            session=session,
            worker_id="worker-lock-check",
            limit=10,
        )
        await session.commit()
        assert len(claimed) == 1
        assert claimed[0].id == task_id
        assert claimed[0].status == "PROCESSING"

    async with session_factory() as session:
        b = (await session.execute(
            select(StorageBlobModel).where(StorageBlobModel.id == blob_id)
        )).scalar_one()
        assert b.status == "DELETION_IN_PROGRESS"

    # 3. Simulate PostgreSQL dialect and invoke enqueue_deletion again while task is PROCESSING
    executed_statements = []

    async with session_factory() as session:
        original_execute = session.execute

        async def tracking_execute(statement, params=None, **kwargs):
            sql_str = str(statement)
            executed_statements.append(sql_str)
            if "pg_advisory_xact_lock" in sql_str:
                from unittest.mock import MagicMock
                mock_res = MagicMock()
                mock_res.scalar_one_or_none.return_value = None
                return mock_res
            return await original_execute(statement, params, **kwargs)

        monkeypatch.setattr(type(settings), "is_sqlite", property(lambda self: False))
        monkeypatch.setattr(session, "execute", tracking_execute)

        task2 = await DeletionOutboxService.enqueue_deletion(session, blob_id)
        await session.commit()

        # Must return the existing active task idempotently
        assert task2 is not None
        assert task2.id == task_id
        assert task2.status == "PROCESSING"

        # Structural Verification:
        # Crucially, enqueue_deletion MUST NOT emit UPDATE storage_deletion_outbox
        # (which would request a Level 5 row lock while holding Level 3 blob FOR UPDATE).
        outbox_updates = [
            s for s in executed_statements
            if "UPDATE" in s.upper() and "STORAGE_DELETION_OUTBOX" in s.upper()
        ]
        assert len(outbox_updates) == 0, (
            f"enqueue_deletion must NEVER issue UPDATE on storage_deletion_outbox for an active task. Emitted: {outbox_updates}"
        )

    # 4. Verify in database that blob status was NOT regressed and task was NOT altered
    async with session_factory() as session:
        b_final = (await session.execute(
            select(StorageBlobModel).where(StorageBlobModel.id == blob_id)
        )).scalar_one()
        assert b_final.status == "DELETION_IN_PROGRESS", "Blob status must NOT regress to DELETION_QUEUED"

        t_final = (await session.execute(
            select(StorageDeletionOutboxModel).where(StorageDeletionOutboxModel.id == task_id)
        )).scalar_one()
        assert t_final.status == "PROCESSING"
        assert t_final.next_retry_at.replace(tzinfo=timezone.utc) == orig_next_retry


@pytest.mark.asyncio
async def test_orphan_reconciliation_existing_available_blob_transitions_and_blocks_association(db_context):
    """
    AUDIT-6.3-04 Regression Test 1:
    Verifies that when an existing AVAILABLE blob expires quarantine and is queued for eviction:
    1. Its status transitions from AVAILABLE to DELETION_QUEUED.
    2. Any subsequent asset association attempt is strictly blocked (preventing association to deleting blob).
    """
    session_factory, _ = db_context

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash="a" * 64,
            storage_path="images/avail_orphan.png",
            file_size_bytes=1024,
        )
        await session.commit()
        blob_id = blob.id
        storage_path = blob.storage_path
        file_hash = blob.file_hash

    # Pass 1: Discovered unreferenced -> enters QUARANTINED
    async with session_factory() as session:
        stats1 = await DeletionOutboxService.reconcile_orphans(session, [
            {"storage_path": storage_path, "blob_id": blob_id, "file_hash": file_hash}
        ])
        await session.commit()
        assert stats1["quarantined"] == 1

    # Simulate quarantine expiration (50 hours elapsed)
    async with session_factory() as session:
        q = (await session.execute(
            select(StorageOrphanQuarantineModel).where(StorageOrphanQuarantineModel.storage_path == storage_path)
        )).scalar_one()
        q.quarantined_at = datetime.now(timezone.utc) - timedelta(hours=50)
        await session.commit()

    # Pass 2: Expiration scan -> queues eviction
    async with session_factory() as session:
        stats2 = await DeletionOutboxService.reconcile_orphans(session, [
            {"storage_path": storage_path, "blob_id": blob_id, "file_hash": file_hash}
        ])
        await session.commit()
        assert stats2["eviction_queued"] == 1

    # Assertions:
    # 1. Blob MUST NOT remain AVAILABLE; it MUST be DELETION_QUEUED!
    async with session_factory() as session:
        b_refreshed = (await session.execute(
            select(StorageBlobModel).where(StorageBlobModel.id == blob_id)
        )).scalar_one()
        assert b_refreshed.status == "DELETION_QUEUED", (
            f"Blob status must be DELETION_QUEUED, but was '{b_refreshed.status}'"
        )

        outbox_task = (await session.execute(
            select(StorageDeletionOutboxModel).where(StorageDeletionOutboxModel.blob_id == blob_id)
        )).scalar_one()
        assert outbox_task.status == "PENDING"

        # 2. Association attempt MUST be blocked by state machine check!
        with pytest.raises(ValueError, match="must be AVAILABLE"):
            await StorageService.associate_asset(
                session=session,
                job_id="test-job-reconcile-block",
                blob_id=blob_id,
                file_hash=file_hash,
                storage_path=storage_path,
                file_size_bytes=1024,
            )


@pytest.mark.asyncio
async def test_orphan_reconciliation_active_refs_and_intents_prevent_eviction_and_reclaim(db_context):
    """
    AUDIT-6.3-04 Regression Test 2:
    Verifies that if an unreferenced blob enters quarantine, but subsequently gains an active
    reference or active upload intent before Pass 2:
    1. Eviction is NOT enqueued.
    2. Quarantine status transitions to RECLAIMED.
    3. The object is preserved.
    """
    session_factory, _ = db_context

    # Case A: Re-referenced via GeneratedAssetModel
    async with session_factory() as session:
        blob_a = await StorageService.create_blob(
            session=session,
            file_hash="b" * 64,
            storage_path="images/reclaimed_ref.png",
            file_size_bytes=1024,
        )
        await session.commit()
        blob_a_id = blob_a.id
        path_a = blob_a.storage_path
        hash_a = blob_a.file_hash

    # Pass 1: enters quarantine
    async with session_factory() as session:
        await DeletionOutboxService.reconcile_orphans(session, [
            {"storage_path": path_a, "blob_id": blob_a_id, "file_hash": hash_a}
        ])
        await session.commit()

    # Re-reference blob_a during quarantine grace period
    async with session_factory() as session:
        await StorageService.associate_asset(
            session=session,
            job_id="job-1",
            blob_id=blob_a_id,
            file_hash=hash_a,
            storage_path=path_a,
            file_size_bytes=1024,
        )
        # Age quarantine record past grace period
        q = (await session.execute(
            select(StorageOrphanQuarantineModel).where(StorageOrphanQuarantineModel.storage_path == path_a)
        )).scalar_one()
        q.quarantined_at = datetime.now(timezone.utc) - timedelta(hours=50)
        await session.commit()

    # Pass 2: Sweeper detects re-reference -> Reclaims!
    async with session_factory() as session:
        stats = await DeletionOutboxService.reconcile_orphans(session, [
            {"storage_path": path_a, "blob_id": blob_a_id, "file_hash": hash_a}
        ])
        await session.commit()

        assert stats["active_preserved"] == 1
        assert stats["eviction_queued"] == 0

        q_refreshed = (await session.execute(
            select(StorageOrphanQuarantineModel).where(StorageOrphanQuarantineModel.storage_path == path_a)
        )).scalar_one()
        assert q_refreshed.status == "RECLAIMED"

        # Verify no outbox task exists
        outbox_tasks = (await session.execute(
            select(StorageDeletionOutboxModel).where(StorageDeletionOutboxModel.blob_id == blob_a_id)
        )).scalars().all()
        assert len(outbox_tasks) == 0

    # Case B: In-flight upload intent protects object
    async with session_factory() as session:
        blob_b = await StorageService.create_blob(
            session=session,
            file_hash="c" * 64,
            storage_path="images/intent_protect.png",
            file_size_bytes=1024,
        )
        # Seed an active unexpired upload intent
        intent = StorageUploadIntentModel(
            id=str(uuid.uuid4()),
            blob_id=blob_b.id,
            file_hash=blob_b.file_hash,
            storage_path=blob_b.storage_path,
            worker_id="worker-intent-test",
            lease_token=str(uuid.uuid4()),
            status="PENDING_UPLOAD",
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=30),
        )
        session.add(intent)
        await session.commit()
        blob_b_id = blob_b.id
        path_b = blob_b.storage_path
        hash_b = blob_b.file_hash

    # Scan with active intent -> preserved
    async with session_factory() as session:
        stats_intent = await DeletionOutboxService.reconcile_orphans(session, [
            {"storage_path": path_b, "blob_id": blob_b_id, "file_hash": hash_b}
        ])
        await session.commit()
        assert stats_intent["active_preserved"] == 1
        assert stats_intent["quarantined"] == 0
        assert stats_intent["eviction_queued"] == 0


@pytest.mark.asyncio
async def test_orphan_reconciliation_repeated_runs_idempotent(db_context):
    """
    AUDIT-6.3-04 Regression Test 3:
    Verifies that repeatedly running reconciliation on an already EVICTION_QUEUED object:
    1. Does not create duplicate outbox tasks.
    2. Does not reset next_retry_at or fencing token.
    3. Keeps stats["eviction_queued"] at 0.
    """
    session_factory, _ = db_context

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash="d" * 64,
            storage_path="images/idempotent_sweep.png",
            file_size_bytes=1024,
        )
        await session.commit()
        blob_id = blob.id
        path = blob.storage_path
        file_hash = blob.file_hash

    # Pass 1 -> Quarantined
    async with session_factory() as session:
        await DeletionOutboxService.reconcile_orphans(session, [
            {"storage_path": path, "blob_id": blob_id, "file_hash": file_hash}
        ])
        q = (await session.execute(
            select(StorageOrphanQuarantineModel).where(StorageOrphanQuarantineModel.storage_path == path)
        )).scalar_one()
        q.quarantined_at = datetime.now(timezone.utc) - timedelta(hours=50)
        await session.commit()

    # Pass 2 -> Eviction queued
    async with session_factory() as session:
        stats_p2 = await DeletionOutboxService.reconcile_orphans(session, [
            {"storage_path": path, "blob_id": blob_id, "file_hash": file_hash}
        ])
        await session.commit()
        assert stats_p2["eviction_queued"] == 1

        task_initial = (await session.execute(
            select(StorageDeletionOutboxModel).where(StorageDeletionOutboxModel.blob_id == blob_id)
        )).scalar_one()
        initial_fencing = task_initial.fencing_token
        initial_next_retry = task_initial.next_retry_at

    # Repeated Pass 2 (scheduler re-run)
    async with session_factory() as session:
        stats_p3 = await DeletionOutboxService.reconcile_orphans(session, [
            {"storage_path": path, "blob_id": blob_id, "file_hash": file_hash}
        ])
        await session.commit()

        # Must not re-enqueue or duplicate
        assert stats_p3["eviction_queued"] == 0

        tasks = (await session.execute(
            select(StorageDeletionOutboxModel).where(StorageDeletionOutboxModel.blob_id == blob_id)
        )).scalars().all()
        assert len(tasks) == 1
        assert tasks[0].fencing_token == initial_fencing
        assert tasks[0].next_retry_at == initial_next_retry


@pytest.mark.asyncio
async def test_orphan_reconciliation_metadata_mismatch_safely_deferred(db_context):
    """
    AUDIT-6.3-04 Regression Test 4:
    Verifies that when storage scanner reports a file_hash that mismatches the catalog blob's hash,
    the object is quarantined/deferred for operator inspection and NOT queued for deletion.
    """
    session_factory, _ = db_context

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash="e" * 64,
            storage_path="images/mismatch_guard.png",
            file_size_bytes=1024,
        )
        await session.commit()
        blob_id = blob.id
        path = blob.storage_path

    # Scanner reports mismatched hash
    mismatched_hash = "f" * 64

    async with session_factory() as session:
        stats = await DeletionOutboxService.reconcile_orphans(session, [
            {"storage_path": path, "blob_id": blob_id, "file_hash": mismatched_hash}
        ])
        await session.commit()

        # Quarantined for safety, but eviction NOT queued
        assert stats["eviction_queued"] == 0
        assert stats["quarantined"] == 1

        # Blob status remains AVAILABLE (catalog is not corrupted)
        b = (await session.execute(
            select(StorageBlobModel).where(StorageBlobModel.id == blob_id)
        )).scalar_one()
        assert b.status == "AVAILABLE"

        # Outbox task is NOT created
        tasks = (await session.execute(
            select(StorageDeletionOutboxModel).where(StorageDeletionOutboxModel.blob_id == blob_id)
        )).scalars().all()
        assert len(tasks) == 0


@pytest.mark.asyncio
async def test_stale_processing_task_recovered_after_lease_expires(db_context):
    """
    AUDIT-6.3-05 Regression Test 1:
    Verifies that a task stuck in PROCESSING past its lease timeout:
    1. Is recovered to PENDING.
    2. Receives a new fencing token to invalidate the stale worker.
    3. Increments its attempt counter and schedules exponential backoff.
    4. Restores its parent blob status to DELETION_QUEUED.
    """
    session_factory, _ = db_context

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash="1" * 64,
            storage_path="images/stale_lease.png",
            file_size_bytes=2048,
        )
        task = await DeletionOutboxService.enqueue_deletion(session, blob.id)
        await session.commit()
        blob_id = blob.id
        task_id = task.id

    # Worker 1 claims task
    async with session_factory() as session:
        claimed = await DeletionOutboxService.claim_pending_tasks(session, worker_id="worker-crash-1", limit=1)
        await session.commit()
        assert len(claimed) == 1
        stale_fencing = claimed[0].fencing_token
        assert claimed[0].status == "PROCESSING"

    # Simulate worker crash and lease expiration (>120s ago)
    now = datetime.now(timezone.utc)
    async with session_factory() as session:
        t = (await session.execute(
            select(StorageDeletionOutboxModel).where(StorageDeletionOutboxModel.id == task_id)
        )).scalar_one()
        t.locked_at = now - timedelta(seconds=200)
        await session.commit()

    # Recovery pass
    async with session_factory() as session:
        recovered = await DeletionOutboxService.recover_stalled_tasks(session, lease_timeout_seconds=120)
        await session.commit()

        assert len(recovered) == 1
        rec = recovered[0]
        assert rec.id == task_id
        assert rec.status == "PENDING"
        assert rec.fencing_token != stale_fencing, "Fencing token must be replaced to fence out stale worker"
        assert rec.locked_at is None
        assert rec.locked_by is None
        assert rec.attempts == 1
        # SQLite returns naive datetime; normalize to UTC
        r_retry = rec.next_retry_at if rec.next_retry_at.tzinfo else rec.next_retry_at.replace(tzinfo=timezone.utc)
        assert r_retry > now

        # Parent blob restored to DELETION_QUEUED
        b = (await session.execute(
            select(StorageBlobModel).where(StorageBlobModel.id == blob_id)
        )).scalar_one()
        assert b.status == "DELETION_QUEUED"


@pytest.mark.asyncio
async def test_non_expired_task_not_reclaimed(db_context):
    """
    AUDIT-6.3-05 Regression Test 2:
    Verifies that an actively leased task whose lease has not expired is NOT recovered.
    """
    session_factory, _ = db_context

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash="2" * 64,
            storage_path="images/active_lease.png",
            file_size_bytes=2048,
        )
        task = await DeletionOutboxService.enqueue_deletion(session, blob.id)
        await session.commit()
        task_id = task.id
        blob_id = blob.id

    # Worker claims task (recent lease)
    async with session_factory() as session:
        claimed = await DeletionOutboxService.claim_pending_tasks(session, worker_id="active-worker-1", limit=1)
        await session.commit()
        active_fencing = claimed[0].fencing_token

    # Recovery pass runs with 120s timeout (task locked only seconds ago)
    async with session_factory() as session:
        recovered = await DeletionOutboxService.recover_stalled_tasks(session, lease_timeout_seconds=120)
        await session.commit()

        assert len(recovered) == 0, "Non-expired task must not be reclaimed"

        # Verify task remains PROCESSING and undisturbed
        t = (await session.execute(
            select(StorageDeletionOutboxModel).where(StorageDeletionOutboxModel.id == task_id)
        )).scalar_one()
        assert t.status == "PROCESSING"
        assert t.fencing_token == active_fencing
        assert t.locked_by == "active-worker-1"

        b = (await session.execute(
            select(StorageBlobModel).where(StorageBlobModel.id == blob_id)
        )).scalar_one()
        assert b.status == "DELETION_IN_PROGRESS"


@pytest.mark.asyncio
async def test_stale_fencing_token_cannot_settle_reclaimed_task(db_context):
    """
    AUDIT-6.3-05 Regression Test 3:
    Verifies that if a crashed/stalled worker attempts to settle a task after it has been
    recovered by another worker, the settlement is rejected due to fencing token mismatch.
    """
    session_factory, _ = db_context

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash="3" * 64,
            storage_path="images/fencing_fenceout.png",
            file_size_bytes=2048,
        )
        task = await DeletionOutboxService.enqueue_deletion(session, blob.id)
        await session.commit()
        task_id = task.id
        blob_id = blob.id

    # Worker 1 claims task
    async with session_factory() as session:
        claimed = await DeletionOutboxService.claim_pending_tasks(session, worker_id="worker-slow-1", limit=1)
        await session.commit()
        stale_fencing = claimed[0].fencing_token

    # Worker 1 stalls past lease expiration
    now = datetime.now(timezone.utc)
    async with session_factory() as session:
        t = (await session.execute(
            select(StorageDeletionOutboxModel).where(StorageDeletionOutboxModel.id == task_id)
        )).scalar_one()
        t.locked_at = now - timedelta(seconds=200)
        await session.commit()

    # Recovery sweeps and reclaims task
    async with session_factory() as session:
        recovered = await DeletionOutboxService.recover_stalled_tasks(session, lease_timeout_seconds=120)
        await session.commit()
        assert len(recovered) == 1

    # Stale Worker 1 wakes up and attempts to settle with stale fencing token
    async with session_factory() as session:
        settled = await DeletionOutboxService.settle_task(
            session=session,
            task_id=task_id,
            fencing_token=stale_fencing,
            success=True,
            error_code=200,
        )
        await session.commit()

        assert settled is None, "Stale worker settlement must be rejected"

        # Verify task is NOT COMPLETED and blob is NOT PURGED
        t = (await session.execute(
            select(StorageDeletionOutboxModel).where(StorageDeletionOutboxModel.id == task_id)
        )).scalar_one()
        assert t.status == "PENDING"

        b = (await session.execute(
            select(StorageBlobModel).where(StorageBlobModel.id == blob_id)
        )).scalar_one()
        assert b.status == "DELETION_QUEUED"


@pytest.mark.asyncio
async def test_concurrent_claimers_cannot_own_same_task_sqlite(db_context):
    """
    AUDIT-6.3-05 Regression Test 4:
    Verifies that concurrent reclaim passes cannot reclaim the same stalled task simultaneously.
    """
    session_factory, _ = db_context

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash="4" * 64,
            storage_path="images/concurrent_reclaim.png",
            file_size_bytes=2048,
        )
        task = await DeletionOutboxService.enqueue_deletion(session, blob.id)
        await session.commit()
        task_id = task.id

    # Claim and simulate timeout
    now = datetime.now(timezone.utc)
    async with session_factory() as session:
        claimed = await DeletionOutboxService.claim_pending_tasks(session, worker_id="w-stall", limit=1)
        t = claimed[0]
        t.locked_at = now - timedelta(seconds=300)
        await session.commit()

    # Worker 1 reclaims
    async with session_factory() as s1:
        rec1 = await DeletionOutboxService.recover_stalled_tasks(s1, lease_timeout_seconds=120)
        await s1.commit()
        assert len(rec1) == 1

    # Worker 2 attempts concurrent reclaim
    async with session_factory() as s2:
        rec2 = await DeletionOutboxService.recover_stalled_tasks(s2, lease_timeout_seconds=120)
        await s2.commit()
        assert len(rec2) == 0, "Second worker must find 0 stalled tasks to recover"


@pytest.mark.asyncio
async def test_lease_recovery_retry_exhaustion_transitions_to_failed(db_context):
    """
    AUDIT-6.3-05 Regression Test 5:
    Verifies that when a stalled task reaches max_attempts, it transitions to FAILED
    with a structured cancel reason, and parent blob remains safely in DELETION_QUEUED.
    """
    session_factory, _ = db_context

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash="5" * 64,
            storage_path="images/max_attempts_exhausted.png",
            file_size_bytes=2048,
        )
        task = await DeletionOutboxService.enqueue_deletion(session, blob.id)
        await session.commit()
        task_id = task.id
        blob_id = blob.id

    # Claim and simulate attempt count near exhaustion (attempts=4, max_attempts=5)
    now = datetime.now(timezone.utc)
    async with session_factory() as session:
        claimed = await DeletionOutboxService.claim_pending_tasks(session, worker_id="w-fail", limit=1)
        t = claimed[0]
        t.attempts = 4
        t.max_attempts = 5
        t.locked_at = now - timedelta(seconds=200)
        await session.commit()

    # Recovery pass triggers retry exhaustion
    async with session_factory() as session:
        recovered = await DeletionOutboxService.recover_stalled_tasks(session, lease_timeout_seconds=120)
        await session.commit()

        assert len(recovered) == 1
        rec = recovered[0]
        assert rec.status == "FAILED"
        assert rec.attempts == 5
        assert rec.completed_at is not None
        assert "max attempts" in rec.cancel_reason

        # Parent blob is NOT PURGED
        b = (await session.execute(
            select(StorageBlobModel).where(StorageBlobModel.id == blob_id)
        )).scalar_one()
        assert b.status == "DELETION_QUEUED"


@pytest.mark.asyncio
async def test_lease_recovery_with_disabled_physical_deletion_is_safe(db_context, monkeypatch):
    """
    AUDIT-6.3-05 Regression Test 6:
    Verifies that lease recovery operates safely when ENABLE_PHYSICAL_ASSET_DELETION=False:
    task is recovered to PENDING without triggering deleter or corrupting blob state.
    """
    session_factory, _ = db_context
    monkeypatch.setattr(settings, "ENABLE_PHYSICAL_ASSET_DELETION", False)

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash="6" * 64,
            storage_path="images/disabled_recovery.png",
            file_size_bytes=2048,
        )
        task = await DeletionOutboxService.enqueue_deletion(session, blob.id)
        await session.commit()
        task_id = task.id
        blob_id = blob.id

    # Manually place task into stalled PROCESSING state
    now = datetime.now(timezone.utc)
    async with session_factory() as session:
        t = (await session.execute(
            select(StorageDeletionOutboxModel).where(StorageDeletionOutboxModel.id == task_id)
        )).scalar_one()
        t.status = "PROCESSING"
        t.locked_at = now - timedelta(seconds=200)
        t.locked_by = "crashed-worker"
        b = (await session.execute(
            select(StorageBlobModel).where(StorageBlobModel.id == blob_id)
        )).scalar_one()
        b.status = "DELETION_IN_PROGRESS"
        await session.commit()

    # Recovery pass
    async with session_factory() as session:
        recovered = await DeletionOutboxService.recover_stalled_tasks(session, lease_timeout_seconds=120)
        await session.commit()

        assert len(recovered) == 1
        assert recovered[0].status == "PENDING"
        assert recovered[0].attempts == 1

        b = (await session.execute(
            select(StorageBlobModel).where(StorageBlobModel.id == blob_id)
        )).scalar_one()
        assert b.status == "DELETION_QUEUED", "Blob must be restored to DELETION_QUEUED, NEVER PURGED"

    # Batch process execution while disabled claims 0 and completes 0
    results = await DeletionOutboxService.process_batch(session_factory=session_factory)
    assert results["claimed"] == 0
    assert results["completed"] == 0


@pytest.mark.asyncio
async def test_stale_processing_task_with_null_locked_at_recovered(db_context):
    """
    Step 4B Regression Test:
    Verifies that a task stuck in PROCESSING with locked_at=None (an anomalous or corrupted lease)
    is safely selected for recovery, receives a new fencing token, resets its lease fields,
    increments attempts with exponential backoff, and reconciles blob status to DELETION_QUEUED without purging.
    """
    session_factory, _ = db_context

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash="7" * 64,
            storage_path="images/null_lease.png",
            file_size_bytes=2048,
        )
        task = await DeletionOutboxService.enqueue_deletion(session, blob.id)
        await session.commit()
        blob_id = blob.id
        task_id = task.id

    # Seed task into PROCESSING status with locked_at=None
    initial_fencing = task.fencing_token
    async with session_factory() as session:
        t = (await session.execute(
            select(StorageDeletionOutboxModel).where(StorageDeletionOutboxModel.id == task_id)
        )).scalar_one()
        t.status = "PROCESSING"
        t.locked_at = None
        t.locked_by = "ghost-worker"
        b = (await session.execute(
            select(StorageBlobModel).where(StorageBlobModel.id == blob_id)
        )).scalar_one()
        b.status = "DELETION_IN_PROGRESS"
        await session.commit()

    now = datetime.now(timezone.utc)

    # Recovery pass
    async with session_factory() as session:
        recovered = await DeletionOutboxService.recover_stalled_tasks(session, lease_timeout_seconds=120)
        await session.commit()

        # 1. Task must be selected for recovery
        assert len(recovered) == 1
        rec = recovered[0]
        assert rec.id == task_id
        assert rec.status == "PENDING"

        # 2. Fencing token must change
        assert rec.fencing_token != initial_fencing

        # 3. Lease fields must be cleared/reset consistently
        assert rec.locked_at is None
        assert rec.locked_by is None

        # 4. Retry accounting & next_retry_at must follow established policy
        assert rec.attempts == 1
        r_retry = rec.next_retry_at if rec.next_retry_at.tzinfo else rec.next_retry_at.replace(tzinfo=timezone.utc)
        assert r_retry > now

        # 5. Blob must be reconciled safely to DELETION_QUEUED, NEVER PURGED
        b = (await session.execute(
            select(StorageBlobModel).where(StorageBlobModel.id == blob_id)
        )).scalar_one()
        assert b.status == "DELETION_QUEUED"


@pytest.mark.asyncio
async def test_already_missing_physical_file_handled_as_idempotent_success(db_context, monkeypatch):
    """
    Idempotent Deletion Test:
    When a physical file is already missing on disk (or deleter raises FileNotFoundError),
    process_batch treats it as idempotent success: task COMPLETED, blob PURGED tombstone.
    """
    session_factory, _ = db_context
    monkeypatch.setattr(settings, "ENABLE_PHYSICAL_ASSET_DELETION", True)

    class MissingFileDeleter(PhysicalBlobStorage):
        async def delete_physical_object(self, storage_path: str) -> bool:
            raise FileNotFoundError(f"No such file: {storage_path}")

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash="e" * 64,
            storage_path="images/missing_on_disk.png",
            file_size_bytes=1024,
        )
        task = await DeletionOutboxService.enqueue_deletion(session, blob.id)
        await session.commit()
        task_id = task.id
        blob_id = blob.id

    results = await DeletionOutboxService.process_batch(
        session_factory=session_factory,
        deleter=MissingFileDeleter(),
    )

    assert results["claimed"] == 1
    assert results["completed"] == 1
    assert results["failed"] == 0

    async with session_factory() as session:
        t = (await session.execute(
            select(StorageDeletionOutboxModel).where(StorageDeletionOutboxModel.id == task_id)
        )).scalar_one()
        assert t.status == "COMPLETED"

        b = (await session.execute(
            select(StorageBlobModel).where(StorageBlobModel.id == blob_id)
        )).scalar_one()
        assert b.status == "PURGED"


@pytest.mark.asyncio
async def test_repeated_settlement_of_completed_entry_is_noop(db_context):
    """
    Idempotency Test:
    Calling settle_task on a task that is already COMPLETED returns None and does not alter state.
    """
    session_factory, _ = db_context

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash="f" * 64,
            storage_path="images/double_settle.png",
            file_size_bytes=1024,
        )
        task = await DeletionOutboxService.enqueue_deletion(session, blob.id)
        await session.commit()
        blob_id = blob.id
        task_id = task.id

    # Claim
    async with session_factory() as session:
        claimed = await DeletionOutboxService.claim_pending_tasks(session, worker_id="w1", limit=1)
        await session.commit()
        token = claimed[0].fencing_token

    # Settle once -> COMPLETED
    async with session_factory() as session:
        settled1 = await DeletionOutboxService.settle_task(
            session, task_id=task_id, fencing_token=token, success=True
        )
        await session.commit()
        assert settled1 is not None
        assert settled1.status == "COMPLETED"

    # Settle second time -> returns None (safe no-op)
    async with session_factory() as session:
        settled2 = await DeletionOutboxService.settle_task(
            session, task_id=task_id, fencing_token=token, success=True
        )
        await session.commit()
        assert settled2 is None

    # Verify blob remains PURGED
    async with session_factory() as session:
        b = (await session.execute(
            select(StorageBlobModel).where(StorageBlobModel.id == blob_id)
        )).scalar_one()
        assert b.status == "PURGED"


@pytest.mark.asyncio
async def test_claim_pending_tasks_cancels_if_blob_has_live_references(db_context):
    """
    Invariant 4 Guard Test:
    If a blob gains active references while an outbox task is PENDING,
    claim_pending_tasks detects the live references, CANCELS the outbox task,
    restores the blob to AVAILABLE, and does not yield it for physical deletion.
    """
    session_factory, _ = db_context

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash="1234" * 16,
            storage_path="images/referenced_guard.png",
            file_size_bytes=1024,
        )
        task = await DeletionOutboxService.enqueue_deletion(session, blob.id)
        await session.commit()
        blob_id = blob.id
        task_id = task.id

    # Simulate concurrent re-reference: associate an asset with the blob
    async with session_factory() as session:
        b = (await session.execute(
            select(StorageBlobModel).where(StorageBlobModel.id == blob_id)
        )).scalar_one()
        b.active_ref_count = 1  # active reference added
        await session.commit()

    # Claiming should detect active reference, cancel task, and return 0 claimed
    async with session_factory() as session:
        claimed = await DeletionOutboxService.claim_pending_tasks(session, worker_id="w-guard", limit=10)
        await session.commit()
        assert len(claimed) == 0

    # Verify task is CANCELLED and blob is restored to AVAILABLE
    async with session_factory() as session:
        t = (await session.execute(
            select(StorageDeletionOutboxModel).where(StorageDeletionOutboxModel.id == task_id)
        )).scalar_one()
        assert t.status == "CANCELLED"
        assert "active references" in t.cancel_reason

        b = (await session.execute(
            select(StorageBlobModel).where(StorageBlobModel.id == blob_id)
        )).scalar_one()
        assert b.status == "AVAILABLE"


@pytest.mark.asyncio
async def test_process_batch_cancellation_releases_claimed_tasks_cleanly(db_context, monkeypatch):
    """
    Cancellation Invariant Test:
    When process_batch receives an asyncio.CancelledError during physical deletion,
    it releases all unsettled claimed tasks back to PENDING and their blobs back to DELETION_QUEUED
    so that tasks are not stranded in PROCESSING until lease timeout.
    """
    session_factory, _ = db_context
    monkeypatch.setattr(settings, "ENABLE_PHYSICAL_ASSET_DELETION", True)

    class CancellingDeleter(PhysicalBlobStorage):
        async def delete_physical_object(self, storage_path: str) -> bool:
            raise asyncio.CancelledError("Batch cancellation requested")

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash="c0ffee" * 10 + "1234",
            storage_path="images/cancel_test.png",
            file_size_bytes=1024,
        )
        task = await DeletionOutboxService.enqueue_deletion(session, blob.id)
        await session.commit()
        task_id = task.id
        blob_id = blob.id

    # Run batch with cancelling deleter; must re-raise CancelledError
    with pytest.raises(asyncio.CancelledError):
        await DeletionOutboxService.process_batch(
            session_factory=session_factory,
            deleter=CancellingDeleter(),
        )

    # Verify task was rolled back to PENDING and blob restored to DELETION_QUEUED
    async with session_factory() as session:
        t = (await session.execute(
            select(StorageDeletionOutboxModel).where(StorageDeletionOutboxModel.id == task_id)
        )).scalar_one()
        assert t.status == "PENDING"
        assert t.locked_at is None
        assert t.locked_by is None

        b = (await session.execute(
            select(StorageBlobModel).where(StorageBlobModel.id == blob_id)
        )).scalar_one()
        assert b.status == "DELETION_QUEUED"


@pytest.mark.asyncio
async def test_settle_task_sanitizes_credentials_in_cancel_reason(db_context, monkeypatch):
    """
    Security Test:
    Error messages passed to settle_task containing sensitive secrets
    (e.g., SUPABASE_SERVICE_ROLE_KEY or API_KEY_SALT) must be redacted before persisting.
    """
    session_factory, _ = db_context
    secret_key = "super_confidential_supabase_service_role_key_9999"
    monkeypatch.setattr(settings, "SUPABASE_SERVICE_ROLE_KEY", SecretStr(secret_key))

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash="sec0" * 16,
            storage_path="images/secret_sanitize.png",
            file_size_bytes=1024,
        )
        task = await DeletionOutboxService.enqueue_deletion(session, blob.id)
        await session.commit()
        task_id = task.id

    async with session_factory() as session:
        claimed = await DeletionOutboxService.claim_pending_tasks(session, worker_id="w-sec", limit=1)
        await session.commit()
        token = claimed[0].fencing_token

    # Settle with fatal 403 and error message containing secret
    leaky_error = f"Authentication failed using Bearer {secret_key}: access denied"
    async with session_factory() as session:
        settled = await DeletionOutboxService.settle_task(
            session,
            task_id=task_id,
            fencing_token=token,
            success=False,
            error_code=403,
            error_message=leaky_error,
        )
        await session.commit()

        assert settled.status == "FAILED"
        assert secret_key not in settled.cancel_reason, "Secret key was leaked in cancel_reason!"
        assert "[REDACTED]" in settled.cancel_reason


@pytest.mark.asyncio
async def test_deletion_outbox_worker_run_once_and_metrics(db_context, monkeypatch):
    """
    Background Daemon Test:
    DeletionOutboxWorker.run_once executes an outbox batch cycle and updates operational metrics.
    """
    session_factory, _ = db_context
    monkeypatch.setattr(settings, "ENABLE_PHYSICAL_ASSET_DELETION", True)

    class DummyDeleter(PhysicalBlobStorage):
        async def delete_physical_object(self, storage_path: str) -> bool:
            return True

    worker = DeletionOutboxWorker(
        session_factory=session_factory,
        worker_id="daemon-worker-test",
        interval_seconds=1.0,
        batch_limit=5,
        deleter=DummyDeleter(),
    )

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash="work" * 16,
            storage_path="images/worker_test.png",
            file_size_bytes=1024,
        )
        await DeletionOutboxService.enqueue_deletion(session, blob.id)
        await session.commit()

    results = await worker.run_once()
    assert results["claimed"] == 1
    assert results["completed"] == 1
    assert worker.metrics["batches_processed"] == 1
    assert worker.metrics["tasks_claimed"] == 1
    assert worker.metrics["tasks_completed"] == 1
    assert worker.metrics["errors"] == 0


@pytest.mark.asyncio
async def test_deletion_outbox_worker_handles_exception_without_crashing(db_context, monkeypatch):
    """
    Resilience Test:
    When an unexpected error occurs during a batch, DeletionOutboxWorker.run_once()
    catches it, records metric, and returns zero counts without crashing.
    """
    session_factory, _ = db_context

    worker = DeletionOutboxWorker(
        session_factory=session_factory,
        worker_id="daemon-worker-err",
        interval_seconds=1.0,
    )

    async def broken_process_batch(*args, **kwargs):
        raise RuntimeError("Database connection suddenly dropped")

    monkeypatch.setattr(DeletionOutboxService, "process_batch", broken_process_batch)

    results = await worker.run_once()
    assert results["claimed"] == 0
    assert results["completed"] == 0
    assert worker.metrics["errors"] == 1


@pytest.mark.asyncio
async def test_deletion_outbox_worker_continuous_run_and_graceful_stop(db_context):
    """
    Lifecycle Test:
    DeletionOutboxWorker runs continuously in background and terminates promptly when stop() is called.
    """
    session_factory, _ = db_context

    worker = DeletionOutboxWorker(
        session_factory=session_factory,
        worker_id="daemon-worker-loop",
        interval_seconds=0.05,
    )

    task = asyncio.create_task(worker.run())
    await asyncio.sleep(0.1)
    assert worker.is_running is True

    worker.stop()
    await asyncio.wait_for(task, timeout=1.0)
    assert worker.is_running is False
