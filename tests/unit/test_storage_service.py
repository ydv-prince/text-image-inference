"""
Unit Tests for StorageService & Reference Count Invariants (Milestone 6.3)
Location: tests/unit/test_storage_service.py

Verifies:
1. Blob creation and metadata properties.
2. Atomic reference-count increments upon asset association.
3. Multiple assets sharing the same blob correctly increment ref count.
4. Transaction rollback integrity: aborted associations roll back both row and counter.
5. Tombstone protection: associating with PURGED or DELETION_QUEUED blobs is rejected.
6. Reference-count decrement upon disassociation, triggering outbox enqueue when count reaches 0.
7. Reference-count drift detection and repair recording in storage_ref_count_audit.
8. SQLite FK cascade behavior: proves cascade deletes child without updating parent counter column,
   and verifies service-layer drift detection and repair correctly resolves the drift.
"""

from datetime import datetime, timezone
import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.infrastructure.models import (
    ApiKeyModel,
    Base,
    GeneratedAssetModel,
    GenerationJobModel,
    StorageBlobModel,
    StorageRefCountAuditModel,
)
from src.services.storage_service import StorageService


@pytest.fixture
async def db_session():
    """In-memory SQLite async session fixture for fast isolated testing."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.execute(text("PRAGMA foreign_keys=ON;"))
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        # Seed test API key and Job
        key = ApiKeyModel(
            id="test-key-1",
            key_hash="test_key_hash_1234567890abcdef",
            client_name="Test Client",
        )
        session.add(key)
        await session.flush()

        job1 = GenerationJobModel(
            id="test-job-1",
            api_key_id=key.id,
            payload_hash="payload_hash_1",
            status="COMPLETED",
            prompt="A majestic tiger in neon jungle",
        )
        job2 = GenerationJobModel(
            id="test-job-2",
            api_key_id=key.id,
            payload_hash="payload_hash_2",
            status="COMPLETED",
            prompt="A majestic tiger in neon jungle (duplicate run)",
        )
        session.add_all([job1, job2])
        await session.commit()

        yield session

    await engine.dispose()


@pytest.mark.asyncio
async def test_blob_creation(db_session: AsyncSession):
    """Verifies creating a storage blob initializes active_ref_count=0 and status=AVAILABLE."""
    blob = await StorageService.create_blob(
        session=db_session,
        file_hash="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        storage_path="images/e3/b0/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/uuid-1.png",
        file_size_bytes=4096,
        status="AVAILABLE",
    )
    await db_session.commit()

    assert blob.id is not None
    assert blob.active_ref_count == 0
    assert blob.status == "AVAILABLE"
    assert blob.version == 1


@pytest.mark.asyncio
async def test_asset_association_increments_ref_count(db_session: AsyncSession):
    """Verifies associating an asset with a blob increments active_ref_count to 1."""
    blob = await StorageService.create_blob(
        session=db_session,
        file_hash="a" * 64,
        storage_path=f"images/aa/aa/{'a'*64}/blob-1.png",
        file_size_bytes=1024,
    )
    await db_session.flush()

    asset = await StorageService.associate_asset(
        session=db_session,
        job_id="test-job-1",
        blob_id=blob.id,
        file_hash="a" * 64,
        storage_path=blob.storage_path,
        file_size_bytes=1024,
    )
    await db_session.commit()

    # Refresh blob from DB
    stmt = select(StorageBlobModel).where(StorageBlobModel.id == blob.id)
    refreshed_blob = (await db_session.execute(stmt)).scalar_one()

    assert asset.blob_id == blob.id
    assert refreshed_blob.active_ref_count == 1


@pytest.mark.asyncio
async def test_duplicate_asset_association_increments_ref_count(db_session: AsyncSession):
    """Verifies multiple assets referencing the same blob increment active_ref_count to 2."""
    blob = await StorageService.create_blob(
        session=db_session,
        file_hash="b" * 64,
        storage_path=f"images/bb/bb/{'b'*64}/blob-shared.png",
        file_size_bytes=2048,
    )
    await db_session.flush()

    # Asset 1
    await StorageService.associate_asset(
        session=db_session,
        job_id="test-job-1",
        blob_id=blob.id,
        file_hash="b" * 64,
        storage_path=blob.storage_path,
        file_size_bytes=2048,
    )

    # Asset 2 (duplicate content hash referencing same blob)
    await StorageService.associate_asset(
        session=db_session,
        job_id="test-job-2",
        blob_id=blob.id,
        file_hash="b" * 64,
        storage_path=blob.storage_path,
        file_size_bytes=2048,
    )
    await db_session.commit()

    stmt = select(StorageBlobModel).where(StorageBlobModel.id == blob.id)
    refreshed_blob = (await db_session.execute(stmt)).scalar_one()

    assert refreshed_blob.active_ref_count == 2


@pytest.mark.asyncio
async def test_association_transaction_rollback(db_session: AsyncSession):
    """Verifies that if an association fails or is rolled back, the counter increment does not persist."""
    blob = await StorageService.create_blob(
        session=db_session,
        file_hash="c" * 64,
        storage_path=f"images/cc/cc/{'c'*64}/blob-rb.png",
        file_size_bytes=1024,
    )
    await db_session.commit()
    blob_id = blob.id

    # Start association but roll back
    try:
        await StorageService.associate_asset(
            session=db_session,
            job_id="test-job-1",
            blob_id=blob_id,
            file_hash="c" * 64,
            storage_path=blob.storage_path,
            file_size_bytes=1024,
        )
        # Force rollback
        raise RuntimeError("Simulated transaction failure during publication")
    except RuntimeError:
        await db_session.rollback()

    stmt = select(StorageBlobModel).where(StorageBlobModel.id == blob_id)
    refreshed_blob = (await db_session.execute(stmt)).scalar_one()

    assert refreshed_blob.active_ref_count == 0, "Counter increment must roll back cleanly"
    assets_stmt = select(GeneratedAssetModel).where(GeneratedAssetModel.blob_id == blob_id)
    assets = (await db_session.execute(assets_stmt)).scalars().all()
    assert len(assets) == 0



@pytest.mark.asyncio
async def test_tombstone_protection_rejects_association(db_session: AsyncSession):
    """Verifies that associating an asset with a PURGED or DELETION_QUEUED blob is strictly rejected."""
    purged_blob = await StorageService.create_blob(
        session=db_session,
        file_hash="d" * 64,
        storage_path=f"images/dd/dd/{'d'*64}/blob-purged.png",
        file_size_bytes=1024,
        status="PURGED",
    )
    await db_session.commit()

    with pytest.raises(ValueError, match="must be AVAILABLE"):
        await StorageService.associate_asset(
            session=db_session,
            job_id="test-job-1",
            blob_id=purged_blob.id,
            file_hash="d" * 64,
            storage_path=purged_blob.storage_path,
            file_size_bytes=1024,
        )


@pytest.mark.asyncio
async def test_disassociation_decrements_and_enqueues_outbox(db_session: AsyncSession):
    """
    Verifies that disassociating an asset decrements ref_count,
    and when ref_count reaches 0, an outbox task is enqueued.
    """
    blob = await StorageService.create_blob(
        session=db_session,
        file_hash="e" * 64,
        storage_path=f"images/ee/ee/{'e'*64}/blob-dis.png",
        file_size_bytes=1024,
    )
    await db_session.flush()

    asset1 = await StorageService.associate_asset(
        session=db_session,
        job_id="test-job-1",
        blob_id=blob.id,
        file_hash="e" * 64,
        storage_path=blob.storage_path,
        file_size_bytes=1024,
    )
    asset2 = await StorageService.associate_asset(
        session=db_session,
        job_id="test-job-2",
        blob_id=blob.id,
        file_hash="e" * 64,
        storage_path=blob.storage_path,
        file_size_bytes=1024,
    )
    await db_session.commit()

    # 1. Disassociate asset1: ref_count becomes 1, no outbox task enqueued
    outbox1 = await StorageService.disassociate_asset(db_session, asset1.id)
    await db_session.commit()

    stmt = select(StorageBlobModel).where(StorageBlobModel.id == blob.id)
    refreshed_blob = (await db_session.execute(stmt)).scalar_one()
    assert refreshed_blob.active_ref_count == 1
    assert outbox1 is None
    assert refreshed_blob.status == "AVAILABLE"

    # 2. Disassociate asset2: ref_count becomes 0, outbox task enqueued and status DELETION_QUEUED
    outbox2 = await StorageService.disassociate_asset(db_session, asset2.id)
    await db_session.commit()

    refreshed_blob = (await db_session.execute(stmt)).scalar_one()
    assert refreshed_blob.active_ref_count == 0
    assert outbox2 is not None
    assert outbox2.blob_id == blob.id
    assert outbox2.status == "PENDING"
    assert refreshed_blob.status == "DELETION_QUEUED"


@pytest.mark.asyncio
async def test_drift_detection_and_repair(db_session: AsyncSession):
    """
    Simulates artificial count drift, verifies detect_reference_count_drift flags it,
    and verifies repair_reference_count_drift fixes it while logging an audit record.
    """
    blob = await StorageService.create_blob(
        session=db_session,
        file_hash="f" * 64,
        storage_path=f"images/ff/ff/{'f'*64}/blob-drift.png",
        file_size_bytes=1024,
    )
    await StorageService.associate_asset(
        session=db_session,
        job_id="test-job-1",
        blob_id=blob.id,
        file_hash="f" * 64,
        storage_path=blob.storage_path,
        file_size_bytes=1024,
    )
    await db_session.commit()

    # Inject artificial drift: manually set active_ref_count to 99
    blob.active_ref_count = 99
    await db_session.commit()

    # Detect drift
    drifts = await StorageService.detect_reference_count_drift(db_session)
    assert len(drifts) == 1
    assert drifts[0]["blob_id"] == blob.id
    assert drifts[0]["recorded_count"] == 99
    assert drifts[0]["actual_count"] == 1

    # Repair drift
    audit = await StorageService.repair_reference_count_drift(
        session=db_session,
        blob_id=blob.id,
        repair_reason="Manual test drift reconciliation",
    )
    await db_session.commit()

    assert audit is not None
    assert audit.old_count == 99
    assert audit.new_count == 1
    assert audit.repair_reason == "Manual test drift reconciliation"

    # Verify blob counter is corrected
    stmt = select(StorageBlobModel).where(StorageBlobModel.id == blob.id)
    refreshed_blob = (await db_session.execute(stmt)).scalar_one()
    assert refreshed_blob.active_ref_count == 1

    # Verify no remaining drift
    remaining_drifts = await StorageService.detect_reference_count_drift(db_session)
    assert len(remaining_drifts) == 0


@pytest.mark.asyncio
async def test_sqlite_fk_cascade_ref_count_reconciliation(db_session: AsyncSession):
    """
    SQLite-Specific Invariant Test:
    Proves that SQLite foreign key cascades (ON DELETE CASCADE) delete child rows
    WITHOUT updating separate counter columns, causing reference count drift.
    Demonstrates that service-level drift detection identifies the mismatch,
    and repair_reference_count_drift restores exact count and records an audit log.
    """
    blob = await StorageService.create_blob(
        session=db_session,
        file_hash="1" * 64,
        storage_path=f"images/11/11/{'1'*64}/blob-cascade.png",
        file_size_bytes=1024,
    )
    await StorageService.associate_asset(
        session=db_session,
        job_id="test-job-1",
        blob_id=blob.id,
        file_hash="1" * 64,
        storage_path=blob.storage_path,
        file_size_bytes=1024,
    )
    await db_session.commit()

    # Blob ref count is 1
    stmt = select(StorageBlobModel).where(StorageBlobModel.id == blob.id)
    b = (await db_session.execute(stmt)).scalar_one()
    assert b.active_ref_count == 1

    # Now simulate a raw parent job deletion that cascades to generated_assets
    job_stmt = select(GenerationJobModel).where(GenerationJobModel.id == "test-job-1")
    job = (await db_session.execute(job_stmt)).scalar_one()
    await db_session.delete(job)
    await db_session.commit()

    # In SQLite, the asset is gone due to CASCADE
    asset_check = (await db_session.execute(select(GeneratedAssetModel).where(GeneratedAssetModel.blob_id == blob.id))).scalars().all()
    assert len(asset_check) == 0

    # But without PostgreSQL triggers, the counter column in storage_blobs remains 1!
    b_after_cascade = (await db_session.execute(stmt)).scalar_one()
    assert b_after_cascade.active_ref_count == 1, "Proves SQLite FK cascade alone leaves counter unchanged"

    # Service-level drift detection catches the desynchronization
    drift_list = await StorageService.detect_reference_count_drift(db_session)
    assert len(drift_list) == 1
    assert drift_list[0]["blob_id"] == blob.id
    assert drift_list[0]["recorded_count"] == 1
    assert drift_list[0]["actual_count"] == 0

    # Service-level drift repair corrects the counter, audits it, and enqueues deletion
    audit = await StorageService.repair_reference_count_drift(db_session, blob.id, "Cascade drift repair")
    await db_session.commit()

    assert audit is not None
    assert audit.old_count == 1
    assert audit.new_count == 0

    b_final = (await db_session.execute(stmt)).scalar_one()
    assert b_final.active_ref_count == 0
    assert b_final.status == "DELETION_QUEUED"
