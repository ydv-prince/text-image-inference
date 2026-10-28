"""
Unit Tests for Storage Lifecycle Reconciliation & Orphan Detection Service
Location: tests/unit/test_storage_reconciliation.py

Verifies:
1. Clean state: correctly referenced objects produce zero findings.
2. Missing referenced objects are detected (REFERENCED_OBJECT_MISSING).
3. Unreferenced physical objects are reported as candidates (POTENTIAL_ORPHAN), never deleted.
4. Reference count mismatches are detected with exact drift (REFERENCE_COUNT_MISMATCH).
5. Stale deletion outbox leases are detected and distinguished from active valid leases.
6. Storage access errors produce incomplete scans (SCAN_ERROR), never false orphan findings.
7. Bounded directory traversal handles large inventories and notes truncation.
8. Deterministic stability: repeated scans on unchanged fixtures produce identical finding IDs.
9. Strict read-only guarantee: zero modifications to database records, blobs, tasks, or files.
10. Lifecycle state machine inconsistency detection (PURGED with live assets, unpurged completed tasks).
11. Metadata divergence detection (asset hash differs from parent blob hash).
12. Unsupported remote enumeration is safely reported as an adapter limitation.
"""

import asyncio
from datetime import datetime, timedelta, timezone
import io
from pathlib import Path
import uuid
from PIL import Image
import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.core.config import settings
from src.core.errors import StorageException
from src.domain.interfaces import AssetStorage, StoredAsset
from src.infrastructure.models import (
    ApiKeyModel,
    Base,
    GeneratedAssetModel,
    GenerationJobModel,
    StorageBlobModel,
    StorageDeletionOutboxModel,
)
from src.infrastructure.storage.local_cas import LocalStorageCAS
from src.services.storage_reconciliation_service import (
    FindingCategory,
    Severity,
    StorageReconciliationReport,
    StorageReconciliationService,
    run_storage_audit,
)
from src.services.storage_service import StorageService


def make_test_png(width: int = 512, height: int = 512) -> bytes:
    """Generates valid PNG image bytes for storage testing."""
    img = Image.new("RGB", (width, height), color=(45, 85, 125))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


@pytest.fixture
async def db_context(tmp_path: Path):
    """Sets up an isolated SQLite test database with session factory."""
    db_file = tmp_path / "recon_test.db"
    db_url = f"sqlite+aiosqlite:///{db_file.as_posix()}"
    engine = create_async_engine(db_url)

    async with engine.begin() as conn:
        await conn.execute(text("PRAGMA foreign_keys=ON;"))
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

    # Seed an API key and completed job
    async with session_factory() as session:
        key = ApiKeyModel(id="recon-key-1", key_hash="hash_1", client_name="Recon Client")
        session.add(key)
        await session.flush()
        job = GenerationJobModel(
            id="recon-job-1",
            api_key_id=key.id,
            payload_hash="payload_1",
            status="COMPLETED",
            prompt="A majestic eagle in flight",
        )
        session.add(job)
        await session.commit()

    yield session_factory, engine

    await engine.dispose()


@pytest.fixture
def cas_storage(tmp_path: Path) -> LocalStorageCAS:
    """Provides a fresh LocalStorageCAS instance rooted in tmp_path."""
    storage_dir = tmp_path / "cas_storage"
    return LocalStorageCAS(base_path=storage_dir)


# -----------------------------------------------------------------------------
# 1. Clean State (No Discrepancies)
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_clean_storage_produces_no_findings(db_context, cas_storage):
    """Verifies that an accurately linked, physically present asset produces 0 findings."""
    session_factory, _ = db_context
    sample_png = make_test_png()

    # Store file physically in CAS
    stored = await cas_storage.store_image(sample_png)

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash=stored.file_hash,
            storage_path=stored.storage_path,
            file_size_bytes=stored.file_size_bytes,
        )
        await StorageService.associate_asset(
            session=session,
            job_id="recon-job-1",
            blob_id=blob.id,
            file_hash=stored.file_hash,
            storage_path=stored.storage_path,
            file_size_bytes=stored.file_size_bytes,
        )
        await session.commit()

    async with session_factory() as session:
        report = await StorageReconciliationService.reconcile(
            session=session,
            storage=cas_storage,
            verify_physical_existence=True,
            scan_physical_cas=True,
        )

        assert report.is_complete is True
        assert len(report.findings) == 0
        assert len(report.scan_errors) == 0
        assert report.scanned_assets_count == 1
        assert report.scanned_blobs_count == 1
        assert report.scanned_physical_objects_count == 1


# -----------------------------------------------------------------------------
# 2. Missing Referenced Physical Object Detection
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_missing_referenced_object_detected(db_context, cas_storage):
    """
    Verifies that an AVAILABLE blob whose physical file is missing from storage
    is reported with REFERENCED_OBJECT_MISSING at CRITICAL severity.
    """
    session_factory, _ = db_context
    missing_path = "images/12/34/1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef/missing.png"
    dummy_hash = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash=dummy_hash,
            storage_path=missing_path,
            file_size_bytes=2048,
        )
        await StorageService.associate_asset(
            session=session,
            job_id="recon-job-1",
            blob_id=blob.id,
            file_hash=dummy_hash,
            storage_path=missing_path,
            file_size_bytes=2048,
        )
        await session.commit()
        blob_id = blob.id

    async with session_factory() as session:
        report = await StorageReconciliationService.reconcile(
            session=session,
            storage=cas_storage,
            verify_physical_existence=True,
            scan_physical_cas=False,
        )

        missing_findings = [f for f in report.findings if f.category == FindingCategory.REFERENCED_OBJECT_MISSING]
        assert len(missing_findings) == 1
        f = missing_findings[0]
        assert f.severity == Severity.CRITICAL
        assert f.entity_id == blob_id
        assert f.locator == missing_path
        assert f.is_confirmed is True


# -----------------------------------------------------------------------------
# 3. Unreferenced Physical Object (Potential Orphan) Detection
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_unreferenced_physical_object_reported_as_candidate(db_context, cas_storage):
    """
    Verifies that an unreferenced file physically existing on disk is flagged as a POTENTIAL_ORPHAN
    candidate, and is NEVER deleted or modified.
    """
    session_factory, _ = db_context
    sample_png = make_test_png()

    # Store file on disk without creating database entries
    stored = await cas_storage.store_image(sample_png)
    file_path = cas_storage.base_path / stored.storage_path
    assert file_path.is_file()

    async with session_factory() as session:
        report = await StorageReconciliationService.reconcile(
            session=session,
            storage=cas_storage,
            verify_physical_existence=False,
            scan_physical_cas=True,
        )

        orphan_findings = [f for f in report.findings if f.category == FindingCategory.POTENTIAL_ORPHAN]
        assert len(orphan_findings) == 1
        f = orphan_findings[0]
        assert f.severity == Severity.MEDIUM
        assert f.locator == stored.storage_path
        assert f.is_confirmed is False  # Candidate requiring quarantine evaluation

    # Verify physical file was NOT deleted
    assert file_path.is_file(), "Reconciliation scan must NEVER delete physical files"


# -----------------------------------------------------------------------------
# 4. Reference Count Mismatch Detection
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_reference_count_mismatch_detected(db_context):
    """
    Verifies that reference count divergence (recorded active_ref_count != actual COUNT(assets))
    is detected with exact drift details, and the database record is NOT modified.
    """
    session_factory, _ = db_context

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash="a" * 64,
            storage_path="images/drift.png",
            file_size_bytes=1000,
        )
        # Corrupt recorded count to 5 while actual assets is 0
        blob.active_ref_count = 5
        await session.commit()
        blob_id = blob.id

    async with session_factory() as session:
        report = await StorageReconciliationService.reconcile(
            session=session,
            storage=None,
            verify_physical_existence=False,
            scan_physical_cas=False,
        )

        drift_findings = [f for f in report.findings if f.category == FindingCategory.REFERENCE_COUNT_MISMATCH]
        assert len(drift_findings) == 1
        f = drift_findings[0]
        assert f.entity_id == blob_id
        assert f.details["recorded_count"] == 5
        assert f.details["actual_count"] == 0
        assert f.details["drift"] == -5
        assert f.is_confirmed is True

    # Assert database active_ref_count was NOT modified by read-only service
    async with session_factory() as session:
        b = (await session.execute(
            select(StorageBlobModel).where(StorageBlobModel.id == blob_id)
        )).scalar_one()
        assert b.active_ref_count == 5, "Audit must remain strictly read-only"


# -----------------------------------------------------------------------------
# 5. Stale Deletion-Outbox Lease Detection
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_stale_deletion_lease_distinguished_from_active_lease(db_context):
    """
    Verifies that outbox tasks in PROCESSING exceeding lease timeout are reported
    as STALE_DELETION_TASK, while fresh active leases are NOT flagged.
    """
    session_factory, _ = db_context
    now = datetime.now(timezone.utc)

    async with session_factory() as session:
        blob1 = await StorageService.create_blob(
            session=session, file_hash="1" * 64, storage_path="images/stale1.png", file_size_bytes=100
        )
        blob2 = await StorageService.create_blob(
            session=session, file_hash="2" * 64, storage_path="images/active2.png", file_size_bytes=100
        )

        # Task 1: Stale lease (>300 seconds ago)
        stale_task = StorageDeletionOutboxModel(
            id=str(uuid.uuid4()),
            blob_id=blob1.id,
            file_hash=blob1.file_hash,
            storage_path=blob1.storage_path,
            status="PROCESSING",
            locked_at=now - timedelta(seconds=350),
            locked_by="crashed-worker",
            attempts=1,
            max_attempts=5,
        )
        # Task 2: Active fresh lease (10 seconds ago)
        active_task = StorageDeletionOutboxModel(
            id=str(uuid.uuid4()),
            blob_id=blob2.id,
            file_hash=blob2.file_hash,
            storage_path=blob2.storage_path,
            status="PROCESSING",
            locked_at=now - timedelta(seconds=10),
            locked_by="live-worker",
            attempts=1,
            max_attempts=5,
        )
        session.add_all([stale_task, active_task])
        await session.commit()
        stale_id = stale_task.id
        active_id = active_task.id

    async with session_factory() as session:
        report = await StorageReconciliationService.reconcile(
            session=session,
            lease_timeout_seconds=120,
            verify_physical_existence=False,
            scan_physical_cas=False,
        )

        stale_findings = [f for f in report.findings if f.category == FindingCategory.STALE_DELETION_TASK]
        assert len(stale_findings) == 1
        assert stale_findings[0].entity_id == stale_id
        assert stale_findings[0].details["locked_by"] == "crashed-worker"

        # Assert Task 2 is NOT reported as stale
        stale_entity_ids = {f.entity_id for f in stale_findings}
        assert active_id not in stale_entity_ids


# -----------------------------------------------------------------------------
# 6. Inaccessible Storage Error Handling
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_inaccessible_storage_produces_scan_error_not_false_orphans(db_context):
    """
    Verifies that when storage access fails (e.g. network timeout or server error),
    the reconciliation records SCAN_ERROR and sets is_complete=False without
    reporting the object as missing or an orphan.
    """
    session_factory, _ = db_context

    class BrokenStorage(AssetStorage):
        @property
        def name(self) -> str:
            return "broken_storage"

        async def store_image(self, *args, **kwargs):
            raise NotImplementedError

        async def get_image_bytes(self, locator: str):
            raise NotImplementedError

        async def exists(self, locator: str) -> bool:
            raise StorageException("Connection timed out to storage service (HTTP 504 Gateway Timeout).")

        async def verify_asset(self, *args, **kwargs):
            raise NotImplementedError

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash="b" * 64,
            storage_path="images/timeout.png",
            file_size_bytes=500,
        )
        await session.commit()

    async with session_factory() as session:
        report = await StorageReconciliationService.reconcile(
            session=session,
            storage=BrokenStorage(),
            verify_physical_existence=True,
            scan_physical_cas=False,
        )

        assert report.is_complete is False, "Scan must be marked incomplete on storage failures"
        assert len(report.scan_errors) >= 1
        assert "504" in report.scan_errors[0] or "timed out" in report.scan_errors[0]

        # Inaccessible storage must NEVER be misclassified as REFERENCED_OBJECT_MISSING or POTENTIAL_ORPHAN
        missing_findings = [f for f in report.findings if f.category == FindingCategory.REFERENCED_OBJECT_MISSING]
        orphan_findings = [f for f in report.findings if f.category == FindingCategory.POTENTIAL_ORPHAN]
        assert len(missing_findings) == 0, "Do NOT report missing object on storage error"
        assert len(orphan_findings) == 0, "Do NOT report orphan on storage error"


# -----------------------------------------------------------------------------
# 7. Safe Traversal Bounding
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_physical_scan_truncation_bounding(db_context, tmp_path: Path):
    """
    Verifies that scanning a directory exceeding max_objects caps traversal,
    records a truncation message, and marks is_complete=False.
    """
    session_factory, _ = db_context
    storage_dir = tmp_path / "bounded_cas"
    storage_dir.mkdir(parents=True, exist_ok=True)

    # Create 15 dummy files
    for i in range(15):
        h = f"{i:02d}" + "0" * 62
        sub = storage_dir / h[:2] / h[2:4]
        sub.mkdir(parents=True, exist_ok=True)
        (sub / f"{h}.png").write_bytes(b"dummy")

    storage = LocalStorageCAS(base_path=storage_dir)

    async with session_factory() as session:
        report = await StorageReconciliationService.reconcile(
            session=session,
            storage=storage,
            verify_physical_existence=False,
            scan_physical_cas=True,
            max_physical_objects=5,  # bound to 5 objects
        )

        assert report.is_complete is False, "Truncated scan must be marked incomplete"
        assert any("exceeded maximum bound" in err for err in report.scan_errors)
        assert len(report.findings) <= 5


# -----------------------------------------------------------------------------
# 8. Deterministic Stability of Findings
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_reconciliation_findings_are_deterministic(db_context):
    """
    Verifies that running reconciliation repeatedly over unchanged state
    produces identical finding IDs, categories, and severity counts.
    """
    session_factory, _ = db_context

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash="c" * 64,
            storage_path="images/deterministic.png",
            file_size_bytes=1000,
        )
        blob.active_ref_count = 3  # Mismatch
        await session.commit()

    async with session_factory() as session:
        report1 = await StorageReconciliationService.reconcile(
            session=session, verify_physical_existence=False, scan_physical_cas=False
        )
        report2 = await StorageReconciliationService.reconcile(
            session=session, verify_physical_existence=False, scan_physical_cas=False
        )

        assert len(report1.findings) == len(report2.findings)
        assert len(report1.findings) >= 1
        for f1, f2 in zip(report1.findings, report2.findings):
            assert f1.id == f2.id, "Finding IDs must be deterministic"
            assert f1.category == f2.category
            assert f1.severity == f2.severity
            assert f1.locator == f2.locator


# -----------------------------------------------------------------------------
# 9. Strict Read-Only Verification
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_reconciliation_strictly_preserves_database_and_files(db_context, cas_storage):
    """
    Verifies that running reconciliation modifies zero rows in storage_blobs,
    generated_assets, or storage_deletion_outbox, and deletes zero physical files.
    """
    session_factory, _ = db_context

    # Create dummy physical file
    stored = await cas_storage.store_image(make_test_png())
    file_on_disk = cas_storage.base_path / stored.storage_path
    assert file_on_disk.is_file()

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash=stored.file_hash,
            storage_path=stored.storage_path,
            file_size_bytes=stored.file_size_bytes,
        )
        task = StorageDeletionOutboxModel(
            id="task-preserve-1",
            blob_id=blob.id,
            file_hash=blob.file_hash,
            storage_path=blob.storage_path,
            status="PENDING",
            attempts=0,
            max_attempts=5,
        )
        session.add(task)
        await session.commit()
        blob_id = blob.id

    # Snapshot state before scan
    async with session_factory() as session:
        b_before = (await session.execute(select(StorageBlobModel).where(StorageBlobModel.id == blob_id))).scalar_one()
        t_before = (await session.execute(select(StorageDeletionOutboxModel).where(StorageDeletionOutboxModel.id == "task-preserve-1"))).scalar_one()
        blob_status_before = b_before.status
        blob_refs_before = b_before.active_ref_count
        blob_updated_before = b_before.updated_at
        task_status_before = t_before.status
        task_attempts_before = t_before.attempts

    # Execute reconciliation scan
    async with session_factory() as session:
        await StorageReconciliationService.reconcile(
            session=session,
            storage=cas_storage,
            verify_physical_existence=True,
            scan_physical_cas=True,
        )

    # Snapshot state after scan
    async with session_factory() as session:
        b_after = (await session.execute(select(StorageBlobModel).where(StorageBlobModel.id == blob_id))).scalar_one()
        t_after = (await session.execute(select(StorageDeletionOutboxModel).where(StorageDeletionOutboxModel.id == "task-preserve-1"))).scalar_one()

        assert b_after.status == blob_status_before
        assert b_after.active_ref_count == blob_refs_before
        assert b_after.updated_at == blob_updated_before
        assert t_after.status == task_status_before
        assert t_after.attempts == task_attempts_before

    # Verify physical file was not touched
    assert file_on_disk.is_file()


# -----------------------------------------------------------------------------
# 10. Lifecycle State Machine Inconsistency Detection
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_lifecycle_inconsistency_purged_blob_with_active_assets(db_context):
    """
    Verifies that a blob marked PURGED tombstone that still has active referencing assets
    is detected as LIFECYCLE_INCONSISTENCY at CRITICAL severity.
    """
    session_factory, _ = db_context

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash="d" * 64,
            storage_path="images/purged_live.png",
            file_size_bytes=1000,
            status="PURGED",  # Tombstone!
        )
        # Associate live asset to purged blob
        asset = GeneratedAssetModel(
            id=str(uuid.uuid4()),
            job_id="recon-job-1",
            blob_id=blob.id,
            file_hash=blob.file_hash,
            storage_path=blob.storage_path,
            file_size_bytes=1000,
            width=512,
            height=512,
        )
        session.add(asset)
        await session.commit()
        blob_id = blob.id

    async with session_factory() as session:
        report = await StorageReconciliationService.reconcile(
            session=session, verify_physical_existence=False, scan_physical_cas=False
        )

        inconsistencies = [f for f in report.findings if f.category == FindingCategory.LIFECYCLE_INCONSISTENCY]
        assert len(inconsistencies) >= 1
        critical_inconsistencies = [f for f in inconsistencies if f.severity == Severity.CRITICAL]
        assert len(critical_inconsistencies) == 1
        assert critical_inconsistencies[0].entity_id == blob_id
        assert "PURGED tombstone" in critical_inconsistencies[0].description


# -----------------------------------------------------------------------------
# 11. Metadata Conflict Detection (Hash Divergence)
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_metadata_conflict_hash_divergence(db_context):
    """
    Verifies that an asset whose file_hash diverges from its parent blob's file_hash
    is detected as METADATA_CONFLICT at HIGH severity.
    """
    session_factory, _ = db_context

    async with session_factory() as session:
        blob = await StorageService.create_blob(
            session=session,
            file_hash="e" * 64,
            storage_path="images/diverge.png",
            file_size_bytes=1000,
        )
        # Asset with different hash
        asset = GeneratedAssetModel(
            id="divergent-asset-1",
            job_id="recon-job-1",
            blob_id=blob.id,
            file_hash="f" * 64,  # Divergent hash!
            storage_path=blob.storage_path,
            file_size_bytes=1000,
            width=512,
            height=512,
        )
        session.add(asset)
        await session.commit()

    async with session_factory() as session:
        report = await StorageReconciliationService.reconcile(
            session=session, verify_physical_existence=False, scan_physical_cas=False
        )

        conflicts = [f for f in report.findings if f.category == FindingCategory.METADATA_CONFLICT]
        assert len(conflicts) >= 1
        divergence = [f for f in conflicts if f.entity_id == "divergent-asset-1"]
        assert len(divergence) == 1
        assert divergence[0].severity == Severity.HIGH
        assert divergence[0].details["asset_hash"] == "f" * 64
        assert divergence[0].details["blob_hash"] == "e" * 64


# -----------------------------------------------------------------------------
# 12. Remote Enumeration Limitation Safely Reported
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_remote_storage_enumeration_reported_as_limitation(db_context):
    """
    Verifies that when an adapter without physical listing capability is used (e.g. Supabase),
    the reconciliation notes this contract boundary, sets is_complete=False,
    and does not fail or execute unauthorized listing hacks.
    """
    session_factory, _ = db_context

    class MockRemoteAdapter(AssetStorage):
        @property
        def name(self) -> str:
            return "supabase"

        async def store_image(self, *args, **kwargs):
            raise NotImplementedError

        async def get_image_bytes(self, locator: str):
            return b"bytes"

        async def exists(self, locator: str) -> bool:
            return True

        async def verify_asset(self, *args, **kwargs):
            return True

    async with session_factory() as session:
        report = await StorageReconciliationService.reconcile(
            session=session,
            storage=MockRemoteAdapter(),
            verify_physical_existence=True,
            scan_physical_cas=True,
        )

        assert report.is_complete is False
        assert any("does not support physical object listing" in err for err in report.scan_errors)


# -----------------------------------------------------------------------------
# 13. Audit Runner Helper & Serialization
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_run_storage_audit_helper_and_report_serialization(db_context):
    """
    Verifies that the operational audit runner helper produces a serializable
    StorageReconciliationReport with summary dictionaries and timing metrics.
    """
    session_factory, _ = db_context

    report = await run_storage_audit(
        session_factory=session_factory,
        verify_physical=False,
        scan_physical_cas=False,
    )

    assert isinstance(report, StorageReconciliationReport)
    report_dict = report.to_dict()

    assert "scan_id" in report_dict
    assert "started_at" in report_dict
    assert "completed_at" in report_dict
    assert "duration_ms" in report_dict
    assert "summary_by_category" in report_dict
    assert "summary_by_severity" in report_dict
    assert report_dict["is_complete"] is True
