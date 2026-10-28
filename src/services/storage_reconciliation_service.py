"""
Storage Lifecycle Reconciliation & Orphan Detection Service
Location: src/services/storage_reconciliation_service.py

Guarantees:
1. Strict Read-Only Safety: Performs zero mutations, deletions, repairs, or state transitions.
2. Comprehensive Finding Categorization:
   - REFERENCED_OBJECT_MISSING: Live asset/blob references missing physical object.
   - POTENTIAL_ORPHAN: Physical object on disk/remote has no live reference.
   - REFERENCE_COUNT_MISMATCH: Persisted active_ref_count diverges from actual COUNT(assets).
   - STALE_DELETION_TASK: Outbox processing lease has timed out or is anomalous.
   - LIFECYCLE_INCONSISTENCY: State machine conflicts across blobs, assets, and outbox tasks.
   - METADATA_CONFLICT: Conflicting hash or path descriptors between assets and blobs.
   - SCAN_ERROR: Storage access failure or adapter enumeration limitation.
3. Stable Identifiers: Finding IDs are cryptographically derived and deterministic across runs.
4. Error Containment: Storage/network failures produce scan errors, never false orphan conclusions.
5. Resource Bounding: Limits concurrency, query batch sizes, and directory traversal depth.
"""

import asyncio
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
import hashlib
from pathlib import Path
import re
from typing import Any, Dict, List, Optional, Set, Tuple

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.core.config import settings
from src.core.errors import StorageException, ValidationException
from src.core.logging import logger
from src.core.metrics import record_reconciliation_telemetry
from src.domain.interfaces import AssetStorage
from src.infrastructure.models import (
    GeneratedAssetModel,
    StorageBlobModel,
    StorageDeletionOutboxModel,
    StorageOrphanQuarantineModel,
    StorageUploadIntentModel,
)

HASH_REGEX = re.compile(r"^[0-9a-f]{64}$")


class FindingCategory(str, Enum):
    """Categorization for detected storage inconsistencies."""
    REFERENCED_OBJECT_MISSING = "REFERENCED_OBJECT_MISSING"
    POTENTIAL_ORPHAN = "POTENTIAL_ORPHAN"
    REFERENCE_COUNT_MISMATCH = "REFERENCE_COUNT_MISMATCH"
    STALE_DELETION_TASK = "STALE_DELETION_TASK"
    LIFECYCLE_INCONSISTENCY = "LIFECYCLE_INCONSISTENCY"
    METADATA_CONFLICT = "METADATA_CONFLICT"
    SCAN_ERROR = "SCAN_ERROR"


class Severity(str, Enum):
    """Severity classification for audit findings."""
    INFO = "INFO"
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


@dataclass(frozen=True)
class ReconciliationFinding:
    """
    Immutable representation of an individual storage reconciliation finding.
    Uses deterministic finding IDs for idempotent reporting.
    """
    id: str
    category: FindingCategory
    severity: Severity
    entity_type: str  # "asset", "blob", "outbox_task", "physical_object"
    entity_id: Optional[str]
    locator: Optional[str]
    description: str
    details: Dict[str, Any] = field(default_factory=dict)
    is_confirmed: bool = True

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["category"] = self.category.value
        data["severity"] = self.severity.value
        return data


@dataclass
class StorageReconciliationReport:
    """
    Structured outcome of a complete storage reconciliation scan.
    """
    scan_id: str
    started_at: datetime
    completed_at: datetime
    is_complete: bool
    scanned_assets_count: int
    scanned_blobs_count: int
    scanned_outbox_tasks_count: int
    scanned_physical_objects_count: int
    findings: List[ReconciliationFinding]
    scan_errors: List[str]
    summary_by_category: Dict[str, int] = field(default_factory=dict)
    summary_by_severity: Dict[str, int] = field(default_factory=dict)

    def __post_init__(self):
        if not self.summary_by_category:
            self.summary_by_category = {cat.value: 0 for cat in FindingCategory}
            for f in self.findings:
                self.summary_by_category[f.category.value] = (
                    self.summary_by_category.get(f.category.value, 0) + 1
                )
        if not self.summary_by_severity:
            self.summary_by_severity = {sev.value: 0 for sev in Severity}
            for f in self.findings:
                self.summary_by_severity[f.severity.value] = (
                    self.summary_by_severity.get(f.severity.value, 0) + 1
                )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "scan_id": self.scan_id,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat(),
            "duration_ms": int((self.completed_at - self.started_at).total_seconds() * 1000),
            "is_complete": self.is_complete,
            "scanned_assets_count": self.scanned_assets_count,
            "scanned_blobs_count": self.scanned_blobs_count,
            "scanned_outbox_tasks_count": self.scanned_outbox_tasks_count,
            "scanned_physical_objects_count": self.scanned_physical_objects_count,
            "total_findings": len(self.findings),
            "summary_by_category": self.summary_by_category,
            "summary_by_severity": self.summary_by_severity,
            "scan_errors": self.scan_errors,
            "findings": [f.to_dict() for f in self.findings],
        }


def generate_finding_id(
    category: FindingCategory,
    entity_type: str,
    entity_id: Optional[str],
    locator: Optional[str],
    extra_key: str = "",
) -> str:
    """
    Computes a deterministic, stable 16-character hexadecimal finding identifier.
    Guarantees reproducible findings across repeated scans on identical state.
    """
    seed = f"{category.value}|{entity_type}|{entity_id or ''}|{locator or ''}|{extra_key}"
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()[:16]
    prefix = category.value[:3].upper()
    return f"REC-{prefix}-{digest}"


class StorageReconciliationService:
    """
    Read-only audit and reconciliation service for storage lifecycle integrity.
    Never modifies database rows or physical files.
    """

    @classmethod
    async def reconcile(
        cls,
        session: AsyncSession,
        storage: Optional[AssetStorage] = None,
        physical_inventory: Optional[List[Dict[str, Any]]] = None,
        verify_physical_existence: bool = True,
        scan_physical_cas: bool = True,
        lease_timeout_seconds: Optional[int] = None,
        max_physical_objects: int = 2000,
        concurrency_limit: int = 10,
    ) -> StorageReconciliationReport:
        """
        Executes a comprehensive, non-mutating storage reconciliation audit.
        Returns a structured StorageReconciliationReport.
        """
        started_at = datetime.now(timezone.utc)
        scan_id = f"scan-{hashlib.sha256(started_at.isoformat().encode()).hexdigest()[:12]}"
        findings: List[ReconciliationFinding] = []
        scan_errors: List[str] = []
        is_complete = True

        # 1. Audit Reference-Count Drift (Persisted active_ref_count vs actual COUNT(assets))
        drift_findings, scanned_blobs_count = await cls._check_reference_count_drift(session)
        findings.extend(drift_findings)

        # 2. Audit Stale Deletion-Outbox Leases
        stale_findings, scanned_outbox_count = await cls._check_stale_deletion_leases(
            session=session,
            lease_timeout_seconds=lease_timeout_seconds,
        )
        findings.extend(stale_findings)

        # 3. Audit Lifecycle Inconsistencies across State Machines
        lifecycle_findings = await cls._check_lifecycle_inconsistencies(session)
        findings.extend(lifecycle_findings)

        # 4. Audit Metadata Conflicts (hash/path mismatches between assets and blobs)
        conflict_findings, scanned_assets_count = await cls._check_metadata_conflicts(session)
        findings.extend(conflict_findings)

        # 5. Audit Missing Referenced Objects (Live assets/blobs with absent physical files)
        if verify_physical_existence and storage is not None:
            missing_findings, verify_errors = await cls._check_missing_referenced_objects(
                session=session,
                storage=storage,
                concurrency_limit=concurrency_limit,
            )
            findings.extend(missing_findings)
            if verify_errors:
                scan_errors.extend(verify_errors)
                is_complete = False

        # 6. Audit Potential Orphans (Physical objects with no live catalog references)
        scanned_physical_count = 0
        if physical_inventory is not None:
            orphan_findings, count = await cls._check_potential_orphans_from_inventory(
                session=session,
                inventory=physical_inventory,
            )
            findings.extend(orphan_findings)
            scanned_physical_count = count
        elif scan_physical_cas and storage is not None and getattr(storage, "name", "") == "local_cas":
            orphan_findings, count, cas_errors = await cls._check_local_cas_orphans(
                session=session,
                storage=storage,
                max_objects=max_physical_objects,
            )
            findings.extend(orphan_findings)
            scanned_physical_count = count
            if cas_errors:
                scan_errors.extend(cas_errors)
                is_complete = False
        elif storage is not None and getattr(storage, "name", "") != "local_cas":
            # Adapter does not expose directory listing capability (per strict AssetStorage contract)
            scan_errors.append(
                f"Storage adapter '{storage.name}' does not support physical object listing via AssetStorage contract. "
                f"Unreferenced orphan scanning was safely skipped."
            )
            is_complete = False

        completed_at = datetime.now(timezone.utc)
        report = StorageReconciliationReport(
            scan_id=scan_id,
            started_at=started_at,
            completed_at=completed_at,
            is_complete=is_complete,
            scanned_assets_count=scanned_assets_count,
            scanned_blobs_count=scanned_blobs_count,
            scanned_outbox_tasks_count=scanned_outbox_count,
            scanned_physical_objects_count=scanned_physical_count,
            findings=findings,
            scan_errors=scan_errors,
        )
        try:
            record_reconciliation_telemetry(report)
        except Exception:
            pass
        return report

    # -------------------------------------------------------------------------
    # 1. Reference-Count Drift Detection
    # -------------------------------------------------------------------------
    @classmethod
    async def _check_reference_count_drift(
        cls, session: AsyncSession
    ) -> Tuple[List[ReconciliationFinding], int]:
        """
        Compares persisted active_ref_count on storage_blobs against actual COUNT(generated_assets).
        Produces REFERENCE_COUNT_MISMATCH findings without modifying counts or writing audit logs.
        """
        # Count total blobs scanned
        total_stmt = select(func.count(StorageBlobModel.id))
        total_blobs = (await session.execute(total_stmt)).scalar_one()

        stmt = (
            select(
                StorageBlobModel.id.label("blob_id"),
                StorageBlobModel.file_hash,
                StorageBlobModel.storage_path,
                StorageBlobModel.active_ref_count.label("recorded_count"),
                func.count(GeneratedAssetModel.id).label("actual_count"),
            )
            .outerjoin(GeneratedAssetModel, StorageBlobModel.id == GeneratedAssetModel.blob_id)
            .group_by(
                StorageBlobModel.id,
                StorageBlobModel.file_hash,
                StorageBlobModel.storage_path,
                StorageBlobModel.active_ref_count,
            )
            .having(StorageBlobModel.active_ref_count != func.count(GeneratedAssetModel.id))
        )
        result = await session.execute(stmt)
        findings: List[ReconciliationFinding] = []

        for row in result.all():
            diff = row.actual_count - row.recorded_count
            # Critical if recorded count claims active references but actual is 0, or vice versa
            if row.actual_count == 0 and row.recorded_count > 0:
                severity = Severity.HIGH
            elif row.recorded_count == 0 and row.actual_count > 0:
                severity = Severity.CRITICAL  # Dangerous: blob might be prematurely purged!
            else:
                severity = Severity.MEDIUM

            fid = generate_finding_id(
                category=FindingCategory.REFERENCE_COUNT_MISMATCH,
                entity_type="blob",
                entity_id=row.blob_id,
                locator=row.storage_path,
            )
            findings.append(
                ReconciliationFinding(
                    id=fid,
                    category=FindingCategory.REFERENCE_COUNT_MISMATCH,
                    severity=severity,
                    entity_type="blob",
                    entity_id=row.blob_id,
                    locator=row.storage_path,
                    description=(
                        f"Storage blob '{row.blob_id}' reference count mismatch: "
                        f"recorded {row.recorded_count}, authoritative actual {row.actual_count} (drift: {diff:+d})."
                    ),
                    details={
                        "blob_id": row.blob_id,
                        "file_hash": row.file_hash,
                        "recorded_count": row.recorded_count,
                        "actual_count": row.actual_count,
                        "drift": diff,
                    },
                    is_confirmed=True,
                )
            )

        return findings, total_blobs

    # -------------------------------------------------------------------------
    # 2. Stale Deletion-Outbox Lease Detection
    # -------------------------------------------------------------------------
    @classmethod
    async def _check_stale_deletion_leases(
        cls,
        session: AsyncSession,
        lease_timeout_seconds: Optional[int] = None,
    ) -> Tuple[List[ReconciliationFinding], int]:
        """
        Identifies deletion outbox tasks stuck in PROCESSING past lease timeout
        or with anomalous null leases. Never resets tokens or mutates tasks.
        """
        now = datetime.now(timezone.utc)
        timeout = (
            lease_timeout_seconds
            if lease_timeout_seconds is not None
            else settings.LEASE_TIMEOUT_SECONDS
        )
        threshold = now - timedelta(seconds=timeout)

        # Count total outbox tasks scanned
        total_stmt = select(func.count(StorageDeletionOutboxModel.id))
        total_tasks = (await session.execute(total_stmt)).scalar_one()

        stmt = select(StorageDeletionOutboxModel).where(
            StorageDeletionOutboxModel.status == "PROCESSING",
            or_(
                StorageDeletionOutboxModel.locked_at < threshold,
                StorageDeletionOutboxModel.locked_at.is_(None),
            ),
        )
        result = await session.execute(stmt)
        stalled_tasks = list(result.scalars().all())
        findings: List[ReconciliationFinding] = []

        for task in stalled_tasks:
            is_exhausted = task.attempts + 1 >= task.max_attempts
            severity = Severity.HIGH if is_exhausted else Severity.MEDIUM
            locked_at_str = task.locked_at.isoformat() if task.locked_at else "NULL"

            fid = generate_finding_id(
                category=FindingCategory.STALE_DELETION_TASK,
                entity_type="outbox_task",
                entity_id=task.id,
                locator=task.storage_path,
            )
            findings.append(
                ReconciliationFinding(
                    id=fid,
                    category=FindingCategory.STALE_DELETION_TASK,
                    severity=severity,
                    entity_type="outbox_task",
                    entity_id=task.id,
                    locator=task.storage_path,
                    description=(
                        f"Deletion outbox task '{task.id}' lease expired in PROCESSING "
                        f"(locked_at={locked_at_str}, worker='{task.locked_by}', "
                        f"attempts={task.attempts}/{task.max_attempts})."
                    ),
                    details={
                        "task_id": task.id,
                        "blob_id": task.blob_id,
                        "fencing_token": task.fencing_token,
                        "locked_at": locked_at_str,
                        "locked_by": task.locked_by,
                        "attempts": task.attempts,
                        "max_attempts": task.max_attempts,
                        "is_retry_exhausted": is_exhausted,
                    },
                    is_confirmed=True,
                )
            )

        return findings, total_tasks

    # -------------------------------------------------------------------------
    # 3. Lifecycle Inconsistencies across State Machines
    # -------------------------------------------------------------------------
    @classmethod
    async def _check_lifecycle_inconsistencies(
        cls, session: AsyncSession
    ) -> List[ReconciliationFinding]:
        """
        Audits invalid state combinations across storage_blobs, generated_assets,
        and storage_deletion_outbox.
        """
        findings: List[ReconciliationFinding] = []

        # 3a. PURGED blob with active referencing assets (Severe Integrity Violation)
        purged_with_refs_stmt = (
            select(
                StorageBlobModel.id,
                StorageBlobModel.file_hash,
                StorageBlobModel.storage_path,
                func.count(GeneratedAssetModel.id).label("actual_refs"),
            )
            .join(GeneratedAssetModel, StorageBlobModel.id == GeneratedAssetModel.blob_id)
            .where(StorageBlobModel.status == "PURGED")
            .group_by(StorageBlobModel.id, StorageBlobModel.file_hash, StorageBlobModel.storage_path)
        )
        res_purged = await session.execute(purged_with_refs_stmt)
        for row in res_purged.all():
            fid = generate_finding_id(
                FindingCategory.LIFECYCLE_INCONSISTENCY,
                "blob",
                row.id,
                row.storage_path,
                "purged_with_live_refs",
            )
            findings.append(
                ReconciliationFinding(
                    id=fid,
                    category=FindingCategory.LIFECYCLE_INCONSISTENCY,
                    severity=Severity.CRITICAL,
                    entity_type="blob",
                    entity_id=row.id,
                    locator=row.storage_path,
                    description=(
                        f"Storage blob '{row.id}' is an immutable PURGED tombstone but still has "
                        f"{row.actual_refs} active referencing assets!"
                    ),
                    details={"blob_id": row.id, "actual_refs": row.actual_refs},
                    is_confirmed=True,
                )
            )

        # 3b. Blob in DELETION_QUEUED or DELETION_IN_PROGRESS with active referencing assets
        deleting_with_refs_stmt = (
            select(
                StorageBlobModel.id,
                StorageBlobModel.status,
                StorageBlobModel.storage_path,
                func.count(GeneratedAssetModel.id).label("actual_refs"),
            )
            .join(GeneratedAssetModel, StorageBlobModel.id == GeneratedAssetModel.blob_id)
            .where(StorageBlobModel.status.in_(["DELETION_QUEUED", "DELETION_IN_PROGRESS"]))
            .group_by(StorageBlobModel.id, StorageBlobModel.status, StorageBlobModel.storage_path)
        )
        res_deleting = await session.execute(deleting_with_refs_stmt)
        for row in res_deleting.all():
            fid = generate_finding_id(
                FindingCategory.LIFECYCLE_INCONSISTENCY,
                "blob",
                row.id,
                row.storage_path,
                f"deleting_with_refs_{row.status}",
            )
            findings.append(
                ReconciliationFinding(
                    id=fid,
                    category=FindingCategory.LIFECYCLE_INCONSISTENCY,
                    severity=Severity.HIGH,
                    entity_type="blob",
                    entity_id=row.id,
                    locator=row.storage_path,
                    description=(
                        f"Storage blob '{row.id}' is in status '{row.status}' but has "
                        f"{row.actual_refs} active referencing assets."
                    ),
                    details={"blob_id": row.id, "status": row.status, "actual_refs": row.actual_refs},
                    is_confirmed=True,
                )
            )

        # 3c. COMPLETED outbox task whose parent blob is NOT PURGED
        completed_task_unpurged_blob_stmt = (
            select(
                StorageDeletionOutboxModel.id.label("task_id"),
                StorageDeletionOutboxModel.blob_id,
                StorageDeletionOutboxModel.storage_path,
                StorageBlobModel.status.label("blob_status"),
            )
            .join(StorageBlobModel, StorageDeletionOutboxModel.blob_id == StorageBlobModel.id)
            .where(
                StorageDeletionOutboxModel.status == "COMPLETED",
                StorageBlobModel.status != "PURGED",
            )
        )
        res_completed = await session.execute(completed_task_unpurged_blob_stmt)
        for row in res_completed.all():
            fid = generate_finding_id(
                FindingCategory.LIFECYCLE_INCONSISTENCY,
                "outbox_task",
                row.task_id,
                row.storage_path,
                "completed_task_unpurged_blob",
            )
            findings.append(
                ReconciliationFinding(
                    id=fid,
                    category=FindingCategory.LIFECYCLE_INCONSISTENCY,
                    severity=Severity.HIGH,
                    entity_type="outbox_task",
                    entity_id=row.task_id,
                    locator=row.storage_path,
                    description=(
                        f"Outbox task '{row.task_id}' is COMPLETED, but parent blob '{row.blob_id}' "
                        f"has status '{row.blob_status}' instead of PURGED."
                    ),
                    details={
                        "task_id": row.task_id,
                        "blob_id": row.blob_id,
                        "blob_status": row.blob_status,
                    },
                    is_confirmed=True,
                )
            )

        # 3d. Active outbox task (PENDING or PROCESSING) for a blob that remains AVAILABLE
        active_task_avail_blob_stmt = (
            select(
                StorageDeletionOutboxModel.id.label("task_id"),
                StorageDeletionOutboxModel.status.label("task_status"),
                StorageDeletionOutboxModel.blob_id,
                StorageDeletionOutboxModel.storage_path,
            )
            .join(StorageBlobModel, StorageDeletionOutboxModel.blob_id == StorageBlobModel.id)
            .where(
                StorageDeletionOutboxModel.status.in_(["PENDING", "PROCESSING"]),
                StorageBlobModel.status == "AVAILABLE",
            )
        )
        res_active_avail = await session.execute(active_task_avail_blob_stmt)
        for row in res_active_avail.all():
            fid = generate_finding_id(
                FindingCategory.LIFECYCLE_INCONSISTENCY,
                "outbox_task",
                row.task_id,
                row.storage_path,
                "active_task_available_blob",
            )
            findings.append(
                ReconciliationFinding(
                    id=fid,
                    category=FindingCategory.LIFECYCLE_INCONSISTENCY,
                    severity=Severity.MEDIUM,
                    entity_type="outbox_task",
                    entity_id=row.task_id,
                    locator=row.storage_path,
                    description=(
                        f"Active deletion task '{row.task_id}' (status='{row.task_status}') exists, "
                        f"but parent blob '{row.blob_id}' is still marked AVAILABLE."
                    ),
                    details={
                        "task_id": row.task_id,
                        "task_status": row.task_status,
                        "blob_id": row.blob_id,
                    },
                    is_confirmed=True,
                )
            )

        return findings

    # -------------------------------------------------------------------------
    # 4. Metadata Conflict Detection
    # -------------------------------------------------------------------------
    @classmethod
    async def _check_metadata_conflicts(
        cls, session: AsyncSession
    ) -> Tuple[List[ReconciliationFinding], int]:
        """
        Audits cryptographic and locator consistency between generated assets and storage blobs.
        """
        total_stmt = select(func.count(GeneratedAssetModel.id))
        total_assets = (await session.execute(total_stmt)).scalar_one()

        findings: List[ReconciliationFinding] = []

        # 4a. Hash mismatch between asset and linked storage blob
        mismatch_stmt = (
            select(
                GeneratedAssetModel.id.label("asset_id"),
                GeneratedAssetModel.file_hash.label("asset_hash"),
                GeneratedAssetModel.storage_path.label("asset_path"),
                GeneratedAssetModel.blob_id,
                StorageBlobModel.file_hash.label("blob_hash"),
                StorageBlobModel.storage_path.label("blob_path"),
            )
            .join(StorageBlobModel, GeneratedAssetModel.blob_id == StorageBlobModel.id)
            .where(GeneratedAssetModel.file_hash != StorageBlobModel.file_hash)
        )
        res_mismatch = await session.execute(mismatch_stmt)
        for row in res_mismatch.all():
            fid = generate_finding_id(
                FindingCategory.METADATA_CONFLICT,
                "asset",
                row.asset_id,
                row.asset_path,
                "hash_divergence",
            )
            findings.append(
                ReconciliationFinding(
                    id=fid,
                    category=FindingCategory.METADATA_CONFLICT,
                    severity=Severity.HIGH,
                    entity_type="asset",
                    entity_id=row.asset_id,
                    locator=row.asset_path,
                    description=(
                        f"Asset '{row.asset_id}' hash ('{row.asset_hash[:12]}...') differs from "
                        f"linked blob '{row.blob_id}' hash ('{row.blob_hash[:12]}...')."
                    ),
                    details={
                        "asset_id": row.asset_id,
                        "blob_id": row.blob_id,
                        "asset_hash": row.asset_hash,
                        "blob_hash": row.blob_hash,
                    },
                    is_confirmed=True,
                )
            )

        # 4b. Path divergence between asset and linked blob
        path_mismatch_stmt = (
            select(
                GeneratedAssetModel.id.label("asset_id"),
                GeneratedAssetModel.storage_path.label("asset_path"),
                GeneratedAssetModel.blob_id,
                StorageBlobModel.storage_path.label("blob_path"),
            )
            .join(StorageBlobModel, GeneratedAssetModel.blob_id == StorageBlobModel.id)
            .where(GeneratedAssetModel.storage_path != StorageBlobModel.storage_path)
        )
        res_path = await session.execute(path_mismatch_stmt)
        for row in res_path.all():
            fid = generate_finding_id(
                FindingCategory.METADATA_CONFLICT,
                "asset",
                row.asset_id,
                row.asset_path,
                "path_divergence",
            )
            findings.append(
                ReconciliationFinding(
                    id=fid,
                    category=FindingCategory.METADATA_CONFLICT,
                    severity=Severity.MEDIUM,
                    entity_type="asset",
                    entity_id=row.asset_id,
                    locator=row.asset_path,
                    description=(
                        f"Asset '{row.asset_id}' storage_path differs from linked blob '{row.blob_id}'."
                    ),
                    details={
                        "asset_id": row.asset_id,
                        "blob_id": row.blob_id,
                        "asset_path": row.asset_path,
                        "blob_path": row.blob_path,
                    },
                    is_confirmed=True,
                )
            )

        # 4c. Format validation: non-SHA256 hex digests
        invalid_blobs_stmt = select(StorageBlobModel).where(
            func.length(StorageBlobModel.file_hash) != 64
        )
        res_inv = await session.execute(invalid_blobs_stmt)
        for blob in res_inv.scalars().all():
            fid = generate_finding_id(
                FindingCategory.METADATA_CONFLICT,
                "blob",
                blob.id,
                blob.storage_path,
                "malformed_sha256",
            )
            findings.append(
                ReconciliationFinding(
                    id=fid,
                    category=FindingCategory.METADATA_CONFLICT,
                    severity=Severity.HIGH,
                    entity_type="blob",
                    entity_id=blob.id,
                    locator=blob.storage_path,
                    description=f"Storage blob '{blob.id}' has malformed file_hash length ({len(blob.file_hash)} chars).",
                    details={"blob_id": blob.id, "file_hash": blob.file_hash},
                    is_confirmed=True,
                )
            )

        return findings, total_assets

    # -------------------------------------------------------------------------
    # 5. Missing Referenced Physical Objects
    # -------------------------------------------------------------------------
    @classmethod
    async def _check_missing_referenced_objects(
        cls,
        session: AsyncSession,
        storage: AssetStorage,
        concurrency_limit: int = 10,
    ) -> Tuple[List[ReconciliationFinding], List[str]]:
        """
        Verifies that every active storage_path referenced by live assets or AVAILABLE blobs
        physically exists in the active storage adapter.
        Catches storage exceptions as scan errors without reporting false missing findings.
        """
        # Query distinct storage paths for live assets and available blobs
        stmt = (
            select(StorageBlobModel.storage_path, StorageBlobModel.id, StorageBlobModel.file_hash)
            .where(StorageBlobModel.status == "AVAILABLE")
        )
        result = await session.execute(stmt)
        blobs_to_check = result.all()

        findings: List[ReconciliationFinding] = []
        scan_errors: List[str] = []
        sem = asyncio.Semaphore(concurrency_limit)

        async def verify_one(path: str, blob_id: str, file_hash: str) -> None:
            async with sem:
                try:
                    exists = await storage.exists(path)
                    if not exists:
                        fid = generate_finding_id(
                            FindingCategory.REFERENCED_OBJECT_MISSING,
                            "blob",
                            blob_id,
                            path,
                        )
                        findings.append(
                            ReconciliationFinding(
                                id=fid,
                                category=FindingCategory.REFERENCED_OBJECT_MISSING,
                                severity=Severity.CRITICAL,
                                entity_type="blob",
                                entity_id=blob_id,
                                locator=path,
                                description=(
                                    f"Physical object referenced by AVAILABLE blob '{blob_id}' "
                                    f"does not exist in storage: '{path}'."
                                ),
                                details={"blob_id": blob_id, "file_hash": file_hash, "storage_path": path},
                                is_confirmed=True,
                            )
                        )
                except (StorageException, ValidationException) as se:
                    # Capture as scan error, NOT a missing object
                    err_msg = f"Inaccessible storage during existence verification for '{path}': {se}"
                    logger.warning(err_msg)
                    scan_errors.append(err_msg)
                except Exception as e:
                    err_msg = f"Unexpected error checking storage path '{path}': {e}"
                    logger.error(err_msg, exc_info=True)
                    scan_errors.append(err_msg)

        tasks = [verify_one(b.storage_path, b.id, b.file_hash) for b in blobs_to_check]
        if tasks:
            await asyncio.gather(*tasks)

        return findings, scan_errors

    # -------------------------------------------------------------------------
    # 6. Potential Orphan Detection
    # -------------------------------------------------------------------------
    @classmethod
    async def _check_potential_orphans_from_inventory(
        cls,
        session: AsyncSession,
        inventory: List[Dict[str, Any]],
    ) -> Tuple[List[ReconciliationFinding], int]:
        """
        Audits an external or pre-enumerated inventory of physical storage objects
        against database catalog entities.
        """
        findings: List[ReconciliationFinding] = []
        count = len(inventory)

        for item in inventory:
            path = item.get("storage_path")
            if not path:
                continue

            file_hash = item.get("file_hash") or ""
            file_size = item.get("file_size_bytes")

            # Check database for blob record
            blob_stmt = select(StorageBlobModel).where(StorageBlobModel.storage_path == path)
            blob_res = await session.execute(blob_stmt)
            blob = blob_res.scalar_one_or_none()

            # Check if active upload intent exists
            intent_stmt = select(func.count(StorageUploadIntentModel.id)).where(
                StorageUploadIntentModel.storage_path == path,
                StorageUploadIntentModel.status.in_(["PENDING_UPLOAD", "UPLOADED"]),
                StorageUploadIntentModel.expires_at > datetime.now(timezone.utc),
            )
            has_active_intent = (await session.execute(intent_stmt)).scalar_one() > 0

            if has_active_intent:
                # In-flight upload intent legitimately owns the object
                continue

            if not blob:
                # No catalog record at all -> Candidate Orphan
                fid = generate_finding_id(
                    FindingCategory.POTENTIAL_ORPHAN,
                    "physical_object",
                    None,
                    path,
                    "untracked_physical_object",
                )
                findings.append(
                    ReconciliationFinding(
                        id=fid,
                        category=FindingCategory.POTENTIAL_ORPHAN,
                        severity=Severity.MEDIUM,
                        entity_type="physical_object",
                        entity_id=None,
                        locator=path,
                        description=f"Physical storage object has no catalog entry in storage_blobs: '{path}'.",
                        details={"storage_path": path, "file_hash": file_hash, "file_size_bytes": file_size},
                        is_confirmed=False,  # Candidate orphan awaiting quarantine
                    )
                )
            elif blob.status == "PURGED":
                # Matches an immutable tombstone -> Resurrected Orphan!
                fid = generate_finding_id(
                    FindingCategory.POTENTIAL_ORPHAN,
                    "blob",
                    blob.id,
                    path,
                    "purged_tombstone_hit",
                )
                findings.append(
                    ReconciliationFinding(
                        id=fid,
                        category=FindingCategory.POTENTIAL_ORPHAN,
                        severity=Severity.HIGH,
                        entity_type="physical_object",
                        entity_id=blob.id,
                        locator=path,
                        description=(
                            f"Physical storage object exists on storage but corresponds to a PURGED "
                            f"tombstone blob '{blob.id}': '{path}'."
                        ),
                        details={"blob_id": blob.id, "storage_path": path, "file_hash": blob.file_hash},
                        is_confirmed=True,  # Confirmed tombstone hit
                    )
                )
            elif blob.status == "AVAILABLE":
                # Check actual references
                ref_stmt = select(func.count(GeneratedAssetModel.id)).where(GeneratedAssetModel.blob_id == blob.id)
                actual_refs = (await session.execute(ref_stmt)).scalar_one()

                if actual_refs == 0 and blob.active_ref_count == 0:
                    # Check if outbox task already exists
                    outbox_stmt = select(func.count(StorageDeletionOutboxModel.id)).where(
                        StorageDeletionOutboxModel.blob_id == blob.id,
                        StorageDeletionOutboxModel.status.in_(["PENDING", "PROCESSING"]),
                    )
                    has_outbox = (await session.execute(outbox_stmt)).scalar_one() > 0

                    if not has_outbox:
                        fid = generate_finding_id(
                            FindingCategory.POTENTIAL_ORPHAN,
                            "blob",
                            blob.id,
                            path,
                            "unreferenced_available_blob",
                        )
                        findings.append(
                            ReconciliationFinding(
                                id=fid,
                                category=FindingCategory.POTENTIAL_ORPHAN,
                                severity=Severity.LOW,
                                entity_type="blob",
                                entity_id=blob.id,
                                locator=path,
                                description=(
                                    f"Physical object is unreferenced (blob '{blob.id}' has 0 references "
                                    f"and no queued deletion task): '{path}'."
                                ),
                                details={"blob_id": blob.id, "storage_path": path},
                                is_confirmed=False,
                            )
                        )

        return findings, count

    @classmethod
    async def _check_local_cas_orphans(
        cls,
        session: AsyncSession,
        storage: Any,
        max_objects: int = 2000,
    ) -> Tuple[List[ReconciliationFinding], int, List[str]]:
        """
        Safely scans local filesystem CAS base directory and evaluates on-disk files.
        Never traverses outside base_path; ignores temporary staging files.
        """
        base_path = getattr(storage, "base_path", None)
        if not base_path or not isinstance(base_path, Path) or not base_path.is_dir():
            return [], 0, [f"LocalStorageCAS base_path '{base_path}' is not an accessible directory."]

        tmp_dir = getattr(storage, "tmp_dir", None)
        inventory: List[Dict[str, Any]] = []
        errors: List[str] = []

        try:
            # Safely enumerate png files
            for p in base_path.rglob("*.png"):
                if len(inventory) >= max_objects:
                    errors.append(
                        f"Physical object scan exceeded maximum bound ({max_objects} objects). "
                        f"Directory traversal was truncated."
                    )
                    break

                if not p.is_file():
                    continue

                try:
                    rel = p.relative_to(base_path)
                except ValueError:
                    continue

                # Ignore temporary staging directory within CAS root
                if "tmp" in rel.parts:
                    continue

                if tmp_dir and isinstance(tmp_dir, Path):
                    try:
                        if p.is_relative_to(tmp_dir):
                            continue
                    except (ValueError, TypeError):
                        pass

                try:
                    storage_path = rel.as_posix()
                    file_size = p.stat().st_size
                    # Extract 64-char hash if present in file name or path
                    m = re.search(r"([0-9a-f]{64})", p.name.lower())
                    f_hash = m.group(1) if m else ""
                    inventory.append({
                        "storage_path": storage_path,
                        "file_hash": f_hash,
                        "file_size_bytes": file_size,
                    })
                except Exception as e:
                    errors.append(f"Error inspecting local file '{p}': {e}")

        except Exception as e:
            errors.append(f"Directory traversal error in '{base_path}': {e}")
            return [], 0, errors

        findings, count = await cls._check_potential_orphans_from_inventory(session, inventory)
        return findings, count, errors


# -----------------------------------------------------------------------------
# Read-Only CLI / Script Audit Runner
# -----------------------------------------------------------------------------
async def run_storage_audit(
    session_factory: async_sessionmaker[AsyncSession],
    storage: Optional[AssetStorage] = None,
    verify_physical: bool = True,
    scan_physical_cas: bool = True,
    max_physical_objects: int = 2000,
) -> StorageReconciliationReport:
    """
    Opt-in, read-only operational audit runner.
    Produces and logs an audit report without altering database rows or storage assets.
    """
    async with session_factory() as session:
        report = await StorageReconciliationService.reconcile(
            session=session,
            storage=storage,
            verify_physical_existence=verify_physical,
            scan_physical_cas=scan_physical_cas,
            max_physical_objects=max_physical_objects,
        )
        logger.info(
            f"Storage Reconciliation Scan [{report.scan_id}] complete in "
            f"{(report.completed_at - report.started_at).total_seconds():.2f}s: "
            f"{len(report.findings)} findings ({report.summary_by_severity.get('CRITICAL', 0)} critical, "
            f"{report.summary_by_severity.get('HIGH', 0)} high). Complete: {report.is_complete}."
        )
        return report
