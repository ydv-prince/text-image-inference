"""
Storage Service & Reference Count Management (Milestone 6.3)
Location: src/services/storage_service.py

Guarantees:
1. Reference Immutability & Tombstone Protection: References can only attach to AVAILABLE blobs.
   Attempting to link to PURGED or DELETION_QUEUED blobs is strictly rejected.
2. Atomic Reference Counting: Service-level maintenance ensures ref counts remain accurate
   on SQLite where PostgreSQL PL/pgSQL triggers are not executed.
3. Drift Detection & Non-Overwriting Repair: Audits mismatches into storage_ref_count_audit.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import uuid

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.config import settings
from src.core.logging import logger
from src.infrastructure.models import (
    GeneratedAssetModel,
    StorageBlobModel,
    StorageRefCountAuditModel,
)


class StorageService:
    """Service managing persistent blob metadata and asset associations."""

    @staticmethod
    async def create_blob(
        session: AsyncSession,
        file_hash: str,
        storage_path: str,
        file_size_bytes: int,
        storage_provider: str = "local_cas",
        mime_type: str = "image/png",
        status: str = "AVAILABLE",
    ) -> StorageBlobModel:
        """Registers a new storage blob in the catalog."""
        blob = StorageBlobModel(
            id=str(uuid.uuid4()),
            file_hash=file_hash,
            storage_path=storage_path,
            storage_provider=storage_provider,
            file_size_bytes=file_size_bytes,
            mime_type=mime_type,
            status=status,
            active_ref_count=0,
            version=1,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        session.add(blob)
        await session.flush()
        return blob

    @staticmethod
    async def associate_asset(
        session: AsyncSession,
        job_id: str,
        blob_id: str,
        file_hash: str,
        storage_path: str,
        file_size_bytes: int,
        storage_provider: str = "local_cas",
        width: int = 512,
        height: int = 512,
        mime_type: str = "image/png",
        is_synthetic: bool = False,
    ) -> GeneratedAssetModel:
        """
        Associates a generated asset with a storage blob.
        Enforces tombstone protection: strictly rejects associations if blob is not AVAILABLE.
        Atomically increments active_ref_count on SQLite.
        """
        # Query referenced blob
        stmt = select(StorageBlobModel).where(StorageBlobModel.id == blob_id)
        result = await session.execute(stmt)
        blob = result.scalar_one_or_none()

        if not blob:
            raise ValueError(f"Referenced storage_blob '{blob_id}' does not exist.")

        # Tombstone & State Protection
        if blob.status != "AVAILABLE":
            raise ValueError(
                f"Cannot associate asset with storage_blob '{blob_id}': status is '{blob.status}' (must be AVAILABLE)."
            )

        asset = GeneratedAssetModel(
            id=str(uuid.uuid4()),
            job_id=job_id,
            blob_id=blob.id,
            file_hash=file_hash,
            storage_path=storage_path,
            storage_provider=storage_provider,
            file_size_bytes=file_size_bytes,
            mime_type=mime_type,
            width=width,
            height=height,
            is_synthetic=is_synthetic,
            created_at=datetime.now(timezone.utc),
        )
        session.add(asset)

        # Reference counting ownership:
        # - On SQLite: incremented here in application logic because SQLite lacks procedural DB triggers.
        # - On PostgreSQL: managed exclusively by trigger trg_40_sync_ref_count_insert to avoid double-counting.
        if settings.is_sqlite:
            blob.active_ref_count = blob.active_ref_count + 1
            blob.updated_at = datetime.now(timezone.utc)

        await session.flush()
        if not settings.is_sqlite:
            await session.refresh(blob, ["active_ref_count", "updated_at"])

        return asset

    @staticmethod
    async def disassociate_asset(
        session: AsyncSession,
        asset_id: str,
    ) -> Optional[Any]:
        """
        Disassociates and deletes an asset row.
        Atomically decrements active_ref_count. If count reaches 0, enqueues deletion outbox task.
        """
        from src.services.deletion_outbox_service import DeletionOutboxService

        stmt = select(GeneratedAssetModel).where(GeneratedAssetModel.id == asset_id)
        result = await session.execute(stmt)
        asset = result.scalar_one_or_none()

        if not asset:
            return None

        # On PostgreSQL, acquire transaction advisory lock derived from asset file_hash
        # before deleting asset and triggering trg_60 row locks on storage_blobs.
        # This guarantees: Advisory Lock (file_hash) -> Level 2 (asset delete) -> Level 3 (blobs trigger).
        if not settings.is_sqlite and asset.file_hash:
            await DeletionOutboxService.acquire_hash_advisory_lock(session, asset.file_hash)

        blob_id = asset.blob_id
        await session.delete(asset)

        outbox_task = None
        if blob_id:
            blob_stmt = select(StorageBlobModel).where(StorageBlobModel.id == blob_id)
            blob_res = await session.execute(blob_stmt)
            blob = blob_res.scalar_one_or_none()

            if blob:
                # Reference counting ownership:
                # - On SQLite: decremented here in application logic.
                # - On PostgreSQL: managed exclusively by trigger trg_60_sync_ref_count_delete.
                if settings.is_sqlite:
                    blob.active_ref_count = max(0, blob.active_ref_count - 1)
                    blob.updated_at = datetime.now(timezone.utc)
                    await session.flush()
                else:
                    await session.flush()
                    await session.refresh(blob, ["active_ref_count", "updated_at"])

                # If references reached zero, enqueue physical deletion via outbox
                if blob.active_ref_count == 0:
                    outbox_task = await DeletionOutboxService.enqueue_deletion(session, blob.id)

        await session.flush()
        return outbox_task

    @staticmethod
    async def detect_reference_count_drift(session: AsyncSession) -> List[Dict[str, Any]]:
        """
        Compares recorded active_ref_count against actual COUNT(generated_assets).
        Returns list of blobs exhibiting count drift.
        """
        stmt = (
            select(
                StorageBlobModel.id.label("blob_id"),
                StorageBlobModel.file_hash,
                StorageBlobModel.active_ref_count.label("recorded_count"),
                func.count(GeneratedAssetModel.id).label("actual_count"),
            )
            .outerjoin(GeneratedAssetModel, StorageBlobModel.id == GeneratedAssetModel.blob_id)
            .group_by(StorageBlobModel.id, StorageBlobModel.file_hash, StorageBlobModel.active_ref_count)
            .having(StorageBlobModel.active_ref_count != func.count(GeneratedAssetModel.id))
        )
        result = await session.execute(stmt)
        drifts = []
        for row in result.all():
            drifts.append({
                "blob_id": row.blob_id,
                "file_hash": row.file_hash,
                "recorded_count": row.recorded_count,
                "actual_count": row.actual_count,
            })
        return drifts

    @staticmethod
    async def repair_reference_count_drift(
        session: AsyncSession,
        blob_id: str,
        repair_reason: str = "Automated drift reconciliation",
    ) -> Optional[StorageRefCountAuditModel]:
        """
        Audits and reconciles reference count drift for a given blob.
        Writes immutable record to storage_ref_count_audit and corrects active_ref_count.
        """
        from src.services.deletion_outbox_service import DeletionOutboxService

        blob_stmt = select(StorageBlobModel).where(StorageBlobModel.id == blob_id)
        blob_res = await session.execute(blob_stmt)
        blob = blob_res.scalar_one_or_none()

        if not blob:
            return None

        # Compute actual references
        count_stmt = select(func.count(GeneratedAssetModel.id)).where(GeneratedAssetModel.blob_id == blob_id)
        count_res = await session.execute(count_stmt)
        actual_count = count_res.scalar_one()

        if blob.active_ref_count != actual_count:
            # On PostgreSQL, acquire transaction-scoped advisory lock derived from file_hash
            # and re-fetch blob with FOR UPDATE to enforce Level 3 lock under advisory lock.
            # This guarantees: Advisory Lock (file_hash) -> Level 3 (storage_blobs FOR UPDATE) -> Level 8 (audit).
            if not settings.is_sqlite and blob.file_hash:
                await session.execute(
                    text("SELECT pg_advisory_xact_lock(fn_hash_to_lock_key(:file_hash));"),
                    {"file_hash": blob.file_hash},
                )
                lock_stmt = select(StorageBlobModel).where(StorageBlobModel.id == blob_id).with_for_update()
                lock_res = await session.execute(lock_stmt)
                locked_blob = lock_res.scalar_one_or_none()
                if locked_blob:
                    blob = locked_blob

            audit = StorageRefCountAuditModel(
                id=str(uuid.uuid4()),
                blob_id=blob.id,
                old_count=blob.active_ref_count,
                new_count=actual_count,
                repair_reason=repair_reason,
                repaired_at=datetime.now(timezone.utc),
            )
            session.add(audit)

            logger.warning(
                f"Reference count drift detected on blob {blob_id}: recorded {blob.active_ref_count}, actual {actual_count}. Repairing."
            )
            blob.active_ref_count = actual_count
            blob.updated_at = datetime.now(timezone.utc)

            if actual_count == 0 and blob.status == "AVAILABLE":
                await DeletionOutboxService.enqueue_deletion(session, blob.id)

            await session.flush()
            return audit

        return None
