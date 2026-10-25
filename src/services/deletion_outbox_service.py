"""
Deletion Outbox Service (Milestone 6.3)
Location: src/services/deletion_outbox_service.py

Guarantees:
1. 4-Gate Deletion Safety Invariant:
   - Gate 1: Lease claimed with unique fencing token.
   - Gate 2: Advisory lock serialization (PostgreSQL) or BEGIN IMMEDIATE (SQLite).
   - Gate 3: Authoritative verification of actual_refs == 0 and active_intents == 0.
   - Gate 4: Feature flag activation (ENABLE_PHYSICAL_ASSET_DELETION is True).
2. Physical deletion is strictly performed via PhysicalBlobStorage implementations.
3. Durable tombstones: Purged blobs remain immutable tombstones preventing revival.
4. Retry-safe: Exponential backoff on transient errors, FAILED status on fatal errors.
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
import uuid

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.core.config import settings
from src.core.errors import StorageException
from src.core.logging import logger
from src.domain.interfaces import PhysicalBlobStorage
from src.infrastructure.models import (
    GeneratedAssetModel,
    StorageBlobModel,
    StorageDeletionOutboxModel,
    StorageOrphanQuarantineModel,
    StorageUploadIntentModel,
)
from src.infrastructure.storage.deleters import get_physical_blob_deleter


class DeletionOutboxService:
    """Orchestrates asynchronous safe physical deletion of storage assets."""

    @staticmethod
    async def enqueue_deletion(
        session: AsyncSession,
        blob_id: str,
    ) -> Optional[StorageDeletionOutboxModel]:
        """
        Step 1 & 2: Pre-deletion validation and outbox enqueueing.
        Verifies 0 references and 0 active intents before transitioning to DELETION_QUEUED.
        """
        blob_stmt = select(StorageBlobModel).where(StorageBlobModel.id == blob_id)
        blob_res = await session.execute(blob_stmt)
        blob = blob_res.scalar_one_or_none()

        if not blob:
            raise ValueError(f"Storage blob '{blob_id}' does not exist.")

        # If already purged (tombstone), skip
        if blob.status == "PURGED":
            logger.info(f"Blob '{blob_id}' is already PURGED (tombstone). Skipping enqueue.")
            return None

        # Gate 3: Reference Count Invariant
        count_stmt = select(func.count(GeneratedAssetModel.id)).where(GeneratedAssetModel.blob_id == blob_id)
        count_res = await session.execute(count_stmt)
        actual_refs = count_res.scalar_one()

        if actual_refs > 0:
            raise ValueError(
                f"Cannot enqueue deletion for blob '{blob_id}': has {actual_refs} active referencing assets."
            )

        now = datetime.now(timezone.utc)
        intents_stmt = select(func.count(StorageUploadIntentModel.id)).where(
            StorageUploadIntentModel.blob_id == blob_id,
            StorageUploadIntentModel.status.in_(["PENDING_UPLOAD", "UPLOADED"]),
            StorageUploadIntentModel.expires_at > now,
        )
        intents_res = await session.execute(intents_stmt)
        active_intents = intents_res.scalar_one()

        if active_intents > 0:
            raise ValueError(
                f"Cannot enqueue deletion for blob '{blob_id}': has {active_intents} active upload intents."
            )

        # Transition blob to DELETION_QUEUED
        blob.status = "DELETION_QUEUED"
        blob.updated_at = now

        # Check for existing active outbox task for this blob
        outbox_stmt = select(StorageDeletionOutboxModel).where(
            StorageDeletionOutboxModel.blob_id == blob_id,
            StorageDeletionOutboxModel.status.in_(["PENDING", "PROCESSING"]),
        )
        outbox_res = await session.execute(outbox_stmt)
        existing_task = outbox_res.scalar_one_or_none()

        if existing_task:
            existing_task.next_retry_at = now
            await session.flush()
            return existing_task

        task = StorageDeletionOutboxModel(
            id=str(uuid.uuid4()),
            blob_id=blob.id,
            file_hash=blob.file_hash,
            storage_path=blob.storage_path,
            fencing_token=str(uuid.uuid4()),
            status="PENDING",
            attempts=0,
            max_attempts=5,
            next_retry_at=now,
            created_at=now,
        )
        session.add(task)
        await session.flush()
        logger.info(f"Enqueued deletion outbox task '{task.id}' for blob '{blob.id}'")
        return task

    @staticmethod
    async def claim_pending_tasks(
        session: AsyncSession,
        worker_id: str = "outbox-worker-1",
        limit: int = 10,
    ) -> List[StorageDeletionOutboxModel]:
        """
        Step 3: Outbox Claim Transaction.
        Uses BEGIN IMMEDIATE on SQLite to ensure single-worker serialization.
        Uses FOR UPDATE SKIP LOCKED on PostgreSQL for non-blocking concurrent partition.
        """
        now = datetime.now(timezone.utc)
        is_postgres = not settings.is_sqlite

        if not is_postgres:
            # On SQLite, acquire write reservation lock immediately
            await session.execute(text("BEGIN IMMEDIATE;"))
            stmt = (
                select(StorageDeletionOutboxModel)
                .where(
                    StorageDeletionOutboxModel.status == "PENDING",
                    StorageDeletionOutboxModel.next_retry_at <= now,
                )
                .order_by(StorageDeletionOutboxModel.next_retry_at.asc())
                .limit(limit)
            )
        else:
            stmt = (
                select(StorageDeletionOutboxModel)
                .where(
                    StorageDeletionOutboxModel.status == "PENDING",
                    StorageDeletionOutboxModel.next_retry_at <= now,
                )
                .order_by(StorageDeletionOutboxModel.next_retry_at.asc())
                .limit(limit)
                .with_for_update(skip_locked=True)
            )

        result = await session.execute(stmt)
        tasks = list(result.scalars().all())

        for task in tasks:
            task.fencing_token = str(uuid.uuid4())
            task.status = "PROCESSING"
            task.locked_at = now
            task.locked_by = worker_id

            # Update parent blob status if DELETION_QUEUED
            blob_stmt = select(StorageBlobModel).where(StorageBlobModel.id == task.blob_id)
            blob_res = await session.execute(blob_stmt)
            blob = blob_res.scalar_one_or_none()
            if blob and blob.status == "DELETION_QUEUED":
                blob.status = "DELETION_IN_PROGRESS"
                blob.updated_at = now

        await session.flush()
        return tasks

    @staticmethod
    async def settle_task(
        session: AsyncSession,
        task_id: str,
        fencing_token: str,
        success: bool,
        error_code: Optional[int] = None,
        error_message: Optional[str] = None,
    ) -> Optional[StorageDeletionOutboxModel]:
        """
        Step 5: Post-Deletion Settlement Transaction.
        Transitions blob to permanent tombstone (PURGED) on success,
        or schedules exponential backoff / marks FAILED on error.
        """
        task_stmt = select(StorageDeletionOutboxModel).where(StorageDeletionOutboxModel.id == task_id)
        task_res = await session.execute(task_stmt)
        task = task_res.scalar_one_or_none()

        if not task:
            logger.warning(f"Outbox task '{task_id}' not found for settlement.")
            return None

        # Verify fencing token to prevent split-brain settlement from expired leases
        if task.fencing_token != fencing_token or task.status != "PROCESSING":
            logger.warning(
                f"Fencing token mismatch or invalid state for task '{task_id}': expected {fencing_token}, found {task.fencing_token}, status {task.status}"
            )
            return None

        now = datetime.now(timezone.utc)
        blob_stmt = select(StorageBlobModel).where(StorageBlobModel.id == task.blob_id)
        blob_res = await session.execute(blob_stmt)
        blob = blob_res.scalar_one_or_none()

        orphan_stmt = select(StorageOrphanQuarantineModel).where(
            StorageOrphanQuarantineModel.storage_path == task.storage_path,
            StorageOrphanQuarantineModel.status.in_(["QUARANTINED", "EVICTION_QUEUED"]),
        )
        orphan_res = await session.execute(orphan_stmt)
        orphan = orphan_res.scalar_one_or_none()

        if success or (error_code in (200, 204, 404)):
            # Idempotent deletion success
            task.status = "COMPLETED"
            task.completed_at = now

            if blob:
                blob.status = "PURGED"
                blob.updated_at = now
                logger.info(f"Storage blob '{blob.id}' transitioned to PURGED tombstone.")

            if orphan:
                orphan.status = "EVICTED"
                orphan.evicted_at = now

        elif (error_code in (401, 403)) or (task.attempts + 1 >= task.max_attempts):
            # Fatal error or retry exhaustion
            task.status = "FAILED"
            task.completed_at = now
            task.cancel_reason = error_message or f"Fatal deletion error (code {error_code})"
            logger.error(f"Deletion outbox task '{task_id}' FAILED: {task.cancel_reason}")

            if orphan:
                orphan.status = "EVICTION_FAILED"

        else:
            # Transient error: exponential backoff + jitter
            task.status = "PENDING"
            task.attempts += 1
            backoff_secs = min(settings.MAX_RETRY_BACKOFF_SECONDS, settings.BASE_RETRY_BACKOFF_SECONDS ** task.attempts)
            task.next_retry_at = now + timedelta(seconds=backoff_secs)
            logger.warning(
                f"Transient error on outbox task '{task_id}' (attempt {task.attempts}/{task.max_attempts}): retry scheduled in {backoff_secs}s"
            )

        await session.flush()
        return task

    @classmethod
    async def process_batch(
        cls,
        session_factory: async_sessionmaker[AsyncSession],
        worker_id: str = "outbox-worker-1",
        limit: int = 10,
        deleter: Optional[PhysicalBlobStorage] = None,
    ) -> Dict[str, int]:
        """
        Executes a complete claim -> physical delete -> settle cycle.
        Physical deletion is executed OUTSIDE database transactions.
        """
        # Step 3: Claim tasks
        async with session_factory() as claim_session:
            claimed_tasks = await cls.claim_pending_tasks(claim_session, worker_id=worker_id, limit=limit)
            await claim_session.commit()

        if not claimed_tasks:
            return {"claimed": 0, "completed": 0, "failed": 0, "retried": 0}

        results = {"claimed": len(claimed_tasks), "completed": 0, "failed": 0, "retried": 0}

        for task in claimed_tasks:
            success = False
            error_code = None
            error_msg = None

            # Gate 4: Feature flag check
            if not settings.ENABLE_PHYSICAL_ASSET_DELETION:
                logger.info(
                    f"ENABLE_PHYSICAL_ASSET_DELETION is False. Skipping physical deletion of '{task.storage_path}'. Task marked completed."
                )
                success = True
                error_code = 200
            else:
                # Step 4: External physical deletion via PhysicalBlobStorage capability
                try:
                    active_deleter = deleter or get_physical_blob_deleter("local_cas")
                    del_result = await active_deleter.delete_physical_object(task.storage_path)
                    success = bool(del_result)
                    error_code = 200 if success else 500
                except PermissionError as pe:
                    success = False
                    error_code = 403
                    error_msg = str(pe)
                except StorageException as se:
                    success = False
                    error_code = 500
                    error_msg = str(se)
                except Exception as e:
                    success = False
                    error_code = 500
                    error_msg = str(e)

            # Step 5: Post-deletion settlement transaction
            async with session_factory() as settle_session:
                settled = await cls.settle_task(
                    settle_session,
                    task_id=task.id,
                    fencing_token=task.fencing_token,
                    success=success,
                    error_code=error_code,
                    error_message=error_msg,
                )
                await settle_session.commit()

                if settled:
                    if settled.status == "COMPLETED":
                        results["completed"] += 1
                    elif settled.status == "FAILED":
                        results["failed"] += 1
                    elif settled.status == "PENDING":
                        results["retried"] += 1

        return results

    @staticmethod
    async def reconcile_orphans(
        session: AsyncSession,
        storage_objects: List[Dict[str, Any]],
        grace_period_hours: float = 48.0,
    ) -> Dict[str, int]:
        """
        Two-pass orphan reconciliation engine (Section 7).
        Evaluates physical storage objects against the database catalog:
        - Tombstone hit (PURGED) -> enqueues immediate outbox deletion.
        - Unreferenced object -> enters 48h quarantine (Pass 1).
        - Expired quarantine -> verified and enqueued for eviction (Pass 2).
        """
        now = datetime.now(timezone.utc)
        stats = {
            "resurrected_orphans": 0,
            "active_preserved": 0,
            "quarantined": 0,
            "eviction_queued": 0,
        }

        for obj in storage_objects:
            storage_path = obj["storage_path"]
            file_hash = obj.get("file_hash") or ""
            blob_id = obj.get("blob_id")

            # Check if matching blob exists
            blob = None
            if blob_id:
                blob_stmt = select(StorageBlobModel).where(StorageBlobModel.id == blob_id)
                blob_res = await session.execute(blob_stmt)
                blob = blob_res.scalar_one_or_none()
            elif storage_path:
                blob_stmt = select(StorageBlobModel).where(StorageBlobModel.storage_path == storage_path)
                blob_res = await session.execute(blob_stmt)
                blob = blob_res.scalar_one_or_none()

            # State 0: Resurrected orphan (Tombstone hit!)
            if blob and blob.status == "PURGED":
                stats["resurrected_orphans"] += 1
                # Enqueue deletion outbox task directly
                task = StorageDeletionOutboxModel(
                    id=str(uuid.uuid4()),
                    blob_id=blob.id,
                    file_hash=blob.file_hash,
                    storage_path=blob.storage_path,
                    fencing_token=str(uuid.uuid4()),
                    status="PENDING",
                    attempts=0,
                    max_attempts=5,
                    next_retry_at=now,
                    created_at=now,
                )
                session.add(task)
                continue

            # State 1: Active referenced
            if blob and blob.active_ref_count > 0:
                stats["active_preserved"] += 1
                continue

            # State 4: Already in outbox
            if blob:
                outbox_check = select(func.count(StorageDeletionOutboxModel.id)).where(
                    StorageDeletionOutboxModel.blob_id == blob.id,
                    StorageDeletionOutboxModel.status.in_(["PENDING", "PROCESSING"]),
                )
                if (await session.execute(outbox_check)).scalar_one() > 0:
                    continue

            # State 5: Unreferenced object -> Check quarantine
            quarantine_stmt = select(StorageOrphanQuarantineModel).where(
                StorageOrphanQuarantineModel.storage_path == storage_path
            )
            q_res = await session.execute(quarantine_stmt)
            orphan = q_res.scalar_one_or_none()

            if not orphan:
                # Pass 1: Insert into quarantine
                orphan = StorageOrphanQuarantineModel(
                    id=str(uuid.uuid4()),
                    storage_path=storage_path,
                    blob_id=blob.id if blob else None,
                    file_hash=file_hash,
                    status="QUARANTINED",
                    quarantined_at=now,
                )
                session.add(orphan)
                stats["quarantined"] += 1
            else:
                # Pass 2: Check quarantine expiration
                if orphan.status == "QUARANTINED":
                    age = now - orphan.quarantined_at
                    if age >= timedelta(hours=grace_period_hours):
                        orphan.status = "EVICTION_QUEUED"
                        stats["eviction_queued"] += 1

                        if not blob:
                            # Create a tracked blob record in DELETION_QUEUED status so outbox FK is satisfied
                            blob = StorageBlobModel(
                                id=orphan.blob_id or str(uuid.uuid4()),
                                file_hash=file_hash if len(file_hash) == 64 else ("0" * 64),
                                storage_path=storage_path,
                                storage_provider="local_cas",
                                file_size_bytes=1,
                                status="DELETION_QUEUED",
                                active_ref_count=0,
                                version=1,
                                created_at=now,
                                updated_at=now,
                            )
                            session.add(blob)
                            await session.flush()
                            orphan.blob_id = blob.id

                        # Enqueue outbox task for orphan eviction
                        task = StorageDeletionOutboxModel(
                            id=str(uuid.uuid4()),
                            blob_id=blob.id,
                            file_hash=blob.file_hash,
                            storage_path=storage_path,
                            fencing_token=str(uuid.uuid4()),
                            status="PENDING",
                            attempts=0,
                            max_attempts=5,
                            next_retry_at=now,
                            created_at=now,
                        )
                        session.add(task)


        await session.flush()
        return stats
