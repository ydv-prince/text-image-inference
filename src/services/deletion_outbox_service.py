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

import asyncio
from datetime import datetime, timedelta, timezone
import time
from typing import Any, Dict, List, Optional
import uuid

from sqlalchemy import func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.core.config import settings
from src.core.errors import StorageException
from src.core.logging import logger
from src.core.metrics import (
    deletion_failures_total,
    deletion_leases_reclaimed_total,
    deletion_processing_duration_seconds,
    deletion_tasks_total,
)
from src.domain.interfaces import PhysicalBlobStorage
from src.infrastructure.models import (
    GeneratedAssetModel,
    StorageBlobModel,
    StorageDeletionOutboxModel,
    StorageOrphanQuarantineModel,
    StorageUploadIntentModel,
)
from src.infrastructure.storage.deleters import get_physical_blob_deleter


def sanitize_error_message(msg: Optional[str]) -> Optional[str]:
    """Sanitizes potential secrets and credential tokens from error messages."""
    if not msg:
        return msg
    sanitized = str(msg)
    if settings.SUPABASE_SERVICE_ROLE_KEY:
        secret = settings.SUPABASE_SERVICE_ROLE_KEY.get_secret_value()
        if secret and secret in sanitized:
            sanitized = sanitized.replace(secret, "[REDACTED]")
    if settings.API_KEY_SALT:
        salt = settings.API_KEY_SALT.get_secret_value()
        if salt and salt in sanitized:
            sanitized = sanitized.replace(salt, "[REDACTED]")
    if settings.WORKER_SHARED_SECRET:
        w_secret = settings.WORKER_SHARED_SECRET.get_secret_value()
        if w_secret and w_secret in sanitized:
            sanitized = sanitized.replace(w_secret, "[REDACTED]")
    return sanitized


def hash_to_lock_key(file_hash: str) -> int:
    """Computes signed 64-bit integer lock key matching PostgreSQL fn_hash_to_lock_key."""
    b = bytes.fromhex(file_hash[:16])
    return int.from_bytes(b, byteorder="big", signed=True)


async def acquire_hash_advisory_lock(session: AsyncSession, file_hash: str) -> None:
    """
    Acquires a transaction-scoped PostgreSQL advisory lock derived from the SHA-256 file_hash
    using fn_hash_to_lock_key.
    The lock is automatically released upon transaction COMMIT or ROLLBACK.
    No-op on SQLite.
    """
    if not settings.is_sqlite and file_hash:
        await session.execute(
            text("SELECT pg_advisory_xact_lock(fn_hash_to_lock_key(:file_hash));"),
            {"file_hash": file_hash},
        )


class DeletionOutboxService:
    """Orchestrates asynchronous safe physical deletion of storage assets."""

    acquire_hash_advisory_lock = staticmethod(acquire_hash_advisory_lock)
    hash_to_lock_key = staticmethod(hash_to_lock_key)

    @staticmethod
    async def enqueue_deletion(
        session: AsyncSession,
        blob_id: str,
    ) -> Optional[StorageDeletionOutboxModel]:
        """
        Step 1 & 2: Pre-deletion validation and outbox enqueueing.
        Verifies 0 references and 0 active intents before transitioning to DELETION_QUEUED.
        """
        # Gate 2: Acquire transaction advisory lock for file_hash on PostgreSQL
        # Snapshot read first (no row lock) to obtain file_hash, then acquire advisory lock
        # before any row-level lock is acquired. This guarantees:
        # Advisory Lock (file_hash) -> Level 3 (storage_blobs FOR UPDATE) -> Level 5 (storage_deletion_outbox).
        if not settings.is_sqlite:
            hash_stmt = select(StorageBlobModel.file_hash).where(StorageBlobModel.id == blob_id)
            hash_res = await session.execute(hash_stmt)
            blob_hash = hash_res.scalar_one_or_none()
            if not blob_hash:
                raise ValueError(f"Storage blob '{blob_id}' does not exist.")
            await acquire_hash_advisory_lock(session, blob_hash)

        blob_stmt = select(StorageBlobModel).where(StorageBlobModel.id == blob_id)
        if not settings.is_sqlite:
            blob_stmt = blob_stmt.with_for_update()
        blob_res = await session.execute(blob_stmt)
        blob = blob_res.scalar_one_or_none()

        if not blob:
            raise ValueError(f"Storage blob '{blob_id}' does not exist.")

        # If already purged (tombstone), skip
        if blob.status == "PURGED":
            logger.info(f"Blob '{blob_id}' is already PURGED (tombstone). Skipping enqueue.")
            return None

        # Check for existing active outbox task for this blob.
        # If an active task is already PENDING or PROCESSING, return it idempotently without
        # modifying it or its parent blob. Crucially, avoiding an UPDATE on storage_deletion_outbox
        # prevents acquiring a Level 5 row lock while holding Level 3 (storage_blobs FOR UPDATE),
        # which completely eliminates the circular wait deadlock against claim_pending_tasks
        # (which holds Level 5 FOR UPDATE SKIP LOCKED and updates Level 3).
        outbox_stmt = select(StorageDeletionOutboxModel).where(
            StorageDeletionOutboxModel.blob_id == blob_id,
            StorageDeletionOutboxModel.status.in_(["PENDING", "PROCESSING"]),
        )
        outbox_res = await session.execute(outbox_stmt)
        existing_task = outbox_res.scalar_one_or_none()

        if existing_task:
            logger.info(
                f"Active deletion outbox task '{existing_task.id}' already exists for blob '{blob_id}' "
                f"(status='{existing_task.status}'). Returning existing task idempotently."
            )
            return existing_task

        # Gate 3: Reference Count Invariant
        count_stmt = select(func.count(GeneratedAssetModel.id)).where(GeneratedAssetModel.blob_id == blob_id)
        count_res = await session.execute(count_stmt)
        actual_refs = count_res.scalar_one()

        if blob.active_ref_count > 0 or actual_refs > 0:
            total_refs = max(blob.active_ref_count, actual_refs)
            raise ValueError(
                f"Cannot enqueue deletion for blob '{blob_id}': has {total_refs} active referencing assets."
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

    @classmethod
    async def recover_stalled_tasks(
        cls,
        session: AsyncSession,
        lease_timeout_seconds: Optional[int] = None,
        limit: int = 10,
        reschedule_delay: Optional[float] = None,
    ) -> List[StorageDeletionOutboxModel]:
        """
        Step 2.5: Stalled Outbox Lease Recovery (AUDIT-6.3-05).
        Identifies tasks stuck in PROCESSING whose worker lease (locked_at) has expired.
        Reclaims them safely:
        1. Invalidates stale worker by replacing fencing_token.
        2. Preserves retry accounting: increments attempts and computes exponential backoff,
           or transitions to FAILED if max_attempts is reached.
        3. Restores parent blob status from DELETION_IN_PROGRESS to DELETION_QUEUED.
        4. Serializes via FOR UPDATE SKIP LOCKED (PostgreSQL) or BEGIN IMMEDIATE (SQLite).
        """
        now = datetime.now(timezone.utc)
        timeout = (
            lease_timeout_seconds
            if lease_timeout_seconds is not None
            else settings.LEASE_TIMEOUT_SECONDS
        )
        threshold = now - timedelta(seconds=timeout)
        is_postgres = not settings.is_sqlite

        predicate = or_(
            StorageDeletionOutboxModel.locked_at < threshold,
            StorageDeletionOutboxModel.locked_at.is_(None),
        )

        if not is_postgres:
            await session.execute(text("BEGIN IMMEDIATE;"))
            stmt = (
                select(StorageDeletionOutboxModel)
                .where(
                    StorageDeletionOutboxModel.status == "PROCESSING",
                    predicate,
                )
                .order_by(StorageDeletionOutboxModel.locked_at.asc())
                .limit(limit)
            )
        else:
            stmt = (
                select(StorageDeletionOutboxModel)
                .where(
                    StorageDeletionOutboxModel.status == "PROCESSING",
                    predicate,
                )
                .order_by(StorageDeletionOutboxModel.locked_at.asc())
                .limit(limit)
                .with_for_update(skip_locked=True)
            )

        result = await session.execute(stmt)
        stalled_tasks = list(result.scalars().all())

        recovered: List[StorageDeletionOutboxModel] = []
        for task in stalled_tasks:
            # 1. Invalidate stale worker by replacing fencing token
            task.fencing_token = str(uuid.uuid4())
            task.locked_at = None
            task.locked_by = None

            # 2. Retry accounting & backoff
            task.attempts += 1
            if task.attempts >= task.max_attempts:
                task.status = "FAILED"
                task.completed_at = now
                task.cancel_reason = (
                    f"Worker lease expired after {timeout}s (max attempts {task.max_attempts} reached)"
                )
                logger.error(
                    f"Deletion outbox task '{task.id}' marked FAILED: {task.cancel_reason}"
                )

                # Update quarantine status if applicable
                orphan_stmt = select(StorageOrphanQuarantineModel).where(
                    StorageOrphanQuarantineModel.storage_path == task.storage_path,
                    StorageOrphanQuarantineModel.status.in_(["QUARANTINED", "EVICTION_QUEUED"]),
                )
                orphan_res = await session.execute(orphan_stmt)
                orphan = orphan_res.scalar_one_or_none()
                if orphan:
                    orphan.status = "EVICTION_FAILED"
            else:
                task.status = "PENDING"
                if reschedule_delay is not None:
                    backoff_secs = reschedule_delay
                else:
                    backoff_secs = min(
                        settings.MAX_RETRY_BACKOFF_SECONDS,
                        settings.BASE_RETRY_BACKOFF_SECONDS ** task.attempts,
                    )
                task.next_retry_at = now + timedelta(seconds=backoff_secs)
                logger.warning(
                    f"Recovered stalled deletion outbox task '{task.id}' (attempt {task.attempts}/{task.max_attempts}): "
                    f"retry scheduled in {backoff_secs}s"
                )

            # 3. Restore parent blob status if in DELETION_IN_PROGRESS
            blob_stmt = select(StorageBlobModel).where(StorageBlobModel.id == task.blob_id)
            blob_res = await session.execute(blob_stmt)
            blob = blob_res.scalar_one_or_none()
            if blob and blob.status == "DELETION_IN_PROGRESS":
                blob.status = "DELETION_QUEUED"
                blob.updated_at = now

            recovered.append(task)

        if recovered:
            await session.flush()
            try:
                deletion_leases_reclaimed_total.inc(len(recovered))
            except Exception:
                pass

        return recovered

    @classmethod
    async def claim_pending_tasks(
        cls,
        session: AsyncSession,
        worker_id: str = "outbox-worker-1",
        limit: int = 10,
        recover_stalled: bool = False,
        lease_timeout_seconds: Optional[int] = None,
    ) -> List[StorageDeletionOutboxModel]:
        """
        Step 3: Outbox Claim Transaction.
        Optionally recovers stalled tasks before claiming if recover_stalled is True.
        Uses BEGIN IMMEDIATE on SQLite to ensure single-worker serialization.
        Uses FOR UPDATE SKIP LOCKED on PostgreSQL for non-blocking concurrent partition.
        """
        if recover_stalled:
            await cls.recover_stalled_tasks(
                session=session,
                lease_timeout_seconds=lease_timeout_seconds,
                limit=limit,
            )

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

        claimed: List[StorageDeletionOutboxModel] = []
        for task in tasks:
            # Invariant 4 Guard: Verify parent blob reference count before processing
            blob_stmt = select(StorageBlobModel).where(StorageBlobModel.id == task.blob_id)
            blob_res = await session.execute(blob_stmt)
            blob = blob_res.scalar_one_or_none()

            # If blob has active references, cancel deletion task and restore blob to AVAILABLE
            if blob and blob.active_ref_count > 0:
                task.status = "CANCELLED"
                task.cancel_reason = (
                    f"Deletion cancelled: blob '{blob.id}' has {blob.active_ref_count} active references."
                )
                logger.warning(task.cancel_reason)
                if blob.status in ("DELETION_QUEUED", "DELETION_IN_PROGRESS"):
                    blob.status = "AVAILABLE"
                    blob.updated_at = now
                continue

            task.fencing_token = str(uuid.uuid4())
            task.status = "PROCESSING"
            task.locked_at = now
            task.locked_by = worker_id

            if blob and blob.status == "DELETION_QUEUED":
                blob.status = "DELETION_IN_PROGRESS"
                blob.updated_at = now

            claimed.append(task)

        await session.flush()
        return claimed

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
        # Gate 2: Acquire transaction advisory lock for file_hash on PostgreSQL
        # Snapshot read first (no row lock) to obtain file_hash and validate lease,
        # then acquire advisory lock before taking any row-level locks. This guarantees:
        # Advisory Lock (file_hash) -> Level 5 (outbox FOR UPDATE) -> Level 3 (blobs update).
        if not settings.is_sqlite:
            lookup_stmt = select(
                StorageDeletionOutboxModel.file_hash,
                StorageDeletionOutboxModel.fencing_token,
                StorageDeletionOutboxModel.status,
            ).where(StorageDeletionOutboxModel.id == task_id)
            lookup_res = await session.execute(lookup_stmt)
            task_info = lookup_res.one_or_none()

            if not task_info:
                logger.warning(f"Outbox task '{task_id}' not found for settlement.")
                return None

            if task_info.fencing_token != fencing_token or task_info.status != "PROCESSING":
                logger.warning(
                    f"Fencing token mismatch or invalid state for task '{task_id}': expected {fencing_token}, found {task_info.fencing_token}, status {task_info.status}"
                )
                return None

            await acquire_hash_advisory_lock(session, task_info.file_hash)

        task_stmt = select(StorageDeletionOutboxModel).where(StorageDeletionOutboxModel.id == task_id)
        if not settings.is_sqlite:
            task_stmt = task_stmt.with_for_update()
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
            task.attempts += 1
            task.status = "FAILED"
            task.completed_at = now
            raw_err = error_message or f"Fatal deletion error (code {error_code})"
            task.cancel_reason = sanitize_error_message(raw_err)
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
        lease_timeout_seconds: Optional[int] = None,
    ) -> Dict[str, int]:
        """
        Executes a complete claim -> physical delete -> settle cycle.
        Physical deletion is executed OUTSIDE database transactions.
        """
        # Gate 4: Feature flag check - physical deletion disabled by default
        if not settings.ENABLE_PHYSICAL_ASSET_DELETION:
            logger.info(
                "ENABLE_PHYSICAL_ASSET_DELETION is False. Physical deletion is disabled; skipping outbox batch."
            )
            return {"claimed": 0, "completed": 0, "failed": 0, "retried": 0}

        # Step 2.5: Recover stalled leases before claiming
        async with session_factory() as recover_session:
            await cls.recover_stalled_tasks(
                recover_session,
                lease_timeout_seconds=lease_timeout_seconds,
                limit=limit,
            )
            await recover_session.commit()

        # Step 3: Claim tasks
        async with session_factory() as claim_session:
            claimed_tasks = await cls.claim_pending_tasks(claim_session, worker_id=worker_id, limit=limit)
            await claim_session.commit()

        if not claimed_tasks:
            return {"claimed": 0, "completed": 0, "failed": 0, "retried": 0}

        results = {"claimed": len(claimed_tasks), "completed": 0, "failed": 0, "retried": 0}

        try:
            for task in claimed_tasks:
                # Re-check Gate 4 inside loop in case flag was modified dynamically
                if not settings.ENABLE_PHYSICAL_ASSET_DELETION:
                    logger.warning(
                        f"ENABLE_PHYSICAL_ASSET_DELETION became False. Releasing claimed task '{task.id}' back to PENDING without penalty."
                    )
                    async with session_factory() as release_session:
                        rel_stmt = select(StorageDeletionOutboxModel).where(StorageDeletionOutboxModel.id == task.id)
                        res = await release_session.execute(rel_stmt)
                        t = res.scalar_one_or_none()
                        if t and t.status == "PROCESSING" and t.fencing_token == task.fencing_token:
                            t.status = "PENDING"
                            t.locked_at = None
                            t.locked_by = None
                        blob_stmt = select(StorageBlobModel).where(StorageBlobModel.id == task.blob_id)
                        b_res = await release_session.execute(blob_stmt)
                        b = b_res.scalar_one_or_none()
                        if b and b.status == "DELETION_IN_PROGRESS":
                            b.status = "DELETION_QUEUED"
                        await release_session.commit()
                    # Objective B: Releasing a task to PENDING without consuming retry attempts
                    # does NOT count as a retry. Keep results["retried"] for actual transient retries.
                    continue

                success = False
                error_code = None
                error_msg = None

                # Step 4: External physical deletion via PhysicalBlobStorage capability
                del_start = time.perf_counter()
                try:
                    active_deleter = deleter or get_physical_blob_deleter(settings.STORAGE_ADAPTER)
                    del_result = await active_deleter.delete_physical_object(task.storage_path)
                    success = bool(del_result)
                    error_code = 200 if success else 500
                except FileNotFoundError:
                    # Idempotent deletion: physical file is already gone
                    success = True
                    error_code = 200
                    error_msg = None
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
                finally:
                    del_duration = time.perf_counter() - del_start
                    try:
                        deletion_processing_duration_seconds.observe(del_duration)
                    except Exception:
                        pass

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
                            try:
                                deletion_tasks_total.inc(labels={"outcome": "completed"})
                            except Exception:
                                pass
                        elif settled.status == "FAILED":
                            results["failed"] += 1
                            cat = "permission_denied" if error_code in (401, 403) else (
                                "max_attempts_exceeded" if task.attempts + 1 >= task.max_attempts else "internal_error"
                            )
                            try:
                                deletion_tasks_total.inc(labels={"outcome": "failed"})
                                deletion_failures_total.inc(labels={"error_category": cat})
                            except Exception:
                                pass
                        elif settled.status == "PENDING":
                            results["retried"] += 1
                            try:
                                deletion_tasks_total.inc(labels={"outcome": "retried"})
                            except Exception:
                                pass

        except asyncio.CancelledError:
            logger.warning("Deletion batch processing cancelled. Releasing unsettled claimed tasks back to PENDING.")
            try:
                async with session_factory() as release_session:
                    for t_item in claimed_tasks:
                        rel_stmt = select(StorageDeletionOutboxModel).where(StorageDeletionOutboxModel.id == t_item.id)
                        res = await release_session.execute(rel_stmt)
                        t = res.scalar_one_or_none()
                        if t and t.status == "PROCESSING" and t.fencing_token == t_item.fencing_token:
                            t.status = "PENDING"
                            t.locked_at = None
                            t.locked_by = None
                        blob_stmt = select(StorageBlobModel).where(StorageBlobModel.id == t_item.blob_id)
                        b_res = await release_session.execute(blob_stmt)
                        b = b_res.scalar_one_or_none()
                        if b and b.status == "DELETION_IN_PROGRESS":
                            b.status = "DELETION_QUEUED"
                    await release_session.commit()
            except Exception as cleanup_err:
                logger.error(f"Error during cancellation cleanup: {cleanup_err}")
            raise

        return results

    @classmethod
    async def reconcile_orphans(
        cls,
        session: AsyncSession,
        storage_objects: List[Dict[str, Any]],
        grace_period_hours: float = 48.0,
    ) -> Dict[str, int]:
        """
        Two-pass orphan reconciliation engine (Section 7).
        Evaluates physical storage objects against the database catalog:
        - Resurrected orphan (PURGED tombstone hit) -> enqueues immediate outbox deletion.
        - Active referenced / in-flight upload -> preserved; if in quarantine, reclaimed.
        - Unreferenced object -> enters quarantine grace period (Pass 1).
        - Expired quarantine -> verified (0 refs, 0 intents) and enqueued for eviction via
          enqueue_deletion, safely transitioning existing blob to DELETION_QUEUED (Pass 2).
        """
        now = datetime.now(timezone.utc)
        stats = {
            "resurrected_orphans": 0,
            "active_preserved": 0,
            "quarantined": 0,
            "eviction_queued": 0,
        }

        # Deterministic canonical ordering prevents inter-batch lock inversions
        sorted_objects = sorted(
            storage_objects,
            key=lambda x: (x.get("storage_path") or "", x.get("file_hash") or ""),
        )

        for obj in sorted_objects:
            storage_path = obj.get("storage_path")
            if not storage_path:
                continue

            file_hash = obj.get("file_hash") or ""
            blob_id = obj.get("blob_id")

            # Check if quarantine record already exists
            quarantine_stmt = select(StorageOrphanQuarantineModel).where(
                StorageOrphanQuarantineModel.storage_path == storage_path
            )
            q_res = await session.execute(quarantine_stmt)
            orphan = q_res.scalar_one_or_none()

            # Check if matching blob exists in catalog
            blob = None
            lookup_blob_id = blob_id or (orphan.blob_id if orphan else None)
            if lookup_blob_id:
                blob_stmt = select(StorageBlobModel).where(StorageBlobModel.id == lookup_blob_id)
                blob_res = await session.execute(blob_stmt)
                blob = blob_res.scalar_one_or_none()
            if not blob:
                blob_stmt = select(StorageBlobModel).where(StorageBlobModel.storage_path == storage_path)
                blob_res = await session.execute(blob_stmt)
                blob = blob_res.scalar_one_or_none()

            # Metadata Validation: If blob exists and obj provides a mismatched file_hash,
            # authoritative identity cannot be established. Defer/quarantine safely without deleting.
            if blob and file_hash and blob.file_hash and blob.file_hash != file_hash:
                logger.warning(
                    f"Orphan reconciliation metadata mismatch for '{storage_path}': "
                    f"storage object hash '{file_hash}' != catalog blob hash '{blob.file_hash}'. "
                    f"Quarantining/deferring object to protect against ambiguous deletion."
                )
                if not orphan:
                    orphan = StorageOrphanQuarantineModel(
                        id=str(uuid.uuid4()),
                        storage_path=storage_path,
                        blob_id=blob.id,
                        file_hash=file_hash,
                        status="QUARANTINED",
                        quarantined_at=now,
                    )
                    session.add(orphan)
                    stats["quarantined"] += 1
                continue

            # State 0: Resurrected orphan (Tombstone hit!)
            if blob and blob.status == "PURGED":
                stats["resurrected_orphans"] += 1
                if orphan and orphan.status == "QUARANTINED":
                    orphan.status = "EVICTION_QUEUED"

                # Check if active outbox task already exists before inserting
                outbox_check = select(func.count(StorageDeletionOutboxModel.id)).where(
                    StorageDeletionOutboxModel.blob_id == blob.id,
                    StorageDeletionOutboxModel.status.in_(["PENDING", "PROCESSING"]),
                )
                if (await session.execute(outbox_check)).scalar_one() == 0:
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

            # Active Invariant Checks: Reference count and In-Flight Upload Intents
            has_active_refs = False
            has_active_intents = False

            if blob:
                if blob.active_ref_count > 0:
                    has_active_refs = True
                else:
                    ref_count_stmt = select(func.count(GeneratedAssetModel.id)).where(
                        GeneratedAssetModel.blob_id == blob.id
                    )
                    if (await session.execute(ref_count_stmt)).scalar_one() > 0:
                        has_active_refs = True

                intents_stmt = select(func.count(StorageUploadIntentModel.id)).where(
                    StorageUploadIntentModel.blob_id == blob.id,
                    StorageUploadIntentModel.status.in_(["PENDING_UPLOAD", "UPLOADED"]),
                    StorageUploadIntentModel.expires_at > now,
                )
                if (await session.execute(intents_stmt)).scalar_one() > 0:
                    has_active_intents = True
            else:
                # Check active upload intent by storage_path even if blob record is absent
                path_intent_stmt = select(func.count(StorageUploadIntentModel.id)).where(
                    StorageUploadIntentModel.storage_path == storage_path,
                    StorageUploadIntentModel.status.in_(["PENDING_UPLOAD", "UPLOADED"]),
                    StorageUploadIntentModel.expires_at > now,
                )
                if (await session.execute(path_intent_stmt)).scalar_one() > 0:
                    has_active_intents = True

            # State 1 & 2: Active referenced or in-flight upload -> preserve and reclaim
            if has_active_refs or has_active_intents:
                stats["active_preserved"] += 1
                if orphan and orphan.status == "QUARANTINED":
                    orphan.status = "RECLAIMED"
                    orphan.reclaimed_at = now
                continue

            # State 4: Already awaiting deletion in outbox
            if blob:
                outbox_check = select(func.count(StorageDeletionOutboxModel.id)).where(
                    StorageDeletionOutboxModel.blob_id == blob.id,
                    StorageDeletionOutboxModel.status.in_(["PENDING", "PROCESSING"]),
                )
                if (await session.execute(outbox_check)).scalar_one() > 0:
                    # Enforce that blob must not remain AVAILABLE while an active outbox task exists
                    if blob.status == "AVAILABLE":
                        blob.status = "DELETION_QUEUED"
                        blob.updated_at = now
                    if orphan and orphan.status == "QUARANTINED":
                        orphan.status = "EVICTION_QUEUED"
                    continue

            # State 5: Unreferenced object -> Quarantine Lifecycle (Pass 1 & Pass 2)
            if not orphan:
                # Pass 1: First discovery -> enter quarantine grace period
                orphan = StorageOrphanQuarantineModel(
                    id=str(uuid.uuid4()),
                    storage_path=storage_path,
                    blob_id=blob.id if blob else None,
                    file_hash=blob.file_hash if blob else file_hash,
                    status="QUARANTINED",
                    quarantined_at=now,
                )
                session.add(orphan)
                stats["quarantined"] += 1
            else:
                # Pass 2: Check quarantine expiration
                if orphan.status == "QUARANTINED":
                    q_at = orphan.quarantined_at if orphan.quarantined_at.tzinfo else orphan.quarantined_at.replace(tzinfo=timezone.utc)
                    age = now - q_at
                    if age >= timedelta(hours=grace_period_hours):
                        # Quarantine expired -> prepare eviction
                        if not blob:
                            # Create tracked blob record in DELETION_QUEUED status so outbox FK is satisfied
                            clean_hash = file_hash if len(file_hash) == 64 else ("0" * 64)
                            blob = StorageBlobModel(
                                id=orphan.blob_id or str(uuid.uuid4()),
                                file_hash=clean_hash,
                                storage_path=storage_path,
                                storage_provider="local_cas",
                                file_size_bytes=obj.get("file_size_bytes") or 1,
                                mime_type=obj.get("mime_type") or "image/png",
                                status="DELETION_QUEUED",
                                active_ref_count=0,
                                version=1,
                                created_at=now,
                                updated_at=now,
                            )
                            session.add(blob)
                            await session.flush()
                            orphan.blob_id = blob.id

                        # Safe Eviction Enqueueing:
                        # Transitions existing blob to DELETION_QUEUED and enqueues outbox task.
                        # If references or intents appeared concurrently, enqueue_deletion raises ValueError.
                        try:
                            task = await cls.enqueue_deletion(session, blob.id)
                            if task:
                                orphan.status = "EVICTION_QUEUED"
                                stats["eviction_queued"] += 1
                        except ValueError as ve:
                            # Concurrently re-referenced during eviction enqueueing!
                            logger.info(
                                f"Orphan '{storage_path}' re-referenced during eviction enqueueing ({ve}). Reclaiming."
                            )
                            orphan.status = "RECLAIMED"
                            orphan.reclaimed_at = now
                            stats["active_preserved"] += 1

        await session.flush()
        return stats


class DeletionOutboxWorker:
    """
    Background worker daemon that periodically processes deletion outbox batches.
    Provides graceful shutdown, cancellation safety, and error containment.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        worker_id: str = "outbox-worker-daemon",
        interval_seconds: float = 10.0,
        batch_limit: int = 10,
        deleter: Optional[PhysicalBlobStorage] = None,
        lease_timeout_seconds: Optional[int] = None,
    ):
        self.session_factory = session_factory
        self.worker_id = worker_id
        self.interval_seconds = interval_seconds
        self.batch_limit = batch_limit
        self.deleter = deleter
        self.lease_timeout_seconds = lease_timeout_seconds
        self._stop_event = asyncio.Event()
        self.is_running = False
        self.metrics: Dict[str, int] = {
            "batches_processed": 0,
            "tasks_claimed": 0,
            "tasks_completed": 0,
            "tasks_failed": 0,
            "tasks_retried": 0,
            "errors": 0,
        }

    async def run_once(self) -> Dict[str, int]:
        """Runs a single outbox batch cycle with error isolation."""
        try:
            results = await DeletionOutboxService.process_batch(
                session_factory=self.session_factory,
                worker_id=self.worker_id,
                limit=self.batch_limit,
                deleter=self.deleter,
                lease_timeout_seconds=self.lease_timeout_seconds,
            )
            self.metrics["batches_processed"] += 1
            self.metrics["tasks_claimed"] += results.get("claimed", 0)
            self.metrics["tasks_completed"] += results.get("completed", 0)
            self.metrics["tasks_failed"] += results.get("failed", 0)
            self.metrics["tasks_retried"] += results.get("retried", 0)
            return results
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.metrics["errors"] += 1
            logger.error(f"DeletionOutboxWorker [{self.worker_id}] cycle error: {e}", exc_info=True)
            return {"claimed": 0, "completed": 0, "failed": 0, "retried": 0}

    async def run(self) -> None:
        """Continuous execution loop until stop() is called."""
        self.is_running = True
        logger.info(
            f"DeletionOutboxWorker [{self.worker_id}] started (interval={self.interval_seconds}s, limit={self.batch_limit})"
        )
        try:
            while not self._stop_event.is_set():
                await self.run_once()
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=self.interval_seconds)
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            logger.info(f"DeletionOutboxWorker [{self.worker_id}] received cancellation.")
        finally:
            self.is_running = False
            logger.info(f"DeletionOutboxWorker [{self.worker_id}] stopped.")

    def stop(self) -> None:
        """Signals the background loop to stop gracefully."""
        self._stop_event.set()
