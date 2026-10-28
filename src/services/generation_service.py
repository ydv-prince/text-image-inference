"""
Generation Orchestration & Idempotency Service
Location: src/services/generation_service.py

Guarantees:
1. Client-defined Idempotency-Key Header:
   - Evaluated atomically per client (scoped to api_key_id).
   - Same key + matching payload hash -> returns existing job (idempotent).
   - Same key + conflicting payload hash -> raises HTTP 409 Conflict.
   - Concurrent race condition -> handled atomically via database unique constraint (uq_jobs_idempotency).
2. Cross-Key Access Isolation:
   - Validates that the requesting client owns the requested job and its assets.
   - Unauthorized cross-key access raises HTTP 403 Forbidden.
"""

import asyncio
from datetime import datetime, timezone
import hashlib
import json
from typing import Optional, Tuple
import uuid
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from src.api.v1.schemas import GenerationCreateRequest
from src.core.logging import logger
from src.core.errors import (
    EntityNotFoundException,
    ForbiddenException,
    IdempotencyConflictException,
    ValidationException,
)
from src.domain.interfaces import AssetStorage
from src.domain.models import JobStatus
from src.infrastructure.database import set_tenant_context
from src.infrastructure.models import ApiKeyModel, GenerationJobModel
from src.infrastructure.providers.registry import provider_registry


class GenerationService:
    """Service handling job ingestion, idempotency hashing, and asset retrieval."""

    @staticmethod
    def compute_payload_hash(req: GenerationCreateRequest) -> str:
        """
        Computes a deterministic SHA-256 fingerprint over normalized request attributes.
        """
        canonical = json.dumps(
            {
                "prompt": req.prompt.strip(),
                "negative_prompt": req.negative_prompt.strip() if req.negative_prompt else None,
                "width": req.width,
                "height": req.height,
                "steps": req.steps,
                "seed": req.seed,
                "model": req.model.strip().lower(),
                "provider": req.provider.strip().lower(),
            },
            sort_keys=True,
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @classmethod
    async def create_or_get_generation(
        cls,
        api_key: ApiKeyModel,
        req: GenerationCreateRequest,
        idempotency_key: Optional[str],
        db: AsyncSession,
    ) -> Tuple[GenerationJobModel, bool]:
        """
        Enqueues a new generation job or idempotently returns an existing job.
        
        Returns:
            Tuple[GenerationJobModel, bool] where bool is True if newly created, False if idempotent hit.
        """
        api_key_id = str(api_key.id)
        await set_tenant_context(db, api_key_id)

        # Validate idempotency key format if present
        clean_idemp_key: Optional[str] = None
        if idempotency_key is not None:
            clean_idemp_key = idempotency_key.strip()
            if not clean_idemp_key:
                raise ValidationException("Idempotency-Key header cannot be empty or whitespace.")
            if len(clean_idemp_key) > 128:
                raise ValidationException("Idempotency-Key header exceeds maximum length of 128 characters.")
            if any(ord(c) < 32 or ord(c) == 127 for c in clean_idemp_key):
                raise ValidationException("Idempotency-Key header contains invalid control characters.")

        payload_hash = cls.compute_payload_hash(req)

        # 1. Check existing job by idempotency key
        if clean_idemp_key:
            stmt = (
                select(GenerationJobModel)
                .options(selectinload(GenerationJobModel.asset))
                .where(
                    GenerationJobModel.api_key_id == api_key_id,
                    GenerationJobModel.idempotency_key == clean_idemp_key,
                )
            )
            res = await db.execute(stmt)
            existing_job = res.scalar_one_or_none()

            if existing_job:
                # Compare canonical payload fingerprints
                if existing_job.payload_hash == payload_hash:
                    return existing_job, False
                raise IdempotencyConflictException(clean_idemp_key)

        # 2. Determine target provider
        requested_provider = req.provider.lower().strip()
        if requested_provider == "auto":
            requested_provider = provider_registry.get_default_provider_name()

        # 3. Create new job record
        now_utc = datetime.now(timezone.utc)
        new_job = GenerationJobModel(
            id=str(uuid.uuid4()),
            api_key_id=api_key_id,
            idempotency_key=clean_idemp_key,
            payload_hash=payload_hash,
            status=JobStatus.PENDING.value,
            prompt=req.prompt.strip(),
            negative_prompt=req.negative_prompt.strip() if req.negative_prompt else None,
            width=req.width,
            height=req.height,
            steps=req.steps,
            seed=req.seed,
            model=req.model.strip(),
            provider_requested=requested_provider,
            created_at=now_utc,
        )
        db.add(new_job)

        try:
            await db.commit()
            return new_job, True
        except IntegrityError:
            # Atomic race on (api_key_id, idempotency_key)
            await db.rollback()
            if clean_idemp_key:
                # Give the winning concurrent transaction a moment to finalize its commit
                for attempt in range(10):
                    await asyncio.sleep(0.02 * (attempt + 1))
                    query_stmt = (
                        select(GenerationJobModel)
                        .options(selectinload(GenerationJobModel.asset))
                        .where(
                            GenerationJobModel.api_key_id == api_key_id,
                            GenerationJobModel.idempotency_key == clean_idemp_key,
                        )
                    )
                    query_res = await db.execute(query_stmt)
                    raced_job = query_res.scalar_one_or_none()
                    if raced_job:
                        if raced_job.payload_hash == payload_hash:
                            return raced_job, False
                        raise IdempotencyConflictException(clean_idemp_key)
            raise

    @staticmethod
    async def get_job_for_client(
        api_key: ApiKeyModel,
        job_id: str,
        db: AsyncSession,
    ) -> GenerationJobModel:
        """
        Queries a job by ID, verifying that the requesting client owns it.
        Raises EntityNotFoundException if missing, or ForbiddenException on cross-key access.
        """
        api_key_id = str(api_key.id)
        await set_tenant_context(db, api_key_id)
        stmt = (
            select(GenerationJobModel)
            .options(selectinload(GenerationJobModel.asset))
            .where(GenerationJobModel.id == job_id)
        )
        res = await db.execute(stmt)
        job = res.scalar_one_or_none()

        if not job:
            raise EntityNotFoundException("GenerationJob", job_id)

        # Authorization check: Client ownership boundary
        if job.api_key_id != api_key_id:
            raise ForbiddenException("You do not have permission to access this generation job.")

        return job

    @classmethod
    async def get_asset_bytes_for_client(
        cls,
        api_key: ApiKeyModel,
        job_id: str,
        storage: AssetStorage,
        db: AsyncSession,
    ) -> Tuple[bytes, GenerationJobModel]:
        """
        Retrieves binary image bytes for a completed job owned by the client.
        Uses authoritative persisted storage_path reference with backward-compatible
        hash fallback.
        """
        job = await cls.get_job_for_client(api_key, job_id, db)

        if job.status != JobStatus.COMPLETED.value or not job.asset:
            raise EntityNotFoundException(
                "Asset",
                f"Asset for job '{job_id}' is not yet available (current status: {job.status}).",
            )

        # Authoritative locator resolution:
        # Prefer persisted storage_path; fall back to file_hash for legacy records or mocks.
        storage_path = getattr(job.asset, "storage_path", None)
        file_hash = getattr(job.asset, "file_hash", None)

        primary_locator = storage_path if (isinstance(storage_path, str) and storage_path.strip()) else None
        fallback_locator = file_hash if (isinstance(file_hash, str) and file_hash.strip()) else None

        locator = primary_locator or fallback_locator
        if not locator:
            raise EntityNotFoundException("Asset file", str(job.id))

        file_bytes: Optional[bytes] = None
        try:
            file_bytes = await storage.get_image_bytes(locator)
        except ValueError as val_err:
            logger.warning(
                "Primary storage retrieval failed with ValueError for job %s: %s",
                job.id,
                val_err,
            )
            file_bytes = None

        # Fallback to file_hash if primary locator (storage_path) returned no bytes
        # and fallback_locator is available and distinct from primary locator
        if not file_bytes and fallback_locator and fallback_locator != locator:
            try:
                file_bytes = await storage.get_image_bytes(fallback_locator)
            except (ValueError, Exception) as fallback_err:
                logger.debug(
                    "Fallback storage retrieval by file_hash failed for job %s: %s",
                    job.id,
                    fallback_err,
                )

        if not file_bytes:
            # Safe identifier: use file_hash if available, else job.id. Never leak internal paths.
            safe_identifier = fallback_locator or str(job.id)
            raise EntityNotFoundException("Asset file", safe_identifier)

        return file_bytes, job
