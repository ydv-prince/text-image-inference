"""
Asynchronous Queue Worker Service
Location: src/services/queue_worker.py

Guarantees:
1. Concurrency: Claims 1 job at a time, honoring hardware bounds.
2. Safety: Validates requests before any provider execution.
3. Explicit Routing: Queries provider explicitly via ProviderRegistry; never switches silently.
4. Resilience: Categorizes errors (deterministic vs transient), applies exponential backoff,
   and prevents infinite loops for malformed inputs.
5. Idempotent Storage: Stores output in CAS before updating completion in the database.
6. Clean Shutdown: Catches CancelledError gracefully without leaving corrupted state.
"""

import asyncio
from typing import Optional
import uuid

from src.core.config import settings
from src.core.errors import AaravException, GPUOutOfMemoryException, ValidationException
from src.core.logging import logger
from src.domain.interfaces import (
    AssetStorage,
    JobCompletionData,
    JobRepository,
    ProviderRequest,
)
from src.infrastructure.providers.registry import provider_registry


class QueueWorker:
    """
    Background worker that continuously claims and executes image generation jobs.
    """

    def __init__(
        self,
        repository: JobRepository,
        storage: AssetStorage,
        worker_id: Optional[str] = None,
        poll_interval: Optional[float] = None,
    ):
        self.repository = repository
        self.storage = storage
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.poll_interval = poll_interval if poll_interval is not None else settings.WORKER_POLL_INTERVAL_SECONDS
        self._stop_event = asyncio.Event()

    async def process_one_job(self) -> bool:
        """
        Attempts to claim and execute a single job from the queue.
        Returns True if a job was processed, False if queue was empty/not ready.
        """
        # 1. Claim next pending job
        claim = await self.repository.claim_next_job(
            worker_id=self.worker_id,
            lease_timeout_seconds=settings.LEASE_TIMEOUT_SECONDS,
        )

        if not claim:
            return False

        logger.info(
            f"Worker '{self.worker_id}' leased job {claim.job_id} (attempt {claim.retry_count + 1}/{claim.max_retries})"
        )

        # 2. Pre-execution Request Validation (Catch deterministic invalid payloads)
        try:
            provider_req = ProviderRequest(
                prompt=claim.prompt,
                negative_prompt=claim.negative_prompt,
                width=claim.width,
                height=claim.height,
                steps=claim.steps,
                seed=claim.seed,
            )
        except (ValueError, ValidationException) as val_err:
            # Deterministic error: payload is permanently invalid; do NOT retry!
            logger.warning(f"Job {claim.job_id} rejected with deterministic validation error: {val_err}")
            await self.repository.fail_job(
                job_id=claim.job_id,
                worker_id=self.worker_id,
                error_code="INVALID_PAYLOAD",
                error_message=str(val_err),
                is_retryable=False,  # Immediate terminal failure
            )
            return True

        # 3. Explicit Provider Selection (No silent switching)
        try:
            provider = provider_registry.get_provider(claim.provider_requested)
        except Exception as prov_err:
            logger.error(f"Failed resolving provider '{claim.provider_requested}' for job {claim.job_id}: {prov_err}")
            await self.repository.fail_job(
                job_id=claim.job_id,
                worker_id=self.worker_id,
                error_code="PROVIDER_NOT_FOUND",
                error_message=str(prov_err),
                is_retryable=False,
            )
            return True

        # 4. Execute Inference (Outside any DB transaction)
        try:
            response = await provider.generate_image(provider_req)
        except GPUOutOfMemoryException as oom:
            # Transient hardware exhaustion: retry with backoff
            logger.error(f"GPU OOM while processing job {claim.job_id}: {oom}")
            backoff = min(settings.BASE_RETRY_BACKOFF_SECONDS * (2 ** claim.retry_count), settings.MAX_RETRY_BACKOFF_SECONDS)
            await self.repository.fail_job(
                job_id=claim.job_id,
                worker_id=self.worker_id,
                error_code="GPU_OUT_OF_MEMORY",
                error_message=str(oom),
                is_retryable=True,
                backoff_seconds=backoff,
            )
            return True
        except Exception as inf_err:
            # General inference failure: retryable if transient
            logger.error(f"Inference error while processing job {claim.job_id}: {inf_err}", exc_info=True)
            backoff = min(settings.BASE_RETRY_BACKOFF_SECONDS * (2 ** claim.retry_count), settings.MAX_RETRY_BACKOFF_SECONDS)
            await self.repository.fail_job(
                job_id=claim.job_id,
                worker_id=self.worker_id,
                error_code="INFERENCE_FAILED",
                error_message=str(inf_err),
                is_retryable=True,
                backoff_seconds=backoff,
            )
            return True

        # 5. Persist Binary Asset into Content-Addressable Storage
        try:
            stored_asset = await self.storage.store_image(
                image_bytes=response.image_bytes,
                mime_type=response.mime_type,
                is_synthetic=response.is_synthetic,
            )
        except Exception as stor_err:
            logger.error(f"Storage failure for job {claim.job_id}: {stor_err}", exc_info=True)
            backoff = min(settings.BASE_RETRY_BACKOFF_SECONDS * (2 ** claim.retry_count), settings.MAX_RETRY_BACKOFF_SECONDS)
            await self.repository.fail_job(
                job_id=claim.job_id,
                worker_id=self.worker_id,
                error_code="STORAGE_FAILED",
                error_message=str(stor_err),
                is_retryable=True,
                backoff_seconds=backoff,
            )
            return True

        # 6. Record Job Completion in Repository
        try:
            completion = JobCompletionData(
                job_id=claim.job_id,
                worker_id=self.worker_id,
                provider_used=response.provider_name,
                execution_time_ms=response.duration_ms,
                peak_vram_mb=response.peak_vram_mb,
                asset=stored_asset,
            )
            await self.repository.complete_job(completion)
            logger.info(f"Job {claim.job_id} successfully completed by worker '{self.worker_id}'.")
        except Exception as comp_err:
            logger.error(f"Failed recording completion for job {claim.job_id}: {comp_err}", exc_info=True)
            # If lease expired or DB failed, lease sweeper or retry will recover
            raise

        return True

    async def run(self) -> None:
        """Runs the continuous worker loop until stopped."""
        logger.info(f"QueueWorker '{self.worker_id}' started.")
        try:
            while not self._stop_event.is_set():
                processed = await self.process_one_job()
                if not processed:
                    await asyncio.sleep(self.poll_interval)
        except asyncio.CancelledError:
            logger.info(f"QueueWorker '{self.worker_id}' received shutdown cancellation.")
        finally:
            logger.info(f"QueueWorker '{self.worker_id}' stopped.")

    def stop(self) -> None:
        """Signals the worker to stop processing."""
        self._stop_event.set()
