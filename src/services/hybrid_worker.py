"""
Hybrid Outbound Polling Worker (Mode B)
Location: src/services/hybrid_worker.py

Security & Operational Guarantees:
1. 100% Outbound HTTPS polling: No inbound ports opened on the development machine.
2. Authenticates via Authorization: Bearer <WORKER_SHARED_SECRET>.
3. Periodic heartbeat keep-alive during long inference runs.
4. Clean failure reporting and graceful shutdown handling.
"""

import asyncio
from datetime import datetime, timezone
import time
from typing import Optional
import uuid
import httpx

from src.core.logging import correlation_id_ctx, logger, set_correlation_id
from src.core.metrics import (
    worker_heartbeats_total,
    worker_inference_duration_seconds,
    worker_jobs_completed_total,
    worker_jobs_failed_total,
    worker_lease_lost_total,
    worker_status_gauge,
    worker_upload_duration_seconds,
)
from src.domain.interfaces import (
    AssetStorage,
    ImageProviderAdapter,
    ProviderRequest,
    ProviderResponse,
)


def classify_error(exc: BaseException) -> str:
    """
    Classifies exceptions into standard, actionable operational error codes (<= 64 chars)
    matching control-plane retry and failure policies.
    """
    if isinstance(exc, asyncio.CancelledError):
        return "WORKER_CANCELLED"

    exc_name = type(exc).__name__
    exc_str = str(exc).lower()

    if isinstance(exc, MemoryError) or "out of memory" in exc_str or "cuda oom" in exc_str:
        return "GPU_OUT_OF_MEMORY"
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)) or "timeout" in exc_str:
        return "INFERENCE_TIMEOUT"
    if "storage" in exc_str or "cas" in exc_str or "bucket" in exc_str or "s3" in exc_str or "upload" in exc_str:
        return "STORAGE_ERROR"
    if isinstance(exc, ValueError):
        return "INVALID_PAYLOAD"

    return exc_name[:64]


class HybridWorker:
    """
    Outbound HTTPS polling worker running on the development machine in Mode B.
    """

    def __init__(
        self,
        base_url: str,
        worker_shared_secret: str,
        provider: ImageProviderAdapter,
        storage: AssetStorage,
        worker_id: Optional[str] = None,
        poll_interval_seconds: float = 2.0,
        heartbeat_interval_seconds: float = 30.0,
        request_timeout_seconds: float = 30.0,
        verify_ssl: bool = True,
        client: Optional[httpx.AsyncClient] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.worker_shared_secret = worker_shared_secret
        self.provider = provider
        self.storage = storage
        self.worker_id = worker_id or f"hybrid-worker-{uuid.uuid4().hex[:8]}"
        self.worker_category = "embedded" if "embedded" in self.worker_id.lower() else "hybrid_gpu"
        self.poll_interval = poll_interval_seconds
        self.heartbeat_interval = heartbeat_interval_seconds
        self.request_timeout = request_timeout_seconds
        self.verify_ssl = verify_ssl
        self._external_client = client
        self._is_running = False
        self._stop_event = asyncio.Event()
        self._current_job_id: Optional[str] = None
        self._lease_lost: bool = False
        self._lease_lost_count: int = 0
        self._jobs_completed: int = 0
        self._jobs_failed: int = 0
        self._heartbeats_sent: int = 0
        self._heartbeats_failed: int = 0
        self._last_inference_duration_ms: Optional[float] = None
        self._last_upload_duration_ms: Optional[float] = None
        self._last_success_at: Optional[str] = None

    @property
    def is_running(self) -> bool:
        """Indicates whether the worker polling loop is actively running."""
        return self._is_running

    @property
    def is_stopping(self) -> bool:
        """Indicates whether a shutdown request has been signaled."""
        return self._stop_event.is_set()

    @property
    def current_job_id(self) -> Optional[str]:
        """Returns the job_id currently being processed, or None if idle."""
        return self._current_job_id

    @property
    def lease_lost(self) -> bool:
        """Indicates whether the lease for the current job was rejected/revoked by the control plane."""
        return self._lease_lost

    @property
    def metrics(self) -> dict:
        """Returns runtime telemetry for worker observability and health checks."""
        return {
            "worker_id": self.worker_id,
            "worker_category": self.worker_category,
            "is_running": self._is_running,
            "is_stopping": self._stop_event.is_set(),
            "current_job_id": self._current_job_id,
            "lease_lost": self._lease_lost,
            "lease_lost_count": self._lease_lost_count,
            "jobs_completed": self._jobs_completed,
            "jobs_failed": self._jobs_failed,
            "heartbeats_sent": self._heartbeats_sent,
            "heartbeats_failed": self._heartbeats_failed,
            "last_inference_duration_ms": self._last_inference_duration_ms,
            "last_upload_duration_ms": self._last_upload_duration_ms,
            "last_success_at": self._last_success_at,
        }

    def _get_headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.worker_shared_secret}",
            "Content-Type": "application/json",
        }

    async def _report_failure(
        self,
        client: httpx.AsyncClient,
        job_id: str,
        error_code: str,
        error_message: str,
        retry_delay_seconds: Optional[int] = None,
    ) -> bool:
        """
        Reports execution failure or relinquishes an acquired lease back to the control plane.
        Catches and logs all network exceptions so cleanup failures never obscure primary exceptions.
        """
        fail_url = f"{self.base_url}/api/v1/worker/jobs/{job_id}/fail"
        fail_payload = {
            "worker_id": self.worker_id,
            "error_code": error_code[:64],
            "error_message": error_message[:500],
        }
        if retry_delay_seconds is not None:
            fail_payload["retry_delay_seconds"] = retry_delay_seconds

        try:
            resp = await client.post(fail_url, json=fail_payload, headers=self._get_headers())
            if resp.status_code == 200:
                logger.info(f"Worker {self.worker_id}: Reported failure/release for job {job_id} ({error_code})")
                return True
            else:
                logger.warning(
                    f"Worker {self.worker_id}: Failed to report failure for job {job_id} ({resp.status_code}): {resp.text}"
                )
                return False
        except Exception as f_err:
            logger.error(f"Worker {self.worker_id}: Network error reporting failure for job {job_id}: {f_err}")
            return False

    async def poll_and_execute_once(self, client: httpx.AsyncClient) -> bool:
        """
        Executes a single poll -> execute -> complete/fail cycle.
        Returns True if a job was leased and processed; False if queue was empty or worker stopping.
        """
        # Invariant 1: Stop claiming new jobs once shutdown has been requested
        if self._stop_event.is_set():
            logger.debug(f"Worker {self.worker_id}: Shutdown requested, skipping job claim.")
            return False

        lease_url = f"{self.base_url}/api/v1/worker/jobs/lease"
        payload = {
            "worker_id": self.worker_id,
            "lease_timeout_seconds": 120,
        }

        try:
            resp = await client.post(lease_url, json=payload, headers=self._get_headers())
        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Network error polling control plane: {e}")
            return False

        if resp.status_code == 204:
            return False

        if resp.status_code != 200:
            logger.error(f"Worker {self.worker_id}: Unexpected lease response {resp.status_code}: {resp.text}")
            return False

        job = resp.json()
        job_id = job["job_id"]
        self._current_job_id = job_id
        self._lease_lost = False
        prev_correlation_id = correlation_id_ctx.get()
        set_correlation_id(job_id)
        logger.info(f"Worker {self.worker_id}: Leased job {job_id} ({job['width']}x{job['height']})")

        # Invariant 2: A lease acquired during a shutdown race is not silently abandoned
        if self._stop_event.is_set():
            logger.info(
                f"Worker {self.worker_id}: Shutdown requested during lease acquisition for job {job_id}. "
                "Relinquishing lease immediately."
            )
            self._jobs_failed += 1
            await self._report_failure(
                client,
                job_id=job_id,
                error_code="WORKER_SHUTDOWN",
                error_message="Worker shutting down during lease acquisition.",
                retry_delay_seconds=1,
            )
            self._current_job_id = None
            set_correlation_id(prev_correlation_id)
            return False

        # Start periodic heartbeat task
        heartbeat_stop = asyncio.Event()
        heartbeat_task = asyncio.create_task(self._heartbeat_loop(client, job_id, heartbeat_stop))

        try:
            # 1. Run inference
            provider_req = ProviderRequest(
                prompt=job["prompt"],
                negative_prompt=job.get("negative_prompt"),
                width=job["width"],
                height=job["height"],
                steps=job["steps"],
                seed=job["seed"],
            )

            provider_resp: ProviderResponse = await self.provider.generate_image(provider_req)
            self._last_inference_duration_ms = float(provider_resp.duration_ms) if provider_resp.duration_ms else None

            # 2. Store asset
            upload_start = time.perf_counter()
            stored_asset = await self.storage.store_image(
                provider_resp.image_bytes,
                mime_type=provider_resp.mime_type,
                is_synthetic=provider_resp.is_synthetic,
            )
            upload_duration = time.perf_counter() - upload_start
            self._last_upload_duration_ms = upload_duration * 1000.0
            try:
                worker_upload_duration_seconds.observe(upload_duration, labels={"worker_category": self.worker_category})
            except Exception:
                pass

            # Invariant 3: Do not report completion if lease ownership was revoked during execution
            if self._lease_lost:
                logger.error(
                    f"Worker {self.worker_id}: Aborting completion report for job {job_id} because lease was lost/revoked."
                )
                self._jobs_failed += 1
                try:
                    worker_jobs_failed_total.inc(labels={"worker_category": self.worker_category, "error_code": "LEASE_LOST"})
                except Exception:
                    pass
                return False

            # 3. Report completion
            complete_url = f"{self.base_url}/api/v1/worker/jobs/{job_id}/complete"
            complete_payload = {
                "worker_id": self.worker_id,
                "file_hash": stored_asset.file_hash,
                "file_size_bytes": stored_asset.file_size_bytes,
                "storage_provider": "local_cas" if "cas" in getattr(self.storage, "name", "").lower() else "remote_private",
                "storage_path": stored_asset.storage_path,
                "public_url": None,
                "mime_type": stored_asset.mime_type,
                "width": stored_asset.width,
                "height": stored_asset.height,
                "execution_time_ms": provider_resp.duration_ms,
                "peak_vram_mb": provider_resp.peak_vram_mb,
                "provider_used": provider_resp.provider_name,
            }

            c_resp = await client.post(complete_url, json=complete_payload, headers=self._get_headers())
            if c_resp.status_code == 200:
                logger.info(f"Worker {self.worker_id}: Successfully completed job {job_id}")
                self._jobs_completed += 1
                self._last_success_at = datetime.now(timezone.utc).isoformat()
                try:
                    worker_jobs_completed_total.inc(labels={"worker_category": self.worker_category})
                    if provider_resp.duration_ms:
                        worker_inference_duration_seconds.observe(
                            provider_resp.duration_ms / 1000.0, labels={"worker_category": self.worker_category}
                        )
                except Exception:
                    pass
                return True
            elif c_resp.status_code == 409:
                logger.error(
                    f"Worker {self.worker_id}: Completion rejected (409 Conflict). Lease was lost on job {job_id}."
                )
                self._lease_lost = True
                self._lease_lost_count += 1
                self._jobs_failed += 1
                try:
                    worker_lease_lost_total.inc(labels={"worker_category": self.worker_category})
                    worker_jobs_failed_total.inc(labels={"worker_category": self.worker_category, "error_code": "LEASE_LOST"})
                except Exception:
                    pass
                return False
            else:
                logger.error(f"Worker {self.worker_id}: Failed to report completion {c_resp.status_code}: {c_resp.text}")
                self._jobs_failed += 1
                try:
                    worker_jobs_failed_total.inc(labels={"worker_category": self.worker_category, "error_code": "COMPLETION_REPORT_FAILED"})
                except Exception:
                    pass
                return False

        except asyncio.CancelledError:
            # Invariant 4: Cancellation relinquishes lease to PENDING without swallowing CancelledError
            logger.warning(f"Worker {self.worker_id}: Execution cancelled for job {job_id}. Relinquishing lease...")
            self._jobs_failed += 1
            try:
                worker_jobs_failed_total.inc(labels={"worker_category": self.worker_category, "error_code": "WORKER_CANCELLED"})
            except Exception:
                pass
            if not self._lease_lost:
                try:
                    await asyncio.wait_for(
                        asyncio.shield(
                            self._report_failure(
                                client,
                                job_id=job_id,
                                error_code="WORKER_CANCELLED",
                                error_message="Worker task was cancelled during processing.",
                                retry_delay_seconds=1,
                            )
                        ),
                        timeout=min(self.request_timeout, 5.0),
                    )
                except (asyncio.TimeoutError, Exception) as c_err:
                    logger.warning(
                        f"Worker {self.worker_id}: Timed out or failed reporting cancellation for job {job_id}: {c_err}"
                    )
            raise

        except Exception as exc:
            self._jobs_failed += 1
            err_code = classify_error(exc)
            try:
                worker_jobs_failed_total.inc(labels={"worker_category": self.worker_category, "error_code": err_code})
            except Exception:
                pass
            logger.error(f"Worker {self.worker_id}: Inference/storage failed for job {job_id} [{err_code}]: {exc}")
            if not self._lease_lost:
                await self._report_failure(
                    client,
                    job_id=job_id,
                    error_code=err_code,
                    error_message=str(exc),
                )
            return True

        finally:
            self._current_job_id = None
            set_correlation_id(prev_correlation_id)
            # Stop heartbeat loop safely and boundedly
            heartbeat_stop.set()
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass

    async def _heartbeat_loop(self, client: httpx.AsyncClient, job_id: str, stop_event: asyncio.Event):
        """Periodically renews the lease while inference is running."""
        hb_url = f"{self.base_url}/api/v1/worker/jobs/{job_id}/heartbeat"
        hb_payload = {"worker_id": self.worker_id}

        while not stop_event.is_set():
            try:
                await asyncio.sleep(self.heartbeat_interval)
                if stop_event.is_set():
                    break
                resp = await client.post(hb_url, json=hb_payload, headers=self._get_headers())
                if resp.status_code == 200:
                    self._heartbeats_sent += 1
                    try:
                        worker_heartbeats_total.inc(labels={"worker_category": self.worker_category, "outcome": "success"})
                    except Exception:
                        pass
                    logger.debug(f"Worker {self.worker_id}: Heartbeat renewed for job {job_id}")
                elif resp.status_code == 409:
                    logger.warning(
                        f"Worker {self.worker_id}: Heartbeat returned 409 Conflict (lease lost) on job {job_id}: {resp.text}"
                    )
                    self._lease_lost = True
                    self._lease_lost_count += 1
                    self._heartbeats_failed += 1
                    try:
                        worker_heartbeats_total.inc(labels={"worker_category": self.worker_category, "outcome": "conflict"})
                        worker_lease_lost_total.inc(labels={"worker_category": self.worker_category})
                    except Exception:
                        pass
                    break
                else:
                    self._heartbeats_failed += 1
                    try:
                        worker_heartbeats_total.inc(labels={"worker_category": self.worker_category, "outcome": "failure"})
                    except Exception:
                        pass
                    logger.warning(f"Worker {self.worker_id}: Heartbeat returned {resp.status_code}: {resp.text}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._heartbeats_failed += 1
                try:
                    worker_heartbeats_total.inc(labels={"worker_category": self.worker_category, "outcome": "failure"})
                except Exception:
                    pass
                logger.warning(f"Worker {self.worker_id}: Heartbeat ping failed: {e}")

    async def run(self):
        """Starts the persistent polling loop."""
        self._is_running = True
        self._stop_event.clear()
        self._lease_lost = False
        self._current_job_id = None
        try:
            worker_status_gauge.set(1.0, labels={"worker_category": self.worker_category})
        except Exception:
            pass
        logger.info(f"HybridWorker {self.worker_id} started. Polling {self.base_url}")

        client = self._external_client or httpx.AsyncClient(
            timeout=httpx.Timeout(
                timeout=self.request_timeout,
                connect=min(10.0, self.request_timeout),
            ),
            verify=self.verify_ssl,
        )
        try:
            while self._is_running and not self._stop_event.is_set():
                had_job = await self.poll_and_execute_once(client)
                if not had_job and not self._stop_event.is_set():
                    # Queue empty, wait for next poll interval unless stopped
                    try:
                        await asyncio.wait_for(self._stop_event.wait(), timeout=self.poll_interval)
                    except asyncio.TimeoutError:
                        pass
        finally:
            self._is_running = False
            try:
                worker_status_gauge.set(0.0, labels={"worker_category": self.worker_category})
            except Exception:
                pass
            if not self._external_client:
                await client.aclose()
            logger.info(f"HybridWorker {self.worker_id} stopped.")

    def stop(self):
        """Signals the polling loop to stop cleanly."""
        self._is_running = False
        self._stop_event.set()
        try:
            worker_status_gauge.set(0.0, labels={"worker_category": self.worker_category})
        except Exception:
            pass
        except Exception:
            pass
