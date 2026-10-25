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
from typing import Optional
import uuid
import httpx

from src.core.logging import logger
from src.domain.interfaces import (
    AssetStorage,
    ImageProviderAdapter,
    ProviderRequest,
    ProviderResponse,
)


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
        self.poll_interval = poll_interval_seconds
        self.heartbeat_interval = heartbeat_interval_seconds
        self.request_timeout = request_timeout_seconds
        self.verify_ssl = verify_ssl
        self._external_client = client
        self._is_running = False
        self._stop_event = asyncio.Event()

    def _get_headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.worker_shared_secret}",
            "Content-Type": "application/json",
        }

    async def poll_and_execute_once(self, client: httpx.AsyncClient) -> bool:
        """
        Executes a single poll -> execute -> complete/fail cycle.
        Returns True if a job was leased and processed; False if queue was empty.
        """
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
        logger.info(f"Worker {self.worker_id}: Leased job {job_id} ({job['width']}x{job['height']})")

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

            # 2. Store asset
            stored_asset = await self.storage.store_image(
                provider_resp.image_bytes,
                mime_type=provider_resp.mime_type,
                is_synthetic=provider_resp.is_synthetic,
            )

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
            else:
                logger.error(f"Worker {self.worker_id}: Failed to report completion {c_resp.status_code}: {c_resp.text}")

            return True

        except Exception as exc:
            logger.error(f"Worker {self.worker_id}: Inference/storage failed for job {job_id}: {exc}")
            fail_url = f"{self.base_url}/api/v1/worker/jobs/{job_id}/fail"
            fail_payload = {
                "worker_id": self.worker_id,
                "error_code": type(exc).__name__,
                "error_message": str(exc)[:500],
            }
            try:
                await client.post(fail_url, json=fail_payload, headers=self._get_headers())
            except Exception as f_err:
                logger.error(f"Worker {self.worker_id}: Failed to report failure for job {job_id}: {f_err}")
            return True

        finally:
            # Stop heartbeat loop
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
                    logger.debug(f"Worker {self.worker_id}: Heartbeat renewed for job {job_id}")
                else:
                    logger.warning(f"Worker {self.worker_id}: Heartbeat returned {resp.status_code}: {resp.text}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Worker {self.worker_id}: Heartbeat ping failed: {e}")

    async def run(self):
        """Starts the persistent polling loop."""
        self._is_running = True
        self._stop_event.clear()
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
                if not had_job:
                    # Queue empty, wait for next poll interval
                    try:
                        await asyncio.wait_for(self._stop_event.wait(), timeout=self.poll_interval)
                    except asyncio.TimeoutError:
                        pass
        finally:
            if not self._external_client:
                await client.aclose()
            logger.info(f"HybridWorker {self.worker_id} stopped.")

    def stop(self):
        """Signals the polling loop to stop cleanly."""
        self._is_running = False
        self._stop_event.set()
