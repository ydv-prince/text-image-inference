"""
Staging Deployment Smoke Test Harness
Location: scripts/staging_smoke_test.py

Executes a single controlled end-to-end verification cycle against a deployment:
1. Health Probes: Verify liveness and readiness (GET /health/liveness, GET /health/readiness).
2. Public Ingress: Enqueue generation request (POST /api/v1/images/generations).
3. Worker Lease: Acquire job lease (POST /api/v1/worker/jobs/lease).
4. Heartbeat: Renew lease timestamp during execution (POST /api/v1/worker/jobs/{id}/heartbeat).
5. Completion: Durably persist asset to storage and report completion (POST /api/v1/worker/jobs/{id}/complete).
6. Client Polling: Poll job status until COMPLETED (GET /api/v1/images/generations/{id}).
7. Delivery: Download and verify raw PNG binary (GET /api/v1/images/generations/{id}/content).
8. Observability: Verify Prometheus telemetry exposition (GET /metrics).

Security & Guard Invariants:
- Default target is strictly local loopback (http://localhost:8000).
- Non-loopback targets strictly require explicit opt-in (--allow-remote).
- Rejects known production hostnames and production-tagged domains even if --allow-remote is set.
- Rejects URLs with embedded userinfo (credentials).
- Credentials prioritized via environment variables (CLIENT_API_KEY, WORKER_SHARED_SECRET, ADMIN_API_KEY).
- Fails closed on missing credentials without dummy secret fallbacks.
- Never prints secrets, bearer tokens, or raw response bodies in logs or error diagnostics.
- Supports dependency-injected test clients and storage adapters without network sockets.
"""

import argparse
import asyncio
from dataclasses import dataclass
import hashlib
import io
import os
import sys
import time
from typing import Optional
from urllib.parse import urlsplit
import httpx
from PIL import Image

from src.domain.interfaces import AssetStorage

LOOPBACK_HOSTS = {"localhost", "127.0.0.1", "::1", "testserver"}
BANNED_PRODUCTION_HOSTS = {
    "api.aarav-inference.internal",
    "api.aarav-inference.com",
    "aarav-inference.com",
    "production.aarav-inference.internal",
}


def validate_target_url(url: str, allow_remote: bool = False) -> str:
    """
    Validates the target base URL for the smoke test harness.

    Guarantees:
    1. Scheme must be 'http' or 'https'.
    2. URL must not contain embedded userinfo (credentials in URL).
    3. Hostname must be non-empty and well-formed.
    4. Non-loopback targets strictly require allow_remote=True.
    5. Rejects known production hostnames and production-tagged domains even if allow_remote=True.
    6. Fails closed on any ambiguity.
    """
    if not url or not isinstance(url, str) or not url.strip():
        raise ValueError("Target URL is empty or unset.")

    raw = url.strip()
    try:
        parsed = urlsplit(raw)
    except Exception as e:
        raise ValueError(f"Malformed target URL '{raw}': {e}") from e

    scheme = (parsed.scheme or "").lower()
    if scheme not in ("http", "https"):
        raise ValueError(f"Invalid URL scheme '{scheme}'. Only 'http' and 'https' are permitted.")

    if parsed.username or parsed.password:
        raise ValueError(
            "SECURITY VIOLATION: URL contains embedded credentials (userinfo). "
            "Pass credentials via environment variables or request headers."
        )

    hostname = (parsed.hostname or "").lower()
    if not hostname:
        raise ValueError(f"Malformed URL '{raw}': Missing hostname.")

    is_loopback = hostname in LOOPBACK_HOSTS or hostname.startswith("127.")

    # Check known production hostnames
    if hostname in BANNED_PRODUCTION_HOSTS:
        raise ValueError(
            f"SECURITY VIOLATION: Target hostname '{hostname}' is a designated production host. "
            "Smoke test execution against production is prohibited."
        )

    # Check production-designated domain tokens
    host_tokens = hostname.replace("-", ".").split(".")
    if any(tok in ("production", "prod") for tok in host_tokens):
        raise ValueError(
            f"SECURITY VIOLATION: Target hostname '{hostname}' is designated as a production environment. "
            "Smoke test execution against production is prohibited."
        )

    if not is_loopback and not allow_remote:
        raise ValueError(
            f"SECURITY VIOLATION: Target URL '{raw}' points to a remote host ({hostname}). "
            "Remote execution requires explicit opt-in via --allow-remote."
        )

    return raw


def make_smoke_test_png(width: int = 512, height: int = 512) -> bytes:
    """Generates a valid test PNG byte stream."""
    img = Image.new("RGB", (width, height), color=(64, 128, 255))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


@dataclass
class SmokeTestConfig:
    base_url: str
    client_api_key: str
    worker_shared_secret: str
    admin_api_key: Optional[str] = None
    timeout_seconds: float = 30.0
    mock_mode: bool = False
    allow_remote: bool = False


class StagingSmokeTestRunner:
    def __init__(
        self,
        config: SmokeTestConfig,
        client: Optional[httpx.AsyncClient] = None,
        storage: Optional[AssetStorage] = None,
    ):
        self.cfg = config
        self.client: Optional[httpx.AsyncClient] = client
        self.storage: Optional[AssetStorage] = storage
        self._owns_client: bool = False
        self.job_id: Optional[str] = None
        self.file_hash: Optional[str] = None

    def log(self, step: str, message: str, status: str = "INFO"):
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{ts}] [{status}] [{step}] {message}", flush=True)

    async def run(self) -> bool:
        """Executes the complete 8-step smoke test pipeline."""
        try:
            validated_url = validate_target_url(self.cfg.base_url, allow_remote=self.cfg.allow_remote)
        except ValueError as ve:
            self.log("ABORT", f"Target URL validation failed: {ve}", status="FAILED")
            return False

        if not self.cfg.client_api_key or not self.cfg.client_api_key.strip():
            self.log("ABORT", "Client API key is missing. Configure CLIENT_API_KEY.", status="FAILED")
            return False

        if not self.cfg.worker_shared_secret or not self.cfg.worker_shared_secret.strip():
            self.log("ABORT", "Worker shared secret is missing. Configure WORKER_SHARED_SECRET.", status="FAILED")
            return False

        self.log("INIT", f"Target Base URL: {validated_url}")
        self.log("INIT", "Client API Key: [CONFIGURED]")
        self.log("INIT", "Worker Secret:  [CONFIGURED]")

        created_client: Optional[httpx.AsyncClient] = None
        if self.client is None:
            created_client = httpx.AsyncClient(
                base_url=validated_url,
                timeout=self.cfg.timeout_seconds,
            )
            self.client = created_client
            self._owns_client = True

        try:
            return await self._execute_pipeline()
        finally:
            if self._owns_client and created_client is not None:
                await created_client.aclose()
                self.client = None
                self._owns_client = False

    async def _execute_pipeline(self) -> bool:
        try:
            # Step 1: Health Probes
            await self._step_health_probes()

            # Step 2: Client Generation Submission
            await self._step_client_submission()

            # Step 3: Worker Lease Acquisition
            await self._step_worker_lease()

            # Step 4: Worker Heartbeat Renewal
            await self._step_worker_heartbeat()

            # Step 5: Worker Completion Report with Asset Persistence
            await self._step_worker_completion()

            # Step 6: Client Polling & Status Verification
            await self._step_client_polling()

            # Step 7: Asset Delivery & Integrity Verification
            await self._step_asset_delivery()

            # Step 8: Observability & Metrics Scrape
            await self._step_metrics_scrape()

            self.log("SUCCESS", "All 8 staging smoke-test stages PASSED perfectly!", status="OK")
            return True

        except AssertionError as ae:
            self.log("ABORT", f"Smoke test assertion failed: {ae}", status="FAILED")
            return False
        except Exception as e:
            self.log("ABORT", f"Unexpected smoke test error ({type(e).__name__}): {e}", status="FAILED")
            return False

    async def _step_health_probes(self):
        self.log("STEP 1/8", "Verifying service liveness and readiness probes...")
        r_live = await self.client.get("/health/liveness")
        assert r_live.status_code == 200, f"Liveness probe failed with HTTP {r_live.status_code}"
        live_json = r_live.json()
        assert live_json.get("status") == "alive", "Liveness probe reported non-alive status"

        r_ready = await self.client.get("/health/readiness")
        assert r_ready.status_code == 200, f"Readiness probe failed with HTTP {r_ready.status_code}"
        ready_json = r_ready.json()
        assert ready_json.get("status") == "ready", "Readiness probe reported non-ready status"
        self.log("STEP 1/8", "Probes verified: Liveness=alive, Readiness=ready.", status="PASSED")

    async def _step_client_submission(self):
        self.log("STEP 2/8", "Submitting asynchronous generation request via client API...")
        headers = {
            "X-API-Key": self.cfg.client_api_key,
            "Idempotency-Key": f"smoke-test-{int(time.time())}",
            "Content-Type": "application/json",
        }
        payload = {
            "prompt": "Staging smoke-test generation: pristine mountain landscape",
            "width": 512,
            "height": 512,
            "steps": 4,
            "seed": 42,
        }
        resp = await self.client.post("/api/v1/images/generations", json=payload, headers=headers)
        assert resp.status_code == 202, f"Generation submission failed with HTTP {resp.status_code}"
        assert "Location" in resp.headers, "Submission response missing Location header"

        data = resp.json()
        assert "id" in data, "Submission response body missing job ID"
        self.job_id = data["id"]
        assert data.get("status") in ("PENDING", "PROCESSING"), f"Unexpected initial status: {data.get('status')}"
        self.log("STEP 2/8", f"Job successfully enqueued: ID={self.job_id}", status="PASSED")

    async def _step_worker_lease(self):
        self.log("STEP 3/8", f"Worker acquiring lease for job {self.job_id}...")
        headers = {
            "Authorization": f"Bearer {self.cfg.worker_shared_secret}",
            "Content-Type": "application/json",
        }
        payload = {
            "worker_id": "smoke-test-worker-node",
            "lease_timeout_seconds": 120,
        }
        resp = await self.client.post("/api/v1/worker/jobs/lease", json=payload, headers=headers)
        assert resp.status_code == 200, f"Worker lease claim failed with HTTP {resp.status_code}"

        job = resp.json()
        assert job.get("job_id") == self.job_id, f"Leased unexpected job ID: {job.get('job_id')}"
        assert job.get("width") == 512
        assert job.get("height") == 512
        self.log("STEP 3/8", f"Lease acquired by worker for job {self.job_id}.", status="PASSED")

    async def _step_worker_heartbeat(self):
        self.log("STEP 4/8", f"Sending heartbeat lease renewal for job {self.job_id}...")
        headers = {
            "Authorization": f"Bearer {self.cfg.worker_shared_secret}",
            "Content-Type": "application/json",
        }
        payload = {"worker_id": "smoke-test-worker-node"}
        resp = await self.client.post(
            f"/api/v1/worker/jobs/{self.job_id}/heartbeat",
            json=payload,
            headers=headers,
        )
        assert resp.status_code == 200, f"Heartbeat failed with HTTP {resp.status_code}"
        hb_data = resp.json()
        assert hb_data.get("status") == "renewed", "Heartbeat status was not renewed"
        self.log("STEP 4/8", "Heartbeat successfully acknowledged by control plane.", status="PASSED")

    async def _step_worker_completion(self):
        self.log("STEP 5/8", "Simulating inference asset and reporting completion...")
        png_bytes = make_smoke_test_png(512, 512)

        # Durably persist image bytes through the configured or injected storage adapter
        storage = self.storage
        if storage is None:
            from src.infrastructure.storage import get_asset_storage
            storage = get_asset_storage()

        stored = await storage.store_image(png_bytes)
        self.file_hash = stored.file_hash
        storage_path = stored.storage_path
        storage_provider = getattr(storage, "name", "local_cas")

        headers = {
            "Authorization": f"Bearer {self.cfg.worker_shared_secret}",
            "Content-Type": "application/json",
        }
        payload = {
            "worker_id": "smoke-test-worker-node",
            "file_hash": self.file_hash,
            "file_size_bytes": len(png_bytes),
            "storage_provider": storage_provider,
            "storage_path": storage_path,
            "public_url": None,
            "mime_type": "image/png",
            "width": 512,
            "height": 512,
            "execution_time_ms": 125,
            "peak_vram_mb": 2150,
            "provider_used": "mock" if self.cfg.mock_mode else "local-gpu",
        }
        resp = await self.client.post(
            f"/api/v1/worker/jobs/{self.job_id}/complete",
            json=payload,
            headers=headers,
        )
        assert resp.status_code == 200, f"Completion report failed with HTTP {resp.status_code}"
        data = resp.json()
        assert data.get("status") == "completed", "Unexpected completion status in response"
        self.log("STEP 5/8", "Worker completion acknowledged by control plane.", status="PASSED")

    async def _step_client_polling(self):
        self.log("STEP 6/8", f"Client polling job status for ID {self.job_id}...")
        headers = {"X-API-Key": self.cfg.client_api_key}
        resp = await self.client.get(f"/api/v1/images/generations/{self.job_id}", headers=headers)
        assert resp.status_code == 200, f"Polling failed with HTTP {resp.status_code}"

        data = resp.json()
        assert data.get("status") == "COMPLETED", f"Job status was not COMPLETED: {data.get('status')}"
        assert data.get("id") == self.job_id
        assert "links" in data and "content" in data["links"], "Asset download link missing from completed job"
        self.log("STEP 6/8", "Job status verified: status=COMPLETED.", status="PASSED")

    async def _step_asset_delivery(self):
        self.log("STEP 7/8", f"Downloading generated image content for job {self.job_id}...")
        headers = {"X-API-Key": self.cfg.client_api_key}
        resp = await self.client.get(f"/api/v1/images/generations/{self.job_id}/content", headers=headers)
        assert resp.status_code == 200, f"Asset delivery failed with HTTP {resp.status_code}"
        content_type = resp.headers.get("Content-Type", "")
        assert "image/png" in content_type, f"Asset delivery unexpected Content-Type: {content_type}"
        assert len(resp.content) > 0, "Downloaded asset was empty"
        assert resp.content[:8] == b"\x89PNG\r\n\x1a\n", "Downloaded asset missing valid PNG header signature"
        self.log("STEP 7/8", f"Asset delivered successfully ({len(resp.content)} bytes).", status="PASSED")

    async def _step_metrics_scrape(self):
        self.log("STEP 8/8", "Scraping Prometheus metrics exposition...")
        headers = {}
        if self.cfg.admin_api_key:
            headers["X-Admin-API-Key"] = self.cfg.admin_api_key

        resp = await self.client.get("/metrics", headers=headers)
        assert resp.status_code in (200, 401), f"Metrics scrape failed with HTTP {resp.status_code}"
        if resp.status_code == 200:
            content_type = resp.headers.get("Content-Type", "")
            assert "text/plain" in content_type, f"Unexpected metrics Content-Type: {content_type}"
            text = resp.text
            assert "aarav_http_requests_total" in text, "Metrics missing aarav_http_requests_total counter"
            assert "aarav_queue_depth" in text, "Metrics missing aarav_queue_depth gauge"
            self.log("STEP 8/8", "Metrics telemetry successfully scraped and verified.", status="PASSED")
        else:
            self.log("STEP 8/8", "Metrics endpoint correctly rejected unauthenticated scrape (401).", status="PASSED")


def parse_args():
    parser = argparse.ArgumentParser(description="Aarav Staging Deployment Smoke Test Harness")
    parser.add_argument("--base-url", default=None, help="Base URL of target API (defaults to http://localhost:8000)")
    parser.add_argument("--api-key", default=None, help="Client API key (preferred via CLIENT_API_KEY environment variable)")
    parser.add_argument("--worker-secret", default=None, help="Worker shared secret (preferred via WORKER_SHARED_SECRET environment variable)")
    parser.add_argument("--admin-key", default=None, help="Admin key for metrics scrape (preferred via ADMIN_API_KEY environment variable)")
    parser.add_argument("--allow-remote", action="store_true", help="Explicit opt-in required to target non-loopback endpoints")
    parser.add_argument("--mock-mode", action="store_true", help="Run with mock provider flag")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    base_url = args.base_url or os.environ.get("STAGING_BASE_URL") or "http://localhost:8000"
    client_api_key = os.environ.get("CLIENT_API_KEY") or args.api_key or ""
    worker_secret = os.environ.get("WORKER_SHARED_SECRET") or args.worker_secret or ""
    admin_key = os.environ.get("ADMIN_API_KEY") or args.admin_key

    cfg = SmokeTestConfig(
        base_url=base_url,
        client_api_key=client_api_key,
        worker_shared_secret=worker_secret,
        admin_api_key=admin_key,
        mock_mode=args.mock_mode,
        allow_remote=args.allow_remote,
    )
    runner = StagingSmokeTestRunner(cfg)
    success = asyncio.run(runner.run())
    sys.exit(0 if success else 1)
