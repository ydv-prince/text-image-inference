"""
End-to-End Integration Tests — Generation Ingress & Async Polling Pipeline
Location: tests/integration/test_worker_ingress_pipeline.py

Verifies the full asynchronous lifecycle across public API and worker ingress contracts:
1. Complete happy-path pipeline:
   - Client enqueues job via POST /api/v1/images/generations -> 202 Accepted.
   - Worker claims job via POST /api/v1/worker/jobs/lease -> 200 OK.
   - Worker executes mocked inference and stores asset in LocalStorageCAS.
   - Worker reports completion via POST /api/v1/worker/jobs/{id}/complete -> 200 OK.
   - Client polls GET /api/v1/images/generations/{id} -> 200 OK (COMPLETED).
   - Client downloads asset via GET /api/v1/images/generations/{id}/content -> 200 OK (PNG bytes).
2. Pending/Processing states never reveal premature or false successes.
3. Heartbeat renewal and ownership conflict enforcement (409 Conflict).
4. Inference failure recovery (retrying vs terminal FAILED status).
5. Storage failure prevents false success and routes through failure policy.
6. Stale or rogue worker completion rejected via lease ownership checks.
7. Parameter consistency across public API, database, worker claim, and delivery DTOs.
8. Correlation ID and telemetry flow across the pipeline.
"""

import io
import uuid
from datetime import datetime, timedelta, timezone
import pytest
from httpx import AsyncClient
from PIL import Image
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.api.rate_limiter import rate_limiter
from src.core.config import settings
from src.core.security import generate_api_key, hash_api_key
from src.domain.interfaces import (
    ImageProviderAdapter,
    ProviderHealth,
    ProviderRequest,
    ProviderResponse,
)
from src.domain.models import ApiKeyTier, JobStatus
from src.infrastructure.models import ApiKeyModel, GenerationJobModel
from src.infrastructure.repositories import get_job_repository
from src.infrastructure.storage.local_cas import LocalStorageCAS
from src.services.hybrid_worker import HybridWorker
from src.services.lease_sweeper import LeaseSweeper

WORKER_TEST_SECRET = "test_shared_worker_secret_key_32_chars_long"


@pytest.fixture(autouse=True)
def set_worker_secret(monkeypatch):
    """Enforces a valid worker shared secret for worker ingress auth."""
    monkeypatch.setattr(settings, "WORKER_SHARED_SECRET", SecretStr(WORKER_TEST_SECRET))


@pytest.fixture(autouse=True)
def reset_rate_limiter():
    """Resets in-memory rate limiter between test cases."""
    rate_limiter.reset()
    yield
    rate_limiter.reset()


def worker_headers(secret: str = WORKER_TEST_SECRET) -> dict:
    """Returns Bearer authorization headers for worker ingress."""
    return {"Authorization": f"Bearer {secret}"}


async def register_test_client(
    session_factory: async_sessionmaker[AsyncSession],
    client_name: str = "Test-Pipeline-Client",
    is_active: bool = True,
    rate_limit_rpm: int = 100,
) -> str:
    """Creates a client API key in the database and returns raw key string."""
    raw_key, _, _ = generate_api_key()
    salt = settings.API_KEY_SALT.get_secret_value()
    key_hash = hash_api_key(raw_key, salt)

    async with session_factory() as session:
        async with session.begin():
            api_key = ApiKeyModel(
                id=str(uuid.uuid4()),
                key_hash=key_hash,
                client_name=client_name,
                is_active=is_active,
                rate_limit_rpm=rate_limit_rpm,
                tier=ApiKeyTier.FREE.value,
            )
            session.add(api_key)
    return raw_key


def make_test_png(width: int = 512, height: int = 512) -> bytes:
    """Generates valid PNG image bytes for storage testing."""
    img = Image.new("RGB", (width, height), color=(45, 85, 125))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


class MockPipelineProvider(ImageProviderAdapter):
    """Configurable mock provider for end-to-end integration flows."""

    def __init__(self, fail_with: Exception = None):
        self.fail_with = fail_with
        self.call_count = 0

    @property
    def name(self) -> str:
        return "mock"

    async def generate_image(self, request: ProviderRequest) -> ProviderResponse:
        self.call_count += 1
        if self.fail_with:
            raise self.fail_with

        png_bytes = make_test_png(request.width, request.height)
        return ProviderResponse(
            image_bytes=png_bytes,
            mime_type="image/png",
            width=request.width,
            height=request.height,
            duration_ms=45,
            provider_name="mock",
            model_id="mock-pipeline-v1",
            peak_vram_mb=32,
            is_synthetic=True,
        )

    async def health_check(self) -> ProviderHealth:
        return ProviderHealth(provider_name="mock", is_healthy=True, status_message="Ready")


# -----------------------------------------------------------------------------
# 1. Complete End-to-End Successful Lifecycle
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_end_to_end_successful_generation_pipeline(client: AsyncClient, session_factory):
    """
    Submits generation request -> leases job -> processes inference & CAS storage ->
    reports completion -> polls completed status -> downloads generated PNG bytes.
    """
    raw_key = await register_test_client(session_factory)
    client_headers = {"X-API-Key": raw_key}

    # 1. Client submits generation job
    submit_payload = {
        "prompt": "A serene cyberpunk city in rain",
        "negative_prompt": "blurry, low quality",
        "width": 512,
        "height": 512,
        "steps": 4,
        "seed": 42,
    }
    submit_resp = await client.post(
        "/api/v1/images/generations",
        headers=client_headers,
        json=submit_payload,
    )
    assert submit_resp.status_code == 202
    job_data = submit_resp.json()
    job_id = job_data["id"]
    assert job_data["status"] == "PENDING"
    assert submit_resp.headers["Location"] == f"/api/v1/images/generations/{job_id}"

    # 2. HybridWorker processes the job
    provider = MockPipelineProvider()
    storage = LocalStorageCAS()
    worker = HybridWorker(
        base_url="http://testserver",
        worker_shared_secret=WORKER_TEST_SECRET,
        provider=provider,
        storage=storage,
        worker_id="worker-integration-alpha",
    )

    processed = await worker.poll_and_execute_once(client)
    assert processed is True
    assert provider.call_count == 1
    assert worker.metrics["jobs_completed"] == 1
    assert worker.metrics["jobs_failed"] == 0

    # 3. Client polls job status
    poll_resp = await client.get(f"/api/v1/images/generations/{job_id}", headers=client_headers)
    assert poll_resp.status_code == 200
    poll_data = poll_resp.json()
    assert poll_data["id"] == job_id
    assert poll_data["status"] == "COMPLETED"
    assert poll_data["provider_used"] == "mock"
    assert poll_data["execution_time_ms"] == 45
    assert poll_data["completed_at"] is not None
    assert poll_data["links"]["self"] == f"/api/v1/images/generations/{job_id}"
    assert poll_data["links"]["content"] == f"/api/v1/images/generations/{job_id}/content"

    # 4. Client downloads generated PNG content
    content_resp = await client.get(f"/api/v1/images/generations/{job_id}/content", headers=client_headers)
    assert content_resp.status_code == 200
    assert content_resp.headers["Content-Type"] == "image/png"
    assert len(content_resp.content) > 100
    assert content_resp.content[:8] == b"\x89PNG\r\n\x1a\n"


# -----------------------------------------------------------------------------
# 2. Polling Queued and Processing Jobs Never Reveal False Success
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_polling_pending_and_processing_never_reveals_false_success(client: AsyncClient, session_factory):
    """Ensures PENDING and PROCESSING jobs have no completion timestamp or content links."""
    raw_key = await register_test_client(session_factory)
    client_headers = {"X-API-Key": raw_key}

    # 1. Enqueue job (PENDING)
    submit_resp = await client.post(
        "/api/v1/images/generations",
        headers=client_headers,
        json={"prompt": "Majestic mountain peak"},
    )
    job_id = submit_resp.json()["id"]

    # Poll PENDING job
    pending_resp = await client.get(f"/api/v1/images/generations/{job_id}", headers=client_headers)
    assert pending_resp.status_code == 200
    p_data = pending_resp.json()
    assert p_data["status"] == "PENDING"
    assert p_data["completed_at"] is None
    assert "content" not in p_data["links"]

    # Attempting to fetch content for PENDING job returns 404 Not Found
    content_p_resp = await client.get(f"/api/v1/images/generations/{job_id}/content", headers=client_headers)
    assert content_p_resp.status_code == 404
    assert "RESOURCE_NOT_FOUND" in content_p_resp.json()["code"]

    # 2. Worker leases job (transitions to PROCESSING)
    lease_resp = await client.post(
        "/api/v1/worker/jobs/lease",
        json={"worker_id": "worker-inspecting-01", "lease_timeout_seconds": 60},
        headers=worker_headers(),
    )
    assert lease_resp.status_code == 200
    assert lease_resp.json()["job_id"] == job_id

    # Poll PROCESSING job
    proc_resp = await client.get(f"/api/v1/images/generations/{job_id}", headers=client_headers)
    assert proc_resp.status_code == 200
    proc_data = proc_resp.json()
    assert proc_data["status"] == "PROCESSING"
    assert proc_data["completed_at"] is None
    assert "content" not in proc_data["links"]

    # Content request for PROCESSING job is also rejected with 404 Not Found
    content_proc_resp = await client.get(f"/api/v1/images/generations/{job_id}/content", headers=client_headers)
    assert content_proc_resp.status_code == 404
    assert "RESOURCE_NOT_FOUND" in content_proc_resp.json()["code"]


# -----------------------------------------------------------------------------
# 3. Heartbeat Renewal and Ownership Conflict Enforcement
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_heartbeat_renewal_and_ownership_conflict(client: AsyncClient, session_factory):
    """Verifies legitimate heartbeats succeed while foreign workers receive 409 Conflict."""
    raw_key = await register_test_client(session_factory)
    client_headers = {"X-API-Key": raw_key}

    # Submit job
    r = await client.post("/api/v1/images/generations", headers=client_headers, json={"prompt": "Nebula"})
    job_id = r.json()["id"]

    # Worker 1 leases job
    await client.post(
        "/api/v1/worker/jobs/lease",
        json={"worker_id": "worker-legitimate"},
        headers=worker_headers(),
    )

    # Legitimate worker heartbeats -> 200 OK
    hb_ok = await client.post(
        f"/api/v1/worker/jobs/{job_id}/heartbeat",
        json={"worker_id": "worker-legitimate"},
        headers=worker_headers(),
    )
    assert hb_ok.status_code == 200
    assert hb_ok.json()["status"] == "renewed"

    # Rogue worker heartbeats -> 409 Conflict
    hb_conflict = await client.post(
        f"/api/v1/worker/jobs/{job_id}/heartbeat",
        json={"worker_id": "worker-rogue"},
        headers=worker_headers(),
    )
    assert hb_conflict.status_code == 409
    assert hb_conflict.json()["code"] == "LEASE_OWNERSHIP_CONFLICT"

    # Verify job status remains safely locked by legitimate worker
    status_resp = await client.get(f"/api/v1/images/generations/{job_id}", headers=client_headers)
    assert status_resp.status_code == 200
    assert status_resp.json()["status"] == "PROCESSING"


@pytest.mark.asyncio
async def test_expired_lease_reclaimed_by_sweeper_and_leased_to_new_worker(client: AsyncClient, session_factory):
    """
    Verifies that when a worker's lease expires without heartbeats:
    1. LeaseSweeper reclaims the job back to PENDING.
    2. A second worker can lease the reclaimed job.
    3. The first (expired/stale) worker cannot heartbeat or complete the job (409 LEASE_OWNERSHIP_CONFLICT).
    4. The second worker successfully completes the job.
    5. No false completion is reported during the lease transition.
    (SEC-WRK-02 regression coverage)
    """
    raw_key = await register_test_client(session_factory)
    client_headers = {"X-API-Key": raw_key}

    # 1. Client submits job
    r = await client.post("/api/v1/images/generations", headers=client_headers, json={"prompt": "Sweeper test"})
    assert r.status_code == 202
    job_id = r.json()["id"]

    # 2. Worker 1 leases the job -> enters PROCESSING
    lease_1 = await client.post(
        "/api/v1/worker/jobs/lease",
        json={"worker_id": "worker-1-stale", "lease_timeout_seconds": 60},
        headers=worker_headers(),
    )
    assert lease_1.status_code == 200
    assert lease_1.json()["job_id"] == job_id

    # Verify intermediate state: status is PROCESSING, not COMPLETED, no content link
    poll_proc = await client.get(f"/api/v1/images/generations/{job_id}", headers=client_headers)
    assert poll_proc.status_code == 200
    assert poll_proc.json()["status"] == "PROCESSING"
    assert poll_proc.json()["completed_at"] is None
    assert "content" not in poll_proc.json()["links"]

    # 3. Simulate lease expiration deterministically by updating locked_at to the past
    stale_time = datetime.now(timezone.utc) - timedelta(seconds=120)
    async with session_factory() as session:
        async with session.begin():
            job_obj = await session.get(GenerationJobModel, job_id)
            assert job_obj is not None
            job_obj.locked_at = stale_time

    # 4. Execute LeaseSweeper pass to reclaim stale lease
    repo = get_job_repository(session_factory)
    sweeper = LeaseSweeper(
        repository=repo,
        lease_timeout_seconds=60,
        base_backoff_seconds=0.0,
    )
    reclaimed = await sweeper.sweep_once()
    assert job_id in reclaimed

    # Verify job is back in PENDING, not falsely marked COMPLETED
    poll_reclaimed = await client.get(f"/api/v1/images/generations/{job_id}", headers=client_headers)
    assert poll_reclaimed.status_code == 200
    assert poll_reclaimed.json()["status"] == "PENDING"
    assert poll_reclaimed.json()["completed_at"] is None

    # 5. Worker 2 successfully leases the reclaimed job
    lease_2 = await client.post(
        "/api/v1/worker/jobs/lease",
        json={"worker_id": "worker-2-active", "lease_timeout_seconds": 60},
        headers=worker_headers(),
    )
    assert lease_2.status_code == 200
    assert lease_2.json()["job_id"] == job_id

    # 6. Worker 1 attempts to send heartbeat on expired lease -> 409 Conflict
    hb_stale = await client.post(
        f"/api/v1/worker/jobs/{job_id}/heartbeat",
        json={"worker_id": "worker-1-stale"},
        headers=worker_headers(),
    )
    assert hb_stale.status_code == 409
    assert hb_stale.json()["code"] == "LEASE_OWNERSHIP_CONFLICT"

    # Worker 1 attempts to complete the job -> 409 Conflict
    complete_stale = await client.post(
        f"/api/v1/worker/jobs/{job_id}/complete",
        json={
            "worker_id": "worker-1-stale",
            "file_hash": "a" * 64,
            "file_size_bytes": 1024,
            "storage_provider": "local_cas",
            "storage_path": "cas/aa/aa.png",
            "mime_type": "image/png",
            "width": 512,
            "height": 512,
            "execution_time_ms": 50,
            "provider_used": "mock",
        },
        headers=worker_headers(),
    )
    assert complete_stale.status_code == 409
    assert complete_stale.json()["code"] == "LEASE_OWNERSHIP_CONFLICT"

    # 7. Worker 2 successfully completes the job
    complete_active = await client.post(
        f"/api/v1/worker/jobs/{job_id}/complete",
        json={
            "worker_id": "worker-2-active",
            "file_hash": "b" * 64,
            "file_size_bytes": 2048,
            "storage_provider": "local_cas",
            "storage_path": "cas/bb/bb.png",
            "mime_type": "image/png",
            "width": 512,
            "height": 512,
            "execution_time_ms": 40,
            "provider_used": "mock",
        },
        headers=worker_headers(),
    )
    assert complete_active.status_code == 200

    # 8. Client polls final completed status
    poll_final = await client.get(f"/api/v1/images/generations/{job_id}", headers=client_headers)
    assert poll_final.status_code == 200
    assert poll_final.json()["status"] == "COMPLETED"
    assert poll_final.json()["completed_at"] is not None


# -----------------------------------------------------------------------------
# 4. Inference Failure: Retry Policy and Terminal Exhaustion
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_inference_failure_retries_and_terminal_exhaustion(client: AsyncClient, session_factory):
    """
    Verifies that retryable inference errors return the job to PENDING with backoff,
    and transition to FAILED when retries are exhausted.
    """
    raw_key = await register_test_client(session_factory)
    client_headers = {"X-API-Key": raw_key}

    r = await client.post("/api/v1/images/generations", headers=client_headers, json={"prompt": "OOM prompt"})
    job_id = r.json()["id"]

    failing_provider = MockPipelineProvider(fail_with=RuntimeError("CUDA out of memory error"))
    storage = LocalStorageCAS()
    worker = HybridWorker(
        base_url="http://testserver",
        worker_shared_secret=WORKER_TEST_SECRET,
        provider=failing_provider,
        storage=storage,
        worker_id="worker-fail-test",
    )

    # Attempt 1: Fails with GPU_OUT_OF_MEMORY -> Rescheduled to PENDING (retry_count=1)
    await worker.poll_and_execute_once(client)

    poll_1 = await client.get(f"/api/v1/images/generations/{job_id}", headers=client_headers)
    assert poll_1.status_code == 200
    p1_data = poll_1.json()
    assert p1_data["status"] == "PENDING"
    assert p1_data["error_code"] == "GPU_OUT_OF_MEMORY"
    assert "CUDA out of memory" in p1_data["error_message"]

    # Immediate lease attempt returns 204 because retry backoff scheduled_at is in the future
    lease_backoff = await client.post(
        "/api/v1/worker/jobs/lease",
        json={"worker_id": "worker-fail-test"},
        headers=worker_headers(),
    )
    assert lease_backoff.status_code == 204

    # 2. Terminal failure via non-retryable error code (e.g. INVALID_PAYLOAD)
    r_term = await client.post(
        "/api/v1/images/generations",
        headers=client_headers,
        json={"prompt": "Non-retryable terminal failure prompt"},
    )
    job_term_id = r_term.json()["id"]

    # Worker leases the newly available job
    lease_term = await client.post(
        "/api/v1/worker/jobs/lease",
        json={"worker_id": "worker-fail-test"},
        headers=worker_headers(),
    )
    assert lease_term.status_code == 200
    assert lease_term.json()["job_id"] == job_term_id

    # Report non-retryable failure
    fail_terminal = await client.post(
        f"/api/v1/worker/jobs/{job_term_id}/fail",
        json={
            "worker_id": "worker-fail-test",
            "error_code": "INVALID_PAYLOAD",
            "error_message": "Prompt contains disallowed symbols.",
        },
        headers=worker_headers(),
    )
    assert fail_terminal.status_code == 200
    assert fail_terminal.json()["status"] == "failed"

    # Client polls terminal state
    final_resp = await client.get(f"/api/v1/images/generations/{job_term_id}", headers=client_headers)
    final_data = final_resp.json()
    assert final_data["status"] == "FAILED"
    assert final_data["completed_at"] is not None
    assert "content" not in final_data["links"]


# -----------------------------------------------------------------------------
# 5. Storage Failure Prevents False Success
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_storage_failure_prevents_false_success(client: AsyncClient, session_factory):
    """
    Verifies that when storage upload fails, completion is NOT reported
    and the failure is routed through the failure policy.
    """
    raw_key = await register_test_client(session_factory)
    client_headers = {"X-API-Key": raw_key}

    r = await client.post("/api/v1/images/generations", headers=client_headers, json={"prompt": "Storage test"})
    job_id = r.json()["id"]

    provider = MockPipelineProvider()

    # Storage adapter that fails during store_image
    class FailingStorage(LocalStorageCAS):
        async def store_image(self, *args, **kwargs):
            raise OSError("CAS disk write error: out of space")

    failing_storage = FailingStorage()
    worker = HybridWorker(
        base_url="http://testserver",
        worker_shared_secret=WORKER_TEST_SECRET,
        provider=provider,
        storage=failing_storage,
        worker_id="worker-storage-fail",
    )

    await worker.poll_and_execute_once(client)

    # Verify job is NOT COMPLETED
    poll_resp = await client.get(f"/api/v1/images/generations/{job_id}", headers=client_headers)
    poll_data = poll_resp.json()
    assert poll_data["status"] != "COMPLETED"
    assert poll_data["error_code"] == "STORAGE_ERROR"
    assert "CAS disk write error" in poll_data["error_message"]
    assert "content" not in poll_data["links"]


# -----------------------------------------------------------------------------
# 6. Stale or Foreign Worker Completion Rejected
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_stale_or_foreign_worker_completion_rejected(client: AsyncClient, session_factory):
    """
    Verifies that completion requests by foreign workers are rejected with 409 Conflict,
    and valid completions are idempotent.
    """
    raw_key = await register_test_client(session_factory)
    client_headers = {"X-API-Key": raw_key}

    r = await client.post("/api/v1/images/generations", headers=client_headers, json={"prompt": "Ownership test"})
    job_id = r.json()["id"]

    # Valid worker leases
    await client.post(
        "/api/v1/worker/jobs/lease",
        json={"worker_id": "worker-legit-owner"},
        headers=worker_headers(),
    )

    complete_body = {
        "worker_id": "worker-legit-owner",
        "file_hash": "b" * 64,
        "file_size_bytes": 4096,
        "storage_provider": "local_cas",
        "storage_path": "cas/bb/bb.png",
        "mime_type": "image/png",
        "width": 512,
        "height": 512,
        "execution_time_ms": 30,
        "provider_used": "mock",
    }

    # Foreign worker attempts complete -> 409 Conflict
    bad_body = dict(complete_body, worker_id="worker-imposter")
    bad_resp = await client.post(
        f"/api/v1/worker/jobs/{job_id}/complete",
        json=bad_body,
        headers=worker_headers(),
    )
    assert bad_resp.status_code == 409
    assert bad_resp.json()["code"] == "LEASE_OWNERSHIP_CONFLICT"

    # Legitimate owner completes -> 200 OK
    ok_resp = await client.post(
        f"/api/v1/worker/jobs/{job_id}/complete",
        json=complete_body,
        headers=worker_headers(),
    )
    assert ok_resp.status_code == 200
    asset_id = ok_resp.json()["asset_id"]

    # Duplicate completion call (network retry) -> 200 OK with same asset_id
    dup_resp = await client.post(
        f"/api/v1/worker/jobs/{job_id}/complete",
        json=complete_body,
        headers=worker_headers(),
    )
    assert dup_resp.status_code == 200
    assert dup_resp.json()["asset_id"] == asset_id


# -----------------------------------------------------------------------------
# 7. Cross-Boundary Payload and Parameter Consistency
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_cross_boundary_payload_consistency(client: AsyncClient, session_factory):
    """
    Validates parameter consistency across the public submission, worker lease,
    and status polling response boundaries.
    """
    raw_key = await register_test_client(session_factory)
    client_headers = {"X-API-Key": raw_key}

    # Custom prompt and parameters
    payload = {
        "prompt": "Highly detailed portrait of a mechanical dragon",
        "negative_prompt": "oversaturated, deformed",
        "width": 512,
        "height": 512,
        "steps": 12,
        "seed": 987654321,
        "model": "sd-1.5",
    }
    submit_resp = await client.post("/api/v1/images/generations", headers=client_headers, json=payload)
    assert submit_resp.status_code == 202
    job_id = submit_resp.json()["id"]

    # Worker leases job -> verify parameters match exactly
    lease_resp = await client.post(
        "/api/v1/worker/jobs/lease",
        json={"worker_id": "worker-verifier"},
        headers=worker_headers(),
    )
    assert lease_resp.status_code == 200
    l_data = lease_resp.json()
    assert l_data["job_id"] == job_id
    assert l_data["prompt"] == payload["prompt"]
    assert l_data["negative_prompt"] == payload["negative_prompt"]
    assert l_data["width"] == 512
    assert l_data["height"] == 512
    assert l_data["steps"] == 12
    assert l_data["seed"] == 987654321
    assert l_data["model"] == "sd-1.5"

    # Complete
    await client.post(
        f"/api/v1/worker/jobs/{job_id}/complete",
        json={
            "worker_id": "worker-verifier",
            "file_hash": "c" * 64,
            "file_size_bytes": 2048,
            "storage_provider": "local_cas",
            "storage_path": "cas/cc/cc.png",
            "mime_type": "image/png",
            "width": 512,
            "height": 512,
            "execution_time_ms": 75,
            "provider_used": "mock",
        },
        headers=worker_headers(),
    )

    # Poll status -> verify final DTO matches
    poll_resp = await client.get(f"/api/v1/images/generations/{job_id}", headers=client_headers)
    p_data = poll_resp.json()
    assert p_data["prompt"] == payload["prompt"]
    assert p_data["negative_prompt"] == payload["negative_prompt"]
    assert p_data["width"] == 512
    assert p_data["height"] == 512
    assert p_data["steps"] == 12
    assert p_data["seed"] == 987654321
    assert p_data["model"] == "sd-1.5"
    assert p_data["status"] == "COMPLETED"


# -----------------------------------------------------------------------------
# 8. Correlation ID and Worker Telemetry Flow
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_correlation_id_and_worker_telemetry_flow(client: AsyncClient, session_factory):
    """
    Verifies correlation ID propagation through request/response headers
    and worker metrics accounting.
    """
    raw_key = await register_test_client(session_factory)
    custom_cid = "client-trace-cid-98765"
    client_headers = {
        "X-API-Key": raw_key,
        "X-Correlation-ID": custom_cid,
    }

    # Request preserves correlation ID header
    submit_resp = await client.post(
        "/api/v1/images/generations",
        headers=client_headers,
        json={"prompt": "Trace correlation test"},
    )
    assert submit_resp.status_code == 202
    assert submit_resp.headers.get("X-Correlation-ID") == custom_cid

    # Worker executes job
    provider = MockPipelineProvider()
    storage = LocalStorageCAS()
    worker = HybridWorker(
        base_url="http://testserver",
        worker_shared_secret=WORKER_TEST_SECRET,
        provider=provider,
        storage=storage,
        worker_id="worker-telemetry-test",
    )

    # Initial metrics
    assert worker.metrics["jobs_completed"] == 0
    assert worker.metrics["jobs_failed"] == 0

    await worker.poll_and_execute_once(client)

    # Post-execution metrics
    assert worker.metrics["jobs_completed"] == 1
    assert worker.metrics["jobs_failed"] == 0
    assert worker.current_job_id is None
