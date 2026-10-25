"""
Integration Tests for Ingestion & Asset Delivery API (Milestone 4)
Location: tests/integration/test_generations_api.py
"""

import asyncio
from pathlib import Path
from PIL import Image
import io
import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.api.rate_limiter import rate_limiter
from src.core.config import settings
from src.core.security import generate_api_key, hash_api_key
from src.domain.models import ApiKeyTier, JobStatus
from src.infrastructure.models import ApiKeyModel, GeneratedAssetModel, GenerationJobModel
from src.infrastructure.repositories.job_sqlite import SqliteJobRepository
from src.infrastructure.storage.local_cas import LocalStorageCAS
from src.main import create_app, lifespan
from src.services.queue_worker import QueueWorker


async def register_test_client(
    session_factory: async_sessionmaker[AsyncSession],
    client_name: str = "Test-Client",
    is_active: bool = True,
    rate_limit_rpm: int = 10,
) -> str:
    """Helper creating a test API key in the database and returning raw key."""
    raw_key, _, _ = generate_api_key()
    salt = settings.API_KEY_SALT.get_secret_value()
    key_hash = hash_api_key(raw_key, salt)

    async with session_factory() as session:
        async with session.begin():
            api_key = ApiKeyModel(
                key_hash=key_hash,
                client_name=client_name,
                is_active=is_active,
                rate_limit_rpm=rate_limit_rpm,
                tier=ApiKeyTier.FREE.value,
            )
            session.add(api_key)
    return raw_key


@pytest.fixture(autouse=True)
def reset_limiter():
    """Resets in-memory rate limiter between tests."""
    rate_limiter.reset()
    yield
    rate_limiter.reset()


# -----------------------------------------------------------------------------
# 1. Authentication: Missing, Invalid, and Revoked Keys
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_auth_rejection_scenarios(client: AsyncClient, session_factory):
    # 1. Missing Key
    resp_missing = await client.post("/api/v1/images/generations", json={"prompt": "test"})
    assert resp_missing.status_code == 401
    assert "Missing API key" in resp_missing.json()["detail"]

    # 2. Invalid Key
    resp_invalid = await client.post(
        "/api/v1/images/generations",
        headers={"X-API-Key": "aarav_live_completely_fake_key_123"},
        json={"prompt": "test"},
    )
    assert resp_invalid.status_code == 401
    assert "Invalid API key" in resp_invalid.json()["detail"]

    # 3. Revoked Key
    revoked_raw_key = await register_test_client(session_factory, is_active=False)
    resp_revoked = await client.post(
        "/api/v1/images/generations",
        headers={"X-API-Key": revoked_raw_key},
        json={"prompt": "test"},
    )
    assert resp_revoked.status_code == 401
    assert "revoked" in resp_revoked.json()["detail"]


# -----------------------------------------------------------------------------
# 2. Rate Limiting: Enforcement and Recovery
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_rate_limiting_enforcement(client: AsyncClient, session_factory):
    raw_key = await register_test_client(session_factory, rate_limit_rpm=2)
    headers = {"X-API-Key": raw_key}
    payload = {"prompt": "A cute red panda"}

    # Request 1 & 2 succeed
    r1 = await client.post("/api/v1/images/generations", headers=headers, json=payload)
    assert r1.status_code == 202

    r2 = await client.post("/api/v1/images/generations", headers=headers, json=payload)
    assert r2.status_code == 202

    # Request 3 is blocked by rate limit
    r3 = await client.post("/api/v1/images/generations", headers=headers, json=payload)
    assert r3.status_code == 429
    assert "Retry-After" in r3.headers
    assert r3.json()["code"] == "RATE_LIMIT_EXCEEDED"


# -----------------------------------------------------------------------------
# 3. Request Validation: Local Caps and Multiple-of-64 Rules
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_generation_request_validation(client: AsyncClient, session_factory):
    raw_key = await register_test_client(session_factory)
    headers = {"X-API-Key": raw_key}

    # 1. Dimension exceeding 512 cap (hardware protection)
    r1 = await client.post(
        "/api/v1/images/generations",
        headers=headers,
        json={"prompt": "Test", "width": 768, "height": 768},
    )
    assert r1.status_code == 422
    assert "less than or equal to 512" in r1.json()["detail"]

    # 2. Non multiple of 64
    r2 = await client.post(
        "/api/v1/images/generations",
        headers=headers,
        json={"prompt": "Test", "width": 500, "height": 512},
    )
    assert r2.status_code == 422
    assert "multiple of 64" in r2.json()["detail"]

    # 3. Empty prompt
    r3 = await client.post(
        "/api/v1/images/generations",
        headers=headers,
        json={"prompt": ""},
    )
    assert r3.status_code == 422


# -----------------------------------------------------------------------------
# 4. Successful Ingestion & Location Header
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_generation_ingestion_success(client: AsyncClient, session_factory):
    raw_key = await register_test_client(session_factory)
    headers = {"X-API-Key": raw_key}
    payload = {
        "prompt": "An ethereal glowing jellyfish in a deep dark trench",
        "negative_prompt": "blurry, low quality",
        "width": 512,
        "height": 512,
        "steps": 4,
        "seed": 42,
    }

    resp = await client.post("/api/v1/images/generations", headers=headers, json=payload)
    assert resp.status_code == 202
    assert "Location" in resp.headers

    data = resp.json()
    assert data["status"] == "PENDING"
    assert data["prompt"] == payload["prompt"]
    assert data["width"] == 512
    assert data["height"] == 512
    assert resp.headers["Location"] == f"/api/v1/images/generations/{data['id']}"


# -----------------------------------------------------------------------------
# 5. Idempotent Duplicate Request
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_idempotency_duplicate_returns_same_job(client: AsyncClient, session_factory):
    raw_key = await register_test_client(session_factory)
    headers = {"X-API-Key": raw_key, "Idempotency-Key": "idemp-client-token-1"}
    payload = {"prompt": "A serene bamboo forest in mist"}

    # First submission
    r1 = await client.post("/api/v1/images/generations", headers=headers, json=payload)
    assert r1.status_code == 202
    job_id_1 = r1.json()["id"]

    # Identical resubmission
    r2 = await client.post("/api/v1/images/generations", headers=headers, json=payload)
    assert r2.status_code == 202
    assert r2.json()["id"] == job_id_1


# -----------------------------------------------------------------------------
# 6. Idempotent Conflicting Payload Returns HTTP 409
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_idempotency_conflicting_payload_raises_409(client: AsyncClient, session_factory):
    raw_key = await register_test_client(session_factory)
    headers = {"X-API-Key": raw_key, "Idempotency-Key": "idemp-conflict-token-99"}

    # Initial request
    r1 = await client.post(
        "/api/v1/images/generations",
        headers=headers,
        json={"prompt": "Golden sunrise"},
    )
    assert r1.status_code == 202

    # Second request with SAME idempotency key but DIFFERENT payload
    r2 = await client.post(
        "/api/v1/images/generations",
        headers=headers,
        json={"prompt": "Midnight starry sky"},
    )
    assert r2.status_code == 409
    assert r2.json()["code"] == "IDEMPOTENCY_CONFLICT"
    assert "idemp-conflict-token-99" in r2.json()["detail"]


# -----------------------------------------------------------------------------
# 7. Concurrent Idempotent Requests
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_concurrent_idempotent_requests(client: AsyncClient, session_factory):
    raw_key = await register_test_client(session_factory)
    headers = {"X-API-Key": raw_key, "Idempotency-Key": "concurrent-idemp-key-42"}
    payload = {"prompt": "A cyberpunk street vendor in neon rain"}

    # Submit 5 concurrent requests with identical key and payload
    tasks = [client.post("/api/v1/images/generations", headers=headers, json=payload) for _ in range(5)]
    results = await asyncio.gather(*tasks)

    # All should succeed with 202 and identical job ID
    job_ids = [r.json()["id"] for r in results]
    assert len(set(job_ids)) == 1
    for r in results:
        assert r.status_code == 202


# -----------------------------------------------------------------------------
# 8. Job Status Authorization & Cross-Key Access Denial
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_cross_key_job_access_denial(client: AsyncClient, session_factory):
    key_a = await register_test_client(session_factory, client_name="Client-A")
    key_b = await register_test_client(session_factory, client_name="Client-B")

    # Client A creates a job
    r_create = await client.post(
        "/api/v1/images/generations",
        headers={"X-API-Key": key_a},
        json={"prompt": "Secret design draft for Client A"},
    )
    job_id = r_create.json()["id"]

    # Client A can poll its own job
    r_a = await client.get(f"/api/v1/images/generations/{job_id}", headers={"X-API-Key": key_a})
    assert r_a.status_code == 200
    assert r_a.json()["id"] == job_id

    # Client B is forbidden from polling Client A's job!
    r_b = await client.get(f"/api/v1/images/generations/{job_id}", headers={"X-API-Key": key_b})
    assert r_b.status_code == 403
    assert r_b.json()["code"] == "FORBIDDEN"


# -----------------------------------------------------------------------------
# 9. Asset Delivery, Private Cache Headers, and Cross-Key Protection
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_asset_delivery_and_private_caching(client: AsyncClient, session_factory, tmp_path: Path):
    key_a = await register_test_client(session_factory, client_name="Client-A")
    key_b = await register_test_client(session_factory, client_name="Client-B")

    # 1. Enqueue job
    r_post = await client.post(
        "/api/v1/images/generations",
        headers={"X-API-Key": key_a},
        json={"prompt": "A vibrant digital mandala"},
    )
    job_id = r_post.json()["id"]

    # Asset is not available yet while PENDING
    r_pending_asset = await client.get(
        f"/api/v1/images/generations/{job_id}/content",
        headers={"X-API-Key": key_a},
    )
    assert r_pending_asset.status_code == 404

    # 2. Run queue worker to complete job and store in CAS
    repo = SqliteJobRepository(session_factory)
    storage = LocalStorageCAS(base_path=tmp_path)
    # Monkeypatch cas_storage used by route handler to use tmp_path
    from src.api.v1 import generations
    generations.cas_storage = storage

    worker = QueueWorker(repository=repo, storage=storage, worker_id="test-e2e-worker")
    processed = await worker.process_one_job()
    assert processed is True

    # 3. Client A downloads asset
    r_content = await client.get(
        f"/api/v1/images/generations/{job_id}/content",
        headers={"X-API-Key": key_a},
    )
    assert r_content.status_code == 200
    assert r_content.headers["Content-Type"] == "image/png"
    # Golden check: safe private caching headers (no public caching on private assets)
    assert "private" in r_content.headers["Cache-Control"]
    assert "no-transform" in r_content.headers["Cache-Control"]
    assert "ETag" in r_content.headers
    assert r_content.headers["X-Content-Type-Options"] == "nosniff"

    # Verify PNG bytes validity
    img = Image.open(io.BytesIO(r_content.content))
    assert img.format == "PNG"
    assert img.size == (512, 512)

    # 4. Client B attempts to download Client A's asset -> 403 Forbidden!
    r_b_content = await client.get(
        f"/api/v1/images/generations/{job_id}/content",
        headers={"X-API-Key": key_b},
    )
    assert r_b_content.status_code == 403
    assert r_b_content.json()["code"] == "FORBIDDEN"


# -----------------------------------------------------------------------------
# 10. End-to-End: Ingest -> Worker -> Status Polling -> Content Retrieval
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_end_to_end_generation_lifecycle(client: AsyncClient, session_factory, tmp_path: Path):
    raw_key = await register_test_client(session_factory)
    headers = {"X-API-Key": raw_key}

    # 1. Ingest
    r_create = await client.post(
        "/api/v1/images/generations",
        headers=headers,
        json={"prompt": "Majestic snowy mountain peak"},
    )
    assert r_create.status_code == 202
    job_id = r_create.json()["id"]

    # 2. Background worker processes
    repo = SqliteJobRepository(session_factory)
    storage = LocalStorageCAS(base_path=tmp_path)
    from src.api.v1 import generations
    generations.cas_storage = storage

    worker = QueueWorker(repository=repo, storage=storage)
    await worker.process_one_job()

    # 3. Poll status
    r_status = await client.get(f"/api/v1/images/generations/{job_id}", headers=headers)
    assert r_status.status_code == 200
    status_data = r_status.json()
    assert status_data["status"] == "COMPLETED"
    assert status_data["provider_used"] == "mock"
    assert status_data["is_synthetic"] is True  # Synthetic watermark labeled!
    assert "content" in status_data["links"]

    # 4. Retrieve content
    content_url = status_data["links"]["content"]
    r_img = await client.get(content_url, headers=headers)
    assert r_img.status_code == 200
    assert r_img.headers["Content-Type"] == "image/png"


# -----------------------------------------------------------------------------
# 11. Application Lifespan Starts and Stops Worker and Sweeper Cleanly
# -----------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_application_lifespan_lifecycle():
    app = create_app()

    # Enable embedded worker for lifespan test
    from src.core.config import settings
    original_val = settings.RUN_EMBEDDED_WORKER
    settings.RUN_EMBEDDED_WORKER = True

    try:
        async with lifespan(app):
            # Assert background worker and sweeper are attached to app.state
            assert hasattr(app.state, "worker")
            assert hasattr(app.state, "sweeper")
            assert app.state.worker is not None
            assert app.state.sweeper is not None

        # After exiting lifespan context, worker and sweeper should have stopped
        assert app.state.worker._stop_event.is_set()
        assert app.state.sweeper._stop_event.is_set()
    finally:
        settings.RUN_EMBEDDED_WORKER = original_val
