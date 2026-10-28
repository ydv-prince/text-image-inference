"""
Integration Tests — API Contract, Idempotency, Tenant Isolation, and Input Hardening
Location: tests/integration/test_api_contract_and_isolation.py

Verifies:
1. Idempotency correctness:
   - Identical payload + same key -> returns existing job (HTTP 202/200).
   - Conflicting payload + same key -> returns HTTP 409 Conflict (IDEMPOTENCY_CONFLICT).
   - Concurrent submissions with same key -> exactly 1 job created, all callers receive same job.
   - Different clients sharing same key -> isolated tenant namespaces, both succeed independently.
   - Empty, whitespace, oversized (>128 chars), or control characters in Idempotency-Key -> HTTP 422.
2. Tenant & asset isolation:
   - Client A cannot retrieve Client B's job (HTTP 403 Forbidden).
   - Client A cannot download Client B's generated image (HTTP 403 Forbidden).
   - Missing, invalid, and revoked API keys -> HTTP 401 Unauthorized.
   - Guessed/random job IDs -> HTTP 404 Not Found (RESOURCE_NOT_FOUND).
   - Worker bearer credentials rejected on client endpoints (HTTP 401).
   - Client API keys rejected on worker endpoints (HTTP 401).
   - Error responses conform strictly to RFC 7807 Problem Details and never expose sensitive internals.
3. Input boundary validation:
   - Whitespace/empty prompt, prompt > 2000 chars -> HTTP 422.
   - Negative prompt > 1000 chars -> HTTP 422.
   - Non-multiple of 64 dimensions, dimensions < 256 or > 512 -> HTTP 422.
   - Steps < 1 or > 25 -> HTTP 422.
   - Seed < -1 -> HTTP 422.
   - Empty/whitespace model or provider -> HTTP 422.
4. Error contracts & rate limiting:
   - Exceeded RPM quota -> HTTP 429 with 'Retry-After' header and RATE_LIMIT_EXCEEDED code.
   - All error responses contain RFC 7807 problem details with correlation IDs.
"""

import asyncio
import io
import uuid
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
from src.domain.models import ApiKeyTier
from src.infrastructure.models import ApiKeyModel
from src.infrastructure.storage.local_cas import LocalStorageCAS
from src.services.hybrid_worker import HybridWorker

WORKER_SHARED_SECRET = "test_worker_auth_secret_32_characters_long"


@pytest.fixture(autouse=True)
def setup_worker_secret(monkeypatch):
    monkeypatch.setattr(settings, "WORKER_SHARED_SECRET", SecretStr(WORKER_SHARED_SECRET))


@pytest.fixture(autouse=True)
def reset_rate_limiter_state():
    rate_limiter.reset()
    yield
    rate_limiter.reset()


async def register_client(
    session_factory: async_sessionmaker[AsyncSession],
    client_name: str,
    rate_limit_rpm: int = 100,
    is_active: bool = True,
) -> str:
    """Helper to insert an API key into the test database and return the raw API key."""
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


def make_png_buffer(width: int = 512, height: int = 512) -> bytes:
    img = Image.new("RGB", (width, height), color=(80, 120, 160))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


class MockIsolationProvider(ImageProviderAdapter):
    @property
    def name(self) -> str:
        return "mock"

    async def generate_image(self, request: ProviderRequest) -> ProviderResponse:
        return ProviderResponse(
            image_bytes=make_png_buffer(request.width, request.height),
            mime_type="image/png",
            width=request.width,
            height=request.height,
            duration_ms=50,
            provider_name="mock",
            is_synthetic=True,
        )

    async def health_check(self) -> ProviderHealth:
        return ProviderHealth(provider_name="mock", is_healthy=True, status_message="Ready")


# =============================================================================
# PHASE 2: IDEMPOTENCY CORRECTNESS
# =============================================================================

@pytest.mark.asyncio
async def test_idempotency_identical_payload_returns_same_job(client: AsyncClient, session_factory):
    """Repeating a request with the same idempotency key and identical payload returns the same job."""
    raw_key = await register_client(session_factory, "Client-Idemp-1")
    headers = {"X-API-Key": raw_key, "Idempotency-Key": "unique-order-key-101"}
    payload = {
        "prompt": "Cyberpunk street in rain",
        "width": 512,
        "height": 512,
        "steps": 4,
        "seed": 42,
    }

    # Initial request -> 202 Accepted
    resp1 = await client.post("/api/v1/images/generations", headers=headers, json=payload)
    assert resp1.status_code == 202
    job_id1 = resp1.json()["id"]

    # Identical replay -> 202 Accepted with same job ID
    resp2 = await client.post("/api/v1/images/generations", headers=headers, json=payload)
    assert resp2.status_code == 202
    job_id2 = resp2.json()["id"]

    assert job_id1 == job_id2
    assert resp2.headers["Location"] == f"/api/v1/images/generations/{job_id1}"


@pytest.mark.asyncio
async def test_idempotency_different_payload_returns_409_conflict(client: AsyncClient, session_factory):
    """Reusing the same idempotency key with a materially different payload returns HTTP 409 Conflict."""
    raw_key = await register_client(session_factory, "Client-Idemp-2")
    headers = {"X-API-Key": raw_key, "Idempotency-Key": "conflict-test-key-202"}

    payload1 = {"prompt": "Initial prompt", "width": 512, "height": 512, "steps": 4}
    resp1 = await client.post("/api/v1/images/generations", headers=headers, json=payload1)
    assert resp1.status_code == 202

    # Modified prompt
    payload2 = {"prompt": "Materially different prompt", "width": 512, "height": 512, "steps": 4}
    resp2 = await client.post("/api/v1/images/generations", headers=headers, json=payload2)
    assert resp2.status_code == 409
    data = resp2.json()
    assert data["code"] == "IDEMPOTENCY_CONFLICT"
    assert "previously used with a different request payload" in data["detail"]


@pytest.mark.asyncio
async def test_idempotency_concurrent_identical_requests_creates_single_job(client: AsyncClient, session_factory):
    """Concurrent identical submissions using the same key create exactly 1 job in the database."""
    raw_key = await register_client(session_factory, "Client-Concurrent")
    headers = {"X-API-Key": raw_key, "Idempotency-Key": "concurrent-race-key-303"}
    payload = {"prompt": "High speed race car", "width": 512, "height": 512, "steps": 5}

    # Dispatch 5 simultaneous concurrent requests
    responses = await asyncio.gather(
        client.post("/api/v1/images/generations", headers=headers, json=payload),
        client.post("/api/v1/images/generations", headers=headers, json=payload),
        client.post("/api/v1/images/generations", headers=headers, json=payload),
        client.post("/api/v1/images/generations", headers=headers, json=payload),
        client.post("/api/v1/images/generations", headers=headers, json=payload),
    )

    # All requests should return 202
    assert all(r.status_code == 202 for r in responses)

    # All returned job IDs must be identical
    job_ids = [r.json()["id"] for r in responses]
    assert len(set(job_ids)) == 1


@pytest.mark.asyncio
async def test_idempotency_key_scoped_per_client(client: AsyncClient, session_factory):
    """Requests from different clients sharing the same idempotency key succeed independently without collision."""
    key_a = await register_client(session_factory, "Client-A")
    key_b = await register_client(session_factory, "Client-B")

    shared_idemp_key = "common-client-order-id-999"

    resp_a = await client.post(
        "/api/v1/images/generations",
        headers={"X-API-Key": key_a, "Idempotency-Key": shared_idemp_key},
        json={"prompt": "Client A prompt", "width": 512, "height": 512},
    )
    assert resp_a.status_code == 202
    job_a_id = resp_a.json()["id"]

    resp_b = await client.post(
        "/api/v1/images/generations",
        headers={"X-API-Key": key_b, "Idempotency-Key": shared_idemp_key},
        json={"prompt": "Client B prompt", "width": 512, "height": 512},
    )
    assert resp_b.status_code == 202
    job_b_id = resp_b.json()["id"]

    assert job_a_id != job_b_id


@pytest.mark.asyncio
async def test_idempotency_malformed_keys_rejected(client: AsyncClient, session_factory):
    """Verifies that empty, whitespace, oversized, or control-character keys are rejected with HTTP 422."""
    raw_key = await register_client(session_factory, "Client-Bad-Keys")
    base_headers = {"X-API-Key": raw_key}
    payload = {"prompt": "Valid prompt", "width": 512, "height": 512}

    # Empty key
    r_empty = await client.post(
        "/api/v1/images/generations",
        headers=dict(base_headers, **{"Idempotency-Key": ""}),
        json=payload,
    )
    assert r_empty.status_code == 422
    assert "cannot be empty" in r_empty.json()["detail"]

    # Whitespace key
    r_spaces = await client.post(
        "/api/v1/images/generations",
        headers=dict(base_headers, **{"Idempotency-Key": "    "}),
        json=payload,
    )
    assert r_spaces.status_code == 422
    assert "cannot be empty" in r_spaces.json()["detail"]

    # Key exceeding 128 characters
    r_long = await client.post(
        "/api/v1/images/generations",
        headers=dict(base_headers, **{"Idempotency-Key": "x" * 129}),
        json=payload,
    )
    assert r_long.status_code == 422
    assert "exceeds maximum length of 128" in r_long.json()["detail"]

    # Key containing control characters (\n, \r)
    r_ctrl = await client.post(
        "/api/v1/images/generations",
        headers=dict(base_headers, **{"Idempotency-Key": "bad\nkey\rheader"}),
        json=payload,
    )
    assert r_ctrl.status_code == 422
    assert "invalid control characters" in r_ctrl.json()["detail"]


# =============================================================================
# PHASE 3: TENANT AND ASSET ISOLATION
# =============================================================================

@pytest.mark.asyncio
async def test_cross_client_job_and_asset_access_forbidden(client: AsyncClient, session_factory):
    """Proves Client A cannot inspect or download Client B's job or asset."""
    key_owner = await register_client(session_factory, "Owner-Client")
    key_intruder = await register_client(session_factory, "Intruder-Client")

    # 1. Owner creates and completes a generation job
    r_create = await client.post(
        "/api/v1/images/generations",
        headers={"X-API-Key": key_owner},
        json={"prompt": "Confidential financial chart"},
    )
    job_id = r_create.json()["id"]

    # Worker completes the job
    worker = HybridWorker(
        base_url="http://testserver",
        worker_shared_secret=WORKER_SHARED_SECRET,
        provider=MockIsolationProvider(),
        storage=LocalStorageCAS(),
        worker_id="worker-isolation-test",
    )
    await worker.poll_and_execute_once(client)

    # 2. Owner can read and download successfully
    owner_poll = await client.get(f"/api/v1/images/generations/{job_id}", headers={"X-API-Key": key_owner})
    assert owner_poll.status_code == 200
    assert owner_poll.json()["status"] == "COMPLETED"

    owner_download = await client.get(f"/api/v1/images/generations/{job_id}/content", headers={"X-API-Key": key_owner})
    assert owner_download.status_code == 200
    # Private asset cache control assertion (SEC-ISO-02)
    cache_control = owner_download.headers.get("Cache-Control", "")
    assert "private" in cache_control, f"Expected 'private' in Cache-Control header, got '{cache_control}'"
    assert "no-transform" in cache_control

    # 3. Intruder cannot poll Client B's job (HTTP 403)
    intruder_poll = await client.get(f"/api/v1/images/generations/{job_id}", headers={"X-API-Key": key_intruder})
    assert intruder_poll.status_code == 403
    assert intruder_poll.json()["code"] == "FORBIDDEN"
    assert "do not have permission" in intruder_poll.json()["detail"]

    # 4. Intruder cannot download Client B's asset (HTTP 403)
    intruder_download = await client.get(f"/api/v1/images/generations/{job_id}/content", headers={"X-API-Key": key_intruder})
    assert intruder_download.status_code == 403
    assert intruder_download.json()["code"] == "FORBIDDEN"


@pytest.mark.asyncio
async def test_authentication_enforcement(client: AsyncClient, session_factory):
    """Verifies missing, invalid, and revoked API keys are rejected with HTTP 401."""
    # Missing API key
    r_missing = await client.get("/api/v1/images/generations/non-existent-id")
    assert r_missing.status_code == 401
    assert r_missing.json()["code"] == "UNAUTHORIZED"

    # Invalid API key
    r_invalid = await client.get(
        "/api/v1/images/generations/non-existent-id",
        headers={"X-API-Key": "aarav_live_totally_fabricated_key_123"},
    )
    assert r_invalid.status_code == 401
    assert r_invalid.json()["code"] == "UNAUTHORIZED"

    # Revoked API key
    revoked_key = await register_client(session_factory, "Revoked-Client", is_active=False)
    r_revoked = await client.get(
        "/api/v1/images/generations/non-existent-id",
        headers={"X-API-Key": revoked_key},
    )
    assert r_revoked.status_code == 401
    assert "revoked" in r_revoked.json()["detail"]


@pytest.mark.asyncio
async def test_guessed_job_ids_return_404_not_found(client: AsyncClient, session_factory):
    """Non-existent job IDs return HTTP 404 without leaking server internals."""
    raw_key = await register_client(session_factory, "Client-Guesser")
    headers = {"X-API-Key": raw_key}
    fake_id = str(uuid.uuid4())

    r_status = await client.get(f"/api/v1/images/generations/{fake_id}", headers=headers)
    assert r_status.status_code == 404
    assert r_status.json()["code"] == "RESOURCE_NOT_FOUND"

    r_content = await client.get(f"/api/v1/images/generations/{fake_id}/content", headers=headers)
    assert r_content.status_code == 404
    assert r_content.json()["code"] == "RESOURCE_NOT_FOUND"


@pytest.mark.asyncio
async def test_worker_and_client_credential_separation(client: AsyncClient, session_factory):
    """Verifies worker credentials cannot access client routes and vice versa."""
    client_key = await register_client(session_factory, "Client-Creds")
    worker_auth_headers = {"Authorization": f"Bearer {WORKER_SHARED_SECRET}"}
    client_auth_headers = {"X-API-Key": client_key}

    # Worker credentials on client generation endpoint -> 401 Unauthorized
    r_client_route = await client.post(
        "/api/v1/images/generations",
        headers=worker_auth_headers,
        json={"prompt": "Test"},
    )
    assert r_client_route.status_code == 401

    # Client credentials on worker lease endpoint -> 401 Unauthorized
    r_worker_route = await client.post(
        "/api/v1/worker/jobs/lease",
        headers=client_auth_headers,
        json={"worker_id": "rogue-client-as-worker"},
    )
    assert r_worker_route.status_code == 401


# =============================================================================
# PHASE 4: INPUT BOUNDARY VALIDATION
# =============================================================================

@pytest.mark.asyncio
async def test_input_validation_boundaries(client: AsyncClient, session_factory):
    """Verifies boundary enforcement for prompt, dimensions, steps, seed, and identifiers."""
    raw_key = await register_client(session_factory, "Client-Validator")
    headers = {"X-API-Key": raw_key}

    # 1. Whitespace prompt
    r = await client.post("/api/v1/images/generations", headers=headers, json={"prompt": "   "})
    assert r.status_code == 422

    # 2. Oversized prompt (> 2000 chars)
    r = await client.post("/api/v1/images/generations", headers=headers, json={"prompt": "x" * 2001})
    assert r.status_code == 422

    # 3. Oversized negative prompt (> 1000 chars)
    r = await client.post(
        "/api/v1/images/generations",
        headers=headers,
        json={"prompt": "Valid", "negative_prompt": "y" * 1001},
    )
    assert r.status_code == 422

    # 4. Non-multiple of 64 dimensions
    r = await client.post("/api/v1/images/generations", headers=headers, json={"prompt": "Valid", "width": 500})
    assert r.status_code == 422

    # 5. Dimension out of bounds (< 256 or > 512)
    r = await client.post("/api/v1/images/generations", headers=headers, json={"prompt": "Valid", "width": 192})
    assert r.status_code == 422

    r = await client.post("/api/v1/images/generations", headers=headers, json={"prompt": "Valid", "width": 576})
    assert r.status_code == 422

    # 6. Steps out of bounds (< 1 or > 25)
    r = await client.post("/api/v1/images/generations", headers=headers, json={"prompt": "Valid", "steps": 0})
    assert r.status_code == 422

    r = await client.post("/api/v1/images/generations", headers=headers, json={"prompt": "Valid", "steps": 26})
    assert r.status_code == 422

    # 7. Seed out of bounds (< -1)
    r = await client.post("/api/v1/images/generations", headers=headers, json={"prompt": "Valid", "seed": -5})
    assert r.status_code == 422

    # 8. Empty or whitespace model
    r = await client.post("/api/v1/images/generations", headers=headers, json={"prompt": "Valid", "model": "  "})
    assert r.status_code == 422

    # 9. Empty or whitespace provider
    r = await client.post("/api/v1/images/generations", headers=headers, json={"prompt": "Valid", "provider": "  "})
    assert r.status_code == 422


# =============================================================================
# PHASE 4: ERROR CONTRACTS & RATE LIMITING
# =============================================================================

@pytest.mark.asyncio
async def test_rate_limiting_contract(client: AsyncClient, session_factory):
    """Verifies that exceeding rate limits returns HTTP 429 with Retry-After header and RFC 7807 problem details."""
    raw_key = await register_client(session_factory, "Client-Rate-Limited", rate_limit_rpm=2)
    headers = {"X-API-Key": raw_key}
    payload = {"prompt": "Rate limit test"}

    # Request 1 & 2 succeed
    r1 = await client.post("/api/v1/images/generations", headers=headers, json=payload)
    assert r1.status_code == 202

    r2 = await client.post("/api/v1/images/generations", headers=headers, json=payload)
    assert r2.status_code == 202

    # Request 3 exceeds quota -> HTTP 429
    r3 = await client.post("/api/v1/images/generations", headers=headers, json=payload)
    assert r3.status_code == 429
    assert "Retry-After" in r3.headers
    assert int(r3.headers["Retry-After"]) > 0

    err_body = r3.json()
    assert err_body["code"] == "RATE_LIMIT_EXCEEDED"
    assert err_body["status"] == 429
    assert "Rate limit exceeded" in err_body["detail"]
    assert "correlation_id" in err_body
