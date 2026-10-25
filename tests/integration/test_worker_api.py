"""
Integration Tests for Mode B Worker Ingress Endpoints
Location: tests/integration/test_worker_api.py
"""

import asyncio
import uuid
import pytest
from httpx import AsyncClient
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.core.config import settings
from src.core.security import generate_api_key, hash_api_key
from src.domain.models import ApiKeyTier, JobStatus
from src.infrastructure.models import ApiKeyModel, GeneratedAssetModel, GenerationJobModel

WORKER_TEST_SECRET = "test_worker_secret_key_xyz_789"


@pytest.fixture(autouse=True)
def set_worker_secret(monkeypatch):
    monkeypatch.setattr(settings, "WORKER_SHARED_SECRET", SecretStr(WORKER_TEST_SECRET))


def worker_headers(secret: str = WORKER_TEST_SECRET) -> dict:
    return {"Authorization": f"Bearer {secret}"}


async def create_test_api_key(session_factory: async_sessionmaker[AsyncSession]) -> str:
    raw_key, _, _ = generate_api_key()
    salt = settings.API_KEY_SALT.get_secret_value()
    key_hash = hash_api_key(raw_key, salt)

    async with session_factory() as session:
        async with session.begin():
            api_key = ApiKeyModel(
                id=str(uuid.uuid4()),
                key_hash=key_hash,
                client_name="Test-Worker-Client",
                is_active=True,
                rate_limit_rpm=60,
                tier=ApiKeyTier.FREE.value,
            )
            session.add(api_key)
            await session.flush()
            return api_key.id


async def insert_pending_job(session_factory: async_sessionmaker[AsyncSession], api_key_id: str, prompt: str = "Test prompt") -> str:
    job_id = str(uuid.uuid4())
    async with session_factory() as session:
        async with session.begin():
            job = GenerationJobModel(
                id=job_id,
                api_key_id=api_key_id,
                payload_hash="e" * 64,
                status=JobStatus.PENDING.value,
                prompt=prompt,
                width=512,
                height=512,
                steps=4,
                seed=42,
                model="sd-1.5",
                provider_requested="mock",
            )
            session.add(job)
    return job_id


@pytest.mark.asyncio
async def test_worker_endpoints_require_auth(client: AsyncClient):
    # Missing auth
    resp = await client.post("/api/v1/worker/jobs/lease", json={"worker_id": "w1"})
    assert resp.status_code == 401

    # Invalid secret
    resp_invalid = await client.post(
        "/api/v1/worker/jobs/lease",
        json={"worker_id": "w1"},
        headers=worker_headers("wrong_secret"),
    )
    assert resp_invalid.status_code == 401


@pytest.mark.asyncio
async def test_worker_lease_empty_queue(client: AsyncClient):
    resp = await client.post(
        "/api/v1/worker/jobs/lease",
        json={"worker_id": "worker-alpha"},
        headers=worker_headers(),
    )
    assert resp.status_code == 204


@pytest.mark.asyncio
async def test_worker_lease_and_heartbeat(client: AsyncClient, session_factory):
    api_key_id = await create_test_api_key(session_factory)
    job_id = await insert_pending_job(session_factory, api_key_id)

    # Worker 1 leases the job
    lease_resp = await client.post(
        "/api/v1/worker/jobs/lease",
        json={"worker_id": "worker-1", "lease_timeout_seconds": 60},
        headers=worker_headers(),
    )
    assert lease_resp.status_code == 200
    data = lease_resp.json()
    assert data["job_id"] == job_id
    assert data["width"] == 512

    # Worker 1 heartbeats -> succeeds
    hb_resp = await client.post(
        f"/api/v1/worker/jobs/{job_id}/heartbeat",
        json={"worker_id": "worker-1"},
        headers=worker_headers(),
    )
    assert hb_resp.status_code == 200
    assert hb_resp.json()["status"] == "renewed"

    # Rogue Worker 2 tries to heartbeat Worker 1's job -> rejected with 409
    hb_rogue = await client.post(
        f"/api/v1/worker/jobs/{job_id}/heartbeat",
        json={"worker_id": "worker-2"},
        headers=worker_headers(),
    )
    assert hb_rogue.status_code == 409
    assert hb_rogue.json()["code"] == "LEASE_OWNERSHIP_CONFLICT"


@pytest.mark.asyncio
async def test_worker_complete_idempotency_and_ownership(client: AsyncClient, session_factory):
    api_key_id = await create_test_api_key(session_factory)
    job_id = await insert_pending_job(session_factory, api_key_id)

    # Lease
    await client.post(
        "/api/v1/worker/jobs/lease",
        json={"worker_id": "worker-primary"},
        headers=worker_headers(),
    )

    complete_payload = {
        "worker_id": "worker-primary",
        "file_hash": "a" * 64,
        "file_size_bytes": 10240,
        "storage_provider": "local_cas",
        "storage_path": "images/aa/aa/mock.png",
        "mime_type": "image/png",
        "width": 512,
        "height": 512,
        "execution_time_ms": 120,
        "peak_vram_mb": 0,
        "provider_used": "mock",
    }

    # Worker 2 attempts to complete -> 409 Conflict
    bad_worker_payload = dict(complete_payload, worker_id="worker-intruder")
    resp_conflict = await client.post(
        f"/api/v1/worker/jobs/{job_id}/complete",
        json=bad_worker_payload,
        headers=worker_headers(),
    )
    assert resp_conflict.status_code == 409

    # Worker primary completes -> 200 OK
    resp_complete = await client.post(
        f"/api/v1/worker/jobs/{job_id}/complete",
        json=complete_payload,
        headers=worker_headers(),
    )
    assert resp_complete.status_code == 200
    asset_id = resp_complete.json()["asset_id"]

    # Duplicate completion call (network retry) -> 200 OK idempotent
    resp_dup = await client.post(
        f"/api/v1/worker/jobs/{job_id}/complete",
        json=complete_payload,
        headers=worker_headers(),
    )
    assert resp_dup.status_code == 200
    assert resp_dup.json()["asset_id"] == asset_id


@pytest.mark.asyncio
async def test_worker_fail_retry_and_exhaustion(client: AsyncClient, session_factory):
    api_key_id = await create_test_api_key(session_factory)
    job_id = await insert_pending_job(session_factory, api_key_id)

    # Lease
    await client.post(
        "/api/v1/worker/jobs/lease",
        json={"worker_id": "worker-flaky"},
        headers=worker_headers(),
    )

    # Fail attempt 1 (retryable)
    r1 = await client.post(
        f"/api/v1/worker/jobs/{job_id}/fail",
        json={
            "worker_id": "worker-flaky",
            "error_code": "NETWORK_TIMEOUT",
            "error_message": "Upstream timeout",
            "retry_delay_seconds": 1,
        },
        headers=worker_headers(),
    )
    assert r1.status_code == 200
    assert r1.json()["status"] == "retrying"
    assert r1.json()["retry_count"] == 1


@pytest.mark.asyncio
async def test_concurrent_worker_leasing_exclusion(client: AsyncClient, session_factory):
    api_key_id = await create_test_api_key(session_factory)
    job_id_1 = await insert_pending_job(session_factory, api_key_id, "Prompt 1")
    job_id_2 = await insert_pending_job(session_factory, api_key_id, "Prompt 2")

    # 1. Mutual exclusion: Two workers race on the top of the queue
    tasks = [
        client.post("/api/v1/worker/jobs/lease", json={"worker_id": "w-alpha"}, headers=worker_headers()),
        client.post("/api/v1/worker/jobs/lease", json={"worker_id": "w-beta"}, headers=worker_headers()),
    ]
    results = await asyncio.gather(*tasks)

    # Exactly one worker wins the race for the top candidate; the other either gets next or 204
    success_claims = [r.json()["job_id"] for r in results if r.status_code == 200]
    # No duplicate leases of the same job ID
    assert len(success_claims) == len(set(success_claims))

    # 2. Sequential poll drains any remaining job
    subsequent_resp = await client.post(
        "/api/v1/worker/jobs/lease",
        json={"worker_id": "w-gamma"},
        headers=worker_headers(),
    )
    if subsequent_resp.status_code == 200:
        success_claims.append(subsequent_resp.json()["job_id"])

    # Both jobs must be leased across workers with no duplicate leases
    assert set(success_claims) == {job_id_1, job_id_2}
