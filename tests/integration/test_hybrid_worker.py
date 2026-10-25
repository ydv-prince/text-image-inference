"""
Integration Tests for HybridWorker Daemon (Mode B)
Location: tests/integration/test_hybrid_worker.py
"""

from pathlib import Path
import pytest
from httpx import AsyncClient
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.core.config import settings
from src.core.security import generate_api_key, hash_api_key
from src.domain.models import ApiKeyTier
from src.infrastructure.models import ApiKeyModel
from src.infrastructure.providers.mock import MockProviderAdapter
from src.infrastructure.storage.local_cas import LocalStorageCAS
from src.services.hybrid_worker import HybridWorker

WORKER_SHARED_SECRET_TEST = "hybrid_daemon_secret_val_123"


@pytest.fixture(autouse=True)
def configure_worker_env(monkeypatch):
    monkeypatch.setattr(settings, "WORKER_SHARED_SECRET", SecretStr(WORKER_SHARED_SECRET_TEST))


async def create_client_api_key(session_factory: async_sessionmaker[AsyncSession]) -> str:
    raw_key, _, _ = generate_api_key()
    salt = settings.API_KEY_SALT.get_secret_value()
    key_hash = hash_api_key(raw_key, salt)

    async with session_factory() as session:
        async with session.begin():
            api_key = ApiKeyModel(
                key_hash=key_hash,
                client_name="Hybrid-Test-Client",
                is_active=True,
                rate_limit_rpm=60,
                tier=ApiKeyTier.FREE.value,
            )
            session.add(api_key)
    return raw_key


@pytest.mark.asyncio
async def test_hybrid_worker_end_to_end_lifecycle(client: AsyncClient, session_factory):
    # 1. Register client and submit job
    client_key = await create_client_api_key(session_factory)
    client_headers = {"X-API-Key": client_key}

    submit_resp = await client.post(
        "/api/v1/images/generations",
        headers=client_headers,
        json={"prompt": "A futuristic city in clouds", "provider": "mock"},
    )
    assert submit_resp.status_code == 202
    job_id = submit_resp.json()["id"]

    # 2. Setup HybridWorker
    mock_provider = MockProviderAdapter(simulated_delay_ms=5)
    storage = LocalStorageCAS(base_path=Path("./tests/scratch_storage"))
    from src.api.v1 import generations
    generations.cas_storage = storage

    worker = HybridWorker(
        base_url="http://testserver",
        worker_shared_secret=WORKER_SHARED_SECRET_TEST,
        provider=mock_provider,
        storage=storage,
        worker_id="hybrid-unit-worker",
    )

    # 3. Hybrid worker executes one outbound poll cycle
    processed = await worker.poll_and_execute_once(client)
    assert processed is True

    # 4. Client polls job status
    status_resp = await client.get(f"/api/v1/images/generations/{job_id}", headers=client_headers)
    assert status_resp.status_code == 200
    job_data = status_resp.json()
    assert job_data["status"] == "COMPLETED"
    assert job_data["provider_used"] == "mock"
    assert job_data["is_synthetic"] is True
    assert "content" in job_data["links"]

    # 5. Client retrieves raw PNG image content
    content_url = job_data["links"]["content"]
    content_resp = await client.get(content_url, headers=client_headers)
    assert content_resp.status_code == 200
    assert content_resp.headers["Content-Type"] == "image/png"
    assert content_resp.headers["Cache-Control"] == "private, no-transform, max-age=86400"
    assert len(content_resp.content) > 100

    # 6. Next poll returns False (queue empty)
    processed_empty = await worker.poll_and_execute_once(client)
    assert processed_empty is False
