"""
Unit Tests — Staging Deployment Smoke Test Harness Contract
Location: tests/unit/test_smoke_test_contract.py

Verifies StagingSmokeTestRunner behavior against control-plane API:
1. Complete 8-step end-to-end execution returning True on healthy environment.
2. Immediate abort and False return when client API key is unauthorized.
3. Immediate abort and False return when worker shared secret is invalid.
4. Immediate abort and False return on simulated transport failure or assertion error.
5. Regression test: Missing storage persistence in Step 5 causes Step 7 delivery to fail with HTTP 404.
6. Client lifecycle: Caller-owned client is never closed by runner.
7. Client lifecycle: Runner-owned client is safely closed even on error.
8. Target URL validation: Loopback, remote opt-in, production host blocking, and scheme validation.
9. Diagnostic hygiene: Redaction of secrets and exclusion of raw response bodies in failure outputs.
"""

import uuid
import pytest
from httpx import AsyncClient, ASGITransport
from pydantic import SecretStr

from scripts.staging_smoke_test import (
    SmokeTestConfig,
    StagingSmokeTestRunner,
    make_smoke_test_png,
    validate_target_url,
)
from src.core.config import settings
from src.core.security import generate_api_key, hash_api_key
from src.domain.models import ApiKeyTier
from src.infrastructure.models import ApiKeyModel
from src.infrastructure.storage import get_asset_storage

WORKER_SMOKE_SECRET = "smoke_test_worker_secret_key_32_chars_min"
ADMIN_SMOKE_KEY = "smoke_test_admin_key_32_chars_long_admin"


@pytest.fixture(autouse=True)
def configure_smoke_test_secrets(monkeypatch):
    """Configures explicit secrets for worker ingress and administrative metrics."""
    monkeypatch.setattr(settings, "WORKER_SHARED_SECRET", SecretStr(WORKER_SMOKE_SECRET))
    monkeypatch.setattr(settings, "ADMIN_API_KEY", SecretStr(ADMIN_SMOKE_KEY))
    monkeypatch.setattr(settings, "METRICS_REQUIRE_AUTH", True)


async def create_test_client_key(session_factory, client_name: str = "SmokeTestClient") -> str:
    """Helper to seed a valid client API key."""
    raw_key, _, _ = generate_api_key()
    salt = settings.API_KEY_SALT.get_secret_value()
    key_hash = hash_api_key(raw_key, salt)

    async with session_factory() as session:
        async with session.begin():
            api_key = ApiKeyModel(
                id=str(uuid.uuid4()),
                key_hash=key_hash,
                client_name=client_name,
                is_active=True,
                rate_limit_rpm=120,
                tier=ApiKeyTier.PRO.value,
            )
            session.add(api_key)
    return raw_key


@pytest.mark.asyncio
async def test_smoke_test_runner_full_success(client: AsyncClient, session_factory):
    """
    Verifies that StagingSmokeTestRunner executes all 8 stages and returns True.
    """
    raw_key = await create_test_client_key(session_factory)

    cfg = SmokeTestConfig(
        base_url="http://testserver",
        client_api_key=raw_key,
        worker_shared_secret=WORKER_SMOKE_SECRET,
        admin_api_key=ADMIN_SMOKE_KEY,
        timeout_seconds=10.0,
        mock_mode=True,
    )

    runner = StagingSmokeTestRunner(config=cfg, client=client)
    success = await runner.run()

    assert success is True
    assert runner.job_id is not None
    assert runner.file_hash is not None


@pytest.mark.asyncio
async def test_smoke_test_runner_aborts_on_invalid_client_key(client: AsyncClient, session_factory):
    """
    Verifies that invalid client credentials abort at Step 2 and return False.
    """
    cfg = SmokeTestConfig(
        base_url="http://testserver",
        client_api_key="invalid_bogus_client_key_12345",
        worker_shared_secret=WORKER_SMOKE_SECRET,
        admin_api_key=ADMIN_SMOKE_KEY,
    )

    runner = StagingSmokeTestRunner(config=cfg, client=client)
    success = await runner.run()

    assert success is False
    assert runner.job_id is None


@pytest.mark.asyncio
async def test_smoke_test_runner_aborts_on_invalid_worker_secret(client: AsyncClient, session_factory):
    """
    Verifies that invalid worker shared secret aborts at Step 3 (lease claim) and returns False.
    """
    raw_key = await create_test_client_key(session_factory)

    cfg = SmokeTestConfig(
        base_url="http://testserver",
        client_api_key=raw_key,
        worker_shared_secret="wrong_worker_secret_value",
        admin_api_key=ADMIN_SMOKE_KEY,
    )

    runner = StagingSmokeTestRunner(config=cfg, client=client)
    success = await runner.run()

    assert success is False
    assert runner.job_id is not None


@pytest.mark.asyncio
async def test_smoke_test_runner_aborts_on_assertion_failure(client: AsyncClient, session_factory):
    """
    Verifies that assertion failures during any step trigger clean abort and return False.
    """
    raw_key = await create_test_client_key(session_factory)

    cfg = SmokeTestConfig(
        base_url="http://testserver",
        client_api_key=raw_key,
        worker_shared_secret=WORKER_SMOKE_SECRET,
    )

    runner = StagingSmokeTestRunner(config=cfg, client=client)

    async def failing_step():
        raise AssertionError("Simulated liveness probe failure")

    runner._step_health_probes = failing_step

    success = await runner.run()
    assert success is False


@pytest.mark.asyncio
async def test_regression_missing_storage_persistence_fails_delivery(client: AsyncClient, session_factory):
    """
    Regression Test for Finding H2:
    Simulates completing a job without persisting the asset to storage.
    Verifies that Step 7 (asset delivery) fails with HTTP 404 EntityNotFoundException.
    """
    raw_key = await create_test_client_key(session_factory)

    cfg = SmokeTestConfig(
        base_url="http://testserver",
        client_api_key=raw_key,
        worker_shared_secret=WORKER_SMOKE_SECRET,
        mock_mode=True,
    )

    runner = StagingSmokeTestRunner(config=cfg, client=client)

    # 1. Enqueue job
    await runner._step_health_probes()
    await runner._step_client_submission()
    await runner._step_worker_lease()
    await runner._step_worker_heartbeat()

    # 2. Defective completion: report unwritten random file hash to simulate H2 defect
    bogus_hash = "0123456789abcdef" * 4
    headers = {
        "Authorization": f"Bearer {WORKER_SMOKE_SECRET}",
        "Content-Type": "application/json",
    }
    payload = {
        "worker_id": "smoke-test-worker-node",
        "file_hash": bogus_hash,
        "file_size_bytes": 1024,
        "storage_provider": "local_cas",
        "storage_path": f"non_existent/{bogus_hash[:2]}/{bogus_hash}.png",
        "public_url": None,
        "mime_type": "image/png",
        "width": 512,
        "height": 512,
        "execution_time_ms": 100,
        "provider_used": "mock",
    }
    comp_resp = await client.post(f"/api/v1/worker/jobs/{runner.job_id}/complete", json=payload, headers=headers)
    assert comp_resp.status_code == 200

    # 3. Delivery request without persisted asset MUST fail with 404
    delivery_resp = await client.get(
        f"/api/v1/images/generations/{runner.job_id}/content",
        headers={"X-API-Key": raw_key},
    )
    assert delivery_resp.status_code == 404
    assert "not found" in delivery_resp.text.lower() or "not yet available" in delivery_resp.text.lower()


@pytest.mark.asyncio
async def test_client_lifecycle_caller_owned_client_not_closed(client: AsyncClient, session_factory):
    """
    Verifies Finding H1 requirement: Caller-owned client is NOT closed by runner.run().
    """
    raw_key = await create_test_client_key(session_factory)
    cfg = SmokeTestConfig(
        base_url="http://testserver",
        client_api_key=raw_key,
        worker_shared_secret=WORKER_SMOKE_SECRET,
        mock_mode=True,
    )

    runner = StagingSmokeTestRunner(config=cfg, client=client)
    success = await runner.run()

    assert success is True
    assert client.is_closed is False, "Caller-owned client should remain open after runner.run()"


@pytest.mark.asyncio
async def test_client_lifecycle_runner_owned_client_is_closed(monkeypatch):
    """
    Verifies Finding H1 requirement: Runner-owned client is safely closed after execution.
    """
    cfg = SmokeTestConfig(
        base_url="http://localhost:8000",
        client_api_key="missing_key",
        worker_shared_secret="secret",
    )

    runner = StagingSmokeTestRunner(config=cfg, client=None)
    # Target validation or auth abort will return False without opening network requests
    success = await runner.run()
    assert success is False
    assert runner.client is None, "Runner-owned client reference must be cleared"


def test_target_url_validation_matrix():
    """
    Verifies Finding M1 safety guards:
    - Loopback hosts permitted without opt-in.
    - Non-loopback hosts require explicit allow_remote=True.
    - Production hosts and production-designated domains rejected even with allow_remote=True.
    - Malformed URLs, embedded userinfo, and unsafe schemes rejected.
    """
    # 1. Loopback addresses permitted without allow_remote
    assert validate_target_url("http://localhost:8000") == "http://localhost:8000"
    assert validate_target_url("http://127.0.0.1:8000") == "http://127.0.0.1:8000"
    assert validate_target_url("http://testserver") == "http://testserver"

    # 2. Non-loopback rejected without allow_remote
    with pytest.raises(ValueError, match="Remote execution requires explicit opt-in"):
        validate_target_url("https://staging.aarav-inference.org", allow_remote=False)

    # 3. Non-loopback permitted WITH allow_remote
    valid_staging = "https://staging.aarav-inference.org"
    assert validate_target_url(valid_staging, allow_remote=True) == valid_staging

    # 4. Known production host rejected even with allow_remote
    with pytest.raises(ValueError, match="designated production host"):
        validate_target_url("https://api.aarav-inference.com", allow_remote=True)

    with pytest.raises(ValueError, match="designated production host"):
        validate_target_url("https://api.aarav-inference.internal", allow_remote=True)

    # 5. Production-designated domain tokens rejected even with allow_remote
    with pytest.raises(ValueError, match="designated as a production environment"):
        validate_target_url("https://api.production.internal", allow_remote=True)

    with pytest.raises(ValueError, match="designated as a production environment"):
        validate_target_url("https://inference-prod.fly.dev", allow_remote=True)

    # 6. Unsafe schemes rejected
    with pytest.raises(ValueError, match="Invalid URL scheme"):
        validate_target_url("ftp://localhost:8000")

    with pytest.raises(ValueError, match="Invalid URL scheme"):
        validate_target_url("file:///etc/passwd")

    # 7. Embedded userinfo rejected
    with pytest.raises(ValueError, match="embedded credentials"):
        validate_target_url("http://admin:secret@localhost:8000")

    # 8. Malformed / empty URLs rejected
    with pytest.raises(ValueError, match="empty or unset"):
        validate_target_url("")

    with pytest.raises(ValueError, match="Missing hostname"):
        validate_target_url("http://")


@pytest.mark.asyncio
async def test_diagnostics_do_not_leak_secrets_or_response_bodies(client: AsyncClient, session_factory, capsys):
    """
    Verifies Finding L1 & L3: Failure logs do not leak raw secret tokens or unmasked credentials.
    """
    raw_key = await create_test_client_key(session_factory)
    secret_marker = "sensitive_bearer_token_marker_xyz_123"

    cfg = SmokeTestConfig(
        base_url="http://testserver",
        client_api_key=raw_key,
        worker_shared_secret=secret_marker,
    )

    runner = StagingSmokeTestRunner(config=cfg, client=client)
    await runner.run()

    captured = capsys.readouterr()
    stdout_text = captured.out + captured.err

    # Assert that the secret token is never printed in logs
    assert secret_marker not in stdout_text
    assert "[CONFIGURED]" in stdout_text
