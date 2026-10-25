"""
Unit Tests for Worker Authentication and Distributed Rate Limiting
Location: tests/unit/test_worker_auth_and_rate_limit.py
"""

from collections import defaultdict
import pytest
from starlette.requests import Request
from pydantic import SecretStr

from src.api.rate_limiter import DistributedSlidingWindowRateLimiter
from src.api.worker_auth import get_authenticated_worker
from src.core.config import settings
from src.core.errors import AuthenticationException, RateLimitExceededException
from src.infrastructure.models import ApiKeyModel


def make_dummy_request(headers: dict) -> Request:
    raw_headers = [(k.lower().encode("latin-1"), v.encode("latin-1")) for k, v in headers.items()]
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/api/v1/worker/jobs/lease",
        "headers": raw_headers,
    }
    return Request(scope)


@pytest.mark.asyncio
async def test_worker_auth_missing_header():
    req = make_dummy_request({})
    with pytest.raises(AuthenticationException) as exc:
        await get_authenticated_worker(req)
    assert "Missing or malformed worker credentials" in str(exc.value)


@pytest.mark.asyncio
async def test_worker_auth_invalid_scheme():
    req = make_dummy_request({"Authorization": "Basic dXNlcjpwYXNz"})
    with pytest.raises(AuthenticationException):
        await get_authenticated_worker(req)


@pytest.mark.asyncio
async def test_worker_auth_mismatched_secret(monkeypatch):
    monkeypatch.setattr(settings, "WORKER_SHARED_SECRET", SecretStr("correct_shared_secret_token_123"))
    req = make_dummy_request({"Authorization": "Bearer wrong_secret_token_456"})
    with pytest.raises(AuthenticationException) as exc:
        await get_authenticated_worker(req)
    assert "Invalid worker authentication credentials" in str(exc.value)


@pytest.mark.asyncio
async def test_worker_auth_success(monkeypatch):
    valid_token = "secure_production_worker_secret_999"
    monkeypatch.setattr(settings, "WORKER_SHARED_SECRET", SecretStr(valid_token))
    req = make_dummy_request({"Authorization": f"Bearer {valid_token}"})
    res = await get_authenticated_worker(req)
    assert res is True


def test_distributed_rate_limiter_cross_node_sync():
    # Shared storage simulating distributed memory store / Redis
    shared_store = defaultdict(list)
    node_a = DistributedSlidingWindowRateLimiter(window_seconds=60.0, shared_store=shared_store)
    node_b = DistributedSlidingWindowRateLimiter(window_seconds=60.0, shared_store=shared_store)

    key = ApiKeyModel(id="shared-tenant-1", rate_limit_rpm=3)

    # 2 requests on Node A
    node_a.check(key)
    node_a.check(key)

    # 1 request on Node B (reaching quota of 3)
    node_b.check(key)

    # 4th request on either node must raise 429
    with pytest.raises(RateLimitExceededException) as exc:
        node_a.check(key)
    assert exc.value.status_code == 429
    assert "Retry-After" in exc.value.headers

    with pytest.raises(RateLimitExceededException):
        node_b.check(key)


@pytest.mark.asyncio
async def test_worker_auth_secret_rotation(monkeypatch):
    primary = "current_active_secret_111"
    previous = "previous_rotated_secret_000"
    monkeypatch.setattr(settings, "WORKER_SHARED_SECRET", SecretStr(primary))
    monkeypatch.setattr(settings, "WORKER_SHARED_SECRET_PREVIOUS", SecretStr(previous))

    # 1. Primary secret succeeds
    req_primary = make_dummy_request({"Authorization": f"Bearer {primary}"})
    assert await get_authenticated_worker(req_primary) is True

    # 2. Previous secret succeeds during rotation window
    req_previous = make_dummy_request({"Authorization": f"Bearer {previous}"})
    assert await get_authenticated_worker(req_previous) is True

    # 3. Third unrelated secret fails
    req_invalid = make_dummy_request({"Authorization": "Bearer completely_wrong_secret"})
    with pytest.raises(AuthenticationException):
        await get_authenticated_worker(req_invalid)


def test_rate_limiter_is_globally_enforced():
    from src.api.rate_limiter import InMemorySlidingWindowRateLimiter

    local_limiter = InMemorySlidingWindowRateLimiter()
    assert local_limiter.is_globally_enforced is False

    dist_limiter = DistributedSlidingWindowRateLimiter()
    assert dist_limiter.is_globally_enforced is True


def test_distributed_rate_limiter_fail_closed_on_store_error():
    class BrokenStore:
        def __getitem__(self, item):
            raise ConnectionError("Simulated Redis network disconnection")

    limiter = DistributedSlidingWindowRateLimiter(
        shared_store=BrokenStore(),
        on_store_failure="fail_closed",
    )
    key = ApiKeyModel(id="tenant-fail-closed", rate_limit_rpm=5)

    with pytest.raises(RateLimitExceededException):
        limiter.check(key)

    assert limiter.is_globally_enforced is False


def test_distributed_rate_limiter_fallback_local_on_store_error():
    class BrokenStore:
        def __getitem__(self, item):
            raise TimeoutError("Simulated Redis timeout")

    limiter = DistributedSlidingWindowRateLimiter(
        shared_store=BrokenStore(),
        on_store_failure="fallback_local",
    )
    key = ApiKeyModel(id="tenant-fallback-local", rate_limit_rpm=2)

    # 1st and 2nd should succeed locally despite shared store failure
    limiter.check(key)
    limiter.check(key)

    # Limiter marks itself as degraded (not globally enforced)
    assert limiter.is_globally_enforced is False

    # 3rd exceeds local quota (2)
    with pytest.raises(RateLimitExceededException):
        limiter.check(key)
