"""
Unit Tests for Authentication & Rate Limiting
Location: tests/unit/test_auth_and_rate_limit.py
"""

from datetime import datetime, timezone
import pytest
from starlette.requests import Request

from src.api.auth import get_authenticated_api_key
from src.api.rate_limiter import SlidingWindowRateLimiter
from src.core.config import settings
from src.core.errors import AuthenticationException, RateLimitExceededException
from src.core.security import generate_api_key, hash_api_key
from src.domain.models import ApiKeyTier
from src.infrastructure.models import ApiKeyModel


def make_dummy_request(headers: dict) -> Request:
    """Creates a lightweight Starlette Request with given headers."""
    scope = {
        "type": "http",
        "headers": [(k.lower().encode("latin-1"), v.encode("latin-1")) for k, v in headers.items()],
    }
    return Request(scope)


@pytest.mark.asyncio
async def test_auth_missing_key_raises_401(db_session):
    """Verifies that requests without API key header raise 401."""
    req = make_dummy_request({})
    with pytest.raises(AuthenticationException, match="Missing API key"):
        await get_authenticated_api_key(req, db_session)


@pytest.mark.asyncio
async def test_auth_invalid_key_raises_401(db_session):
    """Verifies that an unrecognized API key raises 401."""
    req = make_dummy_request({"X-API-Key": "aarav_live_completely_bogus_token_12345"})
    with pytest.raises(AuthenticationException, match="Invalid API key"):
        await get_authenticated_api_key(req, db_session)


@pytest.mark.asyncio
async def test_auth_revoked_key_raises_401(db_session):
    """Verifies that a deactivated/revoked API key raises 401."""
    raw_key, _, _ = generate_api_key()
    salt = settings.API_KEY_SALT.get_secret_value()
    key_hash = hash_api_key(raw_key, salt)

    api_key_model = ApiKeyModel(
        key_hash=key_hash,
        client_name="Revoked-Client",
        is_active=False,  # Revoked!
    )
    db_session.add(api_key_model)
    await db_session.commit()

    req = make_dummy_request({"X-API-Key": raw_key})
    with pytest.raises(AuthenticationException, match="revoked"):
        await get_authenticated_api_key(req, db_session)


@pytest.mark.asyncio
async def test_auth_valid_key_succeeds_and_updates_last_used(db_session):
    """Verifies that a valid API key authenticates and updates last_used_at."""
    raw_key, _, _ = generate_api_key()
    salt = settings.API_KEY_SALT.get_secret_value()
    key_hash = hash_api_key(raw_key, salt)

    api_key_model = ApiKeyModel(
        key_hash=key_hash,
        client_name="Active-Client",
        is_active=True,
    )
    db_session.add(api_key_model)
    await db_session.commit()

    # Authenticate via X-API-Key
    req = make_dummy_request({"X-API-Key": raw_key})
    authenticated = await get_authenticated_api_key(req, db_session)
    assert authenticated.id == api_key_model.id
    assert authenticated.last_used_at is not None

    # Authenticate via Authorization: Bearer
    req_bearer = make_dummy_request({"Authorization": f"Bearer {raw_key}"})
    auth_bearer = await get_authenticated_api_key(req_bearer, db_session)
    assert auth_bearer.id == api_key_model.id


def test_rate_limiter_allows_within_quota():
    """Verifies that requests within RPM quota succeed."""
    limiter = SlidingWindowRateLimiter(window_seconds=60.0)
    api_key = ApiKeyModel(id="client-1", rate_limit_rpm=5)

    for _ in range(5):
        limiter.check(api_key)  # Should not raise


def test_rate_limiter_blocks_over_quota_with_retry_after():
    """Verifies that requests exceeding RPM quota raise RateLimitExceededException."""
    limiter = SlidingWindowRateLimiter(window_seconds=60.0)
    api_key = ApiKeyModel(id="client-2", rate_limit_rpm=3)

    # First 3 succeed
    for _ in range(3):
        limiter.check(api_key)

    # 4th must raise 429
    with pytest.raises(RateLimitExceededException) as exc_info:
        limiter.check(api_key)

    assert exc_info.value.status_code == 429
    assert "Retry-After" in exc_info.value.headers
    retry_after = int(exc_info.value.headers["Retry-After"])
    assert 1 <= retry_after <= 60


def test_rate_limiter_recovers_after_window():
    """Verifies that after the sliding window expires, quota is restored."""
    limiter = SlidingWindowRateLimiter(window_seconds=2.0)
    api_key = ApiKeyModel(id="client-3", rate_limit_rpm=2)

    limiter.check(api_key)
    limiter.check(api_key)

    with pytest.raises(RateLimitExceededException):
        limiter.check(api_key)

    # Simulate window expiration by altering timestamps in window
    limiter._requests[api_key.id] = [t - 3.0 for t in limiter._requests[api_key.id]]

    # Should succeed now
    limiter.check(api_key)
