"""
Unit Tests — Rate Limiter Architectural Policies & Failure Modes
Location: tests/unit/test_rate_limiter_policies.py
"""

from collections import defaultdict
import pytest
from unittest.mock import MagicMock

from src.api.rate_limiter import (
    InMemorySlidingWindowRateLimiter,
    DistributedSlidingWindowRateLimiter,
)
from src.core.errors import RateLimitExceededException
from src.infrastructure.models import ApiKeyModel


class TestRateLimiterPolicies:
    """Verifies architectural invariants and failure handling of rate limiters."""

    def test_in_memory_rate_limiter_is_process_local(self):
        """Verifies that in-memory limiter declares is_globally_enforced=False."""
        limiter = InMemorySlidingWindowRateLimiter()
        assert limiter.is_globally_enforced is False

    def test_distributed_rate_limiter_healthy_is_globally_enforced(self):
        """Verifies that healthy distributed limiter declares is_globally_enforced=True."""
        shared_store = defaultdict(list)
        limiter = DistributedSlidingWindowRateLimiter(shared_store=shared_store)
        assert limiter.is_globally_enforced is True

    def test_distributed_fail_closed_policy_on_store_outage(self):
        """
        When on_store_failure='fail_closed':
        A store outage must immediately raise RateLimitExceededException to protect backend resources.
        """
        # Create a broken store that raises an unexpected exception
        broken_store = MagicMock()
        broken_store.__getitem__.side_effect = RuntimeError("Redis connection timeout / store unreachable")

        limiter = DistributedSlidingWindowRateLimiter(
            shared_store=broken_store,
            on_store_failure="fail_closed",
            window_seconds=60.0,
        )

        mock_key = MagicMock(spec=ApiKeyModel)
        mock_key.id = "api-key-test-001"
        mock_key.rate_limit_rpm = 10

        with pytest.raises(RateLimitExceededException) as exc_info:
            limiter.check(mock_key)

        assert exc_info.value.retry_after == 60

    def test_distributed_fallback_local_policy_on_store_outage(self):
        """
        When on_store_failure='fallback_local':
        A store outage must degrade to node-local enforcement and set is_globally_enforced=False.
        """
        broken_store = MagicMock()
        broken_store.__getitem__.side_effect = ConnectionError("Distributed store connection refused")

        limiter = DistributedSlidingWindowRateLimiter(
            shared_store=broken_store,
            on_store_failure="fallback_local",
            window_seconds=60.0,
        )

        mock_key = MagicMock(spec=ApiKeyModel)
        mock_key.id = "api-key-test-002"
        mock_key.rate_limit_rpm = 2

        # First request degrades to local fallback and succeeds
        limiter.check(mock_key)
        assert limiter.is_globally_enforced is False

        # Second request succeeds
        limiter.check(mock_key)

        # Third request exceeds the local RPM (2) and raises RateLimitExceededException
        with pytest.raises(RateLimitExceededException) as exc_info:
            limiter.check(mock_key)

        assert exc_info.value.retry_after >= 1
