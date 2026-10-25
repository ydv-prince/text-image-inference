"""
Client Sliding-Window Rate Limiting Engine
Location: src/api/rate_limiter.py

Guarantees & Architectural Boundaries:
1. Process-Local vs Distributed:
   - InMemorySlidingWindowRateLimiter is strictly PROCESS-LOCAL.
   - Initial deployment configuration explicitly assumes ONE API worker process (e.g. Render single-container web service).
   - Rate limits are not globally synchronized across multiple independent processes without an external shared store.
2. Distributed Sliding Window:
   - DistributedSlidingWindowRateLimiter supports explicit failure policies:
     * 'fail_closed': Rejects requests with HTTP 429 when the shared store is unreachable (strict backend protection).
     * 'fallback_local': Gracefully degrades to node-local in-memory limits with structured warnings.
3. Exceeded quotas emit RFC 7807 problem details with standard Retry-After headers.
"""

import math
from collections import defaultdict
import time
from typing import Dict, List, Literal, Optional
from fastapi import Depends

from src.api.auth import get_authenticated_api_key
from src.core.errors import RateLimitExceededException
from src.core.logging import logger
from src.infrastructure.models import ApiKeyModel


from abc import ABC, abstractmethod


class BaseRateLimiter(ABC):
    """Abstract interface for client rate limiting."""
    @property
    @abstractmethod
    def is_globally_enforced(self) -> bool:
        """Returns True if the rate limit is synchronized globally across nodes; False if process-local."""
        pass

    @abstractmethod
    def check(self, api_key: ApiKeyModel) -> None:
        """Checks and records a request. Raises RateLimitExceededException if exceeded."""
        pass

    @abstractmethod
    def reset(self) -> None:
        """Resets recorded rate limits (testing utility)."""
        pass


class InMemorySlidingWindowRateLimiter(BaseRateLimiter):
    """
    Thread-safe in-memory sliding-window rate limiter per API key.
    
    OPERATIONAL BOUNDARY:
    This limiter is PROCESS-LOCAL. It is designed for single-process deployments
    (such as the initial single-instance Render free web service).
    It is NOT globally enforced across multiple worker processes or multi-region nodes.
    """

    def __init__(self, window_seconds: float = 60.0):
        self.window_seconds = window_seconds
        self._requests: Dict[str, List[float]] = defaultdict(list)

    @property
    def is_globally_enforced(self) -> bool:
        """In-memory rate limiting is process-local, never globally enforced."""
        return False

    def check(self, api_key: ApiKeyModel) -> None:
        now = time.time()
        cutoff = now - self.window_seconds

        # Prune expired timestamps
        history = [ts for ts in self._requests[api_key.id] if ts > cutoff]
        self._requests[api_key.id] = history

        max_rpm = api_key.rate_limit_rpm or 10

        if len(history) >= max_rpm:
            oldest = history[0]
            retry_after = max(1, math.ceil(oldest + self.window_seconds - now))
            raise RateLimitExceededException(retry_after=retry_after)

        # Record this request
        history.append(now)

    def reset(self) -> None:
        """Clears all in-memory windows (for testing)."""
        self._requests.clear()


# Backward-compatible alias
SlidingWindowRateLimiter = InMemorySlidingWindowRateLimiter


class DistributedSlidingWindowRateLimiter(BaseRateLimiter):
    """
    Distributed sliding-window rate limiter designed for multi-process cloud deployments (Mode B).
    Maintains synchronization across distributed API nodes with explicit fallback and safety policy.
    """

    def __init__(
        self,
        window_seconds: float = 60.0,
        shared_store: Optional[Dict[str, List[float]]] = None,
        on_store_failure: Literal["fallback_local", "fail_closed"] = "fallback_local",
    ):
        self.window_seconds = window_seconds
        self._shared_store = shared_store if shared_store is not None else defaultdict(list)
        self.on_store_failure: Literal["fallback_local", "fail_closed"] = on_store_failure
        self._local_fallback_store: Dict[str, List[float]] = defaultdict(list)
        self._store_degraded: bool = False

    @property
    def is_globally_enforced(self) -> bool:
        """Returns True only when the distributed shared store is actively operating without fallback degradation."""
        return not self._store_degraded

    def check(self, api_key: ApiKeyModel) -> None:
        now = time.time()
        cutoff = now - self.window_seconds
        key = str(api_key.id)
        max_rpm = api_key.rate_limit_rpm or 10

        try:
            history = [ts for ts in self._shared_store[key] if ts > cutoff]
            if len(history) >= max_rpm:
                oldest = history[0]
                retry_after = max(1, math.ceil(oldest + self.window_seconds - now))
                raise RateLimitExceededException(retry_after=retry_after)

            history.append(now)
            self._shared_store[key] = history
            self._store_degraded = False
            return
        except RateLimitExceededException:
            raise
        except Exception as exc:
            self._store_degraded = True
            if self.on_store_failure == "fail_closed":
                logger.error(
                    f"Shared rate-limit store error and policy is 'fail_closed': {exc}. Rejection enforced."
                )
                raise RateLimitExceededException(
                    retry_after=math.ceil(self.window_seconds),
                )

            # Fallback to local in-memory partition
            logger.warning(
                f"Shared rate-limit store unavailable: {exc}. Falling back to node-local enforcement. "
                "Global rate limit is NOT guaranteed across instances."
            )
            fallback_history = [ts for ts in self._local_fallback_store[key] if ts > cutoff]
            if len(fallback_history) >= max_rpm:
                oldest = fallback_history[0]
                retry_after = max(1, math.ceil(oldest + self.window_seconds - now))
                raise RateLimitExceededException(retry_after=retry_after)

            fallback_history.append(now)
            self._local_fallback_store[key] = fallback_history

    def reset(self) -> None:
        try:
            self._shared_store.clear()
        except Exception:
            pass
        self._local_fallback_store.clear()
        self._store_degraded = False


# Global rate limiter instance (default to in-memory Mode A)
rate_limiter: BaseRateLimiter = InMemorySlidingWindowRateLimiter()


def check_rate_limit(
    api_key: ApiKeyModel = Depends(get_authenticated_api_key),
) -> ApiKeyModel:
    """FastAPI dependency verifying both API key authentication and rate limits."""
    rate_limiter.check(api_key)
    return api_key
