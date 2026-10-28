"""
Prometheus Metrics HTTP Middleware
Location: src/api/middleware/metrics.py

Guarantees:
1. Low Latency Overhead: Non-blocking monotonic timer (perf_counter) measurement.
2. Cardinality Safety: Normalizes path variables to route templates ({id}) to prevent label explosion.
3. Resilience: Measurement failures never crash or interrupt HTTP request processing.
4. Selective Filtering: Excludes /metrics endpoint from tracking to prevent scrape noise.
"""

import re
import time
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from src.core.config import settings
from src.core.metrics import http_request_duration_seconds, http_requests_total

# Pattern to normalize dynamic resource identifiers (UUIDs, hashes, numeric IDs, prefixed IDs)
_RESOURCE_ID_PATTERN = re.compile(
    r"/(?:[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}|job-[^/]+|[0-9a-fA-F]{16,64}|\d{4,})(?=/|$)"
)


def normalize_path_template(path: str) -> str:
    """
    Normalizes path dynamic segments into {id} to guarantee bounded label cardinality.
    Example:
        /api/v1/images/generations/550e8400-e29b-41d4-a716-446655440000 -> /api/v1/images/generations/{id}
        /api/v1/images/generations/job-123/content -> /api/v1/images/generations/{id}/content
        /api/v1/worker/jobs/550e8400-e29b-41d4-a716-446655440000/heartbeat -> /api/v1/worker/jobs/{id}/heartbeat
    """
    if not path:
        return "/"
    return _RESOURCE_ID_PATTERN.sub("/{id}", path)


class MetricsMiddleware(BaseHTTPMiddleware):
    """
    ASGI middleware recording HTTP request duration and cumulative counts.
    Ensures bounded label cardinality and never records sensitive request data.
    """

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        if not getattr(settings, "METRICS_ENABLED", True):
            return await call_next(request)

        # Skip recording metrics for the scrape endpoint itself to avoid recursion / noise
        raw_path = request.url.path
        if raw_path == "/metrics":
            return await call_next(request)

        method = request.method
        start_time = time.perf_counter()

        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        except Exception:
            status_code = 500
            raise
        finally:
            duration = time.perf_counter() - start_time
            # Prefer registered route path template from FastAPI scope if available
            route = request.scope.get("route")
            if route and hasattr(route, "path") and route.path:
                endpoint = str(route.path)
            else:
                endpoint = normalize_path_template(raw_path)

            try:
                http_request_duration_seconds.observe(
                    duration,
                    labels={"method": method, "endpoint": endpoint},
                )
                http_requests_total.inc(
                    labels={"method": method, "endpoint": endpoint, "status": str(status_code)},
                )
            except Exception:
                # Telemetry recording must never crash or interrupt request execution
                pass
