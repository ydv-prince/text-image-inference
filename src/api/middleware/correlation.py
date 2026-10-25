"""
Correlation ID Middleware
Location: src/api/middleware/correlation.py
"""

import uuid
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from src.core.logging import set_correlation_id


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """
    Extracts or generates an X-Correlation-ID for each HTTP request,
    propagating it across asynchronous logging and appending it to response headers.
    """

    HEADER_NAME = "X-Correlation-ID"

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        # Extract client-provided correlation ID or generate a new one
        correlation_id = request.headers.get(self.HEADER_NAME)
        if not correlation_id:
            correlation_id = str(uuid.uuid4())

        # Store in async context
        set_correlation_id(correlation_id)

        # Process request
        response = await call_next(request)

        # Attach to outgoing response headers
        response.headers[self.HEADER_NAME] = correlation_id
        return response
