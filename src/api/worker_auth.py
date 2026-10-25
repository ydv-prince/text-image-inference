"""
Worker Bearer Authentication Dependency
Location: src/api/worker_auth.py

Guarantees:
1. Strict Bearer token extraction from Authorization header.
2. Constant-time comparison (hmac.compare_digest) against settings.WORKER_SHARED_SECRET.
3. Rejection of unauthenticated or unauthorized worker requests with HTTP 401.
"""

import hmac
from fastapi import Request
from src.core.config import settings
from src.core.errors import AuthenticationException


async def get_authenticated_worker(request: Request) -> bool:
    """
    Validates the worker bearer secret in constant time.
    Raises HTTP 401 AuthenticationException if missing or invalid.
    """
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise AuthenticationException("Missing or malformed worker credentials. Use 'Authorization: Bearer <token>'.")

    token = auth_header[7:].strip()
    if not token:
        raise AuthenticationException("Worker bearer token cannot be empty.")

    if not settings.WORKER_SHARED_SECRET:
        raise AuthenticationException("Worker shared secret is not configured on the control plane.")

    expected_secret = settings.WORKER_SHARED_SECRET.get_secret_value()
    if hmac.compare_digest(token, expected_secret):
        return True

    # Check previous secret during zero-downtime rotation window
    if settings.WORKER_SHARED_SECRET_PREVIOUS:
        previous_secret = settings.WORKER_SHARED_SECRET_PREVIOUS.get_secret_value()
        if hmac.compare_digest(token, previous_secret):
            return True

    raise AuthenticationException("Invalid worker authentication credentials.")
