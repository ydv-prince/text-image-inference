"""
Domain Exceptions & RFC 7807 Problem Details
Location: src/core/errors.py
"""

from typing import Any, Dict, Optional


class AaravException(Exception):
    """Base exception for Aarav Image Generation platform."""

    def __init__(
        self,
        message: str,
        code: str = "INTERNAL_ERROR",
        status_code: int = 500,
        detail: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message)
        self.message = message
        self.code = code
        self.status_code = status_code
        self.detail = detail or message
        self.extra = extra or {}


class AuthenticationException(AaravException):
    def __init__(self, message: str = "Invalid or missing API key."):
        super().__init__(
            message=message,
            code="UNAUTHORIZED",
            status_code=401,
            detail=message,
        )


class ForbiddenException(AaravException):
    def __init__(self, message: str = "Action not permitted."):
        super().__init__(
            message=message,
            code="FORBIDDEN",
            status_code=403,
            detail=message,
        )


class EntityNotFoundException(AaravException):
    def __init__(self, entity_name: str, entity_id: str):
        msg = f"{entity_name} with identifier '{entity_id}' was not found."
        super().__init__(
            message=msg,
            code="RESOURCE_NOT_FOUND",
            status_code=404,
            detail=msg,
        )


class IdempotencyConflictException(AaravException):
    def __init__(self, key: str):
        msg = f"Idempotency key '{key}' was previously used with a different request payload."
        super().__init__(
            message=msg,
            code="IDEMPOTENCY_CONFLICT",
            status_code=409,
            detail=msg,
        )


class GPUOutOfMemoryException(AaravException):
    def __init__(self, message: str = "Local GPU exceeded available VRAM during tensor allocation."):
        super().__init__(
            message=message,
            code="GPU_OUT_OF_MEMORY",
            status_code=503,
            detail=message,
        )


class ValidationException(AaravException):
    def __init__(self, message: str, extra: Optional[Dict[str, Any]] = None):
        super().__init__(
            message=message,
            code="INVALID_PAYLOAD",
            status_code=422,
            detail=message,
            extra=extra,
        )


class InvalidStateTransitionException(AaravException):
    def __init__(self, current_state: str, target_state: str, job_id: Optional[str] = None):
        msg = f"Cannot transition job {f'({job_id}) ' if job_id else ''}from '{current_state}' to '{target_state}'."
        super().__init__(
            message=msg,
            code="INVALID_STATE_TRANSITION",
            status_code=409,
            detail=msg,
            extra={"current_state": current_state, "target_state": target_state, "job_id": job_id},
        )


class StorageException(AaravException):
    def __init__(self, message: str, detail: Optional[str] = None):
        super().__init__(
            message=message,
            code="STORAGE_ERROR",
            status_code=500,
            detail=detail or message,
        )


class CorruptAssetException(AaravException):
    def __init__(self, message: str = "Asset data failed integrity check or format validation."):
        super().__init__(
            message=message,
            code="CORRUPT_ASSET",
            status_code=422,
            detail=message,
        )


class LeaseExpiredException(AaravException):
    def __init__(self, job_id: str, message: str = "Worker lease on job has expired."):
        super().__init__(
            message=message,
            code="LEASE_EXPIRED",
            status_code=409,
            detail=message,
            extra={"job_id": job_id},
        )


class RateLimitExceededException(AaravException):
    def __init__(self, retry_after: int = 60, message: str = "Rate limit exceeded."):
        super().__init__(
            message=message,
            code="RATE_LIMIT_EXCEEDED",
            status_code=429,
            detail=f"{message} Please retry after {retry_after} seconds.",
            extra={"retry_after": retry_after},
        )
        self.retry_after = retry_after
        self.headers = {"Retry-After": str(retry_after)}


class LeaseOwnershipConflictException(AaravException):
    def __init__(self, job_id: str, worker_id: str, message: str = "Worker does not hold the lease for this job."):
        super().__init__(
            message=message,
            code="LEASE_OWNERSHIP_CONFLICT",
            status_code=409,
            detail=message,
            extra={"job_id": job_id, "worker_id": worker_id},
        )
