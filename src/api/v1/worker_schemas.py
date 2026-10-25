"""
Worker Ingress API Schemas (Mode B)
Location: src/api/v1/worker_schemas.py
"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


class WorkerLeaseRequest(BaseModel):
    worker_id: str = Field(..., min_length=1, max_length=64, description="Unique worker instance identifier")
    lease_timeout_seconds: int = Field(default=120, ge=10, le=600, description="Lease duration in seconds")


class WorkerLeaseResponse(BaseModel):
    job_id: str
    prompt: str
    negative_prompt: Optional[str] = None
    width: int
    height: int
    steps: int
    seed: int
    model: str
    provider_requested: str
    retry_count: int
    max_retries: int
    locked_at: datetime
    lease_timeout_seconds: int


class WorkerHeartbeatRequest(BaseModel):
    worker_id: str = Field(..., min_length=1, max_length=64, description="Worker instance holding the lease")


class WorkerHeartbeatResponse(BaseModel):
    status: str = "renewed"
    job_id: str
    locked_at: datetime


class WorkerCompleteRequest(BaseModel):
    worker_id: str = Field(..., min_length=1, max_length=64, description="Worker instance holding the lease")
    file_hash: str = Field(..., pattern=r"^[a-f0-9]{64}$", description="Content-addressable SHA-256 hash")
    file_size_bytes: int = Field(..., ge=1, description="Binary size in bytes")
    storage_provider: str = Field(default="local_cas", description="Storage backend type")
    storage_path: str = Field(..., min_length=1, description="Relative storage path or bucket key")
    public_url: Optional[str] = Field(default=None, description="Optional public or signed CDN retrieval URL")
    mime_type: str = Field(default="image/png", description="Asset MIME type")
    width: int = Field(..., ge=64, le=4096)
    height: int = Field(..., ge=64, le=4096)
    execution_time_ms: int = Field(..., ge=0, description="Generation duration in milliseconds")
    peak_vram_mb: Optional[int] = Field(default=None, ge=0, description="Peak VRAM telemetry in MiB")
    provider_used: str = Field(..., min_length=1, max_length=32, description="Provider used for generation")


class WorkerCompleteResponse(BaseModel):
    status: str = "completed"
    job_id: str
    asset_id: str


class WorkerFailRequest(BaseModel):
    worker_id: str = Field(..., min_length=1, max_length=64, description="Worker instance holding the lease")
    error_code: str = Field(..., min_length=1, max_length=64, description="Standard error code (e.g. GPU_OUT_OF_MEMORY)")
    error_message: str = Field(..., min_length=1, max_length=1000, description="Detailed failure message")
    retry_delay_seconds: Optional[int] = Field(default=None, ge=1, le=3600, description="Explicit delay before next retry attempt")


class WorkerFailResponse(BaseModel):
    status: str
    job_id: str
    retry_count: int
    max_retries: int
