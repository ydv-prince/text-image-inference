"""
Pure Domain Provider Interfaces & Typed Contracts
Location: src/domain/interfaces.py

Strict Domain Isolation:
- No dependencies on FastAPI, SQLAlchemy, Diffusers, or HTTP clients.
- Clean typed data structures for inference requests, responses, and health telemetry.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class ProviderRequest:
    """Typed request payload passed to an inference provider adapter."""
    prompt: str
    negative_prompt: Optional[str] = None
    width: int = 512
    height: int = 512
    steps: int = 4
    seed: int = -1
    guidance_scale: float = 1.0
    extra_params: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        # Boundary validation
        if not self.prompt or not self.prompt.strip():
            raise ValueError("Prompt must not be empty.")
        if len(self.prompt) > 2000:
            raise ValueError("Prompt exceeds maximum length of 2000 characters.")
        if self.negative_prompt and len(self.negative_prompt) > 1000:
            raise ValueError("Negative prompt exceeds maximum length of 1000 characters.")
        if self.width % 64 != 0 or self.height % 64 != 0:
            raise ValueError(f"Image dimensions ({self.width}x{self.height}) must be multiples of 64.")
        if not (256 <= self.width <= 1536) or not (256 <= self.height <= 1536):
            raise ValueError(f"Image dimensions must be between 256 and 1536 pixels.")
        if not (1 <= self.steps <= 50):
            raise ValueError(f"Inference steps ({self.steps}) must be between 1 and 50.")


@dataclass(frozen=True)
class ProviderResponse:
    """Standardized output returned by an inference provider adapter."""
    image_bytes: bytes
    mime_type: str = "image/png"
    width: int = 512
    height: int = 512
    duration_ms: int = 0
    provider_name: str = "unknown"
    model_id: str = "unknown"
    model_revision: Optional[str] = None
    peak_vram_mb: Optional[int] = None
    is_synthetic: bool = False  # True strictly for mock/test outputs


@dataclass(frozen=True)
class ProviderHealth:
    """Health status and telemetry reported by an inference provider."""
    provider_name: str
    is_healthy: bool
    status_message: str
    is_loaded: bool = False
    device: Optional[str] = None
    vram_allocated_mb: Optional[float] = None
    vram_total_mb: Optional[float] = None


from datetime import datetime
from typing import Any, Dict, List, Optional


class ImageProviderAdapter(ABC):
    """Abstract Base Class for all image generation inference adapters."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique provider identifier (e.g. 'local-gpu', 'mock', 'pollinations')."""
        pass

    @abstractmethod
    async def generate_image(self, request: ProviderRequest) -> ProviderResponse:
        """Executes image generation for the given request."""
        pass

    @abstractmethod
    async def health_check(self) -> ProviderHealth:
        """Inspects and returns current adapter health and readiness."""
        pass


@dataclass(frozen=True)
class JobClaim:
    """Represents a job successfully claimed by a worker lease."""
    job_id: str
    prompt: str
    negative_prompt: Optional[str]
    width: int
    height: int
    steps: int
    seed: int
    model: str
    provider_requested: str
    retry_count: int
    max_retries: int
    locked_by: str
    locked_at: datetime


@dataclass(frozen=True)
class StoredAsset:
    """Metadata of an asset durably saved in Content-Addressable Storage."""
    asset_id: str
    file_hash: str
    storage_path: str
    file_size_bytes: int
    mime_type: str
    width: int
    height: int
    is_synthetic: bool = False


@dataclass(frozen=True)
class JobCompletionData:
    """Payload recording successful job completion and asset reference."""
    job_id: str
    worker_id: str
    provider_used: str
    execution_time_ms: int
    peak_vram_mb: Optional[int]
    asset: StoredAsset
    public_url: Optional[str] = None


class JobRepository(ABC):
    """
    Abstract interface for job persistence, leasing, and lifecycle tracking.
    Implementations must guarantee ACID isolation and never keep locks open during inference.
    """

    @abstractmethod
    async def claim_next_job(self, worker_id: str, lease_timeout_seconds: int = 120) -> Optional[JobClaim]:
        """Atomically leases the oldest eligible pending job for the requesting worker."""
        pass

    @abstractmethod
    async def record_heartbeat(self, job_id: str, worker_id: str) -> bool:
        """Refreshes the lease timestamp for an active job held by worker_id."""
        pass

    @abstractmethod
    async def complete_job(self, completion: JobCompletionData) -> None:
        """Marks the job COMPLETED and attaches the generated asset record atomically."""
        pass

    @abstractmethod
    async def fail_job(
        self,
        job_id: str,
        worker_id: str,
        error_code: str,
        error_message: str,
        is_retryable: bool,
        backoff_seconds: Optional[float] = None,
    ) -> None:
        """Marks the job as failed or re-queues it for retry with exponential backoff."""
        pass

    @abstractmethod
    async def reclaim_expired_leases(
        self,
        lease_timeout_seconds: int = 120,
        max_retries: int = 3,
        base_backoff_seconds: float = 2.0,
        max_backoff_seconds: float = 60.0,
    ) -> List[str]:
        """Sweeps and reclaims stale PROCESSING leases exceeding timeout."""
        pass


class AssetStorage(ABC):
    """
    Abstract interface for Content-Addressable Storage (CAS) of image assets.
    Exposes strictly pure read, write, and integrity verification capabilities.
    """

    @abstractmethod
    async def store_image(
        self,
        image_bytes: bytes,
        mime_type: str = "image/png",
        is_synthetic: bool = False,
    ) -> StoredAsset:
        """Validates, hashes, and atomically stores raw image bytes."""
        pass

    @abstractmethod
    async def get_image_bytes(self, locator: str) -> Optional[bytes]:
        """Retrieves raw image bytes by locator (authoritative storage path or content hash)."""
        pass

    @abstractmethod
    async def exists(self, locator: str) -> bool:
        """Checks if an asset with the given locator exists in storage."""
        pass

    @abstractmethod
    async def verify_asset(
        self,
        locator: str,
        expected_hash: Optional[str] = None,
    ) -> bool:
        """Verifies asset existence and cryptographic SHA-256 integrity."""
        pass


class PhysicalBlobStorage(ABC):
    """
    Dedicated capability interface for physical object deletion.
    Used strictly by DeletionOutboxService; NEVER exposed on AssetStorage or to general callers.
    """

    @abstractmethod
    async def delete_physical_object(self, storage_path: str) -> bool:
        """Deletes a physical object identified by authoritative storage_path."""
        pass



