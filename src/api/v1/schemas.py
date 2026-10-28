"""
Pydantic v2 DTOs & Validation Schemas for Generation API
Location: src/api/v1/schemas.py
"""

from datetime import datetime
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field, field_validator


class GenerationCreateRequest(BaseModel):
    """Client request payload to enqueue an image generation job."""
    prompt: str = Field(
        ...,
        min_length=1,
        max_length=2000,
        description="Text prompt guiding generation",
        examples=["A serene cyberpunk garden with glowing cherry blossoms"],
    )
    negative_prompt: Optional[str] = Field(
        default=None,
        max_length=1000,
        description="Optional negative guidance to avoid unwanted artifacts",
    )
    width: int = Field(
        default=512,
        ge=256,
        le=512,
        description="Image width in pixels (bounded to 512 for local 4 GB VRAM safety)",
    )
    height: int = Field(
        default=512,
        ge=256,
        le=512,
        description="Image height in pixels (bounded to 512 for local 4 GB VRAM safety)",
    )
    steps: int = Field(
        default=4,
        ge=1,
        le=25,
        description="Number of denoising inference steps",
    )
    seed: int = Field(
        default=-1,
        ge=-1,
        le=9223372036854775807,
        description="Numeric seed for reproducibility (-1 for randomized seed)",
    )
    model: str = Field(
        default="sd-1.5",
        min_length=1,
        max_length=64,
        description="Model identifier",
    )
    provider: str = Field(
        default="auto",
        min_length=1,
        max_length=32,
        description="Inference provider identifier ('auto', 'local-gpu', 'mock')",
    )

    @field_validator("prompt")
    @classmethod
    def validate_prompt_not_empty(cls, v: str) -> str:
        clean = v.strip()
        if not clean:
            raise ValueError("Prompt cannot be empty or whitespace.")
        return clean

    @field_validator("negative_prompt")
    @classmethod
    def validate_negative_prompt_clean(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        clean = v.strip()
        return clean if clean else None

    @field_validator("model", "provider")
    @classmethod
    def validate_non_empty_identifiers(cls, v: str) -> str:
        clean = v.strip().lower()
        if not clean:
            raise ValueError("Field cannot be empty or whitespace.")
        return clean

    @field_validator("width", "height")
    @classmethod
    def validate_multiples_of_64(cls, v: int) -> int:
        if v % 64 != 0:
            raise ValueError(f"Image dimension ({v}) must be a multiple of 64.")
        return v


class JobResponse(BaseModel):
    """Full status and telemetry representation of a generation job."""
    id: str
    status: str
    prompt: str
    negative_prompt: Optional[str] = None
    width: int
    height: int
    steps: int
    seed: int
    model: str
    provider_requested: str
    provider_used: Optional[str] = None
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    execution_time_ms: Optional[int] = None
    peak_vram_mb: Optional[int] = None
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    is_synthetic: Optional[bool] = None
    links: Dict[str, str] = Field(default_factory=dict)
