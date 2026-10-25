"""
Mock Image Inference Provider
Location: src/infrastructure/providers/mock.py

Guarantees:
1. Fast (<5ms), deterministic, 100% offline execution.
2. Output is explicitly labeled as synthetic/test-only.
3. NEVER represents mock output as a genuine AI-generated image.
"""

import io
import time
from typing import Optional
from PIL import Image, ImageDraw

from src.domain.interfaces import (
    ImageProviderAdapter,
    ProviderHealth,
    ProviderRequest,
    ProviderResponse,
)


class MockProviderAdapter(ImageProviderAdapter):
    """
    Deterministic mock provider generating synthetic placeholder images
    for fast unit and integration tests without GPU or network access.
    """

    def __init__(self, simulated_delay_ms: int = 0, fail_next: bool = False):
        self._delay_ms = simulated_delay_ms
        self._fail_next = fail_next

    @property
    def name(self) -> str:
        return "mock"

    def set_fail_next(self, fail: bool) -> None:
        """Utility for failure-injection testing."""
        self._fail_next = fail

    async def generate_image(self, request: ProviderRequest) -> ProviderResponse:
        start_time = time.perf_counter()

        if self._fail_next:
            self._fail_next = False
            raise RuntimeError("Injected mock provider failure for testing.")

        if self._delay_ms > 0:
            import asyncio
            await asyncio.sleep(self._delay_ms / 1000.0)

        # Generate a distinct synthetic test image (gray background with banner)
        width, height = request.width, request.height
        image = Image.new("RGB", (width, height), color=(45, 55, 72))  # Slate background
        draw = ImageDraw.Draw(image)

        # Draw diagonal guideline
        draw.line([(0, 0), (width, height)], fill=(74, 85, 104), width=2)
        draw.line([(0, height), (width, 0)], fill=(74, 85, 104), width=2)

        # Draw clear synthetic disclaimer box in center
        box_top = height // 2 - 30
        box_bottom = height // 2 + 30
        draw.rectangle([(20, box_top), (width - 20, box_bottom)], fill=(26, 32, 44), outline=(229, 62, 62), width=2)
        
        # Draw explanatory text
        disclaimer = "SYNTHETIC TEST IMAGE - NOT AI GENERATED"
        draw.text((30, height // 2 - 8), disclaimer, fill=(247, 250, 252))

        # Encode to PNG bytes
        buffer = io.BytesIO()
        image.save(buffer, format="PNG")
        image_bytes = buffer.getvalue()

        duration_ms = int((time.perf_counter() - start_time) * 1000)

        return ProviderResponse(
            image_bytes=image_bytes,
            mime_type="image/png",
            width=width,
            height=height,
            duration_ms=duration_ms,
            provider_name=self.name,
            model_id="mock-v1",
            model_revision="synthetic",
            peak_vram_mb=0,
            is_synthetic=True,
        )

    async def health_check(self) -> ProviderHealth:
        return ProviderHealth(
            provider_name=self.name,
            is_healthy=True,
            status_message="Mock provider ready for offline testing.",
            is_loaded=True,
            device="in-memory",
            vram_allocated_mb=0.0,
            vram_total_mb=0.0,
        )
