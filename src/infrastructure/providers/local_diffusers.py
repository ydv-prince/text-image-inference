"""
Local GPU Image Inference Provider (Hugging Face Diffusers + LCM)
Location: src/infrastructure/providers/local_diffusers.py

Configuration reuses the verified Milestone 0 baseline:
- Base: runwayml/stable-diffusion-v1-5 (CreativeML OpenRAIL-M)
- Adapter: latent-consistency/lcm-lora-sdv1-5 (OpenRAIL++)
- Scheduler: LCMScheduler
- Precision: FP16 with attention and VAE slicing
- Concurrency: Strictly bounded to 1 concurrent task on GPU (asyncio.Lock)
- Resolution: Strictly capped at 512x512 to guarantee 4 GB VRAM safety
"""

import asyncio
import gc
import io
import time
from typing import Optional

from src.core.errors import GPUOutOfMemoryException, ValidationException
from src.core.logging import logger
from src.domain.interfaces import (
    ImageProviderAdapter,
    ProviderHealth,
    ProviderRequest,
    ProviderResponse,
)


class LocalDiffusersAdapter(ImageProviderAdapter):
    """
    Direct hardware inference engine running locally on NVIDIA RTX 2050 Mobile (4 GB VRAM).
    """

    def __init__(
        self,
        base_model_id: str = "runwayml/stable-diffusion-v1-5",
        adapter_id: str = "latent-consistency/lcm-lora-sdv1-5",
        device: str = "cuda",
        max_dimension: int = 512,
    ):
        self.base_model_id = base_model_id
        self.adapter_id = adapter_id
        self.device = device
        self.max_dimension = max_dimension

        self._pipe = None
        self._is_loaded = False
        self._load_error: Optional[str] = None
        self._lock = asyncio.Lock()  # Strictly 1 GPU generation at a time

    @property
    def name(self) -> str:
        return "local-gpu"

    @property
    def is_loaded(self) -> bool:
        return self._is_loaded

    def validate_request(self, request: ProviderRequest) -> None:
        """Validates hardware safety constraints before allocating any GPU tensors."""
        if request.width > self.max_dimension or request.height > self.max_dimension:
            raise ValidationException(
                f"Local GPU provider is strictly limited to maximum {self.max_dimension}x{self.max_dimension} "
                f"resolution to protect 4 GB VRAM. Received request for {request.width}x{request.height}."
            )

    def _initialize_pipeline(self) -> None:
        """Loads and configures the verified PyTorch Diffusers pipeline on the GPU."""
        import torch
        from diffusers import AutoPipelineForText2Image, LCMScheduler

        if not torch.cuda.is_available():
            raise RuntimeError("CUDA is not available to PyTorch. Local GPU adapter requires an active CUDA device.")

        logger.info(f"Loading local base model '{self.base_model_id}' into {self.device} (FP16)...")
        torch.cuda.empty_cache()
        torch.cuda.reset_peak_memory_stats()

        try:
            pipe = AutoPipelineForText2Image.from_pretrained(
                self.base_model_id,
                torch_dtype=torch.float16,
                variant="fp16",
                use_safetensors=True,
            ).to(self.device)

            # Attach LCMScheduler and fuse LoRA
            logger.info(f"Fusing LCM LoRA adapter '{self.adapter_id}' with LCMScheduler...")
            pipe.scheduler = LCMScheduler.from_config(pipe.scheduler.config)
            pipe.load_lora_weights(self.adapter_id)
            pipe.fuse_lora()

            # Enforce 4GB VRAM slicing optimizations
            pipe.enable_attention_slicing()
            pipe.enable_vae_slicing()

            self._pipe = pipe
            self._is_loaded = True
            self._load_error = None
            logger.info("LocalDiffusersAdapter pipeline initialized and ready.")
        except Exception as e:
            self._is_loaded = False
            self._load_error = str(e)
            logger.error(f"Failed to initialize LocalDiffusersAdapter: {e}", exc_info=True)
            raise RuntimeError(f"Model initialization failed: {e}") from e

    async def generate_image(self, request: ProviderRequest) -> ProviderResponse:
        """Executes hardware diffusion inference sequentially under an exclusive lock."""
        # 1. Pre-allocation validation
        self.validate_request(request)

        # 2. Acquire exclusive GPU lock
        async with self._lock:
            # Lazy load on first request if not preloaded
            if not self._is_loaded or self._pipe is None:
                self._initialize_pipeline()

            import torch

            torch.cuda.reset_peak_memory_stats()
            start_time = time.perf_counter()

            try:
                # Configure generator seed
                generator = None
                if request.seed >= 0:
                    generator = torch.Generator(device=self.device).manual_seed(request.seed)

                # Execute 4-step generation
                image = self._pipe(
                    prompt=request.prompt,
                    negative_prompt=request.negative_prompt,
                    width=request.width,
                    height=request.height,
                    num_inference_steps=request.steps,
                    guidance_scale=request.guidance_scale,
                    generator=generator,
                ).images[0]

                duration_ms = int((time.perf_counter() - start_time) * 1000)
                peak_vram_mb = int(torch.cuda.max_memory_allocated() / (1024 * 1024))

                # Encode to PNG bytes
                buffer = io.BytesIO()
                image.save(buffer, format="PNG")
                image_bytes = buffer.getvalue()

                return ProviderResponse(
                    image_bytes=image_bytes,
                    mime_type="image/png",
                    width=image.width,
                    height=image.height,
                    duration_ms=duration_ms,
                    provider_name=self.name,
                    model_id=f"{self.base_model_id}+{self.adapter_id}",
                    model_revision="fp16-lcm",
                    peak_vram_mb=peak_vram_mb,
                    is_synthetic=False,
                )
            except torch.cuda.OutOfMemoryError as oom:
                torch.cuda.empty_cache()
                gc.collect()
                logger.error(f"CUDA OutOfMemory in LocalDiffusersAdapter: {oom}")
                raise GPUOutOfMemoryException(f"CUDA out of memory: {oom}") from oom
            except Exception as e:
                logger.error(f"Inference error in LocalDiffusersAdapter: {e}", exc_info=True)
                raise
            finally:
                # Essential VRAM hygiene: flush temporary buffers after every task
                torch.cuda.empty_cache()
                gc.collect()

    async def health_check(self) -> ProviderHealth:
        """Inspects physical CUDA availability and observed VRAM allocation."""
        try:
            import torch

            if not torch.cuda.is_available():
                return ProviderHealth(
                    provider_name=self.name,
                    is_healthy=False,
                    status_message="CUDA device unavailable.",
                    is_loaded=False,
                )

            device_name = torch.cuda.get_device_name(0)
            vram_total_mb = torch.cuda.get_device_properties(0).total_memory / (1024 * 1024)
            vram_allocated_mb = torch.cuda.memory_allocated() / (1024 * 1024)

            if self._load_error:
                return ProviderHealth(
                    provider_name=self.name,
                    is_healthy=False,
                    status_message=f"Model loading error: {self._load_error}",
                    is_loaded=False,
                    device=device_name,
                    vram_allocated_mb=vram_allocated_mb,
                    vram_total_mb=vram_total_mb,
                )

            return ProviderHealth(
                provider_name=self.name,
                is_healthy=True,
                status_message="Local GPU device ready." if not self._is_loaded else "Model loaded and active on GPU.",
                is_loaded=self._is_loaded,
                device=device_name,
                vram_allocated_mb=vram_allocated_mb,
                vram_total_mb=vram_total_mb,
            )
        except Exception as e:
            return ProviderHealth(
                provider_name=self.name,
                is_healthy=False,
                status_message=f"Error inspecting hardware health: {e}",
                is_loaded=False,
            )
