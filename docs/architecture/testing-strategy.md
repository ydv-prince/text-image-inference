# Testing & Quality Assurance Strategy — Aarav Image Generation API

**Document Version:** 2.0.0 (Local GPU First, Cloud Ready Revision)  
**Status:** PROPOSED (Pending Review Gate)  
**Author:** Principal System Design Engineer & QA Lead  
**System:** Aarav Image Generation API (`Inference-1`)

---

## 1. Test Philosophy & Mock Distinction Rule

In accordance with our core engineering protocols:
- **Mock Provider Role:** The `MockProvider` generates synthetic 512x512 placeholder images with deterministic colored geometry in <5ms. It is used **strictly for automated CI/CD runs, unit tests, and regression tests**.
- **Golden Rule:** **Mock output must NEVER be represented as a real AI-generated image.** User-facing generations and proof-of-concept benchmarks must use the genuine `LocalDiffusersAdapter` running real diffusion weights on the GPU.

```text
               /\
              /  \      Offline Worker & Journey Tests (5%)
             /----\     - Offline Queue Accumulation & Drain
            /      \    Contract Tests (15%)
           /--------\   - OpenAPI 3.1 & RFC 7807 compliance
          /          \  Integration & Failure Injection (30%)
         /------------\ - VRAM OOM Guard, Worker Sudden Kill, SQLite/PG Leases
        /              \ Unit Tests (50%)
       /----------------\ - State Machine, Adapters, Schemas, CAS Hasher
```

---

## 2. Milestone 0: Local GPU Proof-of-Concept Script (`tests/poc/verify_local_gpu.py`)

Before implementing full application services, we validate the hardware and PyTorch environment with an isolated proof-of-concept script:

```python
"""
Proof of Concept: Local GPU Inference Verification
Verifies:
1. PyTorch detects NVIDIA GeForce RTX 2050 Mobile.
2. SD-Turbo loads in FP16 with attention and VAE slicing.
3. Generates 1 real 512x512 image from prompt.
4. Records duration, peak VRAM, and output integrity.
"""
import time
import torch
from PIL import Image
from diffusers import AutoPipelineForText2Image

def run_local_poc():
    print("=== Milestone 0: Local GPU Proof-of-Concept ===")
    assert torch.cuda.is_available(), "CUDA device not detected! Check driver."
    
    device_name = torch.cuda.get_device_name(0)
    total_vram_mb = torch.cuda.get_device_properties(0).total_memory / (1024 * 1024)
    print(f"Device: {device_name} | Total VRAM: {total_vram_mb:.0f} MiB")

    torch.cuda.reset_peak_memory_stats()
    start_load = time.perf_counter()

    print("Loading stabilityai/sd-turbo pipeline (FP16)...")
    pipe = AutoPipelineForText2Image.from_pretrained(
        "stabilityai/sd-turbo",
        torch_dtype=torch.float16,
        variant="fp16",
        use_safetensors=True
    ).to("cuda")

    # Enable 4GB memory optimizations
    pipe.enable_attention_slicing()
    pipe.enable_vae_slicing()
    load_time = time.perf_counter() - start_load
    print(f"Model loaded successfully in {load_time:.2f}s")

    prompt = "A cinematic close-up photograph of an ancient stone statue covered in moss, golden morning light, 8k"
    print(f"Generating image with prompt: '{prompt}'...")
    
    start_gen = time.perf_counter()
    image = pipe(
        prompt=prompt,
        width=512,
        height=512,
        num_inference_steps=1,
        guidance_scale=0.0
    ).images[0]
    gen_time = time.perf_counter() - start_gen

    peak_vram_mb = torch.cuda.max_memory_allocated() / (1024 * 1024)
    output_path = "poc_generated_image.png"
    image.save(output_path)

    print("\n--- PoC Verification Telemetry ---")
    print(f"Status: SUCCESS")
    print(f"Generation Duration: {gen_time:.2f}s")
    print(f"Peak VRAM Used: {peak_vram_mb:.1f} MiB / {total_vram_mb:.0f} MiB")
    print(f"Output File: {output_path} ({image.width}x{image.height})")
    assert peak_vram_mb <= 3500, "VRAM exceeded safe threshold for 4GB card!"
    print("PoC Acceptance Criteria PASSED.")

if __name__ == "__main__":
    run_local_poc()
```

---

## 3. Failure Injection & Chaos Scenarios

| Scenario ID | Tested Failure Mode | Injected Action | Expected Resilient Behavior |
| :--- | :--- | :--- | :--- |
| **FI-1** | **CUDA Out-of-Memory (OOM)** | Request 1536x1536 image on local GPU adapter. | `LocalDiffusersAdapter` catches `torch.cuda.OutOfMemoryError`, invokes `torch.cuda.empty_cache()` and `gc.collect()`, logs telemetry, and transitions job to `FAILED` with code `GPU_OUT_OF_MEMORY` without crashing the Python process. |
| **FI-2** | **Worker Sudden Termination** | Abruptly kill worker process (`kill -9` / `Stop-Process`) while job is in `PROCESSING`. | Hosted `LeaseSweeper` detects expired `locked_at` (>120s), resets job to `PENDING`. When worker restarts, it claims and completes the job. |
| **FI-3** | **Offline Worker Queue Accumulation** | Submit 5 jobs while local worker PC is offline/disconnected. | Jobs accumulate in Supabase PostgreSQL in `PENDING` state. When local worker starts, it claims all 5 jobs sequentially in FIFO order without duplicate execution. |
| **FI-4** | **Concurrent Duplicate Submissions** | Submit 2 identical requests with same `Idempotency-Key` within 10ms. | Database unique constraint on `(api_key_id, idempotency_key)` catches race condition; exactly one job is inserted; second caller receives existing job. |
