"""
Milestone 0: Local GPU Proof-of-Concept Verification Script
Location: tests/poc/verify_local_gpu.py

Model Configuration:
- Base Model: runwayml/stable-diffusion-v1-5 (CreativeML OpenRAIL-M)
- Fast LCM Adapter: latent-consistency/lcm-lora-sdv1-5 (OpenRAIL++)
- Scheduler: LCMScheduler
- Target: 4 inference steps at 512x512
- Precision: FP16 with attention and VAE slicing for 4 GB VRAM safety
"""

import hashlib
import os
import sys
import time
from pathlib import Path


def run_lcm_poc():
    print("=" * 75)
    print("Aarav Image Generation API — Milestone 0: Local GPU Proof of Concept")
    print("Model: runwayml/stable-diffusion-v1-5 + latent-consistency/lcm-lora-sdv1-5")
    print("=" * 75)

    # 1. Environment & Package Audit
    print("\n[Step 1/6] Auditing Environment & Package Versions...")
    print(f"Python Executable: {sys.executable}")
    print(f"Python Version:    {sys.version.split()[0]}")

    import torch
    import diffusers
    import transformers
    import peft
    import PIL

    print(f"PyTorch Version:   {torch.__version__}")
    print(f"Diffusers Version: {diffusers.__version__}")
    print(f"Transformers:      {transformers.__version__}")
    print(f"PEFT Version:      {peft.__version__}")
    print(f"Pillow Version:    {PIL.__version__}")

    # 2. Hardware & CUDA Verification
    print("\n[Step 2/6] Inspecting Physical Hardware & CUDA Acceleration...")
    if not torch.cuda.is_available():
        print("[FATAL] CUDA is not available to PyTorch!")
        sys.exit(1)

    device_name = torch.cuda.get_device_name(0)
    device_properties = torch.cuda.get_device_properties(0)
    total_vram_mb = device_properties.total_memory / (1024 * 1024)
    print(f"CUDA Available:    True")
    print(f"CUDA Version:      {torch.version.cuda}")
    print(f"Device Name:       {device_name}")
    print(f"Physical VRAM:     {total_vram_mb:.0f} MiB")

    # 3. Model Pipeline Loading
    base_model_id = "runwayml/stable-diffusion-v1-5"
    adapter_id = "latent-consistency/lcm-lora-sdv1-5"

    print(f"\n[Step 3/6] Loading Base Model '{base_model_id}' (FP16)...")
    torch.cuda.empty_cache()
    torch.cuda.reset_peak_memory_stats()
    load_start = time.perf_counter()

    from diffusers import AutoPipelineForText2Image, LCMScheduler

    pipe = None
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            print(f"Downloading/Loading base pipeline (Attempt {attempt}/{max_retries})...")
            pipe = AutoPipelineForText2Image.from_pretrained(
                base_model_id,
                torch_dtype=torch.float16,
                variant="fp16",
                use_safetensors=True
            ).to("cuda")
            break
        except Exception as e:
            if attempt == max_retries:
                print(f"[FATAL] Failed to download/load base model after {max_retries} attempts: {e}")
                sys.exit(1)
            print(f"[WARN] Transient download error: {e}. Retrying in 5 seconds...")
            time.sleep(5)

    # 4. Attaching LCMScheduler and LoRA Adapter
    print(f"\n[Step 4/6] Configuring LCMScheduler and Fusing '{adapter_id}'...")
    pipe.scheduler = LCMScheduler.from_config(pipe.scheduler.config)

    for attempt in range(1, max_retries + 1):
        try:
            print(f"Downloading/Loading LCM adapter weights (Attempt {attempt}/{max_retries})...")
            pipe.load_lora_weights(adapter_id)
            pipe.fuse_lora()
            break
        except Exception as e:
            if attempt == max_retries:
                print(f"[FATAL] Failed to download/load LCM adapter after {max_retries} attempts: {e}")
                sys.exit(1)
            print(f"[WARN] Transient download error: {e}. Retrying in 5 seconds...")
            time.sleep(5)

    # Enforce 4GB VRAM Slicing Optimizations
    pipe.enable_attention_slicing()
    pipe.enable_vae_slicing()

    load_duration = time.perf_counter() - load_start
    print(f"Model and LCM-LoRA loaded & fused in {load_duration:.2f} seconds.")

    # 5. Local GPU Inference (4 Steps, 512x512)
    prompt = "A high-detail photograph of an ancient stone temple in a misty bamboo forest, golden hour lighting, cinematic composition"
    negative_prompt = "blurry, distorted, oversaturated, deformed, bad anatomy"
    width, height = 512, 512
    num_steps = 4
    guidance_scale = 1.0  # Recommended for LCM

    print(f"\n[Step 5/6] Executing Local GPU Inference...")
    print(f"Prompt:          '{prompt}'")
    print(f"Resolution:      {width}x{height}")
    print(f"Inference Steps: {num_steps} (LCM 4-step fast generation)")
    print(f"Guidance Scale:  {guidance_scale}")

    torch.cuda.reset_peak_memory_stats()
    gen_start = time.perf_counter()

    try:
        # Fixed generator seed for reproducibility
        generator = torch.Generator(device="cuda").manual_seed(42)
        image = pipe(
            prompt=prompt,
            negative_prompt=negative_prompt,
            width=width,
            height=height,
            num_inference_steps=num_steps,
            guidance_scale=guidance_scale,
            generator=generator
        ).images[0]
        gen_duration = time.perf_counter() - gen_start
    except torch.cuda.OutOfMemoryError as oom:
        print(f"\n[FATAL] CUDA Out of Memory Error: {oom}")
        torch.cuda.empty_cache()
        sys.exit(1)
    except Exception as e:
        print(f"\n[FATAL] Inference failed with error: {e}")
        sys.exit(1)

    peak_allocated_mb = torch.cuda.max_memory_allocated() / (1024 * 1024)
    peak_reserved_mb = torch.cuda.max_memory_reserved() / (1024 * 1024)

    # 6. Output Verification & Persistence
    print("\n[Step 6/6] Verifying Output Image & Telemetry...")
    output_dir = Path("tests/poc/output")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "poc_verified_local_gpu.png"

    image.save(output_file, format="PNG")
    file_bytes = output_file.read_bytes()
    sha256_hash = hashlib.sha256(file_bytes).hexdigest()

    print("\n" + "=" * 75)
    print("PROVE-OF-CONCEPT MEASURED TELEMETRY REPORT")
    print("=" * 75)
    print(f"Execution Status:         SUCCESS (Genuine local GPU image generated)")
    print(f"Base Model ID:            {base_model_id} (CreativeML OpenRAIL-M)")
    print(f"LCM Adapter ID:           {adapter_id} (OpenRAIL++)")
    print(f"Output Dimensions:        {image.width} x {image.height}")
    print(f"Inference Latency:        {gen_duration:.2f} seconds")
    print(f"Peak VRAM Allocated:      {peak_allocated_mb:.1f} MiB")
    print(f"Peak VRAM Reserved:       {peak_reserved_mb:.1f} MiB")
    print(f"Total Physical VRAM:      {total_vram_mb:.0f} MiB")
    print(f"Safe VRAM Margin:         {total_vram_mb - peak_reserved_mb:.1f} MiB")
    print(f"Output Image File:        {output_file.resolve()}")
    print(f"Output File Size:         {len(file_bytes) / 1024:.1f} KB")
    print(f"SHA-256 Digest:           {sha256_hash}")
    print("=" * 75)


if __name__ == "__main__":
    run_lcm_poc()
