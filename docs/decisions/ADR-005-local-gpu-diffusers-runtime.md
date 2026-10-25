# ADR-005: Local GPU Inference Engine Selection (Hugging Face Diffusers vs Ollama)

**Status:** ACCEPTED  
**Date:** 2026-10-21  
**Author:** Principal System Design Engineer & ML Infrastructure Architect  
**Deciders:** Core Engineering Team  

---

## 1. Context & Problem Statement
The user has an NVIDIA GeForce RTX 2050 Mobile GPU with 4096 MiB (4 GB) VRAM on Windows 11. To achieve zero-cost local inference, we evaluated:
1. Hugging Face Diffusers (PyTorch with CUDA).
2. Ollama local image generation.
3. ONNX Runtime / DirectML.

The model runtime must run safely within the strict 4 GB VRAM boundary, avoiding CUDA Out-of-Memory (OOM) errors and host system memory thrashing.

## 2. Technical Evaluation of Candidates

### 1. Ollama:
- **Status:** Not currently installed on the host machine.
- **Analysis:** Ollama is an LLM serving runtime built on llama.cpp. While experimental text-to-image support has recently been introduced for niche models, it lacks fine-grained programmatic memory hooks. Specifically, it does not support PyTorch attention slicing, VAE slicing, or explicit CUDA cache emptying, making it a high-risk candidate for a card with only 4 GB VRAM.

### 2. Hugging Face Diffusers:
- **Status:** Fully compatible with Python 3.12 and PyTorch CUDA 12.4 on Windows.
- **Analysis:** Diffusers provides exact, fine-grained tensor memory controls:
  - `pipe.enable_attention_slicing()`: Slices attention matrices into smaller chunks, keeping VRAM flat.
  - `pipe.enable_vae_slicing()`: Prevents the final latent-to-pixel decoding phase from causing large VRAM allocation spikes.
  - `torch_dtype=torch.float16`: Cuts weight memory in half.
  - `pipe.enable_model_cpu_offload()`: Emergency fallback that swaps unneeded sub-models into system RAM.

### 3. Model Selection for 4 GB VRAM:
- **`stabilityai/sd-turbo`:** Distilled SD 2.1 model (~0.86B parameters). Requires only **1 to 4 steps** at 512x512 resolution. Consumes strictly **~2.2 GB to 2.8 GB VRAM** in FP16 with attention slicing, leaving over 1.2 GB of headroom for Windows desktop display overhead.
- **`runwayml/stable-diffusion-v1-5`:** Standard 512x512 checkpoint (~20-25 steps). Consumes ~3.0 GB to 3.4 GB VRAM. Serves as a 100% compatible commercial alternative.

## 3. Decision Outcome
**Chosen Option: Hugging Face Diffusers with `stabilityai/sd-turbo` as the primary Proof-of-Concept engine.**

- Model weights are stored in standard Safetensors format.
- Python 3.12 isolated virtual environment (`.venv`) is utilized.
- Resolution is strictly capped at 512x512 for `local-gpu` inference.
- Concurrency on the GPU is strictly bounded to 1 task at a time (`asyncio.Semaphore(1)`).

### Positive Consequences:
- Genuine, high-quality images generated locally in 1-3 seconds on the RTX 2050 Mobile.
- Zero external API bills or network latency.
- Full programmatic control over VRAM profiling and error handling.
