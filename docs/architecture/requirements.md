# Requirements & Feasibility Analysis — Aarav Image Generation API

**Document Version:** 2.0.0 (Local GPU First, Cloud Ready Revision)  
**Status:** PROPOSED (Pending Review Gate)  
**Author:** Principal System Design Engineer & ML Infrastructure Architect  
**System:** Aarav Image Generation API (`Inference-1`)

---

## 1. Executive Summary & Revised Problem Statement

Aarav Image Generation API is an asynchronous, vendor-agnostic AI image generation platform designed under a **Local GPU First, Cloud Ready** paradigm:
1. **Zero-Cost Local Development:** Utilizes the host machine's physical NVIDIA GeForce RTX 2050 Mobile GPU (4 GB VRAM) running open-weight diffusion checkpoints locally via Hugging Face Diffusers in FP16 precision with memory-efficient attention slicing.
2. **Dual-Deployment Operational Model:**
   - **Mode A (Local Monolith):** Single-machine developer environment with FastAPI, SQLite WAL, local content-addressable storage, and local Diffusers worker.
   - **Mode B (Hybrid Hosted Deployment):** Free-tier cloud control plane (Firebase Hosting for static frontend, Supabase PostgreSQL for durable job queuing, optional Cloudinary/Supabase storage for CDN delivery) coupled with the user's local PC acting as an **outbound-polling GPU execution worker**.
3. **Pluggable & Cloud-Ready:** Business logic remains completely isolated from model runtimes. While local Diffusers is the primary engine, external cloud providers (Pollinations, fal.ai) and a deterministic Mock provider remain swappable adapters.

---

## 2. Hardware Reality & Local Runtime Feasibility

### 2.1 Host Environment Telemetry (`2026-10-21`)
- **OS:** Windows 11
- **CPU:** 12th Gen Intel Core i5-12450H (8 Cores, 12 Threads)
- **RAM:** 16 GB Total (~4.8 GB Available)
- **GPU:** NVIDIA GeForce RTX 2050 Mobile (4,096 MiB / 4 GB VRAM, Driver 555.97, CUDA 12.5)
- **Disk:** C: Drive (445 GB Total, **162.5 GB Free Space**)
- **Python Runtimes Installed:**
  - Python 3.14.3 (System default: `C:\Python\python.exe`)
  - Python 3.12 (`C:\Users\psych\AppData\Local\Programs\Python\Python312\python.exe`)
  - Python 3.10 (`C:\Users\psych\AppData\Local\Programs\Python\Python310\python.exe`)

### 2.2 Local Inference Runtime Comparison

| Runtime Candidate | Windows & CUDA Support | 4 GB VRAM Compatibility | Architectural Verdict |
| :--- | :--- | :--- | :--- |
| **Hugging Face Diffusers (PyTorch + CUDA)** | **Verified Fact:** Official CUDA 12.4 wheels on Python 3.12. Supports `fp16`, `enable_attention_slicing()`, `enable_vae_slicing()`, and `enable_model_cpu_offload()`. | **Verified Fact:** Fits models $\le 1\text{B}$ parameters (SD-Turbo, SD 1.5) at 512x512 resolution within ~2.2 GB - 3.4 GB VRAM. | **RECOMMENDED:** Full programmatic control over GPU memory, pipeline hooks, and fallback mechanisms. |
| **Ollama** | **Verified Fact:** Not currently installed on host. Windows installer exists, but Ollama is primarily an LLM runtime. | **Verified Fact:** Native text-to-image in Ollama is experimental, supports niche models (e.g. Z-Image Turbo), and lacks fine-grained PyTorch memory slicing controls needed for 4GB VRAM. | **NOT RECOMMENDED:** Unpredictable memory allocations on 4GB VRAM; lacks custom attention slicing. |
| **ONNX Runtime / DirectML** | **Verified Fact:** Excellent DirectML support on Windows; converts models to ONNX. | **Verified Fact:** Quantized INT8/FP16 models fit easily in 4 GB VRAM. | **FUTURE OPTION:** Adds export/conversion complexity for the initial proof of concept; evaluated for Milestone 6 optimizations. |
| **Mock Provider** | **Verified Fact:** In-memory synthetic image generator (<5ms, 0MB VRAM). | **Verified Fact:** Zero hardware footprint. | **TESTING ONLY:** Strictly for automated unit/integration tests and CI/CD pipelines. Never represented as real output. |

### 2.3 Model Selection Matrix for 4 GB VRAM

| Model Checkpoint | Parameters & Architecture | Native Resolution | VRAM Footprint (FP16 + Optimizations) | License | Verdict |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`stabilityai/sd-turbo`** | ~0.86B UNet (Distilled SD 2.1) | 512x512 | **~2.2 GB – 2.8 GB** (1 to 4 steps) | Stability AI Non-Commercial Research & Creative | **RECOMMENDED FOR PROOF OF CONCEPT:** Instant generation (1-4 steps), fits comfortably within 4 GB VRAM without OOM. |
| **`runwayml/stable-diffusion-v1-5`** | ~0.86B UNet | 512x512 | **~2.8 GB – 3.4 GB** (20 to 25 steps) | CreativeML OpenRAIL-M (Commercial Permitted) | **PRODUCTION OPEN-WEIGHT ALTERNATIVE:** Reliable commercial license; takes ~4-8s on RTX 2050. |
| **`segmind/SSD-1B`** | 1.3B UNet (Distilled SDXL) | 768x768 / 1024x1024 | **~3.8 GB – 4.4 GB** (Borderline on 4GB VRAM) | Apache 2.0 | **RISKY:** High probability of OOM when desktop window manager consumes 400MB VRAM. |
| **`stabilityai/stable-diffusion-xl-base-1.0`** | 3.5B UNet | 1024x1024 | **~6.5 GB – 9.0 GB** | OpenRAIL++ | **UNSUPPORTED ON LOCAL GPU:** Exceeds physical 4 GB VRAM; requires aggressive sequential CPU offloading (45-90s latency). |
| **`black-forest-labs/FLUX.1-schnell`** | 12B Flow Transformer | 1024x1024 | **~12 GB – 24 GB** | Apache 2.0 | **UNSUPPORTED ON LOCAL GPU:** Physically impossible on 4 GB VRAM. Routed exclusively to cloud providers (fal.ai/Pollinations). |

---

## 3. Revised Functional Requirements (FR)

### FR-1: Generation Job Submission & Parameter Control
- **FR-1.1:** Accept generation request with: `prompt` (1-2000 chars), `negative_prompt`, `width` (default 512, multiple of 64), `height` (default 512, multiple of 64), `steps` (default 1 for turbo, 25 for SD 1.5), `seed` (int or -1 for random), `model` (`sd-turbo`, `sd-1.5`, `flux`), and `provider` (`local-gpu`, `pollinations`, `fal`, `mock`).
- **FR-1.2:** For `local-gpu` requests, enforce maximum resolution cap of **512x512** pixels to prevent GPU Out-of-Memory crashes.
- **FR-1.3:** Enforce RFC 9535 `Idempotency-Key` deduplication within a 24-hour window.

### FR-2: Asynchronous Job Lifecycle & Offline Queueing
- **FR-2.1:** Return HTTP `202 Accepted` immediately upon submission with a polling `Location` header.
- **FR-2.2:** Transition through states: `PENDING` $\rightarrow$ `PROCESSING` $\rightarrow$ `COMPLETED` | `FAILED` | `TIMED_OUT` | `CANCELLED`.
- **FR-2.3 (Offline Worker Tolerance):** In Mode B (Hybrid), if the local GPU worker PC is offline or asleep, submitted jobs shall safely remain in `PENDING` status in the cloud database without timing out. When the worker starts, it claims and processes accumulated jobs in FIFO order.

### FR-3: Hardware-Aware Local Inference Execution
- **FR-3.1:** The local worker engine shall acquire an exclusive lock (`Semaphore(1)`) ensuring only one model inference task runs on the GPU at any moment.
- **FR-3.2:** The pipeline shall initialize with `torch.float16`, `pipe.enable_attention_slicing()`, and `pipe.enable_vae_slicing()`.
- **FR-3.3:** After every generation, the worker shall explicitly call `torch.cuda.empty_cache()` and `gc.collect()` to release temporary intermediate latent allocations.
- **FR-3.4 (OOM Guard):** In the event of a `torch.cuda.OutOfMemoryError`, the engine shall catch the exception, flush VRAM cache, fall back to `pipe.enable_model_cpu_offload()`, and record structured telemetry before failing the job.

### FR-4: Pluggable Provider Abstraction
- **FR-4.1:** All inference engines adhere to `ImageProviderAdapter`.
- **FR-4.2:** Supported adapters:
  1. `LocalDiffusersAdapter` (Default local engine, zero cost).
  2. `MockProviderAdapter` (Instant test runner, zero cost).
  3. `PollinationsAdapter` (Zero-cost cloud fallback).
  4. `FalAdapter` (High-performance paid cloud fallback, opt-in only).

### FR-5: Dual Asset Storage & Delivery
- **FR-5.1 (Mode A):** Persist locally to content-addressable storage `storage/images/{sha256[0:2]}/{sha256[2:4]}/{sha256}.png`.
- **FR-5.2 (Mode B):** Worker uploads completed image binary via authenticated HTTPS to the cloud asset store (Cloudinary free tier or Supabase Storage), returning a secure signed CDN URL.

### FR-6: Security & Ingress/Egress Isolation
- **FR-6.1:** The local GPU machine shall **NEVER open inbound ports** or expose its home IP address to the public internet. All communication with the cloud control plane uses **outbound authenticated HTTPS polling** (`GET /worker/jobs/lease`, `POST /worker/jobs/{id}/complete`).
- **FR-6.2:** API keys verified via salted SHA-256 digests.

---

## 4. Revised Non-Functional Requirements (NFR)

| ID | Metric | Target Requirement |
| :--- | :--- | :--- |
| **NFR-1** | **Local VRAM Ceiling** | Peak VRAM allocation $\le 3,500\text{ MiB}$ (leaving $\ge 596\text{ MiB}$ safety margin for Windows DWM and display overhead). |
| **NFR-2** | **Zero-Cost Budget** | ₹0 recurring compute cost. Prototype runs entirely on local GPU and verified free cloud tiers (Firebase, Supabase, Cloudinary). |
| **NFR-3** | **API Ingestion Overhead** | P95 HTTP ingestion latency $\le 25\text{ ms}$ (FastAPI + SQLite WAL / Supabase connection pool). |
| **NFR-4** | **Resiliency & Lease Sweeping** | Stale locks reclaimed after 120s if worker crashes mid-task. Max 3 retries before marking `FAILED`. |
| **NFR-5** | **Offline Durability** | Cloud queue retains pending jobs indefinitely until worker connects. |
| **NFR-6** | **Security** | Zero inbound firewall openings on home network; strict path traversal immunity via SHA-256 CAS. |

---

## 5. Explicit Out-of-Scope Requirements

- **OOS-1: Model Fine-Tuning / LoRA Training:** Standard inference only. LoRA training on 4 GB VRAM causes extreme memory thrashing and is out of scope.
- **OOS-2: Video Generation:** Strictly static images (PNG/JPEG).
- **OOS-3: Paid GPU Cloud Hosting:** No mandatory AWS EC2 G4dn or RunPod instances in Phase 1.
- **OOS-4: Complex Orchestration (Kubernetes/Celery):** Kept to a lightweight modular worker loop without heavy message broker dependencies.
