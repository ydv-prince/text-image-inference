# Risk Register & Mitigation Strategy — Aarav Image Generation API

**Document Version:** 2.0.0 (Local GPU First, Cloud Ready Revision)  
**Status:** PROPOSED (Pending Review Gate)  
**Author:** Principal System Design Engineer & Security Reviewer  
**System:** Aarav Image Generation API (`Inference-1`)

---

## 1. Risk Evaluation Matrix

| Risk ID | Category | Risk Description | Severity | Likelihood | Impact Score | Mitigation Strategy | Validation & Testing Method |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **RSK-01** | **Hardware / VRAM** | Local NVIDIA RTX 2050 Mobile runs Out-of-Memory (OOM) during diffusion inference. | **HIGH** | **MEDIUM** | **CRITICAL** | Load `stabilityai/sd-turbo` in `fp16` with `enable_attention_slicing()` and `enable_vae_slicing()`. Cap resolution at 512x512. Bounded concurrency `Semaphore(1)`. Catch `torch.cuda.OutOfMemoryError` and invoke `empty_cache()`/`gc.collect()`. | Automated PoC script (`tests/poc/verify_local_gpu.py`) asserts peak VRAM $\le 3,500\text{ MiB}$. |
| **RSK-02** | **Availability / Hybrid** | Local worker PC is turned off, put to sleep, or loses home internet while public clients submit jobs in Mode B. | **MEDIUM** | **HIGH** | **MEDIUM** | Durable job queue in Supabase PostgreSQL retains `PENDING` jobs safely. Client status endpoint returns `PENDING` with estimated queue position. When PC reboots and worker starts, it drains the queue in FIFO order. | Automated test (`test_failures.py::test_offline_worker_queue`). |
| **RSK-03** | **Quotas / Free Tier** | Free-tier limits exceeded (e.g. Cloudinary 25 credits/month, Supabase 500 MB storage). | **LOW** | **MEDIUM** | **LOW** | Image deduplication via SHA-256 CAS prevents redundant uploads. Implement 30-day asset expiration sweeper to prune old images. Mode A is always available for unlimited local testing. | Unit test for content-addressable deduplication. |
| **RSK-04** | **Security / Ingress** | Exposing home IP or internal network to public internet threats. | **CRITICAL** | **LOW** | **CRITICAL** | Zero inbound firewall openings or port forwards. Communication between local worker and control plane is strictly **outbound HTTPS polling** using a 256-bit secret token. | Security audit confirming no listening sockets on public network interfaces. |
| **RSK-05** | **Integrity / Crash** | Worker process dies while holding a job lease. | **HIGH** | **MEDIUM** | **HIGH** | Row-level lease timestamp (`locked_at`). Hosted `LeaseSweeper` reclaims stale leases (> 120s) and resets jobs to `PENDING` with max 3 retries. | Chaos test killing worker process mid-generation (`test_worker_crash_recovery`). |
| **RSK-06** | **Cost / Spends** | Accidental cloud spend or hidden recurring fees. | **HIGH** | **LOW** | **HIGH** | Architectural guarantee of ₹0 initial spend. No paid cloud GPU instances or mandatory domains. Free tiers selected do not require payment upfront. | Architecture cost audit. |
| **RSK-07** | **Disk / Model Cache** | Hugging Face model weights exhaust local C: drive space. | **LOW** | **LOW** | **LOW** | C: drive has 162.5 GB free. SD-Turbo FP16 weights consume ~2.5 GB. Cache directory can be relocated to D: drive via `HF_HOME` if needed. | Telemetry verified in Stage 0 discovery. |
