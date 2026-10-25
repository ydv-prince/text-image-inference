# Deployment & Infrastructure Plan — Aarav Image Generation API

**Document Version:** 2.0.0 (Local GPU First, Cloud Ready Revision)  
**Status:** PROPOSED (Pending Review Gate)  
**Author:** Principal System Design Engineer & Infrastructure Lead  
**System:** Aarav Image Generation API (`Inference-1`)

---

## 1. Dual Deployment Modes

### Mode A: Local Development Monolith (Single-Machine)
Designed for offline development, local experimentation, and zero-cost debugging:
```powershell
# 1. Activate isolated Python 3.12 environment
.\.venv\Scripts\Activate.ps1

# 2. Run database migrations
alembic upgrade head

# 3. Start local ASGI server with embedded queue worker
python src/main.py
```
- Listens on `http://localhost:8000`.
- Uses SQLite WAL (`aarav_dev.db`).
- Executes SD-Turbo directly on local NVIDIA RTX 2050 Mobile GPU.
- Stores images in `./storage/images/`.

---

### Mode B: Hybrid Hosted Deployment (Distributed Free-Tier)

Separates frontend, control plane, persistent database, asset CDN, and GPU compute across decoupled platforms:

```text
                                  +-----------------------+
                                  |   Firebase Hosting    | (Static Web Frontend)
                                  |   (Free Spark Plan)   |
                                  +-----------+-----------+
                                              | HTTPS
                                              v
+-----------------------+         +-----------+-----------+
|  Cloudinary Storage   | <====== |   Control Plane API   | (FastAPI on Render / Fly.io)
|  (Free 25 Credits/mo) |         |      (Free Tier)      |
+-----------+-----------+         +-----------+-----------+
            ^                                 |
            |                                 v
            | Direct Upload       +-----------+-----------+
            | (Signed)            |  Supabase PostgreSQL  | (Durable Queue & Metadata)
            |                     |      (Free Tier)      |
            |                     +-----------+-----------+
            |                                 ^
            | Outbound HTTPS                  | Outbound Polling
            |                                 | (Bearer Token)
+-----------+---------------------------------+-----------+
|                                                         |
|         Local GPU Worker (Developer PC)                 |
|         - Python 3.12 + PyTorch CUDA 12.4               |
|         - NVIDIA GeForce RTX 2050 Mobile (4 GB VRAM)    |
|         - Zero Inbound Ports / Behind Home NAT          |
|                                                         |
+---------------------------------------------------------+
```

---

## 2. Service Separation & Free-Tier Quota Breakdown

| Role | Provider / Platform | Tier & Limits | Responsibility | Can it run GPU Inference? |
| :--- | :--- | :--- | :--- | :--- |
| **Frontend CDN** | **Firebase Hosting** | Spark (Free): 10 GB storage, 360 MB/day transfer | Serves HTML, CSS, JavaScript, and user dashboard. | **NO.** (Static web files only; cannot run Python or GPU code). |
| **Control Plane API** | **Render / Fly.io / VPS** | Free Web Service (512 MB RAM, sleeps after inactivity) | Ingests requests, validates keys, inserts jobs into PostgreSQL. | **NO.** (CPU-only with 512 MB RAM; crashes if loading PyTorch). |
| **Durable Job Queue** | **Supabase PostgreSQL** | Free Tier: 500 MB DB, 50,000 monthly active users | Stores API keys, job queue, and transaction leases. | **NO.** (Relational SQL database). |
| **Asset Storage & CDN**| **Cloudinary / Supabase**| Free Tier: 25 monthly credits (~25 GB bandwidth) | Delivers optimized images with caching headers. | **NO.** (Binary blob storage and CDN). |
| **GPU Inference Worker**| **Local Home PC** | Self-hosted (RTX 2050 4GB VRAM) | Executes SD-Turbo diffusion inference and uploads assets. | **YES.** (Physical CUDA hardware). |

---

## 3. Important System Design Lessons: Domains, Servers, and GPUs

### A. A Domain is NOT a Server
- A **domain name** (e.g. `aarav-ai.com` or `aarav-api.in`, costing ~₹800 to ₹1,200/year) is purely a human-readable text entry in the global Domain Name System (DNS).
- Purchasing a domain gives you **zero CPU, zero RAM, and zero storage**. It simply routes traffic to an IP address or CNAME (like your Firebase or Render URL).
- For the initial prototype, purchasing a custom domain is **unnecessary**, because Firebase Hosting provides free `*.web.app` and `*.firebaseapp.com` domains, and Render provides free `*.onrender.com` subdomains with free SSL certificates!

### B. A Low-Cost CPU VPS is NOT a GPU Substitute
- Low-cost VPS providers (e.g., Hetzner, DigitalOcean, Linode) offer small virtual machines starting at $4 to $6/month (₹350 to ₹500/mo).
- These servers provide 1 or 2 shared vCPUs and 1 GB to 2 GB of RAM with **no dedicated GPU**.
- Running diffusion models on a CPU takes between **120 and 300 seconds (2 to 5 minutes) per image** and will peg the CPU at 100%, causing HTTP timeouts and host suspension.
- True cloud GPU instances (like RunPod, Modal, or AWS EC2 G4dn) cost between **$0.20 and $0.80 per hour**, translating to ₹12,000 to ₹45,000 per month if left running continuously.
- **The Hybrid Architecture Advantage:** By keeping the control plane on free cloud tiers and routing inference to your **existing RTX 2050 Mobile GPU**, you achieve production-grade responsiveness at **₹0 total recurring cost**.

---

## 4. Runbook: Starting Mode B Hybrid Worker

On the local Windows development machine:

```powershell
# 1. Activate isolated Python 3.12 environment
.\.venv\Scripts\Activate.ps1

# 2. Set environment variables to point to hosted control plane
$env:ENVIRONMENT = "production"
$env:CONTROL_PLANE_URL = "https://aarav-inference-api.onrender.com"
$env:WORKER_SHARED_SECRET = "your-256-bit-worker-secret-here"
$env:CLOUDINARY_URL = "cloudinary://api_key:secret@cloud_name"

# 3. Launch the outbound polling worker
python -m src.services.hybrid_worker
```

*Expected Terminal Output:*
```text
[2026-10-21 23:15:00] [INFO] [hybrid_worker] Connecting to Control Plane: https://aarav-inference-api.onrender.com
[2026-10-21 23:15:01] [INFO] [hybrid_worker] Device detected: NVIDIA GeForce RTX 2050 Mobile (4096 MiB VRAM)
[2026-10-21 23:15:02] [INFO] [hybrid_worker] Loaded SD-Turbo in FP16 with attention slicing. Initial VRAM: 2180 MiB.
[2026-10-21 23:15:04] [INFO] [hybrid_worker] Polling for jobs... (Queue idle, sleeping 2s)
```
