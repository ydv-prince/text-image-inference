# Low-Level Design (LLD) — Aarav Image Generation API

**Document Version:** 2.0.0 (Local GPU First, Cloud Ready Revision)  
**Status:** PROPOSED (Pending Review Gate)  
**Author:** Principal System Design Engineer & ML Infrastructure Architect  
**System:** Aarav Image Generation API (`Inference-1`)

---

## 1. Module & Directory Structure

The codebase is organized as a Modular Monolith adhering to Clean / Hexagonal Architecture. The GPU worker can run embedded in Mode A or as an independent remote polling agent in Mode B.

```text
Inference-1/
├── .agents/                    # Agent role definitions & protocols
├── AGENTS.md                   # System Design First protocol
├── docs/                       # Architecture documentation & ADRs
│   ├── architecture/
│   │   ├── requirements.md
│   │   ├── HLD.md
│   │   ├── LLD.md
│   │   ├── data-model.md
│   │   ├── api-contracts.md
│   │   ├── security.md
│   │   ├── deployment.md
│   │   ├── testing-strategy.md
│   │   ├── traceability-matrix.md
│   │   └── risk-register.md
│   └── decisions/
│       ├── ADR-001-modular-monolith-architecture.md
│       ├── ADR-002-pluggable-provider-abstraction.md
│       ├── ADR-003-transactional-job-queue.md
│       ├── ADR-004-content-addressable-asset-storage.md
│       ├── ADR-005-local-gpu-diffusers-runtime.md
│       ├── ADR-006-hybrid-hosted-architecture.md
│       └── ADR-007-free-tier-stack-and-cost-model.md
├── storage/                    # Content-addressable binary image storage
│   └── images/
├── src/                        # Core application code
│   ├── main.py                 # ASGI application entrypoint
│   ├── core/
│   │   ├── config.py           # Pydantic v2 settings (supports Mode A & B)
│   │   ├── logging.py          # Structured JSON logging & correlation IDs
│   │   ├── telemetry.py        # GPU memory & latency telemetry
│   │   └── errors.py           # Domain exceptions & RFC 7807 problem details
│   ├── domain/                 # Pure domain business rules
│   │   ├── models.py           # GenerationJob, GeneratedAsset, ApiKey entities
│   │   ├── state_machine.py    # Strict state transition invariants
│   │   └── interfaces.py       # ABCs: ImageProviderAdapter, JobRepository, AssetStorage
│   ├── infrastructure/
│   │   ├── database.py         # SQLAlchemy async engine (SQLite or PostgreSQL)
│   │   ├── repositories/       # Concrete SQL repositories
│   │   │   ├── base.py
│   │   │   ├── job_sqlite.py   # SQLite atomic reservation (BEGIN IMMEDIATE)
│   │   │   └── job_postgres.py # PostgreSQL concurrent leasing (FOR UPDATE SKIP LOCKED)
│   │   ├── storage/
│   │   │   ├── local_cas.py    # Content-addressable local filesystem storage
│   │   │   └── cloudinary.py   # Cloudinary asset storage adapter (Mode B)
│   │   └── providers/          # Inference adapters
│   │       ├── base.py         # ImageProviderAdapter ABC
│   │       ├── local_diffusers.py # Direct PyTorch + CUDA SD-Turbo engine
│   │       ├── mock.py         # Deterministic synthetic adapter for tests
│   │       ├── pollinations.py # Free-tier cloud fallback
│   │       ├── fal.py          # Paid serverless cloud fallback
│   │       └── registry.py     # Provider router & circuit breaker
│   ├── services/
│   │   ├── generation_service.py # Ingestion & idempotency coordinator
│   │   ├── queue_worker.py     # Local embedded queue worker (Mode A)
│   │   ├── hybrid_worker.py    # Outbound HTTPS polling agent (Mode B)
│   │   ├── lease_sweeper.py    # Stale lock recovery background loop
│   │   └── auth_service.py     # API key hashing & quota management
│   └── api/
│       ├── router.py           # Route mounting
│       ├── middleware/         # Auth, correlation ID, rate limiting
│       └── v1/
│           ├── generations.py  # User generation endpoints
│           ├── assets.py       # Asset delivery endpoints
│           ├── worker_api.py   # Secure endpoints for Mode B worker polling
│           └── schemas.py      # Pydantic DTOs
└── tests/                      # Automated test suite
```

---

## 2. Hardware-Aware Local GPU Engine (`local_diffusers.py`)

To run inference safely inside 4096 MiB VRAM without triggering CUDA OOM crashes:

```python
import gc
import time
import torch
from diffusers import AutoPipelineForText2Image
from src.domain.interfaces import ImageProviderAdapter, ProviderRequest, ProviderResponse

class LocalDiffusersAdapter(ImageProviderAdapter):
    def __init__(self, model_id: str = "stabilityai/sd-turbo", device: str = "cuda"):
        self.model_id = model_id
        self.device = device
        self._pipe = None
        self._lock = asyncio.Lock()  # Strictly 1 inference task at a time

    def initialize(self):
        """Loads model into FP16 with 4GB VRAM memory slicing."""
        if self._pipe is None:
            self._pipe = AutoPipelineForText2Image.from_pretrained(
                self.model_id,
                torch_dtype=torch.float16,
                variant="fp16",
                use_safetensors=True
            ).to(self.device)
            
            # Crucial optimizations for 4 GB VRAM:
            self._pipe.enable_attention_slicing()
            self._pipe.enable_vae_slicing()
            # If system RAM allows, model CPU offload provides extra safety:
            # self._pipe.enable_model_cpu_offload()

    async def generate_image(self, req: ProviderRequest) -> ProviderResponse:
        async with self._lock:  # Bounded concurrency = 1
            if self._pipe is None:
                self.initialize()
            
            # Record initial VRAM
            torch.cuda.reset_peak_memory_stats()
            start_time = time.perf_counter()
            
            try:
                # Enforce safe resolution cap (512x512)
                width = min(req.width, 512)
                height = min(req.height, 512)
                
                # SD-Turbo uses 1 to 4 steps with guidance_scale=0.0
                generator = torch.Generator(device=self.device).manual_seed(req.seed) if req.seed >= 0 else None
                
                image = self._pipe(
                    prompt=req.prompt,
                    negative_prompt=req.negative_prompt,
                    width=width,
                    height=height,
                    num_inference_steps=req.steps if req.steps <= 4 else 1,
                    guidance_scale=0.0,
                    generator=generator
                ).images[0]
                
                duration_ms = int((time.perf_counter() - start_time) * 1000)
                peak_vram_mb = int(torch.cuda.max_memory_allocated() / (1024 * 1024))
                
                # Convert PIL image to PNG byte buffer
                buffer = io.BytesIO()
                image.save(buffer, format="PNG")
                raw_bytes = buffer.getvalue()
                
                return ProviderResponse(
                    image_bytes=raw_bytes,
                    mime_type="image/png",
                    width=width,
                    height=height,
                    duration_ms=duration_ms,
                    peak_vram_mb=peak_vram_mb,
                    provider_name="local-gpu"
                )
            except torch.cuda.OutOfMemoryError as e:
                # Emergency recovery
                torch.cuda.empty_cache()
                gc.collect()
                raise DomainGPUOOMException(f"CUDA OOM during inference: {str(e)}")
            finally:
                # Memory hygiene after every single call
                torch.cuda.empty_cache()
                gc.collect()
```

---

## 3. Database Queue Semantics: SQLite vs PostgreSQL

Because the system supports both local development and hosted multi-client deployments, we explicitly separate the queue semantics in dedicated repository adapters:

### 3.1 SQLite Transactional Queue (`job_sqlite.py` - Mode A)
SQLite is a serverless, file-based database. It handles concurrency via file locks and Write-Ahead Logging (WAL):
```sql
BEGIN IMMEDIATE;

SELECT id, prompt, width, height, steps, seed, model
FROM generation_jobs
WHERE status = 'PENDING'
  AND (locked_at IS NULL OR locked_at < datetime('now', '-120 seconds'))
ORDER BY created_at ASC
LIMIT 1;

UPDATE generation_jobs
SET status = 'PROCESSING',
    locked_by = :worker_id,
    locked_at = datetime('now'),
    started_at = coalesce(started_at, datetime('now'))
WHERE id = :selected_job_id
  AND status = 'PENDING';

COMMIT;
```

### 3.2 PostgreSQL Concurrent Queue (`job_postgres.py` - Mode B)
In Supabase / PostgreSQL, multiple worker processes might poll simultaneously. Standard `SELECT ... FOR UPDATE` would block other workers. We use `SKIP LOCKED` so workers seamlessly grab different jobs without contention:
```sql
UPDATE generation_jobs
SET status = 'PROCESSING',
    locked_by = :worker_id,
    locked_at = NOW(),
    started_at = COALESCE(started_at, NOW())
WHERE id = (
    SELECT id
    FROM generation_jobs
    WHERE status = 'PENDING'
      AND (locked_at IS NULL OR locked_at < NOW() - INTERVAL '120 seconds')
    ORDER BY created_at ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
RETURNING id, prompt, width, height, steps, seed, model;
```

> [!NOTE]
> PostgreSQL concurrency behavior is implemented in `src/infrastructure/repositories/job_postgres.py` using `SELECT ... FOR UPDATE SKIP LOCKED`. Cross-database live integration testing against an active PostgreSQL instance will be executed once the cloud staging environment is configured in later deployment milestones; automated tests currently execute against SQLite WAL.

---

## 4. Mode B: Outbound Polling Protocol & Endpoints

In Mode B, the local worker acts as an HTTP client polling the hosted control plane:

### 4.1 Worker Lease Endpoint: `POST /api/v1/worker/jobs/lease`
- **Request Headers:** `Authorization: Bearer <WORKER_SHARED_SECRET>`
- **Response:**
  - `200 OK`: Returns job data to process (`job_id`, `prompt`, `width`, `height`, `steps`, `seed`).
  - `204 No Content`: Queue is empty. Worker sleeps for 2 seconds.

### 4.2 Worker Heartbeat Endpoint: `POST /api/v1/worker/jobs/{job_id}/heartbeat`
- Extends the lease timestamp `locked_at = NOW()` if generation takes longer than 30s.

### 4.3 Worker Completion Endpoint: `POST /api/v1/worker/jobs/{job_id}/complete`
- **Request Payload:**
  ```json
  {
    "asset_url": "https://res.cloudinary.com/.../a39fbc48.png",
    "file_hash": "a39fbc481d6501f28b7e283b54d63a8a9bc6810c92bf76de6f827a513515ea21",
    "file_size_bytes": 452810,
    "width": 512,
    "height": 512,
    "duration_ms": 1420,
    "peak_vram_mb": 2480,
    "provider_used": "local-gpu"
  }
  ```
- **Action:** Updates job in Supabase to `COMPLETED` and inserts asset record.

---

## 5. Environment & Python Setup Commands (Windows PowerShell)

To guarantee that Python 3.12 is used without affecting the machine-wide Python 3.14:

```powershell
# 1. Create isolated Python 3.12 virtual environment
py -3.12 -m venv .venv

# 2. Activate virtual environment
.\.venv\Scripts\Activate.ps1

# 3. Upgrade pip and install build tools
python -m pip install --upgrade pip setuptools wheel

# 4. Install PyTorch with official CUDA 12.4 support for Windows
pip install torch torchvision --index-url https://download.pytorch.org/whl/cu124

# 5. Install Diffusers, Transformers, Accelerate, and FastAPI stack
pip install diffusers transformers accelerate safetensors fastapi uvicorn[standard] pydantic-settings sqlalchemy aiosqlite psycopg2-binary httpx pillow

# 6. Verify PyTorch recognizes the RTX 2050 GPU
python -c "import torch; print('CUDA Available:', torch.cuda.is_available()); print('Device Name:', torch.cuda.get_device_name(0)); print('VRAM Total (MB):', torch.cuda.get_device_properties(0).total_memory / (1024*1024))"
```
