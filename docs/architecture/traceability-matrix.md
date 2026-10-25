# Requirements Traceability Matrix — Aarav Image Generation API

**Document Version:** 2.0.0 (Local GPU First, Cloud Ready Revision)  
**Status:** PROPOSED (Pending Review Gate)  
**Author:** Principal System Design Engineer  
**System:** Aarav Image Generation API (`Inference-1`)

---

## 1. Traceability Mapping

| Req ID | Requirement Summary | Architectural Components | Database Entities | Test Verification Suites |
| :--- | :--- | :--- | :--- | :--- |
| **FR-1.1** | Submit Generation Request (Prompt, dimensions, model, seed) | `api.v1.generations`, `services.generation_service` | `generation_jobs` | `tests/unit/test_schemas.py`<br>`tests/integration/test_generations_api.py` |
| **FR-1.2** | Local GPU Resolution Boundary (512x512 cap) | `api.v1.schemas.GenerationCreateRequest` | `generation_jobs.width`, `height` | `tests/integration/test_generations_api.py::test_generation_request_validation` |
| **FR-1.3** | Atomic Idempotency Protection (`Idempotency-Key` + canonical SHA-256) | `services.generation_service` | `generation_jobs.idempotency_key`, `payload_hash`, `uq_jobs_idempotency` | `tests/integration/test_generations_api.py::test_idempotency_*`<br>`tests/integration/test_generations_api.py::test_concurrent_idempotent_requests` |
| **FR-1.4** | Per-Key Sliding Window Rate Limiting with `Retry-After` | `api.rate_limiter.SlidingWindowRateLimiter` | `api_keys.rate_limit_rpm` | `tests/unit/test_auth_and_rate_limit.py`<br>`tests/integration/test_generations_api.py::test_rate_limiting_enforcement` |
| **FR-2.1** | Asynchronous 202 Accepted with Location header | `api.v1.generations` | `generation_jobs.status` | `tests/integration/test_generations_api.py::test_generation_ingestion_success` |
| **FR-2.2** | Job State Machine Progression | `domain.state_machine`, `domain.models.JobStatus` | `generation_jobs.status` | `tests/unit/test_state_machine.py` |
| **FR-2.3** | Offline Worker Queue Durability | `infrastructure.repositories.job_postgres`, `job_sqlite` | `generation_jobs (status='PENDING')` | `tests/integration/test_queue_worker.py` |
| **FR-2.4** | Job Status Polling & Execution Telemetry | `api.v1.generations`, `services.generation_service` | `generation_jobs` | `tests/integration/test_generations_api.py::test_end_to_end_generation_lifecycle` |
| **FR-2.5** | Cross-Key Tenant Isolation & Access Denial (403 Forbidden) | `services.generation_service` | `generation_jobs.api_key_id` | `tests/integration/test_generations_api.py::test_cross_key_job_access_denial` |
| **FR-3.1** | Bounded GPU Concurrency (Semaphore=1) | `infrastructure.providers.local_diffusers` | N/A | `tests/unit/test_providers.py` |
| **FR-3.2** | Local SD-Turbo FP16 with Slicing | `infrastructure.providers.local_diffusers` | N/A | `tests/poc/verify_local_gpu.py` |
| **FR-3.3** | VRAM Memory Hygiene & Garbage Collection | `infrastructure.providers.local_diffusers` | `generation_jobs.peak_vram_mb` | `tests/poc/verify_local_gpu.py` |
| **FR-3.4** | CUDA OOM Catch & Graceful Fail | `infrastructure.providers.local_diffusers` | `generation_jobs.error_code` | `tests/unit/test_providers.py` |
| **FR-4.1** | Pluggable Provider Interface & Registry | `domain.interfaces.ImageProviderAdapter`, `registry` | N/A | `tests/unit/test_providers.py` |
| **FR-5.1** | Content-Addressable Storage (CAS) with SHA-256 | `infrastructure.storage.local_cas` | `generated_assets` | `tests/unit/test_cas_storage.py` |
| **FR-5.2** | Remote Private Storage Adapter (Mode B) | `infrastructure.storage.remote_adapter` | `generated_assets.public_url` | `tests/unit/test_remote_storage.py` |
| **FR-5.3** | Authorized PNG Delivery & Safe Private Caching | `api.v1.generations`, `infrastructure.storage.local_cas` | `generated_assets` | `tests/integration/test_generations_api.py::test_asset_delivery_and_private_caching` |
| **FR-6.1** | Outbound HTTPS Worker Polling (Zero Inbound Ports) | `services.hybrid_worker`, `api.v1.worker` | `generation_jobs.locked_by` | `tests/integration/test_worker_api.py`<br>`tests/integration/test_hybrid_worker.py` |
| **FR-6.2** | Salted SHA-256 API Key Verification & Revocation Check | `api.auth`, `core.security` | `api_keys.key_hash`, `is_active` | `tests/unit/test_auth_and_rate_limit.py`<br>`tests/integration/test_generations_api.py::test_auth_rejection_scenarios` |
| **FR-6.3** | Lifespan Worker & Sweeper Background Lifecycle | `main.lifespan` | N/A | `tests/integration/test_generations_api.py::test_application_lifespan_lifecycle` |
| **FR-6.4** | Worker Mutual Secret Auth & Distributed Rate Limiting | `api.worker_auth`, `api.rate_limiter` | `api_keys.rate_limit_rpm` | `tests/unit/test_worker_auth_and_rate_limit.py` |
| **FR-6.5** | Supabase PostgreSQL Row-Level Security Migration | `alembic.versions.7a1b9c2d3e4f` | `generation_jobs`, `generated_assets`, `api_keys` | `tests/integration/test_alembic_migrations.py` |
| **NFR-1** | VRAM Ceiling $\le 3,500\text{ MiB}$ on RTX 2050 | `infrastructure.providers.local_diffusers` | `generation_jobs.peak_vram_mb` | `tests/poc/verify_local_gpu.py` |
| **NFR-2** | Initial Operating Budget = ₹0 | Free-tier deployment topology | N/A | Architecture verification & cost audit |
| **NFR-3** | Lease Sweeping & Stale Lock Recovery | `services.lease_sweeper` | `generation_jobs.locked_at` | `tests/integration/test_queue_worker.py::test_expired_lease_recovered_by_sweeper` |
| **NFR-4** | Path Traversal Immunity via SHA-256 CAS | `infrastructure.storage.local_cas` | N/A | `tests/unit/test_cas_storage.py::test_cas_path_traversal_prevention` |
| **NFR-5** | PostgreSQL Concurrency Integration Limitation (Visible) | `PostgresJobRepository` | `generation_jobs` | Bounded until real PostgreSQL instance provisioned |

