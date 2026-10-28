# Aarav Image Generation API — Staging Deployment Manifest

- **Document Version:** 1.0.0
- **Phase:** Milestone 9A Preflight Preparation
- **Classification:** Operational Specification (Non-Secret / Placeholder Template)
- **Target Environment:** Staging (Dual Topology: Cloud Control Plane + On-Premise GPU Worker)

---

## 1. Executive Overview

This deployment manifest establishes the exact configuration, topology, access policies, operational runbooks, and verification gates required to transition the Aarav Image Generation API (`Inference-1`) from development into a controlled staging environment.

### Target Deployment Topology
- **Stateless Cloud Control Plane:** FastAPI ASGI application hosted on Linux container / VPS (e.g., Render Web Service, Fly.io, or AWS EC2 CPU instance).
- **Stateless Hybrid Inference Worker:** Standalone Python CLI daemon operating on dedicated hardware with an NVIDIA RTX 2050 Mobile GPU (4GB VRAM), connecting strictly via outbound HTTPS polling.
- **Managed Relational Persistence:** Supabase PostgreSQL with transaction pooler (port 6543) for runtime API queries, direct connection (port 5432) for DDL migrations, and Row-Level Security (RLS).
- **Content-Addressable Asset Storage:** Supabase Storage (or private S3-compatible bucket) with SHA-256 deduplication and private cache controls.

---

## 2. API Process Configuration Specification

The cloud API process must be configured with the following environment variables. In staging, all credentials must use cryptographically secure random values (minimum 256 bits).

| Variable Name | Required | Example / Placeholder Value | Purpose & Invariant Enforcement |
| :--- | :---: | :--- | :--- |
| `ENVIRONMENT` | **YES** | `production` | Enables strict production configuration validation gates. |
| `DEPLOYMENT_MODE` | **YES** | `hybrid` | Enforces Mode B distributed operation. |
| `DATABASE_URL` | **YES** | `postgresql+asyncpg://postgres.<STAGING_PROJECT_REF>:<DB_PASSWORD>@aws-0-<REGION>.pooler.supabase.com:6543/postgres?ssl=require` | Runtime connection string. Must use transaction pooler (port 6543) and enforce TLS (`ssl=require`). |
| `MIGRATION_DATABASE_URL` | **YES** | `postgresql+asyncpg://postgres.<STAGING_PROJECT_REF>:<DB_PASSWORD>@aws-0-<REGION>.pooler.supabase.com:5432/postgres?ssl=require` | Direct session connection string (port 5432) for Alembic DDL migrations and advisory locks. |
| `API_KEY_SALT` | **YES** | `<64_HEX_CHAR_CRYPTOGRAPHIC_SALT>` | Global cryptographic salt for client API key hashing. Must NOT match the development sentinel. |
| `WORKER_SHARED_SECRET` | **YES** | `<64_HEX_CHAR_WORKER_SECRET>` | Shared secret for authenticating hybrid worker requests via `Authorization: Bearer <token>`. |
| `ADMIN_API_KEY` | **YES** | `<64_HEX_CHAR_ADMIN_KEY>` | Master secret for authenticating `/metrics` scrapes and administrative APIs. |
| `METRICS_ENABLED` | **YES** | `true` | Enables Prometheus exposition at `GET /metrics`. |
| `METRICS_REQUIRE_AUTH` | **YES** | `true` | Enforces administrative authorization on the metrics endpoint. |
| `METRICS_CACHE_TTL_SECONDS` | NO | `5.0` | Throttling cache TTL to protect the database from rapid Prometheus scrapes. |
| `STORAGE_ADAPTER` | **YES** | `supabase` | Directs asset persistence to the staging cloud storage bucket. |
| `SUPABASE_URL` | **YES** | `https://<STAGING_PROJECT_REF>.supabase.co` | Base HTTPS URL for the staging Supabase project. Non-HTTPS is rejected. |
| `SUPABASE_STORAGE_BUCKET` | **YES** | `inference-assets-staging` | Target private storage bucket name. |
| `SUPABASE_SERVICE_ROLE_KEY` | **YES** | `<SUPABASE_SERVICE_ROLE_JWT_SECRET>` | Service-role secret for backend asset management. |
| `ENABLE_PHYSICAL_ASSET_DELETION` | **YES** | `false` | Master safety gate. Must remain `false` during staging preflight. |
| `RUN_EMBEDDED_WORKER` | **YES** | `false` | **CRITICAL:** Must be `false` in cloud deployments to prevent loading PyTorch on CPU instances. |
| `RUN_LEASE_SWEEPER` | **YES** | `true` | Runs background lease recovery daemon in the ASGI process. |
| `RUN_DELETION_OUTBOX_WORKER` | **YES** | `true` | Runs background tombstone processor in the ASGI process. |
| `DB_POOL_SIZE` | NO | `5` | Maximum active connections in the local SQLAlchemy pool. |
| `DB_MAX_OVERFLOW` | NO | `10` | Maximum overflow connections allowed during load spikes. |

---

## 3. GPU Worker Configuration Specification

The on-premise GPU worker process operates independently of the web server.

### Environment & Startup Parameters
```powershell
# Worker Environment Configuration (PowerShell)
$env:ENVIRONMENT = "production"
$env:CONTROL_PLANE_URL = "https://<STAGING_API_HOSTNAME>"
$env:WORKER_SHARED_SECRET = "<64_HEX_CHAR_WORKER_SECRET>"
$env:STORAGE_ADAPTER = "supabase"
$env:SUPABASE_URL = "https://<STAGING_PROJECT_REF>.supabase.co"
$env:SUPABASE_STORAGE_BUCKET = "inference-assets-staging"
$env:SUPABASE_SERVICE_ROLE_KEY = "<SUPABASE_SERVICE_ROLE_JWT_SECRET>"

# Worker Startup Command (CLI)
python -m src.worker.daemon `
  --control-plane-url "https://<STAGING_API_HOSTNAME>" `
  --worker-id "gpu-worker-node-01" `
  --provider "local-gpu"
```

### Worker Operational Safeguards
1. **Outbound HTTPS Only:** Listens on zero inbound WAN or LAN network ports.
2. **Lease Renewal Heartbeat:** Transmits lease renewals every 30 seconds (`POST /api/v1/worker/jobs/{id}/heartbeat`) during diffusion inference.
3. **Graceful Cancellation:** On `SIGINT` or `SIGTERM`, ceases polling for new work, allows active inference up to 30 seconds to conclude, or boundedly releases the job back to `PENDING` via `POST /api/v1/worker/jobs/{id}/fail` (`error_code="WORKER_CANCELLED"`).
4. **VRAM Protection:** Strict batch size of 1, FP16 precision, attention slicing enabled, bounded to 512x512 resolution (under 3.0 GB peak VRAM).

---

## 4. Database Prerequisites & Capabilities

The staging PostgreSQL database must satisfy the following technical requirements prior to startup:

1. **PostgreSQL Version:** PostgreSQL 15.x or 16.x.
2. **Extensions:**
   - `pgcrypto` (for UUID and cryptographic hash support).
   - `pg_stat_statements` (for query performance and lock monitoring).
3. **Connection Port Topology:**
   - **Port 6543 (Transaction Pooler):** Used exclusively by the runtime application (`DATABASE_URL`). Must specify `statement_cache_size=0` on `asyncpg` to prevent prepared statement conflicts.
   - **Port 5432 (Session Mode):** Used exclusively for Alembic DDL migrations (`MIGRATION_DATABASE_URL`). Required because transaction poolers do not support DDL transactions, advisory locks, or transactional table locks.
4. **Advisory Lock Capability:** PostgreSQL 64-bit transactional advisory locks (`pg_advisory_xact_lock`) must be supported for deduplicated CAS hash concurrency control.
5. **Transport Encryption:** TLS 1.3 (or 1.2 minimum) enforced on all database connections (`sslmode=require`).

---

## 5. Storage Bucket & Access Policy Prerequisites

1. **Bucket Configuration:**
   - Name: `inference-assets-staging`.
   - Access Mode: **Private** (Public bucket read disabled).
   - Allowed MIME Types: `image/png`, `image/jpeg`, `image/webp`.
   - Max File Size: 10,485,760 bytes (10 MB).
2. **Access Control & RLS Policies:**
   - Client access to raw storage objects is blocked.
   - All asset downloads flow through the authenticated API delivery endpoint (`GET /api/v1/images/generations/{id}/content`), which enforces client isolation and private `Cache-Control` headers.
   - Worker uploads authenticate using `SUPABASE_SERVICE_ROLE_KEY`.

---

## 6. Network Ingress & Egress Rules

```
+---------------------------------------------------------------------------------+
| Component             | Inbound Network Rules      | Outbound Network Rules     |
+-----------------------+----------------------------+----------------------------+
| Control Plane API     | Port 443 (HTTPS) from WAN  | Port 6543 (TCP) to DB      |
|                       | Port 80 (HTTP redirect)    | Port 5432 (TCP) to DB      |
|                       |                            | Port 443 to Supabase API   |
+-----------------------+----------------------------+----------------------------+
| GPU Inference Worker  | ZERO INBOUND PORTS         | Port 443 to API            |
|                       | (Behind NAT / Firewall)    | Port 443 to Storage API    |
+-----------------------+----------------------------+----------------------------+
| PostgreSQL DB         | Port 6543 from API IP      | None                       |
|                       | Port 5432 from Migration IP|                            |
+-----------------------+----------------------------+----------------------------+
| Storage Bucket        | Port 443 from Worker/API   | None                       |
+-----------------------+----------------------------+----------------------------+
```

---

## 7. Health & Metrics Verification

### Health Probes
- **Liveness Probe:** `GET /health/liveness`
  - Purpose: Validates that the ASGI event loop is active and accepting requests.
  - Expected Response: HTTP 200 OK `{"status":"alive","version":"1.0.0"}`.
  - Failure Action: Restart container process.
- **Readiness Probe:** `GET /health/readiness`
  - Purpose: Verifies database query execution (`SELECT 1`) and storage writability.
  - Expected Response: HTTP 200 OK `{"status":"ready","database":"connected","storage":"accessible"}`.
  - Failure Response: HTTP 503 Service Unavailable `{"status":"not_ready",...}`.
  - Failure Action: Remove instance from load balancer traffic routing until dependencies recover.

### Prometheus Scrape Verification
- **Metrics Endpoint:** `GET /metrics`
  - Expected Media Type: `text/plain; version=0.0.4; charset=utf-8`.
  - Authentication: Requires `X-Admin-API-Key: <ADMIN_API_KEY>` or `Authorization: Bearer <ADMIN_API_KEY>`.
  - Unauthenticated Request: HTTP 401 Unauthorized.
  - Key Telemetry Families:
    - `aarav_generation_requests_total`
    - `aarav_queue_depth{status="..."}`
    - `aarav_worker_status{worker_category="hybrid_gpu"}`
    - `aarav_worker_inference_duration_seconds`
    - `aarav_deletion_outbox_queue_depth{status="..."}`

---

## 8. Backup, Rollback & Disaster Recovery Runbook

### Backup Prerequisites
1. **Database:** Pre-migration logical export via `pg_dump` (Port 5432) verified on disk, complemented by Supabase platform daily backups (noting PITR requires paid Pro tier + add-ons).
2. **Schema & Migration Versioning:** The Alembic `alembic_version` table must record the exact applied revision before opening traffic.
3. **Storage:** Cloud object versioning or hourly replication snapshots enabled for `inference-assets-staging`.

### Rollback Criteria & Triggers
Deployment must be immediately aborted and rolled back if any of the following occur:
1. `GET /health/readiness` fails for more than 3 consecutive checks (45 seconds) post-startup.
2. Alembic migration execution produces a DDL error or schema mismatch.
3. Smoke-test generation fails to lease, complete, or deliver the generated asset.
4. Database connection pool exhaustion (`503` or timeout errors logged).
5. Unhandled exceptions occur on public routes.

### Rollback Steps

> [!IMPORTANT]
> **Application Rollback vs Database Schema Rollback:**
> Application rollback and database schema rollback are distinct procedures with different operational risks:
> - **Application Rollback (Step 2):** Immediately re-routes reverse proxy or container traffic to the previous known stable release image. Does NOT alter database schema or tables.
> - **Database Schema Rollback (Step 3):** Reverts DDL migrations via `alembic downgrade -1`. This is ONLY executed if database migrations were applied, if data compatibility requires it, and must strictly connect via `MIGRATION_DATABASE_URL` (direct port 5432, session mode). Never execute migrations or rollbacks against the transaction pooler (port 6543).

```powershell
# 1. Drain & stop on-premise GPU worker
# Send SIGINT / Ctrl+C to worker process; worker releases acquired leases.

# 2. Revert Control Plane API Deployment (Application Rollback)
# Re-route container traffic to the previous known stable commit / release container image.

# 3. Rollback Database Schema (Database Rollback — Requires Separate Authorization)
# Must connect via direct session port 5432 (MIGRATION_DATABASE_URL):
alembic downgrade -1
# Verify current schema matches expected rollback revision:
alembic current

# 4. Verify Service Health on Rollback Target
curl -i https://<STAGING_API_HOSTNAME>/health/liveness
curl -i https://<STAGING_API_HOSTNAME>/health/readiness
```

---

## 9. Staging Smoke-Test Specification

The staging preflight must conclude with a single, controlled end-to-end smoke test validating the complete asynchronous pipeline:

1. **Client Request:** `POST /api/v1/images/generations` with `X-API-Key` and `Idempotency-Key`.
   - Expected: HTTP 202 Accepted, `Location` header, response `status="PENDING"`.
2. **Queue Claim:** Worker polls `POST /api/v1/worker/jobs/lease` with `Authorization: Bearer <WORKER_SHARED_SECRET>`.
   - Expected: HTTP 200 OK with job payload.
3. **Heartbeat:** Worker renews lease via `POST /api/v1/worker/jobs/{id}/heartbeat`.
   - Expected: HTTP 200 OK `{"status":"renewed"}`.
4. **Execution & Storage:** Worker performs inference and uploads generated PNG to `inference-assets-staging`.
5. **Completion Report:** Worker calls `POST /api/v1/worker/jobs/{id}/complete` with `file_hash`, dimensions, and execution time.
   - Expected: HTTP 200 OK `{"status":"completed"}`.
6. **Client Polling:** Client queries `GET /api/v1/images/generations/{id}`.
   - Expected: HTTP 200 OK, `status="COMPLETED"`, asset URLs populated.
7. **Asset Download:** Client queries `GET /api/v1/images/generations/{id}/content`.
   - Expected: HTTP 200 OK, `image/png`, SHA-256 matches `file_hash`.
8. **Telemetry Verification:** `GET /metrics` confirms incremented counters.

---

## 10. Explicit Authorization Gates

Under the project's engineering discipline protocol, each operational stage is strictly decoupled into independent gates.

> [!CAUTION]
> **Mandatory Governance Invariants:**
> 1. **Separate Explicit Written Authorization:** Each gate requires its own separate, explicit written authorization before execution.
> 2. **No Cascading Authorization:** Approval of one gate (such as Gate 01) does **NOT** authorize any subsequent gate (Gates 02–05).
> 3. **Verified Backup Prerequisite:** A verified pre-migration PostgreSQL logical backup (`pg_dump`) on disk is an absolute prerequisite before Gate 01 (DDL execution) can be authorized or performed.
> 4. **Scope of Current Preflight:** No live migration, remote storage mutation, cloud service launch, GPU worker connection, or remote smoke-test execution is authorized by this preflight preparation task.

| Gate ID | Operation | Risk Level | Target Destination | Mandatory Prerequisite | Authorization Status |
| :--- | :--- | :---: | :--- | :--- | :---: |
| **GATE-01** | Running Alembic migrations against live PostgreSQL (`alembic upgrade head`) | **HIGH** (DDL mutation) | Staging Supabase PostgreSQL (Port 5432 Session Pooler) | Verified pre-migration `pg_dump` backup on disk with non-zero size and verified table of contents | **HOLD / NOT AUTHORIZED** |
| **GATE-02** | Provisioning real staging client API keys | **MEDIUM** (Data insert) | Staging PostgreSQL `api_keys` table | GATE-01 completed & schema verified; separate explicit authorization | **HOLD / NOT AUTHORIZED** |
| **GATE-03** | Deploying API service container to cloud hosting provider | **MEDIUM** (Host deployment) | Cloud Container / VPS (`RUN_EMBEDDED_WORKER=false`) | GATE-01 & GATE-02 completed; lean container image built/verified; separate explicit authorization | **HOLD / NOT AUTHORIZED** |
| **GATE-04** | Connecting local GPU worker daemon to live cloud API | **LOW** (Outbound traffic) | Dedicated Workstation (NVIDIA RTX 2050 Mobile) | GATE-03 deployed & `/health/readiness` returning 200; separate explicit authorization | **HOLD / NOT AUTHORIZED** |
| **GATE-05** | Live remote smoke-test execution | **LOW** (Synthetic verification) | Public Staging API Endpoint | GATE-04 active & outbound polling verified; runner executed with `--allow-remote`; separate explicit authorization | **HOLD / NOT AUTHORIZED** |

> [!IMPORTANT]
> **Physical Asset Deletion Safety Invariant:**
> Physical asset deletion (`ENABLE_PHYSICAL_ASSET_DELETION=true`) remains **STRICTLY DISABLED / BLOCKED** across all environments and is **NOT** part of GATE-01 through GATE-05. If deletion enablement is ever considered in the future, it must be treated as a separate post-staging change requiring its own dedicated Architecture Decision Record (ADR), explicit approval, verification plan, and rollback procedure.
