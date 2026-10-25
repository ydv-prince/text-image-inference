# Milestone 6 Deployment-Readiness Plan: Controlled Hybrid Architecture

**Document Version:** 2.0.0  
**Status:** REVISED PROPOSAL — AWAITING EXPLICIT APPROVAL  
**Author:** Principal System Design Engineer & Infrastructure Lead  
**Scope:** Milestone 6 Production Deployment Protocol for `Inference-1`  
**Governing Standard:** `AGENTS.md` (Stage 4 Architecture Decisions & Stage 5 Approval Gate)  
**Safety Constraint:** Zero cloud provisioning, zero account creation, zero remote migrations, and zero public exposure without prior explicit user authorization.

---

## 1. Executive Summary & Core Invariants

The objective of Milestone 6 is to define a strictly controlled, verified, zero-cloud-spend hybrid deployment plan for the Aarav Image Generation API.

### 1.1 Core Architectural Invariants

1. **Dual-Mode Preservation (Local Mode A Untouched)**:
   - **Mode A (Local / Offline)**: Remains 100% operational using SQLite WAL (`sqlite+aiosqlite:///inference.db`), Content-Addressable Storage on local disk, and local mock or GPU inference. Zero cloud dependencies, zero external network calls, zero breaking changes to existing test suites (98 passing tests).
   - **Mode B (Controlled Hybrid Cloud)**: Cloud Control Plane handles public ingestion, rate limiting, and job queuing; persistent PostgreSQL manages durable state with forced Row-Level Security (RLS); local on-premise GPU worker executes heavy inference.

2. **Zero Inbound Ports on Host Machine**:
   - The on-premise GPU worker initiates **outbound-only HTTPS polling** to the Control Plane using a rotating bearer token (`Authorization: Bearer <WORKER_SHARED_SECRET>`).
   - **Zero inbound ports, zero router port forwarding, zero dynamic DNS, and zero firewall ingress rules** are required on the host system.

3. **Zero GPU Cloud Spend**:
   - Heavy inference runs 100% on local hardware: NVIDIA GeForce RTX 2050 Mobile (4 GB VRAM) executing Stable Diffusion 1.5 + LCM LoRA in ~1.3 seconds per 512x512 image (~2.9 GB peak VRAM).
   - Avoids commercial cloud GPU rental costs ($0.20–$0.80/hour; ₹12,000–₹45,000/month).

4. **Authenticated Proxied Asset Delivery**:
   - All private generated assets are delivered via authenticated FastAPI control plane streaming (`GET /api/v1/images/generations/{id}/content`) with tenant ownership checks (`job.api_key_id == api_key.id`).
   - The system does **not** rely on custom HMAC signatures to authenticate against third-party object storage. The underlying storage vault remains completely private with zero public read permissions.

5. **Strictly $0.00 Cloud Spend Target**:
   - Leverages official free-tier quotas on Render (web service) and Supabase (PostgreSQL 16 & storage).
   - Services are configured with hard quotas that pause or suspend operations rather than automatically incurring credit card overage charges.

---

## 2. Target Architecture & Trust Boundaries (Mode B)

### 2.1 Component & Network Topology

```mermaid
flowchart TD
    subgraph Internet ["Untrusted Public Network"]
        Client["API Consumers / Frontend Clients"]
    end

    subgraph CloudControlPlane ["Cloud Control Plane (Render Free Web Service)"]
        Ingress["FastAPI Gateway<br/>- API Key Salted SHA-256 Auth<br/>- Sliding-Window Rate Limiting<br/>- Payload Validation (Multiples of 64)"]
        JobRouter["Job Ingestion & Status<br/>- POST /api/v1/images/generations<br/>- GET /api/v1/images/generations/{id}"]
        AssetProxy["Authenticated Asset Proxy<br/>- GET /api/v1/images/generations/{id}/content<br/>- Tenant Ownership Verification<br/>- Private Cache-Control Streaming"]
        WorkerRouter["Worker Gateway (Private Endpoints)<br/>- /api/v1/worker/jobs/lease<br/>- /api/v1/worker/jobs/heartbeat<br/>- /api/v1/worker/jobs/complete<br/>- /api/v1/worker/jobs/fail"]
    end

    subgraph ManagedDB ["Managed Database (Supabase PostgreSQL 16)"]
        subgraph Ports ["Supabase Connectivity Split"]
            PoolerPort["Port 6543: Transaction Pooler (Supavisor)<br/>- Application Runtime (asyncpg)<br/>- Prepared Statement Cache Disabled"]
            DirectPort["Port 5432: Direct / Session Pooler<br/>- Alembic DDL Migrations (NullPool)<br/>- DDL, Locks & Schema Inspection"]
        end
        Queue["Durable Queue & Catalog Tables<br/>- generation_jobs (FOR UPDATE SKIP LOCKED)<br/>- api_keys (Client Metadata & Salted Hashes)<br/>- generated_assets (Content-Addressable Metadata)"]
        RLS["Forced Row-Level Security<br/>- relrowsecurity = true<br/>- relforcerowsecurity = true<br/>- SET LOCAL request.jwt.claim.api_key_id"]
    end

    subgraph PrivateVault ["Private Asset Storage (Supabase Storage / Cloudinary Vault)"]
        BlobStore["Private Bucket (Zero Public Access)<br/>- Key: sha256_hash.png<br/>- Readable only by Control Plane Service Role<br/>- Writeable only by Worker / Control Plane"]
    end

    subgraph LocalPremise ["On-Premise Inference Node (Developer Machine)"]
        HW["Hybrid Worker Daemon<br/>- Outbound-Only HTTPS Polling<br/>- Zero Inbound Ports<br/>- Periodic Lease Heartbeat (30s)"]
        GPU["Local GPU Inference Pipeline<br/>- NVIDIA RTX 2050 Mobile (4GB VRAM)<br/>- SD 1.5 + LCM LoRA (4 Steps, FP16)<br/>- Output: Content-Addressable PNG"]
    end

    Client -->|HTTPS Ingestion / Status| Ingress
    Ingress --> JobRouter
    Ingress --> AssetProxy
    JobRouter -->|Runtime Queries / Port 6543| PoolerPort
    AssetProxy -->|Ownership Verification / Port 6543| PoolerPort
    AssetProxy -->|Fetch Binary with Service Role| BlobStore
    PoolerPort --> Queue
    Queue --- RLS
    DirectPort -.->|Alembic DDL Migrations Only| Queue

    HW -->|Outbound HTTPS Poll (Bearer Token)| WorkerRouter
    WorkerRouter -->|Atomic Lease (SKIP LOCKED)| PoolerPort
    HW -->|Execute Inference (1.3s)| GPU
    HW -->|Upload PNG with Service Secret| BlobStore
    HW -->|Report Completion & Asset Metadata| WorkerRouter
    Client -->|Download PNG (X-API-Key)| AssetProxy
```

### 2.2 Concrete Private Asset-Delivery Flow

To avoid the security vulnerability of treating custom application HMAC signatures as cloud provider authorization, asset delivery uses an **Authenticated FastAPI Proxy Flow with Tenant Ownership Verification**:

```mermaid
sequenceDiagram
    autonumber
    actor Client as API Consumer
    participant API as FastAPI Control Plane
    participant DB as PostgreSQL (Port 6543)
    participant Storage as Private Asset Vault (Supabase Storage)

    Client->>API: GET /api/v1/images/generations/{id}/content<br/>Header: X-API-Key: <client_key>
    Note over API: 1. Hash key with salt: SHA-256(key + salt)<br/>2. Authenticate client
    API->>DB: Query api_keys WHERE key_hash = :hash AND is_active = true
    DB-->>API: Return api_key_id, tenant_id

    Note over API: 3. Verify tenant ownership
    API->>DB: Query generation_jobs JOIN generated_assets<br/>WHERE id = :id AND api_key_id = :api_key_id
    alt Job Not Found or Tenant Mismatch
        DB-->>API: No matching record
        API-->>Client: HTTP 403 Forbidden / 404 Not Found
    else Job Not Yet Completed
        DB-->>API: Job status = PENDING or PROCESSING
        API-->>Client: HTTP 409 Conflict (Job in progress)
    else Tenant Verified & Job Completed
        DB-->>API: Return asset metadata (file_hash, byte_size, storage_path)
        Note over API: 4. Fetch binary via internal backend credentials
        API->>Storage: GET /storage/v1/object/private-assets/{file_hash}.png<br/>Header: Authorization: Bearer <STORAGE_SERVICE_ROLE_KEY>
        Storage-->>API: Stream Binary PNG bytes
        Note over API: 5. Stream response with private cache headers
        API-->>Client: HTTP 200 OK<br/>Content-Type: image/png<br/>Cache-Control: private, no-transform, max-age=86400<br/>ETag: "{file_hash}"<br/>Content-Disposition: inline; filename="{id}.png"
    end
```

### 2.3 Trust Boundaries & Security Enclaves

| Trust Boundary | Interacting Entities | Protocol & Transport | Authentication & Access Controls | Threat Mitigations |
| :--- | :--- | :--- | :--- | :--- |
| **Boundary 1: Ingestion** | Client &rarr; Control Plane | HTTPS (TLS 1.3) | `X-API-Key` hashed with dedicated salt; distributed sliding-window rate limit | Prevents credential leaks; limits DDoS abuse; validates payload bounds before database write. |
| **Boundary 2: Runtime DB** | Control Plane &rarr; PostgreSQL Pooler | Async TLS (`sslmode=require`) via Port 6543 | Dedicated DB role; transaction-scoped `SET LOCAL request.jwt.claim.api_key_id` | Prevents connection-pool context leakage; enforces RLS across table owners via `FORCE ROW LEVEL SECURITY`. |
| **Boundary 3: DDL Migrations** | CI/Deployer &rarr; PostgreSQL Direct | TLS (`sslmode=require`) via Port 5432 | Direct DB admin role; session locks and advisory locks permitted | Prevents schema corruption caused by transaction multiplexers on port 6543. |
| **Boundary 4: Worker Gateway** | Local Worker &rarr; Control Plane | Outbound HTTPS (TLS 1.3) | `Authorization: Bearer <WORKER_SHARED_SECRET>` with zero-downtime rotation fallback | Zero inbound ports opened on home router; worker spoofing prevented by constant-time verification. |
| **Boundary 5: Asset Delivery** | Client &rarr; Control Plane Proxy | HTTPS (TLS 1.3) | Authenticated `X-API-Key` with strict tenant ownership check (`job.api_key_id == key.id`) | Storage bucket is 100% private. Prevents cross-tenant leaks, bucket enumeration, and signed-URL tampering. |

---

## 3. Supported Database Connectivity & Migration Architecture

### 3.1 Dual Connection Port Topology (Supabase Specific)

A critical architectural distinction must be enforced between **DDL Migrations** and **Application Transaction Pooling**:

```text
+-----------------------------------------------------------------------------------------+
|                               SUPABASE POSTGRESQL 16                                    |
+-----------------------------------------------------------------------------------------+
       |                                                         |
       v (Port 5432: Session / Direct Mode)                      v (Port 6543: Transaction Mode)
+------------------------------------+             +--------------------------------------+
|        ALEMBIC MIGRATIONS          |             |        FASTAPI RUNTIME POOL          |
+------------------------------------+             +--------------------------------------+
| - DDL (CREATE / ALTER TABLE)       |             | - High-concurrency query execution   |
| - Advisory locks (pg_advisory_lock)|             | - Connection multiplexing            |
| - Schema inspection & transactions |             | - Null statement cache (asyncpg)     |
| - Driver: asyncpg / psycopg2       |             | - Driver: asyncpg                    |
| - Engine Pool: NullPool            |             | - Engine Pool: AsyncAdaptedQueuePool |
+------------------------------------+             +--------------------------------------+
```

### 3.2 Detailed Connectivity Comparison

| Parameter | Application Runtime Connectivity | Alembic Migration Connectivity |
| :--- | :--- | :--- |
| **Target Port** | **Port 6543** (Supavisor Transaction Pooler) | **Port 5432** (Direct Connection or Session Pooler) |
| **Connection URL** | `postgresql+asyncpg://postgres.<ref>:<pwd>@aws-0-<region>.pooler.supabase.com:6543/postgres?ssl=require` | `postgresql+asyncpg://postgres.<ref>:<pwd>@aws-0-<region>.pooler.supabase.com:5432/postgres?ssl=require`<br/>*(or `db.<ref>.supabase.co:5432` if IPv6 is supported)* |
| **Pooler Mode** | `mode=transaction` | `mode=session` (or Direct) |
| **Session State** | Discarded after each transaction commit | Retained for entire connection session |
| **DDL Support** | **NO** (Transaction pooler rejects or corrupts DDL) | **YES** (Full support for DDL, locks, and migration transactions) |
| **Advisory Locks** | Not supported (multiplexing breaks lock tracking) | Fully supported |
| **asyncpg Requirement** | `statement_cache_size=0`<br/>`prepared_statement_cache_size=0` | `NullPool` (disposable connection per run) |
| **Environment Variable**| `DATABASE_URL` | `MIGRATION_DATABASE_URL` (falls back to `DATABASE_URL` if direct) |

### 3.3 Alembic Execution Protocol

Alembic must never be executed against the transaction pooler on port 6543. The deployment script dynamically resolves the migration connection string:

```powershell
# Set direct/session connection string for migration execution (Port 5432)
$env:MIGRATION_DATABASE_URL = "postgresql+asyncpg://postgres.<project-ref>:<db-password>@aws-0-<region>.pooler.supabase.com:5432/postgres?ssl=require"

# Execute Alembic migration upgrade
.venv\Scripts\alembic upgrade head
```

---

## 4. Provider Pricing, Official Quotas & Billing Mechanics

*Official provider specifications verified as of October 2026:*

### 4.1 Quota & Cost Breakdown

| Component | Provider & Tier | Hardware / Resource Specs | Inactivity / Idle Behavior | Free Quota Allocations | What Happens When Quotas Are Exceeded? | Potential Paid Triggers (Optional) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Control Plane API** | **Render**<br/>Free Web Service | 512 MB RAM<br/>0.1 vCPU<br/>Stateless (no disk) | **Spins down after 15 min** of zero incoming HTTP traffic.<br/>Cold start delay: **50–60 seconds**. | - 750 free instance hours / month<br/>- 5 GB monthly egress (Hobby workspace)<br/>- 500 build mins/month | **Service suspended / throttled** until billing cycle resets. No surprise billing on free accounts (no credit card required). | Render Starter: **$7.00/mo** (Always-on, 512MB RAM, 0.5 CPU, no sleeping). |
| **Database & Queue** | **Supabase**<br/>Free PostgreSQL 16 | 500 MB database<br/>Shared compute<br/>pgvector included | **Automatically paused after 7 days (168h)** of continuous inactivity.<br/>Data preserved. 1-click restore. | - 500 MB DB storage<br/>- 5 GB standard egress<br/>- 5 GB cached egress<br/>- 2 active projects | **Requests restricted under Fair Use**. Project pauses or fails writes when 500MB full. No auto-charge on free tier. | Supabase Pro: **$25.00/mo** (No pause, 8GB DB, 100GB storage, 250GB egress, point-in-time recovery). |
| **Private Asset Vault** | **Supabase Storage**<br/>Free Tier | S3-compatible private bucket | Always available (tied to Supabase project state). | - 1 GB file storage<br/>- 5 GB bandwidth / month | **Uploads fail (HTTP 413 / 507)** when 1 GB cap reached. Does not auto-charge credit card. | Included in Supabase Pro ($25/mo) with 100GB storage. |
| **Inference Compute** | **Local Hardware**<br/>RTX 2050 Mobile | 4 GB VRAM<br/>2048 CUDA Cores<br/>Ampere Architecture | Host OS power settings; worker polls when system is running. | Unlimited compute on-premise. | N/A (Zero cloud compute usage). | Cloud GPU (RunPod/Modal): **$0.20–$0.80/hr** &rarr; **NOT AUTHORIZED / NOT NEEDED**. |

### 4.2 Distinguishing Free Quotas vs Suspension vs Auto-Pause

```text
+------------------------------------------------------------------------------------+
|                               FREE TIER SAFEGUARDS                                 |
+------------------------------------------------------------------------------------+
| 1. HARD QUOTAS: Render (5 GB egress) and Supabase (500 MB DB, 1 GB storage) enforce|
|    hard caps. Requests are refused/suspended; cards are NOT charged automatically.  |
| 2. IDLE SPIN-DOWN: Render spins down after 15 min. First request takes 50-60s.     |
| 3. INACTIVITY AUTO-PAUSE: Supabase pauses DB after 7 days without queries. Data is |
|    safely preserved and resumed with 1 click in the Supabase web dashboard.        |
| 4. STRICT $0.00 INVARIANT: Deployment proceeds strictly under Free Tier guidelines.|
+------------------------------------------------------------------------------------+
```

---

## 5. Comprehensive Failure & Recovery Test Plan

Prior to public deployment, the hybrid architecture must pass 6 automated and runbook failure-recovery tests:

### Test 1: Cold-Start Latency & Idle Resume (Render 15-Minute Spin-Down)
- **Failure Condition**: The Render free instance spins down after 15 minutes of inactivity. A new client submission arrives while the service is dormant.
- **Test Protocol**:
  1. Allow test control plane to remain idle for 16 minutes until Render dashboard indicates `Sleeping`.
  2. Send `POST /api/v1/images/generations` from automated test harness.
  3. Measure total wake-up and response latency (expected: 50–60 seconds).
  4. Verify hybrid worker polling client: Worker must use `httpx.Timeout(timeout=60.0, connect=15.0)` and treat HTTP 502/504 responses during control plane wake-up as transient retryable states with jittered exponential backoff rather than terminating.
- **Pass Criteria**: Client request successfully completes with HTTP 202 Accepted; worker reconnects automatically without manual restart.

### Test 2: Database Inactivity Auto-Pause & 1-Click Recovery (Supabase 7-Day Pause)
- **Failure Condition**: Supabase automatically pauses the project after 7 days of inactivity.
- **Test Protocol**:
  1. Simulate paused database state by terminating database access or setting invalid host.
  2. Probe control plane `/health/liveness` &rarr; Must return HTTP 200 `{"status": "alive"}` (process is healthy).
  3. Probe control plane `/health/readiness` &rarr; Must return HTTP 503 `{"status": "unready", "database": "unhealthy"}` without unhandled crash.
  4. Trigger database restore (in production: 1-click in Supabase dashboard; in test: re-enable DB container/connection).
  5. Verify control plane connection pool recovers automatically within 30 seconds without requiring a service redeployment.
- **Pass Criteria**: Health probes accurately distinguish liveness from readiness; connection pool automatically self-heals upon database restoration.

### Test 3: Worker Disconnect, Reconnect & Stale Lease Rejection
- **Failure Condition**: Host machine loses internet connectivity or goes to sleep while actively processing a leased job (`status = PROCESSING`).
- **Test Protocol**:
  1. Worker claims Job J1 (`status -> PROCESSING`, `locked_by -> worker-A`, `lease_expires_at = NOW() + 120s`).
  2. Sever worker network connection (simulate crash/sleep).
  3. Allow lease timeout (120 seconds) to elapse.
  4. Control plane sweeper reclaims expired lease: Job J1 transitions back to `PENDING` with `retry_count = 1`.
  5. Restore worker network connection. Old worker attempts to complete Job J1 via `POST /api/v1/worker/jobs/complete`.
  6. Control plane must reject stale completion with **HTTP 409 Conflict** (`"Lease expired or held by another worker"`).
  7. Second worker polling cycle successfully leases Job J1 and completes it normally.
- **Pass Criteria**: Stale worker completion rejected; zero corrupted jobs; job retried and completed cleanly.

### Test 4: Partial / Interrupted Asset Upload Resilience
- **Failure Condition**: Worker network disconnects mid-transfer while uploading binary PNG to remote storage.
- **Test Protocol**:
  1. Simulate aborted HTTP stream during asset binary transmission.
  2. Verify storage vault state: Incomplete or truncated file must be discarded or quarantined; no corrupt record created in `generated_assets`.
  3. Verify job state: Job remains in `PROCESSING` until heartbeat expires, then re-queued to `PENDING` via sweeper.
  4. Subsequent worker run generates complete image, verifies SHA-256 checksum, and completes the upload atomically.
- **Pass Criteria**: No dangling or partial 0-byte assets registered; SHA-256 verification enforces byte integrity.

### Test 5: Authenticated Proxied Asset Delivery & Tenant Isolation
- **Failure Condition**: Client attempts cross-tenant download or unauthorized access to generated images.
- **Test Protocol**:
  1. Client A generates Job J1 with Key A. Asset is generated and stored.
  2. Client A requests `GET /api/v1/images/generations/{J1_id}/content` with Key A &rarr; **HTTP 200 OK**, `Content-Type: image/png`, valid PNG magic bytes (`\x89PNG\r\n\x1a\n`), `Cache-Control: private, no-transform, max-age=86400`.
  3. Client B requests `GET /api/v1/images/generations/{J1_id}/content` with Key B &rarr; **HTTP 403 Forbidden** (`"Access denied to this generation job"`).
  4. Unauthenticated request (no key or invalid key) &rarr; **HTTP 401 Unauthorized**.
  5. Direct access to storage bucket URL &rarr; **HTTP 403 / 404** (bucket is private, zero public access).
- **Pass Criteria**: Zero cross-tenant data leakage; storage bucket remains inaccessible to untrusted clients.

### Test 6: Database Backup & Disaster Recovery (pg_dump / pg_restore)
- **Failure Condition**: Database corruption, migration error, or accidental table drop.
- **Test Protocol**:
  1. Execute point-in-time logical backup over direct port 5432:
     ```powershell
     pg_dump --clean --if-exists -Fc --no-owner --no-privileges `
       -d "postgresql://postgres.<ref>:<pwd>@aws-0-<region>.pooler.supabase.com:5432/postgres" `
       -f "backup_pre_deployment.dump"
     ```
  2. Restore backup into a clean disposable PostgreSQL instance:
     ```powershell
     pg_restore --clean --if-exists -d "postgresql://test_user:test_pwd@127.0.0.1:5433/test_db" backup_pre_deployment.dump
     ```
  3. Verify all tables, primary keys, foreign keys, `alembic_version`, and RLS policies (`relrowsecurity = true`) are intact.
- **Pass Criteria**: Full database state cleanly restored with zero loss of schema or RLS security constraints.

---

## 6. Staged Deployment Sequence, Rollback Gates & Secrets

The deployment follows a strict 5-stage sequential progression. Progression to each subsequent stage requires passing all rollback gates and acceptance criteria.

```text
[Stage 1: Secrets & Local Verification] ---> Gate 1 Passed
                      |
                      v
[Stage 2: Supabase Provisioning & DDL]  ---> Gate 2 Passed
                      |
                      v
[Stage 3: Render Control Plane Deploy]  ---> Gate 3 Passed
                      |
                      v
[Stage 4: Local Hybrid Worker Launch]   ---> Gate 4 Passed
                      |
                      v
[Stage 5: Staged End-to-End Validation] ---> Gate 5 Final Approval
```

### Stage 1: Secret Generation & Local Artifact Verification
- **Prerequisites**: Developer PC; no cloud services required.
- **Secrets Required**:
  ```python
  API_KEY_SALT = secrets.token_hex(32)         # 64-char hex
  WORKER_SHARED_SECRET = secrets.token_hex(32) # 64-char hex
  ```
- **Actions**:
  1. Generate cryptographically strong random secrets using `secrets.token_hex(32)`.
  2. Verify all 98 automated unit and integration tests pass locally:
     ```powershell
     .venv\Scripts\pytest -q
     ```
- **Rollback Gate 1**: If any test fails, abort immediately. Zero cloud resources touched.
- **Acceptance Criteria**: 98 passed (or 91 passed, 7 skipped if local PostgreSQL container is offline); secrets securely stored in local password vault.

### Stage 2: Database Provisioning & Direct DDL Migrations (Port 5432)
- **Prerequisites**: Supabase project created manually via dashboard (Free Tier selected); direct connection string obtained.
- **Secrets & Config Required**:
  - `MIGRATION_DATABASE_URL`: `postgresql+asyncpg://postgres.<ref>:<pwd>@aws-0-<region>.pooler.supabase.com:5432/postgres?ssl=require`
  - `DATABASE_URL`: `postgresql+asyncpg://postgres.<ref>:<pwd>@aws-0-<region>.pooler.supabase.com:6543/postgres?ssl=require`
- **Actions**:
  1. Take pre-migration database snapshot.
  2. Execute Alembic migration against **Port 5432** (Direct/Session connection):
     ```powershell
     $env:MIGRATION_DATABASE_URL = "postgresql+asyncpg://postgres.<ref>:<pwd>@aws-0-<region>.pooler.supabase.com:5432/postgres?ssl=require"
     .venv\Scripts\alembic upgrade head
     ```
  3. Verify RLS policies and table constraints:
     ```sql
     SELECT relname, relrowsecurity, relforcerowsecurity 
     FROM pg_class 
     WHERE relname IN ('generation_jobs', 'generated_assets', 'api_keys');
     ```
- **Rollback Gate 2**: If migration fails or RLS is false, execute `.venv\Scripts\alembic downgrade base` and investigate.
- **Acceptance Criteria**: Schema at `head`; all 3 core tables report `relrowsecurity: true` and `relforcerowsecurity: true`.

### Stage 3: Control Plane Web Service Deployment (Render)
- **Prerequisites**: Render account (Hobby / Free Tier); GitHub repository linked.
- **Secrets & Config Injected into Render Dashboard**:
  - `ENVIRONMENT=production`
  - `DEPLOYMENT_MODE=hybrid`
  - `DATABASE_URL=postgresql+asyncpg://postgres.<ref>:<pwd>@aws-0-<region>.pooler.supabase.com:6543/postgres?ssl=require`
  - `API_KEY_SALT=<from Stage 1>`
  - `WORKER_SHARED_SECRET=<from Stage 1>`
  - `RUN_EMBEDDED_WORKER=false`
  - `LEASE_TIMEOUT_SECONDS=120`
  - `MAX_JOB_RETRIES=3`
- **Actions**:
  1. Deploy web service on Render (Build: `pip install -r requirements.txt`, Start: `uvicorn src.main:app --host 0.0.0.0 --port $PORT`).
  2. Run automated remote health probes:
     ```powershell
     curl -i https://<render-service>.onrender.com/health/liveness
     # Must return: HTTP 200 {"status": "alive"}
     curl -i https://<render-service>.onrender.com/health/readiness
     # Must return: HTTP 200 {"status": "ready", "database": "healthy"}
     ```
- **Rollback Gate 3**: If readiness probe fails or deployment crashes, trigger **Rollback** in Render dashboard to previous commit.
- **Acceptance Criteria**: Both probes return HTTP 200 over TLS 1.3.

### Stage 4: On-Premise Hybrid Worker Startup (Local GPU)
- **Prerequisites**: NVIDIA RTX 2050 Mobile (driver & CUDA operational); local virtual environment.
- **Worker Configuration (.env.worker)**:
  - `DEPLOYMENT_MODE=worker_only`
  - `CONTROL_PLANE_URL=https://<render-service>.onrender.com`
  - `WORKER_SHARED_SECRET=<from Stage 1>`
  - `STORAGE_ADAPTER=remote_vault` (or hybrid storage sync)
  - `LOCAL_GPU_CONCURRENCY=1`
- **Actions**:
  1. Start local worker daemon in terminal:
     ```powershell
     .venv\Scripts\python -m src.worker.daemon
     ```
  2. Worker establishes outbound-only HTTPS polling loop (`POST /api/v1/worker/jobs/lease`).
  3. Verify logs indicate successful authentication: `Worker authenticated; polling for jobs (interval=2.0s)`.
- **Rollback Gate 4**: If worker receives HTTP 401/403, verify `WORKER_SHARED_SECRET` matches control plane; worker stops cleanly without affecting control plane.
- **Acceptance Criteria**: Worker polling stably with zero inbound open ports; zero GPU VRAM leaks.

### Stage 5: End-to-End Validation, Security Verification & Acceptance
- **Actions**:
  1. Execute Test 5 (Create API key &rarr; Submit generation job &rarr; Lease by worker &rarr; GPU inference &rarr; Upload &rarr; Proxied download &rarr; Cross-tenant denial).
  2. Verify total cloud billing across Render and Supabase dashboards is strictly **$0.00**.
- **Rollback Gate 5**: If cross-tenant access succeeds or billing charges appear, shut down worker and suspend Render service immediately.
- **Acceptance Criteria**: All 8 Milestone 6 acceptance criteria fully satisfied.

---

## 7. Preservation of Local Mode A & On-Premise Worker Topology

### 7.1 Local Mode A Invariants (Zero Regression Guarantee)

Local Mode A represents the offline developer workflow and must never be broken by hybrid cloud configurations:

```text
[Developer Workstation: Mode A]
+-------------------------------------------------------------------------+
| - Database: SQLite WAL (inference.db)                                   |
| - Storage: Local FileSystem Content-Addressable Storage (data/storage/) |
| - Worker: Embedded in FastAPI or local process daemon                   |
| - Inference: MockProvider (offline unit tests) or LocalDiffusersAdapter  |
| - Cloud Connectivity: ZERO (Works completely offline without internet)   |
| - Cost: $0.00                                                           |
+-------------------------------------------------------------------------+
```

- When `DEPLOYMENT_MODE=local` (default), the application loads `sqlite+aiosqlite:///inference.db`.
- Local development API binds strictly to `127.0.0.1:8000` (`uvicorn src.main:app --host 127.0.0.1 --port 8000 --reload`), preventing local port exposure across the LAN or Wi-Fi network.
- All automated tests continue to execute against local SQLite and mock providers in under 9 seconds.
- No cloud credentials or network connections are initialized in Mode A.

### 7.2 On-Premise Worker Invariants (Zero Inbound Exposure)

The hybrid worker node operates behind typical consumer NAT and firewall boundaries:
- **No public IP requirement**: The worker host does not need a static or public IP address.
- **No firewall changes**: Windows Defender Firewall and home router NAT require zero modifications.
- **Outbound-only traffic**: Worker opens standard TLS 1.3 outbound HTTPS connections to `https://<render-service>.onrender.com/api/v1/worker/*`.
- **Bearer token security**: Every request includes `Authorization: Bearer <WORKER_SHARED_SECRET>`. The control plane validates this token using constant-time comparison (`secrets.compare_digest`).

---

## 8. Summary of Free vs Billable Boundaries

```text
+-----------------------------------------------------------------------------------------+
|                              STRICT $0.00 FREE BOUNDARY                                 |
+-----------------------------------------------------------------------------------------+
| 1. On-Premise GPU Inference (RTX 2050 Mobile): $0.00 / month                             |
| 2. Render Free Web Service: $0.00 / month (512 MB RAM, 0.1 vCPU, 5 GB egress, 15m sleep)|
| 3. Supabase Free PostgreSQL 16: $0.00 / month (500 MB DB, 5 GB egress, 7d auto-pause)   |
| 4. Supabase Storage Vault: $0.00 / month (1 GB storage, private bucket)                 |
+-----------------------------------------------------------------------------------------+
                                             |
                                             v (Only if manually & explicitly upgraded)
+-----------------------------------------------------------------------------------------+
|                              POTENTIAL BILLABLE TRIGGERS                                |
+-----------------------------------------------------------------------------------------+
| 1. Render Starter Plan: $7.00 / month   --> Removes 15m idle sleep & cold starts        |
| 2. Supabase Pro Plan:   $25.00 / month  --> Removes 7-day auto-pause, adds 8GB DB       |
| 3. Cloud GPU Instances: $150–$300/mo    --> STRICTLY REJECTED (Local GPU handles 100%)  |
+-----------------------------------------------------------------------------------------+
```

---

## 9. Proposed Next Actions (Awaiting User Authorization)

Per the strict safety and governance protocol in `AGENTS.md`, **no remote resources have been provisioned and no cloud deployments have been executed**. 

The proposed next actions for when the user is ready to proceed are:

1. **Review & Approval Gate**: User reviews this revised deployment plan (`docs/architecture/milestone-6-deployment-plan.md`).
2. **Phase 6.1 (Pre-Cloud Local Verification)**: Verify that the codebase cleanly supports `MIGRATION_DATABASE_URL` in `alembic/env.py` and disables statement caching on port 6543 in `src/infrastructure/database.py`. Run `.venv\Scripts\pytest -q` to guarantee zero regressions.
3. **Phase 6.2 (External Accounts & Free Services Setup)**: Once authorized, user registers free accounts on Supabase and Render (without payment details) and provides the project connection details via local environment variables.
4. **Phase 6.3 (Staged Rollout & Failure Verification)**: Execute Stages 1 through 5 sequentially under the explicit rollback gates defined above.
