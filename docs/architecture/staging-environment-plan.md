# Inference-1 — Staging Environment Preparation Plan

- **Document Version:** 1.6.0 (Milestone 9B.6 Container Runtime Contract Audit)
- **Status:** PROPOSAL & OPERATIONAL SPECIFICATION (Pre-Deployment Planning Only)
- **System:** Aarav Image Generation API (`Inference-1`)
- **Target Environment:** Staging (Hybrid Topology: Cloud Control Plane + Local GPU Worker)
- **Author:** Principal System Design Engineer & Technical Lead

---

## 1. Executive Summary & Architecture Topology

This document establishes an actionable, cost-conscious, technically verified staging preparation plan for the Aarav Image Generation API (`Inference-1`). It operationalizes the approved architecture defined in [`docs/architecture/staging-deployment-manifest.md`](staging-deployment-manifest.md) and [`docs/runbooks/staging-readiness.md`](../runbooks/staging-readiness.md), incorporating verified provider constraints, database connection topologies, background daemon lifecycles, and authorization gate ordering.

### Staging Topology Architecture Diagram

```mermaid
flowchart TD
    subgraph Public Internet
        Client[Public Web / Mobile Client]
        Smoke[Staging Smoke Test Runner\nscripts/staging_smoke_test.py --allow-remote]
    end

    subgraph Cloud Infrastructure ["Cloud Control Plane (Render / Linux Container / VPS)"]
        API["FastAPI ASGI Gateway\n(Port 8000 / Uvicorn)\nRUN_EMBEDDED_WORKER=false"]
        Sweeper["LeaseSweeper Daemon\n(In-Process ASGI Lifespan Task)\nReclaims Stale Job Leases"]
        Outbox["DeletionOutbox Worker\n(In-Process ASGI Lifespan Task)\nBatch Tombstone Processor"]
        Probes["Health Probes\n/health/liveness\n/health/readiness"]
        Metrics["Prometheus Telemetry\n/metrics (Auth Guarded)"]

        API --- Sweeper
        API --- Outbox
        API --- Probes
        API --- Metrics
    end

    subgraph Managed Persistence ["Supabase Managed Cloud (PaaS)"]
        PoolerTx["Supavisor Transaction Pooler (Port 6543)\nIPv4 Compatible\nRuntime DATABASE_URL\nstatement_cache_size=0"]
        PoolerSession["Supavisor Session Pooler (Port 5432)\nIPv4 Compatible\nMIGRATION_DATABASE_URL\nDDL Migrations & Advisory Locks"]
        DirectDB["Direct PostgreSQL (Port 5432)\nIPv6 Only (Bypasses Pooler)\nRequires IPv4 Add-on on IPv4-Only WAN"]
        Storage["Supabase Private Storage\n(Bucket: inference-assets-staging)"]
    end

    subgraph On-Premises Workstation ["Developer Workstation (Behind Home NAT)"]
        Worker["Hybrid Worker Daemon\n(python -m src.worker.daemon)\nOutbound HTTPS Polling Only\nZero Inbound Ports"]
        GPU["NVIDIA GeForce RTX 2050 Mobile\n(4 GB VRAM / CUDA 12.4)\nSD-Turbo FP16 (Batch=1)"]
        Worker --- GPU
    end

    Client -->|HTTPS API Key| API
    Smoke -->|HTTPS API Key| API
    API -->|Async Runtime Queries| PoolerTx
    API -.->|Alembic DDL Migrations (Gate 01)| PoolerSession
    API -->|Asset Streaming Delivery /content| Storage
    Worker -->|Outbound Lease & Heartbeat (Bearer Token)| API
    Worker -->|Direct Binary Upload (Service Role)| Storage

    classDef cloud fill:#e1f5fe,stroke:#0288d1,stroke-width:2px;
    classDef paas fill:#ede7f6,stroke:#512da8,stroke-width:2px;
    classDef onprem fill:#e8f5e9,stroke:#388e3c,stroke-width:2px;
    class API,Sweeper,Outbox,Probes,Metrics cloud;
    class PoolerTx,PoolerSession,DirectDB,Storage paas;
    class Worker,GPU onprem;
```

---

## 2. Cloud Hosting Provider Evaluation & Background-Worker Suitability

The staging control plane requires a lightweight Python 3.10+ ASGI web service. It does **not** perform GPU inference, nor does it require local persistent disk storage (all durable state resides in Supabase PostgreSQL and Supabase Storage).

### Critical In-Process Daemon Lifecycle Reality
In `src/main.py:46-73`, background daemons (`LeaseSweeper` and `DeletionOutboxWorker`) are scheduled as in-process `asyncio.Task` instances within FastAPI's ASGI lifespan context. Their execution is strictly bound to the active CPU runtime of the web process:
1. **Sleeping Web Services (Scale-to-Zero):** On free hosting tiers (such as Render Free), the container spins down after 15 minutes of inbound HTTP inactivity. When the service sleeps, the ASGI event loop is suspended. Neither `LeaseSweeper` nor `DeletionOutboxWorker` runs while the container is asleep.
2. **Impact on Worker Leases:** If a local GPU worker claims a job and subsequently crashes, experiences power loss, or loses connectivity while the cloud API is sleeping, the lease cannot be reclaimed at timeout expiration. The job remains stranded in `PROCESSING` until a new external HTTP request wakes the API container (incurring a 25–45s cold start delay), at which point `LeaseSweeper` resumes and recovers the stale lease on its next sweep iteration.
3. **Prohibition of Synthetic Ping Loops:** Using scheduled cron pings (e.g., external uptime monitors pinging `/health/liveness` every 10 minutes) to artificially keep a free instance awake is strictly prohibited:
   - It violates Render's Terms of Service for Free web instances.
   - It depletes the monthly free allowance of 750 instance hours (744 hours in a 31-day month), leading to mid-month service suspension across the account.

### Provider Comparison Matrix

| Evaluation Criteria | Render (Web Service Free) | Render (Starter Tier) | Hetzner Cloud (CX22 VPS) | Fly.io (Hobby / Launch) |
| :--- | :--- | :--- | :--- | :--- |
| **Monthly Compute Cost** | **$0.00 / mo (₹0)** | **$7.00 / mo (~₹580)** | **€3.79 / mo (~$4.20 / ₹350)** | $0.00–$3.00 / mo (CC req.) |
| **Resource Quota** | 512 MB RAM, 0.1 vCPU, 750 hrs/mo | 512 MB RAM, 0.5 vCPU, Unmetered | 2 vCPU, 4 GB RAM, 40 GB NVMe | 256–512 MB RAM |
| **Filesystem Model** | Ephemeral | Ephemeral | Persistent 40 GB NVMe SSD | Ephemeral root |
| **Outbound Egress** | Unrestricted | Unrestricted | 20 TB included | Unrestricted |
| **Process Model / Sleep** | **Sleeps after 15m idle** | **Always On (Zero sleep)** | **Always On (Zero sleep)** | Auto-stop or continuous |
| **Cold-Start Latency** | 20 to 45 seconds | **0 seconds (Instantaneous)**| **0 seconds (Instantaneous)**| 1 to 3 seconds |
| **Background Daemons** | Suspended during sleep | Continuous 24/7 execution | Continuous 24/7 execution | Continuous (if configured) |
| **Secret Management** | Built-in environment dashboard| Built-in environment dashboard| Secure `.env` / systemd / vault | CLI (`fly secrets set`) |
| **Container / Docker** | Native Dockerfile / Git auto | Native Dockerfile / Git auto | Docker / Docker Compose | Native Dockerfile |
| **Automatic TLS** | Included (`*.onrender.com`) | Included (`*.onrender.com`) | Certbot / Let's Encrypt | Included (`*.fly.dev`) |

### Hosting Decision & Recommendation
* **Option A: Zero-Cost Staging (Render Web Service Free — $0.00 / mo):**
  - *Applicability:* Recommended for episodic, manually scheduled staging verification and development smoke tests.
  - *Operational Contract:* The team acknowledges that background daemons pause when idle. Before executing tests or launching the GPU worker, operators must pre-warm the container by calling `GET /health/liveness` to ensure the ASGI loop is active.
* **Option B: Continuous Background Execution (Hetzner Cloud CX22 VPS — ~€3.79 / $4.20 / mo OR Render Starter — $7.00 / mo):**
  - *Applicability:* Recommended if continuous 24/7 background lease sweeping, immediate zero-latency worker polling, and uninterrupted outbox tombstone processing are required without cold starts or sleep interruptions.

---

## 3. Database Connection Modes & Pooling Topology

Connecting a cloud ASGI service and local migration runners to Supabase requires strict protocol separation across ports and pooling modes.

### Supabase Connectivity Matrix

| Connection Mode | Target Host & Port | IP Version | Connection Model | Compatible Workloads | Incompatible Workloads |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Direct PostgreSQL Connection** | `db.<REF>.supabase.co:5432` | **IPv6 Only** (on modern free projects) | Dedicated physical Postgres backend per connection | Full Postgres features, DDL, LISTEN/NOTIFY | **Fails on IPv4-only networks** without Supabase paid IPv4 add-on ($4/mo) |
| **Supavisor Session Pooling** | `aws-0-<REGION>.pooler.supabase.com:5432` | **IPv4 Compatible** | Dedicated Postgres session per client connection | **Alembic DDL migrations**, advisory locks, temporary tables, prepared statements | High-concurrency short queries (connections limited to Postgres max) |
| **Supavisor Transaction Pooling** | `aws-0-<REGION>.pooler.supabase.com:6543` | **IPv4 Compatible** | Connection multiplexing at transaction boundary | **FastAPI runtime API queries**, high concurrency | **Alembic DDL**, session advisory locks, `LISTEN/NOTIFY`, named prepared statement caching |

### Asyncpg Prepared Statement Caching Mitigation
When using transaction pooling (port 6543), client connections are multiplexed across different Postgres server backends. Attempting to execute named prepared statements across different backends causes `DuplicatePreparedStatementError` or cache misses in `asyncpg`.
- **Verified Codebase Safeguard:** In `src/infrastructure/database.py:38-40`, the application engine automatically detects transaction poolers:
  ```python
  if ":6543" in db_url or "pooler" in db_url.lower():
      connect_args["statement_cache_size"] = 0
      connect_args["prepared_statement_cache_size"] = 0
  ```
  This completely eliminates statement cache collisions when connected via port 6543.
- **Migration Protocol:** Alembic migrations must **never** connect to port 6543. They must target Supavisor in Session Mode on port 5432 via `MIGRATION_DATABASE_URL`.

---

## 4. Backup & Disaster Recovery Baseline

### Clarification on PITR vs Logical Backups
- **Supabase Free Tier Backup Reality:** Supabase Free Tier does **not** include Point-In-Time Recovery (PITR). PITR is a paid feature requiring the Pro tier ($25/mo) plus a PITR retention add-on ($100/mo for 7-day retention).
- **Realistic Staging Disaster Recovery Strategy:**
  1. **Pre-Migration Logical Backup (`pg_dump`):** Before executing any DDL schema change (`GATE-01`), operators must generate and verify an offline logical snapshot of the staging database schema and data.
  2. **Supabase Daily Platform Snapshots:** Supabase automatically captures daily platform-level backups accessible from the project dashboard.
  3. **Verification Criterion:** GATE-01 authorization requires the successful generation, non-zero byte validation, and table-of-contents integrity verification of the pre-migration `pg_dump` file.

### Supported Credential Mechanisms & Security Analysis
Providing credentials to `pg_dump` requires evaluating security trade-offs, secret exposure risks, and automation compatibility:

| Credential Mechanism | Operator Interaction | Secret Exposure Risk | PowerShell Type Risk | Recommended Context |
| :--- | :--- | :--- | :--- | :--- |
| **Native Prompt (`pg_dump -W`)** | Interactive password entry | **Minimal:** Masked input in terminal; zero env var exposure; zero memory residue in shell | **Zero:** Bypasses PowerShell runtime entirely | **Primary Recommendation** for manual staging operator execution |
| **Session Variable (`PGPASSWORD`)** | Semi-interactive / script | **Medium:** Stored in plaintext in Process Environment Block (`PEB`); readable by same-user tools | **High Pitfall:** Direct assignment of `SecureString` yields `"System.Security.SecureString"` | Supported for scripted staging with mandatory `try...finally` cleanup |
| **Password File (`pgpass.conf`)** | Non-interactive | **Medium:** Plaintext on local disk; requires strict OS file ACLs (`icacls` / `chmod 0600`) | **Zero:** Read directly by `libpq` | Supported for continuous automated CI/CD runners |

> [!CAUTION]
> **PowerShell `SecureString` Assignment Pitfall:**
> If an operator assigns a PowerShell `SecureString` directly to an environment variable (`$env:PGPASSWORD = Read-Host -AsSecureString`), PowerShell coerces the object via `.ToString()`, which evaluates to the literal string `"System.Security.SecureString"`.
> `pg_dump` subsequently transmits this literal string as the password, failing authentication with `FATAL: password authentication failed for user ...`.
> If using `PGPASSWORD`, the secret must be properly unwrapped into session memory via `[System.Net.NetworkCredential]::new("", $sec).Password` and wrapped in a `try...finally` block to clear the environment immediately.

### Comprehensive Pre-Migration Backup Procedure

1. **Connection Mode Suitability & Authoritative Guidance:**
   - **Endpoint:** `aws-0-<REGION>.pooler.supabase.com:5432` (Supavisor Session Pooling).
   - **Rationale:** According to official Supabase documentation, native PostgreSQL client utilities (`pg_dump`, `pg_restore`, `psql`) require a dedicated session state. On IPv4 networks, Supavisor's Session Mode on port 5432 maintains a 1:1 dedicated connection to the backend Postgres engine, fully supporting `pg_dump`. Direct connections (`db.<REF>.supabase.co:5432`) require native IPv6 routing. Port 6543 (transaction pooling) is strictly incompatible with `pg_dump`.
2. **Secure Credential Handling (Zero Secret Leakage):**
   - Database passwords must **never** be supplied as command-line arguments or logged in shell history.
   - For manual runs, use `pg_dump -W` (Method A). For scripted runs, use ephemeral `$env:PGPASSWORD` with `try...finally` cleanup (Method B).
3. **Dedicated Backup Location & Source Control Exclusion:**
   - Dumps must be saved to a local dedicated directory: `backups/`.
   - `.gitignore` strictly excludes `backups/`, `*.dump`, and `*.sql` to prevent committing schema dumps or sensitive records.
   - Enforce restrictive NTFS filesystem permissions on the backup directory so only the current user has access:
     ```powershell
     icacls "backups" /inheritance:r /grant:r "$($env:USERNAME):(OI)(CI)F"
     ```
4. **Collision Protection (Refusal to Overwrite):**
   - The backup procedure generates a unique timestamped destination (`staging_pre_gate01_<timestamp>.dump`).
   - If the target file already exists, the procedure **must refuse to overwrite it** and abort immediately before invoking any native command.
5. **Immediate Exit-Code Capture & Failure Closed:**
   - Native utility exit codes must be captured immediately into variables (`$dumpExit = $LASTEXITCODE` and `$restoreExit = $LASTEXITCODE`).
   - Never rely on pipeline output or file size alone. If `pg_dump` exits with a non-zero code, the attempt is immediately aborted regardless of file size.
6. **File Existence and Non-Zero Size Verification:**
   - The procedure verifies that the output file exists and has size strictly greater than zero bytes (`(Get-Item $dumpPath).Length -gt 0`).
7. **Archive Integrity Checking via `pg_restore --list`:**
   - The custom-format (`-F c`) archive must be inspected with `pg_restore --list <dump_file>`. This parses the archive header and Table of Contents (TOC), validating that the file is uncorrupted and parseable without executing an actual restore or connecting to a database.
8. **Isolated Single-File Cleanup (Protecting Prior Backups):**
   - If `pg_dump` fails, file validation fails, or `pg_restore --list` fails, failure cleanup targets **only** the single incomplete file created by the current attempt (`Remove-Item -Path $dumpFile -Force`).
   - Older valid backups in `backups/` are **never** deleted or affected.
   - The failed step is logged clearly without exposing passwords or complete connection strings.
9. **Four-Tier Recovery & Restorability Boundary:**
   To maintain absolute engineering rigor, the following four verification tiers are strictly distinguished:
   - **Tier 1 — Process Exit:** `pg_dump` exited with code 0 (database connection, authentication, and dump stream completion succeeded).
   - **Tier 2 — Artifact Non-Emptiness:** Destination file exists and is strictly non-zero bytes (rules out 0-byte aborts).
   - **Tier 3 — Archive Integrity:** `pg_restore --list` exited with code 0 (header, compression, and table-of-contents catalog parsed without corruption).
   - **Tier 4 — Proven Restorability:** An actual restoration drill executed against a separate, isolated disposable database succeeded without constraint violations, missing roles, or DDL errors.
   - *Governance Rule:* **Tiers 1–3 satisfy the pre-migration gating check for GATE-01. Only Tier 4 establishes evidence of restorability.** Performing a Tier 4 restore drill against an isolated database is a separate future operation subject to explicit authorization, and is not executed during preflight.

---

#### Method A: Primary Recommended Command (Native `-W` Prompt)
For manual operators, `pg_dump -W` provides the highest security with zero environment variable exposure:

```powershell
# 1. Prepare directory with restricted ACLs
$backupDir = Join-Path (Get-Location) "backups"
if (-not (Test-Path $backupDir)) { New-Item -ItemType Directory -Path $backupDir | Out-Null }
icacls $backupDir /inheritance:r /grant:r "$($env:USERNAME):(OI)(CI)F" | Out-Null

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$dumpFile = Join-Path $backupDir "staging_pre_gate01_$timestamp.dump"

# 2. Refuse to overwrite existing backup
if (Test-Path $dumpFile) {
    throw "Refusing to overwrite existing backup file: $dumpFile"
}

# 3. Run pg_dump with native interactive password prompt (-W)
Write-Host "Running pg_dump via Supavisor Session Pooler (Port 5432)..."
pg_dump -h "aws-0-<REGION>.pooler.supabase.com" -p 5432 -U "postgres.<STAGING_PROJECT_REF>" -d "postgres" -W -F c -b -v -f $dumpFile

# 4. Capture $LASTEXITCODE immediately; fail closed if non-zero
$dumpExit = $LASTEXITCODE
if ($dumpExit -ne 0) {
    if (Test-Path $dumpFile) { Remove-Item -Path $dumpFile -Force }
    throw "Backup failed at pg_dump stage: native utility exited with code $dumpExit."
}

# 5. Verify file existence and non-zero size
if (-not (Test-Path $dumpFile) -or (Get-Item $dumpFile).Length -le 0) {
    if (Test-Path $dumpFile) { Remove-Item -Path $dumpFile -Force }
    throw "Backup failed at file validation stage: dump file is missing or 0 bytes."
}

# 6. Verify archive integrity (reads TOC without restoring)
Write-Host "Verifying archive integrity with pg_restore --list..."
pg_restore --list $dumpFile | Out-Null
$restoreExit = $LASTEXITCODE
if ($restoreExit -ne 0) {
    if (Test-Path $dumpFile) { Remove-Item -Path $dumpFile -Force }
    throw "Backup integrity check failed: pg_restore --list exited with code $restoreExit."
}

Write-Host "Pre-migration backup verified successfully: $dumpFile ($( (Get-Item $dumpFile).Length ) bytes)"
```

---

#### Method B: Hardened Scripted Execution (Ephemeral `$env:PGPASSWORD`)
When running in an automated PowerShell script:

```powershell
$backupDir = Join-Path (Get-Location) "backups"
if (-not (Test-Path $backupDir)) { New-Item -ItemType Directory -Path $backupDir | Out-Null }
icacls $backupDir /inheritance:r /grant:r "$($env:USERNAME):(OI)(CI)F" | Out-Null

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$dumpFile = Join-Path $backupDir "staging_pre_gate01_$timestamp.dump"

# 1. Refuse to overwrite existing backup
if (Test-Path $dumpFile) {
    throw "Refusing to overwrite existing backup file: $dumpFile"
}

# 2. Securely unwrap password into session memory only
$secPassword = Read-Host -Prompt "Enter Supabase Database Password" -AsSecureString
$env:PGPASSWORD = [System.Net.NetworkCredential]::new("", $secPassword).Password

try {
    # 3. Execute pg_dump
    Write-Host "Running pg_dump via Supavisor Session Pooler (Port 5432)..."
    pg_dump -h "aws-0-<REGION>.pooler.supabase.com" -p 5432 -U "postgres.<STAGING_PROJECT_REF>" -d "postgres" -F c -b -v -f $dumpFile

    # 4. Capture $LASTEXITCODE immediately; fail closed if non-zero
    $dumpExit = $LASTEXITCODE
    if ($dumpExit -ne 0) {
        if (Test-Path $dumpFile) { Remove-Item -Path $dumpFile -Force }
        throw "Backup failed at pg_dump stage: native utility exited with code $dumpExit."
    }

    # 5. Verify file existence and non-zero size
    if (-not (Test-Path $dumpFile) -or (Get-Item $dumpFile).Length -le 0) {
        if (Test-Path $dumpFile) { Remove-Item -Path $dumpFile -Force }
        throw "Backup failed at file validation stage: dump file is missing or 0 bytes."
    }

    # 6. Verify archive integrity (reads TOC without restoring)
    Write-Host "Verifying archive integrity with pg_restore --list..."
    pg_restore --list $dumpFile | Out-Null
    $restoreExit = $LASTEXITCODE
    if ($restoreExit -ne 0) {
        if (Test-Path $dumpFile) { Remove-Item -Path $dumpFile -Force }
        throw "Backup integrity check failed: pg_restore --list exited with code $restoreExit."
    }

    Write-Host "Pre-migration backup verified successfully: $dumpFile ($( (Get-Item $dumpFile).Length ) bytes)"
}
finally {
    # Ensure credential is immediately wiped from session memory
    $env:PGPASSWORD = $null
    Remove-Item Env:\PGPASSWORD -ErrorAction SilentlyContinue
    $secPassword = $null
}
```

---

## 5. Comprehensive Resource Inventory & Provisioning Checklist

All external infrastructure must be provisioned with secure placeholders prior to live execution gates.

### Resource Inventory Table

| Resource Item | Target Provider | Configuration Inputs / Specification | Owning Process | Verification Criterion |
| :--- | :--- | :--- | :--- | :--- |
| **Supabase Project** | Supabase Cloud | Region: Closest to worker (e.g., `ap-south-1` / Mumbai or `eu-central-1`) | PaaS Infrastructure | Dashboard active; project ref `<STAGING_PROJECT_REF>` available |
| **Runtime DB Connection** | Supavisor Transaction Pooler | Port **6543**, TLS enabled (`ssl=require`), `statement_cache_size=0` | Cloud API ASGI (`DATABASE_URL`) | `GET /health/readiness` returns `database: "connected"` |
| **Migration DB Connection**| Supavisor Session Pooler | Port **5432**, TLS enabled (`ssl=require`), session mode | Alembic CLI (`MIGRATION_DATABASE_URL`) | `alembic current` successfully inspects schema without pooler errors |
| **Private Asset Bucket** | Supabase Storage | Bucket Name: `inference-assets-staging`, Access: **Private**, Max: 10 MB | Worker uploads / API delivery | Upload and authenticated download via `/content` succeeds |
| **API Key Cryptographic Salt** | Local Cryptographic Generator | 64-hex-character random string (`python -c "import secrets; print(secrets.token_hex(32))"`) | Cloud API (`API_KEY_SALT`) | Settings validation passes; dev sentinel rejected |
| **Worker Shared Secret** | Local Cryptographic Generator | 64-hex-character random string (`secrets.token_hex(32)`) | Cloud API & GPU Worker | Worker lease endpoint rejects mismatched tokens with HTTP 401 |
| **Administrative API Key** | Local Cryptographic Generator | 64-hex-character random string (`secrets.token_hex(32)`) | Cloud API (`ADMIN_API_KEY`) | `/metrics` rejects unauthenticated scrapes with HTTP 401 |
| **Cloud API Service** | Render (or selected host) | CPU Instance, 512 MB RAM, Dockerfile runtime | Control Plane Web Process | `GET /health/liveness` returns HTTP 200 `{"status":"alive"}` |
| **Local GPU Worker Node** | Windows 11 Workstation | NVIDIA GeForce RTX 2050 Mobile (4GB), CUDA 12.1+, PyTorch 2.4+ | Hybrid Worker Daemon | Outbound polling logs successful job claims without OOM |
| **Pre-Migration Backup Verification**| Local Workstation / CLI | Pre-DDL logical export via `pg_dump` on port 5432 | Database Admin | Backup dump file created, non-empty, and verified before Gate 01 |

---

## 6. Redacted Environment Variable Matrices

### A. Cloud Control Plane API (`.env.staging.api`)
These variables must be injected into the cloud hosting container dashboard.

```ini
# =====================================================================
# Cloud Control Plane API — Staging Configuration (.env.staging.api)
# =====================================================================
ENVIRONMENT=production
DEPLOYMENT_MODE=hybrid
DEBUG=false

# Port 6543: Supavisor Transaction Pooler (IPv4) with asyncpg:
DATABASE_URL=postgresql+asyncpg://postgres.<STAGING_PROJECT_REF>:<DB_PASSWORD>@aws-0-<REGION>.pooler.supabase.com:6543/postgres?ssl=require

# Port 5432: Supavisor Session Pooler (IPv4) for Alembic migrations:
MIGRATION_DATABASE_URL=postgresql+asyncpg://postgres.<STAGING_PROJECT_REF>:<DB_PASSWORD>@aws-0-<REGION>.pooler.supabase.com:5432/postgres?ssl=require

# Database Connection Pool Tuning
DB_POOL_SIZE=5
DB_MAX_OVERFLOW=10
DB_ECHO=false

# Process Architecture (CRITICAL: Must remain false in cloud)
RUN_EMBEDDED_WORKER=false
RUN_LEASE_SWEEPER=true
RUN_DELETION_OUTBOX_WORKER=true
DELETION_OUTBOX_INTERVAL_SECONDS=15.0
DELETION_OUTBOX_BATCH_SIZE=10

# Master Safety Gate (CRITICAL: Must remain false during preflight)
ENABLE_PHYSICAL_ASSET_DELETION=false

# Asset Storage Tier
STORAGE_ADAPTER=supabase
SUPABASE_URL=https://<STAGING_PROJECT_REF>.supabase.co
SUPABASE_STORAGE_BUCKET=inference-assets-staging
SUPABASE_SERVICE_ROLE_KEY=<REDACTED_SUPABASE_SERVICE_ROLE_JWT>

# Observability & Metrics
METRICS_ENABLED=true
METRICS_REQUIRE_AUTH=true
METRICS_CACHE_TTL_SECONDS=5.0

# Cryptographic Authentication Secrets
API_KEY_SALT=<REDACTED_64_HEX_CHAR_SALT>
WORKER_SHARED_SECRET=<REDACTED_64_HEX_CHAR_WORKER_SECRET>
ADMIN_API_KEY=<REDACTED_64_HEX_CHAR_ADMIN_KEY>
```

### B. On-Premises GPU Worker Node (`.env.staging.worker`)
Configured locally on the GPU workstation.

```ini
# =====================================================================
# Hybrid GPU Worker Node — Staging Configuration (.env.staging.worker)
# =====================================================================
ENVIRONMENT=production
CONTROL_PLANE_URL=https://<STAGING_API_HOSTNAME>.onrender.com
WORKER_SHARED_SECRET=<REDACTED_64_HEX_CHAR_WORKER_SECRET>

# Direct Storage Persistence (Bypasses API for heavy binary transfer)
STORAGE_ADAPTER=supabase
SUPABASE_URL=https://<STAGING_PROJECT_REF>.supabase.co
SUPABASE_STORAGE_BUCKET=inference-assets-staging
SUPABASE_SERVICE_ROLE_KEY=<REDACTED_SUPABASE_SERVICE_ROLE_JWT>

# Hardware Protection (RTX 2050 Mobile 4GB VRAM)
LOCAL_GPU_CONCURRENCY=1
MAX_LOCAL_WIDTH=512
MAX_LOCAL_HEIGHT=512
DEFAULT_STEPS=4
MAX_STEPS=25
LEASE_TIMEOUT_SECONDS=120
WORKER_POLL_INTERVAL_SECONDS=1.0
```

---

## 7. Build, Packaging & Container Readiness Audit

### Packaging Audit Findings & Dependency Separation
A static inspection of [`pyproject.toml`](../../pyproject.toml) and provider source code reveals:
1. **Dependency Definition:** `diffusers`, `transformers`, `accelerate`, `safetensors`, and transitively PyTorch (`torch`) are declared in the base `dependencies = [...]` table of `pyproject.toml`.
2. **Runtime Lazy Import:** In `src/infrastructure/providers/local_diffusers.py:70-71`, heavy imports (`import torch`, `from diffusers import AutoPipelineForText2Image`) are lazily deferred inside `LocalDiffusersProvider._initialize_pipeline()`. When running as a stateless Cloud Control Plane (`RUN_EMBEDDED_WORKER=false`), the API boots without loading PyTorch or CUDA libraries into memory.
3. **Container Build Risk:** However, during image build, running standard `pip install .` on a cloud container would download and install ~3.5 GB of CUDA and PyTorch packages. On a 512 MB memory builder, this risks container build timeouts or out-of-memory errors.
4. **Resolution via Derived Manifest:** To eliminate this risk, a dedicated [`requirements-api.txt`](../../requirements-api.txt) artifact has been created at the repository root. It specifies the lean production API dependencies (FastAPI, Uvicorn, Pydantic-Settings, SQLAlchemy, Alembic, Asyncpg, Httpx, Pillow).
5. **Drift Prevention:** Parity between `requirements-api.txt` and `pyproject.toml` is statically enforced by unit tests in `tests/unit/test_staging_readiness.py`, ensuring that every API dependency matches `pyproject.toml` and no GPU-exclusive packages are inadvertently included.
6. **Artifact Status & Docker Daemon Availability:** The multi-stage [`Dockerfile.api`](../../Dockerfile.api) and [`.dockerignore`](../../.dockerignore) now exist as concrete, verified files on disk. Local image building requires an active Docker daemon (which is currently stopped on the local workstation; see Milestone 9B.5 report).

### Verified Production Multi-Stage Dockerfile (`Dockerfile.api`)

```dockerfile
# Stage 1: Build virtual environment with API dependencies only
FROM python:3.10-slim AS builder
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libpq-dev && \
    rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements-api.lock .
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements-api.lock

# Stage 2: Minimal runtime image
FROM python:3.10-slim AS runner
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 curl && \
    rm -rf /var/lib/apt/lists/*

RUN useradd -u 10001 -m -s /bin/bash appuser

COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

COPY src/ /app/src/
COPY alembic/ /app/alembic/
COPY alembic.ini /app/alembic.ini
COPY pyproject.toml /app/pyproject.toml

# Create runtime storage directories and set non-root ownership
RUN mkdir -p /app/storage/images && chown -R appuser:appuser /app

USER appuser

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/health/liveness || exit 1

# Single worker enforced for in-process background daemon safety (LeaseSweeper, DeletionOutboxWorker)
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--timeout-graceful-shutdown", "15"]
```

---

## 8. End-to-End Staging Deployment Sequence & Gate Ordering

The deployment sequence strictly adheres to component dependencies and decoupled authorization gates:

```mermaid
sequenceDiagram
    autonumber
    actor Admin as System Administrator / Human Lead
    participant Supabase as Supabase Cloud PaaS
    participant Container as Cloud API Container (Render)
    participant Worker as GPU Worker Node (Workstation)
    participant Smoke as Smoke Test Harness

    Note over Admin,Supabase: PHASE 1: Infrastructure & Credential Provisioning
    Admin->>Supabase: 1. Provision Staging Project & Private Storage Bucket
    Admin->>Admin: 2. Generate Cryptographic Secrets (API_KEY_SALT, WORKER_SECRET, ADMIN_KEY)
    Admin->>Supabase: 3. Verify Pre-Migration pg_dump Logical Backup (Port 5432)

    Note over Admin,Supabase: PHASE 2: Live Database Migrations (GATE-01)
    Admin->>Supabase: 4. [GATE-01 AUTHORIZATION] Run 'alembic upgrade head' (Port 5432 Session Pooler)
    Supabase-->>Admin: Schema applied successfully; alembic_version verified

    Note over Admin,Supabase: PHASE 3: Staging Client Key Seeding (GATE-02)
    Admin->>Supabase: 5. [GATE-02 AUTHORIZATION] Seed authorized staging client key in api_keys table
    Supabase-->>Admin: Client key record inserted with SHA-256 hash

    Note over Admin,Container: PHASE 4: Cloud API Service Deployment (GATE-03)
    Admin->>Container: 6. [GATE-03 AUTHORIZATION] Deploy API container with staging secrets
    Container-->>Admin: Probes active: GET /health/liveness (200), /health/readiness (200)

    Note over Admin,Worker: PHASE 5: Hybrid GPU Worker Daemon Launch (GATE-04)
    Admin->>Worker: 7. [GATE-04 AUTHORIZATION] Start worker daemon (python -m src.worker.daemon)
    Worker->>Container: Polling outbound: POST /api/v1/worker/jobs/lease (HTTP 204 No Content)

    Note over Admin,Smoke: PHASE 6: Live Remote Smoke Test (GATE-05)
    Admin->>Smoke: 8. [GATE-05 AUTHORIZATION] Execute scripts/staging_smoke_test.py --allow-remote
    Smoke->>Container: Complete 8-stage verification pipeline
    Smoke-->>Admin: All 8 stages PASSED (Exit Code 0)

    Note over Admin,Supabase: PHASE 7: Future Physical Storage Deletion (STRICTLY BLOCKED)
    Admin->>Admin: 9. [SEPARATE FUTURE OPERATION] ENABLE_PHYSICAL_ASSET_DELETION remains false
```

---

## 9. Cost, Quota & Operational Risk Analysis

### Comprehensive Cost Model

| Expense Category | Service Provider | Expected Staging Cost | Monthly Free Quotas & Constraints |
| :--- | :--- | :---: | :--- |
| **Control Plane Compute** | Render Free Web Service | **$0.00** | 512 MB RAM, 750 free instance hours. Sleeps after 15m idle. Background tasks pause during sleep. |
| **Relational Database** | Supabase PostgreSQL | **$0.00** | 500 MB DB storage, 2 active projects. Pauses after 7 days inactivity. Port 5432 session pooler required for DDL. |
| **Binary Asset Storage** | Supabase Storage | **$0.00** | 1 GB storage, 2 GB egress transfer. Private bucket access. |
| **GPU Inference Compute** | Local Hardware (RTX 2050) | **$0.00** | Uses local home electricity (~₹50–₹100/mo at ~50W load). |
| **Public Custom Domain** | Render/Supabase Subdomains | **$0.00** | Default `*.onrender.com` with free TLS certificates. |
| **Total Operating Cost (Option A)** | | **$0.00 / mo (₹0)** | **100% Free-Tier Eligible** (with sleep limitations) |
| **Total Operating Cost (Option B)** | Hetzner CX22 / Render Starter | **$4.20–$7.00 / mo** | **Continuous 24/7 Execution** (Zero sleep, uninterrupted daemons) |

### Operational Risks & Mitigation Strategies

1. **Free-Tier Sleep & Daemon Suspension (Render):**
   - *Risk:* Web service spins down after 15 minutes of inactivity. When asleep, `LeaseSweeper` cannot sweep expired leases, and `DeletionOutboxWorker` cannot process tombstones. Initial request takes 25–45s to wake up.
   - *Mitigation:* Operators accept this limitation for zero-cost staging. Before running smoke tests or launching the GPU worker, send a warm-up request (`curl -i https://<STAGING_API_HOSTNAME>/health/liveness`). If continuous lease sweeping is required, upgrade to Option B (Hetzner CX22 or Render Starter). Do not run synthetic ping loops.
2. **Supabase Inactivity Auto-Pause (7-Day Limit):**
   - *Risk:* Supabase free tier pauses databases after 7 days without queries.
   - *Mitigation:* Perform regular weekly testing sessions or review dashboard before test runs.
3. **Database Connection Routing (IPv6 Direct vs IPv4 Pooler):**
   - *Risk:* Direct connections to `db.<REF>.supabase.co:5432` fail on IPv4-only networks.
   - *Mitigation:* Always route staging traffic through the Supavisor pooler domain (`aws-0-<REGION>.pooler.supabase.com`): port 6543 for runtime API queries, port 5432 for Alembic DDL migrations.
4. **Transaction Pooler Prepared Statement Collisions:**
   - *Risk:* Multiple asyncpg statements with prepared caching fail over transaction poolers.
   - *Mitigation:* Automatically mitigated by `src/infrastructure/database.py` via `statement_cache_size=0`.
5. **Physical Asset Deletion Safety:**
   - *Risk:* Accidental deletion of staging assets.
   - *Mitigation:* `ENABLE_PHYSICAL_ASSET_DELETION=false` is enforced on both API and worker nodes. Gate 05 remains strictly BLOCKED.

---

## 10. Canonical Prerequisite & Authorization Gate Matrix

Every live operation remains gated under the project's engineering discipline protocol. Approval of one gate must **never** imply approval of another.

| Gate ID | Operation | Risk Level | Target Destination | Required Prerequisite | Current Status |
| :--- | :--- | :---: | :--- | :--- | :---: |
| **GATE-01** | Live Database Migrations (`alembic upgrade head`) | **HIGH** (DDL mutation) | Staging Supabase PostgreSQL (Port 5432 Session Pooler) | Verified pre-migration `pg_dump` logical backup on disk with non-zero size and verified table of contents | **HOLD / NOT AUTHORIZED** |
| **GATE-02** | Staging Client API Key Provisioning | **MEDIUM** (Data insert) | Staging PostgreSQL `api_keys` table | GATE-01 completed & schema verified; separate explicit authorization | **HOLD / NOT AUTHORIZED** |
| **GATE-03** | Cloud API Container Deployment | **MEDIUM** (Host deployment) | Cloud Container / VPS (`RUN_EMBEDDED_WORKER=false`) | GATE-01 & GATE-02 completed; lean container image built/verified; separate explicit authorization | **HOLD / NOT AUTHORIZED** |
| **GATE-04** | Hybrid GPU Worker Daemon Launch | **LOW** (Outbound traffic) | Dedicated Workstation (NVIDIA RTX 2050 Mobile) | GATE-03 deployed & `/health/readiness` returning 200; separate explicit authorization | **HOLD / NOT AUTHORIZED** |
| **GATE-05** | Live Remote Smoke-Test Execution | **LOW** (Synthetic verification) | Public Staging API Endpoint | GATE-04 active & outbound polling verified; runner executed with `--allow-remote`; separate explicit authorization | **HOLD / NOT AUTHORIZED** |

> [!IMPORTANT]
> **Physical Asset Deletion Safety Invariant:**
> Physical asset deletion (`ENABLE_PHYSICAL_ASSET_DELETION=true`) remains **STRICTLY DISABLED / BLOCKED** across all environments and is **NOT** part of GATE-01 through GATE-05.
> If deletion enablement is ever considered in the future, it must be treated as a separate post-staging change requiring its own dedicated Architecture Decision Record (ADR), explicit separate approval, comprehensive verification plan, and rollback procedure.

---

## 11. Verification Classification

* **Verified Locally (Offline Tests & Static Inspection):**
  - Application logic, schemas, and API contracts (279 unit tests passed).
  - Target URL security validation and production host blocking (`validate_target_url()`).
  - Worker lease, heartbeat, and asset persistence lifecycles.
  - Production configuration validator rejecting `RUN_EMBEDDED_WORKER=True` in hybrid mode.
  - Statement cache suppression (`statement_cache_size=0`) on port 6543 / pooler in `src/infrastructure/database.py`.
  - Lazy import of `torch` and `diffusers` in `src/infrastructure/providers/local_diffusers.py`.
* **Unverified / Awaiting Separately Authorized Live Staging:**
  - Remote Supabase connection handshake and latency over WAN.
  - Real object persistence to Supabase cloud storage bucket.
  - Public HTTPS polling between on-premises RTX 2050 GPU and cloud container.
  - Container build time and memory usage under lean packaging specification.
