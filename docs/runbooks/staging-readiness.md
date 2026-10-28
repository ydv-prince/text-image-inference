# Staging Deployment Readiness & Operations Runbook

- **System:** Aarav Image Generation Platform (`Inference-1`)
- **Version:** 1.0.0 (Milestone 8 Hardening)
- **Document Status:** Operational Runbook

---

## 1. Deployment Topology & Architecture

The Aarav platform uses a hybrid deployment topology separating stateless control-plane services from hardware-bound GPU compute:

```
[ Public Clients / Web Frontend ]
               |
               | HTTPS (API Key Auth)
               v
+-------------------------------------------------------------+
| Control Plane API Process (Render / Fly.io / VPS)           |
| - Python 3.10+ / FastAPI / Uvicorn (Port 8000)              |
| - Background Lifespan Daemons:                              |
|     * LeaseSweeper (reclaims stale job leases)              |
|     * DeletionOutboxWorker (processes batch tombstones)     |
| - Probes: GET /health/liveness, GET /health/readiness       |
| - Telemetry: GET /metrics (Prometheus v0.0.4 text)          |
+------------------------------+------------------------------+
                               |
        +----------------------+----------------------+
        | (Transaction Pooler: 6543)                  | (Session / DDL: 5432)
        v                                             v
+-----------------------------+             +-----------------------------+
| Supabase PostgreSQL (Queue) |             | Alembic Migrations          |
| - Row-Level Security (RLS)  |             | - Schema DDL changes        |
| - Advisory lock serialization|            | - Advisory lock acquisition |
+-----------------------------+             +-----------------------------+
        ^
        | Outbound HTTPS Polling (Bearer Token Auth)
        |
+-------------------------------------------------------------+
| On-Premise GPU Inference Worker (Developer Workstation)      |
| - Python 3.10+ / PyTorch CUDA 12.4 (NVIDIA RTX 2050 Mobile) |
| - Zero Inbound Listening Ports (100% Outbound HTTPS)        |
| - Daemon CLI: python -m src.worker.daemon                   |
| - Heartbeat lease renewal during inference runs             |
| - Direct Asset Upload -> CAS / Cloud Object Storage         |
+-------------------------------------------------------------+
```

### Component Process Isolation Boundary
1. **Control Plane API:** Runs on cloud container or Linux VPS. Does **NOT** execute diffusion models or load PyTorch weights in production.
2. **Hybrid Inference Worker:** Runs independently on dedicated GPU workstation. Polling is strictly outbound over HTTPS (`POST /api/v1/worker/jobs/lease`). Never exposes open ports to the WAN.
3. **Storage Tier:** Content-Addressable Storage (CAS) with SHA-256 deduplication. Adapters: `local_cas` (local dev) or `supabase` (staging/production bucket).

---

## 2. Reproducible Environment Configuration

### Supported Python Runtimes
- **Runtime:** Python `>=3.10` (Tested on Python 3.10.10).
- **Environment Isolation:** Python virtual environment (`.venv`).

### Installation & Packaging
```powershell
# 1. Clone repository & create isolated virtual environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# 2. Upgrade pip and packaging tools
python -m pip install --upgrade pip setuptools wheel

# 3. Install core dependencies with optional PostgreSQL & Test extras
pip install -e .[postgres,test]
```

### Environment Variable Matrix

| Variable | Type | Default | Required in Production | Purpose |
| :--- | :--- | :--- | :--- | :--- |
| `ENVIRONMENT` | string | `development` | Yes (`production`) | Enforces production security validation gates |
| `DEPLOYMENT_MODE` | string | `local` | Yes (`hybrid`) | Mode A (local monolith) vs Mode B (hybrid distributed) |
| `DATABASE_URL` | string | SQLite dev URL | Yes | Async PostgreSQL runtime connection string (Port 6543, transaction pooler) |
| `MIGRATION_DATABASE_URL` | string | None | Yes | Supavisor Session Pooling connection for Alembic DDL (Port 5432, IPv4) |
| `API_KEY_SALT` | secret | Dev sentinel | Yes | Cryptographic salt for SHA-256 API key hashing |
| `WORKER_SHARED_SECRET` | secret | None | Yes (hybrid mode) | Bearer secret for hybrid worker lease authentication |
| `ADMIN_API_KEY` | secret | None | Yes (if metrics auth) | Administrative master key for `/metrics` and admin tasks |
| `METRICS_ENABLED` | bool | `true` | No | Enables Prometheus `/metrics` exposition |
| `METRICS_REQUIRE_AUTH`| bool | `false` | Recommended (`true`) | Enforces `ADMIN_API_KEY` verification on `/metrics` |
| `METRICS_CACHE_TTL_SECONDS`| float | `5.0` | No | Cache TTL for queue/outbox depth sampling |
| `STORAGE_ADAPTER` | string | `local_cas` | Yes (`supabase`) | Active asset storage adapter |
| `SUPABASE_URL` | string | None | Yes (if supabase) | HTTPS URL to Supabase project |
| `SUPABASE_STORAGE_BUCKET`| string| `inference-assets` | Yes (if supabase) | Storage bucket name |
| `SUPABASE_SERVICE_ROLE_KEY`| secret| None | Yes (if supabase) | Supabase service-role key for backend uploads |
| `ENABLE_PHYSICAL_ASSET_DELETION` | bool | `false` | Safety toggle | Master gate for physical asset deletion |
| `RUN_EMBEDDED_WORKER` | bool | `true` | Must be `false` in cloud | Disables GPU execution on CPU cloud containers |
| `RUN_LEASE_SWEEPER` | bool | `true` | Yes | Runs background lease sweeper daemon |
| `RUN_DELETION_OUTBOX_WORKER` | bool | `true` | Yes | Runs background tombstone deletion processor |

---

## 3. Operational Lifecycles & Startup Procedures

### A. Database Migration Preconditions
> [!IMPORTANT]
> Alembic migrations alter tables, indexes, and triggers. Never run migrations against the Supavisor transaction pooler (port 6543). Supavisor Session Pooling on port 5432 (`aws-0-[REGION].pooler.supabase.com:5432`) is required for IPv4-compatible DDL migrations and transaction locks. Direct connection (`db.[REF].supabase.co:5432`) is IPv6-only on modern Supabase tiers.
>
> **Disaster Recovery Requirement:** Always execute a verified pre-migration logical backup (`pg_dump`) prior to applying DDL changes.
>
> **Four-Tier Recovery & Restorability Boundary:**
> 1. *Tier 1 (Process Exit):* `pg_dump` exits with code 0 (connection and stream completed).
> 2. *Tier 2 (Artifact Non-Emptiness):* Dump file exists and is strictly non-zero bytes.
> 3. *Tier 3 (Archive Integrity):* `pg_restore --list` exits with code 0 (header, compression, and TOC catalog parsed).
> 4. *Tier 4 (Proven Restorability):* Separate restore drill into an isolated disposable database succeeded without constraint or role errors.
>
> *Governance Rule:* Tiers 1–3 satisfy the pre-migration gating check for GATE-01. Only Tier 4 establishes evidence of restorability. Performing an isolated restore drill is a separate future operation subject to explicit authorization.

> [!CAUTION]
> **PowerShell `SecureString` Credential Pitfall:**
> Never assign a `SecureString` directly to an environment variable (`$env:PGPASSWORD = Read-Host -AsSecureString`). PowerShell will convert the object to its type name string: `"System.Security.SecureString"`, causing immediate authentication failure.
> For manual interactive runs, always prefer **Method A (`pg_dump -W`)** which prompts natively and bypasses shell variables entirely.

#### Method A: Primary Recommended Execution (Native `-W` Interactive Prompt)
```powershell
# 1. Create dedicated backup directory with restricted ACLs
$backupDir = Join-Path (Get-Location) "backups"
if (-not (Test-Path $backupDir)) { New-Item -ItemType Directory -Path $backupDir | Out-Null }
icacls $backupDir /inheritance:r /grant:r "$($env:USERNAME):(OI)(CI)F" | Out-Null

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$dumpFile = Join-Path $backupDir "staging_pre_gate01_$timestamp.dump"

# 2. Refuse to overwrite existing backup
if (Test-Path $dumpFile) {
    throw "Refusing to overwrite existing backup file: $dumpFile"
}

# 3. Run pg_dump with native password prompt (no environment variable or history leakage)
Write-Host "Running pg_dump via Supavisor Session Pooler (Port 5432)..."
pg_dump -h "aws-0-[REGION].pooler.supabase.com" -p 5432 -U "postgres.[REF]" -d "postgres" -W -F c -b -v -f $dumpFile

# 4. Capture $LASTEXITCODE immediately; fail closed if non-zero
$dumpExit = $LASTEXITCODE
if ($dumpExit -ne 0) {
    if (Test-Path $dumpFile) { Remove-Item -Path $dumpFile -Force }
    throw "Backup failed at pg_dump stage: native utility exited with code $dumpExit."
}

# 5. Verify backup existence, non-zero size, and delete if failed
if (-not (Test-Path $dumpFile) -or (Get-Item $dumpFile).Length -le 0) {
    if (Test-Path $dumpFile) { Remove-Item -Path $dumpFile -Force }
    throw "Backup failed at file validation stage: dump file missing or 0 bytes."
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

#### Method B: Scripted Execution (Ephemeral `$env:PGPASSWORD` with `try...finally`)
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
    pg_dump -h "aws-0-[REGION].pooler.supabase.com" -p 5432 -U "postgres.[REF]" -d "postgres" -F c -b -v -f $dumpFile

    # 4. Capture $LASTEXITCODE immediately; fail closed if non-zero
    $dumpExit = $LASTEXITCODE
    if ($dumpExit -ne 0) {
        if (Test-Path $dumpFile) { Remove-Item -Path $dumpFile -Force }
        throw "Backup failed at pg_dump stage: native utility exited with code $dumpExit."
    }

    # 5. Verify file existence and non-zero size
    if (-not (Test-Path $dumpFile) -or (Get-Item $dumpFile).Length -le 0) {
        if (Test-Path $dumpFile) { Remove-Item -Path $dumpFile -Force }
        throw "Backup failed at file validation stage: dump file missing or 0 bytes."
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

# 2. Verify migration connection via Session Pooler (Port 5432)
$env:MIGRATION_DATABASE_URL = "postgresql+asyncpg://postgres.[REF]:[PWD]@aws-0-[REGION].pooler.supabase.com:5432/postgres?ssl=require"

# 3. Check migration status without executing
alembic current
alembic heads

# 4. Apply migrations when authorized (GATE-01)
# alembic upgrade head
```

### B. Control Plane API Startup
```powershell
# Start ASGI web application with Uvicorn (Single worker for lifespan background task safety)
uvicorn src.main:app --host 0.0.0.0 --port 8000 --workers 1 --timeout-graceful-shutdown 15
```

### B.1 Local Container Build & Runtime Verification Procedure (PowerShell)
> [!NOTE]
> This procedure is reserved for a future local testing session when Docker Desktop is active. Do not execute if the Docker daemon is stopped. Real staging credentials must never be passed to this verification container.

```powershell
# =============================================================================
# Local API Container Build & Verification Procedure (Run when Docker Desktop is active)
# =============================================================================

# 1. Verify Docker daemon is running
Write-Host "Checking Docker daemon availability..."
docker info | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "Docker daemon is not running or unreachable. Please launch Docker Desktop before proceeding."
}
Write-Host "Docker daemon is active."

# 2. Collision Check: Generate unique test container and image tag names
$valTimestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$valImageTag = "aarav-api-val:$valTimestamp"
$valContainerName = "aarav-api-val-$valTimestamp"

$existingContainer = docker ps -a -q -f "name=^$valContainerName$"
if ($existingContainer) {
    throw "Collision detected: Container '$valContainerName' already exists. Refusing to run."
}

# 3. Build API-Only Image from Dockerfile.api
Write-Host "Building API container image '$valImageTag'..."
docker build -f Dockerfile.api -t $valImageTag .
$buildExit = $LASTEXITCODE
if ($buildExit -ne 0) {
    throw "Docker build failed with exit code $buildExit."
}
Write-Host "Image build successful: $valImageTag"

try {
    # 4. Launch Container with Safe Non-Production Configuration
    # Binds to loopback port 8001 (avoids host port 8000 conflicts)
    Write-Host "Starting test container '$valContainerName' on 127.0.0.1:8001..."
    docker run -d `
        --name $valContainerName `
        -p 127.0.0.1:8001:8000 `
        -e ENVIRONMENT=development `
        -e DEPLOYMENT_MODE=hybrid `
        -e RUN_EMBEDDED_WORKER=false `
        -e RUN_LEASE_SWEEPER=false `
        -e RUN_DELETION_OUTBOX_WORKER=false `
        -e METRICS_REQUIRE_AUTH=false `
        $valImageTag | Out-Null

    $runExit = $LASTEXITCODE
    if ($runExit -ne 0) {
        throw "Docker run failed to start container '$valContainerName' (Exit code $runExit)."
    }

    # 5. Wait for Container Startup & Verify Liveness Probe
    Write-Host "Waiting up to 15s for container initialization..."
    $maxWaitSec = 15
    $started = $false
    for ($i = 0; $i -lt $maxWaitSec; $i++) {
        Start-Sleep -Seconds 1
        try {
            $resp = Invoke-RestMethod -Uri "http://127.0.0.1:8001/health/liveness" -Method Get -TimeoutSec 2
            if ($resp.status -eq "alive") {
                $started = $true
                break
            }
        } catch {
            # Retry until timeout
        }
    }

    if (-not $started) {
        throw "Container verification failed: /health/liveness did not respond with 'alive' within $maxWaitSec seconds."
    }
    Write-Host "Liveness probe verified: status='alive' on 127.0.0.1:8001"

    # 6. Inspect Container Logs for Startup Cleanliness (Zero Secrets Exposed)
    Write-Host "Inspecting container logs..."
    $logs = docker logs --tail 25 $valContainerName
    Write-Host "Recent container log output:"
    $logs | ForEach-Object { Write-Host "  [container] $_" }
}
finally {
    # 7. Clean Teardown: Stop and Remove ONLY the Test Container
    Write-Host "Cleaning up test container '$valContainerName'..."
    docker stop $valContainerName 2>&1 | Out-Null
    docker rm -f $valContainerName 2>&1 | Out-Null
    docker rmi -f $valImageTag 2>&1 | Out-Null
    Write-Host "Cleanup completed. No lingering test containers."
}
```

### C. Hybrid Worker Startup (On-Premise Node)
```powershell
# Launch outbound polling worker connected to control plane
python -m src.worker.daemon `
  --control-plane-url "https://api.aarav-inference.internal" `
  --provider "local-gpu"
```

### D. Graceful Shutdown & Time Budgets
- **API Shutdown Budget:** 15 seconds.
  - Receives `SIGTERM` / `SIGINT`.
  - Background tasks (`lease_sweeper`, `deletion_outbox_worker`) receive `stop()` signals.
  - In-flight HTTP requests complete before connections close.
  - Database engine pool disposed cleanly (`engine.dispose()`).
- **Worker Shutdown Budget:** 30 seconds.
  - Worker enters `is_stopping = True` mode and stops claiming new jobs.
  - Active inference finishes generation if under 30 seconds.
  - If cancellation triggers during long runs, worker boundedly releases lease back to `PENDING` via `POST /api/v1/worker/jobs/{id}/fail` (`error_code="WORKER_CANCELLED"`).
  - Unfinished jobs are **never** reported as completed.

---

## 4. Health, Probes & Observability Verification

### Health Probe Verification
```powershell
# 1. Liveness Probe (Confirms ASGI web process is responsive)
curl -i http://localhost:8000/health/liveness
# Expected: HTTP 200 OK {"status":"alive","timestamp":"...","version":"1.0.0","environment":"..."}

# 2. Readiness Probe (Confirms database connectivity and storage writability)
curl -i http://localhost:8000/health/readiness
# Expected: HTTP 200 OK {"status":"ready","database":"connected","storage":"accessible",...}
```

### Metrics Scrape Verification
```powershell
# Unauthenticated scrape (if METRICS_REQUIRE_AUTH=false)
curl -s http://localhost:8000/metrics

# Authenticated scrape (when METRICS_REQUIRE_AUTH=true)
curl -s -H "X-Admin-API-Key: [ADMIN_API_KEY]" http://localhost:8000/metrics
# Or:
curl -s -H "Authorization: Bearer [ADMIN_API_KEY]" http://localhost:8000/metrics
```

### Multi-Process Metrics Interpretation
1. **Counters (`aarav_http_requests_total`, `aarav_jobs_completed_total`):**
   - Process-local. Each uvicorn worker process tracks its own count. Aggregate in Prometheus using `sum(rate(aarav_http_requests_total[5m]))`.
2. **Persistent Gauges (`aarav_queue_depth`, `aarav_deletion_outbox_queue_depth`):**
   - Cluster-wide. Sampled directly from database `GROUP BY status` with 5s cache TTL. Any replica emits consistent global state.
3. **Worker Gauges (`aarav_worker_status`):**
   - Process-local status of the worker daemon instance (`1=running, 0=stopped`).

---

## 5. Failure Scenarios & Recovery Runbook

| Failure Mode | Detection | System Safeguard | Recovery Action |
| :--- | :--- | :--- | :--- |
| **Worker Abrupt Crash / OOM** | Leased job remains in `PROCESSING` without heartbeats | `LeaseSweeper` detects `locked_at + LEASE_TIMEOUT < now` | Automated: Sweeper reclaims lease back to `PENDING` with retry backoff. Worker restarts automatically via service manager (systemd / NSSM). |
| **Database Pool Exhaustion** | `/health/readiness` returns 503; API logs connection timeout | Scrape endpoint does not crash; returns cached telemetry | Inspect slow queries; check active connections in Supabase dashboard; verify pool size (`DB_POOL_SIZE=5`). |
| **External Storage Outage** | Worker logs upload failure; job transitions to retry | Job not marked `COMPLETED` without verified asset persistence | Worker backs off; retry scheduler attempts upload after backoff delay. |
| **Deletion Outbox Jam / Flag False**| Outbox tasks remain `PENDING` | `ENABLE_PHYSICAL_ASSET_DELETION=False` safely skips execution without consuming retries | Enable flag when permanent deletion is authorized; stale leases recovered automatically via `recover_stalled_tasks`. |

---

## 6. Staging-Readiness Verification Checklist

| Checkpoint | Category | Verification Method | Status |
| :--- | :--- | :--- | :--- |
| **Python Version & Packaging** | Environment | `pyproject.toml` requires `>=3.10`; installs in isolated `.venv` | **VERIFIED** |
| **Configuration TLS Enforcement** | Security | `Settings.validate_production_configuration` rejects non-TLS URLs | **VERIFIED** |
| **Placeholder Secret Rejection** | Security | Rejects default salt, weak worker secrets, and weak admin keys | **VERIFIED** |
| **Metrics Scrape Auth Guard** | Security | Requires `ADMIN_API_KEY` when `METRICS_REQUIRE_AUTH=True` | **VERIFIED** |
| **Label Cardinality Boundaries** | Observability | `FORBIDDEN_LABEL_KEYS` blocks UUIDs, paths, tokens, error strings | **VERIFIED** |
| **Worker Metric Categorization** | Observability | Worker metrics bounded to `worker_category`; raw UUIDs blocked | **VERIFIED** |
| **Throttled Scrape Sampling** | Performance | 5.0s TTL cache with timeout prevents thundering herd on `/metrics` | **VERIFIED** |
| **Liveness / Readiness Probe Split**| Reliability | Liveness passes on DB outage; readiness returns 503 | **VERIFIED** |
| **Queue Durability on Worker Crash**| Reliability | Expired worker leases swept back to `PENDING` with backoff | **VERIFIED** |
| **Deletion Safety Gate 4** | Safety | `ENABLE_PHYSICAL_ASSET_DELETION=False` releases claimed tasks intact | **VERIFIED** |
| **Unit Test Suite (254 tests)** | Quality Gate | Deterministic execution in `tests/unit/` | **VERIFIED** |
| **Mocked Integration Suite (54 tests)**| Quality Gate | Deterministic execution in `tests/integration/` | **VERIFIED** |
| **API Container Artifacts** | Packaging | `Dockerfile.api` & `requirements-api.txt` verified statically against `pyproject.toml` | **VERIFIED** |
| **Local Container Image Build** | Container | `docker build` against local daemon | **BLOCKED (Docker Desktop daemon stopped)** |
| **Live Staging PostgreSQL Migrations**| Infrastructure | Alembic DDL against real cloud database | **BLOCKED (Requires Explicit Authorization)** |
| **Live Cloud GPU Ingress** | Compute | Real RTX 2050 polling live cloud API over WAN | **BLOCKED (Awaiting Staging Cluster Deployment)** |

---

## 7. Canonical Authorization Gate Registry

Every live operational step requires separate, explicit written authorization before execution. Approval of one gate does **not** authorize any subsequent gate.

| Gate ID | Operation | Risk Level | Target Destination | Mandatory Prerequisite | Current Status |
| :--- | :--- | :---: | :--- | :--- | :---: |
| **GATE-01** | Live Database Migrations (`alembic upgrade head`) | **HIGH** (DDL mutation) | Staging Supabase PostgreSQL (Port 5432 Session Pooler) | Verified pre-migration `pg_dump` logical backup on disk with non-zero size and verified table of contents | **HOLD / NOT AUTHORIZED** |
| **GATE-02** | Staging Client API Key Provisioning | **MEDIUM** (Data insert) | Staging PostgreSQL `api_keys` table | GATE-01 completed & schema verified; separate explicit authorization | **HOLD / NOT AUTHORIZED** |
| **GATE-03** | Cloud API Container Deployment | **MEDIUM** (Host deployment) | Cloud Container / VPS (`RUN_EMBEDDED_WORKER=false`) | GATE-01 & GATE-02 completed; lean container image built/verified; separate explicit authorization | **HOLD / NOT AUTHORIZED** |
| **GATE-04** | Hybrid GPU Worker Daemon Launch | **LOW** (Outbound traffic) | Dedicated Workstation (NVIDIA RTX 2050 Mobile) | GATE-03 deployed & `/health/readiness` returning 200; separate explicit authorization | **HOLD / NOT AUTHORIZED** |
| **GATE-05** | Live Remote Smoke-Test Execution | **LOW** (Synthetic verification) | Public Staging API Endpoint | GATE-04 active & outbound polling verified; runner executed with `--allow-remote`; separate explicit authorization | **HOLD / NOT AUTHORIZED** |

> [!IMPORTANT]
> **Physical Asset Deletion Safety Invariant:**
> Physical asset deletion (`ENABLE_PHYSICAL_ASSET_DELETION=true`) remains **STRICTLY DISABLED / BLOCKED** across all environments and is **NOT** part of GATE-01 through GATE-05.
> If deletion enablement is ever considered in the future, it must be treated as a separate post-staging change requiring its own dedicated Architecture Decision Record (ADR), explicit approval, verification plan, and rollback procedure.
