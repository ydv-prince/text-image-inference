# Data Model & Storage Specification — Aarav Image Generation API

**Document Version:** 2.0.0 (Local GPU First, Cloud Ready Revision)  
**Status:** PROPOSED (Pending Review Gate)  
**Author:** Principal System Design Engineer  
**System:** Aarav Image Generation API (`Inference-1`)

---

## 1. Entity-Relationship Diagram (Mermaid)

```mermaid
erDiagram
    API_KEYS ||--o{ GENERATION_JOBS : "authorizes"
    GENERATION_JOBS ||--o| GENERATED_ASSETS : "produces"
    GENERATION_JOBS ||--o{ JOB_EVENTS : "records"

    API_KEYS {
        string id PK "UUIDv7"
        string key_hash UK "Salted SHA-256 Digest"
        string client_name "Human-readable client name"
        string tier "free | pro | enterprise"
        integer rate_limit_rpm "Requests Per Minute"
        boolean is_active "Revocation status flag"
        datetime created_at "Creation timestamp"
        datetime last_used_at "Nullable Timestamp"
    }

    GENERATION_JOBS {
        string id PK "UUIDv7"
        string api_key_id FK "References API_KEYS.id"
        string idempotency_key "Optional RFC 9535 deduplication token"
        string payload_hash "SHA-256 of request parameters"
        string status "PENDING | PROCESSING | COMPLETED | FAILED | CANCELLED | TIMED_OUT"
        string prompt "Text prompt (1-2000 chars)"
        string negative_prompt "Optional negative prompt"
        integer width "Output width (default 512 for local GPU)"
        integer height "Output height (default 512 for local GPU)"
        integer steps "Inference steps (default 1-4 for SD-Turbo)"
        bigint seed "Numeric seed or -1 for random"
        string model "Model identifier (sd-turbo, sd-1.5, flux)"
        string provider_requested "Requested provider (local-gpu, pollinations, fal, mock)"
        string provider_used "Actual provider executing inference"
        string error_code "Nullable error code"
        string error_message "Nullable error message"
        integer retry_count "Current retry attempts"
        integer max_retries "Maximum retry attempts (default 3)"
        string locked_by "Worker ID holding lease (e.g. local-worker-rtx2050)"
        datetime locked_at "Lease timestamp / heartbeat"
        integer execution_time_ms "Total processing duration"
        integer peak_vram_mb "Observed peak VRAM allocation"
        datetime created_at "Creation timestamp"
        datetime scheduled_at "Delayed pickup eligibility"
        datetime started_at "Pickup timestamp"
        datetime completed_at "Completion timestamp"
    }

    GENERATED_ASSETS {
        string id PK "UUIDv7"
        string job_id FK "References GENERATION_JOBS.id"
        string storage_provider "local_cas | cloudinary | supabase"
        string file_hash UK "Cryptographic SHA-256 of image bytes"
        string storage_path "Relative path or remote CDN public ID"
        string public_url "Direct download or CDN delivery URL"
        bigint file_size_bytes "Size in bytes"
        string mime_type "image/png, image/jpeg, image/webp"
        integer width "Verified pixel width"
        integer height "Verified pixel height"
        boolean is_synthetic "Synthetic/test-only flag"
        datetime created_at "Creation timestamp"
    }

    JOB_EVENTS {
        string id PK "UUIDv7"
        string job_id FK "References GENERATION_JOBS.id"
        string event_type "STATE_CHANGE | LEASE_ACQUIRED | HEARTBEAT | RETRY"
        string from_state "Previous state"
        string to_state "New state"
        string message "Descriptive context"
        datetime created_at "Timestamp"
    }
```

---

## 2. Table Schemas & Dialect Specifications

### 2.1 Table: `generation_jobs`

| Column | Type (PG / SQLite) | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `id` | `VARCHAR(36)` | PRIMARY KEY | UUIDv7 string. |
| `api_key_id` | `VARCHAR(36)` | FOREIGN KEY $\rightarrow$ `api_keys(id)` | Authorizing key ID. |
| `idempotency_key` | `VARCHAR(128)` | NULLABLE | Client deduplication token. |
| `payload_hash` | `VARCHAR(64)` | NOT NULL | SHA-256 digest of input parameters. |
| `status` | `VARCHAR(20)` | NOT NULL, DEFAULT `'PENDING'` | State machine value. |
| `prompt` | `TEXT` | NOT NULL | Generation text prompt. |
| `negative_prompt` | `TEXT` | NULLABLE | Negative guidance prompt. |
| `width` | `INTEGER` | NOT NULL, DEFAULT `512` | Pixel width (capped at 512 for local GPU). |
| `height` | `INTEGER` | NOT NULL, DEFAULT `512` | Pixel height (capped at 512 for local GPU). |
| `steps` | `INTEGER` | NOT NULL, DEFAULT `1` | Denoising steps. |
| `seed` | `BIGINT` | NOT NULL, DEFAULT `-1` | Random seed (`-1` = randomized). |
| `model` | `VARCHAR(64)` | NOT NULL, DEFAULT `'sd-turbo'` | Target model. |
| `provider_requested`| `VARCHAR(32)` | NOT NULL, DEFAULT `'local-gpu'`| Requested engine. |
| `provider_used` | `VARCHAR(32)` | NULLABLE | Engine that completed generation. |
| `error_code` | `VARCHAR(64)` | NULLABLE | Machine error code on failure. |
| `error_message` | `TEXT` | NULLABLE | Human-readable error message. |
| `retry_count` | `INTEGER` | NOT NULL, DEFAULT `0` | Retries executed. |
| `max_retries` | `INTEGER` | NOT NULL, DEFAULT `3` | Max retry limit. |
| `locked_by` | `VARCHAR(64)` | NULLABLE | ID of worker holding lease. |
| `locked_at` | `TIMESTAMP` | NULLABLE | Timestamp of lease acquisition/heartbeat. |
| `execution_time_ms` | `INTEGER` | NULLABLE | End-to-end execution latency in ms. |
| `peak_vram_mb` | `INTEGER` | NULLABLE | Peak GPU memory allocated in MB. |
| `created_at` | `TIMESTAMP` | NOT NULL, DEFAULT CURRENT_TIMESTAMP | Submission timestamp. |
| `scheduled_at` | `TIMESTAMP` | NULLABLE | Delay/backoff eligibility timestamp. |
| `started_at` | `TIMESTAMP` | NULLABLE | Worker claim timestamp. |
| `completed_at` | `TIMESTAMP` | NULLABLE | Termination timestamp. |

---

## 3. Database Indexing Strategy (Dual Dialect)

To optimize worker polling and avoid full-table scans:

### In PostgreSQL (Mode B - Supabase):
```sql
-- Partial index: ONLY indexes pending jobs, keeping the index size tiny (<1 MB)
CREATE INDEX idx_jobs_pending_queue 
ON generation_jobs (created_at ASC) 
WHERE status = 'PENDING';

-- Index for lease sweeper: rapidly finds timed-out jobs
CREATE INDEX idx_jobs_stale_leases 
ON generation_jobs (locked_at) 
WHERE status = 'PROCESSING';

-- Unique idempotency enforcement
CREATE UNIQUE INDEX uq_jobs_idempotency 
ON generation_jobs (api_key_id, idempotency_key) 
WHERE idempotency_key IS NOT NULL;
```

### In SQLite (Mode A - Local Dev):
```sql
CREATE INDEX idx_jobs_status_created 
ON generation_jobs (status, created_at ASC);

CREATE INDEX idx_jobs_locked_at 
ON generation_jobs (status, locked_at);

CREATE UNIQUE INDEX uq_jobs_idempotency 
ON generation_jobs (api_key_id, idempotency_key) 
WHERE idempotency_key IS NOT NULL;
```

---

## 4. Asset Storage & CDN Mapping

| Asset Attribute | Mode A (Local CAS) | Mode B (Cloudinary / Supabase) |
| :--- | :--- | :--- |
| **`storage_provider`** | `'local_cas'` | `'cloudinary'` or `'supabase'` |
| **`storage_path`** | `images/a3/9f/a39fbc48....png` | `aarav_assets/a39fbc48` |
| **`public_url`** | `/api/v1/images/{id}/content` | `https://res.cloudinary.com/<cloud>/image/upload/v1/.../a39fbc48.png` |
| **Deduplication** | Filesystem check via SHA-256 before write | Cloudinary asset lookup by public ID (SHA-256) |
