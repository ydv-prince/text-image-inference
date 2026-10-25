# Security Architecture & Threat Model — Aarav Image Generation API

**Document Version:** 2.0.0 (Local GPU First, Cloud Ready Revision)  
**Status:** PROPOSED (Pending Review Gate)  
**Author:** Principal System Design Engineer & Security Reviewer  
**System:** Aarav Image Generation API (`Inference-1`)

---

## 1. Zero-Trust Local Worker Security Model (Mode B)

When running a GPU worker on a home development PC connected to a public cloud control plane:

```text
[ Home PC / Private LAN ]                     [ Public Internet / Cloud ]
+-------------------------+                   +--------------------------+
|                         |                   |                          |
|  Local GPU Worker       |                   |  Control Plane Gateway   |
|  - NO Inbound Ports (X) |                   |  - Port 443 (HTTPS)      |
|  - Behind NAT / CGNAT   |  == Outbound ==>  |  - Requires TLS          |
|  - Worker Bearer Secret |      HTTPS        |  - Validates Auth Token  |
|                         |                   |                          |
+-------------------------+                   +--------------------------+
```

### Core Security Guarantees:
1. **Zero Open Inbound Ports:** The home router requires **no port forwarding**, no UPnP, and no public DNS exposure. The home IP address is never revealed to public clients.
2. **Mutual Secret Authentication:** The worker authenticates every polling request to the control plane using a 256-bit cryptographically secure shared secret (`WORKER_SHARED_SECRET`).
3. **Signed Asset Uploads:** When the worker generates an image, it directly uploads the bytes to Cloudinary or Supabase Storage using server-signed upload credentials, preventing the worker from having full administrative cloud rights.

---

## 2. STRIDE Threat Model (Updated)

| Threat | Category | Vector | Architectural Mitigation |
| :--- | :--- | :--- | :--- |
| **Rogue Worker Impersonation** | **Spoofing** | Attacker calls `/worker/jobs/lease` to steal user prompts. | Worker endpoints are secured with `Authorization: Bearer <WORKER_SHARED_SECRET>` using constant-time `hmac.compare_digest` verification. |
| **VRAM Exhaustion Attack** | **Denial of Service** | Malicious client requests 4096x4096 resolution with 1000 steps to trigger GPU OOM. | Strict schema enforcement: `width` and `height` are forcibly capped at **512x512** for `local-gpu` inference; `steps` are clamped to maximum 4 for SD-Turbo. |
| **Path Traversal on Asset Read** | **Tampering** | Attacker queries `/images/../../windows/system32/cmd.exe`. | Filenames on disk are strictly derived from the SHA-256 hex digest of image bytes (`64` hex characters). Canonical path verification asserts `is_relative_to(base_dir)`. |
| **API Key Compromise** | **Information Disclosure** | Database breach leaks client API keys. | API keys are stored as salted SHA-256 digests. Raw API keys are displayed exactly once at generation time. |
| **Cross-Tenant Job Tampering** | **Elevation of Privilege** | Client A cancels or reads Client B's job. | All user queries append `WHERE id = :job_id AND api_key_id = :auth_key_id`. In Supabase, Row-Level Security (RLS) policies isolate tenant records. |
| **Malicious Image Byte Injection** | **Tampering** | Upstream provider returns executable binary disguised as image. | Storage worker verifies image magic bytes (`\x89PNG\r\n\x1a\n` or `\xff\xd8\xff`) via Python `PIL.Image.verify()` before saving to disk or CDN. |

---

## 3. Secret Management Matrix

| Secret Variable | Mode A (Local) | Mode B (Cloud / Worker) | Storage Location |
| :--- | :--- | :--- | :--- |
| `DATABASE_URL` | `sqlite+aiosqlite:///./aarav_dev.db` | `postgresql+asyncpg://postgres:pwd@db.supabase.co:5432/...` | `.env` (Never committed) |
| `WORKER_SHARED_SECRET`| N/A (Embedded) | 256-bit URL-safe token | Environment variable in Control API and Local Worker |
| `CLOUDINARY_URL` | N/A | `cloudinary://api_key:secret@cloud_name` | Environment variable on Local Worker / Control API |
| `FAL_KEY` | Optional | Optional | `.env` |

All settings classes inherit from `pydantic_settings.BaseSettings` with sensitive fields typed as `pydantic.SecretStr` to prevent accidental logging or terminal leakage.

---

## 4. API Ingestion & Asset Delivery Security (Milestone 4)

### 4.1 Client Authentication & Key Revocation
- **Dual Header Support:** Accepts `X-API-Key: <key>` or `Authorization: Bearer <key>`.
- **Secure Lookup:** Raw keys are never stored. The incoming key is hashed using `hash_api_key(raw_key, salt)` via SHA-256 and verified against `api_keys.key_hash`.
- **Revocation Enforcement:** If `is_active == False`, authentication immediately fails with **HTTP 401 Unauthorized**. Disabled/revoked keys cannot generate jobs or read assets.
- **Activity Telemetry:** Successful authentication touches `last_used_at` asynchronously to record client access without blocking inference.

### 4.2 In-Memory Sliding Window Rate Limiting
- **Quota Model:** Configurable per-key rate limit (`rate_limit_rpm`).
- **Sliding Window:** Evaluated across a 60-second rolling window. Old request timestamps are expired atomically.
- **Standard Header Feedback:** When a client exceeds quota:
  - Responds with **HTTP 429 Too Many Requests**.
  - Includes standard `Retry-After: <seconds>` header computed exactly as $\max(1, \lceil oldest + 60 - now \rceil)$.

### 4.3 Tenant Isolation & Cross-Key Boundaries
- **Job Status Access:** `GET /api/v1/images/generations/{id}` strictly validates `job.api_key_id == auth_key.id`. Mismatches return **HTTP 403 Forbidden**.
- **Asset Content Delivery:** `GET /api/v1/images/generations/{id}/content` enforces identical ownership validation before reading bytes from storage.
- **Private Cache Directives:**
  - `Cache-Control: private, no-transform, max-age=86400`: Allows local browser caching while strictly prohibiting shared CDNs or proxy caches from storing private client assets.
  - `ETag: "{sha256}"`: Enables conditional requests (`304 Not Modified`) without data retransmission.
  - `X-Content-Type-Options: nosniff`: Prevents browser MIME-type sniffing.

---

## 5. Known Limitations & Verification Boundaries

> [!WARNING]
> **PostgreSQL Concurrency Integration Testing Limitation:**
> Ingestion and queue leasing queries are written for high-concurrency PostgreSQL using `SELECT ... FOR UPDATE SKIP LOCKED` (`PostgresJobRepository`). While SQL queries and schema constraints have been verified against Alembic migrations and SQLite WAL concurrency tests, **live multi-connection PostgreSQL concurrency has not yet been executed against a real running PostgreSQL instance**. Live PostgreSQL concurrency testing remains bounded until a real cloud or containerized PostgreSQL instance is provisioned in Milestone 5.

