# Versioned API Contracts — Aarav Image Generation API

**Document Version:** 2.0.0 (Local GPU First, Cloud Ready Revision)  
**Status:** PROPOSED (Pending Review Gate)  
**Author:** Principal System Design Engineer  
**System:** Aarav Image Generation API (`Inference-1`)

---

## 1. User & Client Endpoints

### 1.1 `POST /api/v1/images/generations`
Submit an asynchronous image generation request.

#### Request Headers:
- `X-API-Key` or `Authorization: Bearer <key>`: (Required) Client API Key.
- `Idempotency-Key`: (Optional) Client-supplied idempotency key (ASCII string, max 128 characters).
  - *Standard Note:* RFC 9535 specifies JSONPath, not HTTP idempotency. The Aarav API defines its own `Idempotency-Key` protocol inspired by `draft-ietf-httpapi-idempotency-key-header`.
  - *Semantics:* Keys are scoped per client (`api_key_id`). The system computes a canonical SHA-256 fingerprint of the request payload (`prompt`, `negative_prompt`, `width`, `height`, `steps`, `seed`, `model`, `provider`).
  - *Identical Submission:* Returns the existing job with HTTP 202 Accepted and the matching `Location` header.
  - *Conflicting Payload:* If the key matches an existing record but the payload fingerprint differs, returns **HTTP 409 Conflict** with an RFC 7807 Problem Details payload.
  - *Concurrency Safety:* Database unique constraint `uq_jobs_idempotency` (`api_key_id`, `idempotency_key`) enforces atomic mutual exclusion under concurrent requests.

#### Request Body (`application/json`):
```json
{
  "prompt": "A close-up portrait of an astronaut on Mars, cybernetic helmet, cinematic lighting, 8k",
  "negative_prompt": "blurry, distorted, oversaturated, deformed",
  "width": 512,
  "height": 512,
  "steps": 4,
  "seed": 42,
  "model": "sd-1.5",
  "provider": "auto"
}
```

*Validation Rules:*
- `prompt`: String, 1–2000 characters (non-empty after whitespace strip).
- `width` / `height`: Multiples of 64. When `provider == "local-gpu"` or `"auto"`, strictly capped at **512 × 512** to guarantee local hardware safety (RTX 2050 4 GB VRAM ceiling).
- `steps`: Integer, 1–50 (defaults to 4 for fast LCM LoRA inference).
- `seed`: Integer, $-1$ to $2^{63}-1$ ($-1$ indicates random seed).
- `model`: String, e.g. `"sd-1.5"`, `"sd-turbo"`.
- `provider`: `["auto", "local-gpu", "mock"]`.

#### Response:
- **`202 Accepted`** (Asynchronous Ingestion):
  - **Headers:** `Location: /api/v1/images/generations/{id}`
  - **Body:**
    ```json
    {
      "id": "018f3a5b-9b42-7a2e-8e6f-1c4b721e0001",
      "status": "PENDING",
      "prompt": "A close-up portrait of an astronaut on Mars...",
      "negative_prompt": "blurry, distorted, oversaturated, deformed",
      "width": 512,
      "height": 512,
      "steps": 4,
      "seed": 42,
      "model": "sd-1.5",
      "provider_requested": "mock",
      "provider_used": null,
      "error_code": null,
      "error_message": null,
      "created_at": "2026-10-22T09:00:00Z",
      "started_at": null,
      "completed_at": null,
      "execution_time_ms": null,
      "peak_vram_mb": null,
      "asset": null
    }
    ```

---

### 1.2 `GET /api/v1/images/generations/{id}`
Query job status, hardware execution telemetry, and asset metadata.

#### Authorization:
- Scoped to the owning API key. Cross-client access is strictly denied with **HTTP 403 Forbidden**.

#### Responses:
- **`200 OK` (Pending or Processing):**
  Status is `"PENDING"` or `"PROCESSING"`. `asset` is `null`.
- **`200 OK` (Completed):**
  Status is `"COMPLETED"`. `asset` contains asset metadata:
  ```json
  {
    "id": "018f3a5b-9b42-7a2e-8e6f-1c4b721e0001",
    "status": "COMPLETED",
    "prompt": "A close-up portrait of an astronaut on Mars...",
    "width": 512,
    "height": 512,
    "model": "sd-1.5",
    "provider_requested": "mock",
    "provider_used": "mock",
    "execution_time_ms": 4,
    "peak_vram_mb": 0,
    "created_at": "2026-10-22T09:00:00Z",
    "completed_at": "2026-10-22T09:00:01Z",
    "asset": {
      "id": "a9f8b2c1-0000-0000-0000-000000000001",
      "storage_provider": "local_cas",
      "file_hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
      "file_size_bytes": 104230,
      "mime_type": "image/png",
      "width": 512,
      "height": 512,
      "content_url": "/api/v1/images/generations/018f3a5b-9b42-7a2e-8e6f-1c4b721e0001/content",
      "created_at": "2026-10-22T09:00:01Z"
    }
  }
  ```
- **`200 OK` (Failed):**
  Status is `"FAILED"`. `error_code` and `error_message` describe the terminal failure.

---

### 1.3 `GET /api/v1/images/generations/{id}/content`
Retrieve the generated PNG image content.

#### Authorization & Security Controls:
- **Client Ownership Enforcement:** The requesting client must own the job. Unauthorized access returns **HTTP 403 Forbidden**.
- **Safe Private Caching:**
  - `Cache-Control: private, no-transform, max-age=86400`: Allows the client browser to cache the image locally for 24 hours while explicitly instructing CDNs and intermediate shared caches NOT to store or serve the asset to other clients.
  - `ETag: "{file_hash}"`: Content-addressable SHA-256 digest allows client cache revalidation (`304 Not Modified`).
  - `Content-Type: image/png`: Verified PNG MIME type.
  - `X-Content-Type-Options: nosniff`: Prevents MIME-confusion attacks.
- *Policy Note:* Public immutable caching (`Cache-Control: public, max-age=31536000, immutable`) is strictly reserved for public CDN distribution (e.g. Cloudinary/S3 in Milestone 5) upon explicit user/product configuration.

---

### 1.4 RFC 7807 Error Response Specifications

All error responses adhere to RFC 7807 Problem Details:

```json
{
  "code": "RATE_LIMIT_EXCEEDED",
  "detail": "Rate limit exceeded. Maximum 10 requests per minute allowed.",
  "status_code": 429
}
```

| HTTP Status | Error Code | Trigger Condition | Special Headers |
| :--- | :--- | :--- | :--- |
| **400 Bad Request** | `VALIDATION_ERROR` | Schema failure, non-multiple of 64, or >512x512 | None |
| **401 Unauthorized** | `AUTHENTICATION_FAILED` | Missing, invalid, or revoked API key | None |
| **403 Forbidden** | `FORBIDDEN` | Cross-client job or asset retrieval attempt | None |
| **404 Not Found** | `NOT_FOUND` | Job or asset does not exist | None |
| **409 Conflict** | `IDEMPOTENCY_CONFLICT` | Same `Idempotency-Key` with differing payload | None |
| **429 Too Many Requests**| `RATE_LIMIT_EXCEEDED` | Sliding window RPM limit exceeded | `Retry-After: <seconds>` |

---

### 1.3 `GET /api/v1/providers/status`
Expose local GPU health and cloud provider status.

```json
{
  "active_providers": [
    {
      "name": "local-gpu",
      "type": "hardware_direct",
      "status": "ready",
      "device": "NVIDIA GeForce RTX 2050 Mobile",
      "total_vram_mb": 4096,
      "allocated_vram_mb": 2340,
      "supported_models": ["sd-turbo", "sd-1.5"]
    },
    {
      "name": "mock",
      "type": "in_memory",
      "status": "ready",
      "supported_models": ["all"]
    },
    {
      "name": "pollinations",
      "type": "cloud_free",
      "status": "standby",
      "supported_models": ["flux", "sdxl"]
    }
  ]
}
```

---

## 2. Mode B: Worker Ingress Endpoints (Control Plane)

These endpoints are called exclusively by the **Hybrid Outbound Worker** running on the user's PC via authenticated outbound HTTPS requests. No inbound ports or network listeners are opened on the development machine.

### 2.1 `POST /api/v1/worker/jobs/lease`
Acquire the next unreserved `PENDING` job using `FOR UPDATE SKIP LOCKED` (PostgreSQL) or transactional queue update (SQLite).

- **Headers:** `Authorization: Bearer <WORKER_SHARED_SECRET>`
- **Request Body:**
  ```json
  {
    "worker_id": "hybrid-worker-rtx2050-01",
    "lease_timeout_seconds": 120
  }
  ```
- **Responses:**
  - **`200 OK` (Job Claimed):**
    ```json
    {
      "job_id": "018f3a5b-9b42-7a2e-8e6f-1c4b721e0001",
      "prompt": "An astronaut on Mars...",
      "negative_prompt": "blurry",
      "width": 512,
      "height": 512,
      "steps": 4,
      "seed": 42,
      "model": "sd-1.5",
      "provider_requested": "local-gpu",
      "retry_count": 0,
      "max_retries": 3,
      "locked_at": "2026-10-22T09:30:00Z",
      "lease_timeout_seconds": 120
    }
    ```
  - **`204 No Content`:** Queue is currently empty. Worker should sleep for poll interval and retry.

---

### 2.2 `POST /api/v1/worker/jobs/{job_id}/heartbeat`
Periodically extend lease expiration during execution.

- **Headers:** `Authorization: Bearer <WORKER_SHARED_SECRET>`
- **Request Body:**
  ```json
  {
    "worker_id": "hybrid-worker-rtx2050-01"
  }
  ```
- **Responses:**
  - **`200 OK`:** `{"status": "renewed", "job_id": "...", "locked_at": "..."}`
  - **`409 Conflict`:** Worker does not hold the active lease (e.g. lease expired or stolen by sweeper).

---

### 2.3 `POST /api/v1/worker/jobs/{job_id}/complete`
Report successful execution and provide content-addressable asset metadata.

- **Headers:** `Authorization: Bearer <WORKER_SHARED_SECRET>`
- **Request Body:**
  ```json
  {
    "worker_id": "hybrid-worker-rtx2050-01",
    "file_hash": "a39fbc481d6501f28b7e283b54d63a8a9bc6810c92bf76de6f827a513515ea21",
    "file_size_bytes": 452810,
    "storage_provider": "local_cas",
    "storage_path": "images/a3/9f/a39fbc481d6501f28b7e283b54d63a8a9bc6810c92bf76de6f827a513515ea21.png",
    "public_url": null,
    "mime_type": "image/png",
    "width": 512,
    "height": 512,
    "execution_time_ms": 1420,
    "peak_vram_mb": 2480,
    "provider_used": "local-gpu"
  }
  ```
- **Responses:**
  - **`200 OK`:**
    ```json
    {
      "status": "completed",
      "job_id": "018f3a5b-9b42-7a2e-8e6f-1c4b721e0001",
      "asset_id": "a9f8b2c1-0000-0000-0000-000000000001"
    }
    ```
  - **Idempotency Guarantee:** If the network dropped after a previous call, duplicate `/complete` calls with the same file hash return `200 OK` idempotently without duplicate asset insertion.
  - **`409 Conflict`:** If `job.locked_by != req.worker_id`.

---

### 2.4 `POST /api/v1/worker/jobs/{job_id}/fail`
Report unrecoverable execution failure or schedule retry.

- **Headers:** `Authorization: Bearer <WORKER_SHARED_SECRET>`
- **Request Body:**
  ```json
  {
    "worker_id": "hybrid-worker-rtx2050-01",
    "error_code": "GPU_OUT_OF_MEMORY",
    "error_message": "CUDA out of memory during UNet forward pass.",
    "retry_delay_seconds": 10
  }
  ```
- **Responses:**
  - **`200 OK`:**
    ```json
    {
      "status": "retrying",
      "job_id": "018f3a5b-9b42-7a2e-8e6f-1c4b721e0001",
      "retry_count": 1,
      "max_retries": 3
    }
    ```
    (Or `status: "failed"` if `retry_count >= max_retries`).

