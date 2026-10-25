# ADR-008: Cloud Hybrid Worker Ingress Protocol, Row-Level Security (RLS), and Distributed Rate Limiting

**Status:** APPROVED  
**Date:** 2026-10-22  
**Context:** Aarav Image Generation API (`Inference-1`) — Milestone 5

---

## 1. Context & Problem Statement

In Milestone 5, the Aarav Image Generation API transitions to a **Hybrid Hosted Topology (Mode B)**:
- The **Control Plane** (FastAPI) and **Durable Job Queue** (Supabase PostgreSQL) are cloud-hosted, publicly accessible by web and mobile clients over HTTPS.
- The **Inference Engine** runs on the developer's local home PC equipped with an NVIDIA RTX 2050 Mobile (4 GB VRAM) GPU, operating behind a residential ISP NAT / CGNAT with no public static IP address.
- Multiple API gateway instances may run concurrently in the cloud, requiring distributed rate limiting and synchronized job queue state.
- Client assets must remain private to tenants and protected by row-level isolation and private cache directives.
- Zero infrastructure operating cost (₹0 / $0) must be preserved using validated free-tier services.

---

## 2. Decision & Architecture Principles

### 2.1 Outbound-Only Worker Polling (Zero Inbound Ports)
- The local GPU worker never listens on public or LAN ports.
- The worker operates strictly via **outbound HTTPS calls** to the cloud control plane:
  1. `POST /api/v1/worker/jobs/lease` (Claim next available job)
  2. `POST /api/v1/worker/jobs/{id}/heartbeat` (Keepalive lease renewal every 30s)
  3. `POST /api/v1/worker/jobs/{id}/complete` (Record completion and asset reference)
  4. `POST /api/v1/worker/jobs/{id}/fail` (Record failure or schedule exponential backoff retry)

### 2.2 Worker Mutual Authentication & Lease Ownership
- All `/api/v1/worker/*` endpoints require `Authorization: Bearer <WORKER_SHARED_SECRET>`.
- The secret is verified using constant-time comparison (`hmac.compare_digest`) to prevent timing side-channel attacks.
- Each worker provides a unique `worker_id` (e.g. `worker-host-<uuid>`).
- When a job is leased, `locked_by` is set to the claiming `worker_id`. Subsequent heartbeats, completions, or failures require verifying that `job.locked_by == worker_id`. Unclaimed or stolen jobs reject foreign worker actions with **HTTP 409 Conflict**.

### 2.3 Idempotent Completion & Failure Transitions
- If a network drop occurs after the worker executes inference and calls `/complete`, subsequent retries with the same job ID and identical payload/hash succeed idempotently (returning HTTP 200 with recorded asset data) rather than throwing state-machine or uniqueness errors.

### 2.4 Supabase PostgreSQL Row-Level Security (RLS)
- On PostgreSQL, `ROW LEVEL SECURITY` is enabled on `generation_jobs`, `generated_assets`, and `api_keys`.
- Backend FastAPI control planes connect via the PostgreSQL `service_role`, which bypasses RLS for queue orchestration and leasing.
- Direct PostgREST or external client connections are constrained by RLS policies:
  - `generation_jobs`: `USING (api_key_id = current_setting('request.jwt.claim.api_key_id', true))`
  - `generated_assets`: `USING (job_id IN (SELECT id FROM generation_jobs WHERE api_key_id = current_setting('request.jwt.claim.api_key_id', true)))`
- The existing SQLite dialect ignores RLS statements safely, ensuring 100% backward compatibility for Mode A local development.

### 2.5 Pluggable Shared Rate Limiting
- Defined a `RateLimiter` protocol.
- **Mode A / Local Dev:** `InMemorySlidingWindowRateLimiter` (retained, thread-safe, zero external dependencies).
- **Mode B / Cloud:** `DatabaseSlidingWindowRateLimiter` using PostgreSQL atomic timestamp tracking per `api_key_id`, or Redis rate limiting if an external cache is configured.

### 2.6 Private Remote Asset Storage
- Assets are stored in private Content-Addressable Storage (local CAS in Mode A, Cloudinary private or signed bucket in Mode B).
- Delivered with `Cache-Control: private, no-transform, max-age=86400`, `ETag: "{sha256}"`, and `X-Content-Type-Options: nosniff`.
- Public immutable caching is explicitly disallowed for private tenant assets.

---

## 3. Free-Tier Stack & Cost Model

| Component | Platform | Free Tier Allowances | Monthly Cost |
| :--- | :--- | :--- | :--- |
| **Control Plane API** | Render / Fly.io | Free Web Service (512 MB RAM, sleeps on idle) | ₹0 / $0 |
| **Relational Queue & RLS** | Supabase PostgreSQL | 500 MB DB, 50,000 MAU, 2 projects | ₹0 / $0 |
| **Asset Storage & Delivery** | Cloudinary | 25 monthly credits (~25 GB net storage/bandwidth) | ₹0 / $0 |
| **GPU Inference Worker** | Home PC | NVIDIA RTX 2050 Mobile (4 GB VRAM) | ₹0 / $0 |
| **Total Operating Cost** | | | **₹0 / month** |

---

## 4. Consequences & Trade-offs

### Positive:
- Secure, NAT-traversing architecture that never exposes the user's home network or PC.
- Robust against worker crashes and network interruptions via leased locks, heartbeats, and idempotent completion.
- Complete parity with local Mode A SQLite WAL development.

### Negative / Mitigations:
- Free-tier Render/Fly control plane may experience cold start latency (20-40s) after inactivity; mitigated by worker pinging or health keep-alive.
- Supabase pauses free databases after 7 days of inactivity; mitigated by project activity or simple scheduled health checks.
