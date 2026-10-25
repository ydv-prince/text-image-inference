# ADR-003: Dual-Dialect Transactional Job Queue (SQLite WAL & PostgreSQL SKIP LOCKED)

**Status:** REVISED (v2.0.0)  
**Date:** 2026-10-21  
**Author:** Principal System Design Engineer & Backend Engineer  
**Deciders:** Core Engineering Team  

---

## 1. Context & Problem Statement
Image generation is an asynchronous operation requiring a durable queue. In the initial design, a database-backed queue was chosen over Redis to ensure ACID transaction durability and eliminate dual-write risks.

However, moving between **Mode A (Local single-machine development on SQLite)** and **Mode B (Hosted cloud control plane on PostgreSQL / Supabase)** reveals semantic differences in locking mechanisms:
- In SQLite, the entire database or write transaction is locked during `BEGIN IMMEDIATE`. This is suitable for a single local process, but causes database locked errors if multiple concurrent processes write simultaneously.
- In PostgreSQL, multiple concurrent worker processes polling the queue using standard `SELECT ... FOR UPDATE` will block one another, causing latency spikes and connection pool starvation.

Simply changing `DATABASE_URL` from `sqlite:///` to `postgresql://` without altering queue query semantics does **not** prove semantic compatibility.

## 2. Decision Drivers
- **Semantic Correctness:** Queue claiming must be concurrency-safe and non-blocking in PostgreSQL, while simple and portable in SQLite.
- **Zero In-Flight Loss:** Unclaimed or crashed jobs must be recoverable via lease timeouts.
- **Portability:** Code structure must cleanly support both dialects via dedicated repository implementations adhering to `JobRepository`.

## 3. Decision Outcome
**Chosen Option: Distinct Repository Implementations for SQLite and PostgreSQL.**

### Implementation Strategy:
1. **SQLite Repository (`job_sqlite.py`):**
   Uses `BEGIN IMMEDIATE` and an atomic `UPDATE ... WHERE id = (...)` query inside an isolated transaction. Concurrency is inherently serialized by SQLite's single-writer architecture, which is ideal for single-worker local development.
2. **PostgreSQL Repository (`job_postgres.py`):**
   Utilizes `SELECT ... FOR UPDATE SKIP LOCKED` combined with a `RETURNING` clause:
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
   *Why `SKIP LOCKED`?* If Worker 1 is claiming Job A, Worker 2 instantly skips Job A and claims Job B without waiting or blocking!

### Positive Consequences:
- High throughput concurrent job leasing in PostgreSQL with zero lock contention.
- Zero-dependency local execution in SQLite with ACID safety.
- Clear code separation under `infrastructure/repositories/`.
