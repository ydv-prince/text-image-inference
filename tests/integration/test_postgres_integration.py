"""
PostgreSQL Live Integration Test Suite
Location: tests/integration/test_postgres_integration.py

Verifies against disposable PostgreSQL 16 container:
1. Full Alembic migration upgrade, downgrade, and re-upgrade.
2. Concurrent FOR UPDATE SKIP LOCKED job leasing mutual exclusion.
3. Concurrent identical idempotency-key submissions under transaction isolation.
4. Lease heartbeat, expiry, and stale-worker completion rejection.
5. Retry scheduling and terminal failure behavior.
6. Real RLS policies and tenant isolation using actual non-superuser database roles.
7. Transaction rollback and connection-pool reuse across different tenant contexts.
"""

import asyncio
from datetime import datetime, timedelta, timezone
import os
import uuid
import pytest
from alembic import command
from alembic.config import Config
import asyncpg
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.api.v1.schemas import GenerationCreateRequest
from src.core.errors import (
    IdempotencyConflictException,
    InvalidStateTransitionException,
    LeaseExpiredException,
)
from src.core.security import hash_api_key
from src.domain.interfaces import JobCompletionData, StoredAsset
from src.domain.models import JobStatus, StorageType
from src.infrastructure.models import ApiKeyModel, GenerationJobModel
from src.infrastructure.repositories.job_postgres import PostgresJobRepository
from src.services.generation_service import GenerationService

PG_URL = os.environ.get(
    "POSTGRES_TEST_DATABASE_URL",
    "postgresql+asyncpg://aarav_test:disposable_test_pass_9283@127.0.0.1:5433/aarav_test_db",
)
RAW_PG_URL = PG_URL.replace("+asyncpg", "")


def is_postgres_available() -> bool:
    import socket
    from urllib.parse import urlparse
    parsed = urlparse(RAW_PG_URL)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 5432
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except (OSError, ConnectionRefusedError):
        return False


pytestmark = pytest.mark.skipif(
    not is_postgres_available(),
    reason="Disposable PostgreSQL test container/instance is offline."
)


@pytest.fixture(scope="module")
def pg_engine():
    from sqlalchemy.pool import NullPool
    engine = create_async_engine(PG_URL, echo=False, poolclass=NullPool)
    yield engine
    asyncio.run(engine.dispose())


@pytest.fixture(scope="module")
def pg_session_factory(pg_engine):
    return async_sessionmaker(
        bind=pg_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )


@pytest.fixture(autouse=True, scope="module")
def setup_postgres_schema():
    """Applies migrations to ensure database is at head."""
    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", PG_URL)
    command.upgrade(cfg, "head")
    yield


@pytest.fixture(autouse=True)
async def clean_tables():
    """Ensures each test starts with empty tables."""
    conn = await asyncpg.connect(RAW_PG_URL)
    try:
        await conn.execute("TRUNCATE TABLE generated_assets, generation_jobs, api_keys CASCADE;")
    except Exception:
        pass
    finally:
        await conn.close()
    yield


@pytest.mark.asyncio
async def test_postgres_migration_lifecycle():
    """Tests upgrade -> downgrade -> re-upgrade and asserts RLS flags on PostgreSQL 16."""
    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", PG_URL)

    # 1. Downgrade to base (run in worker thread to prevent event loop collision)
    await asyncio.to_thread(command.downgrade, cfg, "base")

    # Verify tables are dropped
    conn = await asyncpg.connect(RAW_PG_URL)
    tables = await conn.fetch(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename IN ('generation_jobs', 'api_keys', 'generated_assets');"
    )
    assert len(tables) == 0

    # 2. Re-upgrade to head
    await asyncio.to_thread(command.upgrade, cfg, "head")

    # Verify tables exist and RLS is enabled and forced
    rows = await conn.fetch(
        """
        SELECT relname, relrowsecurity, relforcerowsecurity 
        FROM pg_class 
        WHERE relname IN ('generation_jobs', 'generated_assets', 'api_keys')
        ORDER BY relname;
        """
    )
    await conn.close()

    assert len(rows) == 3
    for r in rows:
        assert r["relrowsecurity"] is True, f"RLS not enabled on {r['relname']}"
        assert r["relforcerowsecurity"] is True, f"FORCE RLS not enabled on {r['relname']}"


@pytest.mark.asyncio
async def test_postgres_concurrent_skip_locked_job_leasing(pg_session_factory):
    """
    Tests that multiple concurrent worker connections using FOR UPDATE SKIP LOCKED
    lease distinct jobs without collisions or deadlocks.
    """
    repo = PostgresJobRepository(pg_session_factory)
    now_utc = datetime.now(timezone.utc)

    # 1. Create a dummy API key
    api_key_id = str(uuid.uuid4())
    async with pg_session_factory() as session:
        async with session.begin():
            key = ApiKeyModel(
                id=api_key_id,
                key_hash=hash_api_key(f"test_skip_{uuid.uuid4().hex[:8]}", "test_salt"),
                client_name="Skip-Locked-Tester",
                tier="free",
                rate_limit_rpm=60,
                is_active=True,
                created_at=now_utc,
            )
            session.add(key)

            # Insert 5 pending jobs with required payload_hash
            job_ids = []
            for i in range(5):
                jid = str(uuid.uuid4())
                job_ids.append(jid)
                j = GenerationJobModel(
                    id=jid,
                    api_key_id=api_key_id,
                    payload_hash=f"hash_skip_{i}_{uuid.uuid4().hex[:8]}",
                    status=JobStatus.PENDING.value,
                    prompt=f"Prompt for job {i}",
                    width=512,
                    height=512,
                    steps=4,
                    seed=1000 + i,
                    model="sd15",
                    provider_requested="mock",
                    created_at=now_utc,
                )
                session.add(j)

    # 2. Launch 5 concurrent worker lease tasks
    async def claim_task(worker_idx: int):
        return await repo.claim_next_job(worker_id=f"worker-{worker_idx}")

    claims = await asyncio.gather(*[claim_task(i) for i in range(5)])

    # 3. Assertions
    claimed_job_ids = [c.job_id for c in claims if c is not None]
    assert len(claimed_job_ids) == 5, "All 5 jobs should be leased"
    # Mutual exclusion: All leased job IDs must be unique!
    assert len(set(claimed_job_ids)) == 5, "Duplicate job claimed! FOR UPDATE SKIP LOCKED failed mutual exclusion."

    # 6th attempt should return None (queue exhausted)
    sixth = await repo.claim_next_job(worker_id="worker-extra")
    assert sixth is None


@pytest.mark.asyncio
async def test_postgres_concurrent_idempotent_generation_requests(pg_session_factory):
    """
    Tests that concurrent identical submissions with the same idempotency key
    resolve atomically to the same job record on PostgreSQL.
    """
    now_utc = datetime.now(timezone.utc)
    api_key_id = str(uuid.uuid4())

    async with pg_session_factory() as session:
        async with session.begin():
            key = ApiKeyModel(
                id=api_key_id,
                key_hash=hash_api_key(f"test_idemp_{uuid.uuid4().hex[:8]}", "test_salt"),
                client_name="Idemp-Tester",
                tier="free",
                rate_limit_rpm=60,
                is_active=True,
                created_at=now_utc,
            )
            session.add(key)

    shared_idemp_key = f"idemp-race-{uuid.uuid4().hex[:8]}"
    req = GenerationCreateRequest(
        prompt="A vibrant autumn forest with golden sunlight",
        width=512,
        height=512,
        steps=4,
        seed=42,
    )

    async def submit():
        async with pg_session_factory() as session:
            job, _ = await GenerationService.create_or_get_generation(
                api_key=key,
                req=req,
                idempotency_key=shared_idemp_key,
                db=session,
            )
            return job.id

    # 5 concurrent submissions
    results = await asyncio.gather(*[submit() for _ in range(5)])

    # All 5 must return the exact same job ID!
    assert len(set(results)) == 1, f"Concurrent idempotency race produced divergent jobs: {results}"

    # Conflicting payload must raise IdempotencyConflictException (HTTP 409)
    conflicting_req = GenerationCreateRequest(
        prompt="A completely different prompt",
        width=512,
        height=512,
        steps=4,
        seed=42,
    )
    async with pg_session_factory() as session:
        with pytest.raises(IdempotencyConflictException):
            await GenerationService.create_or_get_generation(
                api_key=key,
                req=conflicting_req,
                idempotency_key=shared_idemp_key,
                db=session,
            )


@pytest.mark.asyncio
async def test_postgres_lease_heartbeat_expiry_and_stale_worker(pg_session_factory):
    """
    Verifies heartbeat extension, expired lease sweeping, and rejection
    of stale worker completion on PostgreSQL.
    """
    repo = PostgresJobRepository(pg_session_factory)
    now_utc = datetime.now(timezone.utc)
    api_key_id = str(uuid.uuid4())
    job_id = str(uuid.uuid4())

    async with pg_session_factory() as session:
        async with session.begin():
            key = ApiKeyModel(
                id=api_key_id,
                key_hash=hash_api_key(f"test_hb_{uuid.uuid4().hex[:8]}", "test_salt"),
                client_name="Heartbeat-Tester",
                tier="free",
                rate_limit_rpm=60,
                is_active=True,
                created_at=now_utc,
            )
            session.add(key)
            job = GenerationJobModel(
                id=job_id,
                api_key_id=api_key_id,
                payload_hash="hb_payload_hash",
                status=JobStatus.PENDING.value,
                prompt="Heartbeat test prompt",
                width=512,
                height=512,
                steps=4,
                seed=999,
                created_at=now_utc,
            )
            session.add(job)

    # 1. Lease job with worker-1
    claim = await repo.claim_next_job(worker_id="worker-1")
    assert claim is not None
    assert claim.job_id == job_id

    # 2. Record heartbeat
    hb_success = await repo.record_heartbeat(job_id=job_id, worker_id="worker-1")
    assert hb_success is True

    # Stale/unauthorized worker heartbeat must return False
    stale_hb = await repo.record_heartbeat(job_id=job_id, worker_id="impostor-worker")
    assert stale_hb is False

    # 3. Simulate expired lease by setting locked_at 300s in the past
    past_utc = now_utc - timedelta(seconds=300)
    async with pg_session_factory() as session:
        async with session.begin():
            await session.execute(
                text("UPDATE generation_jobs SET locked_at = :past WHERE id = :jid"),
                {"past": past_utc, "jid": job_id},
            )

    # 4. Sweeper reclaims expired lease
    reclaimed = await repo.reclaim_expired_leases(lease_timeout_seconds=120)
    assert job_id in reclaimed

    # 5. Worker-1 attempts completion after lease was swept: must be REJECTED!
    asset_data = StoredAsset(
        asset_id=str(uuid.uuid4()),
        file_hash="b" * 64,
        storage_path="images/bb/bb/bb.png",
        file_size_bytes=1024,
        mime_type="image/png",
        width=512,
        height=512,
        is_synthetic=True,
    )
    completion = JobCompletionData(
        job_id=job_id,
        worker_id="worker-1",
        asset=asset_data,
        public_url=None,
        execution_time_ms=150,
        peak_vram_mb=0,
        provider_used="mock",
    )

    # 5a. Worker-1 attempts completion while job is in PENDING -> Rejected with InvalidStateTransitionException
    with pytest.raises(InvalidStateTransitionException):
        await repo.complete_job(completion)

    # 5b. Worker-2 re-leases the job
    async with pg_session_factory() as session:
        async with session.begin():
            await session.execute(
                text("UPDATE generation_jobs SET scheduled_at = :now WHERE id = :jid"),
                {"now": now_utc, "jid": job_id},
            )
    claim2 = await repo.claim_next_job(worker_id="worker-2")
    assert claim2 is not None
    assert claim2.job_id == job_id

    # 5c. Stale Worker-1 attempts completion while Worker-2 holds the lease -> Rejected with LeaseExpiredException!
    with pytest.raises(LeaseExpiredException):
        await repo.complete_job(completion)


@pytest.mark.asyncio
async def test_postgres_retry_scheduling_and_terminal_failure(pg_session_factory):
    """
    Verifies exponential backoff retry scheduling and terminal failure on PostgreSQL.
    """
    repo = PostgresJobRepository(pg_session_factory)
    now_utc = datetime.now(timezone.utc)
    api_key_id = str(uuid.uuid4())
    job_id = str(uuid.uuid4())

    async with pg_session_factory() as session:
        async with session.begin():
            key = ApiKeyModel(
                id=api_key_id,
                key_hash=hash_api_key(f"test_retry_{uuid.uuid4().hex[:8]}", "test_salt"),
                client_name="Retry-Tester",
                tier="free",
                rate_limit_rpm=60,
                is_active=True,
                created_at=now_utc,
            )
            session.add(key)
            job = GenerationJobModel(
                id=job_id,
                api_key_id=api_key_id,
                payload_hash="retry_payload_hash",
                status=JobStatus.PENDING.value,
                prompt="Retry test prompt",
                width=512,
                height=512,
                steps=4,
                seed=777,
                retry_count=0,
                max_retries=2,
                created_at=now_utc,
            )
            session.add(job)

    # Lease 1
    claim1 = await repo.claim_next_job("worker-r1")
    assert claim1 is not None

    # Fail 1 (retryable) -> becomes PENDING with retry_count=1
    await repo.fail_job(
        job_id=job_id,
        worker_id="worker-r1",
        error_code="CUDA_OOM",
        error_message="Simulated temporary OOM",
        is_retryable=True,
        backoff_seconds=0.01,
    )

    await asyncio.sleep(0.05)

    # Lease 2
    claim2 = await repo.claim_next_job("worker-r2")
    assert claim2 is not None
    assert claim2.retry_count == 1

    # Fail 2 (reaches max_retries=2) -> becomes FAILED
    await repo.fail_job(
        job_id=job_id,
        worker_id="worker-r2",
        error_code="CUDA_OOM",
        error_message="Permanent failure after retry",
        is_retryable=True,
    )

    # Verify status is terminal FAILED and cannot be leased again
    claim3 = await repo.claim_next_job("worker-r3")
    assert claim3 is None

    async with pg_session_factory() as session:
        res = await session.execute(select(GenerationJobModel).where(GenerationJobModel.id == job_id))
        final_job = res.scalar_one()
        assert final_job.status == JobStatus.FAILED.value
        assert final_job.retry_count == 2
        assert final_job.error_code == "CUDA_OOM"


@pytest.mark.asyncio
async def test_postgres_rls_tenant_isolation_under_database_role():
    """
    Tests Row-Level Security (RLS) on PostgreSQL using a non-superuser role.
    Verifies that SET LOCAL request.jwt.claim.api_key_id restricts visible rows
    strictly to the designated tenant.
    """
    now_utc = datetime.now(timezone.utc)
    tenant_a_id = str(uuid.uuid4())
    tenant_b_id = str(uuid.uuid4())
    job_a_id = str(uuid.uuid4())
    job_b_id = str(uuid.uuid4())

    # 1. Setup non-superuser role and test data using owner connection
    admin_conn = await asyncpg.connect(RAW_PG_URL)
    await admin_conn.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'aarav_test_client_role') THEN
                CREATE ROLE aarav_test_client_role LOGIN PASSWORD 'test_client_password_123';
            END IF;
        END
        $$;
        GRANT USAGE ON SCHEMA public TO aarav_test_client_role;
        GRANT ALL ON ALL TABLES IN SCHEMA public TO aarav_test_client_role;
    """)

    # Seed jobs for Tenant A and Tenant B
    await admin_conn.execute(
        """
        INSERT INTO api_keys (id, key_hash, client_name, tier, rate_limit_rpm, is_active, created_at)
        VALUES 
            ($1, 'hash_tenant_a', 'Tenant A', 'free', 60, true, $3),
            ($2, 'hash_tenant_b', 'Tenant B', 'free', 60, true, $3);
        """,
        tenant_a_id,
        tenant_b_id,
        now_utc,
    )
    await admin_conn.execute(
        """
        INSERT INTO generation_jobs (id, api_key_id, payload_hash, status, prompt, width, height, steps, seed, model, provider_requested, retry_count, max_retries, created_at)
        VALUES 
            ($1, $2, 'hash_a', 'pending', 'Tenant A prompt', 512, 512, 4, 1, 'sd15', 'mock', 0, 3, $5),
            ($3, $4, 'hash_b', 'pending', 'Tenant B prompt', 512, 512, 4, 2, 'sd15', 'mock', 0, 3, $5);
        """,
        job_a_id,
        tenant_a_id,
        job_b_id,
        tenant_b_id,
        now_utc,
    )
    await admin_conn.close()

    # 2. Connect as non-superuser role (aarav_test_client_role)
    client_pg_url = "postgresql://aarav_test_client_role:test_client_password_123@127.0.0.1:5433/aarav_test_db"
    client_conn = await asyncpg.connect(client_pg_url)

    try:
        # A. Unauthenticated / unset claim -> 0 rows returned
        async with client_conn.transaction():
            rows_none = await client_conn.fetch("SELECT id FROM generation_jobs;")
            assert len(rows_none) == 0, "Unset claim should see zero rows under RLS"

        # B. Transaction scoped to Tenant A -> Only Tenant A's job visible
        async with client_conn.transaction():
            await client_conn.execute(f"SET LOCAL request.jwt.claim.api_key_id = '{tenant_a_id}';")
            rows_a = await client_conn.fetch("SELECT id, api_key_id FROM generation_jobs;")
            assert len(rows_a) == 1
            assert rows_a[0]["id"] == job_a_id
            assert rows_a[0]["api_key_id"] == tenant_a_id

        # C. Transaction scoped to Tenant B -> Only Tenant B's job visible
        async with client_conn.transaction():
            await client_conn.execute(f"SET LOCAL request.jwt.claim.api_key_id = '{tenant_b_id}';")
            rows_b = await client_conn.fetch("SELECT id, api_key_id FROM generation_jobs;")
            assert len(rows_b) == 1
            assert rows_b[0]["id"] == job_b_id
            assert rows_b[0]["api_key_id"] == tenant_b_id

    finally:
        await client_conn.close()


@pytest.mark.asyncio
async def test_postgres_connection_pool_reuse_and_transaction_cleanup():
    """
    Tests that SET LOCAL request.jwt.claim.api_key_id does NOT leak tenant context
    across reused physical connections in a connection pool.
    """
    conn = await asyncpg.connect(RAW_PG_URL)

    try:
        # Transaction 1: Sets Tenant A context and commits
        async with conn.transaction():
            await conn.execute("SET LOCAL request.jwt.claim.api_key_id = 'tenant-xyz-789';")
            val1 = await conn.fetchval("SELECT current_setting('request.jwt.claim.api_key_id', true);")
            assert val1 == "tenant-xyz-789"

        # Transaction 2 on the SAME physical connection: Must be empty/cleared!
        async with conn.transaction():
            val2 = await conn.fetchval("SELECT current_setting('request.jwt.claim.api_key_id', true);")
            assert val2 == "" or val2 is None, f"Tenant context leaked across transaction boundary: {val2}"

        # Transaction 3: Sets Tenant B and rolls back
        try:
            async with conn.transaction():
                await conn.execute("SET LOCAL request.jwt.claim.api_key_id = 'tenant-abc-123';")
                raise RuntimeError("Simulated transaction rollback")
        except RuntimeError:
            pass

        # Transaction 4: Must remain empty after rollback!
        async with conn.transaction():
            val4 = await conn.fetchval("SELECT current_setting('request.jwt.claim.api_key_id', true);")
            assert val4 == "" or val4 is None, f"Tenant context leaked after rollback: {val4}"

    finally:
        await conn.close()
