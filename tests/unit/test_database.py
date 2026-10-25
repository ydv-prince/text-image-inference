"""
Unit Tests for Database Schema, ORM Entities & Integrity Constraints
Location: tests/unit/test_database.py
"""

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.security import hash_api_key
from src.domain.models import ApiKeyTier, JobStatus, ProviderType, StorageType
from src.infrastructure.models import ApiKeyModel, GeneratedAssetModel, GenerationJobModel


@pytest.mark.asyncio
async def test_create_and_query_api_key(db_session: AsyncSession):
    """Verifies creating, persisting, and querying an API key entity."""
    key_hash = hash_api_key("aarav_live_client1", "test_salt")
    api_key = ApiKeyModel(
        key_hash=key_hash,
        client_name="Test-Client-1",
        tier=ApiKeyTier.FREE.value,
        rate_limit_rpm=10,
    )
    db_session.add(api_key)
    await db_session.commit()

    # Query back
    stmt = select(ApiKeyModel).where(ApiKeyModel.key_hash == key_hash)
    result = await db_session.execute(stmt)
    retrieved = result.scalar_one()

    assert retrieved is not None
    assert retrieved.client_name == "Test-Client-1"
    assert retrieved.tier == "free"
    assert retrieved.is_active is True


@pytest.mark.asyncio
async def test_job_and_asset_lifecycle_relationships(db_session: AsyncSession):
    """Verifies relational linkage between ApiKey, GenerationJob, and GeneratedAsset."""
    # 1. Create API Key
    api_key = ApiKeyModel(
        key_hash=hash_api_key("aarav_live_client2", "test_salt"),
        client_name="Test-Client-2",
    )
    db_session.add(api_key)
    await db_session.flush()

    # 2. Create Generation Job
    job = GenerationJobModel(
        api_key_id=api_key.id,
        idempotency_key="idemp-token-12345",
        payload_hash="mock_payload_sha256",
        status=JobStatus.COMPLETED.value,
        prompt="A serene mountain lake at sunrise",
        width=512,
        height=512,
        steps=4,
        model="sd-1.5",
        provider_requested=ProviderType.LOCAL_GPU.value,
        provider_used=ProviderType.LOCAL_GPU.value,
        execution_time_ms=2470,
        peak_vram_mb=3390,
    )
    db_session.add(job)
    await db_session.flush()

    # 3. Create Generated Asset
    asset = GeneratedAssetModel(
        job_id=job.id,
        storage_provider=StorageType.LOCAL_CAS.value,
        file_hash="0b042107aa4b752e44c82afb38e354c7e92ea49c1973d082886b0fb6326d2ea6",
        storage_path="images/0b/04/0b042107....png",
        file_size_bytes=383724,
        mime_type="image/png",
        width=512,
        height=512,
    )
    db_session.add(asset)
    await db_session.commit()

    # Verify query and relationships
    stmt = select(GenerationJobModel).where(GenerationJobModel.id == job.id)
    res = await db_session.execute(stmt)
    queried_job = res.scalar_one()

    assert queried_job.api_key.client_name == "Test-Client-2"
    assert queried_job.asset is not None
    assert queried_job.asset.file_hash.startswith("0b0421")
    assert queried_job.execution_time_ms == 2470
    assert queried_job.peak_vram_mb == 3390


@pytest.mark.asyncio
async def test_idempotency_unique_constraint(db_session: AsyncSession):
    """Verifies that duplicate (api_key_id, idempotency_key) raises an IntegrityError."""
    api_key = ApiKeyModel(
        key_hash=hash_api_key("aarav_live_client3", "test_salt"),
        client_name="Test-Client-3",
    )
    db_session.add(api_key)
    await db_session.flush()

    job1 = GenerationJobModel(
        api_key_id=api_key.id,
        idempotency_key="duplicate_key_1",
        payload_hash="hash1",
        prompt="Prompt 1",
    )
    db_session.add(job1)
    await db_session.commit()

    # Attempt to insert identical key for same client
    job2 = GenerationJobModel(
        api_key_id=api_key.id,
        idempotency_key="duplicate_key_1",
        payload_hash="hash1",
        prompt="Prompt 1 Duplicate",
    )
    db_session.add(job2)

    with pytest.raises(IntegrityError):
        await db_session.commit()
    await db_session.rollback()


@pytest.mark.asyncio
async def test_set_tenant_context_sqlite_safe(db_session: AsyncSession):
    """Verifies that set_tenant_context executes safely as a no-op on SQLite."""
    from src.infrastructure.database import set_tenant_context
    await set_tenant_context(db_session, "test-tenant-uuid-1234")
    # Verify session remains operational
    res = await db_session.execute(select(1))
    assert res.scalar() == 1


@pytest.mark.asyncio
async def test_set_tenant_context_postgresql_emulation(monkeypatch):
    """Verifies that set_tenant_context issues SET LOCAL on PostgreSQL."""
    from src.infrastructure.database import set_tenant_context
    from src.core.config import settings

    monkeypatch.setattr(settings, "DATABASE_URL", "postgresql+asyncpg://user:pass@localhost:5432/testdb")

    executed_sqls = []

    class MockPgSession:
        async def execute(self, statement, params=None):
            executed_sqls.append((str(statement), params))

    mock_sess = MockPgSession()
    await set_tenant_context(mock_sess, "tenant-pg-abc")

    assert len(executed_sqls) == 1
    sql, params = executed_sqls[0]
    assert "SET LOCAL request.jwt.claim.api_key_id" in sql
    assert params == {"api_key_id": "tenant-pg-abc"}
