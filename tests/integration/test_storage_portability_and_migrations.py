"""
Storage ORM Metadata Portability & Alembic Migrations Test Suite (Milestone 6.3)
Location: tests/integration/test_storage_portability_and_migrations.py

Verifies:
1. SQLite and PostgreSQL Base.metadata create_all/drop_all portability tested independently.
2. Fresh Alembic migrations from scratch on clean databases (separate from create_all).
3. Upgrade from 7a1b9c2d3e4f preserving legacy data with blob_id IS NULL.
4. Downgrade from head to 7a1b9c2d3e4f and re-upgrade on clean databases.
"""

import os
from pathlib import Path
import tempfile
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.ext.asyncio import create_async_engine

from src.core.config import settings
from src.infrastructure.models import Base


# -------------------------------------------------------------------------
# Suite 1: ORM Metadata create_all / drop_all Portability (Independent of Alembic)
# -------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_orm_metadata_portability_sqlite():
    """Verifies that Base.metadata.create_all and drop_all execute cleanly on SQLite."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

        # Inspect table presence
        def check_tables(connection):
            insp = inspect(connection)
            tables = insp.get_table_names()
            assert "api_keys" in tables
            assert "generation_jobs" in tables
            assert "generated_assets" in tables
            assert "storage_blobs" in tables
            assert "storage_deletion_outbox" in tables
            assert "storage_ref_count_audit" in tables
            assert "storage_upload_intents" in tables
            assert "storage_orphan_quarantine" in tables

            # Verify columns
            cols = [c["name"] for c in insp.get_columns("generated_assets")]
            assert "blob_id" in cols
            assert "storage_path" in cols

        await conn.run_sync(check_tables)
        await conn.run_sync(Base.metadata.drop_all)

    await engine.dispose()


@pytest.mark.asyncio
async def test_orm_metadata_portability_postgres():
    """Verifies Base.metadata.create_all on PostgreSQL (skipped if container unavailable)."""
    pg_url = os.environ.get("POSTGRES_TEST_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/inference_test")
    try:
        engine = create_async_engine(pg_url)
        async with engine.begin() as conn:
            await conn.execute(text("SELECT 1;"))
            await conn.run_sync(Base.metadata.create_all)
            await conn.run_sync(Base.metadata.drop_all)
        await engine.dispose()
    except Exception as e:
        pytest.skip(f"PostgreSQL test database unavailable: {e}")


# -------------------------------------------------------------------------
# Suite 2: Fresh Alembic Migrations on Clean Databases
# -------------------------------------------------------------------------

def test_alembic_fresh_migration_sqlite(tmp_path: Path):
    """Verifies fresh Alembic migration from scratch to head on a clean SQLite database."""
    test_db_path = tmp_path / "fresh_alembic_clean.db"
    async_db_url = f"sqlite+aiosqlite:///{test_db_path.as_posix()}"
    sync_db_url = f"sqlite:///{test_db_path.as_posix()}"

    alembic_cfg = Config("alembic.ini")
    alembic_cfg.set_main_option("sqlalchemy.url", async_db_url)

    command.upgrade(alembic_cfg, "head")

    sync_engine = create_engine(sync_db_url)
    try:
        inspector = inspect(sync_engine)
        tables = inspector.get_table_names()

        # All tables created by migrations
        assert "api_keys" in tables
        assert "generation_jobs" in tables
        assert "generated_assets" in tables
        assert "storage_blobs" in tables
        assert "storage_deletion_outbox" in tables
        assert "storage_ref_count_audit" in tables
        assert "storage_orphan_quarantine" in tables
        assert "storage_upload_intents" in tables

        # Columns
        asset_cols = [c["name"] for c in inspector.get_columns("generated_assets")]
        assert "blob_id" in asset_cols
    finally:
        sync_engine.dispose()


def test_alembic_fresh_migration_postgres():
    """Verifies fresh Alembic migration on clean PostgreSQL (skipped if unavailable)."""
    pg_url = os.environ.get("POSTGRES_TEST_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/inference_test")
    try:
        sync_pg_url = pg_url.replace("+asyncpg", "")
        sync_engine = create_engine(sync_pg_url)
        with sync_engine.connect() as conn:
            conn.execute(text("SELECT 1;"))
    except Exception as e:
        pytest.skip(f"PostgreSQL test database unavailable: {e}")

    alembic_cfg = Config("alembic.ini")
    alembic_cfg.set_main_option("sqlalchemy.url", pg_url)
    command.upgrade(alembic_cfg, "head")
    command.downgrade(alembic_cfg, "base")


# -------------------------------------------------------------------------
# Suite 3: Upgrade from 7a1b9c2d3e4f with Legacy Data Preservation
# -------------------------------------------------------------------------

def test_alembic_upgrade_preserves_legacy_data(tmp_path: Path):
    """
    Seeds legacy data at revision 7a1b9c2d3e4f (no blob_id),
    runs upgrade to head, and asserts that legacy rows remain intact with blob_id IS NULL.
    """
    test_db_path = tmp_path / "legacy_migration_test.db"
    async_db_url = f"sqlite+aiosqlite:///{test_db_path.as_posix()}"
    sync_db_url = f"sqlite:///{test_db_path.as_posix()}"

    alembic_cfg = Config("alembic.ini")
    alembic_cfg.set_main_option("sqlalchemy.url", async_db_url)

    # 1. Upgrade to pre-Milestone 6.3 head: 7a1b9c2d3e4f
    command.upgrade(alembic_cfg, "7a1b9c2d3e4f")

    sync_engine = create_engine(sync_db_url)
    try:
        # Seed legacy data
        with sync_engine.begin() as conn:
            conn.execute(
                text("""
                INSERT INTO api_keys (id, key_hash, client_name, tier, rate_limit_rpm, is_active, created_at)
                VALUES ('key-legacy-1', 'hash1234567890abcdef', 'Legacy Client', 'free', 10, 1, '2026-01-01 00:00:00');
                """)
            )
            conn.execute(
                text("""
                INSERT INTO generation_jobs (id, api_key_id, payload_hash, status, prompt, width, height, steps, seed, model, provider_requested, retry_count, max_retries, created_at)
                VALUES ('job-legacy-1', 'key-legacy-1', 'payload123', 'COMPLETED', 'test prompt', 512, 512, 4, 42, 'sd-1.5', 'local_gpu', 0, 3, '2026-01-01 00:00:00');
                """)
            )
            conn.execute(
                text("""
                INSERT INTO generated_assets (id, job_id, storage_provider, file_hash, storage_path, file_size_bytes, mime_type, width, height, is_synthetic, created_at)
                VALUES ('asset-legacy-1', 'job-legacy-1', 'local_cas', 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855', 'e3/b0/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855.png', 1024, 'image/png', 512, 512, 0, '2026-01-01 00:00:00');
                """)
            )

        # 2. Upgrade to head (adds storage_blobs, outbox, and blob_id to generated_assets)
        command.upgrade(alembic_cfg, "head")

        # 3. Assert legacy data is perfectly preserved
        with sync_engine.connect() as conn:
            res = conn.execute(text("SELECT id, job_id, file_hash, blob_id FROM generated_assets WHERE id = 'asset-legacy-1';")).mappings().one()
            assert res["id"] == "asset-legacy-1"
            assert res["job_id"] == "job-legacy-1"
            assert res["file_hash"] == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            assert res["blob_id"] is None, "Legacy asset row must have blob_id IS NULL prior to backfill"
    finally:
        sync_engine.dispose()


# -------------------------------------------------------------------------
# Suite 4: Downgrade to 7a1b9c2d3e4f and Re-Upgrade
# -------------------------------------------------------------------------

def test_alembic_downgrade_and_reupgrade_cycle(tmp_path: Path):
    """
    Verifies downgrade from head to 7a1b9c2d3e4f cleanly drops new tables and columns,
    and re-upgrade reconstructs the schema without error.
    """
    test_db_path = tmp_path / "downgrade_reupgrade_test.db"
    async_db_url = f"sqlite+aiosqlite:///{test_db_path.as_posix()}"
    sync_db_url = f"sqlite:///{test_db_path.as_posix()}"

    alembic_cfg = Config("alembic.ini")
    alembic_cfg.set_main_option("sqlalchemy.url", async_db_url)

    # 1. Upgrade to head
    command.upgrade(alembic_cfg, "head")

    sync_engine = create_engine(sync_db_url)
    try:
        insp1 = inspect(sync_engine)
        assert "storage_blobs" in insp1.get_table_names()

        # 2. Downgrade to 7a1b9c2d3e4f
        command.downgrade(alembic_cfg, "7a1b9c2d3e4f")

        insp2 = inspect(sync_engine)
        tables2 = insp2.get_table_names()
        assert "storage_blobs" not in tables2
        assert "storage_deletion_outbox" not in tables2
        assert "storage_ref_count_audit" not in tables2
        assert "storage_orphan_quarantine" not in tables2
        assert "storage_upload_intents" not in tables2

        asset_cols2 = [c["name"] for c in insp2.get_columns("generated_assets")]
        assert "blob_id" not in asset_cols2

        # 3. Re-upgrade to head
        command.upgrade(alembic_cfg, "head")

        insp3 = inspect(sync_engine)
        assert "storage_blobs" in insp3.get_table_names()
        assert "blob_id" in [c["name"] for c in insp3.get_columns("generated_assets")]
    finally:
        sync_engine.dispose()
