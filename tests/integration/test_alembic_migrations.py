"""
Integration Tests for Alembic Migration Lifecycle (Upgrade / Downgrade)
Location: tests/integration/test_alembic_migrations.py
"""

from pathlib import Path
from alembic import command
from alembic.config import Config
import pytest
from sqlalchemy import create_engine, inspect


def test_alembic_full_upgrade_downgrade_cycle(tmp_path: Path):
    """
    Verifies that Alembic can apply all migrations from scratch to head,
    downgrade cleanly back to base, and re-upgrade to head in a disposable database.
    """
    test_db_path = tmp_path / "disposable_migration_test.db"
    async_db_url = f"sqlite+aiosqlite:///{test_db_path.as_posix()}"
    sync_db_url = f"sqlite:///{test_db_path.as_posix()}"

    # Load alembic.ini configuration and override sqlalchemy.url
    alembic_cfg = Config("alembic.ini")
    alembic_cfg.set_main_option("sqlalchemy.url", async_db_url)

    sync_engine = create_engine(sync_db_url)

    # 1. Upgrade from base to head
    command.upgrade(alembic_cfg, "head")

    inspector = inspect(sync_engine)
    tables = inspector.get_table_names()
    assert "api_keys" in tables
    assert "generation_jobs" in tables
    assert "generated_assets" in tables

    # Verify Milestone 3 schema additions
    job_cols = [c["name"] for c in inspector.get_columns("generation_jobs")]
    assert "scheduled_at" in job_cols
    assert "status" in job_cols
    assert "locked_by" in job_cols

    asset_cols = [c["name"] for c in inspector.get_columns("generated_assets")]
    assert "is_synthetic" in asset_cols
    assert "file_hash" in asset_cols

    # 2. Downgrade to base
    command.downgrade(alembic_cfg, "base")

    inspector_after_down = inspect(sync_engine)
    tables_after_down = inspector_after_down.get_table_names()
    assert "generation_jobs" not in tables_after_down
    assert "api_keys" not in tables_after_down
    assert "generated_assets" not in tables_after_down

    # 3. Re-upgrade to head
    command.upgrade(alembic_cfg, "head")
    inspector_reup = inspect(sync_engine)
    assert "generation_jobs" in inspector_reup.get_table_names()
