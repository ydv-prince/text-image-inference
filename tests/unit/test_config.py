"""
Unit Tests for Pydantic v2 Application Configuration
Location: tests/unit/test_config.py
"""

from pathlib import Path
import pytest
from src.core.config import Settings


def test_default_settings():
    """Verifies that default settings load with expected safety thresholds."""
    s = Settings(STORAGE_BASE_PATH="./storage/images")
    assert s.APP_NAME == "Aarav Image Generation API"
    assert s.LOCAL_GPU_CONCURRENCY == 1  # 4 GB VRAM protection
    assert s.MAX_LOCAL_WIDTH == 512
    assert s.MAX_LOCAL_HEIGHT == 512
    assert s.LEASE_TIMEOUT_SECONDS == 120
    assert s.STORAGE_BASE_PATH == Path("./storage/images")


def test_dialect_detection():
    """Verifies dialect helper properties for SQLite and PostgreSQL."""
    sqlite_settings = Settings(DATABASE_URL="sqlite+aiosqlite:///./test.db")
    assert sqlite_settings.is_sqlite is True
    assert sqlite_settings.is_postgresql is False

    pg_settings = Settings(DATABASE_URL="postgresql+asyncpg://user:pass@localhost:5432/db")
    assert pg_settings.is_sqlite is False
    assert pg_settings.is_postgresql is True


def test_custom_environment_override():
    """Verifies environment variables override default settings correctly."""
    custom = Settings(
        ENVIRONMENT="production",
        DEBUG=True,
        DEPLOYMENT_MODE="hybrid",
        LOCAL_GPU_CONCURRENCY=1,
    )
    assert custom.ENVIRONMENT == "production"
    assert custom.DEBUG is True
    assert custom.DEPLOYMENT_MODE == "hybrid"
