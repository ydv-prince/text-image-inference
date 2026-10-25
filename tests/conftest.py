"""
Pytest Test Configuration & Shared Fixtures
Location: tests/conftest.py
"""

import os
from pathlib import Path
from typing import AsyncGenerator
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

# Set testing environment variables before importing app
os.environ["ENVIRONMENT"] = "testing"
os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///:memory:"
os.environ["STORAGE_BASE_PATH"] = "./tests/scratch_storage"
os.environ["RUN_EMBEDDED_WORKER"] = "false"

from sqlalchemy.pool import NullPool
from src.core.config import settings
from src.infrastructure.database import configure_sqlite_pragmas, get_db_session
from src.infrastructure.models import Base
from src.main import create_app

TEST_DB_FILE = Path("./tests/test_run.db")

# Test file-based SQLite async engine with NullPool and WAL for realistic concurrent transaction isolation
test_engine = create_async_engine(
    f"sqlite+aiosqlite:///{TEST_DB_FILE.as_posix()}",
    connect_args={"check_same_thread": False},
    poolclass=NullPool,
)
TestingSessionLocal = async_sessionmaker(
    bind=test_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


@pytest_asyncio.fixture(scope="function")
async def db_session() -> AsyncGenerator[AsyncSession, None]:
    """Provides an isolated database session with freshly initialized schema."""
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with TestingSessionLocal() as session:
        await configure_sqlite_pragmas(session)
        yield session

    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest_asyncio.fixture(scope="function")
async def session_factory() -> AsyncGenerator[async_sessionmaker[AsyncSession], None]:
    """Provides an async_sessionmaker bound to test_engine with freshly initialized schema."""
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield TestingSessionLocal

    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


from src.infrastructure.database import configure_sqlite_pragmas, get_db_session, get_session_factory


@pytest_asyncio.fixture(scope="function")
async def client(session_factory) -> AsyncGenerator[AsyncClient, None]:
    """Provides an async HTTP test client with ASGI transport and overridden db session."""
    app = create_app()

    async def override_get_db_session():
        async with session_factory() as session:
            try:
                await configure_sqlite_pragmas(session)
                yield session
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()

    app.dependency_overrides[get_db_session] = override_get_db_session
    app.dependency_overrides[get_session_factory] = lambda: session_factory
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac
    app.dependency_overrides.clear()


@pytest.fixture(autouse=True)
def cleanup_scratch_storage():
    """Ensures test scratch storage directory is cleaned up before and after tests."""
    scratch_dir = Path("./tests/scratch_storage")
    scratch_dir.mkdir(parents=True, exist_ok=True)
    yield
    # Cleanup after test
    for file in scratch_dir.glob("*"):
        if file.is_file():
            file.unlink(missing_ok=True)
