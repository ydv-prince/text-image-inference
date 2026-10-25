"""
Asynchronous Database Engine & Session Management
Location: src/infrastructure/database.py
"""

from typing import AsyncGenerator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool, QueuePool

from src.core.config import settings
from src.core.logging import logger
from src.infrastructure.models import Base


def get_engine(db_url: str = settings.DATABASE_URL) -> AsyncEngine:
    """Creates a configured AsyncEngine based on dialect."""
    connect_args = {}

    if "sqlite" in db_url.lower():
        # SQLite connection optimizations
        connect_args["check_same_thread"] = False
        engine = create_async_engine(
            db_url,
            echo=settings.DB_ECHO,
            connect_args=connect_args,
            poolclass=NullPool,  # aiosqlite handles pooling internally or NullPool avoids cross-thread locks
        )
    else:
        # PostgreSQL pool configuration
        # When using transaction pooling (e.g. Supabase port 6543 / Supavisor / PgBouncer),
        # disable asyncpg prepared statement caching to prevent cache collisions across multiplexed connections.
        if ":6543" in db_url or "pooler" in db_url.lower():
            connect_args["statement_cache_size"] = 0
            connect_args["prepared_statement_cache_size"] = 0

        engine = create_async_engine(
            db_url,
            echo=settings.DB_ECHO,
            pool_size=settings.DB_POOL_SIZE,
            max_overflow=settings.DB_MAX_OVERFLOW,
            connect_args=connect_args,
            poolclass=QueuePool,
        )

    return engine


# Global engine and session factory
engine: AsyncEngine = get_engine()
async_session_factory = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """Dependency provider returning the active session factory."""
    return async_session_factory


async def configure_sqlite_pragmas(session: AsyncSession) -> None:
    """Applies optimal performance & integrity pragmas for SQLite WAL mode."""
    if settings.is_sqlite:
        await session.execute(text("PRAGMA journal_mode=WAL;"))
        await session.execute(text("PRAGMA synchronous=NORMAL;"))
        await session.execute(text("PRAGMA foreign_keys=ON;"))
        await session.execute(text("PRAGMA busy_timeout=5000;"))


async def set_tenant_context(session: AsyncSession, api_key_id: str) -> None:
    """
    Sets the active tenant ID in PostgreSQL transaction-scoped settings (SET LOCAL).
    Because SET LOCAL is scoped strictly to the current transaction, it automatically
    resets upon commit or rollback, preventing tenant context leakage across pooled connections.
    On SQLite, this is a safe no-op.
    """
    if not settings.is_sqlite:
        await session.execute(
            text("SET LOCAL request.jwt.claim.api_key_id = :api_key_id;"),
            {"api_key_id": api_key_id},
        )


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """Dependency provider for FastAPI route handlers."""
    async with async_session_factory() as session:
        try:
            await configure_sqlite_pragmas(session)
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def check_database_health() -> bool:
    """Executes a ping query to verify database connectivity and readiness."""
    try:
        async with async_session_factory() as session:
            await session.execute(text("SELECT 1;"))
            return True
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        return False


async def init_db() -> None:
    """Creates all database tables directly (for local development and testing)."""
    async with engine.begin() as conn:
        if settings.is_sqlite:
            await conn.execute(text("PRAGMA foreign_keys=ON;"))
        await conn.run_sync(Base.metadata.create_all)
