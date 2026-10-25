"""
Job Repositories Package
Location: src/infrastructure/repositories/__init__.py
"""

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.core.config import settings
from src.domain.interfaces import JobRepository
from src.infrastructure.repositories.base import BaseJobRepository
from src.infrastructure.repositories.job_postgres import PostgresJobRepository
from src.infrastructure.repositories.job_sqlite import SqliteJobRepository


def get_job_repository(session_factory: async_sessionmaker[AsyncSession]) -> JobRepository:
    """Factory selecting the dialect-specialized repository matching configuration."""
    if settings.is_postgresql:
        return PostgresJobRepository(session_factory)
    return SqliteJobRepository(session_factory)


__all__ = [
    "BaseJobRepository",
    "SqliteJobRepository",
    "PostgresJobRepository",
    "get_job_repository",
]
