"""
Base Repository & Helper Mappings
Location: src/infrastructure/repositories/base.py
"""

from datetime import datetime, timezone
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.domain.interfaces import JobClaim, JobRepository
from src.infrastructure.models import GenerationJobModel


class BaseJobRepository(JobRepository):
    """
    Common base repository containing session factory and entity mappers.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self.session_factory = session_factory

    @staticmethod
    def map_row_to_claim(row) -> JobClaim:
        """Constructs an immutable JobClaim from a database result row or model."""
        return JobClaim(
            job_id=row.id,
            prompt=row.prompt,
            negative_prompt=row.negative_prompt,
            width=row.width,
            height=row.height,
            steps=row.steps,
            seed=row.seed,
            model=row.model,
            provider_requested=row.provider_requested,
            retry_count=row.retry_count,
            max_retries=row.max_retries,
            locked_by=row.locked_by or "unknown",
            locked_at=row.locked_at or datetime.now(timezone.utc),
        )
