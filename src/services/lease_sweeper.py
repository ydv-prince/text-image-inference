"""
Stale Lease Sweeper & Recovery Service
Location: src/services/lease_sweeper.py

Guarantees:
1. Reclaims stalled jobs where locked_at exceeds configurable timeout.
2. Optimistic Concurrency Guard: Sweeper cannot stomp leases renewed by active heartbeats.
3. Bounded Backoff: Rescheduled jobs receive exponential backoff with upper bounds.
4. Terminal Accounting: Jobs exceeding max_retries transition to FAILED with structured error telemetry.
"""

import asyncio
from typing import List, Optional

from src.core.config import settings
from src.core.logging import logger
from src.core.metrics import leases_reclaimed_total
from src.domain.interfaces import JobRepository


class LeaseSweeper:
    """
    Background daemon periodically sweeping the job queue for timed-out leases.
    """

    def __init__(
        self,
        repository: JobRepository,
        lease_timeout_seconds: Optional[int] = None,
        sweep_interval_seconds: Optional[float] = None,
        max_retries: Optional[int] = None,
        base_backoff_seconds: Optional[float] = None,
        max_backoff_seconds: Optional[float] = None,
    ):
        self.repository = repository
        self.lease_timeout_seconds = (
            lease_timeout_seconds if lease_timeout_seconds is not None else settings.LEASE_TIMEOUT_SECONDS
        )
        self.sweep_interval_seconds = (
            sweep_interval_seconds if sweep_interval_seconds is not None else settings.SWEEPER_INTERVAL_SECONDS
        )
        self.max_retries = max_retries if max_retries is not None else settings.MAX_JOB_RETRIES
        self.base_backoff_seconds = (
            base_backoff_seconds if base_backoff_seconds is not None else settings.BASE_RETRY_BACKOFF_SECONDS
        )
        self.max_backoff_seconds = (
            max_backoff_seconds if max_backoff_seconds is not None else settings.MAX_RETRY_BACKOFF_SECONDS
        )
        self._stop_event = asyncio.Event()

    async def sweep_once(self) -> List[str]:
        """
        Executes a single sweep pass, returning list of reclaimed job IDs.
        """
        try:
            reclaimed = await self.repository.reclaim_expired_leases(
                lease_timeout_seconds=self.lease_timeout_seconds,
                max_retries=self.max_retries,
                base_backoff_seconds=self.base_backoff_seconds,
                max_backoff_seconds=self.max_backoff_seconds,
            )
            if reclaimed:
                try:
                    leases_reclaimed_total.inc(len(reclaimed))
                except Exception:
                    pass
            return reclaimed
        except Exception as e:
            logger.error(f"Error during lease sweep pass: {e}", exc_info=True)
            return []

    async def run(self) -> None:
        """Runs the continuous sweeper loop until stopped."""
        logger.info(
            f"LeaseSweeper started (timeout={self.lease_timeout_seconds}s, interval={self.sweep_interval_seconds}s)"
        )
        try:
            while not self._stop_event.is_set():
                await self.sweep_once()
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=self.sweep_interval_seconds)
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            logger.info("LeaseSweeper received cancellation request.")
        finally:
            logger.info("LeaseSweeper stopped.")

    def stop(self) -> None:
        """Signals the sweeper loop to stop."""
        self._stop_event.set()
