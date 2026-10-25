"""
Unit Tests — Hosted Lease Sweeper & Lifespan Lifecycle
Location: tests/unit/test_lifespan_sweeper.py
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import FastAPI

from src.main import lifespan
from src.core.config import settings
from src.services.lease_sweeper import LeaseSweeper


class TestLifespanSweeper:
    """Tests lifecycle decoupling between LeaseSweeper and QueueWorker."""

    @pytest.mark.asyncio
    async def test_hosted_configuration_sweeper_runs_worker_disabled(self):
        """
        When RUN_EMBEDDED_WORKER=False and RUN_LEASE_SWEEPER=True (Hosted Cloud API config):
        - LeaseSweeper background task is started.
        - QueueWorker is NOT started.
        - Shutdown cleans up sweeper task without errors.
        """
        app = FastAPI()
        mock_engine = MagicMock()
        mock_engine.dispose = AsyncMock()

        with patch.object(settings, "RUN_EMBEDDED_WORKER", False), \
             patch.object(settings, "RUN_LEASE_SWEEPER", True), \
             patch("src.main.check_database_health", new_callable=AsyncMock) as mock_db, \
             patch("src.main.engine", mock_engine):

            mock_db.return_value = True

            async with lifespan(app):
                # Verify sweeper is running on app state
                assert hasattr(app.state, "sweeper")
                assert isinstance(app.state.sweeper, LeaseSweeper)
                # Verify embedded worker is NOT running
                assert not hasattr(app.state, "worker")

            # After exit, sweeper should be stopped
            assert app.state.sweeper._stop_event.is_set()

    @pytest.mark.asyncio
    async def test_local_configuration_both_run(self):
        """
        When RUN_EMBEDDED_WORKER=True and RUN_LEASE_SWEEPER=True (Local Mode A config):
        - Both LeaseSweeper and QueueWorker are started.
        - Shutdown stops both cleanly.
        """
        app = FastAPI()
        mock_engine = MagicMock()
        mock_engine.dispose = AsyncMock()

        with patch.object(settings, "RUN_EMBEDDED_WORKER", True), \
             patch.object(settings, "RUN_LEASE_SWEEPER", True), \
             patch("src.main.check_database_health", new_callable=AsyncMock) as mock_db, \
             patch("src.main.engine", mock_engine):

            mock_db.return_value = True

            async with lifespan(app):
                assert hasattr(app.state, "sweeper")
                assert hasattr(app.state, "worker")

            assert app.state.sweeper._stop_event.is_set()
            assert app.state.worker._stop_event.is_set()

    @pytest.mark.asyncio
    async def test_sweeper_disabled_configuration(self):
        """
        When RUN_LEASE_SWEEPER=False and RUN_EMBEDDED_WORKER=False:
        - Neither background task is scheduled.
        """
        app = FastAPI()
        mock_engine = MagicMock()
        mock_engine.dispose = AsyncMock()

        with patch.object(settings, "RUN_EMBEDDED_WORKER", False), \
             patch.object(settings, "RUN_LEASE_SWEEPER", False), \
             patch("src.main.check_database_health", new_callable=AsyncMock) as mock_db, \
             patch("src.main.engine", mock_engine):

            mock_db.return_value = True

            async with lifespan(app):
                assert not hasattr(app.state, "sweeper")
                assert not hasattr(app.state, "worker")

    @pytest.mark.asyncio
    async def test_sweeper_reclaims_expired_leases_without_embedded_worker(self):
        """
        Verifies that LeaseSweeper directly reclaims stale leases when running standalone.
        """
        mock_repo = MagicMock()
        mock_repo.reclaim_expired_leases = AsyncMock(return_value=["job-stale-01", "job-stale-02"])

        sweeper = LeaseSweeper(
            repository=mock_repo,
            lease_timeout_seconds=60,
            sweep_interval_seconds=0.1,
            max_retries=3,
        )

        reclaimed = await sweeper.sweep_once()
        assert reclaimed == ["job-stale-01", "job-stale-02"]
        mock_repo.reclaim_expired_leases.assert_called_once_with(
            lease_timeout_seconds=60,
            max_retries=3,
            base_backoff_seconds=2.0,
            max_backoff_seconds=60.0,
        )
