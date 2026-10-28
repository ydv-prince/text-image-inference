"""
Unit Tests — Hybrid Worker Lifecycle, Shutdown, and Lease Recovery Hardening
Location: tests/unit/test_hybrid_worker_lifecycle.py

Validates:
1. Deterministic lifecycle transitions and initial states.
2. Immediate exit on idle shutdown.
3. Pre-poll shutdown skips leasing without network requests.
4. Lease race during shutdown: lease is immediately relinquished via /fail with WORKER_SHUTDOWN.
5. In-flight job graceful drain: completes active job, heartbeats maintained, reports /complete.
6. In-flight job cancellation: catches CancelledError, reports /fail with WORKER_CANCELLED, re-raises CancelledError.
7. Heartbeat 409 Conflict (lease lost): terminates heartbeat loop and marks lease_lost = True.
8. Revoked lease guard: worker aborts /complete report if lease was revoked during processing.
9. Cleanup robustness: network failure during /fail never suppresses original exception or cancellation.
10. Daemon signal handling: 1st signal triggers graceful stop; 2nd signal forces task cancellation.
11. Worker restart: cleanly resets running state and events for subsequent execution.
"""

import asyncio
import signal
from unittest.mock import AsyncMock, MagicMock, patch
import httpx
import pytest

from src.domain.interfaces import ProviderResponse, StoredAsset
from src.core.logging import correlation_id_ctx
from src.services.hybrid_worker import HybridWorker, classify_error
from src.worker.daemon import run_worker


@pytest.fixture
def mock_provider():
    provider = MagicMock()
    provider.name = "mock"
    provider.generate_image = AsyncMock(
        return_value=ProviderResponse(
            image_bytes=b"fake_png_data",
            mime_type="image/png",
            width=512,
            height=512,
            duration_ms=42,
            provider_name="mock",
            model_id="mock-v1",
            peak_vram_mb=128,
            is_synthetic=True,
        )
    )
    return provider


@pytest.fixture
def mock_storage():
    storage = MagicMock()
    storage.name = "local_cas"
    storage.store_image = AsyncMock(
        return_value=StoredAsset(
            asset_id="asset-12345678",
            file_hash="a" * 64,
            storage_path="cas/aa/aaaa.png",
            file_size_bytes=1024,
            mime_type="image/png",
            width=512,
            height=512,
            is_synthetic=True,
        )
    )
    return storage


@pytest.fixture
def worker(mock_provider, mock_storage):
    return HybridWorker(
        base_url="https://api.test-control-plane.com",
        worker_shared_secret="test_secret_32_characters_minimum__",
        provider=mock_provider,
        storage=mock_storage,
        worker_id="test-worker-alpha",
        poll_interval_seconds=0.05,
        heartbeat_interval_seconds=0.05,
        request_timeout_seconds=5.0,
    )


class TestHybridWorkerLifecycle:
    """Deterministic lifecycle and shutdown test suite."""

    def test_worker_initialization(self, worker):
        """Verifies clean initial states and properties."""
        assert worker.is_running is False
        assert worker.is_stopping is False
        assert worker.current_job_id is None
        assert worker.lease_lost is False
        assert worker.worker_id == "test-worker-alpha"

    @pytest.mark.asyncio
    async def test_poll_empty_queue_returns_false(self, worker):
        """HTTP 204 indicates queue is empty; returns False without error."""
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.post.return_value = httpx.Response(status_code=204)

        result = await worker.poll_and_execute_once(mock_client)
        assert result is False
        assert worker.current_job_id is None
        mock_client.post.assert_called_once()
        assert "/jobs/lease" in mock_client.post.call_args[0][0]

    @pytest.mark.asyncio
    async def test_poll_server_error_returns_false(self, worker):
        """HTTP 500 response from control plane is logged and returns False."""
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.post.return_value = httpx.Response(status_code=500, text="Internal Server Error")

        result = await worker.poll_and_execute_once(mock_client)
        assert result is False
        assert worker.current_job_id is None

    @pytest.mark.asyncio
    async def test_poll_network_exception_returns_false(self, worker):
        """Network exception contacting control plane returns False cleanly."""
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.post.side_effect = httpx.ConnectError("Connection refused")

        result = await worker.poll_and_execute_once(mock_client)
        assert result is False
        assert worker.current_job_id is None

    @pytest.mark.asyncio
    async def test_poll_skipped_when_already_stopping(self, worker):
        """Invariant: Stop claiming new jobs once shutdown has been requested."""
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        worker.stop()
        assert worker.is_stopping is True

        result = await worker.poll_and_execute_once(mock_client)
        assert result is False
        mock_client.post.assert_not_called()

    @pytest.mark.asyncio
    async def test_lease_acquired_during_shutdown_race_relinquishes_lease(self, worker):
        """
        Invariant: A lease acquired during a shutdown race is not silently abandoned.
        Worker stops while lease request is in flight -> server returns 200 -> worker immediately
        relinquishes the lease via POST /fail with WORKER_SHUTDOWN and returns False.
        """
        mock_client = AsyncMock(spec=httpx.AsyncClient)

        lease_resp = httpx.Response(
            status_code=200,
            json={
                "job_id": "job-race-999",
                "prompt": "sunset over mountains",
                "width": 512,
                "height": 512,
                "steps": 4,
                "seed": 42,
            },
        )
        fail_resp = httpx.Response(status_code=200, json={"status": "retrying", "job_id": "job-race-999"})

        # Simulate stop() being called while lease request is in-flight
        async def mock_post(url, **kwargs):
            if "/jobs/lease" in url:
                worker.stop()  # Trigger shutdown signal during request!
                return lease_resp
            if "/jobs/job-race-999/fail" in url:
                return fail_resp
            return httpx.Response(status_code=404)

        mock_client.post.side_effect = mock_post

        result = await worker.poll_and_execute_once(mock_client)

        assert result is False
        assert worker.current_job_id is None
        worker.provider.generate_image.assert_not_called()
        worker.storage.store_image.assert_not_called()

        # Verify POST /fail was called with WORKER_SHUTDOWN
        call_urls = [call[0][0] for call in mock_client.post.call_args_list]
        assert any("/jobs/job-race-999/fail" in url for url in call_urls)
        fail_call = next(call for call in mock_client.post.call_args_list if "/jobs/job-race-999/fail" in call[0][0])
        assert fail_call[1]["json"]["error_code"] == "WORKER_SHUTDOWN"
        assert fail_call[1]["json"]["worker_id"] == "test-worker-alpha"
        assert fail_call[1]["json"]["retry_delay_seconds"] == 1

    @pytest.mark.asyncio
    async def test_successful_job_execution_lifecycle(self, worker):
        """Happy path: lease -> inference -> storage -> complete."""
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        lease_resp = httpx.Response(
            status_code=200,
            json={
                "job_id": "job-success-100",
                "prompt": "emerald lake in forest",
                "negative_prompt": "blurry",
                "width": 512,
                "height": 512,
                "steps": 20,
                "seed": 12345,
            },
        )
        complete_resp = httpx.Response(
            status_code=200,
            json={"status": "completed", "job_id": "job-success-100", "asset_id": "asset-12345678"},
        )

        async def mock_post(url, **kwargs):
            if "/jobs/lease" in url:
                return lease_resp
            if "/jobs/job-success-100/complete" in url:
                return complete_resp
            if "/jobs/job-success-100/heartbeat" in url:
                return httpx.Response(status_code=200, json={"status": "renewed"})
            return httpx.Response(status_code=404)

        mock_client.post.side_effect = mock_post

        result = await worker.poll_and_execute_once(mock_client)

        assert result is True
        assert worker.current_job_id is None
        worker.provider.generate_image.assert_called_once()
        worker.storage.store_image.assert_called_once()

        call_urls = [call[0][0] for call in mock_client.post.call_args_list]
        assert any("/jobs/job-success-100/complete" in url for url in call_urls)
        complete_call = next(call for call in mock_client.post.call_args_list if "/jobs/job-success-100/complete" in call[0][0])
        assert complete_call[1]["json"]["file_hash"] == "a" * 64
        assert complete_call[1]["json"]["worker_id"] == "test-worker-alpha"

    @pytest.mark.asyncio
    async def test_graceful_shutdown_completes_in_flight_job(self, worker):
        """
        Graceful Drain: Shutdown requested while job is actively running inference.
        In-flight job runs to completion and completes via /complete, then worker stops.
        """
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        lease_resp = httpx.Response(
            status_code=200,
            json={
                "job_id": "job-drain-200",
                "prompt": "nebula galaxy stars",
                "width": 512,
                "height": 512,
                "steps": 10,
                "seed": 777,
            },
        )
        complete_resp = httpx.Response(
            status_code=200,
            json={"status": "completed", "job_id": "job-drain-200", "asset_id": "asset-12345678"},
        )

        async def mock_post(url, **kwargs):
            if "/jobs/lease" in url:
                return lease_resp
            if "/jobs/job-drain-200/complete" in url:
                return complete_resp
            return httpx.Response(status_code=404)

        mock_client.post.side_effect = mock_post

        # Simulate shutdown signal arriving during inference
        async def slow_inference(req):
            worker.stop()  # Operator requests graceful stop during inference
            assert worker.is_stopping is True
            return ProviderResponse(
                image_bytes=b"rendered_image",
                mime_type="image/png",
                width=512,
                height=512,
                duration_ms=50,
                provider_name="mock",
                model_id="mock-v1",
                is_synthetic=True,
            )

        worker.provider.generate_image.side_effect = slow_inference

        result = await worker.poll_and_execute_once(mock_client)

        assert result is True
        assert worker.is_stopping is True
        assert worker.current_job_id is None
        worker.storage.store_image.assert_called_once()

        # Job must be reported as completed
        call_urls = [call[0][0] for call in mock_client.post.call_args_list]
        assert any("/jobs/job-drain-200/complete" in url for url in call_urls)

    @pytest.mark.asyncio
    async def test_inference_failure_reports_fail(self, worker):
        """Inference failure triggers POST /fail and returns True (cycle completed)."""
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        lease_resp = httpx.Response(
            status_code=200,
            json={
                "job_id": "job-fail-300",
                "prompt": "corrupt model run",
                "width": 512,
                "height": 512,
                "steps": 10,
                "seed": 888,
            },
        )
        fail_resp = httpx.Response(status_code=200, json={"status": "retrying", "job_id": "job-fail-300"})

        async def mock_post(url, **kwargs):
            if "/jobs/lease" in url:
                return lease_resp
            if "/jobs/job-fail-300/fail" in url:
                return fail_resp
            return httpx.Response(status_code=404)

        mock_client.post.side_effect = mock_post
        worker.provider.generate_image.side_effect = RuntimeError("CUDA out of memory")

        result = await worker.poll_and_execute_once(mock_client)

        assert result is True
        assert worker.current_job_id is None
        call_urls = [call[0][0] for call in mock_client.post.call_args_list]
        assert any("/jobs/job-fail-300/fail" in url for url in call_urls)
        fail_call = next(call for call in mock_client.post.call_args_list if "/jobs/job-fail-300/fail" in call[0][0])
        assert fail_call[1]["json"]["error_code"] == "GPU_OUT_OF_MEMORY"
        assert "CUDA out of memory" in fail_call[1]["json"]["error_message"]

    @pytest.mark.asyncio
    async def test_cancellation_during_inference_relinquishes_lease_and_raises(self, worker):
        """
        Invariant: Cancellation must not swallow CancelledError, and must not silently abandon lease.
        Worker reports /fail with WORKER_CANCELLED and re-raises CancelledError.
        """
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        lease_resp = httpx.Response(
            status_code=200,
            json={
                "job_id": "job-cancel-400",
                "prompt": "cancelled generation",
                "width": 512,
                "height": 512,
                "steps": 10,
                "seed": 999,
            },
        )
        fail_resp = httpx.Response(status_code=200, json={"status": "retrying", "job_id": "job-cancel-400"})

        async def mock_post(url, **kwargs):
            if "/jobs/lease" in url:
                return lease_resp
            if "/jobs/job-cancel-400/fail" in url:
                return fail_resp
            return httpx.Response(status_code=404)

        mock_client.post.side_effect = mock_post
        worker.provider.generate_image.side_effect = asyncio.CancelledError()

        with pytest.raises(asyncio.CancelledError):
            await worker.poll_and_execute_once(mock_client)

        assert worker.current_job_id is None
        call_urls = [call[0][0] for call in mock_client.post.call_args_list]
        assert any("/jobs/job-cancel-400/fail" in url for url in call_urls)
        fail_call = next(call for call in mock_client.post.call_args_list if "/jobs/job-cancel-400/fail" in call[0][0])
        assert fail_call[1]["json"]["error_code"] == "WORKER_CANCELLED"
        assert fail_call[1]["json"]["retry_delay_seconds"] == 1

    @pytest.mark.asyncio
    async def test_heartbeat_409_marks_lease_lost_and_stops_loop(self, worker):
        """Heartbeat receiving 409 Conflict marks lease_lost = True and breaks heartbeat loop."""
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.post.return_value = httpx.Response(
            status_code=409,
            text="Worker does not own lease. Reassigned to another worker.",
        )

        stop_event = asyncio.Event()
        worker.heartbeat_interval = 0.01

        # Run heartbeat loop for a few milliseconds
        hb_task = asyncio.create_task(worker._heartbeat_loop(mock_client, "job-lost-500", stop_event))
        await asyncio.sleep(0.05)

        assert worker.lease_lost is True
        assert hb_task.done()  # Loop broke on 409
        stop_event.set()

    @pytest.mark.asyncio
    async def test_lease_lost_aborts_completion_report(self, worker):
        """
        Invariant: Do not release a lease owned by another worker or mark incomplete job completed.
        If lease was lost (e.g. swept during long stall), worker aborts /complete report.
        """
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        lease_resp = httpx.Response(
            status_code=200,
            json={
                "job_id": "job-lost-600",
                "prompt": "stalled generation",
                "width": 512,
                "height": 512,
                "steps": 10,
                "seed": 111,
            },
        )

        async def mock_post(url, **kwargs):
            if "/jobs/lease" in url:
                return lease_resp
            return httpx.Response(status_code=404)

        mock_client.post.side_effect = mock_post

        # Simulate heartbeat detecting lease revocation during inference
        async def inference_with_revocation(req):
            worker._lease_lost = True  # Heartbeat detected 409
            return ProviderResponse(
                image_bytes=b"finished_too_late",
                mime_type="image/png",
                width=512,
                height=512,
                duration_ms=5000,
                provider_name="mock",
                model_id="mock-v1",
                is_synthetic=True,
            )

        worker.provider.generate_image.side_effect = inference_with_revocation

        result = await worker.poll_and_execute_once(mock_client)

        assert result is False
        assert worker.current_job_id is None
        call_urls = [call[0][0] for call in mock_client.post.call_args_list]
        # Must NOT call /complete because lease was lost
        assert not any("/jobs/job-lost-600/complete" in url for url in call_urls)

    @pytest.mark.asyncio
    async def test_completion_409_marks_lease_lost(self, worker):
        """If POST /complete returns 409 Conflict, worker marks lease_lost and returns False."""
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        lease_resp = httpx.Response(
            status_code=200,
            json={
                "job_id": "job-complete-409",
                "prompt": "test prompt",
                "width": 512,
                "height": 512,
                "steps": 5,
                "seed": 222,
            },
        )
        complete_409_resp = httpx.Response(status_code=409, text="Lease expired prior to completion")

        async def mock_post(url, **kwargs):
            if "/jobs/lease" in url:
                return lease_resp
            if "/jobs/job-complete-409/complete" in url:
                return complete_409_resp
            return httpx.Response(status_code=404)

        mock_client.post.side_effect = mock_post

        result = await worker.poll_and_execute_once(mock_client)

        assert result is False
        assert worker.lease_lost is True
        assert worker.current_job_id is None

    @pytest.mark.asyncio
    async def test_cleanup_failure_does_not_mask_original_exception(self, worker):
        """
        Invariant: Cleanup failures do not hide the original job failure or cancellation.
        When /fail throws a network error during error handling, the original failure is preserved.
        """
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        lease_resp = httpx.Response(
            status_code=200,
            json={
                "job_id": "job-cleanup-fail-700",
                "prompt": "test prompt",
                "width": 512,
                "height": 512,
                "steps": 5,
                "seed": 333,
            },
        )

        async def mock_post(url, **kwargs):
            if "/jobs/lease" in url:
                return lease_resp
            if "/jobs/job-cleanup-fail-700/fail" in url:
                raise httpx.ConnectTimeout("Control plane unreachable during fail report")
            return httpx.Response(status_code=404)

        mock_client.post.side_effect = mock_post
        worker.provider.generate_image.side_effect = ValueError("Primary inference error")

        # Must not raise the ConnectTimeout; must handle cleanly
        result = await worker.poll_and_execute_once(mock_client)
        assert result is True
        assert worker.current_job_id is None

    @pytest.mark.asyncio
    async def test_worker_run_idle_shutdown(self, worker):
        """Persistent loop exits promptly when stopped while idle."""
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.post.return_value = httpx.Response(status_code=204)
        worker._external_client = mock_client

        async def stop_after_delay():
            await asyncio.sleep(0.02)
            worker.stop()

        asyncio.create_task(stop_after_delay())
        await worker.run()

        assert worker.is_running is False
        assert worker.is_stopping is True

    @pytest.mark.asyncio
    async def test_worker_restart_lifecycle(self, worker):
        """Worker can be stopped and restarted cleanly for subsequent execution."""
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.post.return_value = httpx.Response(status_code=204)
        worker._external_client = mock_client

        worker.stop()
        assert worker.is_stopping is True

        # Second run clears stop event
        async def stop_second_run():
            await asyncio.sleep(0.02)
            worker.stop()

        asyncio.create_task(stop_second_run())
        await worker.run()

        assert worker.is_stopping is True
        assert worker.current_job_id is None

    def test_classify_error_mapping(self):
        """Verifies actionable error code classification for operational observability."""
        assert classify_error(asyncio.CancelledError()) == "WORKER_CANCELLED"
        assert classify_error(MemoryError("out of memory")) == "GPU_OUT_OF_MEMORY"
        assert classify_error(RuntimeError("CUDA out of memory error")) == "GPU_OUT_OF_MEMORY"
        assert classify_error(TimeoutError("operation timeout")) == "INFERENCE_TIMEOUT"
        assert classify_error(asyncio.TimeoutError()) == "INFERENCE_TIMEOUT"
        assert classify_error(OSError("CAS storage upload failed")) == "STORAGE_ERROR"
        assert classify_error(ValueError("Prompt too long")) == "INVALID_PAYLOAD"
        assert classify_error(ArithmeticError("arithmetic error")) == "ArithmeticError"

        # Cap length at 64 characters
        class VeryLongCustomExceptionNameThatExceedsTheSixtyFourCharacterLimitAllowedByPydantic(Exception):
            pass

        long_err = VeryLongCustomExceptionNameThatExceedsTheSixtyFourCharacterLimitAllowedByPydantic("err")
        assert len(classify_error(long_err)) <= 64

    @pytest.mark.asyncio
    async def test_worker_metrics_tracking(self, worker):
        """Verifies worker metrics and runtime telemetry."""
        assert worker.metrics["jobs_completed"] == 0
        assert worker.metrics["jobs_failed"] == 0
        assert worker.metrics["heartbeats_sent"] == 0

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        lease_resp = httpx.Response(
            status_code=200,
            json={
                "job_id": "job-metrics-1",
                "prompt": "metrics test",
                "width": 512,
                "height": 512,
                "steps": 4,
                "seed": 1,
            },
        )
        complete_resp = httpx.Response(
            status_code=200,
            json={"status": "completed", "job_id": "job-metrics-1", "asset_id": "a-1"},
        )

        async def mock_post(url, **kwargs):
            if "/jobs/lease" in url:
                return lease_resp
            if "/complete" in url:
                return complete_resp
            return httpx.Response(status_code=404)

        mock_client.post.side_effect = mock_post

        await worker.poll_and_execute_once(mock_client)
        assert worker.metrics["jobs_completed"] == 1
        assert worker.metrics["jobs_failed"] == 0

    @pytest.mark.asyncio
    async def test_correlation_id_set_during_job_processing(self, worker):
        """Verifies correlation_id is set to job_id during execution and restored afterward."""
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        lease_resp = httpx.Response(
            status_code=200,
            json={
                "job_id": "job-corr-id-xyz",
                "prompt": "correlation test",
                "width": 512,
                "height": 512,
                "steps": 4,
                "seed": 2,
            },
        )
        complete_resp = httpx.Response(
            status_code=200,
            json={"status": "completed", "job_id": "job-corr-id-xyz", "asset_id": "a-2"},
        )

        captured_correlation_id = None

        async def generate_with_cid_check(req):
            nonlocal captured_correlation_id
            captured_correlation_id = correlation_id_ctx.get()
            return ProviderResponse(
                image_bytes=b"rendered",
                mime_type="image/png",
                width=512,
                height=512,
                duration_ms=10,
                provider_name="mock",
                model_id="mock-v1",
                is_synthetic=True,
            )

        worker.provider.generate_image.side_effect = generate_with_cid_check

        async def mock_post(url, **kwargs):
            if "/jobs/lease" in url:
                return lease_resp
            if "/complete" in url:
                return complete_resp
            return httpx.Response(status_code=404)

        mock_client.post.side_effect = mock_post

        prior_cid = correlation_id_ctx.get()
        await worker.poll_and_execute_once(mock_client)

        assert captured_correlation_id == "job-corr-id-xyz"
        assert correlation_id_ctx.get() == prior_cid

    @pytest.mark.asyncio
    async def test_cancellation_bounded_timeout_when_server_stalls(self, worker):
        """Verifies that cancellation cleanup times out gracefully if /fail hangs, re-raising CancelledError."""
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        worker.request_timeout = 0.05  # fast timeout

        lease_resp = httpx.Response(
            status_code=200,
            json={
                "job_id": "job-cancel-stall",
                "prompt": "stall test",
                "width": 512,
                "height": 512,
                "steps": 4,
                "seed": 3,
            },
        )

        async def mock_post(url, **kwargs):
            if "/jobs/lease" in url:
                return lease_resp
            if "/fail" in url:
                # Hang indefinitely on /fail
                await asyncio.sleep(10.0)
                return httpx.Response(status_code=200)
            return httpx.Response(status_code=404)

        mock_client.post.side_effect = mock_post
        worker.provider.generate_image.side_effect = asyncio.CancelledError()

        with pytest.raises(asyncio.CancelledError):
            await worker.poll_and_execute_once(mock_client)

        assert worker.metrics["jobs_failed"] == 1
        assert worker.current_job_id is None


class TestDaemonShutdownSignals:
    """Tests for daemon signal handling and repeated signals."""

    @pytest.mark.asyncio
    async def test_daemon_repeated_signal_forces_task_cancellation(self):
        """
        Verifies:
        - 1st signal calls worker.stop() (graceful stop).
        - 2nd signal cancels the worker task immediately.
        """
        import argparse
        import os

        args = argparse.Namespace(
            control_plane_url="https://api.example.com",
            secret=None,
            worker_id="test-daemon-worker",
            provider="mock",
            storage="local_cas",
            poll_interval=1.0,
            heartbeat_interval=30.0,
            request_timeout=10.0,
            insecure=False,
            run_once=False,
            check_config=False,
        )

        signal_handlers = {}

        def mock_signal(sig, handler):
            signal_handlers[sig] = handler

        with patch.dict(os.environ, {"WORKER_SHARED_SECRET": "a_valid_test_secret_32_characters_long"}), \
             patch("src.worker.daemon.signal.signal", side_effect=mock_signal), \
             patch("src.worker.daemon.HybridWorker.run", new_callable=AsyncMock) as mock_run:

            # Make run() block until cancelled or stopped
            async def run_block():
                try:
                    await asyncio.sleep(100.0)
                except asyncio.CancelledError:
                    raise

            mock_run.side_effect = run_block

            # Run worker in background task
            worker_task = asyncio.create_task(run_worker(args))
            await asyncio.sleep(0.02)

            handler = signal_handlers.get(signal.SIGINT)
            assert handler is not None

            # First signal: graceful stop
            handler(signal.SIGINT, None)
            assert not worker_task.done()

            # Second signal: forced cancellation
            handler(signal.SIGINT, None)
            await asyncio.sleep(0.02)

            assert worker_task.done()
            exit_code = await worker_task
            assert exit_code == 0
