"""
Unit Tests — On-Premise GPU Worker CLI Daemon
Location: tests/unit/test_worker_cli.py
"""

import argparse
import os
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.worker.daemon import parse_args, setup_adapters, validate_config, run_worker


class TestWorkerCLI:
    """Tests for the hybrid GPU worker CLI daemon."""

    def test_parse_args_defaults(self):
        """Verifies default CLI arguments."""
        args = parse_args([])
        assert args.control_plane_url is not None
        assert args.poll_interval >= 0.1
        assert args.heartbeat_interval >= 5.0
        assert args.insecure is False
        assert args.run_once is False
        assert args.check_config is False

    def test_validate_config_via_env_var(self):
        """Validates configuration when secret is supplied via environment variable (preferred)."""
        args = argparse.Namespace(
            control_plane_url="https://render-test.onrender.com",
            secret=None,
        )
        with patch.dict(os.environ, {"WORKER_SHARED_SECRET": "env_secret_1234567890abcdef"}):
            url, secret = validate_config(args)
            assert url == "https://render-test.onrender.com"
            assert secret == "env_secret_1234567890abcdef"

    def test_validate_config_cli_secret_triggers_warning(self):
        """Supplying --secret triggers a deprecation security warning."""
        args = argparse.Namespace(
            control_plane_url="https://render-test.onrender.com",
            secret="cli_secret_1234567890abcdef",
        )
        with patch.dict(os.environ, {}, clear=True), \
             patch("src.worker.daemon.settings.WORKER_SHARED_SECRET", None), \
             patch("src.worker.daemon.logger.warning") as mock_warn:

            url, secret = validate_config(args)
            assert url == "https://render-test.onrender.com"
            assert secret == "cli_secret_1234567890abcdef"
            # Verify security warning was logged
            warning_messages = [call.args[0] for call in mock_warn.call_args_list]
            assert any("SECURITY WARNING" in msg for msg in warning_messages)

    def test_validate_config_invalid_url_never_discloses_secret(self):
        """Rejects non-HTTP(S) or invalid URLs without disclosing secret in exception."""
        sensitive_token = "super_confidential_token_xyz987"
        args = argparse.Namespace(
            control_plane_url="ftp://invalid-url.com",
            secret=sensitive_token,
        )
        with pytest.raises(ValueError) as exc_info:
            validate_config(args)
        assert sensitive_token not in str(exc_info.value)
        assert "Invalid --control-plane-url" in str(exc_info.value)

    def test_validate_config_missing_secret(self):
        """Rejects invocation when no secret is provided via environment or CLI."""
        args = argparse.Namespace(
            control_plane_url="https://valid-url.com",
            secret=None,
        )
        with patch.dict(os.environ, {}, clear=True), \
             patch("src.worker.daemon.settings.WORKER_SHARED_SECRET", None):
            with pytest.raises(ValueError, match="Worker shared secret is missing"):
                validate_config(args)

    def test_setup_adapters_mock(self):
        """Resolves mock provider and local CAS storage."""
        provider, storage = setup_adapters("mock", "local_cas")
        assert provider.name == "mock"
        assert storage.name == "local_cas"

    def test_setup_adapters_remote_storage(self):
        """Resolves mock provider and remote private storage."""
        provider, storage = setup_adapters("mock", "remote_vault")
        assert provider.name == "mock"
        assert "remote" in storage.name.lower()

    @pytest.mark.asyncio
    async def test_run_worker_check_config(self):
        """Verifies --check-config validates and exits with code 0 without running polling loop."""
        args = argparse.Namespace(
            control_plane_url="https://api.example.com",
            secret=None,
            worker_id="test-worker-01",
            provider="mock",
            storage="local_cas",
            poll_interval=1.0,
            heartbeat_interval=30.0,
            request_timeout=10.0,
            insecure=False,
            run_once=False,
            check_config=True,
        )
        with patch.dict(os.environ, {"WORKER_SHARED_SECRET": "a_valid_test_secret_32_characters_long"}):
            exit_code = await run_worker(args)
            assert exit_code == 0

    @pytest.mark.asyncio
    async def test_run_worker_run_once(self):
        """Verifies --run-once performs a single poll attempt and exits cleanly."""
        args = argparse.Namespace(
            control_plane_url="https://api.example.com",
            secret=None,
            worker_id="test-worker-01",
            provider="mock",
            storage="local_cas",
            poll_interval=1.0,
            heartbeat_interval=30.0,
            request_timeout=10.0,
            insecure=False,
            run_once=True,
            check_config=False,
        )
        with patch.dict(os.environ, {"WORKER_SHARED_SECRET": "a_valid_test_secret_32_characters_long"}), \
             patch("src.worker.daemon.HybridWorker.poll_and_execute_once", new_callable=AsyncMock) as mock_poll:
            mock_poll.return_value = False
            exit_code = await run_worker(args)
            assert exit_code == 0
            mock_poll.assert_called_once()
