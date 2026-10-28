"""
On-Premise GPU Worker CLI Daemon (Mode B)
Location: src/worker/daemon.py

Operational & Security Guarantees:
1. Outbound HTTPS polling only: Listens on zero inbound network ports.
2. Bearer token authentication: Uses WORKER_SHARED_SECRET; never logs or displays token values.
3. TLS verification enabled by default.
4. Graceful termination on SIGINT / SIGTERM / Ctrl+C.
5. Reusable across local Mock and real LocalDiffusersAdapter (NVIDIA RTX 2050 Mobile).
"""

import argparse
import asyncio
import os
import signal
import sys
from typing import Optional
from urllib.parse import urlparse
import uuid

from src.core.config import settings
from src.core.logging import logger, setup_logging
from src.domain.interfaces import AssetStorage, ImageProviderAdapter
from src.infrastructure.providers.registry import provider_registry
from src.infrastructure.storage import get_asset_storage
from src.services.hybrid_worker import HybridWorker


def parse_args(args: Optional[list] = None) -> argparse.Namespace:
    """Parses command-line arguments for the hybrid worker daemon."""
    parser = argparse.ArgumentParser(
        prog="python -m src.worker.daemon",
        description="Aarav Image Generation API — On-Premise GPU Worker Daemon (Mode B)",
    )
    parser.add_argument(
        "--control-plane-url",
        type=str,
        default=os.environ.get("CONTROL_PLANE_URL") or settings.CONTROL_PLANE_URL or "http://localhost:8000",
        help="Base URL of the control plane API (e.g. https://my-service.onrender.com)",
    )
    parser.add_argument(
        "--secret",
        type=str,
        default=None,
        help="[DEPRECATED for security] Worker shared secret. Supply via WORKER_SHARED_SECRET environment variable instead.",
    )
    parser.add_argument(
        "--worker-id",
        type=str,
        default=None,
        help="Unique worker identifier (defaults to auto-generated hex UUID)",
    )
    parser.add_argument(
        "--provider",
        type=str,
        default="local-gpu" if settings.ENVIRONMENT != "testing" else "mock",
        choices=["local-gpu", "mock"],
        help="Inference provider adapter to execute jobs with (default: local-gpu)",
    )
    parser.add_argument(
        "--storage",
        type=str,
        default=settings.STORAGE_ADAPTER,
        choices=["local_cas", "remote_vault"],
        help="Storage adapter to save generated PNG assets (default: local_cas)",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=settings.WORKER_POLL_INTERVAL_SECONDS,
        help="Seconds to wait between empty queue polling cycles (default: 0.5s)",
    )
    parser.add_argument(
        "--heartbeat-interval",
        type=float,
        default=30.0,
        help="Seconds between lease heartbeat renewal requests (default: 30.0s)",
    )
    parser.add_argument(
        "--request-timeout",
        type=float,
        default=30.0,
        help="HTTP request timeout in seconds (default: 30.0s)",
    )
    parser.add_argument(
        "--insecure",
        action="store_true",
        default=False,
        help="Disable TLS certificate verification (NOT recommended for production)",
    )
    parser.add_argument(
        "--run-once",
        action="store_true",
        default=False,
        help="Execute at most one polling cycle and exit (useful for test automation)",
    )
    parser.add_argument(
        "--check-config",
        action="store_true",
        default=False,
        help="Validate configuration and adapter availability without starting polling loop",
    )
    return parser.parse_args(args)


def validate_config(args: argparse.Namespace) -> tuple[str, str]:
    """
    Validates operational configuration parameters.
    Returns (validated_url, validated_secret) or raises ValueError.
    Guarantees secrets are never included in error messages.
    """
    url = args.control_plane_url.strip()
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError(
            f"Invalid --control-plane-url: '{url}'. Must be a valid absolute HTTP or HTTPS URL."
        )

    # Resolve secret: environment variable / settings is preferred.
    # If passed via command line, warn about security exposure in process tables and shell history.
    raw_secret = os.environ.get("WORKER_SHARED_SECRET")
    if not raw_secret and settings.WORKER_SHARED_SECRET:
        raw_secret = settings.WORKER_SHARED_SECRET.get_secret_value()

    if getattr(args, "secret", None):
        logger.warning(
            "SECURITY WARNING: Supplying the worker secret via command-line argument (--secret) is deprecated "
            "because arguments can be inspected in process tables and shell history. "
            "Supply the secret via the WORKER_SHARED_SECRET environment variable instead."
        )
        if not raw_secret:
            raw_secret = args.secret

    if not raw_secret or not raw_secret.strip():
        raise ValueError(
            "Worker shared secret is missing. Set the WORKER_SHARED_SECRET environment variable."
        )

    clean_secret = raw_secret.strip()
    if len(clean_secret) < 16:
        logger.warning("Worker shared secret length is under 16 characters. Recommend 32+ characters.")

    return url, clean_secret


def setup_adapters(provider_name: str, storage_name: str) -> tuple[ImageProviderAdapter, AssetStorage]:
    """Resolves and configures the inference provider and storage adapter."""
    provider = provider_registry.get_provider(provider_name)
    storage = get_asset_storage(storage_name)
    return provider, storage


async def run_worker(args: argparse.Namespace) -> int:
    """Initializes and runs the HybridWorker loop with graceful shutdown."""
    setup_logging(level="DEBUG" if settings.DEBUG else "INFO")

    try:
        control_plane_url, worker_secret = validate_config(args)
    except ValueError as val_err:
        logger.error(f"Configuration error: {val_err}")
        return 1

    try:
        provider, storage = setup_adapters(args.provider, args.storage)
    except Exception as exc:
        logger.error(f"Failed to initialize adapters: {exc}")
        return 1

    worker_id = args.worker_id or f"hybrid-gpu-{uuid.uuid4().hex[:8]}"

    logger.info("=================================================================")
    logger.info("Aarav Inference-1 — On-Premise Hybrid GPU Worker Daemon")
    logger.info(f"Worker ID:         {worker_id}")
    logger.info(f"Target URL:        {control_plane_url}")
    logger.info(f"Provider:          {provider.name}")
    logger.info(f"Storage Adapter:   {storage.name}")
    logger.info(f"Poll Interval:     {args.poll_interval}s")
    logger.info(f"Heartbeat:         {args.heartbeat_interval}s")
    logger.info(f"TLS Verification:  {'Disabled' if args.insecure else 'Enabled'}")
    logger.info("=================================================================")

    if args.check_config:
        logger.info("Configuration check passed successfully.")
        return 0

    worker = HybridWorker(
        base_url=control_plane_url,
        worker_shared_secret=worker_secret,
        provider=provider,
        storage=storage,
        worker_id=worker_id,
        poll_interval_seconds=args.poll_interval,
        heartbeat_interval_seconds=args.heartbeat_interval,
        request_timeout_seconds=args.request_timeout,
        verify_ssl=not args.insecure,
    )

    worker_task: Optional[asyncio.Task] = asyncio.current_task()
    shutdown_count = 0

    # Graceful shutdown handler supporting repeated signals
    def handle_shutdown_signal(*sig_args):
        nonlocal shutdown_count
        shutdown_count += 1
        sig = sig_args[0] if sig_args else signal.SIGTERM
        try:
            sig_name = signal.Signals(sig).name
        except Exception:
            sig_name = str(sig)

        if shutdown_count == 1:
            logger.info(f"Shutdown signal received ({sig_name}). Stopping worker daemon gracefully...")
            worker.stop()
        else:
            logger.warning(
                f"Repeated shutdown signal received ({sig_name}, count={shutdown_count}). Forcing immediate exit..."
            )
            worker.stop()
            if worker_task and not worker_task.done():
                worker_task.cancel()

    # Attach signal handlers where available
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, handle_shutdown_signal)
        except (ValueError, AttributeError):
            pass

    if args.run_once:
        logger.info("Executing single poll cycle (--run-once)...")
        import httpx
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(args.request_timeout, connect=min(10.0, args.request_timeout)),
            verify=not args.insecure,
        ) as client:
            had_job = await worker.poll_and_execute_once(client)
            logger.info(f"Run-once completed (job processed: {had_job}).")
            return 0

    try:
        await worker.run()
    except asyncio.CancelledError:
        logger.info("Worker task cancelled.")
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received.")
        worker.stop()
    except Exception as e:
        logger.error(f"Worker runtime error: {e}", exc_info=True)
        return 1

    return 0


def main(args: Optional[list] = None) -> None:
    """CLI synchronous entry point."""
    parsed = parse_args(args)
    try:
        exit_code = asyncio.run(run_worker(parsed))
    except KeyboardInterrupt:
        logger.info("Worker interrupted by user.")
        exit_code = 0
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
