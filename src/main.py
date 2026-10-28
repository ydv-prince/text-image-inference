"""
FastAPI Application Entrypoint & ASGI Lifecycle
Location: src/main.py
"""

import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from src.api.middleware.correlation import CorrelationIdMiddleware
from src.api.middleware.metrics import MetricsMiddleware
from src.api.router import api_router
from src.core.config import settings
from src.core.errors import AaravException
from src.core.logging import get_correlation_id, logger, setup_logging
from src.infrastructure.database import async_session_factory, check_database_health, engine
from src.infrastructure.repositories import get_job_repository
from src.infrastructure.storage import get_asset_storage
from src.services.lease_sweeper import LeaseSweeper
from src.services.queue_worker import QueueWorker


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application startup and graceful shutdown lifecycle."""
    # 1. Startup phase
    setup_logging(level="DEBUG" if settings.DEBUG else "INFO")
    logger.info(f"Starting {settings.APP_NAME} v{settings.APP_VERSION} ({settings.ENVIRONMENT} mode)")
    logger.info(f"Database dialect: {'SQLite' if settings.is_sqlite else 'PostgreSQL'}")

    db_ready = await check_database_health()
    if db_ready:
        logger.info("Database connectivity verified.")
    else:
        logger.warning("Database connectivity check failed on startup.")

    worker_task: Optional[asyncio.Task] = None
    sweeper_task: Optional[asyncio.Task] = None
    outbox_task: Optional[asyncio.Task] = None

    repo = get_job_repository(async_session_factory)

    # 1. Lease Sweeper: Runs on both local monolith and hosted control plane
    if settings.RUN_LEASE_SWEEPER:
        sweeper = LeaseSweeper(repository=repo)
        app.state.sweeper = sweeper
        sweeper_task = asyncio.create_task(sweeper.run(), name="lease_sweeper")
        logger.info("LeaseSweeper background task started.")

    # 2. Embedded Worker: Runs ONLY in local Mode A when RUN_EMBEDDED_WORKER=true
    if settings.RUN_EMBEDDED_WORKER:
        storage = get_asset_storage()
        worker = QueueWorker(repository=repo, storage=storage, worker_id="embedded-worker")
        app.state.worker = worker
        worker_task = asyncio.create_task(worker.run(), name="queue_worker")
        logger.info("Embedded QueueWorker background task started.")

    # 3. Deletion Outbox Worker: Background cleanup daemon
    if getattr(settings, "RUN_DELETION_OUTBOX_WORKER", False):
        from src.services.deletion_outbox_service import DeletionOutboxWorker
        outbox_worker = DeletionOutboxWorker(
            session_factory=async_session_factory,
            worker_id=f"outbox-worker-{settings.ENVIRONMENT}",
            interval_seconds=settings.DELETION_OUTBOX_INTERVAL_SECONDS,
            batch_limit=settings.DELETION_OUTBOX_BATCH_SIZE,
        )
        app.state.outbox_worker = outbox_worker
        outbox_task = asyncio.create_task(outbox_worker.run(), name="deletion_outbox_worker")
        logger.info("DeletionOutboxWorker background task started.")

    yield

    # 2. Shutdown phase
    logger.info("Initiating graceful shutdown...")
    if worker_task or sweeper_task or outbox_task:
        logger.info("Stopping background tasks...")
        if hasattr(app.state, "worker") and app.state.worker:
            app.state.worker.stop()
        if hasattr(app.state, "sweeper") and app.state.sweeper:
            app.state.sweeper.stop()
        if hasattr(app.state, "outbox_worker") and app.state.outbox_worker:
            app.state.outbox_worker.stop()
        if worker_task:
            worker_task.cancel()
        if sweeper_task:
            sweeper_task.cancel()
        if outbox_task:
            outbox_task.cancel()
        tasks_to_stop = [t for t in (worker_task, sweeper_task, outbox_task) if t is not None]
        await asyncio.gather(*tasks_to_stop, return_exceptions=True)
        logger.info("Background tasks stopped.")

    await engine.dispose()
    logger.info("Database connection pools closed.")


def create_app() -> FastAPI:
    """FastAPI application factory."""
    app = FastAPI(
        title=settings.APP_NAME,
        version=settings.APP_VERSION,
        lifespan=lifespan,
        docs_url="/docs" if settings.ENVIRONMENT != "production" else None,
        redoc_url="/redoc" if settings.ENVIRONMENT != "production" else None,
    )

    # 1. Register Middlewares
    app.add_middleware(CorrelationIdMiddleware)
    app.add_middleware(MetricsMiddleware)

    # 2. Register Global RFC 7807 Error Handlers
    @app.exception_handler(AaravException)
    async def handle_aarav_exception(request: Request, exc: AaravException) -> JSONResponse:
        correlation_id = get_correlation_id()
        logger.warning(
            f"Handled application exception: [{exc.code}] {exc.message}",
            extra={"extra": {"error_code": exc.code, "status_code": exc.status_code}},
        )
        return JSONResponse(
            status_code=exc.status_code,
            headers=getattr(exc, "headers", None),
            content={
                "type": f"https://api.aarav-inference.internal/errors/{exc.code.lower()}",
                "title": exc.message,
                "status": exc.status_code,
                "detail": exc.detail,
                "instance": str(request.url.path),
                "code": exc.code,
                "correlation_id": correlation_id,
            },
        )

    @app.exception_handler(RequestValidationError)
    async def handle_validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        correlation_id = get_correlation_id()
        errors = exc.errors()
        error_msg = "; ".join(f"{'.'.join(str(loc) for loc in err['loc'])}: {err['msg']}" for err in errors)
        return JSONResponse(
            status_code=422,
            content={
                "type": "https://api.aarav-inference.internal/errors/invalid_payload",
                "title": "Invalid Request Payload",
                "status": 422,
                "detail": error_msg,
                "instance": str(request.url.path),
                "code": "INVALID_PAYLOAD",
                "correlation_id": correlation_id,
            },
        )

    @app.exception_handler(Exception)
    async def handle_unhandled_exception(request: Request, exc: Exception) -> JSONResponse:
        correlation_id = get_correlation_id()
        logger.error(
            f"Unhandled server exception: {exc}",
            exc_info=True,
            extra={"extra": {"correlation_id": correlation_id}},
        )
        return JSONResponse(
            status_code=500,
            content={
                "type": "https://api.aarav-inference.internal/errors/internal_error",
                "title": "Internal Server Error",
                "status": 500,
                "detail": "An unexpected error occurred. Please contact support with the correlation ID.",
                "instance": str(request.url.path),
                "code": "INTERNAL_SERVER_ERROR",
                "correlation_id": correlation_id,
            },
        )

    # 3. Mount Routes
    app.include_router(api_router)

    return app


# Application ASGI object
app = create_app()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.main:app", host="127.0.0.1", port=8000, reload=settings.DEBUG)
