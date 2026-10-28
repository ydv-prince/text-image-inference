"""
Top-Level API Router
Location: src/api/router.py
"""

from fastapi import APIRouter
from src.api.health import router as health_router
from src.api.metrics import router as metrics_router
from src.api.v1.generations import router as generations_router
from src.api.v1.worker import router as worker_router

api_router = APIRouter()

# Health endpoints (/health/liveness, /health/readiness)
api_router.include_router(health_router)

# Metrics endpoint (/metrics)
api_router.include_router(metrics_router)

# V1 API endpoints (/api/v1/images/generations, /api/v1/worker)
v1_router = APIRouter(prefix="/api/v1")
v1_router.include_router(generations_router)
v1_router.include_router(worker_router)

api_router.include_router(v1_router)
