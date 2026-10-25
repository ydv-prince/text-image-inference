"""
Unit Tests for Health Probes & Correlation Middleware
Location: tests/unit/test_health.py
"""

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_liveness_probe(client: AsyncClient):
    """Verifies that /health/liveness returns 200 OK and expected metadata."""
    response = await client.get("/health/liveness")
    assert response.status_code == 200

    data = response.json()
    assert data["status"] == "alive"
    assert "timestamp" in data
    assert "version" in data
    assert "environment" in data


@pytest.mark.asyncio
async def test_readiness_probe_healthy(client: AsyncClient):
    """Verifies that /health/readiness checks DB connectivity and storage directory."""
    response = await client.get("/health/readiness")
    assert response.status_code == 200

    data = response.json()
    assert data["status"] == "ready"
    assert data["database"] == "connected"
    assert data["storage"] == "accessible"
    assert "timestamp" in data


@pytest.mark.asyncio
async def test_correlation_id_propagation(client: AsyncClient):
    """Verifies that X-Correlation-ID is generated and returned in headers."""
    response = await client.get("/health/liveness")
    assert response.status_code == 200
    assert "X-Correlation-ID" in response.headers
    generated_id = response.headers["X-Correlation-ID"]
    assert len(generated_id) > 10

    # Test client-provided correlation ID
    custom_id = "test-correlation-client-12345"
    response2 = await client.get("/health/liveness", headers={"X-Correlation-ID": custom_id})
    assert response2.headers["X-Correlation-ID"] == custom_id
