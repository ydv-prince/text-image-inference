"""
Comprehensive Observability & Prometheus Metrics Engine Unit Tests
Location: tests/unit/test_metrics.py
"""

from datetime import datetime, timezone
import re
import secrets
import pytest
from httpx import ASGITransport, AsyncClient
from pydantic import SecretStr
from unittest.mock import AsyncMock, patch

from src.core.config import settings
from src.core.metrics import (
    Counter,
    FORBIDDEN_LABEL_KEYS,
    Gauge,
    Histogram,
    MetricsRegistry,
    _normalize_labels,
    deletion_outbox_queue_depth,
    http_request_duration_seconds,
    http_requests_total,
    metrics_registry,
    queue_depth_gauge,
    record_reconciliation_telemetry,
    reconciliation_findings_current,
    reconciliation_last_scan_duration_seconds,
    reconciliation_last_scan_timestamp_seconds,
    reconciliation_scan_complete_gauge,
    reconciliation_scan_errors_total,
)
from src.api.middleware.metrics import normalize_path_template
from src.main import app
from src.services.storage_reconciliation_service import (
    FindingCategory,
    ReconciliationFinding,
    Severity,
    StorageReconciliationReport,
)


# =============================================================================
# 1. Counter Primitive Tests
# =============================================================================
def test_counter_basic_and_increment():
    counter = Counter("test_counter_total", "Test counter help")
    assert counter.get() == 0.0

    counter.inc()
    assert counter.get() == 1.0

    counter.inc(5.5)
    assert counter.get() == 6.5

    counter.reset()
    assert counter.get() == 0.0


def test_counter_rejects_negative_increment():
    counter = Counter("test_counter_neg", "Help")
    with pytest.raises(ValueError, match="Counter increments must be non-negative"):
        counter.inc(-1.0)


def test_counter_labeled_aggregation():
    counter = Counter("test_labeled_counter", "Help", label_names=("method", "status"))
    counter.inc(1.0, labels={"method": "GET", "status": "200"})
    counter.inc(2.0, labels={"method": "GET", "status": "200"})
    counter.inc(1.0, labels={"method": "POST", "status": "201"})

    assert counter.get(labels={"method": "GET", "status": "200"}) == 3.0
    assert counter.get(labels={"method": "POST", "status": "201"}) == 1.0
    assert counter.get(labels={"method": "DELETE", "status": "404"}) == 0.0


def test_counter_prometheus_exposition_format():
    counter = Counter("aarav_test_counter_total", "A test counter")
    counter.inc(3.0, labels={"env": "prod"})
    lines = counter.collect()

    assert "# HELP aarav_test_counter_total A test counter" in lines
    assert "# TYPE aarav_test_counter_total counter" in lines
    assert 'aarav_test_counter_total{env="prod"} 3' in lines


# =============================================================================
# 2. Gauge Primitive Tests
# =============================================================================
def test_gauge_set_inc_dec():
    gauge = Gauge("test_gauge", "Test gauge help")
    assert gauge.get() == 0.0

    gauge.set(42.0)
    assert gauge.get() == 42.0

    gauge.inc(8.0)
    assert gauge.get() == 50.0

    gauge.dec(15.0)
    assert gauge.get() == 35.0

    gauge.reset()
    assert gauge.get() == 0.0


def test_gauge_labeled_operations():
    gauge = Gauge("test_queue_depth", "Depth", label_names=("status",))
    gauge.set(10, labels={"status": "PENDING"})
    gauge.set(3, labels={"status": "PROCESSING"})

    assert gauge.get(labels={"status": "PENDING"}) == 10.0
    assert gauge.get(labels={"status": "PROCESSING"}) == 3.0

    gauge.inc(2, labels={"status": "PENDING"})
    assert gauge.get(labels={"status": "PENDING"}) == 12.0

    lines = gauge.collect()
    assert 'test_queue_depth{status="PENDING"} 12' in lines
    assert 'test_queue_depth{status="PROCESSING"} 3' in lines


# =============================================================================
# 3. Histogram Primitive Tests
# =============================================================================
def test_histogram_observation_and_buckets():
    buckets = (0.1, 0.5, 1.0, 5.0)
    hist = Histogram("test_duration_seconds", "Duration", buckets=buckets)

    # Observe values: 0.05 (falls in <=0.1), 0.3 (falls in <=0.5), 2.0 (falls in <=5.0)
    hist.observe(0.05)
    hist.observe(0.3)
    hist.observe(2.0)

    lines = hist.collect()
    text = "\n".join(lines)

    assert '# TYPE test_duration_seconds histogram' in text
    assert 'test_duration_seconds_bucket{le="0.1"} 1' in text
    assert 'test_duration_seconds_bucket{le="0.5"} 2' in text
    assert 'test_duration_seconds_bucket{le="1"} 2' in text
    assert 'test_duration_seconds_bucket{le="5"} 3' in text
    assert 'test_duration_seconds_bucket{le="+Inf"} 3' in text
    assert 'test_duration_seconds_count 3' in text
    assert 'test_duration_seconds_sum 2.3500' in text


def test_histogram_labeled_observation():
    hist = Histogram("test_http_duration", "Duration", label_names=("endpoint",), buckets=(0.5, 1.0))
    hist.observe(0.2, labels={"endpoint": "/health"})
    hist.observe(0.8, labels={"endpoint": "/health"})

    lines = hist.collect()
    text = "\n".join(lines)

    assert 'test_http_duration_bucket{endpoint="/health",le="0.5"} 1' in text
    assert 'test_http_duration_bucket{endpoint="/health",le="1"} 2' in text
    assert 'test_http_duration_bucket{endpoint="/health",le="+Inf"} 2' in text
    assert 'test_http_duration_count{endpoint="/health"} 2' in text


# =============================================================================
# 4. Label Cardinality & Security Boundary
# =============================================================================
@pytest.mark.parametrize("forbidden_key", list(FORBIDDEN_LABEL_KEYS))
def test_forbidden_label_keys_rejected(forbidden_key):
    with pytest.raises(ValueError, match="High-cardinality or sensitive label key"):
        _normalize_labels({forbidden_key: "forbidden_value_123"})


def test_label_normalization_and_sanitization():
    norm = _normalize_labels({"b_key": "val1", "a_key": 'val with "quotes" and \nnewline'})
    # Sorted order
    assert norm[0][0] == "a_key"
    assert norm[0][1] == 'val with \\"quotes\\" and \\nnewline'
    assert norm[1][0] == "b_key"
    assert norm[1][1] == "val1"


# =============================================================================
# 5. MetricsRegistry Tests
# =============================================================================
def test_registry_idempotent_registration():
    reg = MetricsRegistry()
    c1 = reg.register_counter("aarav_custom_counter", "Custom counter")
    c1.inc(5)

    # Re-registering with the same name returns identical object without clearing values
    c2 = reg.register_counter("aarav_custom_counter", "Custom counter")
    assert c1 is c2
    assert c2.get() == 5.0


def test_registry_reset_all():
    reg = MetricsRegistry()
    c = reg.register_counter("counter_a", "Help")
    g = reg.register_gauge("gauge_b", "Help")
    c.inc(10)
    g.set(25)

    assert c.get() == 10.0
    assert g.get() == 25.0

    reg.reset_all()

    assert c.get() == 0.0
    assert g.get() == 0.0


def test_registry_export_prometheus_text():
    reg = MetricsRegistry()
    reg.register_counter("app_requests_total", "Total requests").inc(42)
    reg.register_gauge("app_active_connections", "Active connections").set(3)

    exported = reg.export_prometheus_text()
    assert "# TYPE app_requests_total counter" in exported
    assert "app_requests_total 42" in exported
    assert "# TYPE app_active_connections gauge" in exported
    assert "app_active_connections 3" in exported


# =============================================================================
# 6. Path Normalization Tests
# =============================================================================
def test_path_template_normalization():
    # UUIDs
    assert normalize_path_template("/api/v1/images/generations/123e4567-e89b-12d3-a456-426614174000") == "/api/v1/images/generations/{id}"
    assert normalize_path_template("/api/v1/images/generations/123e4567-e89b-12d3-a456-426614174000/content") == "/api/v1/images/generations/{id}/content"
    # Worker routes
    assert normalize_path_template("/api/v1/worker/jobs/123e4567-e89b-12d3-a456-426614174000/heartbeat") == "/api/v1/worker/jobs/{id}/heartbeat"
    assert normalize_path_template("/api/v1/worker/jobs/job-test-uuid-456/fail") == "/api/v1/worker/jobs/{id}/fail"
    # Static routes
    assert normalize_path_template("/health/liveness") == "/health/liveness"
    assert normalize_path_template("/health/readiness") == "/health/readiness"
    assert normalize_path_template("/metrics") == "/metrics"
    assert normalize_path_template("/api/v1/worker/jobs/lease") == "/api/v1/worker/jobs/lease"


# =============================================================================
# 7. Storage Reconciliation Telemetry Bridge Tests
# =============================================================================
def test_record_reconciliation_telemetry():
    t_start = datetime(2026, 10, 27, 12, 0, 0, tzinfo=timezone.utc)
    t_end = datetime(2026, 10, 27, 12, 0, 15, tzinfo=timezone.utc)

    findings = [
        ReconciliationFinding(
            id="f1",
            category=FindingCategory.REFERENCE_COUNT_MISMATCH,
            severity=Severity.HIGH,
            entity_type="blob",
            entity_id="b1",
            locator="loc1",
            description="desc1",
        ),
        ReconciliationFinding(
            id="f2",
            category=FindingCategory.POTENTIAL_ORPHAN,
            severity=Severity.CRITICAL,
            entity_type="blob",
            entity_id="b2",
            locator="loc2",
            description="desc2",
        ),
        ReconciliationFinding(
            id="f3",
            category=FindingCategory.POTENTIAL_ORPHAN,
            severity=Severity.CRITICAL,
            entity_type="blob",
            entity_id="b3",
            locator="loc3",
            description="desc3",
        ),
    ]

    report = StorageReconciliationReport(
        scan_id="scan-test-01",
        started_at=t_start,
        completed_at=t_end,
        is_complete=True,
        scanned_assets_count=10,
        scanned_blobs_count=10,
        scanned_outbox_tasks_count=5,
        scanned_physical_objects_count=10,
        findings=findings,
        scan_errors=["Mock error 1"],
    )

    record_reconciliation_telemetry(report)

    assert reconciliation_last_scan_duration_seconds.get() == 15.0
    assert reconciliation_last_scan_timestamp_seconds.get() == t_end.timestamp()
    assert reconciliation_scan_complete_gauge.get() == 1.0

    # High severity ref count mismatch
    assert reconciliation_findings_current.get(
        labels={"category": "REFERENCE_COUNT_MISMATCH", "severity": "HIGH"}
    ) == 1.0
    # Critical potential orphan count = 2
    assert reconciliation_findings_current.get(
        labels={"category": "POTENTIAL_ORPHAN", "severity": "CRITICAL"}
    ) == 2.0


# =============================================================================
# 8. /metrics HTTP Endpoint Integration Tests
# =============================================================================
@pytest.mark.asyncio
async def test_metrics_endpoint_returns_prometheus_text():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/metrics")
        assert resp.status_code == 200
        assert "text/plain" in resp.headers["content-type"]
        assert "version=0.0.4" in resp.headers["content-type"]

        body = resp.text
        assert "aarav_http_requests_total" in body
        assert "aarav_queue_depth" in body
        assert "aarav_worker_status" in body
        assert "aarav_deletion_outbox_queue_depth" in body


@pytest.mark.asyncio
async def test_metrics_endpoint_disabled_mode():
    with patch.object(settings, "METRICS_ENABLED", False):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/metrics")
            assert resp.status_code == 404


@pytest.mark.asyncio
async def test_metrics_endpoint_auth_enforcement():
    with patch.object(settings, "METRICS_REQUIRE_AUTH", True), patch.object(
        settings, "ADMIN_API_KEY", SecretStr("test-admin-secret-token")
    ):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # 1. Unauthenticated request -> 401
            resp = await client.get("/metrics")
            assert resp.status_code == 401

            # 2. Empty token -> 401
            resp = await client.get("/metrics", headers={"X-Admin-API-Key": ""})
            assert resp.status_code == 401

            # 3. Invalid token -> 401
            resp = await client.get("/metrics", headers={"X-Admin-API-Key": "wrong-token"})
            assert resp.status_code == 401

            # 4. Valid X-Admin-API-Key -> 200
            resp = await client.get("/metrics", headers={"X-Admin-API-Key": "test-admin-secret-token"})
            assert resp.status_code == 200

            # 5. Valid Authorization Bearer header -> 200
            resp = await client.get("/metrics", headers={"Authorization": "Bearer test-admin-secret-token"})
            assert resp.status_code == 200

            # 6. Valid Authorization raw token header -> 200
            resp = await client.get("/metrics", headers={"Authorization": "test-admin-secret-token"})
            assert resp.status_code == 200

    # 7. METRICS_REQUIRE_AUTH is True but ADMIN_API_KEY is None -> 401
    with patch.object(settings, "METRICS_REQUIRE_AUTH", True), patch.object(
        settings, "ADMIN_API_KEY", None
    ):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/metrics", headers={"X-Admin-API-Key": "any-token"})
            assert resp.status_code == 401


@pytest.mark.asyncio
async def test_metrics_auth_uses_constant_time_compare_digest():
    """SEC-METRICS-01: Verifies secrets.compare_digest is used to prevent timing side channels."""
    with patch.object(settings, "METRICS_REQUIRE_AUTH", True), patch.object(
        settings, "ADMIN_API_KEY", SecretStr("secret-token-12345")
    ), patch("src.api.metrics.secrets.compare_digest", wraps=secrets.compare_digest) as mock_compare:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/metrics", headers={"X-Admin-API-Key": "secret-token-12345"})
            assert resp.status_code == 200
            assert mock_compare.called
            assert mock_compare.call_args[0] == ("secret-token-12345", "secret-token-12345")


@pytest.mark.asyncio
async def test_metrics_database_sampling_resilience():
    # If the database query raises an exception during scrape sampling, /metrics must not crash
    with patch("src.api.metrics.async_session_factory", side_effect=RuntimeError("Simulated DB connection failure")):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/metrics")
            # Must return 200 despite database failure
            assert resp.status_code == 200


@pytest.mark.asyncio
async def test_concurrent_scrape_throttling():
    """Verifies that concurrent scrape requests do not cause thundering herd DB queries."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Launch 10 concurrent scrape requests
        import asyncio
        tasks = [client.get("/metrics") for _ in range(10)]
        responses = await asyncio.gather(*tasks)

        # All concurrent scrapes must succeed with 200 OK
        for r in responses:
            assert r.status_code == 200
            assert "aarav_queue_depth" in r.text


def test_worker_metrics_bounded_category():
    """Worker metrics must use bounded worker_category and forbid raw worker_id."""
    from src.core.metrics import worker_status_gauge, worker_jobs_completed_total

    # Forbidden raw worker_id
    with pytest.raises(ValueError, match="High-cardinality or sensitive label key 'worker_id' is strictly prohibited"):
        worker_jobs_completed_total.inc(labels={"worker_id": "hybrid-worker-random-uuid-1234"})

    # Valid bounded worker_category
    worker_status_gauge.set(1.0, labels={"worker_category": "hybrid_gpu"})
    assert worker_status_gauge.get(labels={"worker_category": "hybrid_gpu"}) == 1.0


# =============================================================================
# 9. Health Check Safety & Isolation Test
# =============================================================================
@pytest.mark.asyncio
async def test_health_checks_do_not_leak_secrets():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Liveness
        resp_live = await client.get("/health/liveness")
        assert resp_live.status_code == 200
        live_json = resp_live.json()
        assert live_json["status"] == "alive"
        for sensitive in ("salt", "key", "secret", "password", "token", "base_path"):
            assert sensitive not in live_json

        # Readiness
        resp_ready = await client.get("/health/readiness")
        assert resp_ready.status_code == 200
        ready_json = resp_ready.json()
        assert ready_json["status"] in ("ready", "not_ready")
        for sensitive in ("salt", "key", "secret", "password", "token", "base_path"):
            assert sensitive not in ready_json
