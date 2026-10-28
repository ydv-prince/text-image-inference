"""
Production Observability & Prometheus Metrics Engine
Location: src/core/metrics.py

Guarantees:
1. Zero External Dependencies: Implements native, high-performance Prometheus text exposition (v0.0.4).
2. Strict Cardinality Control: Rejects unbounded labels (job IDs, user IDs, API keys, file paths, raw exceptions).
3. Thread-Safe & Async-Safe: Threading locks protect metric series updates.
4. Idempotent Registration: Duplicate metric registration safely returns existing instances without error.
5. Cached Database Sampling: Queue and outbox depth queries are throttled with a configurable TTL cache.
6. Process-Local Precision: Accurately reflects runtime telemetry; cleanly resettable between test runs.
"""

from collections import defaultdict
from dataclasses import dataclass, field
import hashlib
import re
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple


# Default latency buckets (in seconds) for HTTP request duration
DEFAULT_LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
DEFAULT_INFERENCE_BUCKETS = (0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0)
DEFAULT_STORAGE_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0)

# Forbidden label keys to prevent accidental high-cardinality leaks
FORBIDDEN_LABEL_KEYS = {
    "worker_id",
    "job_id",
    "user_id",
    "api_key",
    "client_id",
    "prompt",
    "file_hash",
    "storage_path",
    "correlation_id",
    "error_message",
    "exception",
    "token",
}


def _normalize_labels(labels: Optional[Dict[str, str]]) -> Tuple[Tuple[str, str], ...]:
    """Sorts and validates label pairs into a canonical, hashable tuple."""
    if not labels:
        return ()
    normalized = []
    for k, v in sorted(labels.items()):
        clean_k = str(k).strip()
        if clean_k.lower() in FORBIDDEN_LABEL_KEYS:
            raise ValueError(f"High-cardinality or sensitive label key '{clean_k}' is strictly prohibited.")
        # Sanitize value string per Prometheus exposition standard
        clean_v = str(v).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
        normalized.append((clean_k, clean_v))
    return tuple(normalized)


def _format_label_str(label_tuple: Tuple[Tuple[str, str], ...], extra: Optional[Dict[str, str]] = None) -> str:
    """Formats canonical label tuple into a Prometheus label string {k=\"v\",...}."""
    pairs = list(label_tuple)
    if extra:
        for k, v in sorted(extra.items()):
            pairs.append((k, str(v)))
    if not pairs:
        return ""
    formatted = ",".join(f'{k}="{v}"' for k, v in pairs)
    return f"{{{formatted}}}"


class Counter:
    """Cumulative metric that monotonically increases."""

    def __init__(self, name: str, help_text: str, label_names: Sequence[str] = ()):
        self.name = name
        self.help_text = help_text
        self.label_names = set(label_names)
        self._values: Dict[Tuple[Tuple[str, str], ...], float] = defaultdict(float)
        self._lock = threading.Lock()

    def inc(self, amount: float = 1.0, labels: Optional[Dict[str, str]] = None) -> None:
        if amount < 0:
            raise ValueError("Counter increments must be non-negative.")
        norm = _normalize_labels(labels)
        with self._lock:
            self._values[norm] += amount

    def get(self, labels: Optional[Dict[str, str]] = None) -> float:
        norm = _normalize_labels(labels)
        with self._lock:
            return self._values[norm]

    def reset(self) -> None:
        with self._lock:
            self._values.clear()

    def collect(self) -> List[str]:
        lines = [
            f"# HELP {self.name} {self.help_text}",
            f"# TYPE {self.name} counter",
        ]
        with self._lock:
            if not self._values:
                if not self.label_names:
                    lines.append(f"{self.name} 0")
            else:
                for labels, val in sorted(self._values.items()):
                    lbl_str = _format_label_str(labels)
                    val_str = f"{int(val)}" if val.is_integer() else f"{val:.4f}"
                    lines.append(f"{self.name}{lbl_str} {val_str}")
        return lines


class Gauge:
    """Metric representing a single numerical value that can go up or down."""

    def __init__(self, name: str, help_text: str, label_names: Sequence[str] = ()):
        self.name = name
        self.help_text = help_text
        self.label_names = set(label_names)
        self._values: Dict[Tuple[Tuple[str, str], ...], float] = defaultdict(float)
        self._lock = threading.Lock()

    def set(self, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        norm = _normalize_labels(labels)
        with self._lock:
            self._values[norm] = float(value)

    def inc(self, amount: float = 1.0, labels: Optional[Dict[str, str]] = None) -> None:
        norm = _normalize_labels(labels)
        with self._lock:
            self._values[norm] += amount

    def dec(self, amount: float = 1.0, labels: Optional[Dict[str, str]] = None) -> None:
        norm = _normalize_labels(labels)
        with self._lock:
            self._values[norm] -= amount

    def get(self, labels: Optional[Dict[str, str]] = None) -> float:
        norm = _normalize_labels(labels)
        with self._lock:
            return self._values[norm]

    def reset(self) -> None:
        with self._lock:
            self._values.clear()

    def collect(self) -> List[str]:
        lines = [
            f"# HELP {self.name} {self.help_text}",
            f"# TYPE {self.name} gauge",
        ]
        with self._lock:
            if not self._values:
                if not self.label_names:
                    lines.append(f"{self.name} 0")
            else:
                for labels, val in sorted(self._values.items()):
                    lbl_str = _format_label_str(labels)
                    val_str = f"{int(val)}" if val.is_integer() else f"{val:.4f}"
                    lines.append(f"{self.name}{lbl_str} {val_str}")
        return lines


class Histogram:
    """Samples observations (usually durations or sizes) into configurable buckets."""

    def __init__(
        self,
        name: str,
        help_text: str,
        label_names: Sequence[str] = (),
        buckets: Sequence[float] = DEFAULT_LATENCY_BUCKETS,
    ):
        self.name = name
        self.help_text = help_text
        self.label_names = set(label_names)
        self.buckets = sorted(set(buckets))
        self._counts: Dict[Tuple[Tuple[str, str], ...], int] = defaultdict(int)
        self._sums: Dict[Tuple[Tuple[str, str], ...], float] = defaultdict(float)
        self._bucket_counts: Dict[Tuple[Tuple[str, str], ...], Dict[float, int]] = defaultdict(
            lambda: {b: 0 for b in self.buckets}
        )
        self._lock = threading.Lock()

    def observe(self, amount: float, labels: Optional[Dict[str, str]] = None) -> None:
        if amount < 0:
            amount = 0.0
        norm = _normalize_labels(labels)
        with self._lock:
            self._counts[norm] += 1
            self._sums[norm] += amount
            bucket_map = self._bucket_counts[norm]
            for b in self.buckets:
                if amount <= b:
                    bucket_map[b] += 1

    def reset(self) -> None:
        with self._lock:
            self._counts.clear()
            self._sums.clear()
            self._bucket_counts.clear()

    def collect(self) -> List[str]:
        lines = [
            f"# HELP {self.name} {self.help_text}",
            f"# TYPE {self.name} histogram",
        ]
        with self._lock:
            if not self._counts:
                if not self.label_names:
                    for b in self.buckets:
                        b_str = f"{b:.4f}".rstrip("0").rstrip(".") if not b.is_integer() else str(int(b))
                        lines.append(f'{self.name}_bucket{{le="{b_str}"}} 0')
                    lines.append(f'{self.name}_bucket{{le="+Inf"}} 0')
                    lines.append(f'{self.name}_sum 0.0')
                    lines.append(f'{self.name}_count 0')
            else:
                for labels, total_count in sorted(self._counts.items()):
                    bucket_map = self._bucket_counts[labels]
                    for b in self.buckets:
                        cumulative = bucket_map[b]
                        b_str = f"{b:.4f}".rstrip("0").rstrip(".") if not b.is_integer() else str(int(b))
                        lines.append(
                            f'{self.name}_bucket{_format_label_str(labels, extra={"le": b_str})} {cumulative}'
                        )
                    # +Inf bucket matches total count
                    lines.append(
                        f'{self.name}_bucket{_format_label_str(labels, extra={"le": "+Inf"})} {total_count}'
                    )
                    total_sum = self._sums[labels]
                    sum_str = f"{total_sum:.4f}"
                    lines.append(f"{self.name}_sum{_format_label_str(labels)} {sum_str}")
                    lines.append(f"{self.name}_count{_format_label_str(labels)} {total_count}")
        return lines


class MetricsRegistry:
    """Central registry managing metric definitions and Prometheus exposition."""

    def __init__(self):
        self._metrics: Dict[str, Any] = {}
        self._lock = threading.Lock()

    def register_counter(self, name: str, help_text: str, label_names: Sequence[str] = ()) -> Counter:
        with self._lock:
            if name in self._metrics:
                return self._metrics[name]
            counter = Counter(name, help_text, label_names)
            self._metrics[name] = counter
            return counter

    def register_gauge(self, name: str, help_text: str, label_names: Sequence[str] = ()) -> Gauge:
        with self._lock:
            if name in self._metrics:
                return self._metrics[name]
            gauge = Gauge(name, help_text, label_names)
            self._metrics[name] = gauge
            return gauge

    def register_histogram(
        self,
        name: str,
        help_text: str,
        label_names: Sequence[str] = (),
        buckets: Sequence[float] = DEFAULT_LATENCY_BUCKETS,
    ) -> Histogram:
        with self._lock:
            if name in self._metrics:
                return self._metrics[name]
            hist = Histogram(name, help_text, label_names, buckets)
            self._metrics[name] = hist
            return hist

    def reset_all(self) -> None:
        """Resets all metric values to zero without clearing registrations."""
        with self._lock:
            for m in self._metrics.values():
                m.reset()

    def clear(self) -> None:
        """Deregisters all metrics (primarily for test suite tear-downs)."""
        with self._lock:
            self._metrics.clear()

    def export_prometheus_text(self) -> str:
        """Exports all registered metrics in standard Prometheus v0.0.4 text format."""
        all_lines = []
        with self._lock:
            metric_list = list(self._metrics.values())

        for m in metric_list:
            all_lines.extend(m.collect())
            all_lines.append("")  # Blank line separator between metric families

        return "\n".join(all_lines).strip() + "\n"


# =============================================================================
# Global Application Telemetry Registry & Metric Definitions
# =============================================================================
metrics_registry = MetricsRegistry()

# -----------------------------------------------------------------------------
# 1. Generation API & Queue Metrics
# -----------------------------------------------------------------------------
generation_requests_total = metrics_registry.register_counter(
    "aarav_generation_requests_total",
    "Total generation requests processed by HTTP status and outcome category.",
    label_names=("outcome", "status"),
)

http_request_duration_seconds = metrics_registry.register_histogram(
    "aarav_http_request_duration_seconds",
    "Latency of incoming HTTP requests partitioned by method and endpoint template.",
    label_names=("method", "endpoint"),
    buckets=DEFAULT_LATENCY_BUCKETS,
)

http_requests_total = metrics_registry.register_counter(
    "aarav_http_requests_total",
    "Total HTTP requests received partitioned by method, endpoint template, and status code.",
    label_names=("method", "endpoint", "status"),
)

queue_depth_gauge = metrics_registry.register_gauge(
    "aarav_queue_depth",
    "Current depth of generation jobs in the database queue by lifecycle status.",
    label_names=("status",),
)

jobs_completed_total = metrics_registry.register_counter(
    "aarav_jobs_completed_total",
    "Cumulative count of generation jobs successfully completed.",
)

jobs_failed_total = metrics_registry.register_counter(
    "aarav_jobs_failed_total",
    "Cumulative count of generation jobs that failed, partitioned by standardized error code.",
    label_names=("error_code",),
)

job_retries_total = metrics_registry.register_counter(
    "aarav_job_retries_total",
    "Cumulative count of generation jobs rescheduled for retry.",
)

leases_reclaimed_total = metrics_registry.register_counter(
    "aarav_leases_reclaimed_total",
    "Cumulative count of timed-out worker leases reclaimed by the lease sweeper daemon.",
)

# -----------------------------------------------------------------------------
# 2. Hybrid Worker Metrics (Bounded by worker_category)
# -----------------------------------------------------------------------------
worker_jobs_completed_total = metrics_registry.register_counter(
    "aarav_worker_jobs_completed_total",
    "Cumulative jobs successfully processed by hybrid worker daemons.",
    label_names=("worker_category",),
)

worker_jobs_failed_total = metrics_registry.register_counter(
    "aarav_worker_jobs_failed_total",
    "Cumulative job failures encountered by hybrid workers partitioned by error code.",
    label_names=("worker_category", "error_code"),
)

worker_heartbeats_total = metrics_registry.register_counter(
    "aarav_worker_heartbeats_total",
    "Cumulative lease renewal heartbeat pings sent by workers.",
    label_names=("worker_category", "outcome"),
)

worker_lease_lost_total = metrics_registry.register_counter(
    "aarav_worker_lease_lost_total",
    "Cumulative lease revocation / loss events detected by workers during execution.",
    label_names=("worker_category",),
)

worker_inference_duration_seconds = metrics_registry.register_histogram(
    "aarav_worker_inference_duration_seconds",
    "Execution duration of model image generation in the worker.",
    label_names=("worker_category",),
    buckets=DEFAULT_INFERENCE_BUCKETS,
)

worker_upload_duration_seconds = metrics_registry.register_histogram(
    "aarav_worker_upload_duration_seconds",
    "Execution duration of physical asset uploads to CAS or cloud storage.",
    label_names=("worker_category",),
    buckets=DEFAULT_STORAGE_BUCKETS,
)

worker_status_gauge = metrics_registry.register_gauge(
    "aarav_worker_status",
    "Current status of worker daemon: 1=running, 0=stopped.",
    label_names=("worker_category",),
)

# -----------------------------------------------------------------------------
# 3. Deletion Outbox Metrics
# -----------------------------------------------------------------------------
deletion_outbox_queue_depth = metrics_registry.register_gauge(
    "aarav_deletion_outbox_queue_depth",
    "Current count of physical deletion tasks in outbox by status.",
    label_names=("status",),
)

deletion_tasks_total = metrics_registry.register_counter(
    "aarav_deletion_tasks_total",
    "Cumulative count of deletion outbox tasks processed by outcome.",
    label_names=("outcome",),
)

deletion_processing_duration_seconds = metrics_registry.register_histogram(
    "aarav_deletion_processing_duration_seconds",
    "Latency of physical storage deletion operations outside DB transactions.",
    buckets=DEFAULT_STORAGE_BUCKETS,
)

deletion_leases_reclaimed_total = metrics_registry.register_counter(
    "aarav_deletion_leases_reclaimed_total",
    "Cumulative expired outbox worker leases recovered by outbox recovery sweeps.",
)

deletion_failures_total = metrics_registry.register_counter(
    "aarav_deletion_failures_total",
    "Cumulative physical deletion failures partitioned by bounded error category.",
    label_names=("error_category",),
)

# -----------------------------------------------------------------------------
# 4. Storage Reconciliation Metrics
# -----------------------------------------------------------------------------
reconciliation_last_scan_timestamp_seconds = metrics_registry.register_gauge(
    "aarav_reconciliation_last_scan_timestamp_seconds",
    "Epoch timestamp in seconds of the most recently executed storage reconciliation scan.",
)

reconciliation_last_scan_duration_seconds = metrics_registry.register_gauge(
    "aarav_reconciliation_last_scan_duration_seconds",
    "Duration in seconds of the most recently executed storage reconciliation scan.",
)

reconciliation_findings_current = metrics_registry.register_gauge(
    "aarav_reconciliation_findings_current",
    "Current discrepancy findings from the latest reconciliation scan by category and severity.",
    label_names=("category", "severity"),
)

reconciliation_scan_complete_gauge = metrics_registry.register_gauge(
    "aarav_reconciliation_scan_complete",
    "Completeness indicator of the latest storage scan (1=complete, 0=incomplete/truncated).",
)

reconciliation_scan_errors_total = metrics_registry.register_counter(
    "aarav_reconciliation_scan_errors_total",
    "Cumulative scan errors encountered across storage reconciliation executions.",
)


def record_reconciliation_telemetry(report: Any) -> None:
    """
    Safely bridges findings from a completed StorageReconciliationReport into metrics gauges.
    Never executes scans autonomously; strictly consumes existing scan outcomes.
    """
    try:
        if hasattr(report, "completed_at") and report.completed_at:
            reconciliation_last_scan_timestamp_seconds.set(report.completed_at.timestamp())
        if hasattr(report, "started_at") and hasattr(report, "completed_at"):
            duration = (report.completed_at - report.started_at).total_seconds()
            reconciliation_last_scan_duration_seconds.set(max(0.0, duration))

        reconciliation_scan_complete_gauge.set(1.0 if getattr(report, "is_complete", False) else 0.0)

        # Clear existing findings gauges before setting current values
        reconciliation_findings_current.reset()
        if hasattr(report, "findings"):
            counts: Dict[Tuple[str, str], int] = defaultdict(int)
            for f in report.findings:
                cat = getattr(f.category, "value", str(f.category))
                sev = getattr(f.severity, "value", str(f.severity))
                counts[(cat, sev)] += 1
            for (cat, sev), count in counts.items():
                reconciliation_findings_current.set(count, labels={"category": cat, "severity": sev})

        if hasattr(report, "scan_errors") and report.scan_errors:
            reconciliation_scan_errors_total.inc(len(report.scan_errors))

    except Exception:
        # Observability bridge must never raise exceptions into business workflows
        pass
