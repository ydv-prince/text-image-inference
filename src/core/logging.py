"""
Structured JSON Logging & Correlation Context
Location: src/core/logging.py
"""

import json
import logging
import sys
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any, Dict, Optional
import uuid

# Global ContextVar for tracking request/correlation ID across async coroutines
correlation_id_ctx: ContextVar[Optional[str]] = ContextVar("correlation_id", default=None)


def get_correlation_id() -> str:
    """Retrieves current correlation ID or generates a fallback UUID."""
    cid = correlation_id_ctx.get()
    if not cid:
        cid = str(uuid.uuid4())
        correlation_id_ctx.set(cid)
    return cid


def set_correlation_id(cid: str) -> None:
    """Sets the active correlation ID in context."""
    correlation_id_ctx.set(cid)


class JSONFormatter(logging.Formatter):
    """Formats log records as structured, single-line JSON objects."""

    SENSITIVE_KEYS = {"api_key", "password", "secret", "token", "authorization"}

    def format(self, record: logging.LogRecord) -> str:
        log_entry: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "correlation_id": correlation_id_ctx.get(),
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        # Include custom extra fields if provided
        if hasattr(record, "extra") and isinstance(record.extra, dict):
            for k, v in record.extra.items():
                if any(s in k.lower() for s in self.SENSITIVE_KEYS):
                    log_entry[k] = "[MASKED]"
                else:
                    log_entry[k] = v

        return json.dumps(log_entry, default=str)


def setup_logging(level: str = "INFO") -> None:
    """Configures root logger with JSON formatting on standard output."""
    root = logging.getLogger()
    root.setLevel(level)

    # Remove existing handlers to avoid duplicates
    for handler in list(root.handlers):
        root.removeHandler(handler)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())
    root.addHandler(handler)


logger = logging.getLogger("aarav.api")
