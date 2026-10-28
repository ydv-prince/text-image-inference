"""
Application Configuration — Pydantic v2 Settings
Location: src/core/config.py
"""

from pathlib import Path
from typing import Literal, Optional
from urllib.parse import urlparse, parse_qs
from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


# Known development-only default salt — must never be used in production.
_DEV_SALT_SENTINEL = "aarav_default_development_salt_do_not_use_in_prod"


def _validate_postgres_tls(url_str: Optional[str], field_name: str) -> None:
    """
    Validates that a PostgreSQL connection URL enforces TLS in production.
    Supports driver-specific TLS configuration (sslmode for sync libpq, ssl/sslmode for asyncpg).
    Never exposes passwords, hostnames, or credentials in validation errors.
    """
    if not url_str or not isinstance(url_str, str):
        return

    try:
        parsed = urlparse(url_str)
    except Exception:
        raise ValueError(f"Malformed {field_name} connection URL.")

    scheme = parsed.scheme.lower()
    # Check if scheme indicates PostgreSQL
    if not (scheme.startswith("postgres") or "postgres" in scheme):
        return

    is_asyncpg = "asyncpg" in scheme
    query_params = parse_qs(parsed.query)

    ssl_val = query_params.get("ssl", [None])[0]
    sslmode_val = query_params.get("sslmode", [None])[0]

    valid_sslmodes = {"require", "verify-ca", "verify-full"}
    valid_ssl_args = {"require", "verify-ca", "verify-full", "true", "1"}

    has_valid_tls = False
    if sslmode_val and sslmode_val.lower() in valid_sslmodes:
        has_valid_tls = True
    elif is_asyncpg and ssl_val and ssl_val.lower() in valid_ssl_args:
        has_valid_tls = True

    if not has_valid_tls:
        raise ValueError(
            f"Production PostgreSQL connection ({field_name}) requires TLS encryption. "
            f"Configure 'sslmode=require' (or 'ssl=require' for asyncpg). Insecure or missing TLS is not permitted in production."
        )


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # Environment & Operational Mode
    ENVIRONMENT: Literal["development", "testing", "production"] = "development"
    APP_NAME: str = "Aarav Image Generation API"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False

    # Mode A (Local Monolith) vs Mode B (Hybrid Hosted)
    DEPLOYMENT_MODE: Literal["local", "hybrid"] = "local"

    # Database Settings
    # Mode A default: SQLite in WAL mode
    # Mode B: e.g. postgresql+asyncpg://postgres:pwd@db.supabase.co:5432/postgres
    DATABASE_URL: str = Field(
        default="sqlite+aiosqlite:///./aarav_dev.db",
        description="Async database connection string (SQLite or PostgreSQL)"
    )
    MIGRATION_DATABASE_URL: Optional[str] = Field(
        default=None,
        description="Direct or session database connection string for Alembic migrations (e.g. Supabase port 5432)"
    )
    DB_ECHO: bool = False
    DB_POOL_SIZE: int = 5
    DB_MAX_OVERFLOW: int = 10

    # Storage Settings
    STORAGE_BASE_PATH: Path = Field(
        default=Path("./storage/images"),
        description="Base path for local Content-Addressable Storage"
    )
    STORAGE_ADAPTER: Literal["local_cas", "remote_vault", "supabase"] = Field(
        default="local_cas",
        description="Active storage adapter: 'local_cas' for filesystem CAS, 'remote_vault' for private cloud bucket, 'supabase' for Supabase Storage"
    )
    SUPABASE_URL: str = Field(
        default="",
        description="Base URL for Supabase project (e.g., https://xyz.supabase.co)"
    )
    SUPABASE_STORAGE_BUCKET: str = Field(
        default="inference-assets",
        description="Target Supabase Storage bucket name"
    )
    SUPABASE_SERVICE_ROLE_KEY: SecretStr = Field(
        default=SecretStr(""),
        description="Supabase service role secret key for backend storage management"
    )
    STORAGE_TIMEOUT_SECONDS: float = Field(
        default=10.0,
        description="Timeout in seconds for external storage API operations"
    )
    ENABLE_PHYSICAL_ASSET_DELETION: bool = Field(
        default=False,
        description="Safety toggle for physical asset deletion operations"
    )


    # Security & API Key Settings
    # A dedicated salt ensures SHA-256 digests cannot be reversed using rainbow tables
    API_KEY_SALT: SecretStr = Field(
        default=SecretStr("aarav_default_development_salt_do_not_use_in_prod"),
        description="Cryptographic salt for hashing client API keys"
    )
    ADMIN_API_KEY: Optional[SecretStr] = Field(
        default=None,
        description="Optional master key for administrative endpoints"
    )
    WORKER_SHARED_SECRET: Optional[SecretStr] = Field(
        default=None,
        description="Shared secret for Mode B hybrid worker authentication"
    )
    WORKER_SHARED_SECRET_PREVIOUS: Optional[SecretStr] = Field(
        default=None,
        description="Previous shared secret allowing zero-downtime worker secret rotation"
    )

    # Hardware & Inference Bounds (4 GB VRAM Protection)
    LOCAL_GPU_CONCURRENCY: int = Field(
        default=1,
        description="Strict maximum of 1 concurrent generation on RTX 2050 Mobile"
    )
    MAX_LOCAL_WIDTH: int = 512
    MAX_LOCAL_HEIGHT: int = 512
    DEFAULT_STEPS: int = 4
    MAX_STEPS: int = 25
    LEASE_TIMEOUT_SECONDS: int = 120
    MAX_JOB_RETRIES: int = 3
    WORKER_POLL_INTERVAL_SECONDS: float = 0.5
    SWEEPER_INTERVAL_SECONDS: float = 10.0
    BASE_RETRY_BACKOFF_SECONDS: float = 2.0
    MAX_RETRY_BACKOFF_SECONDS: float = 60.0
    RUN_EMBEDDED_WORKER: bool = True
    RUN_LEASE_SWEEPER: bool = True
    RUN_DELETION_OUTBOX_WORKER: bool = True
    DELETION_OUTBOX_INTERVAL_SECONDS: float = Field(
        default=10.0,
        description="Polling interval in seconds for the background deletion outbox worker",
    )
    DELETION_OUTBOX_BATCH_SIZE: int = Field(
        default=10,
        description="Maximum deletion tasks to claim and process per outbox batch",
    )
    CONTROL_PLANE_URL: Optional[str] = Field(
        default=None,
        description="Target base URL of the FastAPI control plane for the hybrid worker (Mode B)"
    )

    # Observability & Metrics Settings
    METRICS_ENABLED: bool = Field(
        default=True,
        description="Toggle Prometheus /metrics exposition endpoint and metrics tracking"
    )
    METRICS_REQUIRE_AUTH: bool = Field(
        default=False,
        description="Require admin authentication (ADMIN_API_KEY) to access /metrics"
    )
    METRICS_CACHE_TTL_SECONDS: float = Field(
        default=5.0,
        description="Cache TTL in seconds for database-backed metrics sampling (queue/outbox depth)"
    )

    @model_validator(mode="after")
    def validate_production_configuration(self) -> "Settings":
        """
        Enforces strict production safety invariants:
        1. Insecure default secrets (API_KEY_SALT, WORKER_SHARED_SECRET) must be replaced.
        2. Database transport over PostgreSQL must enforce TLS encryption.
        3. External storage credentials must be populated if active.
        """
        if self.ENVIRONMENT == "production":
            # 1. API_KEY_SALT validation
            salt_val = self.API_KEY_SALT.get_secret_value() if self.API_KEY_SALT else ""
            if not salt_val or not salt_val.strip():
                raise ValueError("API_KEY_SALT must be configured and non-empty in production.")
            if salt_val == _DEV_SALT_SENTINEL:
                raise ValueError(
                    "Insecure default API_KEY_SALT cannot be used in production. "
                    "A strong, unique cryptographic salt must be configured."
                )
            if salt_val.lower().strip() in {"changeme", "default", "secret", "placeholder", "salt", "test"}:
                raise ValueError(
                    "Insecure placeholder API_KEY_SALT cannot be used in production. "
                    "A strong, unique cryptographic salt must be configured."
                )

            # 2. Worker shared secret validation for hybrid mode
            if self.DEPLOYMENT_MODE == "hybrid":
                if not self.WORKER_SHARED_SECRET or not self.WORKER_SHARED_SECRET.get_secret_value().strip():
                    raise ValueError("WORKER_SHARED_SECRET must be configured and non-empty in production hybrid mode.")
                worker_val = self.WORKER_SHARED_SECRET.get_secret_value().lower().strip()
                if worker_val in {"changeme", "secret", "placeholder", "test", "default"}:
                    raise ValueError("Insecure placeholder WORKER_SHARED_SECRET cannot be used in production.")
                if self.RUN_EMBEDDED_WORKER:
                    raise ValueError(
                        "RUN_EMBEDDED_WORKER must be false in production hybrid mode to prevent loading "
                        "GPU/PyTorch diffusion models on stateless cloud control plane instances."
                    )

            # 3. Supabase credentials validation when supabase adapter is active
            if self.STORAGE_ADAPTER == "supabase":
                if not self.SUPABASE_URL or not self.SUPABASE_URL.strip():
                    raise ValueError("SUPABASE_URL must be configured in production when using supabase storage adapter.")
                if not self.SUPABASE_URL.startswith("https://"):
                    raise ValueError("SUPABASE_URL must use HTTPS protocol in production.")
                if not self.SUPABASE_SERVICE_ROLE_KEY or not self.SUPABASE_SERVICE_ROLE_KEY.get_secret_value().strip():
                    raise ValueError(
                        "SUPABASE_SERVICE_ROLE_KEY must be configured and non-empty in production when using supabase storage adapter."
                    )
                srk_val = self.SUPABASE_SERVICE_ROLE_KEY.get_secret_value().lower().strip()
                if srk_val in {"changeme", "secret", "placeholder", "test", "default"}:
                    raise ValueError("Insecure placeholder SUPABASE_SERVICE_ROLE_KEY cannot be used in production.")

            # 4. Admin API key validation (if set)
            if self.ADMIN_API_KEY:
                admin_val = self.ADMIN_API_KEY.get_secret_value().lower().strip()
                if not admin_val or admin_val in {
                    "changeme", "admin", "secret", "placeholder", "test", "default", "aarav_admin_supersecretkey12345"
                }:
                    raise ValueError("Insecure or placeholder ADMIN_API_KEY cannot be used in production.")

            # 5. Database transport TLS validation
            _validate_postgres_tls(self.DATABASE_URL, "DATABASE_URL")
            if self.MIGRATION_DATABASE_URL:
                _validate_postgres_tls(self.MIGRATION_DATABASE_URL, "MIGRATION_DATABASE_URL")

            # 6. Metrics authorization validation in production
            if self.METRICS_REQUIRE_AUTH:
                if not self.ADMIN_API_KEY or not self.ADMIN_API_KEY.get_secret_value().strip():
                    raise ValueError(
                        "ADMIN_API_KEY must be configured and non-empty in production when METRICS_REQUIRE_AUTH is True."
                    )

        return self

    @property
    def is_sqlite(self) -> bool:
        """Determines if the active database is SQLite."""
        return "sqlite" in self.DATABASE_URL.lower()

    @property
    def is_postgresql(self) -> bool:
        """Determines if the active database is PostgreSQL."""
        return "postgres" in self.DATABASE_URL.lower()


# Singleton instance
settings = Settings()
