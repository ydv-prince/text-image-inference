"""
Application Configuration — Pydantic v2 Settings
Location: src/core/config.py
"""

from pathlib import Path
from typing import Literal, Optional
from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


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
    CONTROL_PLANE_URL: Optional[str] = Field(
        default=None,
        description="Target base URL of the FastAPI control plane for the hybrid worker (Mode B)"
    )

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
