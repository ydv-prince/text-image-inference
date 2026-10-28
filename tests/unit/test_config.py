"""
Unit Tests for Pydantic v2 Application Configuration & Production Hardening
Location: tests/unit/test_config.py
"""

from pathlib import Path
import pytest
from pydantic_core import ValidationError

from src.core.config import Settings


def test_default_settings(monkeypatch):
    """Verifies that default settings load with expected safety thresholds in development."""
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    s = Settings(STORAGE_BASE_PATH="./storage/images")
    assert s.APP_NAME == "Aarav Image Generation API"
    assert s.ENVIRONMENT == "development"
    assert s.LOCAL_GPU_CONCURRENCY == 1  # 4 GB VRAM protection
    assert s.MAX_LOCAL_WIDTH == 512
    assert s.MAX_LOCAL_HEIGHT == 512
    assert s.LEASE_TIMEOUT_SECONDS == 120
    assert s.STORAGE_BASE_PATH == Path("./storage/images")


def test_test_environment_settings():
    """Verifies that test environment loads cleanly without production restrictions."""
    s = Settings(ENVIRONMENT="testing")
    assert s.ENVIRONMENT == "testing"
    assert s.is_sqlite is True


def test_dialect_detection():
    """Verifies dialect helper properties for SQLite and PostgreSQL in development."""
    sqlite_settings = Settings(DATABASE_URL="sqlite+aiosqlite:///./test.db")
    assert sqlite_settings.is_sqlite is True
    assert sqlite_settings.is_postgresql is False

    pg_settings = Settings(DATABASE_URL="postgresql+asyncpg://user:pass@localhost:5432/db")
    assert pg_settings.is_sqlite is False
    assert pg_settings.is_postgresql is True


def test_custom_environment_override():
    """Verifies environment variables override default settings correctly with valid secrets."""
    custom = Settings(
        ENVIRONMENT="production",
        DEBUG=True,
        DEPLOYMENT_MODE="hybrid",
        LOCAL_GPU_CONCURRENCY=1,
        API_KEY_SALT="a_secure_production_salt_for_testing_12345",
        WORKER_SHARED_SECRET="a_secure_worker_secret_for_testing_12345",
    )
    assert custom.ENVIRONMENT == "production"
    assert custom.DEBUG is True
    assert custom.DEPLOYMENT_MODE == "hybrid"


def test_production_rejects_default_dev_salt():
    """Production mode must reject the default development API_KEY_SALT sentinel."""
    with pytest.raises(ValidationError) as exc_info:
        Settings(ENVIRONMENT="production")
    err_str = str(exc_info.value)
    assert "Insecure default API_KEY_SALT cannot be used in production" in err_str


@pytest.mark.parametrize("placeholder", ["", "   ", "changeme", "default", "secret", "placeholder", "salt", "test"])
def test_production_rejects_placeholder_salt(placeholder):
    """Production mode must reject empty, whitespace, or known placeholder salts."""
    with pytest.raises(ValidationError) as exc_info:
        Settings(ENVIRONMENT="production", API_KEY_SALT=placeholder)
    assert "API_KEY_SALT" in str(exc_info.value)


def test_production_rejects_missing_or_placeholder_worker_secret_in_hybrid_mode():
    """Production hybrid mode requires a strong, non-placeholder WORKER_SHARED_SECRET."""
    # Missing worker secret in hybrid mode
    with pytest.raises(ValidationError) as exc_info:
        Settings(
            ENVIRONMENT="production",
            DEPLOYMENT_MODE="hybrid",
            API_KEY_SALT="valid_strong_production_salt_999",
            WORKER_SHARED_SECRET=None,
        )
    assert "WORKER_SHARED_SECRET" in str(exc_info.value)

    # Placeholder worker secret in hybrid mode
    with pytest.raises(ValidationError) as exc_info:
        Settings(
            ENVIRONMENT="production",
            DEPLOYMENT_MODE="hybrid",
            API_KEY_SALT="valid_strong_production_salt_999",
            WORKER_SHARED_SECRET="changeme",
        )
    assert "Insecure placeholder WORKER_SHARED_SECRET" in str(exc_info.value)


def test_production_rejects_insecure_supabase_credentials():
    """Production mode requires valid HTTPS URL and non-placeholder service key when supabase storage is active."""
    # Insecure HTTP URL
    with pytest.raises(ValidationError) as exc_info:
        Settings(
            ENVIRONMENT="production",
            STORAGE_ADAPTER="supabase",
            API_KEY_SALT="valid_strong_production_salt_999",
            SUPABASE_URL="http://insecure-supabase.example.com",
            SUPABASE_SERVICE_ROLE_KEY="secret-key-12345",
        )
    assert "SUPABASE_URL must use HTTPS" in str(exc_info.value)

    # Placeholder service role key
    with pytest.raises(ValidationError) as exc_info:
        Settings(
            ENVIRONMENT="production",
            STORAGE_ADAPTER="supabase",
            API_KEY_SALT="valid_strong_production_salt_999",
            SUPABASE_URL="https://secure-supabase.example.com",
            SUPABASE_SERVICE_ROLE_KEY="secret",
        )
    assert "Insecure placeholder SUPABASE_SERVICE_ROLE_KEY" in str(exc_info.value)


def test_production_accepts_valid_secrets():
    """Production mode accepts valid strong secrets."""
    s = Settings(
        ENVIRONMENT="production",
        API_KEY_SALT="strong_production_salt_cryptographic_12345678",
        DEPLOYMENT_MODE="local",
    )
    assert s.ENVIRONMENT == "production"
    assert s.API_KEY_SALT.get_secret_value() == "strong_production_salt_cryptographic_12345678"


@pytest.mark.parametrize(
    "url",
    [
        "postgresql+asyncpg://user:pass@db.example.com:5432/proddb?ssl=require",
        "postgresql+asyncpg://user:pass@db.example.com:5432/proddb?sslmode=require",
        "postgresql+asyncpg://user:pass@db.example.com:5432/proddb?sslmode=verify-ca",
        "postgresql+asyncpg://user:pass@db.example.com:5432/proddb?sslmode=verify-full",
        "postgresql://user:pass@db.example.com:5432/proddb?sslmode=require",
        "postgresql://user:pass@db.example.com:5432/proddb?sslmode=verify-full",
    ],
)
def test_production_accepts_postgres_with_required_tls(url):
    """Production PostgreSQL connections with valid TLS configurations are accepted."""
    s = Settings(
        ENVIRONMENT="production",
        API_KEY_SALT="valid_strong_production_salt_999",
        DATABASE_URL=url,
    )
    assert s.is_postgresql is True


@pytest.mark.parametrize(
    "url",
    [
        "postgresql+asyncpg://user:pass@db.example.com:5432/proddb",
        "postgresql+asyncpg://user:pass@db.example.com:5432/proddb?ssl=disable",
        "postgresql+asyncpg://user:pass@db.example.com:5432/proddb?sslmode=prefer",
        "postgresql+asyncpg://user:pass@db.example.com:5432/proddb?sslmode=allow",
        "postgresql://user:pass@db.example.com:5432/proddb",
        "postgresql://user:pass@db.example.com:5432/proddb?sslmode=disable",
        "postgresql://user:pass@db.example.com:5432/proddb?sslmode=prefer",
    ],
)
def test_production_rejects_postgres_without_tls(url):
    """Production PostgreSQL connections missing TLS or using insecure/fallback modes are rejected."""
    with pytest.raises(ValidationError) as exc_info:
        Settings(
            ENVIRONMENT="production",
            API_KEY_SALT="valid_strong_production_salt_999",
            DATABASE_URL=url,
        )
    assert "requires TLS encryption" in str(exc_info.value)


def test_asyncpg_vs_sync_driver_tls_handling():
    """
    Verifies driver-aware TLS parsing:
    - asyncpg accepts both 'ssl=require' and 'sslmode=require'.
    - synchronous PostgreSQL requires 'sslmode=require' and rejects 'ssl=require'.
    """
    valid_salt = "valid_strong_production_salt_999"

    # asyncpg with ssl=require -> OK
    s_async = Settings(
        ENVIRONMENT="production",
        API_KEY_SALT=valid_salt,
        DATABASE_URL="postgresql+asyncpg://u:p@db.test:5432/db?ssl=require",
    )
    assert s_async.is_postgresql is True

    # sync postgresql with sslmode=require -> OK
    s_sync = Settings(
        ENVIRONMENT="production",
        API_KEY_SALT=valid_salt,
        DATABASE_URL="postgresql://u:p@db.test:5432/db?sslmode=require",
    )
    assert s_sync.is_postgresql is True

    # sync postgresql with only ssl=require -> FAILS (sync driver requires sslmode)
    with pytest.raises(ValidationError) as exc_info:
        Settings(
            ENVIRONMENT="production",
            API_KEY_SALT=valid_salt,
            DATABASE_URL="postgresql://u:p@db.test:5432/db?ssl=require",
        )
    assert "requires TLS encryption" in str(exc_info.value)


def test_migration_database_url_validated_independently():
    """An overridden MIGRATION_DATABASE_URL is validated for TLS independently in production."""
    valid_salt = "valid_strong_production_salt_999"
    secure_app_url = "postgresql+asyncpg://u:p@db.test:5432/db?ssl=require"

    # Insecure MIGRATION_DATABASE_URL must be rejected even if DATABASE_URL is secure
    with pytest.raises(ValidationError) as exc_info:
        Settings(
            ENVIRONMENT="production",
            API_KEY_SALT=valid_salt,
            DATABASE_URL=secure_app_url,
            MIGRATION_DATABASE_URL="postgresql://u:p@db.test:5432/db",  # Missing sslmode
        )
    assert "MIGRATION_DATABASE_URL" in str(exc_info.value)

    # Secure MIGRATION_DATABASE_URL is accepted
    s = Settings(
        ENVIRONMENT="production",
        API_KEY_SALT=valid_salt,
        DATABASE_URL=secure_app_url,
        MIGRATION_DATABASE_URL="postgresql://u:p@db.test:5432/db?sslmode=require",
    )
    assert s.MIGRATION_DATABASE_URL is not None


def test_validation_errors_do_not_expose_credentials():
    """Verifies that validation error messages do not leak passwords, credentials, or full URLs."""
    sensitive_pass = "super_confidential_db_password_XYZ999"
    sensitive_user = "confidential_admin_user"
    insecure_url = f"postgresql+asyncpg://{sensitive_user}:{sensitive_pass}@db.secret-corp.internal:5432/proddb"

    with pytest.raises(ValidationError) as exc_info:
        Settings(
            ENVIRONMENT="production",
            API_KEY_SALT="valid_strong_production_salt_999",
            DATABASE_URL=insecure_url,
        )

    err_str = str(exc_info.value)
    assert sensitive_pass not in err_str, "Database password leaked in validation error!"
    assert sensitive_user not in err_str, "Database username leaked in validation error!"
    assert "secret-corp.internal" not in err_str, "Database hostname leaked in validation error!"
    assert "requires TLS encryption" in err_str


def test_production_hybrid_rejects_embedded_worker():
    """Finding M3: Production hybrid deployment must reject RUN_EMBEDDED_WORKER=True."""
    with pytest.raises(ValidationError) as exc_info:
        Settings(
            ENVIRONMENT="production",
            DEPLOYMENT_MODE="hybrid",
            API_KEY_SALT="valid_strong_production_salt_999",
            WORKER_SHARED_SECRET="valid_strong_worker_secret_12345678",
            RUN_EMBEDDED_WORKER=True,
        )
    err = str(exc_info.value)
    assert "RUN_EMBEDDED_WORKER must be false in production hybrid mode" in err


def test_production_hybrid_accepts_embedded_worker_false():
    """Finding M3: Production hybrid deployment accepts RUN_EMBEDDED_WORKER=False."""
    s = Settings(
        ENVIRONMENT="production",
        DEPLOYMENT_MODE="hybrid",
        API_KEY_SALT="valid_strong_production_salt_999",
        WORKER_SHARED_SECRET="valid_strong_worker_secret_12345678",
        RUN_EMBEDDED_WORKER=False,
    )
    assert s.RUN_EMBEDDED_WORKER is False
    assert s.DEPLOYMENT_MODE == "hybrid"


def test_production_local_accepts_embedded_worker_true():
    """Finding M3: Production local monolith (Mode A) allows embedded worker."""
    s = Settings(
        ENVIRONMENT="production",
        DEPLOYMENT_MODE="local",
        API_KEY_SALT="valid_strong_production_salt_999",
        RUN_EMBEDDED_WORKER=True,
    )
    assert s.RUN_EMBEDDED_WORKER is True
    assert s.DEPLOYMENT_MODE == "local"


def test_development_accepts_embedded_worker_true_in_hybrid():
    """Finding M3: Development environment allows embedded worker even in hybrid mode."""
    s = Settings(
        ENVIRONMENT="development",
        DEPLOYMENT_MODE="hybrid",
        RUN_EMBEDDED_WORKER=True,
    )
    assert s.RUN_EMBEDDED_WORKER is True


def test_production_rejects_example_admin_api_key():
    """Finding L2: Production validation rejects the example ADMIN_API_KEY from .env.example."""
    with pytest.raises(ValidationError) as exc_info:
        Settings(
            ENVIRONMENT="production",
            API_KEY_SALT="valid_strong_production_salt_999",
            DEPLOYMENT_MODE="local",
            ADMIN_API_KEY="aarav_admin_supersecretkey12345",
        )
    err = str(exc_info.value)
    assert "Insecure or placeholder ADMIN_API_KEY" in err


def test_production_rejects_missing_admin_key_when_metrics_auth_required():
    """Verifies that production configuration rejects missing ADMIN_API_KEY when METRICS_REQUIRE_AUTH is True."""
    with pytest.raises(ValidationError) as exc_info:
        Settings(
            ENVIRONMENT="production",
            API_KEY_SALT="valid_strong_production_salt_999",
            DEPLOYMENT_MODE="local",
            METRICS_REQUIRE_AUTH=True,
            ADMIN_API_KEY=None,
        )
    err = str(exc_info.value)
    assert "ADMIN_API_KEY must be configured and non-empty in production when METRICS_REQUIRE_AUTH is True" in err


def test_production_rejects_empty_admin_key_when_metrics_auth_required():
    """Verifies that production configuration rejects empty/whitespace ADMIN_API_KEY when METRICS_REQUIRE_AUTH is True."""
    with pytest.raises(ValidationError) as exc_info:
        Settings(
            ENVIRONMENT="production",
            API_KEY_SALT="valid_strong_production_salt_999",
            DEPLOYMENT_MODE="local",
            METRICS_REQUIRE_AUTH=True,
            ADMIN_API_KEY="   ",
        )
    err = str(exc_info.value)
    assert "Insecure or placeholder ADMIN_API_KEY cannot be used in production" in err


def test_production_accepts_valid_admin_key_when_metrics_auth_required():
    """Verifies that production configuration accepts a strong ADMIN_API_KEY when METRICS_REQUIRE_AUTH is True."""
    s = Settings(
        ENVIRONMENT="production",
        API_KEY_SALT="valid_strong_production_salt_999",
        DEPLOYMENT_MODE="local",
        METRICS_REQUIRE_AUTH=True,
        ADMIN_API_KEY="strong_prod_admin_key_abc123xyz789",
    )
    assert s.ADMIN_API_KEY is not None
    assert s.ADMIN_API_KEY.get_secret_value() == "strong_prod_admin_key_abc123xyz789"
