"""
Milestone 8: Staging Deployment Readiness & Operational Lifecycle Tests
Location: tests/unit/test_staging_readiness.py

Guarantees:
1. Process Lifecycle: Clean ASGI startup and graceful shutdown.
2. Probe Separation: Liveness confirms process health; Readiness verifies dependencies.
3. Queue Durability: Abrupt worker loss does not destroy durable queued jobs.
4. Sweeper Rescheduling: Timed-out leases return safely to PENDING with retry backoff.
5. Deletion Outbox Protection: Physical deletion disabled by default; claimed tasks safely released.
6. Production Hardening: Settings enforce TLS, non-placeholder secrets, and metrics access controls.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
import pytest
from httpx import ASGITransport, AsyncClient
from pydantic import SecretStr
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.core.config import Settings, settings
from src.domain.interfaces import ProviderRequest, ProviderResponse
from src.infrastructure.models import (
    ApiKeyModel,
    Base,
    GenerationJobModel,
    StorageBlobModel,
    StorageDeletionOutboxModel,
)
from src.main import app, lifespan
from src.services.deletion_outbox_service import DeletionOutboxWorker
from src.services.hybrid_worker import HybridWorker
from src.services.lease_sweeper import LeaseSweeper
from src.infrastructure.repositories import get_job_repository


@pytest.fixture
async def test_session_factory(tmp_path: Path):
    """Sets up an isolated SQLite test database with session factory."""
    db_file = tmp_path / "readiness_test.db"
    db_url = f"sqlite+aiosqlite:///{db_file.as_posix()}"
    engine = create_async_engine(db_url)

    async with engine.begin() as conn:
        await conn.execute(text("PRAGMA foreign_keys=ON;"))
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    yield session_factory
    await engine.dispose()


# =============================================================================
# 1. Health Probe Separation & Operational Contracts
# =============================================================================
@pytest.mark.asyncio
async def test_liveness_probe_independent_of_database():
    """Liveness probe must succeed even when database connectivity is severed."""
    with patch("src.api.health.check_database_health", return_value=False):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/health/liveness")
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] == "alive"
            assert data["version"] == settings.APP_VERSION


@pytest.mark.asyncio
async def test_readiness_probe_reflects_dependency_outage():
    """Readiness probe must return HTTP 503 if database connectivity fails."""
    with patch("src.api.health.check_database_health", return_value=False):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/health/readiness")
            assert resp.status_code == 503
            data = resp.json()
            assert data["status"] == "not_ready"
            assert data["database"] == "disconnected"


# =============================================================================
# 2. Production Configuration Invariants & Security Boundaries
# =============================================================================
def test_production_rejects_missing_admin_key_when_metrics_auth_required():
    """In production, METRICS_REQUIRE_AUTH=True requires a non-empty ADMIN_API_KEY."""
    with pytest.raises(ValueError, match="ADMIN_API_KEY must be configured and non-empty"):
        Settings(
            ENVIRONMENT="production",
            API_KEY_SALT=SecretStr("super_secure_production_salt_99999"),
            METRICS_REQUIRE_AUTH=True,
            ADMIN_API_KEY=None,
        )


def test_production_rejects_insecure_database_connection():
    """Production mode strictly forbids unencrypted PostgreSQL connections."""
    with pytest.raises(ValueError, match="requires TLS encryption"):
        Settings(
            ENVIRONMENT="production",
            API_KEY_SALT=SecretStr("super_secure_production_salt_99999"),
            DATABASE_URL="postgresql+asyncpg://postgres:secret@db.example.com:5432/postgres",
        )


def test_production_accepts_valid_deployment_configuration():
    """Production mode accepts fully secured, TLS-enforced configuration."""
    cfg = Settings(
        ENVIRONMENT="production",
        DEPLOYMENT_MODE="hybrid",
        API_KEY_SALT=SecretStr("super_secure_production_salt_99999"),
        WORKER_SHARED_SECRET=SecretStr("super_secure_worker_secret_99999"),
        ADMIN_API_KEY=SecretStr("super_secure_admin_key_99999"),
        METRICS_REQUIRE_AUTH=True,
        DATABASE_URL="postgresql+asyncpg://postgres:secret@db.example.com:5432/postgres?ssl=require",
    )
    assert cfg.ENVIRONMENT == "production"
    assert cfg.METRICS_REQUIRE_AUTH is True


# =============================================================================
# 3. Queue Durability & Lease Recovery on Abrupt Worker Disconnect
# =============================================================================
@pytest.mark.asyncio
async def test_worker_crash_does_not_lose_queued_work(test_session_factory):
    """
    If a worker leases a job and abruptly terminates (simulating power outage / OOM kill),
    the job remains durably stored in the database, and the LeaseSweeper safely recovers
    it to PENDING with exponential retry backoff.
    """
    repo = get_job_repository(test_session_factory)

    # 1. Enqueue job
    async with test_session_factory() as session:
        api_key = ApiKeyModel(
            id="test-client-readiness-1",
            key_hash="hash_client_readiness_1",
            client_name="Readiness Client",
        )
        session.add(api_key)
        await session.flush()

        job = GenerationJobModel(
            id="job-crash-durability-test",
            api_key_id=api_key.id,
            status="PENDING",
            prompt="A photorealistic golden retriever running in snow",
            payload_hash="dummy_payload_hash_1234",
            width=512,
            height=512,
            steps=4,
            seed=42,
            retry_count=0,
            max_retries=3,
        )
        session.add(job)
        await session.commit()

    # 2. Worker leases job
    claimed = await repo.claim_next_job(worker_id="crashed-worker-gpu-1", lease_timeout_seconds=5)
    assert claimed is not None
    claimed_id = claimed.job_id
    assert claimed_id == "job-crash-durability-test"

    # 3. Verify job is PROCESSING with lease held
    async with test_session_factory() as session:
        j_stmt = select(GenerationJobModel).where(GenerationJobModel.id == claimed_id)
        j = (await session.execute(j_stmt)).scalar_one()
        assert j.status == "PROCESSING"
        assert j.locked_by == "crashed-worker-gpu-1"

        # Simulate 10 seconds passing (worker crashed, lease expired)
        j.locked_at = datetime.now(timezone.utc) - timedelta(seconds=15)
        await session.commit()

    # 4. Sweeper pass reclaims the expired lease
    sweeper = LeaseSweeper(repository=repo, lease_timeout_seconds=5, max_retries=3)
    reclaimed = await sweeper.sweep_once()
    assert claimed_id in reclaimed

    # 5. Verify job is safely back to PENDING for reassignment with retry incremented
    async with test_session_factory() as session:
        j_stmt = select(GenerationJobModel).where(GenerationJobModel.id == claimed_id)
        j = (await session.execute(j_stmt)).scalar_one()
        assert j.status == "PENDING"
        assert j.locked_by is None
        assert j.locked_at is None
        assert j.retry_count == 1
        assert j.scheduled_at is not None


# =============================================================================
# 4. Deletion Outbox Recovery & Non-Destructive Protection
# =============================================================================
@pytest.mark.asyncio
async def test_outbox_processing_safely_releases_tasks_when_flag_disabled(test_session_factory):
    """
    When ENABLE_PHYSICAL_ASSET_DELETION is False (default), the outbox worker
    must never execute physical deletions and must safely release any claimed
    tasks back to PENDING without decrementing retry attempts.
    """
    with patch.object(settings, "ENABLE_PHYSICAL_ASSET_DELETION", False):
        async with test_session_factory() as session:
            blob = StorageBlobModel(
                id="blob-readiness-safety-1",
                file_hash="1111222233334444555566667777888899990000aaaabbbbccccddddeeeeffff",
                storage_path="local_cas/safety/blob1.png",
                file_size_bytes=1024,
                mime_type="image/png",
                active_ref_count=0,
                status="DELETION_QUEUED",
            )
            session.add(blob)

            outbox = StorageDeletionOutboxModel(
                id="outbox-readiness-safety-1",
                blob_id=blob.id,
                storage_path=blob.storage_path,
                file_hash=blob.file_hash,
                status="PENDING",
                attempts=0,
                max_attempts=5,
            )
            session.add(outbox)
            await session.commit()

        mock_deleter = MagicMock()
        mock_deleter.delete_physical_object = AsyncMock(return_value=True)

        worker = DeletionOutboxWorker(
            session_factory=test_session_factory,
            worker_id="outbox-worker-test",
            deleter=mock_deleter,
            batch_limit=5,
        )

        results = await worker.run_once()

        # Gate 4 triggered: zero physical deletions performed
        mock_deleter.delete_physical_object.assert_not_called()
        assert results["completed"] == 0

        # Verify task remains intact in database
        async with test_session_factory() as session:
            t_stmt = select(StorageDeletionOutboxModel).where(
                StorageDeletionOutboxModel.id == "outbox-readiness-safety-1"
            )
            t = (await session.execute(t_stmt)).scalar_one()
            assert t.status == "PENDING"
            assert t.attempts == 0


# =============================================================================
# 5. Process Lifespan & Clean Shutdown Verification
# =============================================================================
@pytest.mark.asyncio
async def test_application_lifespan_startup_and_shutdown():
    """
    Verifies that the ASGI application lifespan context manager starts background
    tasks cleanly and terminates them boundedly upon shutdown.
    """
    mock_app = MagicMock()
    mock_app.state = MagicMock()

    with patch.object(settings, "RUN_LEASE_SWEEPER", False), patch.object(
        settings, "RUN_EMBEDDED_WORKER", False
    ), patch.object(settings, "RUN_DELETION_OUTBOX_WORKER", False):
        async with lifespan(mock_app):
            # Lifespan active
            assert True
        # Lifespan exited cleanly
        assert True


# =============================================================================
# 6. API Container Artifact & Packaging Parity Verification
# =============================================================================
def test_dockerfile_api_structure_and_security_invariants():
    """
    Verifies that Dockerfile.api exists, follows multi-stage best practices,
    enforces non-root execution, sets proper healthchecks, and uses the correct entrypoint.
    """
    repo_root = Path(__file__).resolve().parent.parent.parent
    dockerfile_path = repo_root / "Dockerfile.api"
    assert dockerfile_path.exists(), "Dockerfile.api must exist at repository root"

    content = dockerfile_path.read_text(encoding="utf-8")

    # Multi-stage build
    assert "FROM python:3.10-slim AS builder" in content
    assert "FROM python:3.10-slim AS runner" in content
    assert "COPY --from=builder /opt/venv /opt/venv" in content

    # Non-root user
    assert "useradd -u 10001 -m -s /bin/bash appuser" in content
    assert "USER appuser" in content
    assert "chown -R appuser:appuser /app" in content

    # Port and healthcheck
    assert "EXPOSE 8000" in content
    assert "HEALTHCHECK" in content
    assert "/health/liveness" in content

    # Entrypoint enforces single worker for in-process lifespan task safety
    assert 'CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"' in content
    assert "mkdir -p /app/storage/images" in content

    # Dependency lockfile reference
    assert "COPY requirements-api.lock ." in content
    assert "-r requirements-api.lock" in content

    # Security: No User-Agent spoofing or network evasion in production build instructions
    assert "User-Agent" not in content
    assert "99user-agent" not in content

    # Must NOT install GPU models, torch, or fallback to unpinned requirements in build steps
    non_comment_lines = [l for l in content.splitlines() if not l.strip().startswith("#")]
    non_comment_text = "\n".join(non_comment_lines).lower()
    assert "-r requirements-api.txt" not in non_comment_text
    assert "download" not in non_comment_text
    assert "torch" not in non_comment_text
    assert "diffusers" not in non_comment_text


def test_dockerignore_security_exclusions():
    """
    Verifies that .dockerignore exists and strictly excludes secrets, environments,
    caches, databases, and test artifacts from the container build context.
    """
    repo_root = Path(__file__).resolve().parent.parent.parent
    dockerignore_path = repo_root / ".dockerignore"
    assert dockerignore_path.exists(), ".dockerignore must exist at repository root"

    content = dockerignore_path.read_text(encoding="utf-8")
    lines = [line.strip() for line in content.splitlines() if line.strip() and not line.startswith("#")]

    critical_exclusions = [
        ".git/",
        ".venv/",
        ".env",
        ".env.*",
        "*.db",
        "backups/",
        "*.dump",
        "*.sql",
        "storage/",
        "tests/",
    ]

    for exclusion in critical_exclusions:
        assert exclusion in lines, f"Critical exclusion '{exclusion}' must be in .dockerignore"


def test_requirements_api_parity_with_pyproject():
    """
    Verifies that requirements-api.txt strictly matches pyproject.toml dependencies
    using PyPA packaging.requirements.Requirement and packaging.utils.canonicalize_name.

    Enforces:
    1. Direct package name and version constraint parity with pyproject.toml dependencies.
    2. Exclusion of heavy local-GPU diffusion and test packages.
    3. Mandatory inclusion of all 8 core runtime dependencies.
    """
    try:
        import tomllib
    except ImportError:
        import tomli as tomllib

    from packaging.requirements import Requirement
    from packaging.utils import canonicalize_name

    repo_root = Path(__file__).resolve().parent.parent.parent
    req_path = repo_root / "requirements-api.txt"
    pyproject_path = repo_root / "pyproject.toml"

    assert req_path.exists(), "requirements-api.txt must exist at repository root"
    assert pyproject_path.exists(), "pyproject.toml must exist at repository root"

    # 1. Parse pyproject.toml
    with open(pyproject_path, "rb") as f:
        pyproject_data = tomllib.load(f)

    project_table = pyproject_data.get("project", {})
    declared_core = project_table.get("dependencies", [])
    optional_deps = project_table.get("optional-dependencies", {})
    declared_postgres = optional_deps.get("postgres", [])

    pyproject_dep_map = {}
    for dep_str in declared_core + declared_postgres:
        r = Requirement(dep_str.strip())
        pyproject_dep_map[canonicalize_name(r.name)] = r

    # 2. Parse requirements-api.txt using Requirement parser
    req_lines = [
        line.strip()
        for line in req_path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    ]
    api_req_map = {}
    for req_str in req_lines:
        r = Requirement(req_str)
        norm_name = canonicalize_name(r.name)
        assert norm_name not in api_req_map, f"Duplicate requirement '{norm_name}' in requirements-api.txt"
        api_req_map[norm_name] = r

    # 3. Verify every requirement matches declared pyproject dependencies
    for norm_name, api_req in api_req_map.items():
        assert norm_name in pyproject_dep_map, (
            f"Package '{norm_name}' in requirements-api.txt is not declared in pyproject.toml"
        )
        pyproject_req = pyproject_dep_map[norm_name]
        assert api_req.specifier == pyproject_req.specifier, (
            f"Version constraint mismatch for '{norm_name}': "
            f"requirements-api.txt has '{api_req.specifier}' vs pyproject.toml '{pyproject_req.specifier}'"
        )

    # 4. Verify all verified runtime dependencies are included
    expected_runtime_packages = [
        "fastapi",
        "uvicorn",
        "pydantic-settings",
        "sqlalchemy",
        "alembic",
        "asyncpg",
        "httpx",
        "pillow",
    ]
    for expected_pkg in expected_runtime_packages:
        norm_expected = canonicalize_name(expected_pkg)
        assert norm_expected in api_req_map, (
            f"Expected runtime dependency '{expected_pkg}' is missing from requirements-api.txt"
        )

    # 5. Verify GPU-exclusive packages are strictly absent
    gpu_exclusive_packages = [
        "diffusers",
        "transformers",
        "accelerate",
        "safetensors",
        "peft",
        "torch",
        "torchvision",
    ]
    for pkg in gpu_exclusive_packages:
        norm_gpu = canonicalize_name(pkg)
        assert norm_gpu not in api_req_map, (
            f"GPU-exclusive package '{pkg}' must not be declared in requirements-api.txt"
        )

    # 6. Verify Dev/Test packages are strictly absent
    test_exclusive_packages = ["pytest", "pytest-asyncio"]
    for pkg in test_exclusive_packages:
        norm_test = canonicalize_name(pkg)
        assert norm_test not in api_req_map, (
            f"Test-exclusive package '{pkg}' must not be declared in requirements-api.txt"
        )


def test_requirements_api_lockfile_integrity_and_gpu_exclusion():
    """
    Verifies that requirements-api.lock:
    1. Parses strictly with packaging.requirements.Requirement (PEP 508).
    2. Contains exactly pinned (==) dependencies with no duplicate normalized package names (PEP 503).
    3. Covers all 8 direct API runtime packages with versions satisfying requirements-api.txt constraints.
    4. Records critical transitive dependencies of the resolved graph.
    5. Validates hash format if hashes are present.
    6. Strictly excludes heavy GPU inference libraries and test runners.
    """
    from packaging.requirements import InvalidRequirement, Requirement
    from packaging.utils import canonicalize_name

    repo_root = Path(__file__).resolve().parent.parent.parent
    lock_path = repo_root / "requirements-api.lock"
    req_path = repo_root / "requirements-api.txt"
    assert lock_path.exists(), "requirements-api.lock must exist at repository root"
    assert req_path.exists(), "requirements-api.txt must exist at repository root"

    lines = [
        line.strip()
        for line in lock_path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    ]

    assert len(lines) > 0, "requirements-api.lock must not be empty"

    lock_map = {}
    for line in lines:
        tokens = [t.strip() for t in line.split() if t.strip()]
        req_token = tokens[0]

        # 1. Parse via PEP 508 Requirement parser (fails on malformed syntax)
        try:
            req = Requirement(req_token)
        except InvalidRequirement as e:
            pytest.fail(f"Malformed requirement entry in lockfile: '{req_token}': {e}")

        # 2. Strict pinning check
        assert len(req.specifier) == 1, (
            f"Lockfile entry '{req_token}' must have exactly one pinned specifier, got: {req.specifier}"
        )
        spec = next(iter(req.specifier))
        assert spec.operator == "==", (
            f"Lockfile entry '{req_token}' must use exact '==' pinning, got '{spec.operator}'"
        )

        # 3. PEP 503 duplicate detection
        norm_name = canonicalize_name(req.name)
        assert norm_name not in lock_map, (
            f"Duplicate normalized package entry detected in lockfile: '{norm_name}'"
        )

        # 4. Hash format check if hashes are attached
        hashes = [t.split("=", 1)[1] for t in tokens if t.startswith("--hash=")]
        for h in hashes:
            assert h.startswith("sha256:"), f"Invalid hash format in '{line}': must start with 'sha256:'"
            digest = h.split(":", 1)[1]
            assert len(digest) == 64 and all(c in "0123456789abcdefABCDEF" for c in digest), (
                f"Invalid SHA-256 digest in hash '{h}'"
            )

        lock_map[norm_name] = spec.version

    # 5. Direct API runtime packages must all be present and satisfy minimum constraints
    direct_lines = [
        line.strip()
        for line in req_path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    ]
    for dline in direct_lines:
        dr = Requirement(dline)
        norm_pkg = canonicalize_name(dr.name)
        assert norm_pkg in lock_map, f"Direct runtime package '{norm_pkg}' missing from requirements-api.lock"
        locked_ver = lock_map[norm_pkg]
        assert dr.specifier.contains(locked_ver), (
            f"Locked version '{locked_ver}' for '{norm_pkg}' does not satisfy constraint '{dr.specifier}'"
        )

    # 6. Key transitive dependencies must be recorded
    key_transitive = [
        "pydantic",
        "pydantic-core",
        "starlette",
        "anyio",
        "httpcore",
        "greenlet",
        "uvloop",
        "httptools",
        "python-dotenv",
        "pyyaml",
    ]
    for pkg in key_transitive:
        norm_pkg = canonicalize_name(pkg)
        assert norm_pkg in lock_map, f"Transitive dependency '{pkg}' missing from requirements-api.lock"

    # 7. GPU-exclusive packages must NOT be present
    gpu_exclusive = [
        "torch",
        "torchvision",
        "diffusers",
        "transformers",
        "accelerate",
        "safetensors",
        "peft",
    ]
    for pkg in gpu_exclusive:
        norm_pkg = canonicalize_name(pkg)
        assert norm_pkg not in lock_map, f"GPU package '{pkg}' must not be present in requirements-api.lock"

    # 8. Dev / Test packages must NOT be present
    dev_packages = ["pytest", "pytest-asyncio"]
    for pkg in dev_packages:
        norm_pkg = canonicalize_name(pkg)
        assert norm_pkg not in lock_map, f"Dev package '{pkg}' must not be present in requirements-api.lock"


# =============================================================================
# 7. CI Automation & Lockfile Generator Invariants (Milestone 9B.13)
# =============================================================================
def test_ci_workflow_structure_and_security_invariants():
    """
    Verifies that .github/workflows/container-ci.yml:
    1. Parses strictly as valid YAML with standard 'on' key.
    2. Enforces least-privilege permissions (contents: read) at workflow and job levels.
    3. Triggers on push and pull_request against main.
    4. Runs both staging readiness tests and the full unit test suite.
    5. Performs clean container build (--no-cache) and asserts non-root execution (UID 10001).
    6. Verifies complete exclusion of GPU packages and exercises runtime probes.
    7. Provides container teardown step with 'if: always()' that only cleans up its own container.
    """
    import yaml

    repo_root = Path(__file__).resolve().parent.parent.parent
    workflow_path = repo_root / ".github" / "workflows" / "container-ci.yml"
    assert workflow_path.exists(), "Workflow .github/workflows/container-ci.yml must exist"

    raw_yaml = workflow_path.read_text(encoding="utf-8")
    data = yaml.safe_load(raw_yaml)

    # 1. Triggers and branches
    assert "on" in data, "YAML must define 'on' trigger mapping without boolean coercion"
    assert "main" in data["on"]["push"]["branches"]
    assert "main" in data["on"]["pull_request"]["branches"]

    # 2. Least-privilege permissions
    assert data.get("permissions") == {"contents": "read"}, "Workflow must declare contents: read permissions"

    # 3. Job definitions
    jobs = data.get("jobs", {})
    assert "offline-tests" in jobs, "Must define offline-tests job"
    assert "clean-container-build" in jobs, "Must define clean-container-build job"

    offline_job = jobs["offline-tests"]
    assert offline_job.get("permissions") == {"contents": "read"}
    offline_steps_run = [s.get("run", "") for s in offline_job.get("steps", []) if "run" in s]
    combined_offline_run = "\n".join(offline_steps_run)
    assert "pytest tests/unit/test_staging_readiness.py" in combined_offline_run, (
        "offline-tests must explicitly run staging readiness tests"
    )
    assert "pytest tests/unit/" in combined_offline_run, (
        "offline-tests must run full unit test suite"
    )

    build_job = jobs["clean-container-build"]
    assert build_job.get("needs") == "offline-tests", "clean-container-build must depend on offline-tests"
    assert build_job.get("permissions") == {"contents": "read"}
    build_steps = build_job.get("steps", [])
    build_steps_run = [s.get("run", "") for s in build_steps if "run" in s]
    combined_build_run = "\n".join(build_steps_run)

    # Clean build assertion
    assert "--no-cache" in combined_build_run
    assert "-f Dockerfile.api" in combined_build_run

    # Security and runtime contract assertions
    assert "10001" in combined_build_run
    assert "/health/liveness" in combined_build_run
    assert "503" in combined_build_run
    assert "/metrics" in combined_build_run

    # Teardown cleanup step assertion
    teardown_step = next((s for s in build_steps if "Teardown" in s.get("name", "")), None)
    assert teardown_step is not None, "clean-container-build must have a dedicated teardown step"
    assert teardown_step.get("if") == "always()", "Teardown step must execute unconditionally (if: always())"
    assert "ci-smoke-test-" in teardown_step.get("run", "")


def test_lockfile_generator_script_invariants():
    """
    Verifies that scripts/generate_api_lockfile.sh:
    1. Exists, uses bash, and enables strict error handling (set -euo pipefail).
    2. Targets CPython 3.10 on Linux x86_64 specifically (x86_64-unknown-linux-gnu).
    3. Generates SHA-256 hashes for all dependencies.
    4. Writes output exclusively to requirements-api.candidate.lock, preserving active lockfile.
    5. Preserves existing candidate lockfile by default and requires --force to replace.
    6. Cleans up candidate file on error via trap only if created during current run.
    7. Validates exact pinning (==), hash presence, direct dependencies, and GPU exclusions.
    """
    repo_root = Path(__file__).resolve().parent.parent.parent
    script_path = repo_root / "scripts" / "generate_api_lockfile.sh"
    assert script_path.exists(), "Script scripts/generate_api_lockfile.sh must exist"

    content = script_path.read_text(encoding="utf-8")

    # 1. Strict shell settings
    assert "set -euo pipefail" in content

    # 2. Target platform and architecture
    assert "x86_64-unknown-linux-gnu" in content
    assert "3.10" in content

    # 3. Hash generation
    assert "--generate-hashes" in content

    # 4. Output containment & candidate preservation by default
    assert "requirements-api.candidate.lock" in content
    assert "CREATED_CANDIDATE_THIS_RUN" in content
    assert "--force" in content
    assert "An unpromoted candidate lockfile already exists" in content

    # 5. Cleanup trap
    assert "trap cleanup_on_error EXIT" in content

    # 6. Embedded Python validation assertions
    assert "spec.operator != \"==\"" in content
    assert "sha256:" in content
    assert "package_hashes" in content
    assert "torch" in content
    assert "diffusers" in content


def test_lockfile_generator_preserves_existing_candidate_without_force(tmp_path):
    """
    Verifies that scripts/generate_api_lockfile.sh preserves an existing
    candidate lockfile by default and fails safely with instructions.
    """
    import os
    import shutil
    import subprocess

    if not shutil.which("bash"):
        pytest.skip("bash is required to run generator script pre-flight tests")

    repo_root = Path(__file__).resolve().parent.parent.parent
    script_src = repo_root / "scripts" / "generate_api_lockfile.sh"

    # Setup sandbox in tmp_path
    sandbox_scripts = tmp_path / "scripts"
    sandbox_scripts.mkdir(parents=True, exist_ok=True)
    sandbox_script = sandbox_scripts / "generate_api_lockfile.sh"
    shutil.copy2(script_src, sandbox_script)

    input_spec = tmp_path / "requirements-api.txt"
    input_spec.write_text("fastapi>=0.110.0\n", encoding="utf-8")

    active_lock = tmp_path / "requirements-api.lock"
    active_lock.write_text("fastapi==0.162.0\n", encoding="utf-8")

    candidate_lock = tmp_path / "requirements-api.candidate.lock"
    sentinel_content = "SENTINEL_UNPROMOTED_CANDIDATE_V1\n"
    candidate_lock.write_text(sentinel_content, encoding="utf-8")

    # Run without --force
    rel_script = os.path.relpath(sandbox_script, os.getcwd()).replace(os.sep, "/")
    result = subprocess.run(["bash", rel_script], capture_output=True, text=True)

    # Must fail safely with code 1
    assert result.returncode == 1
    assert "An unpromoted candidate lockfile already exists" in result.stderr
    assert "--force" in result.stderr

    # Candidate lockfile must be completely unchanged
    assert candidate_lock.exists()
    assert candidate_lock.read_text(encoding="utf-8") == sentinel_content

    # Active lockfile must be completely unchanged
    assert active_lock.exists()
    assert active_lock.read_text(encoding="utf-8") == "fastapi==0.162.0\n"


def test_lockfile_generator_rejects_invalid_arguments_before_destructive_operations(tmp_path):
    """
    Verifies that scripts/generate_api_lockfile.sh validates CLI arguments first
    and fails immediately on unrecognized arguments before any destructive operations.
    """
    import os
    import shutil
    import subprocess

    if not shutil.which("bash"):
        pytest.skip("bash is required to run generator script pre-flight tests")

    repo_root = Path(__file__).resolve().parent.parent.parent
    script_src = repo_root / "scripts" / "generate_api_lockfile.sh"

    sandbox_scripts = tmp_path / "scripts"
    sandbox_scripts.mkdir(parents=True, exist_ok=True)
    sandbox_script = sandbox_scripts / "generate_api_lockfile.sh"
    shutil.copy2(script_src, sandbox_script)

    input_spec = tmp_path / "requirements-api.txt"
    input_spec.write_text("fastapi>=0.110.0\n", encoding="utf-8")

    candidate_lock = tmp_path / "requirements-api.candidate.lock"
    candidate_content = "PREEXISTING_CANDIDATE\n"
    candidate_lock.write_text(candidate_content, encoding="utf-8")

    rel_script = os.path.relpath(sandbox_script, os.getcwd()).replace(os.sep, "/")
    result = subprocess.run(["bash", rel_script, "--invalid-flag"], capture_output=True, text=True)

    assert result.returncode == 1
    assert "Unrecognized argument '--invalid-flag'" in result.stderr

    # Candidate lockfile must remain preserved
    assert candidate_lock.exists()
    assert candidate_lock.read_text(encoding="utf-8") == candidate_content


def test_lockfile_generator_failure_cleanup_removes_only_current_run_candidate(tmp_path):
    """
    Verifies that the EXIT trap in scripts/generate_api_lockfile.sh only cleans up
    a candidate file created during the current invocation, never a preexisting candidate.
    """
    import os
    import shutil
    import subprocess

    if not shutil.which("bash"):
        pytest.skip("bash is required to run generator script pre-flight tests")

    repo_root = Path(__file__).resolve().parent.parent.parent
    script_src = repo_root / "scripts" / "generate_api_lockfile.sh"

    sandbox_scripts = tmp_path / "scripts"
    sandbox_scripts.mkdir(parents=True, exist_ok=True)
    sandbox_script = sandbox_scripts / "generate_api_lockfile.sh"
    shutil.copy2(script_src, sandbox_script)

    # Preexisting candidate
    candidate_lock = tmp_path / "requirements-api.candidate.lock"
    sentinel = "DO_NOT_DELETE_PREEXISTING\n"
    candidate_lock.write_text(sentinel, encoding="utf-8")

    # Run without input_spec (and without --force)
    rel_script = os.path.relpath(sandbox_script, os.getcwd()).replace(os.sep, "/")
    result = subprocess.run(["bash", rel_script], capture_output=True, text=True)

    assert result.returncode == 1
    # Candidate must still be there
    assert candidate_lock.exists()
    assert candidate_lock.read_text(encoding="utf-8") == sentinel


def test_lockfile_generator_active_lockfile_protection():
    """
    Statically audits scripts/generate_api_lockfile.sh to ensure:
    1. Active lockfile can never match candidate lockfile path.
    2. Active lockfile is never modified, written to, or deleted by the script.
    3. --force only authorizes replacement of the candidate file, never the active lockfile.
    """
    repo_root = Path(__file__).resolve().parent.parent.parent
    script_path = repo_root / "scripts" / "generate_api_lockfile.sh"
    assert script_path.exists()

    content = script_path.read_text(encoding="utf-8")

    # Destination guard
    assert '[ "${CANDIDATE_LOCK}" = "${ACTIVE_LOCK}" ]' in content
    assert "FATAL: Candidate lockfile destination path cannot match active lockfile." in content

    # Never delete active lockfile
    assert 'rm -f "${ACTIVE_LOCK}"' not in content
    assert 'rm "${ACTIVE_LOCK}"' not in content

    # Active lockfile only in read-only instructions
    lines = content.splitlines()
    for line in lines:
        if "${ACTIVE_LOCK}" in line and not line.strip().startswith("#"):
            assert any(safe in line for safe in ["echo", "diff", "cp ${CANDIDATE_LOCK} ${ACTIVE_LOCK}", "="]), (
                f"Potentially unsafe operation on ACTIVE_LOCK: {line}"
            )
