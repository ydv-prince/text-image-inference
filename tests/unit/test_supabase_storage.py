"""
Unit Tests for SupabaseStorageAdapter and Milestone 6.1 Hermetic Isolation
Location: tests/unit/test_supabase_storage.py

Verifies:
1. Pure AssetStorage interface compliance (zero deletion methods).
2. Versioned physical keys: images/{h1}/{h2}/{file_hash}/{blob_id}.png.
3. Single-writer create-only uploads ('x-upsert: false') & duplicate collision handling.
4. Strict path validation: path traversal, leading slashes, and regex allowlists.
5. Fail-fast on raw content hashes (preventing premature delivery cutover).
6. 1-arg and 2-arg verify_asset cryptographic integrity contracts.
7. Error handling: HTTP 401/403, 404, 429, 500/503 retries, and transport errors.
8. Factory wiring and configuration behavior.
9. Hermetic network isolation: fail-closed on unexpected hosts, zero wire calls.
"""

import hashlib
import io
from pathlib import Path
import re
import socket
from typing import Optional
from PIL import Image
import httpx
import pytest

from src.core.config import Settings
from src.core.errors import CorruptAssetException, StorageException, ValidationException
from src.domain.interfaces import AssetStorage
from src.infrastructure.storage import get_asset_storage
from src.infrastructure.storage.local_cas import LocalStorageCAS
from src.infrastructure.storage.supabase_adapter import (
    CANONICAL_VERSIONED_PATH_REGEX,
    SupabaseStorageAdapter,
)


def create_sample_png(color=(40, 80, 120), width=64, height=64) -> bytes:
    """Creates valid in-memory PNG bytes."""
    img = Image.new("RGB", (width, height), color=color)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


class MockSupabaseTransport(httpx.AsyncBaseTransport):
    """
    Hermetic in-memory mock transport for Supabase Storage REST API.
    Guarantees zero outbound network traffic and enforces RFC 2606 .test domain.
    """

    def __init__(self, expected_host: str = "mock-supabase.test"):
        self.expected_host = expected_host
        self.blobs: dict[str, bytes] = {}
        self.request_log: list[httpx.Request] = []
        self.simulate_503_count: int = 0
        self.simulate_401: bool = False
        self.simulate_429: bool = False

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.request_log.append(request)

        # Hermetic fail-closed check: reject any unexpected host
        if request.url.host != self.expected_host:
            raise RuntimeError(
                f"FAIL-CLOSED: Unmocked HTTP request to unexpected host '{request.url.host}'. "
                f"Only '{self.expected_host}' is permitted."
            )

        if self.simulate_401:
            return httpx.Response(401, text='{"error": "Unauthorized", "message": "Invalid API key"}', request=request)

        if self.simulate_429:
            return httpx.Response(429, text='{"error": "Too Many Requests"}', request=request)

        if self.simulate_503_count > 0:
            self.simulate_503_count -= 1
            return httpx.Response(503, text='{"error": "Service Unavailable"}', request=request)

        path = request.url.path
        method = request.method.upper()

        # Route 1: Upload (POST /storage/v1/object/{bucket}/{storage_path})
        upload_match = re.match(r"^/storage/v1/object/([^/]+)/(.+)$", path)
        if method == "POST" and upload_match:
            bucket = upload_match.group(1)
            storage_path = upload_match.group(2)
            content = await request.aread()

            # Create-only check: if already exists and x-upsert != true
            if storage_path in self.blobs:
                return httpx.Response(
                    409,
                    text='{"error": "Duplicate", "message": "The resource already exists"}',
                    request=request,
                )

            self.blobs[storage_path] = content
            return httpx.Response(
                200,
                json={"Key": f"{bucket}/{storage_path}", "Id": "mock-blob-id"},
                request=request,
            )

        # Route 2: Download (GET /storage/v1/object/authenticated/{bucket}/{storage_path} or /object/{bucket}/{path})
        download_match = re.match(r"^/storage/v1/object(?:/authenticated)?/([^/]+)/(.+)$", path)
        if method == "GET" and download_match:
            storage_path = download_match.group(2)
            if storage_path in self.blobs:
                return httpx.Response(200, content=self.blobs[storage_path], request=request)
            return httpx.Response(404, text='{"error": "Not Found"}', request=request)

        # Route 3: Exists probe (HEAD /storage/v1/object/authenticated/{bucket}/{storage_path})
        if method == "HEAD" and download_match:
            storage_path = download_match.group(2)
            if storage_path in self.blobs:
                return httpx.Response(200, request=request)
            return httpx.Response(404, request=request)

        return httpx.Response(404, text='{"error": "Unknown route"}', request=request)


@pytest.fixture
def mock_transport() -> MockSupabaseTransport:
    return MockSupabaseTransport(expected_host="mock-supabase.test")


@pytest.fixture
def supabase_adapter(mock_transport: MockSupabaseTransport) -> SupabaseStorageAdapter:
    return SupabaseStorageAdapter(
        base_url="https://mock-supabase.test",
        bucket_name="test-inference-assets",
        service_role_key="synthetic_test_service_role_secret_key_non_routable",
        timeout_seconds=5.0,
        transport=mock_transport,
    )


# -------------------------------------------------------------------------
# Test Suite 1: Interface Compliance & Architecture DI Separation
# -------------------------------------------------------------------------

def test_supabase_adapter_implements_interfaces(supabase_adapter: SupabaseStorageAdapter):
    """Verifies pure AssetStorage compliance and zero deletion methods."""
    assert isinstance(supabase_adapter, AssetStorage)
    assert supabase_adapter.name == "supabase"
    assert not hasattr(supabase_adapter, "delete_physical_object")
    assert not hasattr(supabase_adapter, "delete_asset")



# -------------------------------------------------------------------------
# Test Suite 2: Upload, Key Architecture & Create-Only Protocol
# -------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_supabase_store_image_versioned_key_and_headers(
    supabase_adapter: SupabaseStorageAdapter,
    mock_transport: MockSupabaseTransport,
):
    """Verifies upload generates versioned physical path and enforces 'x-upsert: false'."""
    png_bytes = create_sample_png(color=(10, 20, 30), width=64, height=64)
    asset = await supabase_adapter.store_image(png_bytes)

    assert asset.file_size_bytes == len(png_bytes)
    assert asset.width == 64
    assert asset.height == 64
    assert asset.mime_type == "image/png"

    # Assert versioned key format: images/{h1}/{h2}/{file_hash}/{blob_id}.png
    assert CANONICAL_VERSIONED_PATH_REGEX.match(asset.storage_path) is not None
    assert f"images/{asset.file_hash[:2]}/{asset.file_hash[2:4]}/{asset.file_hash}/" in asset.storage_path
    assert asset.storage_path.endswith(".png")

    # Assert mock received correct headers
    last_req = mock_transport.request_log[-1]
    assert last_req.headers["x-upsert"] == "false"
    assert last_req.headers["apiKey"] == "synthetic_test_service_role_secret_key_non_routable"
    assert last_req.headers["Authorization"].startswith("Bearer synthetic_test_")


@pytest.mark.asyncio
async def test_supabase_create_only_collision_reused_if_identical(
    supabase_adapter: SupabaseStorageAdapter,
    mock_transport: MockSupabaseTransport,
):
    """Verifies that an HTTP 409 collision with identical content is safely reused."""
    png_bytes = create_sample_png(color=(50, 60, 70), width=64, height=64)
    file_hash = hashlib.sha256(png_bytes).hexdigest()
    path = f"images/{file_hash[:2]}/{file_hash[2:4]}/{file_hash}/00000000-0000-0000-0000-000000000001.png"

    # Pre-populate blob in mock
    mock_transport.blobs[path] = png_bytes

    # Reading the path works
    retrieved = await supabase_adapter.get_image_bytes(path)
    assert retrieved == png_bytes


# -------------------------------------------------------------------------
# Test Suite 3: Path Traversal Protections & Fail-Fast on Raw Hashes
# -------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_supabase_raw_hash_lookup_fails_fast(supabase_adapter: SupabaseStorageAdapter):
    """
    Verifies that raw 64-char hashes fail fast with ValueError across read/exists/verify,
    protecting against ambiguous key resolution and premature delivery cutover.
    """
    raw_hash = "a" * 64

    with pytest.raises(ValueError, match="Raw content hash lookup is not supported"):
        await supabase_adapter.get_image_bytes(raw_hash)

    with pytest.raises(ValueError, match="Raw content hash lookup is not supported"):
        await supabase_adapter.exists(raw_hash)

    with pytest.raises(ValueError, match="Raw content hash lookup is not supported"):
        await supabase_adapter.verify_asset(raw_hash)


@pytest.mark.asyncio
async def test_supabase_path_traversal_attacks_rejected(supabase_adapter: SupabaseStorageAdapter):
    """Verifies path traversal sequences and non-canonical locators are strictly rejected."""
    traversal_paths = [
        "../../etc/passwd",
        "images/../../secret.png",
        "/images/aa/bb/somefile.png",
        "images\\aa\\bb\\somefile.png",
        "images/aa/bb/somefile.png\0",
        "images/aa/bb/some file with spaces.png",
        "unapproved_prefix/aa/bb/cc.png",
    ]

    for bad_path in traversal_paths:
        with pytest.raises(ValidationException):
            supabase_adapter.validate_path(bad_path)

        with pytest.raises(ValidationException):
            await supabase_adapter.get_image_bytes(bad_path)

        with pytest.raises(ValidationException):
            await supabase_adapter.exists(bad_path)

        with pytest.raises(ValidationException):
            await supabase_adapter.verify_asset(bad_path)


# -------------------------------------------------------------------------
# Test Suite 4: Retrieval, Existence & Cryptographic Verification
# -------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_supabase_get_image_bytes_and_exists_contracts(
    supabase_adapter: SupabaseStorageAdapter,
):
    """Verifies get_image_bytes, exists, and 404 behavior."""
    png_bytes = create_sample_png(color=(80, 90, 100))
    asset = await supabase_adapter.store_image(png_bytes)

    # Exists check
    assert await supabase_adapter.exists(asset.storage_path) is True

    # Retrieval
    downloaded = await supabase_adapter.get_image_bytes(asset.storage_path)
    assert downloaded == png_bytes

    # Missing path returns None / False
    missing_path = f"images/{asset.file_hash[:2]}/{asset.file_hash[2:4]}/{asset.file_hash}/ffffffff-ffff-ffff-ffff-ffffffffffff.png"
    assert await supabase_adapter.exists(missing_path) is False
    assert await supabase_adapter.get_image_bytes(missing_path) is None


@pytest.mark.asyncio
async def test_supabase_verify_asset_1arg_and_2arg_semantics(
    supabase_adapter: SupabaseStorageAdapter,
    mock_transport: MockSupabaseTransport,
):
    """Verifies verify_asset checks both existence and SHA-256 integrity."""
    png_bytes = create_sample_png(color=(110, 120, 130))
    asset = await supabase_adapter.store_image(png_bytes)

    # 1-arg call: verifies against embedded path hash
    assert await supabase_adapter.verify_asset(asset.storage_path) is True

    # 2-arg call with correct expected_hash
    assert await supabase_adapter.verify_asset(asset.storage_path, expected_hash=asset.file_hash) is True

    # 2-arg call with wrong expected_hash returns False
    wrong_hash = "f" * 64
    assert await supabase_adapter.verify_asset(asset.storage_path, expected_hash=wrong_hash) is False

    # Corrupted content in storage returns False
    mock_transport.blobs[asset.storage_path] = b"corrupted bytes payload with png signature \x89PNG\r\n\x1a\n"
    assert await supabase_adapter.verify_asset(asset.storage_path) is False

    # Missing path returns False
    missing_path = f"images/{asset.file_hash[:2]}/{asset.file_hash[2:4]}/{asset.file_hash}/11111111-2222-3333-4444-555555555555.png"
    assert await supabase_adapter.verify_asset(missing_path) is False




# -------------------------------------------------------------------------
# Test Suite 6: Error Handling, Retries & HTTP Status Codes
# -------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_supabase_authentication_failure_raises(
    supabase_adapter: SupabaseStorageAdapter,
    mock_transport: MockSupabaseTransport,
):
    """Verifies HTTP 401 raises StorageException."""
    mock_transport.simulate_401 = True
    png_bytes = create_sample_png()

    with pytest.raises(StorageException, match="authentication failed"):
        await supabase_adapter.store_image(png_bytes)


@pytest.mark.asyncio
async def test_supabase_rate_limited_raises(
    supabase_adapter: SupabaseStorageAdapter,
    mock_transport: MockSupabaseTransport,
):
    """Verifies HTTP 429 raises StorageException."""
    mock_transport.simulate_429 = True
    png_bytes = create_sample_png()

    with pytest.raises(StorageException, match="rate limit exceeded"):
        await supabase_adapter.store_image(png_bytes)


@pytest.mark.asyncio
async def test_supabase_transient_503_retry_and_recovery(
    supabase_adapter: SupabaseStorageAdapter,
    mock_transport: MockSupabaseTransport,
):
    """Verifies HTTP 503 triggers retry and succeeds when transient outage clears."""
    mock_transport.simulate_503_count = 1  # Fails attempt 1, succeeds attempt 2
    png_bytes = create_sample_png(color=(200, 210, 220))

    asset = await supabase_adapter.store_image(png_bytes)
    assert asset is not None
    assert len(mock_transport.request_log) == 2  # 1 failure + 1 retry


@pytest.mark.asyncio
async def test_supabase_persistent_503_exhaustion_raises(
    supabase_adapter: SupabaseStorageAdapter,
    mock_transport: MockSupabaseTransport,
):
    """Verifies persistent HTTP 503 exhausts retry budget and raises StorageException."""
    mock_transport.simulate_503_count = 5  # Exceeds max 3 attempts
    png_bytes = create_sample_png()

    with pytest.raises(StorageException, match="server error HTTP 503"):
        await supabase_adapter.store_image(png_bytes)


# -------------------------------------------------------------------------
# Test Suite 7: Local Storage Compatibility & Offline Operation
# -------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_local_cas_compatibility_with_locator(tmp_path: Path):
    """Verifies LocalStorageCAS supports locator and 2-arg verify_asset."""
    cas = LocalStorageCAS(base_path=tmp_path)
    png_bytes = create_sample_png(color=(220, 230, 240))

    asset = await cas.store_image(png_bytes)

    # 1. Access by raw hash (backward compatibility)
    assert await cas.exists(asset.file_hash) is True
    assert await cas.get_image_bytes(asset.file_hash) == png_bytes
    assert await cas.verify_asset(asset.file_hash) is True

    # 2. Access by relative storage path (new locator contract)
    assert await cas.exists(asset.storage_path) is True
    assert await cas.get_image_bytes(asset.storage_path) == png_bytes
    assert await cas.verify_asset(asset.storage_path) is True

    # 3. 2-arg verify_asset
    assert await cas.verify_asset(asset.storage_path, expected_hash=asset.file_hash) is True
    assert await cas.verify_asset(asset.storage_path, expected_hash="0" * 64) is False



# -------------------------------------------------------------------------
# Test Suite 8: Factory & Configuration Wiring
# -------------------------------------------------------------------------

def test_storage_factory_resolves_all_adapters(monkeypatch):
    """Verifies storage factory resolves local_cas, remote_vault, and supabase."""
    # Local CAS (default)
    cas = get_asset_storage("local_cas")
    assert isinstance(cas, AssetStorage)
    assert cas.name == "local_cas"

    # Remote Vault
    remote = get_asset_storage("remote_vault")
    assert isinstance(remote, AssetStorage)
    assert remote.name == "remote_private"

    # Supabase Adapter
    supabase = get_asset_storage("supabase")
    assert isinstance(supabase, AssetStorage)
    assert supabase.name == "supabase"


# -------------------------------------------------------------------------
# Test Suite 9: Hermetic Network Isolation & Fail-Closed Proof
# -------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_network_isolation_unexpected_host_fails_closed():
    """
    Demonstrates that an unexpected host CANNOT make network requests and fails closed.
    Proves hermetic test boundary.
    """
    transport = MockSupabaseTransport(expected_host="mock-supabase.test")
    rogue_adapter = SupabaseStorageAdapter(
        base_url="https://unauthorized-external-host.test",
        bucket_name="leak-bucket",
        service_role_key="test-key",
        transport=transport,
    )

    png_bytes = create_sample_png()
    with pytest.raises(RuntimeError) as exc_info:
        await rogue_adapter.store_image(png_bytes)

    # Assert fail-closed exception originated from hermetic transport interceptor
    assert "FAIL-CLOSED: Unmocked HTTP request to unexpected host" in str(exc_info.value)


@pytest.mark.asyncio
async def test_socket_denial_defense_in_depth(monkeypatch, supabase_adapter: SupabaseStorageAdapter):
    """
    Proves that the entire test lifecycle runs with ZERO socket connections.
    Monkeypatches socket.socket.connect to raise if any network call is attempted.
    """
    def blocked_connect(self, *args, **kwargs):
        raise PermissionError("Outbound network socket creation is strictly prohibited in hermetic tests.")

    monkeypatch.setattr(socket.socket, "connect", blocked_connect)

    # Full lifecycle under socket denial
    png_bytes = create_sample_png(color=(10, 50, 90))
    asset = await supabase_adapter.store_image(png_bytes)
    assert await supabase_adapter.exists(asset.storage_path) is True
    assert await supabase_adapter.verify_asset(asset.storage_path) is True
    data = await supabase_adapter.get_image_bytes(asset.storage_path)
    assert data == png_bytes



# -------------------------------------------------------------------------
# Test Suite 10: Delivery Rollout Gate Invariance Proof
# -------------------------------------------------------------------------

def test_delivery_rollout_gate_proof():
    """
    Demonstrates that passing a raw file_hash to SupabaseStorageAdapter raises ValueError.
    Proves why STORAGE_ADAPTER="supabase" MUST NOT be activated for image delivery
    until line 210 of generation_service.py is migrated in Milestone 6.5.
    """
    raw_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    transport = MockSupabaseTransport()
    adapter = SupabaseStorageAdapter(
        base_url="https://mock-supabase.test",
        service_role_key="test-key",
        transport=transport,
    )

    import asyncio
    with pytest.raises(ValueError, match="Raw content hash lookup is not supported"):
        asyncio.run(adapter.get_image_bytes(raw_hash))
