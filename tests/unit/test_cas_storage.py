"""
Unit Tests for Content-Addressable Storage (CAS)
Location: tests/unit/test_cas_storage.py
"""

import asyncio
import io
from pathlib import Path
from PIL import Image
import pytest

from src.core.errors import CorruptAssetException, StorageException, ValidationException
from src.infrastructure.storage.local_cas import LocalStorageCAS


def create_sample_png(color=(100, 150, 200), width=64, height=64) -> bytes:
    """Helper creating valid PNG byte buffer."""
    img = Image.new("RGB", (width, height), color=color)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


@pytest.mark.asyncio
async def test_cas_store_valid_png(tmp_path: Path):
    """Verifies storing a valid PNG derives correct 2-tier fan-out path."""
    storage = LocalStorageCAS(base_path=tmp_path)
    sample_png = create_sample_png(color=(30, 40, 50), width=128, height=128)

    asset = await storage.store_image(sample_png, is_synthetic=True)

    assert asset.width == 128
    assert asset.height == 128
    assert asset.file_size_bytes == len(sample_png)
    assert asset.is_synthetic is True

    # Verify fanout path structure: {h[0:2]}/{h[2:4]}/{h}.png
    expected_rel = f"{asset.file_hash[:2]}/{asset.file_hash[2:4]}/{asset.file_hash}.png"
    assert asset.storage_path == expected_rel

    abs_file = tmp_path / expected_rel
    assert abs_file.is_file()
    assert abs_file.read_bytes() == sample_png

    # Verify asset integrity check
    assert await storage.verify_asset(asset.file_hash) is True


@pytest.mark.asyncio
async def test_cas_deduplication_identical_content(tmp_path: Path):
    """Verifies that identical PNG bytes produce identical hash and reuse the asset."""
    storage = LocalStorageCAS(base_path=tmp_path)
    sample_png = create_sample_png(color=(70, 80, 90), width=64, height=64)

    asset1 = await storage.store_image(sample_png)
    asset2 = await storage.store_image(sample_png)

    # Identical content hashes
    assert asset1.file_hash == asset2.file_hash
    assert asset1.storage_path == asset2.storage_path
    assert asset1.file_size_bytes == asset2.file_size_bytes

    # Only one file exists on disk
    file_path = tmp_path / asset1.storage_path
    assert file_path.is_file()


@pytest.mark.asyncio
async def test_cas_rejects_corrupt_non_png_bytes(tmp_path: Path):
    """Verifies that non-PNG or corrupted bytes are strictly rejected."""
    storage = LocalStorageCAS(base_path=tmp_path)

    # 1. Plain text
    with pytest.raises(CorruptAssetException, match="Missing PNG signature"):
        await storage.store_image(b"This is not a png file at all!")

    # 2. Incomplete PNG magic bytes
    with pytest.raises(CorruptAssetException):
        await storage.store_image(b"\x89PNG\r\n\x1a\ncorrupted_body_data")

    # 3. Empty buffer
    with pytest.raises(CorruptAssetException):
        await storage.store_image(b"")


@pytest.mark.asyncio
async def test_cas_path_traversal_prevention(tmp_path: Path):
    """Verifies path traversal attacks in hashes are rejected."""
    storage = LocalStorageCAS(base_path=tmp_path)

    # Malicious hash attempt
    with pytest.raises(ValidationException):
        storage.get_relative_path("../../etc/passwd")

    with pytest.raises(ValidationException):
        storage.get_relative_path("..\\..\\windows\\system32\\calc.exe")


@pytest.mark.asyncio
async def test_cas_concurrent_identical_writes(tmp_path: Path):
    """Verifies that concurrent writes of the identical image do not corrupt the asset."""
    storage = LocalStorageCAS(base_path=tmp_path)
    sample_png = create_sample_png(color=(200, 100, 50), width=64, height=64)

    # Launch 10 simultaneous writes
    tasks = [storage.store_image(sample_png) for _ in range(10)]
    results = await asyncio.gather(*tasks)

    # All should return the same valid hash
    first_hash = results[0].file_hash
    for res in results:
        assert res.file_hash == first_hash

    # Disk file is valid and readable
    assert await storage.verify_asset(first_hash) is True
    disk_bytes = await storage.get_image_bytes(first_hash)
    assert disk_bytes == sample_png
