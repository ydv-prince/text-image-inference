"""
Unit Tests for Remote Asset Storage Adapter
Location: tests/unit/test_remote_storage.py
"""

import io
from PIL import Image
import pytest
from src.core.errors import CorruptAssetException, StorageException
from src.infrastructure.storage.remote_adapter import RemoteAssetStorage


def create_test_png(width: int = 512, height: int = 512) -> bytes:
    buf = io.BytesIO()
    img = Image.new("RGB", (width, height), color=(30, 60, 90))
    img.save(buf, format="PNG")
    return buf.getvalue()


@pytest.mark.asyncio
async def test_remote_storage_store_and_retrieve():
    storage = RemoteAssetStorage(bucket_name="test-private-bucket")
    png_bytes = create_test_png(512, 512)

    stored = await storage.store(png_bytes)
    assert stored.file_hash is not None
    assert len(stored.file_hash) == 64
    assert stored.width == 512
    assert stored.height == 512
    assert stored.storage_path.startswith("images/")

    # Retrieve
    retrieved_bytes = await storage.retrieve(stored.file_hash)
    assert retrieved_bytes == png_bytes


@pytest.mark.asyncio
async def test_remote_storage_deduplication():
    storage = RemoteAssetStorage()
    png_bytes = create_test_png(512, 512)

    s1 = await storage.store(png_bytes)
    s2 = await storage.store(png_bytes)

    assert s1.file_hash == s2.file_hash
    assert s1.storage_path == s2.storage_path


@pytest.mark.asyncio
async def test_remote_storage_rejects_corrupt_bytes():
    storage = RemoteAssetStorage()
    with pytest.raises(CorruptAssetException):
        await storage.store(b"not a valid png file header")


def test_remote_storage_signed_url_generation_and_verification():
    storage = RemoteAssetStorage(bucket_name="aarav-secure-vault")
    dummy_hash = "a" * 64
    signed_url = storage.generate_signed_url(dummy_hash, expires_in_seconds=3600)

    assert "aarav-secure-vault" in signed_url
    assert dummy_hash in signed_url
    assert "expires=" in signed_url
    assert "signature=" in signed_url

    # 1. Valid signature verifies
    assert storage.verify_signed_url(signed_url) is True

    # 2. Expired signature is rejected
    expired_url = storage.generate_signed_url(dummy_hash, expires_in_seconds=-10)
    assert storage.verify_signed_url(expired_url) is False

    # 3. Tampered hash in URL is rejected
    tampered_url = signed_url.replace(dummy_hash, "b" * 64)
    assert storage.verify_signed_url(tampered_url) is False

    # 4. Tampered signature is rejected
    tampered_sig_url = signed_url[:-4] + "dead"
    assert storage.verify_signed_url(tampered_sig_url) is False

    # 5. Wrong signing key is rejected
    assert storage.verify_signed_url(signed_url, signing_key=b"unauthorized_attacker_key_32bytes") is False
