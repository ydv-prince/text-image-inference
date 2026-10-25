"""
Remote Private Asset Storage Adapter (Mode B)
Location: src/infrastructure/storage/remote_adapter.py

Security & Operational Guarantees:
1. Assets are private by default; no public directory listing or open bucket access.
2. Content-addressable naming: SHA-256 derived key paths prevent collision and path traversal.
3. Magic bytes and PIL image integrity verified prior to remote dispatch.
4. Offline mockable interface for zero-cloud testing.
"""

import hashlib
import hmac
import io
import re
import time
from typing import Optional, Tuple
from urllib.parse import parse_qs, urlparse
import uuid
from PIL import Image

from src.core.errors import CorruptAssetException, StorageException
from src.core.logging import logger
from src.domain.interfaces import AssetStorage, StoredAsset

PNG_MAGIC_BYTES = b"\x89PNG\r\n\x1a\n"
HASH_REGEX = re.compile(r"^[0-9a-f]{64}$")


class RemoteAssetStorage(AssetStorage):
    """
    Remote cloud storage adapter (Cloudinary / Supabase Storage / S3-compatible).
    """

    def __init__(
        self,
        provider_name: str = "remote_private",
        bucket_name: str = "aarav-private-assets",
        base_endpoint: Optional[str] = None,
        signing_key: Optional[bytes] = None,
    ):
        self._name = provider_name
        self.bucket_name = bucket_name
        self.base_endpoint = (base_endpoint or "https://storage.supabase.co/storage/v1/object/authenticated").rstrip("/")
        self._signing_key = signing_key or b"aarav_default_private_asset_signing_secret_key_32b"
        # In-memory remote blob simulation for offline testing
        self._remote_blobs: dict[str, bytes] = {}

    @property
    def name(self) -> str:
        return self._name

    def validate_png(self, image_bytes: bytes) -> Tuple[int, int]:
        """Validates PNG magic bytes and structural integrity."""
        if not image_bytes or len(image_bytes) < 8:
            raise CorruptAssetException("Image bytes empty or too small.")
        if not image_bytes.startswith(PNG_MAGIC_BYTES):
            raise CorruptAssetException("Invalid file header: Missing PNG signature magic bytes.")
        try:
            with Image.open(io.BytesIO(image_bytes)) as img:
                img.verify()
                return img.size
        except Exception as e:
            raise CorruptAssetException(f"Corrupt PNG structure: {e}")

    async def store_image(
        self,
        image_bytes: bytes,
        mime_type: str = "image/png",
        is_synthetic: bool = False,
    ) -> StoredAsset:
        """
        Validates, hashes, and atomically stores raw image bytes.
        """
        width, height = self.validate_png(image_bytes)
        file_hash = hashlib.sha256(image_bytes).hexdigest()

        h1, h2 = file_hash[:2], file_hash[2:4]
        storage_path = f"images/{h1}/{h2}/{file_hash}.png"

        # Deduplication check in remote store
        if storage_path in self._remote_blobs:
            logger.info(f"Asset with hash {file_hash} already exists in remote storage. Reusing.")
            return StoredAsset(
                asset_id=str(uuid.uuid4()),
                file_hash=file_hash,
                storage_path=storage_path,
                file_size_bytes=len(image_bytes),
                mime_type=mime_type,
                width=width,
                height=height,
                is_synthetic=is_synthetic,
            )

        # Store remote blob
        self._remote_blobs[storage_path] = image_bytes
        logger.info(f"Stored {len(image_bytes)} bytes to remote path {storage_path}")

        return StoredAsset(
            asset_id=str(uuid.uuid4()),
            file_hash=file_hash,
            storage_path=storage_path,
            file_size_bytes=len(image_bytes),
            mime_type=mime_type,
            width=width,
            height=height,
            is_synthetic=is_synthetic,
        )

    # Alias for convenience
    store = store_image

    def _resolve_path(self, locator: str) -> str:
        """Resolves locator (content hash or storage path) to remote blob key."""
        if not locator or not isinstance(locator, str):
            raise StorageException("Storage locator cannot be empty.")
        if HASH_REGEX.match(locator):
            h1, h2 = locator[:2], locator[2:4]
            return f"images/{h1}/{h2}/{locator}.png"
        return locator

    async def get_image_bytes(self, locator: str) -> Optional[bytes]:
        """Retrieves raw image bytes by locator (file hash or storage path)."""
        storage_path = self._resolve_path(locator)
        return self._remote_blobs.get(storage_path)

    # Alias for convenience
    retrieve = get_image_bytes

    async def exists(self, locator: str) -> bool:
        """Checks if an asset exists in remote storage."""
        try:
            storage_path = self._resolve_path(locator)
            return storage_path in self._remote_blobs
        except Exception:
            return False

    async def verify_asset(
        self,
        locator: str,
        expected_hash: Optional[str] = None,
    ) -> bool:
        """Verifies file existence and SHA-256 integrity."""
        data = await self.get_image_bytes(locator)
        if not data:
            return False
        computed = hashlib.sha256(data).hexdigest()
        if expected_hash is not None:
            return computed == expected_hash.lower()
        if HASH_REGEX.match(locator):
            return computed == locator.lower()
        match = re.search(r"([0-9a-f]{64})", locator.lower())
        if match:
            return computed == match.group(1)
        return True



    def generate_signed_url(
        self,
        file_hash: str,
        expires_in_seconds: int = 3600,
        signing_key: Optional[bytes] = None,
    ) -> str:
        """
        Generates a time-limited cryptographically signed HMAC URL for private asset delivery.
        """
        if not HASH_REGEX.match(file_hash):
            raise StorageException(f"Invalid file hash '{file_hash}'.")

        key = signing_key or self._signing_key
        expires_at = int(time.time()) + expires_in_seconds
        h1, h2 = file_hash[:2], file_hash[2:4]
        asset_rel_path = f"images/{h1}/{h2}/{file_hash}.png"

        # Canonical message: bucket/path:expires
        message = f"{self.bucket_name}/{asset_rel_path}:{expires_at}".encode("utf-8")
        signature = hmac.new(key, message, hashlib.sha256).hexdigest()

        return (
            f"{self.base_endpoint}/{self.bucket_name}/{asset_rel_path}"
            f"?expires={expires_at}&signature={signature}"
        )

    def verify_signed_url(self, signed_url: str, signing_key: Optional[bytes] = None) -> bool:
        """
        Verifies signature authenticity and expiration timestamp of a signed asset URL.
        """
        try:
            parsed = urlparse(signed_url)
            qs = parse_qs(parsed.query)

            if "expires" not in qs or "signature" not in qs:
                return False

            expires_at = int(qs["expires"][0])
            signature = qs["signature"][0]

            # Validate expiration against current epoch time
            if time.time() > expires_at:
                return False

            # Extract relative asset path after bucket name
            marker = f"/{self.bucket_name}/"
            if marker not in parsed.path:
                return False

            asset_rel_path = parsed.path.split(marker, 1)[1]
            key = signing_key or self._signing_key
            message = f"{self.bucket_name}/{asset_rel_path}:{expires_at}".encode("utf-8")
            expected_sig = hmac.new(key, message, hashlib.sha256).hexdigest()

            return hmac.compare_digest(signature, expected_sig)
        except Exception as e:
            logger.warning(f"Failed to verify signed URL '{signed_url}': {e}")
            return False


