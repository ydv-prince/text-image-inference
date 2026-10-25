"""
Supabase Storage Remote Adapter (Mode B)
Location: src/infrastructure/storage/supabase_adapter.py

Security & Operational Guarantees:
1. Versioned Physical Keys: Images stored at images/{h1}/{h2}/{file_hash}/{blob_id}.png.
2. Single-Writer Create-Only Protocol: Enforces 'x-upsert: false' on upload; rejects overwrites.
3. Strict Path Traversal Defense: Validates path format and disallows traversal segments (..).
4. Authoritative Path Requirement: Rejects raw content hash lookups; callers must supply storage_path.
5. Pure AssetStorage: Implements read, write, and verify methods; deletion is strictly excluded.
"""

import asyncio
import hashlib
import io
import re
from typing import Optional, Tuple
import uuid
from PIL import Image
import httpx

from src.core.config import settings
from src.core.errors import CorruptAssetException, StorageException, ValidationException
from src.core.logging import logger
from src.domain.interfaces import AssetStorage, StoredAsset

PNG_MAGIC_BYTES = b"\x89PNG\r\n\x1a\n"

# Allowed canonical path patterns
CANONICAL_VERSIONED_PATH_REGEX = re.compile(
    r"^images/[0-9a-f]{2}/[0-9a-f]{2}/[0-9a-f]{64}/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.(png|jpg|jpeg|webp)$"
)
CANONICAL_LEGACY_PATH_REGEX = re.compile(
    r"^images/[0-9a-f]{2}/[0-9a-f]{2}/[0-9a-f]{64}\.(png|jpg|jpeg|webp)$"
)
RAW_HASH_REGEX = re.compile(r"^[0-9a-f]{64}$")
EMBEDDED_HASH_REGEX = re.compile(r"images/[0-9a-f]{2}/[0-9a-f]{2}/([0-9a-f]{64})")


class SupabaseStorageAdapter(AssetStorage):
    """
    Remote cloud storage adapter for Supabase Storage REST API.
    Implements pure read, write, and integrity verification via AssetStorage.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        bucket_name: Optional[str] = None,
        service_role_key: Optional[str] = None,
        timeout_seconds: Optional[float] = None,
        client: Optional[httpx.AsyncClient] = None,
        transport: Optional[httpx.AsyncBaseTransport] = None,
    ):
        raw_url = base_url or settings.SUPABASE_URL
        self.base_url = raw_url.rstrip("/") if raw_url else ""
        self.bucket_name = bucket_name or settings.SUPABASE_STORAGE_BUCKET

        key_value = service_role_key
        if key_value is None:
            srk = settings.SUPABASE_SERVICE_ROLE_KEY
            key_value = srk.get_secret_value() if hasattr(srk, "get_secret_value") else str(srk)
        self.service_role_key = key_value

        self.timeout_seconds = timeout_seconds or settings.STORAGE_TIMEOUT_SECONDS
        self._external_client = client is not None

        if client is not None:
            self._client = client
        elif transport is not None:
            self._client = httpx.AsyncClient(transport=transport, timeout=self.timeout_seconds)
        else:
            self._client = httpx.AsyncClient(timeout=self.timeout_seconds)

    @property
    def name(self) -> str:
        """Adapter identifier."""
        return "supabase"

    async def aclose(self) -> None:
        """Closes the underlying HTTP client session if internally created."""
        if not self._external_client:
            await self._client.aclose()

    async def __aenter__(self) -> "SupabaseStorageAdapter":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.aclose()

    def _get_auth_headers(self, content_type: Optional[str] = None) -> dict[str, str]:
        """Constructs authenticated Supabase service role headers."""
        headers = {
            "Authorization": f"Bearer {self.service_role_key}",
            "apiKey": self.service_role_key,
        }
        if content_type:
            headers["Content-Type"] = content_type
        return headers

    def validate_path(self, storage_path: str) -> None:
        """
        Enforces strict path containment, traversal protection, and format regex.
        Raises ValidationException or ValueError on illegal structures.
        """
        if not storage_path or not isinstance(storage_path, str):
            raise ValidationException("Storage path cannot be empty.")

        # Check for path traversal sequences and illegal characters
        if (
            ".." in storage_path
            or storage_path.startswith("/")
            or storage_path.startswith("\\")
            or "\\" in storage_path
            or "\0" in storage_path
            or re.search(r"\s", storage_path)
        ):
            raise ValidationException(
                f"Path traversal or illegal characters detected in path: '{storage_path}'"
            )

        if not (
            CANONICAL_VERSIONED_PATH_REGEX.match(storage_path)
            or CANONICAL_LEGACY_PATH_REGEX.match(storage_path)
        ):
            raise ValidationException(
                f"Path does not match canonical storage path allowlist: '{storage_path}'"
            )

    def validate_png(self, image_bytes: bytes) -> Tuple[int, int]:
        """Validates that bytes represent a legitimate, uncorrupted PNG image."""
        if not image_bytes or len(image_bytes) < 8:
            raise CorruptAssetException("Image bytes empty or too small to be a valid file.")

        if not image_bytes.startswith(PNG_MAGIC_BYTES):
            raise CorruptAssetException("Invalid file header: Missing PNG signature magic bytes.")

        try:
            with Image.open(io.BytesIO(image_bytes)) as img:
                if img.format != "PNG":
                    raise CorruptAssetException(f"Expected PNG format, detected '{img.format}'.")
                img.verify()
                width, height = img.size
                if width <= 0 or height <= 0:
                    raise CorruptAssetException(f"Invalid image dimensions: {width}x{height}.")
                return width, height
        except Exception as e:
            if isinstance(e, CorruptAssetException):
                raise
            raise CorruptAssetException(f"Corrupted PNG payload: {str(e)}") from e

    def compute_sha256(self, image_bytes: bytes) -> str:
        """Computes lowercase hex SHA-256 digest of raw byte content."""
        return hashlib.sha256(image_bytes).hexdigest().lower()

    async def store_image(
        self,
        image_bytes: bytes,
        mime_type: str = "image/png",
        is_synthetic: bool = False,
    ) -> StoredAsset:
        """
        Validates, hashes, generates versioned physical key, and uploads image bytes
        to Supabase Storage enforcing create-only single-writer semantics ('x-upsert: false').
        """
        width, height = self.validate_png(image_bytes)
        file_hash = self.compute_sha256(image_bytes)
        blob_id = str(uuid.uuid4())
        h1, h2 = file_hash[:2], file_hash[2:4]
        storage_path = f"images/{h1}/{h2}/{file_hash}/{blob_id}.png"

        upload_url = f"{self.base_url}/storage/v1/object/{self.bucket_name}/{storage_path}"
        headers = self._get_auth_headers(content_type=mime_type)
        headers["x-upsert"] = "false"

        max_attempts = 3
        last_error: Optional[Exception] = None

        for attempt in range(1, max_attempts + 1):
            try:
                response = await self._client.post(
                    upload_url,
                    content=image_bytes,
                    headers=headers,
                )

                if response.status_code in (200, 201):
                    logger.info(f"Uploaded asset to Supabase Storage: {storage_path}")
                    return StoredAsset(
                        asset_id=blob_id,
                        file_hash=file_hash,
                        storage_path=storage_path,
                        file_size_bytes=len(image_bytes),
                        mime_type=mime_type,
                        width=width,
                        height=height,
                        is_synthetic=is_synthetic,
                    )

                if response.status_code in (400, 409):
                    # Duplicate object collision (create-only rejection)
                    logger.warning(
                        f"Supabase create-only collision on '{storage_path}': HTTP {response.status_code}. "
                        "Verifying existing object integrity."
                    )
                    existing_bytes = await self.get_image_bytes(storage_path)
                    if existing_bytes and self.compute_sha256(existing_bytes) == file_hash:
                        return StoredAsset(
                            asset_id=blob_id,
                            file_hash=file_hash,
                            storage_path=storage_path,
                            file_size_bytes=len(existing_bytes),
                            mime_type=mime_type,
                            width=width,
                            height=height,
                            is_synthetic=is_synthetic,
                        )
                    raise StorageException(
                        f"Collision on key '{storage_path}' but existing content could not be verified."
                    )

                if response.status_code in (401, 403):
                    raise StorageException(
                        f"Supabase Storage authentication failed with HTTP {response.status_code}: {response.text}"
                    )

                if response.status_code == 429:
                    raise StorageException("Supabase Storage rate limit exceeded.")

                if response.status_code in (500, 502, 503, 504):
                    if attempt < max_attempts:
                        await asyncio.sleep(0.05 * (2**attempt))
                        continue
                    raise StorageException(
                        f"Supabase Storage server error HTTP {response.status_code} after {max_attempts} attempts: {response.text}"
                    )

                raise StorageException(
                    f"Supabase Storage upload failed with HTTP {response.status_code}: {response.text}"
                )

            except httpx.TransportError as e:
                last_error = e
                if attempt < max_attempts:
                    await asyncio.sleep(0.05 * (2**attempt))
                    continue
                raise StorageException(
                    f"Supabase Storage transport error after {max_attempts} attempts: {e}"
                ) from e

        raise StorageException(f"Failed to upload asset to Supabase Storage: {last_error}")

    async def get_image_bytes(self, locator: str) -> Optional[bytes]:
        """
        Retrieves raw image bytes by authoritative storage_path.
        Fails fast with ValueError if called with a raw 64-char content hash.
        """
        if RAW_HASH_REGEX.match(locator):
            raise ValueError(
                f"Raw content hash lookup is not supported on SupabaseStorageAdapter without an "
                f"authoritative storage_path (received: '{locator}'). Callers must supply the "
                f"storage_path recorded in generated_assets."
            )

        self.validate_path(locator)

        download_url = f"{self.base_url}/storage/v1/object/authenticated/{self.bucket_name}/{locator}"
        headers = self._get_auth_headers()

        max_attempts = 3
        last_error: Optional[Exception] = None

        for attempt in range(1, max_attempts + 1):
            try:
                response = await self._client.get(download_url, headers=headers)

                if response.status_code == 200:
                    return response.content

                if response.status_code == 404:
                    return None

                if response.status_code in (401, 403):
                    raise StorageException(
                        f"Supabase Storage authentication failed with HTTP {response.status_code}: {response.text}"
                    )

                if response.status_code == 429:
                    raise StorageException("Supabase Storage rate limit exceeded.")

                if response.status_code in (500, 502, 503, 504):
                    if attempt < max_attempts:
                        await asyncio.sleep(0.05 * (2**attempt))
                        continue
                    raise StorageException(
                        f"Supabase Storage server error HTTP {response.status_code} after {max_attempts} attempts: {response.text}"
                    )

                raise StorageException(
                    f"Supabase Storage download failed with HTTP {response.status_code}: {response.text}"
                )

            except httpx.TransportError as e:
                last_error = e
                if attempt < max_attempts:
                    await asyncio.sleep(0.05 * (2**attempt))
                    continue
                raise StorageException(
                    f"Supabase Storage transport error after {max_attempts} attempts: {e}"
                ) from e

        raise StorageException(f"Failed to retrieve asset from Supabase Storage: {last_error}")

    async def exists(self, locator: str) -> bool:
        """
        Checks whether an object exists at the specified authoritative storage path.
        Fails fast with ValueError if called with a raw 64-char content hash.
        """
        if RAW_HASH_REGEX.match(locator):
            raise ValueError(
                f"Raw content hash lookup is not supported on SupabaseStorageAdapter without an "
                f"authoritative storage_path (received: '{locator}'). Callers must supply the "
                f"storage_path recorded in generated_assets."
            )

        self.validate_path(locator)

        url = f"{self.base_url}/storage/v1/object/authenticated/{self.bucket_name}/{locator}"
        headers = self._get_auth_headers()

        try:
            response = await self._client.head(url, headers=headers)
            if response.status_code == 200:
                return True
            if response.status_code == 404:
                return False
            # Fallback to GET if HEAD method is not supported by mock/endpoint
            if response.status_code == 405:
                get_resp = await self._client.get(url, headers=headers)
                return get_resp.status_code == 200
            if response.status_code in (401, 403):
                raise StorageException(
                    f"Supabase Storage authentication failed with HTTP {response.status_code}"
                )
            raise StorageException(
                f"Supabase Storage exists check failed with HTTP {response.status_code}"
            )
        except httpx.TransportError as e:
            raise StorageException(f"Supabase Storage transport error during exists check: {e}") from e

    async def verify_asset(
        self,
        locator: str,
        expected_hash: Optional[str] = None,
    ) -> bool:
        """
        Verifies asset existence AND cryptographic SHA-256 integrity.
        Reads byte payload and asserts digest strictly matches expected_hash
        or the embedded hash token in the canonical storage path.
        Fails fast with ValueError if called with a raw 64-char content hash.
        """
        if RAW_HASH_REGEX.match(locator):
            raise ValueError(
                f"Raw content hash lookup is not supported on SupabaseStorageAdapter without an "
                f"authoritative storage_path (received: '{locator}'). Callers must supply the "
                f"storage_path recorded in generated_assets."
            )

        self.validate_path(locator)

        try:
            data = await self.get_image_bytes(locator)
            if not data:
                return False

            computed = self.compute_sha256(data)

            if expected_hash is not None:
                if computed != expected_hash.lower():
                    logger.warning(
                        f"Integrity mismatch on '{locator}': expected '{expected_hash}', computed '{computed}'"
                    )
                    return False

            # Extract embedded hash from path
            match = EMBEDDED_HASH_REGEX.search(locator)
            if match:
                embedded = match.group(1).lower()
                if computed != embedded:
                    logger.warning(
                        f"Integrity mismatch against embedded path hash on '{locator}': "
                        f"embedded '{embedded}', computed '{computed}'"
                    )
                    return False

            return True

        except (ValidationException, ValueError):
            raise
        except StorageException as e:
            logger.warning(f"Storage error during verify_asset on '{locator}': {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error verifying asset '{locator}': {e}", exc_info=True)
            return False



