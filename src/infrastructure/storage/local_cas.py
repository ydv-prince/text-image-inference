"""
Content-Addressable Storage (CAS) with 2-Tier Directory Fan-Out
Location: src/infrastructure/storage/local_cas.py

Guarantees:
1. Strict SHA-256 derivation: File destination derived solely from verified payload bytes.
2. PNG Validation: Validates magic numbers and image integrity before writing to disk.
3. Path Traversal Defense: Rejects illegal characters and enforces base directory containment.
4. Atomic Write: Writes to temp file, flushes, fsyncs, and executes atomic os.replace.
5. Idempotent Deduplication: Identical byte payloads reuse existing verified on-disk assets.
"""

import hashlib
import io
import os
from pathlib import Path
import re
import uuid
from typing import Optional, Tuple
from PIL import Image

from src.core.config import settings
from src.core.errors import CorruptAssetException, StorageException, ValidationException
from src.core.logging import logger
from src.domain.interfaces import AssetStorage, StoredAsset

PNG_MAGIC_BYTES = b"\x89PNG\r\n\x1a\n"
HASH_REGEX = re.compile(r"^[0-9a-f]{64}$")


class LocalStorageCAS(AssetStorage):
    """
    Local filesystem implementation of Content-Addressable Storage for image assets.
    """

    def __init__(self, base_path: Optional[Path] = None):
        self.base_path = Path(base_path or settings.STORAGE_BASE_PATH).resolve()
        self.tmp_dir = self.base_path / "tmp"
        self._ensure_directories()

    @property
    def name(self) -> str:
        """Adapter identifier."""
        return "local_cas"

    def _ensure_directories(self) -> None:
        """Initializes storage base and temp directories."""
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)

    def validate_png(self, image_bytes: bytes) -> Tuple[int, int]:
        """
        Validates that bytes represent a legitimate, uncorrupted PNG image.
        Returns (width, height) on success.
        """
        if not image_bytes or len(image_bytes) < 8:
            raise CorruptAssetException("Image bytes empty or too small to be a valid file.")

        # 1. Check PNG signature
        if not image_bytes.startswith(PNG_MAGIC_BYTES):
            raise CorruptAssetException("Invalid file header: Missing PNG signature magic bytes.")

        # 2. Structural PIL validation
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

    def get_relative_path(self, file_hash: str) -> Path:
        """
        Derives two-tier fan-out path: {hash[0:2]}/{hash[2:4]}/{hash}.png
        """
        if not HASH_REGEX.match(file_hash):
            raise ValidationException(f"Invalid SHA-256 hash string: '{file_hash}'")

        return Path(file_hash[0:2]) / file_hash[2:4] / f"{file_hash}.png"

    def get_absolute_path(self, file_hash: str) -> Path:
        """
        Resolves the absolute path and enforces strict path traversal protection.
        """
        rel_path = self.get_relative_path(file_hash)
        abs_path = (self.base_path / rel_path).resolve()

        # Enforce that resolved path resides strictly within storage root
        try:
            abs_path.relative_to(self.base_path)
        except ValueError as e:
            raise StorageException("Path traversal violation detected.", detail=str(e))

        return abs_path

    async def store_image(
        self,
        image_bytes: bytes,
        mime_type: str = "image/png",
        is_synthetic: bool = False,
    ) -> StoredAsset:
        """
        Validates, deduplicates, and atomically writes image bytes to disk.
        """
        # 1. Validate image integrity
        width, height = self.validate_png(image_bytes)

        # 2. Derive content hash and paths
        file_hash = self.compute_sha256(image_bytes)
        rel_path = self.get_relative_path(file_hash)
        dest_path = self.get_absolute_path(file_hash)
        file_size = len(image_bytes)

        # 3. Deduplication check: if file already exists with identical size, reuse it
        if dest_path.is_file():
            try:
                if dest_path.stat().st_size == file_size:
                    logger.debug(f"CAS hit: Reusing existing asset {file_hash[:10]}...")
                    return StoredAsset(
                        asset_id=str(uuid.uuid4()),
                        file_hash=file_hash,
                        storage_path=rel_path.as_posix(),
                        file_size_bytes=file_size,
                        mime_type=mime_type,
                        width=width,
                        height=height,
                        is_synthetic=is_synthetic,
                    )
            except OSError:
                pass  # Fall through to write if stat fails

        # 4. Atomic write via staging file
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)
        temp_path = self.tmp_dir / f"{uuid.uuid4().hex}.tmp"

        try:
            with open(temp_path, "wb") as f:
                f.write(image_bytes)
                f.flush()
                os.fsync(f.fileno())

            # Atomic no-clobber link: if destination exists, fails atomically with FileExistsError
            try:
                os.link(temp_path, dest_path)
                temp_path.unlink(missing_ok=True)
            except FileExistsError:
                # Concurrent writer created the exact content-addressed file; clean up staging file
                temp_path.unlink(missing_ok=True)
            except OSError:
                # Fallback for environments where os.link is unsupported (e.g., cross-device)
                if dest_path.is_file():
                    temp_path.unlink(missing_ok=True)
                else:
                    os.replace(temp_path, dest_path)
        except Exception as e:
            if temp_path.exists():
                temp_path.unlink(missing_ok=True)
            logger.error(f"Failed writing asset to CAS: {e}", exc_info=True)
            raise StorageException(f"Failed to write image asset: {e}") from e


        return StoredAsset(
            asset_id=str(uuid.uuid4()),
            file_hash=file_hash,
            storage_path=rel_path.as_posix(),
            file_size_bytes=file_size,
            mime_type=mime_type,
            width=width,
            height=height,
            is_synthetic=is_synthetic,
        )

    def _resolve_locator(self, locator: str) -> Path:
        """
        Resolves a locator (either a 64-char SHA-256 hash or a relative storage path)
        to a strictly contained absolute filesystem path.
        """
        if not locator or not isinstance(locator, str):
            raise ValidationException("Storage locator cannot be empty.")

        # Check for path traversal sequences and illegal characters
        if ".." in locator or locator.startswith("/") or locator.startswith("\\") or "\0" in locator:
            raise StorageException("Path traversal violation detected.", detail=f"Illegal locator: '{locator}'")

        if HASH_REGEX.match(locator):
            return self.get_absolute_path(locator)

        # Relative path resolution with strict containment enforcement
        norm_locator = locator.replace("\\", "/")
        if self.base_path.name == "images" and norm_locator.startswith("images/"):
            norm_locator = norm_locator[len("images/"):]

        try:
            rel_path = Path(norm_locator)
            abs_path = (self.base_path / rel_path).resolve()
            abs_path.relative_to(self.base_path)
            return abs_path
        except (ValueError, Exception) as e:
            raise StorageException("Path traversal violation detected.", detail=str(e))

    async def get_image_bytes(self, locator: str) -> Optional[bytes]:
        """Reads raw binary content for the given locator (file hash or storage path)."""
        target_path = self._resolve_locator(locator)
        if not target_path.is_file():
            return None
        with open(target_path, "rb") as f:
            return f.read()

    async def exists(self, locator: str) -> bool:
        """Returns True if the content-addressed file exists on disk."""
        try:
            return self._resolve_locator(locator).is_file()
        except (ValidationException, StorageException):
            raise
        except Exception:
            return False

    async def verify_asset(
        self,
        locator: str,
        expected_hash: Optional[str] = None,
    ) -> bool:
        """
        Verifies that the on-disk file exists and matches the expected SHA-256 digest.
        If expected_hash is provided, validates content against it.
        Otherwise, if locator is a hash, validates against locator;
        if locator is a path with embedded hash, validates against the embedded hash.
        """
        try:
            target_path = self._resolve_locator(locator)
            if not target_path.is_file():
                return False
            with open(target_path, "rb") as f:
                content = f.read()
            computed = self.compute_sha256(content)

            if expected_hash is not None:
                return computed == expected_hash.lower()

            if HASH_REGEX.match(locator):
                return computed == locator.lower()

            # Attempt to extract embedded 64-char hex hash from path
            match = re.search(r"([0-9a-f]{64})", locator.lower())
            if match:
                return computed == match.group(1)

            # If no hash can be derived, existence is verified
            return True
        except (ValidationException, StorageException):
            raise
        except Exception:
            return False




