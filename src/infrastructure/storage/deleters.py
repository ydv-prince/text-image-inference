"""
Physical Storage Deleters (Milestone 6.3)
Location: src/infrastructure/storage/deleters.py

Strict Isolation Guarantees:
1. Physical deletion is strictly isolated to PhysicalBlobStorage implementations.
2. AssetStorage interface remains 100% free of deletion methods.
3. Invoked exclusively by DeletionOutboxService after all safety gates pass.
"""

import os
from pathlib import Path
from typing import Optional
import httpx

from src.core.config import settings
from src.core.errors import StorageException, ValidationException
from src.core.logging import logger
from src.domain.interfaces import PhysicalBlobStorage


class LocalCASPhysicalDeleter(PhysicalBlobStorage):
    """
    Physical deleter for local filesystem Content-Addressable Storage.
    Enforces path containment and unlinks files safely.
    """

    def __init__(self, base_path: Optional[Path] = None):
        self.base_path = Path(base_path or settings.STORAGE_BASE_PATH).resolve()

    def _resolve_safe_path(self, storage_path: str) -> Path:
        if not storage_path or not isinstance(storage_path, str):
            raise ValidationException("Storage path cannot be empty.")

        if ".." in storage_path or storage_path.startswith("/") or storage_path.startswith("\\") or "\0" in storage_path:
            raise StorageException("Path traversal violation detected.", detail=f"Illegal path: '{storage_path}'")

        norm = storage_path.replace("\\", "/")
        if self.base_path.name == "images" and norm.startswith("images/"):
            norm = norm[len("images/"):]

        try:
            rel = Path(norm)
            target = (self.base_path / rel).resolve()
            target.relative_to(self.base_path)
            return target
        except Exception as e:
            raise StorageException("Path traversal violation detected.", detail=str(e))

    async def delete_physical_object(self, storage_path: str) -> bool:
        """
        Deletes a physical asset file from local storage.
        Idempotent: returns True if file was deleted or did not exist.
        """
        try:
            target = self._resolve_safe_path(storage_path)
            if target.is_file():
                os.unlink(target)
                logger.info(f"LocalCASPhysicalDeleter: Unlinked '{target}'")
            return True
        except (ValidationException, StorageException):
            raise
        except Exception as e:
            logger.error(f"LocalCASPhysicalDeleter failed for '{storage_path}': {e}")
            raise StorageException(f"Failed to delete local physical object: {e}") from e


class SupabasePhysicalDeleter(PhysicalBlobStorage):
    """
    Physical deleter for Supabase Storage REST API.
    Sends DELETE request to the storage bucket.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        bucket_name: Optional[str] = None,
        service_role_key: Optional[str] = None,
        client: Optional[httpx.AsyncClient] = None,
    ):
        raw_url = base_url or settings.SUPABASE_URL
        self.base_url = raw_url.rstrip("/") if raw_url else ""
        self.bucket_name = bucket_name or settings.SUPABASE_STORAGE_BUCKET
        self.service_role_key = service_role_key or (
            settings.SUPABASE_SERVICE_ROLE_KEY.get_secret_value()
            if settings.SUPABASE_SERVICE_ROLE_KEY
            else ""
        )
        self._client = client

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is not None and not self._client.is_closed:
            return self._client
        headers = {
            "Authorization": f"Bearer {self.service_role_key}",
            "apikey": self.service_role_key,
        }
        self._client = httpx.AsyncClient(
            headers=headers,
            timeout=settings.STORAGE_TIMEOUT_SECONDS,
        )
        return self._client

    async def delete_physical_object(self, storage_path: str) -> bool:
        """
        Deletes a physical object from Supabase Storage via REST API.
        Returns:
            True on HTTP 200, 204, or 404 (idempotent success).
        Raises:
            PermissionError on HTTP 401 or 403 (fatal authentication error).
            StorageException on transient HTTP 429, 5xx, or network timeouts.
        """
        if not storage_path or ".." in storage_path:
            raise ValidationException("Invalid storage path for deletion.")

        client = await self._get_client()
        url = f"{self.base_url}/storage/v1/object/{self.bucket_name}/{storage_path}"

        try:
            response = await client.delete(url)
            if response.status_code in (200, 204, 404):
                logger.info(f"SupabasePhysicalDeleter: Deleted '{storage_path}' (status {response.status_code})")
                return True
            elif response.status_code in (401, 403):
                msg = f"Supabase deletion authentication failed ({response.status_code}): {response.text}"
                logger.error(msg)
                raise PermissionError(msg)
            else:
                msg = f"Supabase deletion failed ({response.status_code}): {response.text}"
                logger.warning(msg)
                raise StorageException(msg)
        except (PermissionError, StorageException):
            raise
        except (httpx.TimeoutException, httpx.NetworkError) as e:
            logger.warning(f"Transient network error deleting '{storage_path}': {e}")
            raise StorageException(f"Network error deleting object: {e}") from e
        except Exception as e:
            logger.error(f"Unexpected error deleting '{storage_path}': {e}", exc_info=True)
            raise StorageException(f"Unexpected deletion error: {e}") from e

    async def close(self) -> None:
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()


class RemoteVaultPhysicalDeleter(PhysicalBlobStorage):
    """
    Physical deleter for simulated remote vault storage.
    """

    def __init__(self, remote_blobs: Optional[dict] = None):
        self.remote_blobs = remote_blobs if remote_blobs is not None else {}

    async def delete_physical_object(self, storage_path: str) -> bool:
        self.remote_blobs.pop(storage_path, None)
        return True


def get_physical_blob_deleter(provider: str = "local_cas") -> PhysicalBlobStorage:
    """Factory resolving the appropriate physical deleter for the given provider."""
    if provider == "supabase":
        return SupabasePhysicalDeleter()
    elif provider == "remote_vault":
        return RemoteVaultPhysicalDeleter()
    return LocalCASPhysicalDeleter()
