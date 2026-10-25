"""
Unit Tests — Hybrid Storage Resolution, Incomplete Uploads & Proxied Delivery
Location: tests/unit/test_hybrid_storage_and_delivery.py
"""

import io
from pathlib import Path
import pytest
from PIL import Image
from unittest.mock import AsyncMock, MagicMock, patch

from src.core.config import settings
from src.core.errors import CorruptAssetException, ForbiddenException, EntityNotFoundException
from src.domain.models import JobStatus
from src.infrastructure.models import ApiKeyModel, GeneratedAssetModel, GenerationJobModel
from src.infrastructure.storage import get_asset_storage, LocalStorageCAS, RemoteAssetStorage
from src.services.generation_service import GenerationService


def make_valid_png_bytes(width: int = 64, height: int = 64) -> bytes:
    """Helper creating minimal verified PNG bytes."""
    buf = io.BytesIO()
    img = Image.new("RGB", (width, height), color=(50, 100, 150))
    img.save(buf, format="PNG")
    return buf.getvalue()


class TestHybridStorageAndDelivery:
    """Verifies storage resolution, partial upload guards, and proxied asset delivery."""

    def test_storage_factory_local_cas(self):
        """Resolves LocalStorageCAS when STORAGE_ADAPTER='local_cas'."""
        with patch.object(settings, "STORAGE_ADAPTER", "local_cas"):
            storage = get_asset_storage("local_cas")
            assert isinstance(storage, LocalStorageCAS)
            assert storage.name == "local_cas"

    def test_storage_factory_remote_vault(self):
        """Resolves RemoteAssetStorage when STORAGE_ADAPTER='remote_vault'."""
        storage = get_asset_storage("remote_vault")
        assert isinstance(storage, RemoteAssetStorage)
        assert storage.name == "remote_private"

    @pytest.mark.asyncio
    async def test_local_cas_rejects_truncated_partial_upload(self, tmp_path: Path):
        """LocalStorageCAS rejects incomplete / truncated bytes and writes no corrupt asset."""
        cas = LocalStorageCAS(base_path=tmp_path)
        valid_bytes = make_valid_png_bytes()

        # Simulate truncated stream (first 15 bytes only)
        truncated_bytes = valid_bytes[:15]

        with pytest.raises(CorruptAssetException):
            await cas.store_image(truncated_bytes)

        # Verify no images were saved to the storage root
        images_dir = tmp_path / "images"
        if images_dir.exists():
            files = list(images_dir.glob("**/*.png"))
            assert len(files) == 0

    @pytest.mark.asyncio
    async def test_remote_storage_rejects_corrupted_upload(self):
        """RemoteAssetStorage rejects non-PNG or corrupted bytes."""
        remote = RemoteAssetStorage()
        corrupt_bytes = b"\x89PNG\r\n\x1a\nCorruptedGarbageBytesPayloadNotValidPNG"

        with pytest.raises(CorruptAssetException):
            await remote.store_image(corrupt_bytes)

    @pytest.mark.asyncio
    async def test_get_asset_bytes_for_client_success(self):
        """Owner client successfully retrieves asset bytes for a COMPLETED job."""
        api_key = MagicMock(spec=ApiKeyModel)
        api_key.id = "tenant-001"

        valid_bytes = make_valid_png_bytes()
        mock_storage = AsyncMock()
        mock_storage.get_image_bytes.return_value = valid_bytes

        mock_asset = MagicMock(spec=GeneratedAssetModel)
        mock_asset.file_hash = "a" * 64

        mock_job = MagicMock(spec=GenerationJobModel)
        mock_job.id = "job-001"
        mock_job.api_key_id = "tenant-001"
        mock_job.status = JobStatus.COMPLETED.value
        mock_job.asset = mock_asset

        with patch("src.services.generation_service.GenerationService.get_job_for_client", new_callable=AsyncMock) as mock_get_job:
            mock_get_job.return_value = mock_job
            mock_db = AsyncMock()

            bytes_out, job_out = await GenerationService.get_asset_bytes_for_client(
                api_key=api_key,
                job_id="job-001",
                storage=mock_storage,
                db=mock_db,
            )

            assert bytes_out == valid_bytes
            assert job_out == mock_job
            mock_storage.get_image_bytes.assert_called_once_with("a" * 64)

    @pytest.mark.asyncio
    async def test_get_asset_bytes_rejects_pending_or_processing_job(self):
        """Throws EntityNotFoundException if job is still in progress (PENDING or PROCESSING)."""
        api_key = MagicMock(spec=ApiKeyModel)
        api_key.id = "tenant-001"

        mock_job = MagicMock(spec=GenerationJobModel)
        mock_job.id = "job-002"
        mock_job.api_key_id = "tenant-001"
        mock_job.status = JobStatus.PROCESSING.value
        mock_job.asset = None

        with patch("src.services.generation_service.GenerationService.get_job_for_client", new_callable=AsyncMock) as mock_get_job:
            mock_get_job.return_value = mock_job
            mock_storage = AsyncMock()
            mock_db = AsyncMock()

            with pytest.raises(EntityNotFoundException, match="Asset for job 'job-002' is not yet available"):
                await GenerationService.get_asset_bytes_for_client(
                    api_key=api_key,
                    job_id="job-002",
                    storage=mock_storage,
                    db=mock_db,
                )

    @pytest.mark.asyncio
    async def test_get_job_for_client_enforces_tenant_ownership(self):
        """Cross-tenant retrieval throws ForbiddenException."""
        api_key = MagicMock(spec=ApiKeyModel)
        api_key.id = "tenant-002"

        mock_job = MagicMock(spec=GenerationJobModel)
        mock_job.id = "job-003"
        mock_job.api_key_id = "tenant-001"  # Owned by tenant-001, requested by tenant-002

        mock_db = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_job
        mock_db.execute.return_value = mock_result

        with patch("src.services.generation_service.set_tenant_context", new_callable=AsyncMock):
            with pytest.raises(ForbiddenException, match="You do not have permission"):
                await GenerationService.get_job_for_client(
                    api_key=api_key,
                    job_id="job-003",
                    db=mock_db,
                )
