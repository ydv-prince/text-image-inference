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
from src.core.errors import CorruptAssetException, ForbiddenException, EntityNotFoundException, StorageException
from src.domain.models import JobStatus
from src.infrastructure.models import ApiKeyModel, GeneratedAssetModel, GenerationJobModel
from src.infrastructure.storage import get_asset_storage, LocalStorageCAS, RemoteAssetStorage
from src.infrastructure.storage.supabase_adapter import SupabaseStorageAdapter
from src.services.generation_service import GenerationService
from tests.unit.test_supabase_storage import MockSupabaseTransport


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

    @pytest.mark.asyncio
    async def test_get_asset_bytes_prefers_authoritative_storage_path(self):
        """Authoritative storage_path is used as the primary locator for asset retrieval."""
        api_key = MagicMock(spec=ApiKeyModel)
        api_key.id = "tenant-001"

        valid_bytes = make_valid_png_bytes()
        mock_storage = AsyncMock()
        mock_storage.get_image_bytes.return_value = valid_bytes

        mock_asset = MagicMock(spec=GeneratedAssetModel)
        mock_asset.storage_path = "images/ab/cd/abcdef1234567890.png"
        mock_asset.file_hash = "abcdef1234567890" * 4

        mock_job = MagicMock(spec=GenerationJobModel)
        mock_job.id = "job-004"
        mock_job.api_key_id = "tenant-001"
        mock_job.status = JobStatus.COMPLETED.value
        mock_job.asset = mock_asset

        with patch("src.services.generation_service.GenerationService.get_job_for_client", new_callable=AsyncMock) as mock_get_job:
            mock_get_job.return_value = mock_job
            mock_db = AsyncMock()

            bytes_out, job_out = await GenerationService.get_asset_bytes_for_client(
                api_key=api_key,
                job_id="job-004",
                storage=mock_storage,
                db=mock_db,
            )

            assert bytes_out == valid_bytes
            assert job_out == mock_job
            mock_storage.get_image_bytes.assert_called_once_with("images/ab/cd/abcdef1234567890.png")

    @pytest.mark.asyncio
    async def test_get_asset_bytes_fallback_when_storage_path_returns_none(self):
        """Falls back to file_hash when storage_path yields no bytes on local store."""
        api_key = MagicMock(spec=ApiKeyModel)
        api_key.id = "tenant-001"

        valid_bytes = make_valid_png_bytes()
        mock_storage = AsyncMock()
        mock_storage.get_image_bytes.side_effect = [None, valid_bytes]

        mock_asset = MagicMock(spec=GeneratedAssetModel)
        mock_asset.storage_path = "images/ab/cd/legacy_missing_file.png"
        mock_asset.file_hash = "b" * 64

        mock_job = MagicMock(spec=GenerationJobModel)
        mock_job.id = "job-005"
        mock_job.api_key_id = "tenant-001"
        mock_job.status = JobStatus.COMPLETED.value
        mock_job.asset = mock_asset

        with patch("src.services.generation_service.GenerationService.get_job_for_client", new_callable=AsyncMock) as mock_get_job:
            mock_get_job.return_value = mock_job
            mock_db = AsyncMock()

            bytes_out, job_out = await GenerationService.get_asset_bytes_for_client(
                api_key=api_key,
                job_id="job-005",
                storage=mock_storage,
                db=mock_db,
            )

            assert bytes_out == valid_bytes
            assert job_out == mock_job
            assert mock_storage.get_image_bytes.call_count == 2
            mock_storage.get_image_bytes.assert_any_call("images/ab/cd/legacy_missing_file.png")
            mock_storage.get_image_bytes.assert_any_call("b" * 64)

    @pytest.mark.asyncio
    async def test_get_asset_bytes_with_supabase_storage_adapter_compatibility(self):
        """Simulates SupabaseStorageAdapter behavior: succeeds with storage_path without raising ValueError."""
        api_key = MagicMock(spec=ApiKeyModel)
        api_key.id = "tenant-001"

        valid_bytes = make_valid_png_bytes()

        async def supabase_mock_get_image_bytes(locator: str):
            if len(locator) == 64 and all(c in "0123456789abcdef" for c in locator.lower()):
                raise ValueError("Raw content hash lookup is not supported on SupabaseStorageAdapter")
            if locator == "images/ab/cd/supabase_stored.png":
                return valid_bytes
            return None

        mock_storage = AsyncMock()
        mock_storage.get_image_bytes.side_effect = supabase_mock_get_image_bytes

        mock_asset = MagicMock(spec=GeneratedAssetModel)
        mock_asset.storage_path = "images/ab/cd/supabase_stored.png"
        mock_asset.file_hash = "c" * 64

        mock_job = MagicMock(spec=GenerationJobModel)
        mock_job.id = "job-006"
        mock_job.api_key_id = "tenant-001"
        mock_job.status = JobStatus.COMPLETED.value
        mock_job.asset = mock_asset

        with patch("src.services.generation_service.GenerationService.get_job_for_client", new_callable=AsyncMock) as mock_get_job:
            mock_get_job.return_value = mock_job
            mock_db = AsyncMock()

            bytes_out, job_out = await GenerationService.get_asset_bytes_for_client(
                api_key=api_key,
                job_id="job-006",
                storage=mock_storage,
                db=mock_db,
            )

            assert bytes_out == valid_bytes
            assert job_out == mock_job

    @pytest.mark.asyncio
    async def test_get_asset_bytes_missing_both_locators_raises_not_found(self):
        """Raises EntityNotFoundException when both storage_path and file_hash are empty or invalid."""
        api_key = MagicMock(spec=ApiKeyModel)
        api_key.id = "tenant-001"

        mock_storage = AsyncMock()

        mock_asset = MagicMock(spec=GeneratedAssetModel)
        mock_asset.storage_path = None
        mock_asset.file_hash = ""

        mock_job = MagicMock(spec=GenerationJobModel)
        mock_job.id = "job-007"
        mock_job.api_key_id = "tenant-001"
        mock_job.status = JobStatus.COMPLETED.value
        mock_job.asset = mock_asset

        with patch("src.services.generation_service.GenerationService.get_job_for_client", new_callable=AsyncMock) as mock_get_job:
            mock_get_job.return_value = mock_job
            mock_db = AsyncMock()

            with pytest.raises(EntityNotFoundException, match="Asset file with identifier 'job-007' was not found."):
                await GenerationService.get_asset_bytes_for_client(
                    api_key=api_key,
                    job_id="job-007",
                    storage=mock_storage,
                    db=mock_db,
                )

    @pytest.mark.asyncio
    async def test_get_asset_bytes_not_found_does_not_leak_private_storage_path(self):
        """Raises EntityNotFoundException using public hash identifier without leaking internal storage paths."""
        api_key = MagicMock(spec=ApiKeyModel)
        api_key.id = "tenant-001"

        mock_storage = AsyncMock()
        mock_storage.get_image_bytes.return_value = None

        mock_asset = MagicMock(spec=GeneratedAssetModel)
        mock_asset.storage_path = "internal/vault/confidential/tenant-001/asset-999.png"
        mock_asset.file_hash = "d" * 64

        mock_job = MagicMock(spec=GenerationJobModel)
        mock_job.id = "job-008"
        mock_job.api_key_id = "tenant-001"
        mock_job.status = JobStatus.COMPLETED.value
        mock_job.asset = mock_asset

        with patch("src.services.generation_service.GenerationService.get_job_for_client", new_callable=AsyncMock) as mock_get_job:
            mock_get_job.return_value = mock_job
            mock_db = AsyncMock()

            with pytest.raises(EntityNotFoundException) as exc_info:
                await GenerationService.get_asset_bytes_for_client(
                    api_key=api_key,
                    job_id="job-008",
                    storage=mock_storage,
                    db=mock_db,
                )

            err_msg = str(exc_info.value)
            assert "internal/vault/confidential" not in err_msg
            assert "d" * 64 in err_msg

    @pytest.mark.asyncio
    async def test_get_asset_bytes_storage_exception_propagates_safely(self):
        """Critical storage infrastructure failure (StorageException) propagates directly."""
        api_key = MagicMock(spec=ApiKeyModel)
        api_key.id = "tenant-001"

        mock_storage = AsyncMock()
        mock_storage.get_image_bytes.side_effect = StorageException("Underlying blob store unreachable.")

        mock_asset = MagicMock(spec=GeneratedAssetModel)
        mock_asset.storage_path = "images/ab/cd/asset.png"
        mock_asset.file_hash = "e" * 64

        mock_job = MagicMock(spec=GenerationJobModel)
        mock_job.id = "job-009"
        mock_job.api_key_id = "tenant-001"
        mock_job.status = JobStatus.COMPLETED.value
        mock_job.asset = mock_asset

        with patch("src.services.generation_service.GenerationService.get_job_for_client", new_callable=AsyncMock) as mock_get_job:
            mock_get_job.return_value = mock_job
            mock_db = AsyncMock()

            with pytest.raises(StorageException, match="Underlying blob store unreachable."):
                await GenerationService.get_asset_bytes_for_client(
                    api_key=api_key,
                    job_id="job-009",
                    storage=mock_storage,
                    db=mock_db,
                )

    @pytest.mark.asyncio
    async def test_get_asset_bytes_real_local_cas_end_to_end(self, tmp_path: Path):
        """Exercises real service retrieval with real LocalStorageCAS instance."""
        cas = LocalStorageCAS(base_path=tmp_path)
        valid_bytes = make_valid_png_bytes(width=32, height=32)
        stored = await cas.store_image(valid_bytes)

        api_key = MagicMock(spec=ApiKeyModel)
        api_key.id = "tenant-001"

        mock_asset = MagicMock(spec=GeneratedAssetModel)
        mock_asset.storage_path = stored.storage_path
        mock_asset.file_hash = stored.file_hash

        mock_job = MagicMock(spec=GenerationJobModel)
        mock_job.id = "job-real-cas"
        mock_job.api_key_id = "tenant-001"
        mock_job.status = JobStatus.COMPLETED.value
        mock_job.asset = mock_asset

        with patch("src.services.generation_service.GenerationService.get_job_for_client", new_callable=AsyncMock) as mock_get_job:
            mock_get_job.return_value = mock_job
            mock_db = AsyncMock()

            bytes_out, job_out = await GenerationService.get_asset_bytes_for_client(
                api_key=api_key,
                job_id="job-real-cas",
                storage=cas,
                db=mock_db,
            )

            assert bytes_out == valid_bytes
            assert job_out == mock_job

    @pytest.mark.asyncio
    async def test_get_asset_bytes_real_local_cas_fallback_to_hash(self, tmp_path: Path):
        """Exercises real LocalStorageCAS fallback from missing storage_path to file_hash."""
        cas = LocalStorageCAS(base_path=tmp_path)
        valid_bytes = make_valid_png_bytes(width=32, height=32)
        stored = await cas.store_image(valid_bytes)

        api_key = MagicMock(spec=ApiKeyModel)
        api_key.id = "tenant-001"

        mock_asset = MagicMock(spec=GeneratedAssetModel)
        # Point storage_path to non-existent location, but supply valid file_hash
        mock_asset.storage_path = "images/99/99/non_existent_path.png"
        mock_asset.file_hash = stored.file_hash

        mock_job = MagicMock(spec=GenerationJobModel)
        mock_job.id = "job-cas-fallback"
        mock_job.api_key_id = "tenant-001"
        mock_job.status = JobStatus.COMPLETED.value
        mock_job.asset = mock_asset

        with patch("src.services.generation_service.GenerationService.get_job_for_client", new_callable=AsyncMock) as mock_get_job:
            mock_get_job.return_value = mock_job
            mock_db = AsyncMock()

            bytes_out, job_out = await GenerationService.get_asset_bytes_for_client(
                api_key=api_key,
                job_id="job-cas-fallback",
                storage=cas,
                db=mock_db,
            )

            assert bytes_out == valid_bytes
            assert job_out == mock_job

    @pytest.mark.asyncio
    async def test_get_asset_bytes_real_supabase_adapter_end_to_end(self):
        """Exercises real service retrieval with real SupabaseStorageAdapter (in-memory transport)."""
        transport = MockSupabaseTransport()
        adapter = SupabaseStorageAdapter(
            base_url="https://mock-supabase.test",
            bucket_name="test-bucket",
            service_role_key="test-key",
            transport=transport,
        )

        valid_bytes = make_valid_png_bytes(width=32, height=32)
        stored = await adapter.store_image(valid_bytes)

        api_key = MagicMock(spec=ApiKeyModel)
        api_key.id = "tenant-001"

        mock_asset = MagicMock(spec=GeneratedAssetModel)
        mock_asset.storage_path = stored.storage_path
        mock_asset.file_hash = stored.file_hash

        mock_job = MagicMock(spec=GenerationJobModel)
        mock_job.id = "job-real-supa"
        mock_job.api_key_id = "tenant-001"
        mock_job.status = JobStatus.COMPLETED.value
        mock_job.asset = mock_asset

        with patch("src.services.generation_service.GenerationService.get_job_for_client", new_callable=AsyncMock) as mock_get_job:
            mock_get_job.return_value = mock_job
            mock_db = AsyncMock()

            bytes_out, job_out = await GenerationService.get_asset_bytes_for_client(
                api_key=api_key,
                job_id="job-real-supa",
                storage=adapter,
                db=mock_db,
            )

            assert bytes_out == valid_bytes
            assert job_out == mock_job

    @pytest.mark.asyncio
    async def test_get_asset_bytes_real_supabase_adapter_rejects_hash_safely(self):
        """Verifies that calling real SupabaseStorageAdapter with only file_hash fails fast without unhandled crash."""
        transport = MockSupabaseTransport()
        adapter = SupabaseStorageAdapter(
            base_url="https://mock-supabase.test",
            bucket_name="test-bucket",
            service_role_key="test-key",
            transport=transport,
        )

        api_key = MagicMock(spec=ApiKeyModel)
        api_key.id = "tenant-001"

        mock_asset = MagicMock(spec=GeneratedAssetModel)
        mock_asset.storage_path = None
        mock_asset.file_hash = "a" * 64

        mock_job = MagicMock(spec=GenerationJobModel)
        mock_job.id = "job-supa-hash-reject"
        mock_job.api_key_id = "tenant-001"
        mock_job.status = JobStatus.COMPLETED.value
        mock_job.asset = mock_asset

        with patch("src.services.generation_service.GenerationService.get_job_for_client", new_callable=AsyncMock) as mock_get_job:
            mock_get_job.return_value = mock_job
            mock_db = AsyncMock()

            with pytest.raises(EntityNotFoundException):
                await GenerationService.get_asset_bytes_for_client(
                    api_key=api_key,
                    job_id="job-supa-hash-reject",
                    storage=adapter,
                    db=mock_db,
                )

        # Confirm zero HTTP requests were made over the wire
        assert len(transport.request_log) == 0

    @pytest.mark.asyncio
    async def test_get_asset_bytes_real_supabase_adapter_server_error_propagates(self):
        """Verifies real SupabaseStorageAdapter HTTP 503 error propagates as StorageException, not swallowed as 404."""
        transport = MockSupabaseTransport()
        transport.simulate_503_count = 10  # Exceeds max 3 retry attempts
        adapter = SupabaseStorageAdapter(
            base_url="https://mock-supabase.test",
            bucket_name="test-bucket",
            service_role_key="test-key",
            transport=transport,
        )

        api_key = MagicMock(spec=ApiKeyModel)
        api_key.id = "tenant-001"

        mock_asset = MagicMock(spec=GeneratedAssetModel)
        h = "a" * 64
        mock_asset.storage_path = f"images/{h[:2]}/{h[2:4]}/{h}.png"
        mock_asset.file_hash = h

        mock_job = MagicMock(spec=GenerationJobModel)
        mock_job.id = "job-supa-503"
        mock_job.api_key_id = "tenant-001"
        mock_job.status = JobStatus.COMPLETED.value
        mock_job.asset = mock_asset

        with patch("src.services.generation_service.GenerationService.get_job_for_client", new_callable=AsyncMock) as mock_get_job:
            mock_get_job.return_value = mock_job
            mock_db = AsyncMock()

            with pytest.raises(StorageException, match="Supabase Storage server error"):
                await GenerationService.get_asset_bytes_for_client(
                    api_key=api_key,
                    job_id="job-supa-503",
                    storage=adapter,
                    db=mock_db,
                )
