"""
Storage Adapters Package
Location: src/infrastructure/storage/__init__.py
"""

from typing import Optional
from src.core.config import settings
from src.domain.interfaces import AssetStorage
from src.infrastructure.storage.local_cas import LocalStorageCAS
from src.infrastructure.storage.remote_adapter import RemoteAssetStorage
from src.infrastructure.storage.supabase_adapter import SupabaseStorageAdapter

# Singleton caches
_local_cas_instance: Optional[LocalStorageCAS] = None
_remote_storage_instance: Optional[RemoteAssetStorage] = None
_supabase_instance: Optional[SupabaseStorageAdapter] = None


def get_asset_storage(adapter_name: Optional[str] = None) -> AssetStorage:
    """
    Factory resolving the active AssetStorage implementation.
    Selects RemoteAssetStorage when configured for 'remote_vault',
    SupabaseStorageAdapter when configured for 'supabase',
    otherwise defaults to local Content-Addressable Storage (Mode A).
    Returns pure AssetStorage interface.
    """
    global _local_cas_instance, _remote_storage_instance, _supabase_instance

    target = adapter_name or settings.STORAGE_ADAPTER

    if target == "remote_vault":
        if _remote_storage_instance is None:
            _remote_storage_instance = RemoteAssetStorage()
        return _remote_storage_instance

    if target == "supabase":
        if _supabase_instance is None:
            _supabase_instance = SupabaseStorageAdapter()
        return _supabase_instance

    if _local_cas_instance is None:
        _local_cas_instance = LocalStorageCAS(base_path=settings.STORAGE_BASE_PATH)
    return _local_cas_instance


__all__ = ["LocalStorageCAS", "RemoteAssetStorage", "SupabaseStorageAdapter", "get_asset_storage"]

