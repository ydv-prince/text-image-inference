"""
Pure Domain Entities & Enums
Location: src/domain/models.py
"""

from enum import Enum


class JobStatus(str, Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    TIMED_OUT = "TIMED_OUT"


class ProviderType(str, Enum):
    LOCAL_GPU = "local-gpu"
    POLLINATIONS = "pollinations"
    FAL = "fal"
    MOCK = "mock"


class StorageType(str, Enum):
    LOCAL_CAS = "local_cas"
    CLOUDINARY = "cloudinary"
    SUPABASE = "supabase"


class ApiKeyTier(str, Enum):
    FREE = "free"
    PRO = "pro"
    ENTERPRISE = "enterprise"
