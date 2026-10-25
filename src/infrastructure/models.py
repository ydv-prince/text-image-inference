"""
SQLAlchemy 2.0 ORM Database Entities
Location: src/infrastructure/models.py
"""

import uuid
from datetime import datetime, timezone
from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, relationship

from src.domain.models import ApiKeyTier, JobStatus, ProviderType, StorageType


class Base(DeclarativeBase):
    """Base declarative class for all SQLAlchemy entities."""
    pass


def generate_uuid() -> str:
    """Generates a UUIDv4 string as standard entity identifier."""
    return str(uuid.uuid4())


class ApiKeyModel(Base):
    __tablename__ = "api_keys"

    id = Column(String(36), primary_key=True, default=generate_uuid)
    key_hash = Column(String(64), unique=True, nullable=False, index=True)
    client_name = Column(String(128), nullable=False)
    tier = Column(String(32), default=ApiKeyTier.FREE.value, nullable=False)
    rate_limit_rpm = Column(Integer, default=10, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    last_used_at = Column(DateTime(timezone=True), nullable=True)

    # Relationships
    jobs = relationship("GenerationJobModel", back_populates="api_key", cascade="all, delete-orphan", lazy="selectin")

    def __repr__(self) -> str:
        return f"<ApiKey id={self.id} client={self.client_name} tier={self.tier} active={self.is_active}>"


class GenerationJobModel(Base):
    __tablename__ = "generation_jobs"

    id = Column(String(36), primary_key=True, default=generate_uuid)
    api_key_id = Column(String(36), ForeignKey("api_keys.id", ondelete="CASCADE"), nullable=False, index=True)
    idempotency_key = Column(String(128), nullable=True)
    payload_hash = Column(String(64), nullable=False)
    status = Column(String(20), default=JobStatus.PENDING.value, nullable=False, index=True)

    # Inference Request Parameters
    prompt = Column(Text, nullable=False)
    negative_prompt = Column(Text, nullable=True)
    width = Column(Integer, default=512, nullable=False)
    height = Column(Integer, default=512, nullable=False)
    steps = Column(Integer, default=4, nullable=False)
    seed = Column(BigInteger, default=-1, nullable=False)
    model = Column(String(64), default="sd-1.5", nullable=False)
    provider_requested = Column(String(32), default=ProviderType.LOCAL_GPU.value, nullable=False)
    provider_used = Column(String(32), nullable=True)

    # Error & Failure Tracking
    error_code = Column(String(64), nullable=True)
    error_message = Column(Text, nullable=True)
    retry_count = Column(Integer, default=0, nullable=False)
    max_retries = Column(Integer, default=3, nullable=False)

    # Worker Lease & Concurrency Management
    locked_by = Column(String(64), nullable=True)
    locked_at = Column(DateTime(timezone=True), nullable=True, index=True)

    # Execution & Hardware Telemetry
    execution_time_ms = Column(Integer, nullable=True)
    peak_vram_mb = Column(Integer, nullable=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False, index=True)
    scheduled_at = Column(DateTime(timezone=True), nullable=True, index=True)
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)

    # Relationships
    api_key = relationship("ApiKeyModel", back_populates="jobs", lazy="selectin")
    asset = relationship("GeneratedAssetModel", back_populates="job", uselist=False, cascade="all, delete-orphan", lazy="selectin")

    __table_args__ = (
        Index("idx_jobs_queue_pickup", "status", "created_at"),
        Index("idx_jobs_queue_pickup_scheduled", "status", "scheduled_at", "created_at"),
        Index("idx_jobs_lease_sweeper", "status", "locked_at"),
        UniqueConstraint("api_key_id", "idempotency_key", name="uq_jobs_idempotency"),
    )

    def __repr__(self) -> str:
        return f"<GenerationJob id={self.id} status={self.status} model={self.model}>"


class GeneratedAssetModel(Base):
    __tablename__ = "generated_assets"

    id = Column(String(36), primary_key=True, default=generate_uuid)
    job_id = Column(String(36), ForeignKey("generation_jobs.id", ondelete="CASCADE"), nullable=False, unique=True, index=True)
    blob_id = Column(String(36), ForeignKey("storage_blobs.id", ondelete="RESTRICT"), nullable=True, index=True)
    storage_provider = Column(String(32), default=StorageType.LOCAL_CAS.value, nullable=False)
    file_hash = Column(String(64), nullable=False, index=True)  # SHA-256
    storage_path = Column(String(512), nullable=False)
    public_url = Column(Text, nullable=True)
    file_size_bytes = Column(BigInteger, nullable=False)
    mime_type = Column(String(32), default="image/png", nullable=False)
    width = Column(Integer, nullable=False)
    height = Column(Integer, nullable=False)
    is_synthetic = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)

    # Relationships
    job = relationship("GenerationJobModel", back_populates="asset")
    blob = relationship("StorageBlobModel", back_populates="assets")

    def __repr__(self) -> str:
        return f"<GeneratedAsset id={self.id} job_id={self.job_id} hash={self.file_hash[:10]}...>"


class StorageBlobModel(Base):
    __tablename__ = "storage_blobs"

    id = Column(String(36), primary_key=True, default=generate_uuid)
    file_hash = Column(String(64), nullable=False, index=True)
    storage_path = Column(String(512), nullable=False, unique=True)
    storage_provider = Column(String(32), default=StorageType.LOCAL_CAS.value, nullable=False)
    file_size_bytes = Column(BigInteger, nullable=False)
    mime_type = Column(String(100), default="image/png", nullable=False)
    status = Column(String(30), default="PENDING_UPLOAD", nullable=False, index=True)
    active_ref_count = Column(Integer, default=0, nullable=False)
    version = Column(Integer, default=1, nullable=False)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)

    # Relationships
    assets = relationship("GeneratedAssetModel", back_populates="blob")
    outbox_tasks = relationship("StorageDeletionOutboxModel", back_populates="blob", cascade="all, delete-orphan")
    upload_intents = relationship("StorageUploadIntentModel", back_populates="blob", cascade="all, delete-orphan")
    audit_logs = relationship("StorageRefCountAuditModel", back_populates="blob", cascade="all, delete-orphan")

    __table_args__ = (
        Index("idx_blobs_hash_status", "file_hash", "status"),
        Index("idx_blobs_status", "status"),
        CheckConstraint(
            "status IN ('PENDING_UPLOAD', 'AVAILABLE', 'DELETION_QUEUED', 'DELETION_IN_PROGRESS', 'PURGED')",
            name="chk_blob_status",
        ),
        CheckConstraint("active_ref_count >= 0", name="chk_ref_count_non_negative"),
        CheckConstraint("file_size_bytes > 0 AND file_size_bytes <= 52428800", name="chk_blob_file_size"),
        CheckConstraint("mime_type IN ('image/png', 'image/jpeg', 'image/webp')", name="chk_blob_mime_type"),
    )

    def __repr__(self) -> str:
        return f"<StorageBlob id={self.id} hash={self.file_hash[:10]}... status={self.status} refs={self.active_ref_count}>"


class StorageDeletionOutboxModel(Base):
    __tablename__ = "storage_deletion_outbox"

    id = Column(String(36), primary_key=True, default=generate_uuid)
    blob_id = Column(String(36), ForeignKey("storage_blobs.id", ondelete="RESTRICT"), nullable=False, index=True)
    file_hash = Column(String(64), nullable=False)
    storage_path = Column(String(512), nullable=False)
    fencing_token = Column(String(36), default=generate_uuid, nullable=False)
    status = Column(String(20), default="PENDING", nullable=False)
    attempts = Column(Integer, default=0, nullable=False)
    max_attempts = Column(Integer, default=5, nullable=False)
    next_retry_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    locked_at = Column(DateTime(timezone=True), nullable=True)
    locked_by = Column(String(64), nullable=True)
    cancel_reason = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    completed_at = Column(DateTime(timezone=True), nullable=True)

    # Relationships
    blob = relationship("StorageBlobModel", back_populates="outbox_tasks")

    __table_args__ = (
        Index("idx_deletion_outbox_pickup", "status", "next_retry_at"),
        CheckConstraint(
            "status IN ('PENDING', 'PROCESSING', 'COMPLETED', 'CANCELLED', 'FAILED')",
            name="chk_deletion_outbox_status",
        ),
    )

    def __repr__(self) -> str:
        return f"<StorageDeletionOutbox id={self.id} blob_id={self.blob_id} status={self.status} attempts={self.attempts}>"


class StorageRefCountAuditModel(Base):
    __tablename__ = "storage_ref_count_audit"

    id = Column(String(36), primary_key=True, default=generate_uuid)
    blob_id = Column(String(36), ForeignKey("storage_blobs.id", ondelete="RESTRICT"), nullable=False, index=True)
    old_count = Column(Integer, nullable=False)
    new_count = Column(Integer, nullable=False)
    repair_reason = Column(Text, nullable=False)
    repaired_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)

    # Relationships
    blob = relationship("StorageBlobModel", back_populates="audit_logs")

    def __repr__(self) -> str:
        return f"<StorageRefCountAudit id={self.id} blob_id={self.blob_id} {self.old_count}->{self.new_count}>"


class StorageUploadIntentModel(Base):
    __tablename__ = "storage_upload_intents"

    id = Column(String(36), primary_key=True, default=generate_uuid)
    blob_id = Column(String(36), ForeignKey("storage_blobs.id", ondelete="RESTRICT"), nullable=False, index=True)
    file_hash = Column(String(64), nullable=False)
    storage_path = Column(String(512), nullable=False)
    job_id = Column(String(36), ForeignKey("generation_jobs.id", ondelete="SET NULL"), nullable=True)
    worker_id = Column(String(64), nullable=False)
    lease_token = Column(String(36), nullable=False)
    status = Column(String(20), default="PENDING_UPLOAD", nullable=False)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    finalized_at = Column(DateTime(timezone=True), nullable=True)

    # Relationships
    blob = relationship("StorageBlobModel", back_populates="upload_intents")

    __table_args__ = (
        Index("idx_upload_intents_blob", "blob_id", "status"),
        Index("idx_upload_intents_hash", "file_hash", "status"),
        Index("idx_upload_intents_expiry", "status", "expires_at"),
        CheckConstraint(
            "status IN ('PENDING_UPLOAD', 'UPLOADED', 'FINALIZED', 'EXPIRED', 'ABORTED')",
            name="chk_upload_intent_status",
        ),
    )

    def __repr__(self) -> str:
        return f"<StorageUploadIntent id={self.id} blob_id={self.blob_id} status={self.status}>"


class StorageOrphanQuarantineModel(Base):
    __tablename__ = "storage_orphan_quarantine"

    id = Column(String(36), primary_key=True, default=generate_uuid)
    storage_path = Column(String(512), nullable=False, unique=True)
    blob_id = Column(String(36), nullable=True)
    file_hash = Column(String(64), nullable=False)
    status = Column(String(20), default="QUARANTINED", nullable=False)
    quarantined_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    evicted_at = Column(DateTime(timezone=True), nullable=True)
    reclaimed_at = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("idx_orphan_quarantine_scan", "status", "quarantined_at"),
        CheckConstraint(
            "status IN ('QUARANTINED', 'RECLAIMED', 'EVICTION_QUEUED', 'EVICTED', 'EVICTION_FAILED')",
            name="chk_quarantine_status",
        ),
    )

    def __repr__(self) -> str:
        return f"<StorageOrphanQuarantine id={self.id} path={self.storage_path} status={self.status}>"

