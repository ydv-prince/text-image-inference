"""add_storage_blobs_and_deletion_outbox

Revision ID: 8b2c3d4e5f60
Revises: 7a1b9c2d3e4f
Create Date: 2026-10-25 23:10:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8b2c3d4e5f60'
down_revision: Union[str, Sequence[str], None] = '7a1b9c2d3e4f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    is_postgres = bind.dialect.name == "postgresql"

    # 1. Create storage_blobs table
    op.create_table(
        "storage_blobs",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("file_hash", sa.String(64), nullable=False),
        sa.Column("storage_path", sa.String(512), nullable=False, unique=True),
        sa.Column("storage_provider", sa.String(32), nullable=False, server_default="local_cas"),
        sa.Column("file_size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("mime_type", sa.String(100), nullable=False, server_default="image/png"),
        sa.Column("status", sa.String(30), nullable=False, server_default="PENDING_UPLOAD"),
        sa.Column("active_ref_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(
            "status IN ('PENDING_UPLOAD', 'AVAILABLE', 'DELETION_QUEUED', 'DELETION_IN_PROGRESS', 'PURGED')",
            name="chk_blob_status",
        ),
        sa.CheckConstraint("active_ref_count >= 0", name="chk_ref_count_non_negative"),
        sa.CheckConstraint("file_size_bytes > 0 AND file_size_bytes <= 52428800", name="chk_blob_file_size"),
        sa.CheckConstraint("mime_type IN ('image/png', 'image/jpeg', 'image/webp')", name="chk_blob_mime_type"),
    )
    op.create_index("idx_blobs_hash_status", "storage_blobs", ["file_hash", "status"])
    op.create_index("idx_blobs_status", "storage_blobs", ["status"])

    # 2. Create storage_deletion_outbox table
    op.create_table(
        "storage_deletion_outbox",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("blob_id", sa.String(36), sa.ForeignKey("storage_blobs.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("file_hash", sa.String(64), nullable=False),
        sa.Column("storage_path", sa.String(512), nullable=False),
        sa.Column("fencing_token", sa.String(36), nullable=False),
        sa.Column("status", sa.String(20), nullable=False, server_default="PENDING"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default="5"),
        sa.Column("next_retry_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("locked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("locked_by", sa.String(64), nullable=True),
        sa.Column("cancel_reason", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status IN ('PENDING', 'PROCESSING', 'COMPLETED', 'CANCELLED', 'FAILED')",
            name="chk_deletion_outbox_status",
        ),
    )
    op.create_index("idx_deletion_outbox_pickup", "storage_deletion_outbox", ["status", "next_retry_at"])
    op.create_index("ix_storage_deletion_outbox_blob_id", "storage_deletion_outbox", ["blob_id"])

    # 3. Create storage_upload_intents table
    op.create_table(
        "storage_upload_intents",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("blob_id", sa.String(36), sa.ForeignKey("storage_blobs.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("file_hash", sa.String(64), nullable=False),
        sa.Column("storage_path", sa.String(512), nullable=False),
        sa.Column("job_id", sa.String(36), sa.ForeignKey("generation_jobs.id", ondelete="SET NULL"), nullable=True),
        sa.Column("worker_id", sa.String(64), nullable=False),
        sa.Column("lease_token", sa.String(36), nullable=False),
        sa.Column("status", sa.String(20), nullable=False, server_default="PENDING_UPLOAD"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finalized_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status IN ('PENDING_UPLOAD', 'UPLOADED', 'FINALIZED', 'EXPIRED', 'ABORTED')",
            name="chk_upload_intent_status",
        ),
    )
    op.create_index("idx_upload_intents_blob", "storage_upload_intents", ["blob_id", "status"])
    op.create_index("idx_upload_intents_hash", "storage_upload_intents", ["file_hash", "status"])
    op.create_index("idx_upload_intents_expiry", "storage_upload_intents", ["status", "expires_at"])

    # 4. Create storage_ref_count_audit table
    op.create_table(
        "storage_ref_count_audit",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("blob_id", sa.String(36), sa.ForeignKey("storage_blobs.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("old_count", sa.Integer(), nullable=False),
        sa.Column("new_count", sa.Integer(), nullable=False),
        sa.Column("repair_reason", sa.Text(), nullable=False),
        sa.Column("repaired_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_storage_ref_count_audit_blob_id", "storage_ref_count_audit", ["blob_id"])

    # 5. Create storage_orphan_quarantine table
    op.create_table(
        "storage_orphan_quarantine",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("storage_path", sa.String(512), nullable=False, unique=True),
        sa.Column("blob_id", sa.String(36), nullable=True),
        sa.Column("file_hash", sa.String(64), nullable=False),
        sa.Column("status", sa.String(20), nullable=False, server_default="QUARANTINED"),
        sa.Column("quarantined_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("evicted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reclaimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status IN ('QUARANTINED', 'RECLAIMED', 'EVICTION_QUEUED', 'EVICTED', 'EVICTION_FAILED')",
            name="chk_quarantine_status",
        ),
    )
    op.create_index("idx_orphan_quarantine_scan", "storage_orphan_quarantine", ["status", "quarantined_at"])

    # 6. Alter generated_assets to add blob_id using batch_alter_table for SQLite compatibility
    with op.batch_alter_table("generated_assets") as batch_op:
        batch_op.add_column(sa.Column("blob_id", sa.String(36), nullable=True))
        batch_op.create_foreign_key(
            "fk_generated_assets_blob_id", "storage_blobs", ["blob_id"], ["id"], ondelete="RESTRICT"
        )
        batch_op.create_index("ix_generated_assets_blob_id", ["blob_id"])


    # 7. PostgreSQL-specific DDL: partial unique indexes, functions, triggers, and RLS
    if is_postgres:
        # Partial unique index for active outbox tasks per blob
        op.execute("""
            CREATE UNIQUE INDEX uq_active_deletion_per_blob 
            ON storage_deletion_outbox (blob_id) 
            WHERE status IN ('PENDING', 'PROCESSING');
        """)

        # Helper function: Advisory lock key derivation
        op.execute("""
            CREATE OR REPLACE FUNCTION fn_hash_to_lock_key(p_hash VARCHAR(64))
            RETURNS bigint AS $$
            DECLARE
                b bytea;
            BEGIN
                b := decode(substring(p_hash FROM 1 FOR 16), 'hex');
                RETURN (
                    (get_byte(b, 0)::bigint << 56) |
                    (get_byte(b, 1)::bigint << 48) |
                    (get_byte(b, 2)::bigint << 40) |
                    (get_byte(b, 3)::bigint << 32) |
                    (get_byte(b, 4)::bigint << 24) |
                    (get_byte(b, 5)::bigint << 16) |
                    (get_byte(b, 6)::bigint << 8)  |
                    (get_byte(b, 7)::bigint)
                );
            END;
            $$ LANGUAGE plpgsql IMMUTABLE STRICT;
        """)

        # Trigger 1: Legacy Asset Writer Fallback
        op.execute("""
            CREATE OR REPLACE FUNCTION fn_legacy_asset_writer_fallback()
            RETURNS TRIGGER AS $$
            DECLARE
                v_blob_id VARCHAR(36);
            BEGIN
                IF NEW.blob_id IS NULL THEN
                    SELECT id INTO v_blob_id
                    FROM storage_blobs
                    WHERE file_hash = NEW.file_hash 
                      AND storage_path = NEW.storage_path
                      AND status = 'AVAILABLE'
                    LIMIT 1
                    FOR UPDATE;

                    IF v_blob_id IS NOT NULL THEN
                        NEW.blob_id := v_blob_id;
                    END IF;
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;

            CREATE TRIGGER trg_10_legacy_asset_writer_fallback
            BEFORE INSERT ON generated_assets
            FOR EACH ROW EXECUTE FUNCTION fn_legacy_asset_writer_fallback();
        """)

        # Trigger 2: Enforce AVAILABLE Blob Status (Transitional)
        op.execute("""
            CREATE OR REPLACE FUNCTION fn_enforce_blob_available_transitional()
            RETURNS TRIGGER AS $$
            DECLARE
                v_blob_status VARCHAR(30);
            BEGIN
                IF NEW.blob_id IS NOT NULL THEN
                    SELECT status INTO v_blob_status
                    FROM storage_blobs
                    WHERE id = NEW.blob_id
                    FOR UPDATE;

                    IF NOT FOUND THEN
                        RAISE EXCEPTION 'Referenced storage_blob id % does not exist', NEW.blob_id
                            USING ERRCODE = 'foreign_key_violation';
                    END IF;

                    IF v_blob_status <> 'AVAILABLE' THEN
                        RAISE EXCEPTION 'Cannot reference storage_blob id % with status % (must be AVAILABLE)', 
                            NEW.blob_id, v_blob_status
                            USING ERRCODE = 'check_violation';
                    END IF;
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;

            CREATE TRIGGER trg_20_enforce_blob_available
            BEFORE INSERT OR UPDATE OF blob_id ON generated_assets
            FOR EACH ROW EXECUTE FUNCTION fn_enforce_blob_available_transitional();
        """)

        # Trigger 3: Prevent Blob Mutation once assigned
        op.execute("""
            CREATE OR REPLACE FUNCTION fn_prevent_blob_mutation()
            RETURNS TRIGGER AS $$
            BEGIN
                IF OLD.blob_id IS NOT NULL AND NEW.blob_id IS DISTINCT FROM OLD.blob_id THEN
                    RAISE EXCEPTION 'blob_id on generated_assets is immutable once assigned (attempted to reassign from % to %)',
                        OLD.blob_id, NEW.blob_id
                        USING ERRCODE = 'restrict_violation';
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;

            CREATE TRIGGER trg_30_prevent_blob_mutation
            BEFORE UPDATE OF blob_id ON generated_assets
            FOR EACH ROW EXECUTE FUNCTION fn_prevent_blob_mutation();
        """)

        # Trigger 4: Sync Ref Count on INSERT
        op.execute("""
            CREATE OR REPLACE FUNCTION fn_sync_ref_count_insert()
            RETURNS TRIGGER AS $$
            BEGIN
                IF NEW.blob_id IS NOT NULL THEN
                    UPDATE storage_blobs
                    SET active_ref_count = active_ref_count + 1, updated_at = NOW()
                    WHERE id = NEW.blob_id;
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;

            CREATE TRIGGER trg_40_sync_ref_count_insert
            AFTER INSERT ON generated_assets
            FOR EACH ROW EXECUTE FUNCTION fn_sync_ref_count_insert();
        """)

        # Trigger 5: Sync Ref Count on UPDATE of blob_id (Backfill)
        op.execute("""
            CREATE OR REPLACE FUNCTION fn_sync_ref_count_update()
            RETURNS TRIGGER AS $$
            BEGIN
                IF OLD.blob_id IS NULL AND NEW.blob_id IS NOT NULL THEN
                    UPDATE storage_blobs
                    SET active_ref_count = active_ref_count + 1, updated_at = NOW()
                    WHERE id = NEW.blob_id;
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;

            CREATE TRIGGER trg_50_sync_ref_count_update
            AFTER UPDATE OF blob_id ON generated_assets
            FOR EACH ROW EXECUTE FUNCTION fn_sync_ref_count_update();
        """)

        # Trigger 6: Sync Ref Count on DELETE / CASCADE
        op.execute("""
            CREATE OR REPLACE FUNCTION fn_sync_ref_count_delete()
            RETURNS TRIGGER AS $$
            BEGIN
                IF OLD.blob_id IS NOT NULL THEN
                    UPDATE storage_blobs
                    SET active_ref_count = active_ref_count - 1, updated_at = NOW()
                    WHERE id = OLD.blob_id;
                END IF;
                RETURN OLD;
            END;
            $$ LANGUAGE plpgsql;

            CREATE TRIGGER trg_60_sync_ref_count_delete
            AFTER DELETE ON generated_assets
            FOR EACH ROW EXECUTE FUNCTION fn_sync_ref_count_delete();
        """)

        # Trigger 7: Prohibit hard-delete of storage_blobs
        op.execute("""
            CREATE OR REPLACE FUNCTION fn_prevent_hard_delete_storage_blobs()
            RETURNS TRIGGER AS $$
            BEGIN
                RAISE EXCEPTION 'Hard deletion of storage_blobs is prohibited. Transition status to PURGED instead.'
                    USING ERRCODE = 'restrict_violation';
            END;
            $$ LANGUAGE plpgsql;

            CREATE TRIGGER trg_prevent_hard_delete_storage_blobs
            BEFORE DELETE ON storage_blobs
            FOR EACH ROW EXECUTE FUNCTION fn_prevent_hard_delete_storage_blobs();
        """)

        # Enable RLS on newly created tables
        for table in [
            "storage_blobs",
            "storage_deletion_outbox",
            "storage_upload_intents",
            "storage_ref_count_audit",
            "storage_orphan_quarantine",
        ]:
            op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;")
            op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY;")


def downgrade() -> None:
    bind = op.get_bind()
    is_postgres = bind.dialect.name == "postgresql"

    if is_postgres:
        op.execute("DROP TRIGGER IF EXISTS trg_prevent_hard_delete_storage_blobs ON storage_blobs;")
        op.execute("DROP FUNCTION IF EXISTS fn_prevent_hard_delete_storage_blobs();")

        op.execute("DROP TRIGGER IF EXISTS trg_60_sync_ref_count_delete ON generated_assets;")
        op.execute("DROP FUNCTION IF EXISTS fn_sync_ref_count_delete();")

        op.execute("DROP TRIGGER IF EXISTS trg_50_sync_ref_count_update ON generated_assets;")
        op.execute("DROP FUNCTION IF EXISTS fn_sync_ref_count_update();")

        op.execute("DROP TRIGGER IF EXISTS trg_40_sync_ref_count_insert ON generated_assets;")
        op.execute("DROP FUNCTION IF EXISTS fn_sync_ref_count_insert();")

        op.execute("DROP TRIGGER IF EXISTS trg_30_prevent_blob_mutation ON generated_assets;")
        op.execute("DROP FUNCTION IF EXISTS fn_prevent_blob_mutation();")

        op.execute("DROP TRIGGER IF EXISTS trg_20_enforce_blob_available ON generated_assets;")
        op.execute("DROP FUNCTION IF EXISTS fn_enforce_blob_available_transitional();")

        op.execute("DROP TRIGGER IF EXISTS trg_10_legacy_asset_writer_fallback ON generated_assets;")
        op.execute("DROP FUNCTION IF EXISTS fn_legacy_asset_writer_fallback();")

        op.execute("DROP FUNCTION IF EXISTS fn_hash_to_lock_key(VARCHAR);")
        op.execute("DROP INDEX IF EXISTS uq_active_deletion_per_blob;")

    with op.batch_alter_table("generated_assets") as batch_op:
        batch_op.drop_index("ix_generated_assets_blob_id")
        batch_op.drop_constraint("fk_generated_assets_blob_id", type_="foreignkey")
        batch_op.drop_column("blob_id")


    op.drop_table("storage_orphan_quarantine")
    op.drop_table("storage_ref_count_audit")
    op.drop_table("storage_upload_intents")
    op.drop_table("storage_deletion_outbox")
    op.drop_table("storage_blobs")
