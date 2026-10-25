"""enable_postgresql_rls_and_policies

Revision ID: 7a1b9c2d3e4f
Revises: 6f38b2518e1a
Create Date: 2026-10-22 15:15:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '7a1b9c2d3e4f'
down_revision: Union[str, Sequence[str], None] = '6f38b2518e1a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Enables PostgreSQL Row-Level Security and tenant isolation policies."""
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        # 1. Enable RLS and Force RLS (applies to table owner as well)
        op.execute("ALTER TABLE generation_jobs ENABLE ROW LEVEL SECURITY;")
        op.execute("ALTER TABLE generation_jobs FORCE ROW LEVEL SECURITY;")
        op.execute("ALTER TABLE generated_assets ENABLE ROW LEVEL SECURITY;")
        op.execute("ALTER TABLE generated_assets FORCE ROW LEVEL SECURITY;")
        op.execute("ALTER TABLE api_keys ENABLE ROW LEVEL SECURITY;")
        op.execute("ALTER TABLE api_keys FORCE ROW LEVEL SECURITY;")

        # 2. Add tenant isolation policies for client PostgREST roles
        op.execute("""
            CREATE POLICY tenant_isolation_jobs ON generation_jobs
            FOR ALL
            USING (api_key_id = current_setting('request.jwt.claim.api_key_id', true));
        """)

        op.execute("""
            CREATE POLICY tenant_isolation_assets ON generated_assets
            FOR ALL
            USING (job_id IN (
                SELECT id FROM generation_jobs
                WHERE api_key_id = current_setting('request.jwt.claim.api_key_id', true)
            ));
        """)

        op.execute("""
            CREATE POLICY tenant_isolation_keys ON api_keys
            FOR SELECT
            USING (id = current_setting('request.jwt.claim.api_key_id', true));
        """)


def downgrade() -> None:
    """Disables PostgreSQL Row-Level Security and drops tenant isolation policies."""
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute("DROP POLICY IF EXISTS tenant_isolation_keys ON api_keys;")
        op.execute("DROP POLICY IF EXISTS tenant_isolation_assets ON generated_assets;")
        op.execute("DROP POLICY IF EXISTS tenant_isolation_jobs ON generation_jobs;")

        op.execute("ALTER TABLE api_keys NO FORCE ROW LEVEL SECURITY;")
        op.execute("ALTER TABLE api_keys DISABLE ROW LEVEL SECURITY;")
        op.execute("ALTER TABLE generated_assets NO FORCE ROW LEVEL SECURITY;")
        op.execute("ALTER TABLE generated_assets DISABLE ROW LEVEL SECURITY;")
        op.execute("ALTER TABLE generation_jobs NO FORCE ROW LEVEL SECURITY;")
        op.execute("ALTER TABLE generation_jobs DISABLE ROW LEVEL SECURITY;")
