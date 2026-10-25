"""add_scheduled_at_and_is_synthetic

Revision ID: 6f38b2518e1a
Revises: 574b357a57cc
Create Date: 2026-10-22 14:10:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6f38b2518e1a'
down_revision: Union[str, Sequence[str], None] = '574b357a57cc'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema with scheduled_at and is_synthetic."""
    with op.batch_alter_table('generation_jobs', schema=None) as batch_op:
        batch_op.add_column(sa.Column('scheduled_at', sa.DateTime(timezone=True), nullable=True))
        batch_op.create_index(batch_op.f('ix_generation_jobs_scheduled_at'), ['scheduled_at'], unique=False)
        batch_op.create_index('idx_jobs_queue_pickup_scheduled', ['status', 'scheduled_at', 'created_at'], unique=False)

    with op.batch_alter_table('generated_assets', schema=None) as batch_op:
        batch_op.add_column(sa.Column('is_synthetic', sa.Boolean(), server_default=sa.false(), nullable=False))


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table('generated_assets', schema=None) as batch_op:
        batch_op.drop_column('is_synthetic')

    with op.batch_alter_table('generation_jobs', schema=None) as batch_op:
        batch_op.drop_index('idx_jobs_queue_pickup_scheduled')
        batch_op.drop_index(batch_op.f('ix_generation_jobs_scheduled_at'))
        batch_op.drop_column('scheduled_at')
