"""add PAUSED to workflowstepstatus enum

Revision ID: e4a2b1c8f901
Revises: d3f1a2b4c5e6
Create Date: 2026-09-29 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op


revision: str = "e4a2b1c8f901"
down_revision: Union[str, Sequence[str], None] = "d3f1a2b4c5e6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add 'PAUSED' value to postgres enum 'workflowstepstatus'
    op.execute("ALTER TYPE workflowstepstatus ADD VALUE IF NOT EXISTS 'PAUSED'")


def downgrade() -> None:
    # PostgreSQL doesn't support removing an enum value easily without recreating the enum type
    pass
