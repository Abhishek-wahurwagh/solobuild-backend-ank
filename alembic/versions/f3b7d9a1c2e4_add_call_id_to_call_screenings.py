"""add provider call ID to call screenings

Revision ID: f3b7d9a1c2e4
Revises: e4a2b1c8f901
Create Date: 2026-10-02 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "f3b7d9a1c2e4"
down_revision: Union[str, Sequence[str], None] = "e4a2b1c8f901"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("call_screenings", sa.Column("call_id", sa.String(length=255), nullable=True))
    op.create_index("uq_call_screenings_call_id", "call_screenings", ["call_id"], unique=True)


def downgrade() -> None:
    op.drop_index("uq_call_screenings_call_id", table_name="call_screenings")
    op.drop_column("call_screenings", "call_id")