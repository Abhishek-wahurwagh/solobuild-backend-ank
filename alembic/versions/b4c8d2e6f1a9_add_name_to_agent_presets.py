"""add name to agent presets

Revision ID: b4c8d2e6f1a9
Revises: f3b7d9a1c2e4
Create Date: 2026-10-03

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "b4c8d2e6f1a9"
down_revision: Union[str, Sequence[str], None] = "f3b7d9a1c2e4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "agent_presets",
        sa.Column("name", sa.String(length=255), nullable=False),
    )


def downgrade() -> None:
    op.drop_column("agent_presets", "name")