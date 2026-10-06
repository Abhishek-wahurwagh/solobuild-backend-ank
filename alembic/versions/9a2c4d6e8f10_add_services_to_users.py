"""add services to users

Revision ID: 9a2c4d6e8f10
Revises: f3b7d9a1c2e4
Create Date: 2026-10-06 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "9a2c4d6e8f10"
down_revision: Union[str, Sequence[str], None] = "f3b7d9a1c2e4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


user_service = postgresql.ENUM(
    "jd-and-resumes-extraction",
    "resume-screening",
    "outbout-calling",
    name="userservice",
)


def upgrade() -> None:
    user_service.create(op.get_bind(), checkfirst=True)
    op.add_column(
        "users",
        sa.Column(
            "services",
            postgresql.ARRAY(user_service),
            server_default=sa.text("ARRAY[]::userservice[]"),
            nullable=False,
        ),
    )


def downgrade() -> None:
    op.drop_column("users", "services")
    user_service.drop(op.get_bind(), checkfirst=True)
