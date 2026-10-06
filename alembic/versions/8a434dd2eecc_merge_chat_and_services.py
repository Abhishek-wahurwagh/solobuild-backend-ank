"""merge_chat_and_services

Revision ID: 8a434dd2eecc
Revises: 9a2c4d6e8f10, e9a5b544f0b1
Create Date: 2026-10-06 18:54:28.771623

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8a434dd2eecc'
down_revision: Union[str, Sequence[str], None] = ('9a2c4d6e8f10', 'e9a5b544f0b1')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
