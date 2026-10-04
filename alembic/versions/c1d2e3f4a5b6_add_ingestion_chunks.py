"""add durable CSV chunk state for idempotent ingestion

Revision ID: c1d2e3f4a5b6
Revises: b4c8d2e6f1a9
Create Date: 2026-10-04 19:55:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "c1d2e3f4a5b6"
down_revision: Union[str, Sequence[str], None] = "b4c8d2e6f1a9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "candidates",
        sa.Column("source_row_key", sa.String(length=255), nullable=True),
    )
    op.create_unique_constraint(
        "uq_candidates_source_row_key",
        "candidates",
        ["source_row_key"],
    )
    op.create_table(
        "ingestion_chunks",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("ingestion_item_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("chunk_index", sa.Integer(), nullable=False),
        sa.Column("row_start", sa.Integer(), nullable=False),
        sa.Column("row_count", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("result", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["ingestion_item_id"], ["ingestion_items.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("ingestion_item_id", "chunk_index", name="uq_ingestion_chunk_item_index"),
    )
    op.create_index("ix_ingestion_chunks_ingestion_item_id", "ingestion_chunks", ["ingestion_item_id"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_ingestion_chunks_ingestion_item_id", table_name="ingestion_chunks")
    op.drop_table("ingestion_chunks")
    op.drop_constraint("uq_candidates_source_row_key", "candidates", type_="unique")
    op.drop_column("candidates", "source_row_key")
