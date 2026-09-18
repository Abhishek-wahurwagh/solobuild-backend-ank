"""add reusable ingestion state and candidate source references

Revision ID: d3f1a2b4c5e6
Revises: cc6f991f975a
Create Date: 2026-09-18 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "d3f1a2b4c5e6"
down_revision: Union[str, Sequence[str], None] = "cc6f991f975a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "ingestion_batches",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("context_type", sa.String(length=100), nullable=False),
        sa.Column("context_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("created_by_user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["created_by_user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_ingestion_batches_context_id", "ingestion_batches", ["context_id"], unique=False)
    op.create_index("ix_ingestion_batches_created_by_user_id", "ingestion_batches", ["created_by_user_id"], unique=False)

    op.create_table(
        "ingestion_items",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("batch_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_key", sa.Text(), nullable=False),
        sa.Column("display_name", sa.Text(), nullable=False),
        sa.Column("member_path", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("current_stage", sa.String(length=100), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["batch_id"], ["ingestion_batches.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("batch_id", "source_key", "member_path", name="uq_ingestion_item_source"),
    )
    op.create_index("ix_ingestion_items_batch_id", "ingestion_items", ["batch_id"], unique=False)

    op.add_column("candidates", sa.Column("file_url", sa.Text(), nullable=True))
    op.add_column("candidates", sa.Column("ingestion_item_id", postgresql.UUID(as_uuid=True), nullable=True))
    op.create_foreign_key(
        "fk_candidates_ingestion_item_id",
        "candidates",
        "ingestion_items",
        ["ingestion_item_id"],
        ["id"],
    )
    op.create_unique_constraint("uq_candidates_ingestion_item_id", "candidates", ["ingestion_item_id"])


def downgrade() -> None:
    op.drop_constraint("uq_candidates_ingestion_item_id", "candidates", type_="unique")
    op.drop_constraint("fk_candidates_ingestion_item_id", "candidates", type_="foreignkey")
    op.drop_column("candidates", "ingestion_item_id")
    op.drop_column("candidates", "file_url")
    op.drop_index("ix_ingestion_items_batch_id", table_name="ingestion_items")
    op.drop_table("ingestion_items")
    op.drop_index("ix_ingestion_batches_created_by_user_id", table_name="ingestion_batches")
    op.drop_index("ix_ingestion_batches_context_id", table_name="ingestion_batches")
    op.drop_table("ingestion_batches")
