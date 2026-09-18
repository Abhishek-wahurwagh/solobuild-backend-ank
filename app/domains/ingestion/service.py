from __future__ import annotations

from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.domains.ingestion.models import IngestionBatch, IngestionItem


async def create_ingestion_batch(
    db: AsyncSession,
    *,
    batch_uuid: UUID,
    context_type: str,
    context_id: UUID,
    created_by_user_id: UUID,
    items: list[dict[str, str]],
) -> IngestionBatch:
    batch = IngestionBatch(
        id=batch_uuid,
        context_type=context_type,
        context_id=context_id,
        created_by_user_id=created_by_user_id,
    )
    db.add(batch)
    for item in items:
        db.add(
            IngestionItem(
                batch_id=batch_uuid,
                source_key=item["source_key"],
                display_name=item["display_name"],
                member_path=item.get("member_path"),
            )
        )
    await db.flush()
    return batch
