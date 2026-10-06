"""
Chat Tool Registry
==================
Defines every function Gemini is allowed to call, grouped by domain.

Architecture notes
------------------
* Each tool is a plain async Python function. No HTTP calls; tools call
  the same underlying service functions that the FastAPI routers use.
* `TOOL_DEFINITIONS` is the Gemini function-declaration list sent on every
  request. Keep it minimal — every unused declaration wastes tokens.
* `dispatch` is the single entry point for the agentic loop. It routes
  a Gemini function-call part to the correct Python function and returns a
  serialisable dict that goes back to Gemini as a tool-response part.
* Calling workflows are intentionally excluded until they are complete.
* To add a new tool: (1) write the async fn, (2) add its declaration to
  TOOL_DEFINITIONS, (3) register it in _REGISTRY.
"""
from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domains.campaigns.models import Campaign, Candidate
from app.domains.campaigns.service import (
    create_batch_tracker,
    enqueue_campaign_screening,
    get_batch_status,
)
from app.core.redis import get_redis_client
from app.domains.users.models import User
from uuid6 import uuid7

logger = logging.getLogger(__name__)


# ===========================================================================
# Tool Implementation Functions
# ===========================================================================

async def tool_list_campaigns(
    db: AsyncSession,
    user: User,
    **_: Any,
) -> list[dict]:
    """Return a compact list of the user's campaigns (id + title only)."""
    result = await db.execute(
        select(Campaign.id, Campaign.title, Campaign.created_at)
        .where(Campaign.created_by_user_id == user.id)
        .order_by(Campaign.created_at.desc())
        .limit(20)  # hard cap — Gemini doesn't need 500 entries
    )
    rows = result.all()
    return [
        {"id": str(r.id), "title": r.title, "created_at": r.created_at.isoformat()}
        for r in rows
    ]


async def tool_get_campaign(
    db: AsyncSession,
    user: User,
    campaign_id: str,
    **_: Any,
) -> dict:
    """Return details for a single campaign including required_fields."""
    result = await db.execute(
        select(Campaign).where(
            Campaign.id == UUID(campaign_id),
            Campaign.created_by_user_id == user.id,
        )
    )
    campaign = result.scalar_one_or_none()
    if campaign is None:
        return {"error": f"Campaign {campaign_id!r} not found."}
    return {
        "id": str(campaign.id),
        "title": campaign.title,
        "required_fields": campaign.required_fields or {},
        "created_at": campaign.created_at.isoformat(),
    }


async def tool_get_candidates(
    db: AsyncSession,
    user: User,
    campaign_id: str,
    limit: int = 20,
    **_: Any,
) -> list[dict]:
    """
    Return a compact candidate list for a campaign.
    Deliberately omits raw extracted_fields to keep the token count low;
    Gemini only needs name / status to make routing decisions.
    """
    # Ownership check
    campaign_check = await db.execute(
        select(Campaign.id).where(
            Campaign.id == UUID(campaign_id),
            Campaign.created_by_user_id == user.id,
        )
    )
    if campaign_check.scalar_one_or_none() is None:
        return [{"error": f"Campaign {campaign_id!r} not found."}]

    result = await db.execute(
        select(
            Candidate.id,
            Candidate.name,
            Candidate.email,
            Candidate.workflow_step,
            Candidate.step_status,
        )
        .where(Candidate.campaign_id == UUID(campaign_id))
        .limit(max(1, min(limit, 50)))  # clamp 1–50
    )
    rows = result.all()
    return [
        {
            "id": str(r.id),
            "name": r.name,
            "email": r.email,
            "workflow_step": r.workflow_step,
            "step_status": r.step_status,
        }
        for r in rows
    ]


async def tool_screen_candidates(
    db: AsyncSession,
    user: User,
    campaign_id: str,
    candidate_ids: list[str] | None = None,
    **_: Any,
) -> dict:
    """Enqueue document screening for a campaign. Returns batch_id + status."""
    campaign_check = await db.execute(
        select(Campaign.id).where(
            Campaign.id == UUID(campaign_id),
            Campaign.created_by_user_id == user.id,
        )
    )
    if campaign_check.scalar_one_or_none() is None:
        return {"error": f"Campaign {campaign_id!r} not found."}

    batch_id = f"screen_{uuid7()}"
    redis = await get_redis_client()
    try:
        await create_batch_tracker(redis, batch_id, file_count=0)
        await enqueue_campaign_screening(
            redis,
            batch_id=batch_id,
            campaign_id=campaign_id,
            candidate_ids=candidate_ids,
        )
    finally:
        await redis.aclose()

    return {
        "batch_id": batch_id,
        "status": "QUEUED",
        "campaign_id": campaign_id,
    }


async def tool_get_batch_status(
    batch_id: str,
    **_: Any,
) -> dict:
    """Return the current processing status of any batch (upload / screen)."""
    redis = await get_redis_client()
    try:
        data = await get_batch_status(redis, batch_id)
    finally:
        await redis.aclose()

    if data is None:
        return {"error": f"Batch {batch_id!r} not found."}

    return {
        "batch_id": batch_id,
        "status": data.get("status", "UNKNOWN"),
        "total": int(data.get("total_candidates", 0)),
        "processed": int(data.get("processed", 0)),
        "failed": int(data.get("failed", 0)),
        "finished_at": data.get("finished_at"),
    }


# ===========================================================================
# Tool Registry  (name → callable)
# ===========================================================================

_REGISTRY: dict[str, Any] = {
    "list_campaigns": tool_list_campaigns,
    "get_campaign": tool_get_campaign,
    "get_candidates": tool_get_candidates,
    "screen_candidates": tool_screen_candidates,
    "get_batch_status": tool_get_batch_status,
}


async def dispatch(
    name: str,
    args: dict[str, Any],
    *,
    db: AsyncSession,
    user: User,
) -> Any:
    """
    Route a Gemini function-call to the correct implementation.
    Returns a JSON-serialisable value. Errors are returned as
    {"error": "..."} dicts so Gemini can relay them to the user gracefully.
    """
    fn = _REGISTRY.get(name)
    if fn is None:
        logger.warning("Chat tool dispatch: unknown tool %r", name)
        return {"error": f"Unknown tool: {name!r}"}

    try:
        return await fn(db=db, user=user, **args)
    except Exception as exc:
        logger.exception("Chat tool %r raised an exception", name)
        return {"error": str(exc)}


# ===========================================================================
# Gemini Function Declarations  (sent to the model on every request)
# ===========================================================================

TOOL_DEFINITIONS = [
    {
        "name": "list_campaigns",
        "description": (
            "List the user's recruitment campaigns. Call this first when the user "
            "refers to 'a campaign' without specifying an ID, to discover available "
            "campaigns and their IDs."
        ),
        "parameters": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "get_campaign",
        "description": "Fetch full details of a specific campaign by its UUID.",
        "parameters": {
            "type": "object",
            "properties": {
                "campaign_id": {
                    "type": "string",
                    "description": "The UUID of the campaign.",
                }
            },
            "required": ["campaign_id"],
        },
    },
    {
        "name": "get_candidates",
        "description": (
            "List candidates for a campaign. Returns id, name, email, and current "
            "workflow status. Use limit to control result size (max 50)."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "campaign_id": {
                    "type": "string",
                    "description": "The UUID of the campaign.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max number of candidates to return (1-50). Default 20.",
                },
            },
            "required": ["campaign_id"],
        },
    },
    {
        "name": "screen_candidates",
        "description": (
            "Enqueue document screening for candidates in a campaign. "
            "If candidate_ids is omitted, all unscreened candidates are processed. "
            "Returns a batch_id you can track with get_batch_status."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "campaign_id": {
                    "type": "string",
                    "description": "The UUID of the campaign.",
                },
                "candidate_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional list of specific candidate UUIDs to screen.",
                },
            },
            "required": ["campaign_id"],
        },
    },
    {
        "name": "get_batch_status",
        "description": (
            "Check the processing status of a batch (upload or screening). "
            "Use the batch_id returned by screen_candidates or upload operations."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "batch_id": {
                    "type": "string",
                    "description": "The batch ID string (e.g. 'screen_...' or 'batch_...').",
                }
            },
            "required": ["batch_id"],
        },
    },
]
