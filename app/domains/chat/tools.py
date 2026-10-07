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

from app.domains.agents.models import Agent, AgentPreset
from app.domains.campaigns.models import Campaign, Candidate, DocumentScreening
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

async def _get_owned_campaign(
    db: AsyncSession,
    user: User,
    campaign_id: str,
) -> Campaign | None:
    try:
        parsed_campaign_id = UUID(campaign_id)
    except (TypeError, ValueError):
        return None
    result = await db.execute(
        select(Campaign).where(
            Campaign.id == parsed_campaign_id,
            Campaign.created_by_user_id == user.id,
        )
    )
    return result.scalar_one_or_none()

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
    campaign = await _get_owned_campaign(db, user, campaign_id)
    if campaign is None:
        return {"error": f"Campaign {campaign_id!r} not found."}
    return {
        "id": str(campaign.id),
        "title": campaign.title,
        "required_fields": campaign.required_fields or {},
        "created_at": campaign.created_at.isoformat(),
    }


async def tool_list_agents(
    db: AsyncSession,
    user: User,
    **_: Any,
) -> list[dict]:
    """List selectable user-owned and preset AI recruiters."""
    user_result = await db.execute(
        select(Agent.id, Agent.name)
        .where(Agent.created_by_user_id == user.id)
        .order_by(Agent.name)
    )
    preset_result = await db.execute(
        select(AgentPreset.id, AgentPreset.name).order_by(AgentPreset.name)
    )
    return [
        {"id": str(row.id), "name": row.name, "is_preset": False}
        for row in user_result.all()
    ] + [
        {"id": str(row.id), "name": row.name, "is_preset": True}
        for row in preset_result.all()
    ]


async def tool_update_campaign_agent(
    db: AsyncSession,
    user: User,
    campaign_id: str,
    agent_id: str,
    **_: Any,
) -> dict:
    """Assign a user-owned or preset AI recruiter to an owned campaign."""
    campaign = await _get_owned_campaign(db, user, campaign_id)
    if campaign is None:
        return {"error": f"Campaign {campaign_id!r} not found."}
    try:
        parsed_agent_id = UUID(agent_id)
    except (TypeError, ValueError):
        return {"error": f"AI recruiter {agent_id!r} not found."}

    agent_result = await db.execute(
        select(Agent.id).where(
            Agent.id == parsed_agent_id,
            Agent.created_by_user_id == user.id,
        )
    )
    preset_result = await db.execute(
        select(AgentPreset.id).where(AgentPreset.id == parsed_agent_id)
    )
    if agent_result.scalar_one_or_none() is None and preset_result.scalar_one_or_none() is None:
        return {"error": f"AI recruiter {agent_id!r} not found."}

    campaign.agent_id = parsed_agent_id
    await db.commit()
    await db.refresh(campaign)
    return {
        "campaign_id": str(campaign.id),
        "campaign_title": campaign.title,
        "agent_id": str(parsed_agent_id),
        "status": "UPDATED",
    }


async def tool_get_candidates(
    db: AsyncSession,
    user: User,
    campaign_id: str,
    limit: int = 20,
    **_: Any,
) -> dict:
    """
    Return a compact candidate list for a campaign.
    Deliberately omits raw extracted_fields to keep the token count low;
    Gemini only needs name / status to make routing decisions.
    """
    campaign = await _get_owned_campaign(db, user, campaign_id)
    if campaign is None:
        return {"error": f"Campaign {campaign_id!r} not found."}

    result = await db.execute(
        select(
            Candidate.id,
            Candidate.name,
            Candidate.email,
            Candidate.workflow_step,
            Candidate.step_status,
        )
        .where(Candidate.campaign_id == UUID(campaign_id))
        .order_by(Candidate.created_at.desc())
        .limit(max(1, min(limit, 100)))  # clamp 1–100
    )
    rows = result.all()
    candidates = [
        {
            "id": str(r.id),
            "name": r.name,
            "email": r.email,
            "workflow_step": r.workflow_step,
            "step_status": r.step_status.value,
        }
        for r in rows
    ]
    return {
        "campaign_id": str(campaign.id),
        "campaign_title": campaign.title,
        "candidates": candidates,
        "total": len(candidates),
    }


async def tool_get_document_screenings(
    db: AsyncSession,
    user: User,
    campaign_id: str,
    limit: int = 50,
    **_: Any,
) -> dict:
    """Return recent document-screening results for a campaign."""
    campaign = await _get_owned_campaign(db, user, campaign_id)
    if campaign is None:
        return {"error": f"Campaign {campaign_id!r} not found."}

    result = await db.execute(
        select(DocumentScreening)
        .where(DocumentScreening.campaign_id == campaign.id)
        .order_by(DocumentScreening.created_at.desc())
        .limit(max(1, min(limit, 100)))
    )
    screenings = result.scalars().all()
    return {
        "campaign_id": str(campaign.id),
        "campaign_title": campaign.title,
        "screenings": [
            {
                "id": str(screening.id),
                "candidate_id": str(screening.candidate_id),
                "match_score": screening.match_score,
                "matched_fields": screening.matched_fields or {},
                "unmatched_fields": screening.unmatched_fields or {},
                "summary": screening.summary,
                "created_at": screening.created_at.isoformat(),
            }
            for screening in screenings
        ],
        "total": len(screenings),
    }


async def tool_get_candidate_document_screenings(
    db: AsyncSession,
    user: User,
    campaign_id: str,
    candidate_id: str,
    limit: int = 10,
    **_: Any,
) -> dict:
    """Return recent document-screening results for one campaign candidate."""
    campaign = await _get_owned_campaign(db, user, campaign_id)
    if campaign is None:
        return {"error": f"Campaign {campaign_id!r} not found."}
    try:
        parsed_candidate_id = UUID(candidate_id)
    except (TypeError, ValueError):
        return {"error": f"Candidate {candidate_id!r} not found in this campaign."}

    candidate_result = await db.execute(
        select(Candidate).where(
            Candidate.id == parsed_candidate_id,
            Candidate.campaign_id == campaign.id,
        )
    )
    candidate = candidate_result.scalar_one_or_none()
    if candidate is None:
        return {"error": f"Candidate {candidate_id!r} not found in this campaign."}

    screening_result = await db.execute(
        select(DocumentScreening)
        .where(
            DocumentScreening.campaign_id == campaign.id,
            DocumentScreening.candidate_id == candidate.id,
        )
        .order_by(DocumentScreening.created_at.desc())
        .limit(max(1, min(limit, 50)))
    )
    screenings = screening_result.scalars().all()
    return {
        "campaign_id": str(campaign.id),
        "campaign_title": campaign.title,
        "candidate": {
            "id": str(candidate.id),
            "name": candidate.name,
            "email": candidate.email,
            "workflow_step": candidate.workflow_step,
            "step_status": candidate.step_status.value,
        },
        "screenings": [
            {
                "id": str(screening.id),
                "match_score": screening.match_score,
                "matched_fields": screening.matched_fields or {},
                "unmatched_fields": screening.unmatched_fields or {},
                "summary": screening.summary,
                "created_at": screening.created_at.isoformat(),
            }
            for screening in screenings
        ],
        "total": len(screenings),
    }


async def tool_find_candidates(
    db: AsyncSession,
    user: User,
    campaign_id: str,
    query: str,
    **_: Any,
) -> dict:
    """Resolve a candidate by name or email, returning possible matches."""
    campaign = await _get_owned_campaign(db, user, campaign_id)
    if campaign is None:
        return {"error": f"Campaign {campaign_id!r} not found."}
    search_term = query.strip()
    if not search_term:
        return {"error": "Provide a candidate name or email to search."}
    result = await db.execute(
        select(
            Candidate.id,
            Candidate.name,
            Candidate.email,
            Candidate.workflow_step,
            Candidate.step_status,
        )
        .where(
            Candidate.campaign_id == campaign.id,
            (Candidate.name.ilike(f"%{search_term}%"))
            | (Candidate.email.ilike(f"%{search_term}%")),
        )
        .order_by(Candidate.name)
        .limit(10)
    )
    matches = [
        {
            "id": str(row.id),
            "name": row.name,
            "email": row.email,
            "workflow_step": row.workflow_step,
            "step_status": row.step_status.value,
        }
        for row in result.all()
    ]
    return {
        "campaign_id": str(campaign.id),
        "campaign_title": campaign.title,
        "query": search_term,
        "matches": matches,
        "total_matches": len(matches),
    }


async def tool_screen_candidates(
    db: AsyncSession,
    user: User,
    campaign_id: str,
    candidate_ids: list[str] | None = None,
    **_: Any,
) -> dict:
    """Enqueue document screening for a campaign. Returns batch_id + status."""
    campaign = await _get_owned_campaign(db, user, campaign_id)
    if campaign is None:
        return {"error": f"Campaign {campaign_id!r} not found."}

    parsed_candidate_ids: list[UUID] | None = None
    if candidate_ids:
        try:
            parsed_candidate_ids = list(dict.fromkeys(UUID(candidate_id) for candidate_id in candidate_ids))
        except (TypeError, ValueError):
            return {"error": "One or more candidate IDs are invalid."}
        candidate_result = await db.execute(
            select(Candidate.id).where(
                Candidate.campaign_id == campaign.id,
                Candidate.id.in_(parsed_candidate_ids),
            )
        )
        if set(candidate_result.scalars().all()) != set(parsed_candidate_ids):
            return {"error": "One or more candidates were not found in this campaign."}

    batch_id = f"screen_{uuid7()}"
    redis = await get_redis_client()
    try:
        await create_batch_tracker(
            redis,
            batch_id,
            file_count=0,
            campaign_id=str(campaign.id),
        )
        await enqueue_campaign_screening(
            redis,
            batch_id=batch_id,
            campaign_id=str(campaign.id),
            candidate_ids=[str(candidate_id) for candidate_id in parsed_candidate_ids]
            if parsed_candidate_ids else None,
        )
    finally:
        await redis.aclose()

    return {
        "batch_id": batch_id,
        "status": "QUEUED",
        "campaign_id": campaign_id,
    }


async def tool_get_batch_status(
    db: AsyncSession,
    user: User,
    campaign_id: str,
    batch_id: str,
    **_: Any,
) -> dict:
    """Return status for a batch belonging to a campaign owned by the user."""
    campaign = await _get_owned_campaign(db, user, campaign_id)
    if campaign is None:
        return {"error": f"Campaign {campaign_id!r} not found."}

    redis = await get_redis_client()
    try:
        data = await get_batch_status(redis, batch_id)
    finally:
        await redis.aclose()

    if data is None:
        return {"error": f"Batch {batch_id!r} not found."}
    if data.get("campaign_id") not in (None, str(campaign.id)):
        return {"error": f"Batch {batch_id!r} not found in this campaign."}

    return {
        "batch_id": batch_id,
        "campaign_id": str(campaign.id),
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
    "list_agents": tool_list_agents,
    "update_campaign_agent": tool_update_campaign_agent,
    "get_candidates": tool_get_candidates,
    "find_candidates": tool_find_candidates,
    "get_document_screenings": tool_get_document_screenings,
    "get_candidate_document_screenings": tool_get_candidate_document_screenings,
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
            "List the user's recruitment campaigns. Use this to answer campaign-list "
            "requests and to resolve a named campaign before any campaign action."
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
        "name": "list_agents",
        "description": "List AI recruiters available to this user, including default preset recruiters.",
        "parameters": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "update_campaign_agent",
        "description": (
            "Assign an available AI recruiter to a campaign. Resolve the campaign and recruiter "
            "from successful list/get tool results first. This changes the campaign immediately."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "campaign_id": {"type": "string", "description": "Campaign UUID."},
                "agent_id": {"type": "string", "description": "AI recruiter UUID from list_agents."},
            },
            "required": ["campaign_id", "agent_id"],
        },
    },
    {
        "name": "get_candidates",
        "description": (
            "List compact candidate identities and workflow statuses for a campaign. "
            "Use this for assistant context; the frontend UI fetches the live candidate list. "
            "The limit is clamped to 1-100."
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
                    "description": "Max number of candidates to return (1-100). Default 20.",
                },
            },
            "required": ["campaign_id"],
        },
    },
    {
        "name": "find_candidates",
        "description": (
            "Find candidates by a user-provided name or email in one campaign. "
            "Use to resolve natural-language candidate references before candidate-specific actions. "
            "If multiple candidates match, ask the user to choose; never pick arbitrarily."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "campaign_id": {"type": "string", "description": "Campaign UUID."},
                "query": {"type": "string", "description": "Candidate name or email fragment."},
            },
            "required": ["campaign_id", "query"],
        },
    },
    {
        "name": "get_document_screenings",
        "description": (
            "Fetch recent resume/document screening results for a campaign. "
            "Use these records to answer questions about actual scores or findings; "
            "the frontend results UI fetches the authoritative live records itself."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "campaign_id": {"type": "string", "description": "Campaign UUID."},
                "limit": {
                    "type": "integer",
                    "description": "Maximum recent records to return (1-100, default 50).",
                },
            },
            "required": ["campaign_id"],
        },
    },
    {
        "name": "get_candidate_document_screenings",
        "description": (
            "Fetch one candidate's recent resume/document screening results within a campaign. "
            "Always verify the candidate belongs to that campaign."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "campaign_id": {"type": "string", "description": "Campaign UUID."},
                "candidate_id": {"type": "string", "description": "Candidate UUID."},
                "limit": {
                    "type": "integer",
                    "description": "Maximum recent records to return (1-50, default 10).",
                },
            },
            "required": ["campaign_id", "candidate_id"],
        },
    },
    {
        "name": "screen_candidates",
        "description": (
            "Enqueue document screening for candidates in a campaign. "
            "If candidate_ids is omitted, all campaign candidates are processed. "
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
            "Check the processing status of a screening batch. The batch must belong "
            "to the specified campaign, and the campaign must belong to the current user."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "batch_id": {
                    "type": "string",
                    "description": "The batch ID returned by screen_candidates.",
                },
                "campaign_id": {
                    "type": "string",
                    "description": "The UUID of the campaign whose batch is being checked.",
                },
            },
            "required": ["campaign_id", "batch_id"],
        },
    },
    {
        "name": "present_ui",
        "description": (
            "Tell the frontend to display an existing hiring interface after you have resolved "
            "the relevant campaign/candidate and fetched facts with backend tools. This does not "
            "perform a business operation. Only use IDs returned by tools in this turn."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "type": {
                    "type": "string",
                    "enum": [
                        "SHOW_CAMPAIGN_LIST",
                        "SHOW_CAMPAIGN_DETAIL",
                        "SHOW_CAMPAIGN_CREATE_FORM",
                        "SHOW_CANDIDATE_LIST",
                        "SHOW_CANDIDATE_UPLOAD",
                        "SHOW_SCREENING_STATUS",
                        "SHOW_BATCH_STATUS",
                        "SHOW_CAMPAIGN_PICKER",
                        "SHOW_SCREENING_RESULTS",
                        "SHOW_CANDIDATE_SCREENING_RESULT",
                    ],
                },
                "payload": {
                    "type": "object",
                    "properties": {
                        "campaign_id": {"type": "string"},
                        "campaign_title": {"type": "string"},
                        "candidate_id": {"type": "string"},
                        "batch_id": {"type": "string"},
                        "initial_title": {"type": "string"},
                        "initial_text": {"type": "string"},
                        "reason": {"type": "string"},
                    },
                },
            },
            "required": ["type", "payload"],
        },
    },
]
