import logging
from uuid import UUID
from typing import Any

from arq import create_pool
from arq.connections import RedisSettings
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.core.config import settings
from app.domains.campaigns.models import (
    Candidate,
    Campaign,
    WorkflowTemplate,
    WorkflowStepStatus,
    WorkflowEventLog
)
from app.core.redis import get_redis_client

logger = logging.getLogger("solo.orchestrator")

# A mapping of service names to their background dispatcher task names in ARQ.
# Each dispatcher fans out one atomic task per candidate, so passing a single
# candidate_id here results in exactly one atomic worker being queued.
SERVICE_TO_TASK_MAP = {
    "document_screening": "dispatch_campaign_screening",
    "outbound_call": "dispatch_campaign_calling",
}

async def enqueue_workflow_task(*, task_name: str, campaign_id: UUID, candidate_id: UUID | None = None) -> None:
    """Enqueue a workflow dispatcher task in ARQ using the app Redis connection."""
    pool = await create_pool(RedisSettings.from_dsn(settings.REDIS_URL))
    try:
        if task_name in ("dispatch_campaign_screening", "dispatch_campaign_calling"):
            from uuid6 import uuid7
            prefix = "call" if task_name == "dispatch_campaign_calling" else "screen"
            await pool.enqueue_job(
                task_name,
                batch_id=f"{prefix}_{uuid7()}",
                campaign_id=str(campaign_id),
                candidate_ids=[str(candidate_id)] if candidate_id else None,
            )
            return

        if candidate_id is not None:
            await pool.enqueue_job(task_name, candidate_id=candidate_id)
    finally:
        await pool.aclose()


def _evaluate_condition(condition: dict, payload: dict) -> bool:
    """Evaluate a simple trigger condition against the event payload."""
    if not condition:
        return True
    
    field = condition.get("field")
    op = condition.get("operator")
    val = condition.get("value")
    
    actual_val = payload.get(field)
    if actual_val is None:
        return False
        
    if op == ">=":
        return actual_val >= val
    elif op == "<=":
        return actual_val <= val
    elif op == "==":
        return actual_val == val
    elif op == ">":
        return actual_val > val
    elif op == "<":
        return actual_val < val
        
    return False


async def on_step_completed(
    db: AsyncSession, 
    candidate_id: UUID, 
    service_name: str, 
    payload: dict[str, Any]
):
    """
    The main Event Dispatcher. Called whenever a core service finishes.
    Determines the next step in the pipeline based on the Campaign's WorkflowTemplate.
    """
    
    # 1. Fetch Candidate & Campaign context
    stmt = select(Candidate).where(Candidate.id == candidate_id)
    candidate = (await db.execute(stmt)).scalar_one_or_none()
    if not candidate:
        logger.error(f"Candidate {candidate_id} not found in orchestrator.")
        return

    stmt = select(Campaign).where(Campaign.id == candidate.campaign_id)
    campaign = (await db.execute(stmt)).scalar_one_or_none()
    if not campaign:
        logger.error(f"Campaign {candidate.campaign_id} not found.")
        return

    # 2. Log the event regardless of workflow routing
    event = WorkflowEventLog(
        campaign_id=campaign.id,
        candidate_id=candidate.id,
        service_name=service_name,
        status="COMPLETED",
        payload=payload
    )
    db.add(event)

    if not campaign.workflow_template_id:
        # If no workflow is attached, we just mark it completed and stop.
        candidate.step_status = WorkflowStepStatus.COMPLETED
        await db.commit()
        return

    stmt = select(WorkflowTemplate).where(WorkflowTemplate.id == campaign.workflow_template_id)
    template = (await db.execute(stmt)).scalar_one_or_none()
    if not template:
        candidate.step_status = WorkflowStepStatus.COMPLETED
        await db.commit()
        return

    # 3. Find workflow rules for the service that just finished
    pipeline_steps = template.template.get("steps", {})
    current_config = pipeline_steps.get(service_name)

    if not current_config or "next" not in current_config:
        # End of the pipeline
        candidate.step_status = WorkflowStepStatus.COMPLETED
        await db.commit()
        return
        
    next_step = current_config["next"]
    exec_mode = current_config.get("execution_mode", "MANUAL")
    trigger_condition = current_config.get("trigger_condition")

    candidate.workflow_step = next_step

    if exec_mode == "MANUAL":
        candidate.step_status = WorkflowStepStatus.READY_FOR_ACTION
        await db.commit()
        logger.info(f"Candidate {candidate_id} is READY_FOR_ACTION for next step: {next_step}")

    elif exec_mode == "AUTOMATIC":
        if trigger_condition and not _evaluate_condition(trigger_condition, payload):
            logger.info(f"Candidate {candidate_id} did not meet condition for {next_step}. Stopping.")
            candidate.step_status = WorkflowStepStatus.COMPLETED
            await db.commit()
            return

        task_name = SERVICE_TO_TASK_MAP.get(next_step)
        if task_name is None:
            logger.error(f"No background task mapping found for service {next_step}")
            candidate.step_status = WorkflowStepStatus.FAILED
            await db.commit()
            return

        candidate.step_status = WorkflowStepStatus.PENDING
        await db.commit()

        logger.info(f"Auto-triggering ARQ task {task_name} for Candidate {candidate_id}")
        await enqueue_workflow_task(
            task_name=task_name,
            campaign_id=campaign.id,
            candidate_id=candidate.id,
        )

