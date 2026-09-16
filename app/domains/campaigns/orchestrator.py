import logging
from uuid import UUID
from typing import Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.domains.campaigns.models import (
    Candidate,
    Campaign,
    WorkflowTemplate,
    WorkflowStepStatus,
    WorkflowEventLog
)
from app.core.redis import get_redis_pool

logger = logging.getLogger("solo.orchestrator")

# A mapping of service names to their background task names in ARQ
SERVICE_TO_TASK_MAP = {
    "document_screening": "task_parse_and_screen",
    "outbound_call": "task_initiate_outbound_call",
}


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
    if not campaign or not campaign.workflow_template_id:
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

    # 2. Log the event
    event = WorkflowEventLog(
        campaign_id=campaign.id,
        candidate_id=candidate.id,
        service_name=service_name,
        status="COMPLETED",
        payload=payload
    )
    db.add(event)

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

    # Update candidate to point to next step
    candidate.workflow_step = next_step
    
    if exec_mode == "MANUAL":
        # Waits for human intervention via API
        candidate.step_status = WorkflowStepStatus.READY_FOR_ACTION
        await db.commit()
        logger.info(f"Candidate {candidate_id} is READY_FOR_ACTION for next step: {next_step}")
        
    elif exec_mode == "AUTOMATIC":
        # Evaluate condition if one exists
        if trigger_condition and not _evaluate_condition(trigger_condition, payload):
            logger.info(f"Candidate {candidate_id} did not meet condition for {next_step}. Stopping.")
            candidate.step_status = WorkflowStepStatus.COMPLETED
            await db.commit()
            return
            
        # Trigger next background task via ARQ
        candidate.step_status = WorkflowStepStatus.PENDING
        await db.commit()
        
        task_name = SERVICE_TO_TASK_MAP.get(next_step)
        if task_name:
            # Note: We assume Redis pool (ARQ) is configured globally or passed in 
            # In real implementation you'd use `arq_pool.enqueue_job(task_name, candidate.id)`
            logger.info(f"Auto-triggering ARQ task {task_name} for Candidate {candidate_id}")
            # Mocking enqueue:
            # await arq_pool.enqueue_job(task_name, candidate.id)
        else:
            logger.error(f"No background task mapping found for service {next_step}")

