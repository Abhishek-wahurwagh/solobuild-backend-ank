from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.domains.auth.dependencies import get_current_user
from app.domains.agents.models import Agent, AgentPreset
from app.domains.users.models import User
from app.domains.agents.schemas import AgentCreate, AgentResponse, AgentUpdate
from app.domains.agents.service import AgentService

router = APIRouter(prefix="/agents")


@router.get(
    "",
    response_model=list[AgentResponse],
    status_code=status.HTTP_200_OK,
)
async def list_agents_endpoint(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    user_agents_result = await db.execute(
        select(Agent)
        .where(Agent.created_by_user_id == current_user.id)
        .order_by(Agent.created_at.desc())
    )
    presets_result = await db.execute(
        select(AgentPreset).order_by(AgentPreset.created_at.desc())
    )

    user_agents = [
        AgentResponse.model_validate(agent).model_copy(update={"is_preset": False})
        for agent in user_agents_result.scalars()
    ]
    presets = [
        AgentResponse.model_validate(preset).model_copy(update={"is_preset": True})
        for preset in presets_result.scalars()
    ]
    return [*user_agents, *presets]


@router.post(
    "",
    response_model=AgentResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_agent_endpoint(
    agent_in: AgentCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    return await AgentService.create_agent(db=db, agent_in=agent_in, current_user=current_user)


@router.get(
    "/{agent_id}",
    response_model=AgentResponse,
    status_code=status.HTTP_200_OK,
)
async def get_agent_endpoint(
    agent_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    result = await db.execute(
        select(Agent).where(
            Agent.id == agent_id,
            Agent.created_by_user_id == current_user.id,
        )
    )
    agent = result.scalar_one_or_none()

    if not agent:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Agent not found",
        )

    return agent


@router.patch(
    "/{agent_id}",
    response_model=AgentResponse,
    status_code=status.HTTP_200_OK,
)
async def update_agent_endpoint(
    agent_id: UUID,
    agent_in: AgentUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    # Ownership check: only the creator can update
    result = await db.execute(
        select(Agent).where(
            Agent.id == agent_id,
            Agent.created_by_user_id == current_user.id,
        )
    )
    if result.scalar_one_or_none() is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Agent not found",
        )

    return await AgentService.update_agent(
        db=db,
        agent_id=agent_id,
        agent_in=agent_in,
    )


@router.delete(
    "/{agent_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_agent_endpoint(
    agent_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    # Ownership check: only the creator can delete
    result = await db.execute(
        select(Agent).where(
            Agent.id == agent_id,
            Agent.created_by_user_id == current_user.id,
        )
    )
    if result.scalar_one_or_none() is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Agent not found",
        )

    await AgentService.remove_agent(db=db, agent_id=agent_id)