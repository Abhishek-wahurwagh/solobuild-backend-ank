from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.domains.auth.dependencies import get_current_user
from app.domains.agents.models import Agent
from app.domains.users.models import User
from app.domains.agents.schemas import AgentCreate, AgentResponse, AgentUpdate
from app.domains.agents.service import AgentService

router = APIRouter(prefix="/agents")


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
    result = await db.execute(select(Agent).where(Agent.id == agent_id))
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
    await AgentService.remove_agent(db=db, agent_id=agent_id)
    return {"Agent Deleted Sucessufully"}