from uuid import UUID

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domains.users.models import User
from app.domains.agents.models import Agent
from app.domains.agents.schemas import AgentCreate, AgentUpdate


class AgentService:
    @staticmethod
    async def create_agent(db: AsyncSession, agent_in: AgentCreate, current_user: User) -> Agent:
        agent = Agent(
            name=agent_in.name,
            conversation_style=agent_in.conversation_style,
            languages=agent_in.languages,
            voice=agent_in.voice,
            interview_instruction=agent_in.interview_instruction,
            created_by_user_id=current_user.id,
        )

        db.add(agent)
        await db.commit()
        await db.refresh(agent)
        return agent

    @staticmethod
    async def update_agent(db: AsyncSession, agent_id: UUID, agent_in: AgentUpdate) -> Agent:
        result = await db.execute(select(Agent).where(Agent.id == agent_id))
        agent = result.scalar_one_or_none()

        if not agent:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Agent not found",
            )

        update_data = agent_in.model_dump(exclude_unset=True, exclude_none=True)

        for field, value in update_data.items():
            setattr(agent, field, value)

        await db.commit()
        await db.refresh(agent)
        return agent

    @staticmethod
    async def remove_agent(db: AsyncSession, agent_id: UUID) -> None:
        result = await db.execute(select(Agent).where(Agent.id == agent_id))
        agent = result.scalar_one_or_none()

        if not agent:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Agent not found",
            )

        await db.delete(agent)
        await db.commit()