from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domains.users.models import User
from app.domains.users.schemas import UserUpdate


async def update_user(
    db: AsyncSession,
    *,
    user: User,
    user_in: UserUpdate,
) -> User:
    updates = user_in.model_dump(exclude_unset=True)

    if "email" in updates:
        email = str(updates["email"]).strip().lower()
        result = await db.execute(
            select(User).where(User.email == email, User.id != user.id)
        )
        if result.scalar_one_or_none() is not None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Email already exists",
            )
        updates["email"] = email

    for field, value in updates.items():
        setattr(user, field, value)

    await db.commit()
    await db.refresh(user)
    return user