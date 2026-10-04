from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.domains.auth.dependencies import get_current_user
from app.domains.users.models import User
from app.domains.users.schemas import UserResponse, UserUpdate
from app.domains.users.services import update_user


router = APIRouter(prefix="/users")


@router.patch(
    "/me",
    response_model=UserResponse,
    status_code=status.HTTP_200_OK,
)
async def update_current_user(
    user_in: UserUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> User:
    return await update_user(db, user=current_user, user_in=user_in)