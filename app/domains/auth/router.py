from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm

from app.core.database import AsyncSession, get_db
from app.domains.auth.dependencies import get_current_user
from app.domains.auth.schemas import (
    AuthRegistrationResponse,
    RefreshTokenRequest,
    TokenPairResponse,
    UserLogin,
)
from app.domains.auth.services import AuthService
from app.domains.users.models import User
from app.domains.users.schemas import UserCreate, UserResponse

router = APIRouter(prefix="/auth")


@router.post(
    "/register",
    response_model=AuthRegistrationResponse,
    status_code=status.HTTP_201_CREATED,
)
async def register_user_endpoint(
    user_in: UserCreate,
    db: AsyncSession = Depends(get_db),
):
    user = await AuthService.register_user(db=db, user_in=user_in)
    token_pair = AuthService.issue_token_pair_for_user(user.id)

    return AuthRegistrationResponse(
        user=user,
        access_token=token_pair.access_token,
        refresh_token=token_pair.refresh_token,
        token_type=token_pair.token_type,
    )


@router.post("/login", response_model=TokenPairResponse, status_code=status.HTTP_200_OK)
async def login_user(
    form_data: OAuth2PasswordRequestForm = Depends(OAuth2PasswordRequestForm),
    db: AsyncSession = Depends(get_db),
):
    credentials = UserLogin(email=form_data.username, password=form_data.password)
    return await AuthService.authenticate_user(db=db, credentials=credentials)


@router.post("/refresh", response_model=TokenPairResponse, status_code=status.HTTP_200_OK)
async def refresh_user_token_pair(
    request: RefreshTokenRequest,
    db: AsyncSession = Depends(get_db),
):
    return await AuthService.refresh_user_token_pair(
        db=db,
        refresh_token=request.refresh_token,
    )

@router.get("/me", response_model=UserResponse)
async def get_me(current_user: User = Depends(get_current_user)) -> User:
    return current_user