from fastapi import HTTPException, status
from jwt import InvalidTokenError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.security import (
    create_access_token,
    create_refresh_token,
    decode_refresh_token,
    hash_password,
    verify_password,
)
from app.domains.auth.schemas import TokenPairResponse, UserLogin
from app.domains.users.models import User
from app.domains.users.schemas import UserCreate


class AuthService:
    @staticmethod
    async def register_user(db: AsyncSession, user_in: UserCreate) -> User:
        email = user_in.email.strip().lower()
        query = select(User).where(User.email == email)
        result = await db.execute(query)
        existing_user = result.scalar_one_or_none()

        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Email already exists",
            )

        hashed_password = hash_password(user_in.password)
        new_user = User(
            name=user_in.name,
            email=email,
            password_hash=hashed_password,
            timezone=user_in.timezone,
            is_active=True,
        )

        db.add(new_user)
        await db.commit()
        await db.refresh(new_user)
        return new_user

    @staticmethod
    def issue_token_pair_for_user(user_id) -> TokenPairResponse:
        access_token = create_access_token(subject=user_id)
        refresh_token = create_refresh_token(subject=user_id)
        return TokenPairResponse(
            access_token=access_token,
            refresh_token=refresh_token,
            token_type="bearer",
        )

    @staticmethod
    async def authenticate_user(db: AsyncSession, credentials: UserLogin) -> TokenPairResponse:
        query = select(User).where(User.email == credentials.email)
        result = await db.execute(query)
        user = result.scalar_one_or_none()

        if not user or not verify_password(credentials.password, user.password_hash):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid email or password",
                headers={"WWW-Authenticate": "Bearer"},
            )

        if not user.is_active:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User account is inactive",
            )

        return AuthService.issue_token_pair_for_user(user.id)

    @staticmethod
    async def refresh_user_token_pair(
        db: AsyncSession,
        refresh_token: str,
    ) -> TokenPairResponse:
        try:
            payload = decode_refresh_token(refresh_token)
        except InvalidTokenError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid refresh token",
            )

        user_id = payload.get("sub")
        if not user_id:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid refresh token",
            )

        user = await db.get(User, user_id)
        if not user or not user.is_active:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User account is inactive or not found",
            )

        return AuthService.issue_token_pair_for_user(user.id)