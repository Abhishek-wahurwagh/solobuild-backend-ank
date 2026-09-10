from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, EmailStr, Field

from app.domains.users.schemas import UserResponse


class UserLogin(BaseModel):
    email: Annotated[EmailStr, Field(max_length=255)]
    password: Annotated[str, Field(min_length=8, max_length=128)]


class RefreshTokenRequest(BaseModel):
    refresh_token: str = Field(min_length=10, max_length=2000)


class TokenPairResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: Literal["bearer"] = "bearer"

    model_config = ConfigDict(from_attributes=True)


class AuthRegistrationResponse(BaseModel):
    user: UserResponse
    access_token: str
    refresh_token: str
    token_type: Literal["bearer"] = "bearer"

    model_config = ConfigDict(from_attributes=True)