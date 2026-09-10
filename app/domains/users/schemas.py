from pydantic import BaseModel, ConfigDict, Field, EmailStr
from typing import Annotated
from datetime import datetime
from uuid import UUID


class UserCreate(BaseModel):
    name: Annotated[str, Field(min_length=3, max_length=100)]
    email: Annotated[EmailStr, Field(max_length=255)]
    password: Annotated[str, Field(min_length=8, max_length=128)]
    timezone: Annotated[str, Field(min_length=3, max_length=50, examples=["UTC"])]


class UserResponse(BaseModel):
    id: UUID
    name: str
    email: EmailStr
    timezone: str
    created_at: datetime
    updated_at: datetime
    is_active: bool

    model_config = ConfigDict(from_attributes=True)