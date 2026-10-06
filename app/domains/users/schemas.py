from pydantic import BaseModel, ConfigDict, EmailStr, Field, model_validator
from typing import Annotated
from datetime import datetime
from uuid import UUID

from app.domains.users.enums import UserService


class UserCreate(BaseModel):
    name: Annotated[str, Field(min_length=3, max_length=100)]
    email: Annotated[EmailStr, Field(max_length=255)]
    password: Annotated[str, Field(min_length=8, max_length=128)]
    timezone: Annotated[str, Field(min_length=3, max_length=50, examples=["UTC"])]


class UserUpdate(BaseModel):
    name: Annotated[str | None, Field(min_length=3, max_length=100)] = None
    email: Annotated[EmailStr | None, Field(max_length=255)] = None
    timezone: Annotated[str | None, Field(min_length=3, max_length=50)] = None

    @model_validator(mode="after")
    def require_update_fields(self) -> "UserUpdate":
        if not self.model_fields_set:
            raise ValueError("At least one field must be provided.")
        if any(getattr(self, field) is None for field in self.model_fields_set):
            raise ValueError("Updated fields cannot be null.")
        return self


class UserResponse(BaseModel):
    id: UUID
    name: str
    email: EmailStr
    timezone: str
    services: list[UserService]
    created_at: datetime
    updated_at: datetime
    is_active: bool

    model_config = ConfigDict(from_attributes=True)