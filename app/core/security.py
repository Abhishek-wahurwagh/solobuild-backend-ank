from datetime import datetime, timedelta, timezone
from typing import Any

import jwt
from jwt.exceptions import InvalidTokenError
from pwdlib import PasswordHash
from pwdlib.hashers.argon2 import Argon2Hasher

from app.core.config import settings

pwd_context = PasswordHash((Argon2Hasher(),))


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    return pwd_context.verify(password, password_hash)


def create_access_token(subject: Any, expires_delta: int | None = None) -> str:
    now = datetime.now(timezone.utc)
    expire_minutes = expires_delta or settings.ACCESS_TOKEN_EXPIRE_MINUTES
    expire = now + timedelta(minutes=expire_minutes)

    payload = {
        "sub": str(subject),
        "type": "access",
        "iat": int(now.timestamp()),
        "exp": int(expire.timestamp()),
    }

    return jwt.encode(payload, settings.SECRET_KEY, algorithm=settings.ALGORITHM)


def create_refresh_token(subject: Any, expires_delta: int | None = None) -> str:
    now = datetime.now(timezone.utc)
    expire_days = expires_delta or settings.REFRESH_TOKEN_EXPIRE_DAYS
    expire = now + timedelta(days=expire_days)

    payload = {
        "sub": str(subject),
        "type": "refresh",
        "iat": int(now.timestamp()),
        "exp": int(expire.timestamp()),
    }

    return jwt.encode(payload, settings.SECRET_KEY, algorithm=settings.ALGORITHM)


def decode_access_token(token: str) -> dict[str, Any]:
    try:
        payload = jwt.decode(
            token,
            settings.SECRET_KEY,
            algorithms=[settings.ALGORITHM],
        )
        if payload.get("type") != "access":
            raise InvalidTokenError("Token type is not access")
        return payload
    except InvalidTokenError as exc:
        raise exc


def decode_refresh_token(token: str) -> dict[str, Any]:
    try:
        payload = jwt.decode(
            token,
            settings.SECRET_KEY,
            algorithms=[settings.ALGORITHM],
        )
        if payload.get("type") != "refresh":
            raise InvalidTokenError("Token type is not refresh")
        return payload
    except InvalidTokenError as exc:
        raise exc
