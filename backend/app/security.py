from __future__ import annotations

from datetime import datetime, timedelta, timezone
from secrets import compare_digest, token_urlsafe
from typing import Annotated, Literal

import jwt
from fastapi import Cookie, Depends, Header, HTTPException, Request, status
from pwdlib import PasswordHash
from pydantic import BaseModel

from .config import settings


password_hash = PasswordHash.recommended()


class Principal(BaseModel):
    subject: str
    role: Literal["user", "admin"]
    expires_at: datetime


def verify_site_password(password: str) -> bool:
    if not settings.app_password_hash:
        return settings.app_env != "production"
    try:
        return password_hash.verify(password, settings.app_password_hash)
    except Exception:
        return False


def create_session_token(subject: str = "site-user", role: str = "user") -> tuple[str, datetime, str]:
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(minutes=settings.access_token_minutes)
    csrf_token = token_urlsafe(32)
    payload = {
        "sub": subject,
        "role": role,
        "iat": int(now.timestamp()),
        "exp": int(expires_at.timestamp()),
        "csrf": csrf_token,
        "iss": "wei-strategy-room",
        "aud": "wei-web",
    }
    encoded = jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_algorithm)
    return encoded, expires_at, csrf_token


def decode_session_token(token: str) -> Principal:
    try:
        payload = jwt.decode(
            token,
            settings.jwt_secret,
            algorithms=[settings.jwt_algorithm],
            audience="wei-web",
            issuer="wei-strategy-room",
        )
        return Principal(
            subject=str(payload["sub"]),
            role=str(payload.get("role", "user")),
            expires_at=datetime.fromtimestamp(payload["exp"], timezone.utc),
        )
    except jwt.PyJWTError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Session is invalid or expired") from exc


async def current_user(
    request: Request,
    session_cookie: Annotated[str | None, Cookie(alias=settings.cookie_name)] = None,
    authorization: Annotated[str | None, Header()] = None,
) -> Principal:
    token = session_cookie
    if authorization and authorization.startswith("Bearer "):
        token = authorization.removeprefix("Bearer ").strip()
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required")
    return decode_session_token(token)


async def require_admin(
    principal: Annotated[Principal, Depends(current_user)],
    x_admin_token: Annotated[str | None, Header()] = None,
) -> Principal:
    valid_admin_token = bool(
        x_admin_token and settings.admin_token and compare_digest(x_admin_token, settings.admin_token)
    )
    if principal.role != "admin" and not valid_admin_token:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Administrator permission required")
    return principal


async def require_csrf(
    request: Request,
    x_csrf_token: Annotated[str | None, Header()] = None,
    csrf_cookie: Annotated[str | None, Cookie(alias=settings.csrf_cookie_name)] = None,
) -> None:
    if request.headers.get("authorization", "").startswith("Bearer "):
        return
    if not x_csrf_token or not csrf_cookie or not compare_digest(x_csrf_token, csrf_cookie):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="CSRF validation failed")


User = Annotated[Principal, Depends(current_user)]
Admin = Annotated[Principal, Depends(require_admin)]

