from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status

from ..config import settings
from ..schemas import LoginRequest, SessionResponse
from ..security import User, create_session_token, require_csrf, verify_site_password


router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/login", response_model=SessionResponse)
async def login(payload: LoginRequest, request: Request, response: Response) -> SessionResponse:
    if not verify_site_password(payload.password):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Password is incorrect")
    token, expires_at, csrf_token = create_session_token()
    cookie_options = {
        "secure": settings.cookie_secure,
        "samesite": settings.cookie_samesite,
        "path": "/",
        "max_age": settings.access_token_minutes * 60,
    }
    response.set_cookie(settings.cookie_name, token, httponly=True, **cookie_options)
    response.set_cookie(settings.csrf_cookie_name, csrf_token, httponly=False, **cookie_options)
    return SessionResponse(expires_at=expires_at, csrf_token=csrf_token, access_token=token)


@router.post("/logout", status_code=204, response_class=Response, dependencies=[Depends(require_csrf)])
async def logout(response: Response) -> Response:
    response.delete_cookie(settings.cookie_name, path="/")
    response.delete_cookie(settings.csrf_cookie_name, path="/")
    response.status_code = 204
    return response


@router.get("/me")
async def me(user: User) -> dict[str, object]:
    return {"authenticated": True, "role": user.role, "expires_at": user.expires_at}
