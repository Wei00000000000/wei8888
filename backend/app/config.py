from __future__ import annotations

from datetime import datetime, timezone
from functools import lru_cache
from typing import Annotated, Literal

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    app_name: str = "Wei Strategy Room API"
    app_env: Literal["development", "test", "production"] = "development"
    api_prefix: str = "/api/v1"
    database_url: str = "sqlite+aiosqlite:///./wei_strategy.db"
    jwt_secret: str = "development-only-change-me-32-characters"
    jwt_algorithm: str = "HS256"
    access_token_minutes: int = Field(default=480, ge=5, le=10080)
    app_password_hash: str = ""
    admin_token: str = ""
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    public_site_url: str = "https://wei00000000000.github.io/wei8888/"
    cors_origins: Annotated[list[str], NoDecode] = [
        "https://wei00000000000.github.io",
        "http://localhost:8000",
        "http://127.0.0.1:8000",
    ]
    allowed_hosts: Annotated[list[str], NoDecode] = [
        "localhost",
        "127.0.0.1",
        "testserver",
        "wei-strategy-api.zeabur.app",
        "*.zeabur.app",
    ]
    cookie_name: str = "wei_session"
    csrf_cookie_name: str = "wei_csrf"
    cookie_secure: bool = False
    cookie_samesite: Literal["lax", "strict", "none"] = "lax"
    auto_create_schema: bool = True
    run_scanner: bool = True
    scanner_timeout_seconds: int = Field(default=720, ge=60, le=1800)
    general_rate_limit: int = Field(default=120, ge=10, le=10000)
    login_rate_limit: int = Field(default=5, ge=1, le=100)
    backtest_rate_limit: int = Field(default=2, ge=1, le=30)
    history_rate_limit: int = Field(default=60, ge=10, le=1000)
    notification_rate_limit: int = Field(default=20, ge=1, le=200)
    own_signal_start_at: datetime | None = Field(
        default=datetime(2026, 7, 12, 13, 57, tzinfo=timezone.utc),
        description="Default cutoff for user-owned live signals. Older imported/replayed rows are kept but excluded.",
    )

    @field_validator("cors_origins", mode="before")
    @classmethod
    def parse_origins(cls, value: object) -> object:
        if isinstance(value, str):
            return [part.strip().rstrip("/") for part in value.split(",") if part.strip()]
        return value

    @field_validator("allowed_hosts", mode="before")
    @classmethod
    def parse_hosts(cls, value: object) -> object:
        if isinstance(value, str):
            return [part.strip() for part in value.split(",") if part.strip()]
        return value

    @field_validator("database_url")
    @classmethod
    def normalize_database_url(cls, value: str) -> str:
        if value.startswith("postgres://"):
            return value.replace("postgres://", "postgresql+asyncpg://", 1)
        if value.startswith("postgresql://"):
            return value.replace("postgresql://", "postgresql+asyncpg://", 1)
        return value

    @model_validator(mode="after")
    def validate_production_secrets(self) -> "Settings":
        if self.app_env == "production":
            weak = not self.jwt_secret or self.jwt_secret.startswith("development-") or len(self.jwt_secret) < 32
            if weak:
                raise ValueError("JWT_SECRET must be a random value of at least 32 characters")
            if not self.app_password_hash.startswith("$argon2"):
                raise ValueError("APP_PASSWORD_HASH must contain an Argon2 hash")
            if len(self.admin_token) < 32:
                raise ValueError("ADMIN_TOKEN must be a separate random value of at least 32 characters")
            if "*" in self.cors_origins:
                raise ValueError("Wildcard CORS is forbidden in production")
            if "*" in self.allowed_hosts:
                raise ValueError("Wildcard trusted hosts are forbidden in production")
            if not self.cookie_secure:
                raise ValueError("COOKIE_SECURE must be true in production")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
