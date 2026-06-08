from __future__ import annotations

import re

from pydantic import BaseModel, field_validator

# SQLAlchemy/Django-style DSNs carry a driver suffix (postgresql+psycopg://,
# postgresql+asyncpg://) that asyncpg does not understand. Strip it so callers can
# pass the same URL they hand to their ORM.
_DRIVER_SCHEME = re.compile(r"^(postgres(?:ql)?)\+[a-z0-9_]+://", re.IGNORECASE)


class PgflowsConfig(BaseModel):
    dsn: str
    workflow_queue: str = "pgflows_workflows"
    step_queue: str = "pgflows_steps"
    step_notify_channel: str = "pgflows_steps"
    worker_concurrency: int = 10
    step_visibility_timeout_seconds: int = 300
    workflow_visibility_timeout_seconds: int = 300
    otel_enabled: bool = True
    otel_service_name: str = "pgflows"
    db_ssl: bool = True

    @field_validator("dsn")
    @classmethod
    def _normalize_dsn(cls, v: str) -> str:
        v = _DRIVER_SCHEME.sub(lambda m: m.group(1).lower() + "://", v)
        if v.startswith("postgres://"):
            v = "postgresql://" + v[len("postgres://") :]
        return v
