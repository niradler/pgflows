from __future__ import annotations

from pydantic import BaseModel


class PyflowsConfig(BaseModel):
    dsn: str
    workflow_queue: str = "pyflows_workflows"
    step_queue: str = "pyflows_steps"
    worker_concurrency: int = 10
    step_visibility_timeout_seconds: int = 300
    otel_enabled: bool = True
    otel_service_name: str = "pyflows"
    db_ssl: bool = True
