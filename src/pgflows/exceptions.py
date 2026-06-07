from __future__ import annotations


class PgflowsError(Exception):
    """Base exception for all pgflows errors."""


class WorkflowNotFoundError(PgflowsError):
    def __init__(self, workflow_id: str) -> None:
        super().__init__(f"Workflow not found: {workflow_id}")
        self.workflow_id = workflow_id


class WorkflowAlreadyExistsError(PgflowsError):
    def __init__(self, workflow_id: str) -> None:
        super().__init__(f"Workflow already exists: {workflow_id}")
        self.workflow_id = workflow_id


class StepExecutionError(PgflowsError):
    def __init__(self, step_name: str, cause: BaseException) -> None:
        super().__init__(f"Step '{step_name}' failed: {cause}")
        self.step_name = step_name
        self.cause = cause


class BackendNotInitializedError(PgflowsError):
    def __init__(self, backend: str) -> None:
        super().__init__(f"Backend '{backend}' has not been initialized — call initialize() first")
        self.backend = backend


class SchedulerJobNotFoundError(PgflowsError):
    def __init__(self, job_id: str) -> None:
        super().__init__(f"Scheduled job not found: {job_id}")
        self.job_id = job_id
