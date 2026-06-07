from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

if TYPE_CHECKING:
    from fastapi import APIRouter

    from pgflows.app import WorkflowApp


class SignalRequest(BaseModel):
    signal_name: str
    data: dict[str, Any] | None = None


class StartRequest(BaseModel):
    input: dict[str, Any]


def create_pgflows_router(app: WorkflowApp, prefix: str = "") -> APIRouter:
    """Create a FastAPI router with pgflows push-mode and management endpoints.

    Mount on your FastAPI app::

        router = create_pgflows_router(app, prefix="/pgflows")
        fastapi_app.include_router(router)

    pg_durable calls POST {base_url}/steps/{step_name} for each step.
    """
    from fastapi import APIRouter, Body, HTTPException

    router = APIRouter(prefix=prefix, tags=["pgflows"])

    @router.post("/steps/{step_name}", summary="Push endpoint for pg_durable")
    async def execute_step(
        step_name: str, payload: dict[str, Any] = Body(...)
    ) -> dict[str, Any]:
        """Execute a registered step. Called by pg_durable df.http() nodes."""
        try:
            step_defn = app.registry.get_step(step_name)
        except KeyError:
            raise HTTPException(status_code=404, detail=f"Step '{step_name}' not registered")

        try:
            input_obj = step_defn.input_type.model_validate(payload)
        except Exception as exc:
            raise HTTPException(status_code=422, detail=str(exc))

        from pgflows.context import StepContext

        ctx = StepContext(instance_id="push-mode", step_name=step_name)
        try:
            result = await step_defn.fn(ctx, input_obj)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc))

        if isinstance(result, BaseModel):
            return result.model_dump()
        return {"result": result}

    @router.post("/workflows/{workflow_name}/start", summary="Start a workflow by name")
    async def start_workflow(
        workflow_name: str, body: StartRequest
    ) -> dict[str, Any]:
        try:
            defn = app.registry.get_workflow(workflow_name)
        except KeyError:
            raise HTTPException(
                status_code=404, detail=f"Workflow '{workflow_name}' not registered"
            )
        try:
            input_obj = defn.input_type.model_validate(body.input)
        except Exception as exc:
            raise HTTPException(status_code=422, detail=str(exc))
        instance_id = await app.start(defn.fn, input_obj)
        return {"instance_id": instance_id}

    @router.get("/workflows/{workflow_id}", summary="Get workflow status")
    async def get_workflow_status(workflow_id: str) -> dict[str, Any]:
        try:
            status = await app.get_status(workflow_id)
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        return status.model_dump()

    @router.delete("/workflows/{workflow_id}", summary="Cancel a workflow")
    async def cancel_workflow(workflow_id: str) -> dict[str, Any]:
        await app.cancel(workflow_id)
        return {"cancelled": True, "instance_id": workflow_id}

    @router.post(
        "/workflows/{workflow_id}/signal", summary="Send a signal to a waiting workflow"
    )
    async def signal_workflow(
        workflow_id: str, body: SignalRequest
    ) -> dict[str, Any]:
        if not app.pg_durable_available:
            raise HTTPException(
                status_code=503, detail="pg_durable extension not installed"
            )
        await app.pg_durable.signal(workflow_id, body.signal_name, body.data)
        return {
            "signalled": True,
            "instance_id": workflow_id,
            "signal": body.signal_name,
        }

    @router.get("/workflows", summary="List workflow instances")
    async def list_workflows(limit: int = 100) -> list[dict[str, Any]]:
        instances = await app.list_workflows(limit=limit)
        return [i.model_dump() for i in instances]

    return router


__all__ = ["SignalRequest", "StartRequest", "create_pgflows_router"]
