from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from pgflows.logger import get_logger

if TYPE_CHECKING:
    from fastapi import APIRouter

    from pgflows.app import WorkflowApp

_log = get_logger("fastapi")


class SignalRequest(BaseModel):
    signal_name: str
    data: dict[str, Any] | None = None


class StartRequest(BaseModel):
    input: dict[str, Any]


def create_pgflows_router(
    app: WorkflowApp,
    prefix: str = "",
    auth_dependency: Any | None = None,
) -> APIRouter:
    """Create a FastAPI router with pgflows push-mode and management endpoints.

    Mount on your FastAPI app::

        router = create_pgflows_router(app, prefix="/pgflows")
        fastapi_app.include_router(router)

    Pass ``auth_dependency`` to protect all routes::

        from fastapi import Depends, Security
        router = create_pgflows_router(app, auth_dependency=verify_token)

    pg_durable calls POST {base_url}/steps/{step_name} for each step.
    """
    from fastapi import APIRouter, Body, Depends, Header, HTTPException, Query

    _dependencies = [Depends(auth_dependency)] if auth_dependency is not None else []
    router = APIRouter(prefix=prefix, tags=["pgflows"], dependencies=_dependencies)

    @router.post("/steps/{step_name}", summary="Push endpoint for pg_durable")
    async def execute_step(
        step_name: str,
        payload: dict[str, Any] = Body(...),
        x_df_instance_id: str | None = Header(default=None),
    ) -> dict[str, Any]:
        """Execute a registered step. Called by pg_durable df.http() nodes.

        Pass the pg_durable instance ID via the X-DF-Instance-ID header using
        $var substitution in df.http()::

            df.http(url, headers='{"X-DF-Instance-ID": "$my_id"}'::jsonb)
        """
        try:
            step_defn = app.registry.get_step(step_name)
        except KeyError:
            raise HTTPException(status_code=404, detail=f"Step '{step_name}' not registered")

        try:
            input_obj = step_defn.input_type.model_validate(payload)
        except Exception as exc:
            raise HTTPException(status_code=422, detail=str(exc))

        instance_id = x_df_instance_id or "push-mode"

        from pgflows.context import StepContext

        ctx = StepContext(instance_id=instance_id, step_name=step_name)
        try:
            with app.telemetry.step_span(instance_id, step_name, 0):
                result = await step_defn.fn(ctx, input_obj)
        except Exception:
            _log.exception("step '%s' execution failed (instance=%s)", step_name, instance_id)
            raise HTTPException(status_code=500, detail="Internal server error")

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
        except Exception:
            raise HTTPException(status_code=404, detail=f"Workflow '{workflow_id}' not found")
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
    async def list_workflows(
        limit: int = Query(default=100, ge=1, le=1000),
    ) -> list[dict[str, Any]]:
        instances = await app.list_workflows(limit=limit)
        return [i.model_dump() for i in instances]

    return router


__all__ = ["SignalRequest", "StartRequest", "create_pgflows_router"]
