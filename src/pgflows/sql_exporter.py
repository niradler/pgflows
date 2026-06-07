from __future__ import annotations

import ast
import inspect
import re
import textwrap
from dataclasses import dataclass, field
from typing import Any, Literal

from pgflows.dsl import pgmq_step
from pgflows.registry import WorkflowRegistry

_SAFE_NAME = re.compile(r"^[A-Za-z0-9_]+$")

ExportMode = Literal["http", "pgmq"]

# Carries the pg_durable instance ID into the step endpoint for telemetry correlation,
# and forwards the {input} durable variable as the request body so steps receive data.
_HTTP_HEADERS = '{"X-DF-Instance-ID": "{sys_instance_id}", "Content-Type": "application/json"}'


def _require_safe_name(value: str, label: str) -> None:
    if not _SAFE_NAME.fullmatch(value):
        raise ValueError(
            f"{label} must contain only letters, digits, and underscores; got {value!r}"
        )


@dataclass
class StepSql:
    step_name: str
    http_url: str
    sql_fragment: str


@dataclass
class DryRunResult:
    workflow_name: str
    steps: list[StepSql]
    sql: str
    input_schema: dict[str, Any] = field(default_factory=dict)


class SqlExporter:
    """Generate pg_durable SQL DSL from registered workflow definitions.

    Two step-invocation bindings are selectable via ``mode``:

    - ``"http"`` (default) — each step becomes a ``df.http()`` call to the push-mode
      FastAPI step endpoint. pg_durable makes an outbound request per step.
    - ``"pgmq"`` — each step becomes a native ``pgmq.send`` + ``pg_notify`` +
      ``df.wait_for_signal`` chain. pg_durable enqueues the step and suspends; a
      ``StepWorker`` runs the Python function and signals the result back. No inbound
      HTTP server required.

    The exported SQL can be imported into any Postgres with pg_durable to transfer
    workflow definitions from dev → prod without code deployment.
    """

    def __init__(
        self,
        registry: WorkflowRegistry,
        base_url: str | None = None,
        *,
        mode: ExportMode = "http",
        step_queue: str = "pgflows_steps",
        notify_channel: str | None = None,
    ) -> None:
        self._registry = registry
        self._mode = mode
        self._step_queue = step_queue
        self._notify_channel = notify_channel or step_queue
        if mode == "http" and base_url is None:
            raise ValueError("base_url is required for mode='http'")
        # Escape single quotes so base_url is safe inside a SQL string literal.
        self._base_url = base_url.rstrip("/").replace("'", "''") if base_url else ""

    def export_workflow(self, workflow_name: str) -> str:
        """Return pg_durable SQL that starts this workflow."""
        _require_safe_name(workflow_name, "workflow_name")
        steps = self._collect_steps(workflow_name)
        dsl = self._build_dsl(steps, workflow_name)
        return textwrap.dedent(f"""\
            -- pgflows export: {workflow_name} (mode={self._mode})
            {self._preamble()}
            SELECT df.start(
                {dsl},
                '{workflow_name}'
            );
        """)

    def dry_run(self, workflow_name: str, input_data: dict[str, Any] | None = None) -> DryRunResult:
        """Trace workflow structure without executing. Returns steps + SQL."""
        _require_safe_name(workflow_name, "workflow_name")
        steps = self._collect_steps(workflow_name)
        dsl = self._build_dsl(steps, workflow_name)
        sql = textwrap.dedent(f"""\
            {self._preamble()}
            SELECT df.start({dsl}, '{workflow_name}');
        """)
        return DryRunResult(
            workflow_name=workflow_name,
            steps=steps,
            sql=sql,
            input_schema=input_data or {},
        )

    def compose(self, workflow_name: str, steps: list[str]) -> str:
        """Build pg_durable DSL from an explicit list of registered step names.

        Unlike export_workflow(), this does not require a Python workflow function —
        compose step sequences at runtime from config, API payloads, or any dynamic
        source. Each step name must already be registered with the app.

        Example::

            sql = exporter.compose("on_call_response", ["page_engineer", "create_ticket"])

        The returned SQL is ready to execute against a Postgres database that has the
        pg_durable extension installed.
        """
        _require_safe_name(workflow_name, "workflow_name")
        step_sqls = []
        for index, step_name in enumerate(steps):
            _require_safe_name(step_name, "step_name")
            self._registry.get_step(step_name)  # raises KeyError if unregistered
            step_sqls.append(self._step_sql(step_name, index))
        dsl = self._build_dsl(step_sqls, workflow_name)
        return textwrap.dedent(f"""\
            -- pgflows runtime workflow: {workflow_name} (mode={self._mode})
            {self._preamble()}
            SELECT df.start(
                {dsl},
                '{workflow_name}'
            );
        """)

    def export_all(self) -> str:
        """Export all registered workflows to a single SQL file."""
        parts = [f"-- pgflows bulk export (mode={self._mode})\n-- base_url: {self._base_url}\n"]
        for name in self._registry.list_workflows():
            parts.append(self.export_workflow(name))
        return "\n".join(parts)

    def _preamble(self) -> str:
        """SQL emitted before df.start() — mode-specific setup."""
        if self._mode == "http":
            return (
                f"SELECT df.setvar('base_url', '{self._base_url}');\n"
                "-- also set the workflow input forwarded to each step as the request body:\n"
                "-- SELECT df.setvar('input', '{\"...\": \"...\"}');"
            )
        return (
            f"-- ensure the step queue exists: SELECT pgmq.create('{self._step_queue}');\n"
            f"-- run a StepWorker listening on channel '{self._notify_channel}'"
        )

    def _step_sql(self, step_name: str, index: int) -> StepSql:
        """Build the per-step DSL fragment for the configured mode."""
        if self._mode == "http":
            fragment = (
                "df.http('{base_url}/steps/" + step_name + "', 'POST', "
                "'{input}', '" + _HTTP_HEADERS + "'::jsonb)"
            )
            return StepSql(
                step_name=step_name,
                http_url=f"{self._base_url}/steps/{step_name}",
                sql_fragment=fragment,
            )
        fragment = str(
            pgmq_step(
                step_name,
                signal=f"__pgflows_{step_name}_{index}",
                queue=self._step_queue,
                notify_channel=self._notify_channel,
            )
        )
        return StepSql(
            step_name=step_name,
            http_url=f"pgmq://{self._step_queue}/{step_name}",
            sql_fragment=fragment,
        )

    def _collect_steps(self, workflow_name: str) -> list[StepSql]:
        """Introspect the workflow function via AST to find ctx.step() calls in order."""
        defn = self._registry.get_workflow(workflow_name)
        source = textwrap.dedent(inspect.getsource(defn.fn))
        tree = ast.parse(source)

        names_with_line: list[tuple[int, str]] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Await):
                continue
            call = node.value
            if not isinstance(call, ast.Call):
                continue
            func = call.func
            if not (isinstance(func, ast.Attribute) and func.attr == "step"):
                continue
            if not call.args:
                continue
            fn_node = call.args[0]
            step_name = fn_node.id if isinstance(fn_node, ast.Name) else "unknown"
            names_with_line.append((node.lineno, step_name))

        ordered = [name for _, name in sorted(names_with_line, key=lambda x: x[0])]
        return [self._step_sql(name, index) for index, name in enumerate(ordered)]

    def _build_dsl(self, steps: list[StepSql], label: str) -> str:
        if not steps:
            return f"'SELECT ''workflow {label} has no steps'''"
        return "\n    ~> ".join(s.sql_fragment for s in steps)
