# Final Code Review — src/pyflows/

## Findings

### [HIGH] Correctness — context.py:72,127
The `raise StepExecutionError(step_name, last_error)` at line 127 is **outside** the
`with self._telemetry.step_span(...)` block that opens at line 72. The span's
`except` branch (which calls `span.set_status(StatusCode.ERROR, ...)`) never fires on
final failure — the span closes as OK even when every retry is exhausted.

Fix: move the final `raise` inside the span block.

```python
with self._telemetry.step_span(self.instance_id, step_name, step_index):
    for attempt in range(1, retry_cfg.max_retries + 2):
        ...
        # (retry sleep here)
    raise StepExecutionError(step_name, last_error)  # type: ignore[arg-type]
```

---

### [HIGH] Correctness — context.py:64-66
With `from __future__ import annotations` at the top of every file (required by the
style guide), all annotations are strings at runtime. `fn.__annotations__.get("return")`
returns the string `"MyModel"`, not the class. The `issubclass(return_hint, BaseModel)`
check always evaluates `False` for strings, so cached step results are always returned as
raw dicts even when the step is typed to return a BaseModel. This silently breaks
callers that expect model instances on replay.

Fix: use `typing.get_type_hints(fn)` (already imported in registry.py) instead of
`fn.__annotations__`:

```python
from typing import get_type_hints
# context.py:64
hints = get_type_hints(fn)
return_hint = hints.get("return")
if return_hint and isinstance(return_hint, type) and issubclass(return_hint, BaseModel):
    return return_hint.model_validate(cached)
return cached
```

---

### [HIGH] Missing error handling — worker.py:48
`asyncio.gather(..., return_exceptions=True)` silently discards all exceptions from
`_handle_message`. If the DB connection drops mid-execution, the exception is returned
as a value and never logged or acted on. The message stays invisible until its
visibility timeout expires with no signal that something went wrong.

Fix: inspect gather results and log unhandled exceptions.

```python
results = await asyncio.gather(*[self._handle_message(m) for m in msgs], return_exceptions=True)
for r in results:
    if isinstance(r, BaseException):
        _log.error("unhandled error in _handle_message", exc_info=r)
return len(msgs)
```

---

### [HIGH] Missing error handling — worker.py:62-102
`_handle_message` does not nack if `update_instance_state` (line 91) or the post-success
`fire` (line 94) raises. In that window the workflow has completed in memory but the
message is neither acked nor nacked. On redelivery after the visibility timeout the
workflow function runs again from scratch, re-executing all steps before the first
checkpoint write — a silent double-execution of side-effectful steps.

Fix: wrap the post-execution writes in a try/except that nacks on failure.

```python
try:
    output = result.model_dump() if isinstance(result, BaseModel) else result
    await self._state.update_instance_state(instance_id, WorkflowState.COMPLETED, output=output)
    await fire(self._plugins, "after_workflow", wf_event, result)
    await self._queue.ack(self._queue_name, msg.message_id)
except Exception:
    _log.exception("failed to finalize workflow %s, nacking", instance_id)
    await self._queue.nack(self._queue_name, msg.message_id)
```

---

### [MED] API consistency — app.py:68
`start()` is typed `input_model: Any` but unconditionally calls `.model_dump()` on it.
A plain dict or non-Pydantic object raises `AttributeError` at runtime with no useful
message. The actual constraint is `BaseModel`.

Fix: change the signature to `input_model: BaseModel`.

---

### [MED] Correctness — app.py:71
`start()` looks up the workflow by `workflow_fn.__name__`. If the workflow was
registered with a custom name (`@app.workflow(name="other_name")`), the registry key
is `"other_name"` but the lookup uses the Python function name — raising `KeyError` at
runtime. There is no test-time failure, only a silent contract violation for users who
use custom names.

Fix: store the resolved name on the function at registration time, then look it up in
`start()`.

In `registry.py:register_workflow`:
```python
fn._pyflows_name = wf_name  # type: ignore[attr-defined]
```

In `app.py:start`:
```python
name = getattr(workflow_fn, "_pyflows_name", workflow_fn.__name__)
defn = self.registry.get_workflow(name)
```

---

### [MED] Correctness — context.py:120-125
`RetryConfig.jitter: bool = True` is defined but never applied in the retry loop.
Multiple workers retrying the same step at the same time hit the exact same delays,
causing retry storms under load.

Fix:
```python
import random
delay = min(
    retry_cfg.initial_delay_seconds * (2 ** (attempt - 1)),
    retry_cfg.max_delay_seconds,
)
if retry_cfg.jitter:
    delay *= random.uniform(0.5, 1.0)
await asyncio.sleep(delay)
```

---

### [MED] Missing error handling — pgmq.py:49-52
`_ensure_queue` has no `_assert_initialized` guard. If called directly (as in `app.py:50`)
before `initialize()` completes, the failure is `AttributeError: 'NoneType' object has no
attribute 'create_queue'` rather than the cleaner `BackendNotInitializedError`.

Fix: add the guard at the top of `_ensure_queue`:
```python
async def _ensure_queue(self, queue: str) -> None:
    self._assert_initialized()
    if queue not in self._known_queues:
        await self._client.create_queue(queue)  # type: ignore[union-attr]
        self._known_queues.add(queue)
```

---

### [MED] Correctness — sql_exporter.py:81
`ast.walk` visits AST nodes in undefined order (not source order). Steps are appended to
`steps` as they are encountered, so the resulting pg_durable DSL may chain steps in the
wrong sequence. The `~>` operator is order-dependent.

Fix: collect `(lineno, StepSql)` and sort before building the DSL:
```python
steps_with_line: list[tuple[int, StepSql]] = []
for node in ast.walk(tree):
    ...
    steps_with_line.append((node.lineno, StepSql(...)))
steps = [s for _, s in sorted(steps_with_line, key=lambda x: x[0])]
```

---

### [LOW] API consistency — context.py:133
`StepContext` uses `workflow_id` for the parameter and attribute that holds the workflow
instance ID. Every other class in the codebase (`WorkflowContext`, `StepEvent`,
`WorkflowEvent`, `WorkflowStatus`) uses `instance_id` for the same concept. Plugin
authors receive both types and will be confused by the mismatch.

Fix: rename `StepContext.workflow_id` → `StepContext.instance_id`.

---

### [LOW] Type safety — types.py:27
`WorkflowStatus.output: Any | None`. Since `Any | None` simplifies to `Any`, this field
carries no type information. All outputs are JSONB-serialized dicts; the type should be
`dict[str, Any] | None`.

---

### [LOW] Missing error handling — pgmq.py:51
`_ensure_queue` does not catch the error pgmq raises when the queue already exists.
Multiple workers starting concurrently may both observe the queue absent from their
per-process `_known_queues` and both call `create_queue`, with one raising.

Fix:
```python
try:
    await self._client.create_queue(queue)  # type: ignore[union-attr]
except Exception:
    pass  # queue already exists — safe to ignore
self._known_queues.add(queue)
```

---

### [LOW] Correctness — worker.py:56-57
`process_batch` returns `len(msgs)`, i.e. the number of messages **dequeued**, not the
number successfully processed (because gather swallows exceptions). If all messages fail,
`process_batch` returns a non-zero value and the worker immediately polls again without
any backoff, spinning at full speed on persistent errors.

This is partially addressed by fixing the HIGH gather finding, but even after that fix
a persistent per-message error (e.g. corrupt payload) will cause a tight loop.

Fix: return the count of successfully processed messages, or add a minimum sleep when
all results are exceptions.

---

13 findings total (4 high, 5 medium, 4 low).
