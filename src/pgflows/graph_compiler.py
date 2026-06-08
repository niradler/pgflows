from __future__ import annotations

from collections.abc import Iterator

from pgflows.dsl import (
    DslNode,
    join3,
    loop,
    sleep,
    sql_node,
    wait_for_schedule,
    wait_for_signal,
    worker_step,
)
from pgflows.exceptions import PgflowsError
from pgflows.graph import (
    BranchNode,
    Condition,
    GraphSpec,
    LoopNode,
    Node,
    ParallelNode,
    SequenceNode,
    SleepNode,
    StepNode,
    WaitScheduleNode,
    WaitSignalNode,
)

# The first node of a graph receives the GraphSpec.input payload (the {input} durable var).
_INITIAL_INPUT = "'{input}'::jsonb"


class GraphCompileError(PgflowsError):
    """A GraphSpec cannot be compiled to a runnable pg_durable graph."""


def compile_graph(spec: GraphSpec, *, step_queue: str, notify_channel: str) -> DslNode:
    """Compile a GraphSpec to a pg_durable DSL node, ready for ``df.start()``.

    Validates the spec against verified pg_durable composition limits first (raising
    ``GraphCompileError`` with a fix-it message), then emits the richest correct DSL.
    """
    _guard(spec.root)
    ctx = _Ctx(step_queue, notify_channel)
    node, _ = _compile(spec.root, ctx, _INITIAL_INPUT)
    return node


class _Ctx:
    def __init__(self, step_queue: str, notify_channel: str) -> None:
        self.step_queue = step_queue
        self.notify_channel = notify_channel
        self._n = 0

    def fresh(self, hint: str) -> str:
        """A unique, ident-safe capture/result name (df vars must be identifiers)."""
        self._n += 1
        safe = "".join(c if c.isalnum() else "_" for c in hint)
        return f"pgflows_{safe}_{self._n}"


def _compile(node: Node, ctx: _Ctx, input_ref: str) -> tuple[DslNode, str | None]:
    """Return (DSL node, output ref). output ref is a jsonb SQL expression downstream
    nodes can consume as their input, or None when the node produces no threadable output.
    """
    if isinstance(node, StepNode):
        return _compile_step(node, ctx, input_ref)
    if isinstance(node, SleepNode):
        return sleep(node.seconds), None
    if isinstance(node, WaitScheduleNode):
        return wait_for_schedule(node.cron), None
    if isinstance(node, WaitSignalNode):
        cap = ctx.fresh("signal")
        return wait_for_signal(node.signal, node.timeout).capture(cap), f"${cap}::jsonb"
    if isinstance(node, SequenceNode):
        return _compile_sequence(node, ctx, input_ref)
    if isinstance(node, ParallelNode):
        return _compile_parallel(node, ctx, input_ref)
    if isinstance(node, BranchNode):
        return _compile_branch(node, ctx, input_ref)
    if isinstance(node, LoopNode):
        return _compile_loop(node, ctx, input_ref)
    raise GraphCompileError(f"unknown node type: {type(node).__name__}")


def _compile_step(node: StepNode, ctx: _Ctx, input_ref: str) -> tuple[DslNode, str]:
    cap = node.capture or ctx.fresh(node.step)
    expr = node.input if node.input is not None else input_ref
    built = worker_step(
        node.step,
        result_key=f"{{sys_instance_id}}:{cap}",
        queue=ctx.step_queue,
        notify_channel=ctx.notify_channel,
        input_expr=expr,
        capture=cap,
    )
    return built, f"${cap}::jsonb"


def _compile_sequence(node: SequenceNode, ctx: _Ctx, input_ref: str) -> tuple[DslNode, str | None]:
    if not node.nodes:
        raise GraphCompileError("a sequence must contain at least one node")
    chained: DslNode | None = None
    current = input_ref
    last_output: str | None = None
    for child in node.nodes:
        built, output = _compile(child, ctx, current)
        chained = built if chained is None else chained >> built
        if output is not None:
            current = output
            last_output = output
    assert chained is not None
    return chained, last_output


def _compile_parallel(node: ParallelNode, ctx: _Ctx, input_ref: str) -> tuple[DslNode, str | None]:
    if len(node.branches) < 2:
        raise GraphCompileError("a parallel node requires at least 2 branches")
    built: list[DslNode] = []
    outputs: list[str | None] = []
    for branch in node.branches:
        bnode, boutput = _compile(branch, ctx, input_ref)
        built.append(bnode)
        outputs.append(boutput)

    if node.mode == "race":
        raced = built[0]
        for extra in built[1:]:
            raced = raced | extra
        return raced, None

    if len(built) == 3:
        joined = join3(built[0], built[1], built[2])
    else:
        joined = built[0]
        for extra in built[1:]:
            joined = joined & extra

    # After a join, branch captures are all visible — merge them into one object so a
    # following step has a single, named input. Override per-step via StepNode.input.
    pairs = [(f"b{i}", out) for i, out in enumerate(outputs) if out is not None]
    if not pairs:
        return joined, None
    merged = "jsonb_build_object(" + ", ".join(f"'{key}', ({out})" for key, out in pairs) + ")"
    return joined, merged


def _compile_branch(node: BranchNode, ctx: _Ctx, input_ref: str) -> tuple[DslNode, None]:
    condition = _compile_condition(node.condition, ctx, input_ref)
    then_node, _ = _compile(node.then, ctx, input_ref)
    if node.else_ is None:
        return condition.if_then(then_node), None
    else_node, _ = _compile(node.else_, ctx, input_ref)
    return condition.if_then(then_node, else_node), None


def _compile_loop(node: LoopNode, ctx: _Ctx, input_ref: str) -> tuple[DslNode, None]:
    body, _ = _compile(node.body, ctx, input_ref)
    if node.while_ is None:
        return loop(body), None
    return loop(body, _compile_condition(node.while_, ctx, input_ref)), None


def _compile_condition(condition: Condition, ctx: _Ctx, input_ref: str) -> DslNode:
    """Run the condition step, then emit a SQL node that SELECTs a single boolean — the
    value ``?>`` / ``df.loop`` branch on (first row's first column; a ``false`` row takes
    the else arm, ``true`` takes then). Truthy reads ``->>'result'`` when present (the
    common ``{result: bool}`` shape), else the whole value's text; falsy values are
    ``false``/``0``/``null``/empty.
    """
    cap = ctx.fresh("cond")
    expr = condition.input if condition.input is not None else input_ref
    step = worker_step(
        condition.step,
        result_key=f"{{sys_instance_id}}:{cap}",
        queue=ctx.step_queue,
        notify_channel=ctx.notify_channel,
        input_expr=expr,
        capture=cap,
    )
    truthy = sql_node(
        f"SELECT COALESCE(NULLIF(${cap}::jsonb->>'result', ''), ${cap}::jsonb::text) "
        "NOT IN ('false', '0', 'null', '\"\"', '')"
    )
    return step >> truthy


def _guard(root: Node) -> None:
    has_loop = any(isinstance(n, LoopNode) for n in _walk(root))
    has_parallel = any(isinstance(n, ParallelNode) for n in _walk(root))
    if has_loop and has_parallel:
        raise GraphCompileError(
            "a loop and a parallel node cannot share one pg_durable instance "
            "(ContinueAsNew replay deadlocks). Model them as separate graphs / start_graph "
            "calls — e.g. a cron loop whose body enqueue()s a separate run."
        )
    _check_race_terminal(root, is_tail=True)


def _walk(node: Node) -> Iterator[Node]:
    yield node
    if isinstance(node, SequenceNode):
        for child in node.nodes:
            yield from _walk(child)
    elif isinstance(node, ParallelNode):
        for child in node.branches:
            yield from _walk(child)
    elif isinstance(node, BranchNode):
        yield from _walk(node.then)
        if node.else_ is not None:
            yield from _walk(node.else_)
    elif isinstance(node, LoopNode):
        yield from _walk(node.body)


def _check_race_terminal(node: Node, *, is_tail: bool) -> None:
    if isinstance(node, ParallelNode) and node.mode == "race" and not is_tail:
        raise GraphCompileError(
            "parallel mode='race' must be terminal (nothing may run after it); "
            "'race ~> next' hangs in pg_durable. Move it to the end of its sequence."
        )
    if isinstance(node, SequenceNode):
        last = len(node.nodes) - 1
        for i, child in enumerate(node.nodes):
            _check_race_terminal(child, is_tail=is_tail and i == last)
    elif isinstance(node, ParallelNode):
        for child in node.branches:
            _check_race_terminal(child, is_tail=False)
    elif isinstance(node, BranchNode):
        _check_race_terminal(node.then, is_tail=is_tail)
        if node.else_ is not None:
            _check_race_terminal(node.else_, is_tail=is_tail)
    elif isinstance(node, LoopNode):
        _check_race_terminal(node.body, is_tail=False)


__all__ = ["GraphCompileError", "compile_graph"]
