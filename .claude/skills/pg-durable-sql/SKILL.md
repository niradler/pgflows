---
name: pg-durable-sql
description: "Generate correct pg_durable SQL code for durable function workflows. USE WHEN: writing pg_durable DSL, creating durable functions, using df.start(), composing ~> |=> & | ?> !> @> operators, building ETL pipelines, loops, parallel joins, race conditions, conditional branching, HTTP requests, signals, cron scheduling, or variable substitution in pg_durable. DO NOT USE FOR: general PostgreSQL queries unrelated to pg_durable, Rust extension development, or duroxide internals."
---

# pg_durable SQL Generation

Generate correct, idiomatic pg_durable durable function SQL using the `df.*` schema functions and operators.

## Critical Rules

1. **All DSL expressions are TEXT.** Operators and functions return JSON-encoded TEXT strings representing a function graph. Only `df.start()` actually executes anything.
2. **SQL strings are auto-wrapped.** Plain SQL strings like `'SELECT 1'` are automatically converted to SQL nodes — you do NOT need `df.sql()`.
3. **Single-quote escaping.** Each DSL node is itself a single-quoted SQL string, so any single quotes _inside_ it must be doubled. To filter `status = 'pending'`, write the whole node as `'SELECT * FROM orders WHERE status = ''pending'''` (note the doubled quotes around `pending` and the closing `'''`).
4. **Operators are SQL-level custom operators.** They work on `TEXT` operands. Parentheses control grouping.
5. **`df.setvar()` must be called BEFORE `df.start()`.** Variables are captured at start time and are immutable during execution.
6. **Two variable syntaxes:** `{varname}` for durable function variables (from `df.setvar`), `$name` for result captures (from `|=>`). Do NOT mix them up.

## Operators — Complete Reference

| Operator | Name         | What It Does                       | Example                            |
| -------- | ------------ | ---------------------------------- | ---------------------------------- |
| `~>`     | Sequence     | Run left, then right               | `'SELECT 1' ~> 'SELECT 2'`         |
| `\|=>`   | Name/Capture | Capture result as named variable   | `'SELECT id FROM t' \|=> 'row_id'` |
| `&`      | Join         | Run both in parallel, wait for ALL | `'SELECT 1' & 'SELECT 2'`          |
| `\|`     | Race         | Run both in parallel, FIRST wins   | `'fast' \| df.sleep(30)`           |
| `?>`     | If-Then      | Conditional then branch            | `'SELECT true' ?> 'then SQL'`      |
| `!>`     | Else         | Conditional else branch            | `'cond' ?> 'then' !> 'else'`       |
| `@>`     | Loop         | Infinite loop (prefix operator)    | `@> ('body' ~> df.sleep(60))`      |

### Operator Precedence and Grouping

- `~>` chains left to right: `'A' ~> 'B' ~> 'C'` means A then B then C
- `&` groups parallel branches: `'A' & 'B' & 'C'` runs all three concurrently
- `?>` and `!>` combine for if/then/else: `condition ?> then_branch !> else_branch`
- `@>` is a PREFIX operator — it goes BEFORE the loop body: `@> (body)`
- Use parentheses to nest: `('A' & 'B') ~> 'C'` means run A and B in parallel, then C

## Functions — Complete Reference

### Node Creation

```sql
-- SQL node (rarely needed — auto-wrap handles this)
df.sql(query TEXT) → TEXT

-- Sleep/pause execution
df.sleep(seconds INT) → TEXT

-- Wait for cron schedule to match
df.wait_for_schedule(cron_expr TEXT) → TEXT
-- Cron format: 'minute hour day_of_month month day_of_week'
-- Examples: '* * * * *' (every min), '0 * * * *' (hourly), '0 0 * * *' (daily midnight)

-- HTTP request
df.http(
    url TEXT,                           -- Required
    method TEXT DEFAULT 'POST',         -- GET, POST, PUT, DELETE, PATCH
    body TEXT DEFAULT NULL,             -- JSON body (supports $var substitution)
    headers JSONB DEFAULT NULL,         -- Custom headers
    timeout_seconds INT DEFAULT 30      -- Timeout
) → TEXT
-- Returns JSON: {"status":200, "body":"...", "headers":{}, "ok":true, "duration_ms":245}

-- Wait for external signal
df.wait_for_signal(
    name TEXT,                          -- Signal name to wait for
    timeout_seconds INT DEFAULT NULL    -- NULL = wait forever
) → TEXT
-- Returns JSON: {"signal_name":"...", "timed_out":false, "data":{...}}
```

### Control Flow

```sql
-- Sequence (function variant of ~>)
df.seq(a TEXT, b TEXT) → TEXT

-- Name result (function variant of |=>)
df.as(fut TEXT, name TEXT) → TEXT

-- Parallel join — wait for ALL (function variant of &)
df.join(a TEXT, b TEXT) → TEXT
df.join3(a TEXT, b TEXT, c TEXT) → TEXT

-- Race — FIRST to complete wins (function variant of |)
df.race(a TEXT, b TEXT) → TEXT

-- Conditional branch (function variant of ?> !>)
df.if(condition TEXT, then_branch TEXT, else_branch TEXT) → TEXT

-- Conditional branch on whether a NAMED result has rows (no SQL re-run).
-- result_name is a capture from |=> earlier in the graph.
df.if_rows(result_name TEXT, then_branch TEXT, else_branch TEXT) → TEXT

-- Loop — infinite or while-condition
df.loop(body TEXT) → TEXT                            -- Infinite loop
df.loop(body TEXT, condition TEXT) → TEXT             -- While-loop: repeats while condition is truthy

-- Break from enclosing loop
df.break() → TEXT                                    -- Exit with NULL
df.break(value TEXT) → TEXT                          -- Exit with return value
```

### Execution & Management

```sql
-- Start a durable function (this is the ONLY function that triggers execution)
df.start(
    fut TEXT,                           -- The DSL graph expression
    label TEXT DEFAULT NULL,            -- Optional friendly name
    database TEXT DEFAULT NULL          -- Optional target database
) → TEXT                                -- Returns 8-char instance ID

-- Cancel a running instance
df.cancel(instance_id TEXT, reason TEXT DEFAULT 'Cancelled by user') → TEXT

-- Send signal to a waiting instance
df.signal(
    instance_id TEXT,                   -- Target instance
    signal_name TEXT,                   -- Must match df.wait_for_signal() name
    signal_data TEXT DEFAULT '{}'       -- Text payload; valid JSON remains structured, other text becomes a JSON string
) → TEXT

Use a JSON object when workflow SQL expects structured fields; use plain text for simple opaque values.

-- Query status
df.status(instance_id TEXT) → TEXT      -- 'pending', 'running', 'completed', 'failed', 'cancelled' (lowercase)

-- Get result
df.result(instance_id TEXT) → TEXT      -- JSON result from final node

-- Visualize graph (dry-run or live)
df.explain(input TEXT) → TEXT           -- Pass instance_id OR DSL expression
```

### Durable Function Variables

```sql
-- Set BEFORE df.start() — captured at start time, immutable during execution
df.setvar(name TEXT, value TEXT) → TEXT
df.getvar(name TEXT) → TEXT
df.unsetvar(name TEXT) → TEXT
df.clearvars() → TEXT
```

### Monitoring

```sql
df.list_instances(status_filter TEXT DEFAULT NULL, limit_count INT DEFAULT 100)
-- Columns: instance_id, label, function_name, status, execution_count, output

df.instance_info(instance_id TEXT)
-- Columns: instance_id, label, function_name, function_version, current_execution_id, status, output

df.instance_nodes(instance_id TEXT, last_n_executions INT DEFAULT 5)
-- Columns: execution_id, node_id, node_type, query, result_name, left_node, right_node, status, result, updated_at

df.instance_executions(instance_id TEXT, limit_count INT DEFAULT 5)
-- Columns: execution_id, status, event_count, duration_ms, output

df.metrics()
-- Columns: total_instances, running_instances, completed_instances, failed_instances, total_executions, total_events
```

Notes: `instance_nodes` returns **more rows than nodes you wrote** — the graph expands
into structural `THEN`/`JOIN`/`IF` nodes that each get a row. `metrics()` is **cluster-wide**,
not per-instance. `status` casing varies by function: `df.status()` is lowercase
(`completed`), while `instance_executions.status` / `instance_info.status` are Title-case
(`Completed`). From pgflows these are wrapped as typed models on `app.pg_durable`
(`instance_info`/`instance_nodes`/`instance_executions`/`metrics`).

## Variable Substitution

There are TWO separate variable systems. Do not confuse them.

### 1. Result Variables: `$name` (from `|=>`)

Capture a step's result and use it later in the same workflow:

```sql
SELECT df.start(
    'SELECT id FROM users WHERE active LIMIT 1' |=> 'user_id'
    ~> 'UPDATE users SET last_seen = now() WHERE id = $user_id'
);
```

- Set by: `|=>` operator or `df.as()` function
- Syntax in SQL: `$name`
- Scope: Within the running durable function instance
- Values: Auto-quoted strings, JSON objects accessible with `$var::jsonb`

### 2. Durable Function Variables: `{name}` (from `df.setvar()`)

Pre-configured values captured when `df.start()` is called:

```sql
SELECT df.setvar('api_url', 'https://api.example.com');
SELECT df.setvar('api_key', 'secret123');

SELECT df.start(
    df.http('{api_url}/data', 'GET', NULL, '{"Authorization": "Bearer {api_key}"}'::jsonb)
);
```

- Set by: `df.setvar()` BEFORE `df.start()`
- Syntax in SQL: `{name}`
- Captured: Snapshot taken at `df.start()` time
- Immutable: Cannot be changed during execution

### 3. System Variables: `{sys_*}`

Automatically available during execution:

- `{sys_instance_id}` — Current instance ID (8-char hex)
- `{sys_label}` — Instance label (if provided to `df.start()`)

## Condition Evaluation (Truthiness)

Used by: `?>`, `!>`, `df.if()`, `df.loop(body, condition)`

The first column of the first row is evaluated:

| Type    | Truthy                                                                                              | Falsy                                                    |
| ------- | --------------------------------------------------------------------------------------------------- | -------------------------------------------------------- |
| Boolean | `true`, `t`                                                                                         | `false`, `f`                                             |
| Number  | Any non-zero                                                                                        | `0`, `0.0`                                               |
| String  | `'true'`, `'t'`, `'yes'`, non-zero numeric strings, and any other non-empty string (e.g. `'hello'`) | `'false'`, `'f'`, `'no'`, `'0'`, `''` (empty/whitespace) |
| Array   | Non-empty `[1,2]`                                                                                   | Empty `[]`                                               |
| Object  | Non-empty `{"a":1}`                                                                                 | Empty `{}`                                               |
| NULL    | —                                                                                                   | Always falsy                                             |

**Best practice:** Use explicit boolean expressions:

```sql
-- Good: explicit boolean
'SELECT COUNT(*) > 0 FROM pending_tasks'
'SELECT EXISTS(SELECT 1 FROM orders WHERE status = ''pending'')'

-- Works but unclear: numeric truthiness
'SELECT COUNT(*) FROM pending_tasks'
```

## Common Patterns

### Sequential ETL Pipeline

```sql
SELECT df.start(
    'DELETE FROM target WHERE loaded_at < now() - interval ''7 days'''
    ~> 'UPDATE staging SET processed_at = now() WHERE processed_at IS NULL'
    ~> 'INSERT INTO target (data) SELECT data FROM staging WHERE processed_at IS NOT NULL',
    'etl-pipeline'
);
```

### Variable Capture and Reuse

```sql
SELECT df.start(
    'SELECT id FROM orders WHERE status = ''pending'' LIMIT 1' |=> 'order_id'
    ~> 'UPDATE orders SET status = ''processing'' WHERE id = $order_id'
    ~> df.sleep(2)
    ~> 'UPDATE orders SET status = ''completed'' WHERE id = $order_id',
    'process-order'
);
```

### Parallel Fan-Out / Fan-In

```sql
-- Using & operator
SELECT df.start(
    ('SELECT COUNT(*) FROM users' & 'SELECT COUNT(*) FROM orders')
    ~> 'INSERT INTO logs (msg) VALUES (''Counts collected'')',
    'parallel-counts'
);

-- Using df.join3() function
SELECT df.start(
    df.join3(
        'SELECT COUNT(*) FROM users',
        'SELECT COUNT(*) FROM orders',
        'SELECT COUNT(*) FROM products'
    ),
    'three-way-count'
);
```

> **Parallelism limitations (observed on the bundled build).** A join (`&`/`join3`) of
> *bare-SQL* branches may never finalize — children show `completed`, the JOIN stays
> `running`. A loop must **not** share an instance with any parallel node (`loop ~> (a&b)`,
> `join3 ~> loop` deadlock under ContinueAsNew replay) — run them as separate `df.start`
> instances. `|` (race) is reliable only as a *terminal* node and does not cancel the loser
> (both side-effects can fire); don't sequence after a race. Accumulated hung instances
> exhaust the worker connection pool — cancel stale `running` instances or restart the DB.

### Race with Timeout

```sql
SELECT df.start(
    df.race(
        'SELECT slow_query()',
        df.sleep(30) ~> 'SELECT ''timeout'' AS result'
    ),
    'query-with-timeout'
);
```

### Conditional Branching

```sql
-- Using operators
SELECT df.start(
    'SELECT COUNT(*) > 10 FROM task_queue WHERE status = ''pending'''
        ?> 'INSERT INTO logs (msg) VALUES (''High load!'')'
        !> 'INSERT INTO logs (msg) VALUES (''Normal load'')',
    'load-check'
);

-- Using df.if() function
SELECT df.start(
    df.if(
        'SELECT EXISTS(SELECT 1 FROM orders WHERE status = ''pending'')',
        'UPDATE orders SET status = ''processing'' WHERE status = ''pending''',
        'INSERT INTO logs (msg) VALUES (''Nothing to process'')'
    ),
    'conditional-processing'
);
```

### Infinite Loop with Sleep

```sql
SELECT df.start(
    @> (
        'INSERT INTO heartbeats (ts) VALUES (now())'
        ~> df.sleep(30)
    ),
    'heartbeat'
);

-- Cancel with: SELECT df.cancel('instance_id', 'Stopping heartbeat');
```

### While-Loop with Break

```sql
SELECT df.start(
    df.loop(
        'UPDATE counter SET val = val + 1'
        ~> df.if(
            'SELECT val >= 10 FROM counter',
            df.break('{"done": true}'),
            'SELECT ''continuing'''
        )
    ),
    'counted-loop'
);
```

### Cron Scheduled Job

```sql
SELECT df.start(
    @> (
        'DELETE FROM logs WHERE created_at < now() - interval ''30 days'''
        ~> df.wait_for_schedule('0 0 * * *')  -- Daily at midnight
    ),
    'daily-cleanup'
);
```

### HTTP Request with Variable Substitution

```sql
SELECT df.setvar('webhook_url', 'https://hooks.example.com/notify');

SELECT df.start(
    'SELECT id, status FROM orders WHERE id = 1' |=> 'order'
    ~> df.http(
        '{webhook_url}',
        'POST',
        '{"order": $order}'
    ),
    'order-webhook'
);
```

### Signal-Based Approval Workflow

```sql
SELECT df.start(
    'INSERT INTO logs (msg) VALUES (''Requesting approval'')'
    ~> df.wait_for_signal('approval', 3600) |=> 'decision'
    ~> df.if(
        -- $decision is the full envelope {"signal_name","timed_out","data":{...}};
        -- your df.signal payload lands under ->'data'. Check timed_out, then read data.
        'SELECT NOT ($decision::jsonb->>''timed_out'')::boolean
              AND coalesce(($decision::jsonb->''data''->>''approved'')::boolean, false)',
        'UPDATE orders SET status = ''approved''',
        'UPDATE orders SET status = ''rejected'''
    ),
    'approval-flow'
);

-- From another session: SELECT df.signal('inst_id', 'approval', '{"approved": true}');
-- The {"approved": true} you pass is nested under ->'data' in the captured $decision.
```

> **Signal envelope.** `df.wait_for_signal` returns `{"signal_name","timed_out","data":{…}}`.
> A `|=>` capture of it is that whole object — the payload from `df.signal` is under
> `->'data'`. Reading `$decision::jsonb->>'approved'` at the top level is always NULL
> (falsy), so the flow silently takes the *else* branch on a real approval.

### Multi-Database Execution

```sql
-- Run in a different database on the same cluster
SELECT df.start(
    'INSERT INTO reports (date, total) SELECT now(), count(*) FROM events',
    'analytics-report',
    'analytics'    -- target database name
);

-- Or with named parameter
SELECT df.start('SELECT 1', database => 'other_db');
```

## Common Mistakes to Avoid

1. **Forgetting to double single quotes inside SQL strings:**

   ```sql
   -- WRONG: breaks SQL parsing
   'SELECT ''pending'''   -- This is correct for the string 'pending'
   'SELECT 'pending''     -- WRONG: unbalanced quotes
   ```

2. **Using `{var}` when you mean `$var` (or vice versa):**

   ```sql
   -- {var} = durable function variable from df.setvar()
   -- $var  = result capture from |=>
   ```

3. **Calling `df.setvar()` inside a running workflow:**

   ```sql
   -- WRONG: will error at runtime
   SELECT df.start('SELECT 1' ~> df.sql('SELECT df.setvar(''x'', ''y'')'));

   -- CORRECT: set before starting
   SELECT df.setvar('x', 'y');
   SELECT df.start('SELECT {x}');
   ```

4. **Forgetting `@>` is a PREFIX operator:**

   ```sql
   -- WRONG: @> goes BEFORE the body
   'body' @> df.sleep(60)

   -- CORRECT
   @> ('body' ~> df.sleep(60))
   ```

5. **Not wrapping parallel branches in parentheses before sequencing:**

   ```sql
   -- WRONG: ambiguous
   'A' & 'B' ~> 'C'

   -- CORRECT: explicit grouping
   ('A' & 'B') ~> 'C'
   ```

6. **Using `df.start()` inside a DSL expression:**

   ```sql
   -- WRONG: df.start() is not a DSL node
   'SELECT 1' ~> df.start('SELECT 2')

   -- CORRECT: df.start() wraps the entire expression
   SELECT df.start('SELECT 1' ~> 'SELECT 2');
   ```

7. **Expecting `df.start()` to block until completion:**
   ```sql
   -- df.start() returns IMMEDIATELY with an instance ID
   -- Use df.status() or df.wait_for_completion() to check progress
   SELECT df.start('long running query');  -- Returns instantly
   ```

## Hard-won gotchas (verified against a live pg_durable + pgmq)

Subtle, cost real debugging time, confirmed by running real workflows — not guesses.

1. **The DSL must be EVALUATED by Postgres, never passed as a bound parameter.**
   `~>`, `|=>`, `&`, `|`, `?>`, `df.http()` are SQL-level operators/functions: Postgres
   evaluates them to build the graph TEXT, then `df.start` runs it.

   ```python
   # WRONG (e.g. asyncpg): operators never evaluated → df gets inert text → fails
   await conn.fetchrow("SELECT df.start($1)", "'A' ~> 'B'")
   # CORRECT: interpolate the DSL into the statement; bind only label/database
   await conn.fetchrow(f"SELECT df.start({dsl}, $1)", label)   # dsl = "'A' ~> 'B'"
   ```
   A bare SQL string still needs quotes: `df.start('SELECT 1')`, not `df.start(SELECT 1)`.

2. **`$capture` expands to the captured node's FIRST-COLUMN VALUE, not the envelope.**
   `df.result`/`instance_nodes` show `{"rows":[{"col":<v>}],"row_count":1}`, but inside
   a later node `$capture` is just `<v>`.

   ```sql
   'SELECT json_build_object(''val'',8) AS result' |=> 'r'
   -- $r IS {"val":8} (the column value), NOT {"rows":[{"result":{"val":8}}]}
   ~> 'SELECT ($r::jsonb->>''val'')::int'
   ```

3. **`~>` binds tighter than `&` / `|`.** `d ~> a & b` parses as `(d ~> a) & b`, so the
   right branch never sees `d`'s captures. Group the whole parallel block:
   `d ~> ((a) & (b))` — wrap the entire `&` expression, not just each operand.

4. **Thread data with captures, not many `df.setvar`s.** With MORE THAN ONE durable
   var set, pg_durable serializes the vars snapshot with non-deterministic key order; a
   **parallel-join replay** then dies with
   `error="nondeterministic: schedule mismatch"`. Keep one config var and pass step
   data through `|=>` captures. (Parallel join works perfectly with ≤1 var — it is the
   orchestrator's job and it handles concurrency + durability correctly.)

5. **Signals are dropped if sent before the waiter is registered.** `df.signal(id,'s')`
   only resumes an instance that has already reached `df.wait_for_signal('s')`. A worker
   woken by a `pg_notify` emitted just before the wait node usually signals too early →
   lost → instance hangs. For worker callbacks, poll a result table instead (row
   persists, race-free): `... ~> df.loop(df.sleep(1), 'SELECT NOT EXISTS(SELECT 1 FROM
   results WHERE key=...)') ~> 'SELECT result FROM results WHERE key=...'`. Reserve
   `wait_for_signal` for genuinely external events (approvals/webhooks) reached first.

6. **Extension name vs schema name.** The extension is `pg_durable`
   (`SELECT 1 FROM pg_extension WHERE extname='pg_durable'`); it only creates the `df`
   schema. Detecting with `extname='df'` is always false.

7. **Deploying alongside pgmq.** `pg_durable` is a pgrx/Rust extension (build from
   source; needs `shared_preload_libraries='pg_durable'`). `pgmq` 1.5.x is pure SQL — its
   extension files can be copied straight into a `pg_durable` PG17 image, no recompile.
