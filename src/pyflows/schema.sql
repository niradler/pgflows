CREATE SCHEMA IF NOT EXISTS pyflows;

CREATE TABLE IF NOT EXISTS pyflows.workflow_definitions (
    name        TEXT PRIMARY KEY,
    version     INT  NOT NULL DEFAULT 1,
    config      JSONB NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pyflows.workflow_instances (
    instance_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_name TEXT NOT NULL REFERENCES pyflows.workflow_definitions(name),
    state         TEXT NOT NULL DEFAULT 'pending',
    input         JSONB NOT NULL,
    output        JSONB,
    error         TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_instances_state
    ON pyflows.workflow_instances(state);

CREATE INDEX IF NOT EXISTS idx_instances_workflow_name
    ON pyflows.workflow_instances(workflow_name);

CREATE TABLE IF NOT EXISTS pyflows.step_results (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    instance_id  UUID NOT NULL
        REFERENCES pyflows.workflow_instances(instance_id) ON DELETE CASCADE,
    step_name    TEXT NOT NULL,
    step_index   INT  NOT NULL,
    state        TEXT NOT NULL DEFAULT 'pending',
    input        JSONB NOT NULL,
    output       JSONB,
    error        TEXT,
    attempt      INT  NOT NULL DEFAULT 1,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    UNIQUE(instance_id, step_name, step_index)
);
