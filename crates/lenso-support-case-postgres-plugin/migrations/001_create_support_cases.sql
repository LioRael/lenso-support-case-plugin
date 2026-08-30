CREATE TABLE support_case_sequences (
    organization_id TEXT PRIMARY KEY,
    next_number BIGINT NOT NULL CHECK (next_number > 0)
);

CREATE TABLE support_cases (
    case_id UUID PRIMARY KEY,
    organization_id TEXT NOT NULL,
    identifier TEXT NOT NULL,
    requester_subject TEXT NOT NULL,
    creator_subject TEXT NOT NULL,
    title TEXT NOT NULL,
    description TEXT NOT NULL,
    priority TEXT NOT NULL CHECK (priority IN ('low', 'normal', 'high', 'urgent')),
    state TEXT NOT NULL CHECK (state IN ('open', 'in_progress', 'waiting_customer', 'resolved', 'closed')),
    assignee_subject TEXT,
    revision BIGINT NOT NULL CHECK (revision > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMPTZ,
    closed_at TIMESTAMPTZ,
    UNIQUE (organization_id, identifier)
);

CREATE INDEX support_cases_organization_updated_idx
    ON support_cases (organization_id, updated_at DESC, case_id DESC);
CREATE INDEX support_cases_requester_idx
    ON support_cases (organization_id, requester_subject, updated_at DESC, case_id DESC);
CREATE INDEX support_cases_assignee_idx
    ON support_cases (organization_id, assignee_subject, updated_at DESC, case_id DESC);

CREATE TABLE support_case_messages (
    message_id UUID PRIMARY KEY,
    case_id UUID NOT NULL REFERENCES support_cases(case_id) ON DELETE CASCADE,
    visibility TEXT NOT NULL CHECK (visibility IN ('public', 'internal')),
    author_subject TEXT NOT NULL,
    body TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX support_case_messages_case_created_idx
    ON support_case_messages (case_id, created_at ASC, message_id ASC);

CREATE TABLE support_case_activity (
    activity_id UUID PRIMARY KEY,
    organization_id TEXT NOT NULL,
    case_id UUID NOT NULL REFERENCES support_cases(case_id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    actor_subject TEXT NOT NULL,
    case_revision BIGINT NOT NULL CHECK (case_revision > 0),
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX support_case_activity_case_idx
    ON support_case_activity (case_id, created_at ASC, activity_id ASC);

CREATE TABLE support_case_commands (
    caller_instance TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    organization_id TEXT NOT NULL,
    case_id UUID NOT NULL REFERENCES support_cases(case_id) ON DELETE CASCADE,
    operation TEXT NOT NULL,
    request_hash BYTEA NOT NULL,
    response JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (caller_instance, idempotency_key)
);

CREATE INDEX support_case_commands_scope_idx
    ON support_case_commands (organization_id, case_id);

CREATE TABLE support_case_retention_receipts (
    action_id TEXT PRIMARY KEY,
    request_hash BYTEA NOT NULL,
    receipt TEXT NOT NULL,
    anonymized_subject TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
