CREATE TABLE IF NOT EXISTS board_threads (
    id text PRIMARY KEY,
    tags text[] NOT NULL,
    status text NOT NULL DEFAULT 'open',
    resolution text,
    resolved_memory_id text,
    resolved_by text,
    resolved_at timestamptz,
    message_count integer NOT NULL DEFAULT 0,
    created_by text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT board_threads_status_valid CHECK (status IN ('open', 'resolved')),
    CONSTRAINT board_threads_tags_nonempty CHECK (cardinality(tags) BETWEEN 1 AND 8),
    CONSTRAINT board_threads_resolution_state CHECK (
        (status = 'open' AND resolved_at IS NULL)
        OR (status = 'resolved' AND resolved_at IS NOT NULL AND length(btrim(resolution)) > 0)
    )
);

CREATE INDEX IF NOT EXISTS board_threads_tags_idx ON board_threads USING gin(tags);
CREATE INDEX IF NOT EXISTS board_threads_status_updated_idx ON board_threads(status, updated_at DESC);

CREATE TABLE IF NOT EXISTS board_messages (
    id text PRIMARY KEY,
    thread_id text NOT NULL REFERENCES board_threads(id) ON DELETE CASCADE,
    body text NOT NULL,
    author text,
    created_by text NOT NULL,
    device_code text,
    installation_code text,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT board_messages_body_nonempty CHECK (length(btrim(body)) > 0)
);

CREATE INDEX IF NOT EXISTS board_messages_thread_idx ON board_messages(thread_id, created_at);
