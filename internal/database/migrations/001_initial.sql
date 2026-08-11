CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS namespaces (
    code text PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT namespaces_code_valid CHECK (code ~ '^[a-z0-9][a-z0-9._-]{1,126}[a-z0-9]$')
);

CREATE TABLE IF NOT EXISTS devices (
    device_code text PRIMARY KEY,
    display_name text NOT NULL,
    status text NOT NULL DEFAULT 'active',
    merged_into_device_code text REFERENCES devices(device_code),
    version bigint NOT NULL DEFAULT 1,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT devices_status_valid CHECK (status IN ('active', 'retired', 'merged'))
);

CREATE TABLE IF NOT EXISTS installations (
    installation_code text PRIMARY KEY,
    device_code text NOT NULL REFERENCES devices(device_code),
    tailnet_identity text,
    hostname text,
    status text NOT NULL DEFAULT 'active',
    version bigint NOT NULL DEFAULT 1,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    last_seen_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT installations_status_valid CHECK (status IN ('active', 'retired', 'migrated'))
);

CREATE UNIQUE INDEX IF NOT EXISTS installations_tailnet_identity_idx
    ON installations(tailnet_identity)
    WHERE tailnet_identity IS NOT NULL AND status = 'active';

CREATE TABLE IF NOT EXISTS device_signals (
    installation_code text NOT NULL REFERENCES installations(installation_code) ON DELETE CASCADE,
    device_code text NOT NULL REFERENCES devices(device_code),
    signal_type text NOT NULL,
    signal_digest bytea NOT NULL,
    weight integer NOT NULL,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    last_seen_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    PRIMARY KEY (installation_code, signal_type),
    CONSTRAINT device_signals_weight_valid CHECK (weight BETWEEN 1 AND 100)
);

CREATE INDEX IF NOT EXISTS device_signals_lookup_idx
    ON device_signals(signal_type, signal_digest, device_code);

CREATE TABLE IF NOT EXISTS device_audit (
    id bigserial PRIMARY KEY,
    installation_code text,
    source_device_code text,
    target_device_code text,
    operation text NOT NULL,
    actor text NOT NULL,
    details jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp()
);

CREATE TABLE IF NOT EXISTS memories (
    id text PRIMARY KEY,
    namespace text NOT NULL REFERENCES namespaces(code),
    scope_type text NOT NULL,
    scope_id text NOT NULL,
    device_code text REFERENCES devices(device_code),
    installation_code text REFERENCES installations(installation_code),
    workspace_code text,
    memory_type text NOT NULL,
    title text NOT NULL,
    content text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    tags text[] NOT NULL DEFAULT '{}'::text[],
    lifecycle_status text NOT NULL DEFAULT 'active',
    verification_state text NOT NULL DEFAULT 'unverified',
    confidence real NOT NULL DEFAULT 0.5,
    evidence jsonb NOT NULL DEFAULT '[]'::jsonb,
    source_id text,
    source_path text,
    source_hash text,
    source_range jsonb,
    observed_at timestamptz,
    expires_at timestamptz,
    version bigint NOT NULL DEFAULT 1,
    supersedes_id text REFERENCES memories(id),
    created_by text NOT NULL,
    updated_by text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    deleted_at timestamptz,
    search_text text GENERATED ALWAYS AS (
        coalesce(title, '') || E'\n' || coalesce(content, '') || E'\n' || coalesce(source_path, '')
    ) STORED,
    search_tsv tsvector GENERATED ALWAYS AS (
        setweight(to_tsvector('simple', coalesce(title, '')), 'A') ||
        setweight(to_tsvector('simple', coalesce(content, '')), 'B') ||
        setweight(to_tsvector('simple', coalesce(source_path, '')), 'C')
    ) STORED,
    embedding vector(1024),
    embedding_model text,
    embedded_at timestamptz,
    CONSTRAINT memories_scope_type_valid CHECK (scope_type IN ('installation', 'device', 'workspace', 'project', 'global')),
    CONSTRAINT memories_type_valid CHECK (memory_type IN ('fact', 'experiment', 'hypothesis', 'decision', 'artifact', 'procedure', 'incident', 'summary')),
    CONSTRAINT memories_status_valid CHECK (lifecycle_status IN ('active', 'superseded', 'refuted', 'expired', 'deleted')),
    CONSTRAINT memories_verification_valid CHECK (verification_state IN ('unverified', 'supported', 'confirmed', 'contested')),
    CONSTRAINT memories_confidence_valid CHECK (confidence >= 0 AND confidence <= 1),
    CONSTRAINT memories_title_nonempty CHECK (length(btrim(title)) > 0),
    CONSTRAINT memories_content_nonempty CHECK (length(btrim(content)) > 0)
);

CREATE INDEX IF NOT EXISTS memories_visibility_idx
    ON memories(namespace, lifecycle_status, scope_type, scope_id, expires_at);
CREATE INDEX IF NOT EXISTS memories_updated_idx ON memories(namespace, updated_at DESC);
CREATE INDEX IF NOT EXISTS memories_observed_idx ON memories(namespace, observed_at DESC);
CREATE INDEX IF NOT EXISTS memories_tags_idx ON memories USING gin(tags);
CREATE INDEX IF NOT EXISTS memories_metadata_idx ON memories USING gin(metadata jsonb_path_ops);
CREATE INDEX IF NOT EXISTS memories_search_tsv_idx ON memories USING gin(search_tsv);
CREATE INDEX IF NOT EXISTS memories_search_trgm_idx ON memories USING gin(search_text gin_trgm_ops);
CREATE INDEX IF NOT EXISTS memories_source_path_trgm_idx ON memories USING gin(source_path gin_trgm_ops);
CREATE INDEX IF NOT EXISTS memories_embedding_hnsw_idx
    ON memories USING hnsw(embedding vector_cosine_ops)
    WHERE embedding IS NOT NULL;

CREATE TABLE IF NOT EXISTS memory_revisions (
    id bigserial PRIMARY KEY,
    memory_id text NOT NULL,
    namespace text NOT NULL,
    from_version bigint,
    to_version bigint,
    operation text NOT NULL,
    before_snapshot jsonb,
    after_snapshot jsonb,
    changed_by text NOT NULL,
    reason text,
    changed_at timestamptz NOT NULL DEFAULT statement_timestamp()
);

CREATE UNIQUE INDEX IF NOT EXISTS memory_revisions_version_idx
    ON memory_revisions(memory_id, to_version)
    WHERE to_version IS NOT NULL;
CREATE INDEX IF NOT EXISTS memory_revisions_history_idx
    ON memory_revisions(namespace, memory_id, changed_at DESC);

CREATE TABLE IF NOT EXISTS memory_relations (
    id bigserial PRIMARY KEY,
    namespace text NOT NULL REFERENCES namespaces(code),
    subject_memory_id text NOT NULL REFERENCES memories(id),
    relation_type text NOT NULL,
    object_memory_id text NOT NULL REFERENCES memories(id),
    evidence jsonb NOT NULL DEFAULT '[]'::jsonb,
    reason text,
    created_by text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT memory_relations_type_valid CHECK (relation_type IN ('supersedes', 'refutes')),
    CONSTRAINT memory_relations_not_self CHECK (subject_memory_id <> object_memory_id),
    UNIQUE (subject_memory_id, relation_type, object_memory_id)
);

CREATE TABLE IF NOT EXISTS ingestion_roots (
    id text PRIMARY KEY,
    namespace text NOT NULL REFERENCES namespaces(code),
    scope_type text NOT NULL,
    scope_id text NOT NULL,
    device_code text REFERENCES devices(device_code),
    installation_code text REFERENCES installations(installation_code),
    workspace_code text,
    root_path text NOT NULL,
    normalized_root_path text NOT NULL,
    recursive boolean NOT NULL,
    include_patterns text[] NOT NULL DEFAULT '{}'::text[],
    exclude_patterns text[] NOT NULL DEFAULT '{}'::text[],
    watch_mode text NOT NULL,
    parser text NOT NULL,
    generation bigint NOT NULL DEFAULT 0,
    status text NOT NULL DEFAULT 'active',
    expires_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT ingestion_roots_scope_valid CHECK (scope_type IN ('installation', 'device', 'workspace', 'project', 'global')),
    CONSTRAINT ingestion_roots_watch_valid CHECK (watch_mode IN ('once', 'sync', 'watch')),
    CONSTRAINT ingestion_roots_status_valid CHECK (status IN ('active', 'stopped', 'expired', 'deleted')),
    UNIQUE (namespace, installation_code, normalized_root_path)
);

CREATE TABLE IF NOT EXISTS ingestion_jobs (
    id text PRIMARY KEY,
    root_id text NOT NULL REFERENCES ingestion_roots(id),
    namespace text NOT NULL REFERENCES namespaces(code),
    generation bigint NOT NULL,
    status text NOT NULL,
    files_seen integer NOT NULL DEFAULT 0,
    created_count integer NOT NULL DEFAULT 0,
    updated_count integer NOT NULL DEFAULT 0,
    unchanged_count integer NOT NULL DEFAULT 0,
    deleted_count integer NOT NULL DEFAULT 0,
    chunk_count integer NOT NULL DEFAULT 0,
    errors jsonb NOT NULL DEFAULT '[]'::jsonb,
    started_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    finished_at timestamptz,
    CONSTRAINT ingestion_jobs_status_valid CHECK (status IN ('running', 'completed', 'partial', 'failed')),
    UNIQUE (root_id, generation)
);

CREATE TABLE IF NOT EXISTS source_contents (
    id text PRIMARY KEY,
    namespace text NOT NULL REFERENCES namespaces(code),
    content_hash text NOT NULL,
    parser text NOT NULL,
    parser_version integer NOT NULL DEFAULT 1,
    chunker_version integer NOT NULL DEFAULT 1,
    content text,
    size bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    UNIQUE (namespace, content_hash, parser, parser_version, chunker_version)
);

CREATE TABLE IF NOT EXISTS sources (
    id text PRIMARY KEY,
    namespace text NOT NULL REFERENCES namespaces(code),
    root_id text NOT NULL REFERENCES ingestion_roots(id),
    scope_type text NOT NULL,
    scope_id text NOT NULL,
    device_code text REFERENCES devices(device_code),
    installation_code text REFERENCES installations(installation_code),
    workspace_code text,
    current_content_id text NOT NULL REFERENCES source_contents(id),
    original_absolute_path text NOT NULL,
    normalized_path text NOT NULL,
    relative_path text,
    source_uri text NOT NULL,
    content_hash text NOT NULL,
    size bigint NOT NULL,
    mtime timestamptz NOT NULL,
    parser text NOT NULL,
    generation bigint NOT NULL,
    lifecycle_status text NOT NULL DEFAULT 'active',
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    expires_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT sources_status_valid CHECK (lifecycle_status IN ('active', 'expired', 'deleted')),
    UNIQUE (namespace, installation_code, normalized_path),
    UNIQUE (source_uri)
);

CREATE INDEX IF NOT EXISTS sources_visibility_idx
    ON sources(namespace, lifecycle_status, scope_type, scope_id, expires_at);
CREATE INDEX IF NOT EXISTS sources_path_trgm_idx ON sources USING gin(original_absolute_path gin_trgm_ops);
CREATE INDEX IF NOT EXISTS sources_hash_idx ON sources(namespace, content_hash);

CREATE TABLE IF NOT EXISTS source_versions (
    id bigserial PRIMARY KEY,
    source_id text NOT NULL REFERENCES sources(id),
    generation bigint NOT NULL,
    content_id text NOT NULL REFERENCES source_contents(id),
    content_hash text NOT NULL,
    size bigint NOT NULL,
    mtime timestamptz NOT NULL,
    observed_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    UNIQUE (source_id, generation)
);

CREATE TABLE IF NOT EXISTS source_chunks (
    id text PRIMARY KEY,
    content_id text NOT NULL REFERENCES source_contents(id) ON DELETE CASCADE,
    namespace text NOT NULL REFERENCES namespaces(code),
    ordinal integer NOT NULL,
    content text NOT NULL,
    content_hash text NOT NULL,
    start_line integer NOT NULL,
    end_line integer NOT NULL,
    start_char integer NOT NULL,
    end_char integer NOT NULL,
    search_tsv tsvector GENERATED ALWAYS AS (to_tsvector('simple', content)) STORED,
    embedding vector(1024),
    embedding_model text,
    embedded_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    UNIQUE (content_id, ordinal)
);

CREATE INDEX IF NOT EXISTS source_chunks_search_tsv_idx ON source_chunks USING gin(search_tsv);
CREATE INDEX IF NOT EXISTS source_chunks_search_trgm_idx ON source_chunks USING gin(content gin_trgm_ops);
CREATE INDEX IF NOT EXISTS source_chunks_embedding_hnsw_idx
    ON source_chunks USING hnsw(embedding vector_cosine_ops)
    WHERE embedding IS NOT NULL;

CREATE TABLE IF NOT EXISTS embedding_jobs (
    id bigserial PRIMARY KEY,
    target_type text NOT NULL,
    target_id text NOT NULL,
    namespace text NOT NULL REFERENCES namespaces(code),
    content_hash text NOT NULL,
    status text NOT NULL DEFAULT 'pending',
    attempts integer NOT NULL DEFAULT 0,
    available_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    locked_at timestamptz,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT embedding_jobs_target_valid CHECK (target_type IN ('memory', 'source_chunk')),
    CONSTRAINT embedding_jobs_status_valid CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
    UNIQUE (target_type, target_id)
);

CREATE INDEX IF NOT EXISTS embedding_jobs_pending_idx
    ON embedding_jobs(status, available_at, id)
    WHERE status IN ('pending', 'processing');

CREATE TABLE IF NOT EXISTS idempotency_records (
    namespace text NOT NULL,
    actor text NOT NULL,
    idempotency_key text NOT NULL,
    method text NOT NULL,
    request_hash text NOT NULL,
    response jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    expires_at timestamptz NOT NULL DEFAULT statement_timestamp() + interval '24 hours',
    PRIMARY KEY (namespace, actor, idempotency_key)
);

CREATE OR REPLACE FUNCTION record_memory_revision() RETURNS trigger AS $$
DECLARE
    actor_name text;
    change_reason text;
    operation_name text;
BEGIN
    actor_name := coalesce(nullif(current_setting('memory.actor', true), ''), 'system');
    change_reason := nullif(current_setting('memory.reason', true), '');

    IF TG_OP = 'INSERT' THEN
        INSERT INTO memory_revisions (
            memory_id, namespace, from_version, to_version, operation,
            before_snapshot, after_snapshot, changed_by, reason
        ) VALUES (
            NEW.id, NEW.namespace, NULL, NEW.version, 'create',
            NULL, to_jsonb(NEW) - 'embedding' - 'search_tsv', actor_name, change_reason
        );
        RETURN NEW;
    END IF;

    operation_name := CASE
        WHEN NEW.lifecycle_status = 'deleted' AND OLD.lifecycle_status <> 'deleted' THEN 'delete'
        WHEN NEW.lifecycle_status = 'superseded' AND OLD.lifecycle_status <> 'superseded' THEN 'supersede'
        WHEN NEW.lifecycle_status = 'refuted' AND OLD.lifecycle_status <> 'refuted' THEN 'refute'
        ELSE 'update'
    END;

    INSERT INTO memory_revisions (
        memory_id, namespace, from_version, to_version, operation,
        before_snapshot, after_snapshot, changed_by, reason
    ) VALUES (
        NEW.id, NEW.namespace, OLD.version, NEW.version, operation_name,
        to_jsonb(OLD) - 'embedding' - 'search_tsv',
        to_jsonb(NEW) - 'embedding' - 'search_tsv',
        actor_name, change_reason
    );

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS memories_revision_trigger ON memories;
CREATE TRIGGER memories_revision_trigger
    AFTER INSERT OR UPDATE ON memories
    FOR EACH ROW EXECUTE FUNCTION record_memory_revision();

