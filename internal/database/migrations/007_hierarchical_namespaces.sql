ALTER TABLE namespaces
    DROP CONSTRAINT IF EXISTS namespaces_code_valid;

ALTER TABLE namespaces
    ADD CONSTRAINT namespaces_code_valid CHECK (
        length(code) BETWEEN 1 AND 128
        AND (
            code ~ '^[a-z0-9]$'
            OR code ~ '^[a-z0-9][a-z0-9._/-]*[a-z0-9]$'
        )
        AND code !~ '//'
        AND code !~ '/[._-]'
        AND code !~ '[._-]/'
    ),
    ADD COLUMN lifecycle_status text NOT NULL DEFAULT 'active',
    ADD COLUMN deleted_at timestamptz,
    ADD COLUMN deletion_reason text,
    ADD COLUMN deleted_by text,
    ADD CONSTRAINT namespaces_status_valid CHECK (
        lifecycle_status IN ('active', 'deleted')
    ),
    ADD CONSTRAINT namespaces_deletion_state_valid CHECK (
        (lifecycle_status = 'active' AND deleted_at IS NULL)
        OR (lifecycle_status = 'deleted' AND deleted_at IS NOT NULL)
    );

ALTER TABLE ingestion_roots
    ADD CONSTRAINT ingestion_roots_namespace_id_key UNIQUE (namespace, id);

ALTER TABLE source_contents
    ADD CONSTRAINT source_contents_namespace_id_key UNIQUE (namespace, id);

ALTER TABLE sources
    ADD CONSTRAINT sources_namespace_id_key UNIQUE (namespace, id);

ALTER TABLE sources
    DROP CONSTRAINT IF EXISTS sources_root_id_fkey,
    DROP CONSTRAINT IF EXISTS sources_current_content_id_fkey,
    ADD CONSTRAINT sources_root_namespace_fkey
        FOREIGN KEY (namespace, root_id)
        REFERENCES ingestion_roots(namespace, id),
    ADD CONSTRAINT sources_content_namespace_fkey
        FOREIGN KEY (namespace, current_content_id)
        REFERENCES source_contents(namespace, id);

ALTER TABLE ingestion_jobs
    DROP CONSTRAINT IF EXISTS ingestion_jobs_root_id_fkey,
    ADD CONSTRAINT ingestion_jobs_root_namespace_fkey
        FOREIGN KEY (namespace, root_id)
        REFERENCES ingestion_roots(namespace, id);

ALTER TABLE source_chunks
    DROP CONSTRAINT IF EXISTS source_chunks_content_id_fkey,
    ADD CONSTRAINT source_chunks_content_namespace_fkey
        FOREIGN KEY (namespace, content_id)
        REFERENCES source_contents(namespace, id)
        ON DELETE CASCADE;

ALTER TABLE source_versions
    ADD COLUMN namespace text;

CREATE FUNCTION populate_source_version_namespace()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.namespace IS NULL THEN
        SELECT source.namespace
        INTO NEW.namespace
        FROM sources AS source
        WHERE source.id = NEW.source_id;
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER source_versions_populate_namespace
    BEFORE INSERT OR UPDATE OF source_id, namespace
    ON source_versions
    FOR EACH ROW
    EXECUTE FUNCTION populate_source_version_namespace();

UPDATE source_versions AS version
SET namespace = source.namespace
FROM sources AS source
WHERE source.id = version.source_id;

ALTER TABLE source_versions
    ALTER COLUMN namespace SET NOT NULL,
    DROP CONSTRAINT IF EXISTS source_versions_source_id_fkey,
    DROP CONSTRAINT IF EXISTS source_versions_content_id_fkey,
    ADD CONSTRAINT source_versions_source_namespace_fkey
        FOREIGN KEY (namespace, source_id)
        REFERENCES sources(namespace, id),
    ADD CONSTRAINT source_versions_content_namespace_fkey
        FOREIGN KEY (namespace, content_id)
        REFERENCES source_contents(namespace, id);

CREATE INDEX IF NOT EXISTS namespaces_code_pattern_idx
    ON namespaces(code text_pattern_ops);

CREATE INDEX IF NOT EXISTS memories_namespace_pattern_idx
    ON memories(namespace text_pattern_ops);

CREATE INDEX IF NOT EXISTS sources_namespace_pattern_idx
    ON sources(namespace text_pattern_ops);

CREATE INDEX IF NOT EXISTS source_chunks_namespace_content_idx
    ON source_chunks(namespace text_pattern_ops, content_id);

CREATE INDEX IF NOT EXISTS memories_supersedes_namespace_idx
    ON memories(namespace, supersedes_id)
    WHERE supersedes_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS memory_relations_subject_namespace_idx
    ON memory_relations(namespace, subject_memory_id);

CREATE INDEX IF NOT EXISTS memory_relations_object_namespace_idx
    ON memory_relations(namespace, object_memory_id);

CREATE INDEX IF NOT EXISTS embedding_jobs_namespace_idx
    ON embedding_jobs(namespace);

CREATE INDEX IF NOT EXISTS ingestion_jobs_namespace_root_idx
    ON ingestion_jobs(namespace, root_id);

CREATE INDEX IF NOT EXISTS sources_namespace_root_idx
    ON sources(namespace, root_id);

CREATE INDEX IF NOT EXISTS sources_namespace_content_idx
    ON sources(namespace, current_content_id);

CREATE INDEX IF NOT EXISTS source_versions_namespace_source_idx
    ON source_versions(namespace, source_id);

CREATE INDEX IF NOT EXISTS source_versions_namespace_content_idx
    ON source_versions(namespace, content_id);
