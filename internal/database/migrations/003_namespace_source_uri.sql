ALTER TABLE sources
    DROP CONSTRAINT IF EXISTS sources_source_uri_key;

CREATE INDEX IF NOT EXISTS sources_namespace_uri_idx
    ON sources(namespace, source_uri);
