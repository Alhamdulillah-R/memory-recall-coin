ALTER TABLE memories
    ADD COLUMN summary text,
    ADD CONSTRAINT memories_summary_length CHECK (summary IS NULL OR length(summary) BETWEEN 1 AND 500);

ALTER TABLE memories
    DROP COLUMN search_text,
    DROP COLUMN search_tsv;

ALTER TABLE memories
    ADD COLUMN search_text text GENERATED ALWAYS AS (
        coalesce(title, '') || E'\n' || coalesce(summary, '') || E'\n'
        || coalesce(content, '') || E'\n' || coalesce(source_path, '')
    ) STORED,
    ADD COLUMN search_tsv tsvector GENERATED ALWAYS AS (
        setweight(to_tsvector('simple', coalesce(title, '')), 'A') ||
        setweight(to_tsvector('simple', coalesce(summary, '')), 'A') ||
        setweight(to_tsvector('simple', coalesce(content, '')), 'B') ||
        setweight(to_tsvector('simple', coalesce(source_path, '')), 'C')
    ) STORED;

CREATE INDEX IF NOT EXISTS memories_search_tsv_idx ON memories USING gin(search_tsv);
CREATE INDEX IF NOT EXISTS memories_search_trgm_idx ON memories USING gin(search_text gin_trgm_ops);
