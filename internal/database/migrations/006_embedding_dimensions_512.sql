DROP INDEX IF EXISTS memories_embedding_hnsw_idx;
DROP INDEX IF EXISTS source_chunks_embedding_hnsw_idx;

UPDATE memories
SET embedding = NULL,
    embedding_model = NULL,
    embedded_at = NULL;

UPDATE source_chunks
SET embedding = NULL,
    embedding_model = NULL,
    embedded_at = NULL;

DELETE FROM embedding_jobs;

ALTER TABLE memories
    ALTER COLUMN embedding TYPE vector(512)
    USING NULL::vector(512);

ALTER TABLE source_chunks
    ALTER COLUMN embedding TYPE vector(512)
    USING NULL::vector(512);

CREATE INDEX memories_embedding_hnsw_idx
    ON memories USING hnsw(embedding vector_cosine_ops)
    WHERE embedding IS NOT NULL;

CREATE INDEX source_chunks_embedding_hnsw_idx
    ON source_chunks USING hnsw(embedding vector_cosine_ops)
    WHERE embedding IS NOT NULL;
