ALTER TABLE embedding_jobs
    ADD COLUMN IF NOT EXISTS embedding_model text;

UPDATE embedding_jobs
SET embedding_model = 'legacy:unknown'
WHERE embedding_model IS NULL OR btrim(embedding_model) = '';

ALTER TABLE embedding_jobs
    ALTER COLUMN embedding_model SET NOT NULL;

CREATE INDEX IF NOT EXISTS embedding_jobs_model_pending_idx
    ON embedding_jobs(embedding_model, status, available_at, id)
    WHERE status IN ('pending', 'processing');
