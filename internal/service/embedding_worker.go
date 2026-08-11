package service

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pgvector/pgvector-go"
)

const maxEmbeddingAttempts = 5

type embeddingJob struct {
	ID          int64
	TargetType  string
	TargetID    string
	Namespace   string
	ContentHash string
	Attempts    int
	LockedAt    time.Time
	Text        string
}

/**
 * RunEmbeddingWorkers processes durable memory and source chunk jobs until context cancellation.
 * @param ctx       service lifecycle
 * @param workers   concurrent worker count
 * @param batchSize maximum jobs per provider call
 * @param logger    structured logger
 */
func (s *Store) RunEmbeddingWorkers(
	ctx context.Context,
	workers int,
	batchSize int,
	logger *slog.Logger,
) {
	if s.embedding == nil || !s.embedding.Enabled() {
		return
	}
	if logger == nil {
		logger = slog.Default()
	}

	var waitGroup sync.WaitGroup
	for workerID := 1; workerID <= workers; workerID++ {
		waitGroup.Add(1)
		go func(id int) {
			defer waitGroup.Done()
			s.runEmbeddingWorker(ctx, id, batchSize, logger)
		}(workerID)
	}
	waitGroup.Wait()
}

func (s *Store) runEmbeddingWorker(ctx context.Context, workerID, batchSize int, logger *slog.Logger) {
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		jobs, err := s.claimEmbeddingJobs(ctx, batchSize)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			logger.Error("[Error] claim embedding jobs", "worker", workerID, "error", err)
			timer.Reset(2 * time.Second)
			continue
		}
		if len(jobs) == 0 {
			timer.Reset(time.Second)
			continue
		}

		inputs := make([]string, len(jobs))
		for index, job := range jobs {
			inputs[index] = job.Text
		}
		vectors, err := s.embedding.Embed(ctx, inputs)
		if err != nil {
			if markErr := s.failEmbeddingJobs(ctx, jobs, err); markErr != nil {
				logger.Error("[Error] record embedding failure", "worker", workerID, "error", markErr)
			}
			logger.Warn("[Warning] embedding batch failed", "worker", workerID, "jobs", len(jobs), "error", err)
			timer.Reset(time.Second)
			continue
		}
		if len(vectors) != len(jobs) {
			err := fmt.Errorf("embedding provider returned %d vectors for %d jobs", len(vectors), len(jobs))
			if markErr := s.failEmbeddingJobs(ctx, jobs, err); markErr != nil {
				logger.Error("[Error] record embedding count failure", "worker", workerID, "error", markErr)
			}
			timer.Reset(time.Second)
			continue
		}

		for index, job := range jobs {
			if len(vectors[index]) != 1024 {
				err := fmt.Errorf("embedding vector for job %d has %d dimensions", job.ID, len(vectors[index]))
				if markErr := s.failEmbeddingJobs(ctx, []embeddingJob{job}, err); markErr != nil {
					logger.Error("[Error] record invalid embedding", "worker", workerID, "job_id", job.ID, "error", markErr)
				}
				continue
			}
			if err := s.completeEmbeddingJob(ctx, job, pgvector.NewVector(vectors[index])); err != nil {
				logger.Error("[Error] store embedding", "worker", workerID, "job_id", job.ID, "error", err)
			}
		}
		timer.Reset(0)
	}
}

func (s *Store) claimEmbeddingJobs(ctx context.Context, batchSize int) ([]embeddingJob, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, WrapError(CodeUnavailable, "begin embedding job claim", err)
	}
	defer rollback(tx)

	rows, err := tx.Query(ctx, `
        SELECT id, target_type, target_id, namespace, content_hash, attempts
        FROM embedding_jobs
        WHERE (
            status = 'pending' AND available_at <= statement_timestamp()
        ) OR (
            status = 'processing' AND locked_at < statement_timestamp() - interval '5 minutes'
        )
        ORDER BY available_at, id
        FOR UPDATE SKIP LOCKED
        LIMIT $1
    `, batchSize)
	if err != nil {
		return nil, WrapError(CodeInternal, "select embedding jobs", err)
	}

	jobs := make([]embeddingJob, 0, batchSize)
	for rows.Next() {
		var job embeddingJob
		if err := rows.Scan(
			&job.ID,
			&job.TargetType,
			&job.TargetID,
			&job.Namespace,
			&job.ContentHash,
			&job.Attempts,
		); err != nil {
			rows.Close()
			return nil, WrapError(CodeInternal, "scan embedding job", err)
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, WrapError(CodeInternal, "iterate embedding jobs", err)
	}
	rows.Close()

	claimed := jobs[:0]
	for _, job := range jobs {
		text, found, err := loadEmbeddingText(ctx, tx, job)
		if err != nil {
			return nil, err
		}
		if !found {
			if _, err := tx.Exec(ctx, "DELETE FROM embedding_jobs WHERE id = $1", job.ID); err != nil {
				return nil, WrapError(CodeInternal, "remove orphan embedding job", err)
			}
			continue
		}
		job.Text = text
		err = tx.QueryRow(ctx, `
            UPDATE embedding_jobs SET
                status = 'processing', attempts = attempts + 1,
                locked_at = statement_timestamp(), updated_at = statement_timestamp()
            WHERE id = $1
			RETURNING attempts, locked_at
		`, job.ID).Scan(&job.Attempts, &job.LockedAt)
		if err != nil {
			return nil, WrapError(CodeInternal, "claim embedding job", err)
		}
		claimed = append(claimed, job)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, WrapError(CodeInternal, "commit embedding job claim", err)
	}

	return claimed, nil
}

func loadEmbeddingText(ctx context.Context, tx pgx.Tx, job embeddingJob) (string, bool, error) {
	var text string
	var err error
	switch job.TargetType {
	case "memory":
		err = tx.QueryRow(ctx, `
            SELECT title || E'\n' || content
            FROM memories
            WHERE id = $1 AND namespace = $2 AND lifecycle_status <> 'deleted'
        `, job.TargetID, job.Namespace).Scan(&text)
	case "source_chunk":
		err = tx.QueryRow(ctx, `
            SELECT content FROM source_chunks
            WHERE id = $1 AND namespace = $2
        `, job.TargetID, job.Namespace).Scan(&text)
	default:
		return "", false, NewError(CodeInternal, "unknown embedding job target")
	}
	if errorsIsNoRows(err) {
		return "", false, nil
	}
	if err != nil {
		return "", false, WrapError(CodeInternal, "load embedding target text", err)
	}

	return text, true, nil
}

func (s *Store) completeEmbeddingJob(ctx context.Context, job embeddingJob, vector pgvector.Vector) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return WrapError(CodeUnavailable, "begin embedding completion", err)
	}
	defer rollback(tx)

	var ownedJobID int64
	err = tx.QueryRow(ctx, `
		SELECT id
		FROM embedding_jobs
		WHERE id = $1 AND status = 'processing' AND content_hash = $2
		  AND attempts = $3 AND locked_at = $4
		FOR UPDATE
	`, job.ID, job.ContentHash, job.Attempts, job.LockedAt).Scan(&ownedJobID)
	if errorsIsNoRows(err) {
		return nil
	}
	if err != nil {
		return WrapError(CodeInternal, "verify embedding job ownership", err)
	}

	var updated int64
	switch job.TargetType {
	case "memory":
		command, err := tx.Exec(ctx, `
            UPDATE memories SET embedding = $2, embedding_model = $3, embedded_at = statement_timestamp()
			WHERE id = $1 AND namespace = $5 AND lifecycle_status <> 'deleted'
              AND encode(digest(title || E'\n' || content, 'sha256'), 'hex') = $4
		`, job.TargetID, vector, s.embeddingProviderName, job.ContentHash, job.Namespace)
		if err != nil {
			return WrapError(CodeInternal, "update memory embedding", err)
		}
		updated = command.RowsAffected()
	case "source_chunk":
		command, err := tx.Exec(ctx, `
            UPDATE source_chunks SET embedding = $2, embedding_model = $3, embedded_at = statement_timestamp()
			WHERE id = $1 AND namespace = $5 AND content_hash = $4
		`, job.TargetID, vector, s.embeddingProviderName, job.ContentHash, job.Namespace)
		if err != nil {
			return WrapError(CodeInternal, "update source chunk embedding", err)
		}
		updated = command.RowsAffected()
	default:
		return NewError(CodeInternal, "unknown embedding job target")
	}
	if updated == 0 {
		if _, err := tx.Exec(ctx, "DELETE FROM embedding_jobs WHERE id = $1", job.ID); err != nil {
			return WrapError(CodeInternal, "remove stale embedding job", err)
		}
	} else {
		if _, err := tx.Exec(ctx, `
            UPDATE embedding_jobs SET
                status = 'completed', locked_at = NULL, last_error = NULL,
                updated_at = statement_timestamp()
            WHERE id = $1
        `, job.ID); err != nil {
			return WrapError(CodeInternal, "complete embedding job", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return WrapError(CodeInternal, "commit embedding completion", err)
	}

	return nil
}

func (s *Store) failEmbeddingJobs(ctx context.Context, jobs []embeddingJob, failure error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return WrapError(CodeUnavailable, "begin embedding failure update", err)
	}
	defer rollback(tx)

	for _, job := range jobs {
		status := "pending"
		if job.Attempts >= maxEmbeddingAttempts {
			status = "failed"
		}
		backoffSeconds := math.Pow(2, float64(job.Attempts))
		if backoffSeconds > 300 {
			backoffSeconds = 300
		}
		if _, err := tx.Exec(ctx, `
            UPDATE embedding_jobs SET
                status = $2, available_at = statement_timestamp() + ($3 * interval '1 second'),
                locked_at = NULL, last_error = left($4, 2000), updated_at = statement_timestamp()
			WHERE id = $1 AND status = 'processing' AND content_hash = $5
			  AND attempts = $6 AND locked_at = $7
		`, job.ID, status, int(backoffSeconds), failure.Error(), job.ContentHash, job.Attempts, job.LockedAt); err != nil {
			return WrapError(CodeInternal, "update failed embedding job", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return WrapError(CodeInternal, "commit embedding failure", err)
	}

	return nil
}
