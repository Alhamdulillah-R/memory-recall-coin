package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// EmbeddingProvider converts text batches to fixed-size vectors.
type EmbeddingProvider interface {
	Enabled() bool
	Embed(context.Context, []string) ([][]float32, error)
}

// Store implements Backend using PostgreSQL as the authoritative state.
type Store struct {
	pool                   *pgxpool.Pool
	embedding              EmbeddingProvider
	embeddingProviderName  string
	signalHMACSecret       []byte
	chunkCharacters        int
	chunkOverlapCharacters int
	version                string
}

// StoreConfig contains non-database settings needed by the PostgreSQL service.
type StoreConfig struct {
	EmbeddingProviderName  string
	SignalHMACSecret       string
	ChunkCharacters        int
	ChunkOverlapCharacters int
	Version                string
}

// NewStore creates the PostgreSQL-backed memory service.
func NewStore(pool *pgxpool.Pool, embedding EmbeddingProvider, cfg StoreConfig) *Store {
	return &Store{
		pool:                   pool,
		embedding:              embedding,
		embeddingProviderName:  cfg.EmbeddingProviderName,
		signalHMACSecret:       []byte(cfg.SignalHMACSecret),
		chunkCharacters:        cfg.ChunkCharacters,
		chunkOverlapCharacters: cfg.ChunkOverlapCharacters,
		version:                cfg.Version,
	}
}

func (s *Store) beginMutation(ctx context.Context, actor, reason string) (pgx.Tx, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, WrapError(CodeUnavailable, "begin database transaction", err)
	}

	if _, err := tx.Exec(ctx, "SELECT set_config('memory.actor', $1, true), set_config('memory.reason', $2, true)", actor, reason); err != nil {
		_ = tx.Rollback(context.Background())
		return nil, WrapError(CodeInternal, "set mutation audit context", err)
	}

	return tx, nil
}

func rollback(tx pgx.Tx) {
	_ = tx.Rollback(context.Background())
}

func ensureNamespace(ctx context.Context, tx pgx.Tx, namespace string) error {
	if _, err := tx.Exec(ctx, "INSERT INTO namespaces(code) VALUES ($1) ON CONFLICT DO NOTHING", namespace); err != nil {
		return WrapError(CodeInternal, "ensure namespace", err)
	}

	return nil
}

func requestHash(input any) (string, error) {
	data, err := json.Marshal(input)
	if err != nil {
		return "", fmt.Errorf("marshal idempotent request: %w", err)
	}
	digest := sha256.Sum256(data)

	return hex.EncodeToString(digest[:]), nil
}

func lockIdempotency(
	ctx context.Context,
	tx pgx.Tx,
	namespace string,
	actor string,
	method string,
	key string,
	input any,
	result any,
) (bool, string, error) {
	if strings.TrimSpace(key) == "" {
		return false, "", nil
	}

	hash, err := requestHash(input)
	if err != nil {
		return false, "", WrapError(CodeInternal, "hash idempotent request", err)
	}
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext($1))", namespace+"\x00"+actor+"\x00"+key); err != nil {
		return false, "", WrapError(CodeInternal, "lock idempotent request", err)
	}

	var storedMethod string
	var storedHash string
	var response []byte
	err = tx.QueryRow(ctx, `
        SELECT method, request_hash, response
        FROM idempotency_records
        WHERE namespace = $1 AND actor = $2 AND idempotency_key = $3
          AND expires_at > statement_timestamp()
    `, namespace, actor, key).Scan(&storedMethod, &storedHash, &response)
	if err == nil {
		if storedMethod != method || storedHash != hash {
			return false, "", NewError(CodeAlreadyExists, "idempotency_key was already used for a different request")
		}
		if err := json.Unmarshal(response, result); err != nil {
			return false, "", WrapError(CodeInternal, "decode idempotent response", err)
		}

		return true, hash, nil
	}
	if !errorsIsNoRows(err) {
		return false, "", WrapError(CodeInternal, "read idempotent request", err)
	}

	return false, hash, nil
}

func saveIdempotency(
	ctx context.Context,
	tx pgx.Tx,
	namespace string,
	actor string,
	method string,
	key string,
	hash string,
	result any,
) error {
	if strings.TrimSpace(key) == "" {
		return nil
	}

	response, err := json.Marshal(result)
	if err != nil {
		return WrapError(CodeInternal, "encode idempotent response", err)
	}
	if _, err := tx.Exec(ctx, `
        INSERT INTO idempotency_records(namespace, actor, idempotency_key, method, request_hash, response)
        VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (namespace, actor, idempotency_key) DO UPDATE SET
			method = excluded.method,
			request_hash = excluded.request_hash,
			response = excluded.response,
			created_at = statement_timestamp(),
			expires_at = statement_timestamp() + interval '24 hours'
    `, namespace, actor, key, method, hash, response); err != nil {
		return WrapError(CodeInternal, "store idempotent response", err)
	}

	return nil
}

func errorsIsNoRows(err error) bool {
	return errors.Is(err, pgx.ErrNoRows)
}
