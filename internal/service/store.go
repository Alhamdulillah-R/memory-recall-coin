package service

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// EmbeddingProvider converts text batches to fixed-size vectors.
type EmbeddingProvider interface {
	Enabled() bool
	Embed(context.Context, []string) ([][]float32, error)
	EmbedQuery(context.Context, string) ([]float32, error)
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

/**
 * resolveNamespaceSelector resolves exactly one namespace path or persistent sequence.
 * @param ctx request context
 * @param namespace canonical path selector; a new path is allowed for write operations
 * @param sequence persistent namespace sequence; it must already exist
 * @return canonical namespace path
 */
func (s *Store) resolveNamespaceSelector(
	ctx context.Context,
	namespace string,
	sequence *int64,
) (string, error) {
	namespace = strings.TrimSpace(namespace)
	if namespace != "" && sequence != nil {
		return "", NewError(CodeInvalidArgument, "namespace and namespace_sequence are mutually exclusive")
	}
	if namespace == "" && sequence == nil {
		return "", NewError(CodeInvalidArgument, "namespace or namespace_sequence is required")
	}
	if namespace != "" {
		return normalizeNamespace(namespace)
	}
	if *sequence < 0 {
		return "", NewError(CodeInvalidArgument, "namespace_sequence must be non-negative")
	}

	err := s.pool.QueryRow(ctx, `
		SELECT code
		FROM namespaces
		WHERE sequence_number = $1
	`, *sequence).Scan(&namespace)
	if errorsIsNoRows(err) {
		serviceErr := NewError(CodeNotFound, "namespace sequence not found")
		serviceErr.Details = map[string]any{"namespace_sequence": *sequence}

		return "", serviceErr
	}
	if err != nil {
		return "", WrapError(CodeInternal, "resolve namespace sequence", err)
	}

	return namespace, nil
}

/**
 * resolveOptionalNamespaceSelector resolves a namespace selector while allowing neither for root discovery.
 * @return canonical namespace path, or an empty path when neither selector is supplied
 */
func (s *Store) resolveOptionalNamespaceSelector(
	ctx context.Context,
	namespace string,
	sequence *int64,
) (string, error) {
	if strings.TrimSpace(namespace) == "" && sequence == nil {
		return "", nil
	}

	return s.resolveNamespaceSelector(ctx, namespace, sequence)
}

func ensureNamespace(ctx context.Context, tx pgx.Tx, namespace string) error {
	if err := lockNamespaceCatalog(ctx, tx, false); err != nil {
		return err
	}
	ancestors, err := lockNamespaceHierarchy(ctx, tx, namespace)
	if err != nil {
		return err
	}
	for _, ancestor := range ancestors {
		if _, err := tx.Exec(ctx, `
			INSERT INTO namespaces(code)
			SELECT $1
			WHERE NOT EXISTS (SELECT 1 FROM namespaces WHERE code = $1)
			ON CONFLICT DO NOTHING
		`, ancestor); err != nil {
			return WrapError(CodeInternal, "ensure namespace hierarchy", err)
		}
	}

	return rejectDeletedNamespace(ctx, tx, namespace, ancestors)
}

func assertNamespaceActive(ctx context.Context, tx pgx.Tx, namespace string) error {
	if err := lockNamespaceCatalog(ctx, tx, false); err != nil {
		return err
	}
	ancestors, err := lockNamespaceHierarchy(ctx, tx, namespace)
	if err != nil {
		return err
	}
	if err := rejectDeletedNamespace(ctx, tx, namespace, ancestors); err != nil {
		return err
	}

	var active bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM namespaces
			WHERE code = $1 AND lifecycle_status = 'active'
		)
	`, namespace).Scan(&active); err != nil {
		return WrapError(CodeInternal, "inspect namespace state", err)
	}
	if !active {
		return NewError(CodeNotFound, "namespace not found")
	}

	return nil
}

func lockNamespaceHierarchy(ctx context.Context, tx pgx.Tx, namespace string) ([]string, error) {
	ancestors := namespaceAncestors(namespace)
	for _, ancestor := range ancestors {
		if err := lockNamespace(ctx, tx, ancestor, false); err != nil {
			return nil, err
		}
	}

	return ancestors, nil
}

func lockNamespaceDeletionHierarchy(ctx context.Context, tx pgx.Tx, namespace string) ([]string, error) {
	ancestors := namespaceAncestors(namespace)
	for index, ancestor := range ancestors {
		exclusive := index == len(ancestors)-1
		if err := lockNamespace(ctx, tx, ancestor, exclusive); err != nil {
			return nil, err
		}
	}

	return ancestors, nil
}

func rejectDeletedNamespace(ctx context.Context, tx pgx.Tx, namespace string, ancestors []string) error {
	var deletedNamespace string
	err := tx.QueryRow(ctx, `
		SELECT code
		FROM namespaces
		WHERE code = ANY($1::text[]) AND lifecycle_status = 'deleted'
		ORDER BY length(code), code
		LIMIT 1
	`, ancestors).Scan(&deletedNamespace)
	if errorsIsNoRows(err) {
		return nil
	}
	if err != nil {
		return WrapError(CodeInternal, "inspect namespace hierarchy", err)
	}

	serviceErr := NewError(CodeFailedPrecondition, "namespace hierarchy contains a deleted namespace")
	serviceErr.Details = map[string]any{
		"namespace":         namespace,
		"deleted_namespace": deletedNamespace,
	}

	return serviceErr
}

func lockNamespace(ctx context.Context, tx pgx.Tx, namespace string, exclusive bool) error {
	function := "pg_advisory_xact_lock_shared"
	if exclusive {
		function = "pg_advisory_xact_lock"
	}
	query := "SELECT " + function + "(hashtextextended('memory-recall-coin:namespace:' || $1, 0))"
	if _, err := tx.Exec(ctx, query, namespace); err != nil {
		return WrapError(CodeInternal, "lock namespace", err)
	}

	return nil
}

func lockActiveNamespaceHierarchies(ctx context.Context, tx pgx.Tx) error {
	if err := lockNamespaceCatalog(ctx, tx, true); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, "SELECT code FROM namespaces WHERE lifecycle_status = 'active'")
	if err != nil {
		return WrapError(CodeInternal, "list active namespaces", err)
	}

	locks := make(map[string]struct{})
	for rows.Next() {
		var namespace string
		if err := rows.Scan(&namespace); err != nil {
			rows.Close()
			return WrapError(CodeInternal, "scan active namespace", err)
		}
		for _, ancestor := range namespaceAncestors(namespace) {
			locks[ancestor] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return WrapError(CodeInternal, "iterate active namespaces", err)
	}
	rows.Close()

	namespaces := make([]string, 0, len(locks))
	for namespace := range locks {
		namespaces = append(namespaces, namespace)
	}
	sort.Slice(namespaces, func(left, right int) bool {
		leftDepth := strings.Count(namespaces[left], "/")
		rightDepth := strings.Count(namespaces[right], "/")
		if leftDepth != rightDepth {
			return leftDepth < rightDepth
		}

		return namespaces[left] < namespaces[right]
	})
	for _, namespace := range namespaces {
		if err := lockNamespace(ctx, tx, namespace, true); err != nil {
			return err
		}
	}

	return nil
}

func lockNamespaceCatalog(ctx context.Context, tx pgx.Tx, exclusive bool) error {
	function := "pg_advisory_xact_lock_shared"
	if exclusive {
		function = "pg_advisory_xact_lock"
	}
	query := "SELECT " + function + "(hashtextextended('memory-recall-coin:namespace-catalog', 0))"
	if _, err := tx.Exec(ctx, query); err != nil {
		return WrapError(CodeInternal, "lock namespace catalog", err)
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
	lockDigest := sha256.Sum256([]byte(namespace + "\x00" + actor + "\x00" + key))
	lockKey := int64(binary.BigEndian.Uint64(lockDigest[:8]))
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", lockKey); err != nil {
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
