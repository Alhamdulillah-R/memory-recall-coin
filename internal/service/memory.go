package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Alhamdulillah-R/memory-recall-coin/internal/domain"
)

const memoryColumns = `
    id, namespace, scope_type, scope_id,
    coalesce(device_code, ''), coalesce(installation_code, ''), coalesce(workspace_code, ''),
    memory_type, title, content, metadata, tags, lifecycle_status, verification_state,
    confidence, evidence, coalesce(source_id, ''), coalesce(source_path, ''),
    coalesce(source_hash, ''), source_range, expires_at, version,
    coalesce(supersedes_id, ''), created_by, updated_by, created_at, updated_at, observed_at
`

type rowScanner interface {
	Scan(...any) error
}

type memorySnapshot struct {
	ID                string          `json:"id"`
	Namespace         string          `json:"namespace"`
	ScopeType         string          `json:"scope_type"`
	ScopeID           string          `json:"scope_id"`
	DeviceCode        string          `json:"device_code"`
	InstallationCode  string          `json:"installation_code"`
	WorkspaceCode     string          `json:"workspace_code"`
	MemoryType        string          `json:"memory_type"`
	Title             string          `json:"title"`
	Content           string          `json:"content"`
	Metadata          json.RawMessage `json:"metadata"`
	Tags              []string        `json:"tags"`
	LifecycleStatus   string          `json:"lifecycle_status"`
	VerificationState string          `json:"verification_state"`
	Confidence        float64         `json:"confidence"`
	Evidence          json.RawMessage `json:"evidence"`
	SourceID          string          `json:"source_id"`
	SourcePath        string          `json:"source_path"`
	SourceHash        string          `json:"source_hash"`
	SourceRange       json.RawMessage `json:"source_range"`
	ExpiresAt         *time.Time      `json:"expires_at"`
	Version           int64           `json:"version"`
	SupersedesID      string          `json:"supersedes_id"`
	CreatedBy         string          `json:"created_by"`
	UpdatedBy         string          `json:"updated_by"`
	CreatedAt         time.Time       `json:"created_at"`
	UpdatedAt         time.Time       `json:"updated_at"`
	ObservedAt        *time.Time      `json:"observed_at"`
}

/**
 * PutMemory creates a memory and its first immutable revision.
 * @param ctx request context
 * @param input memory fields and scope
 * @return created memory or an error
 */
func (s *Store) PutMemory(ctx context.Context, input PutMemoryInput) (domain.Memory, error) {
	idempotencyInput := input
	normalized, err := normalizePutInput(input)
	if err != nil {
		return domain.Memory{}, err
	}

	actor := normalizeActor(normalized.CreatedBy, normalized.Caller)
	tx, err := s.beginMutation(ctx, actor, "memory_put")
	if err != nil {
		return domain.Memory{}, err
	}
	defer rollback(tx)

	var cached domain.Memory
	hit, hash, err := lockIdempotency(
		ctx,
		tx,
		normalized.Namespace,
		actor,
		"memory_put",
		normalized.IdempotencyKey,
		idempotencyInput,
		&cached,
	)
	if err != nil {
		return domain.Memory{}, err
	}
	if hit {
		if err := tx.Commit(ctx); err != nil {
			return domain.Memory{}, WrapError(CodeInternal, "commit idempotent memory_put", err)
		}
		return cached, nil
	}

	memory, err := s.insertMemoryTx(ctx, tx, normalized, actor)
	if err != nil {
		return domain.Memory{}, err
	}
	if err := saveIdempotency(ctx, tx, normalized.Namespace, actor, "memory_put", normalized.IdempotencyKey, hash, memory); err != nil {
		return domain.Memory{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.Memory{}, WrapError(CodeInternal, "commit memory_put", err)
	}

	return memory, nil
}

func normalizePutInput(input PutMemoryInput) (PutMemoryInput, error) {
	input.Namespace = strings.ToLower(strings.TrimSpace(input.Namespace))
	if err := validateNamespace(input.Namespace); err != nil {
		return PutMemoryInput{}, err
	}
	if input.ID == "" {
		input.ID = NewID("mem")
	}
	if err := requireNonEmpty("title", input.Title); err != nil {
		return PutMemoryInput{}, err
	}
	if err := requireNonEmpty("content", input.Content); err != nil {
		return PutMemoryInput{}, err
	}
	if input.Type == "" {
		input.Type = "fact"
	}
	if err := validateMemoryType(input.Type); err != nil {
		return PutMemoryInput{}, err
	}

	scopeType, scopeID, err := validateScope(input.ScopeType, input.ScopeID, input.Caller, input.Namespace)
	if err != nil {
		return PutMemoryInput{}, err
	}
	input.ScopeType = scopeType
	input.ScopeID = scopeID

	if input.VerificationState == "" {
		input.VerificationState = "unverified"
	}
	if err := validateVerificationState(input.VerificationState); err != nil {
		return PutMemoryInput{}, err
	}
	if input.Confidence == nil {
		confidence := 0.5
		input.Confidence = &confidence
	}
	if err := validateConfidence(*input.Confidence); err != nil {
		return PutMemoryInput{}, err
	}

	input.Metadata, err = normalizeJSON(input.Metadata, "{}")
	if err != nil {
		return PutMemoryInput{}, err
	}
	input.Evidence, err = normalizeJSON(input.Evidence, "[]")
	if err != nil {
		return PutMemoryInput{}, err
	}
	if len(input.SourceRange) > 0 {
		input.SourceRange, err = normalizeJSON(input.SourceRange, "null")
		if err != nil {
			return PutMemoryInput{}, err
		}
	}
	input.Tags = normalizeTags(input.Tags)
	input.ExpiresAt, err = validateTTL(input.TTLSeconds, input.ExpiresAt)
	if err != nil {
		return PutMemoryInput{}, err
	}
	input.TTLSeconds = nil

	return input, nil
}

func (s *Store) insertMemoryTx(
	ctx context.Context,
	tx pgx.Tx,
	input PutMemoryInput,
	actor string,
) (domain.Memory, error) {
	if err := ensureNamespace(ctx, tx, input.Namespace); err != nil {
		return domain.Memory{}, err
	}

	deviceCode := nullableString(input.Caller.DeviceCode)
	installationCode := nullableString(input.Caller.InstallationCode)
	workspaceCode := nullableString(input.Caller.WorkspaceCode)
	if input.CreatedBy == "" {
		input.CreatedBy = actor
	}

	row := tx.QueryRow(ctx, `
        INSERT INTO memories (
            id, namespace, scope_type, scope_id, device_code, installation_code, workspace_code,
            memory_type, title, content, metadata, tags, verification_state, confidence, evidence,
            source_id, source_path, source_hash, source_range, observed_at, expires_at,
            supersedes_id, created_by, updated_by
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7,
            $8, $9, $10, $11, $12, $13, $14, $15,
            $16, $17, $18, $19, $20, $21,
            $22, $23, $23
        )
        RETURNING `+memoryColumns,
		input.ID,
		input.Namespace,
		input.ScopeType,
		input.ScopeID,
		deviceCode,
		installationCode,
		workspaceCode,
		input.Type,
		strings.TrimSpace(input.Title),
		strings.TrimSpace(input.Content),
		input.Metadata,
		input.Tags,
		input.VerificationState,
		*input.Confidence,
		input.Evidence,
		nullableString(input.SourceID),
		nullableString(input.SourcePath),
		nullableString(input.SourceHash),
		nullableJSON(input.SourceRange),
		input.ObservedAt,
		input.ExpiresAt,
		nullableString(input.SupersedesID),
		input.CreatedBy,
	)

	memory, err := scanMemory(row)
	if err != nil {
		if isUniqueViolation(err) {
			return domain.Memory{}, NewError(CodeAlreadyExists, "memory ID already exists")
		}
		return domain.Memory{}, WrapError(CodeInternal, "insert memory", err)
	}
	if err := s.enqueueMemoryEmbedding(ctx, tx, memory); err != nil {
		return domain.Memory{}, err
	}

	return memory, nil
}

/**
 * PatchMemory updates mutable fields using expected_version.
 * @return updated memory or a version conflict
 */
func (s *Store) PatchMemory(ctx context.Context, input PatchMemoryInput) (domain.Memory, error) {
	if err := validateNamespace(input.Namespace); err != nil {
		return domain.Memory{}, err
	}
	if input.ExpectedVersion < 1 {
		return domain.Memory{}, NewError(CodeInvalidArgument, "expected_version must be positive")
	}

	actor := normalizeActor(input.UpdatedBy, input.Caller)
	tx, err := s.beginMutation(ctx, actor, input.Reason)
	if err != nil {
		return domain.Memory{}, err
	}
	defer rollback(tx)

	var cached domain.Memory
	hit, hash, err := lockIdempotency(ctx, tx, input.Namespace, actor, "memory_patch", input.IdempotencyKey, input, &cached)
	if err != nil {
		return domain.Memory{}, err
	}
	if hit {
		if err := tx.Commit(ctx); err != nil {
			return domain.Memory{}, WrapError(CodeInternal, "commit idempotent memory_patch", err)
		}
		return cached, nil
	}

	sets := make([]string, 0, 16)
	args := []any{input.Namespace, input.ID, input.ExpectedVersion}
	contentChanged := false
	add := func(expression string, value any) {
		args = append(args, value)
		sets = append(sets, fmt.Sprintf(expression, len(args)))
	}

	if input.Title != nil {
		if err := requireNonEmpty("title", *input.Title); err != nil {
			return domain.Memory{}, err
		}
		add("title = $%d", strings.TrimSpace(*input.Title))
		contentChanged = true
	}
	if input.Content != nil {
		if err := requireNonEmpty("content", *input.Content); err != nil {
			return domain.Memory{}, err
		}
		add("content = $%d", strings.TrimSpace(*input.Content))
		contentChanged = true
	}
	if input.Type != nil {
		if err := validateMemoryType(*input.Type); err != nil {
			return domain.Memory{}, err
		}
		add("memory_type = $%d", *input.Type)
	}
	if len(input.MetadataMerge) > 0 {
		metadata, err := normalizeJSON(input.MetadataMerge, "{}")
		if err != nil {
			return domain.Memory{}, err
		}
		add("metadata = jsonb_strip_nulls(metadata || $%d::jsonb)", metadata)
	}
	if input.ReplaceTags != nil {
		add("tags = $%d", normalizeTags(*input.ReplaceTags))
	}
	if input.VerificationState != nil {
		if err := validateVerificationState(*input.VerificationState); err != nil {
			return domain.Memory{}, err
		}
		add("verification_state = $%d", *input.VerificationState)
	}
	if input.Confidence != nil {
		if err := validateConfidence(*input.Confidence); err != nil {
			return domain.Memory{}, err
		}
		add("confidence = $%d", *input.Confidence)
	}
	if len(input.Evidence) > 0 {
		evidence, err := normalizeJSON(input.Evidence, "[]")
		if err != nil {
			return domain.Memory{}, err
		}
		add("evidence = $%d::jsonb", evidence)
	}
	if input.SourceID != nil {
		add("source_id = $%d", nullableString(*input.SourceID))
	}
	if input.SourcePath != nil {
		add("source_path = $%d", nullableString(*input.SourcePath))
	}
	if input.SourceHash != nil {
		add("source_hash = $%d", nullableString(*input.SourceHash))
	}
	if len(input.SourceRange) > 0 {
		sourceRange, err := normalizeJSON(input.SourceRange, "null")
		if err != nil {
			return domain.Memory{}, err
		}
		add("source_range = $%d::jsonb", nullableJSON(sourceRange))
	}
	if input.ObservedAt != nil {
		add("observed_at = $%d", input.ObservedAt)
	}
	if input.ClearExpiresAt {
		if input.TTLSeconds != nil || input.ExpiresAt != nil {
			return domain.Memory{}, NewError(CodeInvalidArgument, "clear_expires_at cannot be combined with TTL fields")
		}
		sets = append(sets, "expires_at = NULL")
	} else if input.TTLSeconds != nil || input.ExpiresAt != nil {
		expiresAt, err := validateTTL(input.TTLSeconds, input.ExpiresAt)
		if err != nil {
			return domain.Memory{}, err
		}
		add("expires_at = $%d", expiresAt)
	}

	if len(sets) == 0 {
		return domain.Memory{}, NewError(CodeInvalidArgument, "memory_patch contains no mutable fields")
	}
	sets = append(sets, "version = version + 1", "updated_at = statement_timestamp()")
	add("updated_by = $%d", actor)
	if contentChanged {
		sets = append(sets, "embedding = NULL", "embedding_model = NULL", "embedded_at = NULL")
	}

	query := `UPDATE memories SET ` + strings.Join(sets, ", ") + `
        WHERE namespace = $1 AND id = $2 AND version = $3
        RETURNING ` + memoryColumns
	memory, err := scanMemory(tx.QueryRow(ctx, query, args...))
	if errorsIsNoRows(err) {
		return domain.Memory{}, s.versionConflict(ctx, tx, input.Namespace, input.ID, input.ExpectedVersion)
	}
	if err != nil {
		return domain.Memory{}, WrapError(CodeInternal, "patch memory", err)
	}
	if contentChanged {
		if err := s.enqueueMemoryEmbedding(ctx, tx, memory); err != nil {
			return domain.Memory{}, err
		}
	}
	if err := saveIdempotency(ctx, tx, input.Namespace, actor, "memory_patch", input.IdempotencyKey, hash, memory); err != nil {
		return domain.Memory{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Memory{}, WrapError(CodeInternal, "commit memory_patch", err)
	}

	return memory, nil
}

/**
 * GetMemory returns the current or requested historical version.
 * @return memory, or NOT_FOUND when hidden by lifecycle/TTL filters
 */
func (s *Store) GetMemory(ctx context.Context, input GetMemoryInput) (domain.Memory, error) {
	if err := validateNamespace(input.Namespace); err != nil {
		return domain.Memory{}, err
	}
	if input.Version != nil {
		var snapshot []byte
		err := s.pool.QueryRow(ctx, `
            SELECT after_snapshot
            FROM memory_revisions
            WHERE namespace = $1 AND memory_id = $2 AND to_version = $3
			  AND ($4 OR (
				coalesce(after_snapshot->>'lifecycle_status', '') <> 'expired'
				AND (
					after_snapshot->>'expires_at' IS NULL
					OR (after_snapshot->>'expires_at')::timestamptz > statement_timestamp()
				)
			  ))
			  AND ($5 OR coalesce(after_snapshot->>'lifecycle_status', '') <> 'refuted')
			  AND ($6 OR coalesce(after_snapshot->>'lifecycle_status', '') <> 'superseded')
			  AND ($7 OR coalesce(after_snapshot->>'lifecycle_status', '') <> 'deleted')
		`,
			input.Namespace,
			input.ID,
			*input.Version,
			input.IncludeExpired,
			input.IncludeRefuted,
			input.IncludeSuperseded,
			input.IncludeDeleted,
		).Scan(&snapshot)
		if errorsIsNoRows(err) {
			return domain.Memory{}, NewError(CodeNotFound, "memory revision not found")
		}
		if err != nil {
			return domain.Memory{}, WrapError(CodeInternal, "read memory revision", err)
		}

		memory, err := decodeMemorySnapshot(snapshot)
		if err != nil {
			return domain.Memory{}, err
		}
		return memory, nil
	}

	memory, err := scanMemory(s.pool.QueryRow(ctx, `
		SELECT `+memoryColumns+`
		FROM memories
		WHERE namespace = $1 AND id = $2
		  AND ($3 OR (
			lifecycle_status <> 'expired'
			AND (expires_at IS NULL OR expires_at > statement_timestamp())
		  ))
		  AND ($4 OR lifecycle_status <> 'refuted')
		  AND ($5 OR lifecycle_status <> 'superseded')
		  AND ($6 OR lifecycle_status <> 'deleted')
	`,
		input.Namespace,
		input.ID,
		input.IncludeExpired,
		input.IncludeRefuted,
		input.IncludeSuperseded,
		input.IncludeDeleted,
	))
	if errorsIsNoRows(err) {
		return domain.Memory{}, NewError(CodeNotFound, "memory not found")
	}
	if err != nil {
		return domain.Memory{}, WrapError(CodeInternal, "read memory", err)
	}
	return memory, nil
}

/**
 * DeleteMemory performs a soft delete and preserves revisions.
 * @return deleted current version
 */
func (s *Store) DeleteMemory(ctx context.Context, input DeleteMemoryInput) (domain.Memory, error) {
	actor := normalizeActor(input.Actor, input.Caller)
	return s.updateLifecycle(
		ctx,
		input.Namespace,
		input.ID,
		input.ExpectedVersion,
		domain.StatusDeleted,
		input.Reason,
		actor,
		input.IdempotencyKey,
		"memory_delete",
		input,
	)
}

/**
 * History lists append-only revisions newest first.
 * @return revision page
 */
func (s *Store) History(ctx context.Context, input HistoryInput) ([]domain.Revision, error) {
	if err := validateNamespace(input.Namespace); err != nil {
		return nil, err
	}
	if input.Limit <= 0 {
		input.Limit = 50
	}
	if input.Limit > 200 {
		input.Limit = 200
	}

	rows, err := s.pool.Query(ctx, `
        SELECT id, memory_id, from_version, to_version, operation,
               before_snapshot, after_snapshot, changed_by, changed_at
        FROM memory_revisions
        WHERE namespace = $1 AND memory_id = $2
          AND ($3::bigint = 0 OR id < $3)
        ORDER BY id DESC
        LIMIT $4
    `, input.Namespace, input.ID, input.BeforeID, input.Limit)
	if err != nil {
		return nil, WrapError(CodeInternal, "query memory history", err)
	}
	defer rows.Close()

	revisions := make([]domain.Revision, 0, input.Limit)
	for rows.Next() {
		var revision domain.Revision
		if err := rows.Scan(
			&revision.ID,
			&revision.MemoryID,
			&revision.FromVersion,
			&revision.ToVersion,
			&revision.Operation,
			&revision.BeforeSnapshot,
			&revision.AfterSnapshot,
			&revision.ChangedBy,
			&revision.ChangedAt,
		); err != nil {
			return nil, WrapError(CodeInternal, "scan memory revision", err)
		}
		revisions = append(revisions, revision)
	}
	if err := rows.Err(); err != nil {
		return nil, WrapError(CodeInternal, "iterate memory revisions", err)
	}

	return revisions, nil
}

/**
 * RestoreMemory applies a historical snapshot as a new version.
 * @return restored current memory
 */
func (s *Store) RestoreMemory(ctx context.Context, input RestoreMemoryInput) (domain.Memory, error) {
	if err := validateNamespace(input.Namespace); err != nil {
		return domain.Memory{}, err
	}
	if err := requireNonEmpty("memory_id", input.ID); err != nil {
		return domain.Memory{}, err
	}
	if input.RevisionVersion < 1 || input.ExpectedVersion < 1 {
		return domain.Memory{}, NewError(CodeInvalidArgument, "revision_version and expected_version must be positive")
	}
	actor := normalizeActor(input.Actor, input.Caller)
	tx, err := s.beginMutation(ctx, actor, input.Reason)
	if err != nil {
		return domain.Memory{}, err
	}
	defer rollback(tx)

	var cached domain.Memory
	hit, hash, err := lockIdempotency(
		ctx,
		tx,
		input.Namespace,
		actor,
		"memory_restore",
		input.IdempotencyKey,
		input,
		&cached,
	)
	if err != nil {
		return domain.Memory{}, err
	}
	if hit {
		if err := tx.Commit(ctx); err != nil {
			return domain.Memory{}, WrapError(CodeInternal, "commit idempotent memory_restore", err)
		}

		return cached, nil
	}

	var snapshotBytes []byte
	err = tx.QueryRow(ctx, `
        SELECT after_snapshot FROM memory_revisions
        WHERE namespace = $1 AND memory_id = $2 AND to_version = $3
    `, input.Namespace, input.ID, input.RevisionVersion).Scan(&snapshotBytes)
	if errorsIsNoRows(err) {
		return domain.Memory{}, NewError(CodeNotFound, "memory revision not found")
	}
	if err != nil {
		return domain.Memory{}, WrapError(CodeInternal, "read restore revision", err)
	}
	snapshot, err := decodeMemorySnapshot(snapshotBytes)
	if err != nil {
		return domain.Memory{}, err
	}

	memory, err := scanMemory(tx.QueryRow(ctx, `
		UPDATE memories SET
            scope_type = $4, scope_id = $5, memory_type = $6,
            title = $7, content = $8, metadata = $9, tags = $10,
            lifecycle_status = $11, verification_state = $12, confidence = $13,
            evidence = $14, source_id = $15, source_path = $16, source_hash = $17,
            source_range = $18, observed_at = $19, expires_at = $20,
            supersedes_id = $21, version = version + 1, updated_by = $22,
			updated_at = statement_timestamp(), embedding = NULL,
			embedding_model = NULL, embedded_at = NULL,
			deleted_at = CASE WHEN $11 = 'deleted' THEN statement_timestamp() ELSE NULL END
        WHERE namespace = $1 AND id = $2 AND version = $3
        RETURNING `+memoryColumns,
		input.Namespace,
		input.ID,
		input.ExpectedVersion,
		snapshot.ScopeType,
		snapshot.ScopeID,
		snapshot.Type,
		snapshot.Title,
		snapshot.Content,
		snapshot.Metadata,
		snapshot.Tags,
		snapshot.Status,
		snapshot.VerificationState,
		snapshot.Confidence,
		snapshot.Evidence,
		nullableString(snapshot.SourceID),
		nullableString(snapshot.SourcePath),
		nullableString(snapshot.SourceHash),
		nullableJSON(snapshot.SourceRange),
		snapshot.ObservedAt,
		snapshot.ExpiresAt,
		nullableString(snapshot.SupersedesID),
		actor,
	))
	if errorsIsNoRows(err) {
		return domain.Memory{}, s.versionConflict(ctx, tx, input.Namespace, input.ID, input.ExpectedVersion)
	}
	if err != nil {
		return domain.Memory{}, WrapError(CodeInternal, "restore memory", err)
	}
	if memory.Status != domain.StatusDeleted {
		if err := s.enqueueMemoryEmbedding(ctx, tx, memory); err != nil {
			return domain.Memory{}, err
		}
	}
	if err := saveIdempotency(
		ctx,
		tx,
		input.Namespace,
		actor,
		"memory_restore",
		input.IdempotencyKey,
		hash,
		memory,
	); err != nil {
		return domain.Memory{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Memory{}, WrapError(CodeInternal, "commit memory_restore", err)
	}

	return memory, nil
}

/**
 * SupersedeMemory atomically creates a replacement and marks the target superseded.
 * @return replacement memory
 */
func (s *Store) SupersedeMemory(ctx context.Context, input SupersedeMemoryInput) (domain.Memory, error) {
	idempotencyInput := input
	if err := validateNamespace(input.Namespace); err != nil {
		return domain.Memory{}, err
	}
	if err := requireNonEmpty("target_memory_id", input.TargetID); err != nil {
		return domain.Memory{}, err
	}
	if input.ExpectedVersion < 1 {
		return domain.Memory{}, NewError(CodeInvalidArgument, "expected_version must be positive")
	}
	input.Replacement.Namespace = input.Namespace
	input.Replacement.Caller = input.Caller
	replacement, err := normalizePutInput(input.Replacement)
	if err != nil {
		return domain.Memory{}, err
	}

	actor := normalizeActor(input.Actor, input.Caller)
	tx, err := s.beginMutation(ctx, actor, input.Reason)
	if err != nil {
		return domain.Memory{}, err
	}
	defer rollback(tx)

	var cached domain.Memory
	hit, hash, err := lockIdempotency(
		ctx,
		tx,
		input.Namespace,
		actor,
		"memory_supersede",
		input.IdempotencyKey,
		idempotencyInput,
		&cached,
	)
	if err != nil {
		return domain.Memory{}, err
	}
	if hit {
		if err := tx.Commit(ctx); err != nil {
			return domain.Memory{}, WrapError(CodeInternal, "commit idempotent memory_supersede", err)
		}

		return cached, nil
	}

	var currentVersion int64
	var currentUpdatedAt time.Time
	err = tx.QueryRow(ctx, `
		SELECT version, updated_at FROM memories
        WHERE namespace = $1 AND id = $2
        FOR UPDATE
	`, input.Namespace, input.TargetID).Scan(&currentVersion, &currentUpdatedAt)
	if errorsIsNoRows(err) {
		return domain.Memory{}, NewError(CodeNotFound, "target memory not found")
	}
	if err != nil {
		return domain.Memory{}, WrapError(CodeInternal, "lock superseded memory", err)
	}
	if currentVersion != input.ExpectedVersion {
		return domain.Memory{}, versionConflictErrorAt(input.ExpectedVersion, currentVersion, currentUpdatedAt)
	}

	replacement.SupersedesID = input.TargetID
	replacementMemory, err := s.insertMemoryTx(ctx, tx, replacement, actor)
	if err != nil {
		return domain.Memory{}, err
	}
	if _, err := tx.Exec(ctx, `
        UPDATE memories SET lifecycle_status = 'superseded', version = version + 1,
            updated_by = $3, updated_at = statement_timestamp()
        WHERE namespace = $1 AND id = $2
    `, input.Namespace, input.TargetID, actor); err != nil {
		return domain.Memory{}, WrapError(CodeInternal, "mark target superseded", err)
	}
	if _, err := tx.Exec(ctx, `
        INSERT INTO memory_relations(namespace, subject_memory_id, relation_type, object_memory_id, reason, created_by)
        VALUES ($1, $2, 'supersedes', $3, $4, $5)
	`, input.Namespace, replacementMemory.ID, input.TargetID, input.Reason, actor); err != nil {
		return domain.Memory{}, WrapError(CodeInternal, "create supersede relation", err)
	}
	if err := saveIdempotency(
		ctx,
		tx,
		input.Namespace,
		actor,
		"memory_supersede",
		input.IdempotencyKey,
		hash,
		replacementMemory,
	); err != nil {
		return domain.Memory{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Memory{}, WrapError(CodeInternal, "commit memory_supersede", err)
	}

	return replacementMemory, nil
}

/**
 * RefuteMemory marks a target refuted and optionally records a refuting relation.
 * @return refuted current memory
 */
func (s *Store) RefuteMemory(ctx context.Context, input RefuteMemoryInput) (domain.Memory, error) {
	if err := validateNamespace(input.Namespace); err != nil {
		return domain.Memory{}, err
	}
	if err := requireNonEmpty("target_memory_id", input.TargetID); err != nil {
		return domain.Memory{}, err
	}
	if input.ExpectedVersion < 1 {
		return domain.Memory{}, NewError(CodeInvalidArgument, "expected_version must be positive")
	}

	actor := normalizeActor(input.Actor, input.Caller)
	tx, err := s.beginMutation(ctx, actor, input.Reason)
	if err != nil {
		return domain.Memory{}, err
	}
	defer rollback(tx)

	var cached domain.Memory
	hit, hash, err := lockIdempotency(
		ctx,
		tx,
		input.Namespace,
		actor,
		"memory_refute",
		input.IdempotencyKey,
		input,
		&cached,
	)
	if err != nil {
		return domain.Memory{}, err
	}
	if hit {
		if err := tx.Commit(ctx); err != nil {
			return domain.Memory{}, WrapError(CodeInternal, "commit idempotent memory_refute", err)
		}

		return cached, nil
	}

	memory, err := scanMemory(tx.QueryRow(ctx, `
        UPDATE memories SET lifecycle_status = 'refuted', version = version + 1,
            updated_by = $4, updated_at = statement_timestamp()
        WHERE namespace = $1 AND id = $2 AND version = $3
        RETURNING `+memoryColumns,
		input.Namespace,
		input.TargetID,
		input.ExpectedVersion,
		actor,
	))
	if errorsIsNoRows(err) {
		return domain.Memory{}, s.versionConflict(ctx, tx, input.Namespace, input.TargetID, input.ExpectedVersion)
	}
	if err != nil {
		return domain.Memory{}, WrapError(CodeInternal, "refute memory", err)
	}
	if input.RefutingMemoryID != "" {
		evidence, err := normalizeJSON(input.Evidence, "[]")
		if err != nil {
			return domain.Memory{}, err
		}
		command, err := tx.Exec(ctx, `
            INSERT INTO memory_relations(
                namespace, subject_memory_id, relation_type, object_memory_id,
                evidence, reason, created_by
			)
			SELECT $1, subject_memory.id, 'refutes', $3, $4, $5, $6
			FROM memories subject_memory
			WHERE subject_memory.namespace = $1 AND subject_memory.id = $2
		`, input.Namespace, input.RefutingMemoryID, input.TargetID, evidence, input.Reason, actor)
		if err != nil {
			return domain.Memory{}, WrapError(CodeInvalidArgument, "create refute relation", err)
		}
		if command.RowsAffected() == 0 {
			return domain.Memory{}, NewError(CodeInvalidArgument, "refuting memory was not found in the target namespace")
		}
	}
	if err := saveIdempotency(
		ctx,
		tx,
		input.Namespace,
		actor,
		"memory_refute",
		input.IdempotencyKey,
		hash,
		memory,
	); err != nil {
		return domain.Memory{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Memory{}, WrapError(CodeInternal, "commit memory_refute", err)
	}

	return memory, nil
}

/**
 * TouchMemory extends or clears TTL and records a new revision.
 * @return updated memory
 */
func (s *Store) TouchMemory(ctx context.Context, input TouchMemoryInput) (domain.Memory, error) {
	if err := validateNamespace(input.Namespace); err != nil {
		return domain.Memory{}, err
	}
	if err := requireNonEmpty("memory_id", input.ID); err != nil {
		return domain.Memory{}, err
	}
	if input.ExpectedVersion < 1 {
		return domain.Memory{}, NewError(CodeInvalidArgument, "expected_version must be positive")
	}

	selected := 0
	if input.ExtendBySeconds != nil {
		selected++
	}
	if input.ExpiresAt != nil {
		selected++
	}
	if input.Pin {
		selected++
	}
	if selected != 1 {
		return domain.Memory{}, NewError(CodeInvalidArgument, "choose exactly one of extend_by_seconds, expires_at, or pin")
	}
	if input.ExtendBySeconds != nil && *input.ExtendBySeconds <= 0 {
		return domain.Memory{}, NewError(CodeInvalidArgument, "extend_by_seconds must be positive")
	}

	actor := normalizeActor(input.Actor, input.Caller)
	tx, err := s.beginMutation(ctx, actor, input.Reason)
	if err != nil {
		return domain.Memory{}, err
	}
	defer rollback(tx)

	var cached domain.Memory
	hit, hash, err := lockIdempotency(
		ctx,
		tx,
		input.Namespace,
		actor,
		"memory_touch",
		input.IdempotencyKey,
		input,
		&cached,
	)
	if err != nil {
		return domain.Memory{}, err
	}
	if hit {
		if err := tx.Commit(ctx); err != nil {
			return domain.Memory{}, WrapError(CodeInternal, "commit idempotent memory_touch", err)
		}

		return cached, nil
	}

	expression := "expires_at = $5"
	args := []any{input.Namespace, input.ID, input.ExpectedVersion, actor, input.ExpiresAt}
	if input.Pin {
		expression = "expires_at = NULL"
		args = args[:4]
	}
	if input.ExtendBySeconds != nil {
		expression = "expires_at = greatest(coalesce(expires_at, statement_timestamp()), statement_timestamp()) + ($5 * interval '1 second')"
		args[4] = *input.ExtendBySeconds
	}

	query := `UPDATE memories SET ` + expression + `,
        version = version + 1, updated_by = $4, updated_at = statement_timestamp()
        WHERE namespace = $1 AND id = $2 AND version = $3
        RETURNING ` + memoryColumns
	memory, err := scanMemory(tx.QueryRow(ctx, query, args...))
	if errorsIsNoRows(err) {
		return domain.Memory{}, s.versionConflict(ctx, tx, input.Namespace, input.ID, input.ExpectedVersion)
	}
	if err != nil {
		return domain.Memory{}, WrapError(CodeInternal, "touch memory", err)
	}
	if err := saveIdempotency(
		ctx,
		tx,
		input.Namespace,
		actor,
		"memory_touch",
		input.IdempotencyKey,
		hash,
		memory,
	); err != nil {
		return domain.Memory{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Memory{}, WrapError(CodeInternal, "commit memory_touch", err)
	}

	return memory, nil
}

func (s *Store) updateLifecycle(
	ctx context.Context,
	namespace string,
	id string,
	expectedVersion int64,
	status string,
	reason string,
	actor string,
	idempotencyKey string,
	method string,
	idempotencyInput any,
) (domain.Memory, error) {
	if err := validateNamespace(namespace); err != nil {
		return domain.Memory{}, err
	}
	if expectedVersion < 1 {
		return domain.Memory{}, NewError(CodeInvalidArgument, "expected_version must be positive")
	}

	tx, err := s.beginMutation(ctx, actor, reason)
	if err != nil {
		return domain.Memory{}, err
	}
	defer rollback(tx)

	var cached domain.Memory
	hit, hash, err := lockIdempotency(
		ctx,
		tx,
		namespace,
		actor,
		method,
		idempotencyKey,
		idempotencyInput,
		&cached,
	)
	if err != nil {
		return domain.Memory{}, err
	}
	if hit {
		if err := tx.Commit(ctx); err != nil {
			return domain.Memory{}, WrapError(CodeInternal, "commit idempotent "+method, err)
		}

		return cached, nil
	}

	memory, err := scanMemory(tx.QueryRow(ctx, `
        UPDATE memories SET lifecycle_status = $4, version = version + 1,
            updated_by = $5, updated_at = statement_timestamp(),
            deleted_at = CASE WHEN $4 = 'deleted' THEN statement_timestamp() ELSE deleted_at END
        WHERE namespace = $1 AND id = $2 AND version = $3
        RETURNING `+memoryColumns,
		namespace,
		id,
		expectedVersion,
		status,
		actor,
	))
	if errorsIsNoRows(err) {
		return domain.Memory{}, s.versionConflict(ctx, tx, namespace, id, expectedVersion)
	}
	if err != nil {
		return domain.Memory{}, WrapError(CodeInternal, method, err)
	}
	if err := saveIdempotency(
		ctx,
		tx,
		namespace,
		actor,
		method,
		idempotencyKey,
		hash,
		memory,
	); err != nil {
		return domain.Memory{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Memory{}, WrapError(CodeInternal, "commit "+method, err)
	}

	return memory, nil
}

func (s *Store) versionConflict(ctx context.Context, tx pgx.Tx, namespace, id string, expected int64) error {
	var current int64
	var updatedAt time.Time
	err := tx.QueryRow(
		ctx,
		"SELECT version, updated_at FROM memories WHERE namespace = $1 AND id = $2",
		namespace,
		id,
	).Scan(&current, &updatedAt)
	if errorsIsNoRows(err) {
		return NewError(CodeNotFound, "memory not found")
	}
	if err != nil {
		return WrapError(CodeInternal, "read current memory version", err)
	}

	return versionConflictErrorAt(expected, current, updatedAt)
}

func versionConflictError(expected, current int64) *Error {
	err := NewError(CodeConflict, "memory version does not match expected_version")
	err.Details = map[string]any{
		"expected_version": expected,
		"current_version":  current,
	}

	return err
}

func versionConflictErrorAt(expected, current int64, updatedAt time.Time) *Error {
	err := versionConflictError(expected, current)
	err.Details["updated_at"] = updatedAt

	return err
}

func (s *Store) enqueueMemoryEmbedding(ctx context.Context, tx pgx.Tx, memory domain.Memory) error {
	if s.embedding == nil || !s.embedding.Enabled() {
		return nil
	}

	contentHash := hashText(memory.Title + "\n" + memory.Content)
	if _, err := tx.Exec(ctx, `
		INSERT INTO embedding_jobs(
			target_type, target_id, namespace, content_hash, embedding_model, status, available_at
		)
		VALUES ('memory', $1, $2, $3, $4, 'pending', statement_timestamp())
		ON CONFLICT (target_type, target_id) DO UPDATE SET
			namespace = excluded.namespace,
			content_hash = excluded.content_hash,
			status = 'pending', attempts = 0, available_at = statement_timestamp(),
			locked_at = NULL, last_error = NULL, updated_at = statement_timestamp()
	`, memory.ID, memory.Namespace, contentHash, s.embeddingProviderName); err != nil {
		return WrapError(CodeInternal, "enqueue memory embedding", err)
	}

	return nil
}

func scanMemory(row rowScanner) (domain.Memory, error) {
	var memory domain.Memory
	err := row.Scan(
		&memory.ID,
		&memory.Namespace,
		&memory.ScopeType,
		&memory.ScopeID,
		&memory.DeviceCode,
		&memory.InstallationCode,
		&memory.WorkspaceCode,
		&memory.Type,
		&memory.Title,
		&memory.Content,
		&memory.Metadata,
		&memory.Tags,
		&memory.Status,
		&memory.VerificationState,
		&memory.Confidence,
		&memory.Evidence,
		&memory.SourceID,
		&memory.SourcePath,
		&memory.SourceHash,
		&memory.SourceRange,
		&memory.ExpiresAt,
		&memory.Version,
		&memory.SupersedesID,
		&memory.CreatedBy,
		&memory.UpdatedBy,
		&memory.CreatedAt,
		&memory.UpdatedAt,
		&memory.ObservedAt,
	)

	return memory, err
}

func decodeMemorySnapshot(data []byte) (domain.Memory, error) {
	var snapshot memorySnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return domain.Memory{}, WrapError(CodeInternal, "decode memory snapshot", err)
	}

	return domain.Memory{
		ID:                snapshot.ID,
		Namespace:         snapshot.Namespace,
		ScopeType:         snapshot.ScopeType,
		ScopeID:           snapshot.ScopeID,
		DeviceCode:        snapshot.DeviceCode,
		InstallationCode:  snapshot.InstallationCode,
		WorkspaceCode:     snapshot.WorkspaceCode,
		Type:              snapshot.MemoryType,
		Title:             snapshot.Title,
		Content:           snapshot.Content,
		Metadata:          snapshot.Metadata,
		Tags:              snapshot.Tags,
		Status:            snapshot.LifecycleStatus,
		VerificationState: snapshot.VerificationState,
		Confidence:        snapshot.Confidence,
		Evidence:          snapshot.Evidence,
		SourceID:          snapshot.SourceID,
		SourcePath:        snapshot.SourcePath,
		SourceHash:        snapshot.SourceHash,
		SourceRange:       snapshot.SourceRange,
		ExpiresAt:         snapshot.ExpiresAt,
		Version:           snapshot.Version,
		SupersedesID:      snapshot.SupersedesID,
		CreatedBy:         snapshot.CreatedBy,
		UpdatedBy:         snapshot.UpdatedBy,
		CreatedAt:         snapshot.CreatedAt,
		UpdatedAt:         snapshot.UpdatedAt,
		ObservedAt:        snapshot.ObservedAt,
	}, nil
}

func nullableString(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}

	return value
}

func nullableJSON(value json.RawMessage) any {
	if len(value) == 0 || string(value) == "null" {
		return nil
	}

	return value
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func hashText(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}
