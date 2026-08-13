package service

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"

	"github.com/Alhamdulillah-R/memory-recall-coin/internal/domain"
)

const (
	sourceParserVersion  = 1
	sourceChunkerVersion = 2
)

type sourceState struct {
	ID          string
	ContentID   string
	ContentHash string
	Generation  int64
}

type textChunk struct {
	Content   string
	Hash      string
	StartLine int
	EndLine   int
	StartChar int
	EndChar   int
}

/**
 * SyncSources applies a complete local path manifest and incrementally rebuilds changed content.
 * @return ingestion summary with deterministic counters
 */
func (s *Store) SyncSources(ctx context.Context, input SyncSourcesInput) (domain.IngestionSummary, error) {
	namespace, err := s.resolveNamespaceSelector(ctx, input.Namespace, input.NamespaceSequence)
	if err != nil {
		return domain.IngestionSummary{}, err
	}
	input.Namespace = namespace
	input.NamespaceSequence = nil

	normalized, err := normalizeSyncInput(input)
	if err != nil {
		return domain.IngestionSummary{}, err
	}
	if normalized.Caller.InstallationCode == "" || normalized.Caller.DeviceCode == "" {
		return domain.IngestionSummary{}, NewError(CodeInvalidArgument, "path ingestion requires a registered device and installation")
	}

	actor := normalizeActor("", normalized.Caller)
	tx, err := s.beginMutation(ctx, actor, "memory_ingest_path")
	if err != nil {
		return domain.IngestionSummary{}, err
	}
	defer rollback(tx)
	if err := ensureNamespace(ctx, tx, normalized.Namespace); err != nil {
		return domain.IngestionSummary{}, err
	}

	rootID, rootGeneration, err := upsertIngestionRoot(ctx, tx, normalized)
	if err != nil {
		return domain.IngestionSummary{}, err
	}
	ingestionID := NewID("ing")
	if _, err := tx.Exec(ctx, `
        INSERT INTO ingestion_jobs(id, root_id, namespace, generation, status, files_seen)
        VALUES ($1, $2, $3, $4, 'running', $5)
    `, ingestionID, rootID, normalized.Namespace, rootGeneration, len(normalized.Files)); err != nil {
		return domain.IngestionSummary{}, WrapError(CodeInternal, "create ingestion job", err)
	}

	summary := domain.IngestionSummary{
		IngestionID: ingestionID,
		RootPath:    normalized.RootPath,
		Status:      "completed",
		FilesSeen:   len(normalized.Files),
	}
	manifestPaths := make([]string, 0, len(normalized.Files))
	for _, file := range normalized.Files {
		if err := validateIngestedFile(file); err != nil {
			return domain.IngestionSummary{}, err
		}

		normalizedPath := normalizePath(file.AbsolutePath)
		manifestPaths = append(manifestPaths, normalizedPath)
		state, exists, err := findSourceForUpdate(ctx, tx, normalized.Namespace, normalized.Caller.InstallationCode, normalizedPath)
		if err != nil {
			return domain.IngestionSummary{}, err
		}
		contentID, chunkCount, err := s.ensureSourceContent(ctx, tx, normalized.Namespace, file)
		if err != nil {
			return domain.IngestionSummary{}, err
		}

		if !exists {
			if _, err := insertSource(ctx, tx, rootID, normalized, file, normalizedPath, contentID); err != nil {
				return domain.IngestionSummary{}, err
			}
			summary.Created++
			summary.Chunks += chunkCount
			continue
		}
		if state.ContentHash == file.ContentHash && state.ContentID == contentID {
			if err := refreshUnchangedSource(ctx, tx, state.ID, rootID, normalized, file); err != nil {
				return domain.IngestionSummary{}, err
			}
			summary.Unchanged++
			continue
		}

		if err := updateChangedSource(ctx, tx, state, rootID, normalized, file, contentID); err != nil {
			return domain.IngestionSummary{}, err
		}
		summary.Updated++
		summary.Chunks += chunkCount
	}

	if normalized.PruneMissing {
		deleted, err := pruneMissingSources(ctx, tx, rootID, manifestPaths)
		if err != nil {
			return domain.IngestionSummary{}, err
		}
		summary.Deleted = int(deleted)
	}

	if _, err := tx.Exec(ctx, `
        UPDATE ingestion_jobs SET
            status = $2, created_count = $3, updated_count = $4,
            unchanged_count = $5, deleted_count = $6, chunk_count = $7,
            errors = $8, finished_at = statement_timestamp()
        WHERE id = $1
    `,
		ingestionID,
		summary.Status,
		summary.Created,
		summary.Updated,
		summary.Unchanged,
		summary.Deleted,
		summary.Chunks,
		[]byte("[]"),
	); err != nil {
		return domain.IngestionSummary{}, WrapError(CodeInternal, "finish ingestion job", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.IngestionSummary{}, WrapError(CodeInternal, "commit source synchronization", err)
	}

	return summary, nil
}

/**
 * SourceStatus returns sources matching one source ID, path or ingestion root.
 * @return source records and embedding queue counters
 */
func (s *Store) SourceStatus(ctx context.Context, input SourceStatusInput) (domain.SourceStatus, error) {
	namespace, err := s.resolveNamespaceSelector(ctx, input.Namespace, input.NamespaceSequence)
	if err != nil {
		return domain.SourceStatus{}, err
	}
	input.Namespace = namespace
	input.NamespaceSequence = nil
	if input.NamespaceMatch == "" {
		input.NamespaceMatch = domain.NamespaceMatchExact
	}
	switch input.NamespaceMatch {
	case domain.NamespaceMatchExact, domain.NamespaceMatchSubtree:
	default:
		return domain.SourceStatus{}, NewError(CodeInvalidArgument, "namespace_match must be exact or subtree")
	}
	if input.SourceID == "" && input.Path == "" && input.IngestionID == "" {
		err := NewError(CodeInvalidArgument, "at least one source selector is required")
		err.Details = map[string]any{
			"required_any_of": []string{"source_id", "path", "ingestion_id"},
			"example": map[string]any{
				"namespace": "projects/example",
				"path":      `D:\dev\example\README.md`,
			},
			"schema_version": "memory_source_status@1",
		}
		return domain.SourceStatus{}, err
	}
	if input.Limit <= 0 {
		input.Limit = 50
	}
	if input.Limit > 200 {
		input.Limit = 200
	}

	conditions, args := appendNamespaceFilter(nil, nil, "s.namespace", input.Namespace, input.NamespaceMatch)
	if input.SourceID != "" {
		args = append(args, input.SourceID)
		conditions = append(conditions, "s.id = $"+fmt.Sprint(len(args)))
	}
	if input.Path != "" {
		args = append(args, "%"+escapeLikePattern(normalizePath(input.Path))+"%")
		conditions = append(conditions, "s.normalized_path ILIKE $"+fmt.Sprint(len(args))+` ESCAPE E'\\'`)
	}
	if input.IngestionID != "" {
		args = append(args, input.IngestionID)
		conditions = append(conditions, "s.root_id = (SELECT root_id FROM ingestion_jobs WHERE id = $"+fmt.Sprint(len(args))+")")
	}
	args = append(args, input.Limit)

	rows, err := s.pool.Query(ctx, `
        SELECT `+sourceColumns()+`
        FROM sources s
        WHERE `+strings.Join(conditions, " AND ")+`
        ORDER BY s.updated_at DESC, s.id
        LIMIT $`+fmt.Sprint(len(args)), args...)
	if err != nil {
		return domain.SourceStatus{}, WrapError(CodeInternal, "query source status", err)
	}
	defer rows.Close()

	status := domain.SourceStatus{
		Sources:        make([]domain.Source, 0, input.Limit),
		Namespace:      input.Namespace,
		NamespaceMatch: input.NamespaceMatch,
	}
	for rows.Next() {
		source, err := scanSource(rows)
		if err != nil {
			return domain.SourceStatus{}, WrapError(CodeInternal, "scan source status", err)
		}
		status.Sources = append(status.Sources, source)
	}
	if err := rows.Err(); err != nil {
		return domain.SourceStatus{}, WrapError(CodeInternal, "iterate source status", err)
	}

	if len(status.Sources) > 0 {
		sourceIDs := make([]string, 0, len(status.Sources))
		for _, source := range status.Sources {
			sourceIDs = append(sourceIDs, source.ID)
		}
		if err := s.pool.QueryRow(ctx, `
			SELECT
				count(DISTINCT j.id) FILTER (WHERE j.status IN ('pending', 'processing')),
				count(DISTINCT j.id) FILTER (WHERE j.status = 'failed')
			FROM embedding_jobs j
			JOIN source_chunks c ON c.id = j.target_id AND j.target_type = 'source_chunk'
			JOIN sources s ON s.current_content_id = c.content_id
			WHERE s.id = ANY($1::text[]) AND j.embedding_model = $2
		`, sourceIDs, s.embeddingProviderName).Scan(&status.PendingEmbeddings, &status.FailedEmbeddings); err != nil {
			return domain.SourceStatus{}, WrapError(CodeInternal, "count source embeddings", err)
		}
	}

	return status, nil
}

/**
 * DeleteSource soft-deletes the server-side source index without touching the client file.
 * @return deleted source state
 */
func (s *Store) DeleteSource(ctx context.Context, input DeleteSourceInput) (domain.Source, error) {
	namespace, err := s.resolveNamespaceSelector(ctx, input.Namespace, input.NamespaceSequence)
	if err != nil {
		return domain.Source{}, err
	}
	input.Namespace = namespace
	input.NamespaceSequence = nil
	actor := normalizeActor("", input.Caller)
	tx, err := s.beginMutation(ctx, actor, input.Reason)
	if err != nil {
		return domain.Source{}, err
	}
	defer rollback(tx)
	if err := assertNamespaceActive(ctx, tx, input.Namespace); err != nil {
		return domain.Source{}, err
	}

	source, err := scanSource(tx.QueryRow(ctx, `
		UPDATE sources AS s
		SET lifecycle_status = 'deleted', updated_at = statement_timestamp()
		WHERE namespace = $1 AND id = $2
        RETURNING `+sourceColumns(), input.Namespace, input.SourceID))
	if errorsIsNoRows(err) {
		return domain.Source{}, NewError(CodeNotFound, "source not found")
	}
	if err != nil {
		return domain.Source{}, WrapError(CodeInternal, "delete source index", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Source{}, WrapError(CodeInternal, "commit source deletion", err)
	}

	return source, nil
}

/**
 * Health checks PostgreSQL and reports semantic configuration.
 * @return dependency health
 */
func (s *Store) Health(ctx context.Context) (HealthResult, error) {
	if err := s.pool.Ping(ctx); err != nil {
		return HealthResult{
			Status:            "unavailable",
			Database:          "unavailable",
			EmbeddingProvider: s.embeddingProviderName,
			EmbeddingEnabled:  s.embedding != nil && s.embedding.Enabled(),
			Version:           s.version,
		}, WrapError(CodeUnavailable, "database health check failed", err)
	}

	return HealthResult{
		Status:            "ok",
		Database:          "ok",
		EmbeddingProvider: s.embeddingProviderName,
		EmbeddingEnabled:  s.embedding != nil && s.embedding.Enabled(),
		Version:           s.version,
	}, nil
}

func normalizeSyncInput(input SyncSourcesInput) (SyncSourcesInput, error) {
	namespace, err := normalizeNamespace(input.Namespace)
	if err != nil {
		return SyncSourcesInput{}, err
	}
	input.Namespace = namespace
	if err := requireNonEmpty("root_path", input.RootPath); err != nil {
		return SyncSourcesInput{}, err
	}
	scopeType, scopeID, err := validateScope(input.ScopeType, input.ScopeID, input.Caller, input.Namespace)
	if err != nil {
		return SyncSourcesInput{}, err
	}
	input.ScopeType = scopeType
	input.ScopeID = scopeID
	if input.WatchMode == "" {
		input.WatchMode = "once"
	}
	switch input.WatchMode {
	case "once", "sync", "watch":
	default:
		return SyncSourcesInput{}, NewError(CodeInvalidArgument, "watch_mode must be once, sync, or watch")
	}
	if input.Parser == "" {
		input.Parser = "auto"
	}
	input.ExpiresAt, err = validateTTL(input.TTLSeconds, input.ExpiresAt)
	if err != nil {
		return SyncSourcesInput{}, err
	}
	input.TTLSeconds = nil
	input.RootPath = strings.TrimSpace(input.RootPath)
	sort.Slice(input.Files, func(i, j int) bool {
		return normalizePath(input.Files[i].AbsolutePath) < normalizePath(input.Files[j].AbsolutePath)
	})

	seen := make(map[string]struct{}, len(input.Files))
	for _, file := range input.Files {
		path := normalizePath(file.AbsolutePath)
		if _, exists := seen[path]; exists {
			return SyncSourcesInput{}, NewError(CodeInvalidArgument, "manifest contains a duplicate path: "+file.AbsolutePath)
		}
		seen[path] = struct{}{}
	}

	return input, nil
}

func validateIngestedFile(file domain.IngestedFile) error {
	if err := requireNonEmpty("file.absolute_path", file.AbsolutePath); err != nil {
		return err
	}
	if err := requireNonEmpty("file.content_hash", file.ContentHash); err != nil {
		return err
	}
	if file.Size < 0 || file.Size != int64(len([]byte(file.Content))) {
		return NewError(CodeInvalidArgument, "file size does not match UTF-8 content bytes")
	}
	if hashText(file.Content) != strings.ToLower(file.ContentHash) {
		return NewError(CodeInvalidArgument, "file content_hash does not match content")
	}
	if !utf8.ValidString(file.Content) {
		return NewError(CodeInvalidArgument, "file content must be valid UTF-8")
	}

	return nil
}

func upsertIngestionRoot(ctx context.Context, tx pgx.Tx, input SyncSourcesInput) (string, int64, error) {
	rootID := NewID("root")
	var rootGeneration int64
	normalizedRoot := normalizePath(input.RootPath)
	err := tx.QueryRow(ctx, `
        INSERT INTO ingestion_roots(
            id, namespace, scope_type, scope_id, device_code, installation_code,
            workspace_code, root_path, normalized_root_path, recursive,
            include_patterns, exclude_patterns, watch_mode, parser, generation, expires_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6,
            $7, $8, $9, $10,
            COALESCE($11::text[], '{}'::text[]),
            COALESCE($12::text[], '{}'::text[]),
            $13, $14, 1, $15
        )
        ON CONFLICT (namespace, installation_code, normalized_root_path) DO UPDATE SET
            scope_type = excluded.scope_type, scope_id = excluded.scope_id,
            workspace_code = excluded.workspace_code, root_path = excluded.root_path,
            recursive = excluded.recursive, include_patterns = excluded.include_patterns,
            exclude_patterns = excluded.exclude_patterns, watch_mode = excluded.watch_mode,
            parser = excluded.parser, generation = ingestion_roots.generation + 1,
            status = 'active', expires_at = excluded.expires_at,
            updated_at = statement_timestamp()
        RETURNING id, generation
    `,
		rootID,
		input.Namespace,
		input.ScopeType,
		input.ScopeID,
		nullableString(input.Caller.DeviceCode),
		nullableString(input.Caller.InstallationCode),
		nullableString(input.Caller.WorkspaceCode),
		input.RootPath,
		normalizedRoot,
		input.Recursive,
		input.Include,
		input.Exclude,
		input.WatchMode,
		input.Parser,
		input.ExpiresAt,
	).Scan(&rootID, &rootGeneration)
	if err != nil {
		return "", 0, WrapError(CodeInternal, "upsert ingestion root", err)
	}

	return rootID, rootGeneration, nil
}

func (s *Store) ensureSourceContent(
	ctx context.Context,
	tx pgx.Tx,
	namespace string,
	file domain.IngestedFile,
) (string, int, error) {
	contentID := NewID("cnt")
	var insertedID string
	err := tx.QueryRow(ctx, `
        INSERT INTO source_contents(
            id, namespace, content_hash, parser, parser_version, chunker_version, content, size
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (namespace, content_hash, parser, parser_version, chunker_version) DO NOTHING
        RETURNING id
    `,
		contentID,
		namespace,
		file.ContentHash,
		file.Parser,
		sourceParserVersion,
		sourceChunkerVersion,
		file.Content,
		file.Size,
	).Scan(&insertedID)
	newContent := true
	if errorsIsNoRows(err) {
		newContent = false
		err = tx.QueryRow(ctx, `
            SELECT id FROM source_contents
            WHERE namespace = $1 AND content_hash = $2 AND parser = $3
              AND parser_version = $4 AND chunker_version = $5
        `,
			namespace,
			file.ContentHash,
			file.Parser,
			sourceParserVersion,
			sourceChunkerVersion,
		).Scan(&contentID)
	} else if err == nil {
		contentID = insertedID
	}
	if err != nil {
		return "", 0, WrapError(CodeInternal, "ensure source content", err)
	}
	if !newContent {
		return contentID, 0, nil
	}

	chunks := chunkText(file.Content, s.chunkCharacters, s.chunkOverlapCharacters)
	for ordinal, chunk := range chunks {
		chunkID := NewID("chk")
		if _, err := tx.Exec(ctx, `
            INSERT INTO source_chunks(
                id, content_id, namespace, ordinal, content, content_hash,
                start_line, end_line, start_char, end_char
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        `,
			chunkID,
			contentID,
			namespace,
			ordinal,
			chunk.Content,
			chunk.Hash,
			chunk.StartLine,
			chunk.EndLine,
			chunk.StartChar,
			chunk.EndChar,
		); err != nil {
			return "", 0, WrapError(CodeInternal, "insert source chunk", err)
		}
		if s.embedding != nil && s.embedding.Enabled() {
			if _, err := tx.Exec(ctx, `
				INSERT INTO embedding_jobs(
					target_type, target_id, namespace, content_hash, embedding_model
				)
				VALUES ('source_chunk', $1, $2, $3, $4)
			`, chunkID, namespace, chunk.Hash, s.embeddingProviderName); err != nil {
				return "", 0, WrapError(CodeInternal, "enqueue source chunk embedding", err)
			}
		}
	}

	return contentID, len(chunks), nil
}

func findSourceForUpdate(
	ctx context.Context,
	tx pgx.Tx,
	namespace string,
	installationCode string,
	normalizedPath string,
) (sourceState, bool, error) {
	var state sourceState
	err := tx.QueryRow(ctx, `
        SELECT id, current_content_id, content_hash, generation
        FROM sources
        WHERE namespace = $1 AND installation_code = $2 AND normalized_path = $3
        FOR UPDATE
    `, namespace, installationCode, normalizedPath).Scan(
		&state.ID,
		&state.ContentID,
		&state.ContentHash,
		&state.Generation,
	)
	if errorsIsNoRows(err) {
		return sourceState{}, false, nil
	}
	if err != nil {
		return sourceState{}, false, WrapError(CodeInternal, "read existing source", err)
	}

	return state, true, nil
}

func insertSource(
	ctx context.Context,
	tx pgx.Tx,
	rootID string,
	input SyncSourcesInput,
	file domain.IngestedFile,
	normalizedPath string,
	contentID string,
) (string, error) {
	sourceID := NewID("src")
	sourceURI := buildSourceURI(input.Caller.DeviceCode, file.AbsolutePath)
	if _, err := tx.Exec(ctx, `
        INSERT INTO sources(
            id, namespace, root_id, scope_type, scope_id, device_code, installation_code,
            workspace_code, current_content_id, original_absolute_path, normalized_path,
            relative_path, source_uri, content_hash, size, mtime, parser, generation, expires_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7,
            $8, $9, $10, $11,
            $12, $13, $14, $15, $16, $17, 1, $18
        )
    `,
		sourceID,
		input.Namespace,
		rootID,
		input.ScopeType,
		input.ScopeID,
		input.Caller.DeviceCode,
		input.Caller.InstallationCode,
		nullableString(input.Caller.WorkspaceCode),
		contentID,
		file.AbsolutePath,
		normalizedPath,
		nullableString(file.RelativePath),
		sourceURI,
		file.ContentHash,
		file.Size,
		file.MTime,
		file.Parser,
		input.ExpiresAt,
	); err != nil {
		return "", WrapError(CodeInternal, "insert source", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO source_versions(namespace, source_id, generation, content_id, content_hash, size, mtime)
		VALUES ($1, $2, 1, $3, $4, $5, $6)
	`, input.Namespace, sourceID, contentID, file.ContentHash, file.Size, file.MTime); err != nil {
		return "", WrapError(CodeInternal, "insert source version", err)
	}

	return sourceID, nil
}

func refreshUnchangedSource(
	ctx context.Context,
	tx pgx.Tx,
	sourceID string,
	rootID string,
	input SyncSourcesInput,
	file domain.IngestedFile,
) error {
	if _, err := tx.Exec(ctx, `
        UPDATE sources SET
            root_id = $2, scope_type = $3, scope_id = $4,
            workspace_code = $5, original_absolute_path = $6,
            relative_path = $7, size = $8, mtime = $9, parser = $10,
            lifecycle_status = 'active', expires_at = $11,
            updated_at = statement_timestamp()
        WHERE id = $1
    `,
		sourceID,
		rootID,
		input.ScopeType,
		input.ScopeID,
		nullableString(input.Caller.WorkspaceCode),
		file.AbsolutePath,
		nullableString(file.RelativePath),
		file.Size,
		file.MTime,
		file.Parser,
		input.ExpiresAt,
	); err != nil {
		return WrapError(CodeInternal, "refresh unchanged source", err)
	}

	return nil
}

func updateChangedSource(
	ctx context.Context,
	tx pgx.Tx,
	state sourceState,
	rootID string,
	input SyncSourcesInput,
	file domain.IngestedFile,
	contentID string,
) error {
	nextGeneration := state.Generation + 1
	if _, err := tx.Exec(ctx, `
        UPDATE sources SET
            root_id = $2, scope_type = $3, scope_id = $4,
            workspace_code = $5, current_content_id = $6,
            original_absolute_path = $7, relative_path = $8,
            content_hash = $9, size = $10, mtime = $11, parser = $12,
            generation = $13, lifecycle_status = 'active', expires_at = $14,
            updated_at = statement_timestamp()
        WHERE id = $1
    `,
		state.ID,
		rootID,
		input.ScopeType,
		input.ScopeID,
		nullableString(input.Caller.WorkspaceCode),
		contentID,
		file.AbsolutePath,
		nullableString(file.RelativePath),
		file.ContentHash,
		file.Size,
		file.MTime,
		file.Parser,
		nextGeneration,
		input.ExpiresAt,
	); err != nil {
		return WrapError(CodeInternal, "update changed source", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO source_versions(namespace, source_id, generation, content_id, content_hash, size, mtime)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, input.Namespace, state.ID, nextGeneration, contentID, file.ContentHash, file.Size, file.MTime); err != nil {
		return WrapError(CodeInternal, "insert changed source version", err)
	}

	return nil
}

func pruneMissingSources(ctx context.Context, tx pgx.Tx, rootID string, manifestPaths []string) (int64, error) {
	command, err := tx.Exec(ctx, `
        UPDATE sources SET lifecycle_status = 'deleted', updated_at = statement_timestamp()
        WHERE root_id = $1 AND lifecycle_status <> 'deleted'
          AND NOT (normalized_path = ANY($2::text[]))
    `, rootID, manifestPaths)
	if err != nil {
		return 0, WrapError(CodeInternal, "prune missing sources", err)
	}

	return command.RowsAffected(), nil
}

func buildSourceURI(deviceCode, sourcePath string) string {
	uri := url.URL{
		Scheme: "device",
		Host:   deviceCode,
		Path:   "/" + strings.TrimPrefix(cleanPortablePath(sourcePath), "/"),
	}

	return uri.String()
}

func chunkText(content string, maxCharacters, overlapCharacters int) []textChunk {
	runes := []rune(content)
	if len(runes) == 0 {
		return nil
	}

	lineAt := make([]int, len(runes)+1)
	line := 1
	for index, value := range runes {
		lineAt[index] = line
		if value == '\n' {
			line++
		}
	}
	lineAt[len(runes)] = line

	chunks := make([]textChunk, 0, (len(runes)/maxCharacters)+1)
	for start := 0; start < len(runes); {
		end := start + maxCharacters
		if end > len(runes) {
			end = len(runes)
		} else {
			minimumBreak := start + maxCharacters/2
			for cursor := end; cursor > minimumBreak; cursor-- {
				if runes[cursor-1] == '\n' {
					end = cursor
					break
				}
			}
		}

		text := string(runes[start:end])
		chunks = append(chunks, textChunk{
			Content:   text,
			Hash:      hashText(text),
			StartLine: lineAt[start],
			EndLine:   lineAt[end],
			StartChar: start,
			EndChar:   end,
		})
		if end == len(runes) {
			break
		}
		next := end - overlapCharacters
		if next <= start {
			next = end
		}
		start = next
	}

	return chunks
}

func sourceColumns() string {
	return `
        s.id, s.namespace, s.scope_type, s.scope_id,
        coalesce(s.device_code, ''), coalesce(s.installation_code, ''), coalesce(s.workspace_code, ''),
        s.original_absolute_path, coalesce(s.relative_path, ''), s.source_uri,
        s.content_hash, s.size, s.mtime, s.parser, s.generation,
        s.lifecycle_status, s.metadata, s.expires_at, s.created_at, s.updated_at
    `
}

func scanSource(row rowScanner) (domain.Source, error) {
	var source domain.Source
	err := row.Scan(
		&source.ID,
		&source.Namespace,
		&source.ScopeType,
		&source.ScopeID,
		&source.DeviceCode,
		&source.InstallationCode,
		&source.WorkspaceCode,
		&source.OriginalAbsolutePath,
		&source.RelativePath,
		&source.SourceURI,
		&source.ContentHash,
		&source.Size,
		&source.MTime,
		&source.Parser,
		&source.Generation,
		&source.Status,
		&source.Metadata,
		&source.ExpiresAt,
		&source.CreatedAt,
		&source.UpdatedAt,
	)

	return source, err
}
