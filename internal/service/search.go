package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/pgvector/pgvector-go"

	"github.com/Alhamdulillah-R/memory-recall-coin/internal/domain"
	"github.com/Alhamdulillah-R/memory-recall-coin/internal/embedding"
)

const rrfConstant = 60.0

type searchCandidate struct {
	Result    domain.SearchResult
	UpdatedAt time.Time
}

type channelResult struct {
	Name       string
	Candidates []searchCandidate
	Err        error
}

/**
 * SearchMemory executes filtered retrieval channels concurrently and fuses them with RRF.
 * @return ranked memories and source chunks with score breakdown
 */
func (s *Store) SearchMemory(ctx context.Context, input SearchMemoryInput) (domain.SearchResponse, error) {
	startedAt := time.Now()
	normalized, err := normalizeSearchInput(input)
	if err != nil {
		return domain.SearchResponse{}, err
	}

	channels := selectedChannels(normalized.RetrievalMode)
	results := make(chan channelResult, len(channels))
	var waitGroup sync.WaitGroup
	for _, channel := range channels {
		channel := channel
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()

			if channel == "semantic" {
				candidates, queryErr := s.semanticCandidates(ctx, normalized)
				results <- channelResult{Name: channel, Candidates: candidates, Err: queryErr}
				return
			}

			candidates, queryErr := s.textCandidates(ctx, normalized, channel)
			results <- channelResult{Name: channel, Candidates: candidates, Err: queryErr}
		}()
	}
	waitGroup.Wait()
	close(results)

	channelCandidates := make(map[string][]searchCandidate, len(channels))
	semanticError := ""
	semanticEnabled := s.embedding != nil && s.embedding.Enabled()
	for result := range results {
		if result.Err != nil {
			if result.Name == "semantic" && normalized.RetrievalMode == "hybrid" {
				semanticError = result.Err.Error()
				continue
			}
			return domain.SearchResponse{}, result.Err
		}
		channelCandidates[result.Name] = result.Candidates
	}

	fused := fuseCandidates(channelCandidates, normalized.Caller)
	if len(fused) > normalized.Limit {
		fused = fused[:normalized.Limit]
	}

	counts := make(map[string]int, len(channelCandidates))
	for name, candidates := range channelCandidates {
		counts[name] = len(candidates)
	}

	return domain.SearchResponse{
		Results:         fused,
		Query:           normalized.Query,
		ScopeMode:       normalized.ScopeMode,
		SemanticEnabled: semanticEnabled,
		SemanticError:   semanticError,
		DurationMS:      time.Since(startedAt).Milliseconds(),
		CandidateCounts: counts,
	}, nil
}

func normalizeSearchInput(input SearchMemoryInput) (SearchMemoryInput, error) {
	input.Namespace = strings.ToLower(strings.TrimSpace(input.Namespace))
	if err := validateNamespace(input.Namespace); err != nil {
		return SearchMemoryInput{}, err
	}
	input.Query = strings.TrimSpace(input.Query)
	if input.Query == "" {
		return SearchMemoryInput{}, NewError(CodeInvalidArgument, "query is required")
	}
	if input.RetrievalMode == "" {
		input.RetrievalMode = "hybrid"
	}
	switch input.RetrievalMode {
	case "hybrid", "exact", "substring", "lexical", "semantic":
	default:
		return SearchMemoryInput{}, NewError(CodeInvalidArgument, "unsupported retrieval_mode")
	}
	if input.ScopeMode == "" {
		input.ScopeMode = domain.SearchPreferLocal
	}
	switch input.ScopeMode {
	case domain.SearchPreferLocal, domain.SearchLocalOnly, domain.SearchProjectOnly, domain.SearchAllDevices:
	default:
		return SearchMemoryInput{}, NewError(CodeInvalidArgument, "unsupported scope_mode")
	}
	if input.Limit <= 0 {
		input.Limit = 10
	}
	if input.Limit > 100 {
		input.Limit = 100
	}
	if input.CandidateLimit <= 0 {
		input.CandidateLimit = 100
	}
	if input.CandidateLimit < 10 {
		input.CandidateLimit = 10
	}
	if input.CandidateLimit > 500 {
		input.CandidateLimit = 500
	}
	input.TagsAny = normalizeTags(input.TagsAny)
	input.TagsAll = normalizeTags(input.TagsAll)
	for _, memoryType := range input.Types {
		if err := validateMemoryType(memoryType); err != nil {
			return SearchMemoryInput{}, err
		}
	}

	return input, nil
}

func selectedChannels(mode string) []string {
	if mode == "hybrid" {
		return []string{"exact", "substring", "lexical", "semantic"}
	}

	return []string{mode}
}

func (s *Store) textCandidates(ctx context.Context, input SearchMemoryInput, channel string) ([]searchCandidate, error) {
	memoryCandidates, err := s.queryMemoryTextChannel(ctx, input, channel)
	if err != nil {
		return nil, err
	}
	sourceCandidates, err := s.querySourceTextChannel(ctx, input, channel)
	if err != nil {
		return nil, err
	}

	candidates := append(memoryCandidates, sourceCandidates...)
	sort.Slice(candidates, func(i, j int) bool {
		return channelScore(candidates[i].Result.Score, channel) > channelScore(candidates[j].Result.Score, channel)
	})
	if len(candidates) > input.CandidateLimit {
		candidates = candidates[:input.CandidateLimit]
	}

	return candidates, nil
}

func (s *Store) semanticCandidates(ctx context.Context, input SearchMemoryInput) ([]searchCandidate, error) {
	if s.embedding == nil || !s.embedding.Enabled() {
		return nil, NewError(CodeUnavailable, "semantic retrieval is disabled")
	}
	vector, err := s.embedding.EmbedQuery(ctx, input.Query)
	if err != nil {
		return nil, WrapError(CodeUnavailable, "generate query embedding", err)
	}
	if len(vector) != embedding.Dimensions {
		return nil, NewError(CodeInternal, "embedding provider returned an invalid query vector")
	}

	memoryCandidates, err := s.queryMemorySemantic(ctx, input, pgvector.NewVector(vector))
	if err != nil {
		return nil, err
	}
	sourceCandidates, err := s.querySourceSemantic(ctx, input, pgvector.NewVector(vector))
	if err != nil {
		return nil, err
	}
	candidates := append(memoryCandidates, sourceCandidates...)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Result.Score.Semantic > candidates[j].Result.Score.Semantic
	})
	if len(candidates) > input.CandidateLimit {
		candidates = candidates[:input.CandidateLimit]
	}

	return candidates, nil
}

func (s *Store) queryMemoryTextChannel(ctx context.Context, input SearchMemoryInput, channel string) ([]searchCandidate, error) {
	args := []any{input.Query}
	filters, args, err := buildMemoryFilters(input, "m", args)
	if err != nil {
		return nil, err
	}
	args = append(args, input.CandidateLimit)
	limitPlaceholder := fmt.Sprintf("$%d", len(args))

	var predicate string
	var scoreExpression string
	switch channel {
	case "exact":
		predicate = `(m.id = $1 OR lower(m.title) = lower($1) OR lower(m.content) = lower($1)
            OR lower(coalesce(m.source_path, '')) = lower($1) OR lower(coalesce(m.source_hash, '')) = lower($1)
            OR lower($1) = ANY(m.tags))`
		scoreExpression = `CASE
            WHEN m.id = $1 THEN 5.0
            WHEN lower(coalesce(m.source_hash, '')) = lower($1) THEN 4.5
            WHEN lower(coalesce(m.source_path, '')) = lower($1) THEN 4.0
            WHEN lower(m.title) = lower($1) THEN 3.5
            WHEN lower(m.content) = lower($1) THEN 3.0
            WHEN lower($1) = ANY(m.tags) THEN 2.0
            ELSE 0.0 END`
	case "substring":
		predicate = `m.search_text ILIKE '%' || $1 || '%'`
		scoreExpression = `greatest(similarity(m.search_text, $1),
            CASE WHEN strpos(lower(m.search_text), lower($1)) > 0 THEN 0.5 ELSE 0 END)`
	case "lexical":
		predicate = `m.search_tsv @@ websearch_to_tsquery('simple', $1)`
		scoreExpression = `ts_rank_cd(m.search_tsv, websearch_to_tsquery('simple', $1), 32)`
	default:
		return nil, NewError(CodeInternal, "unknown text channel")
	}

	query := `SELECT ` + memorySearchColumns(scoreExpression) + `
        FROM memories m
        WHERE ` + predicate + ` AND ` + filters + `
        ORDER BY channel_score DESC, m.updated_at DESC, m.id
        LIMIT ` + limitPlaceholder

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, WrapError(CodeInternal, "query "+channel+" memory candidates", err)
	}
	defer rows.Close()

	candidates, err := scanSearchCandidates(rows, channel)
	if err != nil {
		return nil, err
	}

	return candidates, nil
}

func (s *Store) querySourceTextChannel(ctx context.Context, input SearchMemoryInput, channel string) ([]searchCandidate, error) {
	if !sourceTypeAllowed(input.Types) || len(input.TagsAll) > 0 || len(input.TagsAny) > 0 {
		return nil, nil
	}

	args := []any{input.Query}
	filters, args, err := buildSourceFilters(input, "s", args)
	if err != nil {
		return nil, err
	}
	args = append(args, input.CandidateLimit)
	limitPlaceholder := fmt.Sprintf("$%d", len(args))

	var predicate string
	var scoreExpression string
	switch channel {
	case "exact":
		predicate = `(c.id = $1 OR s.id = $1 OR lower(c.content) = lower($1)
            OR lower(s.original_absolute_path) = lower($1) OR lower(s.content_hash) = lower($1))`
		scoreExpression = `CASE
            WHEN c.id = $1 OR s.id = $1 THEN 5.0
            WHEN lower(s.content_hash) = lower($1) THEN 4.5
            WHEN lower(s.original_absolute_path) = lower($1) THEN 4.0
            WHEN lower(c.content) = lower($1) THEN 3.0
            ELSE 0.0 END`
	case "substring":
		predicate = `(c.content ILIKE '%' || $1 || '%' OR s.original_absolute_path ILIKE '%' || $1 || '%')`
		scoreExpression = `greatest(similarity(c.content, $1), similarity(s.original_absolute_path, $1),
            CASE WHEN strpos(lower(c.content), lower($1)) > 0 THEN 0.5 ELSE 0 END)`
	case "lexical":
		predicate = `c.search_tsv @@ websearch_to_tsquery('simple', $1)`
		scoreExpression = `ts_rank_cd(c.search_tsv, websearch_to_tsquery('simple', $1), 32)`
	default:
		return nil, NewError(CodeInternal, "unknown source text channel")
	}

	query := `SELECT ` + sourceSearchColumns(scoreExpression) + `
        FROM source_chunks c
        JOIN sources s ON s.current_content_id = c.content_id
        WHERE ` + predicate + ` AND ` + filters + `
        ORDER BY channel_score DESC, s.updated_at DESC, c.id
        LIMIT ` + limitPlaceholder

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, WrapError(CodeInternal, "query "+channel+" source candidates", err)
	}
	defer rows.Close()

	return scanSearchCandidates(rows, channel)
}

func (s *Store) queryMemorySemantic(ctx context.Context, input SearchMemoryInput, vector pgvector.Vector) ([]searchCandidate, error) {
	args := []any{vector}
	filters, args, err := buildMemoryFilters(input, "m", args)
	if err != nil {
		return nil, err
	}
	args = append(args, s.embeddingProviderName)
	modelPlaceholder := fmt.Sprintf("$%d", len(args))
	args = append(args, input.CandidateLimit)

	query := `SELECT ` + memorySearchColumns(`1.0 - (m.embedding <=> $1)`) + `
		FROM memories m
		WHERE m.embedding IS NOT NULL AND m.embedding_model = ` + modelPlaceholder + ` AND ` + filters + `
        ORDER BY m.embedding <=> $1, m.updated_at DESC, m.id
        LIMIT $` + fmt.Sprint(len(args))
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, WrapError(CodeInternal, "query semantic memory candidates", err)
	}
	defer rows.Close()

	return scanSearchCandidates(rows, "semantic")
}

func (s *Store) querySourceSemantic(ctx context.Context, input SearchMemoryInput, vector pgvector.Vector) ([]searchCandidate, error) {
	if !sourceTypeAllowed(input.Types) || len(input.TagsAll) > 0 || len(input.TagsAny) > 0 {
		return nil, nil
	}

	args := []any{vector}
	filters, args, err := buildSourceFilters(input, "s", args)
	if err != nil {
		return nil, err
	}
	args = append(args, s.embeddingProviderName)
	modelPlaceholder := fmt.Sprintf("$%d", len(args))
	args = append(args, input.CandidateLimit)

	query := `SELECT ` + sourceSearchColumns(`1.0 - (c.embedding <=> $1)`) + `
		FROM source_chunks c
		JOIN sources s ON s.current_content_id = c.content_id
		WHERE c.embedding IS NOT NULL AND c.embedding_model = ` + modelPlaceholder + ` AND ` + filters + `
        ORDER BY c.embedding <=> $1, s.updated_at DESC, c.id
        LIMIT $` + fmt.Sprint(len(args))
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, WrapError(CodeInternal, "query semantic source candidates", err)
	}
	defer rows.Close()

	return scanSearchCandidates(rows, "semantic")
}

func buildMemoryFilters(input SearchMemoryInput, alias string, args []any) (string, []any, error) {
	conditions := []string{alias + ".namespace = $" + fmt.Sprint(len(args)+1)}
	args = append(args, input.Namespace)
	conditions = append(conditions, alias+".lifecycle_status <> 'deleted'")
	if !input.IncludeExpired {
		conditions = append(conditions, alias+".lifecycle_status <> 'expired'", "("+alias+".expires_at IS NULL OR "+alias+".expires_at > statement_timestamp())")
	}
	if !input.IncludeRefuted {
		conditions = append(conditions, alias+".lifecycle_status <> 'refuted'")
	}
	if !input.IncludeSuperseded {
		conditions = append(conditions, alias+".lifecycle_status <> 'superseded'")
	}
	conditions, args = appendScopeFilter(conditions, args, input, alias)

	if len(input.TagsAny) > 0 {
		args = append(args, input.TagsAny)
		conditions = append(conditions, alias+".tags && $"+fmt.Sprint(len(args))+"::text[]")
	}
	if len(input.TagsAll) > 0 {
		args = append(args, input.TagsAll)
		conditions = append(conditions, alias+".tags @> $"+fmt.Sprint(len(args))+"::text[]")
	}
	if len(input.Types) > 0 {
		args = append(args, input.Types)
		conditions = append(conditions, alias+".memory_type = ANY($"+fmt.Sprint(len(args))+"::text[])")
	}
	if len(input.MetadataContains) > 0 {
		metadata, err := json.Marshal(input.MetadataContains)
		if err != nil {
			return "", nil, WrapError(CodeInvalidArgument, "encode metadata_contains", err)
		}
		args = append(args, metadata)
		conditions = append(conditions, alias+".metadata @> $"+fmt.Sprint(len(args))+"::jsonb")
	}
	if input.SourcePath != "" {
		args = append(args, "%"+input.SourcePath+"%")
		conditions = append(conditions, alias+".source_path ILIKE $"+fmt.Sprint(len(args)))
	}
	conditions, args = appendTimeFilter(conditions, args, alias+".created_at", input.CreatedAfter, input.CreatedBefore)
	conditions, args = appendTimeFilter(conditions, args, alias+".updated_at", input.UpdatedAfter, input.UpdatedBefore)
	conditions, args = appendTimeFilter(conditions, args, alias+".observed_at", input.ObservedAfter, input.ObservedBefore)

	return strings.Join(conditions, " AND "), args, nil
}

func buildSourceFilters(input SearchMemoryInput, alias string, args []any) (string, []any, error) {
	conditions := []string{
		alias + ".namespace = $" + fmt.Sprint(len(args)+1),
		alias + ".lifecycle_status = 'active'",
	}
	args = append(args, input.Namespace)
	if !input.IncludeExpired {
		conditions = append(conditions, "("+alias+".expires_at IS NULL OR "+alias+".expires_at > statement_timestamp())")
	}
	conditions, args = appendScopeFilter(conditions, args, input, alias)
	if len(input.MetadataContains) > 0 {
		metadata, err := json.Marshal(input.MetadataContains)
		if err != nil {
			return "", nil, WrapError(CodeInvalidArgument, "encode source metadata_contains", err)
		}
		args = append(args, metadata)
		conditions = append(conditions, alias+".metadata @> $"+fmt.Sprint(len(args))+"::jsonb")
	}
	if input.SourcePath != "" {
		args = append(args, "%"+input.SourcePath+"%")
		conditions = append(conditions, alias+".original_absolute_path ILIKE $"+fmt.Sprint(len(args)))
	}
	conditions, args = appendTimeFilter(conditions, args, alias+".created_at", input.CreatedAfter, input.CreatedBefore)
	conditions, args = appendTimeFilter(conditions, args, alias+".updated_at", input.UpdatedAfter, input.UpdatedBefore)
	conditions, args = appendTimeFilter(conditions, args, alias+".mtime", input.ObservedAfter, input.ObservedBefore)

	return strings.Join(conditions, " AND "), args, nil
}

func appendScopeFilter(conditions []string, args []any, input SearchMemoryInput, alias string) ([]string, []any) {
	switch input.ScopeMode {
	case domain.SearchLocalOnly:
		args = append(args, input.Caller.InstallationCode, input.Caller.DeviceCode, input.Caller.WorkspaceCode)
		base := len(args) - 2
		conditions = append(conditions, fmt.Sprintf(`(
            (%s.scope_type = 'installation' AND %s.scope_id = $%d) OR
            (%s.scope_type = 'device' AND %s.scope_id = $%d) OR
            (%s.scope_type = 'workspace' AND %s.scope_id = $%d)
        )`, alias, alias, base, alias, alias, base+1, alias, alias, base+2))
	case domain.SearchProjectOnly:
		conditions = append(conditions, alias+".scope_type = 'project'")
	case domain.SearchPreferLocal, domain.SearchAllDevices:
	}

	return conditions, args
}

func appendTimeFilter(
	conditions []string,
	args []any,
	column string,
	after *time.Time,
	before *time.Time,
) ([]string, []any) {
	if after != nil {
		args = append(args, after)
		conditions = append(conditions, column+" >= $"+fmt.Sprint(len(args)))
	}
	if before != nil {
		args = append(args, before)
		conditions = append(conditions, column+" <= $"+fmt.Sprint(len(args)))
	}

	return conditions, args
}

func memorySearchColumns(scoreExpression string) string {
	return `
        'memory'::text AS kind, m.id, m.id AS memory_id, coalesce(m.source_id, ''),
        m.namespace, m.scope_type, m.scope_id,
        coalesce(m.device_code, ''), coalesce(m.installation_code, ''), coalesce(m.workspace_code, ''),
        m.memory_type, m.title, m.content, m.metadata, m.tags,
        m.lifecycle_status, m.verification_state, m.confidence, m.evidence,
        coalesce(m.source_path, ''), coalesce(m.source_hash, ''), m.source_range,
        m.expires_at, m.version, m.updated_at,
        (` + scoreExpression + `)::double precision AS channel_score`
}

func sourceSearchColumns(scoreExpression string) string {
	return `
        'source_chunk'::text AS kind, c.id, ''::text AS memory_id, s.id AS source_id,
        s.namespace, s.scope_type, s.scope_id,
        coalesce(s.device_code, ''), coalesce(s.installation_code, ''), coalesce(s.workspace_code, ''),
        'artifact'::text AS memory_type,
        coalesce(nullif(s.relative_path, ''), s.original_absolute_path) AS title,
        c.content, s.metadata, '{}'::text[] AS tags,
        s.lifecycle_status, 'supported'::text AS verification_state, 0.7::real AS confidence,
        jsonb_build_array(jsonb_build_object(
            'source_id', s.id, 'path', s.original_absolute_path, 'hash', s.content_hash,
            'start_line', c.start_line, 'end_line', c.end_line,
            'start_char', c.start_char, 'end_char', c.end_char
        )) AS evidence,
        s.original_absolute_path, s.content_hash,
        jsonb_build_object(
            'start_line', c.start_line, 'end_line', c.end_line,
            'start_char', c.start_char, 'end_char', c.end_char
        ) AS source_range,
        s.expires_at, s.generation AS version, s.updated_at,
        (` + scoreExpression + `)::double precision AS channel_score`
}

type rowsScanner interface {
	Next() bool
	Scan(...any) error
	Err() error
}

func scanSearchCandidates(rows rowsScanner, channel string) ([]searchCandidate, error) {
	candidates := make([]searchCandidate, 0)
	for rows.Next() {
		var candidate searchCandidate
		var channelScore float64
		result := &candidate.Result
		if err := rows.Scan(
			&result.Kind,
			&result.ID,
			&result.MemoryID,
			&result.SourceID,
			&result.Namespace,
			&result.ScopeType,
			&result.ScopeID,
			&result.DeviceCode,
			&result.InstallationCode,
			&result.WorkspaceCode,
			&result.Type,
			&result.Title,
			&result.Content,
			&result.Metadata,
			&result.Tags,
			&result.Status,
			&result.VerificationState,
			&result.Confidence,
			&result.Evidence,
			&result.SourcePath,
			&result.SourceHash,
			&result.SourceRange,
			&result.ExpiresAt,
			&result.Version,
			&candidate.UpdatedAt,
			&channelScore,
		); err != nil {
			return nil, WrapError(CodeInternal, "scan "+channel+" search candidate", err)
		}
		switch channel {
		case "exact":
			result.Score.Exact = channelScore
		case "substring":
			result.Score.Substring = channelScore
		case "lexical":
			result.Score.Lexical = channelScore
		case "semantic":
			result.Score.Semantic = channelScore
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, WrapError(CodeInternal, "iterate "+channel+" search candidates", err)
	}

	return candidates, nil
}

func fuseCandidates(channels map[string][]searchCandidate, caller domain.CallerIdentity) []domain.SearchResult {
	merged := make(map[string]*searchCandidate)
	weights := map[string]float64{
		"exact": 4.0, "substring": 2.0, "lexical": 1.5, "semantic": 1.0,
	}
	for channel, candidates := range channels {
		for index, candidate := range candidates {
			key := candidate.Result.Kind + "\x00" + candidate.Result.ID + "\x00" + candidate.Result.SourceID
			current, exists := merged[key]
			if !exists {
				copy := candidate
				current = &copy
				merged[key] = current
			}
			mergeChannelScore(&current.Result.Score, candidate.Result.Score)
			current.Result.Score.RRF += weights[channel] / (rrfConstant + float64(index+1))
		}
	}

	candidates := make([]searchCandidate, 0, len(merged))
	for _, candidate := range merged {
		result := &candidate.Result
		result.Score.Confidence = result.Confidence
		result.Score.Evidence = evidenceScore(result.Evidence)
		result.Score.Locality = localityScore(*result, caller)
		result.IsLocal = result.Score.Locality >= 60
		result.Snippet = makeSnippet(result.Content, "", 360)
		result.Score.Final = finalDiagnosticScore(*result)
		candidates = append(candidates, *candidate)
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		left := candidates[i]
		right := candidates[j]
		if rankStatus(left.Result.Status) != rankStatus(right.Result.Status) {
			return rankStatus(left.Result.Status) > rankStatus(right.Result.Status)
		}
		if left.Result.Score.Exact != right.Result.Score.Exact {
			return left.Result.Score.Exact > right.Result.Score.Exact
		}
		if rankVerification(left.Result.VerificationState) != rankVerification(right.Result.VerificationState) {
			return rankVerification(left.Result.VerificationState) > rankVerification(right.Result.VerificationState)
		}
		if left.Result.Score.Evidence != right.Result.Score.Evidence {
			return left.Result.Score.Evidence > right.Result.Score.Evidence
		}
		if left.Result.Confidence != right.Result.Confidence {
			return left.Result.Confidence > right.Result.Confidence
		}
		if left.Result.Score.Locality != right.Result.Score.Locality {
			return left.Result.Score.Locality > right.Result.Score.Locality
		}
		if left.Result.Score.RRF != right.Result.Score.RRF {
			return left.Result.Score.RRF > right.Result.Score.RRF
		}
		if !left.UpdatedAt.Equal(right.UpdatedAt) {
			return left.UpdatedAt.After(right.UpdatedAt)
		}
		return left.Result.ID < right.Result.ID
	})

	results := make([]domain.SearchResult, len(candidates))
	for index, candidate := range candidates {
		results[index] = candidate.Result
	}

	return results
}

func mergeChannelScore(target *domain.ScoreBreakdown, source domain.ScoreBreakdown) {
	if source.Exact > target.Exact {
		target.Exact = source.Exact
	}
	if source.Substring > target.Substring {
		target.Substring = source.Substring
	}
	if source.Lexical > target.Lexical {
		target.Lexical = source.Lexical
	}
	if source.Semantic > target.Semantic {
		target.Semantic = source.Semantic
	}
}

func channelScore(score domain.ScoreBreakdown, channel string) float64 {
	switch channel {
	case "exact":
		return score.Exact
	case "substring":
		return score.Substring
	case "lexical":
		return score.Lexical
	case "semantic":
		return score.Semantic
	default:
		return 0
	}
}

func sourceTypeAllowed(types []string) bool {
	if len(types) == 0 {
		return true
	}
	for _, memoryType := range types {
		if memoryType == "artifact" {
			return true
		}
	}

	return false
}

func localityScore(result domain.SearchResult, caller domain.CallerIdentity) float64 {
	if result.ScopeType == domain.ScopeInstallation && result.ScopeID == caller.InstallationCode {
		return 100
	}
	if result.ScopeType == domain.ScopeDevice && result.ScopeID == caller.DeviceCode {
		return 80
	}
	if result.ScopeType == domain.ScopeWorkspace && result.ScopeID == caller.WorkspaceCode {
		return 60
	}
	if result.ScopeType == domain.ScopeProject {
		return 40
	}
	if result.ScopeType == domain.ScopeGlobal {
		return 20
	}

	return 0
}

func evidenceScore(raw json.RawMessage) float64 {
	var entries []json.RawMessage
	if json.Unmarshal(raw, &entries) != nil {
		return 0
	}

	return float64(len(entries))
}

func rankStatus(status string) int {
	switch status {
	case domain.StatusActive:
		return 5
	case domain.StatusSuperseded:
		return 4
	case domain.StatusRefuted:
		return 3
	case domain.StatusExpired:
		return 2
	case domain.StatusDeleted:
		return 1
	default:
		return 0
	}
}

func rankVerification(state string) int {
	switch state {
	case "confirmed":
		return 4
	case "supported":
		return 3
	case "unverified":
		return 2
	case "contested":
		return 1
	default:
		return 0
	}
}

func finalDiagnosticScore(result domain.SearchResult) float64 {
	exactBucket := 0.0
	if result.Score.Exact > 0 {
		exactBucket = 1
	}

	return float64(rankStatus(result.Status))*1e9 +
		exactBucket*1e8 + result.Score.Exact*1e7 +
		float64(rankVerification(result.VerificationState))*1e6 +
		result.Score.Evidence*1e5 + result.Confidence*1e4 +
		result.Score.Locality*10 + result.Score.RRF
}

func makeSnippet(content, query string, maxRunes int) string {
	content = strings.TrimSpace(content)
	if utf8.RuneCountInString(content) <= maxRunes {
		return content
	}
	runes := []rune(content)
	start := 0
	if query != "" {
		lowerContent := strings.ToLower(content)
		position := strings.Index(lowerContent, strings.ToLower(query))
		if position >= 0 {
			start = utf8.RuneCountInString(content[:position]) - maxRunes/3
			if start < 0 {
				start = 0
			}
		}
	}
	end := start + maxRunes
	if end > len(runes) {
		end = len(runes)
		start = end - maxRunes
	}
	prefix := ""
	suffix := ""
	if start > 0 {
		prefix = "…"
	}
	if end < len(runes) {
		suffix = "…"
	}

	return prefix + string(runes[start:end]) + suffix
}
