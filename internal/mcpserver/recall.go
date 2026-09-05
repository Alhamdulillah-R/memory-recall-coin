package mcpserver

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/Alhamdulillah-R/memory-recall-coin/internal/domain"
	"github.com/Alhamdulillah-R/memory-recall-coin/internal/service"
)

const (
	recallRetrievalMode = "hybrid"
	recallDefaultLimit  = 10
	recallMaximumLimit  = 100
	recallMaximumRoots  = 8
)

// RecallInput describes an opinionated search over zero or more explicit namespace selectors.
type RecallInput struct {
	Query              string   `json:"query" jsonschema:"natural-language or exact-text recall query"`
	Namespaces         []string `json:"namespaces,omitempty" jsonschema:"explicit namespace paths; omit every selector to recall from all namespaces"`
	NamespaceSequences []int64  `json:"namespace_sequences,omitempty" jsonschema:"explicit stable namespace sequences; omit every selector to recall from all namespaces"`
	NamespaceMatch     string   `json:"namespace_match,omitempty" jsonschema:"exact or subtree; default subtree; ignored when no selector is given"`
	ScopeMode          string   `json:"scope_mode,omitempty" jsonschema:"prefer_local, local_only, project_only, or all_devices; default all_devices"`
	Limit              int      `json:"limit,omitempty" jsonschema:"global maximum results from 1 to 100; default 10"`
}

// RecallResult is the evidence-oriented projection returned by high-level recall.
type RecallResult struct {
	Kind              string          `json:"kind"`
	ID                string          `json:"id"`
	MemoryID          string          `json:"memory_id,omitempty"`
	SourceID          string          `json:"source_id,omitempty"`
	Namespace         string          `json:"namespace"`
	Type              string          `json:"type,omitempty"`
	Title             string          `json:"title"`
	Summary           string          `json:"summary,omitempty"`
	Snippet           string          `json:"snippet"`
	Tags              []string        `json:"tags,omitempty"`
	Status            string          `json:"status"`
	VerificationState string          `json:"verification_state"`
	Confidence        float64         `json:"confidence"`
	Evidence          json.RawMessage `json:"evidence,omitempty"`
	SourcePath        string          `json:"source_path,omitempty"`
	SourceRange       json.RawMessage `json:"source_range,omitempty"`
	InferredClaims    []string        `json:"inferred_claims,omitempty"`
	Score             float64         `json:"score"`
}

// RecallAttempt reports how one explicit namespace selector was resolved and searched.
type RecallAttempt struct {
	RequestedNamespace         string `json:"requested_namespace,omitempty"`
	RequestedNamespaceSequence *int64 `json:"requested_namespace_sequence,omitempty"`
	AllNamespaces              bool   `json:"all_namespaces,omitempty"`
	ResolvedNamespace          string `json:"resolved_namespace,omitempty"`
	ResultCount                int    `json:"result_count"`
	SemanticEnabled            bool   `json:"semantic_enabled"`
	SemanticError              string `json:"semantic_error,omitempty"`
	DurationMS                 int64  `json:"duration_ms"`
}

// RecallResponse 把 curated memory 與 source_chunk 分成兩段回，各自最多 limit 筆。
type RecallResponse struct {
	Results          []RecallResult  `json:"results" jsonschema:"curated memories ranked by score; read these first"`
	SourceChunks     []RecallResult  `json:"source_chunks,omitempty" jsonschema:"raw ingested text chunks ranked by score; supporting material only"`
	Attempts         []RecallAttempt `json:"attempts"`
	Query            string          `json:"query"`
	NamespaceMatch   string          `json:"namespace_match"`
	ScopeMode        string          `json:"scope_mode"`
	DetailLevel      string          `json:"detail_level"`
	RetrievalMode    string          `json:"retrieval_mode"`
	Count            int             `json:"count"`
	MemoryCount      int             `json:"memory_count"`
	SourceChunkCount int             `json:"source_chunk_count"`
	DurationMS       int64           `json:"duration_ms"`
}

type recallSelector struct {
	namespace         string
	namespaceSequence *int64
	allNamespaces     bool
}

// applyRecallInputSchemaConstraints makes the multi-namespace contract explicit to MCP clients.
func applyRecallInputSchemaConstraints(schema *jsonschema.Schema) {
	if schema == nil {
		return
	}

	const namespacePattern = `^[a-z0-9]([a-z0-9._-]*[a-z0-9])?(/[a-z0-9]([a-z0-9._-]*[a-z0-9])?)*$`
	zero := 0.0
	if namespaces, exists := schema.Properties["namespaces"]; exists {
		maxItems := recallMaximumRoots
		namespaces.MaxItems = &maxItems
		namespaces.UniqueItems = true
		if namespaces.Items != nil {
			namespaces.Items.Pattern = namespacePattern
			maxLength := 128
			namespaces.Items.MaxLength = &maxLength
		}
	}
	if sequences, exists := schema.Properties["namespace_sequences"]; exists {
		maxItems := recallMaximumRoots
		sequences.MaxItems = &maxItems
		sequences.UniqueItems = true
		if sequences.Items != nil {
			sequences.Items.Minimum = &zero
		}
	}

	setPropertyEnum(schema, "namespace_match", []string{
		domain.NamespaceMatchExact,
		domain.NamespaceMatchSubtree,
	})
	setPropertyEnum(schema, "scope_mode", []string{
		domain.SearchPreferLocal,
		domain.SearchLocalOnly,
		domain.SearchProjectOnly,
		domain.SearchAllDevices,
	})
	setNumericPropertyRange(schema, "limit", 1, recallMaximumLimit)
	setPropertyDefault(schema, "namespace_match", json.RawMessage(`"subtree"`))
	setPropertyDefault(schema, "scope_mode", json.RawMessage(`"all_devices"`))
	setPropertyDefault(schema, "limit", json.RawMessage(`10`))
}

/**
 * memoryRecall searches each explicit namespace selector with the same opinionated hybrid contract.
 * @return merged and globally ranked memory and source-chunk results
 */
func (h *Handlers) memoryRecall(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	input RecallInput,
) (*mcp.CallToolResult, RecallResponse, error) {
	startedAt := time.Now()
	normalized, err := normalizeRecallInput(input)
	if err != nil {
		return nil, RecallResponse{}, err
	}

	selectors := recallSelectors(normalized)
	results := make([]domain.SearchResult, 0, normalized.Limit)
	resultIndexes := make(map[string]int, normalized.Limit)
	attempts := make([]RecallAttempt, 0, len(selectors))
	for _, selector := range selectors {
		if err := ctx.Err(); err != nil {
			return nil, RecallResponse{}, err
		}

		namespaceMatch := normalized.NamespaceMatch
		if selector.allNamespaces {
			namespaceMatch = domain.NamespaceMatchAll
		}
		response, searchErr := h.backend.SearchMemory(ctx, service.SearchMemoryInput{
			Namespace:         selector.namespace,
			NamespaceSequence: selector.namespaceSequence,
			NamespaceMatch:    namespaceMatch,
			Query:             normalized.Query,
			RetrievalMode:     recallRetrievalMode,
			ScopeMode:         normalized.ScopeMode,
			DetailLevel:       domain.SearchDetailEvidence,
			Kinds:             []string{"memory", "source_chunk"},
			Limit:             normalized.Limit,
			LimitPerKind:      true,
		})
		if searchErr != nil {
			return nil, RecallResponse{}, searchErr
		}

		attempts = append(attempts, newRecallAttempt(selector, response))
		mergeRecallResults(&results, resultIndexes, response.Results)
	}

	sortRecallResults(results)
	memories, chunks := splitRecallResults(results, normalized.Limit)

	responseMatch := normalized.NamespaceMatch
	if len(selectors) == 1 && selectors[0].allNamespaces {
		responseMatch = domain.NamespaceMatchAll
	}

	return nil, RecallResponse{
		Results:          projectRecallResults(memories),
		SourceChunks:     projectRecallResults(chunks),
		Attempts:         attempts,
		Query:            normalized.Query,
		NamespaceMatch:   responseMatch,
		ScopeMode:        normalized.ScopeMode,
		DetailLevel:      domain.SearchDetailEvidence,
		RetrievalMode:    recallRetrievalMode,
		Count:            len(memories) + len(chunks),
		MemoryCount:      len(memories),
		SourceChunkCount: len(chunks),
		DurationMS:       time.Since(startedAt).Milliseconds(),
	}, nil
}

// splitRecallResults 把排好序的結果依 kind 分成兩段，各自截到 limit
func splitRecallResults(results []domain.SearchResult, limit int) ([]domain.SearchResult, []domain.SearchResult) {
	memories := make([]domain.SearchResult, 0, limit)
	chunks := make([]domain.SearchResult, 0, limit)
	for _, result := range results {
		if result.Kind == "memory" && len(memories) < limit {
			memories = append(memories, result)
		}
		if result.Kind == "source_chunk" && len(chunks) < limit {
			chunks = append(chunks, result)
		}
	}

	return memories, chunks
}

func normalizeRecallInput(input RecallInput) (RecallInput, error) {
	input.Query = strings.TrimSpace(input.Query)
	if input.Query == "" {
		return RecallInput{}, service.NewError(service.CodeInvalidArgument, "query is required")
	}
	if len(input.Namespaces)+len(input.NamespaceSequences) > recallMaximumRoots {
		return RecallInput{}, service.NewError(service.CodeInvalidArgument, "at most 8 namespace selectors are allowed")
	}

	for index := range input.Namespaces {
		input.Namespaces[index] = strings.TrimSpace(input.Namespaces[index])
		if input.Namespaces[index] == "" {
			return RecallInput{}, service.NewError(service.CodeInvalidArgument, "namespaces cannot contain an empty path")
		}
	}
	for _, sequence := range input.NamespaceSequences {
		if sequence < 0 {
			return RecallInput{}, service.NewError(service.CodeInvalidArgument, "namespace_sequences must be non-negative")
		}
	}

	if input.NamespaceMatch == "" {
		input.NamespaceMatch = domain.NamespaceMatchSubtree
	}
	if input.NamespaceMatch != domain.NamespaceMatchExact && input.NamespaceMatch != domain.NamespaceMatchSubtree {
		return RecallInput{}, service.NewError(service.CodeInvalidArgument, "namespace_match must be exact or subtree")
	}

	if input.ScopeMode == "" {
		input.ScopeMode = domain.SearchAllDevices
	}
	switch input.ScopeMode {
	case domain.SearchPreferLocal, domain.SearchLocalOnly, domain.SearchProjectOnly, domain.SearchAllDevices:
	default:
		return RecallInput{}, service.NewError(service.CodeInvalidArgument, "unsupported scope_mode")
	}

	if input.Limit == 0 {
		input.Limit = recallDefaultLimit
	}
	if input.Limit < 1 || input.Limit > recallMaximumLimit {
		return RecallInput{}, service.NewError(service.CodeInvalidArgument, "limit must be between 1 and 100")
	}

	return input, nil
}

func projectRecallResults(results []domain.SearchResult) []RecallResult {
	projected := make([]RecallResult, len(results))
	for index, result := range results {
		projected[index] = RecallResult{
			Kind:              result.Kind,
			ID:                result.ID,
			MemoryID:          result.MemoryID,
			SourceID:          result.SourceID,
			Namespace:         result.Namespace,
			Type:              result.Type,
			Title:             result.Title,
			Summary:           result.Summary,
			Snippet:           result.Snippet,
			Tags:              result.Tags,
			Status:            result.Status,
			VerificationState: result.VerificationState,
			Confidence:        result.Confidence,
			Evidence:          result.Evidence,
			SourcePath:        result.SourcePath,
			SourceRange:       result.SourceRange,
			InferredClaims:    result.InferredClaims,
			Score:             result.Score.Final,
		}
	}

	return projected
}

func recallSelectors(input RecallInput) []recallSelector {
	// 完全沒給 selector 就做一次全庫檢索
	if len(input.Namespaces) == 0 && len(input.NamespaceSequences) == 0 {
		return []recallSelector{{allNamespaces: true}}
	}

	selectors := make([]recallSelector, 0, len(input.Namespaces)+len(input.NamespaceSequences))
	for _, namespace := range input.Namespaces {
		selectors = append(selectors, recallSelector{namespace: namespace})
	}
	for _, sequence := range input.NamespaceSequences {
		sequence := sequence
		selectors = append(selectors, recallSelector{namespaceSequence: &sequence})
	}

	return selectors
}

func newRecallAttempt(selector recallSelector, response domain.SearchResponse) RecallAttempt {
	attempt := RecallAttempt{
		RequestedNamespace:         selector.namespace,
		RequestedNamespaceSequence: selector.namespaceSequence,
		AllNamespaces:              selector.allNamespaces,
		ResolvedNamespace:          response.Namespace,
		ResultCount:                len(response.Results),
		SemanticEnabled:            response.SemanticEnabled,
		SemanticError:              response.SemanticError,
		DurationMS:                 response.DurationMS,
	}

	return attempt
}

func mergeRecallResults(
	results *[]domain.SearchResult,
	resultIndexes map[string]int,
	candidates []domain.SearchResult,
) {
	for _, candidate := range candidates {
		key := candidate.Kind + "\x00" + candidate.ID
		index, exists := resultIndexes[key]
		if !exists {
			resultIndexes[key] = len(*results)
			*results = append(*results, candidate)
			continue
		}
		if candidate.Score.Final > (*results)[index].Score.Final {
			(*results)[index] = candidate
		}
	}
}

func sortRecallResults(results []domain.SearchResult) {
	sort.SliceStable(results, func(leftIndex, rightIndex int) bool {
		left := results[leftIndex]
		right := results[rightIndex]
		if left.Score.Final != right.Score.Final {
			return left.Score.Final > right.Score.Final
		}
		if left.Score.Relevance != right.Score.Relevance {
			return left.Score.Relevance > right.Score.Relevance
		}
		if left.Kind != right.Kind {
			return left.Kind < right.Kind
		}

		return left.ID < right.ID
	})
}
