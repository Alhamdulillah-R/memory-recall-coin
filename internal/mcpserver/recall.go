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

// RecallInput describes an opinionated search over one or more explicit namespace selectors.
type RecallInput struct {
	Query              string   `json:"query" jsonschema:"natural-language or exact-text recall query"`
	Namespaces         []string `json:"namespaces,omitempty" jsonschema:"explicit namespace paths; at least one path or sequence is required"`
	NamespaceSequences []int64  `json:"namespace_sequences,omitempty" jsonschema:"explicit stable namespace sequences; at least one path or sequence is required"`
	NamespaceMatch     string   `json:"namespace_match,omitempty" jsonschema:"exact or subtree; default subtree"`
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
	Snippet           string          `json:"snippet"`
	Tags              []string        `json:"tags,omitempty"`
	Status            string          `json:"status"`
	VerificationState string          `json:"verification_state"`
	Confidence        float64         `json:"confidence"`
	Evidence          json.RawMessage `json:"evidence,omitempty"`
	SourcePath        string          `json:"source_path,omitempty"`
	SourceRange       json.RawMessage `json:"source_range,omitempty"`
	Score             float64         `json:"score"`
}

// RecallAttempt reports how one explicit namespace selector was resolved and searched.
type RecallAttempt struct {
	RequestedNamespace         string `json:"requested_namespace,omitempty"`
	RequestedNamespaceSequence *int64 `json:"requested_namespace_sequence,omitempty"`
	ResolvedNamespace          string `json:"resolved_namespace"`
	ResultCount                int    `json:"result_count"`
	SemanticEnabled            bool   `json:"semantic_enabled"`
	SemanticError              string `json:"semantic_error,omitempty"`
	DurationMS                 int64  `json:"duration_ms"`
}

// RecallResponse contains globally ranked results without per-channel candidate diagnostics.
type RecallResponse struct {
	Results        []RecallResult  `json:"results"`
	Attempts       []RecallAttempt `json:"attempts"`
	Query          string          `json:"query"`
	NamespaceMatch string          `json:"namespace_match"`
	ScopeMode      string          `json:"scope_mode"`
	DetailLevel    string          `json:"detail_level"`
	RetrievalMode  string          `json:"retrieval_mode"`
	Count          int             `json:"count"`
	DurationMS     int64           `json:"duration_ms"`
}

type recallSelector struct {
	namespace         string
	namespaceSequence *int64
}

// applyRecallInputSchemaConstraints makes the multi-namespace contract explicit to MCP clients.
func applyRecallInputSchemaConstraints(schema *jsonschema.Schema) {
	if schema == nil {
		return
	}

	const namespacePattern = `^[a-z0-9]([a-z0-9._-]*[a-z0-9])?(/[a-z0-9]([a-z0-9._-]*[a-z0-9])?)*$`
	one := 1
	zero := 0.0
	if namespaces, exists := schema.Properties["namespaces"]; exists {
		namespaces.MinItems = &one
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
		sequences.MinItems = &one
		maxItems := recallMaximumRoots
		sequences.MaxItems = &maxItems
		sequences.UniqueItems = true
		if sequences.Items != nil {
			sequences.Items.Minimum = &zero
		}
	}
	schema.AnyOf = append(schema.AnyOf,
		&jsonschema.Schema{Required: []string{"namespaces"}},
		&jsonschema.Schema{Required: []string{"namespace_sequences"}},
	)

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

		response, searchErr := h.backend.SearchMemory(ctx, service.SearchMemoryInput{
			Namespace:         selector.namespace,
			NamespaceSequence: selector.namespaceSequence,
			NamespaceMatch:    normalized.NamespaceMatch,
			Query:             normalized.Query,
			RetrievalMode:     recallRetrievalMode,
			ScopeMode:         normalized.ScopeMode,
			DetailLevel:       domain.SearchDetailEvidence,
			Kinds:             []string{"memory", "source_chunk"},
			Limit:             normalized.Limit,
		})
		if searchErr != nil {
			return nil, RecallResponse{}, searchErr
		}

		attempts = append(attempts, newRecallAttempt(selector, response))
		mergeRecallResults(&results, resultIndexes, response.Results)
	}

	sortRecallResults(results)
	if len(results) > normalized.Limit {
		results = results[:normalized.Limit]
	}

	return nil, RecallResponse{
		Results:        projectRecallResults(results),
		Attempts:       attempts,
		Query:          normalized.Query,
		NamespaceMatch: normalized.NamespaceMatch,
		ScopeMode:      normalized.ScopeMode,
		DetailLevel:    domain.SearchDetailEvidence,
		RetrievalMode:  recallRetrievalMode,
		Count:          len(results),
		DurationMS:     time.Since(startedAt).Milliseconds(),
	}, nil
}

func normalizeRecallInput(input RecallInput) (RecallInput, error) {
	input.Query = strings.TrimSpace(input.Query)
	if input.Query == "" {
		return RecallInput{}, service.NewError(service.CodeInvalidArgument, "query is required")
	}
	if len(input.Namespaces) == 0 && len(input.NamespaceSequences) == 0 {
		return RecallInput{}, service.NewError(
			service.CodeInvalidArgument,
			"namespaces or namespace_sequences must contain at least one selector",
		)
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
			Snippet:           result.Snippet,
			Tags:              result.Tags,
			Status:            result.Status,
			VerificationState: result.VerificationState,
			Confidence:        result.Confidence,
			Evidence:          result.Evidence,
			SourcePath:        result.SourcePath,
			SourceRange:       result.SourceRange,
			Score:             result.Score.Final,
		}
	}

	return projected
}

func recallSelectors(input RecallInput) []recallSelector {
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
