package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/Alhamdulillah-R/memory-recall-coin/internal/domain"
	"github.com/Alhamdulillah-R/memory-recall-coin/internal/ingest"
	"github.com/Alhamdulillah-R/memory-recall-coin/internal/service"
)

var boolTrue = true
var boolFalse = false

// Options configures one MCP surface over a backend.
type Options struct {
	Version          string
	Logger           *slog.Logger
	IngestionManager *ingest.Manager
	DefaultNamespace string
}

// Handlers connects MCP tools to the typed service backend.
type Handlers struct {
	backend          service.Backend
	ingestionManager *ingest.Manager
}

// IngestPathInput describes a local path scan without exposing internal file payloads.
type IngestPathInput struct {
	Path              string     `json:"path" jsonschema:"local absolute file or directory path"`
	Namespace         string     `json:"namespace,omitempty" jsonschema:"slash-separated namespace path; mutually exclusive with namespace_sequence"`
	NamespaceSequence *int64     `json:"namespace_sequence,omitempty" jsonschema:"stable namespace sequence; mutually exclusive with namespace"`
	ScopeType         string     `json:"scope_type,omitempty" jsonschema:"installation, device, workspace, project, or global"`
	ScopeID           string     `json:"scope_id,omitempty"`
	TTLSeconds        *int64     `json:"ttl_seconds,omitempty"`
	ExpiresAt         *time.Time `json:"expires_at,omitempty"`
	Recursive         *bool      `json:"recursive,omitempty"`
	Include           []string   `json:"include,omitempty" jsonschema:"doublestar include globs relative to path"`
	Exclude           []string   `json:"exclude,omitempty" jsonschema:"doublestar exclude globs relative to path"`
	WatchMode         string     `json:"watch_mode,omitempty" jsonschema:"once, sync, or watch"`
	Parser            string     `json:"parser,omitempty" jsonschema:"auto, text, markdown, or caller-defined parser label"`
	PruneMissing      *bool      `json:"prune_missing,omitempty" jsonschema:"remove server indexes absent from a complete manifest; defaults true for sync/watch"`
}

// PinMemoryInput clears expiration using optimistic concurrency.
type PinMemoryInput struct {
	Namespace         string `json:"namespace,omitempty"`
	NamespaceSequence *int64 `json:"namespace_sequence,omitempty"`
	MemoryID          string `json:"memory_id"`
	ExpectedVersion   int64  `json:"expected_version"`
	Reason            string `json:"reason,omitempty"`
	Actor             string `json:"actor,omitempty"`
	IdempotencyKey    string `json:"idempotency_key,omitempty"`
}

// WatchListInput is intentionally empty.
type WatchListInput struct{}

// WatchStopInput selects a local filesystem watch.
type WatchStopInput struct {
	WatchID string `json:"watch_id"`
}

// IngestionStatusInput selects one central ingestion job.
type IngestionStatusInput struct {
	Namespace         string `json:"namespace,omitempty"`
	NamespaceSequence *int64 `json:"namespace_sequence,omitempty"`
	IngestionID       string `json:"ingestion_id"`
}

// HealthInput is intentionally empty.
type HealthInput struct{}

// HistoryResult wraps revisions because MCP tool output schemas require an object root.
type HistoryResult struct {
	Revisions []domain.Revision `json:"revisions"`
}

// WatchListResult wraps local watches because MCP tool output schemas require an object root.
type WatchListResult struct {
	Watches []ingest.WatchInfo `json:"watches"`
}

type toolError struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

/**
 * New creates an MCP server with the complete memory, source, watch and identity tool set.
 * @return configured MCP server
 */
func New(backend service.Backend, options Options) *mcp.Server {
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}
	handlers := &Handlers{
		backend:          backend,
		ingestionManager: options.IngestionManager,
	}

	server := mcp.NewServer(
		&mcp.Implementation{Name: "memory-recall-coin", Version: options.Version},
		&mcp.ServerOptions{
			Instructions: `Primary workflow: memory_put records durable knowledge, memory_search recalls by meaning or text, memory_list browses by filters without a query, and memory_get reads one exact ID or version. Every memory and source operation requires exactly one namespace selector: the slash-separated namespace path or its stable namespace_sequence. namespace_match defaults to exact, while subtree explicitly includes descendants. Use namespace_list without parent selectors to discover every top-level root, or with exactly one of parent and parent_sequence to browse its descendants. namespace_delete defaults to dry_run=true; pass dry_run=false only after reviewing counts, and recursive=true only when the entire subtree must be removed. Prefer verified evidence and current versions. Use expected_version for every mutation, idempotency_key for safe retries, and memory_supersede or memory_refute instead of silently overwriting conclusions. memory_ingest_path reads paths on the local MCP device; the central service never reads client paths.`,
			Logger:       logger,
			PageSize:     100,
		},
	)

	addTools(server, handlers)

	return server
}

func addTools(server *mcp.Server, handlers *Handlers) {
	addTypedTool(server, tool("memory_put", "Create a versioned memory with evidence, scope and optional TTL.", false, false, false), handlers.putMemory)
	addTypedTool(server, tool("memory_patch", "Patch mutable memory fields using optimistic concurrency.", false, true, false), handlers.patchMemory)
	addTypedTool(server, tool("memory_get", "Read a current memory or historical version by ID.", true, true, false), handlers.getMemory)
	addTypedTool(server, tool("memory_search", "Recall relevant memories and source chunks by exact, substring, lexical, semantic or hybrid retrieval.", true, true, false), handlers.searchMemory)
	addTypedTool(server, tool("memory_list", "Browse memories by scope, type, tags, metadata, lifecycle and time filters without a query.", true, true, false), handlers.listMemory)
	addTypedTool(server, tool("namespace_list", "List every top-level namespace without a parent selector, or browse descendants below exactly one parent path or parent_sequence.", true, true, false), handlers.namespaceList)
	addTypedTool(server, tool("namespace_delete", "Preview or delete one namespace; recursive deletion also removes its complete subtree and matching local watches.", false, true, true), handlers.deleteNamespace)
	addTypedTool(server, tool("memory_delete", "Soft-delete a memory while preserving immutable revision history.", false, true, true), handlers.deleteMemory)
	addTypedTool(server, tool("memory_history", "List append-only revisions for a memory.", true, true, false), handlers.history)
	addTypedTool(server, tool("memory_restore", "Restore a historical snapshot as a new current version.", false, true, false), handlers.restoreMemory)
	addTypedTool(server, tool("memory_supersede", "Atomically create a replacement memory and supersede the target.", false, true, false), handlers.supersedeMemory)
	addTypedTool(server, tool("memory_refute", "Mark a memory refuted and optionally link a refuting memory with evidence.", false, true, true), handlers.refuteMemory)
	addTypedTool(server, tool("memory_touch", "Extend, set or clear a memory TTL using optimistic concurrency.", false, true, false), handlers.touchMemory)
	addTypedTool(server, tool("memory_pin", "Clear a memory expiration and preserve the TTL change in history.", false, true, false), handlers.pinMemory)
	addTypedTool(server, tool("memory_ingest_path", "Scan, hash, upload and optionally watch a local file or directory incrementally.", false, true, false), handlers.ingestPath)
	addTypedTool(server, tool("memory_ingest_status", "Read source and embedding state for one ingestion job.", true, true, false), handlers.ingestionStatus)
	addTypedTool(server, tool("memory_source_status", "Read current hash, generation, path, parser, TTL and embedding state for sources.", true, true, false), handlers.sourceStatus)
	addTypedTool(server, tool("memory_source_delete", "Delete only a server-side source index; never delete the client file.", false, true, true), handlers.deleteSource)
	addTypedTool(server, tool("memory_watch_list", "List active local filesystem watches and their last synchronization result.", true, true, false), handlers.watchList)
	addTypedTool(server, tool("memory_watch_stop", "Stop one active local filesystem watch.", false, true, false), handlers.watchStop)
	addTypedTool(server, tool("device_register", "Register this installation using transient hardware signals and persist its logical identity locally.", false, false, false), handlers.registerDevice)
	addTypedTool(server, tool("device_claim", "Explicitly bind the current installation to an existing logical device.", false, true, true), handlers.claimDevice)
	addTypedTool(server, tool("device_migrate", "Merge a source logical device into a canonical target without rewriting provenance.", false, true, true), handlers.migrateDevice)
	addTypedTool(server, tool("device_whoami", "Resolve the current installation, canonical logical device and workspace identity.", true, true, false), handlers.whoAmI)
	addTypedTool(server, tool("memory_health", "Check central PostgreSQL and embedding provider status.", true, true, false), handlers.health)
}

func addTypedTool[Input, Output any](
	server *mcp.Server,
	definition *mcp.Tool,
	handler mcp.ToolHandlerFor[Input, Output],
) {
	inputSchema, err := jsonschema.For[Input](nil)
	if err != nil {
		panic(fmt.Errorf("build input schema for %s: %w", definition.Name, err))
	}
	outputSchema, err := jsonschema.For[Output](nil)
	if err != nil {
		panic(fmt.Errorf("build output schema for %s: %w", definition.Name, err))
	}
	errorSchema, err := jsonschema.For[toolError](nil)
	if err != nil {
		panic(fmt.Errorf("build error schema for %s: %w", definition.Name, err))
	}

	repairRawJSONProperties(inputSchema)
	repairRawJSONProperties(outputSchema)
	relaxLocalInputDefaults(inputSchema)
	if definition.Name == "namespace_delete" {
		requireInputProperties(inputSchema, "reason")
	}
	applyInputSchemaConstraints(definition.Name, inputSchema)
	if definition.Name == "memory_supersede" {
		removeNestedNamespaceSelectorRequirements(inputSchema.Properties["replacement"])
	}
	applyNamespaceSelectorConstraint(definition.Name, inputSchema)
	definition.InputSchema = inputSchema
	definition.OutputSchema = &jsonschema.Schema{
		Type:  "object",
		OneOf: []*jsonschema.Schema{outputSchema, errorSchema},
	}

	wrappedHandler := func(
		ctx context.Context,
		request *mcp.CallToolRequest,
		input Input,
	) (*mcp.CallToolResult, any, error) {
		result, output, handlerErr := handler(ctx, request, input)
		if handlerErr == nil {
			return result, output, nil
		}

		errorResult, ok := structuredToolError(handlerErr)
		if ok {
			return errorResult, nil, nil
		}

		return nil, nil, handlerErr
	}

	mcp.AddTool[Input, any](server, definition, wrappedHandler)
}

func structuredToolError(err error) (*mcp.CallToolResult, bool) {
	var serviceErr *service.Error
	if !errors.As(err, &serviceErr) {
		return nil, false
	}

	payload := toolError{
		Code:    serviceErr.Code,
		Message: serviceErr.Message,
		Details: serviceErr.Details,
	}
	result := &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: payload.Code + ": " + payload.Message},
		},
		StructuredContent: payload,
	}
	result.SetError(err)

	return result, true
}

func relaxLocalInputDefaults(schema *jsonschema.Schema) {
	if schema == nil {
		return
	}

	required := schema.Required[:0]
	for _, name := range schema.Required {
		if name != "namespace" && name != "scope_type" && name != "type" {
			required = append(required, name)
		}
	}
	schema.Required = required

	for _, property := range schema.Properties {
		relaxLocalInputDefaults(property)
	}
	for _, definition := range schema.Defs {
		relaxLocalInputDefaults(definition)
	}
	for _, definition := range schema.Definitions {
		relaxLocalInputDefaults(definition)
	}
	relaxLocalInputDefaults(schema.Items)
	for _, item := range schema.PrefixItems {
		relaxLocalInputDefaults(item)
	}
	for _, item := range schema.ItemsArray {
		relaxLocalInputDefaults(item)
	}
	for _, item := range schema.AllOf {
		relaxLocalInputDefaults(item)
	}
	for _, item := range schema.AnyOf {
		relaxLocalInputDefaults(item)
	}
	for _, item := range schema.OneOf {
		relaxLocalInputDefaults(item)
	}
}

func removeNestedNamespaceSelectorRequirements(schema *jsonschema.Schema) {
	if schema == nil {
		return
	}

	removeRequiredProperties(schema, "namespace", "namespace_sequence")
	for _, property := range schema.Properties {
		removeNestedNamespaceSelectorRequirements(property)
	}
	for _, definition := range schema.Defs {
		removeNestedNamespaceSelectorRequirements(definition)
	}
	for _, definition := range schema.Definitions {
		removeNestedNamespaceSelectorRequirements(definition)
	}
	removeNestedNamespaceSelectorRequirements(schema.Items)
	for _, item := range schema.PrefixItems {
		removeNestedNamespaceSelectorRequirements(item)
	}
	for _, item := range schema.ItemsArray {
		removeNestedNamespaceSelectorRequirements(item)
	}
	for _, item := range schema.AllOf {
		removeNestedNamespaceSelectorRequirements(item)
	}
	for _, item := range schema.AnyOf {
		removeNestedNamespaceSelectorRequirements(item)
	}
}

func removeRequiredProperties(schema *jsonschema.Schema, names ...string) {
	if schema == nil {
		return
	}

	required := schema.Required[:0]
	for _, requiredName := range schema.Required {
		remove := false
		for _, name := range names {
			if requiredName == name {
				remove = true
				break
			}
		}
		if !remove {
			required = append(required, requiredName)
		}
	}
	schema.Required = required
}

func requireInputProperties(schema *jsonschema.Schema, names ...string) {
	if schema == nil {
		return
	}

	for _, name := range names {
		if _, exists := schema.Properties[name]; !exists {
			continue
		}

		required := false
		for _, requiredName := range schema.Required {
			if requiredName == name {
				required = true
				break
			}
		}
		if !required {
			schema.Required = append(schema.Required, name)
		}
	}
}

func applyInputSchemaConstraints(toolName string, schema *jsonschema.Schema) {
	setNamespacePropertyConstraints(schema, "namespace")
	setOptionalNamespacePropertyConstraints(schema, "parent", "cursor")
	setNumericPropertyMinimum(schema, "namespace_sequence", 0)
	setNumericPropertyMinimum(schema, "parent_sequence", 0)
	setPropertyEnum(schema, "scope_type", []string{
		domain.ScopeInstallation,
		domain.ScopeDevice,
		domain.ScopeWorkspace,
		domain.ScopeProject,
		domain.ScopeGlobal,
	})
	setPropertyEnum(schema, "scope_mode", []string{
		domain.SearchPreferLocal,
		domain.SearchLocalOnly,
		domain.SearchProjectOnly,
		domain.SearchAllDevices,
	})
	setPropertyEnum(schema, "retrieval_mode", []string{
		"hybrid",
		"exact",
		"substring",
		"lexical",
		"semantic",
	})
	setPropertyEnum(schema, "namespace_match", []string{
		domain.NamespaceMatchExact,
		domain.NamespaceMatchSubtree,
	})
	setPropertyEnum(schema, "detail_level", []string{"compact", "full"})
	setPropertyEnum(schema, "verification_state", []string{
		"unverified",
		"supported",
		"confirmed",
		"contested",
	})
	setArrayPropertyEnum(schema, "types", []string{
		"fact",
		"experiment",
		"hypothesis",
		"decision",
		"artifact",
		"procedure",
		"incident",
		"summary",
	})
	setArrayPropertyEnum(schema, "kinds", []string{"memory", "source_chunk"})
	setNumericPropertyRange(schema, "confidence", 0, 1)
	setNumericPropertyRange(schema, "min_relevance", 0, 1)

	switch toolName {
	case "memory_search":
		setNumericPropertyRange(schema, "limit", 1, 100)
		setNumericPropertyRange(schema, "candidate_limit", 10, 500)
	case "memory_list":
		setNumericPropertyRange(schema, "limit", 1, 100)
	case "namespace_list":
		setNumericPropertyRange(schema, "depth", 1, 16)
		setNumericPropertyRange(schema, "limit", 1, 200)
	case "namespace_delete":
		setPropertyDefault(schema, "recursive", json.RawMessage("false"))
		setPropertyDefault(schema, "dry_run", json.RawMessage("true"))
	case "memory_history", "memory_source_status":
		setNumericPropertyRange(schema, "limit", 1, 200)
	}

	switch toolName {
	case "memory_put", "memory_patch", "memory_supersede":
		setPropertyEnum(schema, "type", []string{
			"fact",
			"experiment",
			"hypothesis",
			"decision",
			"artifact",
			"procedure",
			"incident",
			"summary",
		})
	case "memory_ingest_path":
		setPropertyEnum(schema, "watch_mode", []string{"once", "sync", "watch"})
	}
}

func applyNamespaceSelectorConstraint(toolName string, schema *jsonschema.Schema) {
	if schema == nil {
		return
	}

	if toolName == "namespace_list" {
		forbidPropertyPair(schema, "parent", "parent_sequence")
		return
	}
	if _, hasNamespace := schema.Properties["namespace"]; !hasNamespace {
		return
	}
	if _, hasSequence := schema.Properties["namespace_sequence"]; !hasSequence {
		return
	}

	removeRequiredProperties(schema, "namespace", "namespace_sequence")
	schema.OneOf = append(schema.OneOf,
		requireOnlyProperty("namespace", "namespace_sequence"),
		requireOnlyProperty("namespace_sequence", "namespace"),
	)
}

func forbidPropertyPair(schema *jsonschema.Schema, first, second string) {
	schema.Not = &jsonschema.Schema{
		Required: []string{first, second},
	}
}

func requireOnlyProperty(required, forbidden string) *jsonschema.Schema {
	return &jsonschema.Schema{
		Required: []string{required},
		Not: &jsonschema.Schema{
			Required: []string{forbidden},
		},
	}
}

func setNamespacePropertyConstraints(schema *jsonschema.Schema, propertyNames ...string) {
	const pattern = `^[a-z0-9]([a-z0-9._-]*[a-z0-9])?(/[a-z0-9]([a-z0-9._-]*[a-z0-9])?)*$`
	maxLength := 128
	walkSchema(schema, func(current *jsonschema.Schema) {
		for _, propertyName := range propertyNames {
			property, exists := current.Properties[propertyName]
			if !exists {
				continue
			}
			property.Pattern = pattern
			property.MaxLength = &maxLength
		}
	})
}

func setOptionalNamespacePropertyConstraints(schema *jsonschema.Schema, propertyNames ...string) {
	const pattern = `^$|^[a-z0-9]([a-z0-9._-]*[a-z0-9])?(/[a-z0-9]([a-z0-9._-]*[a-z0-9])?)*$`
	maxLength := 128
	walkSchema(schema, func(current *jsonschema.Schema) {
		for _, propertyName := range propertyNames {
			property, exists := current.Properties[propertyName]
			if !exists {
				continue
			}
			property.Pattern = pattern
			property.MaxLength = &maxLength
		}
	})
}

func setPropertyDefault(schema *jsonschema.Schema, propertyName string, value json.RawMessage) {
	walkSchema(schema, func(current *jsonschema.Schema) {
		property, exists := current.Properties[propertyName]
		if exists {
			property.Default = value
		}
	})
}

func setPropertyEnum(schema *jsonschema.Schema, propertyName string, values []string) {
	walkSchema(schema, func(current *jsonschema.Schema) {
		property, exists := current.Properties[propertyName]
		if !exists {
			return
		}
		property.Enum = stringEnum(values)
		if schemaAllowsNull(property) {
			property.Enum = append(property.Enum, nil)
		}
	})
}

func schemaAllowsNull(schema *jsonschema.Schema) bool {
	for _, schemaType := range schema.Types {
		if schemaType == "null" {
			return true
		}
	}

	return false
}

func setArrayPropertyEnum(schema *jsonschema.Schema, propertyName string, values []string) {
	walkSchema(schema, func(current *jsonschema.Schema) {
		property, exists := current.Properties[propertyName]
		if !exists || property.Items == nil {
			return
		}
		property.Items.Enum = stringEnum(values)
	})
}

func setNumericPropertyRange(schema *jsonschema.Schema, propertyName string, minimum, maximum float64) {
	walkSchema(schema, func(current *jsonschema.Schema) {
		property, exists := current.Properties[propertyName]
		if !exists {
			return
		}
		property.Minimum = &minimum
		property.Maximum = &maximum
	})
}

func setNumericPropertyMinimum(schema *jsonschema.Schema, propertyName string, minimum float64) {
	walkSchema(schema, func(current *jsonschema.Schema) {
		property, exists := current.Properties[propertyName]
		if exists {
			property.Minimum = &minimum
		}
	})
}

func stringEnum(values []string) []any {
	result := make([]any, len(values))
	for index, value := range values {
		result[index] = value
	}

	return result
}

func walkSchema(schema *jsonschema.Schema, visit func(*jsonschema.Schema)) {
	if schema == nil {
		return
	}
	visit(schema)

	for _, property := range schema.Properties {
		walkSchema(property, visit)
	}
	for _, definition := range schema.Defs {
		walkSchema(definition, visit)
	}
	for _, definition := range schema.Definitions {
		walkSchema(definition, visit)
	}
	walkSchema(schema.Items, visit)
	for _, item := range schema.PrefixItems {
		walkSchema(item, visit)
	}
	for _, item := range schema.ItemsArray {
		walkSchema(item, visit)
	}
	for _, item := range schema.AllOf {
		walkSchema(item, visit)
	}
	for _, item := range schema.AnyOf {
		walkSchema(item, visit)
	}
	for _, item := range schema.OneOf {
		walkSchema(item, visit)
	}
}

func repairRawJSONProperties(schema *jsonschema.Schema) {
	if schema == nil {
		return
	}

	for name, property := range schema.Properties {
		if isRawJSONSchema(property) {
			switch name {
			case "metadata", "metadata_merge", "before_snapshot", "after_snapshot":
				schema.Properties[name] = nullableJSONObject(property.Description)
				continue
			case "evidence":
				schema.Properties[name] = &jsonschema.Schema{
					Types:       []string{"null", "array"},
					Description: property.Description,
					Items:       &jsonschema.Schema{},
				}
				continue
			case "source_range":
				schema.Properties[name] = nullableSourceRange(property.Description)
				continue
			}
		}

		repairRawJSONProperties(property)
	}
	for _, definition := range schema.Defs {
		repairRawJSONProperties(definition)
	}
	for _, definition := range schema.Definitions {
		repairRawJSONProperties(definition)
	}
	repairRawJSONProperties(schema.Items)
	for _, item := range schema.PrefixItems {
		repairRawJSONProperties(item)
	}
	for _, item := range schema.ItemsArray {
		repairRawJSONProperties(item)
	}
	for _, item := range schema.AllOf {
		repairRawJSONProperties(item)
	}
	for _, item := range schema.AnyOf {
		repairRawJSONProperties(item)
	}
	for _, item := range schema.OneOf {
		repairRawJSONProperties(item)
	}
}

func isRawJSONSchema(schema *jsonschema.Schema) bool {
	if schema == nil || schema.Items == nil || schema.Items.Type != "integer" {
		return false
	}
	if schema.Type == "array" {
		return true
	}

	for _, schemaType := range schema.Types {
		if schemaType == "array" {
			return true
		}
	}

	return false
}

func nullableJSONObject(description string) *jsonschema.Schema {
	return &jsonschema.Schema{
		Types:                []string{"null", "object"},
		Description:          description,
		AdditionalProperties: &jsonschema.Schema{},
	}
}

func nullableSourceRange(description string) *jsonschema.Schema {
	return &jsonschema.Schema{
		Types:       []string{"null", "object"},
		Description: description,
		Properties: map[string]*jsonschema.Schema{
			"start_line": {Type: "integer"},
			"end_line":   {Type: "integer"},
			"start_char": {Type: "integer"},
			"end_char":   {Type: "integer"},
		},
	}
}

func tool(name, description string, readOnly, idempotent, destructive bool) *mcp.Tool {
	return &mcp.Tool{
		Name:        name,
		Description: description,
		Annotations: &mcp.ToolAnnotations{
			Title:           name,
			ReadOnlyHint:    readOnly,
			IdempotentHint:  idempotent,
			DestructiveHint: boolPointer(destructive),
			OpenWorldHint:   &boolFalse,
		},
	}
}

func boolPointer(value bool) *bool {
	if value {
		return &boolTrue
	}

	return &boolFalse
}

func (h *Handlers) putMemory(ctx context.Context, _ *mcp.CallToolRequest, input service.PutMemoryInput) (*mcp.CallToolResult, domain.Memory, error) {
	result, err := h.backend.PutMemory(ctx, input)
	return nil, result, err
}

func (h *Handlers) patchMemory(ctx context.Context, _ *mcp.CallToolRequest, input service.PatchMemoryInput) (*mcp.CallToolResult, domain.Memory, error) {
	result, err := h.backend.PatchMemory(ctx, input)
	return nil, result, err
}

func (h *Handlers) getMemory(ctx context.Context, _ *mcp.CallToolRequest, input service.GetMemoryInput) (*mcp.CallToolResult, domain.Memory, error) {
	result, err := h.backend.GetMemory(ctx, input)
	return nil, result, err
}

func (h *Handlers) searchMemory(ctx context.Context, _ *mcp.CallToolRequest, input service.SearchMemoryInput) (*mcp.CallToolResult, domain.SearchResponse, error) {
	if input.DetailLevel == "" {
		input.DetailLevel = domain.SearchDetailCompact
	}
	result, err := h.backend.SearchMemory(ctx, input)
	return nil, result, err
}

func (h *Handlers) listMemory(ctx context.Context, _ *mcp.CallToolRequest, input service.ListMemoryInput) (*mcp.CallToolResult, domain.MemoryListResponse, error) {
	if input.DetailLevel == "" {
		input.DetailLevel = domain.SearchDetailCompact
	}
	result, err := h.backend.ListMemory(ctx, input)
	return nil, result, err
}

func (h *Handlers) namespaceList(ctx context.Context, _ *mcp.CallToolRequest, input service.NamespaceListInput) (*mcp.CallToolResult, domain.NamespaceListResponse, error) {
	result, err := h.backend.ListNamespaces(ctx, input)
	return nil, result, err
}

func (h *Handlers) deleteNamespace(ctx context.Context, _ *mcp.CallToolRequest, input service.NamespaceDeleteInput) (*mcp.CallToolResult, domain.NamespaceDeleteResult, error) {
	if h.ingestionManager == nil {
		result, err := h.backend.DeleteNamespace(ctx, input)
		return nil, result, err
	}

	if input.ShouldDryRun() {
		result, err := h.backend.DeleteNamespace(ctx, input)
		if err != nil {
			return nil, domain.NamespaceDeleteResult{}, err
		}
		attachAffectedWatches(&result, namespaceWatchIDs(
			h.ingestionManager.List(),
			result.Namespace,
			input.Recursive,
		))

		return nil, result, nil
	}

	previewInput := input
	previewInput.DryRun = &boolTrue
	preview, err := h.backend.DeleteNamespace(ctx, previewInput)
	if err != nil {
		return nil, domain.NamespaceDeleteResult{}, err
	}
	if preview.RequiresRecursive {
		serviceErr := service.NewError(
			service.CodeFailedPrecondition,
			"namespace has active descendants; retry with recursive=true",
		)
		serviceErr.Details = map[string]any{
			"namespace":             preview.Namespace,
			"descendant_namespaces": preview.Counts.DescendantNamespaces,
			"dry_run":               true,
		}

		return nil, domain.NamespaceDeleteResult{}, serviceErr
	}
	result, err := h.backend.DeleteNamespace(ctx, input)
	if err != nil {
		return nil, domain.NamespaceDeleteResult{}, err
	}

	watchIDs := namespaceWatchIDs(h.ingestionManager.List(), result.Namespace, input.Recursive)
	stoppedWatchIDs := make([]string, 0, len(watchIDs))
	for _, watchID := range watchIDs {
		if err := h.ingestionManager.Stop(watchID); err != nil {
			if errors.Is(err, ingest.ErrWatchNotFound) {
				stoppedWatchIDs = append(stoppedWatchIDs, watchID)
				continue
			}
			result.Warnings = append(
				result.Warnings,
				fmt.Sprintf("local watch %s could not be stopped: %v", watchID, err),
			)
			continue
		}
		stoppedWatchIDs = append(stoppedWatchIDs, watchID)
	}
	attachAffectedWatches(&result, stoppedWatchIDs)

	return nil, result, nil
}

func namespaceWatchIDs(watches []ingest.WatchInfo, namespace string, recursive bool) []string {
	target := strings.ToLower(strings.TrimSpace(namespace))
	result := make([]string, 0)
	for _, watch := range watches {
		candidate := strings.ToLower(strings.TrimSpace(watch.Namespace))
		matches := candidate == target
		if recursive && strings.HasPrefix(candidate, target+"/") {
			matches = true
		}
		if matches {
			result = append(result, watch.ID)
		}
	}

	return result
}

func attachAffectedWatches(result *domain.NamespaceDeleteResult, watchIDs []string) {
	result.AffectedWatchIDs = watchIDs
}

func (h *Handlers) deleteMemory(ctx context.Context, _ *mcp.CallToolRequest, input service.DeleteMemoryInput) (*mcp.CallToolResult, domain.Memory, error) {
	result, err := h.backend.DeleteMemory(ctx, input)
	return nil, result, err
}

func (h *Handlers) history(ctx context.Context, _ *mcp.CallToolRequest, input service.HistoryInput) (*mcp.CallToolResult, HistoryResult, error) {
	result, err := h.backend.History(ctx, input)
	return nil, HistoryResult{Revisions: result}, err
}

func (h *Handlers) restoreMemory(ctx context.Context, _ *mcp.CallToolRequest, input service.RestoreMemoryInput) (*mcp.CallToolResult, domain.Memory, error) {
	result, err := h.backend.RestoreMemory(ctx, input)
	return nil, result, err
}

func (h *Handlers) supersedeMemory(ctx context.Context, _ *mcp.CallToolRequest, input service.SupersedeMemoryInput) (*mcp.CallToolResult, domain.Memory, error) {
	result, err := h.backend.SupersedeMemory(ctx, input)
	return nil, result, err
}

func (h *Handlers) refuteMemory(ctx context.Context, _ *mcp.CallToolRequest, input service.RefuteMemoryInput) (*mcp.CallToolResult, domain.Memory, error) {
	result, err := h.backend.RefuteMemory(ctx, input)
	return nil, result, err
}

func (h *Handlers) touchMemory(ctx context.Context, _ *mcp.CallToolRequest, input service.TouchMemoryInput) (*mcp.CallToolResult, domain.Memory, error) {
	result, err := h.backend.TouchMemory(ctx, input)
	return nil, result, err
}

func (h *Handlers) pinMemory(ctx context.Context, _ *mcp.CallToolRequest, input PinMemoryInput) (*mcp.CallToolResult, domain.Memory, error) {
	result, err := h.backend.TouchMemory(ctx, service.TouchMemoryInput{
		Namespace:         input.Namespace,
		NamespaceSequence: input.NamespaceSequence,
		ID:                input.MemoryID,
		ExpectedVersion:   input.ExpectedVersion,
		Pin:               true,
		Reason:            input.Reason,
		Actor:             input.Actor,
		IdempotencyKey:    input.IdempotencyKey,
	})
	return nil, result, err
}

func (h *Handlers) ingestPath(ctx context.Context, _ *mcp.CallToolRequest, input IngestPathInput) (*mcp.CallToolResult, domain.IngestionSummary, error) {
	if h.ingestionManager == nil {
		return nil, domain.IngestionSummary{}, service.NewError(service.CodeUnavailable, "local path ingestion is available only from the stdio MCP bridge")
	}
	watchMode := strings.ToLower(strings.TrimSpace(input.WatchMode))
	if watchMode == "" {
		watchMode = "once"
	}
	if watchMode != "once" && watchMode != "sync" && watchMode != "watch" {
		return nil, domain.IngestionSummary{}, service.NewError(service.CodeInvalidArgument, "watch_mode must be once, sync, or watch")
	}
	pruneMissing := watchMode != "once"
	if input.PruneMissing != nil {
		pruneMissing = *input.PruneMissing
	}
	recursive := true
	if input.Recursive != nil {
		recursive = *input.Recursive
	}
	namespace, err := h.resolveLocalNamespace(ctx, input.Namespace, input.NamespaceSequence)
	if err != nil {
		return nil, domain.IngestionSummary{}, err
	}
	syncInput := service.SyncSourcesInput{
		Namespace:    namespace,
		ScopeType:    input.ScopeType,
		ScopeID:      input.ScopeID,
		RootPath:     input.Path,
		Recursive:    recursive,
		Include:      input.Include,
		Exclude:      input.Exclude,
		WatchMode:    watchMode,
		Parser:       input.Parser,
		TTLSeconds:   input.TTLSeconds,
		ExpiresAt:    input.ExpiresAt,
		PruneMissing: pruneMissing,
	}
	if watchMode == "watch" {
		watch, err := h.ingestionManager.Watch(ctx, syncInput)
		if err != nil {
			return nil, domain.IngestionSummary{}, err
		}
		return nil, watch.LastSummary, nil
	}

	result, err := h.ingestionManager.Sync(ctx, syncInput)
	return nil, result, err
}

func (h *Handlers) resolveLocalNamespace(ctx context.Context, namespace string, sequence *int64) (string, error) {
	namespace = strings.ToLower(strings.TrimSpace(namespace))
	if sequence == nil {
		return namespace, nil
	}

	result, err := h.backend.ListNamespaces(ctx, service.NamespaceListInput{
		ParentSequence: sequence,
		Depth:          1,
		Limit:          1,
	})
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(result.Parent) == "" {
		return "", service.NewError(service.CodeInternal, "namespace sequence resolved to an empty path")
	}

	return strings.ToLower(strings.TrimSpace(result.Parent)), nil
}

func (h *Handlers) ingestionStatus(ctx context.Context, _ *mcp.CallToolRequest, input IngestionStatusInput) (*mcp.CallToolResult, domain.SourceStatus, error) {
	result, err := h.backend.SourceStatus(ctx, service.SourceStatusInput{
		Namespace:         input.Namespace,
		NamespaceSequence: input.NamespaceSequence,
		IngestionID:       input.IngestionID,
	})
	return nil, result, err
}

func (h *Handlers) sourceStatus(ctx context.Context, _ *mcp.CallToolRequest, input service.SourceStatusInput) (*mcp.CallToolResult, domain.SourceStatus, error) {
	result, err := h.backend.SourceStatus(ctx, input)
	return nil, result, err
}

func (h *Handlers) deleteSource(ctx context.Context, _ *mcp.CallToolRequest, input service.DeleteSourceInput) (*mcp.CallToolResult, domain.Source, error) {
	result, err := h.backend.DeleteSource(ctx, input)
	return nil, result, err
}

func (h *Handlers) watchList(_ context.Context, _ *mcp.CallToolRequest, _ WatchListInput) (*mcp.CallToolResult, WatchListResult, error) {
	if h.ingestionManager == nil {
		return nil, WatchListResult{}, service.NewError(service.CodeUnavailable, "local watches are available only from the stdio MCP bridge")
	}

	return nil, WatchListResult{Watches: h.ingestionManager.List()}, nil
}

func (h *Handlers) watchStop(_ context.Context, _ *mcp.CallToolRequest, input WatchStopInput) (*mcp.CallToolResult, ingest.WatchInfo, error) {
	if h.ingestionManager == nil {
		return nil, ingest.WatchInfo{}, service.NewError(service.CodeUnavailable, "local watches are available only from the stdio MCP bridge")
	}
	for _, watch := range h.ingestionManager.List() {
		if watch.ID != input.WatchID {
			continue
		}
		if err := h.ingestionManager.Stop(input.WatchID); err != nil {
			return nil, ingest.WatchInfo{}, err
		}
		return nil, watch, nil
	}

	return nil, ingest.WatchInfo{}, service.NewError(service.CodeNotFound, "watch not found: "+input.WatchID)
}

func (h *Handlers) registerDevice(ctx context.Context, _ *mcp.CallToolRequest, input service.RegisterDeviceInput) (*mcp.CallToolResult, domain.RegistrationResult, error) {
	result, err := h.backend.RegisterDevice(ctx, input)
	return nil, result, err
}

func (h *Handlers) claimDevice(ctx context.Context, _ *mcp.CallToolRequest, input service.ClaimDeviceInput) (*mcp.CallToolResult, service.WhoAmIResult, error) {
	result, err := h.backend.ClaimDevice(ctx, input)
	return nil, result, err
}

func (h *Handlers) migrateDevice(ctx context.Context, _ *mcp.CallToolRequest, input service.MigrateDeviceInput) (*mcp.CallToolResult, service.WhoAmIResult, error) {
	result, err := h.backend.MigrateDevice(ctx, input)
	return nil, result, err
}

func (h *Handlers) whoAmI(ctx context.Context, _ *mcp.CallToolRequest, input service.WhoAmIInput) (*mcp.CallToolResult, service.WhoAmIResult, error) {
	result, err := h.backend.WhoAmI(ctx, input)
	return nil, result, err
}

func (h *Handlers) health(ctx context.Context, _ *mcp.CallToolRequest, _ HealthInput) (*mcp.CallToolResult, service.HealthResult, error) {
	result, err := h.backend.Health(ctx)
	return nil, result, err
}
