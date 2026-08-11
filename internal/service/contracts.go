package service

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Alhamdulillah-R/memory-recall-coin/internal/domain"
)

// PutMemoryInput creates one versioned memory.
type PutMemoryInput struct {
	Namespace         string                `json:"namespace" jsonschema:"slash-separated namespace path; may use configured workspace default when omitted"`
	ID                string                `json:"id,omitempty" jsonschema:"optional stable memory ID; generated when omitted"`
	ScopeType         string                `json:"scope_type" jsonschema:"installation, device, workspace, project, or global"`
	ScopeID           string                `json:"scope_id,omitempty" jsonschema:"scope identifier; inferred from caller identity when omitted"`
	Type              string                `json:"type" jsonschema:"fact, experiment, hypothesis, decision, artifact, procedure, incident, or summary"`
	Title             string                `json:"title"`
	Content           string                `json:"content"`
	Metadata          json.RawMessage       `json:"metadata,omitempty" jsonschema:"arbitrary JSON object"`
	Tags              []string              `json:"tags,omitempty"`
	VerificationState string                `json:"verification_state,omitempty" jsonschema:"unverified, supported, confirmed, or contested"`
	Confidence        *float64              `json:"confidence,omitempty" jsonschema:"confidence from 0 to 1"`
	Evidence          json.RawMessage       `json:"evidence,omitempty" jsonschema:"JSON array of evidence records"`
	SourceID          string                `json:"source_id,omitempty"`
	SourcePath        string                `json:"source_path,omitempty"`
	SourceHash        string                `json:"source_hash,omitempty"`
	SourceRange       json.RawMessage       `json:"source_range,omitempty"`
	SupersedesID      string                `json:"supersedes_id,omitempty"`
	ObservedAt        *time.Time            `json:"observed_at,omitempty"`
	TTLSeconds        *int64                `json:"ttl_seconds,omitempty" jsonschema:"relative TTL in seconds; mutually exclusive with expires_at"`
	ExpiresAt         *time.Time            `json:"expires_at,omitempty"`
	CreatedBy         string                `json:"created_by,omitempty"`
	IdempotencyKey    string                `json:"idempotency_key,omitempty" jsonschema:"stable key for safe write retry"`
	Caller            domain.CallerIdentity `json:"-"`
}

// PatchMemoryInput updates mutable fields with optimistic concurrency.
type PatchMemoryInput struct {
	Namespace         string                `json:"namespace"`
	ID                string                `json:"memory_id"`
	ExpectedVersion   int64                 `json:"expected_version"`
	Title             *string               `json:"title,omitempty"`
	Content           *string               `json:"content,omitempty"`
	Type              *string               `json:"type,omitempty"`
	MetadataMerge     json.RawMessage       `json:"metadata_merge,omitempty" jsonschema:"JSON object merged into current metadata; null values delete keys"`
	ReplaceTags       *[]string             `json:"replace_tags,omitempty"`
	VerificationState *string               `json:"verification_state,omitempty"`
	Confidence        *float64              `json:"confidence,omitempty"`
	Evidence          json.RawMessage       `json:"evidence,omitempty"`
	SourceID          *string               `json:"source_id,omitempty"`
	SourcePath        *string               `json:"source_path,omitempty"`
	SourceHash        *string               `json:"source_hash,omitempty"`
	SourceRange       json.RawMessage       `json:"source_range,omitempty"`
	ObservedAt        *time.Time            `json:"observed_at,omitempty"`
	TTLSeconds        *int64                `json:"ttl_seconds,omitempty"`
	ExpiresAt         *time.Time            `json:"expires_at,omitempty"`
	ClearExpiresAt    bool                  `json:"clear_expires_at,omitempty"`
	Reason            string                `json:"reason,omitempty"`
	UpdatedBy         string                `json:"updated_by,omitempty"`
	IdempotencyKey    string                `json:"idempotency_key,omitempty"`
	Caller            domain.CallerIdentity `json:"-"`
}

// GetMemoryInput reads the current memory or a historical version.
type GetMemoryInput struct {
	Namespace         string                `json:"namespace"`
	ID                string                `json:"memory_id"`
	Version           *int64                `json:"version,omitempty"`
	IncludeExpired    bool                  `json:"include_expired,omitempty"`
	IncludeRefuted    bool                  `json:"include_refuted,omitempty"`
	IncludeSuperseded bool                  `json:"include_superseded,omitempty"`
	IncludeDeleted    bool                  `json:"include_deleted,omitempty"`
	Caller            domain.CallerIdentity `json:"-"`
}

// SearchMemoryInput selects retrieval channels and filters.
type SearchMemoryInput struct {
	Namespace         string                `json:"namespace"`
	NamespaceMatch    string                `json:"namespace_match,omitempty" jsonschema:"exact or subtree; default exact"`
	Query             string                `json:"query"`
	RetrievalMode     string                `json:"retrieval_mode,omitempty" jsonschema:"hybrid, exact, substring, lexical, or semantic; default hybrid"`
	ScopeMode         string                `json:"scope_mode,omitempty" jsonschema:"prefer_local, local_only, project_only, or all_devices"`
	DetailLevel       string                `json:"detail_level,omitempty" jsonschema:"compact or full; default compact"`
	MinRelevance      *float64              `json:"min_relevance,omitempty" jsonschema:"minimum returned score.relevance from 0 to 1"`
	Kinds             []string              `json:"kinds,omitempty" jsonschema:"result kinds: memory or source_chunk"`
	TagsAny           []string              `json:"tags_any,omitempty"`
	TagsAll           []string              `json:"tags_all,omitempty"`
	MetadataContains  map[string]any        `json:"metadata_contains,omitempty"`
	Types             []string              `json:"types,omitempty"`
	SourcePath        string                `json:"source_path,omitempty"`
	CreatedAfter      *time.Time            `json:"created_after,omitempty"`
	CreatedBefore     *time.Time            `json:"created_before,omitempty"`
	UpdatedAfter      *time.Time            `json:"updated_after,omitempty"`
	UpdatedBefore     *time.Time            `json:"updated_before,omitempty"`
	ObservedAfter     *time.Time            `json:"observed_after,omitempty"`
	ObservedBefore    *time.Time            `json:"observed_before,omitempty"`
	IncludeExpired    bool                  `json:"include_expired,omitempty"`
	IncludeRefuted    bool                  `json:"include_refuted,omitempty"`
	IncludeSuperseded bool                  `json:"include_superseded,omitempty"`
	IncludeDeleted    bool                  `json:"include_deleted,omitempty"`
	Limit             int                   `json:"limit,omitempty" jsonschema:"maximum results from 1 to 100; default 10"`
	CandidateLimit    int                   `json:"candidate_limit,omitempty" jsonschema:"per-channel candidate limit from 10 to 500; default 100"`
	Caller            domain.CallerIdentity `json:"-"`
}

// ListMemoryInput browses memories using filters without requiring a search query.
type ListMemoryInput struct {
	Namespace         string                `json:"namespace"`
	NamespaceMatch    string                `json:"namespace_match,omitempty" jsonschema:"exact or subtree; default exact"`
	ScopeMode         string                `json:"scope_mode,omitempty" jsonschema:"prefer_local, local_only, project_only, or all_devices"`
	DetailLevel       string                `json:"detail_level,omitempty" jsonschema:"compact or full; default compact"`
	TagsAny           []string              `json:"tags_any,omitempty"`
	TagsAll           []string              `json:"tags_all,omitempty"`
	MetadataContains  map[string]any        `json:"metadata_contains,omitempty"`
	Types             []string              `json:"types,omitempty"`
	SourcePath        string                `json:"source_path,omitempty"`
	CreatedAfter      *time.Time            `json:"created_after,omitempty"`
	CreatedBefore     *time.Time            `json:"created_before,omitempty"`
	UpdatedAfter      *time.Time            `json:"updated_after,omitempty"`
	UpdatedBefore     *time.Time            `json:"updated_before,omitempty"`
	ObservedAfter     *time.Time            `json:"observed_after,omitempty"`
	ObservedBefore    *time.Time            `json:"observed_before,omitempty"`
	IncludeExpired    bool                  `json:"include_expired,omitempty"`
	IncludeRefuted    bool                  `json:"include_refuted,omitempty"`
	IncludeSuperseded bool                  `json:"include_superseded,omitempty"`
	IncludeDeleted    bool                  `json:"include_deleted,omitempty"`
	Limit             int                   `json:"limit,omitempty" jsonschema:"maximum results from 1 to 100; default 10"`
	Caller            domain.CallerIdentity `json:"-"`
}

// SearchInput converts a filter-only listing request into the shared retrieval contract.
func (input ListMemoryInput) SearchInput() SearchMemoryInput {
	return SearchMemoryInput{
		Namespace:         input.Namespace,
		NamespaceMatch:    input.NamespaceMatch,
		RetrievalMode:     "list",
		ScopeMode:         input.ScopeMode,
		DetailLevel:       input.DetailLevel,
		Kinds:             []string{"memory"},
		TagsAny:           input.TagsAny,
		TagsAll:           input.TagsAll,
		MetadataContains:  input.MetadataContains,
		Types:             input.Types,
		SourcePath:        input.SourcePath,
		CreatedAfter:      input.CreatedAfter,
		CreatedBefore:     input.CreatedBefore,
		UpdatedAfter:      input.UpdatedAfter,
		UpdatedBefore:     input.UpdatedBefore,
		ObservedAfter:     input.ObservedAfter,
		ObservedBefore:    input.ObservedBefore,
		IncludeExpired:    input.IncludeExpired,
		IncludeRefuted:    input.IncludeRefuted,
		IncludeSuperseded: input.IncludeSuperseded,
		IncludeDeleted:    input.IncludeDeleted,
		Limit:             input.Limit,
		CandidateLimit:    input.Limit,
		Caller:            input.Caller,
	}
}

// NamespaceListInput browses the namespace tree below one parent path.
type NamespaceListInput struct {
	Parent         string                `json:"parent,omitempty" jsonschema:"parent namespace; configured caller namespace when omitted"`
	Depth          int                   `json:"depth,omitempty" jsonschema:"maximum descendant depth from 1 to 16; default 1"`
	IncludeDeleted bool                  `json:"include_deleted,omitempty"`
	Limit          int                   `json:"limit,omitempty" jsonschema:"maximum namespaces from 1 to 200; default 100"`
	Cursor         string                `json:"cursor,omitempty"`
	Caller         domain.CallerIdentity `json:"-"`
}

// NamespaceDeleteInput previews or deletes one namespace and optionally its subtree.
type NamespaceDeleteInput struct {
	Namespace string                `json:"namespace" jsonschema:"required target namespace path; no configured default is applied"`
	Recursive bool                  `json:"recursive,omitempty" jsonschema:"include every descendant namespace; default false"`
	DryRun    *bool                 `json:"dry_run,omitempty" jsonschema:"preview affected records without deleting; default true"`
	Reason    string                `json:"reason" jsonschema:"required audit reason"`
	Caller    domain.CallerIdentity `json:"-"`
}

// ShouldDryRun resolves the safe default for namespace deletion.
func (input NamespaceDeleteInput) ShouldDryRun() bool {
	return input.DryRun == nil || *input.DryRun
}

// DeleteMemoryInput soft-deletes a memory using optimistic concurrency.
type DeleteMemoryInput struct {
	Namespace       string                `json:"namespace"`
	ID              string                `json:"memory_id"`
	ExpectedVersion int64                 `json:"expected_version"`
	Reason          string                `json:"reason"`
	Actor           string                `json:"actor,omitempty"`
	IdempotencyKey  string                `json:"idempotency_key,omitempty"`
	Caller          domain.CallerIdentity `json:"-"`
}

// HistoryInput lists immutable revisions for one memory.
type HistoryInput struct {
	Namespace string                `json:"namespace"`
	ID        string                `json:"memory_id"`
	Limit     int                   `json:"limit,omitempty"`
	BeforeID  int64                 `json:"before_revision_id,omitempty"`
	Caller    domain.CallerIdentity `json:"-"`
}

// RestoreMemoryInput restores a snapshot as a new current version.
type RestoreMemoryInput struct {
	Namespace       string                `json:"namespace"`
	ID              string                `json:"memory_id"`
	RevisionVersion int64                 `json:"revision_version"`
	ExpectedVersion int64                 `json:"expected_version"`
	Reason          string                `json:"reason"`
	Actor           string                `json:"actor,omitempty"`
	IdempotencyKey  string                `json:"idempotency_key,omitempty"`
	Caller          domain.CallerIdentity `json:"-"`
}

// SupersedeMemoryInput atomically creates a replacement and supersedes the target.
type SupersedeMemoryInput struct {
	Namespace       string                `json:"namespace"`
	TargetID        string                `json:"target_memory_id"`
	ExpectedVersion int64                 `json:"expected_version"`
	Replacement     PutMemoryInput        `json:"replacement"`
	Reason          string                `json:"reason"`
	Actor           string                `json:"actor,omitempty"`
	IdempotencyKey  string                `json:"idempotency_key,omitempty"`
	Caller          domain.CallerIdentity `json:"-"`
}

// RefuteMemoryInput marks one memory refuted and links a refuting memory when supplied.
type RefuteMemoryInput struct {
	Namespace        string                `json:"namespace"`
	TargetID         string                `json:"target_memory_id"`
	ExpectedVersion  int64                 `json:"expected_version"`
	RefutingMemoryID string                `json:"refuting_memory_id,omitempty"`
	Reason           string                `json:"reason"`
	Evidence         json.RawMessage       `json:"evidence,omitempty"`
	Actor            string                `json:"actor,omitempty"`
	IdempotencyKey   string                `json:"idempotency_key,omitempty"`
	Caller           domain.CallerIdentity `json:"-"`
}

// TouchMemoryInput extends, replaces or clears expiration with optimistic concurrency.
type TouchMemoryInput struct {
	Namespace       string                `json:"namespace"`
	ID              string                `json:"memory_id"`
	ExpectedVersion int64                 `json:"expected_version"`
	ExtendBySeconds *int64                `json:"extend_by_seconds,omitempty"`
	ExpiresAt       *time.Time            `json:"expires_at,omitempty"`
	Pin             bool                  `json:"pin,omitempty" jsonschema:"clear expiration when true"`
	Reason          string                `json:"reason,omitempty"`
	Actor           string                `json:"actor,omitempty"`
	IdempotencyKey  string                `json:"idempotency_key,omitempty"`
	Caller          domain.CallerIdentity `json:"-"`
}

// RegisterDeviceInput creates an installation and resolves its logical device.
type RegisterDeviceInput struct {
	InstallationCode    string                  `json:"installation_code,omitempty"`
	RequestedDeviceCode string                  `json:"requested_device_code,omitempty"`
	DisplayName         string                  `json:"display_name,omitempty"`
	Hostname            string                  `json:"hostname,omitempty"`
	TailnetIdentity     string                  `json:"tailnet_identity,omitempty"`
	Signals             []domain.HardwareSignal `json:"signals,omitempty"`
	Actor               string                  `json:"actor,omitempty"`
	Caller              domain.CallerIdentity   `json:"-"`
}

// ClaimDeviceInput explicitly binds the current installation to an existing logical device.
type ClaimDeviceInput struct {
	InstallationCode string                `json:"installation_code"`
	TargetDeviceCode string                `json:"target_device_code"`
	Confirm          bool                  `json:"confirm"`
	Reason           string                `json:"reason"`
	Actor            string                `json:"actor,omitempty"`
	Caller           domain.CallerIdentity `json:"-"`
}

// MigrateDeviceInput merges a source device identity into a canonical target identity.
type MigrateDeviceInput struct {
	SourceDeviceCode string                `json:"source_device_code"`
	TargetDeviceCode string                `json:"target_device_code"`
	Confirm          bool                  `json:"confirm"`
	Reason           string                `json:"reason"`
	Actor            string                `json:"actor,omitempty"`
	Caller           domain.CallerIdentity `json:"-"`
}

// WhoAmIInput resolves the current installation and canonical device.
type WhoAmIInput struct {
	InstallationCode string                `json:"installation_code,omitempty"`
	Caller           domain.CallerIdentity `json:"-"`
}

// WhoAmIResult reports verified identity state.
type WhoAmIResult struct {
	Registered   bool                  `json:"registered"`
	Device       *domain.Device        `json:"device,omitempty"`
	Installation *domain.Installation  `json:"installation,omitempty"`
	Caller       domain.CallerIdentity `json:"caller"`
}

// SyncSourcesInput uploads a complete local path manifest to the central index.
type SyncSourcesInput struct {
	Namespace    string                `json:"namespace"`
	ScopeType    string                `json:"scope_type"`
	ScopeID      string                `json:"scope_id,omitempty"`
	RootPath     string                `json:"root_path"`
	Recursive    bool                  `json:"recursive"`
	Include      []string              `json:"include,omitempty"`
	Exclude      []string              `json:"exclude,omitempty"`
	WatchMode    string                `json:"watch_mode"`
	Parser       string                `json:"parser"`
	TTLSeconds   *int64                `json:"ttl_seconds,omitempty"`
	ExpiresAt    *time.Time            `json:"expires_at,omitempty"`
	PruneMissing bool                  `json:"prune_missing"`
	Files        []domain.IngestedFile `json:"files"`
	Caller       domain.CallerIdentity `json:"-"`
}

// SourceStatusInput queries source and embedding state.
type SourceStatusInput struct {
	Namespace      string                `json:"namespace"`
	NamespaceMatch string                `json:"namespace_match,omitempty" jsonschema:"exact or subtree; default exact"`
	SourceID       string                `json:"source_id,omitempty"`
	Path           string                `json:"path,omitempty"`
	IngestionID    string                `json:"ingestion_id,omitempty"`
	Limit          int                   `json:"limit,omitempty"`
	Caller         domain.CallerIdentity `json:"-"`
}

// DeleteSourceInput removes only the server-side source index.
type DeleteSourceInput struct {
	Namespace string                `json:"namespace"`
	SourceID  string                `json:"source_id"`
	Reason    string                `json:"reason"`
	Caller    domain.CallerIdentity `json:"-"`
}

// HealthResult reports service and dependency status.
type HealthResult struct {
	Status            string `json:"status"`
	Database          string `json:"database"`
	EmbeddingProvider string `json:"embedding_provider"`
	EmbeddingEnabled  bool   `json:"embedding_enabled"`
	Version           string `json:"version"`
}

// Backend is implemented by the PostgreSQL service and the local HTTP proxy client.
type Backend interface {
	PutMemory(context.Context, PutMemoryInput) (domain.Memory, error)
	PatchMemory(context.Context, PatchMemoryInput) (domain.Memory, error)
	GetMemory(context.Context, GetMemoryInput) (domain.Memory, error)
	SearchMemory(context.Context, SearchMemoryInput) (domain.SearchResponse, error)
	ListMemory(context.Context, ListMemoryInput) (domain.MemoryListResponse, error)
	ListNamespaces(context.Context, NamespaceListInput) (domain.NamespaceListResponse, error)
	DeleteNamespace(context.Context, NamespaceDeleteInput) (domain.NamespaceDeleteResult, error)
	DeleteMemory(context.Context, DeleteMemoryInput) (domain.Memory, error)
	History(context.Context, HistoryInput) ([]domain.Revision, error)
	RestoreMemory(context.Context, RestoreMemoryInput) (domain.Memory, error)
	SupersedeMemory(context.Context, SupersedeMemoryInput) (domain.Memory, error)
	RefuteMemory(context.Context, RefuteMemoryInput) (domain.Memory, error)
	TouchMemory(context.Context, TouchMemoryInput) (domain.Memory, error)
	RegisterDevice(context.Context, RegisterDeviceInput) (domain.RegistrationResult, error)
	ClaimDevice(context.Context, ClaimDeviceInput) (WhoAmIResult, error)
	MigrateDevice(context.Context, MigrateDeviceInput) (WhoAmIResult, error)
	WhoAmI(context.Context, WhoAmIInput) (WhoAmIResult, error)
	SyncSources(context.Context, SyncSourcesInput) (domain.IngestionSummary, error)
	SourceStatus(context.Context, SourceStatusInput) (domain.SourceStatus, error)
	DeleteSource(context.Context, DeleteSourceInput) (domain.Source, error)
	Health(context.Context) (HealthResult, error)
}
