package domain

import (
	"encoding/json"
	"time"
)

const (
	ScopeInstallation = "installation"
	ScopeDevice       = "device"
	ScopeWorkspace    = "workspace"
	ScopeProject      = "project"
	ScopeGlobal       = "global"
)

const (
	StatusActive     = "active"
	StatusSuperseded = "superseded"
	StatusRefuted    = "refuted"
	StatusExpired    = "expired"
	StatusDeleted    = "deleted"
)

const (
	SearchPreferLocal = "prefer_local"
	SearchLocalOnly   = "local_only"
	SearchProjectOnly = "project_only"
	SearchAllDevices  = "all_devices"
)

// CallerIdentity describes the logical device, installation and workspace issuing a request.
type CallerIdentity struct {
	DeviceCode       string `json:"device_code,omitempty"`
	InstallationCode string `json:"installation_code,omitempty"`
	WorkspaceCode    string `json:"workspace_code,omitempty"`
	TailnetIdentity  string `json:"tailnet_identity,omitempty"`
	Actor            string `json:"actor,omitempty"`
}

// Scope identifies where a memory is visible.
type Scope struct {
	Type string `json:"scope_type" jsonschema:"visibility scope: installation, device, workspace, project, or global"`
	ID   string `json:"scope_id,omitempty" jsonschema:"scope identifier; inferred from caller identity when omitted"`
}

// SourceRange identifies the original text range supporting a memory or chunk.
type SourceRange struct {
	StartLine int `json:"start_line,omitempty"`
	EndLine   int `json:"end_line,omitempty"`
	StartChar int `json:"start_char,omitempty"`
	EndChar   int `json:"end_char,omitempty"`
}

// Memory is the current authoritative version of a retained fact or artifact.
type Memory struct {
	ID                string          `json:"id"`
	Namespace         string          `json:"namespace"`
	ScopeType         string          `json:"scope_type"`
	ScopeID           string          `json:"scope_id,omitempty"`
	DeviceCode        string          `json:"device_code,omitempty"`
	InstallationCode  string          `json:"installation_code,omitempty"`
	WorkspaceCode     string          `json:"workspace_code,omitempty"`
	Type              string          `json:"type"`
	Title             string          `json:"title"`
	Content           string          `json:"content"`
	Metadata          json.RawMessage `json:"metadata"`
	Tags              []string        `json:"tags"`
	Status            string          `json:"status"`
	VerificationState string          `json:"verification_state"`
	Confidence        float64         `json:"confidence"`
	Evidence          json.RawMessage `json:"evidence"`
	SourceID          string          `json:"source_id,omitempty"`
	SourcePath        string          `json:"source_path,omitempty"`
	SourceHash        string          `json:"source_hash,omitempty"`
	SourceRange       json.RawMessage `json:"source_range,omitempty"`
	ExpiresAt         *time.Time      `json:"expires_at,omitempty"`
	Version           int64           `json:"version"`
	SupersedesID      string          `json:"supersedes_id,omitempty"`
	CreatedBy         string          `json:"created_by"`
	UpdatedBy         string          `json:"updated_by"`
	CreatedAt         time.Time       `json:"created_at"`
	UpdatedAt         time.Time       `json:"updated_at"`
	ObservedAt        *time.Time      `json:"observed_at,omitempty"`
}

// Revision stores the complete before and after snapshots for one mutation.
type Revision struct {
	ID             int64           `json:"id"`
	MemoryID       string          `json:"memory_id"`
	FromVersion    *int64          `json:"from_version,omitempty"`
	ToVersion      *int64          `json:"to_version,omitempty"`
	Operation      string          `json:"operation"`
	BeforeSnapshot json.RawMessage `json:"before_snapshot,omitempty"`
	AfterSnapshot  json.RawMessage `json:"after_snapshot,omitempty"`
	ChangedBy      string          `json:"changed_by"`
	ChangedAt      time.Time       `json:"changed_at"`
}

// Device describes a long-lived logical device.
type Device struct {
	DeviceCode           string    `json:"device_code"`
	DisplayName          string    `json:"display_name"`
	Status               string    `json:"status"`
	MergedIntoDeviceCode string    `json:"merged_into_device_code,omitempty"`
	CreatedAt            time.Time `json:"created_at"`
	UpdatedAt            time.Time `json:"updated_at"`
}

// Installation describes one operating-system installation bound to a device.
type Installation struct {
	InstallationCode string    `json:"installation_code"`
	DeviceCode       string    `json:"device_code"`
	TailnetIdentity  string    `json:"tailnet_identity,omitempty"`
	Hostname         string    `json:"hostname,omitempty"`
	Status           string    `json:"status"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
	LastSeenAt       time.Time `json:"last_seen_at"`
}

// HardwareSignal is transient registration material; raw values are never persisted.
type HardwareSignal struct {
	Type  string `json:"type" jsonschema:"signal type such as tpm_ek, smbios_uuid, baseboard_serial, bios_serial, disk_serial, hostname, or cpu"`
	Value string `json:"value" jsonschema:"raw local value; the server stores only an HMAC"`
}

// RegistrationResult is returned after resolving or creating a logical device.
type RegistrationResult struct {
	Device            Device        `json:"device"`
	Installation      Installation  `json:"installation"`
	MatchScore        int           `json:"match_score"`
	ClaimRequired     bool          `json:"claim_required"`
	ClaimCandidates   []DeviceMatch `json:"claim_candidates,omitempty"`
	IdentityPersisted bool          `json:"identity_persisted"`
}

// DeviceMatch reports a possible existing device identified by hardware signals.
type DeviceMatch struct {
	DeviceCode string `json:"device_code"`
	Score      int    `json:"score"`
}

// Source records one file uploaded from a client device.
type Source struct {
	ID                   string          `json:"id"`
	Namespace            string          `json:"namespace"`
	ScopeType            string          `json:"scope_type"`
	ScopeID              string          `json:"scope_id,omitempty"`
	DeviceCode           string          `json:"device_code,omitempty"`
	InstallationCode     string          `json:"installation_code,omitempty"`
	WorkspaceCode        string          `json:"workspace_code,omitempty"`
	OriginalAbsolutePath string          `json:"original_absolute_path"`
	RelativePath         string          `json:"relative_path,omitempty"`
	SourceURI            string          `json:"source_uri"`
	ContentHash          string          `json:"content_hash"`
	Size                 int64           `json:"size"`
	MTime                time.Time       `json:"mtime"`
	Parser               string          `json:"parser"`
	Generation           int64           `json:"generation"`
	Status               string          `json:"status"`
	Metadata             json.RawMessage `json:"metadata"`
	ExpiresAt            *time.Time      `json:"expires_at,omitempty"`
	CreatedAt            time.Time       `json:"created_at"`
	UpdatedAt            time.Time       `json:"updated_at"`
}

// ScoreBreakdown makes hybrid ranking inspectable by the caller.
type ScoreBreakdown struct {
	Exact      float64 `json:"exact"`
	Substring  float64 `json:"substring"`
	Lexical    float64 `json:"lexical"`
	Semantic   float64 `json:"semantic"`
	Confidence float64 `json:"confidence"`
	Evidence   float64 `json:"evidence"`
	Locality   float64 `json:"locality"`
	RRF        float64 `json:"rrf"`
	Final      float64 `json:"final"`
}

// SearchResult is a memory or source chunk returned by hybrid retrieval.
type SearchResult struct {
	Kind              string          `json:"kind"`
	ID                string          `json:"id"`
	MemoryID          string          `json:"memory_id,omitempty"`
	SourceID          string          `json:"source_id,omitempty"`
	Namespace         string          `json:"namespace"`
	ScopeType         string          `json:"scope_type"`
	ScopeID           string          `json:"scope_id,omitempty"`
	DeviceCode        string          `json:"device_code,omitempty"`
	InstallationCode  string          `json:"installation_code,omitempty"`
	WorkspaceCode     string          `json:"workspace_code,omitempty"`
	Type              string          `json:"type"`
	Title             string          `json:"title"`
	Content           string          `json:"content"`
	Snippet           string          `json:"snippet"`
	Metadata          json.RawMessage `json:"metadata"`
	Tags              []string        `json:"tags"`
	Status            string          `json:"status"`
	VerificationState string          `json:"verification_state"`
	Confidence        float64         `json:"confidence"`
	Evidence          json.RawMessage `json:"evidence"`
	SourcePath        string          `json:"source_path,omitempty"`
	SourceHash        string          `json:"source_hash,omitempty"`
	SourceRange       json.RawMessage `json:"source_range,omitempty"`
	ExpiresAt         *time.Time      `json:"expires_at,omitempty"`
	Version           int64           `json:"version"`
	IsLocal           bool            `json:"is_local"`
	Score             ScoreBreakdown  `json:"score"`
}

// SearchResponse contains fused results and channel diagnostics.
type SearchResponse struct {
	Results         []SearchResult `json:"results"`
	Query           string         `json:"query"`
	ScopeMode       string         `json:"scope_mode"`
	SemanticEnabled bool           `json:"semantic_enabled"`
	SemanticError   string         `json:"semantic_error,omitempty"`
	DurationMS      int64          `json:"duration_ms"`
	CandidateCounts map[string]int `json:"candidate_counts"`
}

// IngestedFile carries local file content to the central source index.
type IngestedFile struct {
	AbsolutePath string    `json:"absolute_path"`
	RelativePath string    `json:"relative_path,omitempty"`
	ContentHash  string    `json:"content_hash"`
	Content      string    `json:"content"`
	Size         int64     `json:"size"`
	MTime        time.Time `json:"mtime"`
	Parser       string    `json:"parser"`
}

// IngestionSummary reports one completed or scheduled path synchronization.
type IngestionSummary struct {
	IngestionID string   `json:"ingestion_id"`
	WatchID     string   `json:"watch_id,omitempty"`
	RootPath    string   `json:"root_path"`
	Status      string   `json:"status"`
	FilesSeen   int      `json:"files_seen"`
	Created     int      `json:"created"`
	Updated     int      `json:"updated"`
	Unchanged   int      `json:"unchanged"`
	Deleted     int      `json:"deleted"`
	Chunks      int      `json:"chunks"`
	Errors      []string `json:"errors,omitempty"`
	Watching    bool     `json:"watching"`
}

// SourceStatus reports source and embedding state for an ingestion path.
type SourceStatus struct {
	Sources           []Source `json:"sources"`
	PendingEmbeddings int      `json:"pending_embeddings"`
	FailedEmbeddings  int      `json:"failed_embeddings"`
}
