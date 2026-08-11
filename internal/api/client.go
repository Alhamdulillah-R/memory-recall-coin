package api

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Alhamdulillah-R/memory-recall-coin/internal/domain"
	"github.com/Alhamdulillah-R/memory-recall-coin/internal/identity"
	"github.com/Alhamdulillah-R/memory-recall-coin/internal/service"
)

const maxClientResponseBytes = 64 << 20

// ClientConfig configures the local stdio bridge connection and identity defaults.
type ClientConfig struct {
	BaseURL              string
	Token                string
	IdentityFile         string
	DefaultNamespace     string
	DefaultWorkspaceCode string
	DefaultScopeType     string
	AutoRegister         bool
	Timeout              time.Duration
}

// Client implements service.Backend by forwarding typed RPC calls to the central API.
type Client struct {
	endpoint             string
	token                string
	identityFile         string
	defaultNamespace     string
	defaultWorkspaceCode string
	defaultScopeType     string
	autoRegister         bool
	httpClient           *http.Client

	registrationMu sync.Mutex
	identityMu     sync.Mutex
	identityState  identity.State
	identityLoaded bool
	identityExists bool
}

type rpcRequest struct {
	Method string                `json:"method"`
	Params any                   `json:"params"`
	Caller domain.CallerIdentity `json:"caller"`
}

type rpcResponse struct {
	Result json.RawMessage `json:"result,omitempty"`
	Error  *service.Error  `json:"error,omitempty"`
}

/**
 * NewClient creates a central API backend for the local MCP bridge.
 * @return configured client or an error
 */
func NewClient(cfg ClientConfig) (*Client, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if baseURL == "" {
		return nil, errors.New("central API base URL is required")
	}
	if strings.TrimSpace(cfg.Token) == "" {
		return nil, errors.New("central API token is required")
	}
	if cfg.Timeout <= 0 {
		return nil, errors.New("central API timeout must be positive")
	}

	return &Client{
		endpoint:             baseURL + "/v1/rpc",
		token:                cfg.Token,
		identityFile:         cfg.IdentityFile,
		defaultNamespace:     cfg.DefaultNamespace,
		defaultWorkspaceCode: cfg.DefaultWorkspaceCode,
		defaultScopeType:     cfg.DefaultScopeType,
		autoRegister:         cfg.AutoRegister,
		httpClient: &http.Client{
			Timeout: cfg.Timeout,
		},
	}, nil
}

// PutMemory forwards memory_put after applying local namespace and identity defaults.
func (c *Client) PutMemory(ctx context.Context, input service.PutMemoryInput) (domain.Memory, error) {
	caller, err := c.caller(ctx, true)
	if err != nil {
		return domain.Memory{}, err
	}
	c.applyMemoryDefaults(&input.Namespace, &input.ScopeType, &input.ScopeID, &caller)
	input.Caller = caller

	return callRPC[domain.Memory](ctx, c, "memory_put", input, caller)
}

// PatchMemory forwards memory_patch.
func (c *Client) PatchMemory(ctx context.Context, input service.PatchMemoryInput) (domain.Memory, error) {
	caller, err := c.caller(ctx, true)
	if err != nil {
		return domain.Memory{}, err
	}
	c.applyNamespace(&input.Namespace)
	input.Caller = caller

	return callRPC[domain.Memory](ctx, c, "memory_patch", input, caller)
}

// GetMemory forwards memory_get.
func (c *Client) GetMemory(ctx context.Context, input service.GetMemoryInput) (domain.Memory, error) {
	caller, err := c.caller(ctx, true)
	if err != nil {
		return domain.Memory{}, err
	}
	c.applyNamespace(&input.Namespace)
	input.Caller = caller

	return callRPC[domain.Memory](ctx, c, "memory_get", input, caller)
}

// SearchMemory forwards memory_search.
func (c *Client) SearchMemory(ctx context.Context, input service.SearchMemoryInput) (domain.SearchResponse, error) {
	caller, err := c.caller(ctx, true)
	if err != nil {
		return domain.SearchResponse{}, err
	}
	c.applyNamespace(&input.Namespace)
	input.Caller = caller

	response, err := callRPC[domain.SearchResponse](ctx, c, "memory_search", input, caller)
	if err != nil {
		return domain.SearchResponse{}, err
	}
	for index := range response.Results {
		if strings.TrimSpace(response.Results[index].Content) == "" {
			continue
		}
		response.Results[index].Snippet = makeClientSnippet(
			response.Results[index].Content,
			input.Query,
			360,
		)
	}

	return response, nil
}

// ListMemory forwards filter-only browsing through the dedicated central RPC contract.
func (c *Client) ListMemory(ctx context.Context, input service.ListMemoryInput) (domain.MemoryListResponse, error) {
	caller, err := c.caller(ctx, true)
	if err != nil {
		return domain.MemoryListResponse{}, err
	}
	c.applyNamespace(&input.Namespace)
	input.Caller = caller
	if input.DetailLevel == "" {
		input.DetailLevel = domain.SearchDetailCompact
	}

	return callRPC[domain.MemoryListResponse](ctx, c, "memory_list", input, caller)
}

// DeleteMemory forwards memory_delete.
func (c *Client) DeleteMemory(ctx context.Context, input service.DeleteMemoryInput) (domain.Memory, error) {
	caller, err := c.caller(ctx, true)
	if err != nil {
		return domain.Memory{}, err
	}
	c.applyNamespace(&input.Namespace)
	input.Caller = caller

	return callRPC[domain.Memory](ctx, c, "memory_delete", input, caller)
}

// History forwards memory_history.
func (c *Client) History(ctx context.Context, input service.HistoryInput) ([]domain.Revision, error) {
	caller, err := c.caller(ctx, true)
	if err != nil {
		return nil, err
	}
	c.applyNamespace(&input.Namespace)
	input.Caller = caller

	return callRPC[[]domain.Revision](ctx, c, "memory_history", input, caller)
}

// RestoreMemory forwards memory_restore.
func (c *Client) RestoreMemory(ctx context.Context, input service.RestoreMemoryInput) (domain.Memory, error) {
	caller, err := c.caller(ctx, true)
	if err != nil {
		return domain.Memory{}, err
	}
	c.applyNamespace(&input.Namespace)
	input.Caller = caller

	return callRPC[domain.Memory](ctx, c, "memory_restore", input, caller)
}

// SupersedeMemory forwards memory_supersede.
func (c *Client) SupersedeMemory(ctx context.Context, input service.SupersedeMemoryInput) (domain.Memory, error) {
	caller, err := c.caller(ctx, true)
	if err != nil {
		return domain.Memory{}, err
	}
	c.applyNamespace(&input.Namespace)
	input.Caller = caller
	input.Replacement.Caller = caller

	return callRPC[domain.Memory](ctx, c, "memory_supersede", input, caller)
}

// RefuteMemory forwards memory_refute.
func (c *Client) RefuteMemory(ctx context.Context, input service.RefuteMemoryInput) (domain.Memory, error) {
	caller, err := c.caller(ctx, true)
	if err != nil {
		return domain.Memory{}, err
	}
	c.applyNamespace(&input.Namespace)
	input.Caller = caller

	return callRPC[domain.Memory](ctx, c, "memory_refute", input, caller)
}

// TouchMemory forwards memory_touch.
func (c *Client) TouchMemory(ctx context.Context, input service.TouchMemoryInput) (domain.Memory, error) {
	caller, err := c.caller(ctx, true)
	if err != nil {
		return domain.Memory{}, err
	}
	c.applyNamespace(&input.Namespace)
	input.Caller = caller

	return callRPC[domain.Memory](ctx, c, "memory_touch", input, caller)
}

/**
 * RegisterDevice discovers local hardware signals when omitted and persists the resolved identity.
 * @return registration result with identity_persisted=true
 */
func (c *Client) RegisterDevice(ctx context.Context, input service.RegisterDeviceInput) (domain.RegistrationResult, error) {
	c.registrationMu.Lock()
	defer c.registrationMu.Unlock()

	state, exists, err := c.loadIdentity()
	if err != nil {
		return domain.RegistrationResult{}, err
	}

	discovery, err := identity.Discover(ctx)
	if err != nil {
		return domain.RegistrationResult{}, err
	}
	if input.InstallationCode == "" {
		if exists {
			input.InstallationCode = state.InstallationCode
		} else {
			input.InstallationCode = service.NewID("inst")
		}
	}
	if input.Hostname == "" {
		input.Hostname = discovery.Hostname
	}
	if input.DisplayName == "" {
		input.DisplayName = discovery.Hostname
	}
	if input.TailnetIdentity == "" {
		input.TailnetIdentity = discovery.TailnetIdentity
	}
	if len(input.Signals) == 0 {
		input.Signals = discovery.Signals
	}

	caller := domain.CallerIdentity{}
	if exists {
		caller = domain.CallerIdentity{
			DeviceCode:       state.DeviceCode,
			InstallationCode: state.InstallationCode,
			TailnetIdentity:  state.TailnetIdentity,
			Actor:            state.InstallationCode,
		}
	}

	result, err := callRPC[domain.RegistrationResult](ctx, c, "device_register", input, caller)
	if err != nil {
		return domain.RegistrationResult{}, err
	}
	if err := c.persistRegistration(result); err != nil {
		return domain.RegistrationResult{}, err
	}
	result.IdentityPersisted = true

	return result, nil
}

// ClaimDevice forwards device_claim and updates the local identity file.
func (c *Client) ClaimDevice(ctx context.Context, input service.ClaimDeviceInput) (service.WhoAmIResult, error) {
	caller, err := c.caller(ctx, true)
	if err != nil {
		return service.WhoAmIResult{}, err
	}
	if input.InstallationCode == "" {
		input.InstallationCode = caller.InstallationCode
	}
	input.Caller = caller
	result, err := callRPC[service.WhoAmIResult](ctx, c, "device_claim", input, caller)
	if err != nil {
		return service.WhoAmIResult{}, err
	}
	if err := c.persistWhoAmI(result); err != nil {
		return service.WhoAmIResult{}, err
	}

	return result, nil
}

// MigrateDevice forwards device_migrate and refreshes local identity when affected.
func (c *Client) MigrateDevice(ctx context.Context, input service.MigrateDeviceInput) (service.WhoAmIResult, error) {
	caller, err := c.caller(ctx, true)
	if err != nil {
		return service.WhoAmIResult{}, err
	}
	input.Caller = caller
	result, err := callRPC[service.WhoAmIResult](ctx, c, "device_migrate", input, caller)
	if err != nil {
		return service.WhoAmIResult{}, err
	}
	if caller.DeviceCode == input.SourceDeviceCode {
		state, exists, err := c.loadIdentity()
		if err != nil {
			return service.WhoAmIResult{}, err
		}
		if exists {
			state.DeviceCode = input.TargetDeviceCode
			if err := identity.Save(c.identityFile, state); err != nil {
				return service.WhoAmIResult{}, err
			}
			c.setIdentity(state)
		}
	}

	return result, nil
}

// WhoAmI resolves the central identity without forcing registration when auto-register is disabled.
func (c *Client) WhoAmI(ctx context.Context, input service.WhoAmIInput) (service.WhoAmIResult, error) {
	caller, err := c.caller(ctx, c.autoRegister)
	if err != nil {
		return service.WhoAmIResult{}, err
	}
	if input.InstallationCode == "" {
		input.InstallationCode = caller.InstallationCode
	}
	input.Caller = caller

	result, err := callRPC[service.WhoAmIResult](ctx, c, "device_whoami", input, caller)
	if err != nil {
		return service.WhoAmIResult{}, err
	}
	if err := c.persistWhoAmI(result); err != nil {
		return service.WhoAmIResult{}, err
	}

	return result, nil
}

// SyncSources forwards an already scanned local manifest.
func (c *Client) SyncSources(ctx context.Context, input service.SyncSourcesInput) (domain.IngestionSummary, error) {
	caller, err := c.caller(ctx, true)
	if err != nil {
		return domain.IngestionSummary{}, err
	}
	c.applyMemoryDefaults(&input.Namespace, &input.ScopeType, &input.ScopeID, &caller)
	input.Caller = caller

	return callRPC[domain.IngestionSummary](ctx, c, "source_sync", input, caller)
}

// SourceStatus forwards memory_source_status.
func (c *Client) SourceStatus(ctx context.Context, input service.SourceStatusInput) (domain.SourceStatus, error) {
	caller, err := c.caller(ctx, true)
	if err != nil {
		return domain.SourceStatus{}, err
	}
	c.applyNamespace(&input.Namespace)
	input.Caller = caller

	return callRPC[domain.SourceStatus](ctx, c, "memory_source_status", input, caller)
}

// DeleteSource forwards memory_source_delete.
func (c *Client) DeleteSource(ctx context.Context, input service.DeleteSourceInput) (domain.Source, error) {
	caller, err := c.caller(ctx, true)
	if err != nil {
		return domain.Source{}, err
	}
	c.applyNamespace(&input.Namespace)
	input.Caller = caller

	return callRPC[domain.Source](ctx, c, "memory_source_delete", input, caller)
}

// Health forwards memory_health without requiring device registration.
func (c *Client) Health(ctx context.Context) (service.HealthResult, error) {
	return callRPC[service.HealthResult](ctx, c, "memory_health", struct{}{}, domain.CallerIdentity{})
}

func callRPC[T any](ctx context.Context, client *Client, method string, params any, caller domain.CallerIdentity) (T, error) {
	var zero T
	payload, err := json.Marshal(rpcRequest{Method: method, Params: params, Caller: caller})
	if err != nil {
		return zero, fmt.Errorf("encode %s request: %w", method, err)
	}

	body := bytes.NewReader(payload)
	var requestBody io.Reader = body
	contentEncoding := ""
	if len(payload) >= 64<<10 {
		var compressed bytes.Buffer
		writer := gzip.NewWriter(&compressed)
		if _, err := writer.Write(payload); err != nil {
			return zero, fmt.Errorf("compress %s request: %w", method, err)
		}
		if err := writer.Close(); err != nil {
			return zero, fmt.Errorf("finish %s compression: %w", method, err)
		}
		requestBody = bytes.NewReader(compressed.Bytes())
		contentEncoding = "gzip"
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.endpoint, requestBody)
	if err != nil {
		return zero, fmt.Errorf("create %s request: %w", method, err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+client.token)
	if contentEncoding != "" {
		request.Header.Set("Content-Encoding", contentEncoding)
	}

	response, err := client.httpClient.Do(request)
	if err != nil {
		return zero, service.WrapError(service.CodeUnavailable, "call central memory API", err)
	}
	data, readErr := io.ReadAll(io.LimitReader(response.Body, maxClientResponseBytes+1))
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil {
		return zero, fmt.Errorf("read %s response: %w", method, errors.Join(readErr, closeErr))
	}
	if len(data) > maxClientResponseBytes {
		return zero, fmt.Errorf("%s response exceeds %d bytes", method, maxClientResponseBytes)
	}

	var envelope rpcResponse
	if err := json.Unmarshal(data, &envelope); err != nil {
		return zero, fmt.Errorf("decode %s response (%s): %w", method, response.Status, err)
	}
	if envelope.Error != nil {
		return zero, envelope.Error
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return zero, service.NewError(service.CodeUnavailable, "central API returned "+response.Status)
	}
	if err := json.Unmarshal(envelope.Result, &zero); err != nil {
		return zero, fmt.Errorf("decode %s result: %w", method, err)
	}

	return zero, nil
}

func (c *Client) caller(ctx context.Context, requireRegistration bool) (domain.CallerIdentity, error) {
	state, exists, err := c.loadIdentity()
	if err != nil {
		return domain.CallerIdentity{}, err
	}
	if !exists && requireRegistration {
		if !c.autoRegister {
			return domain.CallerIdentity{}, service.NewError(service.CodeUnauthorized, "device is not registered; call device_register")
		}
		if _, err := c.RegisterDevice(ctx, service.RegisterDeviceInput{}); err != nil {
			return domain.CallerIdentity{}, err
		}
		state, exists, err = c.loadIdentity()
		if err != nil {
			return domain.CallerIdentity{}, err
		}
	}
	if !exists {
		return domain.CallerIdentity{WorkspaceCode: c.defaultWorkspaceCode}, nil
	}

	return domain.CallerIdentity{
		DeviceCode:       state.DeviceCode,
		InstallationCode: state.InstallationCode,
		WorkspaceCode:    c.defaultWorkspaceCode,
		TailnetIdentity:  state.TailnetIdentity,
		Actor:            state.InstallationCode,
	}, nil
}

func (c *Client) loadIdentity() (identity.State, bool, error) {
	c.identityMu.Lock()
	defer c.identityMu.Unlock()
	if c.identityLoaded {
		return c.identityState, c.identityExists, nil
	}

	state, exists, err := identity.Load(c.identityFile)
	if err != nil {
		return identity.State{}, false, err
	}
	c.identityState = state
	c.identityExists = exists
	c.identityLoaded = true

	return state, exists, nil
}

func (c *Client) setIdentity(state identity.State) {
	c.identityMu.Lock()
	c.identityState = state
	c.identityExists = true
	c.identityLoaded = true
	c.identityMu.Unlock()
}

func (c *Client) persistRegistration(result domain.RegistrationResult) error {
	state := identity.State{
		DeviceCode:       result.Device.DeviceCode,
		InstallationCode: result.Installation.InstallationCode,
		TailnetIdentity:  result.Installation.TailnetIdentity,
		Hostname:         result.Installation.Hostname,
	}
	if err := identity.Save(c.identityFile, state); err != nil {
		return err
	}
	c.setIdentity(state)

	return nil
}

func (c *Client) persistWhoAmI(result service.WhoAmIResult) error {
	if !result.Registered || result.Device == nil || result.Installation == nil {
		return nil
	}

	return c.persistRegistration(domain.RegistrationResult{
		Device:       *result.Device,
		Installation: *result.Installation,
	})
}

func (c *Client) applyNamespace(namespace *string) {
	if strings.TrimSpace(*namespace) == "" {
		*namespace = c.defaultNamespace
	}
}

func (c *Client) applyMemoryDefaults(namespace, scopeType, scopeID *string, caller *domain.CallerIdentity) {
	c.applyNamespace(namespace)
	if strings.TrimSpace(*scopeType) == "" {
		*scopeType = c.defaultScopeType
	}
	if *scopeType == domain.ScopeWorkspace && caller.WorkspaceCode == "" {
		caller.WorkspaceCode = c.defaultWorkspaceCode
	}
	_ = scopeID
}

func makeClientSnippet(content, query string, maxRunes int) string {
	content = strings.TrimSpace(content)
	runes := []rune(content)
	if len(runes) <= maxRunes {
		return content
	}
	start := 0
	position := strings.Index(strings.ToLower(content), strings.ToLower(query))
	if position >= 0 {
		start = len([]rune(content[:position])) - maxRunes/3
		if start < 0 {
			start = 0
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

var _ service.Backend = (*Client)(nil)
