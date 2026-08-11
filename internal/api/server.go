package api

import (
	"compress/gzip"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Alhamdulillah-R/memory-recall-coin/internal/domain"
	"github.com/Alhamdulillah-R/memory-recall-coin/internal/service"
)

// ServerConfig configures the central HTTP API around a service backend.
type ServerConfig struct {
	Token        string
	MaxBodyBytes int64
	Logger       *slog.Logger
}

// Server serves authenticated RPC plus liveness and readiness endpoints.
type Server struct {
	backend      service.Backend
	token        string
	maxBodyBytes int64
	logger       *slog.Logger

	identityMu    sync.RWMutex
	identityCache map[string]cachedIdentity
}

type cachedIdentity struct {
	Caller    domain.CallerIdentity
	ExpiresAt time.Time
}

type incomingRPCRequest struct {
	Method string                `json:"method"`
	Params json.RawMessage       `json:"params"`
	Caller domain.CallerIdentity `json:"caller"`
}

/**
 * NewServer creates the central authenticated HTTP surface.
 * @return HTTP server wrapper
 */
func NewServer(backend service.Backend, cfg ServerConfig) *Server {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &Server{
		backend:       backend,
		token:         cfg.Token,
		maxBodyBytes:  cfg.MaxBodyBytes,
		logger:        logger,
		identityCache: make(map[string]cachedIdentity),
	}
}

// Handler builds the complete HTTP router.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /readyz", s.handleReady)
	mux.Handle("POST /v1/rpc", s.requireAuth(http.HandlerFunc(s.handleRPC)))

	return s.requestLog(mux)
}

func (s *Server) handleHealth(response http.ResponseWriter, _ *http.Request) {
	writeJSON(response, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReady(response http.ResponseWriter, request *http.Request) {
	health, err := s.backend.Health(request.Context())
	if err != nil {
		writeJSON(response, http.StatusServiceUnavailable, rpcResponse{Error: publicError(err)})
		return
	}

	writeJSON(response, http.StatusOK, health)
}

func (s *Server) handleRPC(response http.ResponseWriter, request *http.Request) {
	body, err := readRequestBody(request, s.maxBodyBytes)
	if err != nil {
		writeRPCError(response, service.NewError(service.CodeInvalidArgument, err.Error()))
		return
	}

	var envelope incomingRPCRequest
	if err := json.Unmarshal(body, &envelope); err != nil {
		writeRPCError(response, service.NewError(service.CodeInvalidArgument, "invalid RPC request JSON"))
		return
	}
	if strings.TrimSpace(envelope.Method) == "" {
		writeRPCError(response, service.NewError(service.CodeInvalidArgument, "RPC method is required"))
		return
	}

	if requestRequiresIdentity(envelope.Method, envelope.Caller) {
		caller, err := s.verifyCaller(request.Context(), envelope.Caller)
		if err != nil {
			writeRPCError(response, err)
			return
		}
		envelope.Caller = caller
	} else if envelope.Method == "device_register" {
		envelope.Caller = domain.CallerIdentity{}
	}

	result, err := s.dispatch(request.Context(), envelope)
	if err != nil {
		writeRPCError(response, err)
		return
	}

	writeJSON(response, http.StatusOK, rpcResponse{Result: marshalResult(result)})
}

func (s *Server) dispatch(ctx context.Context, request incomingRPCRequest) (any, error) {
	switch request.Method {
	case "memory_put":
		return dispatchInput(ctx, request, s.backend.PutMemory)
	case "memory_patch":
		return dispatchInput(ctx, request, s.backend.PatchMemory)
	case "memory_get":
		return dispatchInput(ctx, request, s.backend.GetMemory)
	case "memory_search":
		return dispatchInput(ctx, request, s.backend.SearchMemory)
	case "memory_list":
		return dispatchInput(ctx, request, s.listMemory)
	case "memory_delete":
		return dispatchInput(ctx, request, s.backend.DeleteMemory)
	case "memory_history":
		return dispatchInput(ctx, request, s.backend.History)
	case "memory_restore":
		return dispatchInput(ctx, request, s.backend.RestoreMemory)
	case "memory_supersede":
		return dispatchInput(ctx, request, s.backend.SupersedeMemory)
	case "memory_refute":
		return dispatchInput(ctx, request, s.backend.RefuteMemory)
	case "memory_touch":
		return dispatchInput(ctx, request, s.backend.TouchMemory)
	case "device_register":
		return dispatchInput(ctx, request, s.backend.RegisterDevice)
	case "device_claim":
		result, err := dispatchInput(ctx, request, s.backend.ClaimDevice)
		if err == nil {
			s.clearIdentityCache()
		}
		return result, err
	case "device_migrate":
		result, err := dispatchInput(ctx, request, s.backend.MigrateDevice)
		if err == nil {
			s.clearIdentityCache()
		}
		return result, err
	case "device_whoami":
		return dispatchInput(ctx, request, s.backend.WhoAmI)
	case "source_sync":
		return dispatchInput(ctx, request, s.backend.SyncSources)
	case "memory_source_status":
		return dispatchInput(ctx, request, s.backend.SourceStatus)
	case "memory_source_delete":
		return dispatchInput(ctx, request, s.backend.DeleteSource)
	case "memory_health":
		return s.backend.Health(ctx)
	default:
		return nil, service.NewError(service.CodeInvalidArgument, "unknown RPC method: "+request.Method)
	}
}

func (s *Server) listMemory(ctx context.Context, input service.ListMemoryInput) (domain.SearchResponse, error) {
	return s.backend.SearchMemory(ctx, input.SearchInput())
}

type callerInput interface {
	setCaller(domain.CallerIdentity)
}

func dispatchInput[Input any, Output any](
	ctx context.Context,
	request incomingRPCRequest,
	handler func(context.Context, Input) (Output, error),
) (Output, error) {
	var input Input
	if len(request.Params) == 0 {
		request.Params = json.RawMessage("{}")
	}
	if err := json.Unmarshal(request.Params, &input); err != nil {
		var zero Output
		return zero, service.NewError(service.CodeInvalidArgument, "invalid "+request.Method+" parameters")
	}
	applyCaller(&input, request.Caller)

	return handler(ctx, input)
}

func applyCaller(input any, caller domain.CallerIdentity) {
	switch value := input.(type) {
	case *service.PutMemoryInput:
		value.Caller = caller
	case *service.PatchMemoryInput:
		value.Caller = caller
	case *service.GetMemoryInput:
		value.Caller = caller
	case *service.SearchMemoryInput:
		value.Caller = caller
	case *service.ListMemoryInput:
		value.Caller = caller
	case *service.DeleteMemoryInput:
		value.Caller = caller
	case *service.HistoryInput:
		value.Caller = caller
	case *service.RestoreMemoryInput:
		value.Caller = caller
	case *service.SupersedeMemoryInput:
		value.Caller = caller
		value.Replacement.Caller = caller
	case *service.RefuteMemoryInput:
		value.Caller = caller
	case *service.TouchMemoryInput:
		value.Caller = caller
	case *service.RegisterDeviceInput:
		value.Caller = caller
	case *service.ClaimDeviceInput:
		value.Caller = caller
	case *service.MigrateDeviceInput:
		value.Caller = caller
	case *service.WhoAmIInput:
		value.Caller = caller
	case *service.SyncSourcesInput:
		value.Caller = caller
	case *service.SourceStatusInput:
		value.Caller = caller
	case *service.DeleteSourceInput:
		value.Caller = caller
	}
}

func (s *Server) verifyCaller(ctx context.Context, assertion domain.CallerIdentity) (domain.CallerIdentity, error) {
	if assertion.InstallationCode == "" {
		return domain.CallerIdentity{}, service.NewError(service.CodeUnauthorized, "installation_code is required")
	}
	if cached, ok := s.getCachedIdentity(assertion.InstallationCode); ok {
		if assertion.DeviceCode != "" && assertion.DeviceCode != cached.DeviceCode {
			return domain.CallerIdentity{}, service.NewError(service.CodeUnauthorized, "device_code does not match the registered installation")
		}
		cached.WorkspaceCode = assertion.WorkspaceCode
		return cached, nil
	}

	identityResult, err := s.backend.WhoAmI(ctx, service.WhoAmIInput{
		InstallationCode: assertion.InstallationCode,
		Caller:           assertion,
	})
	if err != nil {
		return domain.CallerIdentity{}, err
	}
	if !identityResult.Registered || identityResult.Device == nil || identityResult.Installation == nil {
		return domain.CallerIdentity{}, service.NewError(service.CodeUnauthorized, "installation is not registered")
	}
	if assertion.DeviceCode != "" && assertion.DeviceCode != identityResult.Device.DeviceCode {
		return domain.CallerIdentity{}, service.NewError(service.CodeUnauthorized, "device_code does not match the registered installation")
	}

	caller := domain.CallerIdentity{
		DeviceCode:       identityResult.Device.DeviceCode,
		InstallationCode: identityResult.Installation.InstallationCode,
		WorkspaceCode:    assertion.WorkspaceCode,
		TailnetIdentity:  identityResult.Installation.TailnetIdentity,
		Actor:            identityResult.Installation.InstallationCode,
	}
	s.cacheIdentity(caller)

	return caller, nil
}

func requestRequiresIdentity(method string, caller domain.CallerIdentity) bool {
	switch method {
	case "device_register":
		return caller.InstallationCode != ""
	case "device_whoami", "memory_health":
		return false
	default:
		return true
	}
}

func (s *Server) getCachedIdentity(installationCode string) (domain.CallerIdentity, bool) {
	s.identityMu.RLock()
	entry, ok := s.identityCache[installationCode]
	s.identityMu.RUnlock()
	if !ok || !entry.ExpiresAt.After(time.Now()) {
		return domain.CallerIdentity{}, false
	}

	return entry.Caller, true
}

func (s *Server) cacheIdentity(caller domain.CallerIdentity) {
	s.identityMu.Lock()
	s.identityCache[caller.InstallationCode] = cachedIdentity{
		Caller:    caller,
		ExpiresAt: time.Now().Add(5 * time.Minute),
	}
	s.identityMu.Unlock()
}

func (s *Server) clearIdentityCache() {
	s.identityMu.Lock()
	s.identityCache = make(map[string]cachedIdentity)
	s.identityMu.Unlock()
}

func (s *Server) requireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		provided := strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer ")
		if subtle.ConstantTimeCompare([]byte(provided), []byte(s.token)) != 1 {
			response.Header().Set("WWW-Authenticate", "Bearer")
			writeJSON(response, http.StatusUnauthorized, rpcResponse{
				Error: service.NewError(service.CodeUnauthorized, "invalid bearer token"),
			})
			return
		}

		next.ServeHTTP(response, request)
	})
}

func (s *Server) requestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		startedAt := time.Now()
		next.ServeHTTP(response, request)
		s.logger.Info(
			"HTTP request",
			"method", request.Method,
			"path", request.URL.Path,
			"remote", request.RemoteAddr,
			"duration_ms", time.Since(startedAt).Milliseconds(),
		)
	})
}

func readRequestBody(request *http.Request, limit int64) ([]byte, error) {
	var reader io.Reader = request.Body
	if request.Header.Get("Content-Encoding") == "gzip" {
		gzipReader, err := gzip.NewReader(request.Body)
		if err != nil {
			_ = request.Body.Close()
			return nil, fmt.Errorf("open gzip request: %w", err)
		}
		defer func() {
			_ = gzipReader.Close()
		}()
		reader = gzipReader
	} else if encoding := request.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
		_ = request.Body.Close()
		return nil, fmt.Errorf("unsupported Content-Encoding %q", encoding)
	}

	data, readErr := io.ReadAll(io.LimitReader(reader, limit+1))
	closeErr := request.Body.Close()
	if readErr != nil || closeErr != nil {
		return nil, errors.Join(readErr, closeErr)
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("request body exceeds %d bytes", limit)
	}

	return data, nil
}

func marshalResult(value any) json.RawMessage {
	data, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage("null")
	}

	return data
}

func publicError(err error) *service.Error {
	var serviceErr *service.Error
	if errors.As(err, &serviceErr) {
		return &service.Error{
			Code:    serviceErr.Code,
			Message: serviceErr.Message,
			Details: serviceErr.Details,
		}
	}

	return service.NewError(service.CodeInternal, "internal service error")
}

func writeRPCError(response http.ResponseWriter, err error) {
	serviceErr := publicError(err)
	status := http.StatusInternalServerError
	switch serviceErr.Code {
	case service.CodeInvalidArgument:
		status = http.StatusBadRequest
	case service.CodeUnauthorized:
		status = http.StatusUnauthorized
	case service.CodeNotFound:
		status = http.StatusNotFound
	case service.CodeConflict, service.CodeAlreadyExists:
		status = http.StatusConflict
	case service.CodeUnavailable:
		status = http.StatusServiceUnavailable
	}

	writeJSON(response, status, rpcResponse{Error: serviceErr})
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json; charset=utf-8")
	response.Header().Set("Cache-Control", "no-store")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}
