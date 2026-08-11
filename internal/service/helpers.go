package service

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/Alhamdulillah-R/memory-recall-coin/internal/domain"
)

var namespacePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{1,126}[a-z0-9]$`)

var memoryTypes = map[string]struct{}{
	"fact": {}, "experiment": {}, "hypothesis": {}, "decision": {},
	"artifact": {}, "procedure": {}, "incident": {}, "summary": {},
}

var verificationStates = map[string]struct{}{
	"unverified": {}, "supported": {}, "confirmed": {}, "contested": {},
}

// NewID creates a sortable identifier with a domain prefix.
func NewID(prefix string) string {
	return prefix + "_" + ulid.MustNew(ulid.Timestamp(time.Now().UTC()), rand.Reader).String()
}

func validateNamespace(namespace string) error {
	if !namespacePattern.MatchString(namespace) {
		return NewError(CodeInvalidArgument, "namespace must match ^[a-z0-9][a-z0-9._-]{1,126}[a-z0-9]$")
	}

	return nil
}

func validateScope(scopeType, scopeID string, caller domain.CallerIdentity, namespace string) (string, string, error) {
	if scopeType == "" {
		scopeType = domain.ScopeWorkspace
	}

	switch scopeType {
	case domain.ScopeInstallation:
		if scopeID == "" {
			scopeID = caller.InstallationCode
		}
	case domain.ScopeDevice:
		if scopeID == "" {
			scopeID = caller.DeviceCode
		}
	case domain.ScopeWorkspace:
		if scopeID == "" {
			scopeID = caller.WorkspaceCode
		}
	case domain.ScopeProject:
		if scopeID == "" {
			scopeID = namespace
		}
	case domain.ScopeGlobal:
		if scopeID == "" {
			scopeID = "global"
		}
	default:
		return "", "", NewError(CodeInvalidArgument, "scope_type must be installation, device, workspace, project, or global")
	}

	if strings.TrimSpace(scopeID) == "" {
		return "", "", NewError(CodeInvalidArgument, "scope_id cannot be inferred from the current caller identity")
	}

	return scopeType, scopeID, nil
}

func validateTTL(ttlSeconds *int64, expiresAt *time.Time) (*time.Time, error) {
	if ttlSeconds != nil && expiresAt != nil {
		return nil, NewError(CodeInvalidArgument, "ttl_seconds and expires_at are mutually exclusive")
	}
	if ttlSeconds != nil {
		if *ttlSeconds <= 0 {
			return nil, NewError(CodeInvalidArgument, "ttl_seconds must be positive")
		}
		maxTTLSeconds := int64((time.Duration(1<<63 - 1)) / time.Second)
		if *ttlSeconds > maxTTLSeconds {
			return nil, NewError(CodeInvalidArgument, "ttl_seconds exceeds the supported duration")
		}

		value := time.Now().UTC().Add(time.Duration(*ttlSeconds) * time.Second)
		return &value, nil
	}
	if expiresAt != nil {
		value := expiresAt.UTC()
		return &value, nil
	}

	return nil, nil
}

func normalizeActor(explicit string, caller domain.CallerIdentity) string {
	if value := strings.TrimSpace(explicit); value != "" {
		return value
	}
	if value := strings.TrimSpace(caller.Actor); value != "" {
		return value
	}
	if value := strings.TrimSpace(caller.InstallationCode); value != "" {
		return value
	}

	return "unknown"
}

func normalizeJSON(raw json.RawMessage, empty string) (json.RawMessage, error) {
	if len(raw) == 0 {
		return json.RawMessage(empty), nil
	}
	if !json.Valid(raw) {
		return nil, NewError(CodeInvalidArgument, "invalid JSON value")
	}

	var buffer bytes.Buffer
	if err := json.Compact(&buffer, raw); err != nil {
		return nil, NewError(CodeInvalidArgument, "invalid JSON value")
	}

	return buffer.Bytes(), nil
}

func normalizeTags(tags []string) []string {
	seen := make(map[string]struct{}, len(tags))
	result := make([]string, 0, len(tags))
	for _, tag := range tags {
		tag = strings.ToLower(strings.TrimSpace(tag))
		if tag == "" {
			continue
		}
		if _, exists := seen[tag]; exists {
			continue
		}
		seen[tag] = struct{}{}
		result = append(result, tag)
	}
	sort.Strings(result)

	return result
}

func validateMemoryType(memoryType string) error {
	if _, ok := memoryTypes[memoryType]; !ok {
		return NewError(CodeInvalidArgument, "unsupported memory type")
	}

	return nil
}

func validateVerificationState(state string) error {
	if _, ok := verificationStates[state]; !ok {
		return NewError(CodeInvalidArgument, "unsupported verification_state")
	}

	return nil
}

func normalizePath(value string) string {
	cleaned := cleanPortablePath(value)
	if isWindowsPath(value) {
		return strings.ToLower(cleaned)
	}

	return cleaned
}

func cleanPortablePath(value string) string {
	value = strings.TrimSpace(value)
	replaced := strings.ReplaceAll(value, `\`, "/")
	uncPath := strings.HasPrefix(replaced, "//")
	cleaned := path.Clean(replaced)
	if uncPath && !strings.HasPrefix(cleaned, "//") {
		cleaned = "/" + cleaned
	}

	return cleaned
}

func isWindowsPath(value string) bool {
	if strings.HasPrefix(value, `\\`) || strings.HasPrefix(value, "//") {
		return true
	}
	if len(value) < 3 || value[1] != ':' {
		return false
	}

	first := value[0]
	return (first >= 'a' && first <= 'z') || (first >= 'A' && first <= 'Z')
}

func validateConfidence(confidence float64) error {
	if confidence < 0 || confidence > 1 {
		return NewError(CodeInvalidArgument, "confidence must be between 0 and 1")
	}

	return nil
}

func requireNonEmpty(name, value string) error {
	if strings.TrimSpace(value) == "" {
		return NewError(CodeInvalidArgument, fmt.Sprintf("%s is required", name))
	}

	return nil
}
