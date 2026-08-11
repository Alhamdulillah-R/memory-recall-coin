package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Alhamdulillah-R/memory-recall-coin/internal/embedding"
)

const defaultMaxFileBytes = 2 << 20

// Config contains runtime settings shared by the central service and local MCP proxy.
type Config struct {
	Mode                      string
	ListenAddress             string
	DatabaseURL               string
	APIToken                  string
	APIURL                    string
	DefaultNamespace          string
	DefaultWorkspaceCode      string
	DefaultScopeType          string
	IdentityFile              string
	SignalHMACSecret          string
	EmbeddingProvider         string
	EmbeddingURL              string
	EmbeddingAPIKey           string
	EmbeddingModel            string
	EmbeddingQueryPrefix      string
	EmbeddingQueryInstruction string
	EmbeddingDimensions       int
	EmbeddingWorkers          int
	EmbeddingBatchSize        int
	MaxFileBytes              int64
	MaxRPCBodyBytes           int64
	ChunkCharacters           int
	ChunkOverlapCharacters    int
	WatchDebounce             time.Duration
	AutoRegister              bool
	RequestTimeout            time.Duration
	ShutdownTimeout           time.Duration
}

// WorkspaceConfig contains defaults resolved from a .memory-recall.json file.
type WorkspaceConfig struct {
	Namespace     string `json:"namespace"`
	WorkspaceCode string `json:"workspace_code"`
	ScopeType     string `json:"scope_type"`
}

/**
 * Load reads runtime configuration from environment variables and workspace configuration.
 * @param mode requested process mode
 * @return validated configuration or an error
 */
func Load(mode string) (Config, error) {
	if mode == "migrate" {
		cfg := Config{
			Mode:        mode,
			DatabaseURL: strings.TrimSpace(os.Getenv("MEMORY_DATABASE_URL")),
		}

		return cfg, cfg.Validate()
	}
	if mode == "version" {
		return Config{Mode: mode}, nil
	}

	workspace, err := loadWorkspaceConfig()
	if err != nil {
		return Config{}, err
	}

	identityFile, err := defaultIdentityFile()
	if err != nil {
		return Config{}, err
	}

	cfg := Config{
		Mode:                      mode,
		ListenAddress:             envString("MEMORY_LISTEN_ADDRESS", ":8080"),
		DatabaseURL:               strings.TrimSpace(os.Getenv("MEMORY_DATABASE_URL")),
		APIToken:                  strings.TrimSpace(os.Getenv("MEMORY_API_TOKEN")),
		APIURL:                    strings.TrimRight(strings.TrimSpace(os.Getenv("MEMORY_API_URL")), "/"),
		DefaultNamespace:          envString("MEMORY_DEFAULT_NAMESPACE", workspace.Namespace),
		DefaultWorkspaceCode:      envString("MEMORY_WORKSPACE_CODE", workspace.WorkspaceCode),
		DefaultScopeType:          envString("MEMORY_DEFAULT_SCOPE", workspace.ScopeType),
		IdentityFile:              envString("MEMORY_IDENTITY_FILE", identityFile),
		SignalHMACSecret:          strings.TrimSpace(os.Getenv("MEMORY_SIGNAL_HMAC_SECRET")),
		EmbeddingProvider:         strings.ToLower(envString("MEMORY_EMBEDDING_PROVIDER", "none")),
		EmbeddingURL:              strings.TrimRight(strings.TrimSpace(os.Getenv("MEMORY_EMBEDDING_URL")), "/"),
		EmbeddingAPIKey:           strings.TrimSpace(os.Getenv("MEMORY_EMBEDDING_API_KEY")),
		EmbeddingModel:            envString("MEMORY_EMBEDDING_MODEL", "text-embedding-3-small"),
		EmbeddingQueryPrefix:      strings.TrimSpace(os.Getenv("MEMORY_EMBEDDING_QUERY_PREFIX")),
		EmbeddingQueryInstruction: strings.TrimSpace(os.Getenv("MEMORY_EMBEDDING_QUERY_INSTRUCTION")),
		EmbeddingDimensions:       envInt("MEMORY_EMBEDDING_DIMENSIONS", embedding.Dimensions),
		EmbeddingWorkers:          envInt("MEMORY_EMBEDDING_WORKERS", 2),
		EmbeddingBatchSize:        envInt("MEMORY_EMBEDDING_BATCH_SIZE", 32),
		MaxFileBytes:              envInt64("MEMORY_MAX_FILE_BYTES", defaultMaxFileBytes),
		MaxRPCBodyBytes:           envInt64("MEMORY_MAX_RPC_BODY_BYTES", 32<<20),
		ChunkCharacters:           envInt("MEMORY_CHUNK_CHARACTERS", 448),
		ChunkOverlapCharacters:    envInt("MEMORY_CHUNK_OVERLAP_CHARACTERS", 64),
		WatchDebounce:             envDuration("MEMORY_WATCH_DEBOUNCE", 750*time.Millisecond),
		AutoRegister:              envBool("MEMORY_AUTO_REGISTER", true),
		RequestTimeout:            envDuration("MEMORY_REQUEST_TIMEOUT", 30*time.Second),
		ShutdownTimeout:           envDuration("MEMORY_SHUTDOWN_TIMEOUT", 15*time.Second),
	}
	if cfg.APIToken == "" && (mode == "serve" || mode == "mcp") {
		cfg.APIToken, err = readSecretFile("MEMORY_API_TOKEN_FILE")
		if err != nil {
			return Config{}, err
		}
	}

	if cfg.DefaultScopeType == "" {
		cfg.DefaultScopeType = "workspace"
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

/**
 * Validate checks configuration required by the selected process mode.
 * @return validation error, or nil
 */
func (c Config) Validate() error {
	switch c.Mode {
	case "migrate":
		if c.DatabaseURL == "" {
			return errors.New("MEMORY_DATABASE_URL is required")
		}

		return nil
	case "serve":
		if c.DatabaseURL == "" {
			return errors.New("MEMORY_DATABASE_URL is required")
		}
		if c.APIToken == "" {
			return errors.New("MEMORY_API_TOKEN is required")
		}
		if c.SignalHMACSecret == "" {
			return errors.New("MEMORY_SIGNAL_HMAC_SECRET is required")
		}
		if c.MaxRPCBodyBytes <= 0 {
			return errors.New("MEMORY_MAX_RPC_BODY_BYTES must be positive")
		}
		if c.ShutdownTimeout <= 0 {
			return errors.New("MEMORY_SHUTDOWN_TIMEOUT must be positive")
		}
	case "mcp":
		if c.APIURL == "" {
			return errors.New("MEMORY_API_URL is required")
		}
		if c.APIToken == "" {
			return errors.New("MEMORY_API_TOKEN is required")
		}
		if c.MaxFileBytes <= 0 {
			return errors.New("MEMORY_MAX_FILE_BYTES must be positive")
		}
		if c.WatchDebounce <= 0 {
			return errors.New("MEMORY_WATCH_DEBOUNCE must be positive")
		}
		if c.RequestTimeout <= 0 {
			return errors.New("MEMORY_REQUEST_TIMEOUT must be positive")
		}

		return nil
	case "version":
		return nil
	default:
		return fmt.Errorf("unsupported mode %q", c.Mode)
	}

	if c.EmbeddingDimensions != embedding.Dimensions {
		return fmt.Errorf(
			"MEMORY_EMBEDDING_DIMENSIONS must be %d, got %d",
			embedding.Dimensions,
			c.EmbeddingDimensions,
		)
	}
	if c.EmbeddingQueryPrefix != "" && c.EmbeddingQueryInstruction != "" {
		return errors.New("MEMORY_EMBEDDING_QUERY_PREFIX and MEMORY_EMBEDDING_QUERY_INSTRUCTION are mutually exclusive")
	}
	if c.ChunkCharacters < 256 {
		return errors.New("MEMORY_CHUNK_CHARACTERS must be at least 256")
	}
	if c.ChunkOverlapCharacters < 0 || c.ChunkOverlapCharacters >= c.ChunkCharacters {
		return errors.New("MEMORY_CHUNK_OVERLAP_CHARACTERS must be non-negative and smaller than chunk size")
	}
	if c.EmbeddingWorkers < 1 || c.EmbeddingBatchSize < 1 {
		return errors.New("embedding worker and batch settings must be positive")
	}
	if c.EmbeddingProvider != "none" && c.EmbeddingProvider != "openai" {
		return fmt.Errorf("unsupported embedding provider %q", c.EmbeddingProvider)
	}
	if c.EmbeddingProvider == "openai" && c.EmbeddingURL == "" {
		return errors.New("MEMORY_EMBEDDING_URL is required for openai provider")
	}

	return nil
}

func loadWorkspaceConfig() (WorkspaceConfig, error) {
	directory, err := os.Getwd()
	if err != nil {
		return WorkspaceConfig{}, fmt.Errorf("resolve working directory: %w", err)
	}

	for {
		path := filepath.Join(directory, ".memory-recall.json")
		data, readErr := os.ReadFile(path)
		if readErr == nil {
			var cfg WorkspaceConfig
			if err := json.Unmarshal(data, &cfg); err != nil {
				return WorkspaceConfig{}, fmt.Errorf("parse %s: %w", path, err)
			}

			return cfg, nil
		}
		if !errors.Is(readErr, os.ErrNotExist) {
			return WorkspaceConfig{}, fmt.Errorf("read %s: %w", path, readErr)
		}

		parent := filepath.Dir(directory)
		if parent == directory {
			return WorkspaceConfig{}, nil
		}
		directory = parent
	}
}

func defaultIdentityFile() (string, error) {
	configDirectory, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("resolve user config directory: %w", err)
	}

	return filepath.Join(configDirectory, "memory-recall-coin", "identity.json"), nil
}

func envString(name, fallback string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}

	return value
}

func envInt(name string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}

	return parsed
}

func envInt64(name string, fallback int64) int64 {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}

	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fallback
	}

	return parsed
}

func envBool(name string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}

	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}

	return parsed
}

func envDuration(name string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}

	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}

	return parsed
}

/**
 * readSecretFile loads a trimmed secret from the file named by an environment variable.
 * @param envName environment variable containing the file path
 * @return secret value or a read error; an unset path returns an empty value
 */
func readSecretFile(envName string) (string, error) {
	path := strings.TrimSpace(os.Getenv(envName))
	if path == "" {
		return "", nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read secret file from %s: %w", envName, err)
	}
	value := strings.TrimSpace(string(data))
	if value == "" {
		return "", fmt.Errorf("secret file from %s is empty", envName)
	}

	return value, nil
}
