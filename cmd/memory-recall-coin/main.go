package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/Alhamdulillah-R/memory-recall-coin/internal/api"
	"github.com/Alhamdulillah-R/memory-recall-coin/internal/config"
	"github.com/Alhamdulillah-R/memory-recall-coin/internal/database"
	"github.com/Alhamdulillah-R/memory-recall-coin/internal/embedding"
	"github.com/Alhamdulillah-R/memory-recall-coin/internal/ingest"
	"github.com/Alhamdulillah-R/memory-recall-coin/internal/mcpserver"
	"github.com/Alhamdulillah-R/memory-recall-coin/internal/service"
)

var (
	version   = "dev"
	revision  = "unknown"
	buildTime = "unknown"
)

const mcpServerClosingCode int64 = -32004

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	mode := "mcp"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}
	if mode == "--version" || mode == "-version" {
		mode = "version"
	}

	if err := run(mode, logger); err != nil {
		logger.Error("[Error] memory-recall-coin stopped", "mode", mode, "error", err)
		os.Exit(1)
	}
}

func run(mode string, logger *slog.Logger) error {
	if mode == "version" {
		fmt.Printf("memory-recall-coin %s revision=%s built=%s\n", version, revision, buildTime)
		return nil
	}

	cfg, err := config.Load(mode)
	if err != nil {
		return err
	}

	switch mode {
	case "serve":
		return runCentralService(cfg, logger)
	case "mcp":
		return runLocalMCP(cfg, logger)
	case "migrate":
		return runMigrations(cfg)
	default:
		return fmt.Errorf("usage: memory-recall-coin [mcp|serve|migrate|version]")
	}
}

func runCentralService(cfg config.Config, logger *slog.Logger) error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	pool, err := openMigratedPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	provider, err := createEmbeddingProvider(cfg)
	if err != nil {
		return err
	}
	store := service.NewStore(pool, provider, service.StoreConfig{
		EmbeddingProviderName:  embeddingProviderIdentity(cfg),
		SignalHMACSecret:       cfg.SignalHMACSecret,
		ChunkCharacters:        cfg.ChunkCharacters,
		ChunkOverlapCharacters: cfg.ChunkOverlapCharacters,
		Version:                version,
	})
	if err := store.ReconcileEmbeddingJobs(ctx); err != nil {
		return err
	}

	workerCtx, stopWorkers := context.WithCancel(ctx)
	defer stopWorkers()
	workersDone := make(chan struct{})
	go func() {
		defer close(workersDone)
		store.RunEmbeddingWorkers(
			workerCtx,
			cfg.EmbeddingWorkers,
			cfg.EmbeddingBatchSize,
			logger,
		)
	}()
	go reconcileEmbeddingsAfterRollout(ctx, store, logger)

	httpAPI := api.NewServer(store, api.ServerConfig{
		Token:        cfg.APIToken,
		MaxBodyBytes: cfg.MaxRPCBodyBytes,
		Logger:       logger,
	})
	httpServer := &http.Server{
		Addr:              cfg.ListenAddress,
		Handler:           httpAPI.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    1 << 20,
	}

	serverError := make(chan error, 1)
	go func() {
		logger.Info("central memory service listening", "address", cfg.ListenAddress, "version", version)
		serverError <- httpServer.ListenAndServe()
	}()

	select {
	case err := <-serverError:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve central HTTP API: %w", err)
		}
	case <-ctx.Done():
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shut down central HTTP API: %w", err)
	}
	stopWorkers()
	select {
	case <-workersDone:
	case <-shutdownCtx.Done():
		return fmt.Errorf("embedding workers did not stop before shutdown deadline")
	}

	return nil
}

func reconcileEmbeddingsAfterRollout(ctx context.Context, store *service.Store, logger *slog.Logger) {
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return
	case <-timer.C:
	}

	if err := store.ReconcileEmbeddingJobs(ctx); err != nil && ctx.Err() == nil {
		logger.Error("[Error] reconcile embeddings after rollout", "error", err)
	}
}

func runLocalMCP(cfg config.Config, logger *slog.Logger) error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	client, err := api.NewClient(api.ClientConfig{
		BaseURL:              cfg.APIURL,
		Token:                cfg.APIToken,
		IdentityFile:         cfg.IdentityFile,
		DefaultNamespace:     cfg.DefaultNamespace,
		DefaultWorkspaceCode: cfg.DefaultWorkspaceCode,
		DefaultScopeType:     cfg.DefaultScopeType,
		AutoRegister:         cfg.AutoRegister,
		Timeout:              cfg.RequestTimeout,
	})
	if err != nil {
		return err
	}
	manager, err := ingest.NewManager(client, cfg.MaxFileBytes, cfg.WatchDebounce)
	if err != nil {
		return err
	}
	defer func() {
		if err := manager.Close(); err != nil {
			logger.Error("[Error] close ingestion manager", "error", err)
		}
	}()

	server := mcpserver.New(client, mcpserver.Options{
		Version:          version,
		Logger:           logger,
		IngestionManager: manager,
		DefaultNamespace: cfg.DefaultNamespace,
	})
	logger.Info("local stdio MCP bridge started", "api_url", cfg.APIURL, "version", version)
	if err := server.Run(ctx, &mcp.StdioTransport{}); err != nil && ctx.Err() == nil && !isNormalStdioClose(err) {
		return fmt.Errorf("run stdio MCP server: %w", err)
	}

	return nil
}

func isNormalStdioClose(err error) bool {
	if errors.Is(err, io.EOF) {
		return true
	}

	var rpcError *jsonrpc.Error
	return errors.As(err, &rpcError) && rpcError.Code == mcpServerClosingCode && strings.HasSuffix(err.Error(), ": EOF")
}

func runMigrations(cfg config.Config) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	bootstrapPool, err := database.Open(ctx, cfg.DatabaseURL, false)
	if err != nil {
		return err
	}
	defer bootstrapPool.Close()

	return database.Migrate(ctx, bootstrapPool)
}

func openMigratedPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	bootstrapPool, err := database.Open(ctx, databaseURL, false)
	if err != nil {
		return nil, err
	}
	if err := database.Migrate(ctx, bootstrapPool); err != nil {
		bootstrapPool.Close()
		return nil, err
	}
	bootstrapPool.Close()

	return database.Open(ctx, databaseURL, true)
}

func createEmbeddingProvider(cfg config.Config) (embedding.Provider, error) {
	switch cfg.EmbeddingProvider {
	case "none":
		return embedding.NewDisabled(), nil
	case "openai":
		return embedding.NewOpenAI(embedding.OpenAIConfig{
			BaseURL:          cfg.EmbeddingURL,
			APIKey:           cfg.EmbeddingAPIKey,
			Model:            cfg.EmbeddingModel,
			Dimensions:       cfg.EmbeddingDimensions,
			QueryPrefix:      cfg.EmbeddingQueryPrefix,
			QueryInstruction: cfg.EmbeddingQueryInstruction,
			Timeout:          cfg.RequestTimeout,
		})
	default:
		return nil, fmt.Errorf("unsupported embedding provider %q", cfg.EmbeddingProvider)
	}
}

func embeddingProviderIdentity(cfg config.Config) string {
	if cfg.EmbeddingProvider == "openai" {
		return cfg.EmbeddingProvider + ":" + cfg.EmbeddingModel
	}

	return cfg.EmbeddingProvider
}

func init() {
	if revision != "unknown" {
		return
	}
	buildInfo, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}
	for _, setting := range buildInfo.Settings {
		if setting.Key == "vcs.revision" {
			revision = setting.Value
		}
	}
}
