package ingest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Alhamdulillah-R/memory-recall-coin/internal/domain"
	"github.com/Alhamdulillah-R/memory-recall-coin/internal/service"
	"github.com/fsnotify/fsnotify"
)

var (
	ErrManagerClosed      = errors.New("ingestion watch manager is closed")
	ErrWatchAlreadyExists = errors.New("path is already being watched in this namespace")
	ErrWatchNotFound      = errors.New("ingestion watch not found")
)

// WatchInfo 描述一个正在运行的递归 path watch。
type WatchInfo struct {
	ID              string                  `json:"id"`
	Namespace       string                  `json:"namespace"`
	RootPath        string                  `json:"root_path"`
	Recursive       bool                    `json:"recursive"`
	Include         []string                `json:"include,omitempty"`
	Exclude         []string                `json:"exclude,omitempty"`
	Parser          string                  `json:"parser"`
	IngestionID     string                  `json:"ingestion_id,omitempty"`
	StartedAt       time.Time               `json:"started_at"`
	LastSyncAt      time.Time               `json:"last_sync_at"`
	SuccessfulSyncs int64                   `json:"successful_syncs"`
	LastError       string                  `json:"last_error,omitempty"`
	LastSummary     domain.IngestionSummary `json:"last_summary"`
}

// Manager 执行一次性 path sync，并管理 debounce 后完整同步的 fsnotify watch。
type Manager struct {
	backend  service.Backend
	scanner  *Scanner
	debounce time.Duration
	logger   *log.Logger

	ctx    context.Context
	cancel context.CancelFunc

	mu       sync.RWMutex
	watches  map[string]*watchJob
	pathKeys map[string]string
	closed   bool
}

type watchJob struct {
	id        string
	key       string
	input     service.SyncSourcesInput
	matcher   pathMatcher
	rootIsDir bool
	watcher   *fsnotify.Watcher
	ctx       context.Context
	cancel    context.CancelFunc
	done      chan struct{}
	trigger   chan struct{}
	watched   map[string]struct{}

	mu   sync.RWMutex
	info WatchInfo
}

/**
 * NewManager 创建本机 source sync 与 watch manager。
 * @param backend      中央 service 或其 HTTP proxy
 * @param maxFileBytes 单文件最大读取字节数
 * @param debounce     文件事件停止后等待多久再执行完整 sync
 * @return             manager 或配置错误
 */
func NewManager(
	backend service.Backend,
	maxFileBytes int64,
	debounce time.Duration,
) (*Manager, error) {
	if backend == nil {
		return nil, errors.New("ingestion backend is required")
	}
	if debounce <= 0 {
		return nil, errors.New("ingestion watch debounce must be positive")
	}

	scanner, err := NewScanner(maxFileBytes)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Manager{
		backend:  backend,
		scanner:  scanner,
		debounce: debounce,
		logger:   log.Default(),
		ctx:      ctx,
		cancel:   cancel,
		watches:  make(map[string]*watchJob),
		pathKeys: make(map[string]string),
	}, nil
}

/**
 * Sync 扫描本机 path，并立即向 backend 提交完整 manifest。
 * @param ctx   request 生命周期
 * @param input path、scope、parser 与 TTL 配置
 * @return      backend summary；本机跳过项会写入 Errors
 */
func (m *Manager) Sync(
	ctx context.Context,
	input service.SyncSourcesInput,
) (domain.IngestionSummary, error) {
	if err := m.ensureOpen(); err != nil {
		return domain.IngestionSummary{}, err
	}

	_, summary, err := m.buildAndSync(ctx, input, false)

	return summary, err
}

/**
 * Watch 完成一次初始 sync，随后递归监听 path 并在 debounce 后执行完整 sync。
 * @param ctx   仅控制 watch 创建与初始 sync；成功后的 watch 由 Stop 或 Close 管理
 * @param input path、scope、过滤、parser 与 TTL 配置
 * @return      watch 状态或创建错误
 */
func (m *Manager) Watch(
	ctx context.Context,
	input service.SyncSourcesInput,
) (WatchInfo, error) {
	input.Namespace = strings.ToLower(strings.TrimSpace(input.Namespace))
	if input.Namespace == "" {
		return WatchInfo{}, errors.New("namespace is required for path watch")
	}

	rootPath, err := resolveRootPath(input.RootPath)
	if err != nil {
		return WatchInfo{}, err
	}
	input.RootPath = rootPath
	input.WatchMode = "watch"

	key := makeWatchKey(input.Namespace, rootPath)
	if err := m.reservePath(key); err != nil {
		return WatchInfo{}, err
	}
	committed := false
	defer func() {
		if !committed {
			m.releasePath(key)
		}
	}()

	builtInput, initialSummary, err := m.buildAndSync(ctx, input, false)
	if err != nil {
		return WatchInfo{}, err
	}

	matcher, err := newPathMatcher(builtInput.Include, builtInput.Exclude)
	if err != nil {
		return WatchInfo{}, err
	}

	rootInfo, err := os.Lstat(rootPath)
	if err != nil {
		return WatchInfo{}, fmt.Errorf("inspect watch root %s: %w", rootPath, err)
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return WatchInfo{}, fmt.Errorf("create filesystem watcher: %w", err)
	}

	jobCtx, cancel := context.WithCancel(m.ctx)
	id, err := newWatchID()
	if err != nil {
		cancel()
		closeErr := watcher.Close()

		return WatchInfo{}, errors.Join(err, closeErr)
	}

	now := time.Now().UTC()
	initialSummary.Watching = true
	initialSummary.WatchID = id
	job := &watchJob{
		id:        id,
		key:       key,
		input:     builtInput,
		matcher:   matcher,
		rootIsDir: rootInfo.IsDir(),
		watcher:   watcher,
		ctx:       jobCtx,
		cancel:    cancel,
		done:      make(chan struct{}),
		trigger:   make(chan struct{}, 1),
		watched:   make(map[string]struct{}),
		info: WatchInfo{
			ID:              id,
			Namespace:       builtInput.Namespace,
			RootPath:        builtInput.RootPath,
			Recursive:       builtInput.Recursive,
			Include:         append([]string(nil), builtInput.Include...),
			Exclude:         append([]string(nil), builtInput.Exclude...),
			Parser:          builtInput.Parser,
			IngestionID:     initialSummary.IngestionID,
			StartedAt:       now,
			LastSyncAt:      now,
			SuccessfulSyncs: 1,
			LastSummary:     initialSummary,
		},
	}

	if err := job.addInitialWatchTargets(); err != nil {
		cancel()
		closeErr := watcher.Close()

		return WatchInfo{}, errors.Join(err, closeErr)
	}
	if err := ctx.Err(); err != nil {
		cancel()
		closeErr := watcher.Close()

		return WatchInfo{}, errors.Join(err, closeErr)
	}
	if err := m.commitWatch(job); err != nil {
		cancel()
		closeErr := watcher.Close()

		return WatchInfo{}, errors.Join(err, closeErr)
	}

	committed = true

	return job.snapshot(), nil
}

/**
 * List 返回当前 manager 管理的全部 watch 快照。
 * @return 按启动时间和 ID 排序的 watch 列表
 */
func (m *Manager) List() []WatchInfo {
	m.mu.RLock()
	jobs := make([]*watchJob, 0, len(m.watches))
	for _, job := range m.watches {
		jobs = append(jobs, job)
	}
	m.mu.RUnlock()

	result := make([]WatchInfo, 0, len(jobs))
	for _, job := range jobs {
		result = append(result, job.snapshot())
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].StartedAt.Equal(result[j].StartedAt) {
			return result[i].ID < result[j].ID
		}

		return result[i].StartedAt.Before(result[j].StartedAt)
	})

	return result
}

/**
 * Stop 停止指定 watch，并等待其后台 sync 退出。
 * @param id Watch 返回的 ID
 * @return   watch 不存在时返回 ErrWatchNotFound
 */
func (m *Manager) Stop(id string) error {
	job, err := m.removeWatch(id)
	if err != nil {
		return err
	}

	job.cancel()
	<-job.done

	return nil
}

/**
 * Close 停止全部 watch，并关闭 manager。
 * @return 当前实现始终返回 nil
 */
func (m *Manager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()

		return nil
	}

	m.closed = true
	jobs := make([]*watchJob, 0, len(m.watches))
	for _, job := range m.watches {
		jobs = append(jobs, job)
	}
	m.watches = make(map[string]*watchJob)
	m.pathKeys = make(map[string]string)
	m.mu.Unlock()

	m.cancel()
	for _, job := range jobs {
		job.cancel()
	}
	for _, job := range jobs {
		<-job.done
	}

	return nil
}

func (m *Manager) buildAndSync(
	ctx context.Context,
	input service.SyncSourcesInput,
	allowMissingRoot bool,
) (service.SyncSourcesInput, domain.IngestionSummary, error) {
	builtInput, skipped, err := m.scanner.Build(ctx, input)
	if err != nil {
		if !allowMissingRoot || !input.PruneMissing || !errors.Is(err, os.ErrNotExist) {
			return input, domain.IngestionSummary{}, err
		}

		rootPath, pathErr := resolveRootPath(input.RootPath)
		if pathErr != nil {
			return input, domain.IngestionSummary{}, pathErr
		}
		if _, rootErr := os.Lstat(rootPath); !errors.Is(rootErr, os.ErrNotExist) {
			if rootErr != nil {
				return input, domain.IngestionSummary{}, fmt.Errorf("inspect missing watch root %s: %w", rootPath, rootErr)
			}

			return input, domain.IngestionSummary{}, err
		}

		builtInput = input
		builtInput.RootPath = rootPath
		builtInput.Files = nil
		if strings.TrimSpace(builtInput.Parser) == "" {
			builtInput.Parser = "auto"
		}
	}

	summary, err := m.backend.SyncSources(ctx, builtInput)
	if err != nil {
		return builtInput, domain.IngestionSummary{}, fmt.Errorf("sync local sources: %w", err)
	}

	summary.RootPath = builtInput.RootPath
	summary.FilesSeen = len(builtInput.Files)
	for _, reason := range skipped {
		summary.Errors = append(summary.Errors, "local scan: "+reason)
	}

	return builtInput, summary, nil
}

func (m *Manager) startWatch(job *watchJob) {
	var workers sync.WaitGroup
	workers.Add(2)

	go func() {
		defer workers.Done()

		m.runWatchEvents(job)
	}()
	go func() {
		defer workers.Done()

		m.runWatchSync(job)
	}()
	go func() {
		workers.Wait()
		close(job.done)
	}()
}

func (m *Manager) runWatchEvents(job *watchJob) {
	defer job.cancel()
	defer func() {
		if err := job.watcher.Close(); err != nil {
			m.logger.Printf("[Error] close path watcher: watch_id=%s error=%v", job.id, err)
		}
	}()

	timer := time.NewTimer(m.debounce)
	defer stopTimer(timer)
	timerChannel := timer.C

	for {
		select {
		case <-job.ctx.Done():
			return
		case event, open := <-job.watcher.Events:
			if !open {
				return
			}
			if !job.eventRelevant(event.Name) {
				continue
			}

			if err := job.handleEvent(event); err != nil {
				job.recordError(err)
				m.logger.Printf(
					"[Error] update recursive watch: watch_id=%s path=%q error=%v",
					job.id,
					event.Name,
					err,
				)
			}

			timerChannel = resetTimer(timer, m.debounce)
		case watchErr, open := <-job.watcher.Errors:
			if !open {
				return
			}

			err := fmt.Errorf("filesystem watch error: %w", watchErr)
			job.recordError(err)
			m.logger.Printf("[Error] filesystem watch failed: watch_id=%s error=%v", job.id, watchErr)
			timerChannel = resetTimer(timer, m.debounce)
		case <-timerChannel:
			select {
			case job.trigger <- struct{}{}:
			default:
			}
			timerChannel = nil
		}
	}
}

func (m *Manager) runWatchSync(job *watchJob) {
	for {
		select {
		case <-job.ctx.Done():
			return
		case <-job.trigger:
			_, summary, err := m.buildAndSync(job.ctx, job.input, true)
			if err != nil {
				if job.ctx.Err() != nil {
					return
				}
				if namespaceDeleted(err) {
					job.recordError(err)
					m.detachWatch(job)
					job.cancel()
					m.logger.Printf(
						"[Warning] stop watch for deleted namespace: watch_id=%s namespace=%q",
						job.id,
						job.input.Namespace,
					)

					return
				}

				job.recordError(err)
				m.logger.Printf(
					"[Error] synchronize watched path: watch_id=%s root=%q error=%v",
					job.id,
					job.input.RootPath,
					err,
				)

				continue
			}

			summary.Watching = true
			summary.WatchID = job.id
			job.recordSuccess(summary)
		}
	}
}

func (m *Manager) ensureOpen() error {
	m.mu.RLock()
	closed := m.closed
	m.mu.RUnlock()

	if closed {
		return ErrManagerClosed
	}

	return nil
}

func (m *Manager) reservePath(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrManagerClosed
	}
	if _, exists := m.pathKeys[key]; exists {
		return fmt.Errorf("%w: %s", ErrWatchAlreadyExists, key)
	}

	m.pathKeys[key] = ""

	return nil
}

func (m *Manager) releasePath(key string) {
	m.mu.Lock()
	if id, exists := m.pathKeys[key]; exists && id == "" {
		delete(m.pathKeys, key)
	}
	m.mu.Unlock()
}

func (m *Manager) commitWatch(job *watchJob) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrManagerClosed
	}
	if id, exists := m.pathKeys[job.key]; !exists || id != "" {
		return fmt.Errorf("%w: %s", ErrWatchAlreadyExists, job.key)
	}
	if _, exists := m.watches[job.id]; exists {
		return fmt.Errorf("watch ID collision: %s", job.id)
	}

	m.pathKeys[job.key] = job.id
	m.watches[job.id] = job
	m.startWatch(job)

	return nil
}

func (m *Manager) removeWatch(id string) (*watchJob, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	job, exists := m.watches[id]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrWatchNotFound, id)
	}

	delete(m.watches, id)
	delete(m.pathKeys, job.key)

	return job, nil
}

func (m *Manager) detachWatch(job *watchJob) {
	m.mu.Lock()
	if current, exists := m.watches[job.id]; exists && current == job {
		delete(m.watches, job.id)
		delete(m.pathKeys, job.key)
	}
	m.mu.Unlock()
}

func namespaceDeleted(err error) bool {
	var serviceErr *service.Error
	if !errors.As(err, &serviceErr) || serviceErr.Code != service.CodeFailedPrecondition {
		return false
	}
	if serviceErr.Details == nil {
		return false
	}

	_, deleted := serviceErr.Details["deleted_namespace"]
	return deleted
}

func (j *watchJob) addInitialWatchTargets() error {
	parentPath := filepath.Dir(j.input.RootPath)
	if err := j.addDirectory(parentPath); err != nil {
		return fmt.Errorf("watch ingestion root parent %s: %w", parentPath, err)
	}
	if !j.rootIsDir {
		return nil
	}

	return j.addDirectoryTree(j.input.RootPath)
}

func (j *watchJob) addDirectoryTree(rootPath string) error {
	err := filepath.WalkDir(rootPath, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return fmt.Errorf("walk watch directory %s: %w", path, walkErr)
		}
		if !entry.IsDir() {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return filepath.SkipDir
		}
		if path != rootPath {
			relativePath, err := filepath.Rel(j.input.RootPath, path)
			if err != nil {
				return fmt.Errorf("resolve watched relative path %s: %w", path, err)
			}
			if !j.input.Recursive || j.matcher.excludesDirectory(filepath.ToSlash(relativePath)) {
				return filepath.SkipDir
			}
		}

		return j.addDirectory(path)
	})
	if err != nil {
		return fmt.Errorf("add recursive watches under %s: %w", rootPath, err)
	}

	return nil
}

func (j *watchJob) addDirectory(path string) error {
	path = filepath.Clean(path)
	key := comparablePath(path)
	if _, exists := j.watched[key]; exists {
		return nil
	}

	if err := j.watcher.Add(path); err != nil {
		return err
	}

	j.watched[key] = struct{}{}

	return nil
}

func (j *watchJob) handleEvent(event fsnotify.Event) error {
	if event.Op&(fsnotify.Remove|fsnotify.Rename) != 0 {
		j.forgetWatchesUnder(event.Name)
	}
	if event.Op&fsnotify.Create == 0 || !j.rootIsDir {
		return nil
	}

	info, err := os.Lstat(event.Name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}

		return fmt.Errorf("inspect created path %s: %w", event.Name, err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil
	}

	relativePath, err := filepath.Rel(j.input.RootPath, event.Name)
	if err != nil {
		return fmt.Errorf("resolve created directory %s: %w", event.Name, err)
	}
	if relativePath == "." {
		return j.addDirectoryTree(event.Name)
	}
	if !j.input.Recursive {
		return nil
	}
	if relativePath != "." && j.matcher.excludesDirectory(filepath.ToSlash(relativePath)) {
		return nil
	}

	return j.addDirectoryTree(event.Name)
}

func (j *watchJob) eventRelevant(eventPath string) bool {
	eventPath = filepath.Clean(eventPath)
	if !j.rootIsDir {
		return pathsEqual(eventPath, j.input.RootPath)
	}

	return pathWithin(j.input.RootPath, eventPath)
}

func (j *watchJob) forgetWatchesUnder(path string) {
	for watchedPath := range j.watched {
		if pathWithin(path, watchedPath) {
			delete(j.watched, watchedPath)
		}
	}
}

func (j *watchJob) recordError(err error) {
	j.mu.Lock()
	j.info.LastError = err.Error()
	j.mu.Unlock()
}

func (j *watchJob) recordSuccess(summary domain.IngestionSummary) {
	j.mu.Lock()
	j.info.LastSyncAt = time.Now().UTC()
	j.info.SuccessfulSyncs++
	j.info.LastError = ""
	j.info.LastSummary = summary
	if summary.IngestionID != "" {
		j.info.IngestionID = summary.IngestionID
	}
	j.mu.Unlock()
}

func (j *watchJob) snapshot() WatchInfo {
	j.mu.RLock()
	info := j.info
	j.mu.RUnlock()

	info.Include = append([]string(nil), info.Include...)
	info.Exclude = append([]string(nil), info.Exclude...)
	info.LastSummary.Errors = append([]string(nil), info.LastSummary.Errors...)

	return info
}

func makeWatchKey(namespace string, rootPath string) string {
	return strings.ToLower(strings.TrimSpace(namespace)) + "\x00" + comparablePath(rootPath)
}

func comparablePath(path string) string {
	path = filepath.Clean(path)
	if runtime.GOOS == "windows" {
		return strings.ToLower(path)
	}

	return path
}

func pathsEqual(left string, right string) bool {
	return comparablePath(left) == comparablePath(right)
}

func pathWithin(rootPath string, candidatePath string) bool {
	rootPath = comparablePath(rootPath)
	candidatePath = comparablePath(candidatePath)

	relativePath, err := filepath.Rel(rootPath, candidatePath)
	if err != nil {
		return false
	}

	return relativePath == "." || (relativePath != ".." && !strings.HasPrefix(relativePath, ".."+string(filepath.Separator)))
}

func newWatchID() (string, error) {
	data := make([]byte, 12)
	if _, err := rand.Read(data); err != nil {
		return "", fmt.Errorf("generate watch ID: %w", err)
	}

	return "watch_" + hex.EncodeToString(data), nil
}

func resetTimer(timer *time.Timer, delay time.Duration) <-chan time.Time {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}

	timer.Reset(delay)

	return timer.C
}

func stopTimer(timer *time.Timer) {
	if timer.Stop() {
		return
	}

	select {
	case <-timer.C:
	default:
	}
}
