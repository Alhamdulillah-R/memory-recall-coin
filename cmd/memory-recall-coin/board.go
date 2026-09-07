package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Alhamdulillah-R/memory-recall-coin/internal/api"
	"github.com/Alhamdulillah-R/memory-recall-coin/internal/config"
	"github.com/Alhamdulillah-R/memory-recall-coin/internal/domain"
	"github.com/Alhamdulillah-R/memory-recall-coin/internal/service"
)

const (
	boardWaitRequestTimeout   = 70 * time.Second
	boardWaitCallSeconds      = 45
	boardWaitHookStdinTimeout = 2 * time.Second
	boardWaitParentCheck      = 5 * time.Second
	boardWaitRetryFloor       = 5 * time.Second
	boardWaitRetryCeiling     = time.Minute
	boardWaitGiveUpAfter      = 15 * time.Minute
	boardWaitWakeExitCode     = 2
	boardWaitDefaultLookback  = 30 * time.Minute
)

// hookPayload 是 Claude Code 餵給 command hook 的 stdin JSON，只取需要的欄位。
type hookPayload struct {
	SessionID     string `json:"session_id"`
	HookEventName string `json:"hook_event_name"`
}

/**
 * runBoardCommand 是給 hook 用的 board CLI：counts 印一行計數，wait 阻塞到板上有新活動。
 * @param args 子命令與旗標
 * @return 執行錯誤；wait 以 exitError code 2 通知 hook 喚醒 agent
 */
func runBoardCommand(cfg config.Config, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: memory-recall-coin board counts | board wait [--tags a,b] [--max-wait 8h]")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	client, err := api.NewClient(api.ClientConfig{
		BaseURL:              cfg.APIURL,
		Token:                cfg.APIToken,
		IdentityFile:         cfg.IdentityFile,
		DefaultNamespace:     cfg.DefaultNamespace,
		DefaultWorkspaceCode: cfg.DefaultWorkspaceCode,
		DefaultScopeType:     cfg.DefaultScopeType,
		SessionID:            cfg.SessionID,
		AutoRegister:         cfg.AutoRegister,
		Timeout:              boardWaitRequestTimeout,
	})
	if err != nil {
		return err
	}

	switch args[0] {
	case "counts":
		return runBoardCounts(ctx, client, cfg.SessionID)
	case "wait":
		return runBoardWait(ctx, client, cfg.SessionID, args[1:])
	default:
		return fmt.Errorf("unknown board subcommand %q; use counts or wait", args[0])
	}
}

/**
 * runBoardCounts 印一行未 resolve 計數，並把伺服器時鐘記成這個 session 之後要從哪裡開始看板。
 */
func runBoardCounts(ctx context.Context, client *api.Client, sessionID string) error {
	counts, err := client.BoardCounts(ctx, service.BoardCountsInput{})
	if err != nil {
		return err
	}
	fmt.Println(counts.Line)
	if sessionID == "" {
		return nil
	}

	probe, err := client.WaitBoard(ctx, service.BoardWaitInput{})
	if err != nil {
		return err
	}

	return writeBoardSince(sessionID, probe.Now)
}

/**
 * runBoardWait 阻塞到板上出現別人的新活動，然後以 exit 2 把這個 session 叫醒。
 */
func runBoardWait(ctx context.Context, client *api.Client, sessionID string, args []string) error {
	flags := flag.NewFlagSet("board wait", flag.ContinueOnError)
	tagList := flags.String("tags", "", "comma-separated namespace tags to watch; empty watches every tag")
	maxWait := flags.Duration("max-wait", 0, "give up silently after this long; 0 waits until the parent process exits")
	lookback := flags.Duration("lookback", boardWaitDefaultLookback, "how far back this watcher may look, which also caps how stale a recorded position may be; 0 only reports activity after it starts")
	if err := flags.Parse(args); err != nil {
		return err
	}
	tags := splitTags(*tagList)
	if sessionID == "" {
		sessionID = readHookPayload().SessionID
	}

	lockKey := sessionID
	if lockKey == "" {
		lockKey = "pid-" + strconv.Itoa(os.Getpid())
	}
	release, duplicate, err := acquireWatcherLock(lockKey)
	if err != nil {
		return err
	}
	if duplicate {
		return nil
	}
	defer release()

	parentPID := os.Getppid()
	deadline := time.Time{}
	if *maxWait > 0 {
		deadline = time.Now().Add(*maxWait)
	}

	start, err := initialBoardSince(ctx, client, sessionID, *lookback)
	if err != nil {
		return err
	}
	since := &domain.Timestamp{Time: start}
	failingSince := time.Time{}
	retryDelay := boardWaitRetryFloor

	for {
		if os.Getppid() != parentPID {
			return nil
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			return nil
		}

		result, err := client.WaitBoard(ctx, service.BoardWaitInput{
			Tags:           tags,
			Since:          since,
			TimeoutSeconds: boardWaitCallSeconds,
		})
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if failingSince.IsZero() {
				failingSince = time.Now()
			}
			if time.Since(failingSince) > boardWaitGiveUpAfter {
				return &exitError{
					code:    boardWaitWakeExitCode,
					message: "board watcher gave up after " + boardWaitGiveUpAfter.String() + " of failures: " + err.Error() + "; call board_counts yourself and start a new turn to re-arm it",
				}
			}
			if !sleepWithParentCheck(ctx, retryDelay, parentPID) {
				return nil
			}
			retryDelay = min(retryDelay*2, boardWaitRetryCeiling)
			continue
		}
		failingSince = time.Time{}
		retryDelay = boardWaitRetryFloor
		if err := writeBoardSince(sessionID, result.Now); err != nil {
			return err
		}
		if result.Changed {
			return &exitError{
				code:    boardWaitWakeExitCode,
				message: result.Line + "\nIf one of these tags is yours, run board_read for it; otherwise ignore this and stop.",
			}
		}
		since = &domain.Timestamp{Time: result.Now}
	}
}

// readHookPayload 在 stdin 不是終端時讀 hook JSON；手動執行時 stdin 通常是終端，直接略過。
func readHookPayload() hookPayload {
	info, err := os.Stdin.Stat()
	if err != nil || info.Mode()&os.ModeCharDevice != 0 {
		return hookPayload{}
	}
	done := make(chan hookPayload, 1)
	go func() {
		var payload hookPayload
		data, err := io.ReadAll(io.LimitReader(os.Stdin, 1<<20))
		if err == nil {
			_ = json.Unmarshal(data, &payload)
		}
		done <- payload
	}()
	select {
	case payload := <-done:
		return payload
	case <-time.After(boardWaitHookStdinTimeout):
		return hookPayload{}
	}
}

// acquireWatcherLock 保證同一個 session 只有一個 watcher；每次 Stop 都會再起一個，多的直接退出。
func acquireWatcherLock(key string) (func(), bool, error) {
	directory, err := watcherStateDirectory()
	if err != nil {
		return nil, false, err
	}
	path := filepath.Join(directory, "board-wait."+sanitizeLockKey(key)+".pid")
	if data, err := os.ReadFile(path); err == nil {
		if pid, err := strconv.Atoi(strings.TrimSpace(string(data))); err == nil && pid != os.Getpid() && processAlive(pid) {
			return nil, true, nil
		}
	}
	if err := os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
		return nil, false, fmt.Errorf("write watcher lock: %w", err)
	}

	return func() { _ = os.Remove(path) }, false, nil
}

/**
 * initialBoardSince 決定這次 watcher 從哪看起：優先接續這個 session 上次記到的位置，沒有才回頭看 lookback 這一段。
 */
func initialBoardSince(
	ctx context.Context,
	client *api.Client,
	sessionID string,
	lookback time.Duration,
) (time.Time, error) {
	probe, err := client.WaitBoard(ctx, service.BoardWaitInput{})
	if err != nil {
		return time.Time{}, err
	}
	if sessionID == "" {
		return probe.Now, nil
	}

	earliest := probe.Now.Add(-lookback)
	recorded, ok, err := readBoardSince(sessionID)
	if err != nil {
		return time.Time{}, err
	}
	if ok && recorded.After(earliest) {
		return recorded, nil
	}

	return earliest, nil
}

// watcherStateDirectory 放 lock 與 since marker，跟著 runtime dir 在重開機時一起清掉。
func watcherStateDirectory() (string, error) {
	directory := os.Getenv("XDG_RUNTIME_DIR")
	if directory == "" {
		directory = os.TempDir()
	}
	directory = filepath.Join(directory, "memory-recall-coin")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return "", fmt.Errorf("create watcher state directory: %w", err)
	}

	return directory, nil
}

// readBoardSince 讀這個 session 上次看到哪；檔案不存在或內容壞掉都當作沒有記錄過。
func readBoardSince(sessionID string) (time.Time, bool, error) {
	path, err := boardSincePath(sessionID)
	if err != nil {
		return time.Time{}, false, err
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("read board since marker: %w", err)
	}
	recorded, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(string(data)))
	if err != nil {
		return time.Time{}, false, nil
	}

	return recorded, true, nil
}

// writeBoardSince 記下這個 session 看到哪；沒有 session id 就不留記錄，行為退回每次都從 watcher 起點開始看。
func writeBoardSince(sessionID string, moment time.Time) error {
	if sessionID == "" {
		return nil
	}
	path, err := boardSincePath(sessionID)
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, []byte(moment.Format(time.RFC3339Nano)), 0o600); err != nil {
		return fmt.Errorf("write board since marker: %w", err)
	}

	return nil
}

func boardSincePath(sessionID string) (string, error) {
	directory, err := watcherStateDirectory()
	if err != nil {
		return "", err
	}

	return filepath.Join(directory, "board-wait."+sanitizeLockKey(sessionID)+".since"), nil
}

func sanitizeLockKey(key string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		default:
			return '_'
		}
	}, key)
}

func sleepWithParentCheck(ctx context.Context, delay time.Duration, parentPID int) bool {
	deadline := time.Now().Add(delay)
	for time.Now().Before(deadline) {
		if os.Getppid() != parentPID {
			return false
		}
		step := min(boardWaitParentCheck, time.Until(deadline))
		select {
		case <-ctx.Done():
			return false
		case <-time.After(step):
		}
	}

	return true
}

func splitTags(value string) []string {
	tags := make([]string, 0)
	for _, tag := range strings.Split(value, ",") {
		tag = strings.TrimSpace(tag)
		if tag != "" {
			tags = append(tags, tag)
		}
	}

	return tags
}
