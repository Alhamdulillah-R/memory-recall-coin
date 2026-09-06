package mcpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const outputNoticeProperty = "notice"

const binaryCheckInterval = 15 * time.Second

// binaryWatch 記住 bridge 啟動時磁碟上 binary 的長相；之後被換掉就代表這個進程已經是舊版。
type binaryWatch struct {
	path    string
	modTime time.Time
	size    int64

	mu        sync.Mutex
	lastCheck time.Time
	isStale   bool
	staleInfo string
}

func newBinaryWatch() *binaryWatch {
	path, err := os.Executable()
	if err != nil {
		return nil
	}
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		path = resolved
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil
	}

	return &binaryWatch{path: path, modTime: info.ModTime(), size: info.Size()}
}

// stale 最多每 15 秒 stat 一次；一旦判定過期就不再翻回來，因為進程不會自己變新。
func (w *binaryWatch) stale() (string, bool) {
	if w == nil {
		return "", false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.isStale {
		return w.staleInfo, true
	}
	now := time.Now()
	if now.Sub(w.lastCheck) < binaryCheckInterval {
		return "", false
	}
	w.lastCheck = now
	info, err := os.Stat(w.path)
	if err != nil {
		return "", false
	}
	if info.ModTime().Equal(w.modTime) && info.Size() == w.size {
		return "", false
	}
	w.isStale = true
	w.staleInfo = fmt.Sprintf(
		"notice: %s changed on disk at %s (this bridge started with the build from %s); call plugin_reload for memory-recall to pick up the new tools and behaviour before continuing",
		w.path,
		info.ModTime().UTC().Format(time.RFC3339),
		w.modTime.UTC().Format(time.RFC3339),
	)

	return w.staleInfo, true
}

/**
 * staleBinaryMiddleware 在每個 tool 結果後面附一段提示，讓還在跑舊 binary 的 session 自己發現要 reload。
 * @param watch 啟動時記下的 binary 狀態；nil 時中介層不做事
 * @return MCP receiving middleware
 */
func staleBinaryMiddleware(watch *binaryWatch) mcp.Middleware {
	return func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, request mcp.Request) (mcp.Result, error) {
			result, err := next(ctx, method, request)
			if err != nil || method != callToolMethod {
				return result, err
			}
			callResult, ok := result.(*mcp.CallToolResult)
			if !ok {
				return result, nil
			}
			if notice, stale := watch.stale(); stale {
				attachNotice(callResult, notice)
			}

			return callResult, nil
		}
	}
}

// allowOutputNotice 讓每個成功輸出的 schema 都認得 notice，這樣中介層塞進去的提示不會被 client 的 output 驗證擋掉。
func allowOutputNotice(schema *jsonschema.Schema) {
	if schema == nil || schema.Type != "object" {
		return
	}
	if schema.Properties == nil {
		schema.Properties = make(map[string]*jsonschema.Schema, 1)
	}
	schema.Properties[outputNoticeProperty] = &jsonschema.Schema{
		Type:        "string",
		Description: "present only when this bridge process is stale and plugin_reload is needed",
	}
}

// attachNotice 把提示放進 structuredContent（Claude Code 只呈現這一份），文字內容跟著重生。
func attachNotice(result *mcp.CallToolResult, notice string) {
	if result.IsError {
		return
	}
	encoded, err := json.Marshal(result.StructuredContent)
	if err != nil {
		return
	}
	fields := make(map[string]any)
	if err := json.Unmarshal(encoded, &fields); err != nil {
		return
	}
	fields[outputNoticeProperty] = notice
	rendered, err := json.Marshal(fields)
	if err != nil {
		return
	}
	result.StructuredContent = fields
	result.Content = []mcp.Content{&mcp.TextContent{Text: string(rendered)}}
}
