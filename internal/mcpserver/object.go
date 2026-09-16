package mcpserver

import (
	"context"
	"errors"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/Alhamdulillah-R/memory-recall-coin/internal/consus"
	"github.com/Alhamdulillah-R/memory-recall-coin/internal/service"
)

// ObjectPutInput 上傳本機路徑；bytes 不經過 MCP 通道，由 bridge 自己讀檔送出。
type ObjectPutInput struct {
	Path        string         `json:"path" jsonschema:"absolute or relative path on the machine running this MCP bridge; the file is read and uploaded locally and never travels through the tool call"`
	Namespace   string         `json:"namespace" jsonschema:"namespace path, shared with memory namespaces so both sides describe the same tree"`
	Name        string         `json:"name,omitempty" jsonschema:"object name; defaults to the file name"`
	TTL         string         `json:"ttl,omitempty" jsonschema:"retention such as 30d or 12h; omit to keep it forever"`
	Tags        []string       `json:"tags,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty" jsonschema:"arbitrary JSON; put the owning memory id here as {\"memory_id\": \"mem_...\"}"`
	MaxVersions int            `json:"max_versions,omitempty" jsonschema:"how many versions to keep; server default is 5"`
	OnConflict  string         `json:"on_conflict,omitempty"`
	Note        string         `json:"note,omitempty" jsonschema:"note recorded on this version"`
}

// ObjectGetInput 取回 object 到本機路徑。
type ObjectGetInput struct {
	ObjectID    string `json:"object_id"`
	Version     int    `json:"version,omitempty" jsonschema:"historical version; omit for the current one"`
	Destination string `json:"destination" jsonschema:"path on the machine running this MCP bridge to write the bytes to"`
}

// ObjectGetResult 回報落地位置與大小。
type ObjectGetResult struct {
	ObjectID string `json:"object_id"`
	Path     string `json:"path"`
	Bytes    int64  `json:"bytes"`
}

// ObjectListInput 列出 namespace 底下的 object。
type ObjectListInput struct {
	Namespace string `json:"namespace,omitempty"`
	Subtree   bool   `json:"subtree,omitempty" jsonschema:"include descendant namespaces"`
	Tag       string `json:"tag,omitempty"`
	Limit     int    `json:"limit,omitempty"`
}

// ObjectListResult 包住清單，因為 MCP 的輸出 schema 需要物件根。
type ObjectListResult struct {
	Count   int             `json:"count"`
	Objects []consus.Object `json:"objects"`
}

// ObjectIDInput 只認一個 object。
type ObjectIDInput struct {
	ObjectID string `json:"object_id"`
}

// ObjectResolveInput 用名字換 object。
type ObjectResolveInput struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

// ObjectPatchInput 只送有給的欄位。
type ObjectPatchInput struct {
	ObjectID    string         `json:"object_id"`
	Name        *string        `json:"name,omitempty"`
	Tags        []string       `json:"tags,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
	MaxVersions *int           `json:"max_versions,omitempty"`
	ExpiresAt   *string        `json:"expires_at,omitempty" jsonschema:"RFC3339 timestamp, or empty string to clear the expiry"`
}

// ObjectTouchInput 重算過期時間。
type ObjectTouchInput struct {
	ObjectID string `json:"object_id"`
	TTL      string `json:"ttl" jsonschema:"new retention measured from now, such as 90d"`
}

// ObjectDeleteResult 軟刪後的回執。
type ObjectDeleteResult struct {
	ObjectID string `json:"object_id"`
	Status   string `json:"status"`
}

func (h *Handlers) objectPut(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	input ObjectPutInput,
) (*mcp.CallToolResult, consus.PutPathResult, error) {
	client, err := h.requireConsus()
	if err != nil {
		return nil, consus.PutPathResult{}, err
	}
	namespace, err := h.resolveLocalNamespace(ctx, input.Namespace, nil)
	if err != nil {
		return nil, consus.PutPathResult{}, err
	}

	result, err := consus.PutPath(ctx, client, consus.PutPathInput{
		Path:        input.Path,
		Namespace:   namespace,
		Name:        input.Name,
		TTL:         input.TTL,
		Tags:        input.Tags,
		Metadata:    input.Metadata,
		MaxVersions: input.MaxVersions,
		OnConflict:  input.OnConflict,
		Note:        input.Note,
	})
	if err != nil {
		return nil, consus.PutPathResult{}, objectError(err)
	}

	return nil, result, nil
}

func (h *Handlers) objectGet(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	input ObjectGetInput,
) (*mcp.CallToolResult, ObjectGetResult, error) {
	client, err := h.requireConsus()
	if err != nil {
		return nil, ObjectGetResult{}, err
	}
	if strings.TrimSpace(input.ObjectID) == "" {
		return nil, ObjectGetResult{}, service.NewError(service.CodeInvalidArgument, "object_id is required")
	}

	written, err := consus.GetPath(ctx, client, input.ObjectID, input.Version, input.Destination)
	if err != nil {
		return nil, ObjectGetResult{}, objectError(err)
	}

	return nil, ObjectGetResult{ObjectID: input.ObjectID, Path: input.Destination, Bytes: written}, nil
}

func (h *Handlers) objectList(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	input ObjectListInput,
) (*mcp.CallToolResult, ObjectListResult, error) {
	client, err := h.requireConsus()
	if err != nil {
		return nil, ObjectListResult{}, err
	}

	objects, err := client.List(ctx, consus.ListInput{
		Namespace: input.Namespace,
		Subtree:   input.Subtree,
		Tag:       input.Tag,
		Limit:     input.Limit,
	})
	if err != nil {
		return nil, ObjectListResult{}, objectError(err)
	}

	return nil, ObjectListResult{Count: len(objects), Objects: objects}, nil
}

func (h *Handlers) objectMeta(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	input ObjectIDInput,
) (*mcp.CallToolResult, consus.Object, error) {
	client, err := h.requireConsus()
	if err != nil {
		return nil, consus.Object{}, err
	}

	result, err := client.Meta(ctx, input.ObjectID)
	if err != nil {
		return nil, consus.Object{}, objectError(err)
	}

	return nil, result, nil
}

func (h *Handlers) objectResolve(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	input ObjectResolveInput,
) (*mcp.CallToolResult, consus.Object, error) {
	client, err := h.requireConsus()
	if err != nil {
		return nil, consus.Object{}, err
	}

	result, err := client.Resolve(ctx, input.Namespace, input.Name)
	if err != nil {
		return nil, consus.Object{}, objectError(err)
	}

	return nil, result, nil
}

func (h *Handlers) objectPatch(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	input ObjectPatchInput,
) (*mcp.CallToolResult, consus.Object, error) {
	client, err := h.requireConsus()
	if err != nil {
		return nil, consus.Object{}, err
	}

	result, err := client.Patch(ctx, input.ObjectID, consus.PatchInput{
		Name:        input.Name,
		Tags:        input.Tags,
		Metadata:    input.Metadata,
		MaxVersions: input.MaxVersions,
		ExpiresAt:   input.ExpiresAt,
	})
	if err != nil {
		return nil, consus.Object{}, objectError(err)
	}

	return nil, result, nil
}

func (h *Handlers) objectTouch(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	input ObjectTouchInput,
) (*mcp.CallToolResult, consus.Object, error) {
	client, err := h.requireConsus()
	if err != nil {
		return nil, consus.Object{}, err
	}
	if strings.TrimSpace(input.TTL) == "" {
		return nil, consus.Object{}, service.NewError(service.CodeInvalidArgument, "ttl is required")
	}

	result, err := client.Touch(ctx, input.ObjectID, input.TTL)
	if err != nil {
		return nil, consus.Object{}, objectError(err)
	}

	return nil, result, nil
}

func (h *Handlers) objectDelete(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	input ObjectIDInput,
) (*mcp.CallToolResult, ObjectDeleteResult, error) {
	client, err := h.requireConsus()
	if err != nil {
		return nil, ObjectDeleteResult{}, err
	}

	if err := client.Delete(ctx, input.ObjectID); err != nil {
		return nil, ObjectDeleteResult{}, objectError(err)
	}

	return nil, ObjectDeleteResult{ObjectID: input.ObjectID, Status: "deleted"}, nil
}

func (h *Handlers) objectRestore(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	input ObjectIDInput,
) (*mcp.CallToolResult, consus.Object, error) {
	client, err := h.requireConsus()
	if err != nil {
		return nil, consus.Object{}, err
	}

	result, err := client.Restore(ctx, input.ObjectID)
	if err != nil {
		return nil, consus.Object{}, objectError(err)
	}

	return nil, result, nil
}

// requireConsus 擋掉沒有本機 consus 客戶端的呼叫，跟 memory_ingest_path 同一套理由。
func (h *Handlers) requireConsus() (*consus.Client, error) {
	if h.consusClient == nil {
		return nil, service.NewError(
			service.CodeUnavailable,
			"object storage is available only from the stdio MCP bridge with CONSUS_URL and CONSUS_TOKEN configured",
		)
	}

	return h.consusClient, nil
}

// objectError 把 consus 的傳輸錯誤包成 service 錯誤，讓工具層的錯誤形狀一致。
func objectError(err error) error {
	var serviceErr *service.Error
	if errors.As(err, &serviceErr) {
		return err
	}

	return service.WrapError(service.CodeUnavailable, "consus request failed: "+err.Error(), err)
}
