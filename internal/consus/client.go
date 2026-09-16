package consus

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	max_error_body_bytes = 8 << 10
	max_json_body_bytes  = 8 << 20
	default_timeout      = 30 * time.Minute
)

// Config 描述怎麼連上 consus。
type Config struct {
	BaseURL string
	Token   string
	Timeout time.Duration
}

// Client 是 consus 的 HTTP 客戶端，只在本機 bridge 使用。
type Client struct {
	baseURL    string
	token      string
	httpClient *http.Client
}

// Object 是一筆 object 記錄；versions 只有 meta 與寫入後的查詢才會帶。
type Object struct {
	ObjectID       string          `json:"object_id"`
	Namespace      string          `json:"namespace"`
	Name           string          `json:"name"`
	CurrentVersion int             `json:"current_version"`
	MaxVersions    int             `json:"max_versions"`
	ExpiresAt      *time.Time      `json:"expires_at,omitempty"`
	Status         string          `json:"status"`
	Tags           []string        `json:"tags"`
	Metadata       map[string]any  `json:"metadata,omitempty"`
	CreatedBy      string          `json:"created_by"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	Size           int64           `json:"size"`
	SHA256         string          `json:"sha256"`
	Versions       []ObjectVersion `json:"versions,omitempty"`
}

// ObjectVersion 是 object 的一個歷史版本。
type ObjectVersion struct {
	Version   int       `json:"version"`
	BlobID    string    `json:"blob_id"`
	SHA256    string    `json:"sha256"`
	Size      int64     `json:"size"`
	CreatedBy string    `json:"created_by"`
	CreatedAt time.Time `json:"created_at"`
	Note      string    `json:"note,omitempty"`
}

// WriteResult 是上傳或掛載後的回執。
type WriteResult struct {
	ObjectID  string     `json:"object_id"`
	Namespace string     `json:"namespace"`
	Name      string     `json:"name"`
	Version   int        `json:"version"`
	BlobID    string     `json:"blob_id"`
	SHA256    string     `json:"sha256"`
	Size      int64      `json:"size"`
	Dedup     bool       `json:"dedup"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}

// PrecheckResult 說明這份內容伺服器是否已經有了。
type PrecheckResult struct {
	Exists      bool   `json:"exists"`
	Size        int64  `json:"size"`
	ContentType string `json:"content_type,omitempty"`
}

// NamespaceStat 是 namespace 樹的一個節點。
type NamespaceStat struct {
	Namespace string `json:"namespace"`
	Segment   string `json:"segment"`
	Objects   int64  `json:"objects"`
	Bytes     int64  `json:"bytes"`
	Children  int64  `json:"children"`
}

// UploadInput 描述一次上傳；Body 由呼叫端負責關閉。
type UploadInput struct {
	Namespace   string
	Name        string
	TTL         string
	Tags        []string
	Metadata    map[string]any
	MaxVersions int
	OnConflict  string
	ContentType string
	SHA256      string
	Size        int64
	Note        string
	Body        io.Reader
}

// LinkInput 把已存在的內容掛成新 object，不傳 bytes。
type LinkInput struct {
	SHA256      string         `json:"sha256"`
	Namespace   string         `json:"namespace"`
	Name        string         `json:"name"`
	ContentType string         `json:"content_type,omitempty"`
	TTL         string         `json:"ttl,omitempty"`
	Tags        []string       `json:"tags,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
	MaxVersions int            `json:"max_versions,omitempty"`
	OnConflict  string         `json:"on_conflict,omitempty"`
	Note        string         `json:"note,omitempty"`
}

// PatchInput 只送有給的欄位。
type PatchInput struct {
	Name        *string        `json:"name,omitempty"`
	Tags        []string       `json:"tags,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
	MaxVersions *int           `json:"max_versions,omitempty"`
	ExpiresAt   *string        `json:"expires_at,omitempty"`
}

// ListInput 篩選 object 清單。
type ListInput struct {
	Namespace string
	Subtree   bool
	Tag       string
	Limit     int
}

/**
 * NewClient 建立 consus 客戶端，baseURL 與 token 缺一不可。
 */
func NewClient(cfg Config) (*Client, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if baseURL == "" {
		return nil, errors.New("consus base URL is required")
	}
	if strings.TrimSpace(cfg.Token) == "" {
		return nil, errors.New("consus token is required")
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = default_timeout
	}

	return &Client{
		baseURL: baseURL,
		token:   strings.TrimSpace(cfg.Token),
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}, nil
}

/**
 * Upload 串流送出 bytes 並建立或覆蓋一個 object。
 */
func (c *Client) Upload(ctx context.Context, input UploadInput) (WriteResult, error) {
	query := url.Values{}
	query.Set("ns", input.Namespace)
	if input.Name != "" {
		query.Set("name", input.Name)
	}
	if input.TTL != "" {
		query.Set("ttl", input.TTL)
	}
	if len(input.Tags) > 0 {
		query.Set("tag", strings.Join(input.Tags, ","))
	}
	if input.MaxVersions > 0 {
		query.Set("max_versions", strconv.Itoa(input.MaxVersions))
	}
	if input.OnConflict != "" {
		query.Set("on_conflict", input.OnConflict)
	}
	if input.Note != "" {
		query.Set("note", input.Note)
	}

	request, err := c.newRequest(ctx, http.MethodPost, "/v1/objects", query, input.Body)
	if err != nil {
		return WriteResult{}, err
	}
	if input.ContentType != "" {
		request.Header.Set("Content-Type", input.ContentType)
	}
	if input.SHA256 != "" {
		request.Header.Set("X-Consus-SHA256", input.SHA256)
	}
	if len(input.Metadata) > 0 {
		encoded, err := json.Marshal(input.Metadata)
		if err != nil {
			return WriteResult{}, fmt.Errorf("encode consus metadata: %w", err)
		}
		request.Header.Set("X-Consus-Metadata", string(encoded))
	}
	request.ContentLength = input.Size

	var result WriteResult
	if err := c.do(request, &result); err != nil {
		return WriteResult{}, err
	}

	return result, nil
}

/**
 * Link 用已知的 sha256 掛出一個新 object，伺服器端已有那份內容時完全不傳 bytes。
 */
func (c *Client) Link(ctx context.Context, input LinkInput) (WriteResult, error) {
	var result WriteResult
	if err := c.postJSON(ctx, "/v1/objects/link", input, &result); err != nil {
		return WriteResult{}, err
	}

	return result, nil
}

/**
 * Precheck 問伺服器是否已經有這份內容。
 */
func (c *Client) Precheck(ctx context.Context, sha256 string) (PrecheckResult, error) {
	var result PrecheckResult
	payload := map[string]string{"sha256": sha256}
	if err := c.postJSON(ctx, "/v1/blobs/precheck", payload, &result); err != nil {
		return PrecheckResult{}, err
	}

	return result, nil
}

/**
 * Meta 讀一筆 object 的完整記錄，含版本列表。
 */
func (c *Client) Meta(ctx context.Context, objectID string) (Object, error) {
	var result Object
	if err := c.getJSON(ctx, "/v1/objects/"+url.PathEscape(objectID)+"/meta", nil, &result); err != nil {
		return Object{}, err
	}

	return result, nil
}

/**
 * Resolve 用 namespace 加名字換出 object。
 */
func (c *Client) Resolve(ctx context.Context, namespace string, name string) (Object, error) {
	query := url.Values{}
	query.Set("ns", namespace)
	query.Set("name", name)

	var result Object
	if err := c.getJSON(ctx, "/v1/resolve", query, &result); err != nil {
		return Object{}, err
	}

	return result, nil
}

/**
 * List 列出 namespace 底下的 object。
 */
func (c *Client) List(ctx context.Context, input ListInput) ([]Object, error) {
	query := url.Values{}
	if input.Namespace != "" {
		query.Set("ns", input.Namespace)
	}
	if input.Subtree {
		query.Set("subtree", "true")
	}
	if input.Tag != "" {
		query.Set("tag", input.Tag)
	}
	if input.Limit > 0 {
		query.Set("limit", strconv.Itoa(input.Limit))
	}

	var result struct {
		Count   int      `json:"count"`
		Objects []Object `json:"objects"`
	}
	if err := c.getJSON(ctx, "/v1/objects", query, &result); err != nil {
		return nil, err
	}

	return result.Objects, nil
}

/**
 * Namespaces 讀 namespace 樹，tree 為 true 時一次拿全部。
 */
func (c *Client) Namespaces(ctx context.Context, namespace string, tree bool) ([]NamespaceStat, error) {
	query := url.Values{}
	if namespace != "" {
		query.Set("ns", namespace)
	}
	if tree {
		query.Set("tree", "true")
	}

	var result struct {
		Namespaces []NamespaceStat `json:"namespaces"`
	}
	if err := c.getJSON(ctx, "/v1/namespaces", query, &result); err != nil {
		return nil, err
	}

	return result.Namespaces, nil
}

/**
 * Download 把 object 的內容寫進 writer，回傳實際寫入的位元組數。
 */
func (c *Client) Download(ctx context.Context, objectID string, version int, out io.Writer) (written int64, err error) {
	query := url.Values{}
	if version > 0 {
		query.Set("version", strconv.Itoa(version))
	}

	request, err := c.newRequest(ctx, http.MethodGet, "/v1/objects/"+url.PathEscape(objectID), query, nil)
	if err != nil {
		return 0, err
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		return 0, fmt.Errorf("download consus object: %w", err)
	}
	defer func() {
		if closeErr := response.Body.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return 0, c.statusError(response)
	}

	written, err = io.Copy(out, response.Body)
	if err != nil {
		return written, fmt.Errorf("write consus object body: %w", err)
	}

	return written, nil
}

/**
 * Patch 改 object 的可變欄位。
 */
func (c *Client) Patch(ctx context.Context, objectID string, input PatchInput) (Object, error) {
	payload, err := json.Marshal(input)
	if err != nil {
		return Object{}, fmt.Errorf("encode consus patch: %w", err)
	}

	request, err := c.newRequest(
		ctx,
		http.MethodPatch,
		"/v1/objects/"+url.PathEscape(objectID),
		nil,
		bytes.NewReader(payload),
	)
	if err != nil {
		return Object{}, err
	}
	request.Header.Set("Content-Type", "application/json")

	var result Object
	if err := c.do(request, &result); err != nil {
		return Object{}, err
	}

	return result, nil
}

/**
 * Touch 重算過期時間，ttl 必填。
 */
func (c *Client) Touch(ctx context.Context, objectID string, ttl string) (Object, error) {
	query := url.Values{}
	query.Set("ttl", ttl)

	request, err := c.newRequest(
		ctx,
		http.MethodPost,
		"/v1/objects/"+url.PathEscape(objectID)+"/touch",
		query,
		nil,
	)
	if err != nil {
		return Object{}, err
	}

	var result Object
	if err := c.do(request, &result); err != nil {
		return Object{}, err
	}

	return result, nil
}

/**
 * Delete 軟刪一個 object。
 */
func (c *Client) Delete(ctx context.Context, objectID string) error {
	request, err := c.newRequest(ctx, http.MethodDelete, "/v1/objects/"+url.PathEscape(objectID), nil, nil)
	if err != nil {
		return err
	}

	return c.do(request, nil)
}

/**
 * Restore 把軟刪的 object 救回來。
 */
func (c *Client) Restore(ctx context.Context, objectID string) (Object, error) {
	request, err := c.newRequest(
		ctx,
		http.MethodPost,
		"/v1/objects/"+url.PathEscape(objectID)+"/restore",
		nil,
		nil,
	)
	if err != nil {
		return Object{}, err
	}

	var result Object
	if err := c.do(request, &result); err != nil {
		return Object{}, err
	}

	return result, nil
}

func (c *Client) newRequest(
	ctx context.Context,
	method string,
	path string,
	query url.Values,
	body io.Reader,
) (*http.Request, error) {
	target := c.baseURL + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	request, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return nil, fmt.Errorf("create consus request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+c.token)
	request.Header.Set("Accept", "application/json")

	return request, nil
}

func (c *Client) postJSON(ctx context.Context, path string, payload any, out any) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode consus request: %w", err)
	}
	request, err := c.newRequest(ctx, http.MethodPost, path, nil, bytes.NewReader(encoded))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")

	return c.do(request, out)
}

func (c *Client) getJSON(ctx context.Context, path string, query url.Values, out any) error {
	request, err := c.newRequest(ctx, http.MethodGet, path, query, nil)
	if err != nil {
		return err
	}

	return c.do(request, out)
}

func (c *Client) do(request *http.Request, out any) (err error) {
	response, err := c.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("call consus: %w", err)
	}
	defer func() {
		if closeErr := response.Body.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return c.statusError(response)
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, max_error_body_bytes))

		return nil
	}

	data, err := io.ReadAll(io.LimitReader(response.Body, max_json_body_bytes))
	if err != nil {
		return fmt.Errorf("read consus response: %w", err)
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("decode consus response (%s): %w", response.Status, err)
	}

	return nil
}

// statusError 把非 2xx 轉成帶伺服器原文的錯誤，原文截斷避免把整頁 HTML 帶回去。
func (c *Client) statusError(response *http.Response) error {
	data, err := io.ReadAll(io.LimitReader(response.Body, max_error_body_bytes))
	if err != nil {
		return fmt.Errorf("consus returned %s", response.Status)
	}
	detail := strings.TrimSpace(string(data))
	if detail == "" {
		return fmt.Errorf("consus returned %s", response.Status)
	}

	return fmt.Errorf("consus returned %s: %s", response.Status, detail)
}
