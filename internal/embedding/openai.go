package embedding

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	maxErrorResponseBytes   int64 = 64 << 10
	maxSuccessResponseBytes int64 = 32 << 20
)

// OpenAIConfig 配置 OpenAI-compatible embeddings endpoint。
type OpenAIConfig struct {
	BaseURL          string
	APIKey           string
	Model            string
	Dimensions       int
	QueryPrefix      string
	QueryInstruction string
	Timeout          time.Duration
}

// OpenAI 使用 OpenAI-compatible /v1/embeddings API 生成向量。
type OpenAI struct {
	endpoint         string
	apiKey           string
	model            string
	dimensions       int
	queryPrefix      string
	queryInstruction string
	client           *http.Client
}

// APIError 描述 embeddings endpoint 返回的非 2xx 响应。
type APIError struct {
	StatusCode int
	Status     string
	Message    string
	Truncated  bool
}

// Error 返回适合日志与 tool error 的简短错误文本。
func (e *APIError) Error() string {
	message := e.Message
	if message == "" {
		message = http.StatusText(e.StatusCode)
	}
	if e.Truncated {
		message += " [truncated]"
	}

	return fmt.Sprintf("embedding API returned %s: %s", e.Status, message)
}

type embeddingRequest struct {
	Input          []string `json:"input"`
	Model          string   `json:"model"`
	Dimensions     int      `json:"dimensions"`
	EncodingFormat string   `json:"encoding_format"`
}

type embeddingResponse struct {
	Data []embeddingData `json:"data"`
}

type embeddingData struct {
	Embedding []float32 `json:"embedding"`
	Index     int       `json:"index"`
}

type errorEnvelope struct {
	Error struct {
		Message string `json:"message"`
	} `json:"error"`
}

/**
 * NewOpenAI 创建固定输出维度的 OpenAI-compatible provider。
 * @param cfg endpoint、认证、model 与 timeout 配置
 * @return    provider 或配置错误
 */
func NewOpenAI(cfg OpenAIConfig) (*OpenAI, error) {
	endpoint, err := resolveEndpoint(cfg.BaseURL)
	if err != nil {
		return nil, err
	}

	model := strings.TrimSpace(cfg.Model)
	if model == "" {
		return nil, errors.New("embedding model is required")
	}
	if cfg.Dimensions != Dimensions {
		return nil, fmt.Errorf("embedding dimensions must be %d, got %d", Dimensions, cfg.Dimensions)
	}
	if cfg.Timeout <= 0 {
		return nil, errors.New("embedding timeout must be positive")
	}
	queryPrefix := strings.TrimSpace(cfg.QueryPrefix)
	queryInstruction := strings.TrimSpace(cfg.QueryInstruction)
	if queryPrefix != "" && queryInstruction != "" {
		return nil, errors.New("embedding query prefix and instruction are mutually exclusive")
	}

	return &OpenAI{
		endpoint:         endpoint,
		apiKey:           strings.TrimSpace(cfg.APIKey),
		model:            model,
		dimensions:       cfg.Dimensions,
		queryPrefix:      queryPrefix,
		queryInstruction: queryInstruction,
		client: &http.Client{
			Timeout: cfg.Timeout,
		},
	}, nil
}

/**
 * EmbedQuery generates one query vector and applies the configured retrieval instruction.
 * @param ctx   request context
 * @param query raw search query
 * @return      one vector using the configured fixed dimensions
 */
func (p *OpenAI) EmbedQuery(ctx context.Context, query string) ([]float32, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, errors.New("embedding query is empty")
	}
	if p.queryPrefix != "" {
		query = p.queryPrefix + query
	} else if p.queryInstruction != "" {
		query = "Instruct: " + p.queryInstruction + "\nQuery: " + query
	}

	vectors, err := p.Embed(ctx, []string{query})
	if err != nil {
		return nil, err
	}

	return vectors[0], nil
}

// Enabled 返回 true，表示该 provider 可以生成 embedding。
func (*OpenAI) Enabled() bool {
	return true
}

/**
 * Embed 批量生成 embedding，并按 response index 恢复 input 顺序。
 * @param ctx    控制请求取消与 deadline
 * @param inputs 待处理文本，空 batch 不发起 HTTP 请求
 * @return       与 inputs 等长且每项符合固定维度的向量
 */
func (p *OpenAI) Embed(ctx context.Context, inputs []string) ([][]float32, error) {
	if len(inputs) == 0 {
		return [][]float32{}, nil
	}
	for index, input := range inputs {
		if strings.TrimSpace(input) == "" {
			return nil, fmt.Errorf("embedding input at index %d is empty", index)
		}
	}

	payload := embeddingRequest{
		Input:          inputs,
		Model:          p.model,
		Dimensions:     p.dimensions,
		EncodingFormat: "float",
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal embedding request: %w", err)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, p.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create embedding request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/json")
	if p.apiKey != "" {
		request.Header.Set("Authorization", "Bearer "+p.apiKey)
	}

	response, err := p.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("send embedding request: %w", err)
	}

	responseLimit := maxSuccessResponseBytes
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		responseLimit = maxErrorResponseBytes
	}

	responseBody, truncated, err := readAndClose(response.Body, responseLimit)
	if err != nil {
		return nil, fmt.Errorf("read embedding response: %w", err)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, parseAPIError(response, responseBody, truncated)
	}
	if truncated {
		return nil, fmt.Errorf("embedding response exceeds %d bytes", maxSuccessResponseBytes)
	}

	var result embeddingResponse
	if err := json.Unmarshal(responseBody, &result); err != nil {
		return nil, fmt.Errorf("decode embedding response: %w", err)
	}

	return validateResponse(result, len(inputs), p.dimensions)
}

/**
 * resolveEndpoint 标准化 base URL，并定位到 /v1/embeddings。
 * @param rawBaseURL 配置的 OpenAI-compatible base URL
 * @return           endpoint URL 或校验错误
 */
func resolveEndpoint(rawBaseURL string) (string, error) {
	rawBaseURL = strings.TrimSpace(rawBaseURL)
	if rawBaseURL == "" {
		return "", errors.New("embedding base URL is required")
	}

	endpoint, err := url.Parse(rawBaseURL)
	if err != nil {
		return "", fmt.Errorf("parse embedding base URL: %w", err)
	}
	if endpoint.Scheme != "http" && endpoint.Scheme != "https" {
		return "", fmt.Errorf("embedding base URL scheme must be http or https, got %q", endpoint.Scheme)
	}
	if endpoint.Host == "" {
		return "", errors.New("embedding base URL must include a host")
	}
	if endpoint.User != nil {
		return "", errors.New("embedding base URL must not include user information")
	}
	if endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return "", errors.New("embedding base URL must not include query or fragment")
	}

	endpoint.Path = strings.TrimRight(endpoint.Path, "/")
	endpoint.RawPath = ""
	switch {
	case strings.HasSuffix(endpoint.Path, "/v1/embeddings"):
	case strings.HasSuffix(endpoint.Path, "/v1"):
		endpoint.Path += "/embeddings"
	default:
		endpoint.Path += "/v1/embeddings"
	}

	return endpoint.String(), nil
}

/**
 * readAndClose 在固定上限内读取 response，并处理 Close error。
 * @param body  response body
 * @param limit 最大保留字节数
 * @return      body、是否截断与 I/O error
 */
func readAndClose(body io.ReadCloser, limit int64) ([]byte, bool, error) {
	data, readErr := io.ReadAll(io.LimitReader(body, limit+1))
	closeErr := body.Close()
	if readErr != nil || closeErr != nil {
		return nil, false, errors.Join(readErr, closeErr)
	}
	if int64(len(data)) <= limit {
		return data, false, nil
	}

	return data[:limit], true, nil
}

func parseAPIError(response *http.Response, body []byte, truncated bool) *APIError {
	message := ""
	var envelope errorEnvelope
	if err := json.Unmarshal(body, &envelope); err == nil {
		message = strings.TrimSpace(envelope.Error.Message)
	}
	if message == "" {
		message = strings.TrimSpace(string(body))
	}

	return &APIError{
		StatusCode: response.StatusCode,
		Status:     response.Status,
		Message:    message,
		Truncated:  truncated,
	}
}

/**
 * validateResponse 验证数量、index 唯一性与固定维度，并重排向量。
 * @param response      provider response
 * @param expectedCount input 数量
 * @param dimensions    expected vector dimensions
 * @return              按 input 顺序排列的向量或错误
 */
func validateResponse(response embeddingResponse, expectedCount, dimensions int) ([][]float32, error) {
	if len(response.Data) != expectedCount {
		return nil, fmt.Errorf(
			"embedding response count mismatch: expected %d, got %d",
			expectedCount,
			len(response.Data),
		)
	}

	vectors := make([][]float32, expectedCount)
	seen := make([]bool, expectedCount)
	for _, item := range response.Data {
		if item.Index < 0 || item.Index >= expectedCount {
			return nil, fmt.Errorf("embedding response index %d is out of range", item.Index)
		}
		if seen[item.Index] {
			return nil, fmt.Errorf("embedding response contains duplicate index %d", item.Index)
		}
		if len(item.Embedding) != dimensions {
			return nil, fmt.Errorf(
				"embedding at index %d has %d dimensions, expected %d",
				item.Index,
				len(item.Embedding),
				dimensions,
			)
		}

		seen[item.Index] = true
		vectors[item.Index] = item.Embedding
	}

	return vectors, nil
}

var _ Provider = (*OpenAI)(nil)
