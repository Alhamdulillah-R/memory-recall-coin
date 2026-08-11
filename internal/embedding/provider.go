package embedding

import (
	"context"
	"errors"
)

// Dimensions 是数据库与 provider 共同使用的固定 embedding 维度。
const Dimensions = 1024

// ErrDisabled 表示 semantic retrieval 已显式关闭。
var ErrDisabled = errors.New("embedding provider is disabled")

// Provider 定义批量生成 embedding 的能力。
type Provider interface {
	Enabled() bool
	Embed(context.Context, []string) ([][]float32, error)
	EmbedQuery(context.Context, string) ([]float32, error)
}

// Disabled 是显式关闭 semantic retrieval 时使用的 provider。
type Disabled struct{}

// NewDisabled 创建一个禁用状态的 provider。
func NewDisabled() *Disabled {
	return &Disabled{}
}

// Enabled 返回 false，表示该 provider 不可生成 embedding。
func (*Disabled) Enabled() bool {
	return false
}

/**
 * Embed 拒绝 embedding 请求。
 * @param ctx    请求 context
 * @param inputs 待处理文本
 * @return       ErrDisabled
 */
func (*Disabled) Embed(_ context.Context, _ []string) ([][]float32, error) {
	return nil, ErrDisabled
}

/**
 * EmbedQuery rejects query embedding requests.
 * @return ErrDisabled
 */
func (*Disabled) EmbedQuery(_ context.Context, _ string) ([]float32, error) {
	return nil, ErrDisabled
}

var _ Provider = (*Disabled)(nil)
