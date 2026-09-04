package domain

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// 輸入端接受的時間格式；沒有時區的當 UTC
var timestampLayouts = []string{
	time.RFC3339Nano,
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04",
	"2006-01-02 15:04",
	"2006-01-02",
}

// Timestamp 是寬鬆解析的時間輸入：RFC3339 之外也接受 YYYY-MM-DD 與 YYYY-MM-DD HH:MM:SS。
type Timestamp struct {
	time.Time
}

// MarshalJSON 輸出 RFC3339Nano，跟 time.Time 一致。
func (t Timestamp) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Time)
}

// UnmarshalJSON 解析字串時間；格式錯誤時回可讀的提示而不是 Go 內部訊息。
func (t *Timestamp) UnmarshalJSON(data []byte) error {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "null" {
		return nil
	}

	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("timestamp must be a JSON string such as \"2026-09-02T00:00:00Z\" or \"2026-09-02\", got %s", trimmed)
	}
	parsed, err := ParseTimestamp(raw)
	if err != nil {
		return err
	}
	t.Time = parsed

	return nil
}

/**
 * ParseTimestamp 依序嘗試支援的格式；無時區的格式視為 UTC。
 */
func ParseTimestamp(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	for _, layout := range timestampLayouts {
		parsed, err := time.Parse(layout, value)
		if err == nil {
			return parsed.UTC(), nil
		}
	}

	return time.Time{}, fmt.Errorf(
		"timestamp %q is invalid: use RFC3339 (2026-09-02T00:00:00Z), \"YYYY-MM-DD\", or \"YYYY-MM-DD HH:MM:SS\" (UTC when no zone)",
		value,
	)
}

// TimeValue 回傳底層 *time.Time，nil 安全。
func (t *Timestamp) TimeValue() *time.Time {
	if t == nil {
		return nil
	}
	value := t.Time

	return &value
}

// TimestampOf 把 *time.Time 包成 *Timestamp，nil 安全。
func TimestampOf(value *time.Time) *Timestamp {
	if value == nil {
		return nil
	}

	return &Timestamp{Time: *value}
}
