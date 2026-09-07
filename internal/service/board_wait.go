package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Alhamdulillah-R/memory-recall-coin/internal/domain"
)

const (
	boardWaitMaxSeconds     = 50
	boardWaitPollInterval   = 2 * time.Second
	boardWaitMaxThreads     = 20
	boardWaitPreviewRunes   = 160
	boardWaitLineMaxThreads = 3
)

// BoardWaitInput 是 long-poll：阻塞到 since 之後有 open thread 出現新活動，或 timeout 到期。
type BoardWaitInput struct {
	Tags           []string              `json:"tags,omitempty" jsonschema:"only threads carrying any of these namespace tags; empty watches every tag"`
	Since          *domain.Timestamp     `json:"since,omitempty" jsonschema:"report open threads updated after this time; omitted returns immediately with the server clock to use as the first since"`
	TimeoutSeconds int                   `json:"timeout_seconds,omitempty" jsonschema:"how long one call may block, 1 to 50; default 50"`
	Caller         domain.CallerIdentity `json:"-"`
}

/**
 * WaitBoard 每兩秒查一次 since 之後更新過的 open thread，有就回，沒有就等到 timeout。
 * @return changed 與新活動的 thread 摘要；changed=false 時 now 是下一輪的 since
 */
func (s *Store) WaitBoard(ctx context.Context, input BoardWaitInput) (domain.BoardWaitResult, error) {
	tags, err := normalizeWaitTags(input.Tags)
	if err != nil {
		return domain.BoardWaitResult{}, err
	}
	if input.Since == nil {
		now, err := s.serverNow(ctx)
		if err != nil {
			return domain.BoardWaitResult{}, err
		}

		return domain.BoardWaitResult{Since: now, Now: now}, nil
	}
	timeout := input.TimeoutSeconds
	if timeout <= 0 || timeout > boardWaitMaxSeconds {
		timeout = boardWaitMaxSeconds
	}
	since := input.Since.Time
	deadline := time.Now().Add(time.Duration(timeout) * time.Second)

	for {
		now, err := s.serverNow(ctx)
		if err != nil {
			return domain.BoardWaitResult{}, err
		}
		threads, err := s.boardActivitySince(ctx, since, tags, input.Caller.SessionID)
		if err != nil {
			return domain.BoardWaitResult{}, err
		}
		if len(threads) > 0 {
			return domain.BoardWaitResult{
				Changed: true,
				Since:   since,
				Now:     now,
				Threads: threads,
				Line:    boardWaitLine(threads),
			}, nil
		}
		if !time.Now().Add(boardWaitPollInterval).Before(deadline) {
			return domain.BoardWaitResult{Since: since, Now: now}, nil
		}
		select {
		case <-ctx.Done():
			return domain.BoardWaitResult{}, WrapError(CodeUnavailable, "board_wait cancelled", ctx.Err())
		case <-time.After(boardWaitPollInterval):
		}
	}
}

func (s *Store) serverNow(ctx context.Context) (time.Time, error) {
	var now time.Time
	if err := s.pool.QueryRow(ctx, "SELECT statement_timestamp()").Scan(&now); err != nil {
		return time.Time{}, WrapError(CodeInternal, "read server clock", err)
	}

	return now, nil
}

/**
 * boardActivitySince 撈 since 之後有別人留言的 open thread；自己這個 session 發的不算新活動。
 */
func (s *Store) boardActivitySince(
	ctx context.Context,
	since time.Time,
	tags []string,
	callerSession string,
) ([]domain.BoardThreadHead, error) {
	args := []any{since, boardWaitMaxThreads, callerSession}
	tagFilter := ""
	if len(tags) > 0 {
		args = append(args, tags)
		tagFilter = " AND t.tags && $4::text[]"
	}
	rows, err := s.pool.Query(ctx, `
		SELECT t.id, t.tags, t.message_count, t.updated_at, coalesce(m.author, ''), m.body
		FROM board_threads t
		JOIN LATERAL (
			SELECT author, body FROM board_messages
			WHERE thread_id = t.id AND created_at > $1
			  AND ($3 = '' OR created_by_session IS DISTINCT FROM $3)
			ORDER BY created_at DESC LIMIT 1
		) m ON true
		WHERE t.status = 'open' AND t.updated_at > $1`+tagFilter+`
		ORDER BY t.updated_at DESC
		LIMIT $2
	`, args...)
	if err != nil {
		return nil, WrapError(CodeInternal, "read board activity", err)
	}
	defer rows.Close()

	threads := make([]domain.BoardThreadHead, 0)
	for rows.Next() {
		var head domain.BoardThreadHead
		var body string
		if err := rows.Scan(&head.ID, &head.Tags, &head.MessageCount, &head.UpdatedAt, &head.LastAuthor, &body); err != nil {
			return nil, WrapError(CodeInternal, "scan board activity", err)
		}
		head.Preview = previewText(body, boardWaitPreviewRunes)
		threads = append(threads, head)
	}
	if err := rows.Err(); err != nil {
		return nil, WrapError(CodeInternal, "iterate board activity", err)
	}

	return threads, nil
}

func normalizeWaitTags(rawTags []string) ([]string, error) {
	tags := make([]string, 0, len(rawTags))
	seen := make(map[string]struct{}, len(rawTags))
	for _, raw := range rawTags {
		if strings.TrimSpace(raw) == "" {
			continue
		}
		tag, err := normalizeNamespace(raw)
		if err != nil {
			return nil, err
		}
		if _, exists := seen[tag]; exists {
			continue
		}
		seen[tag] = struct{}{}
		tags = append(tags, tag)
	}

	return tags, nil
}

// boardWaitLine 是餵給 hook stderr 的那幾行：指標不是正文，正文由 agent 自己 board_read。
func boardWaitLine(threads []domain.BoardThreadHead) string {
	lines := make([]string, 0, len(threads)+1)
	for index, thread := range threads {
		if index == boardWaitLineMaxThreads {
			lines = append(lines, fmt.Sprintf("… and %d more", len(threads)-boardWaitLineMaxThreads))
			break
		}
		author := thread.LastAuthor
		if author == "" {
			author = "unknown"
		}
		lines = append(lines, fmt.Sprintf(
			"board: %s [%s] %d msgs, last by %s: %s",
			thread.ID,
			strings.Join(thread.Tags, " "),
			thread.MessageCount,
			author,
			thread.Preview,
		))
	}

	return strings.Join(lines, "\n")
}

func previewText(body string, maxRunes int) string {
	body = strings.Join(strings.Fields(body), " ")
	runes := []rune(body)
	if len(runes) <= maxRunes {
		return body
	}

	return string(runes[:maxRunes]) + "…"
}
