package service

import (
	"context"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/Alhamdulillah-R/memory-recall-coin/internal/domain"
)

const (
	boardMaxTags         = 8
	boardDefaultReadSize = 20
	boardMaxReadSize     = 100
)

// BoardPostInput 開一個新 thread；tags 是這件事關係到的 namespace。
type BoardPostInput struct {
	Tags   []string              `json:"tags" jsonschema:"1 to 8 existing namespace paths this thread concerns; other agents filter the board by these"`
	Body   string                `json:"body" jsonschema:"what you found, need, or are about to change; write for an agent in another session"`
	Author string                `json:"author,omitempty" jsonschema:"free-form sender label such as the session or task name"`
	Caller domain.CallerIdentity `json:"-"`
}

// BoardReplyInput 在既有 thread 下追一則留言。
type BoardReplyInput struct {
	ThreadID string                `json:"thread_id"`
	Body     string                `json:"body"`
	Author   string                `json:"author,omitempty" jsonschema:"free-form sender label such as the session or task name"`
	Caller   domain.CallerIdentity `json:"-"`
}

// BoardReadInput 拉 thread 正文；預設只拉還沒 resolve 的。
type BoardReadInput struct {
	Tags            []string              `json:"tags,omitempty" jsonschema:"only threads carrying any of these namespace tags; empty reads every tag"`
	IncludeResolved bool                  `json:"include_resolved,omitempty"`
	Since           *domain.Timestamp     `json:"since,omitempty" jsonschema:"only threads updated at or after this time; RFC3339 or YYYY-MM-DD"`
	Limit           int                   `json:"limit,omitempty" jsonschema:"maximum threads from 1 to 100; default 20"`
	Caller          domain.CallerIdentity `json:"-"`
}

// BoardCountsInput 沒有參數。
type BoardCountsInput struct {
	Caller domain.CallerIdentity `json:"-"`
}

// BoardResolveInput 收掉一個 thread：寫結論，可以順手升格成 memory。
type BoardResolveInput struct {
	ThreadID        string                `json:"thread_id"`
	Resolution      string                `json:"resolution" jsonschema:"one or two sentences: the conclusion, or an explicit statement that there was none"`
	PromoteToMemory *PutMemoryInput       `json:"promote_to_memory,omitempty" jsonschema:"when the thread reached a durable conclusion, write it as a memory in the same call; namespace, title, summary and content required"`
	Caller          domain.CallerIdentity `json:"-"`
}

const boardThreadColumns = `
	id, tags, status, coalesce(resolution, ''), coalesce(resolved_memory_id, ''), coalesce(resolved_by, ''),
	resolved_at, message_count, created_by, created_at, updated_at
`

/**
 * PostBoardThread 開新 thread 並寫入第一則留言；tags 必須都是 active namespace。
 */
func (s *Store) PostBoardThread(ctx context.Context, input BoardPostInput) (domain.BoardThread, error) {
	body := strings.TrimSpace(input.Body)
	if err := requireNonEmpty("body", body); err != nil {
		return domain.BoardThread{}, err
	}
	actor := normalizeActor("", input.Caller)
	tx, err := s.beginMutation(ctx, actor, "board_post")
	if err != nil {
		return domain.BoardThread{}, err
	}
	defer rollback(tx)

	tags, err := normalizeBoardTags(ctx, tx, input.Tags)
	if err != nil {
		return domain.BoardThread{}, err
	}

	threadID := NewID("thr")
	if _, err := tx.Exec(ctx, `
		INSERT INTO board_threads(id, tags, created_by)
		VALUES ($1, $2, $3)
	`, threadID, tags, actor); err != nil {
		return domain.BoardThread{}, WrapError(CodeInternal, "insert board thread", err)
	}
	if err := insertBoardMessage(ctx, tx, threadID, body, input.Author, actor, input.Caller); err != nil {
		return domain.BoardThread{}, err
	}

	thread, err := loadBoardThread(ctx, tx, threadID)
	if err != nil {
		return domain.BoardThread{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.BoardThread{}, WrapError(CodeInternal, "commit board_post", err)
	}

	return thread, nil
}

/**
 * ReplyBoardThread 在 open thread 下追留言；resolved 的 thread 不收新留言。
 */
func (s *Store) ReplyBoardThread(ctx context.Context, input BoardReplyInput) (domain.BoardThread, error) {
	body := strings.TrimSpace(input.Body)
	if err := requireNonEmpty("thread_id", input.ThreadID); err != nil {
		return domain.BoardThread{}, err
	}
	if err := requireNonEmpty("body", body); err != nil {
		return domain.BoardThread{}, err
	}
	actor := normalizeActor("", input.Caller)
	tx, err := s.beginMutation(ctx, actor, "board_reply")
	if err != nil {
		return domain.BoardThread{}, err
	}
	defer rollback(tx)

	status, err := lockBoardThread(ctx, tx, input.ThreadID)
	if err != nil {
		return domain.BoardThread{}, err
	}
	if status != "open" {
		return domain.BoardThread{}, NewError(CodeFailedPrecondition, "thread is resolved; post a new thread instead")
	}
	if err := insertBoardMessage(ctx, tx, input.ThreadID, body, input.Author, actor, input.Caller); err != nil {
		return domain.BoardThread{}, err
	}

	thread, err := loadBoardThread(ctx, tx, input.ThreadID)
	if err != nil {
		return domain.BoardThread{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.BoardThread{}, WrapError(CodeInternal, "commit board_reply", err)
	}

	return thread, nil
}

/**
 * BoardCounts 回每個 tag 還有幾條 open thread；SessionStart 只需要 line 那一行。
 */
func (s *Store) BoardCounts(ctx context.Context, _ BoardCountsInput) (domain.BoardCounts, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT tag, count(*)::int, max(updated_at)
		FROM board_threads, unnest(tags) AS tag
		WHERE status = 'open'
		GROUP BY tag
		ORDER BY tag
	`)
	if err != nil {
		return domain.BoardCounts{}, WrapError(CodeInternal, "count board threads", err)
	}
	defer rows.Close()

	counts := domain.BoardCounts{Counts: make([]domain.BoardTagCount, 0)}
	for rows.Next() {
		var count domain.BoardTagCount
		if err := rows.Scan(&count.Tag, &count.OpenThreads, &count.LastActivity); err != nil {
			return domain.BoardCounts{}, WrapError(CodeInternal, "scan board count", err)
		}
		counts.Counts = append(counts.Counts, count)
		counts.TotalOpen += count.OpenThreads
	}
	if err := rows.Err(); err != nil {
		return domain.BoardCounts{}, WrapError(CodeInternal, "iterate board counts", err)
	}
	counts.Line = boardCountsLine(counts.Counts)

	return counts, nil
}

/**
 * ReadBoard 拉 thread 與完整留言，按最近活動排序。
 */
func (s *Store) ReadBoard(ctx context.Context, input BoardReadInput) (domain.BoardReadResponse, error) {
	if input.Limit <= 0 {
		input.Limit = boardDefaultReadSize
	}
	if input.Limit > boardMaxReadSize {
		return domain.BoardReadResponse{}, NewError(CodeInvalidArgument, "limit must be between 1 and 100")
	}
	tags := make([]string, 0, len(input.Tags))
	for _, tag := range input.Tags {
		normalized, err := normalizeNamespace(tag)
		if err != nil {
			return domain.BoardReadResponse{}, err
		}
		tags = append(tags, normalized)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT `+boardThreadColumns+`
		FROM board_threads
		WHERE (cardinality($1::text[]) = 0 OR tags && $1::text[])
		  AND ($2 OR status = 'open')
		  AND ($3::timestamptz IS NULL OR updated_at >= $3)
		ORDER BY updated_at DESC
		LIMIT $4
	`, tags, input.IncludeResolved, input.Since.TimeValue(), input.Limit)
	if err != nil {
		return domain.BoardReadResponse{}, WrapError(CodeInternal, "read board threads", err)
	}
	threads, err := scanBoardThreads(rows)
	if err != nil {
		return domain.BoardReadResponse{}, err
	}
	if len(threads) == 0 {
		return domain.BoardReadResponse{Threads: threads}, nil
	}

	threadIDs := make([]string, len(threads))
	indexes := make(map[string]int, len(threads))
	for index, thread := range threads {
		threadIDs[index] = thread.ID
		indexes[thread.ID] = index
	}
	messageRows, err := s.pool.Query(ctx, `
		SELECT id, thread_id, body, coalesce(author, ''), created_by, created_by_session, created_at
		FROM board_messages
		WHERE thread_id = ANY($1::text[])
		ORDER BY created_at, id
	`, threadIDs)
	if err != nil {
		return domain.BoardReadResponse{}, WrapError(CodeInternal, "read board messages", err)
	}
	defer messageRows.Close()
	for messageRows.Next() {
		var message domain.BoardMessage
		if err := messageRows.Scan(
			&message.ID,
			&message.ThreadID,
			&message.Body,
			&message.Author,
			&message.CreatedBy,
			&message.CreatedBySession,
			&message.CreatedAt,
		); err != nil {
			return domain.BoardReadResponse{}, WrapError(CodeInternal, "scan board message", err)
		}
		index := indexes[message.ThreadID]
		threads[index].Messages = append(threads[index].Messages, message)
	}
	if err := messageRows.Err(); err != nil {
		return domain.BoardReadResponse{}, WrapError(CodeInternal, "iterate board messages", err)
	}

	return domain.BoardReadResponse{Threads: threads, Count: len(threads)}, nil
}

/**
 * ResolveBoardThread 寫結論並歸檔；帶 promote_to_memory 時先寫 memory 再收 thread。
 */
func (s *Store) ResolveBoardThread(ctx context.Context, input BoardResolveInput) (domain.BoardResolveResult, error) {
	resolution := strings.TrimSpace(input.Resolution)
	if err := requireNonEmpty("thread_id", input.ThreadID); err != nil {
		return domain.BoardResolveResult{}, err
	}
	if err := requireNonEmpty("resolution", resolution); err != nil {
		return domain.BoardResolveResult{}, err
	}

	memoryID := ""
	if input.PromoteToMemory != nil {
		promote := *input.PromoteToMemory
		promote.Caller = input.Caller
		memory, err := s.PutMemory(ctx, promote)
		if err != nil {
			return domain.BoardResolveResult{}, err
		}
		memoryID = memory.ID
	}

	actor := normalizeActor("", input.Caller)
	tx, err := s.beginMutation(ctx, actor, "board_resolve")
	if err != nil {
		return domain.BoardResolveResult{}, err
	}
	defer rollback(tx)

	status, err := lockBoardThread(ctx, tx, input.ThreadID)
	if err != nil {
		return domain.BoardResolveResult{}, err
	}
	if status != "open" {
		return domain.BoardResolveResult{}, NewError(CodeFailedPrecondition, "thread is already resolved")
	}
	if _, err := tx.Exec(ctx, `
		UPDATE board_threads SET
			status = 'resolved', resolution = $2, resolved_memory_id = $3,
			resolved_by = $4, resolved_at = statement_timestamp(), updated_at = statement_timestamp()
		WHERE id = $1
	`, input.ThreadID, resolution, nullableString(memoryID), actor); err != nil {
		return domain.BoardResolveResult{}, WrapError(CodeInternal, "resolve board thread", err)
	}

	thread, err := loadBoardThread(ctx, tx, input.ThreadID)
	if err != nil {
		return domain.BoardResolveResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.BoardResolveResult{}, WrapError(CodeInternal, "commit board_resolve", err)
	}

	return domain.BoardResolveResult{Thread: thread, MemoryID: memoryID}, nil
}

func normalizeBoardTags(ctx context.Context, tx pgx.Tx, rawTags []string) ([]string, error) {
	if len(rawTags) == 0 {
		return nil, NewError(CodeInvalidArgument, "tags must contain at least one namespace")
	}
	seen := make(map[string]struct{}, len(rawTags))
	tags := make([]string, 0, len(rawTags))
	for _, raw := range rawTags {
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
	if len(tags) > boardMaxTags {
		return nil, NewError(CodeInvalidArgument, "at most 8 tags are allowed")
	}
	sort.Strings(tags)

	rows, err := tx.Query(ctx, `
		SELECT code FROM namespaces
		WHERE code = ANY($1::text[]) AND lifecycle_status = 'active'
	`, tags)
	if err != nil {
		return nil, WrapError(CodeInternal, "verify board tags", err)
	}
	defer rows.Close()
	known := make(map[string]struct{}, len(tags))
	for rows.Next() {
		var code string
		if err := rows.Scan(&code); err != nil {
			return nil, WrapError(CodeInternal, "scan board tag", err)
		}
		known[code] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, WrapError(CodeInternal, "iterate board tags", err)
	}

	missing := make([]string, 0)
	for _, tag := range tags {
		if _, exists := known[tag]; !exists {
			missing = append(missing, tag)
		}
	}
	if len(missing) > 0 {
		serviceErr := NewError(CodeInvalidArgument, "tags must be existing active namespaces")
		serviceErr.Details = map[string]any{
			"unknown_tags": missing,
			"hint":         "use namespace_list to find the exact path, or namespace_create first",
		}

		return nil, serviceErr
	}

	return tags, nil
}

func insertBoardMessage(
	ctx context.Context,
	tx pgx.Tx,
	threadID string,
	body string,
	author string,
	actor string,
	caller domain.CallerIdentity,
) error {
	if _, err := tx.Exec(ctx, `
		INSERT INTO board_messages(id, thread_id, body, author, created_by, created_by_session, device_code, installation_code)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`,
		NewID("msg"),
		threadID,
		body,
		nullableString(author),
		actor,
		caller.SessionID,
		nullableString(caller.DeviceCode),
		nullableString(caller.InstallationCode),
	); err != nil {
		return WrapError(CodeInternal, "insert board message", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE board_threads SET message_count = message_count + 1, updated_at = statement_timestamp()
		WHERE id = $1
	`, threadID); err != nil {
		return WrapError(CodeInternal, "bump board thread", err)
	}

	return nil
}

func lockBoardThread(ctx context.Context, tx pgx.Tx, threadID string) (string, error) {
	var status string
	err := tx.QueryRow(ctx, "SELECT status FROM board_threads WHERE id = $1 FOR UPDATE", threadID).Scan(&status)
	if errorsIsNoRows(err) {
		return "", NewError(CodeNotFound, "thread not found: "+threadID)
	}
	if err != nil {
		return "", WrapError(CodeInternal, "lock board thread", err)
	}

	return status, nil
}

func loadBoardThread(ctx context.Context, tx pgx.Tx, threadID string) (domain.BoardThread, error) {
	rows, err := tx.Query(ctx, `SELECT `+boardThreadColumns+` FROM board_threads WHERE id = $1`, threadID)
	if err != nil {
		return domain.BoardThread{}, WrapError(CodeInternal, "load board thread", err)
	}
	threads, err := scanBoardThreads(rows)
	if err != nil {
		return domain.BoardThread{}, err
	}
	if len(threads) == 0 {
		return domain.BoardThread{}, NewError(CodeNotFound, "thread not found: "+threadID)
	}
	thread := threads[0]

	messageRows, err := tx.Query(ctx, `
		SELECT id, thread_id, body, coalesce(author, ''), created_by, created_by_session, created_at
		FROM board_messages WHERE thread_id = $1
		ORDER BY created_at, id
	`, threadID)
	if err != nil {
		return domain.BoardThread{}, WrapError(CodeInternal, "load board messages", err)
	}
	defer messageRows.Close()
	for messageRows.Next() {
		var message domain.BoardMessage
		if err := messageRows.Scan(
			&message.ID,
			&message.ThreadID,
			&message.Body,
			&message.Author,
			&message.CreatedBy,
			&message.CreatedBySession,
			&message.CreatedAt,
		); err != nil {
			return domain.BoardThread{}, WrapError(CodeInternal, "scan board message", err)
		}
		thread.Messages = append(thread.Messages, message)
	}
	if err := messageRows.Err(); err != nil {
		return domain.BoardThread{}, WrapError(CodeInternal, "iterate board messages", err)
	}

	return thread, nil
}

func scanBoardThreads(rows pgx.Rows) ([]domain.BoardThread, error) {
	defer rows.Close()
	threads := make([]domain.BoardThread, 0)
	for rows.Next() {
		var thread domain.BoardThread
		if err := rows.Scan(
			&thread.ID,
			&thread.Tags,
			&thread.Status,
			&thread.Resolution,
			&thread.ResolvedMemoryID,
			&thread.ResolvedBy,
			&thread.ResolvedAt,
			&thread.MessageCount,
			&thread.CreatedBy,
			&thread.CreatedAt,
			&thread.UpdatedAt,
		); err != nil {
			return nil, WrapError(CodeInternal, "scan board thread", err)
		}
		threads = append(threads, thread)
	}
	if err := rows.Err(); err != nil {
		return nil, WrapError(CodeInternal, "iterate board threads", err)
	}

	return threads, nil
}

func boardCountsLine(counts []domain.BoardTagCount) string {
	if len(counts) == 0 {
		return "board: empty"
	}
	parts := make([]string, 0, len(counts))
	for _, count := range counts {
		parts = append(parts, count.Tag+" "+strconv.Itoa(count.OpenThreads))
	}

	return "board: " + strings.Join(parts, " · ")
}
