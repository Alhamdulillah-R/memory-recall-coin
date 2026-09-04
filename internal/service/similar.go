package service

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/Alhamdulillah-R/memory-recall-coin/internal/domain"
)

const (
	// 低於這個相似度的不回報
	similarNoticeThreshold = 0.45
	// 標題或內容超過這些門檻視為近似重複，預設拒絕寫入
	similarTitleThreshold   = 0.7
	similarContentThreshold = 0.75
	similarCandidateLimit   = 5
)

/**
 * findSimilarMemories 在同 namespace 的 active memory 裡找標題或內容相近的候選，excludeID 用來略過被 supersede 的目標。
 */
func findSimilarMemories(
	ctx context.Context,
	tx pgx.Tx,
	namespace string,
	title string,
	content string,
	excludeID string,
) ([]domain.SimilarMemory, error) {
	title = strings.TrimSpace(title)
	content = strings.TrimSpace(content)

	rows, err := tx.Query(ctx, `
		SELECT id, title, version, lifecycle_status, updated_at,
		       similarity(lower(title), lower($2))::double precision,
		       similarity(content, $3)::double precision
		FROM memories
		WHERE namespace = $1
		  AND lifecycle_status = 'active'
		  AND ($4 = '' OR id <> $4)
		  AND (
		      lower(title) = lower($2)
		      OR similarity(lower(title), lower($2)) >= $5
		      OR similarity(content, $3) >= $5
		  )
		ORDER BY greatest(similarity(lower(title), lower($2)), similarity(content, $3)) DESC, updated_at DESC
		LIMIT $6
	`, namespace, title, content, excludeID, similarNoticeThreshold, similarCandidateLimit)
	if err != nil {
		return nil, WrapError(CodeInternal, "query similar memories", err)
	}
	defer rows.Close()

	candidates := make([]domain.SimilarMemory, 0, similarCandidateLimit)
	for rows.Next() {
		var candidate domain.SimilarMemory
		if err := rows.Scan(
			&candidate.ID,
			&candidate.Title,
			&candidate.Version,
			&candidate.Status,
			&candidate.UpdatedAt,
			&candidate.TitleSimilarity,
			&candidate.ContentSimilarity,
		); err != nil {
			return nil, WrapError(CodeInternal, "scan similar memory", err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, WrapError(CodeInternal, "iterate similar memories", err)
	}

	return candidates, nil
}

/**
 * rejectNearDuplicates 在沒有 allow_similar 時，把達到重複門檻的候選變成 FAILED_PRECONDITION。
 */
func rejectNearDuplicates(candidates []domain.SimilarMemory, title string, allowSimilar bool) error {
	if allowSimilar {
		return nil
	}

	duplicates := make([]domain.SimilarMemory, 0, len(candidates))
	for _, candidate := range candidates {
		if isNearDuplicate(candidate, title) {
			duplicates = append(duplicates, candidate)
		}
	}
	if len(duplicates) == 0 {
		return nil
	}

	serviceErr := NewError(CodeFailedPrecondition, "a similar active memory already exists in this namespace")
	serviceErr.Details = map[string]any{
		"similar_memories": duplicates,
		"hint":             "memory_patch or memory_supersede the existing memory, or retry memory_put with allow_similar=true",
	}

	return serviceErr
}

func isNearDuplicate(candidate domain.SimilarMemory, title string) bool {
	if strings.EqualFold(strings.TrimSpace(candidate.Title), strings.TrimSpace(title)) {
		return true
	}

	return candidate.TitleSimilarity >= similarTitleThreshold || candidate.ContentSimilarity >= similarContentThreshold
}
