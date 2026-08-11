package service

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/Alhamdulillah-R/memory-recall-coin/internal/domain"
)

/**
 * ListNamespaces browses known descendants of one namespace path.
 * @return namespace summaries ordered by their canonical path
 */
func (s *Store) ListNamespaces(ctx context.Context, input NamespaceListInput) (domain.NamespaceListResponse, error) {
	parent, err := normalizeNamespace(input.Parent)
	if err != nil {
		return domain.NamespaceListResponse{}, err
	}
	input.Parent = parent
	if input.Depth <= 0 {
		input.Depth = 1
	}
	if input.Depth > 16 {
		return domain.NamespaceListResponse{}, NewError(CodeInvalidArgument, "depth must be between 1 and 16")
	}
	if input.Limit <= 0 {
		input.Limit = 100
	}
	if input.Limit > 200 {
		return domain.NamespaceListResponse{}, NewError(CodeInvalidArgument, "limit must be between 1 and 200")
	}
	input.Cursor = strings.ToLower(strings.TrimSpace(input.Cursor))
	if input.Cursor != "" {
		cursor, err := normalizeNamespace(input.Cursor)
		if err != nil {
			return domain.NamespaceListResponse{}, NewError(CodeInvalidArgument, "cursor must be a namespace returned by namespace_list")
		}
		input.Cursor = cursor
		if !namespaceContains(input.Parent, input.Cursor, true) {
			return domain.NamespaceListResponse{}, NewError(CodeInvalidArgument, "cursor must be inside the requested namespace tree")
		}
	}

	var parentStatus string
	err = s.pool.QueryRow(ctx, "SELECT lifecycle_status FROM namespaces WHERE code = $1", input.Parent).Scan(&parentStatus)
	if errorsIsNoRows(err) {
		return domain.NamespaceListResponse{}, NewError(CodeNotFound, "parent namespace not found")
	}
	if err != nil {
		return domain.NamespaceListResponse{}, WrapError(CodeInternal, "read parent namespace", err)
	}
	if parentStatus == "deleted" && !input.IncludeDeleted {
		return domain.NamespaceListResponse{}, NewError(CodeNotFound, "parent namespace is deleted")
	}

	parentDepth := strings.Count(input.Parent, "/")
	rows, err := s.pool.Query(ctx, `
		WITH visible_namespaces AS (
			SELECT code, lifecycle_status, deleted_at
			FROM namespaces
			WHERE $4 OR lifecycle_status = 'active'
		), candidates AS (
			SELECT n.code, n.lifecycle_status, n.deleted_at
			FROM visible_namespaces n
			WHERE n.code LIKE $1 ESCAPE E'\\'
			  AND length(n.code) - length(replace(n.code, '/', '')) - $2 BETWEEN 1 AND $3
			  AND ($5 = '' OR n.code > $5)
			ORDER BY n.code
			LIMIT $6
		), child_counts AS (
			SELECT regexp_replace(code, '/[^/]+$', '') AS namespace, count(*) AS count
			FROM visible_namespaces
			WHERE code LIKE $1 ESCAPE E'\\'
			GROUP BY regexp_replace(code, '/[^/]+$', '')
		), direct_memory_counts AS (
			SELECT namespace, count(*) AS count
			FROM memories
			WHERE namespace LIKE $1 ESCAPE E'\\' AND lifecycle_status <> 'deleted'
			GROUP BY namespace
		), memory_paths AS (
			SELECT string_to_array(namespace, '/') AS segments
			FROM memories
			WHERE namespace LIKE $1 ESCAPE E'\\' AND lifecycle_status <> 'deleted'
		), subtree_memory_counts AS (
			SELECT array_to_string(segments[1:depth], '/') AS namespace, count(*) AS count
			FROM memory_paths
			CROSS JOIN LATERAL generate_series(1, array_length(segments, 1)) AS generated(depth)
			GROUP BY array_to_string(segments[1:depth], '/')
		), direct_source_counts AS (
			SELECT namespace, count(*) AS count
			FROM sources
			WHERE namespace LIKE $1 ESCAPE E'\\' AND lifecycle_status <> 'deleted'
			GROUP BY namespace
		), source_paths AS (
			SELECT string_to_array(namespace, '/') AS segments
			FROM sources
			WHERE namespace LIKE $1 ESCAPE E'\\' AND lifecycle_status <> 'deleted'
		), subtree_source_counts AS (
			SELECT array_to_string(segments[1:depth], '/') AS namespace, count(*) AS count
			FROM source_paths
			CROSS JOIN LATERAL generate_series(1, array_length(segments, 1)) AS generated(depth)
			GROUP BY array_to_string(segments[1:depth], '/')
		)
		SELECT
			c.code,
			coalesce(nullif(regexp_replace(c.code, '/[^/]+$', ''), c.code), ''),
			regexp_replace(c.code, '^.*/', ''),
			c.lifecycle_status,
			c.deleted_at,
			coalesce(children.count, 0),
			coalesce(direct_memories.count, 0),
			coalesce(subtree_memories.count, 0),
			coalesce(direct_sources.count, 0),
			coalesce(subtree_sources.count, 0)
		FROM candidates c
		LEFT JOIN child_counts children ON children.namespace = c.code
		LEFT JOIN direct_memory_counts direct_memories ON direct_memories.namespace = c.code
		LEFT JOIN subtree_memory_counts subtree_memories ON subtree_memories.namespace = c.code
		LEFT JOIN direct_source_counts direct_sources ON direct_sources.namespace = c.code
		LEFT JOIN subtree_source_counts subtree_sources ON subtree_sources.namespace = c.code
		ORDER BY c.code
	`,
		escapeLikePattern(input.Parent)+"/%",
		parentDepth,
		input.Depth,
		input.IncludeDeleted,
		input.Cursor,
		input.Limit+1,
	)
	if err != nil {
		return domain.NamespaceListResponse{}, WrapError(CodeInternal, "list namespace hierarchy", err)
	}
	defer rows.Close()

	namespaces := make([]domain.NamespaceSummary, 0, input.Limit+1)
	for rows.Next() {
		var summary domain.NamespaceSummary
		if err := rows.Scan(
			&summary.Namespace,
			&summary.Parent,
			&summary.Segment,
			&summary.Status,
			&summary.DeletedAt,
			&summary.ChildCount,
			&summary.DirectMemoryCount,
			&summary.SubtreeMemoryCount,
			&summary.DirectSourceCount,
			&summary.SubtreeSourceCount,
		); err != nil {
			return domain.NamespaceListResponse{}, WrapError(CodeInternal, "scan namespace hierarchy", err)
		}
		namespaces = append(namespaces, summary)
	}
	if err := rows.Err(); err != nil {
		return domain.NamespaceListResponse{}, WrapError(CodeInternal, "iterate namespace hierarchy", err)
	}

	nextCursor := ""
	if len(namespaces) > input.Limit {
		nextCursor = namespaces[input.Limit-1].Namespace
		namespaces = namespaces[:input.Limit]
	}

	return domain.NamespaceListResponse{
		Parent:     input.Parent,
		Depth:      input.Depth,
		Namespaces: namespaces,
		Count:      len(namespaces),
		NextCursor: nextCursor,
	}, nil
}

/**
 * DeleteNamespace previews or permanently removes all records owned by a namespace.
 * @return deterministic affected-row counts and deletion state
 */
func (s *Store) DeleteNamespace(ctx context.Context, input NamespaceDeleteInput) (domain.NamespaceDeleteResult, error) {
	namespace, err := normalizeNamespace(input.Namespace)
	if err != nil {
		return domain.NamespaceDeleteResult{}, err
	}
	input.Namespace = namespace
	if err := requireNonEmpty("reason", input.Reason); err != nil {
		return domain.NamespaceDeleteResult{}, err
	}

	dryRun := input.ShouldDryRun()
	actor := normalizeActor("", input.Caller)
	tx, err := s.beginMutation(ctx, actor, input.Reason)
	if err != nil {
		return domain.NamespaceDeleteResult{}, err
	}
	defer rollback(tx)
	if err := lockNamespaceCatalog(ctx, tx, false); err != nil {
		return domain.NamespaceDeleteResult{}, err
	}
	if _, err := lockNamespaceDeletionHierarchy(ctx, tx, input.Namespace); err != nil {
		return domain.NamespaceDeleteResult{}, err
	}

	var namespaceStatus string
	err = tx.QueryRow(ctx, "SELECT lifecycle_status FROM namespaces WHERE code = $1 FOR UPDATE", input.Namespace).Scan(&namespaceStatus)
	if errorsIsNoRows(err) {
		return domain.NamespaceDeleteResult{}, NewError(CodeNotFound, "namespace not found")
	}
	if err != nil {
		return domain.NamespaceDeleteResult{}, WrapError(CodeInternal, "lock namespace deletion target", err)
	}

	pattern := escapeLikePattern(input.Namespace) + "/%"
	var activeDescendants int64
	if err := tx.QueryRow(ctx, `
		SELECT count(*)
		FROM namespaces
		WHERE code LIKE $1 ESCAPE E'\\' AND lifecycle_status = 'active'
	`, pattern).Scan(&activeDescendants); err != nil {
		return domain.NamespaceDeleteResult{}, WrapError(CodeInternal, "count namespace descendants", err)
	}

	namespaces := []string{input.Namespace}
	if input.Recursive {
		rows, err := tx.Query(ctx, `
			SELECT code
			FROM namespaces
			WHERE code = $1 OR code LIKE $2 ESCAPE E'\\'
			ORDER BY length(code), code
			FOR UPDATE
		`, input.Namespace, pattern)
		if err != nil {
			return domain.NamespaceDeleteResult{}, WrapError(CodeInternal, "lock namespace subtree", err)
		}
		namespaces = namespaces[:0]
		for rows.Next() {
			var namespace string
			if err := rows.Scan(&namespace); err != nil {
				rows.Close()
				return domain.NamespaceDeleteResult{}, WrapError(CodeInternal, "scan namespace subtree", err)
			}
			namespaces = append(namespaces, namespace)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return domain.NamespaceDeleteResult{}, WrapError(CodeInternal, "iterate namespace subtree", err)
		}
		rows.Close()
	}
	if !dryRun {
		if err := lockNamespaceRecords(ctx, tx, namespaces); err != nil {
			return domain.NamespaceDeleteResult{}, err
		}
	}

	counts, err := countNamespaceRecords(ctx, tx, namespaces)
	if err != nil {
		return domain.NamespaceDeleteResult{}, err
	}
	counts.Namespaces = int64(len(namespaces))
	counts.DescendantNamespaces = activeDescendants
	result := domain.NamespaceDeleteResult{
		Namespace:         input.Namespace,
		Recursive:         input.Recursive,
		DryRun:            dryRun,
		Deleted:           namespaceStatus == "deleted",
		RequiresRecursive: !input.Recursive && activeDescendants > 0,
		Counts:            counts,
	}
	if dryRun {
		if err := tx.Commit(ctx); err != nil {
			return domain.NamespaceDeleteResult{}, WrapError(CodeInternal, "finish namespace deletion preview", err)
		}

		return result, nil
	}
	if activeDescendants > 0 && !input.Recursive {
		serviceErr := NewError(CodeFailedPrecondition, "namespace has active descendants; retry with recursive=true")
		serviceErr.Details = map[string]any{
			"namespace":             input.Namespace,
			"descendant_namespaces": activeDescendants,
			"dry_run":               true,
		}

		return domain.NamespaceDeleteResult{}, serviceErr
	}

	if err := deleteNamespaceRecords(ctx, tx, namespaces); err != nil {
		return domain.NamespaceDeleteResult{}, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE namespaces
		SET lifecycle_status = 'deleted',
			deleted_at = statement_timestamp(),
			deletion_reason = $2,
			deleted_by = $3
		WHERE code = ANY($1::text[])
		  AND lifecycle_status = 'active'
	`, namespaces, strings.TrimSpace(input.Reason), actor); err != nil {
		return domain.NamespaceDeleteResult{}, WrapError(CodeInternal, "tombstone namespace hierarchy", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.NamespaceDeleteResult{}, WrapError(CodeInternal, "commit namespace deletion", err)
	}

	result.Deleted = true
	return result, nil
}

func lockNamespaceRecords(ctx context.Context, tx pgx.Tx, namespaces []string) error {
	queries := []struct {
		name  string
		query string
	}{
		{name: "embedding jobs", query: "SELECT id FROM embedding_jobs WHERE namespace = ANY($1::text[]) ORDER BY id FOR UPDATE"},
		{name: "memories", query: "SELECT id FROM memories WHERE namespace = ANY($1::text[]) ORDER BY id FOR UPDATE"},
		{name: "source chunks", query: "SELECT id FROM source_chunks WHERE namespace = ANY($1::text[]) ORDER BY id FOR UPDATE"},
		{name: "sources", query: "SELECT id FROM sources WHERE namespace = ANY($1::text[]) ORDER BY id FOR UPDATE"},
	}
	for _, item := range queries {
		rows, err := tx.Query(ctx, item.query, namespaces)
		if err != nil {
			return WrapError(CodeInternal, "lock namespace "+item.name, err)
		}
		for rows.Next() {
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return WrapError(CodeInternal, "iterate namespace "+item.name, err)
		}
		rows.Close()
	}

	return nil
}

func countNamespaceRecords(
	ctx context.Context,
	tx pgx.Tx,
	namespaces []string,
) (domain.NamespaceDeleteCounts, error) {
	var counts domain.NamespaceDeleteCounts
	err := tx.QueryRow(ctx, `
		SELECT
			(SELECT count(*) FROM memories WHERE namespace = ANY($1::text[])),
			(SELECT count(*) FROM memory_revisions WHERE namespace = ANY($1::text[])),
			(SELECT count(*) FROM memory_relations WHERE namespace = ANY($1::text[])),
			(SELECT count(*) FROM sources WHERE namespace = ANY($1::text[])),
			(SELECT count(*) FROM source_versions WHERE namespace = ANY($1::text[])),
			(SELECT count(*) FROM source_chunks WHERE namespace = ANY($1::text[])),
			(SELECT count(*) FROM memories WHERE namespace = ANY($1::text[]) AND embedding IS NOT NULL)
			  + (SELECT count(*) FROM source_chunks WHERE namespace = ANY($1::text[]) AND embedding IS NOT NULL),
			(SELECT count(*) FROM embedding_jobs WHERE namespace = ANY($1::text[])),
			(SELECT count(*) FROM ingestion_roots WHERE namespace = ANY($1::text[])),
			(SELECT count(*) FROM ingestion_roots WHERE namespace = ANY($1::text[]) AND watch_mode = 'watch'),
			(SELECT count(*) FROM ingestion_jobs WHERE namespace = ANY($1::text[])),
			(SELECT count(*) FROM source_contents WHERE namespace = ANY($1::text[])),
			(SELECT count(*) FROM idempotency_records WHERE namespace = ANY($1::text[]))
	`, namespaces).Scan(
		&counts.Memories,
		&counts.MemoryRevisions,
		&counts.MemoryRelations,
		&counts.Sources,
		&counts.SourceVersions,
		&counts.Chunks,
		&counts.Embeddings,
		&counts.EmbeddingJobs,
		&counts.IngestionRoots,
		&counts.WatchRegistrations,
		&counts.IngestionJobs,
		&counts.SourceContents,
		&counts.IdempotencyRecords,
	)
	if err != nil {
		return domain.NamespaceDeleteCounts{}, WrapError(CodeInternal, "count namespace records", err)
	}

	return counts, nil
}

func deleteNamespaceRecords(ctx context.Context, tx pgx.Tx, namespaces []string) error {
	statements := []struct {
		name  string
		query string
	}{
		{name: "embedding jobs", query: "DELETE FROM embedding_jobs WHERE namespace = ANY($1::text[])"},
		{name: "memory relations", query: "DELETE FROM memory_relations WHERE namespace = ANY($1::text[])"},
		{name: "memory revisions", query: "DELETE FROM memory_revisions WHERE namespace = ANY($1::text[])"},
		{name: "memories", query: "DELETE FROM memories WHERE namespace = ANY($1::text[])"},
		{
			name:  "source versions",
			query: "DELETE FROM source_versions WHERE namespace = ANY($1::text[])",
		},
		{name: "source chunks", query: "DELETE FROM source_chunks WHERE namespace = ANY($1::text[])"},
		{name: "sources", query: "DELETE FROM sources WHERE namespace = ANY($1::text[])"},
		{name: "ingestion jobs", query: "DELETE FROM ingestion_jobs WHERE namespace = ANY($1::text[])"},
		{name: "ingestion roots", query: "DELETE FROM ingestion_roots WHERE namespace = ANY($1::text[])"},
		{name: "source contents", query: "DELETE FROM source_contents WHERE namespace = ANY($1::text[])"},
		{name: "idempotency records", query: "DELETE FROM idempotency_records WHERE namespace = ANY($1::text[])"},
	}
	for _, statement := range statements {
		if _, err := tx.Exec(ctx, statement.query, namespaces); err != nil {
			return WrapError(CodeInternal, "delete namespace "+statement.name, err)
		}
	}

	return nil
}

func namespaceContains(parent, namespace string, includeParent bool) bool {
	if includeParent && namespace == parent {
		return true
	}

	return strings.HasPrefix(namespace, parent+"/")
}
