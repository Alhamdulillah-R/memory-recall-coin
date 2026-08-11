package ingest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/Alhamdulillah-R/memory-recall-coin/internal/domain"
	"github.com/Alhamdulillah-R/memory-recall-coin/internal/service"
	"github.com/bmatcuk/doublestar/v4"
)

const binarySampleBytes = 8 << 10

var utf8BOM = []byte{0xef, 0xbb, 0xbf}

// Scanner 将本机文件系统内容构造成完整的 source manifest。
type Scanner struct {
	maxFileBytes int64
}

/**
 * NewScanner 创建限制单文件大小的本机 scanner。
 * @param maxFileBytes 单个文件允许读取的最大字节数
 * @return scanner 或配置错误
 */
func NewScanner(maxFileBytes int64) (*Scanner, error) {
	if maxFileBytes <= 0 || maxFileBytes == math.MaxInt64 {
		return nil, fmt.Errorf("max file bytes must be between 1 and %d", int64(math.MaxInt64-1))
	}

	return &Scanner{maxFileBytes: maxFileBytes}, nil
}

/**
 * Build 枚举并读取 input.RootPath，返回可直接提交给 service.Backend 的完整 manifest。
 * @param ctx   用于取消长目录扫描
 * @param input source 同步配置；传入的 Files 字段会被本机扫描结果覆盖
 * @return      补全后的 input、被安全跳过的文件说明和扫描错误
 */
func (s *Scanner) Build(
	ctx context.Context,
	input service.SyncSourcesInput,
) (service.SyncSourcesInput, []string, error) {
	if err := ctx.Err(); err != nil {
		return input, nil, err
	}

	rootPath, err := resolveRootPath(input.RootPath)
	if err != nil {
		return input, nil, err
	}

	matcher, err := newPathMatcher(input.Include, input.Exclude)
	if err != nil {
		return input, nil, err
	}

	rootInfo, err := os.Lstat(rootPath)
	if err != nil {
		return input, nil, fmt.Errorf("inspect ingestion root %s: %w", rootPath, err)
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 {
		return input, nil, fmt.Errorf("ingestion root must not be a symbolic link: %s", rootPath)
	}
	if !rootInfo.IsDir() && !rootInfo.Mode().IsRegular() {
		return input, nil, fmt.Errorf("ingestion root must be a regular file or directory: %s", rootPath)
	}

	input.RootPath = rootPath
	input.Files = nil
	if strings.TrimSpace(input.Parser) == "" {
		input.Parser = "auto"
	}

	files := make([]domain.IngestedFile, 0)
	skipped := make([]string, 0)

	if rootInfo.Mode().IsRegular() {
		relativePath := filepath.Base(rootPath)
		if matcher.matchesFile(filepath.ToSlash(relativePath)) {
			file, reason, readErr := s.readFile(rootPath, relativePath, input.Parser)
			if readErr != nil {
				return input, skipped, readErr
			}
			if reason != "" {
				skipped = append(skipped, reason)
			} else {
				files = append(files, file)
			}
		}
	} else {
		err = s.walkDirectory(
			ctx,
			rootPath,
			input.Recursive,
			input.Parser,
			matcher,
			&files,
			&skipped,
		)
		if err != nil {
			return input, skipped, err
		}
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].RelativePath < files[j].RelativePath
	})
	sort.Strings(skipped)

	input.Files = files

	return input, skipped, nil
}

func (s *Scanner) walkDirectory(
	ctx context.Context,
	rootPath string,
	recursive bool,
	parser string,
	matcher pathMatcher,
	files *[]domain.IngestedFile,
	skipped *[]string,
) error {
	err := filepath.WalkDir(rootPath, func(path string, entry os.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			return fmt.Errorf("walk %s: %w", path, walkErr)
		}
		if path == rootPath {
			return nil
		}

		relativePath, err := filepath.Rel(rootPath, path)
		if err != nil {
			return fmt.Errorf("resolve relative path for %s: %w", path, err)
		}

		matchPath := filepath.ToSlash(relativePath)
		if entry.IsDir() {
			if !recursive || matcher.excludesDirectory(matchPath) {
				return filepath.SkipDir
			}

			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			*skipped = append(*skipped, fmt.Sprintf("%s: symbolic link skipped", path))

			return nil
		}
		if !entry.Type().IsRegular() {
			*skipped = append(*skipped, fmt.Sprintf("%s: non-regular file skipped", path))

			return nil
		}
		if !matcher.matchesFile(matchPath) {
			return nil
		}

		file, reason, err := s.readFile(path, relativePath, parser)
		if err != nil {
			return err
		}
		if reason != "" {
			*skipped = append(*skipped, reason)

			return nil
		}

		*files = append(*files, file)

		return nil
	})
	if err != nil {
		return fmt.Errorf("scan ingestion root %s: %w", rootPath, err)
	}

	return nil
}

func (s *Scanner) readFile(
	absolutePath string,
	relativePath string,
	parser string,
) (domain.IngestedFile, string, error) {
	file, err := os.Open(absolutePath)
	if err != nil {
		return domain.IngestedFile{}, "", fmt.Errorf("open %s: %w", absolutePath, err)
	}

	before, statErr := file.Stat()
	if statErr != nil {
		closeErr := file.Close()
		if closeErr != nil {
			return domain.IngestedFile{}, "", errors.Join(
				fmt.Errorf("inspect %s: %w", absolutePath, statErr),
				fmt.Errorf("close %s: %w", absolutePath, closeErr),
			)
		}

		return domain.IngestedFile{}, "", fmt.Errorf("inspect %s: %w", absolutePath, statErr)
	}
	if !before.Mode().IsRegular() {
		closeErr := file.Close()
		if closeErr != nil {
			return domain.IngestedFile{}, "", fmt.Errorf("close %s: %w", absolutePath, closeErr)
		}

		return domain.IngestedFile{}, fmt.Sprintf("%s: non-regular file skipped", absolutePath), nil
	}
	if before.Size() > s.maxFileBytes {
		closeErr := file.Close()
		if closeErr != nil {
			return domain.IngestedFile{}, "", fmt.Errorf("close %s: %w", absolutePath, closeErr)
		}

		return domain.IngestedFile{}, fmt.Sprintf(
			"%s: file size %d exceeds max %d bytes",
			absolutePath,
			before.Size(),
			s.maxFileBytes,
		), nil
	}

	data, readErr := io.ReadAll(io.LimitReader(file, s.maxFileBytes+1))
	after, afterStatErr := file.Stat()
	closeErr := file.Close()
	if readErr != nil {
		return domain.IngestedFile{}, "", errors.Join(
			fmt.Errorf("read %s: %w", absolutePath, readErr),
			wrapCloseError(absolutePath, closeErr),
		)
	}
	if afterStatErr != nil {
		return domain.IngestedFile{}, "", errors.Join(
			fmt.Errorf("inspect %s after read: %w", absolutePath, afterStatErr),
			wrapCloseError(absolutePath, closeErr),
		)
	}
	if closeErr != nil {
		return domain.IngestedFile{}, "", fmt.Errorf("close %s: %w", absolutePath, closeErr)
	}
	if int64(len(data)) > s.maxFileBytes {
		return domain.IngestedFile{}, fmt.Sprintf(
			"%s: file size exceeds max %d bytes",
			absolutePath,
			s.maxFileBytes,
		), nil
	}
	if before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return domain.IngestedFile{}, "", fmt.Errorf("file changed while reading: %s", absolutePath)
	}

	content, text := decodeUTF8Text(data)
	if !text {
		return domain.IngestedFile{}, fmt.Sprintf("%s: binary or non-UTF-8 file skipped", absolutePath), nil
	}

	canonicalBytes := []byte(content)
	hash := sha256.Sum256(canonicalBytes)

	return domain.IngestedFile{
		AbsolutePath: absolutePath,
		RelativePath: filepath.ToSlash(relativePath),
		ContentHash:  hex.EncodeToString(hash[:]),
		Content:      content,
		Size:         int64(len(canonicalBytes)),
		MTime:        after.ModTime().UTC(),
		Parser:       resolveParser(parser, absolutePath),
	}, "", nil
}

func decodeUTF8Text(data []byte) (string, bool) {
	payload := bytes.TrimPrefix(data, utf8BOM)
	if !utf8.Valid(payload) {
		return "", false
	}

	sample := payload
	if len(sample) > binarySampleBytes {
		sample = sample[:binarySampleBytes]
	}

	controlBytes := 0
	for _, value := range sample {
		switch value {
		case '\t', '\n', '\r', '\f':
			continue
		case 0:
			return "", false
		default:
			if value < 0x20 {
				controlBytes++
			}
		}
	}
	if len(sample) > 0 && controlBytes*100 >= len(sample) {
		return "", false
	}

	return string(payload), true
}

func resolveParser(parser string, path string) string {
	parser = strings.ToLower(strings.TrimSpace(parser))
	if parser != "" && parser != "auto" {
		return parser
	}

	switch strings.ToLower(filepath.Ext(path)) {
	case ".md", ".markdown", ".mdown", ".mkd":
		return "markdown"
	default:
		return "text"
	}
}

func resolveRootPath(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", errors.New("ingestion root path is required")
	}

	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve ingestion root %s: %w", path, err)
	}

	return filepath.Clean(absolutePath), nil
}

func wrapCloseError(path string, err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("close %s: %w", path, err)
}

type pathMatcher struct {
	include []string
	exclude []string
}

func newPathMatcher(include []string, exclude []string) (pathMatcher, error) {
	normalizedInclude, err := normalizePatterns("include", include)
	if err != nil {
		return pathMatcher{}, err
	}

	normalizedExclude, err := normalizePatterns("exclude", exclude)
	if err != nil {
		return pathMatcher{}, err
	}

	return pathMatcher{
		include: normalizedInclude,
		exclude: normalizedExclude,
	}, nil
}

func (m pathMatcher) matchesFile(path string) bool {
	if matchesAny(m.exclude, path) {
		return false
	}
	if len(m.include) == 0 {
		return true
	}

	return matchesAny(m.include, path)
}

func (m pathMatcher) excludesDirectory(path string) bool {
	return matchesAny(m.exclude, path) || matchesAny(m.exclude, strings.TrimSuffix(path, "/")+"/")
}

func normalizePatterns(kind string, patterns []string) ([]string, error) {
	normalized := make([]string, 0, len(patterns))
	seen := make(map[string]struct{}, len(patterns))

	for _, pattern := range patterns {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			return nil, fmt.Errorf("%s pattern must not be empty", kind)
		}

		pattern = filepath.ToSlash(pattern)
		pattern = strings.TrimPrefix(pattern, "./")
		pattern = strings.TrimPrefix(pattern, "/")
		if pattern == "" || strings.HasPrefix(pattern, "../") || pattern == ".." {
			return nil, fmt.Errorf("%s pattern must stay relative to the ingestion root: %q", kind, pattern)
		}
		if !doublestar.ValidatePattern(pattern) {
			return nil, fmt.Errorf("invalid %s pattern %q", kind, pattern)
		}
		if _, exists := seen[pattern]; exists {
			continue
		}

		seen[pattern] = struct{}{}
		normalized = append(normalized, pattern)
	}

	return normalized, nil
}

func matchesAny(patterns []string, path string) bool {
	for _, pattern := range patterns {
		matched, err := doublestar.Match(pattern, path)
		if err == nil && matched {
			return true
		}
	}

	return false
}
