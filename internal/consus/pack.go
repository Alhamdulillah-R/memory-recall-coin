package consus

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

const max_archive_bytes = 1 << 30

// archive 是打包後落在暫存檔的結果，用完要呼叫 Remove。
type archive struct {
	path   string
	name   string
	sha256 string
	size   int64
}

// Remove 刪掉暫存檔。
func (a archive) Remove() error {
	if a.path == "" {
		return nil
	}

	return os.Remove(a.path)
}

/**
 * packDirectory 把目錄打成 tar.gz 暫存檔，邊寫邊算雜湊與大小，超過上限立刻中止。
 *
 * 歸檔參數對齊 consus CLI 的 Prepare()：PAX 格式、清空 atime 與 ctime、字典序、symlink 不跟隨。
 * 清空而不是填固定值，是因為 Go 的 tar 在 PAX 下看到零值就不寫那條 extended record。
 */
func packDirectory(root string) (archive, error) {
	base := filepath.Base(root)
	file, err := os.CreateTemp("", "consus-pack-*.tar.gz")
	if err != nil {
		return archive{}, fmt.Errorf("create archive temp file: %w", err)
	}
	result := archive{path: file.Name(), name: base + ".tar.gz"}

	digest := sha256.New()
	counter := &countingWriter{}
	gzipWriter, err := gzip.NewWriterLevel(io.MultiWriter(file, digest, counter), gzip.DefaultCompression)
	if err != nil {
		closeAndRemove(file, result)

		return archive{}, fmt.Errorf("create gzip writer: %w", err)
	}
	tarWriter := tar.NewWriter(gzipWriter)

	walkErr := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if counter.written > max_archive_bytes {
			return fmt.Errorf("archive exceeds the %d byte limit", int64(max_archive_bytes))
		}

		return writeArchiveEntry(tarWriter, root, base, path, info)
	})
	if walkErr != nil {
		closeAndRemove(file, result)

		return archive{}, walkErr
	}

	if err := tarWriter.Close(); err != nil {
		closeAndRemove(file, result)

		return archive{}, fmt.Errorf("finish tar: %w", err)
	}
	if err := gzipWriter.Close(); err != nil {
		closeAndRemove(file, result)

		return archive{}, fmt.Errorf("finish gzip: %w", err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(result.path)

		return archive{}, fmt.Errorf("close archive: %w", err)
	}
	if counter.written > max_archive_bytes {
		_ = os.Remove(result.path)

		return archive{}, fmt.Errorf("archive exceeds the %d byte limit", int64(max_archive_bytes))
	}

	result.sha256 = hex.EncodeToString(digest.Sum(nil))
	result.size = counter.written

	return result, nil
}

func writeArchiveEntry(writer *tar.Writer, root string, base string, path string, info os.FileInfo) error {
	link := ""
	if info.Mode()&os.ModeSymlink != 0 {
		target, err := os.Readlink(path)
		if err != nil {
			return fmt.Errorf("read link %s: %w", path, err)
		}
		link = target
	}

	header, err := tar.FileInfoHeader(info, link)
	if err != nil {
		return fmt.Errorf("build tar header for %s: %w", path, err)
	}
	relative, err := filepath.Rel(root, path)
	if err != nil {
		return fmt.Errorf("resolve %s against %s: %w", path, root, err)
	}
	header.Format = tar.FormatPAX
	header.AccessTime = time.Time{}
	header.ChangeTime = time.Time{}
	header.Name = base
	if relative != "." {
		header.Name = base + "/" + filepath.ToSlash(relative)
	}

	if err := writer.WriteHeader(header); err != nil {
		return fmt.Errorf("write tar header for %s: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return nil
	}

	source, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer func() {
		if closeErr := source.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()

	if _, err := io.Copy(writer, source); err != nil {
		return fmt.Errorf("copy %s into archive: %w", path, err)
	}

	return nil
}

func closeAndRemove(file *os.File, result archive) {
	_ = file.Close()
	_ = os.Remove(result.path)
}

// countingWriter 只累加寫出的位元組數，用來在打包途中就擋住超大目錄。
type countingWriter struct {
	written int64
}

func (w *countingWriter) Write(data []byte) (int, error) {
	w.written += int64(len(data))

	return len(data), nil
}
