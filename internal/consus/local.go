package consus

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	stream_buffer_bytes = 1 << 20
	upload_attempts     = 3
	upload_retry_delay  = 3 * time.Second
)

// PutPathInput 描述一次本機上傳；Path 由呼叫端的機器負責解讀。
type PutPathInput struct {
	Path        string
	Namespace   string
	Name        string
	TTL         string
	Tags        []string
	Metadata    map[string]any
	MaxVersions int
	OnConflict  string
	Note        string
}

// PutPathResult 在寫入回執之外補上本機看到的路徑、是否打包過、是否真的送了 bytes。
type PutPathResult struct {
	WriteResult
	Path     string `json:"path"`
	Archived bool   `json:"archived"`
	Uploaded bool   `json:"uploaded"`
}

// localPayload 是準備好要送出的一份 bytes，可能是原檔也可能是打包出來的暫存檔。
type localPayload struct {
	path     string
	source   string
	name     string
	sha256   string
	size     int64
	archived bool
	cleanup  func() error
}

/**
 * PutPath 讀本機路徑、算 sha256，內容已經在伺服器上就直接掛載，否則才真的送 bytes。
 */
func PutPath(ctx context.Context, client *Client, input PutPathInput) (result PutPathResult, err error) {
	payload, err := prepareLocalPayload(input.Path)
	if err != nil {
		return PutPathResult{}, err
	}
	defer func() {
		if cleanupErr := payload.cleanup(); cleanupErr != nil && err == nil {
			err = cleanupErr
		}
	}()

	name := strings.TrimSpace(input.Name)
	if name == "" {
		name = payload.name
	}

	linked, ok, err := linkIfPresent(ctx, client, input, name, payload.sha256)
	if err != nil {
		return PutPathResult{}, err
	}
	if ok {
		return PutPathResult{
			WriteResult: linked,
			Path:        payload.source,
			Archived:    payload.archived,
			Uploaded:    false,
		}, nil
	}

	uploaded, err := uploadWithRetry(ctx, client, input, name, payload)
	if err != nil {
		return PutPathResult{}, err
	}

	return PutPathResult{
		WriteResult: uploaded.result,
		Path:        payload.source,
		Archived:    payload.archived,
		Uploaded:    uploaded.sentBytes,
	}, nil
}

/**
 * GetPath 把 object 取回本機檔案，回傳寫進去的位元組數。
 */
func GetPath(ctx context.Context, client *Client, objectID string, version int, destination string) (written int64, err error) {
	destination = strings.TrimSpace(destination)
	if destination == "" {
		return 0, errors.New("destination path is required")
	}
	absolute, err := filepath.Abs(destination)
	if err != nil {
		return 0, fmt.Errorf("resolve %s: %w", destination, err)
	}
	if err := os.MkdirAll(filepath.Dir(absolute), 0o755); err != nil {
		return 0, fmt.Errorf("create directory for %s: %w", absolute, err)
	}

	file, err := os.Create(absolute)
	if err != nil {
		return 0, fmt.Errorf("create %s: %w", absolute, err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()

	written, err = client.Download(ctx, objectID, version, file)
	if err != nil {
		return written, err
	}

	return written, nil
}

type uploadOutcome struct {
	result    WriteResult
	sentBytes bool
}

// uploadWithRetry 整筆重來，重試前先問 precheck：上一趟可能已經寫進去了，只是回應階段斷線。
func uploadWithRetry(
	ctx context.Context,
	client *Client,
	input PutPathInput,
	name string,
	payload localPayload,
) (uploadOutcome, error) {
	var lastErr error
	for attempt := 1; attempt <= upload_attempts; attempt++ {
		if attempt > 1 {
			linked, ok, err := linkIfPresent(ctx, client, input, name, payload.sha256)
			if err == nil && ok {
				return uploadOutcome{result: linked, sentBytes: false}, nil
			}
			select {
			case <-ctx.Done():
				return uploadOutcome{}, ctx.Err()
			case <-time.After(upload_retry_delay):
			}
		}

		result, err := uploadOnce(ctx, client, input, name, payload)
		if err == nil {
			return uploadOutcome{result: result, sentBytes: true}, nil
		}
		if ctx.Err() != nil {
			return uploadOutcome{}, err
		}
		lastErr = err
	}

	return uploadOutcome{}, fmt.Errorf("upload failed after %d attempts: %w", upload_attempts, lastErr)
}

func uploadOnce(
	ctx context.Context,
	client *Client,
	input PutPathInput,
	name string,
	payload localPayload,
) (result WriteResult, err error) {
	file, err := os.Open(payload.path)
	if err != nil {
		return WriteResult{}, fmt.Errorf("open %s: %w", payload.path, err)
	}
	defer func() {
		// transport 送完請求就會關掉 body，這裡只負責請求沒送出去時的收尾
		if closeErr := file.Close(); closeErr != nil && !errors.Is(closeErr, os.ErrClosed) && err == nil {
			err = closeErr
		}
	}()

	return client.Upload(ctx, UploadInput{
		Namespace:   input.Namespace,
		Name:        name,
		TTL:         input.TTL,
		Tags:        input.Tags,
		Metadata:    input.Metadata,
		MaxVersions: input.MaxVersions,
		OnConflict:  input.OnConflict,
		ContentType: contentTypeFor(name),
		SHA256:      payload.sha256,
		Size:        payload.size,
		Note:        input.Note,
		Body:        file,
	})
}

// linkIfPresent 命中內容定址就掛載，完全不傳 bytes。
func linkIfPresent(
	ctx context.Context,
	client *Client,
	input PutPathInput,
	name string,
	digest string,
) (WriteResult, bool, error) {
	precheck, err := client.Precheck(ctx, digest)
	if err != nil {
		return WriteResult{}, false, err
	}
	if !precheck.Exists {
		return WriteResult{}, false, nil
	}

	linked, err := client.Link(ctx, LinkInput{
		SHA256:      digest,
		Namespace:   input.Namespace,
		Name:        name,
		TTL:         input.TTL,
		Tags:        input.Tags,
		Metadata:    input.Metadata,
		MaxVersions: input.MaxVersions,
		OnConflict:  input.OnConflict,
		Note:        input.Note,
	})
	if err != nil {
		return WriteResult{}, false, err
	}

	return linked, true, nil
}

// prepareLocalPayload 單檔原樣送出，目錄先打成 tar.gz 暫存檔。
func prepareLocalPayload(path string) (localPayload, error) {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return localPayload{}, errors.New("path is required")
	}
	absolute, err := filepath.Abs(trimmed)
	if err != nil {
		return localPayload{}, fmt.Errorf("resolve %s: %w", trimmed, err)
	}
	info, err := os.Stat(absolute)
	if err != nil {
		return localPayload{}, fmt.Errorf("stat %s: %w", absolute, err)
	}

	if info.IsDir() {
		packed, err := packDirectory(absolute)
		if err != nil {
			return localPayload{}, err
		}

		return localPayload{
			path:     packed.path,
			source:   absolute,
			name:     packed.name,
			sha256:   packed.sha256,
			size:     packed.size,
			archived: true,
			cleanup:  packed.Remove,
		}, nil
	}
	if !info.Mode().IsRegular() {
		return localPayload{}, fmt.Errorf("%s is not a regular file", absolute)
	}

	digest, err := hashFile(absolute)
	if err != nil {
		return localPayload{}, err
	}

	return localPayload{
		path:    absolute,
		source:  absolute,
		name:    filepath.Base(absolute),
		sha256:  digest,
		size:    info.Size(),
		cleanup: func() error { return nil },
	}, nil
}

func hashFile(path string) (digest string, err error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open %s: %w", path, err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()

	hash := sha256.New()
	buffer := make([]byte, stream_buffer_bytes)
	if _, err := io.CopyBuffer(hash, file, buffer); err != nil {
		return "", fmt.Errorf("hash %s: %w", path, err)
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

// contentTypeFor 按副檔名推型別；.har 不在系統 mime table 裡，單獨映射。
func contentTypeFor(name string) string {
	if strings.EqualFold(filepath.Ext(name), ".har") {
		return "application/json"
	}
	guessed := mime.TypeByExtension(filepath.Ext(name))
	if guessed == "" {
		return "application/octet-stream"
	}

	return guessed
}
