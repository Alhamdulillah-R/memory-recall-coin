package identity

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/Alhamdulillah-R/memory-recall-coin/internal/domain"
)

// State is the locally persisted logical identity used by the stdio bridge.
type State struct {
	DeviceCode       string    `json:"device_code"`
	InstallationCode string    `json:"installation_code"`
	TailnetIdentity  string    `json:"tailnet_identity,omitempty"`
	Hostname         string    `json:"hostname,omitempty"`
	UpdatedAt        time.Time `json:"updated_at"`
}

// Discovery contains transient local registration signals.
type Discovery struct {
	Hostname        string
	TailnetIdentity string
	Signals         []domain.HardwareSignal
}

/**
 * Load reads a persisted local identity.
 * @return state, whether it exists, and any I/O error
 */
func Load(path string) (State, bool, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return State{}, false, nil
	}
	if err != nil {
		return State{}, false, fmt.Errorf("read identity file %s: %w", path, err)
	}

	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		return State{}, false, fmt.Errorf("parse identity file %s: %w", path, err)
	}
	if strings.TrimSpace(state.DeviceCode) == "" || strings.TrimSpace(state.InstallationCode) == "" {
		return State{}, false, fmt.Errorf("identity file %s is incomplete", path)
	}

	return state, true, nil
}

/**
 * Save atomically persists a local identity with user-only permissions where supported.
 * @param path destination identity file
 * @param state resolved identity
 */
func Save(path string, state State) error {
	state.UpdatedAt = time.Now().UTC()
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode local identity: %w", err)
	}
	data = append(data, '\n')

	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create identity directory %s: %w", directory, err)
	}
	temporary, err := os.CreateTemp(directory, ".identity-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary identity file: %w", err)
	}
	temporaryPath := temporary.Name()
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(temporaryPath)
		}
	}()

	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("restrict temporary identity file: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write temporary identity file: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync temporary identity file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary identity file: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("replace identity file %s: %w", path, err)
	}
	committed = true

	return nil
}

/**
 * Discover collects stable hardware and Tailscale signals without persisting raw values.
 * @return local discovery result
 */
func Discover(ctx context.Context) (Discovery, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return Discovery{}, fmt.Errorf("read hostname: %w", err)
	}

	values := make(map[string]string)
	values["hostname"] = hostname
	values["cpu"] = runtime.GOOS + "/" + runtime.GOARCH

	switch runtime.GOOS {
	case "windows":
		windowsValues, err := discoverWindows(ctx)
		if err == nil {
			for key, value := range windowsValues {
				values[key] = value
			}
		}
	case "linux":
		for key, path := range map[string]string{
			"smbios_uuid":      "/sys/class/dmi/id/product_uuid",
			"baseboard_serial": "/sys/class/dmi/id/board_serial",
			"bios_serial":      "/sys/class/dmi/id/product_serial",
		} {
			if value, err := readSignalFile(path); err == nil {
				values[key] = value
			}
		}
		if value := firstDiskSerial(); value != "" {
			values["disk_serial"] = value
		}
	}

	tailnetIdentity, tailnetNodeID := discoverTailscale(ctx)
	if tailnetNodeID != "" {
		values["tailscale_node_id"] = tailnetNodeID
	}

	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	signals := make([]domain.HardwareSignal, 0, len(keys))
	for _, key := range keys {
		value := normalizeSignal(values[key])
		if value == "" || isPlaceholderSignal(value) {
			continue
		}
		signals = append(signals, domain.HardwareSignal{Type: key, Value: value})
	}

	return Discovery{
		Hostname:        hostname,
		TailnetIdentity: tailnetIdentity,
		Signals:         signals,
	}, nil
}

func discoverWindows(ctx context.Context) (map[string]string, error) {
	script := `$ErrorActionPreference = 'Stop'
$system = Get-CimInstance Win32_ComputerSystemProduct
$board = Get-CimInstance Win32_BaseBoard | Select-Object -First 1
$bios = Get-CimInstance Win32_BIOS | Select-Object -First 1
$disk = Get-CimInstance Win32_DiskDrive | Sort-Object Index | Select-Object -First 1
[ordered]@{
  smbios_uuid = [string]$system.UUID
  baseboard_serial = [string]$board.SerialNumber
  bios_serial = [string]$bios.SerialNumber
  disk_serial = [string]$disk.SerialNumber
} | ConvertTo-Json -Compress`
	command := exec.CommandContext(
		ctx,
		"powershell.exe",
		"-NoLogo",
		"-NoProfile",
		"-NonInteractive",
		"-Command",
		script,
	)
	output, err := command.Output()
	if err != nil {
		return nil, fmt.Errorf("collect Windows hardware signals: %w", err)
	}

	values := make(map[string]string)
	if err := json.Unmarshal(output, &values); err != nil {
		return nil, fmt.Errorf("decode Windows hardware signals: %w", err)
	}

	return values, nil
}

func discoverTailscale(ctx context.Context) (string, string) {
	command := exec.CommandContext(ctx, "tailscale", "status", "--json")
	output, err := command.Output()
	if err != nil {
		return "", ""
	}

	var status struct {
		Self struct {
			ID       string `json:"ID"`
			DNSName  string `json:"DNSName"`
			HostName string `json:"HostName"`
			UserID   int64  `json:"UserID"`
		} `json:"Self"`
	}
	if json.Unmarshal(output, &status) != nil {
		return "", ""
	}

	identity := strings.TrimSuffix(status.Self.DNSName, ".")
	if identity == "" {
		identity = status.Self.ID
	}

	return identity, status.Self.ID
}

func readSignalFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(data)), nil
}

func firstDiskSerial() string {
	paths, err := filepath.Glob("/sys/block/*/device/serial")
	if err != nil {
		return ""
	}
	sort.Strings(paths)
	for _, path := range paths {
		value, err := readSignalFile(path)
		if err == nil && normalizeSignal(value) != "" {
			return value
		}
	}

	return ""
}

func normalizeSignal(value string) string {
	scanner := bufio.NewScanner(strings.NewReader(value))
	parts := make([]string, 0, 2)
	for scanner.Scan() {
		line := strings.ToLower(strings.TrimSpace(scanner.Text()))
		if line != "" {
			parts = append(parts, line)
		}
	}

	return strings.Join(parts, " ")
}

func isPlaceholderSignal(value string) bool {
	switch value {
	case "none", "unknown", "default string", "to be filled by o.e.m.", "system serial number":
		return true
	default:
		return false
	}
}
