package service

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/Alhamdulillah-R/memory-recall-coin/internal/domain"
)

const autoDeviceMatchThreshold = 90
const claimDeviceMatchThreshold = 30

var signalWeights = map[string]int{
	"tpm_ek":            100,
	"smbios_uuid":       90,
	"baseboard_serial":  80,
	"bios_serial":       70,
	"disk_serial":       30,
	"tailscale_node_id": 30,
	"hostname":          10,
	"cpu":               5,
}

type digestedSignal struct {
	Type   string
	Digest []byte
	Weight int
}

/**
 * RegisterDevice creates a new installation and resolves a stable logical device.
 * @return registration result with any explicit-claim candidates
 */
func (s *Store) RegisterDevice(ctx context.Context, input RegisterDeviceInput) (domain.RegistrationResult, error) {
	if input.InstallationCode == "" {
		input.InstallationCode = NewID("inst")
	}
	if input.DisplayName == "" {
		input.DisplayName = input.Hostname
	}
	if input.DisplayName == "" {
		input.DisplayName = "Unnamed device"
	}
	actor := normalizeActor(input.Actor, input.Caller)
	signals, err := s.digestSignals(input.Signals)
	if err != nil {
		return domain.RegistrationResult{}, err
	}

	tx, err := s.beginMutation(ctx, actor, "device_register")
	if err != nil {
		return domain.RegistrationResult{}, err
	}
	defer rollback(tx)
	if err := lockInstallation(ctx, tx, input.InstallationCode); err != nil {
		return domain.RegistrationResult{}, err
	}
	if input.TailnetIdentity != "" {
		if err := lockTailnetIdentity(ctx, tx, input.TailnetIdentity); err != nil {
			return domain.RegistrationResult{}, err
		}
	}

	existing, found, err := findInstallation(ctx, tx, input.InstallationCode)
	if err != nil {
		return domain.RegistrationResult{}, err
	}
	if input.TailnetIdentity != "" {
		boundInstallation, bound, err := findTailnetInstallation(ctx, tx, input.TailnetIdentity)
		if err != nil {
			return domain.RegistrationResult{}, err
		}
		if bound && boundInstallation != input.InstallationCode {
			return domain.RegistrationResult{}, NewError(CodeAlreadyExists, "tailnet identity is already bound to another installation")
		}
	}
	if found {
		if input.Caller.InstallationCode != input.InstallationCode {
			return domain.RegistrationResult{}, NewError(CodeUnauthorized, "existing installation registration requires its verified caller identity")
		}
		if existing.Status != "active" {
			return domain.RegistrationResult{}, NewError(CodeUnauthorized, "inactive installation cannot refresh registration")
		}
		if err := s.refreshInstallation(ctx, tx, existing, input, signals); err != nil {
			return domain.RegistrationResult{}, err
		}
		existing, found, err = findInstallation(ctx, tx, input.InstallationCode)
		if err != nil {
			return domain.RegistrationResult{}, err
		}
		if !found {
			return domain.RegistrationResult{}, NewError(CodeInternal, "installation disappeared during registration refresh")
		}
		device, err := findDevice(ctx, tx, existing.DeviceCode)
		if err != nil {
			return domain.RegistrationResult{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return domain.RegistrationResult{}, WrapError(CodeInternal, "commit device registration refresh", err)
		}

		return domain.RegistrationResult{Device: device, Installation: existing, MatchScore: 100}, nil
	}

	matches, err := s.findDeviceMatches(ctx, tx, signals)
	if err != nil {
		return domain.RegistrationResult{}, err
	}
	selectedDevice := ""
	selectedScore := 0
	claimCandidates := sortedDeviceMatches(matches)
	if len(claimCandidates) > 0 {
		selectedScore = claimCandidates[0].Score
		if selectedScore >= autoDeviceMatchThreshold && (len(claimCandidates) == 1 || claimCandidates[1].Score < selectedScore) {
			selectedDevice = claimCandidates[0].DeviceCode
		}
	}

	claimRequired := false
	if selectedDevice == "" {
		requested := strings.TrimSpace(input.RequestedDeviceCode)
		if requested != "" {
			_, err := findDevice(ctx, tx, requested)
			if err == nil {
				claimRequired = true
			} else if ErrorCode(err) == CodeNotFound {
				selectedDevice = requested
			} else {
				return domain.RegistrationResult{}, err
			}
		}
	}
	if selectedDevice == "" {
		selectedDevice = NewID("dev")
		if len(claimCandidates) > 0 && claimCandidates[0].Score >= claimDeviceMatchThreshold {
			claimRequired = true
		}
	}
	if err := lockDeviceCodes(ctx, tx, selectedDevice); err != nil {
		return domain.RegistrationResult{}, err
	}

	device, err := ensureDevice(ctx, tx, selectedDevice, input.DisplayName)
	if err != nil {
		return domain.RegistrationResult{}, err
	}
	if device.Status != "active" {
		return domain.RegistrationResult{}, NewError(CodeConflict, "matched device is no longer active")
	}
	installation, err := insertInstallation(ctx, tx, input, selectedDevice)
	if err != nil {
		return domain.RegistrationResult{}, err
	}
	if err := upsertSignals(ctx, tx, installation.InstallationCode, selectedDevice, signals); err != nil {
		return domain.RegistrationResult{}, err
	}
	if _, err := tx.Exec(ctx, `
        INSERT INTO device_audit(
            installation_code, target_device_code, operation, actor, details
        ) VALUES ($1, $2, 'register', $3, jsonb_build_object('match_score', $4, 'claim_required', $5))
    `, installation.InstallationCode, selectedDevice, actor, selectedScore, claimRequired); err != nil {
		return domain.RegistrationResult{}, WrapError(CodeInternal, "audit device registration", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.RegistrationResult{}, WrapError(CodeInternal, "commit device registration", err)
	}

	return domain.RegistrationResult{
		Device:          device,
		Installation:    installation,
		MatchScore:      selectedScore,
		ClaimRequired:   claimRequired,
		ClaimCandidates: claimCandidates,
	}, nil
}

/**
 * ClaimDevice explicitly binds one installation to an existing device.
 * @return resolved current identity
 */
func (s *Store) ClaimDevice(ctx context.Context, input ClaimDeviceInput) (WhoAmIResult, error) {
	if !input.Confirm {
		return WhoAmIResult{}, NewError(CodeInvalidArgument, "confirm must be true for device_claim")
	}
	if err := requireNonEmpty("installation_code", input.InstallationCode); err != nil {
		return WhoAmIResult{}, err
	}
	if err := requireNonEmpty("target_device_code", input.TargetDeviceCode); err != nil {
		return WhoAmIResult{}, err
	}
	if input.Caller.InstallationCode != input.InstallationCode {
		return WhoAmIResult{}, NewError(CodeUnauthorized, "device_claim can only modify the caller installation")
	}

	actor := normalizeActor(input.Actor, input.Caller)
	tx, err := s.beginMutation(ctx, actor, input.Reason)
	if err != nil {
		return WhoAmIResult{}, err
	}
	defer rollback(tx)
	if err := lockInstallation(ctx, tx, input.InstallationCode); err != nil {
		return WhoAmIResult{}, err
	}

	installation, found, err := findInstallation(ctx, tx, input.InstallationCode)
	if err != nil {
		return WhoAmIResult{}, err
	}
	if !found {
		return WhoAmIResult{}, NewError(CodeNotFound, "installation not found")
	}
	if installation.Status != "active" {
		return WhoAmIResult{}, NewError(CodeUnauthorized, "inactive installation cannot claim a device")
	}
	device, err := findDevice(ctx, tx, input.TargetDeviceCode)
	if err != nil {
		return WhoAmIResult{}, err
	}
	if device.Status != "active" {
		return WhoAmIResult{}, NewError(CodeInvalidArgument, "target device is not active")
	}

	if _, err := tx.Exec(ctx, `
        UPDATE installations SET device_code = $2, version = version + 1,
            updated_at = statement_timestamp(), last_seen_at = statement_timestamp()
        WHERE installation_code = $1
    `, input.InstallationCode, device.DeviceCode); err != nil {
		return WhoAmIResult{}, WrapError(CodeInternal, "claim installation", err)
	}
	if _, err := tx.Exec(ctx, `
        UPDATE device_signals SET device_code = $2, last_seen_at = statement_timestamp()
        WHERE installation_code = $1
    `, input.InstallationCode, device.DeviceCode); err != nil {
		return WhoAmIResult{}, WrapError(CodeInternal, "claim installation signals", err)
	}
	if _, err := tx.Exec(ctx, `
        INSERT INTO device_audit(
            installation_code, source_device_code, target_device_code, operation, actor, details
        ) VALUES ($1, $2, $3, 'claim', $4, jsonb_build_object('reason', $5))
	`, input.InstallationCode, installation.DeviceCode, device.DeviceCode, actor, input.Reason); err != nil {
		return WhoAmIResult{}, WrapError(CodeInternal, "audit device claim", err)
	}
	installation, found, err = findInstallation(ctx, tx, input.InstallationCode)
	if err != nil {
		return WhoAmIResult{}, err
	}
	if !found {
		return WhoAmIResult{}, NewError(CodeInternal, "installation disappeared during device claim")
	}
	if err := tx.Commit(ctx); err != nil {
		return WhoAmIResult{}, WrapError(CodeInternal, "commit device claim", err)
	}

	return WhoAmIResult{
		Registered:   true,
		Device:       &device,
		Installation: &installation,
		Caller: domain.CallerIdentity{
			DeviceCode:       device.DeviceCode,
			InstallationCode: installation.InstallationCode,
			TailnetIdentity:  installation.TailnetIdentity,
		},
	}, nil
}

/**
 * MigrateDevice aliases a source logical device into a canonical target without rewriting provenance.
 * @return target identity
 */
func (s *Store) MigrateDevice(ctx context.Context, input MigrateDeviceInput) (WhoAmIResult, error) {
	if !input.Confirm {
		return WhoAmIResult{}, NewError(CodeInvalidArgument, "confirm must be true for device_migrate")
	}
	if err := requireNonEmpty("source_device_code", input.SourceDeviceCode); err != nil {
		return WhoAmIResult{}, err
	}
	if err := requireNonEmpty("target_device_code", input.TargetDeviceCode); err != nil {
		return WhoAmIResult{}, err
	}
	if input.SourceDeviceCode == input.TargetDeviceCode {
		return WhoAmIResult{}, NewError(CodeInvalidArgument, "source and target device must differ")
	}
	if input.Caller.DeviceCode != input.SourceDeviceCode {
		return WhoAmIResult{}, NewError(CodeUnauthorized, "device_migrate can only migrate the caller device")
	}

	actor := normalizeActor(input.Actor, input.Caller)
	tx, err := s.beginMutation(ctx, actor, input.Reason)
	if err != nil {
		return WhoAmIResult{}, err
	}
	defer rollback(tx)
	if err := lockDeviceCodes(ctx, tx, input.SourceDeviceCode, input.TargetDeviceCode); err != nil {
		return WhoAmIResult{}, err
	}

	source, err := findDevice(ctx, tx, input.SourceDeviceCode)
	if err != nil {
		return WhoAmIResult{}, err
	}
	if source.Status != "active" {
		return WhoAmIResult{}, NewError(CodeConflict, "source device is not active")
	}
	target, err := findDevice(ctx, tx, input.TargetDeviceCode)
	if err != nil {
		return WhoAmIResult{}, err
	}
	if target.Status != "active" {
		return WhoAmIResult{}, NewError(CodeInvalidArgument, "target device is not active")
	}

	command, err := tx.Exec(ctx, `
        UPDATE devices SET status = 'merged', merged_into_device_code = $2,
            version = version + 1, updated_at = statement_timestamp()
        WHERE device_code = $1 AND status <> 'merged'
	`, input.SourceDeviceCode, input.TargetDeviceCode)
	if err != nil {
		return WhoAmIResult{}, WrapError(CodeInternal, "mark source device merged", err)
	}
	if command.RowsAffected() != 1 {
		return WhoAmIResult{}, NewError(CodeConflict, "source device was concurrently migrated")
	}
	if _, err := tx.Exec(ctx, `
        UPDATE installations SET device_code = $2, version = version + 1,
            updated_at = statement_timestamp()
        WHERE device_code = $1
    `, input.SourceDeviceCode, input.TargetDeviceCode); err != nil {
		return WhoAmIResult{}, WrapError(CodeInternal, "migrate installations", err)
	}
	if _, err := tx.Exec(ctx, `
        UPDATE device_signals SET device_code = $2, last_seen_at = statement_timestamp()
        WHERE device_code = $1
    `, input.SourceDeviceCode, input.TargetDeviceCode); err != nil {
		return WhoAmIResult{}, WrapError(CodeInternal, "migrate device signals", err)
	}
	if _, err := tx.Exec(ctx, `
        INSERT INTO device_audit(source_device_code, target_device_code, operation, actor, details)
        VALUES ($1, $2, 'migrate', $3, jsonb_build_object('reason', $4))
    `, input.SourceDeviceCode, input.TargetDeviceCode, actor, input.Reason); err != nil {
		return WhoAmIResult{}, WrapError(CodeInternal, "audit device migration", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return WhoAmIResult{}, WrapError(CodeInternal, "commit device migration", err)
	}

	return WhoAmIResult{
		Registered: true,
		Device:     &target,
		Caller: domain.CallerIdentity{
			DeviceCode: target.DeviceCode,
		},
	}, nil
}

/**
 * WhoAmI resolves an installation and follows a merged device alias.
 * @return current identity state
 */
func (s *Store) WhoAmI(ctx context.Context, input WhoAmIInput) (WhoAmIResult, error) {
	installationCode := input.InstallationCode
	if installationCode == "" {
		installationCode = input.Caller.InstallationCode
	}
	if installationCode == "" {
		return WhoAmIResult{Registered: false, Caller: input.Caller}, nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return WhoAmIResult{}, WrapError(CodeUnavailable, "begin identity lookup", err)
	}
	defer rollback(tx)

	installation, found, err := findInstallation(ctx, tx, installationCode)
	if err != nil {
		return WhoAmIResult{}, err
	}
	if !found {
		return WhoAmIResult{Registered: false, Caller: input.Caller}, nil
	}
	if installation.Status != "active" {
		return WhoAmIResult{Registered: false, Caller: input.Caller}, nil
	}
	device, err := findDevice(ctx, tx, installation.DeviceCode)
	if err != nil {
		return WhoAmIResult{}, err
	}
	if device.MergedIntoDeviceCode != "" {
		device, err = findDevice(ctx, tx, device.MergedIntoDeviceCode)
		if err != nil {
			return WhoAmIResult{}, err
		}
	}
	if device.Status != "active" {
		return WhoAmIResult{Registered: false, Caller: input.Caller}, nil
	}

	return WhoAmIResult{
		Registered:   true,
		Device:       &device,
		Installation: &installation,
		Caller: domain.CallerIdentity{
			DeviceCode:       device.DeviceCode,
			InstallationCode: installation.InstallationCode,
			WorkspaceCode:    input.Caller.WorkspaceCode,
			TailnetIdentity:  installation.TailnetIdentity,
			Actor:            input.Caller.Actor,
		},
	}, nil
}

func (s *Store) digestSignals(signals []domain.HardwareSignal) ([]digestedSignal, error) {
	seen := make(map[string]struct{}, len(signals))
	result := make([]digestedSignal, 0, len(signals))
	for _, signal := range signals {
		signalType := strings.ToLower(strings.TrimSpace(signal.Type))
		value := strings.TrimSpace(signal.Value)
		weight, ok := signalWeights[signalType]
		if !ok {
			return nil, NewError(CodeInvalidArgument, "unsupported hardware signal type: "+signalType)
		}
		if value == "" {
			continue
		}
		if _, ok := seen[signalType]; ok {
			continue
		}
		seen[signalType] = struct{}{}

		mac := hmac.New(sha256.New, s.signalHMACSecret)
		_, _ = mac.Write([]byte(signalType))
		_, _ = mac.Write([]byte{0})
		_, _ = mac.Write([]byte(value))
		result = append(result, digestedSignal{
			Type:   signalType,
			Digest: mac.Sum(nil),
			Weight: weight,
		})
	}

	return result, nil
}

func (s *Store) findDeviceMatches(ctx context.Context, tx pgx.Tx, signals []digestedSignal) (map[string]int, error) {
	typeSignalScores := make(map[string]map[string]int)
	for _, signal := range signals {
		rows, err := tx.Query(ctx, `
			SELECT signals.device_code, max(signals.weight)
			FROM device_signals signals
			JOIN devices device ON device.device_code = signals.device_code AND device.status = 'active'
			WHERE signals.signal_type = $1 AND signals.signal_digest = $2
			GROUP BY signals.device_code
        `, signal.Type, signal.Digest)
		if err != nil {
			return nil, WrapError(CodeInternal, "match device signal", err)
		}
		for rows.Next() {
			var deviceCode string
			var weight int
			if err := rows.Scan(&deviceCode, &weight); err != nil {
				rows.Close()
				return nil, WrapError(CodeInternal, "scan device signal match", err)
			}
			if typeSignalScores[deviceCode] == nil {
				typeSignalScores[deviceCode] = make(map[string]int)
			}
			if weight > typeSignalScores[deviceCode][signal.Type] {
				typeSignalScores[deviceCode][signal.Type] = weight
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, WrapError(CodeInternal, "iterate device signal matches", err)
		}
		rows.Close()
	}

	result := make(map[string]int, len(typeSignalScores))
	for deviceCode, scores := range typeSignalScores {
		for _, score := range scores {
			result[deviceCode] += score
		}
	}

	return result, nil
}

func sortedDeviceMatches(scores map[string]int) []domain.DeviceMatch {
	result := make([]domain.DeviceMatch, 0, len(scores))
	for deviceCode, score := range scores {
		result = append(result, domain.DeviceMatch{DeviceCode: deviceCode, Score: score})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Score != result[j].Score {
			return result[i].Score > result[j].Score
		}
		return result[i].DeviceCode < result[j].DeviceCode
	})

	return result
}

func findInstallation(ctx context.Context, tx pgx.Tx, installationCode string) (domain.Installation, bool, error) {
	var installation domain.Installation
	err := tx.QueryRow(ctx, `
        SELECT installation_code, device_code, coalesce(tailnet_identity, ''),
               coalesce(hostname, ''), status, created_at, updated_at, last_seen_at
        FROM installations WHERE installation_code = $1
    `, installationCode).Scan(
		&installation.InstallationCode,
		&installation.DeviceCode,
		&installation.TailnetIdentity,
		&installation.Hostname,
		&installation.Status,
		&installation.CreatedAt,
		&installation.UpdatedAt,
		&installation.LastSeenAt,
	)
	if errorsIsNoRows(err) {
		return domain.Installation{}, false, nil
	}
	if err != nil {
		return domain.Installation{}, false, WrapError(CodeInternal, "read installation", err)
	}

	return installation, true, nil
}

func lockInstallation(ctx context.Context, tx pgx.Tx, installationCode string) error {
	if _, err := tx.Exec(
		ctx,
		"SELECT pg_advisory_xact_lock(hashtext('memory-installation:' || $1))",
		installationCode,
	); err != nil {
		return WrapError(CodeInternal, "lock installation", err)
	}

	return nil
}

func lockTailnetIdentity(ctx context.Context, tx pgx.Tx, tailnetIdentity string) error {
	if _, err := tx.Exec(
		ctx,
		"SELECT pg_advisory_xact_lock(hashtext('memory-tailnet:' || $1))",
		tailnetIdentity,
	); err != nil {
		return WrapError(CodeInternal, "lock tailnet identity", err)
	}

	return nil
}

func lockDeviceCodes(ctx context.Context, tx pgx.Tx, deviceCodes ...string) error {
	deviceCodes = append([]string(nil), deviceCodes...)
	sort.Strings(deviceCodes)
	for _, deviceCode := range deviceCodes {
		if _, err := tx.Exec(
			ctx,
			"SELECT pg_advisory_xact_lock(hashtext('memory-device:' || $1))",
			deviceCode,
		); err != nil {
			return WrapError(CodeInternal, "lock device", err)
		}
	}

	return nil
}

func findDevice(ctx context.Context, tx pgx.Tx, deviceCode string) (domain.Device, error) {
	var device domain.Device
	var mergedInto string
	err := tx.QueryRow(ctx, `
        SELECT device_code, display_name, status, coalesce(merged_into_device_code, ''), created_at, updated_at
        FROM devices WHERE device_code = $1
    `, deviceCode).Scan(
		&device.DeviceCode,
		&device.DisplayName,
		&device.Status,
		&mergedInto,
		&device.CreatedAt,
		&device.UpdatedAt,
	)
	device.MergedIntoDeviceCode = mergedInto
	if errorsIsNoRows(err) {
		return domain.Device{}, NewError(CodeNotFound, "device not found")
	}
	if err != nil {
		return domain.Device{}, WrapError(CodeInternal, "read device", err)
	}

	return device, nil
}

func ensureDevice(ctx context.Context, tx pgx.Tx, deviceCode, displayName string) (domain.Device, error) {
	if _, err := tx.Exec(ctx, `
        INSERT INTO devices(device_code, display_name)
        VALUES ($1, $2)
        ON CONFLICT (device_code) DO UPDATE SET
            display_name = CASE WHEN devices.display_name = 'Unnamed device' THEN excluded.display_name ELSE devices.display_name END,
            updated_at = statement_timestamp()
    `, deviceCode, displayName); err != nil {
		return domain.Device{}, WrapError(CodeInternal, "ensure device", err)
	}

	return findDevice(ctx, tx, deviceCode)
}

func insertInstallation(ctx context.Context, tx pgx.Tx, input RegisterDeviceInput, deviceCode string) (domain.Installation, error) {
	var installation domain.Installation
	err := tx.QueryRow(ctx, `
        INSERT INTO installations(installation_code, device_code, tailnet_identity, hostname)
        VALUES ($1, $2, $3, $4)
        RETURNING installation_code, device_code, coalesce(tailnet_identity, ''),
                  coalesce(hostname, ''), status, created_at, updated_at, last_seen_at
    `,
		input.InstallationCode,
		deviceCode,
		nullableString(input.TailnetIdentity),
		nullableString(input.Hostname),
	).Scan(
		&installation.InstallationCode,
		&installation.DeviceCode,
		&installation.TailnetIdentity,
		&installation.Hostname,
		&installation.Status,
		&installation.CreatedAt,
		&installation.UpdatedAt,
		&installation.LastSeenAt,
	)
	if err != nil {
		return domain.Installation{}, WrapError(CodeInternal, "insert installation", err)
	}

	return installation, nil
}

func upsertSignals(ctx context.Context, tx pgx.Tx, installationCode, deviceCode string, signals []digestedSignal) error {
	for _, signal := range signals {
		if _, err := tx.Exec(ctx, `
            INSERT INTO device_signals(
                installation_code, device_code, signal_type, signal_digest, weight
            ) VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (installation_code, signal_type) DO UPDATE SET
                device_code = excluded.device_code,
                signal_digest = excluded.signal_digest,
                weight = excluded.weight,
                last_seen_at = statement_timestamp()
        `, installationCode, deviceCode, signal.Type, signal.Digest, signal.Weight); err != nil {
			return WrapError(CodeInternal, "store device signal", err)
		}
	}

	return nil
}

func (s *Store) refreshInstallation(
	ctx context.Context,
	tx pgx.Tx,
	installation domain.Installation,
	input RegisterDeviceInput,
	signals []digestedSignal,
) error {
	if _, err := tx.Exec(ctx, `
        UPDATE installations SET
            tailnet_identity = coalesce($2, tailnet_identity),
            hostname = coalesce($3, hostname),
            last_seen_at = statement_timestamp(), updated_at = statement_timestamp()
        WHERE installation_code = $1
    `, installation.InstallationCode, nullableString(input.TailnetIdentity), nullableString(input.Hostname)); err != nil {
		return WrapError(CodeInternal, "refresh installation", err)
	}

	return upsertSignals(ctx, tx, installation.InstallationCode, installation.DeviceCode, signals)
}

func findTailnetInstallation(ctx context.Context, tx pgx.Tx, identity string) (string, bool, error) {
	var installationCode string
	err := tx.QueryRow(ctx, `
		SELECT installation_code FROM installations
        WHERE tailnet_identity = $1 AND status = 'active'
	`, identity).Scan(&installationCode)
	if errorsIsNoRows(err) {
		return "", false, nil
	}
	if err != nil {
		return "", false, WrapError(CodeInternal, "resolve tailnet identity", err)
	}

	return installationCode, true, nil
}
