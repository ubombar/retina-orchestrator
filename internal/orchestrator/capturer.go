// Package orchestrator implements a compact, rotating DuckDB capturer for
// high-frequency ForwardingInfoElement streams.
//
// Dependency (go.mod):
//
//	github.com/marcboeker/go-duckdb/v2
package orchestrator

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/dioptra-io/retina-commons/api/v1"
	"github.com/marcboeker/go-duckdb/v2"
)

// FIECapturer is the interface, the Flush() and Close() methods needs to be
// idempotent and should not return error when called more than once.
type FIECapturer interface {
	Capture(ctx context.Context, seq *SequencedFIE) error
	Flush() error
	Close() error
}

// DDBFIECapturer implements Capturer using DuckDB with a compact representation
// optimized for storage.
//
// The full FIE is not stored. Fields that can be reconstructed from the
// corresponding ProbingDirective are omitted. Each stored FIE contains only:
//
//   - probing_directive_id:
//     Stored as UINTEGER/uint32. In the expected dataset PD IDs are bounded
//     by max(uint32) = 4294967295.
//
//   - capture_second:
//     Stored as USMALLINT/uint16. Each DuckDB file represents one configured
//     UTC rotation interval. The value is the number of whole seconds between
//     the beginning of the file's interval and the time at which the FIE was
//     captured by the capturer.
//
//     For a 6-hour interval the range is 0..21599. A uint16 allows intervals
//     up to approximately 18 hours.
//
//     The absolute capture timestamp is reconstructed from the interval start
//     encoded in the database filename plus capture_second.
//
//   - time_deltas:
//     Five timestamps are represented as 6-bit second-resolution deltas and
//     packed into a UINTEGER/uint32. This requires 30 bits, leaving bits 30..31
//     reserved.
//
//     All timestamps are encoded relative to the capturer's local capture time:
//
//     bits  0..5   production -> capture
//     bits  6..11  near sent -> capture
//     bits 12..17  near received -> capture
//     bits 18..23  far sent -> capture
//     bits 24..29  far received -> capture
//     bits 30..31  reserved
//
//     Each 6-bit delta uses the following encoding:
//
//     0..61 = that many whole seconds before the capture timestamp
//     62    = 62 or more seconds before the capture timestamp
//     63    = nil / unknown / timestamp after the capture timestamp
//
//     For any timestamp T:
//
//     delta(T) = capture_time - T
//
//     The capture timestamp itself is reconstructed from the rotation interval
//     start encoded in the database filename plus capture_second.
//
//     Encoding every timestamp relative to capture_time makes each timestamp
//     independent. In particular, probe timestamps can still be represented
//     even when the production timestamp is missing.
//
//     When both the production timestamp and a probe timestamp are available
//     and neither delta is saturated, their relative timing can be reconstructed:
//
//     production_time - probe_time
//     = probe_delta - production_delta
//
//     The capturer's clock is therefore the canonical timeline for file
//     rotation and timestamp reconstruction.
//
//   - reply addresses:
//     Near and far reply addresses are nullable BLOBs. Records are separated
//     by protocol and IP version so IPv4 addresses use exactly 4 bytes and IPv6
//     addresses use exactly 16 bytes.
//
// Table layout:
//
//   - fies_icmpv4:
//     ICMP over IPv4.
//
//   - fies_icmpv6:
//     ICMPv6 over IPv6.
//
//   - fies_udpv4:
//     UDP over IPv4.
//
//   - fies_udpv6:
//     UDP over IPv6.
//
//     The table name therefore encodes both the protocol and IP version, so
//     neither field needs to be stored in every FIE row.
//
//     ICMP over IPv6 and ICMPv6 over IPv4 are considered invalid combinations
//     and are rejected.
//
// The following fields are intentionally not stored because they can be
// reconstructed from the associated ProbingDirective, table name, or other
// metadata: agent ID, source address, destination address, IP version, protocol,
// near TTL, far TTL, and sequence number.
//
// Clock assumptions:
//
//   - Agent and capturer clocks are assumed to be synchronized closely enough
//     that their offset and drift are small relative to the one-second
//     timestamp resolution used by this storage format.
//   - The capture timestamp is generated locally by the capturer and is used
//     exclusively to determine the configured UTC rotation interval.
//   - Agent production timestamps are never used for file rotation.
//   - The production-to-capture delta retains information about transport,
//     processing, and residual clock-offset differences between the agent and
//     capturer.
//   - Probe timestamps and the production timestamp originate from the same
//     agent clock, so their relative deltas are not affected by a constant
//     clock offset between the agent and capturer.
//   - Timestamp deltas are represented with 6 bits. Exact values are retained
//     for deltas from 0 to 61 seconds before the capture timestamp.
//   - A delta of 62 represents any timestamp that occurred 62 seconds or more
//     before the capture timestamp.
//   - A delta of 63 represents an unknown/missing timestamp or a timestamp that
//     is after the capture timestamp. The latter is treated as invalid timing
//     data, typically caused by clock skew or unexpected timestamp ordering.
//
// Storage assumptions / operational design:
//
//   - Some fraction of reply addresses can be NULL.
//   - Expected production load is approximately 17k FIE/s; ingestion has been
//     stress-tested at approximately 180k FIE/s.
//   - At the measured compression ratio, storage is approximately 1.1 GB/h at
//     the expected production rate of 17k FIE/s.
//   - Files are aligned to UTC boundaries according to RotationInterval.
//     The expected production configuration uses 6-hour intervals, resulting
//     in four files per day.
//   - The interval start is encoded in the filename, for example:
//     fies-20260824T120000Z.duckdb
//   - Rotation keeps files small enough to copy elsewhere and reclaim local
//     disk space during long experiments.
type DDBFIECapturer struct {
	currentIntervalHandle *captureHandle
	closed                bool
	mu                    sync.Mutex
	cfg                   DDBFIECapturerConfig
}

type DDBFIECapturerConfig struct {
	// AllowNonEmptyCaptureDir if set to false would give an error in case there
	// are existing files in the capture directory.
	AllowNonEmptyCaptureDir bool `json:"allow_non_empty_capture_dir"`
	// BatchSize is the number of FIEs to batch.
	BatchSize int `json:"batch_size"`
	// CaptureDir is the directory where all the captures are stored.
	CaptureDir string `json:"capture_dir"`
	// RotationInterval is the duration of the database files created. Maximum
	// possible interval is 18 hours.
	RotationInterval time.Duration `json:"rotation_interval"`
}

func validateConfig(cfg *DDBFIECapturerConfig) error {
	if cfg == nil {
		return fmt.Errorf("given config is nil")
	}
	if cfg.RotationInterval < time.Second {
		return fmt.Errorf("rotation interval cannot be less than a second: got %v", cfg.RotationInterval)
	}
	if cfg.RotationInterval > 18*time.Hour {
		return fmt.Errorf("rotation interval cannot be more than 18 hours: got %v", cfg.RotationInterval)
	}
	if cfg.BatchSize <= 0 {
		return fmt.Errorf("batch size cannot be less than 1: %v", cfg.BatchSize)
	}
	if cfg.CaptureDir == "" {
		return fmt.Errorf("capture directory cannot be empty")
	}
	dirEmpty, err := createAndCheckCaptureDir(cfg.CaptureDir)
	if err != nil {
		return err
	}
	if !cfg.AllowNonEmptyCaptureDir && !dirEmpty {
		return fmt.Errorf("given directory is not empty and non-empty capture directories are disabled")
	}
	return nil
}

func NewDDBFIECapturer(cfg *DDBFIECapturerConfig) (*DDBFIECapturer, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, fmt.Errorf("cannot create DDBCapturer: %w", err)
	}
	ddbCap := &DDBFIECapturer{
		cfg: *cfg,
	}
	return ddbCap, nil
}

func (dc *DDBFIECapturer) Capture(ctx context.Context, seq *SequencedFIE) error {
	if seq == nil {
		return fmt.Errorf("cannot capture nil FIE")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	dc.mu.Lock()
	defer dc.mu.Unlock()

	if dc.closed {
		return fmt.Errorf("capturer is closed")
	}

	currentCaptureTime := time.Now().UTC()
	currentCaptureRotation := currentCaptureTime.UTC().Truncate(dc.cfg.RotationInterval)

	// Rotate to the next interval
	if dc.currentIntervalHandle == nil || dc.currentIntervalHandle.interval != currentCaptureRotation {
		if err := dc.rotate(currentCaptureRotation); err != nil {
			return err
		}
	}

	// Write to batch and flush if necessary.
	if err := dc.capture(seq, currentCaptureTime); err != nil {
		return err
	}

	return nil
}

func (dc *DDBFIECapturer) Flush() error {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	return dc.flush()
}

func (dc *DDBFIECapturer) Close() error {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	return dc.close()
}

func (dc *DDBFIECapturer) flush() error {
	if dc.closed {
		return nil
	}
	if dc.currentIntervalHandle == nil {
		return nil
	}
	return dc.currentIntervalHandle.flush()
}

func (dc *DDBFIECapturer) close() error {
	if dc.closed {
		return nil
	}
	dc.closed = true

	handle := dc.currentIntervalHandle
	dc.currentIntervalHandle = nil

	if handle == nil {
		return nil
	}

	return handle.close()
}

func (dc *DDBFIECapturer) rotate(t time.Time) error {
	if dc.currentIntervalHandle != nil {
		old := dc.currentIntervalHandle
		if err := old.close(); err != nil {
			return err
		}
		dc.currentIntervalHandle = nil
	}
	newIntervalHandle, err := openCaptureHandle(t, dc.cfg.CaptureDir)
	if err != nil {
		return err
	}
	dc.currentIntervalHandle = newIntervalHandle
	return nil
}

func (dc *DDBFIECapturer) capture(seq *SequencedFIE, captureTime time.Time) error {
	compFIE, err := compactFIE(seq, captureTime, dc.cfg.RotationInterval)
	if err != nil {
		return err
	}
	if err := dc.currentIntervalHandle.append(compFIE); err != nil {
		return err
	}
	if dc.currentIntervalHandle.pending >= dc.cfg.BatchSize {
		if err := dc.currentIntervalHandle.flush(); err != nil {
			return err
		}
	}
	return nil
}

type captureHandle struct {
	interval  time.Time
	db        *sql.DB
	connector *duckdb.Connector
	conn      driver.Conn
	pending   int

	icmpv4 *duckdb.Appender
	icmpv6 *duckdb.Appender
	udpv4  *duckdb.Appender
	udpv6  *duckdb.Appender
}

// openCaptureHandle creates a new handler from the given time. Given time t is
// not sanitized, this is for allowing different configurations and testing.
//
//nolint:funlen
func openCaptureHandle(t time.Time, captureDir string) (*captureHandle, error) {
	filename := timeToFilename(t)
	path := filepath.Join(captureDir, filename)
	connector, err := duckdb.NewConnector(path, nil)
	if err != nil {
		return nil, fmt.Errorf("create connector: %w", err)
	}

	db := sql.OpenDB(connector)

	const ddl = `
CREATE TABLE IF NOT EXISTS fies_icmpv4 (
    probing_directive_id UINTEGER NOT NULL,
    near_reply_address   BLOB,
    far_reply_address    BLOB,
    capture_second       USMALLINT NOT NULL,
    time_deltas          UINTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS fies_icmpv6 (
    probing_directive_id UINTEGER NOT NULL,
    near_reply_address   BLOB,
    far_reply_address    BLOB,
    capture_second       USMALLINT NOT NULL,
    time_deltas          UINTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS fies_udpv4 (
    probing_directive_id UINTEGER NOT NULL,
    near_reply_address   BLOB,
    far_reply_address    BLOB,
    capture_second       USMALLINT NOT NULL,
    time_deltas          UINTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS fies_udpv6 (
    probing_directive_id UINTEGER NOT NULL,
    near_reply_address   BLOB,
    far_reply_address    BLOB,
    capture_second       USMALLINT NOT NULL,
    time_deltas          UINTEGER NOT NULL
);
`

	if _, err := db.Exec(ddl); err != nil {
		_ = db.Close()
		_ = connector.Close()
		return nil, fmt.Errorf("create schema: %w", err)
	}

	conn, err := connector.Connect(context.Background())
	if err != nil {
		_ = db.Close()
		_ = connector.Close()
		return nil, fmt.Errorf("connect: %w", err)
	}

	icmpv4, err := duckdb.NewAppenderFromConn(conn, "", "fies_icmpv4")
	if err != nil {
		_ = conn.Close()
		_ = db.Close()
		_ = connector.Close()
		return nil, fmt.Errorf("create ICMPv4 appender: %w", err)
	}

	icmpv6, err := duckdb.NewAppenderFromConn(conn, "", "fies_icmpv6")
	if err != nil {
		_ = icmpv4.Close()
		_ = conn.Close()
		_ = db.Close()
		_ = connector.Close()
		return nil, fmt.Errorf("create ICMPv6 appender: %w", err)
	}

	udpv4, err := duckdb.NewAppenderFromConn(conn, "", "fies_udpv4")
	if err != nil {
		_ = icmpv6.Close()
		_ = icmpv4.Close()
		_ = conn.Close()
		_ = db.Close()
		_ = connector.Close()
		return nil, fmt.Errorf("create UDPv4 appender: %w", err)
	}

	udpv6, err := duckdb.NewAppenderFromConn(conn, "", "fies_udpv6")
	if err != nil {
		_ = udpv4.Close()
		_ = icmpv6.Close()
		_ = icmpv4.Close()
		_ = conn.Close()
		_ = db.Close()
		_ = connector.Close()
		return nil, fmt.Errorf("create UDPv6 appender: %w", err)
	}

	return &captureHandle{
		interval:  t,
		db:        db,
		connector: connector,
		conn:      conn,
		icmpv4:    icmpv4,
		icmpv6:    icmpv6,
		udpv4:     udpv4,
		udpv6:     udpv6,
	}, nil
}

func (f *captureHandle) append(row compactedFIE) error {
	var appender *duckdb.Appender

	switch {
	case row.protocol == api.ICMP && row.isIPv4:
		appender = f.icmpv4

	case row.protocol == api.ICMPv6 && !row.isIPv4:
		appender = f.icmpv6

	case row.protocol == api.UDP && row.isIPv4:
		appender = f.udpv4

	case row.protocol == api.UDP && !row.isIPv4:
		appender = f.udpv6

	default:
		return fmt.Errorf("unsupported protocol/IP combination: protocol=%d is_ipv4=%t", row.protocol, row.isIPv4)
	}

	if err := appender.AppendRow(
		row.pdID,
		row.nearReply,
		row.farReply,
		row.captureSecond,
		row.timeDeltas,
	); err != nil {
		return fmt.Errorf("append row: %w", err)
	}

	f.pending++
	return nil
}

func (f *captureHandle) flush() error {
	if f.pending == 0 {
		return nil
	}

	if err := f.icmpv4.Flush(); err != nil {
		return fmt.Errorf("flush ICMPv4 appender: %w", err)
	}

	if err := f.icmpv6.Flush(); err != nil {
		return fmt.Errorf("flush ICMPv6 appender: %w", err)
	}

	if err := f.udpv4.Flush(); err != nil {
		return fmt.Errorf("flush UDPv4 appender: %w", err)
	}

	if err := f.udpv6.Flush(); err != nil {
		return fmt.Errorf("flush UDPv6 appender: %w", err)
	}

	f.pending = 0
	return nil
}

func (f *captureHandle) close() error {
	var firstErr error

	record := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	record(f.flush())

	if f.icmpv4 != nil {
		record(f.icmpv4.Close())
		f.icmpv4 = nil
	}

	if f.icmpv6 != nil {
		record(f.icmpv6.Close())
		f.icmpv6 = nil
	}

	if f.udpv4 != nil {
		record(f.udpv4.Close())
		f.udpv4 = nil
	}

	if f.udpv6 != nil {
		record(f.udpv6.Close())
		f.udpv6 = nil
	}

	if f.conn != nil {
		record(f.conn.Close())
		f.conn = nil
	}

	if f.db != nil {
		if _, err := f.db.Exec("CHECKPOINT"); err != nil {
			record(fmt.Errorf("checkpoint: %w", err))
		}

		record(f.db.Close())
		f.db = nil
	}

	if f.connector != nil {
		record(f.connector.Close())
		f.connector = nil
	}

	return firstErr
}

type compactedFIE struct {
	pdID          uint32
	nearReply     []byte
	farReply      []byte
	captureSecond uint16
	timeDeltas    uint32
	isIPv4        bool
	protocol      api.Protocol
}

//nolint:funlen,gocyclo
func compactFIE(seq *SequencedFIE, captureTime time.Time, rotationInterval time.Duration) (compactedFIE, error) {
	fie := &seq.ForwardingInfoElement

	switch {
	case fie.Protocol == api.ICMP && fie.IPVersion == api.IPv4:
	case fie.Protocol == api.ICMPv6 && fie.IPVersion == api.IPv6:
	case fie.Protocol == api.UDP && fie.IPVersion == api.IPv4:
	case fie.Protocol == api.UDP && fie.IPVersion == api.IPv6:
	default:
		return compactedFIE{}, fmt.Errorf("unsupported protocol/IP combination: protocol=%d ip_version=%d", fie.Protocol, fie.IPVersion)
	}

	if fie.ProbingDirectiveID > math.MaxUint32 {
		return compactedFIE{}, fmt.Errorf("probing directive ID %d exceeds uint32", fie.ProbingDirectiveID)
	}

	var isIPv4 bool
	switch fie.IPVersion {
	case api.IPv4:
		isIPv4 = true
	case api.IPv6:
		isIPv4 = false
	default:
		return compactedFIE{}, fmt.Errorf("unsupported IP version %d", fie.IPVersion)
	}

	captureTime = captureTime.UTC()
	intervalBegin := captureTime.Truncate(rotationInterval)

	captureSeconds := captureTime.Unix() - intervalBegin.Unix()
	if captureSeconds < 0 || captureSeconds > math.MaxUint16 {
		return compactedFIE{}, fmt.Errorf("capture second %d exceeds uint16", captureSeconds)
	}

	var (
		nearReply []byte
		farReply  []byte

		nearSentDelta uint8 = 63
		nearRecvDelta uint8 = 63
		farSentDelta  uint8 = 63
		farRecvDelta  uint8 = 63
	)

	if fie.NearInfo != nil {
		var err error
		nearReply, err = compactIP(fie.NearInfo.ReplyAddress, fie.IPVersion)
		if err != nil {
			return compactedFIE{}, fmt.Errorf("near reply: %w", err)
		}
		nearSentDelta = timestampDelta(captureTime, fie.NearInfo.SentTimestamp)
		nearRecvDelta = timestampDelta(captureTime, fie.NearInfo.ReceivedTimestamp)
	}

	if fie.FarInfo != nil {
		var err error
		farReply, err = compactIP(fie.FarInfo.ReplyAddress, fie.IPVersion)
		if err != nil {
			return compactedFIE{}, fmt.Errorf("far reply: %w", err)
		}
		farSentDelta = timestampDelta(captureTime, fie.FarInfo.SentTimestamp)
		farRecvDelta = timestampDelta(captureTime, fie.FarInfo.ReceivedTimestamp)
	}

	productionDelta := timestampDelta(captureTime, fie.ProductionTimestamp)

	timeDeltas := uint32(productionDelta) |
		uint32(nearSentDelta)<<6 |
		uint32(nearRecvDelta)<<12 |
		uint32(farSentDelta)<<18 |
		uint32(farRecvDelta)<<24

	return compactedFIE{
		pdID:          uint32(fie.ProbingDirectiveID),
		nearReply:     nearReply,
		farReply:      farReply,
		captureSecond: uint16(captureSeconds),
		timeDeltas:    timeDeltas,
		isIPv4:        isIPv4,
		protocol:      fie.Protocol,
	}, nil
}

func timestampDelta(reference, timestamp time.Time) uint8 {
	if timestamp.IsZero() {
		return 63 // nil / unknown
	}

	delta := reference.Unix() - timestamp.UTC().Unix()

	// Timestamp being after its reference indicates clock skew or
	// otherwise unexpected timing data.
	if delta < 0 {
		return 63
	}

	if delta >= 62 {
		return 62
	}

	return uint8(delta)
}

func compactIP(ip net.IP, version api.IPVersion) ([]byte, error) {
	if ip == nil {
		return nil, nil
	}

	switch version {
	case api.IPv4:
		v4 := ip.To4()
		if v4 == nil {
			return nil, fmt.Errorf("expected IPv4 address, got %v", ip)
		}

		return []byte{v4[0], v4[1], v4[2], v4[3]}, nil

	case api.IPv6:
		// If the type is IPv6 but the form is IPv4 (mapped into IPv6) then we
		// will still allow since this might happen. In that case we will encode
		// the address as IPv6 mapped IPv4.
		v6 := ip.To16()
		if v6 == nil {
			return nil, fmt.Errorf("invalid IPv6 address %v", ip)
		}

		out := make([]byte, net.IPv6len)
		copy(out, v6)
		return out, nil

	default:
		return nil, fmt.Errorf("unsupported IP version %d", version)
	}
}

func timeToFilename(t time.Time) string {
	return fmt.Sprintf("fies-%s.duckdb", t.UTC().Format("20060102T150405Z"))
}

func createAndCheckCaptureDir(path string) (bool, error) {
	// Create the directory and all missing parents.
	if err := os.MkdirAll(path, 0o750); err != nil {
		return false, err
	}

	f, err := os.Open(path) //nolint:gosec
	if err != nil {
		return false, err
	}
	defer func() { _ = f.Close() }()

	_, err = f.Readdirnames(1)
	if err == io.EOF {
		return true, nil
	}
	if err != nil {
		return false, err
	}

	return false, nil
}
