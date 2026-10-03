// Copyright (c) 2026 Sorbonne Université
// SPDX-License-Identifier: MIT

package retina

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/duckdb/duckdb-go/v2"
)

// CaptureFormat is written into every capture file as retina.fies.format. See
// FIES2.md for the format, its assumptions and its limits.
const CaptureFormat = "2a"

// CapturerConfig configures the capturer.
type CapturerConfig struct {
	// CaptureDir is where the staging and Parquet files are written.
	CaptureDir string `json:"capture_dir"`
	// AllowNonEmptyCaptureDir allows CaptureDir to contain files already.
	// Staging files found there are finalized at startup.
	AllowNonEmptyCaptureDir bool `json:"allow_non_empty_capture_dir"`
	// BatchSize is how many FIEs are appended before the appender is flushed.
	BatchSize int `json:"batch_size"`
	// RotationInterval is the time span one file covers. At most 18 hours, so
	// that capture_second fits in 16 bits.
	RotationInterval time.Duration `json:"rotation_interval"`
	// RowGroupSize is the number of rows per Parquet row group. Smaller row
	// groups let a PD ID filter skip more precisely; larger ones compress a
	// little better.
	RowGroupSize int `json:"row_group_size"`
	// StagingMemoryLimit is DuckDB's memory limit for the staging file, for
	// example "1GB". Without it DuckDB would take up to 80% of the machine's
	// memory for its buffers.
	StagingMemoryLimit string `json:"staging_memory_limit"`
	// FinalizeMemoryLimit is DuckDB's memory limit while sorting a staging
	// file, for example "2GB". Beyond it the sort spills to disk.
	FinalizeMemoryLimit string `json:"finalize_memory_limit"`
	// FinalizeThreads is the number of DuckDB threads used while sorting.
	FinalizeThreads int `json:"finalize_threads"`
}

func (c *CapturerConfig) validate() error {
	if c.CaptureDir == "" {
		return errors.New("capture directory cannot be empty")
	}
	if c.BatchSize < 1 {
		return fmt.Errorf("batch size must be at least 1: got %d", c.BatchSize)
	}
	if c.RotationInterval < time.Second || c.RotationInterval > 18*time.Hour {
		return fmt.Errorf("rotation interval must be between 1s and 18h: got %v", c.RotationInterval)
	}
	if c.RowGroupSize < 1 {
		return fmt.Errorf("row group size must be at least 1: got %d", c.RowGroupSize)
	}
	if c.StagingMemoryLimit == "" {
		return errors.New("staging memory limit cannot be empty")
	}
	if c.FinalizeMemoryLimit == "" {
		return errors.New("finalize memory limit cannot be empty")
	}
	if c.FinalizeThreads < 1 {
		return fmt.Errorf("finalize threads must be at least 1: got %d", c.FinalizeThreads)
	}
	return nil
}

// Capturer writes FIEs in the fies2a format: one Parquet file per rotation
// interval, sorted by (pd_id, capture_second), compressed with zstd.
//
// A file can only be sorted once its interval is over, so capturing has two
// stages:
//
//  1. Staging. FIEs are appended in arrival order to a DuckDB file,
//     fies2a-<interval start>.staging.duckdb.
//  2. Finalizing. When the interval rotates, the staging file is closed and a
//     background goroutine sorts it into fies2a-<interval start>.parquet,
//     then deletes it. Capturing continues into the next staging file. One
//     file is finalized at a time.
//
// Capture, Flush and Close may be called from several goroutines.
type Capturer struct {
	config CapturerConfig
	logger *slog.Logger
	// now returns the capture time; tests replace it.
	now func() time.Time

	mu      sync.Mutex
	closed  bool
	staging *stagingFile

	// finalizeMu lets one finalization run at a time.
	finalizeMu sync.Mutex
	finalizing sync.WaitGroup
	errMu      sync.Mutex
	errs       []error
}

// NewCapturer creates the capture directory if needed and finalizes the
// staging files left in it by an earlier run.
func NewCapturer(config *CapturerConfig, logger *slog.Logger) (*Capturer, error) {
	if err := config.validate(); err != nil {
		return nil, fmt.Errorf("invalid capturer config: %w", err)
	}
	empty, err := ensureDir(config.CaptureDir)
	if err != nil {
		return nil, fmt.Errorf("cannot create capture directory: %w", err)
	}
	if !empty && !config.AllowNonEmptyCaptureDir {
		return nil, fmt.Errorf("capture directory %s is not empty", config.CaptureDir)
	}
	if logger == nil {
		logger = slog.Default()
	}
	c := &Capturer{config: *config, logger: logger, now: time.Now}

	leftovers, err := filepath.Glob(filepath.Join(config.CaptureDir, "fies2a-*.staging.duckdb"))
	if err != nil {
		return nil, err
	}
	for _, path := range leftovers {
		logger.Info("Finalizing staging file left by an earlier run", slog.String("path", path))
		if err := c.finalize(path); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// Capture appends one FIE to the current interval's staging file.
func (c *Capturer) Capture(fie *FIE) error {
	if fie == nil {
		return errors.New("cannot capture nil FIE")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errors.New("capturer is closed")
	}
	now := c.now().UTC()
	interval := now.Truncate(c.config.RotationInterval)
	if c.staging == nil || !c.staging.interval.Equal(interval) {
		if err := c.rotate(interval); err != nil {
			return err
		}
	}
	return c.staging.append(fie, now, c.config.BatchSize)
}

// Flush writes the appended FIEs to the staging file. A failed finalization
// is logged, not returned: its staging file is kept and capturing goes on.
func (c *Capturer) Flush() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.staging == nil {
		return nil
	}
	return c.staging.flush()
}

// Close closes the current staging file, finalizes it and waits for every
// finalization to end.
func (c *Capturer) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	err := c.closeStaging()
	c.mu.Unlock()

	c.finalizing.Wait()
	return errors.Join(err, c.takeErrors())
}

func (c *Capturer) takeErrors() error {
	c.errMu.Lock()
	defer c.errMu.Unlock()
	err := errors.Join(c.errs...)
	c.errs = nil
	return err
}

// rotate closes the current staging file, starts its finalization and opens
// the staging file of the new interval.
func (c *Capturer) rotate(interval time.Time) error {
	if err := c.closeStaging(); err != nil {
		return err
	}
	staging, err := openStaging(c.config.CaptureDir, interval, c.config.StagingMemoryLimit)
	if err != nil {
		return err
	}
	c.staging = staging
	return nil
}

func (c *Capturer) closeStaging() error {
	staging := c.staging
	if staging == nil {
		return nil
	}
	c.staging = nil
	if err := staging.close(); err != nil {
		return err
	}
	c.finalizing.Add(1)
	go func() {
		defer c.finalizing.Done()
		if err := c.finalize(staging.path); err != nil {
			c.logger.Error("Cannot finalize capture file", slog.String("path", staging.path), slog.Any("err", err))
			c.errMu.Lock()
			c.errs = append(c.errs, err)
			c.errMu.Unlock()
		}
	}()
	return nil
}

func (c *Capturer) finalize(path string) error {
	c.finalizeMu.Lock()
	defer c.finalizeMu.Unlock()
	start := time.Now()
	final, err := finalizeStaging(path, &c.config)
	if err != nil {
		return err
	}
	var size int64
	if info, err := os.Stat(final); err == nil {
		size = info.Size()
	}
	c.logger.Info("Capture file finalized", slog.String("path", final), slog.Int64("bytes", size),
		slog.Float64("took_seconds", time.Since(start).Seconds()))
	return nil
}

// stagingFile is the DuckDB file one interval is appended to.
type stagingFile struct {
	interval  time.Time
	path      string
	connector *duckdb.Connector
	conn      driver.Conn
	appender  *duckdb.Appender
	pending   int
}

const stagingDDL = `
CREATE TABLE IF NOT EXISTS fies2a (
    pd_id            UINTEGER  NOT NULL,
    capture_second   USMALLINT NOT NULL,
    near_reply_addr  BLOB,
    far_reply_addr   BLOB,
    near_reply_age_s UTINYINT,
    far_reply_age_s  UTINYINT,
    fie_transit_s    UTINYINT
);`

func openStaging(dir string, interval time.Time, memoryLimit string) (*stagingFile, error) {
	path := filepath.Join(dir, captureBaseName(interval)+".staging.duckdb")
	connector, err := duckdb.NewConnector(path, nil)
	if err != nil {
		return nil, fmt.Errorf("cannot open staging file %s: %w", path, err)
	}
	conn, err := connector.Connect(context.Background())
	if err != nil {
		_ = connector.Close()
		return nil, fmt.Errorf("cannot connect to staging file: %w", err)
	}
	fail := func(err error) (*stagingFile, error) {
		_ = conn.Close()
		_ = connector.Close()
		return nil, err
	}
	if err := execConn(conn, fmt.Sprintf("SET memory_limit = '%s'", memoryLimit)); err != nil {
		return fail(fmt.Errorf("cannot set staging memory limit: %w", err))
	}
	if err := execConn(conn, stagingDDL); err != nil {
		return fail(fmt.Errorf("cannot create staging table: %w", err))
	}
	appender, err := duckdb.NewAppenderFromConn(conn, "", "fies2a")
	if err != nil {
		return fail(fmt.Errorf("cannot create staging appender: %w", err))
	}
	return &stagingFile{interval: interval, path: path, connector: connector, conn: conn, appender: appender}, nil
}

func (s *stagingFile) append(fie *FIE, now time.Time, batchSize int) error {
	captureSecond := now.Unix() - s.interval.Unix()
	nearAddr, nearAge := encodeReply(fie.Near, fie.NearDelta)
	farAddr, farAge := encodeReply(fie.Far, fie.FarDelta)
	var transit driver.Value // NULL when the agent's clock is ahead
	if d := now.Unix() - fie.CaptureUnix; d >= 0 {
		transit = saturate(uint64(d))
	}
	//nolint:gosec // below 18 hours (64,800 s): the rotation interval is validated
	if err := s.appender.AppendRow(fie.PDID, uint16(captureSecond), nearAddr, farAddr, nearAge, farAge, transit); err != nil {
		return fmt.Errorf("cannot append FIE: %w", err)
	}
	s.pending++
	if s.pending >= batchSize {
		return s.flush()
	}
	return nil
}

func (s *stagingFile) flush() error {
	if s.pending == 0 {
		return nil
	}
	if err := s.appender.Flush(); err != nil {
		return fmt.Errorf("cannot flush staging appender: %w", err)
	}
	s.pending = 0
	return nil
}

func (s *stagingFile) close() error {
	err := errors.Join(s.flush(), s.appender.Close())
	if cerr := execConn(s.conn, "CHECKPOINT"); cerr != nil {
		err = errors.Join(err, fmt.Errorf("cannot checkpoint staging file: %w", cerr))
	}
	return errors.Join(err, s.conn.Close(), s.connector.Close())
}

func execConn(conn driver.Conn, query string) error {
	_, err := conn.(driver.ExecerContext).ExecContext(context.Background(), query, nil)
	return err
}

// encodeReply returns the address as 4 or 16 bytes and its age in seconds, or
// two NULLs when there was no reply.
func encodeReply(addr netip.Addr, delta uint32) (driver.Value, driver.Value) {
	if !addr.IsValid() || addr.IsUnspecified() {
		return nil, nil
	}
	addr = addr.Unmap()
	if addr.Is4() {
		b := addr.As4()
		return b[:], saturate(uint64(delta))
	}
	b := addr.As16()
	return b[:], saturate(uint64(delta))
}

// saturate caps a number of seconds at 255, which then means 255 or more.
func saturate(seconds uint64) uint8 {
	if seconds > 255 {
		return 255
	}
	return uint8(seconds)
}

// finalizeStaging sorts a staging file into its Parquet file, deletes the
// staging file and returns the Parquet file's path. The Parquet file is
// written under a temporary name and renamed once complete, so a reader never
// sees a partial file.
func finalizeStaging(stagingPath string, config *CapturerConfig) (string, error) {
	base := strings.TrimSuffix(stagingPath, ".staging.duckdb")
	if base == stagingPath {
		return "", fmt.Errorf("not a staging file: %s", stagingPath)
	}
	interval, err := parseCaptureBaseName(filepath.Base(base))
	if err != nil {
		return "", err
	}
	final := base + ".parquet"
	tmp := final + ".tmp"
	spill := base + ".spill"

	connector, err := duckdb.NewConnector(stagingPath, nil)
	if err != nil {
		return "", fmt.Errorf("cannot open staging file %s: %w", stagingPath, err)
	}
	db := sql.OpenDB(connector) // closing db also closes the connector
	defer func() { _ = db.Close() }()

	settings := []string{
		fmt.Sprintf("SET memory_limit = '%s'", config.FinalizeMemoryLimit),
		fmt.Sprintf("SET threads = %d", config.FinalizeThreads),
		fmt.Sprintf("SET temp_directory = '%s'", spill),
		"SET preserve_insertion_order = false",
	}
	for _, s := range settings {
		if _, err := db.Exec(s); err != nil {
			return "", fmt.Errorf("%s: %w", s, err)
		}
	}
	//nolint:gosec // every value comes from the operator's config or the file name
	copySQL := fmt.Sprintf(`COPY (SELECT * FROM fies2a ORDER BY pd_id, capture_second)
TO '%s' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE %d,
KV_METADATA {'retina.fies.format': '%s', 'retina.fies.interval_start': '%s', 'retina.fies.interval_seconds': '%d'})`,
		tmp, config.RowGroupSize, CaptureFormat, interval.Format(time.RFC3339), int64(config.RotationInterval/time.Second))
	if _, err := db.Exec(copySQL); err != nil {
		return "", fmt.Errorf("cannot sort %s into parquet: %w", stagingPath, err)
	}
	if err := db.Close(); err != nil {
		return "", fmt.Errorf("cannot close staging file: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		return "", fmt.Errorf("cannot rename parquet file: %w", err)
	}
	return final, errors.Join(os.Remove(stagingPath), removeIfExists(stagingPath+".wal"), os.RemoveAll(spill))
}

const captureTimeLayout = "20060102T150405Z"

func captureBaseName(interval time.Time) string {
	return "fies2a-" + interval.UTC().Format(captureTimeLayout)
}

func parseCaptureBaseName(name string) (time.Time, error) {
	t, err := time.Parse(captureTimeLayout, strings.TrimPrefix(name, "fies2a-"))
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid capture file name %q: %w", name, err)
	}
	return t, nil
}

func removeIfExists(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// ensureDir creates the directory if needed and reports whether it is empty.
func ensureDir(path string) (bool, error) {
	if err := os.MkdirAll(path, 0o750); err != nil {
		return false, err
	}
	f, err := os.Open(path) //nolint:gosec // the capture directory comes from the operator
	if err != nil {
		return false, err
	}
	defer func() { _ = f.Close() }()
	if _, err := f.Readdirnames(1); errors.Is(err, io.EOF) {
		return true, nil
	} else if err != nil {
		return false, err
	}
	return false, nil
}
