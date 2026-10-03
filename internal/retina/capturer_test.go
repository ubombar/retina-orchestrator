// Copyright (c) 2026 Sorbonne Université
// SPDX-License-Identifier: MIT

package retina

import (
	"database/sql"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func testCapturerConfig(dir string) *CapturerConfig {
	return &CapturerConfig{
		CaptureDir: dir, BatchSize: 2, RotationInterval: time.Hour, RowGroupSize: 1000,
		StagingMemoryLimit: "256MB", FinalizeMemoryLimit: "256MB", FinalizeThreads: 1,
	}
}

type capturedRow struct {
	pd                          uint32
	second                      uint16
	nearLen, farLen             sql.NullInt64
	nearAge, farAge, transitAge sql.NullInt64
}

func readCaptureFile(t *testing.T, path string) []capturedRow {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.Query(`SELECT pd_id, capture_second, octet_length(near_reply_addr), octet_length(far_reply_addr),
		near_reply_age_s, far_reply_age_s, fie_transit_s FROM read_parquet(?)`, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var got []capturedRow
	for rows.Next() {
		var r capturedRow
		if err := rows.Scan(&r.pd, &r.second, &r.nearLen, &r.farLen, &r.nearAge, &r.farAge, &r.transitAge); err != nil {
			t.Fatal(err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return got
}

func dirNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func TestCapturer_SortsRotatesAndEncodes(t *testing.T) {
	dir := t.TempDir()
	c, err := NewCapturer(testCapturerConfig(dir), nil)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 10, 3, 11, 0, 0, 0, time.UTC)
	now := start
	c.now = func() time.Time { return now }
	capture := func(at time.Duration, fie FIE) {
		t.Helper()
		now = start.Add(at)
		if err := c.Capture(&fie); err != nil {
			t.Fatal(err)
		}
	}
	v4 := netip.MustParseAddr("192.0.2.1")
	v6 := netip.MustParseAddr("2001:db8::1")
	mapped := netip.MustParseAddr("::ffff:192.0.2.9")

	// Arrival order is not PD order; the second hour starts a new file.
	capture(5*time.Second, FIE{PDID: 7, CaptureUnix: start.Unix() + 3, Near: v6, NearDelta: 1})
	capture(6*time.Second, FIE{PDID: 2, CaptureUnix: start.Unix() + 6, Near: v4, Far: mapped, FarDelta: 300})
	capture(7*time.Second, FIE{PDID: 2, CaptureUnix: start.Unix() + 9}) // agent clock ahead
	capture(time.Hour+time.Second, FIE{PDID: 1, CaptureUnix: start.Unix() + 3600})
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Capture(&FIE{}); err == nil {
		t.Fatal("Capture after Close: got nil error")
	}

	want := []string{"fies2a-20261003T110000Z.parquet", "fies2a-20261003T120000Z.parquet"}
	if got := dirNames(t, dir); !slices.Equal(got, want) {
		t.Fatalf("files = %v, want %v", got, want)
	}
	null := sql.NullInt64{}
	n := func(v int64) sql.NullInt64 { return sql.NullInt64{Int64: v, Valid: true} }
	wantRows := []capturedRow{
		// IPv4 near, IPv4-mapped far stored as 4 bytes, far age saturated.
		{pd: 2, second: 6, nearLen: n(4), farLen: n(4), nearAge: n(0), farAge: n(255), transitAge: n(0)},
		// No replies, agent clock ahead: all NULL.
		{pd: 2, second: 7, nearLen: null, farLen: null, nearAge: null, farAge: null, transitAge: null},
		// IPv6 near, no far.
		{pd: 7, second: 5, nearLen: n(16), farLen: null, nearAge: n(1), farAge: null, transitAge: n(2)},
	}
	if got := readCaptureFile(t, filepath.Join(dir, want[0])); !slices.Equal(got, wantRows) {
		t.Fatalf("rows:\n got %+v\nwant %+v", got, wantRows)
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var format, intervalStart string
	if err := db.QueryRow(`SELECT
		max(decode(value)) FILTER (WHERE decode(key) = 'retina.fies.format'),
		max(decode(value)) FILTER (WHERE decode(key) = 'retina.fies.interval_start')
		FROM parquet_kv_metadata(?)`, filepath.Join(dir, want[0])).Scan(&format, &intervalStart); err != nil {
		t.Fatal(err)
	}
	if format != CaptureFormat || intervalStart != "2026-10-03T11:00:00Z" {
		t.Fatalf("metadata: format %q, interval start %q", format, intervalStart)
	}
}

func TestCapturer_FinalizesLeftoverStagingFiles(t *testing.T) {
	dir := t.TempDir()
	config := testCapturerConfig(dir)

	// A staging file as a crash would leave it: written, never finalized.
	interval := time.Date(2026, 10, 3, 9, 0, 0, 0, time.UTC)
	staging, err := openStaging(dir, interval, config.StagingMemoryLimit)
	if err != nil {
		t.Fatal(err)
	}
	for _, pd := range []uint32{9, 3} {
		if err := staging.append(&FIE{PDID: pd, CaptureUnix: interval.Unix()}, interval, 100); err != nil {
			t.Fatal(err)
		}
	}
	if err := staging.close(); err != nil {
		t.Fatal(err)
	}

	if _, err := NewCapturer(config, nil); err == nil {
		t.Fatal("non-empty directory: got nil error")
	}
	config.AllowNonEmptyCaptureDir = true
	c, err := NewCapturer(config, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if got, want := dirNames(t, dir), []string{"fies2a-20261003T090000Z.parquet"}; !slices.Equal(got, want) {
		t.Fatalf("files = %v, want %v", got, want)
	}
	if got := readCaptureFile(t, filepath.Join(dir, "fies2a-20261003T090000Z.parquet")); len(got) != 2 || got[0].pd != 3 || got[1].pd != 9 {
		t.Fatalf("rows = %+v, want PDs 3 then 9", got)
	}
}

func TestCapturerConfig_Validate(t *testing.T) {
	for name, mutate := range map[string]func(*CapturerConfig){
		"no directory":          func(c *CapturerConfig) { c.CaptureDir = "" },
		"zero batch":            func(c *CapturerConfig) { c.BatchSize = 0 },
		"short rotation":        func(c *CapturerConfig) { c.RotationInterval = time.Millisecond },
		"long rotation":         func(c *CapturerConfig) { c.RotationInterval = 19 * time.Hour },
		"zero row group":        func(c *CapturerConfig) { c.RowGroupSize = 0 },
		"no staging memory":     func(c *CapturerConfig) { c.StagingMemoryLimit = "" },
		"no finalize memory":    func(c *CapturerConfig) { c.FinalizeMemoryLimit = "" },
		"zero finalize threads": func(c *CapturerConfig) { c.FinalizeThreads = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			config := testCapturerConfig(t.TempDir())
			mutate(config)
			if _, err := NewCapturer(config, nil); err == nil {
				t.Fatal("got nil error")
			}
		})
	}
}
