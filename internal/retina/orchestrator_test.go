// Copyright (c) 2026 Sorbonne Université
// SPDX-License-Identifier: MIT

package retina

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func TestOrchestrator_AcceptsAgentAndShutsDown(t *testing.T) {
	// Reserve a free port for the orchestrator to listen on.
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := reserved.Addr().String()
	_ = reserved.Close()

	captureDir := t.TempDir()
	orch, err := NewOrchestrator(&Config{
		Agent: AgentConfig{
			Address:          addr,
			Secret:           "s3cret",
			HandshakeTimeout: time.Second,
		},
		API:       APIConfig{Address: "127.0.0.1:0"},
		Scheduler: SchedulerConfig{StartingPeriod: time.Second, EventQueueSize: 16},
		Capturer: DDBFIECapturerConfig{
			BatchSize:        100,
			CaptureDir:       captureDir,
			RotationInterval: time.Hour,
		},
		CaptureQueueSize:   16,
		CaptureFlushPeriod: time.Second,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- orch.Run(ctx) }()

	agent := dialOrchestrator(t, addr)
	writeLine(t, agent, `{"agent_id":"a1","secret":"s3cret"}`)
	reader := bufio.NewReader(agent)
	if line, _ := reader.ReadString('\n'); line != `{"authenticated":true,"message":"authenticated"}`+"\n" {
		t.Fatalf("unexpected auth response %q", line)
	}

	// The FIE sent here must be in the capture file after shutdown.
	writeLine(t, agent, `7,1000,"192.0.2.1",2,"",0`)

	// A second connection with the same agent id is closed after the handshake.
	duplicate := dialOrchestrator(t, addr)
	writeLine(t, duplicate, `{"agent_id":"a1","secret":"s3cret"}`)
	duplicateReader := bufio.NewReader(duplicate)
	if _, err := duplicateReader.ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	_ = duplicate.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := duplicateReader.ReadString('\n'); err != io.EOF {
		t.Fatalf("duplicate agent: got %v, want EOF", err)
	}
	// The first agent must still be connected: its read times out, not EOF.
	_ = agent.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	var netErr net.Error
	if _, err := reader.ReadString('\n'); !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("first agent: got %v, want a read timeout", err)
	}
	_ = agent.SetReadDeadline(time.Time{})

	// Shutdown must close the still-connected agent and return nil.
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	if _, err := reader.ReadString('\n'); err == nil {
		t.Fatal("expected the agent connection to be closed")
	}

	if got, want := capturedRows(t, captureDir), []string{"7 [192 0 2 1] []"}; !slices.Equal(got, want) {
		t.Fatalf("captured rows: got %q, want %q", got, want)
	}
}

// dialOrchestrator connects to the agent address, waiting for the orchestrator
// to start listening.
func dialOrchestrator(t *testing.T, addr string) net.Conn {
	t.Helper()
	var conn net.Conn
	var err error
	for range 100 {
		if conn, err = net.Dial("tcp", addr); err == nil {
			t.Cleanup(func() { _ = conn.Close() })
			return conn
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(err)
	return nil
}

func writeLine(t *testing.T, conn net.Conn, line string) {
	t.Helper()
	if _, err := fmt.Fprintln(conn, line); err != nil {
		t.Fatal(err)
	}
}

// capturedRows returns the PD id and reply addresses of every captured FIE.
func capturedRows(t *testing.T, captureDir string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(captureDir, "fies-*.duckdb"))
	if err != nil || len(files) != 1 {
		t.Fatalf("capture files: got %v, %v, want one file", files, err)
	}
	db, err := sql.Open("duckdb", files[0])
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.Query("SELECT probing_directive_id, near_reply_address, far_reply_address FROM fies")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	var result []string
	for rows.Next() {
		var id uint32
		var near, far []byte
		if err := rows.Scan(&id, &near, &far); err != nil {
			t.Fatal(err)
		}
		result = append(result, fmt.Sprintf("%d %v %v", id, near, far))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}
