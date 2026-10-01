package orchestrator

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	api "github.com/dioptra-io/retina-commons/api/v1"
)

func newTestResearchScheduler(t *testing.T, period time.Duration, max uint64) *ResearchScheduler {
	t.Helper()
	scheduler, err := NewResearchScheduler(&ResearchSchedulerConfig{
		StartingPeriod: period, MaxIssuanceCount: max,
		MaxEventsPerPass: 64, EventChannelSize: 1024,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	if err != nil {
		t.Fatalf("NewResearchScheduler: %v", err)
	}
	t.Cleanup(func() { _ = scheduler.Close() })
	return scheduler
}

func testPD(agent string) *api.ProbingDirective {
	return &api.ProbingDirective{AgentID: agent, DestinationAddress: net.IPv4(192, 0, 2, 1)}
}

func TestResearchSchedulerIssuesEveryPDBeforeRepeating(t *testing.T) {
	scheduler := newTestResearchScheduler(t, 20*time.Millisecond, 0)
	for range 20 {
		if _, err := scheduler.Insert(testPD("agent")); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}
	seen := make(map[uint64]bool)
	for range 20 {
		pd, err := scheduler.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if seen[pd.ProbingDirectiveID] {
			t.Fatalf("PD %d repeated within first period", pd.ProbingDirectiveID)
		}
		seen[pd.ProbingDirectiveID] = true
	}
}

func TestResearchSchedulerExcludesAndIncludesAgent(t *testing.T) {
	scheduler := newTestResearchScheduler(t, time.Millisecond, 0)
	if _, err := scheduler.Insert(testPD("excluded")); err != nil {
		t.Fatal(err)
	}
	if _, err := scheduler.Insert(testPD("included")); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Agent("excluded", true); err != nil {
		t.Fatal(err)
	}
	for range 20 {
		pd, err := scheduler.Next()
		if err != nil {
			t.Fatal(err)
		}
		if pd.AgentID != "included" {
			t.Fatalf("issued PD for excluded agent %q", pd.AgentID)
		}
	}
	if err := scheduler.Agent("excluded", false); err != nil {
		t.Fatal(err)
	}
	found := false
	for range 20 {
		pd, err := scheduler.Next()
		if err != nil {
			t.Fatal(err)
		}
		if pd.AgentID == "excluded" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("included agent was never issued")
	}
}

func TestResearchSchedulerRetiresAndCloseWakesNext(t *testing.T) {
	scheduler := newTestResearchScheduler(t, time.Millisecond, 1)
	if _, err := scheduler.Insert(testPD("agent")); err != nil {
		t.Fatal(err)
	}
	if _, err := scheduler.Next(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := scheduler.Next(); done <- err }()
	_ = scheduler.Close()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Next error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Next did not wake after Close")
	}
}
