// Copyright (c) 2026 Sorbonne Université
// SPDX-License-Identifier: MIT

package retina

import (
	"context"
	"errors"
	"math"
	"net/netip"
	"slices"
	"strings"
	"testing"
	"time"
	"unsafe"
)

const testPeriod = 100 * time.Millisecond

// startScheduler runs a scheduler until the test ends, or stop is called.
func startScheduler(t *testing.T, maxIssuanceCount uint64) (s *Scheduler, stop func()) {
	t.Helper()
	config := &SchedulerConfig{StartingPeriod: testPeriod, MaxIssuanceCount: maxIssuanceCount, EventQueueSize: 16}
	if err := config.validate(); err != nil {
		t.Fatal(err)
	}
	s = NewScheduler(config)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = s.Run(ctx)
		close(done)
	}()
	stop = func() {
		cancel()
		<-done
	}
	t.Cleanup(stop)
	return s, stop
}

func insertPD(t *testing.T, s *Scheduler, agentID string) uint32 {
	t.Helper()
	id, err := s.Insert([]PD{{AgentID: agentID, Destination: netip.MustParseAddr("192.0.2.1")}})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func newIssuer(t *testing.T, s *Scheduler, agentID string) *Issuer {
	t.Helper()
	issuer, err := s.NewIssuer(agentID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = issuer.Close() })
	return issuer
}

// issueID issues one PD and returns its ID.
func issueID(t *testing.T, issuer *Issuer) uint32 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pd, err := issuer.Issue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return pd.ID
}

func TestScheduler_IssuesOnePeriodAfterInsertThenPeriodically(t *testing.T) {
	s, _ := startScheduler(t, 0)
	issuer := newIssuer(t, s, "a1")

	inserted := time.Now()
	id := insertPD(t, s, "a1")

	for round := 1; round <= 3; round++ {
		if got := issueID(t, issuer); got != id {
			t.Fatalf("round %d: got PD %d, want %d", round, got, id)
		}
		if elapsed, want := time.Since(inserted), time.Duration(round)*testPeriod; elapsed < want {
			t.Fatalf("round %d: issued after %v, want at least %v", round, elapsed, want)
		}
	}
}

func TestScheduler_StalledAgentGetsEachOverduePDOnce(t *testing.T) {
	s, _ := startScheduler(t, 0)
	ids := []uint32{insertPD(t, s, "a1"), insertPD(t, s, "a1"), insertPD(t, s, "a1")}

	// No issuer for three periods: every PD is overdue several times over.
	time.Sleep(3 * testPeriod)
	issuer := newIssuer(t, s, "a1")

	start := time.Now()
	for _, want := range ids {
		if got := issueID(t, issuer); got != want {
			t.Fatalf("got PD %d, want %d", got, want)
		}
	}
	if elapsed := time.Since(start); elapsed > testPeriod/2 {
		t.Fatalf("overdue PDs took %v to issue, want them at once", elapsed)
	}

	// The next round comes one period after the late issuance, not at once.
	if got := issueID(t, issuer); got != ids[0] {
		t.Fatalf("got PD %d, want %d", got, ids[0])
	}
	if elapsed := time.Since(start); elapsed < testPeriod {
		t.Fatalf("second round after %v, want at least %v", elapsed, testPeriod)
	}
}

func TestScheduler_BulkInsert(t *testing.T) {
	s, _ := startScheduler(t, 1)
	a1, a2 := newIssuer(t, s, "a1"), newIssuer(t, s, "a2")
	addr := netip.MustParseAddr("192.0.2.1")

	// An invalid PD rejects the whole batch and uses no IDs.
	if _, err := s.Insert([]PD{{AgentID: "a1", Destination: addr}, {AgentID: "a1"}}); err == nil {
		t.Fatal("expected an error for a PD without destination")
	}
	if _, err := s.Insert(nil); err == nil {
		t.Fatal("expected an error for an empty batch")
	}

	first, err := s.Insert([]PD{
		{AgentID: "a1", Destination: addr},
		{AgentID: "a2", Destination: addr},
		{AgentID: "a1", Destination: addr},
	})
	if err != nil {
		t.Fatal(err)
	}
	if first != 0 {
		t.Fatalf("first ID: got %d, want 0", first)
	}
	if got := []uint32{issueID(t, a1), issueID(t, a1), issueID(t, a2)}; !slices.Equal(got, []uint32{0, 2, 1}) {
		t.Fatalf("issued IDs: got %v, want [0 2 1]", got)
	}
	if next := insertPD(t, s, "a1"); next != 3 {
		t.Fatalf("next ID: got %d, want 3", next)
	}
}

func TestScheduler_InsertFailsWhenIDsRunOut(t *testing.T) {
	s, _ := startScheduler(t, 0)
	s.nextID = math.MaxUint32
	pd := PD{AgentID: "a1", Destination: netip.MustParseAddr("192.0.2.1")}

	if _, err := s.Insert([]PD{pd, pd}); err == nil {
		t.Fatal("expected an error when the batch needs more IDs than are left")
	}
	if id, err := s.Insert([]PD{pd}); err != nil || id != math.MaxUint32 {
		t.Fatalf("last ID: got %d, %v, want %d", id, err, uint32(math.MaxUint32))
	}
	if _, err := s.Insert([]PD{pd}); err == nil {
		t.Fatal("expected an error when no IDs are left")
	}
}

func TestScheduler_PDsShareTheAgentIDString(t *testing.T) {
	s, _ := startScheduler(t, 0)
	issuer := newIssuer(t, s, "a1")
	addr := netip.MustParseAddr("192.0.2.1")

	// Each PD arrives with its own copy of the agent ID, as from a JSON request.
	if _, err := s.Insert([]PD{
		{AgentID: strings.Clone("a1"), Destination: addr},
		{AgentID: strings.Clone("a1"), Destination: addr},
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first, err := issuer.Issue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	second, err := issuer.Issue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if first.AgentID != "a1" || second.AgentID != "a1" {
		t.Fatalf("agent IDs: got %q and %q, want a1", first.AgentID, second.AgentID)
	}
	// Comparing the data pointers is the only way to tell one string from two equal ones.
	if unsafe.StringData(first.AgentID) != unsafe.StringData(second.AgentID) { //nolint:gosec // G103: pointers are only compared
		t.Fatal("the two PDs do not share one agent ID string")
	}
}

func TestScheduler_MaxIssuanceCountRetiresPD(t *testing.T) {
	s, _ := startScheduler(t, 1)
	issuer := newIssuer(t, s, "a1")
	id := insertPD(t, s, "a1")

	if got := issueID(t, issuer); got != id {
		t.Fatalf("got PD %d, want %d", got, id)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*testPeriod)
	defer cancel()
	if pd, err := issuer.Issue(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("after retirement: got %v, %v, want a deadline error", pd, err)
	}
}

func TestScheduler_InsertAnswersWaitingIssuer(t *testing.T) {
	s, _ := startScheduler(t, 0)
	issuer := newIssuer(t, s, "a1")

	issued := make(chan uint32, 1)
	go func() {
		if pd, err := issuer.Issue(context.Background()); err == nil {
			issued <- pd.ID
		}
	}()

	// Let the request reach the empty schedule before inserting.
	time.Sleep(testPeriod / 2)
	id := insertPD(t, s, "a1")
	insertPD(t, s, "other-agent")

	select {
	case got := <-issued:
		if got != id {
			t.Fatalf("got PD %d, want %d", got, id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiting issuer was not answered")
	}
}

func TestScheduler_OneIssuerPerAgent(t *testing.T) {
	s, _ := startScheduler(t, 0)
	first := newIssuer(t, s, "a1")

	if _, err := s.NewIssuer("a1"); err == nil {
		t.Fatal("expected an error for a second issuer of the same agent")
	}
	newIssuer(t, s, "a2")

	_ = first.Close()
	_ = first.Close()
	newIssuer(t, s, "a1")
}

func TestScheduler_IssueUnblocks(t *testing.T) {
	blocked := func(issuer *Issuer, ctx context.Context) <-chan error {
		result := make(chan error, 1)
		go func() {
			_, err := issuer.Issue(ctx)
			result <- err
		}()
		time.Sleep(testPeriod / 2)
		return result
	}
	expect := func(t *testing.T, result <-chan error, want error) {
		t.Helper()
		select {
		case err := <-result:
			if !errors.Is(err, want) {
				t.Fatalf("got %v, want %v", err, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Issue did not unblock")
		}
	}

	t.Run("context", func(t *testing.T) {
		s, _ := startScheduler(t, 0)
		ctx, cancel := context.WithCancel(context.Background())
		result := blocked(newIssuer(t, s, "a1"), ctx)
		cancel()
		expect(t, result, context.Canceled)
	})
	t.Run("close", func(t *testing.T) {
		s, _ := startScheduler(t, 0)
		issuer := newIssuer(t, s, "a1")
		result := blocked(issuer, context.Background())
		_ = issuer.Close()
		expect(t, result, ErrIssuerClosed)
	})
	t.Run("scheduler stopped", func(t *testing.T) {
		s, stop := startScheduler(t, 0)
		issuer := newIssuer(t, s, "a1")
		result := blocked(issuer, context.Background())
		stop()
		expect(t, result, ErrSchedulerStopped)

		if _, err := s.Insert([]PD{{AgentID: "a1", Destination: netip.MustParseAddr("192.0.2.1")}}); !errors.Is(err, ErrSchedulerStopped) {
			t.Fatalf("Insert after stop: got %v, want %v", err, ErrSchedulerStopped)
		}
	})
}
