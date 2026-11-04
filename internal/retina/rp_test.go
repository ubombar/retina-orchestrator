// Copyright (c) 2026 Sorbonne Université
// SPDX-License-Identifier: MIT

package retina

import (
	"context"
	"math"
	"math/rand/v2"
	"net/netip"
	"testing"
	"time"
)

// rpPeriodSeconds is testPeriod in seconds.
var rpPeriodSeconds = testPeriod.Seconds()

// newRPScheduler returns a scheduler that is not running, with n PDs of agent
// "a1" inserted directly (IDs 0..n-1).
func newRPScheduler(t *testing.T, threshold float64, n int) *Scheduler {
	t.Helper()
	config := &SchedulerConfig{StartingPeriod: testPeriod, EventQueueSize: 16, PerAddressImpactThreshold: threshold}
	if err := config.validate(); err != nil {
		t.Fatal(err)
	}
	s := NewScheduler(config)
	pds := make([]PD, n)
	for i := range pds {
		pds[i] = PD{ID: uint32(i), AgentID: "a1", Destination: netip.MustParseAddr("192.0.2.1")}
	}
	s.apply(&event{kind: eventInsert, pds: pds})
	return s
}

func rpAddr(i int) netip.Addr {
	return netip.AddrFrom4([4]byte{198, 51, 100, byte(i)}) //nolint:gosec // G115: tests use fewer than 256 addresses
}

func rpFIE(id uint32, near, far netip.Addr) *FIE {
	return &FIE{PDID: id, AgentID: "a1", Near: near, Far: far}
}

func rpClose(a, b float64) bool {
	return math.Abs(a-b) <= 1e-9*math.Max(1, math.Max(math.Abs(a), math.Abs(b)))
}

func checkInterval(t *testing.T, s *Scheduler, id uint32, factor float64) {
	t.Helper()
	want := float64(testPeriod.Nanoseconds()) * factor
	if got := float64(s.nextInterval(id)); math.Abs(got-want) > 1 {
		t.Errorf("PD %d interval: got %v ns, want %v ns (%v P)", id, got, want, factor)
	}
}

func checkLoad(t *testing.T, s *Scheduler, addr netip.Addr, want float64) {
	t.Helper()
	if got := s.addressLoad(addr); !rpClose(got, want) {
		t.Errorf("load of %v: got %v, want %v", addr, got, want)
	}
}

func TestRP_DisabledKeepsBasePeriod(t *testing.T) {
	s := newRPScheduler(t, 0, 5)
	for id := range uint32(5) {
		s.applyFIE(rpFIE(id, rpAddr(1), rpAddr(2)))
	}
	for id := range uint32(5) {
		checkInterval(t, s, id, 1)
	}
}

func TestRP_UnknownAddressesKeepBasePeriod(t *testing.T) {
	s := newRPScheduler(t, 1, 3)
	for id := range uint32(3) {
		checkInterval(t, s, id, 1)
	}
}

func TestRP_NotSharedKeepsBasePeriod(t *testing.T) {
	for _, threshold := range []float64{1 / rpPeriodSeconds, 100} {
		s := newRPScheduler(t, threshold, 1)
		s.applyFIE(rpFIE(0, rpAddr(1), rpAddr(2)))
		checkInterval(t, s, 0, 1)
		checkLoad(t, s, rpAddr(1), 1/rpPeriodSeconds)
		checkLoad(t, s, rpAddr(2), 1/rpPeriodSeconds)
	}
}

func TestRP_SharedFarSlowsDown(t *testing.T) {
	const n = 6
	// n/(Λ P) = 3.
	threshold := n / (3 * rpPeriodSeconds)
	s := newRPScheduler(t, threshold, n)
	for id := range uint32(n) {
		s.applyFIE(rpFIE(id, rpAddr(100+int(id)), rpAddr(1)))
	}
	checkLoad(t, s, rpAddr(1), n/rpPeriodSeconds)
	for id := range uint32(n) {
		checkInterval(t, s, id, 3)
	}
}

func TestRP_BusierAddressDecides(t *testing.T) {
	// Λ P = 1, so the factor of an address is its number of slots.
	s := newRPScheduler(t, 1/rpPeriodSeconds, 4)
	near, far := rpAddr(1), rpAddr(2)
	s.applyFIE(rpFIE(0, near, far))
	s.applyFIE(rpFIE(1, near, rpAddr(10)))
	s.applyFIE(rpFIE(2, rpAddr(11), far))
	s.applyFIE(rpFIE(3, rpAddr(12), far))
	checkInterval(t, s, 0, 3)
	checkInterval(t, s, 1, 2)
}

func TestRP_NearEqualsFarCountsTwice(t *testing.T) {
	s := newRPScheduler(t, 1/rpPeriodSeconds, 1)
	s.applyFIE(rpFIE(0, rpAddr(1), rpAddr(1)))
	checkLoad(t, s, rpAddr(1), 2/rpPeriodSeconds)
	checkInterval(t, s, 0, 2)
}

func TestRP_MissingReplyKeepsStoredAddress(t *testing.T) {
	s := newRPScheduler(t, 100, 1)
	s.applyFIE(rpFIE(0, rpAddr(1), rpAddr(2)))
	s.applyFIE(rpFIE(0, netip.Addr{}, netip.Addr{}))
	checkLoad(t, s, rpAddr(1), 1/rpPeriodSeconds)
	checkLoad(t, s, rpAddr(2), 1/rpPeriodSeconds)
	s.applyFIE(rpFIE(0, netip.Addr{}, rpAddr(3)))
	checkLoad(t, s, rpAddr(1), 1/rpPeriodSeconds)
	checkLoad(t, s, rpAddr(2), 0)
	checkLoad(t, s, rpAddr(3), 1/rpPeriodSeconds)
}

func TestRP_RouteChangeMovesLoad(t *testing.T) {
	s := newRPScheduler(t, 1/rpPeriodSeconds, 2)
	x, y := rpAddr(1), rpAddr(2)
	s.applyFIE(rpFIE(0, rpAddr(10), x))
	s.applyFIE(rpFIE(1, rpAddr(11), x))
	checkLoad(t, s, x, 2/rpPeriodSeconds)
	checkInterval(t, s, 0, 2)

	s.applyFIE(rpFIE(1, rpAddr(11), y))
	checkLoad(t, s, x, 1/rpPeriodSeconds)
	checkLoad(t, s, y, 1/rpPeriodSeconds)
	checkInterval(t, s, 0, 1)
	checkInterval(t, s, 1, 1)
}

func TestRP_UnknownPDIgnored(t *testing.T) {
	s := newRPScheduler(t, 1, 1)
	s.applyFIE(rpFIE(42, rpAddr(1), rpAddr(2)))
	checkLoad(t, s, rpAddr(1), 0)
	checkLoad(t, s, rpAddr(2), 0)
	checkInterval(t, s, 0, 1)
}

func TestRP_RetireRemovesLoad(t *testing.T) {
	config := &SchedulerConfig{StartingPeriod: testPeriod, MaxIssuanceCount: 2, EventQueueSize: 16, PerAddressImpactThreshold: 100}
	if err := config.validate(); err != nil {
		t.Fatal(err)
	}
	s := NewScheduler(config)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = s.Run(ctx)
		close(done)
	}()
	stop := func() { cancel(); <-done }
	t.Cleanup(func() {
		select {
		case <-done:
		default:
			stop()
		}
	})

	issuer := newIssuer(t, s, "a1")
	id := insertPD(t, s, "a1")
	issueID(t, issuer)
	if err := s.Update(rpFIE(id, rpAddr(1), rpAddr(2))); err != nil {
		t.Fatal(err)
	}
	issueID(t, issuer) // retires the PD; the FIE was applied before
	// A late FIE for the retired PD must be ignored.
	if err := s.Update(rpFIE(id, rpAddr(3), rpAddr(4))); err != nil {
		t.Fatal(err)
	}
	_ = issuer.Close()
	stop()
	for i := 1; i <= 4; i++ {
		checkLoad(t, s, rpAddr(i), 0)
	}
}

func TestRP_LoadNeverExceedsThreshold(t *testing.T) {
	const n, addrs = 500, 20
	threshold := 37.0
	s := newRPScheduler(t, threshold, n)
	rng := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // G404: deterministic test data
	pick := func() netip.Addr {
		if rng.IntN(10) == 0 {
			return netip.Addr{} // unknown
		}
		return rpAddr(rng.IntN(addrs))
	}
	near := make([]netip.Addr, n)
	far := make([]netip.Addr, n)
	for i := range n {
		near[i], far[i] = pick(), pick()
		if rng.IntN(10) == 0 {
			far[i] = near[i]
		}
		s.applyFIE(rpFIE(uint32(i), near[i], far[i]))
	}
	rate := make(map[netip.Addr]float64)
	for i := range n {
		r := 1e9 / float64(s.nextInterval(uint32(i)))
		for _, a := range []netip.Addr{near[i], far[i]} {
			if a.IsValid() {
				rate[a] += r
			}
		}
	}
	for a, r := range rate {
		if r > threshold*(1+1e-9) {
			t.Errorf("address %v: %v hits/s, threshold %v", a, r, threshold)
		}
	}
}

func TestRP_ValidateRejectsBadThreshold(t *testing.T) {
	for _, v := range []float64{-1, math.NaN(), math.Inf(1), math.Inf(-1)} {
		config := &SchedulerConfig{StartingPeriod: testPeriod, EventQueueSize: 1, PerAddressImpactThreshold: v}
		if err := config.validate(); err == nil {
			t.Errorf("threshold %v: expected an error", v)
		}
	}
	for _, v := range []float64{0, 0.5, 1000} {
		config := &SchedulerConfig{StartingPeriod: testPeriod, EventQueueSize: 1, PerAddressImpactThreshold: v}
		if err := config.validate(); err != nil {
			t.Errorf("threshold %v: %v", v, err)
		}
	}
}

// rpLongPeriod is long enough that an issuance is never late in these tests,
// so a PD is due again exactly one interval after its previous due time.
const rpLongPeriod = time.Hour

// newRPIssueScheduler returns a scheduler that is not running, with n PDs of
// agent "a1" inserted directly (IDs 0..n-1) and an issuer connected for them.
func newRPIssueScheduler(t *testing.T, threshold float64, n int) (*Scheduler, *Issuer) {
	t.Helper()
	config := &SchedulerConfig{StartingPeriod: rpLongPeriod, EventQueueSize: 16, PerAddressImpactThreshold: threshold}
	if err := config.validate(); err != nil {
		t.Fatal(err)
	}
	s := NewScheduler(config)
	pds := make([]PD, n)
	for i := range pds {
		pds[i] = PD{ID: uint32(i), AgentID: "a1", Destination: netip.MustParseAddr("192.0.2.1")} //nolint:gosec // G115: small n
	}
	s.apply(&event{kind: eventInsert, pds: pds})
	issuer := &Issuer{scheduler: s, agentID: "a1", issuances: make(chan issuance, 1), closed: make(chan struct{})}
	reply := make(chan error, 1)
	s.apply(&event{kind: eventConnect, issuer: issuer, reply: reply})
	if err := <-reply; err != nil {
		t.Fatal(err)
	}
	return s, issuer
}

// issueAll issues every PD of agent "a1" once, through the issue path.
func issueAll(t *testing.T, s *Scheduler, issuer *Issuer) {
	t.Helper()
	for range s.nodes["a1"].pds {
		s.apply(&event{kind: eventIssue, issuer: issuer})
		select {
		case <-issuer.issuances:
		default:
			t.Fatal("no PD was issued")
		}
	}
}

// dues returns the due time of each PD of agent "a1", indexed by PD ID.
func dues(s *Scheduler) []int64 {
	node := s.nodes["a1"]
	out := make([]int64, len(node.pds))
	for _, entry := range node.heap {
		out[node.pds[entry.index].ID] = entry.due
	}
	return out
}

func checkDues(t *testing.T, s *Scheduler, before []int64, factor float64) {
	t.Helper()
	want := int64(float64(rpLongPeriod) * factor)
	for id, due := range dues(s) {
		if got := due - before[id]; got != want {
			t.Errorf("PD %d: due %v after its previous due time, want %v", id, time.Duration(got), time.Duration(want))
		}
	}
}

func checkStats(t *testing.T, s *Scheduler, want RPStats) {
	t.Helper()
	got := s.rpStats()
	if got.PDs != want.PDs || got.DelayedPDs != want.DelayedPDs || got.Addresses != want.Addresses ||
		got.OverloadedAddresses != want.OverloadedAddresses || !rpClose(got.MaxLoad, want.MaxLoad) {
		t.Errorf("stats: got %+v, want %+v", got, want)
	}
}

func TestRP_IssueDelaysAndReturnsToBasePeriod(t *testing.T) {
	const n = 3
	p := rpLongPeriod.Seconds()
	// The shared far address has load n/P, so Λ = 1/P slows each PD by n.
	s, issuer := newRPIssueScheduler(t, 1/p, n)
	for id := range uint32(n) {
		s.applyFIE(rpFIE(id, rpAddr(10+int(id)), rpAddr(1)))
	}

	before := dues(s)
	issueAll(t, s, issuer)
	checkDues(t, s, before, n)
	checkStats(t, s, RPStats{PDs: n, DelayedPDs: n, Addresses: n + 1, OverloadedAddresses: 1, MaxLoad: n / p})

	// The route changes: each PD now has its own far address.
	for id := range uint32(n) {
		s.applyFIE(rpFIE(id, rpAddr(10+int(id)), rpAddr(20+int(id))))
	}
	before = dues(s)
	issueAll(t, s, issuer)
	checkDues(t, s, before, 1)
	checkStats(t, s, RPStats{PDs: n, DelayedPDs: 0, Addresses: 2*n + 1, OverloadedAddresses: 0, MaxLoad: 1 / p})
}

func TestRP_DisabledIssuesAtBasePeriod(t *testing.T) {
	const n = 3
	s, issuer := newRPIssueScheduler(t, 0, n)
	for id := range uint32(n) {
		s.applyFIE(rpFIE(id, rpAddr(10+int(id)), rpAddr(1)))
	}
	for range 2 {
		before := dues(s)
		issueAll(t, s, issuer)
		checkDues(t, s, before, 1)
	}
	checkStats(t, s, RPStats{PDs: n})
}

func TestRP_StatsThroughRun(t *testing.T) {
	s, _ := startScheduler(t, 0)
	insertPD(t, s, "a1")
	stats, err := s.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.PDs != 1 || stats.DelayedPDs != 0 || stats.Addresses != 0 {
		t.Errorf("stats: got %+v", stats)
	}
}
