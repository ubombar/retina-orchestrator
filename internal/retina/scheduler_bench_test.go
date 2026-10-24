// Copyright (c) 2026 Sorbonne Université
// SPDX-License-Identifier: MIT

package retina

import (
	"context"
	"math/rand/v2"
	"net/netip"
	"testing"
	"time"
)

const (
	benchPDCount   = 1_000_000
	benchAddrCount = 100_000
	benchFIECount  = 1 << 20
	benchAgentID   = "bench-agent"
)

var benchRPModes = []struct {
	name      string
	threshold float64
}{
	{"rp=off", 0},
	{"rp=on", 1},
	// RP on but never throttling: isolates the bookkeeping cost.
	{"rp=on-nothrottle", 1e18},
}

func benchAddrs(r *rand.Rand, n int) []netip.Addr {
	addrs := make([]netip.Addr, n)
	for i := range addrs {
		v := r.Uint32()
		addrs[i] = netip.AddrFrom4([4]byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}) //nolint:gosec // G115: splitting v into bytes
	}
	return addrs
}

// benchPopulate builds a scheduler with n PDs inserted directly, applies one
// initial FIE per PD, and returns FIEs that are ~99% steady state.
func benchPopulate(b *testing.B, threshold float64, n int) (*Scheduler, []FIE) {
	b.Helper()
	r := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // G404: deterministic benchmark data
	addrs := benchAddrs(r, benchAddrCount)
	s := NewScheduler(&SchedulerConfig{
		StartingPeriod:            time.Hour,
		EventQueueSize:            1,
		PerAddressImpactThreshold: threshold,
	})
	pds := make([]PD, n)
	near := make([]netip.Addr, n)
	far := make([]netip.Addr, n)
	for i := range pds {
		pds[i] = PD{ID: uint32(i), AgentID: benchAgentID, Destination: addrs[r.IntN(len(addrs))], NearTTL: 10, Protocol: 1}
		near[i] = addrs[r.IntN(len(addrs))]
		far[i] = addrs[r.IntN(len(addrs))]
	}
	s.apply(&event{kind: eventInsert, pds: pds})
	for i := range n {
		s.applyFIE(&FIE{PDID: uint32(i), AgentID: benchAgentID, Near: near[i], Far: far[i]})
	}
	fies := make([]FIE, benchFIECount)
	for i := range fies {
		id := r.IntN(n)
		f := FIE{PDID: uint32(id), AgentID: benchAgentID, Near: near[id], Far: far[id]} //nolint:gosec // G115: id < n, at most a few million
		if r.IntN(100) == 0 {
			if r.IntN(2) == 0 {
				f.Near = addrs[r.IntN(len(addrs))]
			} else {
				f.Far = addrs[r.IntN(len(addrs))]
			}
		}
		fies[i] = f
	}
	return s, fies
}

func BenchmarkScheduler_ApplyFIE(b *testing.B) {
	for _, mode := range benchRPModes {
		b.Run(mode.name, func(b *testing.B) {
			s, fies := benchPopulate(b, mode.threshold, benchPDCount)
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				s.applyFIE(&fies[i&(benchFIECount-1)])
				i++
			}
		})
	}
}

func BenchmarkScheduler_NextInterval(b *testing.B) {
	for _, mode := range benchRPModes {
		b.Run(mode.name, func(b *testing.B) {
			s, fies := benchPopulate(b, mode.threshold, benchPDCount)
			for i := range fies {
				s.applyFIE(&fies[i])
			}
			b.ReportAllocs()
			var sink int64
			id := uint32(0)
			for b.Loop() {
				sink += s.nextInterval(id)
				id++
				if id == benchPDCount {
					id = 0
				}
			}
			_ = sink
		})
	}
}

func BenchmarkScheduler_Issue(b *testing.B) {
	const n = 100_000
	for _, mode := range benchRPModes {
		b.Run(mode.name, func(b *testing.B) {
			r := rand.New(rand.NewPCG(3, 4)) //nolint:gosec // G404: deterministic benchmark data
			addrs := benchAddrs(r, benchAddrCount)
			s := NewScheduler(&SchedulerConfig{
				StartingPeriod:            time.Nanosecond,
				EventQueueSize:            1024,
				PerAddressImpactThreshold: mode.threshold,
			})
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() { _ = s.Run(ctx); close(done) }()
			defer func() { cancel(); <-done }()

			pds := make([]PD, n)
			for i := range pds {
				pds[i] = PD{AgentID: benchAgentID, Destination: addrs[r.IntN(len(addrs))], NearTTL: 10, Protocol: 1}
			}
			if _, err := s.Insert(pds); err != nil {
				b.Fatal(err)
			}
			for i := range n {
				if err := s.Update(&FIE{PDID: uint32(i), AgentID: benchAgentID, Near: addrs[r.IntN(len(addrs))], Far: addrs[r.IntN(len(addrs))]}); err != nil {
					b.Fatal(err)
				}
			}
			issuer, err := s.NewIssuer(benchAgentID)
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = issuer.Close() }()

			b.ReportAllocs()
			for b.Loop() {
				if _, err := issuer.Issue(ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
