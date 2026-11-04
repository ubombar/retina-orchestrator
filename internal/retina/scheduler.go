// Copyright (c) 2026 Sorbonne Université
// SPDX-License-Identifier: MIT

package retina

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/netip"
	"sync"
	"time"
)

var (
	// ErrSchedulerStopped is returned once the scheduler's Run has returned.
	ErrSchedulerStopped = errors.New("scheduler stopped")
	// ErrIssuerClosed is returned by Issue once the issuer is closed.
	ErrIssuerClosed = errors.New("issuer closed")
)

// SchedulerConfig configures the scheduler.
type SchedulerConfig struct {
	// StartingPeriod is the issuance period of every new PD. It is also the
	// delay between the insertion of a PD and its first issuance.
	StartingPeriod time.Duration `json:"starting_period"`
	// MaxIssuanceCount is how many times a PD is issued before it leaves the
	// schedule. Zero means indefinitely.
	MaxIssuanceCount uint64 `json:"max_issuance_count"`
	// EventQueueSize is the size of the scheduler's event queue. Callers
	// block while it is full.
	EventQueueSize int `json:"event_queue_size"`
	// PerAddressImpactThreshold is the responsible probing threshold Λ, in
	// hits per second on any one address. The load of an address is the sum
	// of 1/P over the near and far slots of the PDs predicted to hit it, and
	// a PD whose busiest address has load M is next issued after
	// P · max(1, M/Λ) instead of P. Zero disables responsible probing.
	PerAddressImpactThreshold float64 `json:"per_address_impact_threshold"`
}

func (c *SchedulerConfig) validate() error {
	if c.StartingPeriod <= 0 {
		return fmt.Errorf("starting period must be positive: got %v", c.StartingPeriod)
	}
	if c.EventQueueSize < 1 {
		return fmt.Errorf("event queue size must be at least 1: got %d", c.EventQueueSize)
	}
	if t := c.PerAddressImpactThreshold; t < 0 || math.IsNaN(t) || math.IsInf(t, 0) {
		return fmt.Errorf("per-address impact threshold must be finite and not negative: got %v", t)
	}
	return nil
}

type eventKind uint8

const (
	eventInsert eventKind = iota
	eventUpdate
	eventConnect
	eventDisconnect
	eventIssue
	eventStats
)

// event is a request to the scheduler goroutine.
type event struct {
	kind   eventKind
	pds    []PD           // eventInsert
	fie    *FIE           // eventUpdate
	issuer *Issuer        // eventConnect, eventDisconnect, eventIssue
	reply  chan<- error   // eventConnect
	stats  chan<- RPStats // eventStats
}

// issuance is the scheduler's answer to an issue request: a PD and the time
// it is due, in nanoseconds since the scheduler's epoch.
type issuance struct {
	pd  PD
	due int64
}

// agentNode is the schedule of one agent. It is created the first time its
// agent ID is seen, by an inserted PD or by a connecting agent, and is never
// removed.
type agentNode struct {
	// agentID is the one copy of the agent ID that all of the node's PDs
	// share, instead of each keeping the string it arrived with.
	agentID string
	pds     []PD
	heap    pdHeap
	// issuer is the open issuer of this agent, nil while it has none.
	issuer *Issuer
	// waiting is set while the issuer has asked for a PD and the heap was
	// empty. The next insert answers it.
	waiting bool
}

// Scheduler holds one schedule per agent ID. All of its state is owned by the
// goroutine running Run; the other methods only queue events for it and are
// safe to call from any goroutine. They block while the event queue is full,
// so Run must be running.
type Scheduler struct {
	config *SchedulerConfig
	epoch  time.Time
	// insertMu makes ID assignment and queuing one step, so that PDs enter
	// the schedule in ID order even when Insert is called concurrently.
	insertMu sync.Mutex
	// nextID is the next PD ID to assign. IDs are 32-bit; it is wider so
	// that it can also say that none are left.
	nextID  uint64
	events  chan event
	stopped chan struct{}
	nodes   map[string]*agentNode
	// pdStates is the responsible probing state of each PD, indexed by ID.
	pdStates []pdState
	// addrs holds the load of each predicted address. Index 0 is the
	// unknown address and always has load 0. addrIndex maps an address to
	// its index in addrs.
	addrs     []addrLoad
	addrIndex map[netip.Addr]uint32
	// delayedPDs counts the PDs whose last interval was longer than their
	// base period.
	delayedPDs int
}

// RPStats describes the responsible probing state of the scheduler.
type RPStats struct {
	// PDs is the number of PDs in the schedule, retired ones included.
	PDs int
	// DelayedPDs is the number of PDs whose last interval was longer than
	// their base period.
	DelayedPDs int
	// Addresses is the number of predicted addresses.
	Addresses int
	// OverloadedAddresses is the number of addresses whose load is above the
	// threshold.
	OverloadedAddresses int
	// MaxLoad is the highest load of an address, in hits per second.
	MaxLoad float64
}

// pdState is what responsible probing keeps per PD.
type pdState struct {
	// near and far index the predicted addresses in addrs, 0 if unknown.
	near, far uint32
	// period is the base period P of the PD, in nanoseconds.
	period  int64
	retired bool
	// delayed is set while the last interval of the PD was longer than its
	// base period.
	delayed bool
}

// addrLoad is a predicted address and its load in hits per second.
type addrLoad struct {
	addr netip.Addr
	load float64
}

// NewScheduler creates an empty scheduler from a valid configuration.
func NewScheduler(config *SchedulerConfig) *Scheduler {
	return &Scheduler{
		config:  config,
		epoch:   time.Now(),
		events:  make(chan event, config.EventQueueSize),
		stopped: make(chan struct{}),
		nodes:   make(map[string]*agentNode),
		// Index 0 is the unknown address.
		addrs:     make([]addrLoad, 1),
		addrIndex: make(map[netip.Addr]uint32),
	}
}

// Run applies events until ctx is done. It must be called once. The scheduler
// never waits on an agent, so a slow agent cannot stall it.
func (s *Scheduler) Run(ctx context.Context) error {
	defer close(s.stopped)
	for {
		select {
		case <-ctx.Done():
			return nil
		case ev := <-s.events:
			s.apply(&ev)
		}
	}
}

// Insert adds PDs to the schedules of their agents in one event and assigns
// their IDs, which are consecutive in the order given: it returns the ID of
// the first one. Either all PDs are inserted or, if one is invalid, none. The
// agents do not need to be connected: their PDs wait for an Issuer.
//
// Insert sets the ID of each PD in pds and hands the slice to the scheduler:
// the caller must not modify it afterwards.
func (s *Scheduler) Insert(pds []PD) (uint32, error) {
	if len(pds) == 0 {
		return 0, errors.New("no PDs to insert")
	}
	for i := range pds {
		if pds[i].AgentID == "" {
			return 0, fmt.Errorf("PD %d: agent id is empty", i)
		}
		if !pds[i].Destination.IsValid() {
			return 0, fmt.Errorf("PD %d: destination is not set", i)
		}
	}

	s.insertMu.Lock()
	defer s.insertMu.Unlock()

	if left := math.MaxUint32 + 1 - s.nextID; uint64(len(pds)) > left {
		return 0, fmt.Errorf("cannot insert %d PDs: only %d of the 32-bit PD ids are left", len(pds), left)
	}
	first := uint32(s.nextID) //nolint:gosec // G115: nextID is at most MaxUint32 here, checked above
	for i := range pds {
		pds[i].ID = first + uint32(i)
	}
	if err := s.send(event{kind: eventInsert, pds: pds}); err != nil {
		return 0, err
	}
	s.nextID += uint64(len(pds))
	return first, nil
}

// Update feeds one FIE back into the schedule of the PD it answers.
func (s *Scheduler) Update(fie *FIE) error {
	return s.send(event{kind: eventUpdate, fie: fie})
}

// NewIssuer returns the issuer of agentID's schedule. It returns an error if
// that agent already has an open issuer: at most one exists per agent ID.
func (s *Scheduler) NewIssuer(agentID string) (*Issuer, error) {
	if agentID == "" {
		return nil, errors.New("agent id is empty")
	}
	issuer := &Issuer{
		scheduler: s,
		agentID:   agentID,
		issuances: make(chan issuance, 1),
		closed:    make(chan struct{}),
	}
	reply := make(chan error, 1)
	if err := s.send(event{kind: eventConnect, issuer: issuer, reply: reply}); err != nil {
		return nil, err
	}
	select {
	case err := <-reply:
		if err != nil {
			return nil, err
		}
		return issuer, nil
	case <-s.stopped:
		return nil, ErrSchedulerStopped
	}
}

// Stats returns the responsible probing state of the scheduler. It costs one
// pass over the predicted addresses on the scheduler goroutine.
func (s *Scheduler) Stats() (RPStats, error) {
	reply := make(chan RPStats, 1)
	if err := s.send(event{kind: eventStats, stats: reply}); err != nil {
		return RPStats{}, err
	}
	select {
	case stats := <-reply:
		return stats, nil
	case <-s.stopped:
		return RPStats{}, ErrSchedulerStopped
	}
}

// now returns the nanoseconds elapsed since the scheduler's epoch, on the
// monotonic clock.
func (s *Scheduler) now() int64 {
	return int64(time.Since(s.epoch))
}

func (s *Scheduler) send(ev event) error { //nolint:gocritic // events are channel values
	// Checked first so that a stopped scheduler never accepts an event, even
	// while its queue has room.
	select {
	case <-s.stopped:
		return ErrSchedulerStopped
	default:
	}
	select {
	case s.events <- ev:
		return nil
	case <-s.stopped:
		return ErrSchedulerStopped
	}
}

// The functions below run on the scheduler goroutine only.

func (s *Scheduler) apply(ev *event) {
	switch ev.kind {
	case eventInsert:
		// The whole batch gets the same first issuance time.
		due := s.now() + int64(s.config.StartingPeriod)
		for i := range ev.pds {
			node := s.node(ev.pds[i].AgentID)
			pd := ev.pds[i]
			s.addPDState(pd.ID)
			pd.AgentID = node.agentID
			node.pds = append(node.pds, pd)
			node.heap.push(heapEntry{due: due, index: len(node.pds) - 1})
			if node.waiting {
				node.waiting = false
				s.issue(node)
			}
		}

	case eventUpdate:
		s.applyFIE(ev.fie)

	case eventConnect:
		s.connect(ev)

	case eventStats:
		ev.stats <- s.rpStats()

	case eventDisconnect:
		if node := s.nodes[ev.issuer.agentID]; node != nil && node.issuer == ev.issuer {
			node.issuer = nil
			node.waiting = false
		}

	case eventIssue:
		node := s.nodes[ev.issuer.agentID]
		// Ignore a request from an issuer that is closed, or that still has
		// an unread answer: the push in issue must never block.
		if node == nil || node.issuer != ev.issuer || len(ev.issuer.issuances) != 0 {
			return
		}
		if len(node.heap) == 0 {
			node.waiting = true
			return
		}
		s.issue(node)
	}
}

// connect attaches the event's issuer to its agent, unless the agent already
// has one, and answers on the event's reply channel.
func (s *Scheduler) connect(ev *event) {
	node := s.node(ev.issuer.agentID)
	if node.issuer != nil {
		ev.reply <- fmt.Errorf("agent %q already has an issuer", ev.issuer.agentID)
		return
	}
	node.issuer = ev.issuer
	ev.reply <- nil
}

// node returns the node of agentID, creating it if needed.
func (s *Scheduler) node(agentID string) *agentNode {
	node, ok := s.nodes[agentID]
	if !ok {
		node = &agentNode{agentID: agentID}
		s.nodes[agentID] = node
	}
	return node
}

// issue hands the node's next PD to its issuer, whether or not it is due yet:
// the issuer does the waiting. The PD is due again one period after it is
// issued, so an agent that asks late shifts its schedule instead of skipping
// or repeating PDs.
func (s *Scheduler) issue(node *agentNode) {
	root := &node.heap[0]
	next := issuance{pd: node.pds[root.index], due: root.due}

	root.issuances++
	if limit := s.config.MaxIssuanceCount; limit != 0 && root.issuances >= limit {
		node.heap.removeRoot()
		s.retire(next.pd.ID)
	} else {
		interval := s.nextInterval(next.pd.ID)
		s.setDelayed(next.pd.ID, interval)
		node.heap.reschedule(max(next.due, s.now()) + interval)
	}
	node.issuer.issuances <- next
}

// addPDState gives a new PD its responsible probing state. Insert assigns IDs
// densely and in order, so the state is appended. A PD whose ID skips ahead
// gets no state and keeps its base period, rather than growing the slice to
// its ID.
func (s *Scheduler) addPDState(pdID uint32) {
	st := pdState{period: int64(s.config.StartingPeriod)}
	switch n := uint64(len(s.pdStates)); {
	case uint64(pdID) < n:
		s.pdStates[pdID] = st
	case uint64(pdID) == n:
		s.pdStates = append(s.pdStates, st)
	}
}

// applyFIE moves the predicted near and far addresses of the FIE's PD to the
// addresses that replied, and their loads with them. A missing reply keeps
// the prediction. FIEs of unknown or retired PDs are ignored, and so are all
// FIEs while responsible probing is disabled.
func (s *Scheduler) applyFIE(fie *FIE) {
	if s.config.PerAddressImpactThreshold == 0 || uint64(fie.PDID) >= uint64(len(s.pdStates)) {
		return
	}
	st := &s.pdStates[fie.PDID]
	if st.retired {
		return
	}
	rate := float64(time.Second) / float64(st.period)
	s.moveSlot(&st.near, fie.Near, rate)
	s.moveSlot(&st.far, fie.Far, rate)
}

// moveSlot points a slot at addr, moving rate from the old address's load to
// the new one. The common case of an unchanged address needs no map lookup.
func (s *Scheduler) moveSlot(slot *uint32, addr netip.Addr, rate float64) {
	if !addr.IsValid() || (*slot != 0 && s.addrs[*slot].addr == addr) {
		return
	}
	idx, ok := s.addrIndex[addr]
	if !ok {
		idx = uint32(len(s.addrs)) //nolint:gosec // G115: at most one address per PD slot
		s.addrs = append(s.addrs, addrLoad{addr: addr})
		s.addrIndex[addr] = idx
	}
	s.unload(*slot, rate)
	s.addrs[idx].load += rate
	*slot = idx
}

// unload takes rate off the load of address idx, clamped at 0 against float
// drift. Index 0 stays at 0.
func (s *Scheduler) unload(idx uint32, rate float64) {
	if idx != 0 {
		s.addrs[idx].load = max(0, s.addrs[idx].load-rate)
	}
}

// retire removes a PD that left the schedule from the loads of its addresses.
func (s *Scheduler) retire(pdID uint32) {
	if uint64(pdID) >= uint64(len(s.pdStates)) {
		return
	}
	st := &s.pdStates[pdID]
	rate := float64(time.Second) / float64(st.period)
	s.unload(st.near, rate)
	s.unload(st.far, rate)
	if st.delayed {
		s.delayedPDs--
	}
	st.near, st.far, st.retired, st.delayed = 0, 0, true, false
}

// setDelayed records whether the PD's next interval is longer than its base
// period, keeping delayedPDs up to date.
func (s *Scheduler) setDelayed(pdID uint32, interval int64) {
	if uint64(pdID) >= uint64(len(s.pdStates)) {
		return
	}
	st := &s.pdStates[pdID]
	delayed := interval > st.period
	switch {
	case delayed && !st.delayed:
		s.delayedPDs++
	case !delayed && st.delayed:
		s.delayedPDs--
	}
	st.delayed = delayed
}

// rpStats returns the current responsible probing state. It scans the
// addresses, so it is meant to be called rarely.
func (s *Scheduler) rpStats() RPStats {
	stats := RPStats{
		PDs:        len(s.pdStates),
		DelayedPDs: s.delayedPDs,
		Addresses:  len(s.addrs) - 1,
	}
	threshold := s.config.PerAddressImpactThreshold
	for i := 1; i < len(s.addrs); i++ {
		load := s.addrs[i].load
		stats.MaxLoad = max(stats.MaxLoad, load)
		if threshold != 0 && load > threshold {
			stats.OverloadedAddresses++
		}
	}
	return stats
}

// addressLoad returns the load of addr in hits per second, 0 if unknown.
func (s *Scheduler) addressLoad(addr netip.Addr) float64 {
	if idx, ok := s.addrIndex[addr]; ok {
		return s.addrs[idx].load
	}
	return 0
}

// nextInterval returns the time in nanoseconds until the PD is due again
// after an issuance now: P · max(1, M/Λ), where M is the larger load of its
// predicted addresses, or P while responsible probing is disabled.
func (s *Scheduler) nextInterval(pdID uint32) int64 {
	if uint64(pdID) >= uint64(len(s.pdStates)) {
		return int64(s.config.StartingPeriod)
	}
	st := &s.pdStates[pdID]
	threshold := s.config.PerAddressImpactThreshold
	if threshold == 0 {
		return st.period
	}
	m := max(s.addrs[st.near].load, s.addrs[st.far].load)
	if m <= threshold {
		return st.period
	}
	return int64(float64(st.period) * m / threshold)
}

// Issuer issues the PDs of one agent. Issue must be called from one goroutine;
// Close is safe to call from any goroutine.
type Issuer struct {
	scheduler *Scheduler
	agentID   string
	issuances chan issuance
	closed    chan struct{}
	closeOnce sync.Once
}

// Issue blocks until the agent's next PD is due and returns it. It returns an
// error if ctx ends, the issuer is closed or the scheduler stops; the issuer
// must then be closed and not used again.
func (i *Issuer) Issue(ctx context.Context) (*PD, error) {
	if err := i.scheduler.send(event{kind: eventIssue, issuer: i}); err != nil {
		return nil, err
	}

	var next issuance
	var timer <-chan time.Time
	for {
		select {
		case next = <-i.issuances:
			wait := time.Duration(next.due - i.scheduler.now())
			if wait <= 0 {
				return &next.pd, nil
			}
			t := time.NewTimer(wait)
			defer t.Stop()
			timer = t.C
		case <-timer:
			return &next.pd, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-i.closed:
			return nil, ErrIssuerClosed
		case <-i.scheduler.stopped:
			return nil, ErrSchedulerStopped
		}
	}
}

// Close detaches the issuer from its agent and unblocks Issue. The agent's PDs
// stay in the schedule and keep their times for the next issuer. Calling Close
// more than once is a no-op.
func (i *Issuer) Close() error {
	i.closeOnce.Do(func() {
		close(i.closed)
		_ = i.scheduler.send(event{kind: eventDisconnect, issuer: i})
	})
	return nil
}

// heapEntry schedules one PD of a node.
type heapEntry struct {
	// due is in nanoseconds since the scheduler's epoch.
	due int64
	// index is the position of the PD in the node's pds.
	index     int
	issuances uint64
}

// pdHeap is a min-heap of entries ordered by due time.
type pdHeap []heapEntry

func (h *pdHeap) push(entry heapEntry) {
	*h = append(*h, entry)
	heap := *h
	for i := len(heap) - 1; i > 0; {
		parent := (i - 1) / 2
		if heap[parent].due <= heap[i].due {
			break
		}
		heap[parent], heap[i] = heap[i], heap[parent]
		i = parent
	}
}

// reschedule gives the root a new due time.
func (h pdHeap) reschedule(due int64) {
	h[0].due = due
	h.siftDown()
}

func (h *pdHeap) removeRoot() {
	heap := *h
	last := len(heap) - 1
	heap[0] = heap[last]
	*h = heap[:last]
	h.siftDown()
}

func (h pdHeap) siftDown() {
	for i := 0; ; {
		least := i
		if left := 2*i + 1; left < len(h) && h[left].due < h[least].due {
			least = left
		}
		if right := 2*i + 2; right < len(h) && h[right].due < h[least].due {
			least = right
		}
		if least == i {
			return
		}
		h[i], h[least] = h[least], h[i]
		i = least
	}
}
