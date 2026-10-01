// Copyright (c) 2026 Sorbonne Université
// SPDX-License-Identifier: MIT

package retina

import (
	"context"
	"errors"
	"fmt"
	"math"
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
}

func (c *SchedulerConfig) validate() error {
	if c.StartingPeriod <= 0 {
		return fmt.Errorf("starting period must be positive: got %v", c.StartingPeriod)
	}
	if c.EventQueueSize < 1 {
		return fmt.Errorf("event queue size must be at least 1: got %d", c.EventQueueSize)
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
)

// event is a request to the scheduler goroutine.
type event struct {
	kind   eventKind
	pds    []PD         // eventInsert
	fie    *FIE         // eventUpdate
	issuer *Issuer      // eventConnect, eventDisconnect, eventIssue
	reply  chan<- error // eventConnect
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
}

// NewScheduler creates an empty scheduler from a valid configuration.
func NewScheduler(config *SchedulerConfig) *Scheduler {
	return &Scheduler{
		config:  config,
		epoch:   time.Now(),
		events:  make(chan event, config.EventQueueSize),
		stopped: make(chan struct{}),
		nodes:   make(map[string]*agentNode),
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
			pd.AgentID = node.agentID
			node.pds = append(node.pds, pd)
			node.heap.push(heapEntry{due: due, index: len(node.pds) - 1})
			if node.waiting {
				node.waiting = false
				s.issue(node)
			}
		}

	case eventUpdate:
		// Nothing uses FIEs yet.

	case eventConnect:
		node := s.node(ev.issuer.agentID)
		if node.issuer != nil {
			ev.reply <- fmt.Errorf("agent %q already has an issuer", ev.issuer.agentID)
			return
		}
		node.issuer = ev.issuer
		ev.reply <- nil

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
	} else {
		node.heap.reschedule(max(next.due, s.now()) + int64(s.config.StartingPeriod))
	}
	node.issuer.issuances <- next
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
