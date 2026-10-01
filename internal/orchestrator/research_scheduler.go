package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"net"
	"sync/atomic"
	"time"

	api "github.com/dioptra-io/retina-commons/api/v1"
)

type ResearchSchedulerConfig struct {
	StartingPeriod   time.Duration `json:"starting_period"`
	MaxIssuanceCount uint64        `json:"max_issuance_count"`
	MaxEventsPerPass int           `json:"max_events_per_pass"`
	EventChannelSize int           `json:"event_channel_size"`
}

func (c *ResearchSchedulerConfig) validate() error {
	if c.StartingPeriod <= 0 {
		return fmt.Errorf("starting period must be positive: %v", c.StartingPeriod)
	}
	if c.MaxEventsPerPass <= 0 {
		return fmt.Errorf("max events per pass must be positive: %v", c.MaxEventsPerPass)
	}
	if c.EventChannelSize < 0 {
		return fmt.Errorf("event channel size cannot be negative: %v", c.EventChannelSize)
	}
	return nil
}

type schedulerRecord struct {
	agent       *agentHeapNode
	pd          *api.ProbingDirective
	period      int64
	issuances   uint64
	lastCapture time.Time
	lastNear    net.IP
	lastFar     net.IP
}

type schedulerEventKind uint8

const (
	schedulerEventInsert schedulerEventKind = iota
	schedulerEventUpdate
	schedulerEventAgent
)

type schedulerEvent struct {
	kind        schedulerEventKind
	agentID     string
	pdid        uint64
	record      *schedulerRecord
	captureTime time.Time
	near, far   net.IP
	exclude     bool
}

// ResearchScheduler uses one issuance heap per agent. Insert, Update, Agent,
// and Close may be called concurrently; Next must be called by one goroutine.
type ResearchScheduler struct {
	cfg      *ResearchSchedulerConfig
	logger   *slog.Logger
	epoch    time.Time
	nextID   atomic.Uint64
	eventCh  chan schedulerEvent
	ctx      context.Context
	cancel   context.CancelFunc
	records  []*schedulerRecord
	agents   map[string]*agentHeapNode
	included *agentHeapNode
	excluded *agentHeapNode
	timer    *time.Timer
}

var _ Scheduler = (*ResearchScheduler)(nil)

func NewResearchScheduler(config *ResearchSchedulerConfig, logger *slog.Logger, _ *EventBus) (*ResearchScheduler, error) {
	if config == nil {
		return nil, fmt.Errorf("cannot create research scheduler because given config is nil")
	}
	if err := config.validate(); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	s := &ResearchScheduler{
		cfg: config, logger: logger, epoch: time.Now(),
		eventCh: make(chan schedulerEvent, config.EventChannelSize),
		ctx:     ctx, cancel: cancel, agents: make(map[string]*agentHeapNode), timer: timer,
	}
	logger.Info("Research scheduler initialized",
		slog.Duration("starting_period", config.StartingPeriod),
		slog.Uint64("max_issuance_count", config.MaxIssuanceCount),
		slog.Int("max_events_per_pass", config.MaxEventsPerPass),
		slog.Int("event_channel_size", config.EventChannelSize))
	return s, nil
}

func (s *ResearchScheduler) Insert(pd *api.ProbingDirective) (uint64, error) {
	if pd == nil {
		return 0, fmt.Errorf("probing directive cannot be nil")
	}
	if pd.AgentID == "" {
		return 0, fmt.Errorf("agent id cannot be empty")
	}
	if pd.DestinationAddress.To16() == nil {
		return 0, fmt.Errorf("invalid destination address: %v", pd.DestinationAddress)
	}
	id := s.nextID.Add(1) - 1
	pdCopy := *pd
	pdCopy.ProbingDirectiveID = id
	err := s.send(schedulerEvent{kind: schedulerEventInsert, agentID: pdCopy.AgentID, pdid: id,
		record: &schedulerRecord{pd: &pdCopy, period: int64(s.cfg.StartingPeriod)}})
	if err != nil {
		return 0, err
	}
	pd.ProbingDirectiveID = id
	return id, nil
}

func (s *ResearchScheduler) Next() (*api.ProbingDirective, error) {
	for {
		saturated, err := s.process()
		if err != nil {
			return nil, err
		}
		var next *agentHeapNode
		at := int64(math.MaxInt64)
		for node := s.included; node != nil; node = node.next {
			if len(node.heap) != 0 && node.heap[0].at < at {
				next, at = node, node.heap[0].at
			}
		}
		now := s.now()
		if next != nil && at <= now {
			pdid := next.heap[0].pdid
			record := s.records[pdid]
			record.issuances++
			if s.cfg.MaxIssuanceCount != 0 && record.issuances == s.cfg.MaxIssuanceCount {
				next.heap.removeRoot()
			} else {
				next.heap.reschedule(following(at, now, record.period))
			}
			return record.pd, nil
		}
		if saturated {
			continue
		}
		if err := s.sleep(next != nil, time.Duration(at-now)); err != nil {
			return nil, err
		}
	}
}

func (s *ResearchScheduler) Update(fie *api.ForwardingInfoElement) error {
	if fie == nil {
		return fmt.Errorf("forwarding info element cannot be nil")
	}
	if fie.ProbingDirectiveID >= s.nextID.Load() {
		return fmt.Errorf("unknown pdid: %d", fie.ProbingDirectiveID)
	}
	var captureTime time.Time
	var near, far net.IP
	if fie.NearInfo != nil {
		captureTime, near = fie.NearInfo.ReceivedTimestamp, fie.NearInfo.ReplyAddress
	}
	if fie.FarInfo != nil {
		if captureTime.IsZero() || fie.FarInfo.ReceivedTimestamp.After(captureTime) {
			captureTime = fie.FarInfo.ReceivedTimestamp
		}
		far = fie.FarInfo.ReplyAddress
	}
	return s.send(schedulerEvent{kind: schedulerEventUpdate, pdid: fie.ProbingDirectiveID,
		captureTime: captureTime, near: near, far: far})
}

// Agent excludes an agent from issuance, or includes it again. Existing PDs
// retain their schedule while their agent is excluded.
func (s *ResearchScheduler) Agent(agentID string, exclude bool) error {
	if agentID == "" {
		return fmt.Errorf("agent id cannot be empty")
	}
	return s.send(schedulerEvent{kind: schedulerEventAgent, agentID: agentID, exclude: exclude})
}

func (s *ResearchScheduler) Close() error { s.cancel(); return nil }
func (s *ResearchScheduler) now() int64   { return int64(time.Since(s.epoch)) }

func (s *ResearchScheduler) send(event schedulerEvent) error { //nolint:gocritic // events are intentionally channel values
	select {
	case s.eventCh <- event:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *ResearchScheduler) process() (bool, error) {
	for range s.cfg.MaxEventsPerPass {
		select {
		case event := <-s.eventCh:
			s.apply(event)
		case <-s.ctx.Done():
			return false, s.ctx.Err()
		default:
			return false, nil
		}
	}
	return true, nil
}

func (s *ResearchScheduler) sleep(timed bool, duration time.Duration) error {
	var wake <-chan time.Time
	if timed {
		s.timer.Reset(duration)
		defer s.timer.Stop()
		wake = s.timer.C
	}
	select {
	case <-wake:
		return nil
	case event := <-s.eventCh:
		s.apply(event)
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *ResearchScheduler) apply(event schedulerEvent) { //nolint:gocritic // events are intentionally channel values
	switch event.kind {
	case schedulerEventInsert:
		s.insert(event)
	case schedulerEventUpdate:
		s.update(event)
	case schedulerEventAgent:
		s.setAgent(event.agentID, event.exclude)
	}
}

func (s *ResearchScheduler) agentFor(agentID string, exclude bool) (*agentHeapNode, bool) {
	node, ok := s.agents[agentID]
	if ok {
		return node, false
	}
	node = &agentHeapNode{agentID: agentID}
	s.agents[agentID] = node
	if exclude {
		pushAgent(&s.excluded, node)
	} else {
		pushAgent(&s.included, node)
	}
	return node, true
}

func (s *ResearchScheduler) setAgent(agentID string, exclude bool) {
	node, created := s.agentFor(agentID, exclude)
	if created {
		return
	}
	if exclude {
		if popAgent(&s.included, node) {
			pushAgent(&s.excluded, node)
		}
	} else if popAgent(&s.excluded, node) {
		pushAgent(&s.included, node)
	}
}

func (s *ResearchScheduler) insert(event schedulerEvent) { //nolint:gocritic // events are intentionally channel values
	for uint64(len(s.records)) <= event.pdid {
		s.records = append(s.records, nil)
	}
	s.records[event.pdid] = event.record
	event.record.agent, _ = s.agentFor(event.agentID, false)
	event.record.agent.heap.push(scheduledEntry{at: s.now(), pdid: event.pdid})
}

func (s *ResearchScheduler) update(event schedulerEvent) { //nolint:gocritic // events are intentionally channel values
	if event.pdid >= uint64(len(s.records)) || s.records[event.pdid] == nil {
		return
	}
	record := s.records[event.pdid]
	record.lastCapture, record.lastNear, record.lastFar = event.captureTime, event.near, event.far
}

func following(at, now, period int64) int64 {
	next := at + period
	if next <= now {
		next += ((now-next)/period + 1) * period
	}
	return next
}
