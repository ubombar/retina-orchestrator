// Copyright (c) 2026 Sorbonne Université
// SPDX-License-Identifier: MIT

package orchestrator

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/dioptra-io/retina-commons/api/v1"
	"github.com/dioptra-io/retina-orchestrator/internal/orchestrator/structures"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// blockingScheduler is a Scheduler whose Update blocks until release is
// closed, like the research scheduler does once its update channel is full.
type blockingScheduler struct {
	updating chan struct{}
	release  chan struct{}
}

func (s *blockingScheduler) Insert(*api.ProbingDirective) (uint64, error) { return 0, nil }
func (s *blockingScheduler) Next() (*api.ProbingDirective, error)         { return nil, nil }
func (s *blockingScheduler) Close() error                                 { return nil }
func (s *blockingScheduler) Update(*api.ForwardingInfoElement) error {
	select {
	case s.updating <- struct{}{}:
	default:
	}
	<-s.release
	return nil
}

func newDispatchTestOrchestrator(t *testing.T, pushTimeout time.Duration, sched Scheduler) *Orchestrator {
	t.Helper()

	pdQueue, err := structures.NewQueue[api.ProbingDirective](1)
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	ring, err := structures.NewRingBuffer[api.ForwardingInfoElement](8)
	if err != nil {
		t.Fatalf("NewRingBuffer: %v", err)
	}
	ebus, err := NewEventBus(64, "", time.Hour)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}

	return &Orchestrator{
		config:        &Config{FIEFilterPolicy: "any", PDPushTimeout: pushTimeout},
		logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		metrics:       NewMetrics(prometheus.NewRegistry()),
		scheduler:     sched,
		pdQueue:       pdQueue,
		fieRingBuffer: ring,
		ebus:          ebus,
	}
}

func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()

	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatalf("read counter: %v", err)
	}
	return m.GetCounter().GetValue()
}

// tcpPair returns both ends of a loopback TCP connection.
func tcpPair(t *testing.T) (server, client *net.TCPConn) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			close(accepted)
			return
		}
		accepted <- conn
	}()

	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	s, ok := <-accepted
	if !ok {
		t.Fatal("accept failed")
	}
	return s.(*net.TCPConn), c.(*net.TCPConn)
}

func TestDispatch_NotConnected(t *testing.T) {
	t.Parallel()
	o := newDispatchTestOrchestrator(t, time.Second, &blockingScheduler{})

	reason, err := o.dispatch(context.Background(), &api.ProbingDirective{AgentID: "nobody"})
	if err != nil || reason != pdDropNotConnected {
		t.Fatalf("dispatch = (%q, %v), want (%q, nil)", reason, err, pdDropNotConnected)
	}
}

func TestDispatch_TimeoutOnFullQueue(t *testing.T) {
	t.Parallel()
	o := newDispatchTestOrchestrator(t, 50*time.Millisecond, &blockingScheduler{})
	consumer, _ := o.pdQueue.NewConsumer("a")
	defer consumer.Close()

	pd := &api.ProbingDirective{AgentID: "a"}
	if reason, err := o.dispatch(context.Background(), pd); err != nil || reason != "" {
		t.Fatalf("first dispatch = (%q, %v), want queued", reason, err)
	}

	start := time.Now()
	reason, err := o.dispatch(context.Background(), pd)
	if err != nil || reason != pdDropTimeout {
		t.Fatalf("dispatch = (%q, %v), want (%q, nil)", reason, err, pdDropTimeout)
	}
	if waited := time.Since(start); waited < 50*time.Millisecond {
		t.Fatalf("dispatch returned after %s, before the push timeout", waited)
	}
	if blocked := counterValue(t, o.metrics.DispatchBlockedSeconds.WithLabelValues("a")); blocked <= 0 {
		t.Fatalf("blocked seconds = %v, want > 0", blocked)
	}
}

func TestDispatch_ContextCancelled(t *testing.T) {
	t.Parallel()
	o := newDispatchTestOrchestrator(t, 0, &blockingScheduler{})
	consumer, _ := o.pdQueue.NewConsumer("a")
	defer consumer.Close()

	pd := &api.ProbingDirective{AgentID: "a"}
	_, _ = o.dispatch(context.Background(), pd)

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(30*time.Millisecond, cancel)
	if reason, err := o.dispatch(ctx, pd); err == nil {
		t.Fatalf("dispatch = (%q, nil), want context error", reason)
	}
}

// Regression test: with the scheduler waiting for room in an agent's queue and
// that agent's receiver waiting for the scheduler in Update, a failing
// connection must still release the queue. Before the consumer was closed from
// the closer goroutine this was a deadlock, because the queue was only
// released after the receiver had returned.
//
//nolint:gocyclo
func TestAgentHandler_FailedConnectionUnblocksDispatch(t *testing.T) {
	t.Parallel()

	sched := &blockingScheduler{updating: make(chan struct{}, 1), release: make(chan struct{})}
	// No push timeout: only the disconnect can end the wait.
	o := newDispatchTestOrchestrator(t, 0, sched)

	serverConn, agentConn := tcpPair(t)
	defer func() { _ = agentConn.Close() }()

	stream, err := newAgentStream(1, serverConn, nil)
	if err != nil {
		t.Fatalf("newAgentStream: %v", err)
	}

	handlerDone := make(chan struct{})
	go func() {
		defer close(handlerDone)
		o.agentHandler(&agentAuthStatus{agentID: "a", remoteAddress: serverConn.RemoteAddr()}, stream)
	}()

	// One FIE parks the receiver goroutine inside scheduler.Update.
	if err := json.NewEncoder(agentConn).Encode(&api.ForwardingInfoElement{}); err != nil {
		t.Fatalf("send FIE: %v", err)
	}
	select {
	case <-sched.updating:
	case <-time.After(5 * time.Second):
		t.Fatal("receiver never reached scheduler.Update")
	}

	// The agent never reads, so PDs pile up in the socket buffers until the
	// sender blocks in write and the queue stays full.
	pd := &api.ProbingDirective{AgentID: "a"}
	deadline := time.Now().Add(4 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		err := o.pdQueue.Push(ctx, "a", pd)
		cancel()
		if err != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("queue never filled up")
		}
	}

	dispatched := make(chan string, 1)
	go func() {
		reason, _ := o.dispatch(context.Background(), pd)
		dispatched <- reason
	}()
	select {
	case reason := <-dispatched:
		t.Fatalf("dispatch returned %q while the queue was full", reason)
	case <-time.After(100 * time.Millisecond):
	}

	// Fail the connection while the receiver is still stuck in Update.
	stream.cancel()

	select {
	case reason := <-dispatched:
		if reason != pdDropDisconnected {
			t.Fatalf("dispatch = %q, want %q", reason, pdDropDisconnected)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("dispatch still blocked after the connection failed")
	}

	close(sched.release)
	select {
	case <-handlerDone:
	case <-time.After(3 * time.Second):
		t.Fatal("agentHandler did not return")
	}

	if dropped := counterValue(t, o.metrics.PDsDroppedTotal.WithLabelValues("a", pdDropDisconnected)); dropped < 1 {
		t.Fatalf("pds_dropped_total{disconnected} = %v, want the PD left in the queue to be counted", dropped)
	}
}
