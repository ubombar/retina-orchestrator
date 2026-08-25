// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT

// Package orchestrator implements the Retina orchestrator, which schedules
// ProbingDirectives (PDs) to connected agents and streams the resulting
// ForwardingInfoElements to HTTP clients.
package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/dioptra-io/retina-commons/api/v1"
	"github.com/dioptra-io/retina-orchestrator/internal/orchestrator/structures"
	"golang.org/x/sync/errgroup"
)

// Config is the main configuration struct used in the orchestrator.
type Config struct {
	// AgentAddress is the TCP listening address for agent connections, in the form "host:port".
	AgentAddress string `json:"agent_address"`

	AgentBufferLength int `json:"agent_buffer_length"`

	// PDQueueSize is the number of PDs that can be queued per agent.
	// Increase this value if agents are slow to consume directives.
	PDQueueSize int `json:"pd_queue_size"`

	RingBufferSize int `json:"ring_buffer_size"`

	// APIAddress is the TCP listening address for the HTTP API server, in the form "host:port".
	APIAddress string `json:"api_address"`

	// APIReadHeaderTimeout defaults to 5 seconds if zero.
	APIReadHeaderTimeout time.Duration `json:"api_read_header_timeout"`

	FIEFilterPolicy string `json:"fie_filter_policy"`

	// Secret is the shared secret for agent authentication.
	// This is an MVS feature and will be removed soon.
	Secret string `json:"secret"`

	EventBusSize int `json:"event_bus_size"`
	// EventsDir is the directory where orchestrator events are persisted as JSONL.
	// If empty, event persistence is disabled.
	EventsDir string `json:"events_dir"`

	// StreamStartFromEarliest controls where a newly connected client's stream
	// begins. False (default) preserves the original behavior: the client only
	// sees FIEs sent after it connects. True starts the client from the
	// earliest FIE still held in the ring buffer.
	StreamStartFromEarliest bool `json:"stream_start_from_earliest"`

	ResearchSchedulerConfig *ResearchSchedulerConfig `json:"research_scheduler_config"`

	CapturerConfig      *DDBFIECapturerConfig `json:"capturer_config"`
	CaptureChannelSize  int                   `json:"capture_channel_size"`
	CapturerFlushPeriod time.Duration         `json:"capturer_flush_period"`
}

// Validate checks all configuration fields and applies defaults where appropriate.
// Returns an error if any required field is missing or invalid.
func (c *Config) Validate() error {
	if c.AgentAddress == "" {
		return fmt.Errorf("AgentAddress cannot be empty")
	}
	if c.CaptureChannelSize < 0 {
		return fmt.Errorf("CaptureChannelSize cannot be negative: got %d", c.CaptureChannelSize)
	}
	if c.CapturerFlushPeriod < time.Second {
		return fmt.Errorf("CapturerFlushPeriod cannot be smaller than a second: got %d", c.CapturerFlushPeriod)
	}
	if c.AgentBufferLength < 8192 {
		return fmt.Errorf("AgentBufferLength is too small: got %d, minimum 8192", c.AgentBufferLength)
	}
	if c.PDQueueSize <= 0 {
		return fmt.Errorf("PDQueueSize must be greater than zero: got %d", c.PDQueueSize)
	}
	if c.RingBufferSize <= 0 {
		return fmt.Errorf("RingBufferSize must be greater than zero: got %d", c.RingBufferSize)
	}
	if c.APIAddress == "" {
		return fmt.Errorf("APIAddress cannot be empty")
	}
	if c.APIReadHeaderTimeout == 0 {
		c.APIReadHeaderTimeout = 5 * time.Second
	}
	if c.FIEFilterPolicy == "" {
		c.FIEFilterPolicy = "both"
	}
	if !slices.Contains([]string{"any", "one", "both"}, c.FIEFilterPolicy) {
		return fmt.Errorf("supported FIE filtering policies are 'any', 'one', or 'both' got %s", c.FIEFilterPolicy)
	}
	return nil
}

type Orchestrator struct {
	config        *Config
	logger        *slog.Logger
	metrics       *Metrics
	scheduler     Scheduler
	agentServer   *agentServer
	apiServer     *apiServer
	pdQueue       *structures.Queue[api.ProbingDirective]
	fieRingBuffer *structures.RingBuffer[api.ForwardingInfoElement]
	ebus          *EventBus
	capturer      FIECapturer
	captureCh     chan *api.ForwardingInfoElement
}

// NewOrchestrator creates a new orchestrator from the given configuration.
// Returns an error if the configuration is invalid or any component creation
// fails.
//
//nolint:funlen
func NewOrchestrator(config *Config, logger *slog.Logger, metrics *Metrics) (*Orchestrator, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	if logger == nil {
		logger = slog.Default()
	}
	if metrics == nil {
		return nil, fmt.Errorf("metrics cannot be nil")
	}

	o := &Orchestrator{
		config:  config,
		logger:  logger,
		metrics: metrics,
	}

	agentServer, err := newAgentServer(&agentServerConfig{
		bufferLength:     config.AgentBufferLength,
		handshakeTimeout: 5 * time.Second,
		address:          config.AgentAddress,
		agentHandler:     o.agentHandler,
		authHandler:      o.agentAuthHandler,
	}, logger, metrics)
	if err != nil {
		return nil, fmt.Errorf("error on creating agent server: %w", err)
	}
	o.agentServer = agentServer

	pdQueue, err := structures.NewQueue[api.ProbingDirective](config.PDQueueSize)
	if err != nil {
		return nil, fmt.Errorf("error on creating pd queue: %w", err)
	}
	o.pdQueue = pdQueue

	var ringBuffer *structures.RingBuffer[api.ForwardingInfoElement]
	if !config.StreamStartFromEarliest {
		ringBuffer, err = structures.NewRingBuffer[api.ForwardingInfoElement](config.RingBufferSize)
	} else {
		ringBuffer, err = structures.NewRingBufferTailFollower[api.ForwardingInfoElement](config.RingBufferSize)
	}
	if err != nil {
		return nil, fmt.Errorf("error on creating ring buffer: %w", err)
	}
	o.fieRingBuffer = ringBuffer

	if config.CapturerConfig != nil {
		capturer, err := NewDDBFIECapturer(config.CapturerConfig)
		if err != nil {
			return nil, err
		}
		o.capturer = capturer
		o.captureCh = make(chan *api.ForwardingInfoElement, config.CaptureChannelSize)
	}

	ebus, err := NewEventBus(config.EventBusSize, config.EventsDir, config.CapturerConfig.RotationInterval)
	if err != nil {
		return nil, fmt.Errorf("error on creating event bus: %w", err)
	}
	o.ebus = ebus

	sched, err := NewResearchScheduler(config.ResearchSchedulerConfig, logger, ebus)
	if err != nil {
		return nil, fmt.Errorf("error on creating research scheduler: %w", err)
	}
	o.scheduler = sched

	apiServer, err := newAPIServer(&apiServerConfig{
		address:           config.APIAddress,
		readHeaderTimeout: config.APIReadHeaderTimeout,
		fieHandler:        o.fieStreamHandler,
		sseHandler:        o.sseHandler,
		insertHandler:     o.insertHandler,
		insertAfterHanler: o.insertAfterHandler,
		logger:            logger,
	})
	if err != nil {
		return nil, fmt.Errorf("error on creating API server: %w", err)
	}
	o.apiServer = apiServer

	return o, nil
}

func (o *Orchestrator) Run(parentCtx context.Context) error {
	group, ctx := errgroup.WithContext(parentCtx)
	group.Go(func() error {
		return o.runAPIServer(ctx)
	})
	group.Go(func() error {
		return o.runAgentServer(ctx)
	})
	group.Go(func() error {
		return o.runScheduler(ctx)
	})
	group.Go(func() error {
		return o.runCapturer(ctx)
	})
	group.Go(func() error {
		<-ctx.Done()
		return o.scheduler.Close()
	})

	err := group.Wait()

	return err
}

//nolint:gocyclo
func (o *Orchestrator) runCapturer(ctx context.Context) error {
	if o.capturer == nil {
		<-ctx.Done()
		return ctx.Err()
	}

	defer func() { _ = o.capturer.Close() }()
	group, ctx := errgroup.WithContext(ctx)

	group.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()

			case fie, ok := <-o.captureCh:
				if !ok {
					return nil
				}

				if err := o.capturer.Capture(ctx, fie); err != nil {
					return err
				}
			}
		}
	})

	group.Go(func() error {
		ticker := time.NewTicker(o.config.CapturerFlushPeriod)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()

			case <-ticker.C:
				if err := o.capturer.Flush(); err != nil {
					return err
				}
			}
		}
	})

	err := group.Wait()

	// Flush anything remaining below the configured batch size.
	if flushErr := o.capturer.Flush(); flushErr != nil && (err == nil || errors.Is(err, ctx.Err())) {
		return flushErr
	}

	if err != nil && !errors.Is(err, ctx.Err()) {
		return err
	}

	return nil
}

func (o *Orchestrator) runScheduler(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		pd, err := o.scheduler.Next()
		if err != nil {
			return fmt.Errorf("cannot get the next PD: %w", err)
		}
		if pd == nil {
			continue
		}

		if err := o.pdQueue.TryPush(pd.AgentID, pd); err != nil {
			o.logger.Debug("PD dropped: no queue for agent",
				slog.String("agent_id", pd.AgentID),
				slog.Uint64("pd_id", pd.ProbingDirectiveID))
		} else {
			o.metrics.AgentQueueSize.WithLabelValues(pd.AgentID).Inc()
		}
	}
}

func (o *Orchestrator) runAPIServer(ctx context.Context) error {
	o.ebus.Emit(&OrchestratorStartedEvent{
		Config: *o.config,
	})

	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return o.apiServer.listenAndServe()
	})
	group.Go(func() error {
		<-ctx.Done()

		o.ebus.Emit(&OrchestratorStoppedEvent{
			Message: ctx.Err().Error(),
		})

		return o.apiServer.close(3 * time.Second)
	})
	if err := group.Wait(); err != nil && !errors.Is(err, ctx.Err()) {
		return err
	}
	return nil
}

func (o *Orchestrator) runAgentServer(ctx context.Context) error {
	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return o.agentServer.listenAndServe()
	})
	group.Go(func() error {
		<-ctx.Done()
		return o.agentServer.close(10 * time.Second)
	})
	if err := group.Wait(); err != nil && !errors.Is(err, ctx.Err()) && !errors.Is(err, ErrServerShutdown) {
		return err
	}
	return nil
}

func (o *Orchestrator) fieStreamHandler(s *fieClient) {
	var closeReason string
	consumer := o.fieRingBuffer.NewConsumer()
	o.metrics.StreamClientsConnected.Inc()
	o.metrics.StreamConnectionsTotal.Inc()
	defer func() {
		consumer.Close()
		o.metrics.StreamClientsConnected.Dec()
		o.metrics.StreamDisconnectionsTotal.WithLabelValues(closeReason).Inc()
		o.logger.Debug("FIE stream closed", slog.String("reason", closeReason))
	}()

	for {
		fie, seq, err := consumer.Pop(s.context())
		if err != nil {
			closeReason = "internal_error"
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				closeReason = "shutdown_or_disconnect"
			}
			return
		}
		seqFIE := &SequencedFIE{
			ForwardingInfoElement: *fie,
			SequenceNumber:        seq,
		}

		o.logger.Debug("Sending FIE to client",
			slog.Uint64("seq", seq),
			slog.Uint64("pd_id", fie.ProbingDirectiveID))
		if err = s.sendFIE(seqFIE); err != nil {
			closeReason = "internal_error"
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				closeReason = "shutdown_or_disconnect"
			}
			return
		}
		o.metrics.FIEsStreamedTotal.Inc()
		o.metrics.StreamLagSeconds.Observe(time.Since(seqFIE.ProductionTimestamp).Seconds())
	}
}

func (o *Orchestrator) sseHandler(s *sseClient) {
	var closeReason string
	consumer := o.ebus.NewConsumer()
	defer func() {
		consumer.Close()
		o.logger.Debug("SSE stream closed", slog.String("reason", closeReason))
	}()

	nextSeq := uint64(0)
	for {
		eventEnvelope, seq, err := consumer.Pop(s.context())
		if err != nil {
			return
		}
		if nextSeq != seq {
			o.logger.Warn("One or more events are skipped on SSE handler",
				slog.Uint64("current_seq", seq),
				slog.Uint64("previous_seq", nextSeq))
		}
		nextSeq = seq + 1

		// Send the event to the sseClient.
		if err := s.sendEvent(eventEnvelope.RetinaEvent); err != nil {
			closeReason = "internal_error"
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				closeReason = "shutdown_or_disconnect"
			}
			return
		}
	}
}

func (o *Orchestrator) insertHandler(pd *api.ProbingDirective) (uint64, error) {
	return o.scheduler.Insert(pd)
}

func (o *Orchestrator) insertAfterHandler(pds []*api.ProbingDirective) {
	o.ebus.Emit(&PDBulkInsertionEvent{
		NumPDs: len(pds),
		PDs:    pds,
	})
}

//nolint:funlen,gocyclo
func (o *Orchestrator) agentHandler(status *agentAuthStatus, s *agentStream) {
	consumer, err := o.pdQueue.NewConsumer(status.agentID)
	if err != nil {
		o.logger.Warn("Agent already connected, rejecting", "agent_id", status.agentID)
		return
	}
	defer consumer.Close()

	o.logger.Info("Agent connected", "agent_id", status.agentID)
	o.metrics.AgentQueueSize.WithLabelValues(status.agentID).Set(0)
	o.ebus.Emit(&AgentConnectedEvent{
		AgentID:       status.agentID,
		RemoteAddress: status.remoteAddress.String(),
	})

	defer func() {
		o.logger.Info("Agent disconnected", "agent_id", status.agentID)
		o.metrics.AgentQueueSize.DeleteLabelValues(status.agentID)
		o.ebus.Emit(&AgentDisconnectedEvent{
			AgentID:       status.agentID,
			RemoteAddress: status.remoteAddress.String(),
		})
	}()

	group, ctx := errgroup.WithContext(s.context())

	group.Go(func() error {
		for {
			fie, err := s.receiveFIE()
			if err != nil {
				return err
			}
			o.metrics.FIEsReceivedTotal.WithLabelValues(status.agentID).Inc()

			o.logger.Debug("FIE received",
				slog.String("agent_id", status.agentID),
				slog.Uint64("pd_id", fie.ProbingDirectiveID),
				slog.Bool("complete", fie.NearInfo != nil && fie.FarInfo != nil))
			if err := o.scheduler.Update(fie); err != nil {
				o.logger.Error("Failed to update scheduler from FIE", "agent_id", status.agentID, "err", err)
			}

			allow, err := o.allowFIE(fie)
			if err != nil {
				return fmt.Errorf("error on filtering FIE: %w", err)
			}
			if !allow {
				continue
			}

			// Before pushing to the ring buffer capture the fie.
			if o.capturer != nil {
				if err := validateFIE(fie); err == nil {
					if err := o.capturer.Capture(ctx, fie); err != nil {
						return err
					}
				} else {
					o.logger.Warn("Invalid FIE, skipping capturing",
						slog.String("agent_id", status.agentID),
						slog.Uint64("pd_id", fie.ProbingDirectiveID),
						slog.Bool("complete", fie.NearInfo != nil && fie.FarInfo != nil),
						slog.String("error", err.Error()))
				}
			}
			_ = o.fieRingBuffer.Push(fie)
		}
	})

	group.Go(func() error {
		for {
			pd, err := consumer.Pop(ctx)
			if err != nil {
				return err
			}

			o.logger.Debug("Sending PD to agent",
				slog.String("agent_id", status.agentID),
				slog.Uint64("pd_id", pd.ProbingDirectiveID),
				slog.String("dest", pd.DestinationAddress.String()))
			if err = s.sendPD(pd); err != nil {
				return err
			}
			o.metrics.PDsSentTotal.WithLabelValues(status.agentID).Inc()
			o.metrics.AgentQueueSize.WithLabelValues(status.agentID).Dec()
		}
	})

	group.Go(func() error {
		<-ctx.Done()
		_ = s.conn.Close()
		return nil
	})

	if err := group.Wait(); err != nil && !errors.Is(err, ctx.Err()) {
		o.logger.Error("Agent stream failed", "agent_id", status.agentID, "err", err)
	}
}

func (o *Orchestrator) agentAuthHandler(auth api.AuthRequest) api.AuthResponse {
	if auth.Secret == o.config.Secret {
		return api.AuthResponse{
			Authenticated: true,
			Message:       "authenticated",
		}
	}
	o.logger.Warn("Agent authentication failed")
	return api.AuthResponse{
		Authenticated: false,
		Message:       "secret is not correct",
	}
}

// allowFIE reports whether a FIE should be streamed based on the policy.
// Returns true if the FIE is allowed.
func (o *Orchestrator) allowFIE(fie *api.ForwardingInfoElement) (bool, error) {
	switch o.config.FIEFilterPolicy {
	case "any": // allow all FIEs
		return true, nil
	case "both": // allow FIEs with two non-nil response addresses
		return fie.NearInfo != nil && fie.FarInfo != nil, nil
	case "one": // allow FIEs with at least one non-nil response address
		return fie.NearInfo != nil || fie.FarInfo != nil, nil
	default:
		return false, fmt.Errorf("unsupported fie filtering policy: %q", o.config.FIEFilterPolicy)
	}
}
