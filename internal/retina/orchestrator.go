// Copyright (c) 2026 Sorbonne Université
// SPDX-License-Identifier: MIT

// Package retina implements the Retina orchestrator.
package retina

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"time"

	"github.com/dioptra-io/retina-commons/api/v1"

	"golang.org/x/sync/errgroup"
)

// Config is the configuration of the orchestrator.
type Config struct {
	Agent     AgentConfig     `json:"agent"`
	API       APIConfig       `json:"api"`
	Scheduler SchedulerConfig `json:"scheduler"`

	// Capturer configures the DuckDB files that received FIEs are written to.
	Capturer DDBFIECapturerConfig `json:"capturer"`
	// CaptureQueueSize is how many received FIEs may wait to be captured.
	// Agents are slowed down, not dropped, while the queue is full.
	CaptureQueueSize int `json:"capture_queue_size"`
	// CaptureFlushPeriod is how often captured FIEs are flushed to disk.
	CaptureFlushPeriod time.Duration `json:"capture_flush_period"`
}

// Validate reports whether the configuration is usable.
func (c *Config) Validate() error {
	if err := c.Scheduler.validate(); err != nil {
		return fmt.Errorf("scheduler: %w", err)
	}
	if c.CaptureQueueSize < 0 {
		return fmt.Errorf("capture queue size cannot be negative: got %d", c.CaptureQueueSize)
	}
	if c.CaptureFlushPeriod <= 0 {
		return fmt.Errorf("capture flush period must be positive: got %v", c.CaptureFlushPeriod)
	}
	return nil
}

// Orchestrator coordinates agents and measurements.
type Orchestrator struct {
	config    *Config
	logger    *slog.Logger
	scheduler *Scheduler
	capturer  FIECapturer
	// fies carries the FIEs received from all agents to the capturer.
	fies chan *FIE
}

// NewOrchestrator creates an orchestrator from the given configuration.
func NewOrchestrator(config *Config, logger *slog.Logger) (*Orchestrator, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	if logger == nil {
		logger = slog.Default()
	}

	capturer, err := NewDDBFIECapturer(&config.Capturer)
	if err != nil {
		return nil, err
	}

	return &Orchestrator{
		config:    config,
		logger:    logger,
		scheduler: NewScheduler(&config.Scheduler),
		capturer:  capturer,
		fies:      make(chan *FIE, config.CaptureQueueSize),
	}, nil
}

// Run runs the orchestrator until ctx is done or a component fails. It returns
// nil on a clean shutdown.
func (o *Orchestrator) Run(ctx context.Context) error {
	listener, err := ListenAgents(&o.config.Agent)
	if err != nil {
		return err
	}
	o.logger.Info("Listening for agents", slog.String("addr", listener.Addr().String()))

	apiListener, err := net.Listen("tcp", o.config.API.Address)
	if err != nil {
		_ = listener.Close()
		return fmt.Errorf("cannot listen for API requests: %w", err)
	}
	o.logger.Info("Listening for API requests", slog.String("addr", apiListener.Addr().String()))
	apiServer := &http.Server{
		Handler:           o.apiHandler(),
		ReadHeaderTimeout: o.config.API.ReadHeaderTimeout,
	}

	group, ctx := errgroup.WithContext(ctx)

	group.Go(func() error {
		<-ctx.Done()
		o.logger.Info("Shutting down")
		return errors.Join(listener.Close(), apiServer.Close())
	})

	group.Go(func() error {
		if err := apiServer.Serve(apiListener); !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("API server failed: %w", err)
		}
		return nil
	})

	group.Go(func() error {
		return o.scheduler.Run(ctx)
	})

	group.Go(func() error {
		return o.runCapturer(ctx)
	})

	group.Go(func() error {
		for {
			conn, err := listener.Accept()
			if ctx.Err() != nil {
				return nil
			}
			if err != nil {
				return fmt.Errorf("cannot accept agent connection: %w", err)
			}
			group.Go(func() error {
				o.serveAgent(ctx, conn)
				return nil
			})
		}
	})

	return group.Wait()
}

// serveAgent handles one agent connection until it fails or ctx is done. A
// failing agent never stops the orchestrator.
func (o *Orchestrator) serveAgent(ctx context.Context, conn *AgentConn) {
	defer func() { _ = conn.Close() }()

	logger := o.logger.With(slog.String("remote_addr", conn.RemoteAddr().String()))

	// Closing the connection is what unblocks the calls on it at shutdown.
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	agentID, err := conn.Handshake()
	if err != nil {
		logger.Warn("Agent handshake failed", slog.Any("err", err))
		return
	}
	logger = logger.With(slog.String("agent_id", agentID))

	// The issuer is also what makes an agent ID unique among connections.
	issuer, err := o.scheduler.NewIssuer(agentID)
	if err != nil {
		logger.Warn("Agent rejected", slog.Any("err", err))
		return
	}
	logger.Info("Agent connected")

	// The sender issues this agent's PDs and sends them. A slow agent only
	// delays its own schedule.
	senderDone := make(chan struct{})
	go func() {
		defer close(senderDone)
		for {
			pd, err := issuer.Issue(ctx)
			if err != nil {
				return
			}
			if err := conn.SendPD(pd); err != nil {
				if ctx.Err() == nil {
					logger.Warn("Cannot send PD to agent", slog.Any("err", err))
				}
				_ = conn.Close()
				return
			}
		}
	}()

	for {
		fie, err := conn.ReceiveFIE()
		if err != nil {
			if ctx.Err() == nil {
				logger.Warn("Agent connection failed", slog.Any("err", err))
			}
			break
		}
		if err := o.scheduler.Update(fie); err != nil {
			logger.Debug("Cannot update scheduler", slog.Uint64("pd_id", uint64(fie.PDID)), slog.Any("err", err))
		}
		// A full queue blocks here, which slows the agent down.
		select {
		case o.fies <- fie:
		case <-ctx.Done():
		}
	}

	_ = conn.Close()
	_ = issuer.Close()
	<-senderDone
	logger.Info("Agent disconnected")
}

// runCapturer writes the received FIEs to the capturer until ctx is done, then
// writes what is still queued and closes the capturer. A capturer failure
// stops the orchestrator.
func (o *Orchestrator) runCapturer(ctx context.Context) (err error) {
	defer func() {
		err = errors.Join(err, o.capturer.Flush(), o.capturer.Close())
	}()

	ticker := time.NewTicker(o.config.CaptureFlushPeriod)
	defer ticker.Stop()

	for {
		select {
		case fie := <-o.fies:
			if err := o.capture(fie); err != nil {
				return err
			}
		case <-ticker.C:
			if err := o.capturer.Flush(); err != nil {
				return fmt.Errorf("cannot flush captured FIEs: %w", err)
			}
		case <-ctx.Done():
			for {
				select {
				case fie := <-o.fies:
					if err := o.capture(fie); err != nil {
						return err
					}
				default:
					return nil
				}
			}
		}
	}
}

// capture writes one FIE to the capturer.
func (o *Orchestrator) capture(fie *FIE) error {
	// Not the orchestrator's context: queued FIEs are still written at shutdown.
	if err := o.capturer.Capture(context.Background(), expandFIE(fie)); err != nil {
		return fmt.Errorf("cannot capture FIE: %w", err)
	}
	return nil
}

// expandFIE converts a FIE into the public API form the capturer takes. A FIE
// carries no sent timestamps, so they are set to the received ones.
func expandFIE(fie *FIE) *api.ForwardingInfoElement {
	capture := time.Unix(fie.CaptureUnix, 0).UTC()
	info := func(reply netip.Addr, delta uint32) *api.Info {
		if !reply.IsValid() {
			return nil
		}
		received := capture.Add(-time.Duration(delta) * time.Second)
		return &api.Info{ReplyAddress: reply.AsSlice(), SentTimestamp: received, ReceivedTimestamp: received}
	}
	return &api.ForwardingInfoElement{
		Agent:               api.Agent{AgentID: fie.AgentID},
		ProbingDirectiveID:  fie.PDID,
		NearInfo:            info(fie.Near, fie.NearDelta),
		FarInfo:             info(fie.Far, fie.FarDelta),
		ProductionTimestamp: capture,
	}
}
