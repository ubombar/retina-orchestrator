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
	"time"

	"golang.org/x/sync/errgroup"
)

// Config is the configuration of the orchestrator.
type Config struct {
	Agent     AgentConfig     `json:"agent"`
	API       APIConfig       `json:"api"`
	Scheduler SchedulerConfig `json:"scheduler"`

	// Capturer configures the fies2a files that received FIEs are written to.
	Capturer CapturerConfig `json:"capturer"`
	// CaptureQueueSize is how many received FIEs may wait to be captured.
	// Agents are slowed down, not dropped, while the queue is full.
	CaptureQueueSize int `json:"capture_queue_size"`
	// CaptureFlushPeriod is how often captured FIEs are flushed to disk.
	CaptureFlushPeriod time.Duration `json:"capture_flush_period"`
}

// Validate reports whether the configuration is usable.
func (c *Config) Validate() error {
	if err := c.Agent.validate(); err != nil {
		return fmt.Errorf("agent: %w", err)
	}
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
	capturer  *Capturer
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

	capturer, err := NewCapturer(&config.Capturer, logger)
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

	stopFlusher := make(chan struct{})
	flusherDone := make(chan struct{})
	go func() {
		defer close(flusherDone)
		o.flushAgent(ctx, conn, logger, stopFlusher)
	}()

	// The sender issues this agent's PDs and buffers them for the flusher. A
	// slow agent only delays its own schedule.
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
	close(stopFlusher)
	<-senderDone
	<-flusherDone
	logger.Info("Agent disconnected")
}

// flushAgent sends the PDs buffered by the sender every flush period, so that
// PDs are written to the agent in groups, not one by one. It returns when stop
// is closed or the connection fails.
func (o *Orchestrator) flushAgent(ctx context.Context, conn *AgentConn, logger *slog.Logger, stop <-chan struct{}) {
	ticker := time.NewTicker(o.config.Agent.FlushPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := conn.Flush(); err != nil {
				if ctx.Err() == nil {
					logger.Warn("Cannot send PDs to agent", slog.Any("err", err))
				}
				_ = conn.Close()
				return
			}
		case <-stop:
			return
		}
	}
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
	if err := o.capturer.Capture(fie); err != nil {
		return fmt.Errorf("cannot capture FIE: %w", err)
	}
	return nil
}
