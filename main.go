// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dioptra-io/retina-orchestrator/internal/retina"
)

func main() {
	if err := run(); err != nil {
		slog.Error("Orchestrator error", slog.Any("err", err))
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	config := &retina.Config{}
	flag.StringVar(&config.Agent.Address, "agent-addr", "localhost:50050", "Listening address for agent connections")
	flag.DurationVar(&config.Agent.HandshakeTimeout, "agent-handshake-timeout", 5*time.Second, "Time an agent has to complete the handshake (0 for no limit)")
	flag.DurationVar(&config.Agent.KeepAliveIdle, "agent-keepalive-idle", 30*time.Second, "Idle time before TCP keepalive probes are sent on an agent connection")
	flag.DurationVar(&config.Agent.KeepAliveInterval, "agent-keepalive-interval", 10*time.Second, "Time between TCP keepalive probes on an agent connection")
	flag.IntVar(&config.Agent.KeepAliveCount, "agent-keepalive-count", 3, "Unanswered TCP keepalive probes before an agent connection is closed")

	flag.StringVar(&config.API.Address, "api-addr", "localhost:8080", "Listening address for the HTTP API")
	flag.DurationVar(&config.API.ReadHeaderTimeout, "api-read-header-timeout", 5*time.Second, "Timeout for reading HTTP request headers (0 for no limit)")

	flag.DurationVar(&config.Scheduler.StartingPeriod, "scheduler-starting-period", 10*time.Second, "Issuance period of every new PD, and the delay before its first issuance")
	flag.Uint64Var(&config.Scheduler.MaxIssuanceCount, "scheduler-max-issuance-count", 0, "Number of times each PD is issued before it leaves the schedule (0 for indefinitely)")
	flag.IntVar(&config.Scheduler.EventQueueSize, "scheduler-event-queue-size", 1024, "Size of the scheduler event queue (at least 1)")

	flag.StringVar(&config.Capturer.CaptureDir, "capturer-capture-dir", "./capture", "Directory where the DuckDB FIE capture files are written")
	flag.BoolVar(&config.Capturer.AllowNonEmptyCaptureDir, "capturer-allow-non-empty-capture-dir", false, "Allow capturing into a directory that already has files")
	flag.DurationVar(&config.Capturer.RotationInterval, "capturer-rotation-interval", time.Hour, "Time span covered by one capture file (at most 18h)")
	flag.IntVar(&config.Capturer.BatchSize, "capturer-batch-size", 100_000, "Number of FIEs appended before the capture file is flushed")
	flag.IntVar(&config.CaptureQueueSize, "capturer-queue-size", 200_000, "Number of received FIEs that may wait to be captured")
	flag.DurationVar(&config.CaptureFlushPeriod, "capturer-flush-period", time.Second, "Interval between periodic flushes of the capture file")
	flag.Parse()

	// The secret is read from the environment only, so that it does not show
	// up in the process list.
	config.Agent.Secret = os.Getenv("RETINA_SECRET")

	stop := context.AfterFunc(ctx, func() { logger.Info("Shut down signal detected") })
	defer stop()

	orch, err := retina.NewOrchestrator(config, logger)
	if err != nil {
		return err
	}

	return orch.Run(ctx)
}
