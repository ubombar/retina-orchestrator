// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT

// @title			IP Routes Live API
// @version		1.0
// @description	Streams forwarding info elements from connected Retina agents.
// @host			iprl.dioptra.io
// @BasePath		/api/v1
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/dioptra-io/retina-orchestrator/internal/orchestrator"
)

func main() {
	if err := run(); err != nil {
		slog.Error("Orchestrator error", "err", err)
		os.Exit(1)
	}
}

//nolint:funlen
func run() error {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.VisitAll(func(f *flag.Flag) {
			fmt.Fprintf(os.Stderr, "  --%s\n", f.Name)
			fmt.Fprintf(os.Stderr, "    \t%s (default %v)\n", f.Usage, f.DefValue)
		})
	}

	var (
		apiAddr              = flag.String("api-addr", envOrDefault("RETINA_API_ADDR", "localhost:8080"), "Listening address for the HTTP API server")
		agentAddr            = flag.String("agent-addr", envOrDefault("RETINA_AGENT_ADDR", "localhost:50050"), "Listening address for agent connections")
		agentBufferLength    = flag.Int("agent-buffer-length", envOrDefaultInt("RETINA_AGENT_BUFFER_LENGTH", 8192), "Buffer length for per-agent PD channels")
		pdQueueSize          = flag.Int("pd-queue-size", envOrDefaultInt("RETINA_PD_QUEUE_SIZE", 100), "The size of the agent queue")
		ringBufferSize       = flag.Int("ring-buffer-size", envOrDefaultInt("RETINA_RING_BUFFER_SIZE", 100), "The size of the ring buffer")
		eventBusSize         = flag.Int("event-bus-size", envOrDefaultInt("RETINA_EVENT_BUS_SIZE", 1024*1024), "Size of the event bus")
		eventsDir            = flag.String("events-dir", envOrDefault("RETINA_EVENTS_DIR", ""), "Directory where orchestrator events are written as JSONL; empty disables event persistence")
		apiReadHeaderTimeout = flag.Duration("api-read-header-timeout", envOrDefaultDuration("RETINA_API_READ_HEADER_TIMEOUT", 5*time.Second), "Timeout for reading HTTP request headers")
		fieFilterPolicy      = flag.String("fie-filter-policy", envOrDefault("RETINA_FIE_FILTER_POLICY", "any"), "FIE filtering policy: any, one, or both")
		logLevel             = flag.String("log-level", envOrDefault("RETINA_LOG_LEVEL", "info"), "Log level (debug, info, warn, error)")
		metricsAddr          = flag.String("metrics-addr", envOrDefault("RETINA_METRICS_ADDR", ":9312"), "Address to expose Prometheus metrics on")

		streamStartFromEarliest = flag.Bool("stream-start-from-earliest", envOrDefaultBool("RETINA_STREAM_START_FROM_EARLIEST", true), "If true, newly connected FIE stream clients start from the earliest FIE still in the ring buffer instead of only future ones")

		// --- ResearchSchedulerConfig (rr- prefix) ---
		rrSeed                        = flag.Uint64("rr-seed", envOrDefaultUInt64("RETINA_RR_SEED", 42), "Seed for the research scheduler's internal RNG")
		rrLearningRate                = flag.Float64("rr-learning-rate", envOrDefaultFloat64("RETINA_RR_LEARNING_RATE", 0.1), "Learning rate (α) for the research scheduler's period adjustment")
		rrSamplingWidth               = flag.Float64("rr-sampling-width", envOrDefaultFloat64("RETINA_RR_SAMPLING_WIDTH", 0.1), "Sampling width (β) for uniform inter-issuance sampling")
		rrImpactThreshold             = flag.Float64("rr-impact-threshold", envOrDefaultFloat64("RETINA_RR_IMPACT_THRESHOLD", 1.0), "Impact threshold (Λ) for the research scheduler's responsible probing")
		rrFIEHistoryCapacity          = flag.Int("rr-fie-history-capacity", envOrDefaultInt("RETINA_RR_FIE_HISTORY_CAPACITY", 6), "Number of FIEs retained per PD for the staleness rule")
		rrMinIssuancePeriod           = flag.Duration("rr-min-issuance-period", envOrDefaultDuration("RETINA_RR_MIN_ISSUANCE_PERIOD", 500*time.Millisecond), "Minimum issuance period (μmin)")
		rrMaxIssuancePeriod           = flag.Duration("rr-max-issuance-period", envOrDefaultDuration("RETINA_RR_MAX_ISSUANCE_PERIOD", 12*time.Hour), "Maximum issuance period (μmax)")
		rrAdmissionRate               = flag.Float64("rr-admission-rate", envOrDefaultFloat64("RETINA_RR_ADMISSION_RATE", 1000), "Admission rate (r₀) for pacing newly inserted PDs' first issuance")
		rrStartingIssuancePeriod      = flag.Duration("rr-starting-issuance-period", envOrDefaultDuration("RETINA_RR_STARTING_ISSUANCE_PERIOD", time.Second*10), "Starting issuance period (Μ) assigned at admission")
		rrStatusInterval              = flag.Duration("rr-status-interval", envOrDefaultDuration("RETINA_RR_STATUS_INTERVAL", 20*time.Second), "Interval between CurrentStatus event emissions")
		rrPeriodDumpInterval          = flag.Duration("rr-period-dump-interval", envOrDefaultDuration("RETINA_RR_PERIOD_DUMP_INTERVAL", 20*time.Second), "Interval between PeriodDump event emissions")
		rrInsertChannelSize           = flag.Int("rr-insert-channel-size", envOrDefaultInt("RETINA_RR_INSERT_CHANNEL_SIZE", 1024), "Buffer size of the research scheduler's insert channel")
		rrUpdateChannelSize           = flag.Int("rr-update-channel-size", envOrDefaultInt("RETINA_RR_UPDATE_CHANNEL_SIZE", 1024), "Buffer size of the research scheduler's FIE update channel")
		rrLatenessTolerance           = flag.Duration("rr-lateness-tolerance", envOrDefaultDuration("RETINA_RR_LATENESS_TOLERANCE", 25*time.Millisecond), "Slack below which an issuance is not considered late")
		rrBusyTolerance               = flag.Duration("rr-busy-tolerance", envOrDefaultDuration("RETINA_RR_BUSY_TOLERANCE", 500*time.Microsecond), "Commitment-window width for the hybrid sleep strategy (Tbusy)")
		rrWaitTolerance               = flag.Duration("rr-wait-tolerance", envOrDefaultDuration("RETINA_RR_WAIT_TOLERANCE", time.Millisecond), "Busy-wait margin to absorb time.After over-sleep")
		rrInitialQueueSize            = flag.Int("rr-initial-queue-size", envOrDefaultInt("RETINA_RR_INITIAL_QUEUE_SIZE", 100_000), "Initial capacity reserved for the scheduling heap")
		rrMaxUpdateDrainPerIssuance   = flag.Int("rr-max-update-drain-per-issuance", envOrDefaultInt("RETINA_RR_MAX_UPDATE_DRAIN_PER_ISSUANCE", 5), "Maximum FIE updates drained per issuance call")
		rrMaxInsertDrainPerIssuance   = flag.Int("rr-max-insert-drain-per-issuance", envOrDefaultInt("RETINA_RR_MAX_INSERT_DRAIN_PER_ISSUANCE", 5), "Maximum inserts drained per issuance call")
		rrDefaultImpactDelay          = flag.Duration("rr-default-impact-delay", envOrDefaultDuration("RETINA_RR_DEFAULT_IMPACT_DELAY", time.Second), "Default estimated delay between a PD's issuance and its impact on an address")
		rrDisableResponsibleProbing   = flag.Bool("rr-disable-responsible-probing", envOrDefaultBool("RETINA_RR_DISABLE_RESPONSIBLE_PROBING", false), "Disable the responsible probing constraint (testing only)")
		rrDisableStaleness            = flag.Bool("rr-disable-staleness", envOrDefaultBool("RETINA_RR_DISABLE_STALENESS", false), "Disable the staleness-based period adjustment (testing only)")
		rrDisablePeriodAdjustedEvents = flag.Bool("rr-disable-period-adjustment-events", envOrDefaultBool("RETINA_RR_DISABLE_PERIOD_ADJUSTED_EVENTS", true), "Disable emitting period adjusted events")
		rrDisablePDInsertedEvents     = flag.Bool("rr-disable-pd-inserted-events", envOrDefaultBool("RETINA_RR_DISABLE_PD_INSERTED_EVENTS", true), "Disable emitting PD inserted events")
		rrDisablePeriodDumpEvents     = flag.Bool("rr-disable-period-dump-events", envOrDefaultBool("RETINA_RR_DISABLE_PERIOD_DUMP_EVENTS", true), "Disable emitting period dump events")
		rrDisableSchedulerLateEvents  = flag.Bool("rr-disable-scheduler-late-events", envOrDefaultBool("RETINA_RR_DISABLE_SCHEDULER_LATE_EVENTS", true), "Disable emitting scheduler late events")

		// --- DDBFIECapturerConfig (capturer- prefix) ---
		capturerEnabled                 = flag.Bool("capturer-enabled", envOrDefaultBool("RETINA_CAPTURER_ENABLED", true), "Enable capturing FIEs to DuckDB")
		capturerAllowNonEmptyCaptureDir = flag.Bool("capturer-allow-non-empty-capture-dir", envOrDefaultBool("RETINA_CAPTURER_ALLOW_NON_EMPTY_CAPTURE_DIR", false), "Allow capturing into a non-empty capture directory")
		capturerBatchSize               = flag.Int("capturer-batch-size", envOrDefaultInt("RETINA_CAPTURER_BATCH_SIZE", 100_000), "Number of FIEs accumulated before flushing DuckDB appenders")
		capturerCaptureDir              = flag.String("capturer-capture-dir", envOrDefault("RETINA_CAPTURER_CAPTURE_DIR", "./capture"), "Directory where DuckDB FIE capture files are stored")
		capturerRotationInterval        = flag.Duration("capturer-rotation-interval", envOrDefaultDuration("RETINA_CAPTURER_ROTATION_INTERVAL", 6*time.Hour), "Rotation interval for DuckDB FIE capture files")
		capturerChannelSize             = flag.Int("capturer-channel-size", envOrDefaultInt("RETINA_CAPTURER_CHANNEL_SIZE", 200_000), "Buffer size of the FIE capture channel")
		capturerFlushPeriod             = flag.Duration("capturer-flush-period", envOrDefaultDuration("RETINA_CAPTURER_FLUSH_PERIOD", time.Second), "Interval between periodic FIE capturer flushes")
	)
	flag.Parse()

	logger := newLogger(*logLevel)

	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector())
	registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	metrics := orchestrator.NewMetrics(registry)
	metricsSrv, err := startMetricsServer(logger, registry, *metricsAddr)
	if err != nil {
		return err
	}

	secret := os.Getenv("RETINA_SECRET")

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	var capturerConfig *orchestrator.DDBFIECapturerConfig

	if *capturerEnabled {
		capturerConfig = &orchestrator.DDBFIECapturerConfig{
			AllowNonEmptyCaptureDir: *capturerAllowNonEmptyCaptureDir,
			BatchSize:               *capturerBatchSize,
			CaptureDir:              *capturerCaptureDir,
			RotationInterval:        *capturerRotationInterval,
		}
	}

	orch, err := orchestrator.NewOrchestrator(&orchestrator.Config{
		AgentAddress:            *agentAddr,
		PDQueueSize:             *pdQueueSize,
		RingBufferSize:          *ringBufferSize,
		AgentBufferLength:       *agentBufferLength,
		APIAddress:              *apiAddr,
		APIReadHeaderTimeout:    *apiReadHeaderTimeout,
		FIEFilterPolicy:         *fieFilterPolicy,
		Secret:                  secret,
		EventBusSize:            *eventBusSize,
		EventsDir:               *eventsDir,
		StreamStartFromEarliest: *streamStartFromEarliest,
		ResearchSchedulerConfig: &orchestrator.ResearchSchedulerConfig{
			Seed:                        *rrSeed,
			LearningRate:                *rrLearningRate,
			SamplingWidth:               *rrSamplingWidth,
			ImpactThreshold:             *rrImpactThreshold,
			FIEHistoryCapacity:          *rrFIEHistoryCapacity,
			MinIssuancePeriod:           *rrMinIssuancePeriod,
			MaxIssuancePeriod:           *rrMaxIssuancePeriod,
			AdmissionRate:               *rrAdmissionRate,
			StartingIssuancePeriod:      *rrStartingIssuancePeriod,
			StatusInterval:              *rrStatusInterval,
			PeriodDumpInterval:          *rrPeriodDumpInterval,
			InsertChannelSize:           *rrInsertChannelSize,
			UpdateChannelSize:           *rrUpdateChannelSize,
			LatenessTolerance:           *rrLatenessTolerance,
			BusyTolerance:               *rrBusyTolerance,
			WaitTolerance:               *rrWaitTolerance,
			InitialQueueSize:            *rrInitialQueueSize,
			MaxUpdateDrainPerIssuance:   *rrMaxUpdateDrainPerIssuance,
			MaxInsertDrainPerIssuance:   *rrMaxInsertDrainPerIssuance,
			DefaultImpactDelay:          *rrDefaultImpactDelay,
			DisableResponsibleProbing:   *rrDisableResponsibleProbing,
			DisableStaleness:            *rrDisableStaleness,
			DisablePeriodAdjustedEvents: *rrDisablePeriodAdjustedEvents,
			DisablePDInsertedEvents:     *rrDisablePDInsertedEvents,
			DisablePeriodDumps:          *rrDisablePeriodDumpEvents,
			DisableSchedulerLateEvents:  *rrDisableSchedulerLateEvents,
		},
		CapturerConfig:      capturerConfig,
		CaptureChannelSize:  *capturerChannelSize,
		CapturerFlushPeriod: *capturerFlushPeriod,
	}, logger, metrics)
	if err != nil {
		return err
	}

	logger.Info("Starting orchestrator",
		slog.String("api_addr", *apiAddr),
		slog.String("agent_addr", *agentAddr),
		slog.String("log_level", *logLevel),
		slog.String("metrics_addr", *metricsAddr),
		slog.Bool("stream_start_from_earliest", *streamStartFromEarliest),
		slog.Float64("rr_impact_threshold", *rrImpactThreshold),
		slog.Float64("rr_min_issuance_period_seconds", rrMinIssuancePeriod.Seconds()),
		slog.Float64("rr_max_issuance_period_seconds", rrMaxIssuancePeriod.Seconds()),
		slog.Float64("rr_admission_rate", *rrAdmissionRate),
		slog.Bool("rr_disable_responsible_probing", *rrDisableResponsibleProbing),
		slog.Bool("rr_disable_staleness", *rrDisableStaleness),
	)

	if err := orch.Run(ctx); !errors.Is(err, ctx.Err()) {
		return err
	}

	shutdown(logger, metricsSrv)
	return nil
}

// startMetricsServer starts an HTTP server exposing Prometheus metrics at /metrics.
// It binds eagerly so that a port conflict is detected before the orchestrator starts.
func startMetricsServer(logger *slog.Logger, registry *prometheus.Registry, addr string) (*http.Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("metrics server: %w", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))

	//nolint:gosec // G112: metrics endpoint is internal-only; timeout omitted intentionally
	srv := &http.Server{Handler: mux}

	go func() {
		logger.Info("Starting metrics server", slog.String("addr", addr))
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("Metrics server failed", slog.Any("err", err))
		}
	}()

	return srv, nil
}

// newLogger creates a JSON logger writing to stdout at the given level.
// Falls back to info if the level string is unrecognized.
func newLogger(level string) *slog.Logger {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: l,
	}))
}

func shutdown(logger *slog.Logger, metricsSrv *http.Server) {
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("Metrics server shutdown failed", slog.Any("err", err))
	}
	logger.Info("Shutting down orchestrator")
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envOrDefaultUInt64(key string, def uint64) uint64 {
	if v := os.Getenv(key); v != "" {
		i, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			slog.Error("Invalid environment variable", slog.String("key", key), slog.String("value", v)) //nolint:gosec // G706: value is from env var, rejected as invalid, slog.String sanitizes output
			os.Exit(1)
		}
		return i
	}
	return def
}

func envOrDefaultInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			slog.Error("Invalid environment variable", slog.String("key", key), slog.String("value", v)) //nolint:gosec // G706: value is from env var, rejected as invalid, slog.String sanitizes output
			os.Exit(1)
		}
		return int(i)
	}
	return def
}

func envOrDefaultFloat64(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		i, err := strconv.ParseFloat(v, 64)
		if err != nil {
			slog.Error("Invalid environment variable", slog.String("key", key), slog.String("value", v)) //nolint:gosec // G706: value is from env var, rejected as invalid, slog.String sanitizes output
			os.Exit(1)
		}
		return i
	}
	return def
}

func envOrDefaultDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			slog.Error("Invalid environment variable", slog.String("key", key), slog.String("value", v)) //nolint:gosec // G706: value is from env var, rejected as invalid, slog.String sanitizes output
			os.Exit(1)
		}
		return d
	}
	return def
}

func envOrDefaultBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			slog.Error("Invalid environment variable", slog.String("key", key), slog.String("value", v)) //nolint:gosec // G706: value is from env var, rejected as invalid, slog.String sanitizes output
			os.Exit(1)
		}
		return b
	}
	return def
}
