// Copyright (c) 2026 Sorbonne Université
// SPDX-License-Identifier: MIT

package orchestrator

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all Prometheus metrics for the orchestrator.
// It is created once and passed to the orchestrator components via constructors.
type Metrics struct {
	// Agent connections
	AgentsConnected          prometheus.Gauge
	AuthFailuresTotal        prometheus.Counter
	AgentDisconnectionsTotal *prometheus.CounterVec
	PDsSentTotal             *prometheus.CounterVec
	FIEsReceivedTotal        *prometheus.CounterVec
	AgentQueueSize           *prometheus.GaugeVec
	PDsDroppedTotal          *prometheus.CounterVec
	DispatchBlockedSeconds   *prometheus.CounterVec

	// PD cycling
	PDsTotal             prometheus.Gauge
	CycleDurationSeconds prometheus.Histogram
	CyclesTotal          prometheus.Counter
	PDsSkippedTotal      prometheus.Counter

	// Streaming endpoint
	StreamClientsConnected    prometheus.Gauge
	StreamConnectionsTotal    prometheus.Counter
	StreamDisconnectionsTotal *prometheus.CounterVec
	FIEsStreamedTotal         prometheus.Counter
	StreamLagSeconds          prometheus.Histogram

	// SSE endpoint
	SSEClientsConnected    prometheus.Gauge
	SSEConnectionsTotal    prometheus.Counter
	SSEDisconnectionsTotal *prometheus.CounterVec
	SSEEventsTotal         *prometheus.CounterVec
}

// NewMetrics creates and registers all orchestrator metrics with the given registry.
//
//nolint:funlen // metric registration is necessarily verbose
func NewMetrics(registry prometheus.Registerer) *Metrics {
	factory := promauto.With(registry)

	return &Metrics{
		// Agent connections
		AgentsConnected: factory.NewGauge(prometheus.GaugeOpts{
			Name: "retina_orchestrator_agents_connected",
			Help: "Number of currently connected agents.",
		}),
		AuthFailuresTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "retina_orchestrator_auth_failures_total",
			Help: "Total number of rejected authentication attempts.",
		}),
		AgentDisconnectionsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "retina_orchestrator_agent_disconnections_total",
			Help: "Total number of agent disconnections, labeled by agent ID.",
		}, []string{"agent_id"}),
		PDsSentTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "retina_orchestrator_pds_sent_total",
			Help: "Total number of probing directives dispatched, labeled by agent ID.",
		}, []string{"agent_id"}),
		FIEsReceivedTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "retina_orchestrator_fies_received_total",
			Help: "Total number of forwarding info elements received, labeled by agent ID.",
		}, []string{"agent_id"}),
		AgentQueueSize: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "retina_orchestrator_agent_queue_size",
			Help: "Current number of probing directives queued for the agent.",
		}, []string{"agent_id"}),
		PDsDroppedTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "retina_orchestrator_pds_dropped_total",
			Help: "Total number of issued probing directives that never reached the agent, labeled by agent ID and reason (not_connected, disconnected, timeout).",
		}, []string{"agent_id", "reason"}),
		DispatchBlockedSeconds: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "retina_orchestrator_dispatch_blocked_seconds_total",
			Help: "Total time the scheduler spent waiting for room in an agent's queue, labeled by agent ID.",
		}, []string{"agent_id"}),

		// PD cycling
		PDsTotal: factory.NewGauge(prometheus.GaugeOpts{
			Name: "retina_orchestrator_pds_total",
			Help: "Total number of probing directives in the current list.",
		}),
		CycleDurationSeconds: factory.NewHistogram(prometheus.HistogramOpts{ // TODO: tune buckets once we have real cycle duration data.
			Name:    "retina_orchestrator_cycle_duration_seconds",
			Help:    "Duration of a full PD cycle in seconds.",
			Buckets: prometheus.DefBuckets,
		}),
		CyclesTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "retina_orchestrator_cycles_total",
			Help: "Total number of completed PD cycles.",
		}),
		PDsSkippedTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "retina_orchestrator_pds_skipped_total",
			Help: "Total number of directives skipped by the Bernoulli experiment due to issuance probability < 1.",
		}),

		// Streaming endpoint
		StreamClientsConnected: factory.NewGauge(prometheus.GaugeOpts{
			Name: "retina_orchestrator_stream_clients_connected",
			Help: "Number of currently active streaming clients.",
		}),
		StreamConnectionsTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "retina_orchestrator_stream_connections_total",
			Help: "Total number of streaming connections opened.",
		}),
		StreamDisconnectionsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "retina_orchestrator_stream_disconnections_total",
			Help: "Total number of streaming disconnections, labeled by reason.",
		}, []string{"reason"}),
		FIEsStreamedTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "retina_orchestrator_fies_streamed_total",
			Help: "Total number of FIEs pushed to streaming clients.",
		}),
		StreamLagSeconds: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "retina_orchestrator_stream_lag_seconds",
			Help:    "Time between receiving a FIE from an agent and delivering it to streaming clients.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0},
		}),

		// SSE endpoint
		SSEClientsConnected: factory.NewGauge(prometheus.GaugeOpts{
			Name: "retina_orchestrator_sse_clients_connected",
			Help: "Number of currently active SSE clients.",
		}),
		SSEConnectionsTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "retina_orchestrator_sse_connections_total",
			Help: "Total number of SSE connections opened.",
		}),
		SSEDisconnectionsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "retina_orchestrator_sse_disconnections_total",
			Help: "Total number of SSE disconnections, labeled by reason.",
		}, []string{"reason"}),
		SSEEventsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "retina_orchestrator_sse_events_total",
			Help: "Total number of SSE events published, labeled by event type.",
		}, []string{"event_type"}),
	}
}
