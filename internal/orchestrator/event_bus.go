// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT

package orchestrator

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	"github.com/dioptra-io/retina-commons/api/v1"

	"github.com/dioptra-io/retina-orchestrator/internal/orchestrator/structures"
)

// ---------------------------------------------------------------------------
// Event infrastructure
// ---------------------------------------------------------------------------

// RetinaBaseEvent is the base of every event. Embed it by value in each
// concrete event type. It is never emitted on its own.
type RetinaBaseEvent struct {
	// Type is the wire discriminator, set by Emit to the concrete Go type
	// name (e.g. "PeriodAdjusted"). Consumers switch on this field.
	Type string `json:"type"`
	// Timestamp is the emission time, set by Emit.
	Timestamp time.Time `json:"timestamp"`
}

// stamp fills the metadata common to every event. It is written once here and
// inherited by every embedder, so no concrete event type needs to implement
// it. Being unexported, it also seals the Event interface to this package:
// only types defined here (which embed RetinaEvent) can satisfy Event.
func (e *RetinaBaseEvent) stamp(typ string) {
	e.Type = typ
	e.Timestamp = time.Now()
}

// RetinaEvent is anything embedding RetinaEvent. The sole (unexported) method
// makes the interface unimplementable from outside this package, so the set of
// event types is closed and known to consumers.
type RetinaEvent interface {
	stamp(string)
}

// ---------------------------------------------------------------------------
// Section 5 events — Scheduler decisions (DSD §5.5)
// ---------------------------------------------------------------------------

// Every event embeds RetinaEvent and follows the naming convention of ending
// in "Event". Payloads follow §5.5; per the DSD's note, exact field names and
// types are not normative and the code is the source of truth.
//
// AgentConnected / AgentDisconnected (§5.5) are intentionally not defined
// here: the Scheduler has no notion of agent liveness, so those events belong
// to whichever component owns agent state and are emitted on the same bus.
// PDRejected (§4.3) is intentionally omitted: identifiers are assigned by an
// atomic counter in Insert, so duplicates are impossible and there is no
// rejection path (see errata).

// OrchestratorStartedEvent is emitted once at initialization. Payload: the
// configuration parameters in effect (§5.5).
type OrchestratorStartedEvent struct {
	RetinaBaseEvent
	Config
}

// OrchestratorStoppedEvent is emitted when the orchestator is stopped, this is
// most likely because of a stop signal. The message (if available) is provided.
type OrchestratorStoppedEvent struct {
	RetinaBaseEvent
	Message string `json:"message"`
}

// AgentConnectedEvent is emitted when an agent becomes available (§5.5).
// The Scheduler itself has no notion of agent liveness (see NewResearchScheduler's
// integration notes); this event is emitted on the same bus by whichever
// component owns agent state.
type AgentConnectedEvent struct {
	RetinaBaseEvent
	AgentID string `json:"agent_id"`
}

// AgentDisconnectedEvent is emitted when an agent becomes unavailable (§5.5).
// Like AgentConnectedEvent, this is emitted by the component that owns agent
// state, not by the Scheduler.
type AgentDisconnectedEvent struct {
	RetinaBaseEvent
	AgentID string `json:"agent_id"`
}

// PDInsertedEvent is emitted when an insertion is applied and the PD is
// admitted into the schedule (§4.3).
type PDInsertedEvent struct {
	RetinaBaseEvent
	ProbingDirectiveID uint64    `json:"probing_directive_id"`
	FirstIssuanceTime  time.Time `json:"first_issuance_time"`
	CurrentPDCount     int       `json:"current_pd_count"`
}

type PDBulkInsertionEvent struct {
	RetinaBaseEvent
	NumPDs int                     `json:"num_pds"`
	PDs    []*api.ProbingDirective `json:"pds"`
}

type PeriodDumpEvent struct {
	RetinaBaseEvent
	NumPDs          int       `json:"num_pds"`
	IssuancePeriods []float64 `json:"issuance_periods"` // maps uint64 -> float64
}

// PeriodAdjustmentRule identifies which rule produced a period change (§4.2,
// §3.4). PeriodAdjustmentRuleNone is the zero value, used internally when no
// rule changed the period (no event is emitted in that case).
type PeriodAdjustmentRule string

const (
	PeriodAdjustmentRuleNone               PeriodAdjustmentRule = ""
	PeriodAdjustmentRuleStalenessSlowDown  PeriodAdjustmentRule = "staleness_slow_down" // §4.2.2
	PeriodAdjustmentRuleStalenessSpeedUp   PeriodAdjustmentRule = "staleness_speed_up"  // §4.2.2
	PeriodAdjustmentRuleResponsibleProbing PeriodAdjustmentRule = "responsible_probing" // §4.2.1
	PeriodAdjustmentRuleClamp              PeriodAdjustmentRule = "clamp"               // §3.4
)

// PeriodAdjustedEvent is emitted on every change to a PD's issuance period,
// from any rule: staleness slow-down or speed-up (§4.2.2), the responsible
// probing floor (§4.2.1), or the μ_min/μ_max clamp (§3.4). A single learning
// step emits at most one such event, attributed to the binding rule.
//
// Beyond the previous/new period and the binding rule, the payload also
// carries the intermediate state of every rule considered during the step,
// not just the one that ended up binding. This is needed to reconstruct why
// a step landed where it did: e.g. staleness may have proposed a slow-down
// that was then overridden by the responsible-probing floor, and without
// StalenessCandidate that intermediate value would be lost. All diagnostic
// fields are always populated, regardless of which rule bound; fields that
// are not meaningful for a given step (e.g. HistoryStable when the FIE
// history isn't yet full) are left at their zero value.
type PeriodAdjustedEvent struct {
	RetinaBaseEvent
	ProbingDirectiveID uint64               `json:"probing_directive_id"`
	PreviousPeriod     float64              `json:"previous_period"`
	NewPeriod          float64              `json:"new_period"`
	Rule               PeriodAdjustmentRule `json:"rule"`

	// --- Staleness diagnostics (§4.2.2) ---

	// FIEHistoryFull reports whether the FIE history had reached capacity
	// (n = m) at this step, i.e. whether staleness was eligible to fire.
	FIEHistoryFull bool `json:"fie_history_full"`
	// HistoryStable is the result of the pairwise-equivalence check, only
	// meaningful when FIEHistoryFull is true.
	HistoryStable bool `json:"history_stable"`
	// StalenessCandidate is the issuance period immediately after the
	// staleness step, before responsible probing or the clamp could
	// override it. Equal to PreviousPeriod when staleness did not fire.
	StalenessCandidate float64 `json:"staleness_candidate"`

	// --- Responsible probing diagnostics (§4.2.1) ---

	// ImpactDelay is the PD's last known impact delay (rec.impactDelay) at
	// the time of this step, in seconds.
	ImpactDelay float64 `json:"impact_delay"`
	// ImpactedNear / ImpactedFar are the near/far addresses the projection
	// assumed this issuance would impact (rec.lastNear / rec.lastFar), i.e.
	// the addresses reserveAndFloor was evaluated against. Empty string
	// means null/no address.
	ImpactedNear string `json:"impacted_near"`
	ImpactedFar  string `json:"impacted_far"`
	// RawImpactFloor is rpFloorPeriod: the tighter of the two per-address
	// GCRA floors, before the 1/(1-β) widening.
	RawImpactFloor float64 `json:"raw_impact_floor"`
	// WorstCaseFloor is RawImpactFloor widened by 1/(1-β), the value
	// actually compared against the staleness candidate.
	WorstCaseFloor float64 `json:"worst_case_floor"`
}

// SchedulerLateEvent is emitted when an overdue PD is issued past its
// scheduled time (§4.1). The Scheduler does not recover the schedule; overdue
// PDs are issued immediately in queue order and this event surfaces the
// condition.
type SchedulerLateEvent struct {
	RetinaBaseEvent
	ProbingDirectiveID uint64    `json:"probing_directive_id"`
	ScheduledTime      time.Time `json:"scheduled_time"`
	ActualTime         time.Time `json:"actual_time"`
}

// CurrentStatusEvent is the periodic aggregate snapshot emitted every
// Tstatus (§5.5), for monitoring and coarse-grained analysis without
// reconstructing state from the per-PD event stream.
type CurrentStatusEvent struct {
	RetinaBaseEvent
	CurrentPDCount                  int     `json:"current_pd_count"`
	CumulativeInsertions            uint64  `json:"cumulative_insertions"`
	CumulativeIssuances             uint64  `json:"cumulative_issuances"`
	CumulativeUpdates               uint64  `json:"cumulative_updates"`
	AggregateRequestedRate          float64 `json:"aggregate_requested_rate"`           // Σ rᵢ = Σ 1/μᵢ, per second
	AggregatePeriodBetweenIssuances float64 `json:"aggregate_period_between_issuances"` // 1 / Σ rᵢ
	RealizedIssuanceRate            float64 `json:"realized_issuance_rate"`             // issuances over the last interval, per second
	RealizedUpdateRate              float64 `json:"realized_update_rate"`
	DistinctImpactedAddrs           int     `json:"distinct_impacted_addrs"`
	PeriodMin                       float64 `json:"period_min"`
	PeriodMax                       float64 `json:"period_max"`
	PDsClampedAtMin                 int     `json:"pds_clamped_at_min"`
	PDsClampedAtMax                 int     `json:"pds_clamped_at_max"`
	PDsWithFullHistory              int     `json:"pds_with_full_history"`
	UpdateChannelOccupancy          int     `json:"update_channel_occupancy"`
	InsertChannelOccupancy          int     `json:"insert_channel_occupancy"`
	CumulativeLateOccurrences       uint64  `json:"cumulative_late_occurrences"`
}

// ---------------------------------------------------------------------------
// Event bus
// ---------------------------------------------------------------------------

type envelope struct{ RetinaEvent }

// EventBus is the emitter through which the Scheduler exposes its decisions
// (§5.5). It wraps the same ring-buffer mechanism used for client FIE
// streaming (outside this document's scope). Emission is non-blocking: a slow
// or absent subscriber is lapped rather than stalling the scheduling loop.
type EventBus struct {
	ring *structures.RingBuffer[envelope]

	eventsDir        string
	eventsFile       *os.File
	rotationInterval time.Duration
	currentRotation  time.Time

	mu sync.Mutex
}

// Emit stamps the event with its type name and emission time, then publishes
// it to the ring. It is non-blocking with best-effort delivery.
//
// The event is passed as &e (a *Event) because the underlying RingBuffer
// stores *T and uses nil for empty slots; here T is the Event interface, so
// the stored element type is *Event. &e is the address of Emit's own
// parameter, distinct per call, so no aliasing occurs across emissions.
func (b *EventBus) Emit(e RetinaEvent) {
	e.stamp(typeName(e))
	currentTime := time.Now().UTC()

	if b.eventsDir != "" {
		data, err := json.Marshal(e)
		if err == nil {
			b.mu.Lock()

			if err := b.rotateIfNeeded(currentTime); err == nil {
				_, _ = b.eventsFile.Write(append(data, '\n'))
			}

			b.mu.Unlock()
		}
	}

	b.ring.Push(&envelope{e})
}

// NewConsumer creates a new consumer for the ringbuffer. It has it's own
// methods for synchronization etc.
func (b *EventBus) NewConsumer() *structures.RingConsumer[envelope] {
	return b.ring.NewConsumer()
}

// eventTypeNames caches the reflect.Type -> wire-name mapping so the
// reflection lookup in typeName runs at most once per concrete event type.
var eventTypeNames sync.Map // reflect.Type -> string

// typeName returns the wire discriminator for an event: its concrete Go type
// name, verbatim (e.g. *PeriodAdjusted -> "PeriodAdjusted"). Because the name
// is derived from the type, it cannot drift out of sync with the struct, but
// renaming a struct renames its wire event accordingly.
func typeName(e any) string {
	t := reflect.TypeOf(e)
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if v, ok := eventTypeNames.Load(t); ok {
		return v.(string)
	}
	name := t.Name()
	eventTypeNames.Store(t, name)
	return name
}

// NewEventBus creates an EventBus backed by a ring buffer of the given
// capacity. Capacity bounds how far a slow subscriber may fall behind before
// it is lapped and starts missing events (§5.5); it must be positive.
func NewEventBus(capacity int, eventsDir string, rotationInterval time.Duration) (*EventBus, error) {
	ring, err := structures.NewRingBufferTailFollower[envelope](capacity)
	if err != nil {
		return nil, fmt.Errorf("cannot create event bus: %w", err)
	}

	bus := &EventBus{
		ring:             ring,
		eventsDir:        eventsDir,
		rotationInterval: rotationInterval,
	}

	// An empty events directory disables event persistence.
	if eventsDir == "" {
		return bus, nil
	}

	if rotationInterval <= 0 {
		return nil, fmt.Errorf("event rotation interval must be positive")
	}

	if err := os.MkdirAll(eventsDir, 0o750); err != nil {
		return nil, fmt.Errorf("cannot create events directory: %w", err)
	}

	currentRotation := time.Now().UTC().Truncate(rotationInterval)

	filename := fmt.Sprintf(
		"events-%s.jsonl",
		currentRotation.Format("20060102T150405Z"),
	)

	//nolint:gosec
	file, err := os.OpenFile(
		filepath.Join(eventsDir, filename),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		0o640,
	)
	if err != nil {
		return nil, fmt.Errorf("cannot create events file: %w", err)
	}

	bus.eventsFile = file
	bus.currentRotation = currentRotation

	return bus, nil
}

func (b *EventBus) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.eventsFile == nil {
		return nil
	}

	return b.eventsFile.Close()
}

func (b *EventBus) rotateIfNeeded(now time.Time) error {
	rotation := now.UTC().Truncate(b.rotationInterval)

	if b.eventsFile != nil && rotation.Equal(b.currentRotation) {
		return nil
	}

	if b.eventsFile != nil {
		if err := b.eventsFile.Close(); err != nil {
			return err
		}
	}

	filename := fmt.Sprintf(
		"events-%s.jsonl",
		rotation.Format("20060102T150405Z"),
	)

	//nolint:gosec
	file, err := os.OpenFile(
		filepath.Join(b.eventsDir, filename),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		0o640,
	)
	if err != nil {
		return err
	}

	b.eventsFile = file
	b.currentRotation = rotation

	return nil
}
