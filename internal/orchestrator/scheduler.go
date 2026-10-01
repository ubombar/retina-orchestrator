package orchestrator

import "github.com/dioptra-io/retina-commons/api/v1"

// Scheduler is the scheduling interface used by the Retina orchestrator.
type Scheduler interface {
	// Insert admits a new PD into the schedule. Safe to call from any
	// goroutine. The ID field of the given PD is disregarded; the
	// Scheduler assigns a new identifier and returns it. No duplicate
	// check is performed.
	Insert(req *api.ProbingDirective) (uint64, error)

	// Next retrieves the next issuable PD. It blocks until a PD is due;
	// if the Scheduler is empty, it blocks until a PD is inserted. An
	// error indicates a fault and is the only condition under which the
	// loop stops.
	Next() (*api.ProbingDirective, error)

	// Update updates the Scheduler's state by the yielded FIE. Safe to
	// call from any goroutine.
	Update(fie *api.ForwardingInfoElement) error

	// Agent excludes an agent from issuance or includes it again.
	Agent(agentID string, exclude bool) error

	// Close stops the scheduler operations.
	Close() error
}
