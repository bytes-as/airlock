package job

import "fmt"

// State is the lifecycle position of a job. States are persisted, so the string
// values are part of the API contract and must not be renamed casually.
type State string

const (
	// StatePending means accepted by the API but not yet admitted to the queue.
	StatePending State = "pending"
	// StateQueued means durably enqueued and waiting for a worker.
	StateQueued State = "queued"
	// StateProvisioning means a worker claimed the job and is creating its environment.
	StateProvisioning State = "provisioning"
	// StateRunning means the environment is up and the agent has been started.
	StateRunning State = "running"
	// StateSucceeded is terminal: the agent exited 0 and artifacts were collected.
	StateSucceeded State = "succeeded"
	// StateFailed is terminal. Consult Job.Failure for why; the kind determines
	// whether a retry is meaningful.
	StateFailed State = "failed"
	// StateCancelled is terminal: ended on request rather than by fault.
	StateCancelled State = "cancelled"
)

// Terminal reports whether the state admits no further transitions.
func (s State) Terminal() bool {
	switch s {
	case StateSucceeded, StateFailed, StateCancelled:
		return true
	}
	return false
}

func (s State) Valid() bool {
	_, ok := transitions[s]
	return ok
}

// transitions is the complete allowed state graph. Anything not listed here is a
// bug, and CanTransition is the single place that decides — no ad-hoc state checks
// scattered through the scheduler.
var transitions = map[State][]State{
	StatePending:      {StateQueued, StateFailed, StateCancelled},
	StateQueued:       {StateProvisioning, StateFailed, StateCancelled},
	StateProvisioning: {StateRunning, StateQueued, StateFailed, StateCancelled},
	StateRunning:      {StateSucceeded, StateFailed, StateCancelled},
	StateSucceeded:    {},
	StateFailed:       {},
	StateCancelled:    {},
}

// CanTransition reports whether from -> to is legal.
//
// Note StateProvisioning -> StateQueued: that is a retry after a provisioning
// failure, and it is deliberately the only backwards edge in the graph. A job
// that already reached StateRunning is never re-queued, because the agent may
// have caused side effects out in the world and we cannot know whether repeating
// them is safe.
func CanTransition(from, to State) bool {
	for _, allowed := range transitions[from] {
		if allowed == to {
			return true
		}
	}
	return false
}

// TransitionError reports an attempt to make an illegal state transition.
type TransitionError struct {
	From, To State
}

func (e *TransitionError) Error() string {
	return fmt.Sprintf("illegal job state transition %s -> %s", e.From, e.To)
}
