package job

import (
	"errors"
	"fmt"
)

// FailureKind classifies *why* a job failed. This is deliberately not a boolean or
// a bare error string: the kind decides whether a retry is meaningful, whether the
// fault is ours or the caller's, and what an operator should look at first.
type FailureKind string

const (
	// FailureProvision means the environment never came up: image pull failed, no
	// capacity, daemon unreachable. The agent never ran, so retrying is safe and
	// usually correct — nothing in the world changed.
	FailureProvision FailureKind = "provision_failed"

	// FailureStart means the environment came up but the agent could not be started
	// or failed its readiness check. Retryable, but a repeat usually means a bad
	// image or payload rather than bad luck.
	FailureStart FailureKind = "start_failed"

	// FailureAgentCrash means the agent ran and exited non-zero. NOT retried
	// automatically: the agent may have had side effects, and a deterministic crash
	// retried three times is three times the cost for the same failure.
	FailureAgentCrash FailureKind = "agent_crashed"

	// FailureDeadline means the job exceeded its deadline and was stopped. Not
	// retried by default — the usual cause is work that does not fit the budget,
	// and retrying spends the budget again to learn the same thing.
	FailureDeadline FailureKind = "deadline_exceeded"

	// FailureReapedOrphan means the reaper destroyed an environment the control
	// plane had lost track of. This is the cost-control safety net firing, and it
	// always indicates a control-plane bug or crash worth alerting on.
	FailureReapedOrphan FailureKind = "reaped_orphan"

	// FailureRejected means the job was refused at admission: rate limit, queue
	// full, or invalid spec. The caller should back off and retry themselves; we
	// never silently absorb it.
	FailureRejected FailureKind = "rejected"

	// FailureInternal is our bug. Retryable, and it should be loud.
	FailureInternal FailureKind = "internal_error"
)

// Retryable reports whether re-running the job could plausibly produce a different
// outcome without a human changing something first.
func (k FailureKind) Retryable() bool {
	switch k {
	case FailureProvision, FailureStart, FailureInternal:
		return true
	}
	return false
}

// Fault attributes the failure, which is what separates "our SLO burned" from
// "the caller sent something bad" on a dashboard.
type Fault string

const (
	FaultSystem Fault = "system" // ours: infrastructure or a bug
	FaultUser   Fault = "user"   // theirs: bad spec, bad agent, over budget
)

func (k FailureKind) Fault() Fault {
	switch k {
	case FailureAgentCrash, FailureDeadline, FailureRejected:
		return FaultUser
	}
	return FaultSystem
}

// Failure is the recorded cause of a terminal failed job.
type Failure struct {
	Kind FailureKind `json:"kind"`
	// Message is safe to show the caller. It must never contain secret material;
	// see Spec.Secrets for why that is a live concern and not a platitude.
	Message string `json:"message"`
	// Attempts is how many times we tried before giving up.
	Attempts int `json:"attempts"`
	// Cause is the underlying error, for logs only. Not serialised to the API.
	Cause error `json:"-"`
}

func (f *Failure) Error() string {
	if f.Message == "" {
		return string(f.Kind)
	}
	return fmt.Sprintf("%s: %s", f.Kind, f.Message)
}

func (f *Failure) Unwrap() error { return f.Cause }

// Newf builds a Failure with a formatted message. The variadic args are formatted
// with %v, so wrap errors in Cause rather than relying on the message for detail.
func (k FailureKind) Newf(format string, args ...any) *Failure {
	return &Failure{Kind: k, Message: fmt.Sprintf(format, args...)}
}

// Wrap builds a Failure carrying an underlying cause.
func (k FailureKind) Wrap(err error, format string, args ...any) *Failure {
	return &Failure{Kind: k, Message: fmt.Sprintf(format, args...), Cause: err}
}

// KindOf extracts the FailureKind from an error chain, defaulting to
// FailureInternal for errors that were never classified. Defaulting to "our bug"
// is intentional: an unclassified error is one we did not think about, and those
// should surface as system fault rather than be quietly blamed on the user.
func KindOf(err error) FailureKind {
	var f *Failure
	if errors.As(err, &f) {
		return f.Kind
	}
	return FailureInternal
}
