package job

import (
	"crypto/rand"
	"encoding/base32"
	"fmt"
	"strings"
	"time"
)

// Priority orders dispatch. Higher runs first; ties break by submission time so a
// steady stream of high-priority work cannot starve an older job of equal priority.
type Priority int

const (
	PriorityLow    Priority = 10
	PriorityNormal Priority = 50
	PriorityHigh   Priority = 90
)

// Spec is the caller's description of the work. It is immutable once submitted:
// everything mutable lives on Job, so a spec can be safely re-read for a retry.
type Spec struct {
	// Image is the container image carrying the agent.
	Image string `json:"image"`
	// Command overrides the image entrypoint. Empty means use the image default.
	Command []string `json:"command,omitempty"`
	// Input is the task payload handed to the agent, opaque to the platform.
	Input map[string]string `json:"input,omitempty"`
	// Env holds non-secret environment variables. Anything sensitive belongs in
	// Secrets: values here are persisted and served over the API in the clear.
	Env map[string]string `json:"env,omitempty"`
	// Secrets are resolved at dispatch and injected into the environment. Only the
	// reference is stored; see SecretRef.
	Secrets []SecretRef `json:"secrets,omitempty"`
	// Deadline bounds the agent's run. This is the innermost of three reaper
	// layers, not the only one — see the reaper package.
	Deadline time.Duration `json:"deadline"`
	// Resources caps CPU and memory. Unset values take platform defaults.
	Resources Resources `json:"resources"`
	// MaxAttempts bounds retries of retryable failures. Zero means the platform
	// default; one means never retry.
	MaxAttempts int `json:"max_attempts,omitempty"`
}

// Resources bounds what one job may consume. Enforced by the driver, so a job
// cannot starve its neighbours on a shared host.
type Resources struct {
	CPUMillis int64 `json:"cpu_millis,omitempty"`
	MemoryMiB int64 `json:"memory_mib,omitempty"`
}

// Job is the platform's record of one unit of work.
type Job struct {
	ID       string   `json:"id"`
	TenantID string   `json:"tenant_id"`
	Priority Priority `json:"priority"`
	Spec     Spec     `json:"spec"`

	State   State    `json:"state"`
	Failure *Failure `json:"failure,omitempty"`

	// EnvID identifies the provisioned environment, once there is one. The reaper
	// relies on this being written *before* provisioning is confirmed, so an
	// environment created by a worker that then crashed is still traceable.
	EnvID string `json:"env_id,omitempty"`

	Attempts int `json:"attempts"`

	SubmittedAt time.Time  `json:"submitted_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	EndedAt     *time.Time `json:"ended_at,omitempty"`
}

// New builds a pending job from a validated spec.
func New(tenantID string, priority Priority, spec Spec, now time.Time) *Job {
	return &Job{
		ID:          NewID(now),
		TenantID:    tenantID,
		Priority:    priority,
		Spec:        spec,
		State:       StatePending,
		SubmittedAt: now,
	}
}

// Transition moves the job to a new state, refusing illegal edges. Callers get an
// error rather than a silent no-op, because a bad transition is a logic bug and
// swallowing it would let a job sit in a state nothing will ever advance.
func (j *Job) Transition(to State, now time.Time) error {
	if !CanTransition(j.State, to) {
		return &TransitionError{From: j.State, To: to}
	}
	j.State = to
	switch to {
	case StateRunning:
		if j.StartedAt == nil {
			j.StartedAt = &now
		}
	case StateSucceeded, StateFailed, StateCancelled:
		j.EndedAt = &now
	}
	return nil
}

// Fail moves the job to a terminal failed state, recording the cause.
func (j *Job) Fail(f *Failure, now time.Time) error {
	if f == nil {
		f = FailureInternal.Newf("job failed without a recorded cause")
	}
	f.Attempts = j.Attempts
	if err := j.Transition(StateFailed, now); err != nil {
		return err
	}
	j.Failure = f
	return nil
}

// ShouldRetry reports whether a failed attempt should go back on the queue.
func (j *Job) ShouldRetry(kind FailureKind, defaultMaxAttempts int) bool {
	if !kind.Retryable() {
		return false
	}
	max := j.Spec.MaxAttempts
	if max <= 0 {
		max = defaultMaxAttempts
	}
	return j.Attempts < max
}

// Duration reports how long the job ran, or how long it has been running.
func (j *Job) Duration(now time.Time) time.Duration {
	if j.StartedAt == nil {
		return 0
	}
	if j.EndedAt != nil {
		return j.EndedAt.Sub(*j.StartedAt)
	}
	return now.Sub(*j.StartedAt)
}

var idEncoding = base32.NewEncoding("0123456789abcdefghjkmnpqrstvwxyz").WithPadding(base32.NoPadding)

// NewID returns a time-ordered, collision-resistant job ID.
//
// Time-prefixed so IDs sort by submission order: that makes range scans over a
// key-value queue cheap and makes logs readable without a join. 80 bits of
// randomness after the timestamp keeps concurrent submissions in the same
// millisecond from colliding.
func NewID(now time.Time) string {
	// 48-bit millisecond timestamp (big-endian) + 80 bits of randomness, the
	// ULID layout. 48 bits of milliseconds runs out in the year 10889.
	var b [16]byte
	ms := uint64(now.UTC().UnixMilli())
	for i := 0; i < 6; i++ {
		b[i] = byte(ms >> (40 - 8*i))
	}
	if _, err := rand.Read(b[6:]); err != nil {
		// crypto/rand does not fail on any supported platform; if it somehow
		// does, a panic is correct — silently issuing weak IDs is worse.
		panic(fmt.Sprintf("ephemera: cannot read random bytes for job ID: %v", err))
	}
	return "job_" + idEncoding.EncodeToString(b[:])
}

// TenantOf returns the tenant portion of a namespaced identifier, if present.
func TenantOf(id string) string {
	if i := strings.IndexByte(id, '/'); i > 0 {
		return id[:i]
	}
	return ""
}
