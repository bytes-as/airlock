// Package driver defines the contract between the control plane and whatever
// actually runs a job: a local process, a container, or a cloud task.
//
// The interface is the load-bearing abstraction of the whole system. Every
// implementation must be able to answer the same six questions honestly:
// can you make me an isolated environment, can you start work in it, can I watch
// it, can I collect what it produced, can you destroy it, and — critically —
// can you tell me about environments I have lost track of.
//
// That last one exists because the control plane can crash. A driver that can
// only describe environments it remembers creating is useless for reaping, and
// unreaped environments cost real money.
package driver

import (
	"context"
	"fmt"
	"time"

	"ephemera/internal/job"
)

// Driver provisions and manages ephemeral execution environments.
//
// Implementations must be safe for concurrent use: the scheduler calls into a
// single Driver from every worker goroutine.
type Driver interface {
	// Name identifies the driver in logs, metrics and API responses.
	Name() string

	// Capabilities reports what this driver actually guarantees. The control
	// plane uses it to refuse specs it cannot honour rather than silently
	// downgrading them — see Capabilities for why that matters.
	Capabilities() Capabilities

	// Create provisions an environment without starting work in it. Splitting
	// create from start means a provisioning failure is distinguishable from a
	// startup failure, which the retry policy depends on.
	Create(ctx context.Context, spec EnvSpec) (Env, error)

	// Start begins the agent inside an already-created environment.
	Start(ctx context.Context, env Env) error

	// Logs streams output until the environment exits or ctx is cancelled. The
	// channel is closed when the stream ends. Callers must drain it, or cancel
	// ctx, or the implementation will block on send.
	Logs(ctx context.Context, env Env) (<-chan LogLine, error)

	// Wait blocks until the agent exits and reports how it went. A non-zero
	// exit is not an error from Wait: it is a Status with an exit code. Wait
	// returns an error only when we could not determine the outcome at all,
	// which is a different and worse situation.
	Wait(ctx context.Context, env Env) (Status, error)

	// Collect gathers the environment's artifacts into dest on the host. It
	// must be callable after the agent has exited but before Destroy, which is
	// the only window in which the environment's filesystem still exists.
	Collect(ctx context.Context, env Env, dest string) ([]Artifact, error)

	// Destroy tears the environment down and releases its resources.
	//
	// Destroy MUST be idempotent and MUST NOT fail merely because the
	// environment is already gone: the reaper calls it speculatively on
	// environments that may have died on their own, and an error there would
	// turn a clean outcome into a false alarm.
	Destroy(ctx context.Context, env Env) error

	// Describe reports the current state of an environment, including one this
	// process did not create.
	Describe(ctx context.Context, env Env) (Status, error)

	// List returns every environment this driver owns, including orphans left
	// by a previous control-plane process. This is what makes reaping possible
	// across a crash, so implementations must derive it from the underlying
	// runtime, never from in-memory bookkeeping.
	List(ctx context.Context) ([]Env, error)
}

// IsolationLevel describes how strong a boundary a driver puts between jobs.
// Stated explicitly so the platform never over-claims: the difference between
// "separate process" and "separate kernel" is the difference between keeping
// out a buggy agent and keeping out a hostile one.
type IsolationLevel int

const (
	// IsolationProcess is an OS process boundary: separate memory and working
	// directory, shared kernel and shared filesystem namespace. Adequate for
	// code you wrote. Not a security boundary against code you did not.
	IsolationProcess IsolationLevel = iota

	// IsolationContainer adds namespaces and cgroups: separate filesystem,
	// network and process trees, with enforced resource limits. Still a shared
	// kernel, so a kernel escape crosses tenants.
	IsolationContainer

	// IsolationMicroVM adds a hypervisor boundary — a separate kernel per job.
	IsolationMicroVM
)

func (l IsolationLevel) String() string {
	switch l {
	case IsolationProcess:
		return "process"
	case IsolationContainer:
		return "container"
	case IsolationMicroVM:
		return "microvm"
	}
	return "unknown"
}

// Capabilities is a driver's honest self-description. The control plane
// consults it at admission: a spec asking for network egress policy against a
// driver that cannot enforce one is rejected, not quietly run without it.
// Silently ignoring a security control is worse than refusing the job, because
// the caller believes they got the control.
type Capabilities struct {
	Isolation IsolationLevel

	// EnforcesResourceLimits reports whether Resources caps are actually applied.
	EnforcesResourceLimits bool

	// EnforcesNetworkPolicy reports whether NetworkPolicy is actually applied.
	EnforcesNetworkPolicy bool

	// SurvivesControlPlaneRestart reports whether List can find environments
	// created by a previous process. Where false, the reaper cannot recover
	// orphans after a crash and must rely on its outermost layer instead.
	SurvivesControlPlaneRestart bool
}

// EnvSpec is everything a driver needs to build one environment.
type EnvSpec struct {
	JobID    string
	TenantID string

	// Image is the container image, for drivers that have one. The process
	// driver ignores it and runs Command directly on the host.
	Image   string
	Command []string

	// Env holds non-secret environment variables.
	Env map[string]string

	// Secrets are resolved values, injected into the environment and never
	// persisted or logged. They are separate from Env so that no code path can
	// serialise a spec and leak them by accident.
	Secrets map[string]job.Secret

	Resources job.Resources
	Network   NetworkPolicy

	// Deadline is the innermost reaper layer: the environment terminates itself
	// after this long, regardless of what the control plane is doing.
	Deadline time.Duration

	// ArtifactDir is the path inside the environment the agent writes outputs to.
	ArtifactDir string

	// Labels are attached to the underlying runtime object so orphans remain
	// identifiable as ours after a control-plane crash.
	Labels map[string]string
}

// NetworkMode selects the egress posture of an environment.
type NetworkMode string

const (
	// NetworkNone gives no network at all. The strongest option, and the right
	// default for payloads that do not need one.
	NetworkNone NetworkMode = "none"

	// NetworkEgress allows outbound internet but denies the CIDRs in
	// DenyCIDRs — cloud metadata endpoints and internal VPC ranges.
	NetworkEgress NetworkMode = "egress"

	// NetworkProxied forces all traffic through ProxyURL, which is how IP
	// rotation and geographic simulation are done.
	NetworkProxied NetworkMode = "proxied"
)

// NetworkPolicy describes what the environment may reach.
type NetworkPolicy struct {
	Mode NetworkMode

	// DenyCIDRs are blocked even under NetworkEgress. The link-local range
	// 169.254.0.0/16 is the one that matters most: it carries the cloud instance
	// metadata service, and reaching it from untrusted code is how a sandbox
	// turns into stolen IAM credentials.
	DenyCIDRs []string

	// ProxyURL is the upstream proxy for NetworkProxied.
	ProxyURL string
}

// DefaultDenyCIDRs are blocked for every networked environment unless a caller
// deliberately overrides them.
func DefaultDenyCIDRs() []string {
	return []string{
		"169.254.0.0/16", // link-local: AWS/GCP/Azure instance metadata
		"10.0.0.0/8",     // RFC1918 — internal VPC ranges
		"172.16.0.0/12",  // RFC1918
		"192.168.0.0/16", // RFC1918
		"100.64.0.0/10",  // carrier-grade NAT, used by some cloud internals
		"fd00::/8",       // IPv6 unique-local
		"fe80::/10",      // IPv6 link-local
	}
}

// Env identifies a provisioned environment. It is persisted with the job, so a
// new control-plane process can address an environment started by an old one.
type Env struct {
	// ID is the driver-native identifier: a container ID, a task ARN, a PID
	// handle. Opaque to everything above the driver.
	ID string

	// Driver names the driver that owns this environment, so a control plane
	// configured with several can route Destroy to the right one.
	Driver string

	JobID     string
	TenantID  string
	CreatedAt time.Time

	// ExpiresAt is the hard wall-clock deadline. The reaper destroys anything
	// past it, whatever the control plane believes about the job's state.
	ExpiresAt time.Time
}

// Expired reports whether the environment has outlived its deadline.
func (e Env) Expired(now time.Time) bool {
	return !e.ExpiresAt.IsZero() && now.After(e.ExpiresAt)
}

// Phase is the coarse lifecycle position of an environment, as the driver sees
// it. Distinct from job.State: a job is a control-plane concept that survives
// retries, an environment is one physical attempt.
type Phase string

const (
	PhaseCreating Phase = "creating"
	PhaseReady    Phase = "ready"
	PhaseRunning  Phase = "running"
	PhaseExited   Phase = "exited"

	// PhaseGone means the environment no longer exists. Reached either by
	// Destroy or by something outside us removing it. Never an error: an
	// environment that is gone is an environment that costs nothing.
	PhaseGone Phase = "gone"
)

// Terminal reports whether the phase admits no further work.
func (p Phase) Terminal() bool { return p == PhaseExited || p == PhaseGone }

// Status is a point-in-time observation of an environment.
type Status struct {
	Phase Phase

	// ExitCode is meaningful only when Phase is PhaseExited.
	ExitCode int

	// OOMKilled distinguishes "the agent failed" from "we did not give it
	// enough memory", which are different bugs with different owners.
	OOMKilled bool

	// DeadlineExceeded reports that the environment was stopped by its own
	// deadline rather than exiting on its own.
	DeadlineExceeded bool

	StartedAt time.Time
	ExitedAt  time.Time

	// Reason is a human-readable detail for operators. Never contains secrets.
	Reason string
}

// Succeeded reports a clean exit.
func (s Status) Succeeded() bool {
	return s.Phase == PhaseExited && s.ExitCode == 0 && !s.DeadlineExceeded && !s.OOMKilled
}

// Failure converts a terminal Status into the control plane's failure taxonomy,
// keeping the mapping in one place instead of scattered across drivers.
func (s Status) Failure() *job.Failure {
	switch {
	case s.Succeeded():
		return nil
	case s.DeadlineExceeded:
		return job.FailureDeadline.Newf("environment exceeded its deadline")
	case s.OOMKilled:
		// User fault by taxonomy: the spec asked for too little memory. Surfaced
		// distinctly so it is never mistaken for an agent bug.
		return job.FailureAgentCrash.Newf("agent was killed for exceeding its memory limit")
	case s.Phase == PhaseGone:
		return job.FailureReapedOrphan.Newf("environment disappeared before it could be collected")
	case s.Phase == PhaseExited:
		return job.FailureAgentCrash.Newf("agent exited with code %d", s.ExitCode)
	default:
		return job.FailureInternal.Newf("environment in non-terminal phase %q when a result was expected", s.Phase)
	}
}

// LogLine is one line of agent output.
type LogLine struct {
	At     time.Time `json:"at"`
	Stream string    `json:"stream"` // "stdout" or "stderr"
	Text   string    `json:"text"`
}

const (
	StreamStdout = "stdout"
	StreamStderr = "stderr"
)

// Artifact is one file the agent produced.
type Artifact struct {
	// Name is the path relative to the environment's artifact directory.
	Name string `json:"name"`
	// Path is where it now lives on the host, after Collect.
	Path string `json:"path"`
	Size int64  `json:"size"`
	// ContentType is sniffed from the extension; it drives inline rendering of
	// screenshots and video in the flight recorder.
	ContentType string `json:"content_type"`
}

// ErrNotFound reports that an environment no longer exists. Destroy must treat
// it as success; Describe should return PhaseGone rather than this error.
var ErrNotFound = fmt.Errorf("environment not found")

// UnsupportedError reports that a driver cannot honour part of a spec. Returned
// at Create rather than ignored, so a caller who asked for an isolation or
// network guarantee never believes they got one they did not.
type UnsupportedError struct {
	Driver  string
	Feature string
}

func (e *UnsupportedError) Error() string {
	return fmt.Sprintf("driver %q cannot enforce %s", e.Driver, e.Feature)
}
