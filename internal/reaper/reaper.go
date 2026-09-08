// Package reaper destroys environments that should no longer exist.
//
// This is the layer that makes the cost guarantee true rather than aspirational.
// The system's promise is that no environment outlives its deadline, and the
// scheduler already destroys environments on every path out of a job — but the
// scheduler is code in a process, and processes die. Everything here exists for
// the cases where the thing that was supposed to clean up did not.
//
// # Three layers, and why one is not enough
//
//  1. In-environment deadline. The environment terminates itself. Survives the
//     control plane crashing, the network partitioning, and the scheduler
//     forgetting. Fails if the environment's own supervision is broken — a
//     wedged kernel, a driver whose timeout mechanism did not arm.
//
//  2. This sweeper. Enumerates what actually exists through the driver, not
//     what we remember creating, and destroys anything past its deadline or
//     belonging to no live job. Survives a scheduler crash mid-job. Fails if
//     the control plane is down altogether.
//
//  3. An infrastructure backstop outside this process entirely: an ECS task
//     stopTimeout, an instance TTL tag with a Lambda sweeper, a
//     `docker run --stop-timeout`. Survives everything above being dead.
//     Implemented in the Terraform and the driver configuration, not here,
//     because a backstop that runs inside the thing it is backing up is not a
//     backstop.
//
// Each layer covers the previous one's failure mode. Layer 2 is the only one
// that can explain *why* something was reaped, which is why it does the
// reporting even though it is not the last line of defence.
package reaper

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/bytes-as/ephemera/internal/driver"
	"github.com/bytes-as/ephemera/internal/job"
	"github.com/bytes-as/ephemera/internal/queue"
)

// Config tunes the sweeper.
type Config struct {
	// Interval is how often to sweep.
	Interval time.Duration

	// MaxLifetime is an absolute cap on how long any environment may exist,
	// regardless of what deadline its job requested.
	//
	// It exists because per-job deadlines are set by callers, and a caller who
	// asks for no deadline, or whose deadline was never recorded because the
	// control plane died between Create and the write, would otherwise have an
	// environment that lives forever. This is the number an operator can point
	// at when asked "what is the worst case?".
	MaxLifetime time.Duration

	// Grace is how long an environment with no matching live job is tolerated
	// before being destroyed.
	//
	// Not zero, because there is a legitimate window: a scheduler has just
	// called Create and has not yet written the job record. Reaping in that
	// window would kill jobs the moment they start, so the sweeper waits long
	// enough for that write to land.
	Grace time.Duration
}

// DefaultConfig returns conservative sweeper settings.
func DefaultConfig() Config {
	return Config{
		Interval:    30 * time.Second,
		MaxLifetime: time.Hour,
		Grace:       2 * time.Minute,
	}
}

func (c Config) Validate() error {
	if c.Interval <= 0 {
		return errors.New("reaper: Interval must be positive")
	}
	if c.MaxLifetime <= 0 {
		return errors.New("reaper: MaxLifetime must be positive")
	}
	if c.Grace < 0 {
		return errors.New("reaper: Grace must not be negative")
	}
	return nil
}

// Reason records why an environment was destroyed. Kept distinct because they
// mean different things to an operator: a deadline reap is the system working,
// an orphan reap is the system recovering from a bug.
type Reason string

const (
	// ReasonDeadline means the environment outlived its own deadline. Expected
	// when an agent hangs; the in-environment layer should usually catch this
	// first, so seeing it here means that layer did not fire.
	ReasonDeadline Reason = "deadline_exceeded"

	// ReasonMaxLifetime means the environment hit the absolute cap. Always
	// worth investigating: it means a deadline was missing or ignored.
	ReasonMaxLifetime Reason = "max_lifetime_exceeded"

	// ReasonOrphaned means no live job claims this environment. The control
	// plane crashed between creating it and finishing with it.
	ReasonOrphaned Reason = "orphaned"
)

// Reaped describes one destroyed environment.
type Reaped struct {
	EnvID    string    `json:"env_id"`
	JobID    string    `json:"job_id,omitempty"`
	TenantID string    `json:"tenant_id,omitempty"`
	Reason   Reason    `json:"reason"`
	Age      string    `json:"age"`
	At       time.Time `json:"at"`
}

// Result summarises a sweep.
type Result struct {
	Scanned int      `json:"scanned"`
	Reaped  []Reaped `json:"reaped,omitempty"`
	// Failed counts environments we tried and failed to destroy. These are the
	// ones still costing money, so they are counted separately from successes.
	Failed int `json:"failed"`
}

// Reaper destroys environments that should not exist.
type Reaper struct {
	cfg    Config
	driver driver.Driver
	queue  queue.Queue
	log    *slog.Logger
	now    func() time.Time
}

// Deps are the reaper's collaborators. Queue is optional: without it the
// sweeper still enforces deadlines and the lifetime cap, but cannot tell an
// orphan from a running job, so it will not reap for that reason.
type Deps struct {
	Driver driver.Driver
	Queue  queue.Queue
	Logger *slog.Logger
	Clock  func() time.Time
}

// New creates a Reaper.
func New(cfg Config, deps Deps) (*Reaper, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if deps.Driver == nil {
		return nil, errors.New("reaper: Driver is required")
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	if deps.Clock == nil {
		deps.Clock = time.Now
	}
	return &Reaper{
		cfg:    cfg,
		driver: deps.Driver,
		queue:  deps.Queue,
		log:    deps.Logger,
		now:    deps.Clock,
	}, nil
}

// Run sweeps on an interval until ctx is cancelled.
func (r *Reaper) Run(ctx context.Context) error {
	caps := r.driver.Capabilities()
	if !caps.SurvivesControlPlaneRestart {
		// Worth saying out loud at startup rather than discovering during an
		// incident: with such a driver, layer 2 cannot see environments this
		// process did not create, so the infrastructure backstop is the only
		// thing standing between a crash and an unbounded bill.
		r.log.Warn("driver cannot enumerate environments across a restart; "+
			"orphans from a previous process will not be reaped by the sweeper",
			"driver", r.driver.Name())
	}

	r.log.Info("reaper starting",
		"interval", r.cfg.Interval,
		"max_lifetime", r.cfg.MaxLifetime,
		"grace", r.cfg.Grace)

	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()

	// Sweep immediately. On startup the interesting environments are the ones
	// the previous process left behind, and waiting a full interval to collect
	// them is a full interval of paying for nothing.
	r.sweepAndLog(ctx)

	for {
		select {
		case <-ctx.Done():
			r.log.Info("reaper stopped")
			return nil
		case <-ticker.C:
			r.sweepAndLog(ctx)
		}
	}
}

func (r *Reaper) sweepAndLog(ctx context.Context) {
	result, err := r.Sweep(ctx, r.now())
	if err != nil {
		r.log.Error("sweep failed", "error", err)
		return
	}
	for _, reaped := range result.Reaped {
		// Always a warning, never info. Every reap means something upstream
		// failed to clean up after itself, and a system where reaps are routine
		// has a bug it has learned to live with.
		r.log.Warn("reaped environment",
			"env", reaped.EnvID,
			"job", reaped.JobID,
			"tenant", reaped.TenantID,
			"reason", reaped.Reason,
			"age", reaped.Age)
	}
	if result.Failed > 0 {
		r.log.Error("environments could not be destroyed and are still consuming resources",
			"count", result.Failed)
	}
}

// Sweep destroys everything that should not exist, and reports what it did.
//
// Exported and side-effect-explicit so it can be triggered on demand and
// asserted on in tests, rather than being a background goroutine whose
// behaviour can only be observed indirectly.
func (r *Reaper) Sweep(ctx context.Context, now time.Time) (Result, error) {
	envs, err := r.driver.List(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("reaper: list environments: %w", err)
	}

	result := Result{Scanned: len(envs)}
	for _, env := range envs {
		if err := ctx.Err(); err != nil {
			return result, err
		}

		reason, ok := r.shouldReap(ctx, env, now)
		if !ok {
			continue
		}

		age := now.Sub(env.CreatedAt)
		if err := r.driver.Destroy(ctx, env); err != nil {
			r.log.Error("could not destroy environment",
				"env", env.ID, "job", env.JobID, "reason", reason, "error", err)
			result.Failed++
			continue
		}

		result.Reaped = append(result.Reaped, Reaped{
			EnvID:    env.ID,
			JobID:    env.JobID,
			TenantID: env.TenantID,
			Reason:   reason,
			Age:      age.Round(time.Second).String(),
			At:       now,
		})

		r.markJobReaped(ctx, env, reason)
	}
	return result, nil
}

// shouldReap decides whether an environment must go, and why.
func (r *Reaper) shouldReap(ctx context.Context, env driver.Env, now time.Time) (Reason, bool) {
	age := now.Sub(env.CreatedAt)

	// The absolute cap first: it is the guarantee an operator quotes, so it
	// must apply even to environments whose deadline is missing or absurd.
	if age > r.cfg.MaxLifetime {
		return ReasonMaxLifetime, true
	}

	if env.Expired(now) {
		return ReasonDeadline, true
	}

	// Orphan detection needs the queue. Without it we cannot distinguish a
	// running job from an abandoned one, and guessing would kill live work.
	if r.queue == nil {
		return "", false
	}

	// Respect the grace window: a scheduler may have just created this
	// environment and not yet written the job record.
	if age < r.cfg.Grace {
		return "", false
	}

	if env.JobID == "" {
		// No job association at all, and old enough that it was not a race.
		return ReasonOrphaned, true
	}

	j, err := r.queue.Get(ctx, env.JobID)
	if errors.Is(err, queue.ErrNotFound) {
		return ReasonOrphaned, true
	}
	if err != nil {
		// Storage is unhappy. Do not reap on incomplete information: destroying
		// a live job's environment is worse than paying for an extra interval.
		r.log.Warn("could not check job while sweeping; leaving environment alone",
			"env", env.ID, "job", env.JobID, "error", err)
		return "", false
	}
	if j.State.Terminal() {
		// The job finished but its environment survived - the scheduler's
		// teardown failed, or the process died between the two.
		return ReasonOrphaned, true
	}
	return "", false
}

// markJobReaped records on the job that its environment was destroyed
// underneath it, so the caller learns why their job stopped.
func (r *Reaper) markJobReaped(ctx context.Context, env driver.Env, reason Reason) {
	if r.queue == nil || env.JobID == "" {
		return
	}
	j, err := r.queue.Get(ctx, env.JobID)
	if err != nil {
		return
	}
	if j.State.Terminal() {
		return // Already resolved; the environment was merely lingering.
	}

	failure := job.FailureReapedOrphan.Newf("environment destroyed by the reaper: %s", reason)
	if reason == ReasonDeadline || reason == ReasonMaxLifetime {
		// Attribute this accurately: the job ran out of time, which is a user
		// fault, rather than being blamed on a control-plane failure.
		failure = job.FailureDeadline.Newf("environment exceeded its lifetime and was reaped (%s)", reason)
	}

	if err := j.Fail(failure, r.now()); err != nil {
		r.log.Error("could not mark reaped job as failed", "job", j.ID, "error", err)
		return
	}
	if err := r.queue.Complete(ctx, j); err != nil {
		r.log.Error("could not persist reaped job", "job", j.ID, "error", err)
	}
}
