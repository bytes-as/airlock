// Package scheduler runs queued jobs through a driver, with bounded
// concurrency and a defined behaviour for every way a job can go wrong.
//
// The worker loop is deliberately boring. All the judgement lives in two
// places: what to do when something fails, and what guarantees hold when this
// process dies mid-job. Everything else is plumbing.
//
// The invariant the whole package is built to preserve: no environment outlives
// the job that created it. Every path out of runJob destroys the environment,
// including the panic path, because a leaked environment costs money for as
// long as it lives.
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/bytes-as/ephemera/internal/admission"
	"github.com/bytes-as/ephemera/internal/artifact"
	"github.com/bytes-as/ephemera/internal/driver"
	"github.com/bytes-as/ephemera/internal/job"
	"github.com/bytes-as/ephemera/internal/logstream"
	"github.com/bytes-as/ephemera/internal/queue"
	"github.com/bytes-as/ephemera/internal/secrets"
)

// Config tunes the scheduler.
type Config struct {
	// Workers is the number of jobs that may run simultaneously in this
	// process. This is the real concurrency limit: admission quotas bound what
	// one tenant may take of it.
	Workers int

	// LeaseTTL is how long a claimed job stays claimed without a heartbeat.
	// Too short and a healthy worker loses jobs to a GC pause; too long and a
	// dead worker's jobs sit idle. It should comfortably exceed
	// HeartbeatInterval.
	LeaseTTL time.Duration

	// HeartbeatInterval is how often a worker renews its lease.
	HeartbeatInterval time.Duration

	// RecoveryInterval is how often lapsed leases are returned to the queue.
	RecoveryInterval time.Duration

	// IdlePoll is how long a worker waits before re-checking an empty queue.
	IdlePoll time.Duration

	// TenantBackoff is how long a worker waits after finding the head of the
	// queue belongs to a tenant at its concurrency quota. See the head-of-line
	// note on claimOne.
	TenantBackoff time.Duration

	// ProvisionTimeout bounds Create and Start - everything before the agent is
	// running.
	//
	// This is the "is it wedged before it even started?" check, and it exists
	// because nothing else can make it. A job's deadline is enforced inside the
	// environment and by the reaper, and both need an environment to act on: if
	// Create never returns, there is nothing to reap, the lease keeps being
	// renewed by a worker that is blocked in a driver call, and that worker is
	// gone for good. Enough of those and the pool is deadlocked with an empty
	// queue and no error anywhere.
	//
	// A daemon that has stopped answering, an image pull that stalls, an ECS
	// API call that hangs - all produce exactly that. Bounded here so they
	// produce a retryable provisioning failure instead.
	ProvisionTimeout time.Duration

	// DefaultDeadline applies to jobs that specify none.
	DefaultDeadline time.Duration

	// MaxDeadline caps what a caller may request. This is a cost control: a
	// caller asking for a 24-hour deadline is asking to spend 24 hours of
	// compute, and that decision belongs to the operator.
	MaxDeadline time.Duration

	// DefaultMaxAttempts bounds retries for jobs that specify none.
	DefaultMaxAttempts int

	// ArtifactDir is the path inside environments where agents write outputs.
	ArtifactDir string

	// DenyCIDRs is the egress deny list applied to every environment.
	DenyCIDRs []string

	// NetworkMode is the egress posture for environments.
	NetworkMode driver.NetworkMode
}

// DefaultConfig returns settings suitable for a single-host deployment.
func DefaultConfig() Config {
	return Config{
		Workers:            8,
		LeaseTTL:           2 * time.Minute,
		HeartbeatInterval:  20 * time.Second,
		RecoveryInterval:   30 * time.Second,
		IdlePoll:           200 * time.Millisecond,
		TenantBackoff:      500 * time.Millisecond,
		ProvisionTimeout:   2 * time.Minute,
		DefaultDeadline:    5 * time.Minute,
		MaxDeadline:        30 * time.Minute,
		DefaultMaxAttempts: 3,
		ArtifactDir:        "/artifacts",
		NetworkMode:        driver.NetworkEgress,
		DenyCIDRs:          driver.DefaultDenyCIDRs(),
	}
}

// Validate reports configuration that would misbehave at runtime.
func (c Config) Validate() error {
	if c.Workers <= 0 {
		return errors.New("scheduler: Workers must be positive")
	}
	if c.LeaseTTL <= 0 || c.HeartbeatInterval <= 0 {
		return errors.New("scheduler: LeaseTTL and HeartbeatInterval must be positive")
	}
	if c.HeartbeatInterval >= c.LeaseTTL {
		// Otherwise a worker's lease expires before it renews, and healthy jobs
		// get taken away from workers that are doing nothing wrong.
		return fmt.Errorf("scheduler: HeartbeatInterval (%s) must be shorter than LeaseTTL (%s)",
			c.HeartbeatInterval, c.LeaseTTL)
	}
	if c.ProvisionTimeout <= 0 {
		return errors.New("scheduler: ProvisionTimeout must be positive, or a wedged driver strands a worker forever")
	}
	if c.MaxDeadline > 0 && c.DefaultDeadline > c.MaxDeadline {
		return fmt.Errorf("scheduler: DefaultDeadline (%s) exceeds MaxDeadline (%s)",
			c.DefaultDeadline, c.MaxDeadline)
	}
	return nil
}

// Scheduler dispatches queued jobs to a driver.
type Scheduler struct {
	cfg Config

	queue     queue.Queue
	driver    driver.Driver
	admission *admission.Controller
	secrets   secrets.Resolver
	artifacts artifact.Store
	logs      *logstream.Broker
	log       *slog.Logger
	now       func() time.Time
}

// Deps are the collaborators a Scheduler needs.
type Deps struct {
	Queue     queue.Queue
	Driver    driver.Driver
	Admission *admission.Controller
	Secrets   secrets.Resolver
	Artifacts artifact.Store
	Logs      *logstream.Broker
	Logger    *slog.Logger
	Clock     func() time.Time
}

// New creates a Scheduler.
func New(cfg Config, deps Deps) (*Scheduler, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if deps.Queue == nil || deps.Driver == nil {
		return nil, errors.New("scheduler: Queue and Driver are required")
	}
	if deps.Admission == nil {
		deps.Admission = admission.New(admission.DefaultLimits())
	}
	if deps.Logs == nil {
		deps.Logs = logstream.NewBroker()
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	if deps.Clock == nil {
		deps.Clock = time.Now
	}
	if err := checkDriverCanHonourConfig(cfg, deps.Driver); err != nil {
		return nil, err
	}
	return &Scheduler{
		cfg:       cfg,
		queue:     deps.Queue,
		driver:    deps.Driver,
		admission: deps.Admission,
		secrets:   deps.Secrets,
		artifacts: deps.Artifacts,
		logs:      deps.Logs,
		log:       deps.Logger,
		now:       deps.Clock,
	}, nil
}

// Run starts the worker pool and the lease-recovery loop, blocking until ctx is
// cancelled and every worker has finished the job it was holding.
//
// Draining rather than aborting on shutdown is the point: a worker killed
// mid-job leaves an environment behind, and while the reaper would eventually
// collect it, "eventually" costs money. Finishing the job in hand is cheaper
// and produces a result the caller asked for.
func (s *Scheduler) Run(ctx context.Context) error {
	s.log.Info("scheduler starting",
		"workers", s.cfg.Workers,
		"driver", s.driver.Name(),
		"isolation", s.driver.Capabilities().Isolation.String())

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.recoveryLoop(ctx)
	}()

	for i := 0; i < s.cfg.Workers; i++ {
		workerID := fmt.Sprintf("worker-%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.worker(ctx, workerID)
		}()
	}

	wg.Wait()
	s.log.Info("scheduler stopped")
	return nil
}

// worker claims and runs jobs until the context is cancelled.
func (s *Scheduler) worker(ctx context.Context, workerID string) {
	for {
		if ctx.Err() != nil {
			return
		}
		wait := s.claimOne(ctx, workerID)
		if wait > 0 {
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return
			}
		}
	}
}

// claimOne attempts to claim and run a single job, returning how long to wait
// before trying again.
func (s *Scheduler) claimOne(ctx context.Context, workerID string) time.Duration {
	j, err := s.queue.Claim(ctx, workerID, s.cfg.LeaseTTL)
	switch {
	case errors.Is(err, queue.ErrEmpty):
		return s.cfg.IdlePoll
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return 0
	case err != nil:
		s.log.Error("claim failed", "worker", workerID, "error", err)
		return s.cfg.IdlePoll
	}

	// The tenant may be at its concurrency quota even though its job is at the
	// head of the queue. Put it back and wait.
	//
	// Known limitation: this is head-of-line blocking. A saturated tenant at
	// the head makes workers claim-and-release in a loop, wasting a little work
	// and delaying jobs behind it. TenantBackoff bounds the waste. The real fix
	// is per-tenant ready queues with round-robin selection, so a blocked
	// tenant is skipped rather than retried - deliberately not built here,
	// because it needs a queue interface that can scan past the head, and at
	// this scale the backoff is sufficient.
	if !s.admission.HasCapacity(j.TenantID) {
		if err := s.queue.Release(ctx, j.ID, workerID, true); err != nil {
			s.log.Error("release after quota block failed", "job", j.ID, "error", err)
		}
		return s.cfg.TenantBackoff
	}

	s.admission.Started(j.TenantID)
	defer s.admission.Finished(j.TenantID)

	s.runJob(ctx, workerID, j)
	return 0
}

// runJob takes a claimed job through its whole lifecycle.
//
// Every exit path destroys the environment. That is the one thing this function
// must not get wrong.
func (s *Scheduler) runJob(ctx context.Context, workerID string, j *job.Job) {
	log := s.log.With("job", j.ID, "tenant", j.TenantID, "worker", workerID, "attempt", j.Attempts)
	started := s.now()

	// Queue writes must outlive shutdown. A result we computed and then failed
	// to record is the worst outcome available: the work was done, the compute
	// was paid for, and we threw the answer away because someone pressed ctrl-C.
	persistCtx, cancelPersist := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancelPersist()

	// The job's own context, detached from shutdown. A job in flight runs to
	// its own deadline or completion; cancelling it on shutdown would strand an
	// environment we are about to stop watching.
	runCtx, cancelRun := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelRun()

	// Losing the lease means another worker may have taken this job. Stop
	// immediately rather than race it to write a result.
	leaseLost := make(chan struct{})
	var leaseOnce sync.Once
	stopHeartbeat := s.startHeartbeat(runCtx, j.ID, workerID, func() {
		leaseOnce.Do(func() { close(leaseLost) })
	})
	defer stopHeartbeat()

	spec, failure := s.buildEnvSpec(runCtx, j)
	if failure != nil {
		s.finish(persistCtx, workerID, log, j, failure, started)
		return
	}

	s.logs.Note(j.ID, "provisioning %s environment", s.driver.Name())

	// Bounded: see Config.ProvisionTimeout. A hang here is invisible to every
	// other safety net, because none of them have an environment to act on yet.
	createCtx, cancelCreate := context.WithTimeout(runCtx, s.cfg.ProvisionTimeout)
	env, err := s.driver.Create(createCtx, spec)
	cancelCreate()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) && runCtx.Err() == nil {
			// Distinguished from an ordinary provisioning error because the
			// remedy is different: this one means the driver or the thing
			// behind it stopped answering, not that the spec was wrong.
			s.finish(persistCtx, workerID, log, j,
				job.FailureProvision.Newf("environment did not provision within %s; the driver stopped responding", s.cfg.ProvisionTimeout),
				started)
			return
		}
		var unsupported *driver.UnsupportedError
		if errors.As(err, &unsupported) {
			// The driver cannot honour the spec. Retrying will not change that,
			// and it is the caller's spec that is wrong.
			//
			// The driver's own message is used verbatim rather than replaced
			// with a generic one: these refusals are written to name what was
			// asked for and what is available instead ("cannot enforce egress
			// from region \"antarctica\" (configured regions: eu, us)"), and
			// flattening that to "cannot honour this spec" throws away the only
			// part the caller can act on.
			s.finish(persistCtx, workerID, log, j, job.FailureRejected.Wrap(err, "%v", err), started)
			return
		}
		s.finish(persistCtx, workerID, log, j, job.FailureProvision.Wrap(err, "could not create environment"), started)
		return
	}

	// Record the environment on the job before anything else can fail. If this
	// process dies now, the reaper still finds the environment through the
	// driver's List, but a job record naming its environment makes the
	// investigation a lookup rather than a search.
	j.EnvID = env.ID
	log = log.With("env", env.ID)

	// pumpDone is closed when the log pump has drained. Nil until the pump
	// starts, so teardown can tell "not started" from "not finished" — a
	// receive on a nil channel blocks forever, and blocking here would hang
	// every job that failed before it ever ran.
	var pumpDone chan struct{}

	// teardown drains the job's output and destroys its environment. It runs
	// at most once, and it must complete before the job is reported terminal.
	//
	// That ordering is the contract: when a caller sees a job in a terminal
	// state, its output has been captured and its environment no longer exists.
	// Reporting "succeeded" while cleanup is still in flight would make the API
	// lie in the two ways that matter most - an operator reading logs that are
	// still arriving, and a cost dashboard showing an environment for a job
	// that is already done.
	var teardownOnce sync.Once
	teardown := func() {
		teardownOnce.Do(func() {
			// Wait for the log pump first: destroying the environment removes
			// the file the pump is reading, and the tail of a failing job's
			// output is exactly the part somebody needed.
			if pumpDone != nil {
				select {
				case <-pumpDone:
				case <-time.After(10 * time.Second):
					log.Warn("log pump did not finish; some agent output may be missing")
				}
			}

			// Its own context, so teardown still runs during shutdown when the
			// job's context is already cancelled.
			destroyCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			if err := s.driver.Destroy(destroyCtx, env); err != nil {
				// The expensive failure: an environment we could not tear down.
				// Log loudly - the reaper is the backstop, but a destroy
				// failure means something is wrong with the runtime itself.
				log.Error("failed to destroy environment; reaper must collect it", "error", err)
				s.logs.Note(j.ID, "warning: environment teardown failed, reaper will collect it")
			} else {
				log.Debug("environment destroyed")
			}
			s.logs.End(j.ID)
		})
	}
	// Backstop for every path that returns without calling teardown explicitly,
	// including a panic. Idempotent, so calling it twice costs nothing.
	defer teardown()

	startCtx, cancelStart := context.WithTimeout(runCtx, s.cfg.ProvisionTimeout)
	err = s.driver.Start(startCtx, env, spec)
	cancelStart()
	if err != nil {
		teardown()
		if errors.Is(err, context.DeadlineExceeded) && runCtx.Err() == nil {
			s.finish(persistCtx, workerID, log, j,
				job.FailureStart.Newf("agent did not start within %s; the environment came up but never became ready", s.cfg.ProvisionTimeout),
				started)
			return
		}
		s.finish(persistCtx, workerID, log, j, job.FailureStart.Wrap(err, "could not start agent"), started)
		return
	}

	if err := j.Transition(job.StateRunning, s.now()); err != nil {
		teardown()
		s.finish(persistCtx, workerID, log, j, job.FailureInternal.Wrap(err, "illegal state transition"), started)
		return
	}
	// Persist the running state so the API reports what is actually happening
	// rather than the state the job was in when it was claimed. Not fatal if it
	// fails: the job is running either way, and killing it over a bookkeeping
	// write would be a poor trade.
	if err := s.queue.Update(persistCtx, j); err != nil {
		log.Warn("could not persist running state", "error", err)
	}
	s.logs.Note(j.ID, "agent started")
	log.Info("job running")

	// Pump driver logs into the broker, redacting any secret the agent echoes.
	redactor := secrets.NewRedactor(spec.Secrets)
	pumpDone = make(chan struct{})
	go func() {
		defer close(pumpDone)
		s.pumpLogs(runCtx, j.ID, env, redactor)
	}()

	// Wait for the agent, the lease, or the caller.
	type result struct {
		status driver.Status
		err    error
	}
	done := make(chan result, 1)
	go func() {
		status, err := s.driver.Wait(runCtx, env)
		done <- result{status, err}
	}()

	var status driver.Status
	select {
	case r := <-done:
		if r.err != nil {
			teardown()
			s.finish(persistCtx, workerID, log, j, job.FailureInternal.Wrap(r.err, "could not determine job outcome"), started)
			return
		}
		status = r.status
	case <-leaseLost:
		log.Warn("lease lost while running; abandoning to avoid double execution")
		s.logs.Note(j.ID, "control plane lost this job's lease; stopping")
		// Do not write a result: another worker owns this job now. The deferred
		// destroy still runs, so the environment does not leak.
		return
	}

	// Collect artifacts before teardown removes the environment. This is the
	// only window in which its filesystem still exists.
	s.collectArtifacts(runCtx, log, j, env)

	// Tear down before recording the outcome, so a terminal job always means a
	// destroyed environment and a complete log.
	teardown()

	if failure := status.Failure(); failure != nil {
		s.finish(persistCtx, workerID, log, j, failure, started)
		return
	}

	if err := j.Transition(job.StateSucceeded, s.now()); err != nil {
		s.finish(persistCtx, workerID, log, j, job.FailureInternal.Wrap(err, "illegal state transition"), started)
		return
	}
	if err := s.queue.Complete(persistCtx, j); err != nil {
		log.Error("could not record success", "error", err)
	}
	log.Info("job succeeded", "duration", s.now().Sub(started).Round(time.Millisecond))
	s.logs.Note(j.ID, "job succeeded")
}

// buildEnvSpec turns a job spec into a driver spec, resolving secrets.
func (s *Scheduler) buildEnvSpec(ctx context.Context, j *job.Job) (driver.EnvSpec, *job.Failure) {
	deadline := j.Spec.Deadline
	if deadline <= 0 {
		deadline = s.cfg.DefaultDeadline
	}
	if s.cfg.MaxDeadline > 0 && deadline > s.cfg.MaxDeadline {
		// Clamp rather than reject: the caller asked for more time than the
		// operator permits, and running with the permitted maximum is more
		// useful than refusing outright. The clamp is announced in the log so
		// nobody is surprised by an early deadline.
		s.logs.Note(j.ID, "requested deadline %s exceeds the maximum %s; using the maximum",
			deadline, s.cfg.MaxDeadline)
		deadline = s.cfg.MaxDeadline
	}

	var resolved map[string]job.Secret
	if len(j.Spec.Secrets) > 0 {
		if s.secrets == nil {
			return driver.EnvSpec{}, job.FailureRejected.Newf("job requests secrets but no secret resolver is configured")
		}
		var err error
		resolved, err = s.secrets.Resolve(ctx, j.Spec.Secrets)
		if err != nil {
			// A user fault: they referenced something that is not there. The
			// error names the reference, never the value.
			return driver.EnvSpec{}, job.FailureRejected.Wrap(err, "could not resolve job secrets")
		}
	}

	return driver.EnvSpec{
		JobID:       j.ID,
		TenantID:    j.TenantID,
		Image:       j.Spec.Image,
		Command:     j.Spec.Command,
		Env:         j.Spec.Env,
		Secrets:     resolved,
		Resources:   j.Spec.Resources,
		Deadline:    deadline,
		ArtifactDir: s.cfg.ArtifactDir,
		Network: driver.NetworkPolicy{
			Mode:      s.cfg.NetworkMode,
			DenyCIDRs: s.cfg.DenyCIDRs,
			// Per job, so one tenant can ask to appear in Frankfurt while
			// everything else rotates freely. The driver refuses what it cannot
			// serve, which surfaces as a rejected job rather than a silent
			// substitution.
			EgressRegion: j.Spec.EgressRegion,
		},
		Labels: map[string]string{
			"ephemera.job":    j.ID,
			"ephemera.tenant": j.TenantID,
		},
	}, nil
}

// pumpLogs forwards driver output into the broker, redacted.
func (s *Scheduler) pumpLogs(ctx context.Context, jobID string, env driver.Env, redactor *secrets.Redactor) {
	lines, err := s.driver.Logs(ctx, env)
	if err != nil {
		s.log.Warn("could not attach to job logs", "job", jobID, "error", err)
		return
	}
	for line := range lines {
		line.Text = redactor.Redact(line.Text)
		s.logs.Publish(jobID, line)
	}
}

// collectArtifacts gathers outputs into the store. A failure here does not fail
// the job: the work was done, and losing the screenshots is worth reporting but
// not worth discarding a successful run over.
func (s *Scheduler) collectArtifacts(ctx context.Context, log *slog.Logger, j *job.Job, env driver.Env) {
	if s.artifacts == nil {
		return
	}
	staging, err := stagingDir(j.ID)
	if err != nil {
		log.Error("could not prepare artifact staging", "error", err)
		return
	}
	defer removeAll(staging)

	collected, err := s.driver.Collect(ctx, env, staging)
	if err != nil {
		log.Error("could not collect artifacts", "error", err)
		s.logs.Note(j.ID, "warning: artifact collection failed: %v", err)
		return
	}
	if len(collected) == 0 {
		return
	}
	stored, err := s.artifacts.Put(ctx, j.ID, collected)
	if err != nil {
		log.Error("could not store artifacts", "error", err)
		s.logs.Note(j.ID, "warning: artifact storage failed: %v", err)
		return
	}
	log.Info("artifacts stored", "count", len(stored))
	s.logs.Note(j.ID, "stored %d artifact(s)", len(stored))
}

// finish records a failure, retrying if the taxonomy says a retry could help.
func (s *Scheduler) finish(ctx context.Context, workerID string, log *slog.Logger, j *job.Job, failure *job.Failure, started time.Time) {
	if j.ShouldRetry(failure.Kind, s.cfg.DefaultMaxAttempts) {
		log.Warn("job failed, retrying",
			"kind", failure.Kind, "reason", failure.Message, "attempts", j.Attempts)
		s.logs.Note(j.ID, "attempt %d failed (%s); requeueing", j.Attempts, failure.Kind)

		// Release with requeue. The job keeps its original queue position, so a
		// job that has already waited is not punished for an infrastructure
		// failure that was not its fault.
		if err := s.queue.Release(ctx, j.ID, workerID, true); err != nil {
			log.Error("could not requeue after failure", "error", err)
		}
		return
	}

	if err := j.Fail(failure, s.now()); err != nil {
		log.Error("could not record failure", "error", err)
		return
	}
	if err := s.queue.Complete(ctx, j); err != nil {
		log.Error("could not persist failed job", "error", err)
	}

	log.Error("job failed",
		"kind", failure.Kind,
		"fault", failure.Kind.Fault(),
		"reason", failure.Message,
		"attempts", j.Attempts,
		"duration", s.now().Sub(started).Round(time.Millisecond))
	s.logs.Note(j.ID, "job failed: %s", failure.Error())

	// End the stream here, not only in teardown.
	//
	// teardown closes over the environment, so it does not exist for a job that
	// failed before one was created - a rejected spec, a provisioning failure,
	// an unresolvable secret. Those jobs reached a terminal state with their log
	// stream still open, and every viewer waiting on it hung forever: `run`
	// printed the submission line and then sat there, with the job already
	// failed on the server. End is idempotent, so the teardown path calling it
	// again costs nothing.
	s.logs.End(j.ID)
}

// startHeartbeat renews the job's lease until the returned stop func is called.
// onLost fires if the lease is no longer ours, which means another worker has
// taken the job.
func (s *Scheduler) startHeartbeat(ctx context.Context, jobID, workerID string, onLost func()) func() {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	go func() {
		defer close(done)
		ticker := time.NewTicker(s.cfg.HeartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				err := s.queue.Heartbeat(ctx, jobID, workerID, s.cfg.LeaseTTL)
				switch {
				case err == nil:
				case errors.Is(err, queue.ErrNotLeaseHolder):
					s.log.Warn("lease no longer held", "job", jobID, "worker", workerID)
					onLost()
					return
				case errors.Is(err, context.Canceled):
					return
				default:
					// A transient storage error. Keep trying: the lease has not
					// necessarily lapsed, and giving up here would abandon a
					// healthy job.
					s.log.Warn("heartbeat failed", "job", jobID, "error", err)
				}
			}
		}
	}()

	return func() {
		cancel()
		<-done
	}
}

// recoveryLoop periodically returns lapsed leases to the queue.
func (s *Scheduler) recoveryLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.RecoveryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			recovered, err := s.queue.RecoverExpired(ctx, s.now())
			if err != nil {
				s.log.Error("lease recovery failed", "error", err)
				continue
			}
			for _, j := range recovered {
				// Always worth a warning: a lapsed lease means a worker died or
				// stalled, and that is not normal operation.
				s.log.Warn("recovered job from a lapsed lease",
					"job", j.ID, "tenant", j.TenantID, "attempts", j.Attempts)
				s.logs.Note(j.ID, "recovered after its worker stopped responding")
			}
		}
	}
}

// Stats reports current dispatch state, for the API and for metrics.
func (s *Scheduler) Stats(ctx context.Context) (Stats, error) {
	qs, err := s.queue.Stats(ctx)
	if err != nil {
		return Stats{}, err
	}
	return Stats{
		Queue:       qs,
		Workers:     s.cfg.Workers,
		Running:     s.admission.TotalRunning(),
		Driver:      s.driver.Name(),
		Isolation:   s.driver.Capabilities().Isolation.String(),
		TenantUsage: s.admission.Snapshot(),
	}, nil
}

// Stats is a point-in-time view of the scheduler.
type Stats struct {
	Queue       queue.Stats                      `json:"queue"`
	Workers     int                              `json:"workers"`
	Running     int                              `json:"running"`
	Driver      string                           `json:"driver"`
	Isolation   string                           `json:"isolation"`
	TenantUsage map[string]admission.TenantUsage `json:"tenant_usage,omitempty"`
}

// stagingDir creates a temporary directory to collect artifacts into before
// they are handed to the store.
//
// Collection is staged rather than written straight into the store because a
// partial collection must not become a partial set of "stored" artifacts that
// an operator then trusts as complete.
func stagingDir(jobID string) (string, error) {
	dir, err := os.MkdirTemp("", "ephemera-artifacts-"+sanitiseForPath(jobID)+"-*")
	if err != nil {
		return "", fmt.Errorf("scheduler: create artifact staging dir: %w", err)
	}
	return dir, nil
}

func removeAll(dir string) {
	if dir == "" {
		return
	}
	_ = os.RemoveAll(dir)
}

// sanitiseForPath strips anything from an identifier that a filesystem would
// object to, so a hostile job ID cannot influence where staging happens.
func sanitiseForPath(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '-':
			out = append(out, r)
		}
	}
	if len(out) == 0 {
		return "job"
	}
	return string(out)
}

// checkDriverCanHonourConfig refuses to start when the configuration demands a
// control the driver does not implement.
//
// Failing at startup rather than per-job is the whole point. A mismatch here is
// an operator error, and the two bad alternatives are worse: silently dropping
// the control leaves the operator believing jobs are isolated when they are
// not, and failing every job at Create turns a config typo into an outage that
// looks like a driver bug.
//
// The message names the fix, because the person reading it at 3am is the person
// who has to apply it.
func checkDriverCanHonourConfig(cfg Config, d driver.Driver) error {
	caps := d.Capabilities()

	needsNetworkPolicy := len(cfg.DenyCIDRs) > 0 ||
		(cfg.NetworkMode != "" && cfg.NetworkMode != driver.NetworkEgress)

	if needsNetworkPolicy && !caps.EnforcesNetworkPolicy {
		return fmt.Errorf(
			"scheduler: configuration requires network policy (mode %q, %d deny CIDRs) "+
				"but the %q driver does not enforce it; use the docker or fargate driver, "+
				"or clear NetworkMode and DenyCIDRs to run without egress isolation",
			cfg.NetworkMode, len(cfg.DenyCIDRs), d.Name())
	}
	return nil
}

// RelaxedFor returns a copy of the config with controls the driver cannot
// enforce removed.
//
// This exists so a composition root can deliberately and visibly downgrade -
// "I know the process driver has no network isolation and I accept that for
// local development". It returns the list of what it dropped so the caller can
// warn about it. Never call this to make an error go away: the dropped controls
// are real, and someone must be told they are gone.
func (c Config) RelaxedFor(d driver.Driver) (Config, []string) {
	caps := d.Capabilities()
	var dropped []string

	if !caps.EnforcesNetworkPolicy {
		if len(c.DenyCIDRs) > 0 {
			dropped = append(dropped, fmt.Sprintf("egress deny list (%d CIDRs)", len(c.DenyCIDRs)))
			c.DenyCIDRs = nil
		}
		if c.NetworkMode != "" && c.NetworkMode != driver.NetworkEgress {
			dropped = append(dropped, fmt.Sprintf("network mode %q", c.NetworkMode))
		}
		c.NetworkMode = driver.NetworkEgress
	}
	return c, dropped
}
