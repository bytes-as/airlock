package reaper

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bytes-as/ephemera/internal/driver"
	"github.com/bytes-as/ephemera/internal/driver/process"
	"github.com/bytes-as/ephemera/internal/job"
	"github.com/bytes-as/ephemera/internal/queue/embedded"
)

// Like the driver and scheduler tests, these run real environments: a real
// process driver with real child processes, and a real bbolt queue. The clock
// is the only thing injected, because waiting an hour to test a one-hour
// lifetime cap is not a test anyone runs twice.

func TestHelperProcess(t *testing.T) {
	if os.Getenv("EPHEMERA_TEST_HELPER") != "1" {
		return
	}
	time.Sleep(60 * time.Second)
	os.Exit(0)
}

var base = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

type fixture struct {
	t      *testing.T
	driver *process.Driver
	queue  *embedded.Queue
	reaper *Reaper
	now    time.Time
}

func newFixture(t *testing.T, cfg Config, withQueue bool) *fixture {
	t.Helper()
	root := t.TempDir()

	f := &fixture{t: t, now: base}

	drv, err := process.New(filepath.Join(root, "envs"), process.WithClock(func() time.Time { return f.now }))
	if err != nil {
		t.Fatalf("new driver: %v", err)
	}
	f.driver = drv

	deps := Deps{
		Driver: drv,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Clock:  func() time.Time { return f.now },
	}
	if withQueue {
		q, err := embedded.Open(filepath.Join(root, "queue.db"), embedded.WithClock(func() time.Time { return f.now }))
		if err != nil {
			t.Fatalf("open queue: %v", err)
		}
		t.Cleanup(func() { q.Close() })
		f.queue = q
		deps.Queue = q
	}

	r, err := New(cfg, deps)
	if err != nil {
		t.Fatalf("new reaper: %v", err)
	}
	f.reaper = r
	return f
}

// createEnv provisions a real environment with the given deadline.
func (f *fixture) createEnv(jobID string, deadline time.Duration) driver.Env {
	f.t.Helper()
	spec := driver.EnvSpec{
		JobID:    jobID,
		TenantID: "tenant-a",
		Command:  []string{os.Args[0], "-test.run=TestHelperProcess"},
		Env:      map[string]string{"EPHEMERA_TEST_HELPER": "1"},
		Deadline: deadline,
	}
	env, err := f.driver.Create(context.Background(), spec)
	if err != nil {
		f.t.Fatalf("create env: %v", err)
	}
	return env
}

func (f *fixture) enqueue(jobID string) *job.Job {
	f.t.Helper()
	j := job.New("tenant-a", job.PriorityNormal, job.Spec{Image: "img"}, f.now)
	j.ID = jobID
	if err := f.queue.Enqueue(context.Background(), j); err != nil {
		f.t.Fatalf("enqueue: %v", err)
	}
	return j
}

func (f *fixture) envCount() int {
	f.t.Helper()
	envs, err := f.driver.List(context.Background())
	if err != nil {
		f.t.Fatalf("list: %v", err)
	}
	return len(envs)
}

func TestReapsEnvironmentPastItsDeadline(t *testing.T) {
	f := newFixture(t, DefaultConfig(), false)
	env := f.createEnv("job_1", 5*time.Minute)

	// Not yet due.
	result, err := f.reaper.Sweep(context.Background(), f.now)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(result.Reaped) != 0 {
		t.Fatalf("reaped a live environment: %+v", result.Reaped)
	}
	if result.Scanned != 1 {
		t.Errorf("Scanned = %d, want 1", result.Scanned)
	}

	// Past the deadline.
	f.now = base.Add(6 * time.Minute)
	result, err = f.reaper.Sweep(context.Background(), f.now)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(result.Reaped) != 1 {
		t.Fatalf("reaped %d, want 1: %+v", len(result.Reaped), result.Reaped)
	}
	if result.Reaped[0].Reason != ReasonDeadline {
		t.Errorf("Reason = %s, want %s", result.Reaped[0].Reason, ReasonDeadline)
	}
	if result.Reaped[0].EnvID != env.ID {
		t.Errorf("reaped %s, want %s", result.Reaped[0].EnvID, env.ID)
	}
	if f.envCount() != 0 {
		t.Error("environment still exists after being reaped")
	}
}

// TestMaxLifetimeCapsEnvironmentsWithNoDeadline is the guarantee an operator
// quotes: whatever the caller asked for, nothing lives past the cap.
func TestMaxLifetimeCapsEnvironmentsWithNoDeadline(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxLifetime = 30 * time.Minute

	f := newFixture(t, cfg, false)
	f.createEnv("job_1", 0) // no deadline at all

	f.now = base.Add(29 * time.Minute)
	result, _ := f.reaper.Sweep(context.Background(), f.now)
	if len(result.Reaped) != 0 {
		t.Fatalf("reaped before the cap: %+v", result.Reaped)
	}

	f.now = base.Add(31 * time.Minute)
	result, err := f.reaper.Sweep(context.Background(), f.now)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(result.Reaped) != 1 {
		t.Fatalf("an environment with no deadline outlived the cap: %+v", result)
	}
	if result.Reaped[0].Reason != ReasonMaxLifetime {
		t.Errorf("Reason = %s, want %s", result.Reaped[0].Reason, ReasonMaxLifetime)
	}
}

// TestOrphanFromACrashedControlPlaneIsReaped is the scenario the whole package
// exists for: the process that created an environment died without cleaning up.
func TestOrphanFromACrashedControlPlaneIsReaped(t *testing.T) {
	f := newFixture(t, DefaultConfig(), true)

	// An environment whose job was never recorded - the control plane died
	// between Create and the queue write.
	f.createEnv("job_never_recorded", time.Hour)

	// Inside the grace window, it is left alone: it might be a job that is
	// starting right now.
	f.now = base.Add(30 * time.Second)
	result, _ := f.reaper.Sweep(context.Background(), f.now)
	if len(result.Reaped) != 0 {
		t.Fatalf("reaped inside the grace window, which would kill jobs as they start: %+v", result.Reaped)
	}

	// Past grace, with still no job record, it is an orphan.
	f.now = base.Add(5 * time.Minute)
	result, err := f.reaper.Sweep(context.Background(), f.now)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(result.Reaped) != 1 || result.Reaped[0].Reason != ReasonOrphaned {
		t.Fatalf("orphan not reaped: %+v", result)
	}
	if f.envCount() != 0 {
		t.Error("orphan still exists")
	}
}

func TestLiveJobIsNeverReaped(t *testing.T) {
	f := newFixture(t, DefaultConfig(), true)
	f.enqueue("job_live")
	f.createEnv("job_live", time.Hour)

	// Well past grace, but the job is queued and its deadline is far off.
	f.now = base.Add(20 * time.Minute)
	result, err := f.reaper.Sweep(context.Background(), f.now)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(result.Reaped) != 0 {
		t.Fatalf("reaped a live job's environment: %+v", result.Reaped)
	}
	if f.envCount() != 1 {
		t.Error("live environment destroyed")
	}
}

// TestEnvironmentOutlivingAFinishedJobIsReaped: the scheduler's teardown
// failed, or the process died between finishing and destroying.
func TestEnvironmentOutlivingAFinishedJobIsReaped(t *testing.T) {
	f := newFixture(t, DefaultConfig(), true)

	j := f.enqueue("job_done")
	claimed, err := f.queue.Claim(context.Background(), "worker-1", time.Hour)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := claimed.Transition(job.StateRunning, f.now); err != nil {
		t.Fatalf("transition: %v", err)
	}
	if err := claimed.Transition(job.StateSucceeded, f.now); err != nil {
		t.Fatalf("transition: %v", err)
	}
	if err := f.queue.Complete(context.Background(), claimed); err != nil {
		t.Fatalf("complete: %v", err)
	}

	f.createEnv(j.ID, time.Hour)

	f.now = base.Add(5 * time.Minute)
	result, err := f.reaper.Sweep(context.Background(), f.now)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(result.Reaped) != 1 || result.Reaped[0].Reason != ReasonOrphaned {
		t.Fatalf("environment outliving a finished job was not reaped: %+v", result)
	}
}

// TestReapedJobIsMarkedFailedWithAnHonestReason: the caller must learn why
// their job stopped, and the fault must be attributed correctly.
func TestReapedJobIsMarkedFailedWithAnHonestReason(t *testing.T) {
	f := newFixture(t, DefaultConfig(), true)

	j := f.enqueue("job_hung")
	if _, err := f.queue.Claim(context.Background(), "worker-1", time.Hour); err != nil {
		t.Fatalf("claim: %v", err)
	}
	f.createEnv(j.ID, time.Minute)

	f.now = base.Add(2 * time.Minute)
	if _, err := f.reaper.Sweep(context.Background(), f.now); err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	stored, err := f.queue.Get(context.Background(), j.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if stored.State != job.StateFailed {
		t.Fatalf("state = %s, want failed", stored.State)
	}
	// A job reaped for outliving its deadline ran out of time. That is the
	// caller's problem, not a control-plane failure, and the taxonomy must say so.
	if stored.Failure.Kind != job.FailureDeadline {
		t.Errorf("kind = %s, want %s", stored.Failure.Kind, job.FailureDeadline)
	}
	if stored.Failure.Kind.Fault() != job.FaultUser {
		t.Errorf("fault = %s, want user", stored.Failure.Kind.Fault())
	}
}

func TestOrphanedJobIsAttributedToTheSystem(t *testing.T) {
	f := newFixture(t, DefaultConfig(), true)

	j := f.enqueue("job_orphan")
	if _, err := f.queue.Claim(context.Background(), "worker-1", 24*time.Hour); err != nil {
		t.Fatalf("claim: %v", err)
	}
	// Environment exists, job is claimed, but the environment has no deadline
	// and its job record is about to look abandoned.
	f.createEnv(j.ID, 0)

	// Force the orphan path by completing the job while its environment lives.
	claimed, _ := f.queue.Get(context.Background(), j.ID)
	claimed.Transition(job.StateRunning, f.now)
	claimed.Fail(job.FailureInternal.Newf("worker vanished"), f.now)
	f.queue.Complete(context.Background(), claimed)

	f.now = base.Add(5 * time.Minute)
	result, err := f.reaper.Sweep(context.Background(), f.now)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(result.Reaped) != 1 || result.Reaped[0].Reason != ReasonOrphaned {
		t.Fatalf("want one orphan reap, got %+v", result)
	}
	// The job was already terminal, so the reaper must not rewrite its outcome.
	stored, _ := f.queue.Get(context.Background(), j.ID)
	if stored.Failure.Kind != job.FailureInternal {
		t.Errorf("reaper overwrote an already-recorded failure: %s", stored.Failure.Kind)
	}
}

// TestWithoutAQueueDeadlinesStillApply: the sweeper degrades sensibly. It
// cannot identify orphans without job records, but the cost guarantee holds.
func TestWithoutAQueueDeadlinesStillApply(t *testing.T) {
	f := newFixture(t, DefaultConfig(), false)
	f.createEnv("job_1", time.Minute)
	f.createEnv("", time.Hour) // would be an orphan, but we cannot know

	f.now = base.Add(2 * time.Minute)
	result, err := f.reaper.Sweep(context.Background(), f.now)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(result.Reaped) != 1 {
		t.Fatalf("reaped %d, want only the expired one: %+v", len(result.Reaped), result.Reaped)
	}
	if result.Reaped[0].Reason != ReasonDeadline {
		t.Errorf("Reason = %s", result.Reaped[0].Reason)
	}
}

func TestSweepReportsWhatItScanned(t *testing.T) {
	f := newFixture(t, DefaultConfig(), false)
	for i := 0; i < 3; i++ {
		f.createEnv("job_x", time.Hour)
	}

	result, err := f.reaper.Sweep(context.Background(), f.now)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if result.Scanned != 3 {
		t.Errorf("Scanned = %d, want 3", result.Scanned)
	}
	if result.Failed != 0 {
		t.Errorf("Failed = %d, want 0", result.Failed)
	}
}

func TestSweepOnAnEmptySystem(t *testing.T) {
	f := newFixture(t, DefaultConfig(), true)
	result, err := f.reaper.Sweep(context.Background(), f.now)
	if err != nil {
		t.Fatalf("Sweep on empty system: %v", err)
	}
	if result.Scanned != 0 || len(result.Reaped) != 0 {
		t.Errorf("result = %+v, want empty", result)
	}
}

// TestRunSweepsImmediately: on startup the interesting environments are the
// ones the previous process left behind, and waiting a full interval to collect
// them is a full interval of paying for nothing.
func TestRunSweepsImmediately(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Interval = time.Hour // so only the startup sweep can fire

	f := newFixture(t, cfg, false)
	f.createEnv("job_1", time.Minute)
	f.now = base.Add(10 * time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.reaper.Run(ctx)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && f.envCount() > 0 {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done

	if f.envCount() != 0 {
		t.Error("startup sweep did not run; an orphan survived a full interval")
	}
}

func TestConfigValidation(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"no interval", func(c *Config) { c.Interval = 0 }},
		{"no max lifetime", func(c *Config) { c.MaxLifetime = 0 }},
		{"negative grace", func(c *Config) { c.Grace = -time.Second }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := DefaultConfig()
			c.mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Error("Validate accepted a config that would misbehave")
			}
		})
	}
	if err := DefaultConfig().Validate(); err != nil {
		t.Errorf("default config rejected: %v", err)
	}
	if _, err := New(DefaultConfig(), Deps{}); err == nil {
		t.Error("New accepted a nil driver")
	}
}
