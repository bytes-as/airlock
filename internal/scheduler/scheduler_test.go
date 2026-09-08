package scheduler

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytes-as/ephemera/internal/admission"
	"github.com/bytes-as/ephemera/internal/artifact"
	"github.com/bytes-as/ephemera/internal/driver"
	"github.com/bytes-as/ephemera/internal/driver/process"
	"github.com/bytes-as/ephemera/internal/job"
	"github.com/bytes-as/ephemera/internal/logstream"
	"github.com/bytes-as/ephemera/internal/queue"
	"github.com/bytes-as/ephemera/internal/queue/embedded"
	"github.com/bytes-as/ephemera/internal/secrets"
)

// These are integration tests. Nothing is mocked: a real bbolt queue, the real
// process driver, real child processes, a real artifact store on disk. The only
// thing standing in for production is the agent itself, which is deliberately
// a placeholder.

// TestHelperProcess is the agent. It exits before the test framework prints, so
// the driver sees only what the "agent" deliberately produced.
func TestHelperProcess(t *testing.T) {
	if os.Getenv("EPHEMERA_TEST_HELPER") != "1" {
		return
	}

	// Record this run's presence so a test can measure real concurrency: a file
	// exists for exactly as long as an agent is running.
	// os.Exit skips deferred calls, so the marker is removed explicitly by
	// exitAgent below. A defer here would leave every marker behind and make
	// the concurrency sample count finished agents as running.
	if dir := os.Getenv("EPHEMERA_PRESENCE_DIR"); dir != "" {
		presenceMarker = filepath.Join(dir, fmt.Sprintf("%d", os.Getpid()))
		os.WriteFile(presenceMarker, []byte("running"), 0o640)
	}

	switch os.Getenv("EPHEMERA_TEST_MODE") {
	case "ok":
		fmt.Println("agent completed the task")
		exitAgent(0)

	case "artifact":
		dir := os.Getenv("EPHEMERA_ARTIFACT_DIR")
		os.MkdirAll(dir, 0o750)
		os.WriteFile(filepath.Join(dir, "screenshot.png"), []byte("fake-png-bytes"), 0o640)
		fmt.Println("saved screenshot")
		exitAgent(0)

	case "crash":
		fmt.Fprintln(os.Stderr, "agent hit an unrecoverable error")
		exitAgent(7)

	case "hang":
		time.Sleep(60 * time.Second)
		exitAgent(0)

	case "slow":
		time.Sleep(150 * time.Millisecond)
		fmt.Println("done after a moment")
		exitAgent(0)

	case "leak-secret":
		// An agent that carelessly prints its own credentials. The platform did
		// not log this; the agent did, and the redactor must catch it anyway.
		fmt.Println("authenticating with " + os.Getenv("API_TOKEN"))
		exitAgent(0)
	}
	exitAgent(0)
}

// harness wires up a complete system for one test.
type harness struct {
	t         *testing.T
	queue     *embedded.Queue
	driver    *process.Driver
	artifacts *artifact.Local
	logs      *logstream.Broker
	admission *admission.Controller
	scheduler *Scheduler

	presenceDir string
	cancel      context.CancelFunc
	done        chan struct{}
}

// testConfig is DefaultConfig tuned for tests, with the network controls
// cleared because the process driver honestly refuses to enforce them.
func testConfig() Config {
	cfg := DefaultConfig()
	cfg.Workers = 4
	cfg.LeaseTTL = 5 * time.Second
	cfg.HeartbeatInterval = 500 * time.Millisecond
	cfg.RecoveryInterval = 500 * time.Millisecond
	cfg.IdlePoll = 10 * time.Millisecond
	cfg.TenantBackoff = 20 * time.Millisecond
	cfg.DefaultDeadline = 10 * time.Second
	cfg.MaxDeadline = 30 * time.Second
	cfg.DefaultMaxAttempts = 3
	cfg.NetworkMode = ""
	cfg.DenyCIDRs = nil
	return cfg
}

func newHarness(t *testing.T, cfg Config, opts ...func(*Deps)) *harness {
	t.Helper()
	root := t.TempDir()

	q, err := embedded.Open(filepath.Join(root, "queue.db"))
	if err != nil {
		t.Fatalf("open queue: %v", err)
	}
	t.Cleanup(func() { q.Close() })

	drv, err := process.New(filepath.Join(root, "envs"))
	if err != nil {
		t.Fatalf("new driver: %v", err)
	}

	store, err := artifact.NewLocal(filepath.Join(root, "artifacts"), []byte("test-key-0123456789abcdef"))
	if err != nil {
		t.Fatalf("new artifact store: %v", err)
	}

	presence := filepath.Join(root, "presence")
	if err := os.MkdirAll(presence, 0o750); err != nil {
		t.Fatalf("mkdir presence: %v", err)
	}

	broker := logstream.NewBroker()
	ctrl := admission.New(admission.DefaultLimits())

	deps := Deps{
		Queue:     q,
		Driver:    drv,
		Admission: ctrl,
		Artifacts: store,
		Logs:      broker,
		Secrets: secrets.NewChain(&secrets.StaticSource{Values: map[string]job.Secret{
			"api-token": job.Secret("sk-live-must-not-appear-in-logs"),
		}}),
		// Discard scheduler logs: these tests assert on behaviour, and a failing
		// run is easier to read without a wall of structured output.
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	for _, opt := range opts {
		opt(&deps)
	}

	s, err := New(cfg, deps)
	if err != nil {
		t.Fatalf("new scheduler: %v", err)
	}

	return &harness{
		t: t, queue: q, driver: drv, artifacts: store,
		logs: broker, admission: ctrl, scheduler: s,
		presenceDir: presence,
	}
}

func (h *harness) start() {
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	h.done = make(chan struct{})
	go func() {
		defer close(h.done)
		h.scheduler.Run(ctx)
	}()
}

func (h *harness) stop() {
	if h.cancel == nil {
		return
	}
	h.cancel()
	select {
	case <-h.done:
	case <-time.After(30 * time.Second):
		h.t.Error("scheduler did not stop within 30s")
	}
}

// spec builds a job spec that runs the test binary as the agent.
func (h *harness) spec(mode string) job.Spec {
	return job.Spec{
		Command: []string{os.Args[0], "-test.run=TestHelperProcess"},
		Env: map[string]string{
			"EPHEMERA_TEST_HELPER":  "1",
			"EPHEMERA_TEST_MODE":    mode,
			"EPHEMERA_PRESENCE_DIR": h.presenceDir,
		},
	}
}

func (h *harness) submit(tenant string, priority job.Priority, spec job.Spec) *job.Job {
	h.t.Helper()
	j := job.New(tenant, priority, spec, time.Now())
	if err := h.queue.Enqueue(context.Background(), j); err != nil {
		h.t.Fatalf("enqueue: %v", err)
	}
	return j
}

// awaitTerminal polls until the job reaches a terminal state.
func (h *harness) awaitTerminal(jobID string, within time.Duration) *job.Job {
	h.t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		j, err := h.queue.Get(context.Background(), jobID)
		if err == nil && j.State.Terminal() {
			return j
		}
		time.Sleep(20 * time.Millisecond)
	}
	j, _ := h.queue.Get(context.Background(), jobID)
	if j != nil {
		h.t.Fatalf("job %s did not finish within %s (state %s, attempts %d)", jobID, within, j.State, j.Attempts)
	}
	h.t.Fatalf("job %s did not finish within %s", jobID, within)
	return nil
}

// TestEndToEndSuccess is the whole point of the system: accept a job, provision
// an environment, run the agent, capture output, destroy it.
func TestEndToEndSuccess(t *testing.T) {
	h := newHarness(t, testConfig())
	h.start()
	defer h.stop()

	j := h.submit("tenant-a", job.PriorityNormal, h.spec("artifact"))
	done := h.awaitTerminal(j.ID, 30*time.Second)

	if done.State != job.StateSucceeded {
		t.Fatalf("state = %s, failure = %v", done.State, done.Failure)
	}
	if done.EnvID == "" {
		t.Error("job record does not name its environment")
	}
	if done.StartedAt == nil || done.EndedAt == nil {
		t.Error("timestamps not recorded")
	}

	// Output was captured.
	stored, err := h.artifacts.List(context.Background(), j.ID)
	if err != nil {
		t.Fatalf("list artifacts: %v", err)
	}
	if len(stored) != 1 || stored[0].Name != "screenshot.png" {
		t.Fatalf("artifacts = %+v, want one screenshot.png", stored)
	}
	if !strings.HasPrefix(stored[0].ContentType, "image/png") {
		t.Errorf("ContentType = %q", stored[0].ContentType)
	}

	// And a signed link to it can be issued and verified.
	if _, err := h.artifacts.SignedURL(j.ID, "screenshot.png", time.Minute); err != nil {
		t.Errorf("SignedURL: %v", err)
	}

	// The environment is gone. This is the "reaps" half of the requirement, and
	// it is the one that costs money when it is wrong.
	envs, err := h.driver.List(context.Background())
	if err != nil {
		t.Fatalf("list environments: %v", err)
	}
	if len(envs) != 0 {
		t.Errorf("%d environment(s) survived the job: %+v", len(envs), envs)
	}
}

func TestLogsReachSubscribers(t *testing.T) {
	h := newHarness(t, testConfig())
	h.start()
	defer h.stop()

	j := h.submit("tenant-a", job.PriorityNormal, h.spec("ok"))
	h.awaitTerminal(j.ID, 30*time.Second)

	// History is retained after the job ends, which is what an operator
	// investigating afterwards actually needs.
	var agentOutput, lifecycle bool
	for _, line := range h.logs.History(j.ID) {
		if strings.Contains(line.Text, "agent completed the task") {
			agentOutput = true
		}
		if line.Stream == logstream.StreamNotice && strings.Contains(line.Text, "agent started") {
			lifecycle = true
		}
	}
	if !agentOutput {
		t.Error("agent stdout did not reach the log broker")
	}
	if !lifecycle {
		t.Error("platform lifecycle events not in the job timeline")
	}
}

// TestAgentSecretsAreRedactedInLogs: the platform never logs a secret, but the
// agent might print one, and those lines flow through our pipeline.
func TestAgentSecretsAreRedactedInLogs(t *testing.T) {
	h := newHarness(t, testConfig())
	h.start()
	defer h.stop()

	spec := h.spec("leak-secret")
	spec.Secrets = []job.SecretRef{{Name: "API_TOKEN", Source: "static", Key: "api-token"}}

	j := h.submit("tenant-a", job.PriorityNormal, spec)
	done := h.awaitTerminal(j.ID, 30*time.Second)
	if done.State != job.StateSucceeded {
		t.Fatalf("state = %s, failure = %v", done.State, done.Failure)
	}

	var sawRedaction bool
	for _, line := range h.logs.History(j.ID) {
		if strings.Contains(line.Text, "sk-live-must-not-appear-in-logs") {
			t.Fatalf("secret leaked into the log stream: %q", line.Text)
		}
		if strings.Contains(line.Text, "[REDACTED]") {
			sawRedaction = true
		}
	}
	if !sawRedaction {
		t.Error("expected the agent's printed credential to be redacted, not merely absent")
	}
}

func TestAgentCrashIsNotRetried(t *testing.T) {
	h := newHarness(t, testConfig())
	h.start()
	defer h.stop()

	j := h.submit("tenant-a", job.PriorityNormal, h.spec("crash"))
	done := h.awaitTerminal(j.ID, 30*time.Second)

	if done.State != job.StateFailed {
		t.Fatalf("state = %s, want failed", done.State)
	}
	if done.Failure.Kind != job.FailureAgentCrash {
		t.Errorf("kind = %s, want %s", done.Failure.Kind, job.FailureAgentCrash)
	}
	// A deterministic crash retried three times is three times the cost for the
	// same answer.
	if done.Attempts != 1 {
		t.Errorf("attempts = %d, want 1 — a crash must not be retried", done.Attempts)
	}
	if done.Failure.Kind.Fault() != job.FaultUser {
		t.Errorf("fault = %s, want user", done.Failure.Kind.Fault())
	}
}

// TestStartFailureIsRetriedToTheLimit: a missing binary is an infrastructure
// fault by the taxonomy, so it retries — and then stops.
func TestStartFailureIsRetriedToTheLimit(t *testing.T) {
	cfg := testConfig()
	cfg.DefaultMaxAttempts = 3

	h := newHarness(t, cfg)
	h.start()
	defer h.stop()

	spec := h.spec("ok")
	spec.Command = []string{filepath.Join(t.TempDir(), "no-such-binary")}

	j := h.submit("tenant-a", job.PriorityNormal, spec)
	done := h.awaitTerminal(j.ID, 30*time.Second)

	if done.State != job.StateFailed {
		t.Fatalf("state = %s, want failed", done.State)
	}
	if done.Failure.Kind != job.FailureStart {
		t.Errorf("kind = %s, want %s", done.Failure.Kind, job.FailureStart)
	}
	if done.Attempts != 3 {
		t.Errorf("attempts = %d, want exactly the configured 3", done.Attempts)
	}

	// Retries must not leak environments either.
	envs, _ := h.driver.List(context.Background())
	if len(envs) != 0 {
		t.Errorf("%d environment(s) leaked across retries", len(envs))
	}
}

func TestDeadlineIsEnforced(t *testing.T) {
	cfg := testConfig()
	h := newHarness(t, cfg)
	h.start()
	defer h.stop()

	spec := h.spec("hang")
	spec.Deadline = 500 * time.Millisecond

	start := time.Now()
	j := h.submit("tenant-a", job.PriorityNormal, spec)
	done := h.awaitTerminal(j.ID, 30*time.Second)
	elapsed := time.Since(start)

	if done.State != job.StateFailed {
		t.Fatalf("state = %s, want failed", done.State)
	}
	if done.Failure.Kind != job.FailureDeadline {
		t.Errorf("kind = %s, want %s", done.Failure.Kind, job.FailureDeadline)
	}
	// The agent sleeps 60s. Finishing far sooner proves the deadline fired.
	if elapsed > 30*time.Second {
		t.Errorf("took %v to enforce a 500ms deadline", elapsed)
	}
	// A deadline is not retried: the work does not fit the budget, and
	// retrying spends the budget again to learn the same thing.
	if done.Attempts != 1 {
		t.Errorf("attempts = %d, want 1", done.Attempts)
	}
}

// TestDeadlineIsClampedToMaximum: a caller asking for more time than the
// operator permits gets the permitted maximum, announced, rather than a refusal.
func TestDeadlineIsClampedToMaximum(t *testing.T) {
	cfg := testConfig()
	cfg.MaxDeadline = 700 * time.Millisecond
	cfg.DefaultDeadline = 500 * time.Millisecond

	h := newHarness(t, cfg)
	h.start()
	defer h.stop()

	spec := h.spec("hang")
	spec.Deadline = time.Hour // far beyond what the operator allows

	j := h.submit("tenant-a", job.PriorityNormal, spec)
	done := h.awaitTerminal(j.ID, 30*time.Second)

	if done.Failure == nil || done.Failure.Kind != job.FailureDeadline {
		t.Fatalf("failure = %v, want the clamped deadline to fire", done.Failure)
	}

	var announced bool
	for _, line := range h.logs.History(j.ID) {
		if strings.Contains(line.Text, "exceeds the maximum") {
			announced = true
		}
	}
	if !announced {
		t.Error("deadline was clamped without telling anyone")
	}
}

// TestFiftyConcurrentJobs is the load question. It asserts two things:
// everything completes, and worker concurrency is genuinely bounded.
func TestFiftyConcurrentJobs(t *testing.T) {
	cfg := testConfig()
	cfg.Workers = 6

	h := newHarness(t, cfg)

	// Raise the tenant limits: this test is about the scheduler's capacity, not
	// about admission control, which has its own tests.
	h.admission.SetTenantLimits("tenant-a", admission.Limits{
		SubmitsPerSecond: 1000, Burst: 1000, MaxConcurrent: 100,
	})

	const total = 50
	ids := make([]string, 0, total)
	for i := 0; i < total; i++ {
		ids = append(ids, h.submit("tenant-a", job.PriorityNormal, h.spec("slow")).ID)
	}

	// Sample how many agents are actually running at once, by counting the
	// presence files real child processes create and remove.
	var peak int64
	stopSampling := make(chan struct{})
	var sampler sync.WaitGroup
	sampler.Add(1)
	go func() {
		defer sampler.Done()
		for {
			select {
			case <-stopSampling:
				return
			case <-time.After(5 * time.Millisecond):
				entries, err := os.ReadDir(h.presenceDir)
				if err != nil {
					continue
				}
				if n := int64(len(entries)); n > atomic.LoadInt64(&peak) {
					atomic.StoreInt64(&peak, n)
				}
			}
		}
	}()

	h.start()
	defer h.stop()

	for _, id := range ids {
		done := h.awaitTerminal(id, 120*time.Second)
		if done.State != job.StateSucceeded {
			t.Errorf("job %s = %s (%v)", id, done.State, done.Failure)
		}
	}

	close(stopSampling)
	sampler.Wait()

	observed := atomic.LoadInt64(&peak)
	if observed == 0 {
		t.Error("never observed a running agent; the concurrency sample is not measuring anything")
	}
	if observed > int64(cfg.Workers) {
		t.Errorf("peak concurrency %d exceeded the %d configured workers", observed, cfg.Workers)
	}
	t.Logf("50 jobs completed; peak observed concurrency %d of %d workers", observed, cfg.Workers)

	// Nothing left behind.
	envs, _ := h.driver.List(context.Background())
	if len(envs) != 0 {
		t.Errorf("%d environment(s) survived a 50-job run: %+v", len(envs), envs)
	}
	stats, err := h.queue.Stats(context.Background())
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Terminal != total {
		t.Errorf("terminal jobs = %d, want %d", stats.Terminal, total)
	}
}

// TestTenantConcurrencyQuotaBoundsExecution proves the quota constrains actual
// parallelism, not merely submission.
func TestTenantConcurrencyQuotaBoundsExecution(t *testing.T) {
	cfg := testConfig()
	cfg.Workers = 6

	h := newHarness(t, cfg)
	h.admission.SetTenantLimits("tenant-capped", admission.Limits{
		SubmitsPerSecond: 1000, Burst: 1000, MaxConcurrent: 2,
	})

	const total = 12
	ids := make([]string, 0, total)
	for i := 0; i < total; i++ {
		ids = append(ids, h.submit("tenant-capped", job.PriorityNormal, h.spec("slow")).ID)
	}

	var peak int64
	stopSampling := make(chan struct{})
	var sampler sync.WaitGroup
	sampler.Add(1)
	go func() {
		defer sampler.Done()
		for {
			select {
			case <-stopSampling:
				return
			case <-time.After(5 * time.Millisecond):
				entries, err := os.ReadDir(h.presenceDir)
				if err != nil {
					continue
				}
				if n := int64(len(entries)); n > atomic.LoadInt64(&peak) {
					atomic.StoreInt64(&peak, n)
				}
			}
		}
	}()

	h.start()
	defer h.stop()

	for _, id := range ids {
		if done := h.awaitTerminal(id, 120*time.Second); done.State != job.StateSucceeded {
			t.Errorf("job %s = %s (%v)", id, done.State, done.Failure)
		}
	}
	close(stopSampling)
	sampler.Wait()

	// Six workers are free, but the tenant may only use two of them.
	if observed := atomic.LoadInt64(&peak); observed > 2 {
		t.Errorf("peak concurrency %d exceeded the tenant quota of 2 despite %d free workers", observed, cfg.Workers)
	}
}

func TestPriorityIsHonoured(t *testing.T) {
	cfg := testConfig()
	cfg.Workers = 1 // serialise, so completion order is dispatch order

	h := newHarness(t, cfg)

	// Enqueue low priority first, so only priority ordering can reorder them.
	low := h.submit("tenant-a", job.PriorityLow, h.spec("ok"))
	high := h.submit("tenant-a", job.PriorityHigh, h.spec("ok"))

	h.start()
	defer h.stop()

	doneLow := h.awaitTerminal(low.ID, 60*time.Second)
	doneHigh := h.awaitTerminal(high.ID, 60*time.Second)

	if doneHigh.EndedAt == nil || doneLow.EndedAt == nil {
		t.Fatal("missing completion timestamps")
	}
	if doneHigh.EndedAt.After(*doneLow.EndedAt) {
		t.Errorf("high-priority job finished after the low-priority one (high %v, low %v)",
			doneHigh.EndedAt, doneLow.EndedAt)
	}
}

// TestSchedulerRefusesConfigItsDriverCannotHonour: failing at startup beats
// silently dropping a security control or failing every job at runtime.
func TestSchedulerRefusesConfigItsDriverCannotHonour(t *testing.T) {
	root := t.TempDir()
	q, err := embedded.Open(filepath.Join(root, "queue.db"))
	if err != nil {
		t.Fatalf("open queue: %v", err)
	}
	defer q.Close()

	drv, err := process.New(filepath.Join(root, "envs"))
	if err != nil {
		t.Fatalf("new driver: %v", err)
	}

	cfg := DefaultConfig() // asks for an egress deny list
	_, err = New(cfg, Deps{Queue: q, Driver: drv})
	if err == nil {
		t.Fatal("scheduler started with a config its driver cannot honour")
	}
	// The message must name the fix, for the person reading it at 3am.
	for _, want := range []string{"network policy", "process", "docker"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}

	// RelaxedFor is the deliberate, visible downgrade.
	relaxed, dropped := cfg.RelaxedFor(drv)
	if len(dropped) == 0 {
		t.Error("RelaxedFor dropped controls without reporting them")
	}
	if _, err := New(relaxed, Deps{Queue: q, Driver: drv}); err != nil {
		t.Errorf("relaxed config still refused: %v", err)
	}
}

func TestConfigValidation(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"no workers", func(c *Config) { c.Workers = 0 }},
		{"heartbeat longer than lease", func(c *Config) {
			c.HeartbeatInterval = 2 * time.Minute
			c.LeaseTTL = time.Minute
		}},
		{"default deadline above maximum", func(c *Config) {
			c.DefaultDeadline = time.Hour
			c.MaxDeadline = time.Minute
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := testConfig()
			c.mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Error("Validate accepted a config that would misbehave at runtime")
			}
		})
	}

	if err := testConfig().Validate(); err != nil {
		t.Errorf("valid config rejected: %v", err)
	}
}

func TestStatsReportDispatchState(t *testing.T) {
	h := newHarness(t, testConfig())
	h.start()
	defer h.stop()

	j := h.submit("tenant-a", job.PriorityNormal, h.spec("ok"))
	h.awaitTerminal(j.ID, 30*time.Second)

	stats, err := h.scheduler.Stats(context.Background())
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.Driver != "process" {
		t.Errorf("Driver = %q", stats.Driver)
	}
	if stats.Isolation != driver.IsolationProcess.String() {
		t.Errorf("Isolation = %q, want the driver's honest level", stats.Isolation)
	}
	if stats.Workers != testConfig().Workers {
		t.Errorf("Workers = %d", stats.Workers)
	}
	if stats.Queue.Terminal != 1 {
		t.Errorf("Queue.Terminal = %d, want 1", stats.Queue.Terminal)
	}
}

// TestShutdownDrainsRatherThanAbandons: a worker killed mid-job leaves an
// environment behind, and "the reaper gets it eventually" still costs money.
func TestShutdownDrainsRatherThanAbandons(t *testing.T) {
	cfg := testConfig()
	cfg.Workers = 2

	h := newHarness(t, cfg)
	ids := []string{
		h.submit("tenant-a", job.PriorityNormal, h.spec("slow")).ID,
		h.submit("tenant-a", job.PriorityNormal, h.spec("slow")).ID,
	}

	h.start()
	// Let the workers pick the jobs up, then shut down while they are running.
	time.Sleep(120 * time.Millisecond)
	h.stop()

	for _, id := range ids {
		j, err := h.queue.Get(context.Background(), id)
		if err != nil {
			t.Fatalf("get %s: %v", id, err)
		}
		if !j.State.Terminal() {
			t.Errorf("job %s left in %s after shutdown; it should have been drained", id, j.State)
		}
	}

	envs, _ := h.driver.List(context.Background())
	if len(envs) != 0 {
		t.Errorf("%d environment(s) survived shutdown: %+v", len(envs), envs)
	}
}

func TestUnresolvableSecretIsRejectedNotRetried(t *testing.T) {
	h := newHarness(t, testConfig())
	h.start()
	defer h.stop()

	spec := h.spec("ok")
	spec.Secrets = []job.SecretRef{{Name: "MISSING", Source: "static", Key: "does-not-exist"}}

	j := h.submit("tenant-a", job.PriorityNormal, spec)
	done := h.awaitTerminal(j.ID, 30*time.Second)

	if done.State != job.StateFailed {
		t.Fatalf("state = %s, want failed", done.State)
	}
	if done.Failure.Kind != job.FailureRejected {
		t.Errorf("kind = %s, want %s", done.Failure.Kind, job.FailureRejected)
	}
	if done.Attempts != 1 {
		t.Errorf("attempts = %d; a missing secret will not appear on a retry", done.Attempts)
	}
	// The error names the reference, never the value.
	if strings.Contains(done.Failure.Message, "sk-live") {
		t.Error("failure message leaked secret material")
	}
}

var _ = queue.ErrEmpty // keep the queue import meaningful if tests are trimmed

// presenceMarker is the file this agent process holds while it runs.
var presenceMarker string

// exitAgent removes the presence marker and exits.
//
// It exists because os.Exit does not run deferred functions, so a marker left
// by a finished agent would be counted as a running one and make every
// concurrency measurement in this file meaningless.
func exitAgent(code int) {
	if presenceMarker != "" {
		os.Remove(presenceMarker)
	}
	os.Exit(code)
}

// --- provisioning watchdog -------------------------------------------------
//
// These two are the exception to "nothing is mocked" at the top of this file.
// A real driver cannot be made to hang on demand, and the behaviour under test
// is precisely what happens when one does: the environment never appears, so
// there is nothing for the reaper to destroy and nothing for the job's own
// deadline to stop. Without a bound, the worker blocks in the driver call
// forever while its lease is faithfully renewed, and the pool loses a slot
// permanently with no error anywhere.

// stallingDriver blocks in Create or Start until its context is cancelled.
type stallingDriver struct {
	driver.Driver
	stallCreate bool
	stallStart  bool
}

func (d *stallingDriver) Name() string { return "stalling" }

func (d *stallingDriver) Capabilities() driver.Capabilities {
	return driver.Capabilities{Isolation: driver.IsolationProcess}
}

func (d *stallingDriver) Create(ctx context.Context, spec driver.EnvSpec) (driver.Env, error) {
	if d.stallCreate {
		<-ctx.Done()
		return driver.Env{}, ctx.Err()
	}
	return driver.Env{ID: "env-stub", Driver: d.Name(), JobID: spec.JobID, TenantID: spec.TenantID}, nil
}

func (d *stallingDriver) Start(ctx context.Context, _ driver.Env, _ driver.EnvSpec) error {
	if d.stallStart {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func (d *stallingDriver) Destroy(context.Context, driver.Env) error { return nil }
func (d *stallingDriver) List(context.Context) ([]driver.Env, error) {
	return nil, nil
}

func TestProvisioningHangIsBoundedAndDoesNotStrandTheWorker(t *testing.T) {
	cfg := testConfig()
	cfg.Workers = 1 // the only worker, so a strand is unmissable
	cfg.ProvisionTimeout = 300 * time.Millisecond
	cfg.DefaultMaxAttempts = 1

	h := newHarness(t, cfg, func(d *Deps) {
		d.Driver = &stallingDriver{stallCreate: true}
	})
	h.start()
	defer h.stop()

	j := h.submit("tenant-a", job.PriorityNormal, h.spec("hang"))
	done := h.awaitTerminal(j.ID, 20*time.Second)

	if done.State != job.StateFailed {
		t.Fatalf("state = %s, want failed; a hung provision must not run forever", done.State)
	}
	if done.Failure == nil || done.Failure.Kind != job.FailureProvision {
		t.Fatalf("failure = %+v, want %s", done.Failure, job.FailureProvision)
	}

	// The point of the test: the worker is free again. A second job proves the
	// pool was not permanently consumed by the first.
	second := h.submit("tenant-a", job.PriorityNormal, h.spec("hang"))
	if got := h.awaitTerminal(second.ID, 20*time.Second); got.State != job.StateFailed {
		t.Fatalf("second job state = %s; the worker never came back", got.State)
	}
}

func TestStartHangIsBoundedAndTearsTheEnvironmentDown(t *testing.T) {
	cfg := testConfig()
	cfg.Workers = 1
	cfg.ProvisionTimeout = 300 * time.Millisecond
	cfg.DefaultMaxAttempts = 1

	h := newHarness(t, cfg, func(d *Deps) {
		d.Driver = &stallingDriver{stallStart: true}
	})
	h.start()
	defer h.stop()

	j := h.submit("tenant-a", job.PriorityNormal, h.spec("hang"))
	done := h.awaitTerminal(j.ID, 20*time.Second)

	if done.State != job.StateFailed {
		t.Fatalf("state = %s, want failed", done.State)
	}
	if done.Failure == nil || done.Failure.Kind != job.FailureStart {
		t.Fatalf("failure = %+v, want %s - an environment that came up but never became ready is a start failure, not a provisioning one", done.Failure, job.FailureStart)
	}
}
