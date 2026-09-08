package process

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bytes-as/ephemera/internal/driver"
	"github.com/bytes-as/ephemera/internal/job"
)

// The tests below run real child processes. The child is this same test binary,
// re-executed with EPHEMERA_TEST_HELPER=1, which is the standard Go pattern for
// exercising process handling without shipping a fixture binary or depending on
// shell utilities that differ across Windows, macOS and Linux.

// TestHelperProcess is the agent under test. It is not a test; it exits before
// the testing framework can print anything, so the driver sees only the output
// the "agent" deliberately produced.
func TestHelperProcess(t *testing.T) {
	if os.Getenv("EPHEMERA_TEST_HELPER") != "1" {
		return
	}

	switch os.Getenv("EPHEMERA_TEST_MODE") {
	case "echo":
		os.Stdout.WriteString("hello from stdout\n")
		os.Stderr.WriteString("hello from stderr\n")
		os.Exit(0)

	case "crash":
		os.Stderr.WriteString("something went wrong\n")
		os.Exit(3)

	case "sleep":
		time.Sleep(60 * time.Second)
		os.Exit(0)

	case "artifact":
		dir := os.Getenv("EPHEMERA_ARTIFACT_DIR")
		if dir == "" {
			os.Stderr.WriteString("no artifact dir provided\n")
			os.Exit(1)
		}
		if err := os.MkdirAll(filepath.Join(dir, "nested"), 0o750); err != nil {
			os.Exit(1)
		}
		os.WriteFile(filepath.Join(dir, "screenshot.png"), []byte("not-really-a-png"), 0o640)
		os.WriteFile(filepath.Join(dir, "nested", "result.json"), []byte(`{"ok":true}`), 0o640)
		os.Stdout.WriteString("wrote artifacts\n")
		os.Exit(0)

	case "env":
		// Print the values the driver injected so the test can assert on them.
		os.Stdout.WriteString("JOB=" + os.Getenv("EPHEMERA_JOB_ID") + "\n")
		os.Stdout.WriteString("PUBLIC=" + os.Getenv("PUBLIC_SETTING") + "\n")
		os.Stdout.WriteString("SECRET=" + os.Getenv("API_TOKEN") + "\n")
		os.Exit(0)
	}
	os.Exit(0)
}

// helperSpec builds a spec that re-executes this test binary in the given mode.
func helperSpec(mode string, deadline time.Duration) driver.EnvSpec {
	return driver.EnvSpec{
		JobID:    "job_test",
		TenantID: "tenant-a",
		Command:  []string{os.Args[0], "-test.run=TestHelperProcess"},
		Env: map[string]string{
			"EPHEMERA_TEST_HELPER": "1",
			"EPHEMERA_TEST_MODE":   mode,
		},
		Deadline: deadline,
	}
}

func newDriver(t *testing.T) *Driver {
	t.Helper()
	d, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return d
}

// runToCompletion is the happy path used by several tests: create, start, wait.
func runToCompletion(t *testing.T, d *Driver, spec driver.EnvSpec) (driver.Env, driver.Status) {
	t.Helper()
	ctx := context.Background()

	env, err := d.Create(ctx, spec)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := d.Start(ctx, env, spec); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	status, err := d.Wait(waitCtx, env)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	return env, status
}

func TestLifecycleSucceeds(t *testing.T) {
	d := newDriver(t)
	env, status := runToCompletion(t, d, helperSpec("echo", 0))

	if !status.Succeeded() {
		t.Fatalf("status = %+v, want success", status)
	}
	if status.Failure() != nil {
		t.Errorf("Failure() = %v, want nil for a clean exit", status.Failure())
	}
	if env.Driver != "process" {
		t.Errorf("env.Driver = %q, want process", env.Driver)
	}
	if status.StartedAt.IsZero() || status.ExitedAt.IsZero() {
		t.Error("timestamps not recorded")
	}
}

func TestLogsCaptureBothStreams(t *testing.T) {
	d := newDriver(t)
	ctx := context.Background()
	spec := helperSpec("echo", 0)

	env, err := d.Create(ctx, spec)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := d.Start(ctx, env, spec); err != nil {
		t.Fatalf("Start: %v", err)
	}

	logCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	lines, err := d.Logs(logCtx, env)
	if err != nil {
		t.Fatalf("Logs: %v", err)
	}

	streams := map[string][]string{}
	for line := range lines {
		streams[line.Stream] = append(streams[line.Stream], line.Text)
		if line.At.IsZero() {
			t.Error("log line has no timestamp")
		}
	}

	if !containsSubstring(streams[driver.StreamStdout], "hello from stdout") {
		t.Errorf("stdout not captured: %v", streams[driver.StreamStdout])
	}
	if !containsSubstring(streams[driver.StreamStderr], "hello from stderr") {
		t.Errorf("stderr not captured: %v", streams[driver.StreamStderr])
	}
}

// TestLogsAreReplayableAfterExit matters for the flight recorder: an operator
// investigating a failure arrives after the job is over, and must still be able
// to read everything it printed.
func TestLogsAreReplayableAfterExit(t *testing.T) {
	d := newDriver(t)
	env, status := runToCompletion(t, d, helperSpec("echo", 0))
	if !status.Succeeded() {
		t.Fatalf("setup: job did not succeed: %+v", status)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	lines, err := d.Logs(ctx, env)
	if err != nil {
		t.Fatalf("Logs after exit: %v", err)
	}
	var texts []string
	for line := range lines {
		texts = append(texts, line.Text)
	}
	if !containsSubstring(texts, "hello from stdout") {
		t.Errorf("logs not replayable after exit: %v", texts)
	}
}

func TestCrashMapsToAgentCrashFailure(t *testing.T) {
	d := newDriver(t)
	_, status := runToCompletion(t, d, helperSpec("crash", 0))

	if status.Succeeded() {
		t.Fatal("crashing agent reported success")
	}
	if status.ExitCode != 3 {
		t.Errorf("ExitCode = %d, want 3", status.ExitCode)
	}
	failure := status.Failure()
	if failure == nil {
		t.Fatal("Failure() = nil for a crashed agent")
	}
	if failure.Kind != job.FailureAgentCrash {
		t.Errorf("Kind = %s, want %s", failure.Kind, job.FailureAgentCrash)
	}
	// A crash is the caller's problem, and must never be retried automatically.
	if failure.Kind.Retryable() {
		t.Error("agent crash classified as retryable")
	}
	if failure.Kind.Fault() != job.FaultUser {
		t.Errorf("fault = %s, want user", failure.Kind.Fault())
	}
}

// TestDeadlineKillsRunawayAgent is the innermost reaper layer: the environment
// must end itself on time with no help from the control plane.
func TestDeadlineKillsRunawayAgent(t *testing.T) {
	d := newDriver(t)
	start := time.Now()
	_, status := runToCompletion(t, d, helperSpec("sleep", 500*time.Millisecond))
	elapsed := time.Since(start)

	if !status.DeadlineExceeded {
		t.Errorf("DeadlineExceeded = false for an agent that outran its deadline: %+v", status)
	}
	if status.Succeeded() {
		t.Error("a deadline kill reported success")
	}
	// The helper sleeps for 60s. Anything close to that means the deadline did
	// not fire; allow generous slack for slow CI without letting a no-op pass.
	if elapsed > 30*time.Second {
		t.Errorf("took %v to enforce a 500ms deadline", elapsed)
	}
	if failure := status.Failure(); failure == nil || failure.Kind != job.FailureDeadline {
		t.Errorf("Failure() = %v, want %s", failure, job.FailureDeadline)
	}
}

func TestCollectGathersNestedArtifacts(t *testing.T) {
	d := newDriver(t)
	env, status := runToCompletion(t, d, helperSpec("artifact", 0))
	if !status.Succeeded() {
		t.Fatalf("agent failed: %+v", status)
	}

	dest := filepath.Join(t.TempDir(), "collected")
	artifacts, err := d.Collect(context.Background(), env, dest)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(artifacts) != 2 {
		t.Fatalf("collected %d artifacts, want 2: %+v", len(artifacts), artifacts)
	}

	byName := map[string]driver.Artifact{}
	for _, a := range artifacts {
		byName[a.Name] = a
	}

	png, ok := byName["screenshot.png"]
	if !ok {
		t.Fatalf("screenshot.png not collected: %+v", artifacts)
	}
	if !strings.HasPrefix(png.ContentType, "image/png") {
		t.Errorf("ContentType = %q, want image/png", png.ContentType)
	}
	if png.Size == 0 {
		t.Error("artifact size not recorded")
	}
	if _, err := os.Stat(png.Path); err != nil {
		t.Errorf("collected artifact not on disk: %v", err)
	}

	// Nested paths must be preserved with forward slashes, so the name is a
	// usable object key whatever the host OS.
	if _, ok := byName["nested/result.json"]; !ok {
		t.Errorf("nested artifact missing or wrongly named: %+v", artifacts)
	}
}

func TestCollectOnJobThatProducedNothing(t *testing.T) {
	d := newDriver(t)
	env, _ := runToCompletion(t, d, helperSpec("echo", 0))

	artifacts, err := d.Collect(context.Background(), env, filepath.Join(t.TempDir(), "out"))
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(artifacts) != 0 {
		t.Errorf("got %d artifacts, want none", len(artifacts))
	}
}

// TestSecretsReachTheAgentButNotTheDisk is the security claim of the whole
// system in one test: the credential must arrive in the process environment and
// must not appear in anything we persist.
func TestSecretsReachTheAgentButNotTheDisk(t *testing.T) {
	const secretValue = "sk-live-do-not-log-me"

	d := newDriver(t)
	ctx := context.Background()
	spec := helperSpec("env", 0)
	spec.Env["PUBLIC_SETTING"] = "visible"
	spec.Secrets = map[string]job.Secret{"API_TOKEN": job.Secret(secretValue)}

	env, err := d.Create(ctx, spec)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := d.Start(ctx, env, spec); err != nil {
		t.Fatalf("Start: %v", err)
	}

	logCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	lines, err := d.Logs(logCtx, env)
	if err != nil {
		t.Fatalf("Logs: %v", err)
	}
	var texts []string
	for line := range lines {
		texts = append(texts, line.Text)
	}

	// The agent genuinely received it.
	if !containsSubstring(texts, "SECRET="+secretValue) {
		t.Errorf("secret did not reach the agent: %v", texts)
	}
	if !containsSubstring(texts, "PUBLIC=visible") {
		t.Errorf("non-secret env var did not reach the agent: %v", texts)
	}

	// ...and it is nowhere in the state we persisted. The agent chose to print
	// it, which is its right; the platform must not have written it anywhere.
	raw, err := os.ReadFile(filepath.Join(d.envDir(env.ID), stateFileName))
	if err != nil {
		t.Fatalf("read state file: %v", err)
	}
	if strings.Contains(string(raw), secretValue) {
		t.Error("secret leaked into the persisted state file")
	}
	var st stateFile
	if err := json.Unmarshal(raw, &st); err != nil {
		t.Fatalf("state file is not valid JSON: %v", err)
	}
	if strings.Contains(strings.Join(st.Command, " "), secretValue) {
		t.Error("secret leaked into the persisted command line")
	}
}

// TestCreateRefusesGuaranteesItCannotKeep covers the honesty rule: a driver
// must reject a spec it cannot honour rather than run it without the control.
func TestCreateRefusesGuaranteesItCannotKeep(t *testing.T) {
	d := newDriver(t)
	ctx := context.Background()

	cases := []struct {
		name    string
		mutate  func(*driver.EnvSpec)
		feature string
	}{
		{"network none", func(s *driver.EnvSpec) { s.Network.Mode = driver.NetworkNone }, "network mode"},
		{"network proxied", func(s *driver.EnvSpec) { s.Network.Mode = driver.NetworkProxied }, "network mode"},
		{"deny cidrs", func(s *driver.EnvSpec) { s.Network.DenyCIDRs = driver.DefaultDenyCIDRs() }, "network deny lists"},
		{"memory limit", func(s *driver.EnvSpec) { s.Resources.MemoryMiB = 512 }, "resource limits"},
		{"cpu limit", func(s *driver.EnvSpec) { s.Resources.CPUMillis = 500 }, "resource limits"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			spec := helperSpec("echo", 0)
			c.mutate(&spec)
			_, err := d.Create(ctx, spec)
			var unsupported *driver.UnsupportedError
			if !errors.As(err, &unsupported) {
				t.Fatalf("Create error = %v, want *driver.UnsupportedError", err)
			}
			if !strings.Contains(unsupported.Feature, c.feature) {
				t.Errorf("Feature = %q, want it to mention %q", unsupported.Feature, c.feature)
			}
		})
	}

	// Egress is the ambient behaviour of a host process, so accepting it is
	// truthful rather than a silent downgrade.
	spec := helperSpec("echo", 0)
	spec.Network.Mode = driver.NetworkEgress
	if _, err := d.Create(ctx, spec); err != nil {
		t.Errorf("Create with plain egress = %v, want accepted", err)
	}
}

func TestCreateRejectsEmptyCommand(t *testing.T) {
	d := newDriver(t)
	spec := helperSpec("echo", 0)
	spec.Command = nil
	if _, err := d.Create(context.Background(), spec); err == nil {
		t.Fatal("Create accepted a spec with no command")
	}
}

// TestOrphanSurvivesControlPlaneRestart is the reaper's core requirement: a
// brand-new control plane, sharing only the state directory, must be able to
// find and destroy an environment it never created.
func TestOrphanSurvivesControlPlaneRestart(t *testing.T) {
	root := t.TempDir()
	ctx := context.Background()

	first, err := New(root)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	spec := helperSpec("sleep", 0)
	env, err := first.Create(ctx, spec)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := first.Start(ctx, env, spec); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// A different Driver instance: no shared memory, only the directory. This
	// stands in for the control plane having crashed and restarted.
	second, err := New(root)
	if err != nil {
		t.Fatalf("New (restarted): %v", err)
	}

	envs, err := second.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(envs) != 1 || envs[0].ID != env.ID {
		t.Fatalf("List = %+v, want the orphaned environment %s", envs, env.ID)
	}
	if envs[0].JobID != spec.JobID {
		t.Errorf("orphan lost its job association: %+v", envs[0])
	}

	status, err := second.Describe(ctx, envs[0])
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}
	if status.Phase != driver.PhaseRunning {
		t.Errorf("Phase = %s, want running", status.Phase)
	}

	// The restarted control plane can kill it — the whole point of the exercise.
	if err := second.Destroy(ctx, envs[0]); err != nil {
		t.Fatalf("Destroy orphan: %v", err)
	}
	after, err := second.List(ctx)
	if err != nil {
		t.Fatalf("List after destroy: %v", err)
	}
	if len(after) != 0 {
		t.Errorf("environment still listed after destroy: %+v", after)
	}
	if _, err := os.Stat(first.envDir(env.ID)); !os.IsNotExist(err) {
		t.Error("environment directory survived destroy")
	}
}

// TestDestroyIsIdempotent matters because the reaper calls Destroy
// speculatively on environments that may already have died on their own.
func TestDestroyIsIdempotent(t *testing.T) {
	d := newDriver(t)
	env, _ := runToCompletion(t, d, helperSpec("echo", 0))
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if err := d.Destroy(ctx, env); err != nil {
			t.Fatalf("Destroy call %d: %v", i+1, err)
		}
	}
	// Destroying something that never existed is also success.
	if err := d.Destroy(ctx, driver.Env{ID: "env_never_existed", Driver: "process"}); err != nil {
		t.Errorf("Destroy of unknown environment = %v, want nil", err)
	}
}

func TestDescribeUnknownEnvironmentIsGoneNotError(t *testing.T) {
	d := newDriver(t)
	status, err := d.Describe(context.Background(), driver.Env{ID: "env_nope", Driver: "process"})
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}
	if status.Phase != driver.PhaseGone {
		t.Errorf("Phase = %s, want gone", status.Phase)
	}
	if !status.Phase.Terminal() {
		t.Error("gone should be terminal")
	}
}

func TestListIgnoresDebrisWithoutFailing(t *testing.T) {
	d := newDriver(t)
	ctx := context.Background()
	spec := helperSpec("echo", 0)
	if _, err := d.Create(ctx, spec); err != nil {
		t.Fatalf("Create: %v", err)
	}
	// A stray directory with no state file must not blind the sweep to the
	// real environments beside it.
	if err := os.MkdirAll(filepath.Join(d.root, "not-an-environment"), 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	envs, err := d.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(envs) != 1 {
		t.Errorf("List = %+v, want exactly the one real environment", envs)
	}
}

func TestEnvExpiry(t *testing.T) {
	base := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	env := driver.Env{ExpiresAt: base.Add(time.Minute)}

	if env.Expired(base) {
		t.Error("environment reported expired before its deadline")
	}
	if !env.Expired(base.Add(2 * time.Minute)) {
		t.Error("environment not reported expired after its deadline")
	}
	// An environment with no deadline never expires on time alone.
	if (driver.Env{}).Expired(base) {
		t.Error("environment with no deadline reported as expired")
	}
}

func TestCapabilitiesAreHonest(t *testing.T) {
	d := newDriver(t)
	caps := d.Capabilities()

	if caps.Isolation != driver.IsolationProcess {
		t.Errorf("Isolation = %s, want process", caps.Isolation)
	}
	if caps.EnforcesResourceLimits || caps.EnforcesNetworkPolicy {
		t.Error("process driver must not claim controls it does not implement")
	}
	if !caps.SurvivesControlPlaneRestart {
		t.Error("state is on disk, so restart survival should be claimed")
	}
}

func containsSubstring(haystack []string, needle string) bool {
	for _, s := range haystack {
		if strings.Contains(s, needle) {
			return true
		}
	}
	return false
}
