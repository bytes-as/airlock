//go:build docker

// These tests need a live Docker daemon. Run them with:
//
//	go test -tags docker ./internal/driver/docker/
//
// They are behind a build tag so the default `go test ./...` stays honest on a
// machine without Docker: a suite that silently skips its most important tests
// reports green while proving nothing.
//
// IMPORTANT: as of this commit these have never been executed — the machine
// this driver was written on had no Docker. Expect failures on the first run
// and budget time for them. See _private/notes/mac-handoff-checklist.md.
package docker

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"ephemera/internal/driver"
	"ephemera/internal/job"
)

// testImage is small, present in most caches, and has a shell.
const testImage = "alpine:3.19"

func newTestDriver(t *testing.T, mutate func(*Config)) *Driver {
	t.Helper()

	cfg := DefaultConfig()
	// Alpine has no nobody-writable tmpfs quirks, but running as a specific
	// non-root uid keeps the test honest about the hardening we ship.
	cfg.User = "65534:65534"
	if mutate != nil {
		mutate(&cfg)
	}

	d, err := New(cfg)
	if err != nil {
		t.Skipf("docker daemon not available: %v", err)
	}
	return d
}

func specFor(command []string, deadline time.Duration) driver.EnvSpec {
	return driver.EnvSpec{
		JobID:    "job_integration_" + strings.ReplaceAll(time.Now().Format("150405.000"), ".", ""),
		TenantID: "tenant-test",
		Image:    testImage,
		Command:  command,
		Deadline: deadline,
	}
}

// cleanup destroys an environment even if the test fails, so a failing run does
// not leave containers behind on the developer's machine.
func cleanup(t *testing.T, d *Driver, env driver.Env) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := d.Destroy(ctx, env); err != nil {
			t.Errorf("cleanup: destroy %s: %v", env.ID, err)
		}
	})
}

func TestIntegrationLifecycle(t *testing.T) {
	d := newTestDriver(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	spec := specFor([]string{"sh", "-c", "echo hello from the container; echo a warning >&2"}, time.Minute)
	env, err := d.Create(ctx, spec)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	cleanup(t, d, env)

	if err := d.Start(ctx, env, spec); err != nil {
		t.Fatalf("Start: %v", err)
	}

	lines, err := d.Logs(ctx, env)
	if err != nil {
		t.Fatalf("Logs: %v", err)
	}
	streams := map[string][]string{}
	for line := range lines {
		streams[line.Stream] = append(streams[line.Stream], line.Text)
	}

	status, err := d.Wait(ctx, env)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if !status.Succeeded() {
		t.Fatalf("status = %+v, want success", status)
	}

	if !contains(streams[driver.StreamStdout], "hello from the container") {
		t.Errorf("stdout = %v", streams[driver.StreamStdout])
	}
	// The two streams must stay distinguishable through Docker's framing.
	if !contains(streams[driver.StreamStderr], "a warning") {
		t.Errorf("stderr = %v", streams[driver.StreamStderr])
	}
}

func TestIntegrationArtifactCollection(t *testing.T) {
	d := newTestDriver(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	spec := specFor([]string{"sh", "-c",
		"mkdir -p /artifacts/nested && printf 'png' > /artifacts/shot.png && printf '{}' > /artifacts/nested/result.json"},
		time.Minute)

	env, err := d.Create(ctx, spec)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	cleanup(t, d, env)
	if err := d.Start(ctx, env, spec); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := d.Wait(ctx, env); err != nil {
		t.Fatalf("Wait: %v", err)
	}

	artifacts, err := d.Collect(ctx, env, t.TempDir())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(artifacts) != 2 {
		t.Fatalf("collected %d artifacts, want 2: %+v", len(artifacts), artifacts)
	}

	names := map[string]bool{}
	for _, a := range artifacts {
		names[a.Name] = true
	}
	if !names["shot.png"] || !names["nested/result.json"] {
		t.Errorf("names = %v", names)
	}
}

func TestIntegrationExitCodeAndCrash(t *testing.T) {
	d := newTestDriver(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	spec := specFor([]string{"sh", "-c", "echo failing >&2; exit 7"}, time.Minute)
	env, err := d.Create(ctx, spec)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	cleanup(t, d, env)
	if err := d.Start(ctx, env, spec); err != nil {
		t.Fatalf("Start: %v", err)
	}

	status, err := d.Wait(ctx, env)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if status.ExitCode != 7 {
		t.Errorf("ExitCode = %d, want 7", status.ExitCode)
	}
	failure := status.Failure()
	if failure == nil || failure.Kind != job.FailureAgentCrash {
		t.Errorf("Failure() = %v, want agent_crashed", failure)
	}
}

// TestIntegrationOOMIsDistinctFromACrash: "the agent failed" and "we gave it
// too little memory" are different bugs with different owners.
func TestIntegrationOOMIsDistinctFromACrash(t *testing.T) {
	d := newTestDriver(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	spec := specFor([]string{"sh", "-c", "dd if=/dev/zero of=/dev/null bs=1M count=1 2>/dev/null; " +
		"head -c 200m /dev/zero | tail -c 200m > /dev/null"}, time.Minute)
	spec.Resources.MemoryMiB = 16

	env, err := d.Create(ctx, spec)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	cleanup(t, d, env)
	if err := d.Start(ctx, env, spec); err != nil {
		t.Fatalf("Start: %v", err)
	}

	status, err := d.Wait(ctx, env)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	// Whether the kernel OOM-kills depends on the host, so this asserts the
	// weaker but still meaningful property: it did not silently succeed.
	if status.Succeeded() {
		t.Error("a job well over its memory limit reported success")
	}
	t.Logf("status: exit=%d oom=%v reason=%q", status.ExitCode, status.OOMKilled, status.Reason)
}

// TestIntegrationNoNetworkMeansNoNetwork is deep-dive 1's strongest claim:
// with NetworkNone there is no interface at all.
func TestIntegrationNoNetworkMeansNoNetwork(t *testing.T) {
	d := newTestDriver(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	spec := specFor([]string{"sh", "-c",
		"wget -q -T 3 -O - http://169.254.169.254/latest/meta-data/ && echo REACHED || echo BLOCKED"},
		time.Minute)
	spec.Network = driver.NetworkPolicy{Mode: driver.NetworkNone}

	env, err := d.Create(ctx, spec)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	cleanup(t, d, env)
	if err := d.Start(ctx, env, spec); err != nil {
		t.Fatalf("Start: %v", err)
	}

	lines, err := d.Logs(ctx, env)
	if err != nil {
		t.Fatalf("Logs: %v", err)
	}
	var output []string
	for line := range lines {
		output = append(output, line.Text)
	}
	if _, err := d.Wait(ctx, env); err != nil {
		t.Fatalf("Wait: %v", err)
	}

	if contains(output, "REACHED") {
		t.Errorf("instance metadata was reachable from a network-disabled container: %v", output)
	}
	if !contains(output, "BLOCKED") {
		t.Errorf("expected the request to fail; output was %v", output)
	}
}

// TestIntegrationInternalNetworkBlocksMetadata is the structural egress claim:
// on an internal network there is no route off the network, so the metadata
// endpoint is unreachable by construction rather than by rule.
func TestIntegrationInternalNetworkBlocksMetadata(t *testing.T) {
	network := "ephemera-test-internal"
	d := newTestDriver(t, func(c *Config) { c.Network = network })

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	spec := specFor([]string{"sh", "-c",
		"wget -q -T 3 -O - http://169.254.169.254/latest/meta-data/ && echo REACHED || echo BLOCKED"},
		time.Minute)
	spec.Network = driver.NetworkPolicy{Mode: driver.NetworkProxied}

	env, err := d.Create(ctx, spec)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	cleanup(t, d, env)
	if err := d.Start(ctx, env, spec); err != nil {
		t.Fatalf("Start: %v", err)
	}

	lines, err := d.Logs(ctx, env)
	if err != nil {
		t.Fatalf("Logs: %v", err)
	}
	var output []string
	for line := range lines {
		output = append(output, line.Text)
	}
	if _, err := d.Wait(ctx, env); err != nil {
		t.Fatalf("Wait: %v", err)
	}

	if contains(output, "REACHED") {
		t.Errorf("instance metadata reachable from an internal network: %v", output)
	}
}

// TestIntegrationOrphanSurvivesControlPlaneRestart: a fresh driver instance,
// sharing only the daemon, must find and destroy what an old one left.
func TestIntegrationOrphanSurvivesControlPlaneRestart(t *testing.T) {
	first := newTestDriver(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	spec := specFor([]string{"sh", "-c", "sleep 120"}, 5*time.Minute)
	env, err := first.Create(ctx, spec)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := first.Start(ctx, env, spec); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// A different Driver instance: no shared memory, only the daemon.
	second := newTestDriver(t, nil)
	cleanup(t, second, env)

	envs, err := second.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	var found *driver.Env
	for i := range envs {
		if envs[i].ID == env.ID {
			found = &envs[i]
		}
	}
	if found == nil {
		t.Fatalf("restarted driver did not find the orphan among %d environments", len(envs))
	}
	if found.JobID != spec.JobID {
		t.Errorf("orphan lost its job association: %+v", found)
	}
	if found.ExpiresAt.IsZero() {
		t.Error("orphan lost its deadline; the reaper could not enforce it")
	}

	status, err := second.Describe(ctx, *found)
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}
	if status.Phase != driver.PhaseRunning {
		t.Errorf("Phase = %s, want running", status.Phase)
	}

	if err := second.Destroy(ctx, *found); err != nil {
		t.Fatalf("Destroy orphan: %v", err)
	}
	after, err := second.Describe(ctx, *found)
	if err != nil {
		t.Fatalf("Describe after destroy: %v", err)
	}
	if after.Phase != driver.PhaseGone {
		t.Errorf("Phase = %s after destroy, want gone", after.Phase)
	}
}

func TestIntegrationDestroyIsIdempotent(t *testing.T) {
	d := newTestDriver(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	spec := specFor([]string{"sh", "-c", "true"}, time.Minute)
	env, err := d.Create(ctx, spec)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := d.Destroy(ctx, env); err != nil {
			t.Fatalf("Destroy call %d: %v", i+1, err)
		}
	}
	if err := d.Destroy(ctx, driver.Env{ID: "0123456789abcdef", Driver: "docker"}); err != nil {
		t.Errorf("Destroy of an unknown container = %v, want nil", err)
	}
}

func TestIntegrationSecretsReachTheContainerNotTheLabels(t *testing.T) {
	const secret = "sk-live-integration-secret"

	d := newTestDriver(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	spec := specFor([]string{"sh", "-c", "echo token is $API_TOKEN"}, time.Minute)
	spec.Secrets = map[string]job.Secret{"API_TOKEN": job.Secret(secret)}

	env, err := d.Create(ctx, spec)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	cleanup(t, d, env)
	if err := d.Start(ctx, env, spec); err != nil {
		t.Fatalf("Start: %v", err)
	}

	lines, err := d.Logs(ctx, env)
	if err != nil {
		t.Fatalf("Logs: %v", err)
	}
	var output []string
	for line := range lines {
		output = append(output, line.Text)
	}

	if !contains(output, "token is "+secret) {
		t.Errorf("secret did not reach the container: %v", output)
	}

	// It must not be visible to anyone who can inspect the container.
	summaries, err := d.client.ContainerList(ctx, map[string]string{LabelJob: spec.JobID})
	if err != nil {
		t.Fatalf("ContainerList: %v", err)
	}
	for _, summary := range summaries {
		for key, value := range summary.Labels {
			if strings.Contains(value, secret) {
				t.Errorf("secret leaked into label %s", key)
			}
		}
		for _, name := range summary.Names {
			if strings.Contains(name, secret) {
				t.Error("secret leaked into the container name")
			}
		}
	}
}

func TestIntegrationCreateRefusesUnenforceablePolicy(t *testing.T) {
	d := newTestDriver(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	spec := specFor([]string{"true"}, time.Minute)
	spec.Network = driver.NetworkPolicy{Mode: driver.NetworkEgress, DenyCIDRs: driver.DefaultDenyCIDRs()}

	_, err := d.Create(ctx, spec)
	var unsupported *driver.UnsupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("Create error = %v, want *driver.UnsupportedError", err)
	}
}

func TestIntegrationReadOnlyRootFilesystem(t *testing.T) {
	d := newTestDriver(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// The image must be read-only; only the granted tmpfs mounts are writable.
	spec := specFor([]string{"sh", "-c",
		"touch /usr/local/should-fail 2>/dev/null && echo WRITABLE || echo READONLY; " +
			"touch /artifacts/ok && echo ARTIFACTS_WRITABLE"}, time.Minute)

	env, err := d.Create(ctx, spec)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	cleanup(t, d, env)
	if err := d.Start(ctx, env, spec); err != nil {
		t.Fatalf("Start: %v", err)
	}

	lines, err := d.Logs(ctx, env)
	if err != nil {
		t.Fatalf("Logs: %v", err)
	}
	var output []string
	for line := range lines {
		output = append(output, line.Text)
	}

	if contains(output, "WRITABLE") {
		t.Errorf("root filesystem was writable: %v", output)
	}
	if !contains(output, "ARTIFACTS_WRITABLE") {
		t.Errorf("artifact directory was not writable, so agents cannot save output: %v", output)
	}
}

func contains(values []string, needle string) bool {
	for _, v := range values {
		if strings.Contains(v, needle) {
			return true
		}
	}
	return false
}

var _ = os.Getenv
