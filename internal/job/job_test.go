package job

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"
)

var epoch = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

func TestTransitionGraph(t *testing.T) {
	t.Parallel()
	cases := []struct {
		from, to State
		want     bool
	}{
		{StatePending, StateQueued, true},
		{StateQueued, StateProvisioning, true},
		{StateProvisioning, StateRunning, true},
		{StateRunning, StateSucceeded, true},
		// The one legal backwards edge: retry after a provisioning failure.
		{StateProvisioning, StateQueued, true},
		// A job that reached Running is never re-queued: the agent may have had
		// side effects out in the world.
		{StateRunning, StateQueued, false},
		{StateRunning, StateProvisioning, false},
		// Terminal states are terminal.
		{StateSucceeded, StateRunning, false},
		{StateFailed, StateQueued, false},
		{StateCancelled, StateRunning, false},
		// No skipping ahead.
		{StatePending, StateRunning, false},
		{StateQueued, StateSucceeded, false},
	}
	for _, c := range cases {
		if got := CanTransition(c.from, c.to); got != c.want {
			t.Errorf("CanTransition(%s, %s) = %v, want %v", c.from, c.to, got, c.want)
		}
	}
}

func TestTerminalStatesHaveNoOutEdges(t *testing.T) {
	t.Parallel()
	for state, out := range transitions {
		if state.Terminal() && len(out) != 0 {
			t.Errorf("terminal state %s has out-edges %v", state, out)
		}
		if !state.Terminal() && len(out) == 0 {
			t.Errorf("non-terminal state %s is a dead end", state)
		}
	}
}

func TestTransitionRejectsIllegalEdge(t *testing.T) {
	t.Parallel()
	j := New("tenant-a", PriorityNormal, Spec{Image: "img"}, epoch)
	if err := j.Transition(StateRunning, epoch); err == nil {
		t.Fatal("expected pending -> running to be rejected")
	}
	// The rejected transition must not have mutated the job.
	if j.State != StatePending {
		t.Errorf("state mutated on failed transition: %s", j.State)
	}
	var te *TransitionError
	if err := j.Transition(StateSucceeded, epoch); !errors.As(err, &te) {
		t.Errorf("want *TransitionError, got %T", err)
	}
}

func TestTransitionStampsTimes(t *testing.T) {
	t.Parallel()
	j := New("tenant-a", PriorityNormal, Spec{Image: "img"}, epoch)
	mustTransition(t, j, StateQueued, epoch)
	mustTransition(t, j, StateProvisioning, epoch)

	if j.StartedAt != nil {
		t.Error("StartedAt set before the agent started")
	}
	start := epoch.Add(2 * time.Second)
	mustTransition(t, j, StateRunning, start)
	if j.StartedAt == nil || !j.StartedAt.Equal(start) {
		t.Fatalf("StartedAt = %v, want %v", j.StartedAt, start)
	}

	end := start.Add(30 * time.Second)
	mustTransition(t, j, StateSucceeded, end)
	if j.EndedAt == nil || !j.EndedAt.Equal(end) {
		t.Fatalf("EndedAt = %v, want %v", j.EndedAt, end)
	}
	if got := j.Duration(end); got != 30*time.Second {
		t.Errorf("Duration = %v, want 30s", got)
	}
}

func TestFailRecordsCauseAndAttempts(t *testing.T) {
	t.Parallel()
	j := New("tenant-a", PriorityNormal, Spec{Image: "img"}, epoch)
	mustTransition(t, j, StateQueued, epoch)
	j.Attempts = 3

	underlying := errors.New("connection refused")
	if err := j.Fail(FailureProvision.Wrap(underlying, "docker daemon unreachable"), epoch); err != nil {
		t.Fatalf("Fail: %v", err)
	}
	if j.State != StateFailed {
		t.Fatalf("state = %s, want failed", j.State)
	}
	if j.Failure.Attempts != 3 {
		t.Errorf("Attempts = %d, want 3", j.Failure.Attempts)
	}
	if !errors.Is(j.Failure, underlying) {
		t.Error("underlying cause not reachable via errors.Is")
	}
	if KindOf(j.Failure) != FailureProvision {
		t.Errorf("KindOf = %s, want %s", KindOf(j.Failure), FailureProvision)
	}
}

func TestFailWithoutCauseStillTerminates(t *testing.T) {
	t.Parallel()
	j := New("t", PriorityNormal, Spec{Image: "img"}, epoch)
	mustTransition(t, j, StateQueued, epoch)
	if err := j.Fail(nil, epoch); err != nil {
		t.Fatalf("Fail(nil): %v", err)
	}
	if j.Failure == nil || j.Failure.Kind != FailureInternal {
		t.Errorf("want a synthesised internal failure, got %+v", j.Failure)
	}
}

func TestRetryPolicy(t *testing.T) {
	t.Parallel()
	// Infrastructure faults retry; faults caused by the job itself do not.
	retryable := []FailureKind{FailureProvision, FailureStart, FailureInternal}
	terminal := []FailureKind{FailureAgentCrash, FailureDeadline, FailureReapedOrphan, FailureRejected}

	for _, k := range retryable {
		if !k.Retryable() {
			t.Errorf("%s should be retryable", k)
		}
	}
	for _, k := range terminal {
		if k.Retryable() {
			t.Errorf("%s should not be retryable", k)
		}
	}

	j := New("t", PriorityNormal, Spec{Image: "img", MaxAttempts: 3}, epoch)
	j.Attempts = 2
	if !j.ShouldRetry(FailureProvision, 5) {
		t.Error("attempt 2 of 3 should retry")
	}
	j.Attempts = 3
	if j.ShouldRetry(FailureProvision, 5) {
		t.Error("attempt 3 of 3 should not retry")
	}
	// A crash is never retried, however many attempts remain.
	j.Attempts = 0
	if j.ShouldRetry(FailureAgentCrash, 5) {
		t.Error("agent crash must never auto-retry")
	}
}

func TestFaultAttribution(t *testing.T) {
	t.Parallel()
	user := []FailureKind{FailureAgentCrash, FailureDeadline, FailureRejected}
	system := []FailureKind{FailureProvision, FailureStart, FailureInternal, FailureReapedOrphan}
	for _, k := range user {
		if k.Fault() != FaultUser {
			t.Errorf("%s: want user fault", k)
		}
	}
	for _, k := range system {
		if k.Fault() != FaultSystem {
			t.Errorf("%s: want system fault", k)
		}
	}
}

func TestKindOfUnclassifiedErrorIsSystemFault(t *testing.T) {
	t.Parallel()
	// An error we never classified is one we did not think about. It must land as
	// our fault, not be quietly blamed on the caller.
	k := KindOf(errors.New("something we forgot to wrap"))
	if k != FailureInternal {
		t.Fatalf("KindOf = %s, want %s", k, FailureInternal)
	}
	if k.Fault() != FaultSystem {
		t.Errorf("unclassified error attributed to %s, want system", k.Fault())
	}
}

// TestIDsAreTimeOrdered is the regression test for the ID layout: the timestamp
// must occupy its own bytes, undisturbed by the random suffix. An earlier version
// let the random fill overwrite the low bytes of the timestamp, which collapsed
// ordering granularity to roughly a minute.
func TestIDsAreTimeOrdered(t *testing.T) {
	t.Parallel()
	var ids []string
	for i := 0; i < 200; i++ {
		ids = append(ids, NewID(epoch.Add(time.Duration(i)*time.Millisecond)))
	}
	if !sort.StringsAreSorted(ids) {
		t.Error("IDs generated 1ms apart do not sort in time order")
	}
}

func TestIDsAreUniqueWithinTheSameMillisecond(t *testing.T) {
	t.Parallel()
	seen := make(map[string]bool, 10000)
	for i := 0; i < 10000; i++ {
		id := NewID(epoch)
		if seen[id] {
			t.Fatalf("duplicate ID %s at iteration %d", id, i)
		}
		seen[id] = true
	}
}

func TestIDFormat(t *testing.T) {
	t.Parallel()
	id := NewID(epoch)
	if !strings.HasPrefix(id, "job_") {
		t.Errorf("id %q missing job_ prefix", id)
	}
	// Base32 of 16 bytes, unpadded, is 26 characters.
	if body := strings.TrimPrefix(id, "job_"); len(body) != 26 {
		t.Errorf("id body %q is %d chars, want 26", body, len(body))
	}
}

// TestSecretsDoNotLeak covers the three routes a credential actually escapes:
// a format verb, a %#v dump, and JSON encoding of a struct that contains it.
func TestSecretsDoNotLeak(t *testing.T) {
	t.Parallel()
	const real = "hunter2-super-secret"
	s := Secret(real)

	for _, format := range []string{"%s", "%v", "%q", "%#v", "%+v"} {
		if got := fmt.Sprintf(format, s); strings.Contains(got, real) {
			t.Errorf("Sprintf(%q) leaked the secret: %s", format, got)
		}
	}

	wrapper := struct {
		Token Secret `json:"token"`
	}{Token: s}
	encoded, err := json.Marshal(wrapper)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(encoded), real) {
		t.Errorf("JSON encoding leaked the secret: %s", encoded)
	}

	if s.Reveal() != real {
		t.Error("Reveal must return the true value")
	}
}

func mustTransition(t *testing.T, j *Job, to State, now time.Time) {
	t.Helper()
	if err := j.Transition(to, now); err != nil {
		t.Fatalf("transition to %s: %v", to, err)
	}
}
