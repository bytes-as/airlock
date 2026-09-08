package admission

import (
	"sync"
	"testing"
	"time"
)

var base = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *clock { return &clock{t: base} }

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func TestBurstThenSustainedRate(t *testing.T) {
	c := newClock()
	ctrl := New(Limits{SubmitsPerSecond: 10, Burst: 5, MaxConcurrent: 0}, WithClock(c.Now))

	// The burst allowance is spendable immediately: a client submitting a batch
	// of legitimate work should not be throttled for behaving normally.
	for i := 0; i < 5; i++ {
		if d := ctrl.Admit("tenant-a", 0); !d.Allowed() {
			t.Fatalf("submission %d refused within burst: %s", i, d.Error())
		}
	}

	refused := ctrl.Admit("tenant-a", 0)
	if refused.Outcome != OutcomeRateLimited {
		t.Fatalf("Outcome = %s, want rate_limited once burst is spent", refused.Outcome)
	}
	if refused.RetryAfter <= 0 {
		t.Error("a refusal with no retry hint invites a retry storm")
	}

	// At 10/sec, one token takes 100ms.
	c.Advance(100 * time.Millisecond)
	if d := ctrl.Admit("tenant-a", 0); !d.Allowed() {
		t.Errorf("refused after waiting for a refill: %s", d.Error())
	}
}

func TestRefillIsCappedAtBurst(t *testing.T) {
	c := newClock()
	ctrl := New(Limits{SubmitsPerSecond: 10, Burst: 5}, WithClock(c.Now))

	// An idle tenant must not bank unlimited credit and then flood.
	c.Advance(time.Hour)

	allowed := 0
	for i := 0; i < 100; i++ {
		if ctrl.Admit("tenant-a", 0).Allowed() {
			allowed++
		}
	}
	if allowed != 5 {
		t.Errorf("allowed %d immediate submissions after an idle hour, want burst of 5", allowed)
	}
}

func TestRetryHintIsActionable(t *testing.T) {
	c := newClock()
	ctrl := New(Limits{SubmitsPerSecond: 4, Burst: 1}, WithClock(c.Now))

	ctrl.Admit("tenant-a", 0) // spends the only token
	d := ctrl.Admit("tenant-a", 0)
	if d.Allowed() {
		t.Fatal("second submission allowed with a burst of 1")
	}

	// At 4/sec a token takes 250ms. Waiting exactly that long must work: a
	// retry hint the client obeys and is then refused again is worse than none.
	c.Advance(d.RetryAfter)
	if again := ctrl.Admit("tenant-a", 0); !again.Allowed() {
		t.Errorf("refused after honouring RetryAfter of %v: %s", d.RetryAfter, again.Error())
	}
}

func TestRetryHintNeverSubMillisecond(t *testing.T) {
	c := newClock()
	ctrl := New(Limits{SubmitsPerSecond: 100000, Burst: 1}, WithClock(c.Now))
	ctrl.Admit("t", 0)
	d := ctrl.Admit("t", 0)
	if d.Allowed() {
		t.Fatal("expected refusal")
	}
	if d.RetryAfter < time.Millisecond {
		t.Errorf("RetryAfter = %v; sub-millisecond hints invite the hot loop we are preventing", d.RetryAfter)
	}
}

func TestTenantsAreIsolated(t *testing.T) {
	c := newClock()
	ctrl := New(Limits{SubmitsPerSecond: 1, Burst: 2}, WithClock(c.Now))

	// Tenant A exhausts its allowance.
	ctrl.Admit("tenant-a", 0)
	ctrl.Admit("tenant-a", 0)
	if ctrl.Admit("tenant-a", 0).Allowed() {
		t.Fatal("tenant-a not limited")
	}

	// Tenant B is unaffected. Multi-tenancy means one noisy tenant cannot
	// degrade another.
	for i := 0; i < 2; i++ {
		if d := ctrl.Admit("tenant-b", 0); !d.Allowed() {
			t.Errorf("tenant-b refused because tenant-a misbehaved: %s", d.Error())
		}
	}
}

// TestConcurrencyQuotaIsIndependentOfRate is the reason there are two controls:
// a tenant can be well within its submission rate and still be entitled to no
// more simultaneous execution.
func TestConcurrencyQuotaIsIndependentOfRate(t *testing.T) {
	c := newClock()
	ctrl := New(Limits{SubmitsPerSecond: 1000, Burst: 1000, MaxConcurrent: 3}, WithClock(c.Now))

	for i := 0; i < 3; i++ {
		if d := ctrl.Admit("tenant-a", 0); !d.Allowed() {
			t.Fatalf("submission %d refused: %s", i, d.Error())
		}
		ctrl.Started("tenant-a")
	}

	d := ctrl.Admit("tenant-a", 0)
	if d.Outcome != OutcomeQuotaExceeded {
		t.Fatalf("Outcome = %s, want quota_exceeded (rate limit is nowhere near)", d.Outcome)
	}

	// Finishing one frees exactly one slot.
	ctrl.Finished("tenant-a")
	if !ctrl.Admit("tenant-a", 0).Allowed() {
		t.Error("slot not released after a job finished")
	}
}

// TestQueueFullOutranksTenantLimits: when the whole system is saturated, the
// operator needs to see saturation rather than have the caller blamed.
func TestQueueFullOutranksTenantLimits(t *testing.T) {
	c := newClock()
	ctrl := New(Limits{SubmitsPerSecond: 1000, Burst: 1000, MaxConcurrent: 1},
		WithClock(c.Now), WithGlobalCapacity(10))

	ctrl.Started("tenant-a") // tenant is also at its own quota

	d := ctrl.Admit("tenant-a", 10)
	if d.Outcome != OutcomeQueueFull {
		t.Errorf("Outcome = %s, want queue_full to take precedence", d.Outcome)
	}
	if d.RetryAfter <= 0 {
		t.Error("queue_full refusal must carry a retry hint")
	}
}

func TestPerTenantOverrides(t *testing.T) {
	c := newClock()
	ctrl := New(Limits{SubmitsPerSecond: 1, Burst: 1},
		WithClock(c.Now),
		WithTenantLimits(map[string]Limits{
			"premium": {SubmitsPerSecond: 100, Burst: 50, MaxConcurrent: 20},
		}))

	allowed := 0
	for i := 0; i < 50; i++ {
		if ctrl.Admit("premium", 0).Allowed() {
			allowed++
		}
	}
	if allowed != 50 {
		t.Errorf("premium tenant allowed %d of 50, want its own larger burst", allowed)
	}

	if !ctrl.Admit("standard", 0).Allowed() {
		t.Error("standard tenant refused its first submission")
	}
	if ctrl.Admit("standard", 0).Allowed() {
		t.Error("standard tenant should be limited by the default burst of 1")
	}
}

func TestSetTenantLimitsAtRuntime(t *testing.T) {
	c := newClock()
	ctrl := New(Limits{SubmitsPerSecond: 1, Burst: 1}, WithClock(c.Now))

	ctrl.Admit("tenant-a", 0)
	if ctrl.Admit("tenant-a", 0).Allowed() {
		t.Fatal("expected the default limit to bite")
	}

	ctrl.SetTenantLimits("tenant-a", Limits{SubmitsPerSecond: 100, Burst: 100})
	c.Advance(time.Second)
	if !ctrl.Admit("tenant-a", 0).Allowed() {
		t.Error("raised limits did not take effect")
	}
}

// TestFinishedClampsAtZero: an unbalanced Finished is a bug, but a negative
// counter would silently grant unlimited concurrency, turning a small bug into
// an outage.
func TestFinishedClampsAtZero(t *testing.T) {
	ctrl := New(Limits{SubmitsPerSecond: 10, Burst: 10, MaxConcurrent: 2})

	for i := 0; i < 5; i++ {
		ctrl.Finished("tenant-a")
	}
	if got := ctrl.Running("tenant-a"); got != 0 {
		t.Fatalf("Running = %d, want 0", got)
	}

	ctrl.Started("tenant-a")
	ctrl.Started("tenant-a")
	if ctrl.HasCapacity("tenant-a") {
		t.Error("quota not enforced after unbalanced Finished calls")
	}
}

// TestHasCapacityDoesNotSpendTokens: the scheduler asks whether an
// already-queued job may start. A job is admitted once, at submission, and must
// not be charged against the submission rate again on every dispatch attempt.
func TestHasCapacityDoesNotSpendTokens(t *testing.T) {
	c := newClock()
	ctrl := New(Limits{SubmitsPerSecond: 1, Burst: 3, MaxConcurrent: 5}, WithClock(c.Now))

	for i := 0; i < 100; i++ {
		ctrl.HasCapacity("tenant-a")
	}

	allowed := 0
	for i := 0; i < 3; i++ {
		if ctrl.Admit("tenant-a", 0).Allowed() {
			allowed++
		}
	}
	if allowed != 3 {
		t.Errorf("allowed %d of 3, want the full burst — HasCapacity consumed tokens", allowed)
	}
}

func TestUnlimitedConcurrency(t *testing.T) {
	ctrl := New(Limits{SubmitsPerSecond: 1000, Burst: 1000, MaxConcurrent: 0})
	for i := 0; i < 100; i++ {
		ctrl.Started("tenant-a")
	}
	if !ctrl.HasCapacity("tenant-a") {
		t.Error("MaxConcurrent of 0 should mean unlimited")
	}
	if !ctrl.Admit("tenant-a", 0).Allowed() {
		t.Error("submission refused despite unlimited concurrency")
	}
}

func TestSnapshotReportsUsage(t *testing.T) {
	c := newClock()
	ctrl := New(Limits{SubmitsPerSecond: 10, Burst: 10, MaxConcurrent: 4}, WithClock(c.Now))

	ctrl.Admit("tenant-a", 0)
	ctrl.Started("tenant-a")
	ctrl.Started("tenant-a")

	snap := ctrl.Snapshot()
	usage, ok := snap["tenant-a"]
	if !ok {
		t.Fatalf("tenant-a missing from snapshot: %+v", snap)
	}
	if usage.Running != 2 {
		t.Errorf("Running = %d, want 2", usage.Running)
	}
	if usage.MaxConcurrent != 4 {
		t.Errorf("MaxConcurrent = %d, want 4", usage.MaxConcurrent)
	}
	if usage.TokensAvailable >= 10 {
		t.Errorf("TokensAvailable = %v, want less than the burst after a submission", usage.TokensAvailable)
	}
}

// TestConcurrentAdmitIsRaceFree exercises the controller the way the API will:
// many goroutines admitting at once. Run with -race, this is the test that
// catches a missing lock.
func TestConcurrentAdmitIsRaceFree(t *testing.T) {
	c := newClock()
	ctrl := New(Limits{SubmitsPerSecond: 1000, Burst: 100, MaxConcurrent: 50}, WithClock(c.Now))

	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted := 0

	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 25; j++ {
				if ctrl.Admit("tenant-a", 0).Allowed() {
					mu.Lock()
					accepted++
					mu.Unlock()
					ctrl.Started("tenant-a")
					ctrl.Finished("tenant-a")
				}
			}
		}()
	}
	wg.Wait()

	// The clock never advances, so no tokens are refilled: acceptance is capped
	// at exactly the burst. Anything more means tokens were double-spent.
	if accepted != 100 {
		t.Errorf("accepted %d submissions, want exactly the burst of 100", accepted)
	}
	if got := ctrl.TotalRunning(); got != 0 {
		t.Errorf("TotalRunning = %d after all jobs finished, want 0", got)
	}
}

func TestDecisionErrorIsInformative(t *testing.T) {
	d := Decision{Outcome: OutcomeRateLimited, RetryAfter: 250 * time.Millisecond, Reason: "too fast"}
	msg := d.Error()
	for _, want := range []string{"rate_limited", "too fast", "250ms"} {
		if !contains(msg, want) {
			t.Errorf("Error() = %q, want it to mention %q", msg, want)
		}
	}
	if (Decision{Outcome: OutcomeAccepted}).Error() != "" {
		t.Error("an accepted decision should have no error string")
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (func() bool {
		for i := 0; i+len(needle) <= len(haystack); i++ {
			if haystack[i:i+len(needle)] == needle {
				return true
			}
		}
		return false
	})()
}
