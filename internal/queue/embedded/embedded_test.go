package embedded

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bytes-as/airlock/internal/job"
	"github.com/bytes-as/airlock/internal/queue"
)

var base = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

// clock is a hand-driven time source. Lease expiry is a time-dependent
// behaviour, and testing it by sleeping would make the suite both slow and
// flaky; advancing a clock explicitly makes it neither.
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

func newQueue(t *testing.T, opts ...Option) (*Queue, *clock) {
	t.Helper()
	c := newClock()
	opts = append([]Option{WithClock(c.Now)}, opts...)
	q, err := Open(filepath.Join(t.TempDir(), "queue.db"), opts...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { q.Close() })
	return q, c
}

// submit enqueues a job with an explicit submission time, so ordering tests can
// control the tiebreak precisely.
func submit(t *testing.T, q *Queue, tenant string, priority job.Priority, at time.Time) *job.Job {
	t.Helper()
	j := job.New(tenant, priority, job.Spec{Image: "img", Command: []string{"run"}}, at)
	if err := q.Enqueue(context.Background(), j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	return j
}

func TestEnqueueTransitionsToQueued(t *testing.T) {
	q, _ := newQueue(t)
	ctx := context.Background()

	j := submit(t, q, "tenant-a", job.PriorityNormal, base)
	stored, err := q.Get(ctx, j.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if stored.State != job.StateQueued {
		t.Errorf("State = %s, want queued", stored.State)
	}
}

// TestEnqueueIsIdempotent guards the retry path: a client that resubmits after
// a timeout must not get the work done twice.
func TestEnqueueIsIdempotent(t *testing.T) {
	q, _ := newQueue(t)
	ctx := context.Background()

	j := job.New("tenant-a", job.PriorityNormal, job.Spec{Image: "img"}, base)
	for i := 0; i < 3; i++ {
		if err := q.Enqueue(ctx, j); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}

	stats, err := q.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.Ready != 1 {
		t.Errorf("Ready = %d, want 1 after three identical enqueues", stats.Ready)
	}
}

func TestClaimOrdersByPriorityThenAge(t *testing.T) {
	q, _ := newQueue(t)
	ctx := context.Background()

	// Submitted deliberately out of dispatch order.
	normalOld := submit(t, q, "t", job.PriorityNormal, base)
	low := submit(t, q, "t", job.PriorityLow, base.Add(time.Second))
	highNew := submit(t, q, "t", job.PriorityHigh, base.Add(2*time.Second))
	highOld := submit(t, q, "t", job.PriorityHigh, base.Add(-time.Hour))
	normalNew := submit(t, q, "t", job.PriorityNormal, base.Add(3*time.Second))

	want := []string{
		highOld.ID,   // high priority, waited longest
		highNew.ID,   // high priority
		normalOld.ID, // normal, older
		normalNew.ID, // normal, newer
		low.ID,       // low
	}

	for i, wantID := range want {
		claimed, err := q.Claim(ctx, "worker-1", time.Minute)
		if err != nil {
			t.Fatalf("Claim %d: %v", i, err)
		}
		if claimed.ID != wantID {
			t.Fatalf("claim %d = %s (priority %d), want %s", i, claimed.ID, claimed.Priority, wantID)
		}
	}

	if _, err := q.Claim(ctx, "worker-1", time.Minute); !errors.Is(err, queue.ErrEmpty) {
		t.Errorf("Claim on drained queue = %v, want ErrEmpty", err)
	}
}

// TestHighPriorityDoesNotStarveEqualPeers is the fairness property: within a
// priority level the queue is strictly FIFO, so a busy stream of high-priority
// work cannot indefinitely postpone another high-priority job.
func TestHighPriorityDoesNotStarveEqualPeers(t *testing.T) {
	q, _ := newQueue(t)
	ctx := context.Background()

	first := submit(t, q, "t", job.PriorityHigh, base)
	for i := 1; i <= 20; i++ {
		submit(t, q, "t", job.PriorityHigh, base.Add(time.Duration(i)*time.Second))
	}

	claimed, err := q.Claim(ctx, "worker-1", time.Minute)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if claimed.ID != first.ID {
		t.Errorf("claimed %s, want the oldest high-priority job %s", claimed.ID, first.ID)
	}
}

func TestClaimMarksProvisioningAndCountsAttempts(t *testing.T) {
	q, _ := newQueue(t)
	ctx := context.Background()
	submit(t, q, "t", job.PriorityNormal, base)

	claimed, err := q.Claim(ctx, "worker-1", time.Minute)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if claimed.State != job.StateProvisioning {
		t.Errorf("State = %s, want provisioning", claimed.State)
	}
	if claimed.Attempts != 1 {
		t.Errorf("Attempts = %d, want 1", claimed.Attempts)
	}

	stats, _ := q.Stats(ctx)
	if stats.Ready != 0 || stats.Claimed != 1 {
		t.Errorf("Stats = %+v, want ready 0 / claimed 1", stats)
	}
}

// TestExpiredLeaseIsRecovered is the crash-survival property: a worker that
// dies holding a job must not take the job with it.
func TestExpiredLeaseIsRecovered(t *testing.T) {
	q, c := newQueue(t)
	ctx := context.Background()
	original := submit(t, q, "t", job.PriorityNormal, base)

	claimed, err := q.Claim(ctx, "worker-doomed", 30*time.Second)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	// The worker dies here: no heartbeat, no release, no completion.
	c.Advance(31 * time.Second)

	recovered, err := q.RecoverExpired(ctx, c.Now())
	if err != nil {
		t.Fatalf("RecoverExpired: %v", err)
	}
	if len(recovered) != 1 || recovered[0].ID != original.ID {
		t.Fatalf("recovered = %+v, want job %s", recovered, original.ID)
	}
	if recovered[0].State != job.StateQueued {
		t.Errorf("recovered job state = %s, want queued", recovered[0].State)
	}

	// It must be claimable again, by someone else.
	again, err := q.Claim(ctx, "worker-healthy", time.Minute)
	if err != nil {
		t.Fatalf("Claim after recovery: %v", err)
	}
	if again.ID != claimed.ID {
		t.Errorf("claimed %s, want the recovered job %s", again.ID, claimed.ID)
	}
	if again.Attempts != 2 {
		t.Errorf("Attempts = %d, want 2 on the second claim", again.Attempts)
	}
}

func TestHeartbeatPreventsRecovery(t *testing.T) {
	q, c := newQueue(t)
	ctx := context.Background()
	submit(t, q, "t", job.PriorityNormal, base)

	claimed, err := q.Claim(ctx, "worker-1", 30*time.Second)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	// A healthy worker on a long job: still alive, still working.
	for i := 0; i < 5; i++ {
		c.Advance(20 * time.Second)
		if err := q.Heartbeat(ctx, claimed.ID, "worker-1", 30*time.Second); err != nil {
			t.Fatalf("Heartbeat %d: %v", i, err)
		}
		recovered, err := q.RecoverExpired(ctx, c.Now())
		if err != nil {
			t.Fatalf("RecoverExpired: %v", err)
		}
		if len(recovered) != 0 {
			t.Fatalf("heartbeated job was recovered from under its worker: %+v", recovered)
		}
	}
}

// TestLeaseGuardsAgainstDoubleExecution is the correctness core of leasing: a
// worker whose lease lapsed while it was busy must be refused, not allowed to
// write its result over the worker that took over.
func TestLeaseGuardsAgainstDoubleExecution(t *testing.T) {
	q, c := newQueue(t)
	ctx := context.Background()
	submit(t, q, "t", job.PriorityNormal, base)

	stalled, err := q.Claim(ctx, "worker-stalled", 30*time.Second)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	c.Advance(31 * time.Second)
	if _, err := q.RecoverExpired(ctx, c.Now()); err != nil {
		t.Fatalf("RecoverExpired: %v", err)
	}
	if _, err := q.Claim(ctx, "worker-took-over", time.Minute); err != nil {
		t.Fatalf("Claim by second worker: %v", err)
	}

	// The stalled worker wakes up and tries to act on a job it no longer holds.
	if err := q.Heartbeat(ctx, stalled.ID, "worker-stalled", time.Minute); !errors.Is(err, queue.ErrNotLeaseHolder) {
		t.Errorf("Heartbeat by stale worker = %v, want ErrNotLeaseHolder", err)
	}
	if err := q.Release(ctx, stalled.ID, "worker-stalled", true); !errors.Is(err, queue.ErrNotLeaseHolder) {
		t.Errorf("Release by stale worker = %v, want ErrNotLeaseHolder", err)
	}
}

// TestRequeuePreservesQueuePosition: a job that already waited, then failed to
// provision, should not go to the back of the queue behind everything submitted
// since. It has waited longest, so it goes first.
func TestRequeuePreservesQueuePosition(t *testing.T) {
	q, _ := newQueue(t)
	ctx := context.Background()

	old := submit(t, q, "t", job.PriorityNormal, base)
	newer := submit(t, q, "t", job.PriorityNormal, base.Add(time.Minute))

	claimed, err := q.Claim(ctx, "worker-1", time.Minute)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if claimed.ID != old.ID {
		t.Fatalf("claimed %s, want %s", claimed.ID, old.ID)
	}

	// Provisioning failed; put it back.
	if err := q.Release(ctx, old.ID, "worker-1", true); err != nil {
		t.Fatalf("Release: %v", err)
	}

	next, err := q.Claim(ctx, "worker-1", time.Minute)
	if err != nil {
		t.Fatalf("Claim after requeue: %v", err)
	}
	if next.ID != old.ID {
		t.Errorf("claimed %s after requeue, want the older job %s to keep its place (newer was %s)", next.ID, old.ID, newer.ID)
	}
}

func TestCompleteRetainsTheRecord(t *testing.T) {
	q, _ := newQueue(t)
	ctx := context.Background()
	submit(t, q, "t", job.PriorityNormal, base)

	claimed, err := q.Claim(ctx, "worker-1", time.Minute)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if err := claimed.Transition(job.StateRunning, base); err != nil {
		t.Fatalf("transition: %v", err)
	}
	if err := claimed.Transition(job.StateSucceeded, base); err != nil {
		t.Fatalf("transition: %v", err)
	}
	if err := q.Complete(ctx, claimed); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	// The record survives, because "what happened to job X" is the question an
	// operator actually asks.
	stored, err := q.Get(ctx, claimed.ID)
	if err != nil {
		t.Fatalf("Get after complete: %v", err)
	}
	if stored.State != job.StateSucceeded {
		t.Errorf("State = %s, want succeeded", stored.State)
	}

	stats, _ := q.Stats(ctx)
	if stats.Terminal != 1 || stats.Claimed != 0 || stats.Ready != 0 {
		t.Errorf("Stats = %+v, want terminal 1 / claimed 0 / ready 0", stats)
	}
}

func TestCompleteRejectsNonTerminalJob(t *testing.T) {
	q, _ := newQueue(t)
	ctx := context.Background()
	submit(t, q, "t", job.PriorityNormal, base)

	claimed, _ := q.Claim(ctx, "worker-1", time.Minute)
	if err := q.Complete(ctx, claimed); err == nil {
		t.Error("Complete accepted a job still in provisioning")
	}
}

// TestCapacityRefusesRatherThanAccumulates: backpressure means saying no at
// admission, not accepting work we will never start.
func TestCapacityRefusesRatherThanAccumulates(t *testing.T) {
	q, _ := newQueue(t, WithCapacity(3))
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		submit(t, q, "t", job.PriorityNormal, base.Add(time.Duration(i)*time.Second))
	}

	overflow := job.New("t", job.PriorityNormal, job.Spec{Image: "img"}, base.Add(time.Hour))
	if err := q.Enqueue(ctx, overflow); !errors.Is(err, queue.ErrFull) {
		t.Fatalf("Enqueue past capacity = %v, want ErrFull", err)
	}

	// Refused means refused: no partial record left behind.
	if _, err := q.Get(ctx, overflow.ID); !errors.Is(err, queue.ErrNotFound) {
		t.Errorf("Get on refused job = %v, want ErrNotFound", err)
	}

	// Draining one makes room again.
	if _, err := q.Claim(ctx, "worker-1", time.Minute); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if err := q.Enqueue(ctx, overflow); err != nil {
		t.Errorf("Enqueue after draining = %v, want success", err)
	}
}

func TestStatsReportsTenantBreakdownAndAge(t *testing.T) {
	q, _ := newQueue(t)
	ctx := context.Background()

	submit(t, q, "tenant-a", job.PriorityNormal, base)
	submit(t, q, "tenant-a", job.PriorityNormal, base.Add(time.Minute))
	submit(t, q, "tenant-b", job.PriorityNormal, base.Add(2*time.Minute))

	stats, err := q.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.ByTenant["tenant-a"] != 2 || stats.ByTenant["tenant-b"] != 1 {
		t.Errorf("ByTenant = %v, want a:2 b:1", stats.ByTenant)
	}
	// Queue age is the number that reveals a system falling behind; depth alone
	// can look healthy while the oldest job starves.
	if !stats.OldestReady.Equal(base) {
		t.Errorf("OldestReady = %v, want %v", stats.OldestReady, base)
	}
}

func TestListFilters(t *testing.T) {
	q, _ := newQueue(t)
	ctx := context.Background()

	submit(t, q, "tenant-a", job.PriorityNormal, base)
	submit(t, q, "tenant-a", job.PriorityNormal, base.Add(time.Second))
	submit(t, q, "tenant-b", job.PriorityNormal, base.Add(2*time.Second))

	byTenant, err := q.List(ctx, queue.Filter{TenantID: "tenant-a"})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(byTenant) != 2 {
		t.Errorf("tenant-a jobs = %d, want 2", len(byTenant))
	}

	// Newest first.
	if len(byTenant) == 2 && byTenant[0].ID < byTenant[1].ID {
		t.Error("List is not newest-first")
	}

	limited, err := q.List(ctx, queue.Filter{Limit: 2})
	if err != nil {
		t.Fatalf("List with limit: %v", err)
	}
	if len(limited) != 2 {
		t.Errorf("limited list = %d, want 2", len(limited))
	}

	claimed, err := q.List(ctx, queue.Filter{State: job.StateQueued})
	if err != nil {
		t.Fatalf("List by state: %v", err)
	}
	if len(claimed) != 3 {
		t.Errorf("queued jobs = %d, want 3", len(claimed))
	}
}

// TestConcurrentClaimsNeverDuplicate is the property that matters under load:
// many workers racing on one queue must each get a distinct job, and every job
// must go to exactly one worker.
func TestConcurrentClaimsNeverDuplicate(t *testing.T) {
	q, _ := newQueue(t)
	ctx := context.Background()

	const jobs = 50
	const workers = 12
	for i := 0; i < jobs; i++ {
		submit(t, q, "t", job.PriorityNormal, base.Add(time.Duration(i)*time.Millisecond))
	}

	var mu sync.Mutex
	seen := map[string]string{}
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID string) {
			defer wg.Done()
			for {
				claimed, err := q.Claim(ctx, workerID, time.Minute)
				if errors.Is(err, queue.ErrEmpty) {
					return
				}
				if err != nil {
					t.Errorf("Claim: %v", err)
					return
				}
				mu.Lock()
				if prev, dup := seen[claimed.ID]; dup {
					t.Errorf("job %s claimed twice: by %s and %s", claimed.ID, prev, workerID)
				}
				seen[claimed.ID] = workerID
				mu.Unlock()
			}
		}(fmt.Sprintf("worker-%d", w))
	}
	wg.Wait()

	if len(seen) != jobs {
		t.Errorf("claimed %d distinct jobs, want %d", len(seen), jobs)
	}
}

// TestDurabilityAcrossReopen: the queue is the system's memory, so closing and
// reopening the process must not lose work.
func TestDurabilityAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "queue.db")
	ctx := context.Background()
	c := newClock()

	first, err := Open(path, WithClock(c.Now))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	j := job.New("tenant-a", job.PriorityHigh, job.Spec{Image: "img"}, base)
	if err := first.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	second, err := Open(path, WithClock(c.Now))
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer second.Close()

	claimed, err := second.Claim(ctx, "worker-after-restart", time.Minute)
	if err != nil {
		t.Fatalf("Claim after reopen: %v", err)
	}
	if claimed.ID != j.ID {
		t.Errorf("claimed %s, want the job enqueued before restart %s", claimed.ID, j.ID)
	}
	if claimed.Priority != job.PriorityHigh {
		t.Errorf("Priority = %d, want %d — job details did not survive", claimed.Priority, job.PriorityHigh)
	}
}

func TestGetUnknownJob(t *testing.T) {
	q, _ := newQueue(t)
	if _, err := q.Get(context.Background(), "job_nope"); !errors.Is(err, queue.ErrNotFound) {
		t.Errorf("Get unknown = %v, want ErrNotFound", err)
	}
}
