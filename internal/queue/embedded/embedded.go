// Package embedded implements queue.Queue on top of bbolt, a pure-Go embedded
// key/value store.
//
// Why embedded: the reviewer of this system should be able to run it with one
// command and no other services. Redis or SQS would each add a dependency to
// install and operate, in exchange for capabilities we do not need at this
// scale. bbolt is a library, not a service — it cross-compiles anywhere, needs
// no cgo, and gives us ACID transactions, which is the one property a job queue
// genuinely cannot do without.
//
// What it costs: a bbolt file is opened by exactly one process. That makes the
// control plane single-instance. The ceiling here is not a job rate, it is a
// host count — see the scale ladder in the README for what replaces this.
package embedded

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/bytes-as/ephemera/internal/job"
	"github.com/bytes-as/ephemera/internal/queue"
)

var (
	// bucketJobs holds the authoritative record: jobID -> job JSON.
	bucketJobs = []byte("jobs")
	// bucketReady is the dispatch index: orderKey -> jobID. Iterating this
	// bucket in byte order yields exactly the order jobs should run in.
	bucketReady = []byte("ready")
	// bucketLeases holds jobID -> lease JSON for claimed jobs.
	bucketLeases = []byte("leases")
	// bucketIndex holds jobID -> orderKey so a job's ready entry can be found
	// and removed without scanning.
	bucketIndex = []byte("index")
)

// lease records who holds a job and until when.
type lease struct {
	WorkerID  string    `json:"worker_id"`
	ClaimedAt time.Time `json:"claimed_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

// Queue is a durable, lease-based priority queue.
type Queue struct {
	db  *bolt.DB
	now func() time.Time

	// capacity bounds the ready set. Zero means unbounded.
	capacity int
}

// Option configures a Queue.
type Option func(*Queue)

// WithClock overrides the time source, so lease expiry can be tested without
// sleeping through it.
func WithClock(now func() time.Time) Option {
	return func(q *Queue) { q.now = now }
}

// WithCapacity bounds the number of ready jobs. Enqueue returns queue.ErrFull
// beyond it, which is what turns an overloaded system into one that says no
// instead of one that silently accumulates work it will never start.
func WithCapacity(n int) Option {
	return func(q *Queue) { q.capacity = n }
}

// Open creates or opens a queue at path.
func Open(path string, opts ...Option) (*Queue, error) {
	db, err := bolt.Open(path, 0o640, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		// A timeout here almost always means another control plane already has
		// the file open. Say so, because "resource temporarily unavailable" is
		// a miserable thing to debug at 3am.
		return nil, fmt.Errorf("queue: open %s (is another instance running?): %w", path, err)
	}

	q := &Queue{db: db, now: time.Now}
	for _, opt := range opts {
		opt(q)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{bucketJobs, bucketReady, bucketLeases, bucketIndex} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return fmt.Errorf("create bucket %s: %w", name, err)
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("queue: initialise: %w", err)
	}
	return q, nil
}

func (q *Queue) Close() error { return q.db.Close() }

// orderKey builds the dispatch-order key for a job.
//
// Layout: [1 byte inverted priority][8 bytes submission millis][job ID].
//
// bbolt iterates keys in byte order, so inverting the priority makes high
// priority sort first, and the timestamp after it breaks ties oldest-first.
// That second part is what stops a steady stream of high-priority work from
// starving an equally-prioritised job that has been waiting all afternoon:
// within a priority level, the queue is strictly FIFO.
func orderKey(j *job.Job) []byte {
	priority := j.Priority
	if priority < 0 {
		priority = 0
	}
	if priority > 255 {
		priority = 255
	}

	key := make([]byte, 0, 9+len(j.ID))
	key = append(key, byte(255-priority))
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], uint64(j.SubmittedAt.UTC().UnixMilli()))
	key = append(key, ts[:]...)
	key = append(key, j.ID...)
	return key
}

func encodeJob(j *job.Job) ([]byte, error) {
	encoded, err := json.Marshal(j)
	if err != nil {
		return nil, fmt.Errorf("queue: encode job %s: %w", j.ID, err)
	}
	return encoded, nil
}

func decodeJob(raw []byte) (*job.Job, error) {
	var j job.Job
	if err := json.Unmarshal(raw, &j); err != nil {
		return nil, fmt.Errorf("queue: decode job: %w", err)
	}
	return &j, nil
}

// Enqueue durably records a job and makes it claimable.
//
// Idempotent by job ID: a client retrying a submission that already succeeded
// gets the same job back rather than a second copy of the work.
func (q *Queue) Enqueue(ctx context.Context, j *job.Job) error {
	if j == nil || j.ID == "" {
		return errors.New("queue: cannot enqueue a job without an ID")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// Enqueue reports back through j: the caller (and therefore the API
	// response) must see the state that was actually stored, not the pending
	// state it arrived in. Assigned only after the transaction commits, so a
	// failed enqueue leaves the caller's job untouched.
	var stored *job.Job
	err := q.db.Update(func(tx *bolt.Tx) error {
		jobs := tx.Bucket(bucketJobs)
		if existing := jobs.Get([]byte(j.ID)); existing != nil {
			// Already present. Not an error and not a duplicate - hand back
			// what is on record so a retrying client sees the true state.
			found, err := decodeJob(existing)
			if err != nil {
				return err
			}
			stored = found
			return nil
		}

		ready := tx.Bucket(bucketReady)
		if q.capacity > 0 && ready.Stats().KeyN >= q.capacity {
			return queue.ErrFull
		}

		queued := *j
		if queued.State == job.StatePending {
			if err := queued.Transition(job.StateQueued, q.now()); err != nil {
				return err
			}
		}

		encoded, err := encodeJob(&queued)
		if err != nil {
			return err
		}
		if err := jobs.Put([]byte(queued.ID), encoded); err != nil {
			return err
		}

		key := orderKey(&queued)
		if err := ready.Put(key, []byte(queued.ID)); err != nil {
			return err
		}
		if err := tx.Bucket(bucketIndex).Put([]byte(queued.ID), key); err != nil {
			return err
		}
		stored = &queued
		return nil
	})
	if err != nil {
		return err
	}
	if stored != nil {
		*j = *stored
	}
	return nil
}

// Claim leases the highest-priority ready job to a worker.
func (q *Queue) Claim(ctx context.Context, workerID string, leaseFor time.Duration) (*job.Job, error) {
	if workerID == "" {
		return nil, errors.New("queue: claim requires a worker ID")
	}
	if leaseFor <= 0 {
		return nil, errors.New("queue: claim requires a positive lease duration")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var claimed *job.Job
	err := q.db.Update(func(tx *bolt.Tx) error {
		ready := tx.Bucket(bucketReady)
		cursor := ready.Cursor()
		key, jobID := cursor.First()
		if key == nil {
			return queue.ErrEmpty
		}

		jobs := tx.Bucket(bucketJobs)
		raw := jobs.Get(jobID)
		if raw == nil {
			// Index entry with no job behind it. Drop it and let the caller
			// retry rather than wedge the head of the queue forever.
			_ = ready.Delete(key)
			return queue.ErrEmpty
		}

		j, err := decodeJob(raw)
		if err != nil {
			return err
		}

		now := q.now()
		if err := j.Transition(job.StateProvisioning, now); err != nil {
			return err
		}
		j.Attempts++

		encoded, err := encodeJob(j)
		if err != nil {
			return err
		}
		if err := jobs.Put(jobID, encoded); err != nil {
			return err
		}
		if err := ready.Delete(key); err != nil {
			return err
		}
		if err := tx.Bucket(bucketIndex).Delete(jobID); err != nil {
			return err
		}

		encodedLease, err := json.Marshal(lease{
			WorkerID:  workerID,
			ClaimedAt: now,
			ExpiresAt: now.Add(leaseFor),
		})
		if err != nil {
			return err
		}
		if err := tx.Bucket(bucketLeases).Put(jobID, encodedLease); err != nil {
			return err
		}

		claimed = j
		return nil
	})
	if err != nil {
		return nil, err
	}
	return claimed, nil
}

// Heartbeat extends a lease the worker still holds.
func (q *Queue) Heartbeat(ctx context.Context, jobID, workerID string, extendBy time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return q.db.Update(func(tx *bolt.Tx) error {
		held, err := q.leaseHeldBy(tx, jobID, workerID)
		if err != nil {
			return err
		}
		held.ExpiresAt = q.now().Add(extendBy)
		encoded, err := json.Marshal(held)
		if err != nil {
			return err
		}
		return tx.Bucket(bucketLeases).Put([]byte(jobID), encoded)
	})
}

// leaseHeldBy fetches a lease and verifies the worker holds it.
func (q *Queue) leaseHeldBy(tx *bolt.Tx, jobID, workerID string) (lease, error) {
	raw := tx.Bucket(bucketLeases).Get([]byte(jobID))
	if raw == nil {
		return lease{}, queue.ErrNotLeaseHolder
	}
	var held lease
	if err := json.Unmarshal(raw, &held); err != nil {
		return lease{}, fmt.Errorf("queue: decode lease for %s: %w", jobID, err)
	}
	if held.WorkerID != workerID {
		return lease{}, queue.ErrNotLeaseHolder
	}
	return held, nil
}

// Complete records a terminal outcome and drops the lease.
//
// The job record is kept. A queue that deletes finished work cannot answer the
// question an operator actually asks, which is what happened to job X.
func (q *Queue) Complete(ctx context.Context, j *job.Job) error {
	if j == nil || j.ID == "" {
		return errors.New("queue: cannot complete a job without an ID")
	}
	if !j.State.Terminal() {
		return fmt.Errorf("queue: cannot complete job %s in non-terminal state %s", j.ID, j.State)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	return q.db.Update(func(tx *bolt.Tx) error {
		jobs := tx.Bucket(bucketJobs)
		if jobs.Get([]byte(j.ID)) == nil {
			return queue.ErrNotFound
		}
		encoded, err := encodeJob(j)
		if err != nil {
			return err
		}
		if err := jobs.Put([]byte(j.ID), encoded); err != nil {
			return err
		}
		if err := tx.Bucket(bucketLeases).Delete([]byte(j.ID)); err != nil {
			return err
		}
		// Defensive: a terminal job must never remain in the dispatch index.
		if key := tx.Bucket(bucketIndex).Get([]byte(j.ID)); key != nil {
			if err := tx.Bucket(bucketReady).Delete(key); err != nil {
				return err
			}
			if err := tx.Bucket(bucketIndex).Delete([]byte(j.ID)); err != nil {
				return err
			}
		}
		return nil
	})
}

// Release gives up a lease, optionally returning the job to the ready set.
func (q *Queue) Release(ctx context.Context, jobID, workerID string, requeue bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return q.db.Update(func(tx *bolt.Tx) error {
		if _, err := q.leaseHeldBy(tx, jobID, workerID); err != nil {
			return err
		}
		if err := tx.Bucket(bucketLeases).Delete([]byte(jobID)); err != nil {
			return err
		}
		if !requeue {
			return nil
		}
		return q.makeReady(tx, jobID)
	})
}

// makeReady returns a claimed job to the dispatch index.
func (q *Queue) makeReady(tx *bolt.Tx, jobID string) error {
	raw := tx.Bucket(bucketJobs).Get([]byte(jobID))
	if raw == nil {
		return queue.ErrNotFound
	}
	j, err := decodeJob(raw)
	if err != nil {
		return err
	}
	if j.State.Terminal() {
		return fmt.Errorf("queue: refusing to requeue terminal job %s (%s)", jobID, j.State)
	}
	if j.State != job.StateQueued {
		if err := j.Transition(job.StateQueued, q.now()); err != nil {
			return err
		}
	}

	encoded, err := encodeJob(j)
	if err != nil {
		return err
	}
	if err := tx.Bucket(bucketJobs).Put([]byte(jobID), encoded); err != nil {
		return err
	}

	// Requeue on the original submission time, not now. A job that has already
	// waited and then failed to provision should not go to the back of the
	// queue behind everything submitted since — it has waited longest, so it
	// goes first.
	key := orderKey(j)
	if err := tx.Bucket(bucketReady).Put(key, []byte(jobID)); err != nil {
		return err
	}
	return tx.Bucket(bucketIndex).Put([]byte(jobID), key)
}

// RecoverExpired returns lapsed leases to the ready set.
//
// This is what makes a worker crash survivable: the job was never removed from
// storage, only leased, so recovery is a matter of noticing the lease is stale.
func (q *Queue) RecoverExpired(ctx context.Context, now time.Time) ([]*job.Job, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var recovered []*job.Job
	err := q.db.Update(func(tx *bolt.Tx) error {
		leases := tx.Bucket(bucketLeases)

		var expired []string
		if err := leases.ForEach(func(jobID, raw []byte) error {
			var held lease
			if err := json.Unmarshal(raw, &held); err != nil {
				// An undecodable lease blocks its job forever. Treat it as
				// expired so the job can be recovered rather than stranded.
				expired = append(expired, string(jobID))
				return nil
			}
			if now.After(held.ExpiresAt) {
				expired = append(expired, string(jobID))
			}
			return nil
		}); err != nil {
			return err
		}

		for _, jobID := range expired {
			if err := leases.Delete([]byte(jobID)); err != nil {
				return err
			}
			if err := q.makeReady(tx, jobID); err != nil {
				if errors.Is(err, queue.ErrNotFound) {
					continue
				}
				return err
			}
			raw := tx.Bucket(bucketJobs).Get([]byte(jobID))
			j, err := decodeJob(raw)
			if err != nil {
				return err
			}
			recovered = append(recovered, j)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return recovered, nil
}

// Update persists an in-flight job record.
func (q *Queue) Update(ctx context.Context, j *job.Job) error {
	if j == nil || j.ID == "" {
		return errors.New("queue: cannot update a job without an ID")
	}
	if j.State.Terminal() {
		return fmt.Errorf("queue: use Complete for terminal job %s (%s)", j.ID, j.State)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return q.db.Update(func(tx *bolt.Tx) error {
		jobs := tx.Bucket(bucketJobs)
		if jobs.Get([]byte(j.ID)) == nil {
			return queue.ErrNotFound
		}
		encoded, err := encodeJob(j)
		if err != nil {
			return err
		}
		return jobs.Put([]byte(j.ID), encoded)
	})
}

// Get returns a job by ID.
func (q *Queue) Get(ctx context.Context, jobID string) (*job.Job, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var j *job.Job
	err := q.db.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket(bucketJobs).Get([]byte(jobID))
		if raw == nil {
			return queue.ErrNotFound
		}
		var err error
		j, err = decodeJob(raw)
		return err
	})
	if err != nil {
		return nil, err
	}
	return j, nil
}

// List returns jobs matching filter, newest first.
func (q *Queue) List(ctx context.Context, filter queue.Filter) ([]*job.Job, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var out []*job.Job
	err := q.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketJobs).ForEach(func(_, raw []byte) error {
			j, err := decodeJob(raw)
			if err != nil {
				return err
			}
			if filter.TenantID != "" && j.TenantID != filter.TenantID {
				return nil
			}
			if filter.State != "" && j.State != filter.State {
				return nil
			}
			out = append(out, j)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	// Job IDs are time-ordered, so sorting by ID descending is newest-first
	// without needing to parse timestamps.
	sort.Slice(out, func(i, j int) bool { return out[i].ID > out[j].ID })
	if filter.Limit > 0 && len(out) > filter.Limit {
		out = out[:filter.Limit]
	}
	return out, nil
}

// Stats reports queue depth, including the age of the oldest waiting job.
func (q *Queue) Stats(ctx context.Context) (queue.Stats, error) {
	if err := ctx.Err(); err != nil {
		return queue.Stats{}, err
	}

	stats := queue.Stats{ByTenant: map[string]int{}}
	err := q.db.View(func(tx *bolt.Tx) error {
		stats.Claimed = tx.Bucket(bucketLeases).Stats().KeyN

		ready := map[string]bool{}
		if err := tx.Bucket(bucketReady).ForEach(func(_, jobID []byte) error {
			ready[string(jobID)] = true
			return nil
		}); err != nil {
			return err
		}
		stats.Ready = len(ready)

		return tx.Bucket(bucketJobs).ForEach(func(id, raw []byte) error {
			j, err := decodeJob(raw)
			if err != nil {
				return err
			}
			if j.State.Terminal() {
				stats.Terminal++
				return nil
			}
			if ready[string(id)] {
				stats.ByTenant[j.TenantID]++
				if stats.OldestReady.IsZero() || j.SubmittedAt.Before(stats.OldestReady) {
					stats.OldestReady = j.SubmittedAt
				}
			}
			return nil
		})
	})
	if err != nil {
		return queue.Stats{}, err
	}
	return stats, nil
}

// PendingForTenant reports how many ready jobs a tenant has waiting. The
// scheduler uses it for fairness decisions without pulling full stats.
func (q *Queue) PendingForTenant(ctx context.Context, tenantID string) (int, error) {
	stats, err := q.Stats(ctx)
	if err != nil {
		return 0, err
	}
	return stats.ByTenant[tenantID], nil
}

// compile-time checks
var (
	_ queue.Queue = (*Queue)(nil)
	_             = bytes.Compare
)
