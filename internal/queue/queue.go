// Package queue defines durable job storage and dispatch ordering.
//
// The queue is the system's memory. Everything else — the scheduler, the API,
// the reaper — is stateless and can be restarted; the queue is what makes a job
// survive that restart. So the interface is written around a single question:
// what happens when the process holding a job dies halfway through it?
//
// The answer is leases. A worker does not remove a job from the queue, it
// claims it for a bounded time and must keep saying so. If the worker dies, the
// lease expires and the job becomes claimable again. Nothing is lost because a
// process went away, and nothing runs twice because two processes disagreed.
package queue

import (
	"context"
	"errors"
	"time"

	"ephemera/internal/job"
)

// Queue stores jobs durably and hands them out in dispatch order.
//
// Implementations must be safe for concurrent use by many workers.
type Queue interface {
	// Enqueue durably records a job and makes it claimable. It is idempotent
	// by job ID: re-enqueuing a job already present is not an error and does
	// not duplicate it, so an API retry cannot run the same work twice.
	Enqueue(ctx context.Context, j *job.Job) error

	// Claim leases the highest-priority ready job to a worker for leaseFor.
	// It returns ErrEmpty when nothing is ready — an ordinary condition, not a
	// failure, and the scheduler's idle path.
	Claim(ctx context.Context, workerID string, leaseFor time.Duration) (*job.Job, error)

	// Heartbeat extends a held lease. A worker running a long job must call
	// this, or the job will be considered abandoned and handed to someone else
	// while it is still running.
	Heartbeat(ctx context.Context, jobID, workerID string, extendBy time.Duration) error

	// Complete records a terminal outcome and releases the lease. The job stays
	// readable — an operator investigating a failure needs the record, and a
	// queue that forgets finished jobs cannot answer "what happened".
	Complete(ctx context.Context, j *job.Job) error

	// Release gives up a lease. With requeue true the job returns to the ready
	// set for another attempt; with requeue false it stays claimed by nobody
	// and will be recovered when its lease expires.
	Release(ctx context.Context, jobID, workerID string, requeue bool) error

	// Get returns a job by ID, whatever its state.
	Get(ctx context.Context, jobID string) (*job.Job, error)

	// List returns jobs matching a filter, newest first.
	List(ctx context.Context, filter Filter) ([]*job.Job, error)

	// RecoverExpired returns jobs whose leases have lapsed to the ready set and
	// reports which ones moved. This is how work survives a worker crash, and
	// it is deliberately a separate call rather than a background goroutine
	// inside the queue: recovery is a policy decision with observable
	// consequences, so it belongs where it can be scheduled, logged and tested.
	RecoverExpired(ctx context.Context, now time.Time) ([]*job.Job, error)

	// Stats reports queue depth by state, for backpressure decisions and metrics.
	Stats(ctx context.Context) (Stats, error)

	// Close releases the underlying storage.
	Close() error
}

// Filter narrows a List query. Zero values mean "no constraint".
type Filter struct {
	TenantID string
	State    job.State
	Limit    int
}

// Stats is a point-in-time view of queue depth.
type Stats struct {
	// Ready is the number of jobs waiting to be claimed.
	Ready int `json:"ready"`
	// Claimed is the number currently leased to a worker.
	Claimed int `json:"claimed"`
	// Terminal is the number in a finished state, still retained for inspection.
	Terminal int `json:"terminal"`
	// ByTenant is the ready count per tenant, which is what fairness and
	// per-tenant rate limiting decisions are made from.
	ByTenant map[string]int `json:"by_tenant,omitempty"`
	// OldestReady is the submission time of the longest-waiting ready job.
	// Queue *age* is the number that tells you a system is falling behind;
	// depth alone can look healthy while the oldest job starves.
	OldestReady time.Time `json:"oldest_ready,omitempty"`
}

var (
	// ErrEmpty reports that no job is ready to claim. Expected, not a fault.
	ErrEmpty = errors.New("queue: no job ready")

	// ErrNotFound reports that no job exists with the given ID.
	ErrNotFound = errors.New("queue: job not found")

	// ErrNotLeaseHolder reports that a worker acted on a job it does not hold.
	// This is the guard against two workers running the same job: a worker
	// whose lease expired while it was busy will be refused here rather than
	// allowed to write a result over someone else's.
	ErrNotLeaseHolder = errors.New("queue: worker does not hold this job's lease")

	// ErrFull reports that the queue is at capacity and the job was refused.
	// Refusing at admission is the whole point of backpressure: accepting work
	// we cannot start, and dropping it silently later, is the failure mode this
	// prevents.
	ErrFull = errors.New("queue: at capacity")
)
