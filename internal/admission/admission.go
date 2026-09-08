// Package admission decides whether a job may enter the system, and says so in
// a form the caller can act on.
//
// The design rule here is that an overloaded system must degrade predictably.
// There are exactly three honest answers to a submission — accepted, queued
// behind others, or refused with a reason and a time to come back — and the
// one answer that is never acceptable is accepting work we have no intention
// of starting. Silent drops are the failure mode this package exists to
// prevent: they turn a load problem into a correctness problem, because the
// caller believes their job is running.
//
// Two independent controls, because they bound different things:
//
//   - A token bucket bounds the *rate* of submissions. It protects the control
//     plane from a client in a retry loop.
//   - A concurrency quota bounds *simultaneous execution*. It protects the
//     compute budget, and it is what stops one tenant consuming every worker
//     while everyone else waits.
//
// A tenant can be well under its rate limit and still be at its concurrency
// quota — fifty submissions spread over a minute, each running for an hour.
// Enforcing only the rate would let that tenant take the whole cluster.
package admission

import (
	"fmt"
	"math"
	"sync"
	"time"
)

// Outcome is the decision reached for a submission.
type Outcome string

const (
	// OutcomeAccepted means the job may be enqueued.
	OutcomeAccepted Outcome = "accepted"
	// OutcomeRateLimited means the tenant is submitting too fast.
	OutcomeRateLimited Outcome = "rate_limited"
	// OutcomeQuotaExceeded means the tenant already has its maximum number of
	// jobs running. Distinct from rate limiting because the remedy differs:
	// wait for your own work to finish, rather than simply slow down.
	OutcomeQuotaExceeded Outcome = "quota_exceeded"
	// OutcomeQueueFull means the system as a whole is saturated. This one is
	// not the tenant's fault, and it is the signal to add capacity.
	OutcomeQueueFull Outcome = "queue_full"
)

// Decision is the answer to an admission request, carrying enough detail for
// the caller to do something sensible rather than guess.
type Decision struct {
	Outcome Outcome

	// RetryAfter is how long to wait before trying again. Always set on a
	// refusal: telling a client "no" without telling it "when" produces a
	// retry storm, which is precisely what the rate limit was defending against.
	RetryAfter time.Duration

	// Reason is a human-readable explanation, safe to return over the API.
	Reason string
}

// Allowed reports whether the job may proceed.
func (d Decision) Allowed() bool { return d.Outcome == OutcomeAccepted }

func (d Decision) Error() string {
	if d.Allowed() {
		return ""
	}
	return fmt.Sprintf("%s: %s (retry after %s)", d.Outcome, d.Reason, d.RetryAfter.Round(time.Millisecond))
}

// Limits configures admission for a tenant.
type Limits struct {
	// SubmitsPerSecond is the sustained submission rate.
	SubmitsPerSecond float64
	// Burst is how many submissions may arrive at once before the sustained
	// rate applies. Without burst, a client submitting a batch of legitimate
	// work gets throttled for behaving normally.
	Burst float64
	// MaxConcurrent caps simultaneously running jobs. Zero means unlimited.
	MaxConcurrent int
}

// DefaultLimits are applied to tenants with no explicit configuration.
//
// Deliberately generous. These exist to stop abuse - a client stuck in a retry
// loop, a runaway script - not to punish a caller for submitting a batch of
// legitimate work. Set them too tight and the common case becomes a wall of
// 429s while the workers sit idle, which trains clients to ignore backpressure
// precisely when it starts to matter.
//
// The real limiter on parallelism is the scheduler's worker pool. The
// concurrency quota below it exists so one tenant cannot take the whole pool.
func DefaultLimits() Limits {
	return Limits{SubmitsPerSecond: 50, Burst: 100, MaxConcurrent: 25}
}

// Controller applies admission decisions across tenants.
//
// Safe for concurrent use: the API handles submissions on many goroutines.
type Controller struct {
	now func() time.Time

	mu        sync.Mutex
	defaults  Limits
	perTenant map[string]Limits
	buckets   map[string]*bucket
	running   map[string]int

	// globalCapacity bounds total queued+running work. Zero means unlimited.
	globalCapacity int
}

// bucket is a token bucket, refilled lazily.
//
// Lazy refill rather than a background ticker: a system with ten thousand
// tenants would otherwise run ten thousand timers to maintain counters nobody
// is reading. Computing the refill on access costs one multiplication and
// scales to any number of idle tenants.
type bucket struct {
	tokens     float64
	lastRefill time.Time
}

// Option configures a Controller.
type Option func(*Controller)

// WithClock overrides the time source, so refill behaviour can be tested
// without sleeping through it.
func WithClock(now func() time.Time) Option {
	return func(c *Controller) { c.now = now }
}

// WithTenantLimits sets per-tenant overrides.
func WithTenantLimits(limits map[string]Limits) Option {
	return func(c *Controller) {
		for tenant, l := range limits {
			c.perTenant[tenant] = l
		}
	}
}

// WithGlobalCapacity bounds total in-flight work across all tenants.
func WithGlobalCapacity(n int) Option {
	return func(c *Controller) { c.globalCapacity = n }
}

// New creates a Controller applying defaults to unconfigured tenants.
func New(defaults Limits, opts ...Option) *Controller {
	c := &Controller{
		now:       time.Now,
		defaults:  defaults,
		perTenant: map[string]Limits{},
		buckets:   map[string]*bucket{},
		running:   map[string]int{},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// LimitsFor returns the effective limits for a tenant.
func (c *Controller) LimitsFor(tenantID string) Limits {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.limitsLocked(tenantID)
}

func (c *Controller) limitsLocked(tenantID string) Limits {
	if l, ok := c.perTenant[tenantID]; ok {
		return l
	}
	return c.defaults
}

// Admit decides whether a tenant may submit a job now.
//
// depth is the current total of queued and running work, used for the global
// capacity check. It is passed in rather than held here so that the queue
// remains the single source of truth on depth.
//
// Admit consumes a token on success. It must therefore be called exactly once
// per submission, at the point of decision.
func (c *Controller) Admit(tenantID string, depth int) Decision {
	c.mu.Lock()
	defer c.mu.Unlock()

	limits := c.limitsLocked(tenantID)
	now := c.now()

	// Global capacity first. When the whole system is saturated, telling a
	// tenant they are individually over quota would be misleading — the
	// operator needs to see saturation, not blame the caller.
	if c.globalCapacity > 0 && depth >= c.globalCapacity {
		return Decision{
			Outcome:    OutcomeQueueFull,
			RetryAfter: 5 * time.Second,
			Reason:     fmt.Sprintf("system at capacity (%d in flight)", depth),
		}
	}

	// Concurrency quota before the rate limit, so a tenant sitting at its
	// quota is told the useful thing rather than being throttled first and
	// discovering the quota later.
	if limits.MaxConcurrent > 0 && c.running[tenantID] >= limits.MaxConcurrent {
		return Decision{
			Outcome:    OutcomeQuotaExceeded,
			RetryAfter: 2 * time.Second,
			Reason: fmt.Sprintf("tenant already running %d of %d permitted concurrent jobs",
				c.running[tenantID], limits.MaxConcurrent),
		}
	}

	b := c.bucketLocked(tenantID, limits, now)
	c.refill(b, limits, now)

	if b.tokens < 1 {
		return Decision{
			Outcome:    OutcomeRateLimited,
			RetryAfter: waitForOneToken(b.tokens, limits.SubmitsPerSecond),
			Reason: fmt.Sprintf("tenant exceeded %g submissions per second",
				limits.SubmitsPerSecond),
		}
	}

	b.tokens--
	return Decision{Outcome: OutcomeAccepted}
}

func (c *Controller) bucketLocked(tenantID string, limits Limits, now time.Time) *bucket {
	b, ok := c.buckets[tenantID]
	if !ok {
		// A new tenant starts with a full burst allowance rather than empty:
		// making a first-time caller wait for a refill would be a strange
		// welcome, and it is the burst budget that exists for exactly this.
		b = &bucket{tokens: limits.Burst, lastRefill: now}
		c.buckets[tenantID] = b
	}
	return b
}

func (c *Controller) refill(b *bucket, limits Limits, now time.Time) {
	elapsed := now.Sub(b.lastRefill).Seconds()
	if elapsed <= 0 {
		return
	}
	b.lastRefill = now
	b.tokens = math.Min(limits.Burst, b.tokens+elapsed*limits.SubmitsPerSecond)
}

// waitForOneToken reports how long until the bucket holds a whole token.
func waitForOneToken(tokens, perSecond float64) time.Duration {
	if perSecond <= 0 {
		return time.Minute
	}
	needed := 1 - tokens
	if needed <= 0 {
		return 0
	}
	wait := time.Duration(needed / perSecond * float64(time.Second))
	// Round up to something a client can act on. Sub-millisecond retry hints
	// invite exactly the hot loop we are trying to prevent.
	if wait < time.Millisecond {
		return time.Millisecond
	}
	return wait
}

// Started records that a job began running, consuming a concurrency slot.
func (c *Controller) Started(tenantID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.running[tenantID]++
}

// Finished releases a concurrency slot.
//
// It clamps at zero rather than going negative. An unbalanced Finished is a
// bug, but a negative counter would silently grant a tenant unlimited
// concurrency from then on, turning a small bug into an outage.
func (c *Controller) Finished(tenantID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.running[tenantID] > 0 {
		c.running[tenantID]--
	}
	if c.running[tenantID] == 0 {
		delete(c.running, tenantID)
	}
}

// Running reports a tenant's current concurrent job count.
func (c *Controller) Running(tenantID string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.running[tenantID]
}

// TotalRunning reports concurrent jobs across all tenants.
func (c *Controller) TotalRunning() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	total := 0
	for _, n := range c.running {
		total += n
	}
	return total
}

// HasCapacity reports whether a tenant could start another job right now,
// without consuming a rate-limit token.
//
// The scheduler needs this: it decides whether to dispatch an already-queued
// job, and that decision must not be charged against the submission rate. A
// job is admitted once, when it is submitted, not again every time a worker
// considers picking it up.
func (c *Controller) HasCapacity(tenantID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	limits := c.limitsLocked(tenantID)
	if limits.MaxConcurrent <= 0 {
		return true
	}
	return c.running[tenantID] < limits.MaxConcurrent
}

// SetTenantLimits updates a tenant's limits at runtime.
func (c *Controller) SetTenantLimits(tenantID string, limits Limits) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.perTenant[tenantID] = limits
}

// Snapshot reports current usage per tenant, for metrics and the API.
func (c *Controller) Snapshot() map[string]TenantUsage {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := make(map[string]TenantUsage, len(c.running))
	for tenant, n := range c.running {
		limits := c.limitsLocked(tenant)
		usage := TenantUsage{Running: n, MaxConcurrent: limits.MaxConcurrent}
		if b, ok := c.buckets[tenant]; ok {
			usage.TokensAvailable = b.tokens
		}
		out[tenant] = usage
	}
	return out
}

// TenantUsage is a point-in-time view of one tenant's consumption.
type TenantUsage struct {
	Running         int     `json:"running"`
	MaxConcurrent   int     `json:"max_concurrent"`
	TokensAvailable float64 `json:"tokens_available"`
}
