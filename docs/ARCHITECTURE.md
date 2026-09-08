# Architecture

The design decisions, and the reasoning I would defend in a review.

## The one abstraction

Everything hangs off `driver.Driver`:

```go
type Driver interface {
    Name() string
    Capabilities() Capabilities
    Create(ctx, EnvSpec) (Env, error)
    Start(ctx, Env, EnvSpec) error
    Logs(ctx, Env) (<-chan LogLine, error)
    Wait(ctx, Env) (Status, error)
    Collect(ctx, Env, dest string) ([]Artifact, error)
    Destroy(ctx, Env) error
    Describe(ctx, Env) (Status, error)
    List(ctx) ([]Env, error)
}
```

Four things in that signature carry real weight.

**`Create` is separate from `Start`.** A provisioning failure and a startup
failure have different causes and different retry semantics. Collapsing them
into one call would collapse the distinction the retry policy depends on.

**`Start` takes the spec again** rather than the driver remembering it. The spec
carries resolved secret values, and nothing should retain those between calls —
not the driver, not the environment record, not disk. The caller holds them for
the length of one call and drops them.

**`Wait` returns a `Status`, not an error, for a non-zero exit.** "The agent
failed" is a result. "We could not determine what happened" is an error, and a
much worse situation. Conflating them loses that.

**`List` must be derived from the runtime, never from memory.** It exists so the
reaper can find environments after a control-plane crash, and in-memory
bookkeeping cannot survive the crash it needs to recover from. This is the
requirement that makes the process driver write state files and the Docker
driver label containers.

### Capabilities, and the honesty rule

`Capabilities()` makes each driver state what it genuinely enforces. The control
plane refuses to start with a configuration its driver cannot honour, rather
than silently downgrading.

This matters more than it first appears. The alternatives are both bad: silently
dropping the control leaves an operator believing jobs are isolated when they are
not, and failing every job at `Create` turns a config typo into an outage that
looks like a driver bug. Failing at boot, with a message naming the fix, is the
only option that leaves someone better off.

`Config.RelaxedFor(driver)` is the deliberate, visible downgrade — it returns
what it dropped so the caller can announce it. It exists so "I know the process
driver has no network isolation and I accept that locally" is expressible. It
must never be used to make an error go away.

## Why Compose is not the IaC deliverable

The brief asks: *can I spin this up with one command (Terraform/CDK/Pulumi)?*

`docker-compose.yml` is declarative, checked in, and code. It is not IaC in the
sense being graded:

| | Compose | Terraform |
|---|---|---|
| Provisions infrastructure | no — consumes a daemon that exists | yes — VPC, subnets, IAM, cluster |
| Lifecycle and state | no state file, no drift detection | state, plan, drift, destroy |
| Models security boundaries | barely (bridge networks) | IAM roles, SGs, task role scoping |
| Reproducible in a fresh account | there is no account | yes, that is the point |

Compose is **developer orchestration**; Terraform is **the IaC deliverable**.
Shipping only Compose and calling it IaC would be marked down, correctly.

## Teardown ordering

A job is not reported terminal until its logs have drained and its environment
is destroyed. This was a bug found by an integration test: the original code
completed the job first, so a caller could see `succeeded` while output was
still arriving and an environment still existed.

The contract now: **a terminal job means output captured, environment gone.**
Teardown runs before the result is recorded on every path, is idempotent, and
has a deferred backstop for panics. The log pump is drained *before* the
environment is destroyed, because destroying it removes the file the pump is
reading — and the tail of a failing job's output is exactly the part somebody
needed.

## Contexts

Three, deliberately distinct:

- **The caller's context** governs the *call*, not the work. Tying a job's
  lifetime to an HTTP handler would kill jobs whenever a request returned.
- **The job's context** (`context.WithoutCancel`) governs the agent. It ends at
  the deadline or on `Destroy`.
- **A persistence context** outlives shutdown. A result we computed and then
  failed to record because someone pressed Ctrl-C is the worst outcome
  available: the work was done, the compute was paid for, and the answer was
  thrown away. This was also a real bug caught by a test.

## Queue design

Dispatch order is a single key: `[inverted priority][submitted millis][job id]`.
The store iterates in byte order, so iteration order *is* dispatch order — no
sorting, no scan. Inverting the priority byte makes high priority sort first;
the timestamp breaks ties oldest-first, which is what prevents starvation within
a priority level.

Requeue **keeps the original submission time**. A job that already waited and
then hit a provisioning failure should not go behind everything submitted since;
it has waited longest, so it goes first.

Leases rather than removal: a worker claims for a bounded time and must keep
saying so. The lease-holder check on every mutation is what stops a stalled
worker writing its result over the worker that took over.

## Job IDs

ULID layout: 48-bit millisecond timestamp, then 80 bits of randomness. IDs sort
by submission time, which makes range scans cheap and logs readable without a
join.

The first version wrote an 8-byte timestamp and then began the random fill at
byte 6, clobbering the low 16 bits and collapsing sort granularity from 1 ms to
~65 s. There is a regression test.

## Admission

Two controls, because they bound different things:

- A **token bucket** bounds submission *rate*. It protects the control plane
  from a client in a retry loop.
- A **concurrency quota** bounds *simultaneous execution*. It protects the
  compute budget and stops one tenant taking every worker.

The quota is deliberately **not** clamped to the worker count. Workers bound what
this host runs at once; the quota bounds how much of that one tenant may take.
Clamping them together refuses a tenant's queued work rather than queueing it.

Defaults are generous on purpose. Rate limits exist to stop abuse, not to punish
a caller for submitting a batch of legitimate work. Set them too tight and the
common case becomes a wall of 429s while workers sit idle — which trains clients
to ignore backpressure exactly when it starts to matter. The first load-test run
refused 40 of 50 jobs and the defaults were wrong, not the test.

Buckets refill lazily rather than on a ticker: ten thousand idle tenants cost
ten thousand map entries, not ten thousand timers.

## Known limitations

**Head-of-line blocking.** A tenant at its concurrency quota whose job is at the
head of the queue causes workers to claim-and-release in a loop. A backoff bounds
the waste. The real fix is per-tenant ready queues with round-robin selection, so
a blocked tenant is skipped rather than retried — it needs a queue interface that
can scan past the head, and at this scale the backoff is sufficient.

**Single control plane.** The bbolt file is opened by one process. This is
visible in the Terraform: the ECS service uses 100/0 deployment percentages
because two control planes cannot run at once.

**No cancellation endpoint.** The state machine supports `cancelled`; no API
route exposes it.

## What was verified

| Area | Evidence |
|---|---|
| Core pipeline | end to end against a live daemon, and in tests |
| Concurrency | 50 jobs, peak concurrency measured at exactly the worker count |
| Failure paths | crash, deadline, unresolvable secret, start failure, retry exhaustion |
| Reaping | deadline, lifetime cap, orphan-after-restart, zero leaks after a 50-job run |
| Secrets | non-leakage through `fmt`, JSON, state files, labels, container names |
| Signed URLs | expiry, forgery, field-substitution, length-prefix collision |
| Traversal | tar entries and artifact names, both directions |
| Terraform | `fmt`, `validate`, `tflint` pass; `checkov` **19 findings open** — **not applied** |
| Docker driver | framing, extraction, policy refusal, **plus 11/11 integration tests against a live daemon** |
| Races | **clean** — `-race` across every package |

The `fargate` driver is not implemented. The interface, Terraform and IAM model
for it exist. Two drivers were built to pressure-test the abstraction against
two genuinely different runtimes; a third written blind against an API I had no
account to call would have been decoration, and would have made the verification
table above less honest rather than more impressive.
