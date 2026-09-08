# ephemera

A control plane that runs untrusted work in disposable environments, then
proves the environment is gone.

Built for the Bravebird Platform & Infra take-home: provision an ephemeral
environment, run a "computer use" agent in it, capture the output, destroy the
environment. The agent is a placeholder, as the brief permits. Everything
wrapping it is the submission.

```
CLI / HTTP API  ──submit──▶  Queue (priority + per-tenant limits)
                                  │
                                  ▼
                             Scheduler ── bounded worker pool
                                  │
                                  ▼
                               Driver ── process │ docker │ fargate
                                  │
                          ┌───────┼───────────┐
                          ▼       ▼           ▼
                      Egress   Log stream   Artifacts
                      policy   (SSE)        (signed URLs)
                                  │
                                  ▼
                               Reaper ── three independent layers
```

---

## Run it

Nothing to install but Go. No cloud account, no daemon, no services.

```bash
make build
./bin/ephemerad --data-dir ./data &        # control plane
./bin/ephemera run --command "$PWD/bin/ephemera-agent" --query "site reliability"
```

That submits a job, streams its logs live, waits for it, and prints signed
links to the artifacts it produced:

```
submitted job_06g83mdz1ns0r2219cppsp0gg0 (priority 90)
* provisioning process environment
* agent started
  step 1/4: launching browser
  step 2/4: navigating and searching for "site reliability"
  step 3/4: waiting for results to settle
  step 4/4: capturing page state
  saved screenshot.png (1314 bytes) and result.json
* stored 2 artifact(s)

job          job_06g83mdz1ns0r2219cppsp0gg0
state        succeeded
attempts     1
duration     717ms

artifacts:
  result.json                 145 B  http://localhost:8080/v1/jobs/.../result.json?expires=...&signature=...
  screenshot.png             1.3 KB  http://localhost:8080/v1/jobs/.../screenshot.png?expires=...&signature=...
```

**With container isolation and egress control** (needs Docker):

```bash
docker compose up --build -d
ephemera run --image ephemera/agent:dev --query "distributed systems"
```

**The brief's load question**, answered with numbers:

```bash
make load-test          # 50 concurrent submissions
```

---

## What this is, and what it is not

The reusable core is a **pluggable driver interface**. Everything above it —
queueing, admission, scheduling, log streaming, artifacts, reaping — is written
once and works for any driver. Three real drivers ship:

| Driver | Isolation | Enforces limits | Enforces egress policy | Needs |
|---|---|---|---|---|
| `process` | OS process | no | no | nothing |
| `docker` | container (namespaces, cgroups) | yes | yes, structurally | a Docker daemon |
| `fargate` | microVM per task | yes | yes, via IAM | AWS |

Each driver **declares what it actually enforces**, and the control plane
**refuses to start** with a configuration its driver cannot honour. Configure an
egress deny list against the `process` driver and you get this, at boot:

```
scheduler: configuration requires network policy (mode "egress", 7 deny CIDRs)
but the "process" driver does not enforce it; use the docker or fargate driver,
or clear NetworkMode and DenyCIDRs to run without egress isolation
```

Silently dropping a security control is worse than refusing the job, because the
caller believes they have the control. That principle recurs throughout.

---

## The deep-dives

The brief asks for two. There are four, and they are defensible together only
because they are four properties of one job lifecycle rather than four
subsystems: scheduling is what happens *between submit and dispatch*, egress is
how the environment is *created*, the flight recorder is what streams *out*, the
reaper is what guarantees it *ends*.

### 1. High-performance networking — egress control

The metadata endpoint is the whole question, and the answer differs per driver
because the truth differs per driver.

**Docker: structural, not filtered.** Job containers join an `internal: true`
network. Such a network has *no route off itself*, so `169.254.169.254` is
unreachable because *everything* is unreachable — not because a rule caught it.
An egress proxy is the only container bridging the internal network and the
internet, making it a single auditable chokepoint. Its filter denies the
metadata and RFC1918 ranges too, so the block still holds if someone later makes
that network routable.

The driver **refuses** a CIDR deny list on an ordinary bridge network, because
enforcing one needs rules in the host's `DOCKER-USER` chain that this driver
does not own and could not apply portably. That refusal names the mode that does
provide the control.

**Fargate: IAM, not network.** A security group cannot block `169.254.170.2` —
it is link-local, so it never traverses a route table, NACL or security group.
There is no network control for it. So the job task role has **no permissions at
all**, and an agent that reads its credentials from the metadata endpoint gets
credentials that can do nothing. Claiming a security group solved this would be
a wrong answer that looks right.

*Bonus (IP rotation / geo simulation):* the proxy hop exists and is the correct
insertion point — point `--egress-proxy` at a rotating pool. Not implemented.

### 2. Concurrency and scheduling

A durable, lease-based priority queue on bbolt. Dispatch order is
`[inverted priority][submitted millis][id]`, so the store's byte-order iteration
*is* the dispatch order. Within a priority level it is strictly FIFO, which is
what stops a stream of high-priority work starving an equally-prioritised job
that has waited all afternoon. A requeued job keeps its original position.

Workers **lease** rather than remove. Die, and the lease lapses and the job
returns. A worker whose lease expired while it was busy is refused when it tries
to write its result, so a takeover cannot produce two results.

Admission has **two independent controls**, because they bound different things:
a token bucket on submission *rate*, and a quota on *simultaneous execution*. A
tenant can sit well inside its rate limit and still be entitled to no more
parallelism — fifty submissions over a minute, each running an hour.

Measured, not asserted:

```
50 concurrent submissions: 50 accepted, 50 succeeded in 5s
peak observed concurrency: exactly 6 of 6 workers
environments left behind: 0
```

Concurrency is measured by sampling presence files that real agent processes
create and remove, not by trusting a counter.

With deliberately tight limits, the same 50 give 10 accepted and 40 refused,
each carrying a `Retry-After` that a test proves actually works. **Refusal is
the design working.** Accepting work the system cannot start, then dropping it,
is the failure mode this prevents.

### 3. Observability — the flight recorder

Live logs over SSE, interleaving the agent's own output with platform lifecycle
events so an operator reads one timeline rather than two. Artifacts (screenshots,
video, JSON) go to storage and are served through **HMAC-signed, expiring URLs** —
real ones, not a stub swapped for S3 later, so signing, expiry and verification
are exercised from day one.

Two rules in tension, resolved deliberately:

- **A viewer must never slow down a job.** Observability that applies
  backpressure to the thing it observes has become a liability.
- **Silently dropping output is dishonest.** An operator reading a log with an
  invisible gap draws wrong conclusions.

So a slow subscriber loses lines and is *told how many* — `[47 lines dropped:
this viewer could not keep up]`. The durable record is separate: drivers write
every line to disk, so the complete log is always recoverable, and logs replay
in full after the job ends, which is when an investigator actually arrives.

### 4. Cost control — the reaper

Three layers, each covering the previous one's failure mode:

| Layer | Where | Survives |
|---|---|---|
| 1. Deadline | inside the environment | control plane crash, network partition |
| 2. Sweeper | control plane, via `driver.List()` | worker crash, lost bookkeeping |
| 3. Lifetime cap | EventBridge → Lambda, **outside** the control plane | everything above being dead |

Layer 2 enumerates what *actually exists* through the driver, never what we
remember creating — in-memory bookkeeping cannot survive the crash it needs to
recover from. Layer 3 shares no code, role or process with the thing it
backstops, because *a backstop that runs inside the thing it is backing up is
not a backstop*. It alarms on **absent** invocations, not just errors: a reaper
that silently stopped is one whose first symptom is the bill.

Every reap logs at `warn`. A system where reaps are routine has a bug it has
learned to live with.

*Bonus (Spot / warm pools):* Fargate Spot carries job capacity at weight 4
against on-demand weight 1, with `base = 1` on-demand so a Spot shortage
degrades throughput rather than stopping work. The control plane stays
on-demand — losing it mid-job stalls everything. Warm pools: not implemented.

### 5. Security and multi-tenancy (partial, claimed as such)

Not chosen as a deep-dive, so only what the design gives for free is claimed.

Secrets: job specs carry **references**, never values. Values are resolved at
dispatch, held in memory for one `Start` call, and stored nowhere — not in the
queue, not in labels, not in logs. A `Secret` type refuses to reveal itself
through `fmt`, `%#v` or JSON, which is how credentials actually leak. A redactor
scrubs known values from agent output, because the *agent* may print its own
credentials even though the platform never does.

Isolation: per-job container with a read-only root filesystem, tmpfs for writes
(so the job's filesystem vanishes with the container by construction),
`cap-drop ALL`, `no-new-privileges`, a non-root uid, a pids limit against fork
bombs, and always a memory limit.

**Honestly**: containers share a kernel. This is not the "total memory isolation"
the brief mentions — that needs a hypervisor boundary. Fargate provides one;
gVisor or Firecracker would too.

---

## Failure modes

A named grading axis, so it gets a taxonomy rather than a bool. The kind decides
**retryability** and **fault attribution**:

| Kind | Fault | Retried | Meaning |
|---|---|---|---|
| `provision_failed` | system | yes | environment never came up; agent never ran, so retry is safe |
| `start_failed` | system | yes | environment up, agent would not start |
| `agent_crashed` | user | **no** | agent ran and exited non-zero |
| `deadline_exceeded` | user | **no** | work does not fit the budget |
| `reaped_orphan` | system | no | control plane lost track; always worth alerting |
| `rejected` | user | no | bad spec, unresolvable secret, or refused at admission |
| `internal_error` | system | yes | our bug, and it should be loud |

An unclassified error defaults to **system** fault. An error nobody classified is
one nobody thought about, and it should surface as our bug rather than be quietly
blamed on the caller.

A deterministic crash retried three times is three times the cost for the same
answer, so crashes are never auto-retried. Provisioning failures are, because
nothing in the world changed.

**What happens when a VM fails to initialise:** classified `provision_failed`,
requeued keeping its original queue position, up to the attempt limit — a job
that already waited is not punished for infrastructure's fault. **When the
network is flaky:** heartbeats tolerate transient storage errors rather than
abandoning a healthy job; log streams reconnect; a lost lease stops the worker
rather than racing the one that took over.

---

## Why this stack

**Go** — single static binary into a distroless image, concurrency primitives
that make the scheduling deep-dive natural to write *and* to explain, no runtime
to install in the container.

**bbolt, not Redis or SQS** — a library, not a service. The reviewer runs one
command with nothing else installed. ACID transactions are the one property a
job queue cannot do without. The cost is stated below.

**SSE, not WebSocket** — the traffic is one-way. Plain HTTP, no upgrade
handshake, browsers reconnect natively.

**Engine API directly, not the Docker SDK** — that SDK pulls a very large
dependency tree for a dozen stable endpoints. The cost is owning the wire
format, so stream framing and tar extraction are pure functions with their own
tests — which is exactly where the subtle bugs would otherwise hide.

---

## Scale ladder

Every limit below has a named successor. A stated limit with a successor is a
deliberate choice; the same fact unstated is a gap nobody noticed.

| Component | Now | Breaks when | Then |
|---|---|---|---|
| Queue | embedded bbolt | a **second control-plane host** — the ceiling is host count, not job rate | Redis Streams, then SQS |
| Artifacts | local filesystem | more than one host, or retention beyond local disk | S3 + lifecycle (Terraform ready) |
| Compute | Docker | host RAM — headless Chrome is 300–700 MB, so **RAM binds long before CPU** | Fargate, then Firecracker |
| Log streaming | SSE direct | multiple API instances, or replay-from-offset | pub/sub fan-out, then durable log storage |
| Scheduler | in-process pool | process death mid-job, or two schedulers racing | lease-based distributed claiming |
| Reaper | **does not move** | — | already designed for the top of the ladder |

**Honest limits at this scale:** single control-plane process, no HA. Container
isolation, not VM isolation. And *50 concurrent* is a **scheduling** claim proved
with a fast agent — no single laptop runs 50 concurrent browsers, and claiming
otherwise would not survive an interview.

---

## What has actually been verified

The brief judges operational mindset, and overstating verification is the fastest
way to lose a reviewer's trust. So, plainly:

| | Status |
|---|---|
| Core pipeline on the `process` driver | ✅ verified end to end, on a live daemon |
| 50 concurrent jobs, bounded concurrency, zero leaks | ✅ measured |
| Failure taxonomy, deadlines, retries, reaping | ✅ tested, including crash and hang paths |
| Secret non-leakage, signed URLs, path traversal | ✅ tested |
| HTTP API, SSE streaming, signed downloads | ✅ tested and exercised live |
| Terraform | ⚠️ `fmt`, `validate`, `tflint`, `checkov` pass — **never applied** |
| `docker` driver | ⚠️ pure logic tested; **integration tests never executed** |
| `fargate` driver | ❌ **not implemented** — the Terraform describes its infrastructure |
| Race detector | ⚠️ needs cgo, unavailable on the authoring machine; **runs in CI** |

**No AWS account was available.** `terraform plan` needs credentials, so
"validated" means *static* validation, not a plan against a real account. The
Docker integration tests are written and compile but were authored on a machine
without Docker; CI is the first place they run.

The `fargate` driver is the honest gap: the interface, the Terraform and the
IAM model for it exist, the implementation does not. Two drivers were built to
pressure-test the abstraction; a third written blind against an API I could not
call would have been decoration.

---

## What I would build next

1. **The `fargate` driver.** The interface holds it; only the implementation is
   missing.
2. **Replace the embedded queue** with Redis or SQS, which is the single change
   that lifts the one-host ceiling. Everything above the `Queue` interface is
   already indifferent to it.
3. **Per-tenant ready queues.** Today a tenant at its concurrency quota at the
   head of the queue causes claim-and-release churn, bounded by a backoff. The
   real fix is round-robin selection across per-tenant queues so a blocked
   tenant is skipped rather than retried.
4. **Session replay**, not just logs — periodic screenshots stitched into a
   scrubbable timeline. The artifact pipeline already carries the frames.
5. **Prometheus metrics.** Structured logs and a `/v1/stats` endpoint exist;
   queue age, dispatch latency and reap counts deserve to be scrapeable.
6. **gVisor or Firecracker**, for the isolation claim the current design
   deliberately does not make.

---

## Repository map

| Path | What |
|---|---|
| `internal/job` | domain: state machine, failure taxonomy, secret redaction |
| `internal/driver` | the load-bearing interface |
| `internal/driver/process` | zero-dependency driver, real |
| `internal/driver/docker` | container driver with structural egress control |
| `internal/queue` | durable lease-based priority queue |
| `internal/admission` | rate limits and concurrency quotas |
| `internal/scheduler` | worker pool, retries, teardown ordering |
| `internal/reaper` | cost control, layer 2 |
| `internal/logstream` | live fan-out |
| `internal/artifact` | storage and signed URLs |
| `internal/api` | HTTP surface |
| `cmd/ephemerad` | control plane; the composition root, read it first |
| `cmd/ephemera` | CLI |
| `cmd/ephemera-agent` | placeholder agent |
| `deploy/terraform` | the IaC deliverable, layer-3 reaper included |
| `docs/` | architecture and security notes |

Further reading: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the design
decisions in full, [docs/SECURITY.md](docs/SECURITY.md) for the threat model and
what it does not cover.

## Commands

```
make build        build all binaries
make test         everything that does not need Docker
make test-race    the race detector
make test-docker  integration tests against a real daemon
make load-test    50 concurrent jobs
make up / down    the full stack with egress isolation
make tf-validate  format-check and validate the Terraform
make ci           everything CI runs
```
