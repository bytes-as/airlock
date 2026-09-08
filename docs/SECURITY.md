# Security notes

What this system defends against, how, and — the more useful half — what it
does not.

## Threat model

The agent is **untrusted code**. It may be buggy, it may be compromised through
whatever it browses, and it may be actively hostile. The properties worth
protecting, in order:

1. A compromised agent must not obtain cloud credentials.
2. A compromised agent must not reach other tenants' work, or internal services.
3. A compromised agent must not read secrets belonging to other jobs.
4. A compromised agent must not outlive its deadline, because compute is money.
5. Credentials must not leak into logs, artifacts, or persisted records.

## 1. Cloud credentials

The instance metadata endpoint is the attack that matters: reach
`169.254.169.254` from untrusted code and a sandbox becomes stolen IAM
credentials.

**Docker.** Job containers join an `internal: true` network, which has no route
off itself. The endpoint is unreachable because everything is unreachable — a
structural property, not a filter that can be mis-specified. An egress proxy is
the only bridge out, and its filter denies the metadata and RFC1918 ranges too,
so the block survives someone later making that network routable.

**Fargate.** No network control can block `169.254.170.2`: it is link-local and
never traverses a route table, NACL or security group. So the defence is IAM
rather than network — the job task role has *no permissions*. An agent that
steals those credentials gains nothing.

ECS gives a task two roles and conflating them is the common mistake:

- **execution role** — used by the ECS agent to pull images and write logs. The
  container never holds these credentials.
- **task role** — assumed by the code inside, reachable from it. Anything this
  role can do, a compromised agent can do.

The task role carries an explicit `Deny *` rather than simply having no policy
attached. The effect is identical today; the difference is that it documents
intent, and the next person to touch the file has to consciously contend with it
rather than assume an oversight.

**Process driver: no defence at all.** It shares the host's network. It is for
local development with code you wrote. `Capabilities()` says so, and the control
plane warns at boot.

## 2. Tenant and network isolation

The job security group permits outbound 80, 443 and DNS to the VPC resolver.
Nothing permits the VPC CIDR, so databases, internal services and other tenants'
tasks are unreachable *because they were never allowed* — a stronger position
than a deny rule someone must maintain.

There are no inbound rules. A job task is not a server; nothing should ever
connect to it.

The API returns **404, not 403**, for a job belonging to another tenant.
Confirming existence would leak the shape of other tenants' work.

## 3. Filesystem and process isolation

Per job: read-only root filesystem, tmpfs for writes (so the filesystem vanishes
with the container by construction rather than by remembering to delete it),
`cap-drop ALL`, `no-new-privileges` (a setuid binary cannot regain what was
dropped), non-root uid, a pids limit against fork bombs, and always a memory
limit.

**What this is not.** Containers share a kernel. A kernel exploit crosses the
boundary. The brief mentions "total filesystem and memory isolation" — that
requires a hypervisor boundary, which Fargate provides per task and which gVisor
or Firecracker would provide on self-managed hosts. The Docker driver does not
provide it and does not claim to.

## 4. Secrets

Job specs carry **references**, never values:

```json
{"name": "API_TOKEN", "source": "aws-secrets-manager", "key": "prod/agent-token"}
```

Values are resolved at dispatch, held in process memory for the length of one
`Start` call, injected into the environment, and stored nowhere. Not in the
queue, not in labels, not in the container name, not in logs.

Four layers, because the interesting leaks are accidental:

1. **`job.Secret`** refuses to reveal itself through `fmt`, `%#v`, `%+v` and
   JSON — the routes credentials actually escape by, which is a struct dumped
   into a log line during debugging that nobody noticed. `Reveal()` is the only
   way out, and `grep Reveal(` audits the blast radius.
2. **Refs, not values, in persisted records** — so the job record is safe to
   store, serve over the API, and paste into a bug report.
3. **Source routing without fallback** — a reference names its store. A fallback
   chain would let a secret resolve from the wrong one, which is how a dev
   credential reaches a prod job.
4. **Output redaction** — the platform never logs a secret, but the *agent* may
   print one. Known values are scrubbed from the log stream on the way out.
   Values under six characters are ignored: a credential short enough to collide
   with ordinary text cannot be usefully protected this way, and matching it
   would redact everything.

`FileSource` contains lookups inside its root, because otherwise a key of
`../../.ssh/id_rsa` turns a secret reference into arbitrary host file read. Its
errors carry no paths — paths to secret material are themselves worth not
logging.

## 5. Untrusted input from the agent

Everything an agent produces is untrusted:

- **Artifact names** come from its filesystem. Traversal is refused on write and
  on read.
- **Tar streams** from `docker cp` can carry absolute paths, `..`, and symlinks.
  A naive extractor writes through all three; a symlink pointing at
  `/etc/passwd` turns "collect the screenshots" into host file read. Symlinks
  and hard links are skipped entirely — they have no legitimate use in an
  artifact set. Each file is bounded by its declared size, so a stream that lies
  about its length cannot fill the disk.
- **Log frames** carry a length header. It is bounded, so a corrupt stream
  cannot make us allocate gigabytes.

## 6. Artifact access

Signed URLs, not sessions. The HMAC covers job ID, artifact name and expiry as
one **length-prefixed** message, so `("ab","c")` and `("a","bc")` cannot collide,
an expiry cannot be edited, and a signature cannot be replayed against a
different artifact.

Signature is verified **before** expiry. Reporting "expired" to an unsigned
request would confirm to an attacker that their forgery was otherwise
well-formed. Comparison is constant-time.

## 7. API authentication

Bearer tokens mapped to tenants. Every failure — missing token, wrong scheme,
unknown token, empty token — returns a **byte-identical** response, so a probe
cannot tell when it has found a real one.

With no tokens configured the server runs open and **says so loudly at startup**.
That is fine for a local demo and unacceptable anywhere else, and the warning is
what keeps it from being a silent hole.

## Known weaknesses

Stated rather than discovered.

**The Docker socket is root on the host.** The control plane mounts it, because
its job is creating and destroying containers. Anything that executes in that
container can start a privileged container mounting `/`. Mitigations shipped: a
distroless image with no shell or package manager, a read-only socket mount, and
a non-root user. Mitigation *not* shipped: a socket proxy allowing only the
endpoints this driver calls. That is the right answer for production and it is
not here.

**Egress filtering is a denylist.** Agents legitimately browse the open web, so
an allowlist would be unusable. A denylist is evadable — a DNS name resolving to
a blocked address, for one. The structural control (a network with no route off
it) is what actually holds; the filter is the second layer that keeps holding if
someone makes that network routable.

**No per-tenant network segmentation.** All job containers share one internal
network, so a compromised agent could reach another running job's container over
that network. Per-tenant networks are the fix and are not implemented.

**No audit log.** Submissions and lifecycle events are in structured logs, but
there is no tamper-evident audit trail.

**Secrets are visible to the agent by design.** Anything injected into the
environment can be read by the code running there, and by anything that
compromises it. That is inherent to injection; the mitigation is scope and
rotation, not the transport.

**No signed images or provenance.** The driver pulls whatever tag it is given.
Digest pinning and signature verification are the fix.

**No egress rate limiting.** A compromised agent can use as much bandwidth as
the NAT gateway allows, which is a cost attack even when it is not a data one.
