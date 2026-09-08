# ephemera — Runbook

Every command needed to build, run, test, deploy and tear down this system.

**You do not need to understand the codebase to use this page.** Each section
says what to run, what you should see, and what to do when you see something
else. Commands are copy-pasteable in order.

If you only have ten minutes, do [Part 1](#part-1--run-it-locally-10-minutes).

---

## Contents

- [Part 0 — Prerequisites](#part-0--prerequisites)
- [Part 1 — Run it locally (10 minutes)](#part-1--run-it-locally-10-minutes)
- [Part 2 — Run the full stack in Docker](#part-2--run-the-full-stack-in-docker)
- [Part 3 — Every command, explained](#part-3--every-command-explained)
- [Part 4 — The test suites](#part-4--the-test-suites)
- [Part 5 — Deploy to AWS](#part-5--deploy-to-aws)
- [Part 6 — Tear down AWS](#part-6--tear-down-aws)
- [Part 7 — Troubleshooting](#part-7--troubleshooting)

---

## Part 0 — Prerequisites

| Tool | Needed for | Install | Check |
|---|---|---|---|
| Go 1.27+ | everything | `brew install go` | `go version` |
| Docker | Parts 2, 4 | [Docker Desktop](https://docker.com) | `docker info` |
| Terraform 1.6+ | Part 5 | `brew install hashicorp/tap/terraform` | `terraform version` |
| AWS CLI v2 | Part 5 | `brew install awscli` | `aws --version` |

> **Note on Terraform:** `brew install terraform` no longer works — HashiCorp
> moved it out of homebrew-core when the licence changed. You need the tap:
> `brew install hashicorp/tap/terraform`.

Clone over HTTPS (no SSH key required):

```bash
git clone https://github.com/bytes-as/ephemera.git
cd ephemera
```

---

## Part 1 — Run it locally (10 minutes)

The fastest path. Needs **only Go** — no Docker, no cloud account, no services.

```bash
# 1. Build the three binaries into ./bin
make build

# 2. Start the control plane in the background
./bin/ephemerad --data-dir ./data &

# 3. Submit a job and watch it run
./bin/ephemera run \
  --command "$PWD/bin/ephemera-agent" \
  --query "site reliability"
```

**What you should see:** the job is submitted, its logs stream live, and you get
signed artifact links:

```
submitted job_06g83mdz1ns0r2219cppsp0gg0 (priority 50)
* provisioning process environment
* agent started
  step 1/4: launching browser
  ...
* stored 2 artifact(s)

job          job_06g83...
state        succeeded
artifacts:
  result.json      145 B  http://localhost:8080/v1/jobs/.../result.json?expires=...&signature=...
  screenshot.png  1.3 KB  http://localhost:8080/v1/jobs/.../screenshot.png?expires=...&signature=...
```

Fetch an artifact (paste a URL from above — the signature is required):

```bash
curl "PASTE_THE_URL_HERE"
```

Stop it and clean up:

```bash
kill %1
make clean
```

> **What this does and does not prove.** This uses the `process` driver, which
> runs the agent as a plain subprocess. It exercises the whole pipeline —
> queue, scheduler, deadlines, retries, reaper, artifacts, signed links — but it
> is **not** container isolation and enforces **no** network policy. For the
> real isolation story, do Part 2.

---

## Part 2 — Run the full stack in Docker

This is the one to run if you want to see what the system actually claims:
per-job containers, structural egress isolation, and a metadata endpoint that
is unreachable by construction.

```bash
docker compose up --build -d
```

First run takes a few minutes (it builds three images). Then:

```bash
docker compose ps
```

**What you should see** — both services up, control plane healthy:

```
NAME                       STATUS                   PORTS
ephemera-control-plane-1   Up 10 seconds (healthy)  0.0.0.0:8080->8080/tcp
ephemera-egress-proxy-1    Up 10 seconds            8888/tcp
```

> **Linux users, one extra step.** The control plane needs to reach the Docker
> socket, which on Linux is owned by the `docker` group rather than root. Start
> the stack like this instead:
>
> ```bash
> EPHEMERA_DOCKER_GID=$(stat -c '%g' /var/run/docker.sock) docker compose up --build -d
> ```
>
> On macOS and Windows the default is already correct.

### Run a job in a real container

```bash
./bin/ephemera run \
  --image ephemera/agent:dev \
  --query "distributed systems" \
  --server http://localhost:8080
```

You should see the same live log stream as Part 1, but the agent is now running
in its own container with no route to the internet except through the proxy.

### Prove the egress control (the security claim)

```bash
# Cloud metadata must be unreachable from the jobs network.
docker run --rm --network ephemera_jobs alpine:3.19 \
  sh -c 'wget -q -T 3 -O - http://169.254.169.254/ 2>&1 || echo BLOCKED-GOOD'

# And there is no direct route to the internet at all.
docker run --rm --network ephemera_jobs alpine:3.19 \
  sh -c 'wget -q -T 3 -O /dev/null http://example.com 2>&1 || echo NO-ROUTE-GOOD'
```

Both should print the `-GOOD` line. That is the egress claim, checked rather
than asserted.

### Behaviour under load: 50 concurrent jobs

```bash
AGENT_IMAGE=ephemera/agent:dev ./scripts/load-test.sh 50 http://localhost:8080
```

**What you should see** — everything admitted, everything finished, nothing
left running:

```
submission results after 0s:
  accepted (202):      50
  rate limited (429):  0
  queue full (503):    0

outcome after 7s:
  succeeded: 50
  failed:    0

queue drained, nothing left running
```

### Confirm nothing leaked

```bash
docker ps -aq --filter "label=ephemera.managed=true" | wc -l   # want 0
ls -1 ./data/environments | wc -l                              # want 0
```

### Stop everything

```bash
docker compose down -v
rm -rf ./data
```

---

## Part 3 — Every command, explained

Run `make help` to list these at any time.

### Build and run

| Command | What it does |
|---|---|
| `make build` | Builds `ephemerad` (control plane), `ephemera` (CLI) and `ephemera-agent` into `./bin`. |
| `make run` | Builds, then runs the control plane with the `process` driver and debug logging. |
| `make demo` | Runs one job end to end against a control plane you already started. |
| `make clean` | Deletes `./bin` and `./data`. |

### Docker stack

| Command | What it does |
|---|---|
| `make up` | `docker compose up --build -d`. Starts control plane + egress proxy. |
| `make logs` | Follows the control plane's logs. |
| `make down` | Stops the stack and removes its volumes. |
| `make load-test` | Submits 50 concurrent jobs and reports the outcome. |

### Quality gates

| Command | What it does |
|---|---|
| `make lint` | `gofmt` check plus `go vet`. |
| `make fmt` | Formats the tree. |
| `make cross` | Builds for linux/amd64, linux/arm64 and darwin/arm64. |
| `make tf-validate` | `terraform fmt -check`, `init -backend=false`, `validate`. **Touches no AWS account.** |
| `make ci` | Everything CI runs: lint, cross, test, tf-validate. |

### AWS (these DO touch your account)

| Command | What it does |
|---|---|
| `make aws-push TAG=v1` | Builds both images for **linux/arm64**, logs in to your ECR, pushes, and prints the image references Terraform needs. |
| `make aws-verify-images TAG=v1` | Confirms the pushed images are arm64. Catches the mismatch that otherwise appears as `exec format error` at task start. |

### The control plane's own flags

`./bin/ephemerad --help` prints all of them. The ones that matter:

| Flag | Env var | Default | Meaning |
|---|---|---|---|
| `--addr` | `EPHEMERA_ADDR` | `:8080` | Address the HTTP API listens on. |
| `--data-dir` | `EPHEMERA_DATA_DIR` | `./data` | Where the queue, artifacts and environments live. |
| `--driver` | `EPHEMERA_DRIVER` | `process` | `process`, `docker` or `fargate`. |
| `--workers` | `EPHEMERA_WORKERS` | `8` | Jobs that may run simultaneously. |
| `--queue-depth` | `EPHEMERA_QUEUE_DEPTH` | `1000` | Queued jobs before submissions are refused with 503. |
| `--max-env-lifetime` | `EPHEMERA_MAX_ENV_LIFETIME` | `1h` | Hard cap. No environment outlives this, whatever else breaks. |
| `--default-deadline` | `EPHEMERA_DEFAULT_DEADLINE` | `5m` | Deadline for jobs that request none. |
| `--tokens` | `EPHEMERA_TOKENS` | *(empty)* | `token=tenant` pairs. **Empty means no authentication.** |
| `--signing-key` | `EPHEMERA_SIGNING_KEY` | *(generated)* | Key for artifact links. Generated per run if unset, which invalidates old links on restart. |

Docker driver only:

| Flag | Env var | Meaning |
|---|---|---|
| `--docker-network` | `EPHEMERA_DOCKER_NETWORK` | Network job containers join. Use an `internal` network for egress control. |
| `--egress-proxy` | `EPHEMERA_EGRESS_PROXY` | Proxy URL injected into jobs on an internal network. |
| `--pull-policy` | `EPHEMERA_PULL_POLICY` | `always`, `if-missing` or `never`. |

Fargate driver only — all of these come from Terraform outputs (Part 5):

| Flag | Env var | Meaning |
|---|---|---|
| `--ecs-cluster` | `EPHEMERA_ECS_CLUSTER` | ECS cluster name. |
| `--job-task-definition` | `EPHEMERA_JOB_TASK_DEFINITION` | Task definition family for jobs. |
| `--subnets` | `EPHEMERA_SUBNETS` | Comma-separated subnet IDs. |
| `--security-groups` | `EPHEMERA_SECURITY_GROUP` | Comma-separated security group IDs. |
| `--artifact-bucket` | `EPHEMERA_ARTIFACT_BUCKET` | S3 bucket agents upload artifacts to. |
| `--job-log-group` | `EPHEMERA_JOB_LOG_GROUP` | CloudWatch Logs group job output goes to. |

### The CLI

```bash
./bin/ephemera run    --command <path> | --image <image> [--query <text>]  # submit and follow
./bin/ephemera submit --image <image>                                      # submit, do not wait
./bin/ephemera logs   <job-id>                                             # stream logs
./bin/ephemera get    <job-id>                                             # job status
./bin/ephemera list   [--state succeeded|failed|running]                   # list jobs
./bin/ephemera stats                                                       # queue and worker state
```

All accept `--server <url>` (default `http://localhost:8080`) and
`--token <token>` when authentication is on.

### The HTTP API

```bash
# Submit
curl -X POST http://localhost:8080/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{"image":"ephemera/agent:dev","env":{"EPHEMERA_QUERY":"hello"},"priority":"normal"}'

# Status, logs (server-sent events), artifacts
curl http://localhost:8080/v1/jobs/<job-id>
curl -N http://localhost:8080/v1/jobs/<job-id>/logs
curl http://localhost:8080/v1/jobs/<job-id>/artifacts

# Health and capacity
curl http://localhost:8080/healthz
curl http://localhost:8080/readyz
curl http://localhost:8080/v1/stats
```

---

## Part 4 — The test suites

| Command | Needs | What it covers |
|---|---|---|
| `make test` | Go only | All 12 packages. No Docker, no cloud. |
| `make test-race` | Go + cgo | The same suite under the race detector. |
| `make test-docker` | Docker running | Adds 11 integration tests against a live daemon. |
| `make test-all` | all three | Everything. |

```bash
make test        # expect: ok for every package
make test-race   # expect: ok, and no WARNING: DATA RACE
make test-docker # expect: ok, including 11 TestIntegration* tests
```

> **Read the skips.** `make test-docker` reporting `ok` while every
> `TestIntegration*` line says `SKIP` means the tests never ran — the suite is
> green and proves nothing. Check with:
>
> ```bash
> go test -tags docker ./internal/driver/docker/ -run TestIntegration -v | grep -E '^--- '
> ```
>
> You want `PASS` on all 11. If you see `SKIP: ... docker daemon not available`,
> the daemon is unreachable — see [Part 7](#part-7--troubleshooting).

---

## Part 5 — Deploy to AWS

> ### Read this first
>
> **The `fargate` driver has never been executed against AWS.** It compiles, it
> is unit tested against fakes, and it has never spoken to ECS. The Terraform
> has never been applied. Expect to fix things on the first run.
>
> **This will cost money.** A NAT gateway alone is roughly $32/month before any
> traffic, and it is billed whether or not you run a single job. Do
> [Part 6](#part-6--tear-down-aws) when you are finished.
>
> Use a **personal or sandbox account**, never a shared or corporate one.

### 5.1 Point the AWS CLI at the right account

```bash
export AWS_PROFILE=my-personal-profile
export AWS_REGION=eu-west-1

# Confirm you are where you think you are. Check this output carefully.
aws sts get-caller-identity
```

### 5.2 Validate without touching anything

Safe: no credentials used, no API calls, no state.

```bash
make tf-validate
```

### 5.3 Create the registries first

The task definitions need images that exist, so the registries come first.

```bash
cd deploy/terraform
terraform init
terraform apply -target=aws_ecr_repository.control_plane -target=aws_ecr_repository.agent
```

Read the plan before typing `yes`.

### 5.4 Build and push the images

One command, from the repo root. It reads the registry URLs from Terraform,
logs Docker in, and builds for the right architecture:

```bash
cd ../..           # back to the repo root
make aws-push TAG=v1
```

It prints the two image references you need for the next step. Check they are
the architecture ECS expects:

```bash
make aws-verify-images TAG=v1     # want: "ok: ... is arm64" twice
```

> **Why this is a `make` target and not a `docker build` you type yourself.**
> The task definitions run on Graviton (ARM64, for the cost). An image built on
> an amd64 laptop without `--platform linux/arm64` pushes fine, deploys fine,
> and then dies on start with `exec format error` — a message that says nothing
> about architecture and sends people looking at their application code. The
> flag lives in the Makefile so it cannot be forgotten.

### 5.5 Apply the rest

```bash
cd deploy/terraform

# Use the two values `make aws-push` printed.
terraform apply \
  -var "control_plane_image=<control_plane_image from aws-push>" \
  -var "agent_image=<agent_image from aws-push>"
```

Takes about 5 minutes, mostly the NAT gateway.

### 5.6 Set the signing key

Terraform creates the secret but deliberately not its value — a secret in state
is a secret in every backup of that state.

```bash
aws secretsmanager put-secret-value \
  --secret-id "$(terraform output -raw signing_key_secret_arn)" \
  --secret-string "$(openssl rand -hex 32)"
```

Then restart the service so it picks the key up:

```bash
aws ecs update-service \
  --cluster "$(terraform output -raw cluster_name)" \
  --service ephemera-control-plane \
  --force-new-deployment
```

### 5.7 Check it came up

```bash
CLUSTER=$(terraform output -raw cluster_name)

# Is the service running its task?
aws ecs describe-services --cluster "$CLUSTER" --services ephemera-control-plane \
  --query 'services[0].{running:runningCount,desired:desiredCount,status:status}'

# What is the control plane saying?
aws logs tail /ephemera/dev/control-plane --follow
```

**You want** `msg=ready ... driver=fargate`. Anything else — see
[Part 7](#part-7--troubleshooting).

### 5.8 Submit a job

The control plane sits in a private subnet with no public endpoint, which is
deliberate. Reach it with a port-forward through ECS Exec:

```bash
TASK=$(aws ecs list-tasks --cluster "$CLUSTER" --service-name ephemera-control-plane \
  --query 'taskArns[0]' --output text)

aws ecs execute-command --cluster "$CLUSTER" --task "$TASK" \
  --container control-plane --interactive --command "/bin/sh"
```

> ECS Exec requires `enableExecuteCommand` on the service. If it is not enabled,
> the simpler route while testing is to put an ALB in front of the service, or
> run the control plane locally against the AWS driver — see 5.9.

### 5.9 Easier: run the control plane locally, jobs on Fargate

Often the most practical way to try the AWS path, and the quickest way to see
errors. The control plane runs on your laptop with your credentials; jobs run
as real Fargate tasks.

```bash
cd deploy/terraform

export AWS_REGION=$(terraform output -raw region 2>/dev/null || echo "$AWS_REGION")
export EPHEMERA_DRIVER=fargate
export EPHEMERA_ECS_CLUSTER=$(terraform output -raw cluster_name)
export EPHEMERA_JOB_TASK_DEFINITION=ephemera-dev-job
export EPHEMERA_SUBNETS=$(terraform output -json private_subnet_ids | tr -d '[]" ' )
export EPHEMERA_SECURITY_GROUP=$(terraform output -raw job_security_group_id)
export EPHEMERA_ARTIFACT_BUCKET=$(terraform output -raw artifact_bucket)
export EPHEMERA_JOB_LOG_GROUP=/ephemera/dev/jobs

cd ../..
./bin/ephemerad --data-dir ./data
```

Then, in another terminal:

```bash
./bin/ephemera run --query "hello from fargate"
```

Watch the task appear:

```bash
aws ecs list-tasks --cluster "$EPHEMERA_ECS_CLUSTER" --started-by ephemera
```

### 5.10 Confirm nothing was left behind

The cost guarantee, checked:

```bash
aws ecs list-tasks --cluster "$CLUSTER" --started-by ephemera --desired-status RUNNING
```

Should be empty once your jobs have finished. If it is not, the reaper's
Lambda is the backstop:

```bash
aws lambda invoke --function-name "$(terraform output -raw reaper_function_name)" /dev/stdout
```

---

## Part 6 — Tear down AWS

**Do this when you are done.** The NAT gateway bills by the hour.

```bash
cd deploy/terraform

# The bucket refuses to delete while it holds objects, on purpose. Empty it
# only when you are sure you do not want the artifacts.
aws s3 rm "s3://$(terraform output -raw artifact_bucket)" --recursive

terraform destroy
```

Confirm nothing survives:

```bash
aws ecs list-clusters
aws ec2 describe-nat-gateways --filter "Name=state,Values=available"
aws s3 ls | grep ephemera
```

All three should be empty of ephemera resources. **A NAT gateway left running
is the single most expensive mistake available here.**

---

## Part 7 — Troubleshooting

### Local

**`make test-docker` reports `ok` but every integration test says SKIP**

The daemon is unreachable from the test. Check `docker info` works. This project
pins the Engine API version in `internal/driver/docker/client.go`; if your
daemon is older than that pin, every test skips with "daemon not available"
while the daemon is in fact running.

**`docker compose up` — control plane restarting, "permission denied" on /data**

The image runs as uid 65532 and the data directory is owned by someone else.
`rm -rf ./data` and bring the stack up again.

**`docker compose up` — control plane restarting, "permission denied" on docker.sock**

You are on Linux. Start with the socket's group:

```bash
EPHEMERA_DOCKER_GID=$(stat -c '%g' /var/run/docker.sock) docker compose up -d
```

**Jobs fail with `start_failed` / "mounts denied"**

Docker Desktop is not sharing the project directory. Settings → Resources →
File Sharing, add the path, restart Docker.

**`./scripts/load-test.sh: Permission denied`**

`chmod +x scripts/load-test.sh`.

**Port 8080 already in use**

```bash
EPHEMERA_PORT=8081 docker compose up -d
# or, locally:
./bin/ephemerad --addr :8081
```

### AWS

**Task stops immediately with `CannotPullContainerError`**

Either the image does not exist at the tag you referenced, or the private subnet
has no route to ECR. Check the NAT gateway exists and the S3 VPC endpoint is
present (ECR pulls layers from S3).

**Task stops with `exec format error`**

You pushed an amd64 image. The task definitions run ARM64. Rebuild with
`--platform linux/arm64` (step 5.4).

**Control plane logs `unknown driver "fargate"`**

You are running an image built before the Fargate driver existed. Rebuild and
push.

**Control plane starts but every job fails immediately**

Read `/ephemera/dev/control-plane`. The most likely causes are a missing
`EPHEMERA_JOB_LOG_GROUP`, subnets that cannot reach ECR, or a task definition
family name that does not match.

**Jobs run but no logs appear**

The log stream name must be `job/agent/<task-id>`. If the task definition's
`awslogs-stream-prefix` is not `job`, or the container is not named `agent`, the
control plane will look in the wrong place. These are `--job-container-name` and
the driver's `LogStreamPrefix`.

**Jobs succeed but produce no artifacts**

The agent uploads its own artifacts to a presigned S3 URL. If it crashed, it
uploaded nothing — that is a known limitation, documented in the README. Check
whether the object exists:

```bash
aws s3 ls "s3://$(terraform output -raw artifact_bucket)/jobs/" --recursive
```

**`terraform destroy` fails on the S3 bucket**

By design — it will not silently delete artifacts. Empty it first (Part 6).

---

## Where else to look

- [README](../README.md) — what this system is, the design, and exactly what has and has not been verified.
- [docs/ARCHITECTURE.md](ARCHITECTURE.md) — how the pieces fit together.
- [docs/SECURITY.md](SECURITY.md) — the isolation and secret-handling model, including its limits.
