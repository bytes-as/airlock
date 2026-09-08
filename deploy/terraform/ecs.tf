# ECS cluster, task definitions and the control plane service.

resource "aws_ecs_cluster" "main" {
  name = local.name

  setting {
    # Container Insights costs money per metric. It is on because the first
    # question during an incident is "what was CPU and memory doing", and
    # discovering you were not collecting it is a bad moment to have.
    name  = "containerInsights"
    value = "enabled"
  }
}

# Capacity providers: Spot for jobs, on-demand for the control plane.
#
# Deep-dive 4's bonus. Spot is roughly 70 percent cheaper and can be reclaimed
# with two minutes' notice. Job tasks tolerate that well - they are short, and
# the failure taxonomy already classifies an interrupted job as retryable
# infrastructure failure rather than an agent bug. The control plane does not
# tolerate it: losing it mid-job means leases lapse and work stalls, so it stays
# on on-demand. Same cluster, different risk appetite, deliberately.
resource "aws_ecs_cluster_capacity_providers" "main" {
  cluster_name       = aws_ecs_cluster.main.name
  capacity_providers = ["FARGATE", "FARGATE_SPOT"]

  default_capacity_provider_strategy {
    capacity_provider = "FARGATE_SPOT"
    weight            = var.spot_weight
    base              = 0
  }

  default_capacity_provider_strategy {
    capacity_provider = "FARGATE"
    weight            = 1
    # base = 1 keeps at least one on-demand task available, so a Spot capacity
    # shortage degrades throughput rather than stopping work entirely.
    base = 1
  }
}

# --- logging ---

resource "aws_cloudwatch_log_group" "jobs" {
  name              = "/ephemera/${var.environment}/jobs"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "control_plane" {
  name              = "/ephemera/${var.environment}/control-plane"
  retention_in_days = var.log_retention_days
}

# --- job task definition ---

resource "aws_ecs_task_definition" "job" {
  family                   = "${local.name}-job"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.job_cpu
  memory                   = var.job_memory

  execution_role_arn = aws_iam_role.execution.arn
  # The powerless role. See iam.tf for why this is empty rather than narrow.
  task_role_arn = aws_iam_role.job_task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64" # Graviton: cheaper per vCPU-hour than x86.
  }

  container_definitions = jsonencode([
    {
      name      = "agent"
      image     = var.agent_image
      essential = true

      # Hardening that mirrors what the Docker driver applies locally, so the
      # two paths do not drift into different security postures.
      readonlyRootFilesystem = true
      user                   = "65534:65534"

      linuxParameters = {
        # Fargate does not support capability dropping the way EC2 does, but it
        # does support this, and a fork bomb in one task should not be able to
        # exhaust the task's process table.
        initProcessEnabled = true
      }

      mountPoints = [
        {
          sourceVolume  = "artifacts"
          containerPath = "/artifacts"
          readOnly      = false
        }
      ]

      environment = [
        { name = "EPHEMERA_ARTIFACT_DIR", value = "/artifacts" },
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.jobs.name
          "awslogs-region"        = var.region
          "awslogs-stream-prefix" = "job"
        }
      }

      # A container-level stop timeout, so a task that ignores SIGTERM is
      # SIGKILLed rather than lingering. This is a small piece of the reaper
      # story: the innermost layer inside AWS itself.
      stopTimeout = 30
    }
  ])

  volume {
    name = "artifacts"
    # No configuration block means an ephemeral volume that lives and dies with
    # the task. The job's filesystem vanishing with the task is a guarantee of
    # the platform rather than something the control plane has to remember.
  }

  tags = { Name = "${local.name}-job" }
}

# --- control plane task definition ---

resource "aws_ecs_task_definition" "control_plane" {
  family                   = "${local.name}-control-plane"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.control_plane_cpu
  memory                   = var.control_plane_memory

  execution_role_arn = aws_iam_role.execution.arn
  task_role_arn      = aws_iam_role.control_plane.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }

  container_definitions = jsonencode([
    {
      name      = "control-plane"
      image     = var.control_plane_image
      essential = true

      portMappings = [
        { containerPort = 8080, protocol = "tcp" }
      ]

      environment = [
        { name = "EPHEMERA_ADDR", value = ":8080" },
        { name = "EPHEMERA_DRIVER", value = "fargate" },
        { name = "EPHEMERA_LOG_FORMAT", value = "json" },
        { name = "EPHEMERA_ECS_CLUSTER", value = aws_ecs_cluster.main.name },
        { name = "EPHEMERA_JOB_TASK_DEFINITION", value = aws_ecs_task_definition.job.family },
        { name = "EPHEMERA_ARTIFACT_BUCKET", value = aws_s3_bucket.artifacts.id },
        { name = "EPHEMERA_QUEUE_URL", value = aws_sqs_queue.jobs.url },
        { name = "EPHEMERA_SUBNETS", value = join(",", aws_subnet.private[*].id) },
        { name = "EPHEMERA_SECURITY_GROUP", value = aws_security_group.job.id },
        { name = "EPHEMERA_MAX_ENV_LIFETIME", value = "${var.max_job_lifetime_minutes}m" },
      ]

      # The signing key comes from Secrets Manager, injected by the ECS agent
      # using the *execution* role. It never appears in the task definition,
      # which is readable by anyone with ecs:DescribeTaskDefinition.
      secrets = [
        {
          name      = "EPHEMERA_SIGNING_KEY"
          valueFrom = aws_secretsmanager_secret.signing_key.arn
        }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.control_plane.name
          "awslogs-region"        = var.region
          "awslogs-stream-prefix" = "control-plane"
        }
      }

      healthCheck = {
        # /readyz, not /healthz: readiness reflects whether the process can
        # actually serve, which is what should gate traffic.
        command     = ["CMD", "/usr/local/bin/ephemera", "stats", "--server", "http://localhost:8080"]
        interval    = 15
        timeout     = 5
        retries     = 3
        startPeriod = 20
      }

      # Longer than a job's grace period: the control plane drains in-flight
      # work on shutdown rather than abandoning environments, and killing it
      # mid-drain would leak exactly what the drain exists to prevent.
      stopTimeout = 120
    }
  ])

  tags = { Name = "${local.name}-control-plane" }
}

resource "aws_ecs_service" "control_plane" {
  name            = "${local.name}-control-plane"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.control_plane.arn
  desired_count   = 1

  # On-demand, not Spot. See the capacity provider comment above.
  capacity_provider_strategy {
    capacity_provider = "FARGATE"
    weight            = 1
    base              = 1
  }

  network_configuration {
    subnets          = aws_subnet.private[*].id
    security_groups  = [aws_security_group.control_plane.id]
    assign_public_ip = false
  }

  # Rolling deploys with a circuit breaker: a control plane that fails to start
  # rolls back automatically rather than leaving the system without one.
  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  # deployment_maximum_percent 100 and minimum 0 means "stop the old task before
  # starting the new one". Normally the wrong choice - but the queue is a bbolt
  # file opened by exactly one process, so two control planes cannot run at
  # once. This is the single-instance constraint from the scale ladder showing
  # up in the infrastructure, and it is why replacing the embedded queue is the
  # first step to horizontal scale.
  deployment_maximum_percent         = 100
  deployment_minimum_healthy_percent = 0

  enable_execute_command = false # No shell into production containers.

  depends_on = [aws_ecs_cluster_capacity_providers.main]
}
