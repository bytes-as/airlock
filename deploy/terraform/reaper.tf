# The outermost reaper layer.
#
# Layers 1 and 2 live in the application: the task's own stop timeout, and the
# control plane's sweeper. Both share a fate - if the control plane is wedged,
# crashed, or has been deleted by a bad deploy, neither helps, and Fargate tasks
# keep billing by the second until somebody notices.
#
# So this layer runs outside the control plane entirely: an EventBridge schedule
# firing a Lambda that lists tasks in the cluster and stops any that have
# outlived the cap. It shares no code, no role and no process with the thing it
# is backstopping. That independence is the entire point - a backstop that runs
# inside the thing it is backing up is not a backstop.
#
# It is also the only layer that can bound the worst case in a sentence an
# operator can hold onto: "no task in this cluster outlives
# max_job_lifetime_minutes, whatever else is broken."

data "archive_file" "reaper" {
  type        = "zip"
  output_path = "${path.module}/.build/reaper.zip"

  source {
    filename = "index.py"
    content  = <<-PYTHON
      """Stop ECS tasks that have outlived the configured maximum lifetime.

      Deliberately dependency-free and short. This runs when other things have
      already failed, so it must not itself depend on much: no layers, no
      packages beyond the boto3 the Lambda runtime already provides, no shared
      library with the control plane it is cleaning up after.
      """
      import datetime
      import os

      import boto3

      CLUSTER = os.environ["CLUSTER"]
      MAX_LIFETIME_MINUTES = int(os.environ["MAX_LIFETIME_MINUTES"])

      ecs = boto3.client("ecs")


      def handler(event, context):
          cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
              minutes=MAX_LIFETIME_MINUTES
          )

          scanned = 0
          stopped = []
          failed = []

          paginator = ecs.get_paginator("list_tasks")
          for page in paginator.paginate(cluster=CLUSTER, desiredStatus="RUNNING"):
              arns = page.get("taskArns", [])
              if not arns:
                  continue

              # describe_tasks accepts at most 100 ARNs per call.
              for batch_start in range(0, len(arns), 100):
                  batch = arns[batch_start : batch_start + 100]
                  described = ecs.describe_tasks(cluster=CLUSTER, tasks=batch)

                  for task in described.get("tasks", []):
                      scanned += 1

                      # The control plane's own task must never be reaped. It is
                      # long-lived by design, and stopping it would turn a cost
                      # control into an outage.
                      group = task.get("group", "")
                      if "control-plane" in group:
                          continue

                      started = task.get("startedAt") or task.get("createdAt")
                      if started is None:
                          # Still provisioning. Leave it: an age we cannot
                          # measure is not an age we should act on.
                          continue

                      if started > cutoff:
                          continue

                      arn = task["taskArn"]
                      age_minutes = (
                          datetime.datetime.now(datetime.timezone.utc) - started
                      ).total_seconds() / 60

                      try:
                          ecs.stop_task(
                              cluster=CLUSTER,
                              task=arn,
                              reason=f"ephemera reaper: exceeded {MAX_LIFETIME_MINUTES}m lifetime cap",
                          )
                          stopped.append({"task": arn, "age_minutes": round(age_minutes, 1)})
                      except Exception as exc:  # noqa: BLE001
                          # One task we cannot stop must not abort the sweep -
                          # the others are still costing money.
                          failed.append({"task": arn, "error": str(exc)})

          # Printed as one line so CloudWatch Logs Insights can query it, and so
          # a reap is visible in the log even when nothing is watching metrics.
          result = {
              "scanned": scanned,
              "stopped": len(stopped),
              "failed": len(failed),
              "details": stopped,
              "errors": failed,
          }
          print(result)
          return result
    PYTHON
  }
}

resource "aws_lambda_function" "reaper" {
  function_name = "${local.name}-reaper"
  description   = "Infrastructure-level backstop: stops ECS tasks that outlive the lifetime cap, independently of the control plane."

  role    = aws_iam_role.reaper.arn
  handler = "index.handler"
  runtime = "python3.12"

  filename         = data.archive_file.reaper.output_path
  source_code_hash = data.archive_file.reaper.output_base64sha256

  # Generous enough to page through a large cluster, bounded so a wedged call
  # cannot run for fifteen minutes.
  timeout = 120

  memory_size   = 256
  architectures = ["arm64"]

  # One reaper at a time. It is idempotent, so an overlapping run is not a
  # correctness problem, but this is the backstop for a *cost* guarantee and an
  # unbounded fan-out of it calling ECS in a loop would be its own incident.
  reserved_concurrent_executions = 1

  # A reaper that fails silently is indistinguishable from a reaper that had
  # nothing to do, and the difference is "no tasks are leaking" versus "tasks
  # are leaking and nobody knows". Failed asynchronous invocations land here.
  dead_letter_config {
    target_arn = aws_sqs_queue.reaper_dlq.arn
  }

  tracing_config {
    mode = "Active"
  }

  environment {
    variables = {
      CLUSTER              = aws_ecs_cluster.main.name
      MAX_LIFETIME_MINUTES = var.max_job_lifetime_minutes
    }
  }
}

# Failures of the outermost cost guarantee, kept where an alarm can see them.
resource "aws_sqs_queue" "reaper_dlq" {
  name                      = "${local.name}-reaper-dlq"
  message_retention_seconds = 1209600 # 14 days, the maximum
  sqs_managed_sse_enabled   = true

  tags = { Name = "${local.name}-reaper-dlq" }
}

resource "aws_cloudwatch_log_group" "reaper" {
  name              = "/aws/lambda/${local.name}-reaper"
  retention_in_days = var.log_retention_days
}

# Every minute.
#
# The interval is a cost decision: at Fargate's per-second billing, a one-minute
# sweep bounds the overrun to roughly a minute of one task's cost, and the
# Lambda invocations are a rounding error against that. A five-minute sweep
# would save nothing worth having.
resource "aws_cloudwatch_event_rule" "reaper" {
  name                = "${local.name}-reaper"
  description         = "Sweep for overdue job tasks"
  schedule_expression = "rate(1 minute)"
}

resource "aws_cloudwatch_event_target" "reaper" {
  rule      = aws_cloudwatch_event_rule.reaper.name
  target_id = "reaper"
  arn       = aws_lambda_function.reaper.arn
}

resource "aws_lambda_permission" "reaper" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.reaper.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.reaper.arn
}

# A reaper that stops running is a reaper nobody notices has stopped, and the
# first sign would be the bill. Alarm on the absence of successful invocations
# rather than on errors alone.
resource "aws_cloudwatch_metric_alarm" "reaper_not_running" {
  alarm_name          = "${local.name}-reaper-not-running"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Invocations"
  namespace           = "AWS/Lambda"
  period              = 900
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "The lifetime reaper has not run in 15 minutes; tasks may be outliving their cap."

  # Missing data is the failure here, not the absence of one.
  treat_missing_data = "breaching"

  dimensions = {
    FunctionName = aws_lambda_function.reaper.function_name
  }
}

resource "aws_cloudwatch_metric_alarm" "reaper_errors" {
  alarm_name          = "${local.name}-reaper-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "The lifetime reaper is failing."
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.reaper.function_name
  }
}

# Cost visibility, so a leak shows up as an alert rather than as a surprise at
# the end of the month.
resource "aws_cloudwatch_metric_alarm" "running_task_count" {
  alarm_name          = "${local.name}-too-many-running-tasks"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "RunningTaskCount"
  namespace           = "ECS/ContainerInsights"
  period              = 300
  statistic           = "Maximum"
  threshold           = 100
  alarm_description   = "Sustained high task count. Either genuine load, or environments are not being reaped."
  treat_missing_data  = "notBreaching"

  dimensions = {
    ClusterName = aws_ecs_cluster.main.name
  }
}
