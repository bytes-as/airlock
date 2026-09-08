# Artifacts, queue and secrets.

# --- artifacts ---

resource "aws_s3_bucket" "artifacts" {
  bucket_prefix = "${local.name}-artifacts-"

  # Not force_destroy. `terraform destroy` should fail on a bucket holding
  # objects rather than silently deleting evidence someone may still need.
  force_destroy = false
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id

  # All four, explicitly. Artifacts are screenshots of whatever the agent was
  # looking at, which is exactly the category of data that should never become
  # publicly readable by accident. Access is via presigned URLs only.
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    # S3 charges per KMS request; for this volume of small objects, SSE-S3 is
    # the right trade unless a compliance regime requires a customer-managed
    # key, in which case switch this to aws:kms and accept the cost.
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_versioning" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id

  versioning_configuration {
    # Off deliberately. Artifacts are write-once and never updated, so versions
    # would only accumulate cost. The lifecycle rule below assumes this.
    status = "Disabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id

  rule {
    id     = "expire-artifacts"
    status = "Enabled"

    filter {}

    # Cost control. Screenshots and video are the bulk of the bytes and stop
    # being useful quickly, so retention is enforced by the platform rather than
    # left to whoever remembers to clean up.
    expiration {
      days = var.artifact_retention_days
    }

    # Incomplete uploads are invisible in the console and still billed. This is
    # the classic S3 bill nobody can explain.
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

# --- queue ---
#
# The scale ladder's next rung. The shipped control plane uses an embedded bbolt
# queue, which caps it at one host. SQS is provisioned here so the migration is
# a driver swap rather than an infrastructure project.

resource "aws_sqs_queue" "jobs_dlq" {
  name                      = "${local.name}-jobs-dlq"
  message_retention_seconds = 1209600 # 14 days, the maximum

  sqs_managed_sse_enabled = true
}

resource "aws_sqs_queue" "jobs" {
  name = "${local.name}-jobs"

  # Visibility timeout must exceed the longest a job can take, or SQS
  # redelivers a message whose job is still running and the work happens twice.
  # Derived from the lifetime cap rather than hardcoded, so the two cannot drift
  # apart when someone raises the cap.
  visibility_timeout_seconds = var.max_job_lifetime_minutes * 60 + 60

  message_retention_seconds = 345600 # 4 days
  sqs_managed_sse_enabled   = true

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.jobs_dlq.arn
    # Three attempts, matching the control plane's default retry budget. A
    # message that fails repeatedly is a poison pill; moving it aside keeps one
    # bad job from blocking the queue behind it.
    maxReceiveCount = 3
  })
}

# A DLQ nobody watches is a DLQ that silently swallows work.
resource "aws_cloudwatch_metric_alarm" "dlq_not_empty" {
  alarm_name          = "${local.name}-dlq-not-empty"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Maximum"
  threshold           = 0
  alarm_description   = "Jobs have failed repeatedly and landed in the dead letter queue."
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.jobs_dlq.name
  }
}

# Queue age, not depth. Depth can look healthy while the oldest job starves;
# age is the number that reveals a system falling behind.
resource "aws_cloudwatch_metric_alarm" "queue_age" {
  alarm_name          = "${local.name}-queue-backlog"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "ApproximateAgeOfOldestMessage"
  namespace           = "AWS/SQS"
  period              = 60
  statistic           = "Maximum"
  threshold           = 900 # 15 minutes
  alarm_description   = "The oldest queued job has been waiting over 15 minutes; the system is not keeping up."
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.jobs.name
  }
}

# --- secrets ---

resource "aws_secretsmanager_secret" "signing_key" {
  name_prefix = "${local.name}-signing-key-"
  description = "HMAC key for presigned artifact URLs. Rotating it invalidates every outstanding link."

  # Long enough to recover from an accidental delete, short enough not to block
  # recreating the stack for a fortnight.
  recovery_window_in_days = 7
}

# Terraform creates the secret but not its value.
#
# A random_password resource would put the key in the state file, and state is
# far more widely readable than Secrets Manager. So the container gets the
# secret injected by the ECS agent, and the value is set out of band:
#
#   aws secretsmanager put-secret-value \
#     --secret-id <arn> --secret-string "$(openssl rand -base64 32)"
#
# Until it is set, the control plane generates an ephemeral key and warns that
# artifact links will not survive a restart.
