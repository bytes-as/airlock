# IAM, and the actual answer to "how does the agent not reach your metadata".
#
# The key idea: ECS gives a task two distinct roles, and conflating them is the
# most common way a sandbox leaks.
#
#   execution role - used by the ECS *agent*, not by your code. Pulls the image,
#                    writes the log stream. The container never holds these
#                    credentials.
#
#   task role      - assumed by the code inside the container, and reachable
#                    from it at 169.254.170.2. Anything this role can do, a
#                    compromised agent can do.
#
# So the job task role is empty. Not "narrow" - empty. An agent that reads the
# metadata endpoint gets credentials that can do nothing at all, which makes the
# unblockable link-local endpoint a non-issue rather than a hole to apologise
# for. Artifacts move through the control plane, which has its own role, rather
# than being uploaded by the job.
#
# This costs one hop for artifacts. It buys the property that a fully
# compromised agent has no AWS permissions whatsoever.

data "aws_caller_identity" "current" {}

# --- execution role: used by ECS itself, never by job code ---

data "aws_iam_policy_document" "ecs_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }

    # Confused-deputy guard: without these, any ECS task in any account that
    # somehow referenced this role could assume it.
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_iam_role" "execution" {
  name               = "${local.name}-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
  description        = "Used by the ECS agent to pull images and write logs. Never held by container code."
}

resource "aws_iam_role_policy_attachment" "execution" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# --- job task role: deliberately powerless ---

resource "aws_iam_role" "job_task" {
  name               = "${local.name}-job-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json

  description = "Role assumed by agent code. Intentionally has NO permissions: a compromised agent that reads the task metadata endpoint gains nothing."
}

# An explicit deny-all rather than simply attaching nothing.
#
# Attaching nothing has the same effect today, but says nothing about intent -
# it reads like an oversight, and the next person to touch this file may
# "helpfully" attach S3 access. An explicit deny documents the decision, and
# anything added later must consciously contend with it.
data "aws_iam_policy_document" "job_deny_all" {
  statement {
    sid       = "DenyEverything"
    effect    = "Deny"
    actions   = ["*"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "job_deny_all" {
  name   = "deny-all"
  role   = aws_iam_role.job_task.id
  policy = data.aws_iam_policy_document.job_deny_all.json
}

# --- control plane role: narrow, but not empty ---

resource "aws_iam_role" "control_plane" {
  name               = "${local.name}-control-plane"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
  description        = "Control plane: runs and stops job tasks, stores artifacts, reads the queue."
}

data "aws_iam_policy_document" "control_plane" {
  # Run job tasks - and only the job task definition, not anything in the
  # cluster. Without the resource constraint this is "run any container you
  # like on my compute", which is an expensive thing to hand out.
  statement {
    sid    = "RunJobTasks"
    effect = "Allow"
    actions = [
      "ecs:RunTask",
      "ecs:StopTask",
      "ecs:DescribeTasks",
      "ecs:ListTasks",
    ]
    resources = [
      "${replace(aws_ecs_task_definition.job.arn, "/:\\d+$/", "")}:*",
      "arn:aws:ecs:${var.region}:${data.aws_caller_identity.current.account_id}:task/${aws_ecs_cluster.main.name}/*",
    ]

    condition {
      test     = "ArnEquals"
      variable = "ecs:cluster"
      values   = [aws_ecs_cluster.main.arn]
    }
  }

  # Handing the two task roles to a task is a privileged act in its own right:
  # PassRole without a constraint means "pass any role", which is a privilege
  # escalation path straight to admin.
  statement {
    sid       = "PassTaskRoles"
    effect    = "Allow"
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.job_task.arn, aws_iam_role.execution.arn]

    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["ecs-tasks.amazonaws.com"]
    }
  }

  # Artifacts: write and read within the bucket, but no permission to delete or
  # to alter the bucket itself. Retention is the lifecycle policy's job, so the
  # control plane has no reason to be able to erase evidence.
  statement {
    sid    = "Artifacts"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:GetObject",
      "s3:ListBucket",
    ]
    resources = [
      aws_s3_bucket.artifacts.arn,
      "${aws_s3_bucket.artifacts.arn}/*",
    ]
  }

  statement {
    sid    = "Queue"
    effect = "Allow"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:SendMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
      "sqs:ChangeMessageVisibility",
    ]
    resources = [aws_sqs_queue.jobs.arn]
  }

  statement {
    sid       = "Logs"
    effect    = "Allow"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogStreams"]
    resources = ["${aws_cloudwatch_log_group.jobs.arn}:*"]
  }
}

resource "aws_iam_role_policy" "control_plane" {
  name   = "control-plane"
  role   = aws_iam_role.control_plane.id
  policy = data.aws_iam_policy_document.control_plane.json
}

# --- reaper role: the outermost layer needs its own identity ---
#
# The reaper Lambda does not reuse the control plane's role. It exists to clean
# up after the control plane has failed, so sharing an identity with the thing
# it is backstopping would be a strange dependency to build in.

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "reaper" {
  name               = "${local.name}-reaper"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
  description        = "Infrastructure-level reaper. Stops overdue tasks even when the control plane is dead."
}

data "aws_iam_policy_document" "reaper" {
  # List and stop, but never run. The reaper's entire authority is to end
  # things; it has no business starting them, and a bug in it should not be able
  # to cost money.
  statement {
    sid       = "FindAndStopOverdueTasks"
    effect    = "Allow"
    actions   = ["ecs:ListTasks", "ecs:DescribeTasks", "ecs:StopTask"]
    resources = ["*"]

    condition {
      test     = "ArnEquals"
      variable = "ecs:cluster"
      values   = [aws_ecs_cluster.main.arn]
    }
  }

  statement {
    sid       = "Logs"
    effect    = "Allow"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:${var.region}:${data.aws_caller_identity.current.account_id}:*"]
  }
}

resource "aws_iam_role_policy" "reaper" {
  name   = "reaper"
  role   = aws_iam_role.reaper.id
  policy = data.aws_iam_policy_document.reaper.json
}
