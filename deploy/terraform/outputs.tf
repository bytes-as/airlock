output "cluster_name" {
  description = "ECS cluster running the control plane and job tasks."
  value       = aws_ecs_cluster.main.name
}

output "artifact_bucket" {
  description = "S3 bucket holding job artifacts."
  value       = aws_s3_bucket.artifacts.id
}

output "queue_url" {
  description = "SQS queue URL for the distributed dispatch path."
  value       = aws_sqs_queue.jobs.url
}

output "dead_letter_queue_url" {
  description = "Dead letter queue. Messages here are jobs that failed repeatedly."
  value       = aws_sqs_queue.jobs_dlq.url
}

output "job_security_group_id" {
  description = "Security group applied to job tasks."
  value       = aws_security_group.job.id
}

output "private_subnet_ids" {
  description = "Private subnets job tasks run in."
  value       = aws_subnet.private[*].id
}

output "signing_key_secret_arn" {
  description = "Secrets Manager ARN for the artifact URL signing key. Terraform creates the secret but not its value; set it with `aws secretsmanager put-secret-value`."
  value       = aws_secretsmanager_secret.signing_key.arn
}

output "job_task_role_arn" {
  description = "Role assumed by agent code. Intentionally has no permissions."
  value       = aws_iam_role.job_task.arn
}

output "reaper_function_name" {
  description = "Lambda enforcing the lifetime cap independently of the control plane."
  value       = aws_lambda_function.reaper.function_name
}

output "max_job_lifetime_minutes" {
  description = "The worst-case guarantee: no job task in this cluster outlives this, whatever else is broken."
  value       = var.max_job_lifetime_minutes
}

# A deliberately blunt summary of what this configuration has and has not been
# through, printed on every apply. Overstating verification is the fastest way
# to lose a reviewer's trust, so the code says it out loud.
output "verification_status" {
  description = "What has actually been verified about this configuration."
  value       = <<-EOT
    This Terraform is statically validated (fmt, validate, tflint, checkov) in CI.
    It has NOT been applied to a real AWS account - the author had none available.
    Expect to fix things on first apply. See docs/ARCHITECTURE.md for the full
    list of what is verified versus what is written but unproven.
  EOT
}
