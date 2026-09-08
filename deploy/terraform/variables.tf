variable "region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "eu-west-1"
}

variable "environment" {
  description = "Environment name, used in resource names and tags."
  type        = string
  default     = "dev"

  validation {
    condition     = can(regex("^[a-z0-9-]{2,20}$", var.environment))
    error_message = "environment must be 2-20 lowercase alphanumeric or hyphen characters."
  }
}

variable "cost_center" {
  description = "Cost allocation tag applied to every resource."
  type        = string
  default     = "platform"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC."
  type        = string
  default     = "10.42.0.0/16"
}

variable "availability_zone_count" {
  description = "Number of availability zones to spread across."
  type        = number
  default     = 2

  validation {
    # One AZ has no redundancy; beyond three, NAT gateway cost grows faster
    # than the availability it buys for this workload.
    condition     = var.availability_zone_count >= 2 && var.availability_zone_count <= 3
    error_message = "availability_zone_count must be 2 or 3."
  }
}

variable "control_plane_image" {
  description = "Container image for the control plane."
  type        = string
  default     = "airlock/control-plane:dev"
}

variable "agent_image" {
  description = "Container image agents run in."
  type        = string
  default     = "airlock/agent:dev"
}

variable "control_plane_cpu" {
  description = "Fargate CPU units for the control plane (1024 = 1 vCPU)."
  type        = number
  default     = 512
}

variable "control_plane_memory" {
  description = "Fargate memory in MiB for the control plane."
  type        = number
  default     = 1024
}

variable "job_cpu" {
  description = "Fargate CPU units for a job task."
  type        = number
  default     = 1024
}

# 2 GiB rather than the 512 MiB people reach for first: a headless Chrome is
# 300-700 MiB before it renders anything, and an OOM-killed browser is a
# confusing failure to debug.
variable "job_memory" {
  description = "Fargate memory in MiB for a job task. A headless browser needs more than people expect."
  type        = number
  default     = 2048
}

# The outermost reaper layer, enforced outside the control plane, so it holds
# even when the control plane is dead. This is the number an operator quotes
# when asked what the worst case is.
variable "max_job_lifetime_minutes" {
  description = "Absolute cap on how long any job task may live, enforced by the Lambda reaper."
  type        = number
  default     = 30

  validation {
    condition     = var.max_job_lifetime_minutes > 0 && var.max_job_lifetime_minutes <= 720
    error_message = "max_job_lifetime_minutes must be between 1 and 720."
  }
}

# Screenshots and video are the bulk of the bytes and stop being useful quickly.
variable "artifact_retention_days" {
  description = "How long job artifacts are kept before the lifecycle rule expires them."
  type        = number
  default     = 30
}

# Never zero: CloudWatch's default is to keep logs forever, which is a bill that
# grows without anyone deciding it should.
variable "log_retention_days" {
  description = "CloudWatch log retention in days."
  type        = number
  default     = 14
}

# Spot is roughly 70 percent cheaper and can be reclaimed with two minutes'
# notice. This workload tolerates that: jobs are short, and an interrupted job
# is already classified as retryable infrastructure failure rather than an agent
# bug.
variable "spot_weight" {
  description = "Share of job capacity served by Fargate Spot, against on-demand weight 1."
  type        = number
  default     = 4
}

# Turning this off makes the environment fully air-gapped: cheaper and safer,
# and useless for an agent whose job is to browse the web.
variable "enable_nat_gateway" {
  description = "Whether to create NAT gateways so job tasks can reach the internet."
  type        = bool
  default     = true
}

# Deliberately empty rather than 0.0.0.0/0. An open control plane is worse than
# an unreachable one, so a deployment that forgets this produces the safer of
# the two failures and the operator must state who may call it.
variable "allowed_api_cidrs" {
  description = "CIDRs permitted to reach the control plane API. Empty means nobody."
  type        = list(string)
  default     = []
}
