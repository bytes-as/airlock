# Container registries for the two images this system runs.
#
# These exist because without them the deployment has a hole in the middle: the
# task definitions reference `var.control_plane_image` and `var.agent_image`,
# and until this file was added there was nowhere in the account for those
# images to live. Following the runbook, you would `terraform apply`
# successfully and then discover you had infrastructure pointing at images that
# did not exist anywhere you could push to.
#
# Two repositories rather than one, because they have genuinely different
# lifecycles and blast radii: the control plane holds the Docker socket
# equivalent (an ECS task role that can run tasks), and the agent runs untrusted
# payloads. Keeping them separate means an image tag policy or a scan finding on
# one does not force a decision about the other.

resource "aws_ecr_repository" "control_plane" {
  name = "${local.name}-control-plane"

  # Scan on push rather than on a schedule: the useful moment to learn an image
  # has a critical CVE is before it is deployed, not a day later.
  image_scanning_configuration {
    scan_on_push = true
  }

  # IMMUTABLE means a tag, once pushed, always refers to the same image. This is
  # the difference between "we deployed v3" being a fact and being a guess: with
  # mutable tags, `:v3` can be overwritten and the running task no longer
  # matches the thing that was reviewed.
  image_tag_mutability = "IMMUTABLE"

  encryption_configuration {
    encryption_type = "AES256"
  }

  tags = { Name = "${local.name}-control-plane" }
}

resource "aws_ecr_repository" "agent" {
  name = "${local.name}-agent"

  image_scanning_configuration {
    scan_on_push = true
  }

  image_tag_mutability = "IMMUTABLE"

  encryption_configuration {
    encryption_type = "AES256"
  }

  tags = { Name = "${local.name}-agent" }
}

# Untagged images are build residue: every push that replaces a tag orphans the
# previous manifest, and orphaned manifests are pure storage cost with no way to
# deploy them. Expiring them keeps the bill honest without touching anything
# that can still be rolled back to.
resource "aws_ecr_lifecycle_policy" "control_plane" {
  repository = aws_ecr_repository.control_plane.name
  policy     = local.ecr_lifecycle_policy
}

resource "aws_ecr_lifecycle_policy" "agent" {
  repository = aws_ecr_repository.agent.name
  policy     = local.ecr_lifecycle_policy
}

locals {
  ecr_lifecycle_policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Expire untagged images after 7 days"
        selection = {
          tagStatus   = "untagged"
          countType   = "sinceImagePushed"
          countUnit   = "days"
          countNumber = 7
        }
        action = { type = "expire" }
      },
      {
        rulePriority = 2
        description  = "Keep the 20 most recent tagged images"
        selection = {
          tagStatus   = "any"
          countType   = "imageCountMoreThan"
          countNumber = 20
        }
        action = { type = "expire" }
      },
    ]
  })
}
