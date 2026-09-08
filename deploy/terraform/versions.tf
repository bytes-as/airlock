# Provider versions are pinned, not floated.
#
# `~>` on the minor version means a `terraform init` six months from now
# produces the same plan it does today for the same code. Infrastructure that
# changes because an unpinned provider changed is the kind of surprise nobody
# has time for.

terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.40"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }

  # Backend is deliberately not configured here.
  #
  # State location is an environment decision, not a code one: the same
  # configuration is applied to dev and prod with different backends. Hardcoding
  # an S3 bucket would also make `terraform init -backend=false` - which is how
  # this is validated in CI without credentials - the odd path out rather than
  # the normal one.
  #
  # Configure it at init time:
  #   terraform init -backend-config=env/prod.backend.hcl
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project     = "airlock"
      Environment = var.environment
      ManagedBy   = "terraform"
      # Cost allocation is not an afterthought here: this system's whole premise
      # is that ephemeral compute is expensive when it leaks, and you cannot
      # find a leak you are not tagging for.
      CostCenter = var.cost_center
    }
  }
}
