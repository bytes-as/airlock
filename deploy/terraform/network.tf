# Network layout, and the egress story in AWS terms.
#
# The question this file answers: how does the agent get internet access without
# reaching instance metadata or other VPC resources? On Fargate the honest answer
# has three parts, and only one of them is a firewall rule.
#
# 1. Job tasks run in private subnets. They have no public IP and no inbound
#    route from the internet; outbound goes through a NAT gateway.
#
# 2. Security groups are stateful and allow-only. The job security group permits
#    outbound 80/443 and DNS, and nothing else - so the RDS instance and the
#    internal service on port 8080 next door are unreachable, because they were
#    never allowed rather than because a deny rule caught them.
#
# 3. The metadata endpoint is the interesting case, and it cannot be solved with
#    a security group: 169.254.170.2 is link-local, so it never traverses a
#    subnet route table, a NACL or a security group. There is no network control
#    that blocks it on Fargate.
#
#    The mitigation is therefore not network at all - it is IAM. See iam.tf: the
#    job task role has *no permissions*. An agent that reads its own credentials
#    from the metadata endpoint gets a set of credentials that can do nothing.
#    That is the correct answer for Fargate, and pretending a security group
#    solves it would be a wrong answer that looks right.

data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  name = "ephemera-${var.environment}"
  azs  = slice(data.aws_availability_zones.available.names, 0, var.availability_zone_count)

  # /20 public and /20 private per AZ out of a /16. Generous for the subnet
  # count, because running out of addresses mid-incident is a bad afternoon and
  # resizing a subnet means recreating it.
  public_subnets  = [for i, az in local.azs : cidrsubnet(var.vpc_cidr, 4, i)]
  private_subnets = [for i, az in local.azs : cidrsubnet(var.vpc_cidr, 4, i + 8)]
}

resource "aws_vpc" "main" {
  cidr_block = var.vpc_cidr

  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = local.name }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = local.name }
}

resource "aws_subnet" "public" {
  count = length(local.public_subnets)

  vpc_id                  = aws_vpc.main.id
  cidr_block              = local.public_subnets[count.index]
  availability_zone       = local.azs[count.index]
  map_public_ip_on_launch = false # Only the NAT gateways need public IPs.

  tags = {
    Name = "${local.name}-public-${local.azs[count.index]}"
    Tier = "public"
  }
}

resource "aws_subnet" "private" {
  count = length(local.private_subnets)

  vpc_id            = aws_vpc.main.id
  cidr_block        = local.private_subnets[count.index]
  availability_zone = local.azs[count.index]

  tags = {
    Name = "${local.name}-private-${local.azs[count.index]}"
    Tier = "private"
  }
}

# One NAT gateway per AZ.
#
# A single shared NAT would be cheaper (~$32/month each) but makes one AZ a
# single point of failure for every job's egress, which defeats the point of
# spreading across AZs at all. Cost control belongs in job lifetime and Spot
# usage, not in removing the redundancy the multi-AZ layout exists for.
resource "aws_eip" "nat" {
  count = var.enable_nat_gateway ? length(local.azs) : 0

  domain = "vpc"
  tags   = { Name = "${local.name}-nat-${local.azs[count.index]}" }
}

resource "aws_nat_gateway" "main" {
  count = var.enable_nat_gateway ? length(local.azs) : 0

  allocation_id = aws_eip.nat[count.index].id
  subnet_id     = aws_subnet.public[count.index].id

  tags = { Name = "${local.name}-${local.azs[count.index]}" }

  depends_on = [aws_internet_gateway.main]
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = { Name = "${local.name}-public" }
}

resource "aws_route_table_association" "public" {
  count = length(aws_subnet.public)

  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

# One private route table per AZ, so each AZ egresses through its own NAT.
resource "aws_route_table" "private" {
  count = length(local.azs)

  vpc_id = aws_vpc.main.id
  tags   = { Name = "${local.name}-private-${local.azs[count.index]}" }
}

resource "aws_route" "private_nat" {
  count = var.enable_nat_gateway ? length(local.azs) : 0

  route_table_id         = aws_route_table.private[count.index].id
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.main[count.index].id
}

resource "aws_route_table_association" "private" {
  count = length(aws_subnet.private)

  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private[count.index].id
}

# --- security groups ---

# The job security group is the network half of tenant isolation.
#
# Egress is restricted to what a browsing agent genuinely needs. Note what is
# absent: no rule permits the VPC CIDR, so a job cannot reach the control plane,
# a database, or another tenant's task. Those are unreachable because they were
# never allowed, which is a stronger position than a deny rule someone has to
# remember to maintain.
resource "aws_security_group" "job" {
  name        = "${local.name}-job"
  description = "Job tasks: outbound web only, no inbound, no VPC-internal access"
  vpc_id      = aws_vpc.main.id

  tags = { Name = "${local.name}-job" }
}

# No inbound rules at all. A job task is not a server; nothing should ever
# initiate a connection to it. Security groups are stateful, so replies to the
# task's own outbound connections still work.

resource "aws_vpc_security_group_egress_rule" "job_https" {
  security_group_id = aws_security_group.job.id
  description       = "HTTPS to the internet"
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = 443
  to_port           = 443
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "job_http" {
  security_group_id = aws_security_group.job.id
  description       = "HTTP to the internet"
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = 80
  to_port           = 80
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "job_dns_udp" {
  security_group_id = aws_security_group.job.id
  description       = "DNS"
  cidr_ipv4         = var.vpc_cidr # the VPC resolver only, not arbitrary DNS
  from_port         = 53
  to_port           = 53
  ip_protocol       = "udp"
}

# --- control plane ---

resource "aws_security_group" "control_plane" {
  name        = "${local.name}-control-plane"
  description = "Control plane API"
  vpc_id      = aws_vpc.main.id

  tags = { Name = "${local.name}-control-plane" }
}

# Inbound only from explicitly named CIDRs. The variable defaults to an empty
# list, so a deployment that forgets to set it produces an unreachable API
# rather than an open one - the safer of the two failures.
resource "aws_vpc_security_group_ingress_rule" "control_plane_api" {
  count = length(var.allowed_api_cidrs)

  security_group_id = aws_security_group.control_plane.id
  description       = "API access"
  cidr_ipv4         = var.allowed_api_cidrs[count.index]
  from_port         = 8080
  to_port           = 8080
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "control_plane_https" {
  security_group_id = aws_security_group.control_plane.id
  description       = "AWS APIs, ECR, S3, SQS"
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = 443
  to_port           = 443
  ip_protocol       = "tcp"
}

# --- VPC endpoints ---
#
# S3 traffic goes over a gateway endpoint rather than the NAT gateway. This is
# both a cost and a security decision: artifact upload is the bulk of this
# system's bytes, NAT charges per gigabyte processed, and traffic that never
# leaves the AWS network never traverses the internet at all.
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = aws_route_table.private[*].id

  tags = { Name = "${local.name}-s3" }
}
