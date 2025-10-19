terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

locals {
  project = "vertica-db"
  ingress_rules = {
    vertica = {
      description = "Vertica SQL"
      port        = 5433
    }
    mcp = {
      description = "MCP HTTP/SSE"
      port        = 8000
    }
  }
}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["137112412989"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }
}

resource "aws_security_group" "db_sg" {
  name        = "${local.project}-sg"
  description = "Allow access to Vertica and MCP ports"
  vpc_id      = data.aws_vpc.default.id

  dynamic "ingress" {
    for_each = var.allowed_cidrs
    content {
      description = local.ingress_rules.vertica.description
      from_port   = local.ingress_rules.vertica.port
      to_port     = local.ingress_rules.vertica.port
      protocol    = "tcp"
      cidr_blocks = [ingress.value]
    }
  }

  dynamic "ingress" {
    for_each = var.allowed_cidrs
    iterator = cidr
    content {
      description = local.ingress_rules.mcp.description
      from_port   = local.ingress_rules.mcp.port
      to_port     = local.ingress_rules.mcp.port
      protocol    = "tcp"
      cidr_blocks = [cidr.value]
    }
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Project = local.project
  }
}

data "aws_iam_policy_document" "assume_ec2" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ec2_role" {
  name               = "${local.project}-ec2-role"
  assume_role_policy = data.aws_iam_policy_document.assume_ec2.json

  tags = {
    Project = local.project
  }
}

resource "aws_iam_role_policy" "ecr_ssm" {
  name = "${local.project}-ecr-ssm"
  role = aws_iam_role.ec2_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["ecr:GetAuthorizationToken", "ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["ssm:*", "ec2messages:*", "ssmmessages:*"]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_instance_profile" "ec2_profile" {
  name = "${local.project}-profile"
  role = aws_iam_role.ec2_role.name
}

locals {
  compose_yaml = templatefile("${path.module}/templates/compose.remote.yml.tmpl", {
    vertica_image = var.vertica_image
  })
}

resource "aws_instance" "host" {
  ami                    = data.aws_ami.al2023.id
  instance_type          = var.instance_type
  subnet_id              = element(data.aws_subnets.default.ids, 0)
  vpc_security_group_ids = [aws_security_group.db_sg.id]
  iam_instance_profile   = aws_iam_instance_profile.ec2_profile.name
  key_name               = var.key_name
  associate_public_ip_address = true

  instance_market_options {
    market_type = "spot"
  }

  metadata_options {
    http_tokens = "required"
  }

  user_data = templatefile("${path.module}/user_data.sh", {
    aws_account_id = var.aws_account_id
    vertica_image  = var.vertica_image
    aws_region     = var.aws_region
    compose_file   = local.compose_yaml
  })

  root_block_device {
    volume_size = 20
    volume_type = "gp3"
  }

  ebs_block_device {
    device_name           = "/dev/sdh"
    volume_size           = 50
    volume_type           = "gp3"
    delete_on_termination = true
  }

  tags = {
    Project = local.project
    Role    = "db-and-mcp"
  }
}
