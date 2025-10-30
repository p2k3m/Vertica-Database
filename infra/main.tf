terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

resource "random_password" "additional_admin" {
  length      = 20
  special     = false
  min_lower   = 2
  min_upper   = 2
  min_numeric = 2
}

resource "random_password" "smoke_test" {
  length      = 20
  special     = false
  min_lower   = 2
  min_upper   = 2
  min_numeric = 2
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

data "aws_ssm_parameter" "al2023" {
  # Resolve the Amazon Linux 2023 AMI via the public SSM Parameter so we always
  # receive an image with the Systems Manager agent baked in.
  name = var.ami_ssm_parameter_name
}

resource "aws_security_group" "db_sg" {
  name        = local.sg_name
  description = "Allow access to Vertica"
  vpc_id      = data.aws_vpc.default.id

  dynamic "ingress" {
    for_each = var.allowed_cidrs
    content {
      description = "Vertica SQL"
      from_port   = 5433
      to_port     = 5433
      protocol    = "tcp"
      cidr_blocks = [ingress.value]
    }
  }

  dynamic "ingress" {
    for_each = var.allowed_cidrs
    content {
      description = "ICMP ping"
      from_port   = -1
      to_port     = -1
      protocol    = "icmp"
      cidr_blocks = [ingress.value]
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
  name               = local.role_name
  assume_role_policy = data.aws_iam_policy_document.assume_ec2.json

  tags = {
    Project = local.project
  }
}

resource "aws_iam_instance_profile" "ec2_profile" {
  name = local.profile_name
  role = aws_iam_role.ec2_role.name
}

resource "aws_iam_role_policy_attachment" "ssm_core" {
  role       = aws_iam_role.ec2_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy_attachment" "cloudwatch_logs" {
  role       = aws_iam_role.ec2_role.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess"
}

resource "aws_iam_role_policy_attachment" "ecr_read_only" {
  role       = aws_iam_role.ec2_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
}

resource "aws_cloudwatch_log_group" "smoke_test" {
  name              = "/aws/vertica/${local.project}/smoke-test"
  retention_in_days = 30

  tags = {
    Project = local.project
  }
}

resource "aws_instance" "host" {
  ami                         = data.aws_ssm_parameter.al2023.value
  instance_type               = var.instance_type
  subnet_id                   = element(data.aws_subnets.default.ids, 0)
  vpc_security_group_ids      = [aws_security_group.db_sg.id]
  iam_instance_profile        = aws_iam_instance_profile.ec2_profile.name
  associate_public_ip_address = true

  dynamic "instance_market_options" {
    for_each = var.use_spot ? [1] : []
    content {
      market_type = "spot"
    }
  }

  user_data_base64 = base64encode(templatefile("${path.module}/user_data.sh", {
    vertica_image             = var.vertica_image,
    aws_account_id            = var.aws_account_id,
    aws_region                = var.aws_region,
    bootstrap_admin_username  = local.bootstrap_admin_user,
    bootstrap_admin_password  = local.bootstrap_admin_pass,
    additional_admin_username = local.additional_admin_user,
    additional_admin_password = local.additional_admin_pass,
    vertica_db_name           = local.vertica_db,
    vertica_port              = local.vertica_port
  }))
  user_data_replace_on_change = true

  root_block_device {
    volume_type           = "gp3"
    volume_size           = 50
    delete_on_termination = true
  }

  tags = {
    Name    = local.instance_name
    Project = local.project
    Role    = "db"
  }
}

resource "aws_ssm_document" "vertica_smoke_test" {
  name          = "${local.project}-vertica-smoke-test"
  document_type = "Command"
  target_type   = "/AWS::EC2::Instance"
  content = templatefile("${path.module}/ssm-smoke-test.json.tpl", {
    vertica_image                = var.vertica_image
    vertica_port                 = tostring(local.vertica_port)
    bootstrap_admin_username     = local.bootstrap_admin_user
    bootstrap_admin_password     = local.bootstrap_admin_pass
    additional_admin_username    = local.additional_admin_user
    additional_admin_password    = local.additional_admin_pass
    smoke_test_username          = local.smoke_test_user
    smoke_test_password          = local.smoke_test_pass
    vertica_db_name              = local.vertica_db
    vertica_python_wheel_b64     = trimspace(file("${path.module}/assets/vertica_python-1.4.0-py3-none-any.whl.b64"))
  })

  tags = {
    Project = local.project
  }
}

resource "aws_ssm_association" "vertica_smoke_test" {
  name = aws_ssm_document.vertica_smoke_test.name

  targets {
    key    = "InstanceIds"
    values = [aws_instance.host.id]
  }

  compliance_severity         = "HIGH"
  max_concurrency             = "1"
  max_errors                  = "1"
  apply_only_at_cron_interval = false

  depends_on = [aws_instance.host]
}
