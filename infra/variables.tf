variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "ap-south-1"
}

variable "aws_account_id" {
  description = "AWS account ID that hosts the Vertica container image"
  type        = string
}

variable "key_name" {
  description = "Optional EC2 key pair to attach to the instance"
  type        = string
  default     = null
}

variable "instance_type" {
  description = "EC2 instance type to provision"
  type        = string
  default     = "t3.xlarge"
}

variable "allowed_cidrs" {
  description = "CIDR blocks that are allowed to reach Vertica and MCP ports"
  type        = list(string)
  default     = [
    "0.0.0.0/0",
  ]
}

variable "vertica_image" {
  description = "Full image URI for the Vertica CE container"
  type        = string
  default     = "957650740525.dkr.ecr.ap-south-1.amazonaws.com/vertica-ce:v1.0"
}
