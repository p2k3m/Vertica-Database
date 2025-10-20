variable "aws_region" {
  type    = string
  default = "ap-south-1"
}

variable "aws_account_id" {
  type = string
}

variable "allowed_cidrs" {
  type    = list(string)
  default = ["0.0.0.0/0"]
}

variable "instance_type" {
  type    = string
  default = "t3.xlarge"
}

variable "use_spot" {
  type    = bool
  default = true
}

variable "ami_ssm_parameter_name" {
  type        = string
  description = "SSM public parameter that resolves to the base AMI ID used for the Vertica host"
  default     = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

variable "vertica_image" {
  type        = string
  description = "Docker image for the Vertica deployment"
  default     = "957650740525.dkr.ecr.ap-south-1.amazonaws.com/vertica-ce:v1.0"
}

variable "additional_admin_username" {
  type        = string
  default     = "appadmin"
  description = "Username for the additional Vertica administrator account"
}

variable "additional_admin_password" {
  type        = string
  default     = ""
  description = "Optional password for the additional Vertica administrator. Leave blank to auto-generate."
  sensitive   = true
}

variable "smoke_test_username" {
  type        = string
  default     = "smoketester"
  description = "Username created during the SSM smoke test to validate connectivity"
}

variable "smoke_test_password" {
  type        = string
  default     = ""
  description = "Optional password used during the SSM smoke test. Leave blank to auto-generate."
  sensitive   = true
}
