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

variable "vertica_image" {
  type    = string
  default = "957650740525.dkr.ecr.ap-south-1.amazonaws.com/vertica-ce:v1.0"
}
