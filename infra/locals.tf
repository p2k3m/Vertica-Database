locals {
  project       = "vertica-db"
  name_prefix   = local.project
  sg_name       = "${local.project}-sg"
  role_name     = "${local.project}-ec2-role"
  profile_name  = "${local.project}-profile"
  instance_name = "${local.project}-host"
}
