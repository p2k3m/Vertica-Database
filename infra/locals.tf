locals {
  project               = "vertica-db"
  name_prefix           = local.project
  sg_name               = "${local.project}-sg"
  role_name             = "${local.project}-ec2-role"
  profile_name          = "${local.project}-profile"
  instance_name         = "${local.project}-host"
  vertica_port          = 5433
  bootstrap_admin_user  = "dbadmin"
  bootstrap_admin_pass  = ""
  additional_admin_user = var.additional_admin_username
  additional_admin_pass = trimspace(var.additional_admin_password) != "" ? var.additional_admin_password : random_password.additional_admin.result
  smoke_test_user       = var.smoke_test_username
  smoke_test_pass       = trimspace(var.smoke_test_password) != "" ? var.smoke_test_password : random_password.smoke_test.result
  vertica_user          = local.additional_admin_user
  vertica_pass          = local.additional_admin_pass
  vertica_db            = "VMart"
}
