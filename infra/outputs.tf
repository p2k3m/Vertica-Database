output "public_ip" {
  description = "Public IP address of the Vertica host"
  value       = aws_instance.host.public_ip
}

output "public_dns" {
  description = "Public DNS name of the Vertica host"
  value       = aws_instance.host.public_dns
}

locals {
  connection_host = aws_instance.host.public_dns != "" ? aws_instance.host.public_dns : aws_instance.host.public_ip
  connection_auth = local.vertica_pass == "" ? local.vertica_user : "${local.vertica_user}:${local.vertica_pass}"
}

output "sg_id" {
  description = "Security group ID allowing access to Vertica"
  value       = aws_security_group.db_sg.id
}

output "instance_id" {
  description = "Instance ID hosting Vertica"
  value       = aws_instance.host.id
}

output "connection_details" {
  description = "Connection information for the Vertica service"
  value = {
    connection_url = "vertica://${local.connection_auth}@${local.connection_host}:${local.vertica_port}/${local.vertica_db}"
    public_ip      = aws_instance.host.public_ip
    public_dns     = aws_instance.host.public_dns
    host           = local.connection_host
    port           = local.vertica_port
    username       = local.vertica_user
    password       = local.vertica_pass
    database       = local.vertica_db
    bootstrap_admin_username  = local.bootstrap_admin_user
    bootstrap_admin_password  = local.bootstrap_admin_pass
    additional_admin_username = local.additional_admin_user
    additional_admin_password = local.additional_admin_pass
  }
}

output "additional_admin_username" {
  description = "Username for the additional Vertica administrator"
  value       = local.additional_admin_user
}

output "additional_admin_password" {
  description = "Password for the additional Vertica administrator"
  value       = local.additional_admin_pass
  sensitive   = true
}
