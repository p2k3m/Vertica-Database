output "public_ip" {
  description = "Public IP address of the Vertica host"
  value       = aws_instance.host.public_ip
}

output "public_dns" {
  description = "Public DNS name of the Vertica host"
  value       = aws_instance.host.public_dns
}

locals {
  connection_host     = aws_instance.host.public_dns != "" ? aws_instance.host.public_dns : aws_instance.host.public_ip
  connection_username = local.vertica_user
  connection_password = local.vertica_pass == "" ? "" : nonsensitive(local.vertica_pass)
  connection_url      = local.connection_password == "" ? "vertica://${local.connection_username}@${local.connection_host}:${local.vertica_port}/${local.vertica_db}" : "vertica://${local.connection_username}:${local.connection_password}@${local.connection_host}:${local.vertica_port}/${local.vertica_db}"
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
    connection_url            = local.connection_url
    public_ip                 = aws_instance.host.public_ip
    public_dns                = aws_instance.host.public_dns
    host                      = local.connection_host
    port                      = local.vertica_port
    username                  = local.connection_username
    password                  = local.connection_password
    database                  = local.vertica_db
    bootstrap_admin_username  = local.bootstrap_admin_user
    bootstrap_admin_password  = local.bootstrap_admin_pass
    additional_admin_username = local.additional_admin_user
    additional_admin_password = local.connection_password
  }
  sensitive = true
}

output "connection_instructions" {
  description = "Human-readable instructions for connecting to Vertica from any location"
  value = join(
    "\n",
    [
      format(
        "Connect to the Vertica database using either DNS (%s) or the public IP address (%s).",
        local.connection_host,
        aws_instance.host.public_ip,
      ),
      "",
      format("Connection URL: %s", local.connection_url),
      "",
      "CLI example using vsql:",
      format(
        "  vsql -h %s -p %d -d %s -U %s -w %s",
        local.connection_host,
        local.vertica_port,
        local.vertica_db,
        local.connection_username,
        local.connection_password,
      ),
      "",
      format(
        "The database listens on TCP port %d and allows ingress from the configured CIDR blocks (defaults to 0.0.0.0/0).",
        local.vertica_port,
      ),
    ],
  )
  sensitive = true
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
