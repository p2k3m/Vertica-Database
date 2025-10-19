output "public_ip" {
  description = "Public IP address of the Vertica host"
  value       = aws_instance.host.public_ip
}

output "public_dns" {
  description = "Public DNS name of the Vertica host"
  value       = aws_instance.host.public_dns
}

output "sg_id" {
  description = "Security group ID allowing access to Vertica"
  value       = aws_security_group.db_sg.id
}

output "instance_id" {
  description = "Instance ID hosting Vertica"
  value       = aws_instance.host.id
}
