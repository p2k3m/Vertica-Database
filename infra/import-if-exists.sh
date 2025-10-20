#!/usr/bin/env bash
set -euo pipefail
# Import resources that already exist (same names/tags) to avoid duplicates.

# Terraform prompts for unset variables by default which causes this helper script
# to hang in non-interactive environments (for example GitHub Actions) while it
# waits for values such as `var.aws_account_id`.  Explicitly disable interactive
# input for all Terraform commands via the TF_INPUT environment variable.  This
# works for every Terraform subcommand (including `import`, which does not accept
# an `-input` flag) without relying on per-command CLI flags.
export TF_INPUT=0

tf(){ terraform "$@"; }

get_json(){ aws "$@" --output json; }
exists_in_state(){ tf state list | grep -q "$1"; }

# SG by name
SG_NAME="vertica-db-sg"
SG_ID=$(aws ec2 describe-security-groups --filters Name=group-name,Values="$SG_NAME" --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || true)
if [ "$SG_ID" != "None" ] && [ -n "$SG_ID" ] && ! exists_in_state aws_security_group.db_sg; then
  tf import aws_security_group.db_sg "$SG_ID" || true
fi

# IAM role/profile by name
ROLE_NAME="vertica-db-ec2-role"
ROLE_ARN=$(aws iam get-role --role-name "$ROLE_NAME" --query 'Role.Arn' --output text 2>/dev/null || true)
if [ -n "$ROLE_ARN" ] && ! exists_in_state aws_iam_role.ec2_role; then
  tf import aws_iam_role.ec2_role "$ROLE_NAME" || true
fi
PROFILE_NAME="vertica-db-profile"
PROFILE_NAME_OUT=$(aws iam get-instance-profile --instance-profile-name "$PROFILE_NAME" --query 'InstanceProfile.InstanceProfileName' --output text 2>/dev/null || true)
if [ -n "$PROFILE_NAME_OUT" ] && ! exists_in_state aws_iam_instance_profile.ec2_profile; then
  tf import aws_iam_instance_profile.ec2_profile "$PROFILE_NAME" || true
fi

# EC2 by tag Name=vertica-db-host
IID=$(aws ec2 describe-instances --filters "Name=tag:Name,Values=vertica-db-host" "Name=instance-state-name,Values=pending,running,stopping,stopped" --query 'Reservations[0].Instances[0].InstanceId' --output text 2>/dev/null || true)
if [ "$IID" != "None" ] && [ -n "$IID" ] && ! exists_in_state aws_instance.host; then
  tf import aws_instance.host "$IID" || true
fi
