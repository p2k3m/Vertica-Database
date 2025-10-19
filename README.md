# Vertica-Database

Provision a Vertica Community Edition instance on AWS in minutes. Terraform spins up a Spot t3.xlarge instance in the default VPC, applies a hardened security group, formats a 50 GB gp3 data volume, and launches Vertica via Docker Compose. GitHub Actions workflows orchestrate deploy/destroy runs and execute a smoke test using vertica-python to confirm the database is reachable.

## Repository layout

```
infra/                  # Terraform, user-data template, and Compose template
  backend-bootstrap.sh  # Creates S3 bucket + DynamoDB table for TF remote state
  main.tf               # Core infrastructure (SG, IAM, EC2, EBS, user data)
  outputs.tf            # Terraform outputs
  user_data.sh          # cloud-init script rendered with templatefile
  templates/
    compose.remote.yml.tmpl
.github/workflows/      # GitHub Actions pipelines (apply/destroy)
tests/                  # pytest integration test and helper
```

## Prerequisites

1. Create the following repository secrets:
   - `AWS_ACCESS_KEY_ID`
   - `AWS_SECRET_ACCESS_KEY`
   - `AWS_REGION` (defaults to `ap-south-1` in Terraform variables)
   - `AWS_ACCOUNT_ID` (ECR registry that hosts the Vertica image)
2. (Optional) Define a repository variable `VERTICA_IMAGE` with an alternative image URI.
3. Ensure the IAM user backing the secrets can manage EC2, VPC, SSM, ECR, S3, and DynamoDB in the chosen region.

## Deploying Vertica via GitHub Actions

1. Open **Actions → Deploy Vertica DB (apply) → Run workflow**.
2. (Optional) Override the allowed CIDR, instance type, or image before starting the run.
3. The workflow performs:
   - secret preflight validation,
   - remote state bootstrap (S3 bucket + DynamoDB lock table),
   - Terraform init/fmt/validate plus TFLint,
   - `terraform apply` using Spot capacity,
   - waits for port 5433, and
   - runs a Python smoke test (`SELECT 1` and a table list).
4. After success, the job summary lists the public IP, DNS, and security group ID.

### Connecting to Vertica

Use the published outputs:

| Parameter | Value |
|-----------|-------|
| Host      | `public_ip` output |
| Port      | `5433` |
| User      | `dbadmin` |
| Password  | *(empty)* |
| Database  | `VMart` |

You can also run `tests/test_connect.py` locally by setting `DB_HOST` to the public IP and installing dependencies from `tests/requirements.txt`.

## Tearing down the stack

Run **Actions → Destroy Vertica DB (destroy) → Run workflow**. The workflow reuses the same backend bootstrapper, reconfigures the remote state, and executes `terraform destroy -auto-approve`. The final step is tolerant of partial failures to allow repeated runs.

## Security considerations

- **Ingress control:** By default the security group allows `0.0.0.0/0`. Supply a custom CIDR (e.g., office or home IP) in the workflow dispatch form to restrict access to ports 5433 (Vertica) and 8000 (reserved for a future MCP service).
- **Least privilege IAM:** The EC2 instance role only receives ECR pull, SSM Session Manager, and CloudWatch Logs permissions required by the bootstrap script.
- **IMDSv2 enforced:** User data requires IMDSv2 (`http_tokens = "required"`) for metadata access.
- **Data volume:** A dedicated 50 GB gp3 EBS volume is mounted on `/var/lib/vertica` and flagged for deletion on termination.

## Cost notes

- Spot t3.xlarge in ap-south-1 is roughly $0.15/hour (check the [AWS Spot price history](https://aws.amazon.com/ec2/spot/pricing/) for the latest rate). On-demand costs about $0.1664/hour.
- The additional gp3 volume (50 GB) is approximately $0.0045/hour.
- Remote state storage (S3 + DynamoDB) is pennies per month but still accumulates—destroy the stack when not in use.

To switch to on-demand instances, set the `instance_type` input as desired and remove the Spot block in `infra/main.tf` or create a Boolean variable to toggle `instance_market_options`.

## Local development & testing

```
python -m venv .venv
source .venv/bin/activate
pip install -r tests/requirements.txt
export DB_HOST=<public_ip>
pytest tests/test_connect.py -q
```

The helper `tests/wait_for_port.py` mirrors the GitHub Actions wait logic.

## Troubleshooting tips

- Review `/var/log/user-data.log` via SSM Session Manager for bootstrap details. The script installs Docker + Compose, formats/mounts the EBS volume, authenticates to ECR, writes `/opt/compose.remote.yml`, and starts the Vertica container.
- Cloud-init logs and Docker health checks report to the system journal. Attach the instance profile to Session Manager to tail logs without SSH.
- If the workflow fails early, re-run it—remote state bootstrapping is idempotent.

## Extending the stack

- Add CloudWatch agent configuration in `infra/user_data.sh` to ship logs.
- Introduce environment protection rules on the workflow or configure approvals for production environments.
- Adjust Terraform variables (allowed CIDRs, instance type, image) via workflow inputs or by editing `infra/variables.tf`.
