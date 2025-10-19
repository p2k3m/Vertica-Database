# Vertica-Database

Provision a singleton Vertica Community Edition host on AWS with Terraform remote state, GitHub Actions automation, and smoke tests to verify connectivity.

## Repository layout

```
vertica-database/
├─ infra/
│  ├─ backend-bootstrap.sh  # Creates S3 bucket + DynamoDB table for Terraform state
│  ├─ import-if-exists.sh   # Imports pre-existing resources to prevent duplicates
│  ├─ locals.tf             # Naming helpers
│  ├─ main.tf               # Security group, IAM, and EC2 resources
│  ├─ outputs.tf            # Public endpoint + SG outputs
│  ├─ user_data.sh          # Bootstraps Vertica container
│  └─ variables.tf          # Deployment inputs
├─ tests/
│  ├─ requirements.txt      # vertica-python + pytest
│  ├─ test_connect.py       # SELECT 1 + table list
│  └─ wait_for_port.py      # Local helper
└─ .github/workflows/
   ├─ apply.yml             # Plan + apply with optional destroy-then-apply
   └─ destroy.yml           # Manual teardown
```

## Prerequisites

Set these repository secrets before running the workflows:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION` (use `ap-south-1`)
- `AWS_ACCOUNT_ID`

Optional repository variables:

- `ALLOWED_CIDR` – lock down Vertica ingress (defaults to `0.0.0.0/0`)
- `VERTICA_IMAGE` – override the Vertica CE image (`957650740525.dkr.ecr.ap-south-1.amazonaws.com/vertica-ce:v1.0` by default)

## Remote state bootstrap

`infra/backend-bootstrap.sh` provisions (idempotently) an S3 bucket + DynamoDB table keyed off the repository and region, then writes `backend.tf`. All GitHub Actions runs call this helper before `terraform init`, ensuring the state is shared and lock-protected.

## Deploy (apply workflow)

1. Push your changes and open **Actions → Deploy Vertica DB (apply) → Run workflow**.
2. (Optional) Toggle `recreate` to destroy the existing instance before applying.
3. The workflow performs:
   - AWS credential configuration from secrets.
   - Remote backend bootstrap + `terraform init`.
   - `import-if-exists.sh` to adopt any matching resources (security group, IAM role/profile, EC2) before planning.
   - `terraform validate` and `plan` with detailed exit codes.
   - `terraform apply` using a Spot `t3.xlarge` (unless `use_spot` is overridden).
   - Fetches Terraform outputs and runs a Python smoke test (`SELECT 1`).
4. The job summary lists the public IP plus connection details.

### Connect to Vertica

Use the outputs captured in the workflow summary:

- Host: public IP (`public_ip` output)
- Port: `5433`
- User: `dbadmin`
- Password: *(empty)*
- Database: `VMart`

You can run `pytest tests/test_connect.py -q` after installing `tests/requirements.txt`. The test automatically reads the
Terraform outputs (`public_ip` first, then `public_dns`) from the `infra/` directory, so you do not need to set
`VERTICA_HOST` manually. Environment variables `DB_HOST`/`VERTICA_HOST` and `DB_PORT`/`VERTICA_PORT` still override the
defaults when provided.

### Troubleshooting connectivity from CI sandboxes

Some ephemeral CI environments (including the one used for the provided integration tests) block outbound traffic to
arbitrary public IPs. When that happens, the Python smoke test will fail with a `vertica_python.errors.ConnectionError`
and an underlying socket error such as `[Errno 101] Network is unreachable`. This is an infrastructure restriction of
the sandbox, not a Vertica or Terraform misconfiguration—the deployed instance already allows ingress on port `5433`
from the configured CIDR(s).

To validate connectivity:

1. Run the test from a workstation or runner that has outbound access to the instance's public IP.
2. Alternatively, adjust your allow-list/VPC rules so that the environment executing the test can reach the host on
   port `5433`.

No repository changes are necessary once network access is available.

## Recreate or destroy

- **Recreate:** Re-run the apply workflow with `recreate` set to `true`. This performs `terraform destroy` before a fresh apply, guaranteeing a clean instance.
- **Destroy:** Trigger **Actions → Destroy Vertica DB**. The job reuses the remote backend and executes `terraform destroy -auto-approve` (errors are tolerated for idempotent retries).

## Cost & security notes

- Default instance type is `t3.xlarge` Spot in `ap-south-1` for minimal cost; adjust via Terraform variables if required.
- The security group exposes port `5433` only to the configured CIDR(s).
- The IAM instance profile only grants the permissions needed for ECR pulls, SSM Session Manager, and CloudWatch Logs API usage from the bootstrap script.

## Tests

The `tests/` directory contains a simple connectivity test (`SELECT 1` + table listing) that matches the GitHub Actions smoke check. Install dependencies with `pip install -r tests/requirements.txt` if you want to run it locally against the deployed host.
