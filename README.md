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
- `VERTICA_IMAGE` – override the Vertica CE image (defaults to the private ECR build `957650740525.dkr.ecr.ap-south-1.amazonaws.com/vertica-ce:v1.0`)
- `SMOKE_TEST_USERNAME` / `SMOKE_TEST_PASSWORD` – customise the throwaway account created by the SSM smoke test

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
   - Fetches Terraform outputs and runs a detailed Systems Manager smoke test that validates the SSM agent, Docker state, ECR image availability, Vertica connectivity (localhost + public IP), and user authentication (bootstrap, primary admin, and generated smoke-test account).
4. The job summary lists the public IP plus connection details.

### Connect to Vertica

Use the outputs captured in the workflow summary. Terraform now exposes a consolidated
`connection_details` output and the helper script below prints the same information
locally:

```bash
terraform -chdir=infra output connection_details
python scripts/show_connection_info.py
```

Expected fields:

- Connection URL: `vertica://<appadmin>:<password>@<public-host>:5433/VMart`
- Public IP (`public_ip` output)
- Public DNS (`public_dns` output)
- Port: `5433`
- Primary admin user: value of `additional_admin_username` output (defaults to `appadmin`)
- Primary admin password: value of `additional_admin_password` output
- Smoke-test user: value of `smoke_test_username` output (defaults to `smoketester`)
- Smoke-test password: value of `smoke_test_password` output
- Bootstrap user: `dbadmin` (empty password, retained for compatibility)
- Database: `VMart`

The Terraform security group allows TCP/5433 from `0.0.0.0/0` by default, so the
instance is publicly reachable unless you override `allowed_cidrs`.

You can run `pytest tests/test_connect.py -q` after installing `tests/requirements.txt`. The test automatically reads the
Terraform outputs (`public_ip` first, then `public_dns`) from the `infra/` directory, so you do not need to set
`VERTICA_HOST` manually. Environment variables `DB_HOST`/`VERTICA_HOST` and `DB_PORT`/`VERTICA_PORT` still override the
defaults when provided.

For a quick manual check without installing pytest, run the bundled smoke-test script:

```bash
python scripts/vertica_smoke.py --timeout 15
```

The helper prints the host and port it targets before attempting to connect. In sandboxes that block outbound
connections, the script exits successfully after emitting a warning such as:

```
Target Vertica endpoint: 203.0.113.10:5433
Network unreachable when connecting to Vertica host at 203.0.113.10:5433. This sandbox likely blocks outbound traffic.
```

Passing `--require-service` forces a non-zero exit code instead, which is useful when you expect the network path to be
open and want the script to fail loudly.

### How the automated smoke test works

The GitHub Actions workflow uses AWS Systems Manager to execute a Python script _on the Vertica instance itself_. This
avoids the outbound-network restrictions that affect some CI sandboxes while still verifying that the database is up and
can answer queries. Every run streams verbose diagnostics (SSM agent status, Docker details, port checks, Vertica SQL output)
into the CloudWatch log group referenced by the `ssm_smoke_test_log_group` Terraform output. If you prefer to verify
connectivity from your own workstation or a different environment, use the pytest or CLI helpers in the `tests/` and `scripts/`
directories as described above.

## Recreate or destroy

- **Recreate:** Re-run the apply workflow with `recreate` set to `true`. This performs `terraform destroy` before a fresh apply, guaranteeing a clean instance.
- **Destroy:** Trigger **Actions → Destroy Vertica DB**. The job reuses the remote backend and executes `terraform destroy -auto-approve` (errors are tolerated for idempotent retries).

## Cost & security notes

- Default instance type is `t3.xlarge` Spot in `ap-south-1` for minimal cost; adjust via Terraform variables if required.
- The security group exposes port `5433` only to the configured CIDR(s).
- The IAM instance profile only grants the permissions needed for ECR pulls, SSM Session Manager, and CloudWatch Logs API usage from the bootstrap script.

## Tests

The `tests/` directory contains a simple connectivity test (`SELECT 1` + table listing) that matches the GitHub Actions smoke check. Install dependencies with `pip install -r tests/requirements.txt` if you want to run it locally against the deployed host.

## Automated pipeline self-healing

The `scripts/auto_pipeline_fix.py` helper coordinates a closed-loop remediation
flow when a workflow fails. It automatically gathers the failing log excerpts,
requests a unified diff from an LLM, pushes the fix to a new branch, opens a
pull request, enables auto-merge, and monitors the checks. The fixer now
prioritises failing pull-request runs (falling back to `main` branch failures)
so that regressions introduced on feature branches can be healed before merging.
If the checks fail again the process repeats with the fresh failure details
until either the checks pass or the iteration budget is exhausted.

### Required environment

The automation is designed to run from within a clone of the repository and
relies on the following environment variables:

| Variable | Purpose |
| --- | --- |
| `GITHUB_TOKEN` | Token with `repo` and `workflow` scopes used for REST/GraphQL calls and pushing branches |
| `GITHUB_REPOSITORY` | Target repository in `owner/name` format |
| `GIT_AUTHOR_NAME` / `GIT_AUTHOR_EMAIL` | Git identity configured before creating commits |
| `OPENAI_API_KEY` | Credential used to access the OpenAI API |

Optional variables adjust runtime behaviour:

- `OPENAI_MODEL` (default `gpt-4.1`)
- `AUTO_MERGE_METHOD` (default `SQUASH`)
- `AUTOFIX_BRANCH_PREFIX` (default `autofix`)
- `AUTOFIX_MAX_ITERATIONS` (default `3`)
- `AUTOFIX_POLL_INTERVAL` (default `30` seconds)

### Manual execution

With the environment set, invoke the fixer from the repository root:

```bash
python scripts/auto_pipeline_fix.py
```

The script exits successfully once the checks for the auto-merge PR pass and the
merge completes. If the iteration limit is reached first, it exits non-zero so
that the invoking workflow can surface the failure for manual review.
