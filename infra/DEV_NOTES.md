# Terraform CLI usage notes

This repository requires access to the public Terraform provider registry and AWS APIs when running
`terraform init` and `terraform plan`. In restricted environments you can still run `terraform init`
by downloading the required provider plug-ins manually and configuring a filesystem mirror via a
`.terraformrc` file. However, `terraform plan` still needs valid AWS credentials because it queries
live AWS data sources.

The CI snippet requested by the user was executed locally with the following outcome:

- `terraform validate` succeeds after initialising with a local plug-in mirror.
- `terraform plan` fails because no AWS credentials are available in this sandbox.

