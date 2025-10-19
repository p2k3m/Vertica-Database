#!/usr/bin/env bash
set -euo pipefail
: "${AWS_REGION:?set AWS_REGION}" || exit 1
# Deterministic bucket per repo+region
BUCKET="tfstate-${GITHUB_REPOSITORY//\//-}-${AWS_REGION}"
TABLE="tf-locks"
aws s3api create-bucket --bucket "$BUCKET" --create-bucket-configuration LocationConstraint=${AWS_REGION} 2>/dev/null || true
aws s3api put-bucket-versioning --bucket "$BUCKET" --versioning-configuration Status=Enabled
aws dynamodb create-table --table-name "$TABLE" \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST 2>/dev/null || true
cat > backend.tf <<EOF2
terraform {
  backend "s3" {
    region         = "${AWS_REGION}"
    bucket         = "${BUCKET}"
    key            = "state/terraform.tfstate"
    dynamodb_table = "${TABLE}"
    encrypt        = true
  }
}
EOF2
