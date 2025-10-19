#!/usr/bin/env bash
set -euo pipefail
: "${AWS_REGION:?set AWS_REGION}" || exit 1
# Deterministic bucket per repo+region. Bucket names must be lower-case and only
# contain letters, numbers, and hyphens, so normalise the repository component
# before using it.
REPO_SLUG=${GITHUB_REPOSITORY//\//-}
REPO_SLUG=${REPO_SLUG:-local}
REPO_SLUG=$(printf '%s' "$REPO_SLUG" | tr '[:upper:]' '[:lower:]')
REPO_SLUG=$(printf '%s' "$REPO_SLUG" | tr -c 'a-z0-9-' '-')
BUCKET="tfstate-${REPO_SLUG}-${AWS_REGION}"
TABLE="tf-locks"
if [[ "$AWS_REGION" == "us-east-1" ]]; then
  aws s3api create-bucket --bucket "$BUCKET" 2>/dev/null || true
else
  aws s3api create-bucket --bucket "$BUCKET" \
    --create-bucket-configuration LocationConstraint="${AWS_REGION}" 2>/dev/null || true
fi
aws s3api wait bucket-exists --bucket "$BUCKET"
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
