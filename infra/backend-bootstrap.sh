#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${AWS_REGION:-}" ]]; then
  echo "AWS_REGION must be set" >&2
  exit 1
fi

REPO=${GITHUB_REPOSITORY:-vertica-database/local}
OWNER=${GITHUB_REPOSITORY_OWNER:-${REPO%%/*}}
NAME=${REPO##*/}
OWNER_SANITIZED=$(echo "$OWNER" | tr '[:upper:]' '[:lower:]' | tr -cd 'a-z0-9')
NAME_SANITIZED=$(echo "$NAME" | tr '[:upper:]' '[:lower:]' | tr -cd 'a-z0-9-')
BUCKET="bkt-${OWNER_SANITIZED}-${NAME_SANITIZED}-tf"
TABLE="tf-locks"

if [[ "$AWS_REGION" == "us-east-1" ]]; then
  aws s3api create-bucket --bucket "$BUCKET" || true
else
  aws s3api create-bucket --bucket "$BUCKET" --create-bucket-configuration LocationConstraint="$AWS_REGION" || true
fi
aws s3api put-bucket-encryption --bucket "$BUCKET" --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}' || true
aws s3api put-bucket-versioning --bucket "$BUCKET" --versioning-configuration Status=Enabled || true
aws dynamodb create-table \
  --table-name "$TABLE" \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST || true

echo "bucket = \"$BUCKET\"" > backend.conf
echo "dynamodb_table = \"$TABLE\"" >> backend.conf
