#!/usr/bin/env bash
set -euo pipefail

: "${AWS_REGION:?set AWS_REGION}" || exit 1

REPO_SLUG=${GITHUB_REPOSITORY:-local}
REPO_SLUG=${REPO_SLUG//\//-}
REPO_SLUG=$(printf '%s' "$REPO_SLUG" | tr '[:upper:]' '[:lower:]')
REPO_SLUG=$(printf '%s' "$REPO_SLUG" | tr -c 'a-z0-9-' '-')

BUCKET="tfstate-${REPO_SLUG}-${AWS_REGION}"
TABLE="tf-locks"
KEY_PATH="state/terraform.tfstate"
TARGET_PATHS=(
  "${BUCKET}/${KEY_PATH}"
  "${KEY_PATH}"
)
STALE_AFTER_SECONDS=${STALE_AFTER_SECONDS:-1800}

declare -A SEEN_LOCKS=()

if ! command -v jq >/dev/null 2>&1; then
  echo "jq not found; skipping Terraform lock cleanup" >&2
  exit 0
fi

if ! command -v aws >/dev/null 2>&1; then
  echo "aws CLI not found; skipping Terraform lock cleanup" >&2
  exit 0
fi

PYTHON_BIN=${PYTHON_BIN:-python3}
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  PYTHON_BIN=python
fi

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "Python interpreter not found; skipping Terraform lock cleanup" >&2
  exit 0
fi

parse_timestamp() {
  local raw="$1"
  [[ -z "$raw" ]] && return 1

  "$PYTHON_BIN" - "$raw" <<'PY'
import sys
from datetime import datetime, timezone

raw = sys.argv[1]

for fmt in (
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y-%m-%d %H:%M:%S.%f %z",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%d %H:%M:%S %z",
):
    try:
        dt = datetime.strptime(raw, fmt)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        print(int(dt.timestamp()))
        sys.exit(0)
    except ValueError:
        continue

sys.exit(1)
PY
}

is_stale() {
  local created_ts="$1"
  if [[ -z "$created_ts" ]]; then
    return 1
  fi

  local epoch
  if ! epoch=$(parse_timestamp "$created_ts" 2>/dev/null); then
    return 1
  fi

  local now_epoch
  now_epoch=$(date -u +%s)
  (( now_epoch - epoch >= STALE_AFTER_SECONDS ))
}

clear_lock_item() {
  local item_json="$1"

  local lock_id
  lock_id=$(jq -r '.LockID.S // empty' <<<"$item_json")
  if [[ -z "$lock_id" ]]; then
    return
  fi

  if [[ -n "${SEEN_LOCKS[$lock_id]:-}" ]]; then
    return
  fi

  local created
  created=$(jq -r '(.Created.S // empty) // (.Info.S | fromjson? | .Created // empty)' <<<"$item_json" 2>/dev/null || true)
  if ! is_stale "$created"; then
    return
  fi

  SEEN_LOCKS[$lock_id]=1

  local key_json
  key_json=$(jq -nc --arg key "$lock_id" '{LockID:{S:$key}}')
  aws dynamodb delete-item \
    --table-name "$TABLE" \
    --key "$key_json" \
    --output json >/dev/null

  echo "Removed stale Terraform lock for ${lock_id}" >&2
}

process_path() {
  local path="$1"
  local matches
  matches=$(jq -r --arg path "$path" '.Items[]? | select(.Path.S == $path) | @base64' <<<"$scan_output" 2>/dev/null || true)
  if [[ -z "$matches" ]]; then
    return
  fi

  while IFS= read -r encoded; do
    [[ -z "$encoded" ]] && continue
    local item_json
    item_json=$(echo "$encoded" | base64 --decode)
    clear_lock_item "$item_json" || true
  done <<<"$matches"
}

scan_output=$(aws dynamodb scan --table-name "$TABLE" --output json 2>/dev/null || true)
if [[ -z "$scan_output" ]] || [[ "$scan_output" == "{}" ]]; then
  exit 0
fi

for candidate_path in "${TARGET_PATHS[@]}"; do
  process_path "$candidate_path"
done
