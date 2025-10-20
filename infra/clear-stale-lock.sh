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
import re
import sys
from datetime import datetime, timezone

raw = sys.argv[1].strip()
if not raw:
    sys.exit(1)

variants = {raw}

def add_variant(value):
    value = value.strip()
    if value:
        variants.add(value)

for suffix in (" UTC", " GMT", " Z"):
    if raw.endswith(suffix):
        add_variant(raw[: -len(suffix)])

if raw.endswith("Z"):
    add_variant(raw[:-1] + "+00:00")

if "T" in raw:
    add_variant(raw.replace("T", " "))

normalised = set()
for candidate in variants:
    candidate = candidate.strip()
    if not candidate:
        continue

    match = re.search(r"\.\d+", candidate)
    if match:
        frac = match.group(0)[1:]
        if len(frac) > 6:
            candidate = candidate[: match.start() + 1] + frac[:6] + candidate[match.end():]
        elif len(frac) < 6:
            candidate = candidate[: match.start() + 1] + frac.ljust(6, "0") + candidate[match.end():]

    candidate = re.sub(r"([+-]\d{2})(\d{2})(?!:)", r"\1:\2", candidate)

    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"

    normalised.add(candidate)

for candidate in normalised:
    try:
        dt = datetime.fromisoformat(candidate)
    except ValueError:
        continue

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    print(int(dt.timestamp()))
    sys.exit(0)

for candidate in normalised:
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%d %H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S%z",
    ):
        try:
            dt = datetime.strptime(candidate, fmt)
        except ValueError:
            continue

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)

        print(int(dt.timestamp()))
        sys.exit(0)

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
  local future_tolerance=${FUTURE_TOLERANCE_SECONDS:-300}

  if (( epoch > now_epoch + future_tolerance )); then
    return 0
  fi

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

  local operation
  operation=$(jq -r '(.Operation.S // empty) // (.Info.S | fromjson? | .Operation // empty)' <<<"$item_json" 2>/dev/null || true)

  local should_remove=1
  local reason=""

  if is_stale "$created"; then
    reason="stale for at least ${STALE_AFTER_SECONDS}s"
  elif [[ "$operation" == "OperationTypeInvalid" ]]; then
    reason="invalid operation state"
  else
    should_remove=0
  fi

  if (( should_remove == 0 )); then
    return
  fi

  SEEN_LOCKS[$lock_id]=1

  local key_json
  key_json=$(jq -nc --arg key "$lock_id" '{LockID:{S:$key}}')
  aws dynamodb delete-item \
    --table-name "$TABLE" \
    --key "$key_json" \
    --output json >/dev/null

  if [[ -n "$reason" ]]; then
    reason=" (${reason})"
  fi

  echo "Removed Terraform lock for ${lock_id}${reason}" >&2
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

start_key=""
while :; do
  if [[ -n "$start_key" ]]; then
    scan_output=$(aws dynamodb scan \
      --table-name "$TABLE" \
      --exclusive-start-key "$start_key" \
      --output json 2>/dev/null || true)
  else
    scan_output=$(aws dynamodb scan \
      --table-name "$TABLE" \
      --output json 2>/dev/null || true)
  fi

  if [[ -z "$scan_output" ]] || [[ "$scan_output" == "{}" ]]; then
    break
  fi

  for candidate_path in "${TARGET_PATHS[@]}"; do
    process_path "$candidate_path"
  done

  start_key=$(jq -c '.LastEvaluatedKey // empty' <<<"$scan_output" 2>/dev/null || true)
  if [[ -z "$start_key" ]] || [[ "$start_key" == "null" ]]; then
    break
  fi
done
