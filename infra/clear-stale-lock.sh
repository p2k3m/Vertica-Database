#!/usr/bin/env bash
set -euo pipefail

: "${AWS_REGION:?set AWS_REGION}" || exit 1

TABLE="tf-locks"
KEY_PATH="state/terraform.tfstate"
STALE_AFTER_SECONDS=${STALE_AFTER_SECONDS:-1800}

log() {
  local ts
  ts=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  echo "[clear-stale-lock] ${ts} $*" >&2
}

declare -A SEEN_LOCKS=()

if ! command -v jq >/dev/null 2>&1; then
  log "jq not found; skipping Terraform lock cleanup"
  exit 0
fi

if ! command -v aws >/dev/null 2>&1; then
  log "aws CLI not found; skipping Terraform lock cleanup"
  exit 0
fi

PYTHON_BIN=${PYTHON_BIN:-python3}
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  PYTHON_BIN=python
fi

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  log "Python interpreter not found; skipping Terraform lock cleanup"
  exit 0
fi

determine_repo_identifier() {
  local slug_source="${GITHUB_REPOSITORY:-}"

  if [[ -z "$slug_source" ]] && command -v git >/dev/null 2>&1; then
    local remote
    remote=$(git config --get remote.origin.url 2>/dev/null || true)
    if [[ -n "$remote" ]]; then
      slug_source=$("$PYTHON_BIN" - "$remote" <<'PY' || true
import re
import sys

remote = sys.argv[1].strip()
if not remote:
    sys.exit(1)

remote = remote.rstrip('/')
remote = re.sub(r"\.git$", "", remote)

if remote.startswith("git@"):
    remote = remote.split(":", 1)[-1]
elif "://" in remote:
    remote = remote.split("://", 1)[-1]

remote = remote.lstrip("/")
remote = re.sub(r"^github\.com[:/]", "", remote, flags=re.IGNORECASE)

parts = [part for part in re.split(r"[/:]", remote) if part]
if len(parts) >= 2:
    print(parts[-2] + "/" + parts[-1])
    sys.exit(0)

sys.exit(1)
PY
)
    fi
  fi

  if [[ -z "$slug_source" ]]; then
    slug_source=local
  fi

  printf '%s' "$slug_source"
}

REPO_SLUG=$(determine_repo_identifier)
REPO_SLUG=${REPO_SLUG//\//-}
REPO_SLUG=$(printf '%s' "$REPO_SLUG" | tr '[:upper:]' '[:lower:]')
REPO_SLUG=$(printf '%s' "$REPO_SLUG" | tr -c 'a-z0-9-' '-')

BUCKET="tfstate-${REPO_SLUG}-${AWS_REGION}"
TARGET_PATHS=(
  "${BUCKET}/${KEY_PATH}"
  "${KEY_PATH}"
)

log "AWS region: ${AWS_REGION}"
log "DynamoDB table: ${TABLE}"
log "Repository slug: ${REPO_SLUG}"
log "Expected lock paths: ${TARGET_PATHS[*]}"

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
    log "Skipping lock with missing LockID"
    return
  fi

  if [[ -n "${SEEN_LOCKS[$lock_id]:-}" ]]; then
    log "Lock ${lock_id} already processed; skipping"
    return
  fi

  local parsed created operation
  parsed=$(jq -r '
    def normalise:
      if type == "object" then
        if has("S") then .S
        elif has("N") then .N
        elif has("BOOL") then (if .BOOL then "true" else "false" end)
        elif has("NULL") then empty
        elif has("M") then (.M | with_entries(.value |= (.value | normalise)))
        elif has("L") then (.L | map(normalise))
        else with_entries(.value |= (.value | normalise))
        end
      else .
      end;

    def info_object:
      if .Info.S? then
        (.Info.S | fromjson? // {})
      elif .Info.M? then
        (.Info.M | with_entries(.value |= (.value | normalise)))
      else
        {}
      end;

    def pick_created($info):
      [
        (.Created.S // empty),
        ($info.Created // empty),
        ($info.created // empty),
        ($info.CreatedAt // empty),
        ($info.created_at // empty)
      ]
      | map(select(. != ""))
      | first // empty;

    def pick_operation($info):
      [
        (.Operation.S // empty),
        ($info.Operation // empty),
        ($info.operation // empty)
      ]
      | map(select(. != ""))
      | first // empty;

    (info_object) as $info |
    [pick_created($info), pick_operation($info)]
    | @tsv
  ' <<<"$item_json" 2>/dev/null || true)

  IFS=$'\t' read -r created operation <<<"${parsed:-$'\t'}"

  local should_remove=1
  local reason=""

  log "Evaluating lock ${lock_id}: created='${created}' operation='${operation}'"

  if is_stale "$created"; then
    reason="stale for at least ${STALE_AFTER_SECONDS}s"
  elif [[ "$operation" == "OperationTypeInvalid" ]]; then
    reason="invalid operation state"
  else
    should_remove=0
  fi

  if (( should_remove == 0 )); then
    log "Leaving lock ${lock_id} untouched (path does not appear stale): created='${created}' operation='${operation}'"
    return
  fi

  SEEN_LOCKS[$lock_id]=1

  local key_json
  key_json=$(jq -nc --arg key "$lock_id" '{LockID:{S:$key}}')
  local delete_output=""
  if delete_output=$(aws dynamodb delete-item \
    --table-name "$TABLE" \
    --key "$key_json" \
    --output json \
    --region "$AWS_REGION" 2>&1); then
    if [[ -n "$reason" ]]; then
      reason=" (${reason})"
    fi
    log "Removed Terraform lock for ${lock_id}${reason}"
  else
    log "Failed to delete lock ${lock_id}; AWS CLI output: ${delete_output}"
  fi
}

process_path() {
  local path="$1"
  local matches
  matches=$(jq -r --arg path "$path" '
    .Items[]? |
    (.Path.S // "") as $p |
    select($p == $path or ($p | endswith($path))) |
    @base64
  ' <<<"$scan_output" 2>/dev/null || true)

  if [[ -z "$matches" ]]; then
    log "No locks found for path ${path}"
    return
  fi

  while IFS= read -r encoded; do
    [[ -z "$encoded" ]] && continue
    local item_json
    item_json=$(echo "$encoded" | base64 --decode)
    local current_lock_id
    current_lock_id=$(jq -r '.LockID.S // "unknown"' <<<"$item_json")
    log "Processing lock for path ${path}: ${current_lock_id}"
    clear_lock_item "$item_json" || true
  done <<<"$matches"
}

start_key=""
while :; do
  if [[ -n "$start_key" ]]; then
    scan_output=$(aws dynamodb scan \
      --table-name "$TABLE" \
      --exclusive-start-key "$start_key" \
      --output json \
      --region "$AWS_REGION" 2>&1) || {
        log "Failed to scan DynamoDB table ${TABLE}: ${scan_output}"
        break
      }
  else
    scan_output=$(aws dynamodb scan \
      --table-name "$TABLE" \
      --output json \
      --region "$AWS_REGION" 2>&1) || {
        log "Failed to scan DynamoDB table ${TABLE}: ${scan_output}"
        break
      }
  fi

  if [[ -z "$scan_output" ]] || [[ "$scan_output" == "{}" ]]; then
    log "No DynamoDB items returned; finishing"
    break
  fi

  item_count=$(jq '.Items | length' <<<"$scan_output" 2>/dev/null || echo 0)
  log "Fetched ${item_count} lock candidate(s) from DynamoDB"

  for candidate_path in "${TARGET_PATHS[@]}"; do
    process_path "$candidate_path"
  done

  start_key=$(jq -c '.LastEvaluatedKey // empty' <<<"$scan_output" 2>/dev/null || true)
  if [[ -z "$start_key" ]] || [[ "$start_key" == "null" ]]; then
    log "Reached end of DynamoDB scan"
    break
  fi
done

log "Terraform lock cleanup finished"
