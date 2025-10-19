#!/usr/bin/env bash
set -euo pipefail
exec > >(tee /var/log/user-data.log | logger -t user-data -s 2>/dev/console) 2>&1

AWS_ACCOUNT_ID="${aws_account_id}"
VERTICA_IMAGE="${vertica_image}"
DEFAULT_REGION="${aws_region}"

# Install dependencies
DNF_CMD=$(command -v dnf || echo "")
if [[ -n "$DNF_CMD" ]]; then
  dnf update -y
  dnf install -y docker docker-compose-plugin jq nmap-ncat xfsprogs
else
  yum update -y
  amazon-linux-extras enable docker
  yum install -y docker docker-compose-plugin jq nmap-ncat xfsprogs
fi
systemctl enable --now docker

# Prepare data volume (formatted as XFS and mounted on /var/lib/vertica)
DATA_DEVICE=""
if [[ -b /dev/nvme1n1 ]]; then
  DATA_DEVICE="/dev/nvme1n1"
elif [[ -b /dev/xvdh ]]; then
  DATA_DEVICE="/dev/xvdh"
elif [[ -b /dev/sdh ]]; then
  DATA_DEVICE="/dev/sdh"
fi

if [[ -n "$DATA_DEVICE" ]]; then
  if ! blkid "$DATA_DEVICE" >/dev/null 2>&1; then
    mkfs.xfs -f "$DATA_DEVICE"
  fi
  mkdir -p /var/lib/vertica
  mount "$DATA_DEVICE" /var/lib/vertica
  if ! grep -q "/var/lib/vertica" /etc/fstab; then
    echo "$DATA_DEVICE /var/lib/vertica xfs defaults,nofail 0 2" >> /etc/fstab
  fi
fi
mkdir -p /var/lib/vertica
chmod 700 /var/lib/vertica

# Authenticate to ECR (best effort)
IMDS_REGION=$$(curl -s http://169.254.169.254/latest/dynamic/instance-identity/document | jq -r .region || true)
REGION=$${IMDS_REGION:-$${DEFAULT_REGION}}
if [[ -n "$${AWS_ACCOUNT_ID}" ]]; then
  aws ecr get-login-password --region "$${REGION}" \
    | docker login --username AWS --password-stdin "$${AWS_ACCOUNT_ID}".dkr.ecr."$${REGION}".amazonaws.com || true
fi

# Render docker compose definition
cat >/opt/compose.remote.yml <<'YAML'
${compose_file}
YAML

# Start Vertica
export VERTICA_IMAGE
export AWS_ACCOUNT_ID
docker compose -f /opt/compose.remote.yml pull || true
docker compose -f /opt/compose.remote.yml up -d

# Smoke check with retries
for attempt in $$(seq 1 60); do
  if nc -z 127.0.0.1 5433; then
    echo "Vertica is accepting connections"
    exit 0
  fi
  sleep 5
  echo "Waiting for Vertica (attempt $${attempt})"
done

exit 1
