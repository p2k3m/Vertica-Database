#!/usr/bin/env bash
set -euo pipefail
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

# Install Docker and helpers
amazon-linux-extras enable docker || true
yum install -y amazon-ssm-agent docker jq nmap-ncat python3 python3-pip
systemctl enable --now docker
systemctl enable --now amazon-ssm-agent

# Discover region/account from metadata
REGION=$(curl -s http://169.254.169.254/latest/dynamic/instance-identity/document | jq -r .region)
ACCOUNT_ID=$(curl -s http://169.254.169.254/latest/dynamic/instance-identity/document | jq -r .accountId)
if [[ -z "$REGION" || "$REGION" == "null" ]]; then
  REGION="${aws_region}"
fi
if [[ -z "$ACCOUNT_ID" || "$ACCOUNT_ID" == "null" ]]; then
  ACCOUNT_ID="${aws_account_id}"
fi

# Determine Vertica image for later reuse
VERTICA_IMAGE="${vertica_image}"

# ECR (or ECR Public) login if the image requires it (best effort)
if [[ "$VERTICA_IMAGE" =~ ^([0-9]+\.dkr\.ecr\.([a-z0-9-]+)\.amazonaws\.com)(/.+)$ ]]; then
  ECR_HOST="$${BASH_REMATCH[1]}"
  ECR_REGION="$${BASH_REMATCH[2]}"
  aws ecr get-login-password --region "$ECR_REGION" \
    | docker login --username AWS --password-stdin "$ECR_HOST" || true
elif [[ "$VERTICA_IMAGE" =~ ^public\.ecr\.aws/ ]]; then
  aws ecr-public get-login-password --region us-east-1 \
    | docker login --username AWS --password-stdin public.ecr.aws || true
fi

# Render compose (Vertica only)
cat >/opt/compose.remote.yml <<'YAML'
services:
  vertica:
    image: ${vertica_image}
    container_name: vertica_ce
    restart: always
    ports: ["5433:5433"]
    ulimits:
      nofile: { soft: 65536, hard: 65536 }
    volumes:
      - /var/lib/vertica:/data
    environment:
      - VERTICA_DB_NAME=VMart
      - VERTICA_DB_USER=dbadmin
      - VERTICA_DB_PASSWORD=
    healthcheck:
      test: ["CMD", "bash", "-lc", "nc -z localhost 5433"]
      interval: 15s
      timeout: 3s
      retries: 20
YAML

mkdir -p /var/lib/vertica
chmod 700 /var/lib/vertica

# Start Vertica only
curl -fsSL https://get.docker.com | sh || true
command -v docker compose >/dev/null 2>&1 || \
  curl -L https://github.com/docker/compose/releases/download/v2.29.7/docker-compose-linux-x86_64 -o /usr/local/bin/docker-compose && chmod +x /usr/local/bin/docker-compose

(docker compose -f /opt/compose.remote.yml up -d) || (docker-compose -f /opt/compose.remote.yml up -d)

# Wait for Vertica container health/port
deadline=$((SECONDS + 1800))
while [ $SECONDS -lt $deadline ]; do
  if docker inspect vertica_ce >/dev/null 2>&1; then
    status=$(docker inspect --format '{{.State.Health.Status}}' vertica_ce 2>/dev/null || echo "unknown")
    case "$status" in
      healthy)
        nc -z 127.0.0.1 5433 && exit 0
        ;;
      unhealthy)
        docker logs vertica_ce || true
        exit 1
        ;;
    esac
  fi
  sleep 5
done

docker logs vertica_ce || true
exit 1
