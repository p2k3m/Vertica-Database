#!/usr/bin/env bash
set -euo pipefail
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

# Install Docker and helpers
amazon-linux-extras enable docker || true
yum install -y docker jq nmap-ncat
systemctl enable --now docker

# Discover region/account from metadata
REGION=$(curl -s http://169.254.169.254/latest/dynamic/instance-identity/document | jq -r .region)
ACCOUNT_ID=$(curl -s http://169.254.169.254/latest/dynamic/instance-identity/document | jq -r .accountId)
if [[ -z "$REGION" || "$REGION" == "null" ]]; then
  REGION="${aws_region}"
fi
if [[ -z "$ACCOUNT_ID" || "$ACCOUNT_ID" == "null" ]]; then
  ACCOUNT_ID="${aws_account_id}"
fi

# ECR login (best effort)
aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin $${ACCOUNT_ID}.dkr.ecr.$${REGION}.amazonaws.com || true

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

# Wait for port
for i in {1..60}; do
  nc -z 127.0.0.1 5433 && exit 0 || sleep 5
done
exit 1
