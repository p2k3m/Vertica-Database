#!/usr/bin/env bash
set -euo pipefail
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

# Install Docker and helpers
amazon-linux-extras enable docker || true
yum install -y amazon-ssm-agent docker docker-compose-plugin jq nmap-ncat python3 python3-pip
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

# Ensure the SSM agent knows which region to register in
install -d -m 0755 /etc/amazon/ssm
cat >/etc/amazon/ssm/amazon-ssm-agent.json <<EOF
{
  "Agent": {
    "Region": "$${REGION}"
  }
}
EOF
systemctl restart amazon-ssm-agent

# Determine Vertica configuration for later reuse
VERTICA_IMAGE="${vertica_image}"
BOOTSTRAP_ADMIN_USER="${bootstrap_admin_username}"
BOOTSTRAP_ADMIN_PASS="${bootstrap_admin_password}"
ADDITIONAL_ADMIN_USER="${additional_admin_username}"
ADDITIONAL_ADMIN_PASS="${additional_admin_password}"
VERTICA_DB_NAME="${vertica_db_name}"
VERTICA_PORT="${vertica_port}"

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
    ports: ["${vertica_port}:${vertica_port}"]
    ulimits:
      nofile: { soft: 65536, hard: 65536 }
    volumes:
      - /var/lib/vertica:/data
    environment:
      - VERTICA_DB_NAME=${vertica_db_name}
      - VERTICA_DB_USER=${bootstrap_admin_username}
      - VERTICA_DB_PASSWORD=${bootstrap_admin_password}
    healthcheck:
      test: ["CMD", "bash", "-lc", "nc -z localhost ${vertica_port}"]
      interval: 15s
      timeout: 3s
      retries: 20
YAML

mkdir -p /var/lib/vertica
chmod 700 /var/lib/vertica

# Start Vertica only
curl -fsSL https://get.docker.com | sh || true
if ! docker compose version >/dev/null 2>&1; then
  curl -L https://github.com/docker/compose/releases/download/v2.29.7/docker-compose-linux-x86_64 -o /usr/local/bin/docker-compose
  chmod +x /usr/local/bin/docker-compose
fi

(docker compose -f /opt/compose.remote.yml up -d) || (docker-compose -f /opt/compose.remote.yml up -d)

# Wait for Vertica container health/port
deadline=$((SECONDS + 1800))
ready=0
while [ $SECONDS -lt $deadline ]; do
  if docker inspect vertica_ce >/dev/null 2>&1; then
    status=$(docker inspect --format '{{.State.Health.Status}}' vertica_ce 2>/dev/null || echo "unknown")
    case "$status" in
      healthy)
        if nc -z 127.0.0.1 "$VERTICA_PORT"; then
          ready=1
          break
        fi
        ;;
      unhealthy)
        docker logs vertica_ce || true
        exit 1
        ;;
    esac
  fi
  sleep 5
done

if [ "$ready" -ne 1 ]; then
  docker logs vertica_ce || true
  exit 1
fi

# Ensure the additional admin user exists and can authenticate
pip3 install --quiet vertica-python

export BOOTSTRAP_ADMIN_USER BOOTSTRAP_ADMIN_PASS ADDITIONAL_ADMIN_USER ADDITIONAL_ADMIN_PASS VERTICA_DB_NAME VERTICA_PORT

python3 <<'PY'
import os
import re

import vertica_python

HOST = "127.0.0.1"
PORT = int(os.environ["VERTICA_PORT"])
DATABASE = os.environ["VERTICA_DB_NAME"]
BOOT_USER = os.environ["BOOTSTRAP_ADMIN_USER"]
BOOT_PASS = os.environ.get("BOOTSTRAP_ADMIN_PASS", "")
ADMIN_USER = os.environ["ADDITIONAL_ADMIN_USER"]
ADMIN_PASS = os.environ["ADDITIONAL_ADMIN_PASS"]

if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", ADMIN_USER):
    raise SystemExit(f"Invalid additional admin username: {ADMIN_USER!r}")

def _connection_config(user: str, password: str) -> dict:
    return {
        "host": HOST,
        "port": PORT,
        "user": user,
        "password": password,
        "database": DATABASE,
        "autocommit": True,
    }


with vertica_python.connect(**_connection_config(BOOT_USER, BOOT_PASS)) as conn:
    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute("SELECT 1 FROM users WHERE user_name = %s", [ADMIN_USER])
    exists = cursor.fetchone() is not None

    if exists:
        cursor.execute(f'ALTER USER "{ADMIN_USER}" IDENTIFIED BY %s', [ADMIN_PASS])
    else:
        cursor.execute(f'CREATE USER "{ADMIN_USER}" IDENTIFIED BY %s', [ADMIN_PASS])

    cursor.execute(f'ALTER USER "{ADMIN_USER}" SUPERUSER')
    cursor.execute(f'GRANT ALL PRIVILEGES ON DATABASE "{DATABASE}" TO "{ADMIN_USER}"')
    cursor.execute(f'GRANT USAGE ON SCHEMA PUBLIC TO "{ADMIN_USER}"')
    cursor.execute(f'GRANT ALL PRIVILEGES ON SCHEMA PUBLIC TO "{ADMIN_USER}"')

with vertica_python.connect(**_connection_config(ADMIN_USER, ADMIN_PASS)) as conn:
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    value = cursor.fetchone()
    if not value or value[0] != 1:
        raise SystemExit("Unexpected response while validating additional admin credentials")

print(f"Verified Vertica additional admin user '{ADMIN_USER}' with database '{DATABASE}'")
PY

exit 0
