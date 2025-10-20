import json
import os
import shutil
import socket
import subprocess
import sys
import time
import uuid
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from typing import Optional

import vertica_python

DB_NAME = 'VMart'
DB_PORT = 5433
DBADMIN_USER = 'dbadmin'
DBADMIN_PASSWORD = ''
ADMIN_USER = os.environ['ADMIN_USER']
ADMIN_PASSWORD = os.environ['ADMIN_PASSWORD']

if not ADMIN_USER:
    raise SystemExit('Missing ADMIN_USER value')
if ADMIN_PASSWORD is None:
    raise SystemExit('Missing ADMIN_PASSWORD value')

STEP_SEPARATOR = '=' * 72


def log(message: str) -> None:
    print(message, flush=True)


def run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    log(STEP_SEPARATOR)
    log(f'$ {" ".join(command)}')
    result = subprocess.run(command, capture_output=True, text=True)
    if result.stdout:
        log(result.stdout.rstrip())
    if result.stderr:
        log(f'[stderr] {result.stderr.rstrip()}')
    if result.returncode != 0:
        raise SystemExit(f'Command {command!r} failed with exit code {result.returncode}')
    return result


_METADATA_TOKEN: Optional[str] = None


def get_metadata_token(timeout: float = 2.0) -> Optional[str]:
    """Return an IMDSv2 session token, or None if the token endpoint is unavailable."""

    global _METADATA_TOKEN
    if _METADATA_TOKEN is not None:
        return _METADATA_TOKEN

    request = Request(
        'http://169.254.169.254/latest/api/token',
        method='PUT',
        headers={'X-aws-ec2-metadata-token-ttl-seconds': '21600'},
        data=b'',
    )

    try:
        with urlopen(request, timeout=timeout) as response:
            token = response.read().decode('utf-8').strip()
    except HTTPError:
        return None

    _METADATA_TOKEN = token
    return token


def fetch_metadata(path: str, timeout: float = 2.0) -> str:
    url = f'http://169.254.169.254/latest/{path.lstrip("/")}'

    token = get_metadata_token(timeout)
    headers = {'X-aws-ec2-metadata-token': token} if token else None

    request = Request(url, headers=headers or {})

    try:
        with urlopen(request, timeout=timeout) as response:
            return response.read().decode('utf-8').strip()
    except HTTPError as exc:
        if token and exc.code == 401:
            # Token might have expired; refresh and retry once.
            global _METADATA_TOKEN
            _METADATA_TOKEN = None
            return fetch_metadata(path, timeout)
        raise


def _docker_info() -> subprocess.CompletedProcess[str]:
    """Run ``docker info`` and return the completed process."""

    return subprocess.run(
        ['docker', 'info'],
        capture_output=True,
        text=True,
    )


def _attempt_install_docker() -> bool:
    """Try to install the Docker CLI using available package managers."""

    install_sequences: list[list[list[str]]] = []

    if shutil.which('amazon-linux-extras') and shutil.which('yum'):
        install_sequences.append(
            [
                ['amazon-linux-extras', 'enable', 'docker'],
                ['yum', 'install', '-y', 'docker'],
            ]
        )

    if shutil.which('dnf'):
        install_sequences.append([
            ['dnf', 'install', '-y', 'docker'],
        ])

    if shutil.which('yum'):
        install_sequences.append([
            ['yum', 'install', '-y', 'docker'],
        ])

    if shutil.which('apt-get'):
        install_sequences.append(
            [
                ['apt-get', 'update'],
                ['apt-get', 'install', '-y', 'docker.io'],
            ]
        )

    if not install_sequences:
        log('No supported package manager found to install Docker')
        return False

    for sequence in install_sequences:
        commands_preview = ' && '.join(' '.join(part) for part in sequence)
        log(STEP_SEPARATOR)
        log(f'Attempting to install Docker using: {commands_preview}')
        try:
            for command in sequence:
                run_command(command)
        except SystemExit as exc:
            log(f'Docker installation attempt failed: {exc}')
            continue

        if shutil.which('docker') is not None:
            return True

    return shutil.which('docker') is not None


def ensure_docker_service() -> None:
    if shutil.which('docker') is None:
        log(STEP_SEPARATOR)
        log('Docker CLI is not available on the instance; attempting installation')
        if not _attempt_install_docker():
            raise SystemExit('Docker CLI is not available on the instance and installation failed')

    info_result = _docker_info()
    if info_result.returncode == 0:
        return

    if shutil.which('systemctl') is None:
        log(STEP_SEPARATOR)
        log('Docker CLI found but daemon is unreachable and systemctl is unavailable')
        if info_result.stderr:
            log(f'[stderr] {info_result.stderr.rstrip()}')
        raise SystemExit('Unable to manage docker daemon without systemctl')

    log(STEP_SEPARATOR)
    log('Docker daemon unavailable; attempting to start docker.service via systemctl')
    start_result = subprocess.run(
        ['systemctl', 'start', 'docker'],
        capture_output=True,
        text=True,
    )
    if start_result.returncode != 0:
        if start_result.stdout:
            log(start_result.stdout.rstrip())
        if start_result.stderr:
            log(f'[stderr] {start_result.stderr.rstrip()}')

        if 'Unit docker.service not found' in (start_result.stderr or '') and shutil.which('service'):
            log('Attempting to start docker via the legacy service command')
            legacy_result = subprocess.run(
                ['service', 'docker', 'start'],
                capture_output=True,
                text=True,
            )
            if legacy_result.returncode != 0:
                if legacy_result.stdout:
                    log(legacy_result.stdout.rstrip())
                if legacy_result.stderr:
                    log(f'[stderr] {legacy_result.stderr.rstrip()}')
            else:
                info_result = _docker_info()
                if info_result.returncode == 0:
                    return

        raise SystemExit('Failed to start docker daemon')

    info_result = _docker_info()
    if info_result.returncode != 0:
        if info_result.stderr:
            log(f'[stderr] {info_result.stderr.rstrip()}')
        raise SystemExit('Docker daemon did not become available after start attempt')


def _docker_inspect(container: str, template: str) -> Optional[str]:
    result = subprocess.run(
        ['docker', 'inspect', '--format', template, container],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return None
    value = result.stdout.strip()
    if value == '<no value>':
        return None
    return value or None


def _compose_command() -> Optional[list[str]]:
    compose_paths = [
        ['docker', 'compose', '-f', '/opt/compose.remote.yml', 'up', '-d'],
        ['docker-compose', '-f', '/opt/compose.remote.yml', 'up', '-d'],
    ]
    for command in compose_paths:
        if shutil.which(command[0]) is not None:
            return command
    return None


def ensure_vertica_container_running(timeout: float = 900.0) -> None:
    log(STEP_SEPARATOR)
    log('Ensuring Vertica container vertica_ce is running')

    command = _compose_command()
    deadline = time.time() + timeout
    last_status: tuple[Optional[str], Optional[str]] = (None, None)

    while time.time() < deadline:
        status = _docker_inspect('vertica_ce', '{{.State.Status}}')
        health = _docker_inspect('vertica_ce', '{{if .State.Health}}{{.State.Health.Status}}{{end}}')
        if status == 'running' and (not health or health == 'healthy'):
            log(f'Vertica container status: {status}, health: {health or "unknown"}')
            return

        if (status, health) != last_status:
            last_status = (status, health)
            log(f'Current Vertica container status: {status or "<absent>"}, health: {health or "<unknown>"}')

        if status is None:
            if command is None:
                raise SystemExit('Docker compose is not available to start vertica_ce container')
            run_command(command)
        elif status not in {'running', 'restarting'}:
            run_command(['docker', 'start', 'vertica_ce'])

        time.sleep(5)

    raise SystemExit(
        'Vertica container vertica_ce did not reach running & healthy state before timeout'
    )


def wait_for_port(host: str, port: int, timeout: float = 900.0) -> None:
    deadline = time.time() + timeout
    last_error: Optional[Exception] = None
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=5.0):
                return
        except OSError as exc:
            last_error = exc
            time.sleep(5)
    raise SystemExit(f'Port {host}:{port} did not become reachable: {last_error}')


def connect_and_query(label: str, host: str, user: str, password: str) -> None:
    log(STEP_SEPARATOR)
    log(f'[{label}] Connecting to Vertica at {host}:{DB_PORT} as {user!r}')
    config = {
        'host': host,
        'port': DB_PORT,
        'user': user,
        'password': password,
        'database': DB_NAME,
        'autocommit': True,
    }
    with vertica_python.connect(**config) as connection:
        cursor = connection.cursor()
        cursor.execute('SELECT 1')
        value = cursor.fetchone()
        if not value or value[0] != 1:
            raise SystemExit(f'Unexpected response from SELECT 1 during {label}')
        log(f'[{label}] SELECT 1 -> {value[0]}')


def main() -> int:
    log('Beginning in-instance Vertica smoke test with detailed diagnostics')
    hostname = socket.gethostname()
    local_ipv4 = fetch_metadata('meta-data/local-ipv4')
    public_ipv4 = fetch_metadata('meta-data/public-ipv4')
    log(f'Instance hostname: {hostname}')
    log(f'Instance local IPv4: {local_ipv4}')
    log(f'Instance public IPv4: {public_ipv4}')

    ensure_docker_service()
    ensure_vertica_container_running()
    wait_for_port('127.0.0.1', DB_PORT, timeout=900.0)
    log('Verified Vertica port 5433 is accepting TCP connections on localhost')

    run_command(['docker', 'ps', '--format', '{{.Names}}\t{{.Status}}'])
    image_result = run_command(['docker', 'inspect', '--format', '{{.Config.Image}}', 'vertica_ce'])
    image_name = image_result.stdout.strip()
    if image_name:
        log(f'Vertica container image: {image_name}')
        run_command(['docker', 'pull', image_name])
    run_command(['docker', 'inspect', '--format', '{{json .NetworkSettings.Ports}}', 'vertica_ce'])

    connect_and_query('dbadmin@localhost', '127.0.0.1', DBADMIN_USER, DBADMIN_PASSWORD)
    connect_and_query('bootstrap_admin@localhost', '127.0.0.1', ADMIN_USER, ADMIN_PASSWORD)

    try:
        connect_and_query('dbadmin@public_ip', public_ipv4, DBADMIN_USER, DBADMIN_PASSWORD)
    except Exception as exc:
        log(f'[dbadmin@public_ip] Connection attempt failed: {exc}')
        raise

    smoke_user = f'smoke_{uuid.uuid4().hex[:8]}'
    smoke_pass = uuid.uuid4().hex
    log(STEP_SEPARATOR)
    log(f'Creating smoke test user {smoke_user!r}')
    smoke_user_created = False
    with vertica_python.connect(host='127.0.0.1', port=DB_PORT, user=ADMIN_USER, password=ADMIN_PASSWORD, database=DB_NAME, autocommit=True) as admin_conn:
        admin_cursor = admin_conn.cursor()
        admin_cursor.execute(f'CREATE USER "{smoke_user}" IDENTIFIED BY %s', [smoke_pass])
        admin_cursor.execute(f'GRANT ALL PRIVILEGES ON DATABASE "{DB_NAME}" TO "{smoke_user}"')
        admin_cursor.execute(f'GRANT USAGE ON SCHEMA PUBLIC TO "{smoke_user}"')
        admin_cursor.execute(f'GRANT ALL PRIVILEGES ON SCHEMA PUBLIC TO "{smoke_user}"')
        smoke_user_created = True

    try:
        connect_and_query('smoke_user@localhost', '127.0.0.1', smoke_user, smoke_pass)
    finally:
        if smoke_user_created:
            log(STEP_SEPARATOR)
            log(f'Dropping smoke test user {smoke_user!r}')
            with vertica_python.connect(host='127.0.0.1', port=DB_PORT, user=ADMIN_USER, password=ADMIN_PASSWORD, database=DB_NAME, autocommit=True) as admin_conn:
                admin_conn.cursor().execute(f'DROP USER "{smoke_user}" CASCADE')

    log(STEP_SEPARATOR)
    log('All smoke test checks completed successfully')
    log('SMOKE_TEST_SUCCESS')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
