import json
import os
import platform
import re
import shlex
import shutil
import socket
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
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
UNHEALTHY_HEALTHCHECK_GRACE_PERIOD_SECONDS = 300.0


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


def _container_uptime_seconds(container: str) -> Optional[float]:
    """Return the container uptime in seconds, if available."""

    started_at = _docker_inspect(container, '{{.State.StartedAt}}')
    if not started_at:
        return None

    normalized = started_at.replace('Z', '+00:00')
    try:
        started_dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None

    now = datetime.now(timezone.utc)
    if started_dt.tzinfo is None:
        started_dt = started_dt.replace(tzinfo=timezone.utc)

    return max(0.0, (now - started_dt).total_seconds())


def _docker_compose_plugin_available() -> bool:
    """Return True if ``docker compose`` is usable via the CLI plugin."""

    result = subprocess.run(
        ['docker', 'compose', 'version'],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def _ensure_docker_compose_cli() -> None:
    """Ensure that either ``docker compose`` or ``docker-compose`` is available."""

    if _docker_compose_plugin_available() or shutil.which('docker-compose') is not None:
        return

    log(STEP_SEPARATOR)
    log('Docker Compose CLI not available; attempting installation')

    install_sequences: list[list[list[str]]] = []

    if shutil.which('dnf'):
        install_sequences.append([
            ['dnf', 'install', '-y', 'docker-compose-plugin'],
        ])

    if shutil.which('yum'):
        install_sequences.append([
            ['yum', 'install', '-y', 'docker-compose-plugin'],
        ])

    if shutil.which('apt-get'):
        install_sequences.append([
            ['apt-get', 'update'],
            ['apt-get', 'install', '-y', 'docker-compose-plugin'],
        ])

    pip_executable = shutil.which('pip3') or shutil.which('pip')
    if pip_executable:
        install_sequences.append([
            [pip_executable, 'install', '--quiet', '--upgrade', 'docker-compose'],
        ])

    for sequence in install_sequences:
        commands_preview = ' && '.join(' '.join(part) for part in sequence)
        log(STEP_SEPARATOR)
        log(f'Attempting to install Docker Compose using: {commands_preview}')
        try:
            for command in sequence:
                run_command(command)
        except SystemExit as exc:
            log(f'Docker Compose installation attempt failed: {exc}')
            continue

        if _docker_compose_plugin_available() or shutil.which('docker-compose') is not None:
            return

    if _download_docker_compose_binary():
        return

    if not (_docker_compose_plugin_available() or shutil.which('docker-compose') is not None):
        raise SystemExit('Docker Compose CLI is not available after installation attempts')


def _download_docker_compose_binary(version: str = 'v2.27.1') -> bool:
    """Attempt to download the standalone Docker Compose binary as a fallback."""

    system = sys.platform
    if not system.startswith('linux'):
        return False

    architecture = platform.machine().lower()
    arch_map = {
        'x86_64': 'x86_64',
        'amd64': 'x86_64',
        'aarch64': 'aarch64',
        'arm64': 'aarch64',
    }

    mapped_arch = arch_map.get(architecture)
    if mapped_arch is None:
        log(f'Unsupported architecture for Docker Compose binary download: {architecture}')
        return False

    url = (
        'https://github.com/docker/compose/releases/download/'
        f'{version}/docker-compose-linux-{mapped_arch}'
    )
    destination = Path('/usr/local/bin/docker-compose')

    log(STEP_SEPARATOR)
    log(f'Attempting to download Docker Compose binary from {url}')

    try:
        with urlopen(url, timeout=60) as response:
            binary = response.read()
    except Exception as exc:  # pragma: no cover - network failure path
        log(f'Failed to download Docker Compose binary: {exc}')
        return False

    try:
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(binary)
        destination.chmod(0o755)
    except Exception as exc:  # pragma: no cover - filesystem failure path
        log(f'Failed to write Docker Compose binary to {destination}: {exc}')
        return False

    if shutil.which('docker-compose') is None:
        log('Docker Compose binary download completed but command is still unavailable')
        return False

    log('Docker Compose binary installed successfully')
    return True


_COMPOSE_FILE_CANDIDATES = [
    Path('/opt/compose.remote.yml'),
    Path('/opt/compose.remote.yaml'),
    Path('/opt/compose.yml'),
    Path('/opt/compose.yaml'),
]

_USER_DATA_PATHS = [
    Path('/var/lib/cloud/instance/user-data.txt'),
    Path('/var/lib/cloud/data/user-data'),
    *Path('/var/lib/cloud/instances').glob('*/user-data.txt'),
]


def _reconstruct_compose_file_from_user_data() -> Optional[Path]:
    """Attempt to recreate the compose file from persisted user-data."""

    marker = "cat >/opt/compose.remote.yml <<'YAML'"
    terminator = "\nYAML"

    for user_data_path in _USER_DATA_PATHS:
        try:
            if not user_data_path.is_file():
                continue
            content = user_data_path.read_text()
        except Exception as exc:  # pragma: no cover - filesystem access failure path
            log(f'Failed to read {user_data_path}: {exc}')
            continue

        start_index = content.find(marker)
        if start_index == -1:
            continue

        compose_payload = content[start_index + len(marker) :]
        end_index = compose_payload.find(terminator)
        if end_index == -1:
            continue

        compose_payload = compose_payload[:end_index].lstrip('\n')
        destination = Path('/opt/compose.remote.yml')

        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text(compose_payload + '\n')
            destination.chmod(0o644)
        except Exception as exc:  # pragma: no cover - filesystem failure path
            log(f'Failed to write compose file to {destination}: {exc}')
            return None

        log(f'Reconstructed compose file at {destination} from {user_data_path}')
        return destination

    return None


def _compose_file() -> Optional[Path]:
    """Return the compose file path if present on disk."""

    for candidate in _COMPOSE_FILE_CANDIDATES:
        if candidate.is_file():
            return candidate

    return _reconstruct_compose_file_from_user_data()

    return None


def _compose_up(compose_file: Path, *, force_recreate: bool = False) -> None:
    """Run ``docker compose up`` (or ``docker-compose up``) for ``compose_file``."""

    extra_args: list[str] = ['--force-recreate'] if force_recreate else []
    commands: list[list[str]] = []

    if _docker_compose_plugin_available():
        commands.append(
            ['docker', 'compose', '-f', str(compose_file), 'up', '-d', *extra_args]
        )

    docker_compose_exe = shutil.which('docker-compose')
    if docker_compose_exe is not None:
        commands.append(
            [docker_compose_exe, '-f', str(compose_file), 'up', '-d', *extra_args]
        )

    if not commands:
        raise SystemExit('Docker Compose CLI is not available to manage Vertica container')

    last_error: Optional[BaseException] = None
    for command in commands:
        try:
            run_command(command)
        except SystemExit as exc:
            last_error = exc
            continue
        else:
            return

    if last_error is not None:
        raise last_error


_ECR_PRIVATE_RE = re.compile(
    r'^(?P<registry>[0-9]+\.dkr\.ecr\.(?P<region>[a-z0-9-]+)\.amazonaws\.com)(?P<path>/.+)$'
)
_ECR_PUBLIC_RE = re.compile(r'^(?P<registry>public\.ecr\.aws)(?P<path>/.+)$')
_ECR_LOGIN_ATTEMPTS: set[str] = set()
_URLLIB3_REPAIR_ATTEMPTED = False
_PYTHON_SITE_PACKAGES_RE = re.compile(
    r'File "(?P<path>/usr/(?:local/)?lib(?:64)?/python[0-9.]+/site-packages)/'
)


def _aws_cli_import_check() -> bool:
    aws_executable = shutil.which('aws')
    if not aws_executable:
        return False

    result = subprocess.run(
        [aws_executable, '--version'],
        capture_output=True,
        text=True,
    )

    if result.returncode == 0:
        return True

    combined_output = ''.join((result.stdout or '', result.stderr or ''))
    if combined_output.strip() and "ModuleNotFoundError: No module named 'urllib3'" not in combined_output:
        log(combined_output.rstrip())

    return "ModuleNotFoundError: No module named 'urllib3'" not in combined_output


def _repair_missing_urllib3(failure_output: Optional[str] = None) -> bool:
    """Attempt to reinstall urllib3 when the AWS CLI import fails."""

    global _URLLIB3_REPAIR_ATTEMPTED

    if _URLLIB3_REPAIR_ATTEMPTED:
        return False

    _URLLIB3_REPAIR_ATTEMPTED = True

    log(STEP_SEPARATOR)
    log('Attempting to reinstall urllib3 for AWS CLI compatibility')

    aws_executable = shutil.which('aws')
    python_from_aws: Optional[str] = None

    if aws_executable:
        try:
            first_line = Path(aws_executable).read_text(errors='ignore').splitlines()[0]
        except (OSError, IndexError):
            first_line = ''
        if first_line.startswith('#!'):
            shebang_cmd = first_line[2:].strip()
            if shebang_cmd:
                candidate = shlex.split(shebang_cmd)[0]
                if candidate and Path(candidate).exists():
                    python_from_aws = candidate

    install_commands: list[list[str]] = []
    if python_from_aws:
        install_commands.append([python_from_aws, '-m', 'pip'])

    pip3_exe = shutil.which('pip3')
    if pip3_exe and (not install_commands or pip3_exe not in {cmd[0] for cmd in install_commands}):
        install_commands.append([pip3_exe])

    pip_exe = shutil.which('pip')
    if pip_exe and (not install_commands or pip_exe not in {cmd[0] for cmd in install_commands}):
        install_commands.append([pip_exe])

    target_paths: set[Path] = set()
    if failure_output:
        for match in _PYTHON_SITE_PACKAGES_RE.finditer(failure_output):
            try:
                target_paths.add(Path(match.group('path')))
            except (OSError, ValueError):
                continue

    for path in list(target_paths):
        try:
            path.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            log(f'Unable to create target directory {path}: {exc}')
            target_paths.discard(path)

    if not install_commands:
        log('Unable to repair missing urllib3 dependency because pip is unavailable')
        return False

    for base_command in install_commands:
        variants: list[list[str]] = [[]]
        variants.extend([['--target', str(path)] for path in target_paths])

        for variant in variants:
            for allow_break in (False, True):
                extra_args = [*variant]
                if allow_break:
                    extra_args.append('--break-system-packages')

                command = [*base_command, 'install', '--quiet', 'urllib3<2', *extra_args]
                result = subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                )
                if result.returncode == 0:
                    if _aws_cli_import_check():
                        return True
                    continue

                if result.stdout:
                    log(result.stdout.rstrip())
                if result.stderr:
                    log(f'[stderr] {result.stderr.rstrip()}')

                if (
                    not allow_break
                    and 'externally-managed-environment' in (result.stderr or '')
                ):
                    continue

                break

    package_manager_sequences: list[list[list[str]]] = []

    if shutil.which('dnf'):
        package_manager_sequences.append([
            ['dnf', 'install', '-y', 'python3-urllib3'],
        ])

    if shutil.which('yum'):
        package_manager_sequences.append([
            ['yum', 'install', '-y', 'python3-urllib3'],
        ])

    if shutil.which('apt-get'):
        package_manager_sequences.append([
            ['apt-get', 'update'],
            ['apt-get', 'install', '-y', 'python3-urllib3'],
        ])

    for sequence in package_manager_sequences:
        commands_preview = ' && '.join(' '.join(part) for part in sequence)
        log(STEP_SEPARATOR)
        log('Attempting to install urllib3 using: ' + commands_preview)
        for command in sequence:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
            )
            if result.stdout:
                log(result.stdout.rstrip())
            if result.stderr:
                log(f'[stderr] {result.stderr.rstrip()}')
            if result.returncode != 0:
                break
        else:
            if _aws_cli_import_check():
                return True

    log('Failed to reinstall urllib3 dependency')
    return False


def _run_aws_cli(args: list[str]) -> subprocess.CompletedProcess[str]:
    """Run an AWS CLI command, attempting to repair missing urllib3 once."""

    needs_retry = True

    while True:
        try:
            return subprocess.run(
                args,
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as exc:
            combined_output = ''.join((exc.stdout or '', exc.stderr or ''))
            if (
                needs_retry
                and "ModuleNotFoundError: No module named 'urllib3'" in combined_output
                and _repair_missing_urllib3(combined_output)
            ):
                needs_retry = False
                continue

            if exc.stdout:
                log(exc.stdout.rstrip())
            if exc.stderr:
                log(f'[stderr] {exc.stderr.rstrip()}')
            raise


def _extract_compose_image(compose_file: Path) -> Optional[str]:
    try:
        for line in compose_file.read_text().splitlines():
            stripped = line.strip()
            if stripped.startswith('image:'):
                return stripped.split(':', 1)[1].strip()
    except Exception as exc:  # pragma: no cover - filesystem failure path
        log(f'Failed to read compose file {compose_file}: {exc}')
    return None


def _ensure_ecr_login_for_image(image_name: str) -> None:
    match = _ECR_PRIVATE_RE.match(image_name)
    registry: Optional[str]
    region: Optional[str]

    if match:
        registry = match.group('registry')
        region = match.group('region')
    else:
        match_public = _ECR_PUBLIC_RE.match(image_name)
        if not match_public:
            return
        registry = match_public.group('registry')
        region = 'us-east-1'

    if registry in _ECR_LOGIN_ATTEMPTS:
        return

    _ECR_LOGIN_ATTEMPTS.add(registry)

    if shutil.which('aws') is None:
        log(
            'AWS CLI is not available on the instance; unable to perform docker login for '
            f'{registry}'
        )
        return

    if match:
        log(STEP_SEPARATOR)
        log(f'Attempting ECR login for registry {registry} in region {region}')
        try:
            password_result = _run_aws_cli(
                ['aws', 'ecr', 'get-login-password', '--region', region]
            )
        except subprocess.CalledProcessError as exc:  # pragma: no cover - runtime failure path
            raise SystemExit('Failed to retrieve ECR login password') from exc
    else:
        log(STEP_SEPARATOR)
        log(f'Attempting ECR Public login for registry {registry}')
        try:
            password_result = _run_aws_cli(
                ['aws', 'ecr-public', 'get-login-password', '--region', region]
            )
        except subprocess.CalledProcessError as exc:  # pragma: no cover - runtime failure path
            raise SystemExit('Failed to retrieve ECR Public login password') from exc

    password = password_result.stdout.strip()
    if not password:
        log('ECR login password command returned empty output; skipping docker login')
        return

    log(STEP_SEPARATOR)
    log(f'Logging in to Docker registry {registry}')
    login_result = subprocess.run(
        ['docker', 'login', '--username', 'AWS', '--password-stdin', registry],
        input=password,
        capture_output=True,
        text=True,
    )
    if login_result.stdout:
        log(login_result.stdout.rstrip())
    if login_result.stderr:
        log(f'[stderr] {login_result.stderr.rstrip()}')
    if login_result.returncode != 0:
        raise SystemExit(f'Docker login for {registry} failed with exit code {login_result.returncode}')


def _ensure_ecr_login_if_needed(compose_file: Path) -> None:
    image_name = _extract_compose_image(compose_file)
    if not image_name:
        return

    _ensure_ecr_login_for_image(image_name)


def ensure_vertica_container_running(
    timeout: float = 1800.0, compose_timeout: float = 300.0
) -> None:
    log(STEP_SEPARATOR)
    log('Ensuring Vertica container vertica_ce is running')

    _ensure_docker_compose_cli()
    deadline = time.time() + timeout
    last_status: tuple[Optional[str], Optional[str]] = (None, None)
    compose_file: Optional[Path] = None
    compose_missing_logged = False

    restart_attempts = 0
    recreate_attempts = 0

    compose_deadline = time.time() + compose_timeout

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
            if compose_file is None:
                compose_file = _compose_file()
            if compose_file is None:
                if not compose_missing_logged:
                    compose_missing_logged = True
                    log(
                        'Compose file not yet available; checked: '
                        + ', '.join(str(candidate) for candidate in _COMPOSE_FILE_CANDIDATES)
                    )
                if time.time() >= compose_deadline:
                    raise SystemExit(
                        'Compose file was not detected within the allotted '
                        f'{compose_timeout:.0f} seconds; aborting'
                    )
                time.sleep(5)
                continue

            if compose_missing_logged:
                log('Compose file detected; attempting to start vertica_ce via docker compose')
                compose_missing_logged = False
            _ensure_ecr_login_if_needed(compose_file)
            _compose_up(compose_file)
            restart_attempts = 0
            recreate_attempts = 0
        elif status not in {'running', 'restarting'}:
            run_command(['docker', 'start', 'vertica_ce'])
            restart_attempts = 0
            recreate_attempts = 0
        elif health == 'unhealthy':
            uptime = _container_uptime_seconds('vertica_ce')
            if uptime is not None and uptime < UNHEALTHY_HEALTHCHECK_GRACE_PERIOD_SECONDS:
                log(
                    'Vertica container health reported unhealthy but uptime '
                    f'{uptime:.0f}s is within grace period; waiting for recovery'
                )
                time.sleep(10)
                continue

            if restart_attempts < 3:
                log('Vertica container health check reported unhealthy; restarting container')
                run_command(['docker', 'restart', 'vertica_ce'])
                restart_attempts += 1
                time.sleep(10)
                continue

            if compose_file is None:
                compose_file = _compose_file()
            if compose_file is not None and recreate_attempts < 2:
                log('Vertica container remains unhealthy; recreating via docker compose')
                _ensure_ecr_login_if_needed(compose_file)
                _compose_up(compose_file, force_recreate=True)
                recreate_attempts += 1
                restart_attempts = 0
                time.sleep(15)
                continue

            log('Vertica container is still unhealthy after recovery attempts; collecting diagnostics')
            try:
                run_command(['docker', 'ps', '--filter', 'name=vertica_ce'])
            except SystemExit:
                pass
            try:
                run_command(['docker', 'logs', '--tail', '200', 'vertica_ce'])
            except SystemExit:
                pass
            raise SystemExit(
                'Vertica container vertica_ce remained unhealthy after restart and recreate attempts'
            )

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
    wait_for_port('127.0.0.1', DB_PORT)
    log('Verified Vertica port 5433 is accepting TCP connections on localhost')

    run_command(['docker', 'ps', '--format', '{{.Names}}\t{{.Status}}'])
    image_result = run_command(['docker', 'inspect', '--format', '{{.Config.Image}}', 'vertica_ce'])
    image_name = image_result.stdout.strip()
    if image_name:
        log(f'Vertica container image: {image_name}')
        _ensure_ecr_login_for_image(image_name)
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
