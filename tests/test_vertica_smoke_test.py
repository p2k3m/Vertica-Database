import importlib
import os
from datetime import datetime, timedelta, timezone
from typing import Optional
from pathlib import Path
import subprocess

import pytest


os.environ.setdefault('ADMIN_USER', 'test-admin')
os.environ.setdefault('ADMIN_PASSWORD', 'test-password')

smoke = importlib.import_module('scripts.vertica_smoke_test')


def _set_fixed_now(monkeypatch, moment: datetime) -> None:
    original_datetime = smoke.datetime

    class FixedDatetime(original_datetime):
        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            return moment if tz else moment.replace(tzinfo=None)

    monkeypatch.setattr(smoke, 'datetime', FixedDatetime)


@pytest.mark.parametrize(
    'started_at, expected_dt',
    [
        (
            '2024-01-01T00:00:09.123456789Z',
            datetime(2024, 1, 1, 0, 0, 9, 123456, tzinfo=timezone.utc),
        ),
        (
            '2024-01-01T00:00:09Z',
            datetime(2024, 1, 1, 0, 0, 9, tzinfo=timezone.utc),
        ),
        (
            '2024-01-01T05:30:09.987654321+05:30',
            datetime(2024, 1, 1, 5, 30, 9, 987654, tzinfo=timezone(timedelta(hours=5, minutes=30))),
        ),
    ],
)
def test_container_uptime_seconds_handles_high_precision(monkeypatch, started_at, expected_dt):
    reference_now = datetime(2024, 1, 1, 0, 0, 10, tzinfo=timezone.utc)
    _set_fixed_now(monkeypatch, reference_now)
    monkeypatch.setattr(smoke, '_docker_inspect', lambda container, template: started_at)

    uptime = smoke._container_uptime_seconds('vertica_ce')

    expected = max(0.0, (reference_now - expected_dt).total_seconds())

    assert uptime == pytest.approx(expected, rel=1e-9)


@pytest.mark.parametrize(
    'raw, normalized',
    [
        ('2024-01-01T00:00:09.123456789Z', '2024-01-01T00:00:09.123456+00:00'),
        ('2024-01-01T00:00:09Z', '2024-01-01T00:00:09.000000+00:00'),
        ('2024-01-01T05:30:09.987654321+05:30', '2024-01-01T05:30:09.987654+05:30'),
    ],
)
def test_normalize_docker_timestamp(raw, normalized):
    assert smoke._normalize_docker_timestamp(raw) == normalized


def test_container_uptime_seconds_returns_none_for_invalid(monkeypatch):
    _set_fixed_now(monkeypatch, datetime(2024, 1, 1, tzinfo=timezone.utc))
    monkeypatch.setattr(smoke, '_docker_inspect', lambda container, template: 'invalid')

    assert smoke._container_uptime_seconds('vertica_ce') is None


def test_container_uptime_seconds_handles_zero_timestamp(monkeypatch):
    reference_now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    _set_fixed_now(monkeypatch, reference_now)
    monkeypatch.setattr(
        smoke,
        '_docker_inspect',
        lambda container, template: '0001-01-01T00:00:00Z',
    )

    assert smoke._container_uptime_seconds('vertica_ce') == 0.0


def test_ensure_vertica_respects_unhealthy_grace(monkeypatch):
    current_time = {'value': 0.0}

    def fake_time() -> float:
        return current_time['value']

    def fake_sleep(seconds: float) -> None:
        current_time['value'] += seconds

    calls: list[list[str]] = []

    def fake_run_command(command: list[str]):  # pragma: no cover - should not run
        calls.append(command)
        raise AssertionError('run_command should not be invoked during grace period')

    health_states = iter(['unhealthy', 'healthy'])

    def fake_docker_inspect(container: str, template: str) -> Optional[str]:
        if template == '{{.State.Status}}':
            return 'running'
        if template == '{{if .State.Health}}{{.State.Health.Status}}{{end}}':
            return next(health_states)
        raise AssertionError(f'Unexpected template: {template}')

    monkeypatch.setattr(smoke, '_ensure_docker_compose_cli', lambda: None)
    monkeypatch.setattr(smoke, '_container_uptime_seconds', lambda container: 600.0)
    monkeypatch.setattr(smoke, '_docker_inspect', fake_docker_inspect)
    monkeypatch.setattr(smoke, 'run_command', fake_run_command)
    monkeypatch.setattr(smoke.time, 'time', fake_time)
    monkeypatch.setattr(smoke.time, 'sleep', fake_sleep)
    monkeypatch.setattr(smoke, 'log', lambda message: None)

    smoke.ensure_vertica_container_running(timeout=30.0, compose_timeout=0.0)

    assert not calls


def test_ensure_vertica_resets_data_directories(monkeypatch):
    current_time = {'value': 0.0}

    def fake_time() -> float:
        return current_time['value']

    def fake_sleep(seconds: float) -> None:
        current_time['value'] += seconds

    compose_calls: list[bool] = []
    reset_calls: list[bool] = []

    def fake_run_command(command: list[str]) -> None:
        if command[:2] == ['docker', 'restart']:
            return
        raise AssertionError(f'Unexpected command: {command}')

    def fake_docker_inspect(container: str, template: str) -> Optional[str]:
        if template == '{{.State.Status}}':
            return 'running'
        if template == '{{if .State.Health}}{{.State.Health.Status}}{{end}}':
            return 'healthy' if reset_calls else 'unhealthy'
        raise AssertionError(f'Unexpected template: {template}')

    def fake_compose_up(path: Path, force_recreate: bool = False) -> None:
        compose_calls.append(force_recreate)

    def fake_reset() -> bool:
        reset_calls.append(True)
        return True

    monkeypatch.setattr(smoke, '_ensure_docker_compose_cli', lambda: None)
    monkeypatch.setattr(smoke, '_compose_file', lambda: Path('compose.yaml'))
    monkeypatch.setattr(smoke, '_ensure_ecr_login_if_needed', lambda path: None)
    monkeypatch.setattr(smoke, '_compose_up', fake_compose_up)
    monkeypatch.setattr(smoke, '_container_uptime_seconds', lambda container: 1000.0)
    monkeypatch.setattr(smoke, '_docker_inspect', fake_docker_inspect)
    monkeypatch.setattr(smoke, '_reset_vertica_data_directories', fake_reset)
    monkeypatch.setattr(smoke, '_sanitize_vertica_data_directories', lambda: None)
    monkeypatch.setattr(smoke, '_log_container_tail', lambda container, tail=200: None)
    monkeypatch.setattr(smoke, '_log_health_log_entries', lambda container, count: count)
    monkeypatch.setattr(smoke, 'run_command', fake_run_command)
    monkeypatch.setattr(smoke, 'log', lambda message: None)
    monkeypatch.setattr(smoke.time, 'time', fake_time)
    monkeypatch.setattr(smoke.time, 'sleep', fake_sleep)
    monkeypatch.setattr(smoke, 'UNHEALTHY_HEALTHCHECK_GRACE_PERIOD_SECONDS', 15.0)

    smoke.ensure_vertica_container_running(timeout=1000.0, compose_timeout=0.0)

    assert compose_calls == [True, True, True]
    assert reset_calls == [True]


def test_seed_default_admintools_conf(tmp_path, monkeypatch):
    logs: list[str] = []

    monkeypatch.setattr(smoke, 'log', logs.append)

    config_dir = tmp_path / 'config'
    smoke._seed_default_admintools_conf(config_dir)

    conf_path = config_dir / 'admintools.conf'
    assert conf_path.exists()
    content = conf_path.read_text()
    assert '[Configuration]' in content
    assert 'admintools_config_version = 110' in content
    assert 'hosts = 127.0.0.1' in content
    assert 'node0001 = 127.0.0.1' in content
    assert conf_path.stat().st_mode & 0o777 == 0o666


def test_seed_default_admintools_conf_is_idempotent(tmp_path, monkeypatch):
    config_dir = tmp_path / 'config'
    config_dir.mkdir()

    smoke._seed_default_admintools_conf(config_dir)

    existing = config_dir / 'admintools.conf'
    original_content = existing.read_text()

    smoke._seed_default_admintools_conf(config_dir)

    assert existing.read_text() == original_content


def test_seed_default_admintools_conf_rebuilds_invalid_file(tmp_path, monkeypatch):
    logs: list[str] = []

    monkeypatch.setattr(smoke, 'log', logs.append)

    config_dir = tmp_path / 'config'
    config_dir.mkdir()
    existing = config_dir / 'admintools.conf'
    existing.write_text('custom')

    smoke._seed_default_admintools_conf(config_dir)

    content = existing.read_text()
    assert '[Configuration]' in content
    assert 'hosts = 127.0.0.1' in content
    assert any('attempting to rebuild it with safe defaults' in entry for entry in logs)


def test_ensure_container_admintools_conf_readable_adjusts(monkeypatch):
    logs: list[str] = []
    calls: list[list[str]] = []

    def fake_log(message: str) -> None:
        logs.append(message)

    def fake_which(name: str) -> Optional[str]:
        return '/usr/bin/docker' if name == 'docker' else None

    def fake_run(command, capture_output=True, text=True):
        calls.append(command)
        script = command[-1]
        if 'test -e' in script:
            return subprocess.CompletedProcess(command, 0, stdout='', stderr='')
        if 'test -r' in script:
            return subprocess.CompletedProcess(command, 1, stdout='', stderr='')
        if 'chmod' in script:
            return subprocess.CompletedProcess(command, 0, stdout='', stderr='')
        raise AssertionError(f'Unexpected command: {command}')

    monkeypatch.setattr(smoke, 'log', fake_log)
    monkeypatch.setattr(smoke.shutil, 'which', fake_which)
    monkeypatch.setattr(smoke.subprocess, 'run', fake_run)

    adjusted = smoke._ensure_container_admintools_conf_readable('vertica_ce')

    assert adjusted is True
    assert any('Detected unreadable admintools.conf' in entry for entry in logs)
    assert any('chmod a+r' in cmd[-1] for cmd in calls)


def test_ensure_container_admintools_conf_readable_noop(monkeypatch):
    logs: list[str] = []

    def fake_log(message: str) -> None:
        logs.append(message)

    def fake_which(name: str) -> Optional[str]:
        return '/usr/bin/docker' if name == 'docker' else None

    def fake_run(command, capture_output=True, text=True):
        script = command[-1]
        if 'test -e' in script:
            return subprocess.CompletedProcess(command, 0, stdout='', stderr='')
        if 'test -r' in script:
            return subprocess.CompletedProcess(command, 0, stdout='', stderr='')
        raise AssertionError(f'Unexpected command: {command}')

    monkeypatch.setattr(smoke, 'log', fake_log)
    monkeypatch.setattr(smoke.shutil, 'which', fake_which)
    monkeypatch.setattr(smoke.subprocess, 'run', fake_run)

    adjusted = smoke._ensure_container_admintools_conf_readable('vertica_ce')

    assert adjusted is False
    assert not any('Detected unreadable admintools.conf' in entry for entry in logs)


def test_reset_vertica_data_directories_handles_multiple_mount_points(tmp_path, monkeypatch):
    varlib = tmp_path / 'var_lib'
    data = tmp_path / 'data'
    for base in (varlib, data):
        (base / 'vertica').mkdir(parents=True)

    monkeypatch.setattr(
        smoke,
        'VERTICA_DATA_DIRECTORIES',
        [varlib, data],
        raising=False,
    )

    removed = smoke._reset_vertica_data_directories()

    assert removed is True
    assert not (varlib / 'vertica').exists()
    assert not (data / 'vertica').exists()


def test_connect_and_query_prefers_tls(monkeypatch):
    captured_config: dict[str, object] = {}

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            class Cursor:
                def execute(self, query):
                    return None

                def fetchone(self):
                    return (1,)

            return Cursor()

    def fake_connect(**config):
        captured_config.update(config)
        return FakeConnection()

    monkeypatch.setattr(smoke, 'vertica_python', type('Module', (), {'connect': fake_connect}))

    assert smoke.connect_and_query('label', 'host', 'user', 'password', attempts=1, delay=0)

    assert captured_config['tlsmode'] == 'prefer'


def test_connect_and_query_respects_env_tlsmode(monkeypatch):
    captured_config: dict[str, object] = {}

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            class Cursor:
                def execute(self, query):
                    return None

                def fetchone(self):
                    return (1,)

            return Cursor()

    def fake_connect(**config):
        captured_config.update(config)
        return FakeConnection()

    monkeypatch.setenv('VERTICA_TLSMODE', 'require')
    monkeypatch.setattr(smoke, 'vertica_python', type('Module', (), {'connect': fake_connect}))

    assert smoke.connect_and_query('label', 'host', 'user', 'password', attempts=1, delay=0)

    assert captured_config['tlsmode'] == 'require'


def test_connect_and_query_nonfatal(monkeypatch):
    messages: list[str] = []

    def fake_log(message: str) -> None:
        messages.append(message)

    class FakeErrorsModule:
        class ConnectionError(Exception):
            pass

    def fake_connect(**config):
        raise FakeErrorsModule.ConnectionError('boom')

    monkeypatch.setattr(smoke, 'log', fake_log)
    monkeypatch.setattr(smoke, 'time', type('Module', (), {'sleep': lambda _: None}))
    monkeypatch.setattr(
        smoke,
        'vertica_python',
        type('Module', (), {'connect': fake_connect, 'errors': FakeErrorsModule}),
    )

    result = smoke.connect_and_query(
        'label', 'host', 'user', 'password', attempts=2, delay=0, fatal=False
    )

    assert result is False
    assert any('Failed to connect to Vertica' in message for message in messages)


def test_ecr_login_handles_aws_cli_failure(monkeypatch):
    messages: list[str] = []

    def fake_log(message: str) -> None:
        messages.append(message)

    def fake_run_aws_cli(args):
        raise subprocess.CalledProcessError(1, args)

    monkeypatch.setattr(smoke, 'log', fake_log)
    monkeypatch.setattr(smoke, '_run_aws_cli', fake_run_aws_cli)
    monkeypatch.setattr(smoke.shutil, 'which', lambda name: '/usr/bin/aws' if name == 'aws' else None)
    smoke._ECR_LOGIN_RESULTS.clear()

    with pytest.raises(SystemExit) as excinfo:
        smoke._ensure_ecr_login_for_image(
            '123456789012.dkr.ecr.us-east-1.amazonaws.com/repo:tag'
        )

    assert 'Failed to retrieve ECR login password' in str(excinfo.value)
    assert any('Attempting ECR login for registry' in msg for msg in messages)


def test_ecr_login_handles_docker_login_failure(monkeypatch):
    messages: list[str] = []

    def fake_log(message: str) -> None:
        messages.append(message)

    def fake_run_aws_cli(args):
        return subprocess.CompletedProcess(args, 0, stdout='token', stderr='')

    def fake_subprocess_run(command, **kwargs):
        return subprocess.CompletedProcess(command, 1, stdout='', stderr='error')

    monkeypatch.setattr(smoke, 'log', fake_log)
    monkeypatch.setattr(smoke, '_run_aws_cli', fake_run_aws_cli)
    monkeypatch.setattr(smoke.subprocess, 'run', fake_subprocess_run)
    monkeypatch.setattr(smoke.shutil, 'which', lambda name: '/usr/bin/aws' if name == 'aws' else None)
    smoke._ECR_LOGIN_RESULTS.clear()

    with pytest.raises(SystemExit) as excinfo:
        smoke._ensure_ecr_login_for_image(
            '123456789012.dkr.ecr.us-east-1.amazonaws.com/repo:tag'
        )

    assert 'Docker login for 123456789012.dkr.ecr.us-east-1.amazonaws.com failed' in str(excinfo.value)
    assert any('Logging in to Docker registry' in msg for msg in messages)


def test_pull_image_failure_is_non_fatal(monkeypatch):
    messages: list[str] = []

    def fake_log(message: str) -> None:
        messages.append(message)

    def failing_run_command(command):
        raise SystemExit('Command failed with exit code 1')

    monkeypatch.setattr(smoke, 'log', fake_log)
    monkeypatch.setattr(smoke, 'run_command', failing_run_command)
    smoke._ECR_LOGIN_RESULTS.clear()

    smoke._pull_image_if_possible('my-image:latest')

    assert any('Docker pull for my-image:latest failed' in msg for msg in messages)


def test_compose_up_removes_stale_vertica_container(monkeypatch):
    compose_path = Path('/opt/compose.remote.yml')
    run_calls: list[list[str]] = []
    removal_calls: list[int] = []

    def fake_run_command(command: list[str]):
        run_calls.append(command)
        if len(run_calls) == 1:
            raise SystemExit('initial failure')

    def fake_remove() -> bool:
        removal_calls.append(1)
        return True

    monkeypatch.setattr(smoke, '_docker_compose_plugin_available', lambda: True)
    monkeypatch.setattr(smoke.shutil, 'which', lambda name: None)
    monkeypatch.setattr(smoke, 'run_command', fake_run_command)
    monkeypatch.setattr(smoke, '_remove_stale_vertica_container', fake_remove)
    monkeypatch.setattr(smoke, 'log', lambda message: None)

    smoke._compose_up(compose_path)

    assert len(run_calls) == 2
    assert len(removal_calls) == 1


def test_compose_up_raises_when_stale_container_removal_fails(monkeypatch):
    compose_path = Path('/opt/compose.remote.yml')
    run_calls: list[list[str]] = []
    removal_calls: list[int] = []

    def fake_run_command(command: list[str]):
        run_calls.append(command)
        raise SystemExit('persistent failure')

    def fake_remove() -> bool:
        removal_calls.append(1)
        return False

    monkeypatch.setattr(smoke, '_docker_compose_plugin_available', lambda: True)
    monkeypatch.setattr(smoke.shutil, 'which', lambda name: None)
    monkeypatch.setattr(smoke, 'run_command', fake_run_command)
    monkeypatch.setattr(smoke, '_remove_stale_vertica_container', fake_remove)
    monkeypatch.setattr(smoke, 'log', lambda message: None)

    with pytest.raises(SystemExit):
        smoke._compose_up(compose_path)

    assert len(run_calls) == 1
    assert len(removal_calls) == 1


def test_ensure_primary_admin_user_creates_user(monkeypatch):
    executed: list[tuple[str, tuple | list | None]] = []

    class FakeCursor:
        def __init__(self):
            self._last_query: Optional[str] = None

        def execute(self, query, params=None):
            executed.append((query, params))
            self._last_query = query

        def fetchone(self):
            if self._last_query and 'SELECT 1 FROM users' in self._last_query:
                return None
            return (1,)

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return FakeCursor()

    monkeypatch.setattr(smoke.vertica_python, 'connect', lambda **config: FakeConnection())

    smoke._ensure_primary_admin_user('dbadmin', '', 'appadmin', 'secret')

    statements = [statement for statement, _ in executed]
    assert any(statement.startswith('CREATE USER "appadmin"') for statement in statements)
    assert any('GRANT ALL PRIVILEGES ON DATABASE "VMart"' in statement for statement in statements)


def test_ensure_primary_admin_user_rotates_password(monkeypatch):
    executed: list[tuple[str, tuple | list | None]] = []

    class FakeCursor:
        def __init__(self):
            self._last_query: Optional[str] = None

        def execute(self, query, params=None):
            executed.append((query, params))
            self._last_query = query

        def fetchone(self):
            if self._last_query and 'SELECT 1 FROM users' in self._last_query:
                return (1,)
            return (1,)

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return FakeCursor()

    monkeypatch.setattr(smoke.vertica_python, 'connect', lambda **config: FakeConnection())

    smoke._ensure_primary_admin_user('dbadmin', '', 'appadmin', 'secret')

    statements = [statement for statement, _ in executed]
    assert any(statement.startswith('ALTER USER "appadmin"') for statement in statements)
    assert all(not statement.startswith('CREATE USER') for statement in statements[1:])


def test_ensure_primary_admin_user_skips_when_matching_bootstrap(monkeypatch):
    called = False

    def fake_connect(**config):
        nonlocal called
        called = True
        raise AssertionError('connect should not be called')

    monkeypatch.setattr(smoke.vertica_python, 'connect', fake_connect)

    smoke._ensure_primary_admin_user('appadmin', '', 'appadmin', 'secret')

    assert called is False
