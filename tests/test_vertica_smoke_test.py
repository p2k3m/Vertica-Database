import importlib
import os
import shlex
import subprocess
import textwrap
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from types import SimpleNamespace

import pytest


os.environ.setdefault('ADMIN_USER', 'test-admin')
os.environ.setdefault('ADMIN_PASSWORD', 'test-password')

smoke = importlib.import_module('scripts.vertica_smoke_test')


def _command_contains_license_option(command: str, license_path: str) -> bool:
    normalized = command.replace('\n', ' ')
    quoted = shlex.quote(license_path)

    path_tokens = (license_path, quoted)
    short_flags = ('-l', '-L', '-k', '-K', '-f')
    long_flags = (
        '--license',
        '--file',
        '--key',
        '--key-file',
        '--keyfile',
        '--license-key',
        '--license_key',
        '--license-file',
        '--licensefile',
        '--license-path',
        '--license_path',
        '--license-key-file',
        '--license_key_file',
        '--licensekey',
        '--licensekeyfile',
    )

    for token in path_tokens:
        for flag in (*short_flags, *long_flags):
            if f'{flag} {token}' in normalized or f'{flag}={token}' in normalized:
                return True

    if quoted in normalized.split():
        return True

    export_prefixes = (
        'export VERTICA_DB_LICENSE=',
        'export VERTICA_DB_LICENSE_FILE=',
        'export VERTICA_LICENSE=',
        'export VERTICA_LICENSE_FILE=',
        'export VERTICA_LICENSE_PATH=',
        'export LICENSE_FILE=',
    )

    for line in command.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        for prefix in export_prefixes:
            if stripped.startswith(prefix) and quoted in stripped:
                return True

    return False


@pytest.fixture(autouse=True)
def _disable_container_restart(monkeypatch):
    monkeypatch.setattr(smoke, '_restart_vertica_container', lambda *args, **kwargs: False)


@pytest.fixture(autouse=True)
def _reset_same_file_state(monkeypatch):
    monkeypatch.setattr(smoke, '_CONFIG_COPY_SAME_FILE_LOG_CACHE', {})
    monkeypatch.setattr(smoke, '_VERTICA_CONFIG_SAME_FILE_RECOVERED', {})
    monkeypatch.setattr(smoke, '_EULA_PROMPT_LOG_CACHE', {})
    monkeypatch.setattr(smoke, '_ADMINTOOLS_LICENSE_TARGET_CACHE', {}, raising=False)


@pytest.fixture(autouse=True)
def _reset_admintools_template_cache(monkeypatch):
    monkeypatch.setattr(smoke, '_DEFAULT_ADMINTOOLS_CONF_CACHE', None, raising=False)


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


def test_container_restart_count(monkeypatch):
    responses = {'{{.RestartCount}}': '3'}

    monkeypatch.setattr(
        smoke,
        '_docker_inspect',
        lambda container, template: responses.get(template),
    )

    assert smoke._container_restart_count('vertica_ce') == 3

    responses['{{.RestartCount}}'] = ''
    assert smoke._container_restart_count('vertica_ce') is None

    responses['{{.RestartCount}}'] = 'invalid'
    assert smoke._container_restart_count('vertica_ce') is None


def test_container_reports_eula_prompt(monkeypatch):
    calls = {'count': 0}

    def fake_time() -> float:
        return 1_700_000_000.0 + calls['count'] * 5.0

    def fake_run(cmd, *, capture_output, text):
        calls['count'] += 1

        class Result:
            returncode = 0
            stdout = (
                "Starting MC agent\nOutput is not a tty --- can't reliably display EULA\n"
                if calls['count'] == 1
                else ''
            )
            stderr = ''

        return Result()

    monkeypatch.setattr(smoke.shutil, 'which', lambda name: '/usr/bin/docker')
    monkeypatch.setattr(smoke.subprocess, 'run', fake_run)
    monkeypatch.setattr(smoke.time, 'time', fake_time)

    try:
        assert smoke._container_reports_eula_prompt('vertica_ce') is True
        # Cached result should avoid re-running docker logs until TTL expires
        assert smoke._container_reports_eula_prompt('vertica_ce') is True
        # Advance time beyond TTL to force refresh without the pattern present
        calls['count'] = int(smoke._EULA_PROMPT_LOG_TTL_SECONDS / 5.0) + 2
        assert smoke._container_reports_eula_prompt('vertica_ce') is False
    finally:
        smoke._EULA_PROMPT_LOG_CACHE.pop('vertica_ce', None)


def test_log_indicates_eula_prompt_keyword_detection():
    message = "Management Console EULA acceptance required. Please accept the license"
    assert smoke._log_indicates_eula_prompt(message)

    not_prompt = "EULA already accepted successfully"
    assert not smoke._log_indicates_eula_prompt(not_prompt)


def test_detect_container_python_executable_prefers_known_path(monkeypatch):
    commands: list[list[str]] = []

    def fake_run(cmd, *, capture_output, text):
        commands.append(cmd)

        class Result:
            returncode = 0
            stdout = ''
            stderr = ''

        return Result()

    monkeypatch.setattr(smoke, 'shutil', SimpleNamespace(which=lambda name: '/usr/bin/docker'))
    monkeypatch.setattr(smoke.subprocess, 'run', fake_run)

    python_path = smoke._detect_container_python_executable('vertica_ce')

    assert python_path == '/opt/vertica/oss/python3/bin/python3'
    assert commands[0][:5] == ['docker', 'exec', 'vertica_ce', 'test', '-x']


def test_accept_vertica_eula_success(monkeypatch):
    recorded: list[list[str]] = []

    def fake_run(cmd, *, capture_output, text):
        recorded.append(cmd)

        class Result:
            returncode = 0
            stdout = 'accepted'
            stderr = ''

        return Result()

    monkeypatch.setattr(smoke, 'shutil', SimpleNamespace(which=lambda name: '/usr/bin/docker'))
    monkeypatch.setattr(smoke, '_detect_container_python_executable', lambda container: '/opt/vertica/python3')
    monkeypatch.setattr(smoke.subprocess, 'run', fake_run)
    logs: list[str] = []
    monkeypatch.setattr(smoke, 'log', logs.append)

    assert smoke._accept_vertica_eula('vertica_ce') is True
    assert recorded[-1][:4] == ['docker', 'exec', 'vertica_ce', '/opt/vertica/python3']
    assert any('Recorded Vertica EULA acceptance' in message for message in logs)


def test_run_admintools_license_command_falls_back(monkeypatch):
    commands: list[str] = []

    responses = [
        SimpleNamespace(returncode=1, stdout='', stderr='Unknown tool list_license'),
        SimpleNamespace(returncode=0, stdout='License installed', stderr=''),
    ]

    def fake_exec(container, command, message, allow_root_fallback=True):
        commands.append(command[-1])
        return responses.pop(0)

    monkeypatch.setattr(smoke, '_docker_exec_prefer_container_admin', fake_exec)

    result = smoke._run_admintools_license_command(
        'vertica_ce',
        smoke._admintools_license_command_variants('list'),
        'missing docker',
        action='list',
    )

    assert result is not None
    assert result.returncode == 0
    assert commands[0].endswith('license_audit')
    assert commands[1].endswith('list_license')


def test_run_admintools_license_command_retries_after_index_error(monkeypatch):
    commands: list[str] = []

    fatal_message = 'Unhandled exception during admintools operation\nlist index out of range'

    def fake_exec(container, command, message, allow_root_fallback=True):
        commands.append(command[-1])
        return SimpleNamespace(returncode=1, stdout=fatal_message, stderr='')

    monkeypatch.setattr(smoke, '_docker_exec_prefer_container_admin', fake_exec)

    result = smoke._run_admintools_license_command(
        'vertica_ce',
        (
            '/opt/vertica/bin/admintools -t list_license',
            '/opt/vertica/bin/admintools license -k list',
        ),
        'missing docker',
        action='list',
    )

    assert result is not None
    assert result.returncode == 1
    assert commands == [
        '/opt/vertica/bin/admintools -t list_license',
    ]
    assert result.stdout == fatal_message


def test_run_admintools_license_command_stops_on_fatal_without_unknown(monkeypatch):
    commands: list[str] = []

    fatal_message = 'Unhandled exception during admintools operation\ninternal failure'

    responses = [
        SimpleNamespace(returncode=1, stdout=fatal_message, stderr=''),
        SimpleNamespace(returncode=0, stdout='should not run', stderr=''),
    ]

    def fake_exec(container, command, message, allow_root_fallback=True):
        commands.append(command[-1])
        return responses.pop(0)

    monkeypatch.setattr(smoke, '_docker_exec_prefer_container_admin', fake_exec)

    result = smoke._run_admintools_license_command(
        'vertica_ce',
        (
            '/opt/vertica/bin/admintools -t list_license',
            '/opt/vertica/bin/admintools license -k list',
        ),
        'missing docker',
        action='list',
    )

    assert result is not None
    assert result.returncode == 1
    assert commands == ['/opt/vertica/bin/admintools -t list_license']


def test_parse_admintools_help_for_license_targets():
    output = textwrap.dedent(
        '''
        Available tools:
          create_db
          license_keys
          manage_License
          license-usage
          license_keys
          somethingelse
        '''
    )

    targets = smoke._parse_admintools_help_for_license_targets(output)

    assert targets == ('license_keys', 'manage_License', 'license-usage')


def test_run_admintools_license_command_discovers_help_targets(monkeypatch):
    commands: list[str] = []

    responses = [
        SimpleNamespace(returncode=1, stdout='', stderr='Unknown tool list_license'),
        SimpleNamespace(returncode=1, stdout='', stderr='no such option: -k list'),
        SimpleNamespace(returncode=0, stdout='License installed', stderr=''),
    ]

    def fake_exec(container, command, message, allow_root_fallback=True):
        commands.append(command[-1])
        if not responses:
            raise AssertionError('unexpected command execution')
        return responses.pop(0)

    monkeypatch.setattr(smoke, '_docker_exec_prefer_container_admin', fake_exec)
    monkeypatch.setattr(
        smoke,
        '_discover_admintools_license_targets',
        lambda _container: ('license_keys',),
    )
    
    def fake_variants(action, *, license_path=None, extra_targets=()):
        if action == 'list':
            targets = ('license',) + extra_targets
            return tuple(
                f'/opt/vertica/bin/admintools -t {target} -k list'
                for target in targets
            )
        if action == 'install':
            if license_path is None:
                raise ValueError('license_path must be provided for install action')
            targets = ('license',) + extra_targets
            return tuple(
                f'/opt/vertica/bin/admintools -t {target} -k install -f {license_path}'
                for target in targets
            )
        raise ValueError(f'Unexpected action {action!r}')

    monkeypatch.setattr(
        smoke,
        '_admintools_license_command_variants',
        fake_variants,
    )

    result = smoke._run_admintools_license_command(
        'vertica_ce',
        (
            '/opt/vertica/bin/admintools -t list_license',
            '/opt/vertica/bin/admintools -t license -k list',
        ),
        'missing docker',
        action='list',
    )

    assert result is not None
    assert result.returncode == 0
    assert any(cmd.endswith('license_keys -k list') for cmd in commands)


def test_run_admintools_license_command_supports_command_targets(monkeypatch):
    commands: list[str] = []

    def fake_exec(container, command, message, allow_root_fallback=True):
        assert container == 'vertica_ce'
        commands.append(command[-1])
        script = command[-1]

        if 'admintools manage_license -k list' in script and ' -t ' not in script:
            return SimpleNamespace(returncode=0, stdout='installed', stderr='')

        if '-t manage_license' in script:
            return SimpleNamespace(returncode=1, stdout='', stderr='Unknown tool manage_license')

        if 'license' in script:
            return SimpleNamespace(returncode=1, stdout='', stderr='Unknown command license')

        return SimpleNamespace(returncode=1, stdout='', stderr='Unknown tool list_license')

    monkeypatch.setattr(smoke, '_docker_exec_prefer_container_admin', fake_exec)

    result = smoke._run_admintools_license_command(
        'vertica_ce',
        smoke._admintools_license_command_variants('list'),
        'missing docker',
        action='list',
    )

    assert result is not None
    assert result.returncode == 0
    assert any('admintools -t manage_license -k list' in cmd for cmd in commands)
    assert any('admintools manage_license -k list' in cmd for cmd in commands)


def test_run_admintools_license_command_retries_on_fatal_with_unknown(monkeypatch):
    commands: list[str] = []

    responses = [
        SimpleNamespace(
            returncode=1,
            stdout='',
            stderr=(
                'Unhandled exception during admintools operation\n'
                'ATMain.py: error: no such option: -k'
            ),
        ),
        SimpleNamespace(returncode=0, stdout='installed', stderr=''),
    ]

    def fake_exec(container, command, message, allow_root_fallback=True):
        assert container == 'vertica_ce'
        commands.append(command[-1])
        return responses.pop(0)

    monkeypatch.setattr(smoke, '_docker_exec_prefer_container_admin', fake_exec)

    result = smoke._run_admintools_license_command(
        'vertica_ce',
        ('first', 'second'),
        'missing docker',
        action='list',
    )

    assert result is not None
    assert result.returncode == 0
    assert commands == ['first', 'second']


def test_run_admintools_license_command_limits_unknown_attempts(monkeypatch):
    commands: list[str] = []
    logs: list[str] = []

    def fake_exec(container, command, message, allow_root_fallback=True):
        commands.append(command[-1])
        return SimpleNamespace(returncode=1, stdout='', stderr='Unknown tool install_license')

    monkeypatch.setattr(smoke, '_docker_exec_prefer_container_admin', fake_exec)
    monkeypatch.setattr(smoke, 'log', logs.append)

    result = smoke._run_admintools_license_command(
        'vertica_ce',
        tuple(f'command-{index}' for index in range(100)),
        'missing docker',
        action='install',
        license_path='/data/vertica/config/license.key',
    )

    assert result is not None
    assert result.returncode == 1
    assert len(commands) == smoke._ADMINTOOLS_LICENSE_UNKNOWN_ATTEMPT_LIMIT
    assert any('repeated unknown responses' in message for message in logs)


def test_admintools_license_command_variants_include_subcommands():
    list_variants = smoke._admintools_license_command_variants('list')
    assert any('license_audit' in command for command in list_variants)
    assert any('-t license -k list' in command for command in list_variants)
    assert any('license list' in command for command in list_variants)
    assert any('license_keys -k list' in command for command in list_variants)
    install_variants = smoke._admintools_license_command_variants(
        'install',
        license_path='/data/vertica/config/license.key',
    )
    assert any('-t license install' in command for command in install_variants)
    assert any('license register' in command for command in install_variants)
    assert any('register_license' in command for command in install_variants)
    assert any('install_license ' in command or command.endswith('install_license') for command in install_variants)
    assert any('license -k install' in command for command in install_variants)
    assert any('license install --file' in command for command in install_variants)
    assert any('license_keys' in command for command in install_variants)
    assert any('upgrade_license_key' in command for command in install_variants)


def test_admintools_license_target_commands_skip_subcommands_for_audit():
    commands = smoke._admintools_license_target_commands(
        'license_audit',
        'list',
        None,
    )
    assert '/opt/vertica/bin/admintools -t license_audit' in commands
    assert '/opt/vertica/bin/admintools license_audit list' in commands
    assert all('-k list' not in command for command in commands)


def test_admintools_license_target_commands_skip_subcommands_for_upgrade():
    commands = smoke._admintools_license_target_commands(
        'upgrade_license_key',
        'install',
        '/data/vertica/config/license.key',
    )
    assert any('--license' in command for command in commands)
    assert any('--file' in command for command in commands)
    assert all('-k install' not in command for command in commands)


def test_license_option_variants_include_extended_flags():
    variants = smoke._license_option_variants('/data/vertica/config/license.key')
    assert '--license /data/vertica/config/license.key' in variants
    assert '--license-path=/data/vertica/config/license.key' in variants
    assert '--license_file /data/vertica/config/license.key' in variants
    assert '--license-key=/data/vertica/config/license.key' in variants
    assert '--key-file /data/vertica/config/license.key' in variants
    assert '--path=/data/vertica/config/license.key' in variants
    assert '--file /data/vertica/config/license.key' in variants
    assert '/data/vertica/config/license.key' in variants
    assert '-f /data/vertica/config/license.key' in variants
    assert '-F=/data/vertica/config/license.key' in variants
    assert '-k /data/vertica/config/license.key' in variants
    assert '-K=/data/vertica/config/license.key' in variants
    assert '-l /data/vertica/config/license.key' not in variants
    create_variants = smoke._license_option_variants(
        '/data/vertica/config/license.key', include_create_short_flag=True
    )
    assert '/data/vertica/config/license.key' in create_variants
    assert '-l /data/vertica/config/license.key' in create_variants
    assert '-L=/data/vertica/config/license.key' in create_variants
    assert '-f /data/vertica/config/license.key' in create_variants
    assert '-F=/data/vertica/config/license.key' in create_variants


def test_license_option_variants_only_include_plain_path_for_admintools():
    variants = smoke._license_option_variants('/data/vertica/config/license.key')
    assert any(not variant.startswith('-') for variant in variants)

    create_variants = smoke._license_option_variants(
        '/data/vertica/config/license.key', include_create_short_flag=True
    )
    assert any(not variant.startswith('-') for variant in create_variants)


def test_install_vertica_license_uses_fallback(monkeypatch):
    commands: list[str] = []

    def fake_discover(_container: str) -> list[str]:
        return ['/opt/vertica/config/license.dat']

    def fake_exec(container, command, message, allow_root_fallback=True):
        commands.append(command[-1])
        if 'install_license' in command[-1]:
            return SimpleNamespace(returncode=1, stdout='', stderr='Unknown tool install_license')
        return SimpleNamespace(returncode=0, stdout='Installed', stderr='')

    monkeypatch.setattr(smoke, '_discover_container_license_files', fake_discover)
    monkeypatch.setattr(smoke, '_docker_exec_prefer_container_admin', fake_exec)

    assert smoke._install_vertica_license('vertica_ce') is True
    assert any(
        (
            ' -t license ' in cmd
            or ' license ' in cmd
        )
        and 'install_license' not in cmd
        for cmd in commands
    )


def test_install_vertica_license_deploys_fallback_when_admintools_missing(monkeypatch):
    paths: list[str] = ['/opt/vertica/config/d5415.dat']
    monkeypatch.setattr(
        smoke,
        '_discover_container_license_files',
        lambda _container: paths,
    )

    monkeypatch.setattr(
        smoke,
        '_docker_exec_prefer_container_admin',
        lambda *args, **kwargs: SimpleNamespace(
            returncode=1,
            stdout='',
            stderr='Unknown tool install_license',
        ),
    )

    called: list[tuple[str, str, tuple[str, ...]]] = []

    def fake_deploy(
        container: str,
        source: str,
        *,
        extra_destinations: tuple[str, ...] = (),
    ) -> bool:
        called.append((container, source, extra_destinations))
        return True

    monkeypatch.setattr(smoke, '_deploy_vertica_license_fallback', fake_deploy)

    assert smoke._install_vertica_license('vertica_ce') is True
    assert called == [
        (
            'vertica_ce',
            paths[0],
            tuple(paths),
        )
    ]


def test_install_vertica_license_fallback_failure(monkeypatch):
    monkeypatch.setattr(
        smoke,
        '_discover_container_license_files',
        lambda _container: ['/opt/vertica/config/license.dat'],
    )

    monkeypatch.setattr(
        smoke,
        '_docker_exec_prefer_container_admin',
        lambda *args, **kwargs: SimpleNamespace(
            returncode=1,
            stdout='',
            stderr='Unknown tool install_license',
        ),
    )

    monkeypatch.setattr(
        smoke,
        '_deploy_vertica_license_fallback',
        lambda *_args, **_kwargs: False,
    )

    assert smoke._install_vertica_license('vertica_ce') is False


def test_install_vertica_license_handles_no_such_option(monkeypatch):
    monkeypatch.setattr(
        smoke,
        '_discover_container_license_files',
        lambda _container: ['/opt/vertica/config/license.dat'],
    )

    monkeypatch.setattr(
        smoke,
        '_docker_exec_prefer_container_admin',
        lambda *args, **kwargs: SimpleNamespace(
            returncode=1,
            stdout='',
            stderr='ATMain.py: error: no such option: --install',
        ),
    )

    called: list[tuple[str, str, tuple[str, ...]]] = []

    def fake_deploy(
        container: str,
        source: str,
        *,
        extra_destinations: tuple[str, ...] = (),
    ) -> bool:
        called.append((container, source, extra_destinations))
        return True

    monkeypatch.setattr(smoke, '_deploy_vertica_license_fallback', fake_deploy)

    assert smoke._install_vertica_license('vertica_ce') is True
    assert called == [
        (
            'vertica_ce',
            '/opt/vertica/config/license.dat',
            ('/opt/vertica/config/license.dat',),
        )
    ]


def test_install_vertica_license_handles_index_error(monkeypatch):
    monkeypatch.setattr(
        smoke,
        '_discover_container_license_files',
        lambda _container: ['/opt/vertica/config/license.dat'],
    )

    monkeypatch.setattr(
        smoke,
        '_docker_exec_prefer_container_admin',
        lambda *args, **kwargs: SimpleNamespace(
            returncode=1,
            stdout='',
            stderr=(
                'Unhandled exception during admintools operation\n'
                'error message: list index out of range\n'
            ),
        ),
    )

    called: list[tuple[str, str, tuple[str, ...]]] = []

    def fake_deploy(
        container: str,
        source: str,
        *,
        extra_destinations: tuple[str, ...] = (),
    ) -> bool:
        called.append((container, source, extra_destinations))
        return True

    monkeypatch.setattr(smoke, '_deploy_vertica_license_fallback', fake_deploy)

    assert smoke._install_vertica_license('vertica_ce') is True
    assert called == [
        (
            'vertica_ce',
            '/opt/vertica/config/license.dat',
            ('/opt/vertica/config/license.dat',),
        )
    ]


def test_deploy_vertica_license_fallback_handles_same_file(monkeypatch):
    align_calls: list[str] = []

    def fake_align(container, path, friendly_name, *, context='admintools.conf'):
        align_calls.append(path)
        return True, False

    def fake_exec(container, script, message):
        assert container == 'vertica_ce'
        src = None
        dest = None
        for line in script.splitlines():
            if line.startswith('src='):
                src = line.split('=', 1)[1].strip("'\"")
            if line.startswith('dest='):
                dest = line.split('=', 1)[1].strip("'\"")
        if dest == '/data/vertica/config/license.dat':
            return SimpleNamespace(
                returncode=1,
                stdout='',
                stderr=(
                    f"cp: '{src}' and '{dest}' are the same file\n"
                ),
            )
        return SimpleNamespace(returncode=0, stdout='', stderr='')

    monkeypatch.setattr(smoke, '_align_container_path_identity', fake_align)
    monkeypatch.setattr(smoke, '_docker_exec_root_shell', fake_exec)

    result = smoke._deploy_vertica_license_fallback(
        'vertica_ce', '/opt/vertica/config/license.dat'
    )

    assert result is True
    assert '/opt/vertica/config/license.dat' in align_calls
    assert '/data/vertica/config/license.dat' in align_calls


def test_deploy_vertica_license_fallback_supports_missing_install(monkeypatch):
    scripts: list[str] = []

    def fake_align(container, path, friendly_name, *, context='admintools.conf'):
        return True, False

    def fake_exec(container, script, message):
        scripts.append(script)
        return SimpleNamespace(returncode=0, stdout='', stderr='')

    monkeypatch.setattr(smoke, '_align_container_path_identity', fake_align)
    monkeypatch.setattr(smoke, '_docker_exec_root_shell', fake_exec)

    result = smoke._deploy_vertica_license_fallback(
        'vertica_ce', '/opt/vertica/config/license.dat'
    )

    assert result is True
    assert any('command -v install >/dev/null 2>&1' in script for script in scripts)
    assert any('cp -- "$src" "$tmp"' in script for script in scripts)


def test_deploy_vertica_license_fallback_skips_non_license_targets(monkeypatch):
    destinations: list[str] = []

    def fake_align(container, path, friendly_name, *, context='admintools.conf'):
        return True, False

    def fake_exec(container, script, message):
        for line in script.splitlines():
            if line.startswith('dest='):
                destinations.append(line.split('=', 1)[1].strip("'\""))
        return SimpleNamespace(returncode=0, stdout='', stderr='')

    monkeypatch.setattr(smoke, '_align_container_path_identity', fake_align)
    monkeypatch.setattr(smoke, '_docker_exec_root_shell', fake_exec)

    result = smoke._deploy_vertica_license_fallback(
        'vertica_ce',
        '/opt/vertica/config/license.key',
        extra_destinations=(
            '/opt/vertica/README',
            '/opt/vertica/config/license.dat',
        ),
    )

    assert result is True
    assert '/opt/vertica/config/license.dat' in destinations
    assert '/opt/vertica/README' not in destinations


def test_discover_license_includes_known_candidates(monkeypatch):
    def fake_exec(container, command, message, allow_root_fallback=True):
        assert container == 'vertica_ce'
        return SimpleNamespace(returncode=0, stdout='', stderr='')

    def fake_which(tool: str) -> Optional[str]:
        return '/usr/bin/docker' if tool == 'docker' else None

    def fake_run(args, capture_output, text):
        command = args[-1]
        if 'license.dat' in command:
            return SimpleNamespace(returncode=0, stdout='', stderr='')
        return SimpleNamespace(returncode=1, stdout='', stderr='')

    monkeypatch.setattr(smoke, '_docker_exec_prefer_container_admin', fake_exec)
    monkeypatch.setattr(smoke.shutil, 'which', fake_which)
    monkeypatch.setattr(smoke.subprocess, 'run', fake_run)

    candidates = smoke._discover_container_license_files('vertica_ce')

    assert '/opt/vertica/config/license.dat' in candidates


def test_discover_license_prioritises_known_paths(monkeypatch):
    def fake_exec(container, command, message, allow_root_fallback=True):
        assert container == 'vertica_ce'
        stdout = '\n'.join(
            [
                '/opt/vertica/oss/python3/lib/python3.11/LICENSE.txt',
                '/opt/vertica/config/share/license/Vertica_CE.license.key',
            ]
        )
        return SimpleNamespace(returncode=0, stdout=stdout, stderr='')

    def fake_which(tool: str) -> Optional[str]:
        return '/usr/bin/docker' if tool == 'docker' else None

    def fake_run(args, capture_output, text):
        command = args[-1]
        if 'Vertica_CE.license.key' in command:
            return SimpleNamespace(returncode=0, stdout='', stderr='')
        return SimpleNamespace(returncode=1, stdout='', stderr='')

    monkeypatch.setattr(smoke, '_docker_exec_prefer_container_admin', fake_exec)
    monkeypatch.setattr(smoke.shutil, 'which', fake_which)
    monkeypatch.setattr(smoke.subprocess, 'run', fake_run)

    candidates = smoke._discover_container_license_files('vertica_ce')

    assert candidates[0] in {
        '/opt/vertica/config/share/license/Vertica_CE.license.key',
        '/opt/vertica/share/license/Vertica_CE.license.key',
    }
    assert '/opt/vertica/oss/python3/lib/python3.11/LICENSE.txt' not in candidates


def test_discover_license_omits_python_help_files(monkeypatch):
    def fake_exec(container, command, message, allow_root_fallback=True):
        assert container == 'vertica_ce'
        stdout = '\n'.join(
            [
                '/opt/vertica/oss/python3/lib/python3.11/site-packages/vertica/help_files/SelectdatabasetoUpgradeLicenseKey.txt',
                '/opt/vertica/config/license.key',
            ]
        )
        return SimpleNamespace(returncode=0, stdout=stdout, stderr='')

    monkeypatch.setattr(smoke, '_docker_exec_prefer_container_admin', fake_exec)
    monkeypatch.setattr(smoke.shutil, 'which', lambda tool: None)

    candidates = smoke._discover_container_license_files('vertica_ce')

    assert '/opt/vertica/config/license.key' in candidates
    assert (
        '/opt/vertica/oss/python3/lib/python3.11/site-packages/vertica/help_files/SelectdatabasetoUpgradeLicenseKey.txt'
        not in candidates
    )


def test_discover_license_includes_hex_named_paths(monkeypatch):
    def fake_exec(container, command, message, allow_root_fallback=True):
        assert container == 'vertica_ce'
        return SimpleNamespace(
            returncode=0,
            stdout='/opt/vertica/config/d5415f948449e9d4c421b568f2411140.dat\n',
            stderr='',
        )

    def fake_which(tool: str) -> Optional[str]:
        return '/usr/bin/docker' if tool == 'docker' else None

    def fake_run(args, capture_output, text):
        return SimpleNamespace(returncode=1, stdout='', stderr='')

    monkeypatch.setattr(smoke, '_docker_exec_prefer_container_admin', fake_exec)
    monkeypatch.setattr(smoke.shutil, 'which', fake_which)
    monkeypatch.setattr(smoke.subprocess, 'run', fake_run)

    candidates = smoke._discover_container_license_files('vertica_ce')

    assert '/opt/vertica/config/d5415f948449e9d4c421b568f2411140.dat' in candidates


def test_discover_license_prefers_data_config_paths(monkeypatch):
    def fake_exec(container, command, message, allow_root_fallback=True):
        assert container == 'vertica_ce'
        stdout = '\n'.join(
            [
                '/opt/vertica/config/license.dat',
                '/data/vertica/config/license.dat',
            ]
        )
        return SimpleNamespace(returncode=0, stdout=stdout, stderr='')

    monkeypatch.setattr(smoke, '_docker_exec_prefer_container_admin', fake_exec)
    monkeypatch.setattr(smoke.shutil, 'which', lambda tool: None)

    candidates = smoke._discover_container_license_files('vertica_ce')

    assert candidates[0] == '/data/vertica/config/license.dat'
    assert candidates[1] == '/opt/vertica/config/license.dat'


def test_ensure_vertica_license_installed_disables_root_fallback(monkeypatch):
    responses = [
        SimpleNamespace(returncode=0, stdout='No license installed', stderr=''),
        SimpleNamespace(returncode=0, stdout='License installed', stderr=''),
    ]
    observed: list[bool] = []

    def fake_run(
        container: str,
        commands: tuple[str, ...],
        message: str,
        *,
        allow_root_fallback: bool,
        action=None,
        license_path=None,
    ) -> SimpleNamespace:
        observed.append(allow_root_fallback)
        return responses.pop(0)

    monkeypatch.setattr(smoke, '_run_admintools_license_command', fake_run)
    monkeypatch.setattr(smoke, '_install_vertica_license', lambda container: True)

    status = smoke._ensure_vertica_license_installed('vertica_ce')
    assert status.installed is True
    assert status.verified is True
    assert observed == [False, False]


def test_ensure_vertica_license_installed_handles_missing_list_tool(monkeypatch):
    observed: list[bool] = []

    def fake_run(
        container: str,
        commands: tuple[str, ...],
        message: str,
        *,
        allow_root_fallback: bool,
        action=None,
        license_path=None,
    ) -> SimpleNamespace:
        observed.append(allow_root_fallback)
        return SimpleNamespace(
            returncode=1, stdout='', stderr='Unknown tool list_license'
        )

    installed: list[str] = []

    def fake_install(container: str) -> bool:
        installed.append(container)
        return True

    monkeypatch.setattr(smoke, '_run_admintools_license_command', fake_run)
    monkeypatch.setattr(smoke, '_install_vertica_license', fake_install)

    status = smoke._ensure_vertica_license_installed('vertica_ce')
    assert status.installed is True
    assert status.verified is False
    assert observed == [False]
    assert installed == ['vertica_ce']


def test_ensure_vertica_license_installed_handles_verification_index_error(monkeypatch):
    responses = [
        SimpleNamespace(
            returncode=1,
            stdout=(
                'Unhandled exception during admintools operation\n'
                'error message: list index out of range\n'
            ),
            stderr='',
        ),
        SimpleNamespace(
            returncode=1,
            stdout=(
                'Unhandled exception during admintools operation\n'
                'error message: list index out of range\n'
            ),
            stderr='',
        ),
    ]

    def fake_run(
        container: str,
        commands: tuple[str, ...],
        message: str,
        *,
        allow_root_fallback: bool,
        action=None,
        license_path=None,
    ) -> SimpleNamespace:
        assert container == 'vertica_ce'
        assert allow_root_fallback is False
        return responses.pop(0)

    installs: list[str] = []

    def fake_install(container: str) -> bool:
        installs.append(container)
        return True

    monkeypatch.setattr(smoke, '_run_admintools_license_command', fake_run)
    monkeypatch.setattr(smoke, '_install_vertica_license', fake_install)

    status = smoke._ensure_vertica_license_installed('vertica_ce')

    assert status.installed is True
    assert status.verified is False
    assert installs == ['vertica_ce']
    assert responses == []


def test_ensure_vertica_license_installed_tolerates_verification_no_license(monkeypatch):
    responses = [
        SimpleNamespace(returncode=1, stdout='No license has been installed', stderr=''),
        SimpleNamespace(returncode=0, stdout='No license has been installed', stderr=''),
    ]

    def fake_run(
        container: str,
        commands: tuple[str, ...],
        message: str,
        *,
        allow_root_fallback: bool,
        action=None,
        license_path=None,
    ) -> SimpleNamespace:
        assert container == 'vertica_ce'
        assert allow_root_fallback is False
        return responses.pop(0)

    installs: list[str] = []

    def fake_install(container: str) -> bool:
        installs.append(container)
        return True

    monkeypatch.setattr(smoke, '_run_admintools_license_command', fake_run)
    monkeypatch.setattr(smoke, '_install_vertica_license', fake_install)

    status = smoke._ensure_vertica_license_installed('vertica_ce')

    assert status.installed is True
    assert status.verified is False
    assert installs == ['vertica_ce']
    assert responses == []


def test_attempt_creation_prefers_license_candidate(monkeypatch):
    commands: list[str] = []

    def fake_fetch_env(container: str) -> dict[str, str]:
        assert container == 'vertica_ce'
        return {'VERTICA_DB_PASSWORD': 'secret'}

    def fake_discover(container: str) -> list[str]:
        return ['/opt/vertica/config/license.key']

    def fake_ensure(container: str) -> smoke.LicenseStatus:
        return smoke.LicenseStatus(True, False)

    responses = [
        SimpleNamespace(returncode=1, stdout='', stderr='Unknown option --license'),
        SimpleNamespace(returncode=0, stdout='Created', stderr=''),
    ]

    def fake_exec(container, command, message, allow_root_fallback=True):
        commands.append(command[-1])
        return responses.pop(0)

    monkeypatch.setattr(smoke, '_fetch_container_env', fake_fetch_env)
    monkeypatch.setattr(smoke, '_discover_container_license_files', fake_discover)
    monkeypatch.setattr(smoke, '_ensure_vertica_license_installed', fake_ensure)
    monkeypatch.setattr(smoke, '_docker_exec_prefer_container_admin', fake_exec)

    assert smoke._attempt_vertica_database_creation('vertica_ce', 'VMart') is True
    assert any(
        _command_contains_license_option(cmd, '/opt/vertica/config/license.key')
        for cmd in commands
    )
    assert any(
        line.splitlines()[-1].startswith('/opt/vertica/bin/admintools -t create_db')
        for line in commands
    )
    assert any(
        '-c ' in line.splitlines()[-1] and '-D ' in line.splitlines()[-1]
        for line in commands
    )


def test_attempt_creation_disables_root_fallback(monkeypatch):
    observed: list[bool] = []

    def fake_fetch_env(container: str) -> dict[str, str]:
        assert container == 'vertica_ce'
        return {'VERTICA_DB_PASSWORD': 'secret'}

    monkeypatch.setattr(smoke, '_fetch_container_env', fake_fetch_env)
    monkeypatch.setattr(smoke, '_discover_container_license_files', lambda container: [])
    monkeypatch.setattr(
        smoke,
        '_ensure_vertica_license_installed',
        lambda container: smoke.LicenseStatus(True, True),
    )

    def fake_exec(container, command, message, allow_root_fallback=True):
        observed.append(allow_root_fallback)
        return SimpleNamespace(returncode=0, stdout='Created', stderr='')

    monkeypatch.setattr(smoke, '_docker_exec_prefer_container_admin', fake_exec)

    assert smoke._attempt_vertica_database_creation('vertica_ce', 'VMart') is True
    assert observed
    assert all(value is False for value in observed)


def test_attempt_creation_retries_when_license_required(monkeypatch):
    commands: list[str] = []

    def fake_fetch_env(container: str) -> dict[str, str]:
        assert container == 'vertica_ce'
        return {'VERTICA_DB_PASSWORD': 'secret'}

    def fake_discover(container: str) -> list[str]:
        return ['/opt/vertica/config/license.key']

    def fake_ensure(container: str) -> smoke.LicenseStatus:
        return smoke.LicenseStatus(True, False)

    responses = [
        SimpleNamespace(
            returncode=1,
            stdout='',
            stderr=(
                'Error: a database license has not been installed.\n'
                'You must supply a valid license using the -l (--license) option.\n'
            ),
        ),
        SimpleNamespace(returncode=0, stdout='Created', stderr=''),
    ]

    def fake_exec(container, command, message, allow_root_fallback=True):
        commands.append(command[-1])
        return responses.pop(0)

    monkeypatch.setattr(smoke, '_fetch_container_env', fake_fetch_env)
    monkeypatch.setattr(smoke, '_discover_container_license_files', fake_discover)
    monkeypatch.setattr(smoke, '_ensure_vertica_license_installed', fake_ensure)
    monkeypatch.setattr(smoke, '_docker_exec_prefer_container_admin', fake_exec)

    assert smoke._attempt_vertica_database_creation('vertica_ce', 'VMart') is True
    assert len(commands) == 2
    first = commands[0].splitlines()[-1]
    second = commands[1].splitlines()[-1]
    assert first.startswith('/opt/vertica/bin/admintools -t create_db')
    assert second.startswith('/opt/vertica/bin/admintools -t create_db')
    assert '-c ' in first and '-D ' in first
    assert '-c ' in second and '-D ' in second
    assert _command_contains_license_option(second, '/opt/vertica/config/license.key')


def test_attempt_creation_continues_without_installed_license(monkeypatch):
    commands: list[str] = []

    def fake_fetch_env(container: str) -> dict[str, str]:
        assert container == 'vertica_ce'
        return {'VERTICA_DB_PASSWORD': 'secret'}

    def fake_discover(container: str) -> list[str]:
        return ['/opt/vertica/config/license.key']

    def fake_ensure(container: str) -> smoke.LicenseStatus:
        return smoke.LicenseStatus(False, False)

    def fake_exec(container, command, message, allow_root_fallback=True):
        commands.append(command[-1])
        return SimpleNamespace(returncode=0, stdout='Created', stderr='')

    monkeypatch.setattr(smoke, '_fetch_container_env', fake_fetch_env)
    monkeypatch.setattr(smoke, '_discover_container_license_files', fake_discover)
    monkeypatch.setattr(smoke, '_ensure_vertica_license_installed', fake_ensure)
    monkeypatch.setattr(smoke, '_docker_exec_prefer_container_admin', fake_exec)

    assert smoke._attempt_vertica_database_creation('vertica_ce', 'VMart') is True
    assert commands


def test_attempt_creation_retries_after_invalid_license_status(monkeypatch):
    monkeypatch.setattr(smoke, '_fetch_container_env', lambda container: {})
    monkeypatch.setattr(
        smoke,
        '_discover_container_license_files',
        lambda container: ['/data/vertica/config/license.key'],
    )

    statuses = iter(
        [
            smoke.LicenseStatus(True, False),
            smoke.LicenseStatus(True, True),
        ]
    )

    monkeypatch.setattr(
        smoke,
        '_ensure_vertica_license_installed',
        lambda container: next(statuses),
    )

    responses = [
        SimpleNamespace(
            returncode=1,
            stdout='',
            stderr=(
                'Error: License key /data/vertica/config/license.key not installed.\n'
                'Invalid license status\n'
            ),
        ),
        SimpleNamespace(returncode=0, stdout='Created', stderr=''),
    ]

    commands: list[str] = []

    def fake_exec(container, command, message, allow_root_fallback=True):
        commands.append(command[-1])
        return responses.pop(0)

    monkeypatch.setattr(smoke, '_docker_exec_prefer_container_admin', fake_exec)

    assert smoke._attempt_vertica_database_creation('vertica_ce', 'VMart') is True
    assert len(commands) == 2
    first_command = commands[0]
    assert first_command.splitlines()[-1].startswith(
        '/opt/vertica/bin/admintools -t create_db'
    )
    assert '-c ' in first_command.splitlines()[-1]
    assert '-D ' in first_command.splitlines()[-1]
    assert _command_contains_license_option(
        first_command, '/data/vertica/config/license.key'
    )


def test_attempt_creation_continues_when_license_installation_failed(monkeypatch):
    monkeypatch.setattr(smoke, '_fetch_container_env', lambda container: {})
    monkeypatch.setattr(
        smoke,
        '_discover_container_license_files',
        lambda container: ['/opt/vertica/config/license.key'],
    )

    monkeypatch.setattr(
        smoke,
        '_ensure_vertica_license_installed',
        lambda container: smoke.LicenseStatus(False, False),
    )

    commands: list[str] = []

    def fake_exec(container, command, message, allow_root_fallback=True):  # pragma: no cover - should not run
        commands.append(command[-1])
        return SimpleNamespace(returncode=0, stdout='unexpected', stderr='')

    monkeypatch.setattr(smoke, '_docker_exec_prefer_container_admin', fake_exec)

    logs: list[str] = []
    monkeypatch.setattr(smoke, 'log', logs.append)

    assert smoke._attempt_vertica_database_creation('vertica_ce', 'VMart') is True
    assert commands
    assert any(
        'continuing with database creation using discovered license files' in entry
        for entry in logs
    )


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
            try:
                return next(health_states)
            except StopIteration:
                return 'healthy'
        if template == '{{.RestartCount}}':
            return '0'
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


def _compose_with_environment(tmp_path, content: str) -> Path:
    compose = tmp_path / 'compose.yml'
    compose.write_text(content)
    return compose


def test_ensure_compose_accepts_eula_adds_block_when_missing(tmp_path):
    compose = _compose_with_environment(
        tmp_path,
        textwrap.dedent(
            '''
            services:
              vertica_ce:
                image: vertica/vertica-ce:latest
                restart: always
            '''
        ).lstrip(),
    )

    assert smoke._ensure_compose_accepts_eula(compose) is True

    updated = compose.read_text().splitlines()

    assert any('image: vertica/vertica-ce:latest' in line for line in updated)
    assert any('restart: always' in line for line in updated)
    assert any(line.strip().startswith('VERTICA_ACCEPT_EULA') for line in updated)


def test_ensure_compose_accepts_eula_handles_inline_list(tmp_path):
    compose = _compose_with_environment(
        tmp_path,
        textwrap.dedent(
            '''
            services:
              vertica_ce:
                environment: [FOO=bar, BAR=baz]
            '''
        ).lstrip(),
    )

    assert smoke._ensure_compose_accepts_eula(compose) is True

    updated = compose.read_text().splitlines()

    assert any('FOO=bar' in line for line in updated)
    assert any('BAR=baz' in line for line in updated)
    assert any('VERTICA_ACCEPT_EULA' in line for line in updated)


def test_ensure_compose_accepts_eula_handles_inline_mapping(tmp_path):
    compose = _compose_with_environment(
        tmp_path,
        textwrap.dedent(
            '''
            services:
              vertica_ce:
                environment: {FOO: bar, BAR: baz}
            '''
        ).lstrip(),
    )

    assert smoke._ensure_compose_accepts_eula(compose) is True

    updated = compose.read_text().splitlines()

    assert any('FOO: bar' in line for line in updated)
    assert any('BAR: baz' in line for line in updated)
    assert any('VERTICA_ACCEPT_EULA' in line for line in updated)

def test_ensure_compose_accepts_eula_no_changes_required(tmp_path):
    environment_entries = '\n'.join(
        f'- {key}={value}'
        for key, value in smoke._EULA_ENVIRONMENT_VARIABLES.items()
    )
    environment_block = textwrap.indent(environment_entries, '      ')
    compose_content = (
        "services:\n"
        "  vertica_ce:\n"
        "    image: vertica/vertica-ce:latest\n"
        "    environment:\n"
        f"{environment_block}\n"
    )
    compose = _compose_with_environment(tmp_path, compose_content)

    original = compose.read_text()
    assert smoke._ensure_compose_accepts_eula(compose) is True
    assert compose.read_text() == original


def test_ensure_compose_accepts_eula_finds_service_by_container_name(tmp_path):
    compose = _compose_with_environment(
        tmp_path,
        textwrap.dedent(
            """
            services:
              vertica:
                container_name: vertica_ce
                image: vertica/vertica-ce:latest
            """
        ).lstrip(),
    )

    assert smoke._ensure_compose_accepts_eula(compose) is True

    updated = compose.read_text().splitlines()

    assert any('container_name: vertica_ce' in line for line in updated)
    assert any(line.strip().startswith('environment:') for line in updated)
    assert any('VERTICA_ACCEPT_EULA' in line for line in updated)


def test_ensure_vertica_respects_starting_grace(monkeypatch):
    current_time = {'value': 0.0}

    def fake_time() -> float:
        return current_time['value']

    def fake_sleep(seconds: float) -> None:
        current_time['value'] += seconds

    calls: list[list[str]] = []

    def fake_run_command(command: list[str]):  # pragma: no cover - should not run
        calls.append(command)
        raise AssertionError('run_command should not be invoked during grace period')

    health_states = iter(['starting', 'healthy'])

    def fake_docker_inspect(container: str, template: str) -> Optional[str]:
        if template == '{{.State.Status}}':
            return 'running'
        if template == '{{if .State.Health}}{{.State.Health.Status}}{{end}}':
            try:
                return next(health_states)
            except StopIteration:
                return 'healthy'
        if template == '{{.RestartCount}}':
            return '0'
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
    monkeypatch.setattr(smoke, 'UNHEALTHY_HEALTHCHECK_GRACE_PERIOD_SECONDS', 5.0)

    smoke.ensure_vertica_container_running(timeout=1000.0, compose_timeout=0.0)

    assert compose_calls == [True, True, True]
    assert reset_calls == [True]


def test_ensure_vertica_recreates_on_eula_prompt(monkeypatch):
    current_time = {'value': 0.0}

    def fake_time() -> float:
        return current_time['value']

    def fake_sleep(seconds: float) -> None:
        current_time['value'] += seconds

    compose_calls: list[bool] = []
    eula_checks: list[bool] = []
    accept_calls: list[bool] = []

    def fake_compose_up(path: Path, force_recreate: bool = False) -> None:
        compose_calls.append(force_recreate)

    def fake_sanitize() -> None:
        pass

    health_states = iter(['unhealthy', 'healthy'])

    def fake_docker_inspect(container: str, template: str) -> Optional[str]:
        if template == '{{.State.Status}}':
            return 'running'
        if template == '{{if .State.Health}}{{.State.Health.Status}}{{end}}':
            return next(health_states)
        if template == '{{.RestartCount}}':
            return '0'
        raise AssertionError(f'unexpected template: {template}')

    def fake_eula_prompt(container: str) -> bool:
        observed = not eula_checks
        eula_checks.append(True)
        return observed

    def fake_accept_eula(container: str) -> bool:
        accept_calls.append(True)
        return False

    monkeypatch.setattr(smoke, '_ensure_docker_compose_cli', lambda: None)
    monkeypatch.setattr(smoke, '_compose_file', lambda: Path('compose.yml'))
    monkeypatch.setattr(smoke, '_ensure_ecr_login_if_needed', lambda path: None)
    monkeypatch.setattr(smoke, '_compose_up', fake_compose_up)
    monkeypatch.setattr(smoke, '_sanitize_vertica_data_directories', fake_sanitize)
    monkeypatch.setattr(smoke, '_container_uptime_seconds', lambda container: 5.0)
    monkeypatch.setattr(smoke, '_docker_inspect', fake_docker_inspect)
    monkeypatch.setattr(smoke, '_container_is_responding', lambda: False)
    monkeypatch.setattr(smoke, '_container_reports_eula_prompt', fake_eula_prompt)
    monkeypatch.setattr(smoke, '_accept_vertica_eula', fake_accept_eula)
    monkeypatch.setattr(smoke, '_log_container_tail', lambda container, tail=200: None)
    monkeypatch.setattr(smoke, '_log_health_log_entries', lambda container, count: count)
    monkeypatch.setattr(smoke, '_ensure_container_admintools_conf_readable', lambda container: False)
    monkeypatch.setattr(smoke, '_reset_vertica_data_directories', lambda: False)
    monkeypatch.setattr(smoke, 'run_command', lambda command: None)
    monkeypatch.setattr(smoke, 'log', lambda message: None)
    monkeypatch.setattr(smoke.time, 'time', fake_time)
    monkeypatch.setattr(smoke.time, 'sleep', fake_sleep)
    monkeypatch.setattr(smoke, 'UNHEALTHY_HEALTHCHECK_GRACE_PERIOD_SECONDS', 30.0)

    smoke.ensure_vertica_container_running(timeout=120.0, compose_timeout=0.0)

    assert compose_calls and compose_calls[0] is True
    # Only the first unhealthy observation should trigger the recreate
    assert len(compose_calls) == 1
    assert eula_checks == [True]
    assert accept_calls == [True]


def test_ensure_vertica_accepts_eula_without_recreate(monkeypatch):
    current_time = {'value': 0.0}

    def fake_time() -> float:
        return current_time['value']

    def fake_sleep(seconds: float) -> None:
        current_time['value'] += seconds

    compose_calls: list[bool] = []
    accept_calls: list[bool] = []

    def fake_compose_up(path: Path, force_recreate: bool = False) -> None:
        compose_calls.append(force_recreate)

    health_states = iter(['unhealthy', 'healthy'])

    def fake_docker_inspect(container: str, template: str) -> Optional[str]:
        if template == '{{.State.Status}}':
            return 'running'
        if template == '{{if .State.Health}}{{.State.Health.Status}}{{end}}':
            return next(health_states)
        if template == '{{.RestartCount}}':
            return '0'
        raise AssertionError(f'unexpected template: {template}')

    def fake_eula_prompt(container: str) -> bool:
        return not accept_calls

    def fake_accept(container: str) -> bool:
        accept_calls.append(True)
        return True

    monkeypatch.setattr(smoke, '_ensure_docker_compose_cli', lambda: None)
    monkeypatch.setattr(smoke, '_compose_file', lambda: Path('compose.yml'))
    monkeypatch.setattr(smoke, '_ensure_ecr_login_if_needed', lambda path: None)
    monkeypatch.setattr(smoke, '_compose_up', fake_compose_up)
    monkeypatch.setattr(smoke, '_sanitize_vertica_data_directories', lambda: None)
    monkeypatch.setattr(smoke, '_container_uptime_seconds', lambda container: 5.0)
    monkeypatch.setattr(smoke, '_docker_inspect', fake_docker_inspect)
    monkeypatch.setattr(smoke, '_container_is_responding', lambda: False)
    monkeypatch.setattr(smoke, '_container_reports_eula_prompt', fake_eula_prompt)
    monkeypatch.setattr(smoke, '_accept_vertica_eula', fake_accept)
    monkeypatch.setattr(smoke, '_log_container_tail', lambda container, tail=200: None)
    monkeypatch.setattr(smoke, '_log_health_log_entries', lambda container, count: count)
    monkeypatch.setattr(smoke, '_ensure_container_admintools_conf_readable', lambda container: False)
    monkeypatch.setattr(smoke, '_reset_vertica_data_directories', lambda: False)
    monkeypatch.setattr(smoke, 'run_command', lambda command: None)
    monkeypatch.setattr(smoke, 'log', lambda message: None)
    monkeypatch.setattr(smoke.time, 'time', fake_time)
    monkeypatch.setattr(smoke.time, 'sleep', fake_sleep)
    monkeypatch.setattr(smoke, 'UNHEALTHY_HEALTHCHECK_GRACE_PERIOD_SECONDS', 30.0)

    smoke.ensure_vertica_container_running(timeout=120.0, compose_timeout=0.0)

    assert compose_calls == []
    assert accept_calls == [True]


def test_ensure_vertica_restarts_prolonged_starting(monkeypatch):
    current_time = {'value': 0.0}

    def fake_time() -> float:
        return current_time['value']

    def fake_sleep(seconds: float) -> None:
        current_time['value'] += seconds

    restart_commands: list[list[str]] = []

    def fake_run_command(command: list[str]) -> None:
        if command[:3] == ['docker', 'restart', 'vertica_ce']:
            restart_commands.append(command)
            return
        if command[:3] == ['docker', 'ps', '--filter']:
            return
        if command[:3] == ['docker', 'logs', '--tail']:
            return
        raise AssertionError(f'Unexpected command: {command}')

    def fake_docker_inspect(container: str, template: str) -> Optional[str]:
        if template == '{{.State.Status}}':
            return 'running'
        if template == '{{if .State.Health}}{{.State.Health.Status}}{{end}}':
            return 'starting'
        if template == '{{.RestartCount}}':
            return '0'
        raise AssertionError(f'Unexpected template: {template}')

    monkeypatch.setattr(smoke, '_ensure_docker_compose_cli', lambda: None)
    monkeypatch.setattr(smoke, '_container_uptime_seconds', lambda container: 100.0)
    monkeypatch.setattr(smoke, '_docker_inspect', fake_docker_inspect)
    monkeypatch.setattr(smoke, '_container_is_responding', lambda: False)
    monkeypatch.setattr(smoke, '_sanitize_vertica_data_directories', lambda: None)
    monkeypatch.setattr(smoke, '_log_container_tail', lambda container, tail=200: None)
    monkeypatch.setattr(smoke, '_log_health_log_entries', lambda container, count: count)
    monkeypatch.setattr(smoke, '_compose_file', lambda: None)
    monkeypatch.setattr(smoke, '_reset_vertica_data_directories', lambda: False)
    monkeypatch.setattr(smoke, 'run_command', fake_run_command)
    monkeypatch.setattr(smoke, 'log', lambda message: None)
    monkeypatch.setattr(smoke.time, 'time', fake_time)
    monkeypatch.setattr(smoke.time, 'sleep', fake_sleep)
    monkeypatch.setattr(smoke, 'UNHEALTHY_HEALTHCHECK_GRACE_PERIOD_SECONDS', 5.0)

    with pytest.raises(SystemExit) as excinfo:
        smoke.ensure_vertica_container_running(timeout=90.0, compose_timeout=0.0)

    assert 'stuck in starting state' in str(excinfo.value)
    assert restart_commands  # restarts should have been attempted


def test_sanitize_retains_missing_observation_until_container_confirms(monkeypatch, tmp_path):
    base_time = 1_700_000_000.0
    vertica_root = tmp_path / 'vertica'
    config_path = vertica_root / 'config'
    config_path.mkdir(parents=True)
    smoke._OBSERVED_VERTICA_CONFIG_DIRECTORIES.add(config_path)

    monkeypatch.setattr(smoke, 'VERTICA_DATA_DIRECTORIES', [tmp_path])
    monkeypatch.setattr(smoke, '_candidate_vertica_roots', lambda base: [vertica_root])
    monkeypatch.setattr(smoke, '_ensure_directory', lambda path: path.mkdir(parents=True, exist_ok=True) or True)
    monkeypatch.setattr(smoke, '_ensure_known_identity_tree', lambda *args, **kwargs: None)
    monkeypatch.setattr(smoke, '_align_identity_with_parent', lambda path: None)
    monkeypatch.setattr(smoke, '_ensure_known_identity', lambda path: None)
    monkeypatch.setattr(smoke, '_docker_inspect', lambda container, template: 'running' if template == '{{.State.Status}}' else 'unhealthy')
    monkeypatch.setattr(smoke, '_container_uptime_seconds', lambda container: 1000.0)
    monkeypatch.setattr(smoke, '_container_restart_count', lambda container: 0)
    monkeypatch.setattr(smoke, '_synchronize_container_admintools_conf', lambda container, source: False)
    monkeypatch.setattr(smoke, '_container_path_exists', lambda container, path: False)
    monkeypatch.setattr(smoke, 'log', lambda message: None)
    monkeypatch.setattr(smoke.time, 'time', lambda: base_time)

    smoke._ADMINTOOLS_CONF_MISSING_OBSERVED_AT[config_path] = base_time - 600

    try:
        smoke._sanitize_vertica_data_directories()
        assert (config_path / 'admintools.conf').exists()
        assert smoke._ADMINTOOLS_CONF_MISSING_OBSERVED_AT[config_path] == base_time - 600
        assert smoke._ADMINTOOLS_CONF_SEEDED_AT[config_path] == base_time
    finally:
        smoke._ADMINTOOLS_CONF_MISSING_OBSERVED_AT.pop(config_path, None)
        smoke._ADMINTOOLS_CONF_SEEDED_AT.pop(config_path, None)
        smoke._OBSERVED_VERTICA_CONFIG_DIRECTORIES.discard(config_path)


def test_sanitize_clears_missing_observation_when_container_has_config(monkeypatch, tmp_path):
    base_time = 1_700_000_100.0
    vertica_root = tmp_path / 'vertica'
    config_path = vertica_root / 'config'
    config_path.mkdir(parents=True)
    smoke._OBSERVED_VERTICA_CONFIG_DIRECTORIES.add(config_path)

    monkeypatch.setattr(smoke, 'VERTICA_DATA_DIRECTORIES', [tmp_path])
    monkeypatch.setattr(smoke, '_candidate_vertica_roots', lambda base: [vertica_root])
    monkeypatch.setattr(smoke, '_ensure_directory', lambda path: path.mkdir(parents=True, exist_ok=True) or True)
    monkeypatch.setattr(smoke, '_ensure_known_identity_tree', lambda *args, **kwargs: None)
    monkeypatch.setattr(smoke, '_align_identity_with_parent', lambda path: None)
    monkeypatch.setattr(smoke, '_ensure_known_identity', lambda path: None)
    monkeypatch.setattr(smoke, '_docker_inspect', lambda container, template: 'running' if template == '{{.State.Status}}' else 'unhealthy')
    monkeypatch.setattr(smoke, '_container_uptime_seconds', lambda container: 1000.0)
    monkeypatch.setattr(smoke, '_container_restart_count', lambda container: 0)

    synchronized_sources: list[Path] = []

    def fake_sync(container: str, source: Path) -> bool:
        synchronized_sources.append(source)
        return True

    monkeypatch.setattr(smoke, '_synchronize_container_admintools_conf', fake_sync)
    monkeypatch.setattr(smoke, '_container_path_exists', lambda container, path: True)
    monkeypatch.setattr(smoke, 'log', lambda message: None)
    monkeypatch.setattr(smoke.time, 'time', lambda: base_time)

    smoke._ADMINTOOLS_CONF_MISSING_OBSERVED_AT[config_path] = base_time - 600

    try:
        smoke._sanitize_vertica_data_directories()
        assert (config_path / 'admintools.conf').exists()
        assert synchronized_sources == [config_path / 'admintools.conf']
        assert config_path not in smoke._ADMINTOOLS_CONF_MISSING_OBSERVED_AT
        assert config_path not in smoke._ADMINTOOLS_CONF_SEEDED_AT
    finally:
        smoke._ADMINTOOLS_CONF_MISSING_OBSERVED_AT.pop(config_path, None)
        smoke._ADMINTOOLS_CONF_SEEDED_AT.pop(config_path, None)
        smoke._OBSERVED_VERTICA_CONFIG_DIRECTORIES.discard(config_path)


def test_sanitize_seeds_unobserved_config_after_grace(monkeypatch, tmp_path):
    base_time = 1_700_000_500.0
    vertica_root = tmp_path / 'vertica'
    vertica_root.mkdir()
    config_path = vertica_root / 'config'

    ensure_calls: list[Path] = []
    seed_calls: list[Path] = []

    current_time = {'value': base_time}

    def fake_time() -> float:
        return current_time['value']

    def fake_candidate_roots(base: Path) -> list[Path]:
        assert base == tmp_path
        return [vertica_root]

    def fake_ensure_directory(path: Path) -> bool:
        ensure_calls.append(path)
        path.mkdir(parents=True, exist_ok=True)
        return True

    def fake_seed(config_dir: Path) -> tuple[bool, bool]:
        seed_calls.append(config_dir)
        (config_dir / 'admintools.conf').write_text('test')
        return True, True

    monkeypatch.setattr(smoke, 'VERTICA_DATA_DIRECTORIES', [tmp_path])
    monkeypatch.setattr(smoke, '_candidate_vertica_roots', fake_candidate_roots)
    monkeypatch.setattr(smoke, '_ensure_directory', fake_ensure_directory)
    monkeypatch.setattr(smoke, '_ensure_known_identity_tree', lambda *args, **kwargs: None)
    monkeypatch.setattr(smoke, '_align_identity_with_parent', lambda path: None)
    monkeypatch.setattr(smoke, '_ensure_known_identity', lambda path: None)
    monkeypatch.setattr(smoke, '_docker_inspect', lambda container, template: 'running')
    monkeypatch.setattr(
        smoke, '_container_uptime_seconds', lambda container: smoke.ADMINTOOLS_CONF_MISSING_GRACE_PERIOD_SECONDS + 1
    )
    monkeypatch.setattr(
        smoke, '_container_restart_count', lambda container: smoke.ADMINTOOLS_CONF_MISSING_RESTART_THRESHOLD
    )
    monkeypatch.setattr(smoke, '_seed_default_admintools_conf', fake_seed)
    monkeypatch.setattr(smoke, '_synchronize_container_admintools_conf', lambda container, source: True)
    monkeypatch.setattr(smoke, '_container_path_exists', lambda container, path: True)
    monkeypatch.setattr(smoke, 'log', lambda message: None)
    monkeypatch.setattr(smoke.time, 'time', fake_time)

    smoke._ADMINTOOLS_CONF_MISSING_OBSERVED_AT.pop(config_path, None)
    smoke._ADMINTOOLS_CONF_SEEDED_AT.pop(config_path, None)
    smoke._OBSERVED_VERTICA_CONFIG_DIRECTORIES.discard(config_path)

    try:
        smoke._sanitize_vertica_data_directories()
        assert config_path not in smoke._OBSERVED_VERTICA_CONFIG_DIRECTORIES

        current_time['value'] = base_time + smoke.ADMINTOOLS_CONF_MISSING_GRACE_PERIOD_SECONDS + 10
        smoke._sanitize_vertica_data_directories()

        assert config_path in smoke._OBSERVED_VERTICA_CONFIG_DIRECTORIES
        assert ensure_calls.count(config_path) >= 1
        assert seed_calls == [config_path]
    finally:
        smoke._ADMINTOOLS_CONF_MISSING_OBSERVED_AT.pop(config_path, None)
        smoke._ADMINTOOLS_CONF_SEEDED_AT.pop(config_path, None)
        smoke._OBSERVED_VERTICA_CONFIG_DIRECTORIES.discard(config_path)


def test_candidate_vertica_roots_includes_base_when_config_missing(tmp_path):
    base_path = tmp_path / 'data' / 'vertica'
    base_path.mkdir(parents=True)

    # ``DB_NAME`` defaults to ``VMart`` so create a directory to mimic the
    # database-specific root while leaving ``config/`` absent to exercise the
    # regression scenario.
    (base_path / smoke.DB_NAME).mkdir()

    candidates = smoke._candidate_vertica_roots(base_path)

    assert base_path in candidates


def test_sanitize_removes_relative_opt_config_symlink(tmp_path, monkeypatch):
    base_path = tmp_path / 'vertica_data'
    base_path.mkdir()
    config_path = base_path / 'config'
    config_path.symlink_to(Path('../opt/vertica/config'))

    ensure_calls: list[Path] = []
    logs: list[str] = []

    def fake_candidate_roots(base: Path) -> list[Path]:
        return [base]

    def fake_ensure_directory(path: Path) -> bool:
        ensure_calls.append(path)
        if path == config_path:
            path.mkdir(parents=True, exist_ok=True)
        return True

    monkeypatch.setattr(smoke, 'VERTICA_DATA_DIRECTORIES', [base_path], raising=False)
    monkeypatch.setattr(smoke, '_candidate_vertica_roots', fake_candidate_roots)
    monkeypatch.setattr(smoke, '_ensure_directory', fake_ensure_directory)
    monkeypatch.setattr(smoke, '_ensure_known_identity_tree', lambda *args, **kwargs: None)
    monkeypatch.setattr(smoke, '_docker_inspect', lambda container, template: None)
    monkeypatch.setattr(smoke, '_container_uptime_seconds', lambda container: 0.0)
    monkeypatch.setattr(smoke, '_container_restart_count', lambda container: 0)
    monkeypatch.setattr(smoke, '_seed_default_admintools_conf', lambda config_dir: (True, False))
    monkeypatch.setattr(smoke, '_synchronize_container_admintools_conf', lambda container, source: True)
    monkeypatch.setattr(smoke, '_container_path_exists', lambda container, path: True)
    monkeypatch.setattr(smoke, 'log', lambda message: logs.append(message))

    smoke._sanitize_vertica_data_directories()

    assert not config_path.is_symlink()
    assert any('Removing confusing symlink' in entry for entry in logs)
    assert base_path in ensure_calls


def test_sanitize_rebuilds_config_after_same_file_logs(monkeypatch, tmp_path):
    base_path = tmp_path / 'vertica'
    base_path.mkdir()
    config_path = base_path / 'config'
    config_path.mkdir()
    (config_path / 'placeholder').write_text('test')

    logs: list[str] = []
    restart_requests: list[tuple[str, str]] = []

    def fake_restart(container: str, reason: str) -> bool:
        restart_requests.append((container, reason))
        return True

    def fake_candidate_roots(base: Path) -> list[Path]:
        assert base == base_path
        return [base]

    def fake_ensure_directory(path: Path) -> bool:
        path.mkdir(parents=True, exist_ok=True)
        return True

    monkeypatch.setattr(smoke, 'VERTICA_DATA_DIRECTORIES', [base_path], raising=False)
    monkeypatch.setattr(smoke, '_candidate_vertica_roots', fake_candidate_roots)
    monkeypatch.setattr(smoke, '_ensure_directory', fake_ensure_directory)
    monkeypatch.setattr(smoke, '_ensure_known_identity_tree', lambda *args, **kwargs: None)
    monkeypatch.setattr(smoke, '_align_identity_with_parent', lambda path: None)
    monkeypatch.setattr(smoke, '_ensure_known_identity', lambda path: None)
    monkeypatch.setattr(smoke, '_docker_inspect', lambda container, template: 'running')
    monkeypatch.setattr(smoke, '_container_uptime_seconds', lambda container: 1000.0)
    monkeypatch.setattr(smoke, '_container_restart_count', lambda container: 0)
    seed_calls: list[Path] = []

    def fake_seed(config_dir: Path) -> tuple[bool, bool]:
        seed_calls.append(config_dir)
        return True, False

    monkeypatch.setattr(smoke, '_seed_default_admintools_conf', fake_seed)
    monkeypatch.setattr(smoke, '_synchronize_container_admintools_conf', lambda container, source: False)
    monkeypatch.setattr(smoke, '_container_path_exists', lambda container, path: False)
    monkeypatch.setattr(smoke, '_container_reports_config_same_file_issue', lambda container: True)
    monkeypatch.setattr(smoke, '_restart_vertica_container', fake_restart, raising=False)
    monkeypatch.setattr(smoke, 'log', lambda message: logs.append(message))

    smoke._OBSERVED_VERTICA_CONFIG_DIRECTORIES.add(config_path)

    smoke._sanitize_vertica_data_directories()

    assert config_path.exists()
    assert base_path.exists()
    assert set(smoke._VERTICA_CONFIG_SAME_FILE_RECOVERED) == {config_path}
    assert restart_requests == [('vertica_ce', 'apply recovered configuration defaults')]
    assert seed_calls == [config_path]
    assert any('identical Vertica configuration source' in entry for entry in logs)


def test_ensure_vertica_rechecks_sanitize_during_unhealthy(monkeypatch):
    current_time = {'value': 0.0}

    def fake_time() -> float:
        return current_time['value']

    def fake_sleep(seconds: float) -> None:
        current_time['value'] += seconds

    health_states = iter(['unhealthy'] * 5 + ['healthy'])

    def fake_docker_inspect(container: str, template: str) -> Optional[str]:
        if template == '{{.State.Status}}':
            return 'running'
        if template == '{{if .State.Health}}{{.State.Health.Status}}{{end}}':
            return next(health_states)
        raise AssertionError(f'Unexpected template: {template}')

    sanitize_calls: list[float] = []

    def fake_sanitize() -> None:
        sanitize_calls.append(current_time['value'])

    monkeypatch.setattr(smoke, '_ensure_docker_compose_cli', lambda: None)
    monkeypatch.setattr(smoke, '_container_uptime_seconds', lambda container: 10.0)
    monkeypatch.setattr(smoke, '_docker_inspect', fake_docker_inspect)
    monkeypatch.setattr(smoke, '_sanitize_vertica_data_directories', fake_sanitize)
    monkeypatch.setattr(smoke, '_ensure_container_admintools_conf_readable', lambda container: False)
    monkeypatch.setattr(smoke, '_log_container_tail', lambda container, tail=200: None)
    monkeypatch.setattr(smoke, '_log_health_log_entries', lambda container, count: count)
    monkeypatch.setattr(smoke, 'run_command', lambda command: None)
    monkeypatch.setattr(smoke, 'log', lambda message: None)
    monkeypatch.setattr(smoke.time, 'time', fake_time)
    monkeypatch.setattr(smoke.time, 'sleep', fake_sleep)

    smoke.ensure_vertica_container_running(timeout=120.0, compose_timeout=0.0)

    assert sanitize_calls == [0.0, 10.0, 20.0, 30.0, 40.0]


def test_health_log_indicates_missing_database():
    entries = [
        {
            'Output': 'vsql: Database VMart is not defined. Defined databases []',
            'ExitCode': 1,
        }
    ]

    assert smoke._health_log_indicates_missing_database(entries, 'VMart') is True
    assert smoke._health_log_indicates_missing_database([], 'VMart') is False
    assert (
        smoke._health_log_indicates_missing_database(
            [{'Output': 'Some other error'}], 'VMart'
        )
        is False
    )


def test_container_logs_indicate_missing_database(monkeypatch):
    results = [
        SimpleNamespace(
            returncode=0,
            stdout='Database VMart is not defined. Defined databases []',
            stderr='',
        ),
        SimpleNamespace(returncode=1, stdout='', stderr=''),
        SimpleNamespace(returncode=0, stdout='All good here', stderr=''),
    ]

    def fake_run(command, capture_output, text):
        assert command == ['docker', 'logs', '--tail', '200', 'vertica_ce']
        assert capture_output is True
        assert text is True
        return results.pop(0)

    monkeypatch.setattr(smoke.subprocess, 'run', fake_run)

    assert smoke._container_logs_indicate_missing_database('vertica_ce', 'VMart') is True
    assert smoke._container_logs_indicate_missing_database('vertica_ce', 'VMart') is False
    assert smoke._container_logs_indicate_missing_database('vertica_ce', 'VMart') is False


def test_ensure_vertica_creates_database_when_missing(monkeypatch):
    current_time = {'value': 0.0}

    def fake_time() -> float:
        return current_time['value']

    def fake_sleep(seconds: float) -> None:
        current_time['value'] += seconds

    health_states = iter(['starting', 'starting', 'healthy'])

    def fake_docker_inspect(container: str, template: str) -> Optional[str]:
        if template == '{{.State.Status}}':
            return 'running'
        if template == '{{if .State.Health}}{{.State.Health.Status}}{{end}}':
            return next(health_states)
        if template == '{{.RestartCount}}':
            return '0'
        raise AssertionError(f'unexpected template: {template}')

    health_log_calls = {'count': 0}

    def fake_health_log(container: str) -> list[dict[str, object]]:
        health_log_calls['count'] += 1
        if health_log_calls['count'] <= 2:
            return [
                {
                    'Output': 'Database VMart is not defined. Defined databases []',
                    'ExitCode': 1,
                }
            ]
        return []

    creation_calls: list[tuple[str, str, float]] = []

    def fake_attempt_creation(container: str, database: str) -> bool:
        creation_calls.append((container, database, current_time['value']))
        return True

    monkeypatch.setattr(smoke, '_ensure_docker_compose_cli', lambda: None)
    monkeypatch.setattr(smoke, '_sanitize_vertica_data_directories', lambda: None)
    monkeypatch.setattr(smoke, '_ensure_container_admintools_conf_readable', lambda container: False)
    monkeypatch.setattr(smoke, '_reset_vertica_data_directories', lambda: False)
    monkeypatch.setattr(smoke, '_compose_file', lambda: Path('compose.yml'))
    monkeypatch.setattr(smoke, '_ensure_ecr_login_if_needed', lambda path: None)
    monkeypatch.setattr(smoke, '_compose_up', lambda path, force_recreate=False: None)
    monkeypatch.setattr(smoke, '_container_uptime_seconds', lambda container: 600.0)
    monkeypatch.setattr(smoke, '_docker_inspect', fake_docker_inspect)
    monkeypatch.setattr(smoke, '_docker_health_log', fake_health_log)
    monkeypatch.setattr(smoke, '_log_health_log_entries', lambda container, count: count)
    monkeypatch.setattr(smoke, '_attempt_vertica_database_creation', fake_attempt_creation)
    monkeypatch.setattr(smoke, '_container_is_responding', lambda: False)
    monkeypatch.setattr(smoke, '_container_reports_eula_prompt', lambda container: False)
    monkeypatch.setattr(smoke, '_accept_vertica_eula', lambda container: False)
    monkeypatch.setattr(smoke, 'run_command', lambda command: None)
    monkeypatch.setattr(smoke, 'log', lambda message: None)
    monkeypatch.setattr(smoke.time, 'time', fake_time)
    monkeypatch.setattr(smoke.time, 'sleep', fake_sleep)
    monkeypatch.setattr(smoke, 'UNHEALTHY_HEALTHCHECK_GRACE_PERIOD_SECONDS', 0.0)

    smoke.ensure_vertica_container_running(timeout=120.0, compose_timeout=0.0)

    assert creation_calls
    container, database, observed_time = creation_calls[0]
    assert container == 'vertica_ce'
    assert database == smoke.DB_NAME
    assert observed_time >= 0.0


def test_ensure_vertica_creates_database_when_logs_indicate_missing(monkeypatch):
    current_time = {'value': 0.0}

    def fake_time() -> float:
        return current_time['value']

    def fake_sleep(seconds: float) -> None:
        current_time['value'] += seconds

    health_states = iter(['starting', 'starting', 'healthy'])

    def fake_docker_inspect(container: str, template: str) -> Optional[str]:
        if template == '{{.State.Status}}':
            return 'running'
        if template == '{{if .State.Health}}{{.State.Health.Status}}{{end}}':
            return next(health_states)
        if template == '{{.RestartCount}}':
            return '0'
        raise AssertionError(f'unexpected template: {template}')

    detection_calls = {'count': 0}

    def fake_logs_missing(container: str, database: str, tail: int = 200) -> bool:
        detection_calls['count'] += 1
        return detection_calls['count'] <= 2

    creation_calls: list[tuple[str, str, float]] = []

    def fake_attempt_creation(container: str, database: str) -> bool:
        creation_calls.append((container, database, current_time['value']))
        return True

    monkeypatch.setattr(smoke, '_ensure_docker_compose_cli', lambda: None)
    monkeypatch.setattr(smoke, '_sanitize_vertica_data_directories', lambda: None)
    monkeypatch.setattr(smoke, '_ensure_container_admintools_conf_readable', lambda container: False)
    monkeypatch.setattr(smoke, '_reset_vertica_data_directories', lambda: False)
    monkeypatch.setattr(smoke, '_compose_file', lambda: Path('compose.yml'))
    monkeypatch.setattr(smoke, '_ensure_ecr_login_if_needed', lambda path: None)
    monkeypatch.setattr(smoke, '_compose_up', lambda path, force_recreate=False: None)
    monkeypatch.setattr(smoke, '_container_uptime_seconds', lambda container: 600.0)
    monkeypatch.setattr(smoke, '_docker_inspect', fake_docker_inspect)
    monkeypatch.setattr(smoke, '_docker_health_log', lambda container: [])
    monkeypatch.setattr(smoke, '_container_logs_indicate_missing_database', fake_logs_missing)
    monkeypatch.setattr(smoke, '_log_health_log_entries', lambda container, count: count)
    monkeypatch.setattr(smoke, '_attempt_vertica_database_creation', fake_attempt_creation)
    monkeypatch.setattr(smoke, '_container_is_responding', lambda: False)
    monkeypatch.setattr(smoke, '_container_reports_eula_prompt', lambda container: False)
    monkeypatch.setattr(smoke, '_accept_vertica_eula', lambda container: False)
    monkeypatch.setattr(smoke, 'run_command', lambda command: None)
    monkeypatch.setattr(smoke, 'log', lambda message: None)
    monkeypatch.setattr(smoke.time, 'time', fake_time)
    monkeypatch.setattr(smoke.time, 'sleep', fake_sleep)
    monkeypatch.setattr(smoke, 'UNHEALTHY_HEALTHCHECK_GRACE_PERIOD_SECONDS', 0.0)

    smoke.ensure_vertica_container_running(timeout=120.0, compose_timeout=0.0)

    assert creation_calls
    container, database, observed_time = creation_calls[0]
    assert container == 'vertica_ce'
    assert database == smoke.DB_NAME
    assert observed_time >= 0.0


def test_sanitize_seeds_admintools_conf_after_restarts(tmp_path, monkeypatch):
    base = tmp_path / 'data'
    base.mkdir()
    vertica_root = base / 'vertica'
    vertica_root.mkdir()

    logs: list[str] = []
    monkeypatch.setattr(smoke, 'log', logs.append)
    monkeypatch.setattr(smoke, 'VERTICA_DATA_DIRECTORIES', [base])
    monkeypatch.setattr(smoke, '_ensure_known_identity_tree', lambda *args, **kwargs: None)
    monkeypatch.setattr(smoke, '_ensure_known_identity', lambda path: None)
    monkeypatch.setattr(smoke, '_ensure_vertica_admin_identity', lambda path: None)
    monkeypatch.setattr(smoke, '_container_uptime_seconds', lambda container: 60.0)
    monkeypatch.setattr(smoke, '_container_restart_count', lambda container: 3)

    def fake_inspect(container: str, template: str) -> Optional[str]:
        if template == '{{.State.Status}}':
            return 'running'
        if template == '{{if .State.Health}}{{.State.Health.Status}}{{end}}':
            return 'unhealthy'
        raise AssertionError(f'unexpected template: {template}')

    monkeypatch.setattr(smoke, '_docker_inspect', fake_inspect)

    seed_calls: list[Path] = []

    seeded_paths: set[Path] = set()

    def fake_seed(path: Path) -> tuple[bool, bool]:
        seed_calls.append(path)
        path.mkdir(parents=True, exist_ok=True)
        changed = path not in seeded_paths
        seeded_paths.add(path)
        return True, changed

    monkeypatch.setattr(smoke, '_seed_default_admintools_conf', fake_seed)
    monkeypatch.setattr(smoke, '_synchronize_container_admintools_conf', lambda *args, **kwargs: False)
    smoke._OBSERVED_VERTICA_CONFIG_DIRECTORIES.update(
        {base / 'config', vertica_root / 'config', base / smoke.DB_NAME / 'config'}
    )

    smoke._sanitize_vertica_data_directories()

    assert seed_calls
    expected_targets = {
        base / 'config',
        vertica_root / 'config',
        base / 'VMart' / 'config',
    }
    assert set(seed_calls).issubset(expected_targets)
    assert any('restart count' in entry for entry in logs)
    smoke._OBSERVED_VERTICA_CONFIG_DIRECTORIES.clear()


def test_sanitize_seeds_admintools_conf_after_missing_duration(tmp_path, monkeypatch):
    base = tmp_path / 'data'
    base.mkdir()
    vertica_root = base / 'vertica'
    vertica_root.mkdir()

    logs: list[str] = []
    current_time = {'value': 0.0}

    def fake_time() -> float:
        return current_time['value']

    monkeypatch.setattr(smoke, 'log', logs.append)
    monkeypatch.setattr(smoke, 'VERTICA_DATA_DIRECTORIES', [base])
    monkeypatch.setattr(smoke, '_ensure_known_identity_tree', lambda *args, **kwargs: None)
    monkeypatch.setattr(smoke, '_ensure_known_identity', lambda path: None)
    monkeypatch.setattr(smoke, '_ensure_vertica_admin_identity', lambda path: None)
    monkeypatch.setattr(smoke.time, 'time', fake_time)
    monkeypatch.setattr(smoke.time, 'sleep', lambda seconds: None)
    monkeypatch.setattr(smoke, '_container_uptime_seconds', lambda container: 10.0)
    monkeypatch.setattr(smoke, '_container_restart_count', lambda container: 0)

    def fake_inspect(container: str, template: str) -> Optional[str]:
        if template == '{{.State.Status}}':
            return 'running'
        if template == '{{if .State.Health}}{{.State.Health.Status}}{{end}}':
            return 'unhealthy'
        raise AssertionError(f'unexpected template: {template}')

    monkeypatch.setattr(smoke, '_docker_inspect', fake_inspect)

    seed_calls: list[Path] = []

    seeded_paths: set[Path] = set()

    def fake_seed(path: Path) -> tuple[bool, bool]:
        seed_calls.append(path)
        path.mkdir(parents=True, exist_ok=True)
        changed = path not in seeded_paths
        seeded_paths.add(path)
        return True, changed

    monkeypatch.setattr(smoke, '_seed_default_admintools_conf', fake_seed)
    monkeypatch.setattr(smoke, '_synchronize_container_admintools_conf', lambda *args, **kwargs: False)
    smoke._ADMINTOOLS_CONF_MISSING_OBSERVED_AT.clear()
    smoke._ADMINTOOLS_CONF_SEEDED_AT.clear()
    smoke._OBSERVED_VERTICA_CONFIG_DIRECTORIES.update(
        {base / 'config', vertica_root / 'config', base / smoke.DB_NAME / 'config'}
    )

    smoke._sanitize_vertica_data_directories()
    assert not seed_calls

    current_time['value'] = smoke.ADMINTOOLS_CONF_MISSING_GRACE_PERIOD_SECONDS + 1

    smoke._sanitize_vertica_data_directories()

    assert seed_calls
    assert any('missing for' in entry for entry in logs)
    smoke._ADMINTOOLS_CONF_MISSING_OBSERVED_AT.clear()
    smoke._ADMINTOOLS_CONF_SEEDED_AT.clear()
    smoke._OBSERVED_VERTICA_CONFIG_DIRECTORIES.clear()


def test_sanitize_rebuilds_config_after_seed_timeout(tmp_path, monkeypatch):
    base = tmp_path / 'data'
    base.mkdir()
    vertica_root = base / 'vertica'
    vertica_root.mkdir()

    logs: list[str] = []
    current_time = {'value': 0.0}

    def fake_time() -> float:
        return current_time['value']

    monkeypatch.setattr(smoke, 'log', logs.append)
    monkeypatch.setattr(smoke, 'VERTICA_DATA_DIRECTORIES', [base])
    monkeypatch.setattr(smoke, '_ensure_known_identity_tree', lambda *args, **kwargs: None)
    monkeypatch.setattr(smoke, '_ensure_known_identity', lambda path: None)
    monkeypatch.setattr(smoke, '_ensure_vertica_admin_identity', lambda path: None)
    monkeypatch.setattr(smoke.time, 'time', fake_time)
    monkeypatch.setattr(smoke.time, 'sleep', lambda seconds: None)
    monkeypatch.setattr(smoke, '_container_uptime_seconds', lambda container: 600.0)
    monkeypatch.setattr(smoke, '_container_restart_count', lambda container: 0)
    monkeypatch.setattr(smoke, '_synchronize_container_admintools_conf', lambda *args, **kwargs: True)
    monkeypatch.setattr(smoke, 'ADMINTOOLS_CONF_SEED_RECOVERY_SECONDS', 5.0)

    def fake_inspect(container: str, template: str) -> Optional[str]:
        if template == '{{.State.Status}}':
            return 'running'
        if template == '{{if .State.Health}}{{.State.Health.Status}}{{end}}':
            return 'unhealthy'
        raise AssertionError(template)

    monkeypatch.setattr(smoke, '_docker_inspect', fake_inspect)

    seed_calls: list[Path] = []

    seeded_paths: set[Path] = set()

    def fake_seed(path: Path) -> tuple[bool, bool]:
        seed_calls.append(path)
        path.mkdir(parents=True, exist_ok=True)
        changed = path not in seeded_paths
        seeded_paths.add(path)
        return True, changed

    monkeypatch.setattr(smoke, '_seed_default_admintools_conf', fake_seed)

    removal_calls: list[Path] = []

    def fake_rmtree(path: Path) -> None:
        removal_calls.append(path)

    monkeypatch.setattr(smoke.shutil, 'rmtree', fake_rmtree)

    def fake_run(args, capture_output=True, text=True, **kwargs):
        if args == ['docker', 'rm', '-f', 'vertica_ce']:
            return subprocess.CompletedProcess(args, 0, '', '')
        raise AssertionError(args)

    monkeypatch.setattr(smoke.subprocess, 'run', fake_run)

    smoke._ADMINTOOLS_CONF_MISSING_OBSERVED_AT.clear()
    smoke._ADMINTOOLS_CONF_SEEDED_AT.clear()
    smoke._OBSERVED_VERTICA_CONFIG_DIRECTORIES.update(
        {vertica_root / 'config', base / 'config', base / smoke.DB_NAME / 'config'}
    )

    smoke._sanitize_vertica_data_directories()

    assert seed_calls == [vertica_root / 'config', base / 'config']
    assert not removal_calls

    current_time['value'] = 10.0

    smoke._sanitize_vertica_data_directories()

    assert removal_calls == [vertica_root, base / 'config']
    assert any('remains missing for' in entry for entry in logs)

    smoke._ADMINTOOLS_CONF_MISSING_OBSERVED_AT.clear()
    smoke._ADMINTOOLS_CONF_SEEDED_AT.clear()


def test_sanitize_defers_seeding_until_config_observed(tmp_path, monkeypatch):
    base = tmp_path / 'data'
    base.mkdir()

    logs: list[str] = []
    current_time = {'value': 0.0}

    def fake_time() -> float:
        return current_time['value']

    monkeypatch.setattr(smoke, 'log', logs.append)
    monkeypatch.setattr(smoke, 'VERTICA_DATA_DIRECTORIES', [base])
    monkeypatch.setattr(smoke, '_ensure_known_identity_tree', lambda *args, **kwargs: None)
    monkeypatch.setattr(smoke, '_ensure_known_identity', lambda path: None)
    monkeypatch.setattr(smoke, '_ensure_vertica_admin_identity', lambda path: None)
    monkeypatch.setattr(smoke.time, 'time', fake_time)
    monkeypatch.setattr(smoke.time, 'sleep', lambda seconds: None)
    monkeypatch.setattr(smoke, '_container_uptime_seconds', lambda container: 600.0)
    monkeypatch.setattr(smoke, '_container_restart_count', lambda container: 0)

    def fake_inspect(container: str, template: str) -> Optional[str]:
        if template == '{{.State.Status}}':
            return 'running'
        if template == '{{if .State.Health}}{{.State.Health.Status}}{{end}}':
            return 'unhealthy'
        raise AssertionError(template)

    monkeypatch.setattr(smoke, '_docker_inspect', fake_inspect)

    seed_calls: list[Path] = []

    def fake_seed(path: Path) -> tuple[bool, bool]:
        seed_calls.append(path)
        return True, True

    monkeypatch.setattr(smoke, '_seed_default_admintools_conf', fake_seed)
    monkeypatch.setattr(smoke, '_synchronize_container_admintools_conf', lambda *args, **kwargs: False)
    smoke._ADMINTOOLS_CONF_MISSING_OBSERVED_AT.clear()
    smoke._ADMINTOOLS_CONF_SEEDED_AT.clear()
    smoke._OBSERVED_VERTICA_CONFIG_DIRECTORIES.clear()

    smoke._sanitize_vertica_data_directories()
    assert not seed_calls

    current_time['value'] = smoke.ADMINTOOLS_CONF_MISSING_GRACE_PERIOD_SECONDS + 600.0
    smoke._sanitize_vertica_data_directories()

    assert seed_calls == [base / 'config']
    assert any('creating directory and seeding defaults to assist recovery' in entry for entry in logs)
    smoke._ADMINTOOLS_CONF_MISSING_OBSERVED_AT.clear()
    smoke._ADMINTOOLS_CONF_SEEDED_AT.clear()
    smoke._OBSERVED_VERTICA_CONFIG_DIRECTORIES.clear()


def test_sanitize_restarts_container_after_admintools_seed(tmp_path, monkeypatch):
    base = tmp_path / 'data'
    base.mkdir()
    config_dir = base / 'config'
    config_dir.mkdir()

    logs: list[str] = []
    restart_requests: list[tuple[str, str]] = []

    monkeypatch.setattr(smoke, 'log', logs.append)
    monkeypatch.setattr(smoke, 'VERTICA_DATA_DIRECTORIES', [base])
    monkeypatch.setattr(smoke, '_ensure_known_identity_tree', lambda *args, **kwargs: None)
    monkeypatch.setattr(smoke, '_ensure_known_identity', lambda path: None)
    monkeypatch.setattr(smoke, '_ensure_vertica_admin_identity', lambda path: None)
    monkeypatch.setattr(smoke, '_container_uptime_seconds', lambda container: 600.0)
    monkeypatch.setattr(smoke, '_container_restart_count', lambda container: 0)

    def fake_inspect(container: str, template: str) -> Optional[str]:
        if template == '{{.State.Status}}':
            return 'running'
        if template == '{{if .State.Health}}{{.State.Health.Status}}{{end}}':
            return 'unhealthy'
        raise AssertionError(f'unexpected template: {template}')

    monkeypatch.setattr(smoke, '_docker_inspect', fake_inspect)

    def fake_seed(path: Path) -> tuple[bool, bool]:
        path.mkdir(parents=True, exist_ok=True)
        (path / 'admintools.conf').write_text('test')
        return True, True

    monkeypatch.setattr(smoke, '_seed_default_admintools_conf', fake_seed)
    monkeypatch.setattr(smoke, '_synchronize_container_admintools_conf', lambda *args, **kwargs: True)

    def record_restart(container: str, reason: str) -> bool:
        restart_requests.append((container, reason))
        return True

    monkeypatch.setattr(smoke, '_restart_vertica_container', record_restart)

    observed = {config_dir, base / 'vertica' / 'config', base / smoke.DB_NAME / 'config'}
    smoke._OBSERVED_VERTICA_CONFIG_DIRECTORIES.update(observed)
    smoke._ADMINTOOLS_CONF_MISSING_OBSERVED_AT[config_dir] = 0.0

    smoke._sanitize_vertica_data_directories()

    assert restart_requests == [('vertica_ce', 'apply seeded admintools.conf')]

    smoke._OBSERVED_VERTICA_CONFIG_DIRECTORIES.clear()
    smoke._ADMINTOOLS_CONF_MISSING_OBSERVED_AT.clear()


def test_seed_default_admintools_conf(tmp_path, monkeypatch):
    logs: list[str] = []

    monkeypatch.setattr(smoke, 'log', logs.append)

    config_dir = tmp_path / 'config'
    success, changed = smoke._seed_default_admintools_conf(config_dir)
    assert success is True
    assert changed is True

    conf_path = config_dir / 'admintools.conf'
    assert conf_path.exists()
    content = conf_path.read_text()
    assert '[Configuration]' in content
    assert 'admintools_config_version = 110' in content
    assert 'hosts = 127.0.0.1' in content
    assert 'node0001 = 127.0.0.1' in content
    assert conf_path.stat().st_mode & 0o777 == 0o666


def test_seed_default_admintools_conf_uses_image_template(tmp_path, monkeypatch):
    config_dir = tmp_path / 'config'
    config_dir.mkdir()

    template = smoke.DEFAULT_ADMINTOOLS_CONF.replace('format = 3', 'format = 42')

    monkeypatch.setattr(smoke, '_image_default_admintools_conf', lambda: template)

    success, changed = smoke._seed_default_admintools_conf(config_dir)

    assert success is True
    assert changed is True

    content = (config_dir / 'admintools.conf').read_text()
    assert 'format = 42' in content


def test_seed_default_admintools_conf_is_idempotent(tmp_path, monkeypatch):
    config_dir = tmp_path / 'config'
    config_dir.mkdir()

    success, changed = smoke._seed_default_admintools_conf(config_dir)
    assert success is True
    assert changed is True

    existing = config_dir / 'admintools.conf'
    original_content = existing.read_text()

    success, changed = smoke._seed_default_admintools_conf(config_dir)
    assert success is True
    assert changed is False

    assert existing.read_text() == original_content


def test_seed_default_admintools_conf_rebuilds_invalid_file(tmp_path, monkeypatch):
    logs: list[str] = []

    monkeypatch.setattr(smoke, 'log', logs.append)

    config_dir = tmp_path / 'config'
    config_dir.mkdir()
    existing = config_dir / 'admintools.conf'
    existing.write_text('custom')

    success, changed = smoke._seed_default_admintools_conf(config_dir)
    assert success is True
    assert changed is True

    content = existing.read_text()
    assert '[Configuration]' in content
    assert 'hosts = 127.0.0.1' in content
    assert any('attempting to rebuild it with safe defaults' in entry for entry in logs)


def test_seed_default_admintools_conf_removes_symlink(tmp_path, monkeypatch):
    logs: list[str] = []

    monkeypatch.setattr(smoke, 'log', logs.append)

    config_dir = tmp_path / 'config'
    config_dir.mkdir()
    existing = config_dir / 'admintools.conf'
    existing.symlink_to(existing)

    success, changed = smoke._seed_default_admintools_conf(config_dir)

    assert success is True
    assert changed is True
    assert existing.exists()
    assert not existing.is_symlink()
    assert any('Removing symlinked admintools.conf' in entry for entry in logs)


def test_image_default_admintools_conf_uses_known_paths(monkeypatch):
    monkeypatch.setattr(smoke, '_resolve_vertica_image_name', lambda: 'image')
    monkeypatch.setattr(smoke.shutil, 'which', lambda _: '/usr/bin/docker')

    login_calls: list[str] = []

    def fake_login(image_name: str) -> bool:
        login_calls.append(image_name)
        return True

    monkeypatch.setattr(smoke, '_ensure_ecr_login_for_image', fake_login)

    captured_args: list[list[str]] = []

    def fake_run(args, capture_output=True, text=True, **kwargs):
        captured_args.append(args)
        assert args[:5] == ['docker', 'run', '--rm', '--entrypoint', '/bin/sh']
        assert args[5] == 'image'
        assert args[6] == '-c'
        script = args[7]
        assert 'find /opt/vertica -maxdepth 6 -type f -name admintools.conf' in script
        assert args[8] == '--'
        assert '/opt/vertica/config/admintools.conf' in args[9:]
        assert '/opt/vertica/config/admintools/admintools.conf' in args[9:]
        assert '/opt/vertica/share/admintools/admintools.conf' in args[9:]
        return subprocess.CompletedProcess(args, 0, stdout='template-content', stderr='')

    monkeypatch.setattr(smoke.subprocess, 'run', fake_run)

    template = smoke._image_default_admintools_conf()

    assert template == 'template-content'
    assert captured_args
    assert login_calls == ['image']


def test_image_default_admintools_conf_logs_failure(monkeypatch):
    logs: list[str] = []

    monkeypatch.setattr(smoke, 'log', logs.append)
    monkeypatch.setattr(smoke, '_resolve_vertica_image_name', lambda: 'image')
    monkeypatch.setattr(smoke.shutil, 'which', lambda _: '/usr/bin/docker')

    def fake_run(args, capture_output=True, text=True, **kwargs):
        return subprocess.CompletedProcess(args, 1, stdout='', stderr='cat: not found\n')

    monkeypatch.setattr(smoke.subprocess, 'run', fake_run)

    assert smoke._image_default_admintools_conf() is None
    assert any('[stderr] cat: not found' in entry for entry in logs)
    assert any('Failed to extract admintools.conf template from Vertica image image' in entry for entry in logs)


def test_image_default_admintools_conf_handles_login_failure(monkeypatch):
    logs: list[str] = []

    monkeypatch.setattr(smoke, 'log', logs.append)

    image_name = '123456789012.dkr.ecr.us-east-1.amazonaws.com/vertica-ce:latest'

    monkeypatch.setattr(smoke, '_resolve_vertica_image_name', lambda: image_name)
    monkeypatch.setattr(smoke.shutil, 'which', lambda _: '/usr/bin/docker')

    def fake_login(_: str) -> bool:
        raise SystemExit('login failed')

    monkeypatch.setattr(smoke, '_ensure_ecr_login_for_image', fake_login)

    def fail_run(*args, **kwargs):  # pragma: no cover - should not be invoked
        raise AssertionError('docker run should not be invoked when registry login fails')

    monkeypatch.setattr(smoke.subprocess, 'run', fail_run)

    assert smoke._image_default_admintools_conf() is None
    assert any('Unable to authenticate with registry for Vertica image' in entry for entry in logs)
    assert any('login failed' in entry for entry in logs)


def test_synchronize_container_admintools_conf_success(tmp_path, monkeypatch):
    source = tmp_path / 'admintools.conf'
    source.write_text('test')

    logs: list[str] = []

    monkeypatch.setattr(smoke, 'log', logs.append)
    monkeypatch.setattr(smoke.shutil, 'which', lambda cmd: '/usr/bin/docker' if cmd == 'docker' else None)
    monkeypatch.setattr(smoke, '_container_path_exists', lambda container, path: True)
    monkeypatch.setattr(smoke, '_container_dbadmin_identity', lambda container: (1000, 1000))

    def fake_run(args, capture_output=True, text=True, **kwargs):
        if args[:3] == ['docker', 'exec', '--user']:
            assert args[3] in {'0', 'dbadmin'}
            assert args[4] == 'vertica_ce'
            command = args[5:]
            if command == ['rm', '-f', '/opt/vertica/config/admintools.conf']:
                return subprocess.CompletedProcess(args, 0, '', '')
            if command == ['mkdir', '-p', '/opt/vertica/config']:
                return subprocess.CompletedProcess(args, 0, '', '')
            if command == ['sh', '-c', 'stat -c "%u:%g" /opt/vertica/config/admintools.conf']:
                return subprocess.CompletedProcess(args, 0, '0:0', '')
            if command == ['sh', '-c', 'chown 1000:1000 /opt/vertica/config/admintools.conf']:
                return subprocess.CompletedProcess(args, 0, '', '')
            raise AssertionError(args)
        if args[:2] == ['docker', 'cp']:
            assert args[2].endswith('admintools.conf')
            assert args[3] == 'vertica_ce:/opt/vertica/config/admintools.conf'
            return subprocess.CompletedProcess(args, 0, '', '')
        raise AssertionError(args)

    monkeypatch.setattr(smoke.subprocess, 'run', fake_run)

    assert smoke._synchronize_container_admintools_conf('vertica_ce', source) is True
    assert any('Copied admintools.conf into Vertica container' in entry for entry in logs)


def test_synchronize_container_admintools_conf_recovers_non_directory_parent(tmp_path, monkeypatch):
    source = tmp_path / 'admintools.conf'
    source.write_text('test')

    logs: list[str] = []

    monkeypatch.setattr(smoke, 'log', logs.append)
    monkeypatch.setattr(smoke.shutil, 'which', lambda cmd: '/usr/bin/docker' if cmd == 'docker' else None)
    monkeypatch.setattr(smoke, '_container_path_exists', lambda container, path: True)
    monkeypatch.setattr(smoke, '_container_dbadmin_identity', lambda container: (1000, 1000))

    mkdir_attempts: list[str] = []

    def fake_run(args, capture_output=True, text=True, **kwargs):
        if args[:3] == ['docker', 'exec', '--user']:
            assert args[3] in {'0', 'dbadmin'}
            assert args[4] == 'vertica_ce'
            command = args[5:]
            if command == ['rm', '-f', '/opt/vertica/config/admintools.conf']:
                return subprocess.CompletedProcess(args, 0, '', '')
            if command == ['mkdir', '-p', '/opt/vertica/config']:
                mkdir_attempts.append(args[3])
                if len(mkdir_attempts) < 3:
                    return subprocess.CompletedProcess(
                        args,
                        1,
                        '',
                        "mkdir: cannot create directory '/opt/vertica/config': File exists",
                    )
                return subprocess.CompletedProcess(args, 0, '', '')
            if command == ['rm', '-rf', '/opt/vertica/config']:
                return subprocess.CompletedProcess(args, 0, '', '')
            if command == ['sh', '-c', 'stat -c "%u:%g" /opt/vertica/config/admintools.conf']:
                return subprocess.CompletedProcess(args, 0, '0:0', '')
            if command == ['sh', '-c', 'chown 1000:1000 /opt/vertica/config/admintools.conf']:
                return subprocess.CompletedProcess(args, 0, '', '')
            raise AssertionError(args)
        if args[:2] == ['docker', 'cp']:
            assert args[2].endswith('admintools.conf')
            assert args[3] == 'vertica_ce:/opt/vertica/config/admintools.conf'
            return subprocess.CompletedProcess(args, 0, '', '')
        raise AssertionError(args)

    monkeypatch.setattr(smoke.subprocess, 'run', fake_run)

    assert smoke._synchronize_container_admintools_conf('vertica_ce', source) is True
    assert any('attempting to rebuild directory' in entry for entry in logs)
    assert any('Rebuilt admintools.conf directory inside container' in entry for entry in logs)


def test_synchronize_container_admintools_conf_fallback(tmp_path, monkeypatch):
    source = tmp_path / 'admintools.conf'
    source.write_text('test')

    logs: list[str] = []

    monkeypatch.setattr(smoke, 'log', logs.append)
    monkeypatch.setattr(smoke.shutil, 'which', lambda cmd: '/usr/bin/docker' if cmd == 'docker' else None)
    monkeypatch.setattr(smoke, '_container_path_exists', lambda container, path: True)
    monkeypatch.setattr(smoke, '_container_dbadmin_identity', lambda container: (1000, 1000))

    def fake_run(args, capture_output=True, text=True, **kwargs):
        if args[:3] == ['docker', 'exec', '--user']:
            command = args[5:]
            if command == ['rm', '-f', '/opt/vertica/config/admintools.conf']:
                return subprocess.CompletedProcess(args, 0, '', '')
            if command == ['mkdir', '-p', '/opt/vertica/config']:
                return subprocess.CompletedProcess(args, 0, '', '')
            if command == ['sh', '-c', 'stat -c "%u:%g" /opt/vertica/config/admintools.conf']:
                return subprocess.CompletedProcess(args, 0, '0:0', '')
            if command == ['sh', '-c', 'chown 1000:1000 /opt/vertica/config/admintools.conf']:
                return subprocess.CompletedProcess(args, 0, '', '')
            if command[:2] == ['sh', '-c']:
                script = command[2]
                assert '__VERTICA_ADMINTOOLS_CONF__' in script
                assert "/opt/vertica/config/admintools.conf" in script
                assert 'rm -rf /opt/vertica/config' in script
                return subprocess.CompletedProcess(args, 0, '', '')
        if args[:2] == ['docker', 'cp']:
            return subprocess.CompletedProcess(args, 1, '', 'cp failed')
        raise AssertionError(args)

    monkeypatch.setattr(smoke.subprocess, 'run', fake_run)

    assert smoke._synchronize_container_admintools_conf('vertica_ce', source) is True
    assert any('exec fallback' in entry for entry in logs)


def test_synchronize_container_admintools_conf_fallback_failure(tmp_path, monkeypatch):
    source = tmp_path / 'admintools.conf'
    source.write_text('test')

    logs: list[str] = []

    monkeypatch.setattr(smoke, 'log', logs.append)
    monkeypatch.setattr(smoke.shutil, 'which', lambda cmd: '/usr/bin/docker' if cmd == 'docker' else None)
    monkeypatch.setattr(smoke, '_container_path_exists', lambda container, path: True)
    monkeypatch.setattr(smoke, '_container_dbadmin_identity', lambda container: (1000, 1000))

    def fake_run(args, capture_output=True, text=True, **kwargs):
        if args[:3] == ['docker', 'exec', '--user']:
            command = args[5:]
            if command == ['rm', '-f', '/opt/vertica/config/admintools.conf']:
                return subprocess.CompletedProcess(args, 0, '', '')
            if command == ['mkdir', '-p', '/opt/vertica/config']:
                return subprocess.CompletedProcess(args, 0, '', '')
            if command == ['sh', '-c', 'stat -c "%u:%g" /opt/vertica/config/admintools.conf']:
                return subprocess.CompletedProcess(args, 0, '0:0', '')
            if command == ['sh', '-c', 'chown 1000:1000 /opt/vertica/config/admintools.conf']:
                return subprocess.CompletedProcess(args, 0, '', '')
            if command[:2] == ['sh', '-c']:
                return subprocess.CompletedProcess(args, 1, '', 'exec failed')
        if args[:2] == ['docker', 'cp']:
            return subprocess.CompletedProcess(args, 1, '', 'cp failed')
        raise AssertionError(args)

    monkeypatch.setattr(smoke.subprocess, 'run', fake_run)

    assert smoke._synchronize_container_admintools_conf('vertica_ce', source) is False
    assert any('Failed to write admintools.conf inside container using exec fallback' in entry for entry in logs)


def test_synchronize_container_admintools_conf_exec_after_successful_copy(tmp_path, monkeypatch):
    source = tmp_path / 'admintools.conf'
    source.write_text('test')

    logs: list[str] = []

    monkeypatch.setattr(smoke, 'log', logs.append)
    monkeypatch.setattr(smoke.shutil, 'which', lambda cmd: '/usr/bin/docker' if cmd == 'docker' else None)
    monkeypatch.setattr(smoke, '_container_path_exists', lambda container, path: False)

    exec_calls: list[tuple[str, str, str]] = []

    def fake_exec(container: str, target: str, content: str) -> bool:
        exec_calls.append((container, target, content))
        return True

    monkeypatch.setattr(smoke, '_write_container_admintools_conf', fake_exec)

    def fake_run(args, capture_output=True, text=True, **kwargs):
        if args[:3] == ['docker', 'exec', '--user']:
            if args[5:] == ['rm', '-f', '/opt/vertica/config/admintools.conf']:
                return subprocess.CompletedProcess(args, 0, '', '')
            if args[5:] == ['mkdir', '-p', '/opt/vertica/config']:
                return subprocess.CompletedProcess(args, 0, '', '')
        if args[:2] == ['docker', 'cp']:
            return subprocess.CompletedProcess(args, 0, '', '')
        raise AssertionError(args)

    monkeypatch.setattr(smoke.subprocess, 'run', fake_run)

    assert smoke._synchronize_container_admintools_conf('vertica_ce', source) is True
    assert any('still missing inside container after docker cp' in entry for entry in logs)
    assert exec_calls


def test_synchronize_container_admintools_conf_exec_after_inconclusive_check(tmp_path, monkeypatch):
    source = tmp_path / 'admintools.conf'
    source.write_text('test')

    logs: list[str] = []

    monkeypatch.setattr(smoke, 'log', logs.append)
    monkeypatch.setattr(smoke.shutil, 'which', lambda cmd: '/usr/bin/docker' if cmd == 'docker' else None)
    monkeypatch.setattr(smoke, '_container_path_exists', lambda container, path: None)

    exec_calls: list[tuple[str, str, str]] = []

    def fake_exec(container: str, target: str, content: str) -> bool:
        exec_calls.append((container, target, content))
        return True

    monkeypatch.setattr(smoke, '_write_container_admintools_conf', fake_exec)

    def fake_run(args, capture_output=True, text=True, **kwargs):
        if args[:3] == ['docker', 'exec', '--user']:
            if args[5:] == ['rm', '-f', '/opt/vertica/config/admintools.conf']:
                return subprocess.CompletedProcess(args, 0, '', '')
            if args[5:] == ['mkdir', '-p', '/opt/vertica/config']:
                return subprocess.CompletedProcess(args, 0, '', '')
        if args[:2] == ['docker', 'cp']:
            return subprocess.CompletedProcess(args, 0, '', '')
        raise AssertionError(args)

    monkeypatch.setattr(smoke.subprocess, 'run', fake_run)

    assert smoke._synchronize_container_admintools_conf('vertica_ce', source) is True
    assert any('Unable to verify admintools.conf inside container after docker cp' in entry for entry in logs)
    assert exec_calls


def test_synchronize_container_admintools_conf_missing_docker(tmp_path, monkeypatch):
    source = tmp_path / 'admintools.conf'
    source.write_text('test')

    monkeypatch.setattr(smoke.shutil, 'which', lambda cmd: None)

    assert smoke._synchronize_container_admintools_conf('vertica_ce', source) is False


def test_container_admintools_conf_targets_for_known_roots():
    host_path = Path('/var/lib/vertica/config/admintools.conf')

    targets = smoke._container_admintools_conf_targets(host_path)

    assert '/opt/vertica/config/admintools.conf' in targets
    assert '/var/lib/vertica/config/admintools.conf' in targets
    assert '/data/vertica/config/admintools.conf' in targets


def test_container_admintools_conf_targets_for_unknown_root(tmp_path):
    host_path = tmp_path / 'config' / 'admintools.conf'

    targets = smoke._container_admintools_conf_targets(host_path)

    assert targets == ['/opt/vertica/config/admintools.conf']


def test_ensure_known_identity_aligns_vertica_admin(tmp_path, monkeypatch):
    base = tmp_path / 'vertica'
    config_dir = base / 'config'
    config_dir.mkdir(parents=True)
    target = config_dir / 'admintools.conf'
    target.write_text('test')

    monkeypatch.setattr(smoke, 'VERTICA_DATA_DIRECTORIES', [base], raising=False)
    monkeypatch.setattr(smoke.os, 'geteuid', lambda: 0)

    calls: list[Path] = []

    def fake_align(path: Path) -> None:
        calls.append(path)

    monkeypatch.setattr(smoke, '_ensure_vertica_admin_identity', fake_align)

    smoke._ensure_known_identity(target)

    assert calls == [target]


def test_vertica_admin_identity_candidates_uses_container_identity(monkeypatch):
    def missing_user(name: str):
        raise KeyError(name)

    def fake_getpwuid(uid: int):
        raise KeyError(uid)

    monkeypatch.setattr(smoke.pwd, 'getpwnam', missing_user)
    monkeypatch.setattr(smoke.pwd, 'getpwuid', fake_getpwuid)
    monkeypatch.setattr(smoke, '_container_dbadmin_identity', lambda container: (1000, 1001))

    candidates = smoke._vertica_admin_identity_candidates()

    assert candidates[0] == (1000, 1001)


def test_vertica_admin_identity_candidates_includes_known_fallback(monkeypatch):
    def missing_user(name: str):
        raise KeyError(name)

    fallback_entry = SimpleNamespace(
        pw_uid=smoke.VERTICA_ADMIN_FALLBACK_UID,
        pw_gid=smoke.VERTICA_ADMIN_FALLBACK_GID,
    )

    def fake_getpwuid(uid: int):
        if uid == smoke.VERTICA_ADMIN_FALLBACK_UID:
            return fallback_entry
        raise KeyError(uid)

    monkeypatch.setattr(smoke.pwd, 'getpwnam', missing_user)
    monkeypatch.setattr(smoke.pwd, 'getpwuid', fake_getpwuid)
    monkeypatch.setattr(smoke, '_container_dbadmin_identity', lambda container: None)

    candidates = smoke._vertica_admin_identity_candidates()

    assert (
        smoke.VERTICA_ADMIN_FALLBACK_UID,
        smoke.VERTICA_ADMIN_FALLBACK_GID,
    ) in candidates


def test_vertica_admin_identity_candidates_includes_numeric_fallback(monkeypatch):
    def missing_user(name: str):
        raise KeyError(name)

    def missing_uid(uid: int):
        raise KeyError(uid)

    monkeypatch.setattr(smoke.pwd, 'getpwnam', missing_user)
    monkeypatch.setattr(smoke.pwd, 'getpwuid', missing_uid)
    monkeypatch.setattr(smoke, '_container_dbadmin_identity', lambda container: None)

    candidates = smoke._vertica_admin_identity_candidates()

    assert (
        smoke.VERTICA_ADMIN_FALLBACK_UID,
        smoke.VERTICA_ADMIN_FALLBACK_GID,
    ) in candidates


def test_discover_existing_vertica_admin_identities_prefers_non_root(tmp_path, monkeypatch):
    base = tmp_path / 'vertica'
    config_dir = base / 'config'
    config_dir.mkdir(parents=True)

    candidate = config_dir / 'agent.conf'
    candidate.write_text('test')

    os.chown(candidate, 4242, 4343)

    monkeypatch.setattr(smoke, 'VERTICA_DATA_DIRECTORIES', [base], raising=False)
    monkeypatch.setattr(smoke.os, 'geteuid', lambda: 0)
    monkeypatch.setattr(smoke.os, 'getegid', lambda: 0)

    identities = smoke._discover_existing_vertica_admin_identities()

    assert identities and identities[0] == (4242, 4343)


def test_ensure_vertica_admin_identity_uses_discovered_candidates(tmp_path, monkeypatch):
    base = tmp_path / 'vertica'
    config_dir = base / 'config'
    config_dir.mkdir(parents=True)

    target = config_dir / 'admintools.conf'
    target.write_text('test')

    monkeypatch.setattr(smoke, 'VERTICA_DATA_DIRECTORIES', [base], raising=False)
    monkeypatch.setattr(smoke.os, 'geteuid', lambda: 0)
    monkeypatch.setattr(smoke, '_vertica_admin_identity_candidates', lambda: [])
    monkeypatch.setattr(
        smoke,
        '_discover_existing_vertica_admin_identities',
        lambda **kwargs: [(4242, 4343)],
    )

    chown_calls: list[tuple[Path, int, int]] = []

    def fake_chown(path, uid, gid):
        chown_calls.append((Path(path), uid, gid))

    monkeypatch.setattr(smoke.os, 'chown', fake_chown)

    smoke._ensure_vertica_admin_identity(target)

    assert chown_calls == [(target, 4242, 4343)]


def test_ensure_vertica_admin_identity_prefers_candidate_order(tmp_path, monkeypatch):
    base = tmp_path / 'vertica'
    config_dir = base / 'config'
    config_dir.mkdir(parents=True)

    target = config_dir / 'admintools.conf'
    target.write_text('test')

    monkeypatch.setattr(smoke, 'VERTICA_DATA_DIRECTORIES', [base], raising=False)
    monkeypatch.setattr(smoke.os, 'geteuid', lambda: 0)
    monkeypatch.setattr(
        smoke,
        '_discover_existing_vertica_admin_identities',
        lambda **kwargs: [],
    )
    monkeypatch.setattr(
        smoke,
        '_vertica_admin_identity_candidates',
        lambda: [(1111, 1111), (2222, 2222)],
    )

    original_stat = smoke.Path.stat

    def fake_stat(self):
        if self == target:
            return SimpleNamespace(st_uid=0, st_gid=0)
        return original_stat(self)

    monkeypatch.setattr(smoke.Path, 'stat', fake_stat)

    chown_calls: list[tuple[Path, int, int]] = []

    def fake_chown(path, uid, gid):
        chown_calls.append((Path(path), uid, gid))

    monkeypatch.setattr(smoke.os, 'chown', fake_chown)

    smoke._ensure_vertica_admin_identity(target)

    assert chown_calls == [(target, 1111, 1111)]


def test_ensure_container_admintools_conf_readable_adjusts(monkeypatch):
    logs: list[str] = []
    calls: list[list[str]] = []
    test_r_invocations = 0

    def fake_log(message: str) -> None:
        logs.append(message)

    def fake_which(name: str) -> Optional[str]:
        return '/usr/bin/docker' if name == 'docker' else None

    def fake_run(command, capture_output=True, text=True):
        calls.append(command)
        script = command[-1]
        nonlocal test_r_invocations
        if 'test -e' in script:
            return subprocess.CompletedProcess(command, 0, stdout='', stderr='')
        if 'printf' in script and 'id -u dbadmin' in script:
            return subprocess.CompletedProcess(command, 0, stdout='1001:1001', stderr='')
        if 'stat -c' in script:
            return subprocess.CompletedProcess(command, 0, stdout='0:0', stderr='')
        if 'chown' in script:
            return subprocess.CompletedProcess(command, 0, stdout='', stderr='')
        if 'test -r' in script:
            test_r_invocations += 1
            return subprocess.CompletedProcess(command, 1 if test_r_invocations < 3 else 0, stdout='', stderr='')
        if 'chmod' in script:
            return subprocess.CompletedProcess(command, 0, stdout='', stderr='')
        raise AssertionError(f'Unexpected command: {command}')

    monkeypatch.setattr(smoke, 'log', fake_log)
    monkeypatch.setattr(smoke.shutil, 'which', fake_which)
    monkeypatch.setattr(smoke.subprocess, 'run', fake_run)

    adjusted = smoke._ensure_container_admintools_conf_readable('vertica_ce')

    assert adjusted is True
    assert any('Detected unreadable admintools.conf' in entry for entry in logs)
    assert any('chown' in cmd[-1] for cmd in calls)
    assert any('Aligned admintools.conf ownership inside container' in entry for entry in logs)
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
        if 'printf' in script and 'id -u dbadmin' in script:
            return subprocess.CompletedProcess(command, 0, stdout='1001:1001', stderr='')
        if 'stat -c' in script:
            return subprocess.CompletedProcess(command, 0, stdout='1001:1001', stderr='')
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


def test_reset_vertica_data_directories_removes_config_directories(tmp_path, monkeypatch):
    base = tmp_path / 'vertica_data'
    config_dir = base / 'config'
    config_dir.mkdir(parents=True)
    (config_dir / 'admintools.conf').write_text('test')

    monkeypatch.setattr(smoke, 'VERTICA_DATA_DIRECTORIES', [base], raising=False)

    removed = smoke._reset_vertica_data_directories()

    assert removed is True
    assert not config_dir.exists()


def test_reset_vertica_data_directories_removes_config_symlinks(tmp_path, monkeypatch):
    base = tmp_path / 'vertica_data'
    target = tmp_path / 'shared_config'
    target.mkdir(parents=True)
    (target / 'admintools.conf').write_text('test')
    base.mkdir(parents=True)
    (base / 'config').symlink_to(target)

    monkeypatch.setattr(smoke, 'VERTICA_DATA_DIRECTORIES', [base], raising=False)

    removed = smoke._reset_vertica_data_directories()

    assert removed is True
    assert not (base / 'config').exists()


def test_connect_and_query_disables_tls_by_default(monkeypatch):
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

    assert captured_config['tlsmode'] == 'disable'
    assert (
        captured_config['connection_timeout']
        == smoke.VERTICA_CLIENT_CONNECT_TIMEOUT_SECONDS
    )


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
    assert (
        captured_config['connection_timeout']
        == smoke.VERTICA_CLIENT_CONNECT_TIMEOUT_SECONDS
    )


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
    monkeypatch.setattr(
        smoke,
        'time',
        SimpleNamespace(sleep=lambda _: None, time=lambda: 0.0),
    )
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


def test_connect_and_query_deadline_limits_attempts(monkeypatch):
    attempts: list[None] = []
    messages: list[str] = []
    current = {'value': 0.0}

    class FakeErrorsModule:
        class ConnectionError(Exception):
            pass

    def fake_time() -> float:
        return current['value']

    def fake_sleep(seconds: float) -> None:
        current['value'] += seconds

    def fake_log(message: str) -> None:
        messages.append(message)

    def fake_connect(**config):
        attempts.append(None)
        raise FakeErrorsModule.ConnectionError('boom')

    monkeypatch.setattr(smoke, 'log', fake_log)
    monkeypatch.setattr(
        smoke,
        'vertica_python',
        type('Module', (), {'connect': fake_connect, 'errors': FakeErrorsModule}),
    )
    monkeypatch.setattr(
        smoke,
        'time',
        SimpleNamespace(time=fake_time, sleep=fake_sleep),
    )

    deadline = fake_time() + 25.0
    result = smoke.connect_and_query(
        'label',
        'host',
        'user',
        'password',
        attempts=10,
        delay=10.0,
        fatal=False,
        deadline=deadline,
    )

    assert result is False
    assert len(attempts) == 3
    assert current['value'] <= 25.0
    assert any('Failed to connect to Vertica' in message for message in messages)


def test_connect_and_query_deadline_raises_when_fatal(monkeypatch):
    class FakeErrorsModule:
        class ConnectionError(Exception):
            pass

    def fake_connect(**config):
        raise FakeErrorsModule.ConnectionError('boom')

    monkeypatch.setattr(
        smoke,
        'vertica_python',
        type('Module', (), {'connect': fake_connect, 'errors': FakeErrorsModule}),
    )
    monkeypatch.setattr(
        smoke,
        'time',
        SimpleNamespace(time=lambda: 100.0, sleep=lambda _seconds: None),
    )

    with pytest.raises(SystemExit, match='No time remaining'):
        smoke.connect_and_query('label', 'host', 'user', 'password', deadline=100.0)


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
    removal_calls: list[bool] = []

    def fake_run_command(command: list[str]):
        run_calls.append(command)
        if len(run_calls) == 1:
            raise smoke.CommandError(command, 1, '', '')

    def fake_remove(*, force: bool = False) -> bool:
        removal_calls.append(force)
        return True

    monkeypatch.setattr(smoke, '_docker_compose_plugin_available', lambda: True)
    monkeypatch.setattr(smoke.shutil, 'which', lambda name: None)
    monkeypatch.setattr(smoke, 'run_command', fake_run_command)
    monkeypatch.setattr(smoke, '_remove_stale_vertica_container', fake_remove)
    monkeypatch.setattr(smoke, 'log', lambda message: None)

    smoke._compose_up(compose_path)

    assert len(run_calls) == 2
    assert removal_calls == [False]


def test_compose_up_raises_when_stale_container_removal_fails(monkeypatch):
    compose_path = Path('/opt/compose.remote.yml')
    run_calls: list[list[str]] = []
    removal_calls: list[bool] = []

    def fake_run_command(command: list[str]):
        run_calls.append(command)
        raise smoke.CommandError(command, 1, '', '')

    def fake_remove(*, force: bool = False) -> bool:
        removal_calls.append(force)
        return False

    monkeypatch.setattr(smoke, '_docker_compose_plugin_available', lambda: True)
    monkeypatch.setattr(smoke.shutil, 'which', lambda name: None)
    monkeypatch.setattr(smoke, 'run_command', fake_run_command)
    monkeypatch.setattr(smoke, '_remove_stale_vertica_container', fake_remove)
    monkeypatch.setattr(smoke, 'log', lambda message: None)

    with pytest.raises(SystemExit):
        smoke._compose_up(compose_path)

    assert len(run_calls) == 1
    assert removal_calls == [False]


def test_compose_up_forces_container_removal_on_conflict(monkeypatch):
    compose_path = Path('/opt/compose.remote.yml')
    run_calls: list[list[str]] = []
    removal_calls: list[bool] = []

    def fake_run_command(command: list[str]):
        run_calls.append(command)
        if len(run_calls) == 1:
            raise smoke.CommandError(
                command,
                1,
                '',
                'Error response from daemon: Conflict. '
                'The container name "/vertica_ce" is already in use by container "abc123".',
            )
        return subprocess.CompletedProcess(command, 0, '', '')

    def fake_remove(*, force: bool = False) -> bool:
        removal_calls.append(force)
        return force

    monkeypatch.setattr(smoke, '_docker_compose_plugin_available', lambda: True)
    monkeypatch.setattr(smoke.shutil, 'which', lambda name: None)
    monkeypatch.setattr(smoke, 'run_command', fake_run_command)
    monkeypatch.setattr(smoke, '_remove_stale_vertica_container', fake_remove)
    monkeypatch.setattr(smoke, 'log', lambda message: None)

    smoke._compose_up(compose_path)

    assert len(run_calls) == 2
    assert removal_calls == [False, True]


def test_ensure_primary_admin_user_creates_user(monkeypatch):
    executed: list[tuple[str, tuple | list | None]] = []
    configs: list[dict[str, object]] = []

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

    def fake_connect(**config):
        configs.append(config)
        return FakeConnection()

    monkeypatch.setattr(smoke.vertica_python, 'connect', fake_connect)

    smoke._ensure_primary_admin_user('dbadmin', '', 'appadmin', 'secret')

    statements = [statement for statement, _ in executed]
    assert any(statement.startswith('CREATE USER "appadmin"') for statement in statements)
    assert any('GRANT ALL PRIVILEGES ON DATABASE "VMart"' in statement for statement in statements)
    assert configs[0]['connection_timeout'] == smoke.VERTICA_CLIENT_CONNECT_TIMEOUT_SECONDS


def test_ensure_primary_admin_user_rotates_password(monkeypatch):
    executed: list[tuple[str, tuple | list | None]] = []
    configs: list[dict[str, object]] = []

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

    def fake_connect(**config):
        configs.append(config)
        return FakeConnection()

    monkeypatch.setattr(smoke.vertica_python, 'connect', fake_connect)

    smoke._ensure_primary_admin_user('dbadmin', '', 'appadmin', 'secret')

    statements = [statement for statement, _ in executed]
    assert any(statement.startswith('ALTER USER "appadmin"') for statement in statements)
    assert all(not statement.startswith('CREATE USER') for statement in statements[1:])
    assert configs[0]['connection_timeout'] == smoke.VERTICA_CLIENT_CONNECT_TIMEOUT_SECONDS


def test_ensure_primary_admin_user_skips_when_matching_bootstrap(monkeypatch):
    called = False

    def fake_connect(**config):
        nonlocal called
        called = True
        raise AssertionError('connect should not be called')

    monkeypatch.setattr(smoke.vertica_python, 'connect', fake_connect)

    smoke._ensure_primary_admin_user('appadmin', '', 'appadmin', 'secret')

    assert called is False
