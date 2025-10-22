import configparser
import grp
import json
import os
import platform
import pwd
import re
import shlex
import shutil
import socket
import subprocess
import sys
import textwrap
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
BOOTSTRAP_ADMIN_DEFAULT_USER = 'dbadmin'
VERTICA_ADMIN_FALLBACK_UID = 500
VERTICA_ADMIN_FALLBACK_GID = 500
# Vertica community edition container images historically used the legacy
# ``dbadmin`` identity with uid/gid 500.  Newer releases have switched to a more
# conventional uid/gid of 1000 for the administrator account.  Include both
# combinations when attempting to align ownership on the host so that the smoke
# test can preemptively fix permissions even before the container starts (and
# exposes its runtime identity for discovery via ``docker exec``).
VERTICA_ADMIN_COMMON_IDENTITIES = (
    (VERTICA_ADMIN_FALLBACK_UID, VERTICA_ADMIN_FALLBACK_GID),
    (1000, 1000),
)
ADMIN_USER = os.environ['ADMIN_USER']
ADMIN_PASSWORD = os.environ['ADMIN_PASSWORD']

if not ADMIN_USER:
    raise SystemExit('Missing ADMIN_USER value')
if ADMIN_PASSWORD is None:
    raise SystemExit('Missing ADMIN_PASSWORD value')

STEP_SEPARATOR = '=' * 72
# The Vertica container can take several minutes to transition from "starting"
# to a healthy state while it performs initial database setup work. Allow a
# generous grace period before attempting restarts so we do not thrash the
# container during long but successful bootstraps.
UNHEALTHY_HEALTHCHECK_GRACE_PERIOD_SECONDS = 900.0


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


def _quote_identifier(identifier: str) -> str:
    """Return ``identifier`` quoted for use in Vertica SQL statements."""

    return '"' + identifier.replace('"', '""') + '"'


_METADATA_TOKEN: Optional[str] = None

# Vertica container images historically mounted their persistent data directory
# at ``/var/lib/vertica`` on the host.  Newer infrastructure variants (including
# the publicly distributed containers) instead mount ``/data/vertica``.  Handle
# both locations so the smoke test can reset or seed configuration regardless of
# which layout the instance uses.
VERTICA_DATA_DIRECTORIES = [Path('/var/lib/vertica'), Path('/data/vertica')]
# Vertica container images have historically run as uid/gid 500, but newer builds
# may choose a different runtime identity. Use permissive modes rather than
# forcing ownership so that any future uid/gid changes continue to work.
_VERTICA_DATA_DIR_MODE = 0o777
_VERTICA_CONTAINER_ADMINTOOLS_PATH = '/opt/vertica/config/admintools.conf'


def _is_within_vertica_data_directories(candidate: Path) -> bool:
    """Return ``True`` when ``candidate`` is inside a known Vertica data root."""

    try:
        resolved_candidate = candidate.resolve()
    except OSError:
        resolved_candidate = candidate

    for base in VERTICA_DATA_DIRECTORIES:
        try:
            resolved_base = base.resolve()
        except OSError:
            resolved_base = base

        if resolved_candidate == resolved_base:
            return True

        try:
            if resolved_candidate.is_relative_to(resolved_base):
                return True
        except AttributeError:
            try:
                resolved_candidate.relative_to(resolved_base)
            except ValueError:
                continue
            else:
                return True

    return False


def _vertica_admin_identity_candidates() -> list[tuple[int, int]]:
    """Return potential ``(uid, gid)`` pairs for the Vertica admin user."""

    names: list[str] = []
    for env_var in ('VERTICA_DB_USER', 'DBADMIN_USER'):
        value = os.getenv(env_var)
        if value:
            stripped = value.strip()
            if stripped:
                names.append(stripped)

    names.append(BOOTSTRAP_ADMIN_DEFAULT_USER)

    candidates: list[tuple[int, int]] = []
    seen: set[tuple[int, int]] = set()

    for name in names:
        try:
            entry = pwd.getpwnam(name)
        except KeyError:
            continue
        except OSError as exc:
            log(f'Unable to resolve passwd entry for {name!r}: {exc}')
            continue

        pair = (entry.pw_uid, entry.pw_gid)
        if pair not in seen:
            candidates.append(pair)
            seen.add(pair)

    container_identity = _container_dbadmin_identity('vertica_ce')
    if container_identity is not None and container_identity not in seen:
        candidates.append(container_identity)
        seen.add(container_identity)

    # Include known-good uid/gid pairs even when the host does not provide
    # corresponding passwd/group entries.  ``os.chown`` accepts raw numeric
    # identifiers which allows us to align ownership with the container's
    # expected administrator identity ahead of time.
    for uid, gid in VERTICA_ADMIN_COMMON_IDENTITIES:
        try:
            entry = pwd.getpwuid(uid)
        except KeyError:
            entry = None
        except OSError as exc:
            log(
                'Unable to resolve fallback Vertica admin identity '
                f'uid {uid}: {exc}'
            )
            entry = None

        if entry is not None:
            pair = (entry.pw_uid, entry.pw_gid)
        else:
            pair = (uid, gid)

        if pair not in seen:
            candidates.append(pair)
            seen.add(pair)

    return candidates


def _ensure_vertica_admin_identity(path: Path) -> None:
    """Attempt to align ``path`` ownership with the Vertica admin identity."""

    if os.geteuid() != 0:
        return

    if not _is_within_vertica_data_directories(path):
        return

    try:
        stat_info = path.stat()
    except OSError as exc:
        log(f'Unable to inspect ownership of {path} while aligning with Vertica admin: {exc}')
        return

    candidates = _vertica_admin_identity_candidates()
    if not candidates:
        return

    for uid, gid in candidates:
        if stat_info.st_uid == uid and stat_info.st_gid == gid:
            return

    for uid, gid in candidates:
        try:
            os.chown(path, uid, gid)
        except OSError as exc:
            log(
                'Unable to adjust ownership on '
                f'{path} to uid {uid} gid {gid} for Vertica admin compatibility: {exc}'
            )
            continue
        else:
            log(
                'Adjusted ownership on '
                f'{path} to uid {uid} gid {gid} for Vertica admin compatibility'
            )
            return

DEFAULT_ADMINTOOLS_CONF = textwrap.dedent(
    """
    [Configuration]
        format = 3
        install_opts =
        default_base = /home/dbadmin
        controlmode = pt2pt
        controlsubnet = default
        spreadlog = False
        last_port = 5433
        tmp_dir = /tmp
        atdebug = False
        atgui_default_license = False
        unreachable_host_caching = True
        aws_metadata_conn_timeout = 2
        rebalance_shards_timeout = 36000
        database_state_change_poll_timeout = 21600
        wait_for_shutdown_timeout = 3600
        pexpect_verbose_logging = False
        sync_catalog_retries = 2000
        client_connect_timeout_sec = 5.0
        admintools_config_version = 110
        thread_timeout = 1200

    [Cluster]
        hosts = 127.0.0.1

    [Nodes]
        node0001 = 127.0.0.1

    [SSHConfig]
        ssh_user =
        ssh_ident =
        ssh_options = -oConnectTimeout=30 -o TCPKeepAlive=no -o ServerAliveInterval=15 -o ServerAliveCountMax=2 -o StrictHostKeyChecking=no -o BatchMode=yes

    [BootstrapParameters]
        awsendpoint = null
        awsregion = null
    """
).strip() + "\n"


def _candidate_vertica_roots(base_path: Path) -> list[Path]:
    """Return potential Vertica data roots located under ``base_path``."""

    candidates: list[Path] = []

    known_names = {
        'vertica',
    }

    for value in (DB_NAME, os.getenv('VERTICA_DB_NAME'), os.getenv('DB_NAME')):
        if not value:
            continue
        stripped = value.strip()
        if not stripped:
            continue
        known_names.add(stripped)

    for name in known_names:
        candidates.append(base_path / name)

    config_dir = base_path / 'config'
    if config_dir.exists():
        candidates.append(base_path)

    try:
        for entry in base_path.iterdir():
            if not entry.is_dir():
                continue
            if entry.name in known_names:
                candidates.append(entry)
                continue
            if (entry / 'config').exists():
                candidates.append(entry)
    except OSError as exc:
        log(f'Unable to inspect contents of {base_path}: {exc}')

    unique_candidates: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = os.fspath(candidate)
        if key in seen:
            continue
        seen.add(key)
        unique_candidates.append(candidate)

    return unique_candidates


def _ensure_directory(path: Path) -> bool:
    try:
        if path.exists() and path.is_symlink():
            log(f'Removing unexpected symlink at {path}')
            path.unlink()
    except OSError as exc:
        log(f'Unable to inspect {path}: {exc}')
        return False

    try:
        path.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        log(f'Unable to create directory {path}: {exc}')
        return False

    try:
        os.chmod(path, _VERTICA_DATA_DIR_MODE)
    except OSError as exc:
        log(f'Unable to adjust permissions on {path}: {exc}')

    _ensure_known_identity(path)

    return True


def _sanitize_vertica_data_directories() -> None:
    log(STEP_SEPARATOR)
    log('Ensuring Vertica data directories are accessible to the container')

    for base_path in VERTICA_DATA_DIRECTORIES:
        if not _ensure_directory(base_path):
            continue

        candidate_roots = _candidate_vertica_roots(base_path) or [base_path / 'vertica']

        for vertica_root in candidate_roots:
            if vertica_root.exists() and vertica_root.is_symlink():
                try:
                    target = os.readlink(vertica_root)
                except OSError as exc:
                    log(f'Unable to inspect symlink {vertica_root}: {exc}')
                else:
                    if target.startswith('/data') or target.startswith('data'):
                        log(
                            f'Removing recursive symlink {vertica_root} -> {target} '
                            'to avoid Vertica bootstrap loops'
                        )
                        try:
                            vertica_root.unlink()
                        except OSError as exc:
                            log(f'Unable to remove {vertica_root}: {exc}')

            if not _ensure_directory(vertica_root):
                continue

            config_path = vertica_root / 'config'
            if config_path.exists():
                if config_path.is_symlink():
                    try:
                        target = os.readlink(config_path)
                    except OSError as exc:
                        log(f'Unable to inspect symlink {config_path}: {exc}')
                    else:
                        remove_symlink = False
                        if target.startswith('/data') or target.startswith('data'):
                            remove_symlink = True
                        else:
                            try:
                                if os.path.isabs(target):
                                    resolved_target = Path(target).resolve()
                                else:
                                    resolved_target = (config_path.parent / target).resolve()
                            except FileNotFoundError:
                                resolved_target = None

                            if resolved_target and resolved_target == Path('/opt/vertica/config'):
                                remove_symlink = True

                        if remove_symlink:
                            log(
                                f'Removing confusing symlink {config_path} -> {target} '
                                'to allow Vertica to recreate configuration files'
                            )
                            try:
                                config_path.unlink()
                            except OSError as exc:
                                log(f'Unable to remove {config_path}: {exc}')
                            else:
                                continue

                if config_path.exists() and config_path.is_dir():
                    _ensure_known_identity_tree(config_path, max_depth=2)
                    admintools_conf = config_path / 'admintools.conf'
                    if not admintools_conf.exists():
                        container_status = _docker_inspect(
                            'vertica_ce', '{{.State.Status}}'
                        )
                        container_health = _docker_inspect(
                            'vertica_ce', '{{if .State.Health}}{{.State.Health.Status}}{{end}}'
                        )
                        should_preserve = (
                            container_status in {'running', 'restarting'}
                            and container_health == 'healthy'
                        )

                        if should_preserve:
                            log(
                                'Detected missing admintools.conf but Vertica '
                                f'container is currently {container_status} '
                                '(healthy); skipping directory removal to avoid '
                                'disrupting the running container'
                            )
                        else:
                            removal_attempted = False
                            health_display = container_health or '<unknown>'
                            status_display = container_status or '<absent>'
                            if container_status in {'running', 'restarting'}:
                                log(
                                    'Detected missing admintools.conf while '
                                    f'container status is {status_display} with '
                                    f'health {health_display}; stopping vertica_ce '
                                    'container to allow configuration rebuild'
                                )
                                removal_attempted = True
                                try:
                                    removal = subprocess.run(
                                        ['docker', 'rm', '-f', 'vertica_ce'],
                                        capture_output=True,
                                        text=True,
                                    )
                                except FileNotFoundError:
                                    log(
                                        'Docker CLI unavailable while attempting to '
                                        'remove vertica_ce container; continuing '
                                        'with directory cleanup'
                                    )
                                else:
                                    if removal.stdout:
                                        log(removal.stdout.rstrip())
                                    if removal.stderr:
                                        log(f'[stderr] {removal.stderr.rstrip()}')
                                    if removal.returncode != 0:
                                        log(
                                            'Failed to remove vertica_ce container '
                                            f'prior to configuration cleanup: exit '
                                            f'code {removal.returncode}'
                                        )
                            else:
                                log(
                                    'Detected missing admintools.conf while '
                                    f'container status is {status_display} with '
                                    f'health {health_display}; removing incomplete '
                                    'data directory to allow Vertica to rebuild it '
                                    'during startup'
                                )
                            if removal_attempted:
                                log(
                                    'Removing incomplete Vertica data directory at '
                                    f'{vertica_root} (admintools.conf missing) '
                                    'after stopping container'
                                )
                            else:
                                log(
                                    'Removing incomplete Vertica data directory at '
                                    f'{vertica_root} (admintools.conf missing) to '
                                    'allow Vertica to rebuild it during startup'
                                )
                            try:
                                shutil.rmtree(vertica_root)
                            except FileNotFoundError:
                                pass
                            except OSError as exc:
                                log(f'Unable to remove {vertica_root}: {exc}')
                            else:
                                _ensure_directory(vertica_root)
                                _seed_default_admintools_conf(vertica_root / 'config')
                            continue

            # When the Vertica container starts for the first time it populates the
            # ``config`` directory with critical bootstrap files such as
            # ``admintools.conf``.  Creating the directory ahead of time confuses the
            # container's bootstrap logic (the source and destination of the
            # configuration copy become identical) which in turn leaves the
            # configuration incomplete.  Only adjust permissions when the directory
            # already exists and otherwise allow the container to create it during
            # startup.
            if config_path.exists():
                _ensure_directory(config_path)
                _ensure_known_identity_tree(config_path, max_depth=2)
                _seed_default_admintools_conf(config_path)


def _seed_default_admintools_conf(config_dir: Path) -> None:
    """Ensure ``admintools.conf`` exists with safe defaults."""

    admintools_conf = config_dir / 'admintools.conf'
    if admintools_conf.exists():
        if _admintools_conf_needs_rebuild(admintools_conf):
            log(
                'Existing admintools.conf is missing critical configuration; '
                'attempting to rebuild it with safe defaults'
            )
        else:
            return

    if config_dir.exists() and config_dir.is_symlink():
        try:
            config_dir.unlink()
        except OSError as exc:
            log(f'Unable to remove symlinked config directory {config_dir}: {exc}')
            return

    if not _ensure_directory(config_dir):
        return

    try:
        admintools_conf.write_text(DEFAULT_ADMINTOOLS_CONF)
    except OSError as exc:
        log(f'Unable to write default admintools.conf at {admintools_conf}: {exc}')
        return

    _align_identity_with_parent(admintools_conf)

    try:
        os.chmod(admintools_conf, 0o666)
    except OSError as exc:
        log(
            'Unable to relax permissions on '
            f'{admintools_conf}: {exc}'
        )

    _ensure_known_identity(admintools_conf)


def _align_identity_with_parent(path: Path) -> None:
    """Attempt to match ``path`` ownership to its parent directory."""

    if os.geteuid() != 0:
        return

    parent = path.parent

    try:
        parent_stat = parent.stat()
    except OSError as exc:
        log(f'Unable to determine ownership of {parent} when adjusting {path}: {exc}')
        return

    try:
        current_stat = path.stat()
    except OSError as exc:
        log(f'Unable to inspect ownership of {path}: {exc}')
        return

    if (
        current_stat.st_uid == parent_stat.st_uid
        and current_stat.st_gid == parent_stat.st_gid
    ):
        return

    try:
        os.chown(path, parent_stat.st_uid, parent_stat.st_gid)
    except OSError as exc:
        log(
            'Unable to align ownership on '
            f'{path} with parent {parent}: {exc}'
        )
    else:
        log(
            'Aligned ownership on '
            f'{path} to uid {parent_stat.st_uid} gid {parent_stat.st_gid} '
            f'to match parent {parent}'
        )


def _ensure_known_identity(path: Path) -> None:
    """Ensure ``path`` is owned by a user and group present in /etc/passwd and /etc/group."""

    if os.geteuid() != 0:
        return

    try:
        stat_info = path.stat()
    except OSError as exc:
        log(f'Unable to inspect ownership of {path}: {exc}')
        return

    uid = stat_info.st_uid
    gid = stat_info.st_gid

    needs_adjustment = False
    preserve_unknown_identity = False

    try:
        pwd.getpwuid(uid)
    except KeyError:
        needs_adjustment = True
        if _is_within_vertica_data_directories(path):
            preserve_unknown_identity = True
        else:
            uid = 0
    except OSError as exc:
        log(f'Unable to resolve user for {path}: {exc}')

    try:
        grp.getgrgid(gid)
    except KeyError:
        needs_adjustment = True
        if _is_within_vertica_data_directories(path):
            preserve_unknown_identity = True
        else:
            gid = 0
    except OSError as exc:
        log(f'Unable to resolve group for {path}: {exc}')

    if preserve_unknown_identity:
        log(
            'Skipping ownership adjustment on '
            f'{path} because it is managed by the Vertica container '
            'and uses a UID/GID not present on the host'
        )
        return

    if needs_adjustment:
        try:
            os.chown(path, uid, gid)
        except OSError as exc:
            log(f'Unable to adjust ownership on {path}: {exc}')
        else:
            log(
                'Adjusted ownership on '
                f'{path} to uid {uid} gid {gid} to ensure Vertica tooling can resolve it'
            )

    if _is_within_vertica_data_directories(path):
        _ensure_vertica_admin_identity(path)


def _ensure_known_identity_tree(path: Path, *, max_depth: int = 2) -> None:
    """Ensure ``path`` and its descendants up to ``max_depth`` have known identities."""

    if os.geteuid() != 0:
        return

    stack: list[tuple[Path, int]] = [(path, 0)]

    while stack:
        current, depth = stack.pop()
        _ensure_known_identity(current)

        if depth >= max_depth:
            continue

        try:
            with os.scandir(current) as entries:
                for entry in entries:
                    child_path = Path(entry.path)
                    _ensure_known_identity(child_path)

                    try:
                        is_dir = entry.is_dir(follow_symlinks=False)
                    except OSError as exc:
                        log(f'Unable to inspect {child_path}: {exc}')
                        continue

                    if is_dir:
                        stack.append((child_path, depth + 1))
        except NotADirectoryError:
            continue
        except OSError as exc:
            log(f'Unable to inspect contents of {current}: {exc}')


def _admintools_conf_needs_rebuild(admintools_conf: Path) -> bool:
    """Return ``True`` when ``admintools_conf`` lacks critical configuration."""

    parser = configparser.ConfigParser()
    try:
        with admintools_conf.open('r') as stream:
            parser.read_file(stream)
    except (OSError, configparser.Error) as exc:
        log(f'Unable to parse existing admintools.conf ({admintools_conf}): {exc}')
        return True

    # ``admintools`` requires that the ``Cluster`` section provides a ``hosts``
    # value describing the node topology.  When this setting is missing the
    # Vertica container exits repeatedly during bootstrap which ultimately
    # leaves the Docker health check in an unhealthy state.  Treat the file as
    # corrupt so we can replace it with a known-good default.
    if not parser.has_section('Cluster') or not parser.has_option('Cluster', 'hosts'):
        return True

    # Ensure the Nodes section at least contains an entry for the first node so
    # the management tools can discover the primary address.
    if not parser.has_section('Nodes'):
        return True

    if not parser.has_option('Nodes', 'node0001'):
        return True

    return False


def _container_dbadmin_identity(container: str) -> Optional[tuple[int, int]]:
    """Return the ``(uid, gid)`` for ``dbadmin`` inside ``container`` when available."""

    if shutil.which('docker') is None:
        return None

    command = [
        'docker',
        'exec',
        '--user',
        '0',
        container,
        'sh',
        '-c',
        "uid=$(id -u dbadmin 2>/dev/null) && gid=$(id -g dbadmin 2>/dev/null) && printf '%s:%s' \"$uid\" \"$gid\"",
    ]

    try:
        result = subprocess.run(command, capture_output=True, text=True)
    except FileNotFoundError:
        log('Docker CLI is not available while resolving dbadmin identity inside container')
        return None

    if result.returncode != 0:
        return None

    output = result.stdout.strip()
    if not output or ':' not in output:
        return None

    uid_str, gid_str = output.split(':', 1)
    try:
        return int(uid_str), int(gid_str)
    except ValueError:
        log(f'Unexpected dbadmin identity output inside container: {output!r}')
        return None


def _ensure_container_admintools_conf_readable(container: str) -> bool:
    """Relax permissions on ``admintools.conf`` inside ``container`` if required.

    Returns ``True`` when a permission adjustment was attempted, otherwise ``False``.
    """

    if shutil.which('docker') is None:
        return False

    quoted_path = shlex.quote(_VERTICA_CONTAINER_ADMINTOOLS_PATH)

    def _docker_exec(user: str, command: str, missing_cli_message: str) -> Optional[subprocess.CompletedProcess[str]]:
        try:
            return subprocess.run(
                [
                    'docker',
                    'exec',
                    '--user',
                    user,
                    container,
                    'sh',
                    '-c',
                    command,
                ],
                capture_output=True,
                text=True,
            )
        except FileNotFoundError:
            log(missing_cli_message)
            return None

    exists_result = _docker_exec(
        '0',
        f'test -e {quoted_path}',
        'Docker CLI is not available while inspecting admintools.conf inside container',
    )
    if exists_result is None or exists_result.returncode != 0:
        return False

    adjustments_made = False

    target_identity = _container_dbadmin_identity(container)
    require_named_chown = target_identity is None

    if target_identity is not None:
        owner_result = _docker_exec(
            '0',
            f'stat -c "%u:%g" {quoted_path}',
            'Docker CLI is not available while inspecting admintools.conf ownership inside container',
        )
        if owner_result is None:
            return False
        if owner_result.returncode == 0:
            owner_output = owner_result.stdout.strip()
            if owner_output:
                try:
                    current_uid_str, current_gid_str = owner_output.split(':', 1)
                    current_identity = (int(current_uid_str), int(current_gid_str))
                except ValueError:
                    current_identity = None
                    log(f'Unexpected ownership output for admintools.conf inside container: {owner_output!r}')
                else:
                    if current_identity != target_identity:
                        uid, gid = target_identity
                        chown_result = _docker_exec(
                            '0',
                            f'chown {uid}:{gid} {quoted_path}',
                            'Docker CLI is not available while adjusting admintools.conf ownership inside container',
                        )
                        if chown_result is None:
                            return False
                        if chown_result.stdout:
                            log(chown_result.stdout.rstrip())
                        if chown_result.stderr:
                            log(f'[stderr] {chown_result.stderr.rstrip()}')
                        if chown_result.returncode == 0:
                            adjustments_made = True
                            log(
                                'Aligned admintools.conf ownership inside container '
                                f'with dbadmin (uid {uid} gid {gid})'
                            )
                        else:
                            log('Failed to adjust admintools.conf ownership inside container')
                            require_named_chown = True
            else:
                require_named_chown = True
        else:
            require_named_chown = True

    if require_named_chown:
        chown_result = _docker_exec(
            '0',
            f'chown dbadmin:dbadmin {quoted_path}',
            'Docker CLI is not available while aligning admintools.conf ownership inside container',
        )
        if chown_result is None:
            return False
        if chown_result.stdout:
            log(chown_result.stdout.rstrip())
        if chown_result.stderr:
            log(f'[stderr] {chown_result.stderr.rstrip()}')
        if chown_result.returncode == 0:
            adjustments_made = True
            log('Aligned admintools.conf ownership inside container with dbadmin account')
        else:
            log('Failed to align admintools.conf ownership inside container using dbadmin account')

    readable_result = _docker_exec(
        'dbadmin',
        f'test -r {quoted_path}',
        'Docker CLI is not available while validating admintools.conf permissions inside container',
    )
    if readable_result is None:
        return adjustments_made

    if readable_result.returncode == 0:
        return adjustments_made

    log('Detected unreadable admintools.conf inside container; attempting to relax permissions')

    fix_result = _docker_exec(
        '0',
        f'chmod a+r {quoted_path}',
        'Docker CLI is not available while adjusting admintools.conf permissions inside container',
    )
    if fix_result is None:
        return adjustments_made

    adjustments_made = True

    if fix_result.stdout:
        log(fix_result.stdout.rstrip())
    if fix_result.stderr:
        log(f'[stderr] {fix_result.stderr.rstrip()}')

    if fix_result.returncode != 0:
        log('Failed to adjust admintools.conf permissions inside container')
        return True

    readable_result = _docker_exec(
        'dbadmin',
        f'test -r {quoted_path}',
        'Docker CLI is not available while validating admintools.conf permissions inside container',
    )
    if readable_result is None:
        return True
    if readable_result.returncode == 0:
        return True

    parent_dir = shlex.quote(str(Path(_VERTICA_CONTAINER_ADMINTOOLS_PATH).parent))

    dir_fix_result = _docker_exec(
        '0',
        f'chmod a+rx {parent_dir}',
        'Docker CLI is not available while adjusting admintools.conf directory permissions inside container',
    )
    if dir_fix_result is None:
        return True

    adjustments_made = True

    if dir_fix_result.stdout:
        log(dir_fix_result.stdout.rstrip())
    if dir_fix_result.stderr:
        log(f'[stderr] {dir_fix_result.stderr.rstrip()}')

    if dir_fix_result.returncode != 0:
        log('Failed to adjust admintools.conf directory permissions inside container')
        return True

    readable_result = _docker_exec(
        'dbadmin',
        f'test -r {quoted_path}',
        'Docker CLI is not available while validating admintools.conf permissions inside container',
    )
    if readable_result is None:
        return True
    if readable_result.returncode != 0:
        log('Unable to verify admintools.conf readability inside container after permission adjustments')

    return True



def _reset_vertica_data_directories() -> bool:
    """Remove Vertica data directories to allow a clean container bootstrap."""

    log(STEP_SEPARATOR)
    log('Attempting to reset Vertica data directories for a clean bootstrap')

    removed_any = False

    for base_path in VERTICA_DATA_DIRECTORIES:
        for vertica_root in _candidate_vertica_roots(base_path) or [base_path / 'vertica']:
            if not vertica_root.exists():
                continue

            if vertica_root == base_path:
                log(
                    f'Skipping removal of base data directory {vertica_root} '
                    'while attempting reset'
                )
                continue

            log(f'Removing Vertica data directory at {vertica_root}')
            try:
                shutil.rmtree(vertica_root)
            except FileNotFoundError:
                continue
            except OSError as exc:
                log(f'Unable to remove {vertica_root}: {exc}')
            else:
                removed_any = True

    return removed_any


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


def _wait_for_docker_daemon(
    timeout_seconds: float = 180.0,
    interval_seconds: float = 3.0,
) -> tuple[bool, Optional[subprocess.CompletedProcess[str]]]:
    """Wait for ``docker info`` to succeed within ``timeout_seconds``."""

    deadline = time.monotonic() + max(timeout_seconds, 0.0)
    last_result: Optional[subprocess.CompletedProcess[str]] = None

    while time.monotonic() < deadline:
        last_result = _docker_info()
        if last_result.returncode == 0:
            return True, last_result
        time.sleep(interval_seconds)

    if last_result is None:
        last_result = _docker_info()

    return last_result.returncode == 0, last_result


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

    ready, info_result = _wait_for_docker_daemon(timeout_seconds=30.0, interval_seconds=2.0)
    if ready:
        return

    if shutil.which('systemctl') is None:
        log(STEP_SEPARATOR)
        log('Docker CLI found but daemon is unreachable and systemctl is unavailable')
        if info_result.stdout:
            log(info_result.stdout.rstrip())
        if info_result.stderr:
            log(f'[stderr] {info_result.stderr.rstrip()}')
        raise SystemExit('Unable to manage docker daemon without systemctl')

    log(STEP_SEPARATOR)
    log('Docker daemon unavailable; attempting to enable and start docker.service via systemctl')
    start_result = subprocess.run(
        ['systemctl', 'enable', '--now', 'docker'],
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
                raise SystemExit('Unable to start docker daemon via service command')
        else:
            raise SystemExit('Unable to start docker daemon via systemctl')

    ready, info_result = _wait_for_docker_daemon(timeout_seconds=180.0, interval_seconds=3.0)
    if ready:
        return

    if info_result and info_result.stdout:
        log(info_result.stdout.rstrip())
    if info_result and info_result.stderr:
        log(f'[stderr] {info_result.stderr.rstrip()}')
    raise SystemExit('Docker daemon did not start successfully')


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


def _docker_health_log(container: str) -> list[dict[str, object]]:
    """Return the Docker health check log entries for ``container``."""

    try:
        result = subprocess.run(
            ['docker', 'inspect', '--format', '{{json .State.Health.Log}}', container],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        log('Docker CLI is not available while inspecting container health log')
        return []
    if result.returncode != 0:
        return []

    output = result.stdout.strip()
    if not output or output == 'null':
        return []

    try:
        entries = json.loads(output)
    except json.JSONDecodeError:
        return []

    if not isinstance(entries, list):
        return []

    health_entries: list[dict[str, object]] = []
    for entry in entries:
        if isinstance(entry, dict):
            health_entries.append(entry)
    return health_entries


def _log_health_log_entries(
    container: str,
    seen_count: int,
) -> int:
    """Log new Docker health check entries for ``container``.

    Returns the updated count of observed entries.
    """

    entries = _docker_health_log(container)
    if not entries:
        return seen_count

    if len(entries) < seen_count:
        seen_count = 0

    new_entries = entries[seen_count:]
    if not new_entries:
        return seen_count

    for entry in new_entries:
        exit_code = entry.get('ExitCode')
        output = entry.get('Output') or ''
        start_ts = entry.get('Start') or '<unknown>'
        end_ts = entry.get('End') or '<unknown>'
        log(
            '  - Health check invocation '
            f'started {start_ts}, ended {end_ts}, exit code {exit_code}'
        )
        formatted_output = str(output).rstrip()
        if formatted_output:
            for line in formatted_output.splitlines():
                log(f'      {line}')

    return len(entries)


def _log_container_tail(container: str, tail: int = 200) -> None:
    """Log the most recent ``tail`` lines of stdout/stderr from ``container``."""

    result = subprocess.run(
        ['docker', 'logs', '--tail', str(tail), container],
        capture_output=True,
        text=True,
    )

    log(STEP_SEPARATOR)
    log(f'Last {tail} log lines from {container}')
    if result.stdout:
        log(result.stdout.rstrip())
    if result.stderr:
        log(f'[stderr] {result.stderr.rstrip()}')
    if result.returncode != 0:
        log(f'Unable to read logs from {container}: exit code {result.returncode}')


def _fetch_container_env(container: str) -> dict[str, str]:
    """Return the environment variables for ``container`` as a dictionary."""

    result = subprocess.run(
        ['docker', 'inspect', '--format', '{{json .Config.Env}}', container],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return {}

    output = result.stdout.strip()
    if not output:
        return {}

    try:
        env_entries = json.loads(output)
    except json.JSONDecodeError:
        return {}

    env: dict[str, str] = {}
    for entry in env_entries:
        if not isinstance(entry, str) or '=' not in entry:
            continue
        key, value = entry.split('=', 1)
        env[key] = value
    return env


_DOCKER_TIMESTAMP_PATTERN = re.compile(
    r'^(?P<base>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})'
    r'(?:\.(?P<fraction>\d+))?'
    r'(?P<tz>Z|[+-]\d{2}:\d{2})?$'
)


def _normalize_docker_timestamp(value: str) -> Optional[str]:
    """Normalize Docker timestamps so they can be parsed by :func:`datetime.fromisoformat`."""

    match = _DOCKER_TIMESTAMP_PATTERN.match(value.strip())
    if not match:
        return None

    base = match.group('base')
    fraction = match.group('fraction') or '0'
    tz = match.group('tz')

    # ``datetime.fromisoformat`` supports up to microseconds precision. Docker returns
    # nanosecond precision, so we truncate or pad the fractional portion accordingly.
    fraction = (fraction + '000000')[:6]

    if not tz or tz == 'Z':
        tz = '+00:00'

    return f'{base}.{fraction}{tz}'


def _container_uptime_seconds(container: str) -> Optional[float]:
    """Return the container uptime in seconds, if available."""

    started_at = _docker_inspect(container, '{{.State.StartedAt}}')
    if not started_at:
        return None

    normalized = _normalize_docker_timestamp(started_at)
    if not normalized:
        return None

    # Docker reports the zero ``StartedAt`` timestamp ("0001-01-01T00:00:00Z")
    # while the container is still starting. Treat this as "no uptime" so the
    # caller can continue waiting instead of interpreting it as an ancient
    # start time and triggering premature recovery attempts.
    if normalized.startswith('0001-01-01T00:00:00.000000'):
        return 0.0

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

    compose_version = 'v2.29.7'

    if _docker_compose_plugin_available() or shutil.which('docker-compose') is not None:
        return

    if _install_docker_compose_plugin(compose_version):
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

    if _install_docker_compose_plugin(compose_version):
        return

    if _download_docker_compose_binary(version=compose_version):
        return

    if not (_docker_compose_plugin_available() or shutil.which('docker-compose') is not None):
        raise SystemExit('Docker Compose CLI is not available after installation attempts')


def _download_compose_binary(destination: Path, version: str) -> bool:
    """Download a Docker Compose binary to ``destination``."""

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

    return True


def _download_docker_compose_binary(version: str = 'v2.29.7') -> bool:
    """Attempt to download the standalone Docker Compose binary as a fallback."""

    destination = Path('/usr/local/bin/docker-compose')
    if _download_compose_binary(destination, version):
        if shutil.which('docker-compose') is None:
            log('Docker Compose binary download completed but command is still unavailable')
            return False

        log('Docker Compose binary installed successfully')
        return True

    return False


def _install_docker_compose_plugin(version: str = 'v2.29.7') -> bool:
    """Install the Docker Compose CLI plugin if possible."""

    plugin_dir = Path('/usr/libexec/docker/cli-plugins')
    plugin_path = plugin_dir / 'docker-compose'

    if _download_compose_binary(plugin_path, version):
        if shutil.which('docker-compose') is None:
            fallback_path = Path('/usr/local/bin/docker-compose')
            try:
                fallback_path.parent.mkdir(parents=True, exist_ok=True)
                if fallback_path.exists() or fallback_path.is_symlink():
                    fallback_path.unlink()
                fallback_path.symlink_to(plugin_path)
            except OSError as exc:
                log(f'Unable to create docker-compose symlink: {exc}')

        return _docker_compose_plugin_available() or shutil.which('docker-compose') is not None

    return False


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


def _remove_stale_vertica_container() -> bool:
    """Attempt to remove a stale ``vertica_ce`` container if present."""

    try:
        presence_check = subprocess.run(
            [
                'docker',
                'ps',
                '--all',
                '--format',
                '{{.ID}}\t{{.Names}}',
            ],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        log('Docker CLI is unavailable while checking for stale Vertica containers')
        return False

    if presence_check.returncode != 0:
        if presence_check.stderr:
            log(f'[stderr] {presence_check.stderr.rstrip()}')
        return False

    container_ids = []
    for line in presence_check.stdout.splitlines():
        if not line.strip():
            continue

        try:
            container_id, container_name = line.split('\t', 1)
        except ValueError:
            container_id, container_name = line.strip(), ''

        if container_name.strip() == 'vertica_ce':
            container_ids.append(container_id.strip())

    if not container_ids:
        return False

    log('Removing stale Vertica container vertica_ce to resolve docker compose conflict')

    removal = subprocess.run(
        ['docker', 'rm', '-f', 'vertica_ce'],
        capture_output=True,
        text=True,
    )

    if removal.stdout:
        log(removal.stdout.rstrip())
    if removal.stderr:
        log(f'[stderr] {removal.stderr.rstrip()}')

    if removal.returncode != 0:
        log('Failed to remove stale Vertica container vertica_ce')
        return False

    log('Removed stale Vertica container vertica_ce')
    return True


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
    removal_attempted = False

    for command in commands:
        while True:
            try:
                run_command(command)
            except SystemExit as exc:
                last_error = exc
                if not removal_attempted and _remove_stale_vertica_container():
                    removal_attempted = True
                    log('Retrying docker compose after removing stale Vertica container')
                    continue
                break
            else:
                return

    if last_error is not None:
        raise last_error


_ECR_PRIVATE_RE = re.compile(
    r'^(?P<registry>[0-9]+\.dkr\.ecr\.(?P<region>[a-z0-9-]+)\.amazonaws\.com)(?P<path>/.+)$'
)
_ECR_PUBLIC_RE = re.compile(r'^(?P<registry>public\.ecr\.aws)(?P<path>/.+)$')
_ECR_LOGIN_RESULTS: dict[str, bool] = {}
_URLLIB3_REPAIR_ATTEMPTED = False
_PYTHON_SITE_PACKAGES_RE = re.compile(
    r'File "(?P<path>/usr/(?:local/)?lib(?:64)?/python[0-9.]+/site-packages)/'
)

_BOOTSTRAP_ADMIN_CREDENTIALS: Optional[tuple[str, str]] = None


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


def _ensure_ecr_login_for_image(image_name: str) -> bool:
    match = _ECR_PRIVATE_RE.match(image_name)
    registry: Optional[str]
    region: Optional[str]

    if match:
        registry = match.group('registry')
        region = match.group('region')
    else:
        match_public = _ECR_PUBLIC_RE.match(image_name)
        if not match_public:
            return True
        registry = match_public.group('registry')
        region = 'us-east-1'

    cached = _ECR_LOGIN_RESULTS.get(registry)
    if cached is not None:
        return cached

    if shutil.which('aws') is None:
        log(
            'AWS CLI is not available on the instance; unable to perform docker login for '
            f'{registry}'
        )
        raise SystemExit('AWS CLI is required to authenticate against ECR but is not installed')

    if match:
        log(STEP_SEPARATOR)
        log(f'Attempting ECR login for registry {registry} in region {region}')
        try:
            password_result = _run_aws_cli(
                ['aws', 'ecr', 'get-login-password', '--region', region]
            )
        except subprocess.CalledProcessError as exc:  # pragma: no cover - runtime failure path
            if exc.stdout:
                log(exc.stdout.rstrip())
            if exc.stderr:
                log(f'[stderr] {exc.stderr.rstrip()}')
            raise SystemExit(
                f'Failed to retrieve ECR login password for registry {registry} in region {region}'
            )
    else:
        log(STEP_SEPARATOR)
        log(f'Attempting ECR Public login for registry {registry}')
        try:
            password_result = _run_aws_cli(
                ['aws', 'ecr-public', 'get-login-password', '--region', region]
            )
        except subprocess.CalledProcessError as exc:  # pragma: no cover - runtime failure path
            if exc.stdout:
                log(exc.stdout.rstrip())
            if exc.stderr:
                log(f'[stderr] {exc.stderr.rstrip()}')
            raise SystemExit(
                f'Failed to retrieve ECR Public login password for registry {registry}'
            )

    password = password_result.stdout.strip()
    if not password:
        raise SystemExit('ECR login password command returned empty output')

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
        raise SystemExit(
            f'Docker login for {registry} failed with exit code {login_result.returncode}'
        )

    _ECR_LOGIN_RESULTS[registry] = True
    return True


def _pull_image_if_possible(image_name: str) -> None:
    try:
        run_command(['docker', 'pull', image_name])
    except SystemExit as exc:
        log(f'Docker pull for {image_name} failed: {exc}')


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
    data_reset_attempted = False
    unhealthy_observed_at: Optional[float] = None
    unhealthy_logged_duration: Optional[float] = None
    health_log_count = 0
    last_unhealthy_log_dump: Optional[float] = None
    admintools_permissions_checked = False

    compose_deadline = time.time() + compose_timeout

    while time.time() < deadline:
        status = _docker_inspect('vertica_ce', '{{.State.Status}}')
        health = _docker_inspect('vertica_ce', '{{if .State.Health}}{{.State.Health.Status}}{{end}}')
        if status:
            health_log_count = _log_health_log_entries('vertica_ce', health_log_count)
        if status == 'running' and (not health or health == 'healthy'):
            unhealthy_observed_at = None
            unhealthy_logged_duration = None
            last_unhealthy_log_dump = None
            admintools_permissions_checked = False
            log(f'Vertica container status: {status}, health: {health or "unknown"}')
            return

        if (status, health) != last_status:
            last_status = (status, health)
            log(f'Current Vertica container status: {status or "<absent>"}, health: {health or "<unknown>"}')
            if health != 'unhealthy':
                unhealthy_observed_at = None
                unhealthy_logged_duration = None
                last_unhealthy_log_dump = None
                admintools_permissions_checked = False

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
            unhealthy_observed_at = None
            unhealthy_logged_duration = None
            last_unhealthy_log_dump = None
            admintools_permissions_checked = False
        elif status not in {'running', 'restarting'}:
            run_command(['docker', 'start', 'vertica_ce'])
            restart_attempts = 0
            recreate_attempts = 0
            unhealthy_observed_at = None
            unhealthy_logged_duration = None
            last_unhealthy_log_dump = None
            admintools_permissions_checked = False
        elif health == 'unhealthy':
            now = time.time()
            if unhealthy_observed_at is None:
                unhealthy_observed_at = now

            unhealthy_duration = now - unhealthy_observed_at
            if (
                unhealthy_duration >= 120
                and (last_unhealthy_log_dump is None or now - last_unhealthy_log_dump >= 120)
            ):
                _log_container_tail('vertica_ce', tail=200)
                last_unhealthy_log_dump = now

            if not admintools_permissions_checked:
                admintools_permissions_checked = True
                if _ensure_container_admintools_conf_readable('vertica_ce'):
                    log('Relaxed admintools.conf permissions inside container; waiting for recovery')
                    time.sleep(5)
                    continue
            if (
                unhealthy_duration
                < UNHEALTHY_HEALTHCHECK_GRACE_PERIOD_SECONDS
            ):
                if (
                    unhealthy_logged_duration is None
                    or unhealthy_duration - unhealthy_logged_duration >= 30
                    or unhealthy_duration < unhealthy_logged_duration
                    ):
                        log(
                            'Vertica container health reported unhealthy but has '
                            f'been unhealthy for {unhealthy_duration:.0f}s; '
                            'waiting for recovery'
                        )
                        unhealthy_logged_duration = unhealthy_duration
                time.sleep(10)
                continue

            uptime = _container_uptime_seconds('vertica_ce')
            if uptime is None:
                log(
                    'Vertica container health reported unhealthy but uptime '
                    'could not be determined; assuming the container is still starting'
                )
                time.sleep(10)
                continue
            if uptime is not None and uptime < UNHEALTHY_HEALTHCHECK_GRACE_PERIOD_SECONDS:
                log(
                    'Vertica container health reported unhealthy but uptime '
                    f'{uptime:.0f}s is within grace period; waiting for recovery'
                )
                unhealthy_logged_duration = unhealthy_duration
                time.sleep(10)
                continue

            if restart_attempts < 3:
                log('Vertica container health check reported unhealthy; restarting container')
                run_command(['docker', 'restart', 'vertica_ce'])
                restart_attempts += 1
                time.sleep(10)
                unhealthy_observed_at = None
                unhealthy_logged_duration = None
                last_unhealthy_log_dump = None
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
                unhealthy_observed_at = None
                unhealthy_logged_duration = None
                last_unhealthy_log_dump = None
                continue

            if not data_reset_attempted:
                data_reset_attempted = True
                if compose_file is None:
                    compose_file = _compose_file()
                if _reset_vertica_data_directories():
                    log('Vertica data directories reset; re-running bootstrap sequence')
                    _sanitize_vertica_data_directories()
                    restart_attempts = 0
                    recreate_attempts = 0
                    unhealthy_observed_at = None
                    unhealthy_logged_duration = None
                    last_unhealthy_log_dump = None
                    if compose_file is not None:
                        _ensure_ecr_login_if_needed(compose_file)
                        _compose_up(compose_file, force_recreate=True)
                    time.sleep(15)
                    continue
                log('Failed to reset Vertica data directories or nothing to reset')

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


def _bootstrap_admin_credentials() -> tuple[str, str]:
    """Return the Vertica bootstrap administrator credentials."""

    global _BOOTSTRAP_ADMIN_CREDENTIALS
    if _BOOTSTRAP_ADMIN_CREDENTIALS is not None:
        return _BOOTSTRAP_ADMIN_CREDENTIALS

    container_env = _fetch_container_env('vertica_ce')

    user_source: str
    if 'VERTICA_DB_USER' in container_env:
        user = container_env['VERTICA_DB_USER'] or BOOTSTRAP_ADMIN_DEFAULT_USER
        user_source = 'container configuration'
    elif os.getenv('DBADMIN_USER'):
        user = os.environ['DBADMIN_USER']
        user_source = 'environment variable DBADMIN_USER'
    else:
        user = BOOTSTRAP_ADMIN_DEFAULT_USER
        user_source = 'built-in default'

    password_source: str
    password = container_env.get('VERTICA_DB_PASSWORD')
    if password is not None:
        password_source = 'container configuration'
    else:
        env_password = os.environ.get('DBADMIN_PASSWORD')
        if env_password is not None:
            password = env_password
            password_source = 'environment variable DBADMIN_PASSWORD'
        else:
            password = ''
            password_source = 'empty password default'

    log(f'Using bootstrap admin user {user!r} from {user_source}')
    log(f'Resolved bootstrap admin password from {password_source}')

    _BOOTSTRAP_ADMIN_CREDENTIALS = (user, password)
    return _BOOTSTRAP_ADMIN_CREDENTIALS


def _ensure_primary_admin_user(
    bootstrap_user: str,
    bootstrap_password: str,
    admin_user: str,
    admin_password: str,
) -> None:
    """Ensure the primary admin user exists with the expected credentials."""

    if admin_user == bootstrap_user:
        return

    log(STEP_SEPARATOR)
    log(
        'Ensuring primary admin user '
        f"{admin_user!r} exists and has the expected credentials"
    )
    config = {
        'host': '127.0.0.1',
        'port': DB_PORT,
        'user': bootstrap_user,
        'password': bootstrap_password,
        'database': DB_NAME,
        'autocommit': True,
        'tlsmode': 'disable',
    }

    with vertica_python.connect(**config) as connection:
        cursor = connection.cursor()
        cursor.execute('SELECT 1 FROM users WHERE user_name = %s', [admin_user])
        exists = cursor.fetchone() is not None

        if exists:
            log(f'Primary admin user {admin_user!r} already exists; rotating password')
            cursor.execute(
                f'ALTER USER {_quote_identifier(admin_user)} IDENTIFIED BY %s',
                [admin_password],
            )
        else:
            log(f'Creating primary admin user {admin_user!r}')
            cursor.execute(
                f'CREATE USER {_quote_identifier(admin_user)} IDENTIFIED BY %s',
                [admin_password],
            )

        grants = [
            f'GRANT CONNECT ON DATABASE {_quote_identifier(DB_NAME)} TO '
            f'{_quote_identifier(admin_user)}',
            f'GRANT ALL PRIVILEGES ON DATABASE {_quote_identifier(DB_NAME)} TO '
            f'{_quote_identifier(admin_user)}',
            f'GRANT USAGE ON SCHEMA PUBLIC TO {_quote_identifier(admin_user)}',
            f'GRANT ALL PRIVILEGES ON SCHEMA PUBLIC TO {_quote_identifier(admin_user)}',
        ]

        for statement in grants:
            cursor.execute(statement)


def _resolve_tlsmode() -> str:
    """Determine the TLS mode to use for Vertica connections."""

    for key in ("VERTICA_TLSMODE", "DB_TLSMODE"):
        value = os.getenv(key)
        if value:
            stripped = value.strip()
            if stripped:
                return stripped
    return "prefer"


def connect_and_query(
    label: str,
    host: str,
    user: str,
    password: str,
    *,
    attempts: int = 30,
    delay: float = 10.0,
    fatal: bool = True,
) -> bool:
    log(STEP_SEPARATOR)
    log(f'[{label}] Connecting to Vertica at {host}:{DB_PORT} as {user!r}')
    config = {
        'host': host,
        'port': DB_PORT,
        'user': user,
        'password': password,
        'database': DB_NAME,
        'autocommit': True,
        'tlsmode': _resolve_tlsmode(),
    }

    last_error: Optional[BaseException] = None

    for attempt in range(1, attempts + 1):
        try:
            with vertica_python.connect(**config) as connection:
                cursor = connection.cursor()
                cursor.execute('SELECT 1')
                value = cursor.fetchone()
                if not value or value[0] != 1:
                    raise SystemExit(
                        f'Unexpected response from SELECT 1 during {label}'
                    )
                log(f'[{label}] SELECT 1 -> {value[0]}')
                return True
        except Exception as exc:  # pragma: no cover - runtime failure path
            last_error = exc
            if attempt >= attempts:
                break

            log(
                f'[{label}] Connection attempt {attempt} failed with {exc!r}; '
                f'retrying in {delay:.0f}s ({attempts - attempt} attempt(s) remaining)'
            )
            time.sleep(delay)

    if last_error:
        message = f'[{label}] Failed to connect to Vertica: {last_error}'
        if fatal:
            raise SystemExit(message) from last_error
        log(message)
        return False

    return True


def main() -> int:
    log('Beginning in-instance Vertica smoke test with detailed diagnostics')
    hostname = socket.gethostname()
    local_ipv4 = fetch_metadata('meta-data/local-ipv4')
    public_ipv4 = fetch_metadata('meta-data/public-ipv4')
    log(f'Instance hostname: {hostname}')
    log(f'Instance local IPv4: {local_ipv4}')
    log(f'Instance public IPv4: {public_ipv4}')

    ensure_docker_service()
    _sanitize_vertica_data_directories()
    ensure_vertica_container_running()
    wait_for_port('127.0.0.1', DB_PORT)
    log('Verified Vertica port 5433 is accepting TCP connections on localhost')

    run_command(['docker', 'ps', '--format', '{{.Names}}\t{{.Status}}'])
    image_result = run_command(['docker', 'inspect', '--format', '{{.Config.Image}}', 'vertica_ce'])
    image_name = image_result.stdout.strip()
    if image_name:
        log(f'Vertica container image: {image_name}')
        _ensure_ecr_login_for_image(image_name)
        _pull_image_if_possible(image_name)
    run_command(['docker', 'inspect', '--format', '{{json .NetworkSettings.Ports}}', 'vertica_ce'])

    bootstrap_user, bootstrap_password = _bootstrap_admin_credentials()
    connect_and_query(
        f'{bootstrap_user}@localhost', '127.0.0.1', bootstrap_user, bootstrap_password
    )
    _ensure_primary_admin_user(
        bootstrap_user, bootstrap_password, ADMIN_USER, ADMIN_PASSWORD
    )
    connect_and_query('primary_admin@localhost', '127.0.0.1', ADMIN_USER, ADMIN_PASSWORD)

    if not connect_and_query(
        f'{bootstrap_user}@public_ip',
        public_ipv4,
        bootstrap_user,
        bootstrap_password,
        fatal=False,
    ):
        log(
            f'[{bootstrap_user}@public_ip] Connection attempts failed; continuing without '
            'treating this as fatal'
        )

    smoke_user = f'smoke_{uuid.uuid4().hex[:8]}'
    smoke_pass = uuid.uuid4().hex
    log(STEP_SEPARATOR)
    log(f'Creating smoke test user {smoke_user!r}')
    smoke_user_created = False
    with vertica_python.connect(host='127.0.0.1', port=DB_PORT, user=ADMIN_USER, password=ADMIN_PASSWORD, database=DB_NAME, autocommit=True) as admin_conn:
        admin_cursor = admin_conn.cursor()
        admin_cursor.execute(
            f'CREATE USER {_quote_identifier(smoke_user)} IDENTIFIED BY %s',
            [smoke_pass],
        )
        admin_cursor.execute(
            f'GRANT ALL PRIVILEGES ON DATABASE {_quote_identifier(DB_NAME)} '
            f'TO {_quote_identifier(smoke_user)}'
        )
        admin_cursor.execute(
            f'GRANT USAGE ON SCHEMA PUBLIC TO {_quote_identifier(smoke_user)}'
        )
        admin_cursor.execute(
            f'GRANT ALL PRIVILEGES ON SCHEMA PUBLIC TO {_quote_identifier(smoke_user)}'
        )
        smoke_user_created = True

    try:
        connect_and_query('smoke_user@localhost', '127.0.0.1', smoke_user, smoke_pass)
    finally:
        if smoke_user_created:
            log(STEP_SEPARATOR)
            log(f'Dropping smoke test user {smoke_user!r}')
            with vertica_python.connect(host='127.0.0.1', port=DB_PORT, user=ADMIN_USER, password=ADMIN_PASSWORD, database=DB_NAME, autocommit=True) as admin_conn:
                admin_conn.cursor().execute(
                    f'DROP USER {_quote_identifier(smoke_user)} CASCADE'
                )

    log(STEP_SEPARATOR)
    log('All smoke test checks completed successfully')
    log('SMOKE_TEST_SUCCESS')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
