import configparser
import errno
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
import tempfile
import textwrap
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
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
# During the first bootstrap Vertica is responsible for populating
# ``admintools.conf`` inside the persistent data directory.  If the file is
# still missing several minutes after the container is running we treat the
# bootstrap as stuck and rebuild the directory.
ADMINTOOLS_CONF_MISSING_GRACE_PERIOD_SECONDS = 300.0
# Treat repeated container restarts as a bootstrap failure even if the uptime
# grace period has not elapsed so we can seed ``admintools.conf`` proactively.
ADMINTOOLS_CONF_MISSING_RESTART_THRESHOLD = 2

# Recent Vertica container images require the license acceptance tokens to use
# uppercase ``ACCEPT`` rather than the lowercase values older releases accepted.
# Normalise the values here so the automation remains compatible with both
# behaviours.
_EULA_ENVIRONMENT_VARIABLES: dict[str, str] = {
    'VERTICA_EULA_ACCEPTED': '1',
    'VERTICA_DB_EULA': 'ACCEPT',
    'VERTICA_DB_EULA_ACCEPTED': '1',
    # Newer Vertica container images ship an updated web-based Management Console
    # agent that enforces its own EULA prompt during startup.  The agent shares
    # the same container as the database which means that an unanswered prompt
    # leaves the Docker health check stuck in an unhealthy state (the agent
    # repeatedly attempts to display the license text and exits).  Accept the
    # additional agreements up-front so the container can progress to a healthy
    # state without requiring interactive acknowledgement.
    'VERTICA_EULA': 'ACCEPT',
    'VERTICA_MC_EULA': 'ACCEPT',
    'VERTICA_MC_EULA_ACCEPTED': '1',
}


def log(message: str) -> None:
    print(message, flush=True)


class CommandError(SystemExit):
    """Exception raised when ``run_command`` encounters a failure."""

    def __init__(
        self,
        command: list[str],
        returncode: int,
        stdout: str,
        stderr: str,
    ) -> None:
        self.command = command
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        super().__init__(
            f'Command {command!r} failed with exit code {returncode}'
        )


def run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    log(STEP_SEPARATOR)
    log(f'$ {" ".join(command)}')
    result = subprocess.run(command, capture_output=True, text=True)
    if result.stdout:
        log(result.stdout.rstrip())
    if result.stderr:
        log(f'[stderr] {result.stderr.rstrip()}')
    if result.returncode != 0:
        raise CommandError(command, result.returncode, result.stdout, result.stderr)
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
# Map known host Vertica data directories to potential locations inside the
# container.  Some infrastructure variants mount ``/var/lib/vertica`` from the
# host into ``/data/vertica`` inside the container, while others keep the same
# path on both sides of the bind mount.  Include both possibilities so recovery
# logic can seed ``admintools.conf`` wherever Vertica expects it.
_VERTICA_CONTAINER_DATA_DIRECTORY_MAPPINGS: dict[Path, tuple[str, ...]] = {
    Path('/var/lib/vertica'): ('/var/lib/vertica', '/data/vertica'),
    Path('/data/vertica'): ('/data/vertica',),
}
ADMINTOOLS_CONF_SEED_RECOVERY_SECONDS = 300.0

# Track when ``admintools.conf`` was first observed missing for each Vertica
# data directory.  Some bootstrap failures repeatedly start the container and
# leave the uptime below the normal grace period, which previously prevented the
# recovery logic from ever attempting to seed a default configuration.  Record
# the initial observation time so we can fall back to remediation once the
# missing file persists beyond the configured threshold regardless of the
# current container uptime.
_ADMINTOOLS_CONF_MISSING_OBSERVED_AT: dict[Path, float] = {}
# Track when a default ``admintools.conf`` was last written for each Vertica
# data directory.  When the file remains missing inside the container long after
# seeding completes the most reliable recovery option is to rebuild the data
# directory from scratch so the Vertica bootstrap logic can repopulate it.
# Track Vertica configuration directories that have existed on disk.  The
# smoke test only seeds ``admintools.conf`` after the container has populated
# ``config/`` at least once; creating the directory prematurely interferes with
# Vertica's first-run bootstrap copy and leaves ``admintools.conf`` missing
# inside the container.  Remember which directories have been observed so we can
# distinguish a fresh bootstrap (where we must wait for Vertica to copy the
# defaults) from a corrupted installation that genuinely needs recovery.
_ADMINTOOLS_CONF_SEEDED_AT: dict[Path, float] = {}
_VERTICA_CONTAINER_RESTART_THROTTLE_SECONDS = 60.0
_LAST_VERTICA_CONTAINER_RESTART: Optional[float] = None
_OBSERVED_VERTICA_CONFIG_DIRECTORIES: set[Path] = set()


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
        include_pair: Optional[tuple[int, int]]
        try:
            entry = pwd.getpwuid(uid)
        except KeyError:
            if container_identity == (uid, gid):
                include_pair = container_identity
            else:
                continue
        except OSError as exc:
            log(
                'Unable to resolve fallback Vertica admin identity '
                f'uid {uid}: {exc}'
            )
            continue
        else:
            include_pair = (entry.pw_uid, entry.pw_gid)

        if include_pair not in seen:
            candidates.append(include_pair)
            seen.add(include_pair)

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

    # Always consider ``base_path`` itself so callers can manage Vertica
    # installations that persist shared configuration directly under the data
    # directory (for example ``/data/vertica/config``).  Previous behaviour only
    # included ``base_path`` when ``config/`` already existed which prevented the
    # smoke test from creating the directory or seeding ``admintools.conf`` when
    # the configuration tree was missing entirely.
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
        if path.is_symlink():
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
            preexisting_root = vertica_root.exists()
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

            if not preexisting_root:
                # Avoid seeding configuration in directories that were created
                # solely for this run.  Fresh directories are empty by design
                # and will be populated by the Vertica container during its
                # initial bootstrap sequence.  Attempting to seed
                # ``admintools.conf`` immediately can interfere with the
                # container's first-run copy step (which expects to populate an
                # empty target) and triggers bootstrap loops similar to the
                # production failure this logic is mitigating.  Revisit the
                # directory on the next pass once Vertica has created its
                # expected structure.
                continue

            config_path = vertica_root / 'config'
            if config_path.is_symlink():
                try:
                    target = os.readlink(config_path)
                except OSError as exc:
                    log(f'Unable to inspect symlink {config_path}: {exc}')
                else:
                    remove_symlink = False
                    normalized_target = os.path.normpath(target)
                    if normalized_target.startswith('/data') or normalized_target.startswith('data'):
                        remove_symlink = True
                    elif normalized_target == '/opt/vertica/config':
                        remove_symlink = True
                    else:
                        target_parts = PurePosixPath(normalized_target).parts
                        if target_parts[-3:] == ('opt', 'vertica', 'config'):
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

            config_exists = config_path.exists() and config_path.is_dir()
            config_observed = config_path in _OBSERVED_VERTICA_CONFIG_DIRECTORIES
            current_time = time.time()

            if not config_exists:
                if config_observed:
                    if not _ensure_directory(config_path):
                        continue
                    _ensure_known_identity_tree(config_path, max_depth=2)
                    config_exists = True
                else:
                    observed_at = _ADMINTOOLS_CONF_MISSING_OBSERVED_AT.get(config_path)
                    if observed_at is None:
                        _ADMINTOOLS_CONF_MISSING_OBSERVED_AT[config_path] = current_time
                        log(
                            'Detected missing admintools.conf but Vertica '
                            'configuration directory has not been observed yet; '
                            'allowing the container to complete its initial '
                            'bootstrap before seeding defaults'
                        )
                        continue

                    missing_duration = current_time - observed_at
                    container_status = _docker_inspect(
                        'vertica_ce', '{{.State.Status}}'
                    )
                    container_health = _docker_inspect(
                        'vertica_ce', '{{if .State.Health}}{{.State.Health.Status}}{{end}}'
                    )
                    status_display = container_status or '<absent>'
                    health_display = container_health or '<unknown>'
                    uptime = _container_uptime_seconds('vertica_ce')
                    restart_count = _container_restart_count('vertica_ce')
                    restart_display = 'unknown' if restart_count is None else str(restart_count)

                    within_missing_grace = missing_duration < ADMINTOOLS_CONF_MISSING_GRACE_PERIOD_SECONDS
                    uptime_within_grace = (
                        uptime is None
                        or uptime < ADMINTOOLS_CONF_MISSING_GRACE_PERIOD_SECONDS
                    )
                    restart_within_threshold = (
                        restart_count is None
                        or restart_count < ADMINTOOLS_CONF_MISSING_RESTART_THRESHOLD
                    )

                    if within_missing_grace and uptime_within_grace and restart_within_threshold:
                        log(
                            'Detected missing Vertica configuration directory '
                            f'while container status is {status_display} with '
                            f'health {health_display}; missing for '
                            f'{missing_duration:.0f}s which remains within '
                            'grace period so allowing bootstrap to continue '
                            'before seeding defaults'
                        )
                        continue

                    log(
                        'Vertica configuration directory has not been '
                        f'populated after {missing_duration:.0f}s with '
                        f'status {status_display} and health {health_display}; '
                        'creating directory and seeding defaults to assist '
                        'recovery'
                    )

                    if not _ensure_directory(config_path):
                        continue

                    config_exists = True
                    _OBSERVED_VERTICA_CONFIG_DIRECTORIES.add(config_path)
                    _ensure_known_identity_tree(config_path, max_depth=2)

            if config_exists:
                _OBSERVED_VERTICA_CONFIG_DIRECTORIES.add(config_path)
                _ensure_directory(config_path)
                _ensure_known_identity_tree(config_path, max_depth=2)

            admintools_conf = config_path / 'admintools.conf'
            if admintools_conf.exists():
                _ADMINTOOLS_CONF_MISSING_OBSERVED_AT.pop(config_path, None)
                _ADMINTOOLS_CONF_SEEDED_AT.pop(config_path, None)
            else:
                config_observed = config_path in _OBSERVED_VERTICA_CONFIG_DIRECTORIES
                if not config_observed:
                    _ADMINTOOLS_CONF_MISSING_OBSERVED_AT.pop(config_path, None)
                    _ADMINTOOLS_CONF_SEEDED_AT.pop(config_path, None)
                    log(
                        'Detected missing admintools.conf but Vertica ' 
                        'configuration directory has not been observed yet; '
                        'allowing the container to complete its initial '
                        'bootstrap before seeding defaults'
                    )
                    continue
                container_status = _docker_inspect(
                    'vertica_ce', '{{.State.Status}}'
                )
                container_health = _docker_inspect(
                    'vertica_ce', '{{if .State.Health}}{{.State.Health.Status}}{{end}}'
                )
                status_display = container_status or '<absent>'
                health_display = container_health or '<unknown>'

                observed_at = _ADMINTOOLS_CONF_MISSING_OBSERVED_AT.get(config_path)
                if observed_at is None:
                    observed_at = time.time()
                    _ADMINTOOLS_CONF_MISSING_OBSERVED_AT[config_path] = observed_at
                missing_duration = time.time() - observed_at

                force_directory_rebuild = False
                seeded_at = _ADMINTOOLS_CONF_SEEDED_AT.get(config_path)
                if seeded_at is not None:
                    since_seed = time.time() - seeded_at
                    if since_seed >= ADMINTOOLS_CONF_SEED_RECOVERY_SECONDS:
                        log(
                            'admintools.conf remains missing for '
                            f'{since_seed:.0f}s after seeding a default '
                            'configuration; removing the Vertica data '
                            'directory to allow bootstrap to repopulate it'
                        )
                        force_directory_rebuild = True
                        _ADMINTOOLS_CONF_SEEDED_AT.pop(config_path, None)

                if (
                    not force_directory_rebuild
                    and container_status in {'running', 'restarting'}
                ):
                    uptime = _container_uptime_seconds('vertica_ce')
                    restart_count = _container_restart_count('vertica_ce')
                    restart_display = 'unknown' if restart_count is None else str(restart_count)

                    if (
                        uptime is None
                        and (
                            restart_count is None
                            or restart_count < ADMINTOOLS_CONF_MISSING_RESTART_THRESHOLD
                        )
                        and missing_duration < ADMINTOOLS_CONF_MISSING_GRACE_PERIOD_SECONDS
                    ):
                        log(
                            'Detected missing admintools.conf while Vertica '
                            f'container status is {status_display} with health '
                            f'{health_display}; container uptime is unknown and '
                            f'restart count {restart_display} is below the '
                            'restart threshold so allowing the running '
                            'container to complete bootstrap before modifying '
                            'the data directory'
                        )
                        continue

                    if (
                        uptime is not None
                        and uptime < ADMINTOOLS_CONF_MISSING_GRACE_PERIOD_SECONDS
                        and (
                            restart_count is None
                            or restart_count < ADMINTOOLS_CONF_MISSING_RESTART_THRESHOLD
                        )
                        and missing_duration < ADMINTOOLS_CONF_MISSING_GRACE_PERIOD_SECONDS
                    ):
                        log(
                            'Detected missing admintools.conf while Vertica '
                            f'container status is {status_display} with health '
                            f'{health_display}; container uptime '
                            f'{uptime:.0f}s is within grace period and '
                            f'restart count {restart_display} is below the '
                            'restart threshold so allowing the running '
                            'container to complete bootstrap before modifying '
                            'the data directory'
                        )
                        continue

                    uptime_display = 'unknown' if uptime is None else f'{uptime:.0f}s'
                    if missing_duration >= ADMINTOOLS_CONF_MISSING_GRACE_PERIOD_SECONDS:
                        log(
                            'Detected missing admintools.conf while Vertica '
                            f'container status is {status_display} with health '
                            f'{health_display}; missing for '
                            f'{missing_duration:.0f}s which exceeds the grace '
                            'period so attempting to seed default configuration '
                            'to recover'
                        )
                    else:
                        log(
                            'Detected missing admintools.conf while Vertica '
                            f'container status is {status_display} with health '
                            f'{health_display}; uptime {uptime_display} and restart '
                            f'count {restart_display} exceed recovery thresholds; '
                            'attempting to seed default configuration to recover'
                        )
                    seed_success, seed_changed = _seed_default_admintools_conf(config_path)
                    if seed_success:
                        if (
                            seed_changed
                            or config_path not in _ADMINTOOLS_CONF_SEEDED_AT
                        ):
                            _ADMINTOOLS_CONF_SEEDED_AT[config_path] = time.time()
                        _OBSERVED_VERTICA_CONFIG_DIRECTORIES.add(config_path)
                        _ensure_known_identity_tree(config_path, max_depth=2)
                        synchronized = _synchronize_container_admintools_conf(
                            'vertica_ce', admintools_conf
                        )
                        if synchronized:
                            log(
                                'Copied admintools.conf into Vertica container to '
                                'assist recovery'
                            )

                        container_has_admintools = _container_path_exists(
                            'vertica_ce', _VERTICA_CONTAINER_ADMINTOOLS_PATH
                        )

                        should_restart = seed_changed or synchronized

                        if container_has_admintools is True:
                            log('Seeded default admintools.conf to assist Vertica recovery')
                            _ADMINTOOLS_CONF_MISSING_OBSERVED_AT.pop(config_path, None)
                            _ADMINTOOLS_CONF_SEEDED_AT.pop(config_path, None)
                        elif container_has_admintools is False:
                            log(
                                'Seeded default admintools.conf to assist Vertica '
                                'recovery but configuration remains missing inside '
                                'container; continuing to monitor'
                            )
                            should_restart = True
                        else:
                            log(
                                'Seeded default admintools.conf to assist Vertica '
                                'recovery but was unable to verify configuration '
                                'inside container; continuing to monitor'
                            )
                            should_restart = True

                        if should_restart:
                            _restart_vertica_container(
                                'vertica_ce', 'apply seeded admintools.conf'
                            )
                        continue

                    log(
                        'Failed to seed default admintools.conf for Vertica '
                        'recovery; falling back to rebuilding data directory'
                    )

                elif (
                    container_status in {'created', 'paused', 'exited'}
                    and container_health == 'healthy'
                ):
                    log(
                        'Detected missing admintools.conf but Vertica '
                        f'container is {status_display} (healthy); '
                        'skipping directory removal to avoid disrupting '
                        'the container'
                    )
                    continue

                removal_attempted = False
                if container_status in {None, 'dead'}:
                    log(
                        'Detected missing admintools.conf while Vertica '
                        f'container status is {status_display}; removing '
                        'incomplete data directory to allow Vertica to '
                        'rebuild it during startup'
                    )
                else:
                    log(
                        'Detected missing admintools.conf while Vertica '
                        f'container status is {status_display} with health '
                        f'{health_display}; removing incomplete data '
                        'directory to allow Vertica to rebuild it during '
                        'startup'
                    )

                if container_status:
                    removal_attempted = True
                    try:
                        removal = subprocess.run(
                            ['docker', 'rm', '-f', 'vertica_ce'],
                            capture_output=True,
                            text=True,
                        )
                    except FileNotFoundError:
                        removal_attempted = False
                        log(
                            'Docker CLI unavailable while attempting to '
                            'remove vertica_ce container; continuing with '
                            'directory cleanup'
                        )
                    else:
                        if removal.stdout:
                            log(removal.stdout.rstrip())
                        if removal.stderr:
                            log(f'[stderr] {removal.stderr.rstrip()}')
                        if removal.returncode != 0:
                            log(
                                'Failed to remove vertica_ce container prior '
                                f'to configuration cleanup: exit code '
                                f'{removal.returncode}'
                            )

                removal_target = vertica_root
                recreate_root = True

                if vertica_root == base_path:
                    # ``vertica_root`` may refer to the top-level Vertica data
                    # directory (for example ``/data/vertica``).  Removing the
                    # entire tree would delete unrelated cluster state such as
                    # the ``VMart`` database and MC agent directories.  Focus
                    # on the configuration subdirectory instead so the
                    # container can repopulate ``config`` without disrupting
                    # other persistent data.
                    removal_target = config_path
                    recreate_root = False

                if removal_attempted:
                    log(
                        'Removing incomplete Vertica data directory at '
                        f'{removal_target} (admintools.conf missing) after '
                        'stopping container'
                    )
                else:
                    log(
                        'Removing incomplete Vertica data directory at '
                        f'{removal_target} (admintools.conf missing) to allow '
                        'Vertica to rebuild it during startup'
                    )

                try:
                    shutil.rmtree(removal_target)
                except FileNotFoundError:
                    pass
                except OSError as exc:
                    log(f'Unable to remove {removal_target}: {exc}')
                else:
                    # Recreate the Vertica root directory so Docker can
                    # mount it, but avoid pre-populating the ``config``
                    # subdirectory.  When ``config`` exists before the
                    # container starts the bootstrap step that copies
                    # default configuration files notices the source and
                    # destination paths are identical and aborts the
                    # copy.  This leaves ``admintools.conf`` missing
                    # inside the container which prevents Vertica from
                    # finishing startup.  Allow the container to
                    # repopulate ``config`` from scratch instead.
                    if recreate_root:
                        _ensure_directory(vertica_root)
                    else:
                        _ensure_directory(base_path)
                    _ADMINTOOLS_CONF_MISSING_OBSERVED_AT.pop(config_path, None)
                    _ADMINTOOLS_CONF_SEEDED_AT.pop(config_path, None)
                    _OBSERVED_VERTICA_CONFIG_DIRECTORIES.discard(config_path)
                continue

            if config_path.exists() or config_path.is_symlink():
                _ensure_directory(config_path)
                _ensure_known_identity_tree(config_path, max_depth=2)
                _seed_default_admintools_conf(config_path)


def _seed_default_admintools_conf(config_dir: Path) -> tuple[bool, bool]:
    """Ensure ``admintools.conf`` exists with safe defaults.

    Returns a tuple ``(success, changed)`` where ``success`` indicates that the
    configuration file is present (either because it already existed or was
    created) and ``changed`` notes whether the call wrote a new file or updated
    an invalid one.  Callers use ``changed`` to decide when to start recovery
    timers so that repeated idempotent invocations do not continuously extend
    the observation window for a missing configuration inside the container.
    """

    admintools_conf = config_dir / 'admintools.conf'
    needs_rebuild = False

    try:
        is_symlink = admintools_conf.is_symlink()
    except OSError as exc:
        if exc.errno not in (errno.ENOENT, errno.EINVAL):
            log(
                'Unable to inspect admintools.conf while checking for symlinks '
                f'({admintools_conf}): {exc}'
            )
            return False, False
        is_symlink = False

    if is_symlink:
        try:
            target = os.readlink(admintools_conf)
        except OSError:
            target = None
        if target is None:
            log(
                'Removing symlinked admintools.conf at '
                f'{admintools_conf} to rebuild a regular file'
            )
        else:
            log(
                'Removing symlinked admintools.conf at '
                f'{admintools_conf} -> {target} to rebuild a regular file'
            )
        try:
            admintools_conf.unlink()
        except OSError as exc:
            log(
                'Unable to remove symlinked admintools.conf '
                f'{admintools_conf}: {exc}'
            )
            return False, False

    try:
        exists = admintools_conf.exists()
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            log(
                'Encountered recursive symlink at admintools.conf '
                f'({admintools_conf}); rebuilding with safe defaults'
            )
            try:
                admintools_conf.unlink()
            except OSError as unlink_exc:
                log(
                    'Unable to remove recursive admintools.conf symlink '
                    f'{admintools_conf}: {unlink_exc}'
                )
                return False, False
            exists = False
        else:
            log(
                'Unable to determine whether admintools.conf exists '
                f'({admintools_conf}): {exc}'
            )
            return False, False

    if exists:
        if _admintools_conf_needs_rebuild(admintools_conf):
            log(
                'Existing admintools.conf is missing critical configuration; '
                'attempting to rebuild it with safe defaults'
            )
            needs_rebuild = True
        else:
            return True, False

    if config_dir.is_symlink():
        try:
            config_dir.unlink()
        except OSError as exc:
            log(f'Unable to remove symlinked config directory {config_dir}: {exc}')
            return False, False

    if not _ensure_directory(config_dir):
        return False, False

    try:
        admintools_conf.write_text(DEFAULT_ADMINTOOLS_CONF)
    except OSError as exc:
        log(f'Unable to write default admintools.conf at {admintools_conf}: {exc}')
        return False, False

    _align_identity_with_parent(admintools_conf)

    try:
        os.chmod(admintools_conf, 0o666)
    except OSError as exc:
        log(
            'Unable to relax permissions on '
            f'{admintools_conf}: {exc}'
        )

    _ensure_known_identity(admintools_conf)

    return True, True


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


def _write_container_admintools_conf(container: str, target: str, content: str) -> bool:
    """Write ``content`` to ``target`` inside ``container`` using ``docker exec``."""

    if shutil.which('docker') is None:
        log('Docker CLI is not available while writing admintools.conf inside container')
        return False

    quoted_target = shlex.quote(target)
    parent = os.path.dirname(target) or '/'
    quoted_parent = shlex.quote(parent)

    if not content.endswith('\n'):
        content += '\n'

    heredoc = '__VERTICA_ADMINTOOLS_CONF__'
    script = '\n'.join(
        [
            'set -e',
            f'mkdir -p {quoted_parent}',
            f"cat <<'{heredoc}' > {quoted_target}",
            content,
            heredoc,
            f'chmod 666 {quoted_target} || true',
        ]
    )

    try:
        result = subprocess.run(
            [
                'docker',
                'exec',
                '--user',
                '0',
                container,
                'sh',
                '-c',
                script,
            ],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        log('Docker CLI is not available while writing admintools.conf inside container')
        return False

    if result.stdout:
        log(result.stdout.rstrip())
    if result.stderr:
        log(f'[stderr] {result.stderr.rstrip()}')

    if result.returncode != 0:
        log('Failed to write admintools.conf inside container using exec fallback')
        return False

    log(f'Seeded admintools.conf inside Vertica container at {target} using exec fallback')
    return True


def _container_admintools_conf_targets(host_path: Path) -> list[str]:
    """Return potential container paths for ``host_path``."""

    targets: list[str] = []
    seen: set[str] = set()

    def add(path: str) -> None:
        if path not in seen:
            targets.append(path)
            seen.add(path)

    add(_VERTICA_CONTAINER_ADMINTOOLS_PATH)

    try:
        resolved_host = host_path.resolve(strict=False)
    except OSError:
        resolved_host = host_path

    for host_base, container_bases in _VERTICA_CONTAINER_DATA_DIRECTORY_MAPPINGS.items():
        try:
            resolved_base = host_base.resolve(strict=False)
        except OSError:
            resolved_base = host_base

        try:
            relative = resolved_host.relative_to(resolved_base)
        except ValueError:
            continue

        for container_base in container_bases:
            candidate = Path(container_base) / relative
            add(os.fspath(candidate))

    return targets


def _synchronize_container_admintools_conf(container: str, source: Path) -> bool:
    """Copy host ``admintools.conf`` into ``container`` when possible."""

    if not source.exists():
        return False

    if shutil.which('docker') is None:
        log('Docker CLI is not available while copying admintools.conf into container')
        return False

    container_target = _VERTICA_CONTAINER_ADMINTOOLS_PATH
    container_parent = os.path.dirname(container_target)

    try:
        content = source.read_text()
    except OSError as exc:
        log(f'Unable to read admintools.conf for container synchronization: {exc}')
        return False

    targets = _container_admintools_conf_targets(source)
    overall_success = True

    try:
        with tempfile.TemporaryDirectory() as staging_dir:
            staging_path = Path(staging_dir)
            staged_conf = staging_path / 'admintools.conf'
            shutil.copy2(source, staged_conf)

            for container_target in targets:
                container_parent = os.path.dirname(container_target) or '/'

                try:
                    remove_result = subprocess.run(
                        [
                            'docker',
                            'exec',
                            '--user',
                            '0',
                            container,
                            'rm',
                            '-f',
                            container_target,
                        ],
                        capture_output=True,
                        text=True,
                    )
                except FileNotFoundError:
                    log('Docker CLI is not available while removing admintools.conf inside container before synchronization')
                    return False

                if remove_result.stdout:
                    log(remove_result.stdout.rstrip())
                if remove_result.stderr:
                    log(f'[stderr] {remove_result.stderr.rstrip()}')

                if remove_result.returncode != 0:
                    log(
                        'Failed to remove existing admintools.conf inside container prior to synchronization; '
                        'attempting to continue'
                    )

                try:
                    mkdir_result = subprocess.run(
                        [
                            'docker',
                            'exec',
                            '--user',
                            '0',
                            container,
                            'mkdir',
                            '-p',
                            container_parent,
                        ],
                        capture_output=True,
                        text=True,
                    )
                except FileNotFoundError:
                    log('Docker CLI is not available while preparing admintools.conf directory inside container')
                    return False

                if mkdir_result.returncode != 0:
                    if mkdir_result.stdout:
                        log(mkdir_result.stdout.rstrip())
                    if mkdir_result.stderr:
                        log(f'[stderr] {mkdir_result.stderr.rstrip()}')
                    log(
                        'Failed to prepare admintools.conf directory inside container; '
                        'attempting exec fallback'
                    )
                    if not _write_container_admintools_conf(container, container_target, content):
                        overall_success = False
                    continue

                try:
                    copy_result = subprocess.run(
                        [
                            'docker',
                            'cp',
                            os.fspath(staged_conf),
                            f'{container}:{container_target}',
                        ],
                        capture_output=True,
                        text=True,
                    )
                except FileNotFoundError:
                    log('Docker CLI is not available while copying admintools.conf into container')
                    return False

                if copy_result.stdout:
                    log(copy_result.stdout.rstrip())
                if copy_result.stderr:
                    log(f'[stderr] {copy_result.stderr.rstrip()}')

                if copy_result.returncode != 0:
                    log(
                        'Failed to copy admintools.conf into container via docker cp; '
                        'attempting exec fallback'
                    )
                    if not _write_container_admintools_conf(container, container_target, content):
                        overall_success = False
                    continue

                log(
                    'Copied admintools.conf into Vertica container at '
                    f'{container_target} from host data directory'
                )

                exists_after_copy = _container_path_exists(container, container_target)
                if exists_after_copy is False or exists_after_copy is None:
                    if exists_after_copy is False:
                        log(
                            'admintools.conf still missing inside container after docker cp; '
                            'attempting exec fallback'
                        )
                    else:
                        log(
                            'Unable to verify admintools.conf inside container after docker cp; '
                            'attempting exec fallback'
                        )
                    if not _write_container_admintools_conf(container, container_target, content):
                        overall_success = False
                    continue
    except OSError as exc:
        log(f'Unable to stage admintools.conf for container synchronization: {exc}')
        return False

    return overall_success


def _restart_vertica_container(container: str, reason: str) -> bool:
    """Restart ``container`` when enough time has elapsed since the last restart."""

    global _LAST_VERTICA_CONTAINER_RESTART

    now = time.time()
    if (
        _LAST_VERTICA_CONTAINER_RESTART is not None
        and now - _LAST_VERTICA_CONTAINER_RESTART
        < _VERTICA_CONTAINER_RESTART_THROTTLE_SECONDS
    ):
        elapsed = now - _LAST_VERTICA_CONTAINER_RESTART
        log(
            f'Skipping restart of {container} ({reason}); last restart '
            f'{elapsed:.0f}s ago'
        )
        return False

    log(f'Restarting {container} to {reason}')
    try:
        run_command(['docker', 'restart', container])
    except CommandError as exc:
        log(
            f'Failed to restart {container} to {reason}: '
            f'exit code {exc.returncode}'
        )
        return False

    _LAST_VERTICA_CONTAINER_RESTART = now
    time.sleep(5)
    return True


def _container_path_exists(container: str, path: str) -> Optional[bool]:
    """Return ``True`` when ``path`` exists inside ``container``.

    Returns ``None`` when the Docker CLI is unavailable or the existence check
    fails for an unexpected reason (for example when the container is not
    running).  Callers treat ``None`` as inconclusive so that recovery logic can
    continue gathering evidence before attempting destructive remediation.
    """

    if shutil.which('docker') is None:
        log('Docker CLI is not available while checking container path existence')
        return None

    try:
        result = subprocess.run(
            ['docker', 'exec', container, 'test', '-e', path],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        log('Docker CLI is not available while checking container path existence')
        return None

    if result.returncode == 0:
        return True

    if result.returncode == 1:
        return False

    if result.stdout:
        log(result.stdout.rstrip())
    if result.stderr:
        log(f'[stderr] {result.stderr.rstrip()}')
    log(
        'Unable to determine container path existence because docker exec '
        f'returned exit code {result.returncode}'
    )
    return None


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

        config_path = base_path / 'config'
        try:
            config_exists = config_path.exists() or config_path.is_symlink()
        except OSError as exc:
            log(f'Unable to inspect Vertica config path {config_path}: {exc}')
            config_exists = False

        if config_exists:
            log(f'Removing Vertica configuration directory at {config_path}')
            try:
                if config_path.is_symlink() or config_path.is_file():
                    config_path.unlink()
                else:
                    shutil.rmtree(config_path)
            except FileNotFoundError:
                pass
            except OSError as exc:
                log(f'Unable to remove {config_path}: {exc}')
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


def _container_restart_count(container: str) -> Optional[int]:
    """Return the Docker restart count for ``container`` if available."""

    raw_value = _docker_inspect(container, '{{.RestartCount}}')
    if not raw_value:
        return None

    try:
        return int(raw_value)
    except (TypeError, ValueError):
        return None


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


def _ensure_compose_accepts_eula(compose_file: Path) -> bool:
    """Ensure ``compose_file`` sets the environment variables for EULA acceptance."""

    try:
        original = compose_file.read_text()
    except OSError as exc:
        log(f'Unable to read {compose_file} while ensuring EULA acceptance: {exc}')
        return False

    missing = [
        key
        for key in _EULA_ENVIRONMENT_VARIABLES
        if f'{key}=' not in original
    ]

    if not missing:
        return False

    lines = original.splitlines()
    updated = False

    for index, line in enumerate(lines):
        if line.strip() != 'environment:':
            continue

        indent = line[: len(line) - len(line.lstrip())]
        value_indent = indent + '  '

        insert_position = index + 1
        while insert_position < len(lines) and lines[insert_position].startswith(
            value_indent + '- '
        ):
            insert_position += 1

        new_entries = [
            f"{value_indent}- {key}={_EULA_ENVIRONMENT_VARIABLES[key]}"
            for key in _EULA_ENVIRONMENT_VARIABLES
            if key in missing
        ]

        if not new_entries:
            return False

        lines = lines[:insert_position] + new_entries + lines[insert_position:]
        updated = True
        break

    if not updated:
        log(
            f'Compose file {compose_file} lacks an environment block; unable to insert '
            'EULA acceptance variables automatically'
        )
        return False

    try:
        compose_file.write_text('\n'.join(lines) + '\n')
    except OSError as exc:
        log(f'Unable to update {compose_file} with EULA acceptance variables: {exc}')
        return False

    log(
        'Updated compose file {compose} to include Vertica EULA acceptance variables'.format(
            compose=compose_file
        )
    )
    return True

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


def _remove_stale_vertica_container(*, force: bool = False) -> bool:
    """Attempt to remove a stale ``vertica_ce`` container if present."""

    def _attempt_removal(message: str) -> bool:
        log(message)
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

    if force:
        return _attempt_removal(
            'Force-removing stale Vertica container vertica_ce after compose conflict'
        )

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

    for line in presence_check.stdout.splitlines():
        if not line.strip():
            continue

        try:
            container_id, container_name = line.split('\t', 1)
        except ValueError:
            container_id, container_name = line.strip(), ''

        if container_name.strip() == 'vertica_ce':
            return _attempt_removal(
                'Removing stale Vertica container vertica_ce to resolve docker compose conflict'
            )

    return False


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
                conflict_detected = False
                if isinstance(exc, CommandError):
                    failure_output = (exc.stderr or '') + (exc.stdout or '')
                    conflict_detected = 'is already in use by container' in failure_output

                if not removal_attempted:
                    removed = _remove_stale_vertica_container()
                    if not removed and conflict_detected:
                        removed = _remove_stale_vertica_container(force=True)
                    if removed:
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


def _container_is_responding() -> bool:
    """Return ``True`` when Vertica accepts connections on localhost."""

    try:
        wait_for_port('127.0.0.1', DB_PORT, timeout=5.0)
    except SystemExit:
        return False

    try:
        bootstrap_user, bootstrap_password = _bootstrap_admin_credentials()
    except SystemExit:
        return False

    return connect_and_query(
        'health_override@localhost',
        '127.0.0.1',
        bootstrap_user,
        bootstrap_password,
        attempts=1,
        delay=1.0,
        fatal=False,
    )


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
    last_direct_connect_attempt: Optional[float] = None

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
            _ensure_compose_accepts_eula(compose_file)
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
                    _sanitize_vertica_data_directories()
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
                _sanitize_vertica_data_directories()
                time.sleep(10)
                continue

            if (
                last_direct_connect_attempt is None
                or now - last_direct_connect_attempt >= 60
            ):
                last_direct_connect_attempt = now
                if _container_is_responding():
                    log(
                        'Vertica container health remains unhealthy but direct '
                        'connection succeeded; proceeding despite health check'
                    )
                    return

            uptime = _container_uptime_seconds('vertica_ce')
            if uptime is None:
                log(
                    'Vertica container health reported unhealthy but uptime '
                    'could not be determined; assuming the container is still starting'
                )
                _sanitize_vertica_data_directories()
                time.sleep(10)
                continue
            if uptime is not None and uptime < UNHEALTHY_HEALTHCHECK_GRACE_PERIOD_SECONDS:
                log(
                    'Vertica container health reported unhealthy but uptime '
                    f'{uptime:.0f}s is within grace period; waiting for recovery'
                )
                unhealthy_logged_duration = unhealthy_duration
                _sanitize_vertica_data_directories()
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
                _ensure_compose_accepts_eula(compose_file)
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
                        _ensure_compose_accepts_eula(compose_file)
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
