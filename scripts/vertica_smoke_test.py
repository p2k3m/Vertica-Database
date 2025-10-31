import ast
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
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from typing import NamedTuple, Optional

import vertica_python

DB_NAME = 'VMart'
DB_PORT = 5433
# Limit Vertica client connection attempts so unreachable endpoints do not hang
# the smoke test for extended periods.  Five seconds balances responsiveness
# with allowing transient network hiccups to recover.
VERTICA_CLIENT_CONNECT_TIMEOUT_SECONDS = 5.0
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
    (1000, 1000),
    (VERTICA_ADMIN_FALLBACK_UID, VERTICA_ADMIN_FALLBACK_GID),
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
# The SSM document that invokes this smoke test grants a 30 minute execution
# window. Reserve a small buffer so that the command finishes before the
# Systems Manager plugin times out even when recovery steps consume most of the
# allowance.
SMOKE_TEST_OVERALL_TIMEOUT_SECONDS = 1740.0
SMOKE_TEST_CONTAINER_RESERVE_SECONDS = 300.0
SMOKE_TEST_PORT_RESERVE_SECONDS = 180.0
SMOKE_TEST_MINIMUM_RESERVE_SECONDS = 60.0
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
    'VERTICA_ACCEPT_EULA': 'ACCEPT',
    'VERTICA_EULA': 'ACCEPT',
    'VERTICA_EULA_ACCEPTANCE': 'ACCEPT',
    'VERTICA_EULA_ACCEPTED': '1',
    'ACCEPT_EULA': 'ACCEPT',
    'VERTICA_DB_EULA': 'ACCEPT',
    'VERTICA_DB_EULA_ACCEPTED': '1',
    'VERTICA_LICENSE': 'ACCEPT',
    'VERTICA_LICENSE_ACCEPTED': '1',
    'VERTICA_LICENSE_STATUS': 'ACCEPT',
    'VERTICA_ACCEPT_LICENSE': 'ACCEPT',
    'VERTICA_LICENSE_ACCEPTANCE': 'ACCEPT',
    # Newer Vertica container images ship an updated web-based Management Console
    # agent that enforces its own EULA prompt during startup.  The agent shares
    # the same container as the database which means that an unanswered prompt
    # leaves the Docker health check stuck in an unhealthy state (the agent
    # repeatedly attempts to display the license text and exits).  Accept the
    # additional agreements up-front so the container can progress to a healthy
    # state without requiring interactive acknowledgement.
    'VERTICA_MC_ACCEPT_EULA': 'ACCEPT',
    'VERTICA_MC_EULA': 'ACCEPT',
    'VERTICA_MC_EULA_ACCEPTED': '1',
    'VERTICA_MC_ACCEPT_LICENSE': 'ACCEPT',
    'VERTICA_MC_LICENSE': 'ACCEPT',
    'VERTICA_MC_LICENSE_ACCEPTED': '1',
    'VERTICA_MC_LICENSE_ACCEPTANCE': 'ACCEPT',
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
_CONFIG_COPY_SAME_FILE_PATTERNS: tuple[str, ...] = (
    "cp: '/opt/vertica/config' and '/data/vertica/config' are the same file",
    "cp: '/opt/vertica/config' and '/var/lib/vertica/config' are the same file",
)
_CONFIG_COPY_SAME_FILE_LOG_CACHE: dict[str, tuple[float, bool]] = {}
_CONFIG_COPY_SAME_FILE_LOG_TTL_SECONDS = 30.0
# Track the last recovery attempt for each Vertica configuration directory so we
# can retry if the container continues to report identical source/destination
# paths while avoiding rapid-fire deletions when the copy succeeds.
_VERTICA_CONFIG_SAME_FILE_RECOVERED: dict[Path, float] = {}
_VERTICA_CONFIG_SAME_FILE_RECOVERY_RETRY_SECONDS = 180.0
# Detect repeated Vertica license prompts so the smoke test can inject the
# acceptance environment variables and recreate the container automatically when
# new image versions introduce additional checks.
_EULA_PROMPT_LOG_PATTERNS: tuple[str, ...] = (
    "Output is not a tty --- can't reliably display EULA",
)
_EULA_PROMPT_KEYWORD_SETS: tuple[tuple[str, ...], ...] = (
    ("eula", "accept", "required"),
    ("eula", "acceptance", "required"),
    ("license", "accept", "required"),
    ("eula", "prompt"),
)
_EULA_PROMPT_LOG_CACHE: dict[str, tuple[float, bool]] = {}
_EULA_PROMPT_LOG_TTL_SECONDS = 30.0

# Recent Vertica releases replaced the legacy ``admintools`` license management
# helpers (``list_license`` and ``install_license``) with a consolidated
# ``license`` target that accepts sub-commands.  Continue to recognise the
# historic output that indicated an unknown tool while also treating the newer
# "unknown option" style failures as signals to try an alternate invocation.
_ADMINTOOLS_UNKNOWN_LICENSE_PATTERNS: tuple[str, ...] = (
    'unknown tool',
    'unknown command',
    'unknown option',
    'unrecognized option',
    'unrecognised option',
    'invalid argument',
    'invalid option',
    'unrecognized argument',
    'unrecognised argument',
    'unrecognized arguments',
    'unrecognised arguments',
    'unexpected argument',
    'unexpected option',
    'unexpected arguments',
    'not recognized',
    'not recognised',
    'no such option',
    'no such command',
    'list index out of range',
)

_ADMINTOOLS_FATAL_LICENSE_PATTERNS: tuple[str, ...] = (
    'unhandled exception during admintools operation',
)

_ADMINTOOLS_LICENSE_TARGET_CACHE: dict[str, tuple[float, tuple[str, ...]]] = {}
_ADMINTOOLS_LICENSE_TARGET_CACHE_TTL_SECONDS = 300.0
_ADMINTOOLS_HELP_LICENSE_PATTERN = re.compile(
    r'(?<!\S)([A-Za-z0-9_-]*license[A-Za-z0-9_-]*)',
    re.IGNORECASE,
)

_ADMINTOOLS_LICENSE_HELP_KEYWORDS: dict[str, tuple[str, ...]] = {
    'list': ('list', 'show', 'status', 'display', 'view'),
    'install': ('install', 'add', 'apply', 'update', 'load', 'set', 'deploy'),
}
_ADMINTOOLS_LICENSE_HELP_COMMAND_CACHE: dict[
    tuple[str, str, Optional[str]],
    tuple[float, tuple[str, ...]],
] = {}
_ADMINTOOLS_LICENSE_HELP_COMMAND_CACHE_TTL_SECONDS = 300.0


def _license_option_variants(
    license_path: str, *, include_create_short_flag: bool = False
) -> tuple[str, ...]:
    """Return possible Vertica admintools license flag variants for ``license_path``."""

    quoted = shlex.quote(license_path)

    # The legacy implementation attempted to enumerate every possible spelling of
    # the Vertica license flag which resulted in thousands of candidate
    # combinations once additional admintools targets were considered.  That
    # approach dramatically increased smoke test execution time when admintools
    # rejected each option in turn.  Restrict the recognised spellings to the
    # commonly supported variants so the command matrix remains manageable.
    variants: list[str] = []

    if include_create_short_flag:
        variants.append(f'-l {quoted}')
        variants.append(f'-l={quoted}')

    variants.extend(
        [
            f'-f {quoted}',
            f'--file {quoted}',
            f'--file={quoted}',
            f'--license {quoted}',
            f'--license={quoted}',
            f'--license-file {quoted}',
            f'--license-file={quoted}',
            f'--license_file {quoted}',
            f'--license_file={quoted}',
            f'--license-key {quoted}',
            f'--license-key={quoted}',
            f'--license_key {quoted}',
            f'--license_key={quoted}',
            f'--key {quoted}',
            f'--key={quoted}',
            quoted,
        ]
    )

    # Preserve ordering while removing duplicates.
    return tuple(dict.fromkeys(variants))


class LicenseStatus(NamedTuple):
    """Represents the outcome of a Vertica license installation attempt."""

    installed: bool
    verified: bool


def _parse_admintools_help_for_license_targets(output: str) -> tuple[str, ...]:
    """Extract possible admintools license targets from ``output``."""

    seen: list[str] = []
    for match in _ADMINTOOLS_HELP_LICENSE_PATTERN.finditer(output):
        target = match.group(1).strip()
        if not target or target in seen:
            continue
        seen.append(target)

    return tuple(seen)


def _discover_admintools_license_targets(container: str) -> tuple[str, ...]:
    """Return cached admintools targets that appear related to licensing."""

    now = time.monotonic()
    cached = _ADMINTOOLS_LICENSE_TARGET_CACHE.get(container)
    if cached and now - cached[0] < _ADMINTOOLS_LICENSE_TARGET_CACHE_TTL_SECONDS:
        return cached[1]

    help_scripts = (
        'set -euo pipefail\n/opt/vertica/bin/admintools -t help',
        'set -euo pipefail\n/opt/vertica/bin/admintools --help',
        'set -euo pipefail\n/opt/vertica/bin/admintools -h',
    )

    targets: tuple[str, ...] = ()

    for script in help_scripts:
        result = _docker_exec_prefer_container_admin(
            container,
            ['sh', '-c', script],
            'Docker CLI is not available while probing Vertica admintools help',
        )

        if result is None:
            return ()

        if result.returncode == 0:
            targets = _parse_admintools_help_for_license_targets(result.stdout)
            break

        combined = f"{result.stdout}\n{result.stderr}".lower()
        if not any(
            pattern in combined for pattern in _ADMINTOOLS_UNKNOWN_LICENSE_PATTERNS
        ):
            break

    _ADMINTOOLS_LICENSE_TARGET_CACHE[container] = (now, targets)
    return targets


def _parse_admintools_license_help_actions(output: str) -> dict[str, tuple[str, ...]]:
    """Return help fragments keyed by action extracted from ``output``."""

    if not output:
        return {}

    actions: dict[str, list[str]] = {key: [] for key in _ADMINTOOLS_LICENSE_HELP_KEYWORDS}
    seen: set[str] = set()

    tokens = re.findall(r'(--[A-Za-z0-9][A-Za-z0-9_-]*|[A-Za-z0-9_-]+)', output)

    for token in tokens:
        if not token:
            continue
        lower = token.lower()
        if token in seen:
            continue
        seen.add(token)

        for action, keywords in _ADMINTOOLS_LICENSE_HELP_KEYWORDS.items():
            if any(keyword in lower for keyword in keywords):
                actions[action].append(token)
                break

    return {
        action: tuple(values)
        for action, values in actions.items()
        if values
    }


def _admintools_license_help_action_commands(
    action: str,
    fragments: tuple[str, ...],
    license_path: Optional[str],
) -> tuple[str, ...]:
    """Return admintools invocations derived from help ``fragments``."""

    if not fragments:
        return ()

    commands: list[str] = []
    seen: set[str] = set()

    def _add(command: str) -> None:
        command = command.strip()
        if command and command not in seen:
            seen.add(command)
            commands.append(command)

    base_cli = '/opt/vertica/bin/admintools license'

    if action == 'list':
        for fragment in fragments:
            _add(f'{base_cli} {fragment}')
            if fragment.startswith('--'):
                normalised = fragment.lstrip('-')
                _add(f'{base_cli} --action {normalised}')
            else:
                _add(f'{base_cli} --{fragment}')
                _add(f'{base_cli} --action {fragment}')

        return tuple(commands)

    if action == 'install':
        if license_path is None:
            return ()

        fragments_with_path = _license_option_variants(license_path)
        quoted_path = shlex.quote(license_path)

        for fragment in fragments:
            _add(f'{base_cli} {fragment}')

            for option in fragments_with_path:
                _add(f'{base_cli} {fragment} {option}')
                _add(f'{base_cli} {option} {fragment}')

            if fragment.endswith('='):
                _add(f'{base_cli} {fragment}{quoted_path}')
            elif fragment.startswith('--'):
                _add(f'{base_cli} {fragment} {quoted_path}')
                _add(f'{base_cli} {fragment}={quoted_path}')
            else:
                _add(f'{base_cli} {fragment} {quoted_path}')

        return tuple(commands)

    return ()


def _discover_admintools_license_help_commands(
    container: str,
    action: str,
    license_path: Optional[str],
) -> tuple[str, ...]:
    """Return additional admintools commands discovered from help output."""

    cache_key = (container, action, license_path)
    now = time.monotonic()
    cached = _ADMINTOOLS_LICENSE_HELP_COMMAND_CACHE.get(cache_key)
    if cached and now - cached[0] < _ADMINTOOLS_LICENSE_HELP_COMMAND_CACHE_TTL_SECONDS:
        return cached[1]

    help_scripts = (
        'set -euo pipefail\n/opt/vertica/bin/admintools license --help',
        'set -euo pipefail\n/opt/vertica/bin/admintools -t license --help',
        'set -euo pipefail\n/opt/vertica/bin/admintools --help',
    )

    collected: list[str] = []

    for script in help_scripts:
        result = _docker_exec_prefer_container_admin(
            container,
            ['sh', '-c', script],
            'Docker CLI is not available while probing Vertica admintools license help',
        )

        if result is None:
            return ()

        output = f"{result.stdout}\n{result.stderr}" if result.stdout or result.stderr else ''
        collected.append(output)

        parsed = _parse_admintools_license_help_actions(output)
        fragments = parsed.get(action)
        if fragments:
            commands = _admintools_license_help_action_commands(action, fragments, license_path)
            _ADMINTOOLS_LICENSE_HELP_COMMAND_CACHE[cache_key] = (now, commands)
            return commands

    combined_output = '\n'.join(collected)
    fragments = _parse_admintools_license_help_actions(combined_output).get(action, ())
    commands = _admintools_license_help_action_commands(action, fragments, license_path)
    _ADMINTOOLS_LICENSE_HELP_COMMAND_CACHE[cache_key] = (now, commands)
    return commands


def _license_candidate_sort_key(path: str) -> tuple[int, int, str]:
    """Return a priority tuple that favours genuine Vertica license files."""

    lower = path.lower()

    # Vertica copies the accepted Community Edition license into
    # ``/data/vertica/config`` where the ``dbadmin`` user can always read it.  The
    # smoke test invokes ``admintools`` as ``dbadmin``, so prefer these
    # destinations over the read-only copies in ``/opt``.
    if lower.startswith('/data/vertica/config/'):
        priority = 0
    # Prefer explicitly known Vertica Community Edition license paths regardless
    # of discovery order.  These locations historically stored the bundled CE
    # license even as new container images shuffled auxiliary directories.
    elif path in _KNOWN_LICENSE_PATH_CANDIDATES:
        priority = 1
    # Next, prioritise files that reside in Vertica's dedicated ``license``
    # directories to avoid unrelated third-party ``LICENSE`` documents that also
    # live under ``/opt/vertica`` (for example, Python package metadata).
    elif '/share/license/' in lower or '/config/license' in lower:
        priority = 2
    # Explicit Vertica-specific filenames (``*.license``/``*.lic``/``*.dat``/``*.key``)
    # are stronger signals than generic text documents.
    elif lower.endswith(('.license', '.lic', '.dat', '.key')):
        priority = 3
    # Any remaining candidates that still contain ``vertica`` in the path are
    # more plausible than unrelated system licenses.
    elif 'vertica' in lower:
        priority = 4
    else:
        priority = 5

    # Vertica ships the actual license material as ``*.key`` files.  Prefer
    # these before ``*.dat`` (which can be placeholders) and finally any other
    # extension so we surface genuine license data when retrying ``create_db``
    # with the ``--license`` option.
    if lower.endswith('.key'):
        extension_priority = 0
    elif lower.endswith('.dat'):
        extension_priority = 1
    elif lower.endswith(('.license', '.lic')):
        extension_priority = 2
    else:
        extension_priority = 3

    # Within each bucket prefer shorter paths to stabilise ordering while still
    # considering the raw path as a final tiebreaker.
    return (priority, extension_priority, len(path), lower)
# Some Vertica container revisions no longer expose dedicated admintools license
# sub-commands and instead expect callers to supply the bundled Community
# Edition license file directly to ``create_db``.  Include a set of known
# absolute paths that historically stored the community license so the smoke
# test can still discover it even when ``find`` misses the location (for
# example, due to deeply nested directory structures or symlinks).
_KNOWN_LICENSE_PATH_CANDIDATES: tuple[str, ...] = (
    '/data/vertica/config/license.dat',
    '/data/vertica/config/license.key',
    '/opt/vertica/config/license.dat',
    '/opt/vertica/config/license.key',
    '/opt/vertica/config/share/license.dat',
    '/opt/vertica/config/share/license.key',
    '/opt/vertica/config/share/license/license.dat',
    '/opt/vertica/config/share/license/license.key',
    '/opt/vertica/config/share/license/VerticaCE_AWS.license.key',
    '/opt/vertica/config/share/license/Vertica_CE.license.key',
    '/opt/vertica/config/share/license/Vertica_Community_Edition.license.key',
    '/opt/vertica/share/license.dat',
    '/opt/vertica/share/license.key',
    '/opt/vertica/share/license/vertica.license',
    '/opt/vertica/share/license/Vertica_CE.license.key',
)

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
_CONTAINER_EXEC_USER_CACHE: dict[str, str] = {}


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
        include_pair: Optional[tuple[int, int]] = None
        try:
            entry = pwd.getpwuid(uid)
        except KeyError:
            if container_identity == (uid, gid):
                include_pair = container_identity
            else:
                include_pair = (uid, gid)
        except OSError as exc:
            log(
                'Unable to resolve fallback Vertica admin identity '
                f'uid {uid}: {exc}'
            )
            include_pair = (uid, gid)
        else:
            include_pair = (entry.pw_uid, entry.pw_gid)

        if include_pair and include_pair not in seen:
            candidates.append(include_pair)
            seen.add(include_pair)

    return candidates


def _discover_existing_vertica_admin_identities(
    *, max_depth: int = 3
) -> list[tuple[int, int]]:
    """Return uid/gid pairs observed within existing Vertica data directories."""

    counts: Counter[tuple[int, int]] = Counter()
    visited: set[Path] = set()

    for base in VERTICA_DATA_DIRECTORIES:
        if not base.exists():
            continue

        stack: list[tuple[Path, int]] = [(base, 0)]

        while stack:
            current, depth = stack.pop()
            if current in visited:
                continue
            visited.add(current)

            try:
                stat_info = os.stat(current, follow_symlinks=False)
            except OSError:
                continue

            identity = (stat_info.st_uid, stat_info.st_gid)
            counts[identity] += 1

            if depth >= max_depth:
                continue

            try:
                is_dir = current.is_dir()
            except OSError:
                continue

            if not is_dir:
                continue

            try:
                with os.scandir(current) as entries:
                    for entry in entries:
                        child_path = Path(entry.path)
                        try:
                            child_stat = os.stat(entry.path, follow_symlinks=False)
                        except OSError:
                            continue

                        child_identity = (child_stat.st_uid, child_stat.st_gid)
                        counts[child_identity] += 1

                        if depth + 1 <= max_depth:
                            try:
                                is_child_dir = entry.is_dir(follow_symlinks=False)
                            except TypeError:
                                is_child_dir = entry.is_dir()
                            except OSError:
                                continue

                            if is_child_dir:
                                stack.append((child_path, depth + 1))
            except OSError:
                continue

    root_identity = (0, 0)
    host_identity = (os.geteuid(), os.getegid())
    ignored = {root_identity, host_identity}

    identities: list[tuple[int, int]] = []
    for identity, _ in counts.most_common():
        if identity in ignored:
            continue
        identities.append(identity)

    return identities


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

    candidates: list[tuple[int, int]] = []
    seen_candidates: set[tuple[int, int]] = set()

    for identity in _discover_existing_vertica_admin_identities():
        if identity not in seen_candidates:
            candidates.append(identity)
            seen_candidates.add(identity)

    for identity in _vertica_admin_identity_candidates():
        if identity not in seen_candidates:
            candidates.append(identity)
            seen_candidates.add(identity)

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

# ``DEFAULT_ADMINTOOLS_CONF`` acts as a fallback when the Vertica image is not
# available or ``docker`` cannot be invoked.  When possible the smoke test loads
# the template directly from the container image so that future Vertica
# releases, which may ship updated defaults, continue to work without requiring
# code changes.
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

_DEFAULT_ADMINTOOLS_CONF_CACHE: Optional[str] = None


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

    same_file_issue_detected = _container_reports_config_same_file_issue('vertica_ce')

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

            now = time.time()

            try:
                config_exists = config_path.exists()
            except OSError:
                config_exists = False

            try:
                config_is_symlink = config_path.is_symlink()
            except OSError:
                config_is_symlink = False

            if not config_exists and not config_is_symlink:
                _VERTICA_CONFIG_SAME_FILE_RECOVERED.pop(config_path, None)

            last_same_file_recovery = _VERTICA_CONFIG_SAME_FILE_RECOVERED.get(config_path)
            allow_same_file_recovery = True
            if last_same_file_recovery is not None:
                if now - last_same_file_recovery < _VERTICA_CONFIG_SAME_FILE_RECOVERY_RETRY_SECONDS:
                    allow_same_file_recovery = False

            if same_file_issue_detected and allow_same_file_recovery:
                if config_exists or config_is_symlink:
                    log(
                        'Detected identical Vertica configuration source and '
                        f'destination paths; ensuring persisted configuration at {config_path} contains defaults'
                    )

                    if config_is_symlink:
                        try:
                            config_path.unlink()
                        except OSError as exc:
                            log(f'Unable to remove symlinked {config_path}: {exc}')
                            continue

                    try:
                        if config_path.exists() and not config_path.is_dir():
                            config_path.unlink()
                    except OSError as exc:
                        log(f'Unable to remove unexpected entry at {config_path}: {exc}')
                        continue

                    if not _ensure_directory(config_path):
                        continue

                    _OBSERVED_VERTICA_CONFIG_DIRECTORIES.add(config_path)
                    _ensure_known_identity_tree(config_path, max_depth=2)

                    admintools_conf = config_path / 'admintools.conf'
                    seed_success, _ = _seed_default_admintools_conf(config_path)
                    if seed_success:
                        _ADMINTOOLS_CONF_SEEDED_AT[config_path] = now
                        _ADMINTOOLS_CONF_MISSING_OBSERVED_AT.pop(config_path, None)
                        _ensure_known_identity_tree(config_path, max_depth=2)
                        _synchronize_container_admintools_conf(
                            'vertica_ce', admintools_conf
                        )
                    else:
                        log(
                            'Failed to seed default admintools.conf while recovering identical configuration paths; '
                            'deferring retry'
                        )
                        _ADMINTOOLS_CONF_SEEDED_AT.pop(config_path, None)

                    _VERTICA_CONFIG_SAME_FILE_RECOVERED[config_path] = now
                    _restart_vertica_container(
                        'vertica_ce', 'apply recovered configuration defaults'
                    )
                    continue

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
            current_time = now

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
                    # Remember when we first noticed the configuration directory
                    # was missing so the grace-period timers can elapse.  The
                    # previous implementation cleared the timestamp on every
                    # pass which meant the recovery logic never accumulated
                    # enough time to trigger.  This left environments stuck in
                    # a bootstrap loop waiting for Vertica to populate
                    # ``admintools.conf`` while repeatedly resetting the
                    # observation window.
                    _ADMINTOOLS_CONF_MISSING_OBSERVED_AT.setdefault(
                        config_path, time.time()
                    )
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


def _resolve_vertica_image_name() -> Optional[str]:
    """Return the Vertica container image name when available."""

    if shutil.which('docker') is None:
        return None

    try:
        inspect_result = subprocess.run(
            ['docker', 'inspect', '--format', '{{.Config.Image}}', 'vertica_ce'],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        log('Docker CLI is not available while resolving Vertica image name')
        inspect_result = None
    else:
        if inspect_result.returncode == 0:
            value = inspect_result.stdout.strip()
            if value:
                return value

    compose_file = _compose_file()
    if compose_file is not None:
        image_name = _extract_compose_image(compose_file)
        if image_name:
            return image_name

    return None


def _image_default_admintools_conf() -> Optional[str]:
    """Attempt to read ``admintools.conf`` from the Vertica container image."""

    image_name = _resolve_vertica_image_name()
    if not image_name:
        return None

    if shutil.which('docker') is None:
        return None

    try:
        _ensure_ecr_login_for_image(image_name)
    except SystemExit as exc:
        detail = exc.code
        log(
            'Unable to authenticate with registry for Vertica image '
            f'{image_name} while extracting admintools.conf template; '
            'falling back to bundled defaults'
        )
        if detail not in (None, 0):
            log(f'[detail] {detail}')
        return None

    search_paths = [
        '/opt/vertica/config/admintools.conf',
        '/opt/vertica/config/admintools/admintools.conf',
        '/opt/vertica/share/admintools/admintools.conf',
        '/opt/vertica/share/admintools.conf',
        '/opt/vertica/share/config/admintools.conf',
    ]

    search_script_lines = [
        'for path in "$@"; do',
        '  if [ -f "$path" ]; then',
        '    cat "$path"',
        '    exit 0',
        '  fi',
        'done',
        'if command -v find >/dev/null 2>&1; then',
        "  candidate=$(find /opt/vertica -maxdepth 6 -type f -name admintools.conf 2>/dev/null | head -n 1)",
        '  if [ -n "$candidate" ]; then',
        '    cat "$candidate"',
        '    exit 0',
        '  fi',
        'fi',
        'exit 1',
    ]

    try:
        result = subprocess.run(
            [
                'docker',
                'run',
                '--rm',
                '--entrypoint',
                '/bin/sh',
                image_name,
                '-c',
                '\n'.join(search_script_lines),
                '--',
                *search_paths,
            ],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        log('Docker CLI is not available while extracting admintools.conf template from image')
        return None

    if result.returncode != 0:
        if result.stderr:
            log(f'[stderr] {result.stderr.rstrip()}')
        log(
            'Failed to extract admintools.conf template from Vertica image '
            f'{image_name}: exit code {result.returncode}'
        )
        return None

    content = result.stdout
    if not content:
        log(
            'Vertica image {image} returned an empty admintools.conf template; '
            'falling back to bundled defaults'.format(image=image_name)
        )
        return None

    return content


def _load_default_admintools_conf() -> str:
    """Return the best available ``admintools.conf`` template."""

    global _DEFAULT_ADMINTOOLS_CONF_CACHE

    if _DEFAULT_ADMINTOOLS_CONF_CACHE is not None:
        return _DEFAULT_ADMINTOOLS_CONF_CACHE

    template = _image_default_admintools_conf()
    if template:
        if not template.endswith('\n'):
            template += '\n'
        _DEFAULT_ADMINTOOLS_CONF_CACHE = template
        return template

    _DEFAULT_ADMINTOOLS_CONF_CACHE = DEFAULT_ADMINTOOLS_CONF
    return _DEFAULT_ADMINTOOLS_CONF_CACHE


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
        admintools_conf.write_text(_load_default_admintools_conf())
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
                    except TypeError:
                        is_dir = entry.is_dir()
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


def _preferred_container_admin_user(container: str) -> str:
    """Return the preferred ``docker exec`` user for Vertica admin operations."""

    cached_user = _CONTAINER_EXEC_USER_CACHE.get(container)
    if cached_user:
        return cached_user

    identity = _container_dbadmin_identity(container)
    user = 'dbadmin' if identity is not None else '0'
    _CONTAINER_EXEC_USER_CACHE[container] = user
    return user


def _docker_exec_prefer_container_admin(
    container: str,
    command: list[str],
    missing_cli_message: str,
    *,
    allow_root_fallback: bool = True,
) -> Optional[subprocess.CompletedProcess[str]]:
    """Run ``command`` inside ``container`` preferring the Vertica admin user."""

    preferred_user = _preferred_container_admin_user(container)
    users_to_try: list[str] = [preferred_user]
    if allow_root_fallback and preferred_user != '0':
        users_to_try.append('0')

    last_result: Optional[subprocess.CompletedProcess[str]] = None

    for index, user in enumerate(users_to_try):
        try:
            result = subprocess.run(
                ['docker', 'exec', '--user', user, container, *command],
                capture_output=True,
                text=True,
            )
        except FileNotFoundError:
            log(missing_cli_message)
            return None

        if result.stdout:
            log(result.stdout.rstrip())
        if result.stderr:
            log(f'[stderr] {result.stderr.rstrip()}')

        last_result = result

        if result.returncode == 0 or index == len(users_to_try) - 1:
            return result

        log(
            'Container command failed with exit code '
            f"{result.returncode} when run as {user}; retrying as {users_to_try[index + 1]}"
        )

    return last_result


def _docker_exec_root_shell(
    container: str,
    script: str,
    missing_cli_message: str,
) -> Optional[subprocess.CompletedProcess[str]]:
    """Run ``script`` inside ``container`` as ``root`` using ``/bin/sh``."""

    try:
        result = subprocess.run(
            ['docker', 'exec', '--user', '0', container, 'sh', '-c', script],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        log(missing_cli_message)
        return None

    if result.stdout:
        log(result.stdout.rstrip())
    if result.stderr:
        log(f'[stderr] {result.stderr.rstrip()}')

    return result


def _admintools_license_target_commands(
    target: str,
    action: str,
    license_path: Optional[str],
) -> tuple[str, ...]:
    """Return admintools invocations for ``target`` handling ``action``."""

    bases = (
        f'/opt/vertica/bin/admintools -t {target}',
        f'/opt/vertica/bin/admintools {target}',
    )

    if action == 'list':
        commands: list[str] = []
        for base in bases:
            commands.append(f'{base} -k list')
            commands.append(f'{base} --list')
            commands.append(f'{base} --action list')
            commands.append(f'{base} list')

        return tuple(dict.fromkeys(commands))

    if action == 'install':
        if license_path is None:
            raise ValueError('license_path must be provided for install action')

        commands: list[str] = []
        for base in bases:
            for fragment in _license_option_variants(license_path):
                commands.append(f'{base} -k install {fragment}')
                commands.append(f'{base} --install {fragment}')
                commands.append(f'{base} --action install {fragment}')
                commands.append(f'{base} install {fragment}')

        return tuple(dict.fromkeys(commands))

    raise ValueError(f'Unsupported admintools license action: {action}')


def _admintools_license_command_variants(
    action: str,
    *,
    license_path: Optional[str] = None,
    extra_targets: tuple[str, ...] = (),
) -> tuple[str, ...]:
    """Return potential admintools invocations for ``action``.

    ``action`` should be either ``'list'`` or ``'install'``.  Newer Vertica
    releases route license management through ``admintools`` targets that expose
    sub-commands while older releases continue to provide dedicated helper
    targets.  Include both styles so callers can transparently fall back without
    needing to duplicate this command construction logic.  ``extra_targets``
    allows callers to append dynamically discovered target names.
    """

    known_targets = (
        'license',
        'db_license',
        'manage_license',
        'license_manager',
        'license-manager',
    )

    commands: list[str] = []

    base_cli = '/opt/vertica/bin/admintools'

    if action == 'list':
        commands.extend(
            (
                f'{base_cli} -t list_license',
                f'{base_cli} license --list',
                f'{base_cli} license --action list',
                f'{base_cli} license list',
            )
        )
    elif action == 'install':
        if license_path is None:
            raise ValueError('license_path must be provided for install action')
        fragments = _license_option_variants(license_path)
        commands.extend(
            f'{base_cli} -t install_license {fragment}' for fragment in fragments
        )
        commands.extend(
            f'{base_cli} license --install {fragment}' for fragment in fragments
        )
        commands.extend(
            f'{base_cli} license --action install {fragment}' for fragment in fragments
        )
        commands.extend(
            f'{base_cli} license install {fragment}' for fragment in fragments
        )
    else:
        raise ValueError(f'Unsupported admintools license action: {action}')

    for target in (*known_targets, *extra_targets):
        commands.extend(
            _admintools_license_target_commands(target, action, license_path)
        )

    seen: list[str] = []
    for command in commands:
        if command not in seen:
            seen.append(command)

    return tuple(seen)


def _run_admintools_license_command(
    container: str,
    commands: tuple[str, ...],
    missing_cli_message: str,
    *,
    allow_root_fallback: bool = True,
    action: Optional[str] = None,
    license_path: Optional[str] = None,
) -> Optional[subprocess.CompletedProcess[str]]:
    """Execute possible admintools license commands until one succeeds.

    Returns the first ``subprocess.CompletedProcess`` whose exit status is zero
    or whose failure does not resemble an "unknown command" style response.  If
    every variant fails with an unknown-command pattern the last result is
    returned so callers can surface a helpful log message.
    """

    commands_to_try: list[str] = list(commands)
    attempted: set[str] = set()
    last_result: Optional[subprocess.CompletedProcess[str]] = None
    unknown_tool_encountered = False
    extra_targets_added = False
    help_commands_added = False

    index = 0

    while True:
        while index < len(commands_to_try):
            command = commands_to_try[index]
            index += 1

            if command in attempted:
                continue
            attempted.add(command)

            result = _docker_exec_prefer_container_admin(
                container,
                ['sh', '-c', command],
                missing_cli_message,
                allow_root_fallback=allow_root_fallback,
            )

            if result is None:
                return None

            last_result = result

            if result.returncode == 0:
                return result

            combined = f"{result.stdout}\n{result.stderr}".lower()
            fatal = any(
                pattern in combined for pattern in _ADMINTOOLS_FATAL_LICENSE_PATTERNS
            )
            unknown = any(
                pattern in combined for pattern in _ADMINTOOLS_UNKNOWN_LICENSE_PATTERNS
            )

            if fatal:
                if 'list index out of range' in combined or not unknown:
                    return result

            if not unknown:
                return result

            unknown_tool_encountered = True

        if not unknown_tool_encountered or action is None:
            return last_result

        unknown_tool_encountered = False
        expanded = False

        if not extra_targets_added:
            extra_targets_added = True
            extra_targets = _discover_admintools_license_targets(container)
            if extra_targets:
                extended_commands = _admintools_license_command_variants(
                    action,
                    license_path=license_path,
                    extra_targets=extra_targets,
                )
                for extra_command in extended_commands:
                    if extra_command not in attempted and extra_command not in commands_to_try:
                        commands_to_try.append(extra_command)
                        expanded = True

        if not expanded and not help_commands_added:
            help_commands_added = True
            help_commands = _discover_admintools_license_help_commands(
                container,
                action,
                license_path,
            )
            for extra_command in help_commands:
                if extra_command not in attempted and extra_command not in commands_to_try:
                    commands_to_try.append(extra_command)
                    expanded = True

        if not expanded:
            return last_result


def _align_container_path_identity(
    container: str,
    path: str,
    friendly_name: str,
    *,
    context: str = 'admintools.conf',
) -> tuple[bool, bool]:
    """Attempt to align ``path`` ownership with ``dbadmin`` inside ``container``.

    Returns a tuple ``(success, adjusted)`` where ``success`` indicates the
    alignment check completed without errors and ``adjusted`` is ``True`` when
    ownership changes were applied.
    """

    if shutil.which('docker') is None:
        return False, False

    quoted_path = shlex.quote(path)

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

    target_identity = _container_dbadmin_identity(container)
    require_named_chown = target_identity is None
    adjusted = False

    if target_identity is not None:
        uid, gid = target_identity
        owner_result = _docker_exec(
            '0',
            f'stat -c "%u:%g" {quoted_path}',
            'Docker CLI is not available while '
            f'inspecting {context} ownership inside container',
        )
        if owner_result is None:
            return False, adjusted
        if owner_result.returncode == 0:
            owner_output = owner_result.stdout.strip()
            if owner_output:
                try:
                    current_uid_str, current_gid_str = owner_output.split(':', 1)
                    current_identity = (int(current_uid_str), int(current_gid_str))
                except ValueError:
                    current_identity = None
                    log(
                        'Unexpected ownership output for '
                        f'{context} inside container: {owner_output!r}'
                    )
                else:
                    if current_identity == target_identity:
                        return True, adjusted
            else:
                require_named_chown = True
        else:
            require_named_chown = True

        chown_result = _docker_exec(
            '0',
            f'chown {uid}:{gid} {quoted_path}',
            f'Docker CLI is not available while adjusting {friendly_name} inside container',
        )
        if chown_result is None:
            return False, adjusted
        if chown_result.stdout:
            log(chown_result.stdout.rstrip())
        if chown_result.stderr:
            log(f'[stderr] {chown_result.stderr.rstrip()}')
        if chown_result.returncode == 0:
            log(
                f'Aligned {friendly_name} inside container with dbadmin '
                f'(uid {uid} gid {gid})'
            )
            adjusted = True
        else:
            log(f'Failed to adjust {friendly_name} inside container')
            require_named_chown = True

    if require_named_chown:
        chown_result = _docker_exec(
            '0',
            f'chown dbadmin:dbadmin {quoted_path}',
            f'Docker CLI is not available while aligning {friendly_name} inside container',
        )
        if chown_result is None:
            return False, adjusted
        if chown_result.stdout:
            log(chown_result.stdout.rstrip())
        if chown_result.stderr:
            log(f'[stderr] {chown_result.stderr.rstrip()}')
        if chown_result.returncode == 0:
            log(f'Aligned {friendly_name} inside container with dbadmin account')
            adjusted = True
        else:
            log(f'Failed to align {friendly_name} inside container using dbadmin account')
            return False, adjusted

    return True, adjusted


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

    success, adjustments_made = _align_container_path_identity(
        container,
        _VERTICA_CONTAINER_ADMINTOOLS_PATH,
        'admintools.conf ownership',
    )
    if not success:
        return adjustments_made

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
        return adjustments_made

    directory_result = _docker_exec(
        '0',
        f'chmod a+rx {shlex.quote(os.path.dirname(_VERTICA_CONTAINER_ADMINTOOLS_PATH))}',
        'Docker CLI is not available while adjusting admintools.conf directory permissions inside container',
    )
    if directory_result is None:
        return adjustments_made

    if directory_result.stdout:
        log(directory_result.stdout.rstrip())
    if directory_result.stderr:
        log(f'[stderr] {directory_result.stderr.rstrip()}')

    if directory_result.returncode != 0:
        log('Failed to adjust admintools.conf directory permissions inside container')
        return adjustments_made

    readable_result = _docker_exec(
        'dbadmin',
        f'test -r {quoted_path}',
        'Docker CLI is not available while validating admintools.conf permissions inside container',
    )
    if readable_result is None:
        return adjustments_made

    if readable_result.returncode != 0:
        log('Unable to verify admintools.conf readability inside container after permission adjustments')

    return adjustments_made


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
            'set -euo pipefail',
            f'if [ -e {quoted_parent} ] && [ ! -d {quoted_parent} ]; then rm -rf {quoted_parent}; fi',
            f'mkdir -p {quoted_parent}',
            f"cat <<'{heredoc}' > {quoted_target}",
            content,
            heredoc,
            f'chmod 666 {quoted_target} || true',
        ]
    )

    result = _docker_exec_prefer_container_admin(
        container,
        ['sh', '-c', script],
        'Docker CLI is not available while writing admintools.conf inside container',
    )
    if result is None:
        return False

    if result.returncode != 0:
        log('Failed to write admintools.conf inside container using exec fallback')
        return False

    log(f'Seeded admintools.conf inside Vertica container at {target} using exec fallback')

    success, _ = _align_container_path_identity(
        container,
        target,
        'admintools.conf ownership',
    )
    if not success:
        return False

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

                remove_result = _docker_exec_prefer_container_admin(
                    container,
                    ['rm', '-f', container_target],
                    'Docker CLI is not available while removing admintools.conf inside container before synchronization',
                )
                if remove_result is None:
                    return False

                if remove_result.returncode != 0:
                    log(
                        'Failed to remove existing admintools.conf inside container prior to synchronization; '
                        'attempting to continue'
                    )

                mkdir_result = _docker_exec_prefer_container_admin(
                    container,
                    ['mkdir', '-p', container_parent],
                    'Docker CLI is not available while preparing admintools.conf directory inside container',
                )
                if mkdir_result is None:
                    return False

                if mkdir_result.returncode != 0:
                    mkdir_stdout = mkdir_result.stdout.rstrip() if mkdir_result.stdout else ''
                    mkdir_stderr = mkdir_result.stderr.rstrip() if mkdir_result.stderr else ''
                    if mkdir_stdout:
                        log(mkdir_stdout)
                    if mkdir_stderr:
                        log(f'[stderr] {mkdir_stderr}')

                    retried_mkdir = False
                    if 'File exists' in mkdir_stderr or 'Not a directory' in mkdir_stderr:
                        log(
                            'Detected non-directory entry for admintools.conf parent inside container; '
                            'attempting to rebuild directory'
                        )
                        remove_result = _docker_exec_prefer_container_admin(
                            container,
                            ['rm', '-rf', container_parent],
                            'Docker CLI is not available while removing existing admintools.conf parent inside container',
                        )
                        if remove_result is None:
                            return False

                        if remove_result.returncode == 0:
                            mkdir_retry = _docker_exec_prefer_container_admin(
                                container,
                                ['mkdir', '-p', container_parent],
                                'Docker CLI is not available while preparing admintools.conf directory inside container',
                            )
                            if mkdir_retry is None:
                                return False

                            retried_mkdir = True
                            if mkdir_retry.returncode == 0:
                                log(
                                    'Rebuilt admintools.conf directory inside container after removing '
                                    'conflicting entry'
                                )
                                mkdir_result = mkdir_retry

                    if mkdir_result.returncode != 0:
                        if not retried_mkdir:
                            log(
                                'Failed to prepare admintools.conf directory inside container; '
                                'attempting exec fallback'
                            )
                        else:
                            log(
                                'Failed to prepare admintools.conf directory inside container after '
                                'rebuilding conflicting entry; attempting exec fallback'
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

                success, _ = _align_container_path_identity(
                    container,
                    container_target,
                    'admintools.conf ownership',
                )
                if not success:
                    overall_success = False
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


def _container_reports_config_same_file_issue(container: str) -> bool:
    """Return ``True`` when container logs report identical config paths."""

    now = time.time()
    cached = _CONFIG_COPY_SAME_FILE_LOG_CACHE.get(container)
    if cached and now - cached[0] < _CONFIG_COPY_SAME_FILE_LOG_TTL_SECONDS:
        return cached[1]

    detected = False

    if shutil.which('docker') is not None:
        try:
            result = subprocess.run(
                ['docker', 'logs', '--tail', '200', container],
                capture_output=True,
                text=True,
            )
        except FileNotFoundError:
            detected = False
        else:
            output_parts = []
            if result.stdout:
                output_parts.append(result.stdout)
            if result.stderr:
                output_parts.append(result.stderr)
            if output_parts:
                combined = '\n'.join(part.rstrip() for part in output_parts)
                detected = any(
                    pattern in combined for pattern in _CONFIG_COPY_SAME_FILE_PATTERNS
                )
    _CONFIG_COPY_SAME_FILE_LOG_CACHE[container] = (now, detected)
    return detected


def _log_indicates_eula_prompt(message: str) -> bool:
    """Return ``True`` when ``message`` suggests an unattended EULA prompt."""

    if any(pattern in message for pattern in _EULA_PROMPT_LOG_PATTERNS):
        return True

    lowered = message.lower()
    return any(
        all(keyword in lowered for keyword in keywords)
        for keywords in _EULA_PROMPT_KEYWORD_SETS
    )


def _container_reports_eula_prompt(container: str) -> bool:
    """Return ``True`` when Vertica logs show an unattended EULA prompt."""

    now = time.time()
    cached = _EULA_PROMPT_LOG_CACHE.get(container)
    if cached and now - cached[0] < _EULA_PROMPT_LOG_TTL_SECONDS:
        return cached[1]

    detected = False

    if shutil.which('docker') is not None:
        try:
            result = subprocess.run(
                ['docker', 'logs', '--tail', '200', container],
                capture_output=True,
                text=True,
            )
        except FileNotFoundError:
            detected = False
        else:
            output_parts = []
            if result.stdout:
                output_parts.append(result.stdout)
            if result.stderr:
                output_parts.append(result.stderr)
            if output_parts:
                combined = '\n'.join(part.rstrip() for part in output_parts)
                detected = _log_indicates_eula_prompt(combined)

    _EULA_PROMPT_LOG_CACHE[container] = (now, detected)
    return detected


def _detect_container_python_executable(container: str) -> Optional[str]:
    """Return the Python executable available inside ``container`` if any."""

    if shutil.which('docker') is None:
        return None

    candidates = [
        '/opt/vertica/oss/python3/bin/python3',
        '/opt/vertica/bin/python3',
    ]

    for candidate in candidates:
        try:
            result = subprocess.run(
                ['docker', 'exec', container, 'test', '-x', candidate],
                capture_output=True,
                text=True,
            )
        except FileNotFoundError:
            return None

        if result.returncode == 0:
            return candidate

    try:
        which_result = subprocess.run(
            ['docker', 'exec', container, 'which', 'python3'],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        return None

    if which_result.returncode == 0:
        for line in which_result.stdout.splitlines():
            candidate = line.strip()
            if candidate:
                return candidate

    return None


def _accept_vertica_eula(container: str = 'vertica_ce') -> bool:
    """Attempt to record Vertica EULA acceptance inside ``container``."""

    if shutil.which('docker') is None:
        log('Docker CLI is unavailable while attempting to accept Vertica EULA')
        return False

    python_exec = _detect_container_python_executable(container)
    if not python_exec:
        log(
            'Unable to determine Python interpreter inside Vertica container while '
            'attempting to record EULA acceptance'
        )
        return False

    script = (
        'import vertica.shared.logging; '
        'import vertica.tools.eula_checker; '
        'vertica.shared.logging.setup_admintool_logging(); '
        'vertica.tools.eula_checker.EulaChecker().write_acceptance()'
    )

    try:
        result = subprocess.run(
            ['docker', 'exec', container, python_exec, '-c', script],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        log('Docker CLI is unavailable while attempting to accept Vertica EULA')
        return False

    if result.stdout:
        log(result.stdout.rstrip())
    if result.stderr:
        log(f'[stderr] {result.stderr.rstrip()}')

    if result.returncode != 0:
        log(
            'Failed to record Vertica EULA acceptance inside container '
            f'{container}: exit code {result.returncode}'
        )
        return False

    log(f'Recorded Vertica EULA acceptance within container {container}')
    return True


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


def _health_log_indicates_missing_database(
    entries: list[dict[str, object]], database: str
) -> bool:
    """Return ``True`` when health log output references a missing database."""

    if not entries:
        return False

    database_token = f'database {database}'.lower()

    for entry in entries:
        output = entry.get('Output')
        if not output:
            continue
        normalized = str(output).lower()
        if database_token in normalized and 'not defined' in normalized:
            return True

    return False


def _container_logs_indicate_missing_database(
    container: str, database: str, tail: int = 200
) -> bool:
    """Return ``True`` when container logs reference a missing database."""

    try:
        result = subprocess.run(
            ['docker', 'logs', '--tail', str(tail), container],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        return False

    if result.returncode != 0:
        return False

    database_token = f'database {database}'.lower()
    combined_output = '\n'.join(filter(None, [result.stdout, result.stderr]))
    if not combined_output:
        return False

    normalized = combined_output.lower()
    return database_token in normalized and 'not defined' in normalized


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


def _discover_container_license_files(container: str) -> list[str]:
    """Return potential Vertica license paths inside ``container``."""

    search_script = r"""
search_roots="/opt/vertica /opt/vertica/config /opt/vertica/config/share /opt/vertica/share /opt/vertica/packages /data/vertica /data/vertica/config"
for dir in $search_roots; do
  if [ -d "$dir" ]; then
    find "$dir" -maxdepth 8 -type f \
      \( -iname '*license*.dat' -o -iname '*license*.lic' -o -iname '*license*.txt' \
         -o -iname '*license*.key' -o -iname '*license*.xml' -o -iname '*license*.json' \
         -o -iname '*license*.cfg' -o -iname '*license*.conf' \
         -o -iname '*eula*.txt' -o -iname '*eula*.lic' -o -iname '*eula*.dat' \) \
      -print 2>/dev/null
  fi
done

for pattern in /opt/vertica/*.lic /opt/vertica/*.dat /opt/vertica/*.key \
               /opt/vertica/config/*.lic /opt/vertica/config/*.dat /opt/vertica/config/*.key \
               /opt/vertica/config/share/license/* /opt/vertica/share/license/*; do
  for candidate in $pattern; do
    if [ -f "$candidate" ]; then
      printf '%s\n' "$candidate"
    fi
  done
done

for candidate in "${VERTICA_DB_LICENSE:-}" "${VERTICA_LICENSE_FILE:-}" "${VERTICA_LICENSE_PATH:-}"; do
  if [ -n "$candidate" ] && [ -f "$candidate" ]; then
    printf '%s\n' "$candidate"
  fi
done
"""

    result = _docker_exec_prefer_container_admin(
        container,
        ['sh', '-c', search_script],
        'Docker CLI is not available while discovering Vertica licenses',
    )

    if result is None:
        return []

    candidates: list[str] = []
    seen: set[str] = set()
    for line in result.stdout.splitlines():
        normalized = line.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        candidates.append(normalized)

    if shutil.which('docker') is not None:
        missing_cli_message = 'Docker CLI is not available while probing known Vertica license paths'
        for path in _KNOWN_LICENSE_PATH_CANDIDATES:
            quoted = shlex.quote(path)
            check = subprocess.run(
                ['docker', 'exec', '--user', '0', container, 'sh', '-c', f'test -r {quoted}'],
                capture_output=True,
                text=True,
            )
            if check.returncode == 0 and path not in seen:
                candidates.append(path)
                seen.add(path)
            elif check.returncode not in (0, 1):
                log(missing_cli_message)
                break

    return sorted(candidates, key=_license_candidate_sort_key)


def _install_vertica_license(container: str) -> bool:
    """Attempt to install a Vertica license inside ``container``."""

    license_paths = _discover_container_license_files(container)
    if not license_paths:
        log('Unable to locate Vertica license files inside the container')
        return False

    for path in license_paths:
        quoted = shlex.quote(path)
        log(f'Attempting to install Vertica license from {path}')
        result = _run_admintools_license_command(
            container,
            _admintools_license_command_variants('install', license_path=quoted),
            'Docker CLI is not available while installing Vertica license',
            allow_root_fallback=False,
            action='install',
            license_path=quoted,
        )

        if result is None:
            return False

        combined = f"{result.stdout}\n{result.stderr}".lower()

        if any(pattern in combined for pattern in _ADMINTOOLS_UNKNOWN_LICENSE_PATTERNS):
            if _deploy_vertica_license_fallback(container, path):
                log(
                    'admintools does not support the legacy install_license tool; '
                    'deployed license via fallback locations'
                )
                return True
            log(
                'admintools does not support the legacy install_license tool; '
                'skipping in-container license installation'
            )
            return False

        if result.returncode == 0:
            log(f'Successfully installed Vertica license from {path}')
            return True

        if 'already installed' in combined or 'already licensed' in combined:
            log('Vertica license already installed according to admintools output')
            return True

    log('Failed to install Vertica license using discovered files')
    return False


_VERTICA_LICENSE_FALLBACK_PATHS: tuple[str, ...] = (
    '/opt/vertica/config/license.dat',
    '/opt/vertica/config/license.key',
    '/data/vertica/config/license.dat',
    '/data/vertica/config/license.key',
)


def _deploy_vertica_license_fallback(
    container: str,
    source_path: str,
    *,
    extra_destinations: tuple[str, ...] = (),
) -> bool:
    """Copy ``source_path`` to known Vertica license destinations inside ``container``."""

    missing_cli_message = (
        'Docker CLI is not available while deploying Vertica license fallback'
    )

    quoted_source = shlex.quote(source_path)
    deployed = False

    destinations = list(
        dict.fromkeys((*_VERTICA_LICENSE_FALLBACK_PATHS, *extra_destinations))
    )

    for destination in destinations:
        if destination == source_path:
            log(
                'Vertica license source and destination are identical; '
                f'skipping copy for {destination}'
            )
            deployed = True
            success, _ = _align_container_path_identity(
                container,
                destination,
                f'Vertica license at {destination}',
                context='Vertica license',
            )
            if not success:
                log(
                    'Failed to align Vertica license ownership inside container '
                    f'at {destination}'
                )
            continue

        quoted_destination = shlex.quote(destination)
        script = '\n'.join(
            (
                'set -euo pipefail',
                f'src={quoted_source}',
                f'dest={quoted_destination}',
                'dest_dir=$(dirname "$dest")',
                'if [ -e "$dest_dir" ] && [ ! -d "$dest_dir" ]; then rm -rf "$dest_dir"; fi',
                'mkdir -p "$dest_dir"',
                'if command -v install >/dev/null 2>&1; then',
                '  install -m 0644 "$src" "$dest"',
                'else',
                '  tmp="${dest}.tmp.$$"',
                '  trap \'rm -f "$tmp"\' EXIT INT TERM',
                '  umask 022',
                '  cp -- "$src" "$tmp"',
                '  chmod 0644 "$tmp"',
                '  mv -- "$tmp" "$dest"',
                '  trap - EXIT INT TERM',
                'fi',
            )
        )
        result = _docker_exec_root_shell(container, script, missing_cli_message)
        if result is None:
            return False

        if result.returncode != 0:
            combined_output = f"{result.stdout}\n{result.stderr}".lower()
            if 'are the same file' in combined_output:
                log(
                    'Vertica license copy reported identical source and destination; '
                    f'treating {destination} as already provisioned'
                )
                deployed = True
                success, _ = _align_container_path_identity(
                    container,
                    destination,
                    f'Vertica license at {destination}',
                    context='Vertica license',
                )
                if not success:
                    log(
                        'Failed to align Vertica license ownership inside container '
                        f'at {destination}'
                    )
                continue

            log(
                'Failed to copy Vertica license from '
                f'{source_path} to {destination}'
            )
            continue

        deployed = True

        success, _ = _align_container_path_identity(
            container,
            destination,
            f'Vertica license at {destination}',
            context='Vertica license',
        )
        if not success:
            log(
                'Failed to align Vertica license ownership inside container '
                f'at {destination}'
            )

    return deployed


_LICENSE_ERROR_PATH_PATTERN = re.compile(r'/((?:opt|data)/vertica/[^\s"\']+)')


def _extract_license_error_paths(message: str) -> tuple[str, ...]:
    """Return unique Vertica license paths extracted from ``message``."""

    if not message:
        return ()

    candidates = []
    for match in _LICENSE_ERROR_PATH_PATTERN.finditer(message):
        path = '/' + match.group(1)
        if path not in candidates:
            candidates.append(path)
    return tuple(candidates)


def _ensure_vertica_license_installed(container: str) -> LicenseStatus:
    """Ensure that a Vertica license is installed inside ``container``."""

    status = _run_admintools_license_command(
        container,
        _admintools_license_command_variants('list'),
        'Docker CLI is not available while checking Vertica license status',
        allow_root_fallback=False,
        action='list',
    )

    if status is None:
        return LicenseStatus(False, False)

    combined = f"{status.stdout}\n{status.stderr}".lower()
    status_unknown = any(
        pattern in combined for pattern in _ADMINTOOLS_UNKNOWN_LICENSE_PATTERNS
    )

    if status_unknown:
        log(
            'admintools does not provide the list_license tool; attempting manual '
            'license installation'
        )
        installed = _install_vertica_license(container)
        if not installed:
            return LicenseStatus(False, False)
        log(
            'admintools does not provide a reliable license status command; '
            'assuming license installation succeeded'
        )
        return LicenseStatus(True, False)

    if status.returncode == 0:
        if 'no license' not in combined and 'not been installed' not in combined:
            return LicenseStatus(True, True)
        log(
            'Vertica license status indicates no license is installed; attempting '
            'installation'
        )
        installed = _install_vertica_license(container)
    else:
        log(
            'Vertica license status indicates no license is installed; attempting '
            'installation'
        )
        installed = _install_vertica_license(container)

    if not installed:
        return LicenseStatus(False, False)

    verification = _run_admintools_license_command(
        container,
        _admintools_license_command_variants('list'),
        'Docker CLI is not available while verifying Vertica license status',
        allow_root_fallback=False,
        action='list',
    )

    if verification is None:
        return LicenseStatus(False, False)

    if verification.returncode == 0:
        combined_verification = f"{verification.stdout}\n{verification.stderr}".lower()
        if 'no license' in combined_verification or 'not been installed' in combined_verification:
            log('Vertica license verification still reports no license installed')
            return LicenseStatus(False, False)
        return LicenseStatus(True, True)

    combined_verification = f"{verification.stdout}\n{verification.stderr}".lower()

    if any(
        pattern in combined_verification
        for pattern in _ADMINTOOLS_UNKNOWN_LICENSE_PATTERNS
    ):
        log(
            'admintools does not provide a reliable license status command; '
            'assuming license installation succeeded'
        )
        return LicenseStatus(True, False)

    return LicenseStatus(False, False)


def _attempt_vertica_database_creation(container: str, database: str) -> bool:
    """Attempt to create ``database`` inside ``container`` using admintools."""

    env = _fetch_container_env(container)
    password = env.get('VERTICA_DB_PASSWORD')
    if password is None:
        password = os.environ.get('DBADMIN_PASSWORD', '')

    host = (
        env.get('VERTICA_DB_HOST')
        or env.get('VERTICA_HOST')
        or '127.0.0.1'
    )

    license_candidates = _discover_container_license_files(container)
    license_status = _ensure_vertica_license_installed(container)
    license_verified = license_status.verified

    log(
        'Invoking Vertica admintools to create database '
        f"{database!r} inside container {container}"
    )

    base_command = (
        "/opt/vertica/bin/admintools -t create_db -s "
        f"{shlex.quote(host)} -d {shlex.quote(database)} -p {shlex.quote(password)}"
    )

    def _create_command_variants(
        license_path: Optional[str],
    ) -> tuple[str, ...]:
        if not license_path:
            return (base_command,)

        fragments = _license_option_variants(
            license_path, include_create_short_flag=True
        )
        commands = [
            f'{base_command} {fragment}'.strip() for fragment in fragments
        ]
        return tuple(dict.fromkeys(commands))

    def _run_create(
        license_path: Optional[str] = None,
    ) -> Optional[subprocess.CompletedProcess[str]]:
        commands = _create_command_variants(license_path)
        last_result: Optional[subprocess.CompletedProcess[str]] = None

        for command in commands:
            script = 'set -euo pipefail\n' + command
            result = _docker_exec_prefer_container_admin(
                container,
                ['sh', '-c', script],
                'Docker CLI is not available while creating Vertica database',
            )

            if result is None:
                return None

            last_result = result

            if result.returncode == 0:
                return result

            combined_attempt = f"{result.stdout}\n{result.stderr}".lower()
            if (
                license_path is not None
                and any(
                    pattern in combined_attempt
                    for pattern in _ADMINTOOLS_UNKNOWN_LICENSE_PATTERNS
                )
            ):
                continue

            return result

        return last_result

    initial_attempts: list[Optional[str]] = []

    if not license_verified and license_candidates:
        initial_attempts.append(license_candidates[0])

    initial_attempts.append(None)

    result: Optional[subprocess.CompletedProcess[str]] = None
    attempted: set[Optional[str]] = set()

    for license_path in initial_attempts:
        if license_path in attempted:
            continue
        attempted.add(license_path)
        result = _run_create(license_path)
        if result is None:
            return False
        if result.returncode == 0:
            log('Requested Vertica database creation inside container; waiting for recovery')
            return True
        combined_attempt = f"{result.stdout}\n{result.stderr}".lower()
        if (
            license_path is not None
            and any(
                pattern in combined_attempt
                for pattern in _ADMINTOOLS_UNKNOWN_LICENSE_PATTERNS
            )
        ):
            log(
                'admintools reported that the create_db command does not support '
                'the supplied license flag; retrying without explicit license path'
            )
            continue
        break

    if result is None:
        return False

    raw_output = f"{result.stdout}\n{result.stderr}"
    combined = raw_output.lower()
    license_error = 'license' in combined and (
        'not been installed' in combined
        or 'not installed' in combined
        or 'no license' in combined
        or 'invalid license status' in combined
    )

    if license_error:
        log(
            'Vertica reported that no license is installed while creating '
            'the database; attempting to install the default license'
        )
        error_paths = _extract_license_error_paths(raw_output)
        if license_verified or _ensure_vertica_license_installed(container).installed:
            log('Retrying Vertica database creation after installing license')
            retry_result = _run_create()
            if retry_result is not None and retry_result.returncode == 0:
                log('Requested Vertica database creation inside container; waiting for recovery')
                return True

        if error_paths and license_candidates:
            log(
                'Attempting to seed Vertica license using paths reported in '
                'admintools output'
            )
            seeded = False
            for candidate in license_candidates:
                if _deploy_vertica_license_fallback(
                    container,
                    candidate,
                    extra_destinations=error_paths,
                ):
                    seeded = True
                    log(
                        'Retrying Vertica database creation after seeding license '
                        'paths reported by admintools'
                    )
                    retry_result = _run_create()
                    if retry_result is not None and retry_result.returncode == 0:
                        log(
                            'Requested Vertica database creation inside container; '
                            'waiting for recovery'
                        )
                        return True
            if seeded:
                log('Vertica license seeding did not resolve create_db failure; continuing')

        for path in license_candidates:
            log(f'Retrying Vertica database creation using license file {path}')
            retry_result = _run_create(path)
            if retry_result is not None:
                if retry_result.returncode == 0:
                    log('Requested Vertica database creation inside container; waiting for recovery')
                    return True
                combined_retry = f"{retry_result.stdout}\n{retry_result.stderr}".lower()
                if any(
                    pattern in combined_retry
                    for pattern in _ADMINTOOLS_UNKNOWN_LICENSE_PATTERNS
                ):
                    log(
                        'admintools reported that the create_db command does not '
                        'support the supplied license flag; stopping license retries'
                    )
                    break

    log(
        'Vertica database creation command exited with '
        f'code {result.returncode}; continuing recovery attempts'
    )
    return False


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


def _strip_inline_comment(text: str) -> str:
    """Return ``text`` without any trailing YAML-style comment."""

    in_single = False
    in_double = False

    for index, char in enumerate(text):
        if char == "'" and not in_double:
            in_single = not in_single
            continue
        if char == '"' and not in_single:
            in_double = not in_double
            continue
        if char == '#' and not in_single and not in_double:
            return text[:index].rstrip()

    return text.rstrip()


def _parse_inline_environment_list(payload: str) -> Optional[list[str]]:
    """Parse an inline YAML list used for ``environment`` entries."""

    payload = payload.strip()
    if not payload:
        return []

    literal = f'[{payload}]'
    try:
        parsed = ast.literal_eval(literal)
    except (SyntaxError, ValueError):
        items = []
        for part in payload.split(','):
            item = part.strip()
            if not item:
                continue
            if (item.startswith('"') and item.endswith('"')) or (
                item.startswith("'") and item.endswith("'")
            ):
                item = item[1:-1]
            items.append(item)
        return items

    if isinstance(parsed, list):
        return [str(element) if not isinstance(element, str) else element for element in parsed]

    return None


def _parse_inline_environment_mapping(payload: str) -> Optional[dict[str, str]]:
    """Parse an inline YAML mapping used for ``environment`` entries."""

    payload = payload.strip()
    if not payload:
        return {}

    literal = f'{{{payload}}}'
    try:
        parsed = ast.literal_eval(literal)
    except (SyntaxError, ValueError):
        entries: dict[str, str] = {}
        for part in payload.split(','):
            if not part.strip():
                continue
            key, sep, value = part.partition(':')
            if not sep:
                return None
            key = key.strip()
            value = value.strip()
            if (key.startswith('"') and key.endswith('"')) or (
                key.startswith("'") and key.endswith("'")
            ):
                key = key[1:-1]
            if (value.startswith('"') and value.endswith('"')) or (
                value.startswith("'") and value.endswith("'")
            ):
                value = value[1:-1]
            entries[key] = value
        return entries

    if isinstance(parsed, dict):
        return {
            str(key): '' if value is None else str(value)
            for key, value in parsed.items()
        }

    return None


def _convert_inline_environment(
    remainder: str, value_indent: str
) -> Optional[tuple[str, list[str]]]:
    """Return the normalized lines for an inline ``environment`` block."""

    remainder = _strip_inline_comment(remainder)

    if remainder == '{}':
        return 'mapping', []
    if remainder == '[]':
        return 'list', []

    if remainder.startswith('[') and remainder.endswith(']'):
        items = _parse_inline_environment_list(remainder[1:-1])
        if items is None:
            return None
        return 'list', [f"{value_indent}- {item}" for item in items]

    if remainder.startswith('{') and remainder.endswith('}'):
        mapping = _parse_inline_environment_mapping(remainder[1:-1])
        if mapping is None:
            return None
        return (
            'mapping',
            [
                f"{value_indent}{key}: {value}"
                for key, value in mapping.items()
            ],
        )

    try:
        import yaml  # type: ignore
    except Exception:  # pragma: no cover - yaml is optional
        yaml = None  # type: ignore

    if yaml is not None:
        try:
            parsed = yaml.safe_load(remainder)
        except Exception:  # pragma: no cover - unsafe content
            parsed = None
        if isinstance(parsed, list):
            return 'list', [f"{value_indent}- {str(item)}" for item in parsed]
        if isinstance(parsed, dict):
            return (
                'mapping',
                [
                    f"{value_indent}{str(key)}: {'' if value is None else str(value)}"
                    for key, value in parsed.items()
                ],
            )

    return None


def _ensure_compose_accepts_eula(compose_file: Path) -> bool:
    """Ensure ``compose_file`` sets the environment variables for EULA acceptance."""

    try:
        original = compose_file.read_text()
    except OSError as exc:
        log(f'Unable to read {compose_file} while ensuring EULA acceptance: {exc}')
        return False

    lines = original.splitlines()
    updated = False
    ensured = False
    index = 0

    while index < len(lines):
        raw_line = lines[index]
        stripped = raw_line.strip()

        if not stripped.startswith('environment:'):
            index += 1
            continue

        indent = raw_line[: len(raw_line) - len(raw_line.lstrip())]
        value_indent = indent + '  '

        remainder = stripped[len('environment:') :].strip()
        inline_mode: Optional[str] = None
        if remainder:
            conversion = _convert_inline_environment(remainder, value_indent)
            if conversion is None:
                index += 1
                continue
            inline_mode, new_lines = conversion
            lines[index] = f'{indent}environment:'
            lines = lines[: index + 1] + new_lines + lines[index + 1 :]
            updated = True

        block_start = index + 1
        block_end = block_start
        while block_end < len(lines):
            next_line = lines[block_end]
            if not next_line.startswith(value_indent):
                break
            block_end += 1

        block_lines = lines[block_start:block_end]

        existing_keys: set[str] = set()
        block_type: Optional[str] = inline_mode
        for entry in block_lines:
            stripped_entry = entry.strip()
            if not stripped_entry or stripped_entry.startswith('#'):
                continue
            if stripped_entry.startswith('- '):
                block_type = block_type or 'list'
                assignment = stripped_entry[2:]
                key, _, _ = assignment.partition('=')
                if key:
                    existing_keys.add(key.strip())
            else:
                block_type = block_type or 'mapping'
                key, _, _ = stripped_entry.partition(':')
                if key:
                    existing_keys.add(key.strip())

        if block_type is None:
            block_type = 'mapping'

        missing = [
            key
            for key in _EULA_ENVIRONMENT_VARIABLES
            if key not in existing_keys
        ]

        if not missing:
            ensured = True
            index = block_end
            continue

        if block_type == 'list':
            new_entries = [
                f"{value_indent}- {key}={_EULA_ENVIRONMENT_VARIABLES[key]}"
                for key in missing
            ]
        else:
            new_entries = [
                f"{value_indent}{key}: {_EULA_ENVIRONMENT_VARIABLES[key]}"
                for key in missing
            ]

        lines = lines[:block_end] + new_entries + lines[block_end:]
        block_end += len(new_entries)
        updated = True
        ensured = True
        index = block_end

    if ensured:
        # The compose file already contains the necessary environment block.
        if not updated:
            return True
    else:
        # No ``environment`` block was found; synthesize one under the Vertica service
        service_indent: Optional[str] = None
        service_index: Optional[int] = None

        for candidate_index, candidate_line in enumerate(lines):
            stripped_candidate = candidate_line.strip()
            if not stripped_candidate or stripped_candidate.startswith('#'):
                continue
            if stripped_candidate == 'vertica_ce:':
                service_indent = candidate_line[: len(candidate_line) - len(candidate_line.lstrip())]
                service_index = candidate_index + 1
                break

        if service_indent is None or service_index is None:
            # Fall back to locating the service that sets ``container_name: vertica_ce``
            target = 'container_name: vertica_ce'
            for candidate_index, candidate_line in enumerate(lines):
                if candidate_line.strip() != target:
                    continue

                container_indent_len = len(candidate_line) - len(candidate_line.lstrip())
                for reverse_index in range(candidate_index - 1, -1, -1):
                    previous_line = lines[reverse_index]
                    stripped_previous = previous_line.strip()
                    if not stripped_previous or stripped_previous.startswith('#'):
                        continue

                    previous_indent_len = len(previous_line) - len(previous_line.lstrip())
                    if previous_indent_len < container_indent_len and stripped_previous.endswith(':'):
                        service_indent = previous_line[: len(previous_line) - len(previous_line.lstrip())]
                        service_index = reverse_index + 1
                        break

                if service_indent is not None:
                    break

        if service_indent is None or service_index is None:
            return False

        insert_at = len(lines)
        for candidate_index in range(service_index, len(lines)):
            candidate_line = lines[candidate_index]
            stripped_candidate = candidate_line.strip()
            if not stripped_candidate:
                continue
            candidate_indent = candidate_line[: len(candidate_line) - len(candidate_line.lstrip())]
            if len(candidate_indent) <= len(service_indent) and not stripped_candidate.startswith('#'):
                insert_at = candidate_index
                break

        block_indent = service_indent + '  '
        value_indent = block_indent + '  '

        new_lines = [f'{block_indent}environment:'] + [
            f'{value_indent}{key}: {_EULA_ENVIRONMENT_VARIABLES[key]}'
            for key in _EULA_ENVIRONMENT_VARIABLES
        ]

        lines = lines[:insert_at] + new_lines + lines[insert_at:]
        updated = True
        ensured = True

    if not ensured:
        return False

    if updated:
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
    degraded_observed_at: Optional[float] = None
    degraded_logged_duration: Optional[float] = None
    health_log_count = 0
    last_degraded_log_dump: Optional[float] = None
    admintools_permissions_checked = False
    last_direct_connect_attempt: Optional[float] = None
    eula_recreate_attempted = False
    eula_acceptance_attempted = False
    eula_acceptance_successful = False
    database_creation_attempted = False
    database_missing_logged = False

    compose_deadline = time.time() + compose_timeout

    while time.time() < deadline:
        status = _docker_inspect('vertica_ce', '{{.State.Status}}')
        health = _docker_inspect('vertica_ce', '{{if .State.Health}}{{.State.Health.Status}}{{end}}')
        health_entries: list[dict[str, object]] = []
        if status:
            health_entries = _docker_health_log('vertica_ce')
        health_log_count = _log_health_log_entries('vertica_ce', health_log_count)
        if status == 'running' and (not health or health == 'healthy'):
            degraded_observed_at = None
            degraded_logged_duration = None
            last_degraded_log_dump = None
            admintools_permissions_checked = False
            log(f'Vertica container status: {status}, health: {health or "unknown"}')
            return

        if (status, health) != last_status:
            previous_status, previous_health = last_status
            last_status = (status, health)
            log(f'Current Vertica container status: {status or "<absent>"}, health: {health or "<unknown>"}')
            if health not in {'unhealthy', 'starting'} or previous_health != health:
                degraded_observed_at = None
                degraded_logged_duration = None
                last_degraded_log_dump = None
                admintools_permissions_checked = False
                eula_acceptance_attempted = False

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
            degraded_observed_at = None
            degraded_logged_duration = None
            last_degraded_log_dump = None
            admintools_permissions_checked = False
            eula_acceptance_attempted = False
            eula_acceptance_successful = False
            database_creation_attempted = False
            database_missing_logged = False
        elif status not in {'running', 'restarting'}:
            run_command(['docker', 'start', 'vertica_ce'])
            restart_attempts = 0
            recreate_attempts = 0
            degraded_observed_at = None
            degraded_logged_duration = None
            last_degraded_log_dump = None
            admintools_permissions_checked = False
            eula_acceptance_attempted = False
            eula_acceptance_successful = False
            database_creation_attempted = False
            database_missing_logged = False
        elif health in {'unhealthy', 'starting'}:
            now = time.time()
            if degraded_observed_at is None:
                degraded_observed_at = now

            degraded_duration = now - degraded_observed_at
            state_is_unhealthy = health == 'unhealthy'
            missing_database_reason: Optional[str] = None
            if _health_log_indicates_missing_database(health_entries, DB_NAME):
                missing_database_reason = 'health checks'
            elif (
                status == 'running'
                and not database_creation_attempted
                and _container_logs_indicate_missing_database('vertica_ce', DB_NAME)
            ):
                missing_database_reason = 'container logs'

            if (
                degraded_duration >= 120
                and (
                    last_degraded_log_dump is None
                    or now - last_degraded_log_dump >= 120
                )
            ):
                _log_container_tail('vertica_ce', tail=200)
                last_degraded_log_dump = now

            if not admintools_permissions_checked:
                admintools_permissions_checked = True
                if _ensure_container_admintools_conf_readable('vertica_ce'):
                    log('Relaxed admintools.conf permissions inside container; waiting for recovery')
                    _sanitize_vertica_data_directories()
                    time.sleep(5)
                    continue

            if (
                status == 'running'
                and missing_database_reason is not None
                and not database_creation_attempted
            ):
                if not database_missing_logged:
                    if missing_database_reason == 'health checks':
                        log(
                            'Vertica health checks indicate the configured database '
                            f"{DB_NAME!r} is missing; attempting to create it inside the container"
                        )
                    else:
                        log(
                            'Vertica container logs indicate the configured database '
                            f"{DB_NAME!r} is missing; attempting to create it inside the container"
                        )
                    database_missing_logged = True
                if _attempt_vertica_database_creation('vertica_ce', DB_NAME):
                    database_creation_attempted = True
                    degraded_observed_at = None
                    degraded_logged_duration = None
                    last_degraded_log_dump = None
                    continue
                database_creation_attempted = True

            if degraded_duration < UNHEALTHY_HEALTHCHECK_GRACE_PERIOD_SECONDS:
                if (
                    degraded_logged_duration is None
                    or degraded_duration - degraded_logged_duration >= 30
                    or degraded_duration < degraded_logged_duration
                ):
                    if state_is_unhealthy:
                        log(
                            'Vertica container health reported unhealthy but has '
                            f'been unhealthy for {degraded_duration:.0f}s; '
                            'waiting for recovery'
                        )
                    else:
                        log(
                            'Vertica container health remains in starting state '
                            f'after {degraded_duration:.0f}s; waiting for readiness'
                        )
                    degraded_logged_duration = degraded_duration
                _sanitize_vertica_data_directories()
                eula_prompt_detected = _container_reports_eula_prompt('vertica_ce')
                if eula_prompt_detected and not eula_acceptance_successful and not eula_acceptance_attempted:
                    eula_acceptance_attempted = True
                    if _accept_vertica_eula('vertica_ce'):
                        eula_acceptance_successful = True
                        restart_attempts = 0
                        recreate_attempts = 0
                        degraded_observed_at = None
                        degraded_logged_duration = None
                        last_degraded_log_dump = None
                        admintools_permissions_checked = False
                        database_creation_attempted = False
                        database_missing_logged = False
                        time.sleep(10)
                        continue
                if not eula_recreate_attempted and eula_prompt_detected:
                    if compose_file is None:
                        compose_file = _compose_file()
                    if compose_file is not None:
                        log(
                            'Vertica container logs indicate the EULA prompt is blocking startup; '
                            'ensuring acceptance variables and recreating container'
                        )
                        _ensure_compose_accepts_eula(compose_file)
                        _ensure_ecr_login_if_needed(compose_file)
                        _compose_up(compose_file, force_recreate=True)
                        eula_recreate_attempted = True
                        restart_attempts = 0
                        recreate_attempts = 0
                        degraded_observed_at = None
                        degraded_logged_duration = None
                        last_degraded_log_dump = None
                        admintools_permissions_checked = False
                        eula_acceptance_attempted = False
                        eula_acceptance_successful = False
                        database_creation_attempted = False
                        database_missing_logged = False
                        time.sleep(15)
                        continue
                time.sleep(10)
                continue

            if (
                last_direct_connect_attempt is None
                or now - last_direct_connect_attempt >= 60
            ):
                last_direct_connect_attempt = now
                if _container_is_responding():
                    if state_is_unhealthy:
                        log(
                            'Vertica container health remains unhealthy but direct '
                            'connection succeeded; proceeding despite health check'
                        )
                    else:
                        log(
                            'Vertica container health check is still reporting '
                            'starting but direct connection succeeded; proceeding '
                            'despite health check'
                        )
                    return

            uptime = _container_uptime_seconds('vertica_ce')
            if uptime is None:
                if state_is_unhealthy:
                    log(
                        'Vertica container health reported unhealthy but uptime '
                        'could not be determined; assuming the container is still starting'
                    )
                else:
                    log(
                        'Vertica container health remains in starting state but '
                        'uptime could not be determined; continuing to wait'
                    )
                _sanitize_vertica_data_directories()
                time.sleep(10)
                continue
            if uptime < UNHEALTHY_HEALTHCHECK_GRACE_PERIOD_SECONDS:
                if state_is_unhealthy:
                    log(
                        'Vertica container health reported unhealthy but uptime '
                        f'{uptime:.0f}s is within grace period; waiting for recovery'
                    )
                else:
                    log(
                        'Vertica container health remains in starting state but '
                        f'uptime {uptime:.0f}s is within grace period; waiting for readiness'
                    )
                degraded_logged_duration = degraded_duration
                _sanitize_vertica_data_directories()
                time.sleep(10)
                continue

            if restart_attempts < 3:
                if state_is_unhealthy:
                    log('Vertica container health check reported unhealthy; restarting container')
                else:
                    log('Vertica container health check remained in starting state; restarting container')
                run_command(['docker', 'restart', 'vertica_ce'])
                restart_attempts += 1
                time.sleep(10)
                degraded_observed_at = None
                degraded_logged_duration = None
                last_degraded_log_dump = None
                eula_acceptance_attempted = False
                eula_acceptance_successful = False
                database_creation_attempted = False
                database_missing_logged = False
                continue

            if compose_file is None:
                compose_file = _compose_file()
            if compose_file is not None and recreate_attempts < 2:
                if state_is_unhealthy:
                    log('Vertica container remains unhealthy; recreating via docker compose')
                else:
                    log('Vertica container remains in starting state; recreating via docker compose')
                _ensure_compose_accepts_eula(compose_file)
                _ensure_ecr_login_if_needed(compose_file)
                _compose_up(compose_file, force_recreate=True)
                recreate_attempts += 1
                restart_attempts = 0
                time.sleep(15)
                degraded_observed_at = None
                degraded_logged_duration = None
                last_degraded_log_dump = None
                eula_acceptance_attempted = False
                eula_acceptance_successful = False
                database_creation_attempted = False
                database_missing_logged = False
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
                    degraded_observed_at = None
                    degraded_logged_duration = None
                    last_degraded_log_dump = None
                    eula_acceptance_attempted = False
                    eula_acceptance_successful = False
                    database_creation_attempted = False
                    database_missing_logged = False
                    if compose_file is not None:
                        _ensure_compose_accepts_eula(compose_file)
                        _ensure_ecr_login_if_needed(compose_file)
                        _compose_up(compose_file, force_recreate=True)
                    time.sleep(15)
                    continue
                log('Failed to reset Vertica data directories or nothing to reset')

            if state_is_unhealthy:
                log('Vertica container is still unhealthy after recovery attempts; collecting diagnostics')
            else:
                log('Vertica container is still in starting state after recovery attempts; collecting diagnostics')
            try:
                run_command(['docker', 'ps', '--filter', 'name=vertica_ce'])
            except SystemExit:
                pass
            try:
                run_command(['docker', 'logs', '--tail', '200', 'vertica_ce'])
            except SystemExit:
                pass
            state_summary = 'unhealthy' if state_is_unhealthy else 'stuck in starting state'
            raise SystemExit(
                f'Vertica container vertica_ce remained {state_summary} after restart and recreate attempts'
            )

        time.sleep(5)

    raise SystemExit(
        'Vertica container vertica_ce did not reach running & healthy state before timeout'
    )


def wait_for_port(host: str, port: int, timeout: float = 600.0) -> None:
    deadline = time.time() + timeout
    last_error: Optional[Exception] = None
    next_status_update = time.time()
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=5.0):
                return
        except OSError as exc:
            last_error = exc
            now = time.time()
            if now >= next_status_update:
                remaining = max(0.0, deadline - now)
                log(
                    'Port '
                    f'{host}:{port} is not reachable yet ({exc}); '
                    f'{remaining:.0f}s remaining before timeout'
                )
                next_status_update = now + 60.0
            time.sleep(5)
    raise SystemExit(
        f'Port {host}:{port} did not become reachable within {timeout:.0f}s: {last_error}'
    )


def _remaining_seconds(deadline: float) -> float:
    return max(0.0, deadline - time.time())


def _deadline_limited_timeout(
    desired: float,
    *,
    deadline: float,
    reserve: float,
    minimum: float,
    maximum: float,
) -> float:
    remaining = _remaining_seconds(deadline)
    budget = max(0.0, remaining - reserve)
    if budget <= 0.0:
        return 0.0
    limited = min(desired, budget)
    if limited < minimum:
        return limited
    return min(maximum, limited)


def _ensure_time_budget(deadline: float, *, reserve: float, context: str) -> None:
    remaining = _remaining_seconds(deadline)
    if remaining <= reserve:
        raise SystemExit(
            'Smoke test exhausted its time budget '
            f'{context}; remaining {remaining:.0f}s with reserve '
            f'{reserve:.0f}s'
        )


def _sleep_with_deadline(delay: float, deadline: Optional[float]) -> None:
    if delay <= 0:
        return
    if deadline is None:
        time.sleep(delay)
        return
    remaining = deadline - time.time()
    if remaining <= 0:
        return
    time.sleep(min(delay, remaining))


def _connection_attempt_budget(
    deadline: Optional[float], delay: float, attempts: int
) -> int:
    if deadline is None:
        return attempts
    remaining = _remaining_seconds(deadline)
    if remaining <= 0:
        return 0
    per_attempt = max(delay, VERTICA_CLIENT_CONNECT_TIMEOUT_SECONDS)
    allowed = int(remaining // per_attempt) + 1
    return max(1, min(attempts, allowed))


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
        'tlsmode': _resolve_tlsmode(),
        'connection_timeout': VERTICA_CLIENT_CONNECT_TIMEOUT_SECONDS,
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
    return "disable"


def connect_and_query(
    label: str,
    host: str,
    user: str,
    password: str,
    *,
    attempts: int = 30,
    delay: float = 10.0,
    fatal: bool = True,
    deadline: Optional[float] = None,
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
        'connection_timeout': VERTICA_CLIENT_CONNECT_TIMEOUT_SECONDS,
    }

    last_error: Optional[BaseException] = None

    max_attempts = _connection_attempt_budget(deadline, delay, attempts)
    if max_attempts == 0:
        message = (
            f'[{label}] No time remaining to attempt Vertica connection before deadline'
        )
        if fatal:
            raise SystemExit(message)
        log(message)
        return False

    for attempt in range(1, max_attempts + 1):
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
            if attempt >= max_attempts:
                break

            remaining_display = ''
            if deadline is not None:
                remaining_display = f'; ~{_remaining_seconds(deadline):.0f}s until deadline'

            remaining_attempts = max_attempts - attempt
            sleep_duration = delay
            if deadline is not None:
                sleep_duration = min(delay, max(0.0, deadline - time.time()))

            log(
                f'[{label}] Connection attempt {attempt} failed with {exc!r}'
                f'{remaining_display}; retrying in {sleep_duration:.0f}s '
                f'({remaining_attempts} attempt(s) remaining)'
            )
            if sleep_duration <= 0:
                continue
            _sleep_with_deadline(sleep_duration, deadline)

    if last_error:
        message = f'[{label}] Failed to connect to Vertica: {last_error}'
        if fatal:
            raise SystemExit(message) from last_error
        log(message)
        return False

    return True


def main() -> int:
    log('Beginning in-instance Vertica smoke test with detailed diagnostics')
    overall_deadline = time.time() + SMOKE_TEST_OVERALL_TIMEOUT_SECONDS
    hostname = socket.gethostname()
    local_ipv4 = fetch_metadata('meta-data/local-ipv4')
    public_ipv4 = fetch_metadata('meta-data/public-ipv4')
    log(f'Instance hostname: {hostname}')
    log(f'Instance local IPv4: {local_ipv4}')
    log(f'Instance public IPv4: {public_ipv4}')

    ensure_docker_service()
    _sanitize_vertica_data_directories()
    container_timeout = _deadline_limited_timeout(
        1500.0,
        deadline=overall_deadline,
        reserve=SMOKE_TEST_CONTAINER_RESERVE_SECONDS,
        minimum=60.0,
        maximum=1500.0,
    )
    ensure_vertica_container_running(timeout=container_timeout)
    _ensure_time_budget(
        overall_deadline,
        reserve=SMOKE_TEST_PORT_RESERVE_SECONDS,
        context='after ensuring Vertica container readiness',
    )
    log(STEP_SEPARATOR)
    log('Waiting for Vertica port 5433 to accept TCP connections on localhost')
    port_timeout = _deadline_limited_timeout(
        600.0,
        deadline=overall_deadline,
        reserve=SMOKE_TEST_PORT_RESERVE_SECONDS,
        minimum=30.0,
        maximum=600.0,
    )
    wait_for_port('127.0.0.1', DB_PORT, timeout=port_timeout)
    _ensure_time_budget(
        overall_deadline,
        reserve=SMOKE_TEST_MINIMUM_RESERVE_SECONDS,
        context='after waiting for Vertica port 5433',
    )
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
        f'{bootstrap_user}@localhost',
        '127.0.0.1',
        bootstrap_user,
        bootstrap_password,
        deadline=overall_deadline,
    )
    _ensure_primary_admin_user(
        bootstrap_user, bootstrap_password, ADMIN_USER, ADMIN_PASSWORD
    )
    connect_and_query(
        'primary_admin@localhost',
        '127.0.0.1',
        ADMIN_USER,
        ADMIN_PASSWORD,
        deadline=overall_deadline,
    )

    if not connect_and_query(
        f'{bootstrap_user}@public_ip',
        public_ipv4,
        bootstrap_user,
        bootstrap_password,
        fatal=False,
        deadline=overall_deadline,
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
    with vertica_python.connect(host='127.0.0.1', port=DB_PORT, user=ADMIN_USER, password=ADMIN_PASSWORD, database=DB_NAME, autocommit=True, tlsmode=_resolve_tlsmode(), connection_timeout=VERTICA_CLIENT_CONNECT_TIMEOUT_SECONDS) as admin_conn:
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
        connect_and_query(
            'smoke_user@localhost',
            '127.0.0.1',
            smoke_user,
            smoke_pass,
            deadline=overall_deadline,
        )
    finally:
        if smoke_user_created:
            log(STEP_SEPARATOR)
            log(f'Dropping smoke test user {smoke_user!r}')
            with vertica_python.connect(host='127.0.0.1', port=DB_PORT, user=ADMIN_USER, password=ADMIN_PASSWORD, database=DB_NAME, autocommit=True, tlsmode=_resolve_tlsmode(), connection_timeout=VERTICA_CLIENT_CONNECT_TIMEOUT_SECONDS) as admin_conn:
                admin_conn.cursor().execute(
                    f'DROP USER {_quote_identifier(smoke_user)} CASCADE'
                )

    log(STEP_SEPARATOR)
    log('All smoke test checks completed successfully')
    log('SMOKE_TEST_SUCCESS')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
