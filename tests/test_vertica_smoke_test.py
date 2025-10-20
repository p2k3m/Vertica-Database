import importlib
import os
from datetime import datetime, timedelta, timezone

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
