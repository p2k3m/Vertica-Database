import sys
from types import SimpleNamespace

import pytest


class _PlaceholderSession:
    def __init__(self, *args, **kwargs):
        raise AssertionError("Session should be patched in tests")


sys.modules.setdefault(
    "requests",
    SimpleNamespace(Session=_PlaceholderSession, HTTPError=Exception),
)


class _OpenAIStub:
    def __init__(self, *args, **kwargs):
        raise AssertionError("OpenAI client should be patched in tests")


sys.modules.setdefault("openai", SimpleNamespace(OpenAI=_OpenAIStub))

from scripts import auto_fix_pipeline


class DummyResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload

    def raise_for_status(self):
        return None


class DummySession:
    def __init__(self, responses):
        self.headers = {}
        self._responses = list(responses)
        self.calls = []

    def get(self, url, params=None, timeout=None):
        if not self._responses:
            raise AssertionError("Unexpected GET call with no prepared response")
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        return DummyResponse(self._responses.pop(0))


def _build_run(**overrides):
    payload = {
        "id": 1,
        "head_sha": "deadbeef",
        "html_url": "https://example.com/run/1",
        "status": "completed",
        "conclusion": "failure",
        "name": "CI",
        "event": "push",
        "head_branch": "main",
        "pull_requests": [],
    }
    payload.update(overrides)
    return payload


@pytest.fixture
def patch_requests_session(monkeypatch):
    def factory(responses):
        session = DummySession(responses)
        monkeypatch.setattr(auto_fix_pipeline.requests, "Session", lambda: session)
        return session

    return factory


def test_latest_failed_run_prefers_pull_requests(patch_requests_session):
    pr_run = _build_run(id=2, event="pull_request", head_branch="feature", pull_requests=[{"number": 42}])
    session = patch_requests_session([{ "workflow_runs": [pr_run] }])

    client = auto_fix_pipeline.GitHubClient("token", "owner", "repo")
    run = client.latest_failed_run()

    assert run is not None
    assert run.event == "pull_request"
    assert run.pull_requests == [42]
    assert session.calls[0]["params"]["event"] == "pull_request"


def test_latest_failed_run_falls_back_to_main(patch_requests_session):
    main_run = _build_run(id=5)
    session = patch_requests_session([
        {"workflow_runs": []},
        {"workflow_runs": [main_run]},
    ])

    client = auto_fix_pipeline.GitHubClient("token", "owner", "repo")
    run = client.latest_failed_run()

    assert run is not None
    assert run.head_branch == "main"
    assert run.event == "push"
    assert len(session.calls) == 2
    assert session.calls[0]["params"]["event"] == "pull_request"
    assert session.calls[1]["params"]["branch"] == "main"


def test_latest_failed_run_returns_none_when_no_failures(patch_requests_session):
    session = patch_requests_session([
        {"workflow_runs": []},
        {"workflow_runs": []},
    ])

    client = auto_fix_pipeline.GitHubClient("token", "owner", "repo")
    run = client.latest_failed_run()

    assert run is None
    assert len(session.calls) == 2
