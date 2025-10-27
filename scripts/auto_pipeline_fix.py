"""Automation tool to repair failing GitHub Actions runs using an LLM.

This module implements the following feedback loop:

1. Detect the most recent failed workflow run on ``main``.
2. Download and summarise the failure logs.
3. Ask an LLM for a unified diff that addresses the failure.
4. Create a fix branch, apply the diff, push it, and open a pull request.
5. Enable auto-merge for the PR and monitor the resulting status checks.
6. If the checks fail, feed the fresh logs back to the LLM and retry (up to a
   bounded number of iterations).

The script expects to run inside a local clone of the repository and requires
access to both the GitHub and OpenAI APIs.  It is designed to be invoked by a
pipeline job after a workflow run fails, ensuring that a remediation attempt is
made automatically before alerting a human.
"""

from __future__ import annotations

import dataclasses
import io
import json
import os
import subprocess
import tempfile
import textwrap
import time
import zipfile
from typing import Dict, List, Optional, Sequence

import requests


# ---------------------------------------------------------------------------
# Data structures


@dataclasses.dataclass
class WorkflowRun:
    """Minimal representation of a GitHub workflow run."""

    id: int
    head_sha: str
    html_url: str
    status: str
    conclusion: Optional[str]
    event: str
    branch: str


@dataclasses.dataclass
class PullRequest:
    number: int
    url: str
    head_sha: str
    node_id: str


class AutoPipelineFixer:
    """Orchestrates the feedback loop described in the module docstring."""

    def __init__(
        self,
        *,
        repo: str,
        github_token: str,
        openai_token: str,
        openai_model: str,
        branch_prefix: str = "autofix",
        max_iterations: int = 3,
        poll_interval: int = 30,
        auto_merge_method: str = "SQUASH",
    ) -> None:
        self.repo = repo
        self.github_token = github_token
        self.openai_token = openai_token
        self.openai_model = openai_model
        self.branch_prefix = branch_prefix
        self.max_iterations = max_iterations
        self.poll_interval = poll_interval
        self.auto_merge_method = auto_merge_method

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self.github_token}",
                "X-GitHub-Api-Version": "2022-11-28",
            }
        )

    # ------------------------------------------------------------------
    # Public API

    def run(self) -> None:
        """Execute the auto-fix loop until success or iteration limit."""

        for attempt in range(1, self.max_iterations + 1):
            print(f"[autofix] Attempt {attempt}/{self.max_iterations}")

            failing_run = self._get_latest_failed_run_on_main()
            if not failing_run:
                print("[autofix] No failed workflow run detected on main; exiting")
                return

            print(
                f"[autofix] Targeting workflow run {failing_run.id} "
                f"(sha={failing_run.head_sha})"
            )

            log_excerpt = self._download_and_extract_logs(failing_run.id)
            prompt = self._build_llm_prompt(failing_run, log_excerpt)
            diff = self._request_diff_from_llm(prompt)

            branch_name = self._create_fix_branch()
            self._apply_diff(diff)
            commit_sha = self._commit_changes()
            pr = self._open_pull_request(branch_name)
            self._enable_auto_merge(pr)
            if self._wait_for_checks(commit_sha):
                print("[autofix] Checks succeeded; waiting for merge via auto-merge")
                self._wait_for_pr_merge(pr)
                print("[autofix] Auto-fix completed successfully")
                return

            print("[autofix] Checks failed; gathering new logs for next iteration")
            failing_run = self._get_failed_run_for_sha(commit_sha)
            if failing_run is None:
                print("[autofix] Unable to locate failing run for commit; aborting")
                return

        print("[autofix] Maximum attempts exhausted; manual intervention required")

    # ------------------------------------------------------------------
    # GitHub API helpers

    def _github_api(self, method: str, path: str, **kwargs) -> requests.Response:
        url = f"https://api.github.com{path}"
        response = self.session.request(method, url, **kwargs)
        if response.status_code >= 400:
            raise RuntimeError(
                f"GitHub API request failed ({response.status_code}): {response.text}"
            )
        return response

    def _get_latest_failed_run_on_main(self) -> Optional[WorkflowRun]:
        params = {"branch": "main", "status": "failure", "per_page": 1}
        response = self._github_api("GET", f"/repos/{self.repo}/actions/runs", params=params)
        data = response.json()
        runs = data.get("workflow_runs", [])
        if not runs:
            return None
        return self._parse_workflow_run(runs[0])

    def _get_failed_run_for_sha(self, sha: str) -> Optional[WorkflowRun]:
        params = {"head_sha": sha, "per_page": 1, "status": "failure"}
        response = self._github_api("GET", f"/repos/{self.repo}/actions/runs", params=params)
        runs = response.json().get("workflow_runs", [])
        if not runs:
            return None
        return self._parse_workflow_run(runs[0])

    def _parse_workflow_run(self, payload: Dict[str, object]) -> WorkflowRun:
        return WorkflowRun(
            id=int(payload["id"]),
            head_sha=str(payload["head_sha"]),
            html_url=str(payload.get("html_url", "")),
            status=str(payload.get("status", "")),
            conclusion=payload.get("conclusion") and str(payload.get("conclusion")),
            event=str(payload.get("event", "")),
            branch=str(payload.get("head_branch", "")),
        )

    def _download_and_extract_logs(self, run_id: int) -> str:
        print(f"[autofix] Downloading logs for run {run_id}")
        response = self._github_api(
            "GET", f"/repos/{self.repo}/actions/runs/{run_id}/logs", stream=True
        )
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            snippets: List[str] = []
            for name in sorted(zf.namelist()):
                if not name.endswith(".txt"):
                    continue
                with zf.open(name) as fh:
                    text = fh.read().decode("utf-8", errors="replace")
                tail = self._extract_relevant_log_tail(text)
                if tail:
                    snippets.append(f"===== {name} =====\n{tail}")
            if not snippets:
                return "[no log output extracted]"
        return "\n\n".join(snippets)

    @staticmethod
    def _extract_relevant_log_tail(text: str, max_lines: int = 120) -> str:
        lines = text.strip().splitlines()
        if not lines:
            return ""
        failure_markers = [
            "error",
            "fail",
            "exception",
            "traceback",
            "could not",
        ]
        lower_lines = [line.lower() for line in lines]
        indices = [i for i, line in enumerate(lower_lines) if any(k in line for k in failure_markers)]
        if indices:
            start = max(0, indices[-1] - max_lines // 2)
        else:
            start = max(0, len(lines) - max_lines)
        excerpt = lines[start : start + max_lines]
        return "\n".join(excerpt)

    # ------------------------------------------------------------------
    # LLM interaction

    def _build_llm_prompt(self, run: WorkflowRun, logs: str) -> str:
        guidance = textwrap.dedent(
            f"""
            A GitHub Actions workflow run on branch {run.branch} failed. The
            failing run URL is {run.html_url}.  The relevant log excerpts are
            shown below.  Respond ONLY with a unified diff that fixes the
            problem.  Do not include prose explanations.  The diff must apply to
            the repository root and may create, modify, or delete files as
            needed.

            {logs}
            """
        ).strip()
        return guidance

    def _request_diff_from_llm(self, prompt: str) -> str:
        print("[autofix] Requesting fix from LLM")
        headers = {
            "Authorization": f"Bearer {self.openai_token}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": self.openai_model,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are an automated code remediation assistant. "
                        "Output ONLY unified diffs that apply cleanly with `git apply`."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            "temperature": 0,
        }

        response = requests.post(
            os.environ.get("OPENAI_API_BASE", "https://api.openai.com") + "/v1/chat/completions",
            headers=headers,
            json=payload,
            timeout=120,
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"OpenAI API request failed ({response.status_code}): {response.text}"
            )

        message = response.json()["choices"][0]["message"]["content"].strip()
        if not message.startswith("diff"):
            raise ValueError(
                "LLM response did not start with a unified diff. Received: "
                f"{message[:200]}"
            )
        return message

    # ------------------------------------------------------------------
    # Git helpers

    def _create_fix_branch(self) -> str:
        timestamp = int(time.time())
        branch = f"{self.branch_prefix}/{timestamp}"
        print(f"[autofix] Creating branch {branch}")
        self._run_git(["fetch", "origin", "main"])
        self._run_git(["checkout", "main"])
        self._run_git(["reset", "--hard", "origin/main"])
        self._run_git(["checkout", "-b", branch])
        return branch

    def _apply_diff(self, diff: str) -> None:
        print("[autofix] Applying diff from LLM")
        with tempfile.NamedTemporaryFile("w", delete=False) as tmp:
            tmp.write(diff)
            tmp_path = tmp.name
        try:
            self._run_git(["apply", tmp_path])
        finally:
            os.unlink(tmp_path)

    def _commit_changes(self) -> str:
        status = self._run_git(["status", "--porcelain"], capture_output=True)
        if not status.strip():
            raise RuntimeError("No changes detected after applying diff")
        self._run_git(["config", "user.name", os.environ["GIT_AUTHOR_NAME"]])
        self._run_git(["config", "user.email", os.environ["GIT_AUTHOR_EMAIL"]])
        self._run_git(["commit", "-am", "chore: auto-remediate failing workflow"])
        sha = self._run_git(["rev-parse", "HEAD"], capture_output=True).strip()
        print(f"[autofix] Created commit {sha}")
        self._run_git(["push", "origin", "HEAD"])
        return sha

    def _open_pull_request(self, branch: str) -> PullRequest:
        print("[autofix] Opening pull request")
        data = {
            "title": "chore: auto-remediate failing workflow",
            "head": branch,
            "base": "main",
            "body": textwrap.dedent(
                """
                ## Summary
                * Automated fix produced by the pipeline remediation bot.
                * See workflow logs for context.
                """
            ).strip(),
        }
        response = self._github_api(
            "POST", f"/repos/{self.repo}/pulls", json=data
        )
        payload = response.json()
        pr = PullRequest(
            number=payload["number"],
            url=payload["html_url"],
            head_sha=payload["head"]["sha"],
            node_id=payload["node_id"],
        )
        print(f"[autofix] Opened PR #{pr.number}: {pr.url}")
        return pr

    def _enable_auto_merge(self, pr: PullRequest) -> None:
        print(f"[autofix] Enabling auto-merge for PR #{pr.number}")
        mutation = textwrap.dedent(
            """
            mutation($prId: ID!, $mergeMethod: PullRequestMergeMethod!) {
              enablePullRequestAutoMerge(input: {
                pullRequestId: $prId,
                mergeMethod: $mergeMethod
              }) {
                pullRequest { number }
              }
            }
            """
        )
        response = requests.post(
            "https://api.github.com/graphql",
            headers={
                "Authorization": f"Bearer {self.github_token}",
                "Content-Type": "application/json",
            },
            json={
                "query": mutation,
                "variables": {"prId": pr.node_id, "mergeMethod": self.auto_merge_method},
            },
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"Failed to enable auto-merge: {response.status_code} {response.text}"
            )

    def _wait_for_checks(self, sha: str) -> bool:
        print(f"[autofix] Polling checks for commit {sha}")
        while True:
            response = self._github_api(
                "GET", f"/repos/{self.repo}/commits/{sha}/check-suites"
            )
            suites = response.json().get("check_suites", [])
            if suites and all(suite.get("status") == "completed" for suite in suites):
                conclusions = {suite.get("conclusion") for suite in suites}
                success = conclusions == {"success"}
                print(f"[autofix] Check suites completed with conclusions: {conclusions}")
                return success
            print("[autofix] Checks still running; sleeping")
            time.sleep(self.poll_interval)

    def _wait_for_pr_merge(self, pr: PullRequest) -> None:
        while True:
            response = self._github_api("GET", f"/repos/{self.repo}/pulls/{pr.number}")
            if response.json().get("merged"):
                print(f"[autofix] PR #{pr.number} merged")
                return
            time.sleep(self.poll_interval)

    def _run_git(self, args: Sequence[str], capture_output: bool = False) -> str:
        result = subprocess.run(
            ["git", *args],
            check=True,
            capture_output=capture_output,
            text=True,
        )
        if capture_output:
            return result.stdout
        return ""


# ---------------------------------------------------------------------------
# Entrypoint


def validate_environment() -> Dict[str, str]:
    required_vars = {
        "GITHUB_TOKEN",
        "OPENAI_API_KEY",
        "GITHUB_REPOSITORY",
        "GIT_AUTHOR_NAME",
        "GIT_AUTHOR_EMAIL",
    }
    missing = [var for var in required_vars if not os.environ.get(var)]
    if missing:
        raise EnvironmentError(
            "Missing required environment variables: " + ", ".join(missing)
        )

    env = {var: os.environ[var] for var in required_vars}
    env["OPENAI_MODEL"] = os.environ.get("OPENAI_MODEL", "gpt-4.1")
    env["AUTO_MERGE_METHOD"] = os.environ.get("AUTO_MERGE_METHOD", "SQUASH")
    env["AUTOFIX_BRANCH_PREFIX"] = os.environ.get("AUTOFIX_BRANCH_PREFIX", "autofix")
    env["AUTOFIX_MAX_ITERATIONS"] = os.environ.get("AUTOFIX_MAX_ITERATIONS", "3")
    env["AUTOFIX_POLL_INTERVAL"] = os.environ.get("AUTOFIX_POLL_INTERVAL", "30")
    return env


def main() -> None:
    env = validate_environment()
    fixer = AutoPipelineFixer(
        repo=env["GITHUB_REPOSITORY"],
        github_token=env["GITHUB_TOKEN"],
        openai_token=env["OPENAI_API_KEY"],
        openai_model=env["OPENAI_MODEL"],
        branch_prefix=env["AUTOFIX_BRANCH_PREFIX"],
        max_iterations=int(env["AUTOFIX_MAX_ITERATIONS"]),
        poll_interval=int(env["AUTOFIX_POLL_INTERVAL"]),
        auto_merge_method=env["AUTO_MERGE_METHOD"],
    )
    fixer.run()


if __name__ == "__main__":
    main()
