"""Automation utility to iteratively fix failing GitHub Actions pipelines using an LLM.

This script orchestrates the following workflow:

1. Detect the most recent failed workflow run on the repository's main branch.
2. Download and summarize the failing logs via a large language model (LLM).
3. Ask the LLM to generate a unified diff patch that addresses the failure.
4. Clone the repository, apply the patch on a dedicated branch, push the branch,
   and open a pull request.
5. Enable auto-merge on the pull request and poll GitHub until all required
   checks succeed.
6. If the fix does not resolve the failure, the process repeats with the new
   error output (up to a caller-provided iteration limit).

The implementation purposely avoids direct network or subprocess side effects
outside of the GitHub and OpenAI APIs so it can be tested deterministically.
Actual execution requires exporting the following environment variables:

* ``GITHUB_TOKEN`` – Personal access token with ``repo`` and ``workflow`` scopes.
* ``OPENAI_API_KEY`` – API key for the target LLM provider.

Usage example::

    python scripts/auto_fix_pipeline.py --owner my-org --repo my-repo \
        --working-dir /tmp/workspace

The module is intentionally written without relying on asynchronous I/O to keep
it easy to integrate in existing automation environments such as GitHub
Actions, Jenkins, or custom cron jobs.
"""
from __future__ import annotations

import argparse
import dataclasses
import io
import logging
import os
import pathlib
import shutil
import tempfile
import textwrap
import time
from typing import Dict, Iterable, List, Optional

import requests
from openai import OpenAI
from zipfile import ZipFile

LOGGER = logging.getLogger(__name__)
DEFAULT_POLL_INTERVAL = 30  # seconds
DEFAULT_MAX_ITERATIONS = 5


@dataclasses.dataclass
class WorkflowRun:
    """Representation of a GitHub Actions workflow run."""

    id: int
    head_sha: str
    html_url: str
    status: str
    conclusion: Optional[str]
    name: str


@dataclasses.dataclass
class PullRequest:
    """Representation of a GitHub pull request."""

    number: int
    html_url: str
    head_sha: str


class GitHubClient:
    """Small helper around the GitHub REST API."""

    api_root = "https://api.github.com"

    def __init__(self, token: str, owner: str, repo: str) -> None:
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {token}",
                "User-Agent": "vertica-auto-fixer",
            }
        )
        self.owner = owner
        self.repo = repo

    # -- Workflow run helpers -------------------------------------------------
    def latest_failed_run(self) -> Optional[WorkflowRun]:
        """Return the most recent failed run on the main branch."""
        url = f"{self.api_root}/repos/{self.owner}/{self.repo}/actions/runs"
        params = {"branch": "main", "status": "failure", "per_page": 1}
        LOGGER.debug("Fetching latest failed workflow run: %s", url)
        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        runs = data.get("workflow_runs", [])
        if not runs:
            return None
        run = runs[0]
        return WorkflowRun(
            id=run["id"],
            head_sha=run["head_sha"],
            html_url=run["html_url"],
            status=run["status"],
            conclusion=run.get("conclusion"),
            name=run["name"],
        )

    def download_logs(self, run_id: int) -> bytes:
        """Download the compressed logs for a workflow run."""
        url = f"{self.api_root}/repos/{self.owner}/{self.repo}/actions/runs/{run_id}/logs"
        LOGGER.debug("Downloading logs for run %s", run_id)
        response = self.session.get(url, timeout=60)
        response.raise_for_status()
        return response.content

    def open_pull_request(self, title: str, body: str, head: str, base: str = "main") -> PullRequest:
        """Open a pull request from head → base."""
        url = f"{self.api_root}/repos/{self.owner}/{self.repo}/pulls"
        payload = {"title": title, "body": body, "head": head, "base": base}
        LOGGER.debug("Opening PR with payload: %s", payload)
        response = self.session.post(url, json=payload, timeout=30)
        response.raise_for_status()
        pr = response.json()
        head_data = pr["head"]
        return PullRequest(number=pr["number"], html_url=pr["html_url"], head_sha=head_data["sha"])

    def enable_auto_merge(self, pr_number: int, merge_method: str = "squash") -> None:
        """Enable auto-merge for the given PR via the GraphQL API."""
        url = f"{self.api_root}/graphql"
        query = textwrap.dedent(
            """
            mutation(
              $nodeId: ID!,
              $method: PullRequestMergeMethod!
            ) {
              enablePullRequestAutoMerge(
                input: {
                  pullRequestId: $nodeId,
                  mergeMethod: $method
                }
              ) {
                pullRequest {
                  number
                }
              }
            }
            """
        )
        # Fetch PR node ID first
        pr_url = f"{self.api_root}/repos/{self.owner}/{self.repo}/pulls/{pr_number}"
        pr_response = self.session.get(pr_url, timeout=30)
        pr_response.raise_for_status()
        node_id = pr_response.json()["node_id"]
        payload = {
            "query": query,
            "variables": {
                "method": merge_method.upper(),
                "nodeId": node_id,
            },
        }
        LOGGER.debug("Enabling auto-merge for PR #%s", pr_number)
        response = self.session.post(url, json=payload, timeout=30)
        response.raise_for_status()

    def list_check_runs(self, sha: str) -> List[Dict[str, str]]:
        url = f"{self.api_root}/repos/{self.owner}/{self.repo}/commits/{sha}/check-runs"
        LOGGER.debug("Listing check runs for SHA %s", sha)
        response = self.session.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data.get("check_runs", [])

    def merge_pull_request(self, pr_number: int, merge_method: str = "squash") -> None:
        url = f"{self.api_root}/repos/{self.owner}/{self.repo}/pulls/{pr_number}/merge"
        payload = {"merge_method": merge_method}
        LOGGER.debug("Merging PR #%s", pr_number)
        response = self.session.put(url, json=payload, timeout=30)
        response.raise_for_status()


class LLMClient:
    """Wrapper around the OpenAI client for summarization and patch generation."""

    def __init__(self, api_key: str, model: str = "gpt-4.1-mini") -> None:
        self.client = OpenAI(api_key=api_key)
        self.model = model

    def summarize_logs(self, log_excerpt: str) -> str:
        messages = [
            {"role": "system", "content": "Summarize the GitHub Actions failure log."},
            {"role": "user", "content": log_excerpt},
        ]
        LOGGER.debug("Requesting log summary from LLM")
        response = self.client.chat.completions.create(model=self.model, messages=messages, max_tokens=256)
        return response.choices[0].message.content.strip()

    def propose_patch(self, summary: str, repo_overview: str) -> str:
        prompt = textwrap.dedent(
            f"""
            You are an automated code fixing agent. Based on the following failing
            workflow summary and repository context, produce a unified diff patch
            (only the diff, nothing else). Apply best practices, include tests
            when necessary, and ensure the patch can be applied with ``git apply``.

            Failure summary:
            {summary}

            Repository context:
            {repo_overview}
            """
        )
        messages = [
            {"role": "system", "content": "Return only valid unified diff."},
            {"role": "user", "content": prompt},
        ]
        LOGGER.debug("Requesting patch from LLM")
        response = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            max_tokens=2048,
            temperature=0.2,
        )
        patch = response.choices[0].message.content.strip()
        return patch


class RepositoryManager:
    """Handle Git operations in a temporary workspace."""

    def __init__(self, repo_url: str, working_dir: pathlib.Path) -> None:
        self.repo_url = repo_url
        self.working_dir = working_dir

    def clone(self) -> pathlib.Path:
        if self.working_dir.exists():
            shutil.rmtree(self.working_dir)
        self.working_dir.mkdir(parents=True)
        LOGGER.debug("Cloning %s into %s", self.repo_url, self.working_dir)
        run_git(["clone", self.repo_url, "repo"], cwd=self.working_dir)
        return self.working_dir / "repo"

    def create_branch(self, repo_path: pathlib.Path, branch_name: str) -> None:
        run_git(["checkout", "-b", branch_name], cwd=repo_path)

    def apply_patch(self, repo_path: pathlib.Path, patch: str) -> None:
        LOGGER.debug("Applying patch to repository")
        run_git(["apply", "-"], cwd=repo_path, input_data=patch.encode("utf-8"))

    def commit_all(self, repo_path: pathlib.Path, message: str) -> None:
        run_git(["add", "-A"], cwd=repo_path)
        run_git(["commit", "-m", message], cwd=repo_path)

    def push(self, repo_path: pathlib.Path, branch_name: str, force: bool = False) -> None:
        args = ["push", "origin", branch_name]
        if force:
            args.insert(2, "--force")
        run_git(args, cwd=repo_path)


def run_git(args: Iterable[str], cwd: pathlib.Path, input_data: Optional[bytes] = None) -> None:
    """Run a git command with logging."""
    from subprocess import PIPE, run  # Local import to avoid global dependency.

    cmd = ["git", *args]
    LOGGER.debug("Running command: %s", " ".join(cmd))
    completed = run(cmd, cwd=str(cwd), input=input_data, stdout=PIPE, stderr=PIPE)
    if completed.returncode != 0:
        raise RuntimeError(
            f"Git command failed: {' '.join(cmd)}\nSTDOUT: {completed.stdout.decode()}\nSTDERR: {completed.stderr.decode()}"
        )


def extract_log_excerpt(archive_bytes: bytes, limit: int = 4000) -> str:
    """Extract a concise log excerpt from a workflow log archive."""
    with tempfile.TemporaryDirectory() as tmpdir:
        archive_path = pathlib.Path(tmpdir) / "logs.zip"
        archive_path.write_bytes(archive_bytes)
        buffer = io.StringIO()
        with ZipFile(archive_path) as archive:
            members = sorted(archive.infolist(), key=lambda m: m.file_size, reverse=True)
            for member in members:
                if member.is_dir():
                    continue
                with archive.open(member) as extracted:
                    chunk = extracted.read(limit)
                    buffer.write(chunk.decode("utf-8", errors="replace"))
                    if buffer.tell() >= limit:
                        break
        return buffer.getvalue()[:limit]


def poll_checks(client: GitHubClient, sha: str, interval: int = DEFAULT_POLL_INTERVAL, timeout: int = 3600) -> bool:
    """Poll check runs for a commit SHA until completion."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        runs = client.list_check_runs(sha)
        if not runs:
            LOGGER.debug("No check runs yet for %s; sleeping", sha)
            time.sleep(interval)
            continue
        statuses = {run["status"] for run in runs}
        conclusions = {run.get("conclusion") for run in runs if run.get("conclusion")}
        LOGGER.debug("Statuses: %s | Conclusions: %s", statuses, conclusions)
        if statuses == {"completed"}:
            if all(conclusion == "success" for conclusion in conclusions):
                return True
            if any(conclusion == "failure" for conclusion in conclusions):
                return False
        time.sleep(interval)
    raise TimeoutError(f"Timed out waiting for checks on {sha}")


def generate_repo_overview(repo_path: pathlib.Path, max_files: int = 50) -> str:
    """Produce a lightweight overview of the repository structure."""
    files: List[str] = []
    for path in repo_path.rglob("*"):
        if path.is_file() and ".git" not in path.parts:
            files.append(str(path.relative_to(repo_path)))
        if len(files) >= max_files:
            break
    return "\n".join(files)


def auto_fix_pipeline(owner: str, repo: str, repo_url: str, working_dir: pathlib.Path, *,
                      model: str = "gpt-4.1-mini", max_iterations: int = DEFAULT_MAX_ITERATIONS,
                      poll_interval: int = DEFAULT_POLL_INTERVAL) -> None:
    """Top-level orchestration function."""
    github_token = os.environ.get("GITHUB_TOKEN")
    openai_key = os.environ.get("OPENAI_API_KEY")
    if not github_token or not openai_key:
        raise EnvironmentError("GITHUB_TOKEN and OPENAI_API_KEY must be set")

    github = GitHubClient(github_token, owner, repo)
    llm = LLMClient(openai_key, model=model)
    repo_manager = RepositoryManager(repo_url, working_dir)

    for iteration in range(1, max_iterations + 1):
        LOGGER.info("Iteration %s/%s", iteration, max_iterations)
        run = github.latest_failed_run()
        if not run:
            LOGGER.info("No failing workflow runs detected. Exiting.")
            return

        LOGGER.info("Found failing run %s (%s)", run.id, run.html_url)
        log_archive = github.download_logs(run.id)
        log_excerpt = extract_log_excerpt(log_archive)
        summary = llm.summarize_logs(log_excerpt)
        LOGGER.info("LLM summary: %s", summary)

        repo_path = repo_manager.clone()
        overview = generate_repo_overview(repo_path)
        patch = llm.propose_patch(summary, overview)
        LOGGER.debug("Proposed patch:\n%s", patch)

        branch_name = f"auto-fix/{int(time.time())}"
        repo_manager.create_branch(repo_path, branch_name)
        repo_manager.apply_patch(repo_path, patch)
        repo_manager.commit_all(repo_path, f"Automated fix for failing workflow ({run.name})")
        repo_manager.push(repo_path, branch_name)

        pr_title = f"Automated fix for workflow failure {run.id}"
        pr_body = textwrap.dedent(
            f"""
            ## Summary
            - Automated fix attempt for workflow run [{run.id}]({run.html_url}).
            - Failure summary provided by LLM:
              > {summary}

            Generated by ``scripts/auto_fix_pipeline.py``.
            """
        )
        pr = github.open_pull_request(pr_title, pr_body, head=branch_name)
        LOGGER.info("Opened PR %s", pr.html_url)

        try:
            github.enable_auto_merge(pr.number)
        except requests.HTTPError as exc:
            LOGGER.warning("Failed to enable auto-merge: %s", exc)

        checks_passed = poll_checks(github, pr.head_sha, interval=poll_interval)
        if checks_passed:
            LOGGER.info("Checks succeeded; merging PR #%s", pr.number)
            github.merge_pull_request(pr.number)
            LOGGER.info("Pipeline stabilized; exiting")
            return

        LOGGER.warning("Checks failed for PR #%s; continuing iteration", pr.number)

    raise RuntimeError("Exceeded maximum iterations without stabilizing the pipeline")


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--owner", required=True, help="GitHub repository owner")
    parser.add_argument("--repo", required=True, help="GitHub repository name")
    parser.add_argument("--repo-url", required=True, help="Git clone URL")
    parser.add_argument("--working-dir", type=pathlib.Path, required=True, help="Temporary working directory")
    parser.add_argument("--model", default="gpt-4.1-mini", help="LLM model identifier")
    parser.add_argument("--max-iterations", type=int, default=DEFAULT_MAX_ITERATIONS)
    parser.add_argument("--poll-interval", type=int, default=DEFAULT_POLL_INTERVAL)
    parser.add_argument("--log-level", default="INFO", help="Logging level (INFO, DEBUG, ...)")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))
    auto_fix_pipeline(
        owner=args.owner,
        repo=args.repo,
        repo_url=args.repo_url,
        working_dir=args.working_dir,
        model=args.model,
        max_iterations=args.max_iterations,
        poll_interval=args.poll_interval,
    )


if __name__ == "__main__":
    main()
