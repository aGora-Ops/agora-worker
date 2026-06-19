import base64
import io
import zipfile

import httpx

class GitHubRemediationClient:
    """Synchronous GitHub API client used by the Celery remediation worker."""

    BASE_URL = "https://api.github.com"

    def __init__(self, token: str) -> None:
        self._token = token
        self._client = httpx.Client(
            base_url=self.BASE_URL,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            timeout=60.0,
            follow_redirects=True,
        )

    def _get(self, path: str, **kwargs) -> dict | list:
        response = self._client.get(path, **kwargs)
        response.raise_for_status()
        return response.json()

    def _post(self, path: str, **kwargs) -> dict:
        response = self._client.post(path, **kwargs)
        response.raise_for_status()
        return response.json()

    def _put(self, path: str, **kwargs) -> dict:
        response = self._client.put(path, **kwargs)
        response.raise_for_status()
        return response.json()

    def get_org_repos(self, org: str, per_page: int = 100) -> list[dict]:
        """Return all repositories for an organization, handling pagination."""
        repos: list[dict] = []
        page = 1
        while True:
            batch = self._get(
                f"/orgs/{org}/repos",
                params={"per_page": per_page, "page": page, "type": "all"},
            )
            if not batch:
                break
            repos.extend(batch)
            if len(batch) < per_page:
                break
            page += 1
        return repos

    def get_repo_runs(
        self, owner: str, repo: str, per_page: int = 100, page: int = 1
    ) -> tuple[list[dict], int | None]:
        """
        Return one page of workflow runs for a repo (all workflows, most recent
        first) plus the remaining GitHub API rate limit from the response headers
        (None if the header is absent).
        """
        response = self._client.get(
            f"/repos/{owner}/{repo}/actions/runs",
            params={"per_page": per_page, "page": page},
        )
        response.raise_for_status()
        data = response.json()
        remaining = response.headers.get("X-RateLimit-Remaining")
        return data.get("workflow_runs", []), int(remaining) if remaining is not None else None

    def get_run_logs(self, owner: str, repo: str, run_id: int) -> str:
        """
        Download the log archive for a workflow run and return the last 300 lines
        of concatenated log text.
        """
        response = self._client.get(
            f"/repos/{owner}/{repo}/actions/runs/{run_id}/logs",
            follow_redirects=True,
        )
        response.raise_for_status()

        zip_bytes = io.BytesIO(response.content)
        all_lines: list[str] = []

        try:
            with zipfile.ZipFile(zip_bytes) as zf:
                for name in sorted(zf.namelist()):
                    if name.endswith(".txt"):
                        with zf.open(name) as f:
                            content = f.read().decode("utf-8", errors="replace")
                            all_lines.extend(content.splitlines())
        except zipfile.BadZipFile:
            all_lines = response.text.splitlines()

        last_300 = all_lines[-300:] if len(all_lines) > 300 else all_lines
        return "\n".join(last_300)

    def get_workflow_yaml(self, owner: str, repo: str, path: str, ref: str) -> str:
        """Return the raw YAML content of a workflow file."""
        response = self._client.get(
            f"/repos/{owner}/{repo}/contents/{path}",
            params={"ref": ref},
            headers={
                "Authorization": f"Bearer {self._token}",
                "Accept": "application/vnd.github.raw+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        response.raise_for_status()
        return response.text

    def get_file_sha(self, owner: str, repo: str, path: str, ref: str) -> str | None:
        """Return the blob SHA of a file at the given ref, or None if not found."""
        try:
            data = self._get(f"/repos/{owner}/{repo}/contents/{path}", params={"ref": ref})
            if isinstance(data, dict):
                return data.get("sha")
            return None
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                return None
            raise

    def create_fix_branch(
        self, owner: str, repo: str, base_sha: str, branch_name: str
    ) -> None:
        """Create a new branch from base_sha."""
        self._post(
            f"/repos/{owner}/{repo}/git/refs",
            json={"ref": f"refs/heads/{branch_name}", "sha": base_sha},
        )

    def commit_fix(
        self,
        owner: str,
        repo: str,
        branch: str,
        path: str,
        content: str,
        message: str,
        current_sha: str | None,
    ) -> None:
        """
        Create or update a file in the repository using the Contents API.

        Args:
            owner: Repository owner.
            repo: Repository name.
            branch: Target branch name.
            path: File path within the repo (e.g. '.github/workflows/ci.yml').
            content: New file content as a plain string.
            message: Commit message.
            current_sha: Existing file blob SHA (required when updating; None for new files).
        """
        encoded_content = base64.b64encode(content.encode("utf-8")).decode("utf-8")
        payload: dict = {
            "message": message,
            "content": encoded_content,
            "branch": branch,
        }
        if current_sha:
            payload["sha"] = current_sha

        self._put(f"/repos/{owner}/{repo}/contents/{path}", json=payload)

    def create_pr(
        self,
        owner: str,
        repo: str,
        head: str,
        base: str,
        title: str,
        body: str,
    ) -> dict:
        """Open a pull request and return the PR object."""
        return self._post(
            f"/repos/{owner}/{repo}/pulls",
            json={
                "title": title,
                "body": body,
                "head": head,
                "base": base,
                "maintainer_can_modify": True,
            },
        )

    def close(self) -> None:
        self._client.close()
