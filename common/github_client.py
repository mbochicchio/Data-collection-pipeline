"""
GitHub REST API client for the Data-collection-pipeline.

Responsibilities:
- Authenticate with a Personal Access Token
- Fetch repository metadata (language, default branch, etc.)
- Fetch the latest commit on the default branch
- Handle rate limiting gracefully (wait and retry)
- Retry on transient network / server errors with exponential backoff

This module has no Airflow dependency and can be used standalone or in tests.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config.settings import (
    GITHUB_API_BASE,
    GITHUB_API_MAX_RETRIES,
    GITHUB_API_RETRY_BACKOFF,
    GITHUB_TOKEN,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data Transfer Objects
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RepoMetadata:
    """
    Relevant fields from the GitHub repository API response.

    Only fields used by the pipeline are included; the full raw response
    is stored in ``extra`` for forward compatibility.
    """

    full_name: str
    owner: str
    repo_name: str
    language: str | None          # Primary language as reported by GitHub
    default_branch: str
    description: str | None
    is_fork: bool
    is_archived: bool
    stars: int
    forks: int
    open_issues: int
    created_at: datetime
    updated_at: datetime
    extra: dict[str, Any]         # Full raw API response for archival


@dataclass(frozen=True)
class CommitInfo:
    """
    The fields from a GitHub commit object that the pipeline needs.
    """

    sha: str                      # Full 40-character commit SHA
    message: str                  # First line of the commit message
    committed_at: datetime        # Committer date (UTC)
    author_name: str
    author_email: str


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class GitHubClient:
    """
    Thin wrapper around the GitHub REST API v3.

    Instantiate once per Airflow task; it is not thread-safe.

    Example::

        client = GitHubClient()
        meta   = client.get_repo_metadata("apache/spark")
        commit = client.get_latest_commit(meta.default_branch, "apache", "spark")
    """

    def __init__(
        self,
        token: str = GITHUB_TOKEN,
        api_base: str = GITHUB_API_BASE,
        max_retries: int = GITHUB_API_MAX_RETRIES,
        retry_backoff: float = GITHUB_API_RETRY_BACKOFF,
    ) -> None:
        self._api_base = api_base.rstrip("/")
        self._session = self._build_session(token, max_retries, retry_backoff)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_repo_metadata(self, full_name: str) -> RepoMetadata:
        """
        Fetch repository-level metadata from the GitHub API.

        Args:
            full_name: Repository identifier in the form ``owner/repo_name``.

        Returns:
            A :class:`RepoMetadata` dataclass populated from the API response.

        Raises:
            requests.HTTPError: On non-retryable HTTP errors (e.g. 404).
        """
        url = f"{self._api_base}/repos/{full_name}"
        data = self._get(url)

        return RepoMetadata(
            full_name=data["full_name"],
            owner=data["owner"]["login"],
            repo_name=data["name"],
            language=data.get("language"),          # None if GitHub cannot detect it
            default_branch=data["default_branch"],
            description=data.get("description"),
            is_fork=data.get("fork", False),
            is_archived=data.get("archived", False),
            stars=data.get("stargazers_count", 0),
            forks=data.get("forks_count", 0),
            open_issues=data.get("open_issues_count", 0),
            created_at=self._parse_dt(data["created_at"]),
            updated_at=self._parse_dt(data["updated_at"]),
            extra=data,
        )

    def get_latest_commit(self, branch: str, owner: str, repo_name: str) -> CommitInfo:
        """
        Fetch the HEAD commit of *branch* for the given repository.

        Args:
            branch:    Branch name (typically ``default_branch`` from :meth:`get_repo_metadata`).
            owner:     Repository owner login.
            repo_name: Repository name.

        Returns:
            A :class:`CommitInfo` dataclass for the HEAD commit.

        Raises:
            requests.HTTPError: On non-retryable HTTP errors.
        """
        url = f"{self._api_base}/repos/{owner}/{repo_name}/commits/{branch}"
        data = self._get(url)

        commit = data["commit"]
        # Use committer date as the canonical timestamp (matches git log default)
        committed_at_raw = commit["committer"]["date"]

        return CommitInfo(
            sha=data["sha"],
            message=commit["message"].splitlines()[0],  # first line only
            committed_at=self._parse_dt(committed_at_raw),
            author_name=commit["author"]["name"],
            author_email=commit["author"]["email"],
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get(self, url: str, params: dict | None = None) -> dict:
        """
        Perform a GET request, handle rate limiting, and return parsed JSON.

        GitHub rate-limit headers are inspected on every response.
        If the remaining quota is zero the method sleeps until the reset time
        before retrying (instead of burning retries on 403/429 responses).
        """
        logger.debug("GET %s", url)
        response = self._session.get(url, params=params)

        # Primary rate limit: X-RateLimit-Remaining hits 0
        if response.status_code in (403, 429):
            reset_ts = int(response.headers.get("X-RateLimit-Reset", 0))
            if reset_ts:
                wait = max(reset_ts - int(time.time()), 0) + 5  # +5s buffer
                logger.warning(
                    "GitHub rate limit reached. Sleeping %s seconds until reset.", wait
                )
                time.sleep(wait)
                response = self._session.get(url, params=params)

        response.raise_for_status()
        return response.json()

    @staticmethod
    def _build_session(token: str, max_retries: int, backoff: float) -> requests.Session:
        """
        Build a requests Session with auth headers and an automatic retry policy.

        Retries are attempted on connection errors and 5xx responses.
        403/429 (rate limit) are handled separately in :meth:`_get`.
        """
        session = requests.Session()

        if token:
            session.headers["Authorization"] = f"Bearer {token}"
        else:
            logger.warning(
                "No GITHUB_TOKEN configured. API requests will be heavily rate-limited."
            )

        session.headers["Accept"] = "application/vnd.github+json"
        session.headers["X-GitHub-Api-Version"] = "2022-11-28"

        retry_policy = Retry(
            total=max_retries,
            backoff_factor=backoff,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry_policy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        return session

    @staticmethod
    def _parse_dt(value: str) -> datetime:
        """Parse an ISO-8601 datetime string from the GitHub API into a UTC datetime."""
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
