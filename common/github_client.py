"""
GitHub REST API client for the Data-collection-pipeline.

Responsibilities:
- Authenticate with one or more Personal Access Tokens
- Rotate tokens automatically when the rate limit is reached
- Fetch repository metadata (language, default branch, etc.)
- Fetch the latest commit on the default branch
- Retry on transient network / server errors with exponential backoff

Token configuration
-------------------
Set one or more tokens in the environment:

    # Single token (backward compatible)
    GITHUB_TOKEN=ghp_abc123

    # Multiple tokens — comma-separated (recommended for large datasets)
    GITHUB_TOKENS=ghp_abc123,ghp_def456,ghp_ghi789

If both are set, GITHUB_TOKENS takes precedence.
If neither is set, requests are unauthenticated (60 req/hour — avoid this).

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
    GITHUB_TOKENS,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data Transfer Objects
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RepoMetadata:
    full_name: str
    owner: str
    repo_name: str
    language: str | None
    default_branch: str
    description: str | None
    is_fork: bool
    is_archived: bool
    stars: int
    forks: int
    open_issues: int
    created_at: datetime
    updated_at: datetime
    extra: dict[str, Any]


@dataclass(frozen=True)
class CommitInfo:
    sha: str
    message: str
    committed_at: datetime
    author_name: str
    author_email: str


# ---------------------------------------------------------------------------
# Token pool
# ---------------------------------------------------------------------------


@dataclass
class _TokenState:
    """Tracks rate-limit state for a single GitHub token."""
    token: str
    remaining: int = 5000
    reset_at: int = 0

    @property
    def is_exhausted(self) -> bool:
        return self.remaining == 0 and int(time.time()) < self.reset_at

    @property
    def seconds_until_reset(self) -> int:
        return max(self.reset_at - int(time.time()), 0) + 5


class _TokenPool:
    """
    Manages a pool of GitHub tokens and selects the best available one.

    Selection strategy:
    - Pick the token with the highest remaining quota.
    - If all tokens are exhausted, wait for the one that resets soonest.
    """

    def __init__(self, tokens: list[str]) -> None:
        if not tokens:
            raise ValueError("At least one GitHub token must be provided.")
        self._pool = [_TokenState(token=t) for t in tokens]
        logger.info("Token pool initialised with %d token(s).", len(self._pool))

    @property
    def current(self) -> _TokenState:
        available = [t for t in self._pool if not t.is_exhausted]
        if available:
            return max(available, key=lambda t: t.remaining)
        soonest = min(self._pool, key=lambda t: t.reset_at)
        wait = soonest.seconds_until_reset
        logger.warning(
            "All %d GitHub token(s) exhausted. Waiting %d seconds for reset.",
            len(self._pool), wait,
        )
        time.sleep(wait)
        soonest.remaining = 5000
        return soonest

    def update(self, token: str, remaining: int, reset_at: int) -> None:
        for state in self._pool:
            if state.token == token:
                state.remaining = remaining
                state.reset_at = reset_at
                if remaining < 100:
                    logger.warning(
                        "Token ...%s is running low: %d requests remaining.",
                        token[-4:], remaining,
                    )
                return


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class GitHubClient:
    """
    Thin wrapper around the GitHub REST API v3 with multi-token rotation.

    Instantiate once per Airflow task; it is not thread-safe.

    Example::

        client = GitHubClient()
        meta   = client.get_repo_metadata("apache/spark")
        commit = client.get_latest_commit(meta.default_branch, "apache", "spark")
    """

    def __init__(
        self,
        tokens: list[str] | None = None,
        api_base: str = GITHUB_API_BASE,
        max_retries: int = GITHUB_API_MAX_RETRIES,
        retry_backoff: float = GITHUB_API_RETRY_BACKOFF,
    ) -> None:
        if tokens is None:
            tokens = self._resolve_tokens()
        self._pool = _TokenPool(tokens)
        self._api_base = api_base.rstrip("/")
        self._session = self._build_session(max_retries, retry_backoff)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_repo_metadata(self, full_name: str) -> RepoMetadata:
        url = f"{self._api_base}/repos/{full_name}"
        data = self._get(url)
        return RepoMetadata(
            full_name=data["full_name"],
            owner=data["owner"]["login"],
            repo_name=data["name"],
            language=data.get("language"),
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
        url = f"{self._api_base}/repos/{owner}/{repo_name}/commits/{branch}"
        data = self._get(url)
        commit = data["commit"]
        return CommitInfo(
            sha=data["sha"],
            message=commit["message"].splitlines()[0],
            committed_at=self._parse_dt(commit["committer"]["date"]),
            author_name=commit["author"]["name"],
            author_email=commit["author"]["email"],
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get(self, url: str, params: dict | None = None) -> dict:
        """
        Perform a GET request using the best available token.
        Rotates to the next token on 403/429 and retries.
        """
        num_tokens = len(self._pool._pool)
        for attempt in range(num_tokens + 1):
            token_state = self._pool.current
            self._session.headers["Authorization"] = f"Bearer {token_state.token}"

            logger.debug("GET %s (token ...%s)", url, token_state.token[-4:])
            response = self._session.get(url, params=params)

            remaining = int(response.headers.get("X-RateLimit-Remaining", token_state.remaining))
            reset_at = int(response.headers.get("X-RateLimit-Reset", token_state.reset_at))
            self._pool.update(token_state.token, remaining, reset_at)

            if response.status_code in (403, 429):
                logger.warning(
                    "Token ...%s hit rate limit. Switching token (attempt %d/%d).",
                    token_state.token[-4:], attempt + 1, num_tokens,
                )
                self._pool.update(token_state.token, 0, reset_at)
                continue

            response.raise_for_status()
            return response.json()

        raise RuntimeError(f"All tokens exhausted after {num_tokens} attempts for {url}")

    @staticmethod
    def _resolve_tokens() -> list[str]:
        multi = GITHUB_TOKENS
        if multi:
            tokens = [t.strip() for t in multi.split(",") if t.strip()]
            if tokens:
                logger.info("Loaded %d token(s) from GITHUB_TOKENS.", len(tokens))
                return tokens
        single = GITHUB_TOKEN
        if single:
            logger.info("Loaded 1 token from GITHUB_TOKEN.")
            return [single]
        logger.warning("No GitHub token configured. Heavily rate-limited (60/hour).")
        return [""]

    @staticmethod
    def _build_session(max_retries: int, backoff: float) -> requests.Session:
        session = requests.Session()
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
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)