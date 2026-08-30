from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol
from urllib.parse import quote

import httpx

from .config import GitLabConfig
from .models import Issue, IssueDisposition


class TrackerError(RuntimeError):
    pass


class IssueTracker(Protocol):
    async def list_active_issues(self) -> list[Issue]: ...

    async def get_issue(self, issue_id: str) -> Issue | None: ...

    def disposition(self, issue: Issue | None) -> IssueDisposition: ...

    async def close(self) -> None: ...


def _normalized(values: Iterable[str]) -> set[str]:
    return {value.strip().casefold() for value in values if value.strip()}


class GitLabIssueTracker:
    """Read-only GitLab adapter. The scheduler never mutates issues or merge requests."""

    def __init__(self, config: GitLabConfig, *, client: httpx.AsyncClient | None = None) -> None:
        self.config = config
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            base_url=f"{config.base_url}/api/v4/",
            headers={"PRIVATE-TOKEN": config.token},
            timeout=config.request_timeout_seconds,
        )
        self._project_path = quote(config.project, safe="")
        self._active_states = _normalized(config.active_states)
        self._terminal_states = _normalized(config.terminal_states)
        self._required_labels = _normalized(config.required_labels)
        self._active_labels = _normalized(config.active_labels)
        self._terminal_labels = _normalized(config.terminal_labels)

    def disposition(self, issue: Issue | None) -> IssueDisposition:
        if issue is None:
            return IssueDisposition.MISSING
        state = issue.state.strip().casefold()
        labels = _normalized(issue.labels)
        if state in self._terminal_states or labels.intersection(self._terminal_labels):
            return IssueDisposition.TERMINAL
        if state not in self._active_states:
            return IssueDisposition.NON_ACTIVE
        if not self._required_labels.issubset(labels):
            return IssueDisposition.NON_ACTIVE
        if self._active_labels and not labels.intersection(self._active_labels):
            return IssueDisposition.NON_ACTIVE
        return IssueDisposition.ACTIVE

    async def list_active_issues(self) -> list[Issue]:
        issues: list[Issue] = []
        page = 1
        while True:
            params: dict[str, str | int] = {
                "scope": "all",
                "state": "opened",
                "per_page": self.config.per_page,
                "page": page,
                "order_by": "created_at",
                "sort": "asc",
            }
            if self.config.required_labels:
                params["labels"] = ",".join(self.config.required_labels)
            response = await self._request(
                "GET", f"projects/{self._project_path}/issues", params=params
            )
            payload = response.json()
            if not isinstance(payload, list):
                raise TrackerError("GitLab issue list returned a non-list response")
            issues.extend(self._normalize(item) for item in payload)
            next_page = response.headers.get("X-Next-Page", "").strip()
            if not next_page:
                break
            page = int(next_page)
        return [issue for issue in issues if self.disposition(issue) is IssueDisposition.ACTIVE]

    async def get_issue(self, issue_id: str) -> Issue | None:
        try:
            project, iid_text = issue_id.rsplit(":", 1)
            iid = int(iid_text)
        except (ValueError, TypeError) as error:
            raise TrackerError(f"invalid scheduler issue id {issue_id!r}") from error
        if project != self.config.project:
            raise TrackerError(
                f"issue {issue_id!r} belongs to project {project!r}, expected {self.config.project!r}"
            )
        response = await self._request(
            "GET", f"projects/{self._project_path}/issues/{iid}", allow_not_found=True
        )
        if response is None:
            return None
        payload = response.json()
        if not isinstance(payload, dict):
            raise TrackerError("GitLab issue response is not an object")
        return self._normalize(payload)

    async def _request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, str | int] | None = None,
        allow_not_found: bool = False,
    ) -> httpx.Response | None:
        try:
            response = await self._client.request(method, url, params=params)
        except httpx.HTTPError as error:
            raise TrackerError(f"GitLab request failed: {error}") from error
        if allow_not_found and response.status_code == 404:
            return None
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as error:
            raise TrackerError(
                f"GitLab request failed with HTTP {response.status_code}: {response.text[:500]}"
            ) from error
        return response

    def _normalize(self, payload: dict[str, object]) -> Issue:
        try:
            iid = int(payload["iid"])
            title = str(payload["title"])
            state = str(payload["state"])
        except (KeyError, TypeError, ValueError) as error:
            raise TrackerError(f"GitLab issue is missing required fields: {error}") from error
        labels_value = payload.get("labels") or []
        if not isinstance(labels_value, list):
            labels_value = []
        identifier = f"{self.config.project}#{iid}"
        return Issue(
            id=f"{self.config.project}:{iid}",
            project_id=self.config.project,
            iid=iid,
            identifier=identifier,
            title=title,
            description=(
                str(payload["description"]) if payload.get("description") is not None else None
            ),
            state=state,
            labels=tuple(str(label) for label in labels_value),
            web_url=str(payload["web_url"]) if payload.get("web_url") else None,
            updated_at=str(payload["updated_at"]) if payload.get("updated_at") else None,
            raw=dict(payload),
        )

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()
