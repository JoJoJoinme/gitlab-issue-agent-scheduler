from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path

from .config import RepositoryConfig
from .models import Issue, WorkspaceInfo
from .state import workspace_key


class WorkspaceError(RuntimeError):
    pass


class WorkspaceManager:
    """Creates one durable git worktree per GitLab issue."""

    def __init__(self, state_root: Path, config: RepositoryConfig) -> None:
        self.state_root = state_root
        self.config = config
        repo_digest = hashlib.sha256(config.clone_url.encode("utf-8")).hexdigest()[:16]
        self.control_repo = state_root / "repositories" / repo_digest
        self.workspaces_root = state_root / "workspaces"
        self._lock = asyncio.Lock()

    async def ensure(self, issue: Issue) -> WorkspaceInfo:
        async with self._lock:
            await self._ensure_control_repo()
            path = self.workspaces_root / workspace_key(issue.identifier)
            branch = f"{self.config.branch_prefix}{issue.iid}"
            if path.exists():
                if not await self._is_worktree(path):
                    raise WorkspaceError(f"workspace path exists but is not a git worktree: {path}")
                current_branch = await self._git(
                    "rev-parse", "--abbrev-ref", "HEAD", cwd=path, check=False
                )
                return WorkspaceInfo(
                    path=path,
                    branch=current_branch.strip() or branch,
                    created_now=False,
                )

            self.workspaces_root.mkdir(parents=True, exist_ok=True)
            if self.config.fetch_on_create:
                await self._git(
                    "fetch", "--prune", "origin", self.config.default_branch, cwd=self.control_repo
                )
            await self._git("worktree", "prune", cwd=self.control_repo)
            await self._git(
                "worktree",
                "add",
                "--force",
                "-B",
                branch,
                str(path),
                f"origin/{self.config.default_branch}",
                cwd=self.control_repo,
            )
            return WorkspaceInfo(path=path, branch=branch, created_now=True)

    async def snapshot(self, path: Path, *, max_chars: int = 12000) -> str:
        if not path.exists() or not await self._is_worktree(path):
            return "workspace is absent or not a git worktree"
        commands = [
            ("branch", "rev-parse", "--abbrev-ref", "HEAD"),
            ("head", "log", "-1", "--pretty=format:%h %s"),
            ("status", "status", "--short", "--branch"),
            ("diff_stat", "diff", "--stat"),
            ("staged_diff_stat", "diff", "--cached", "--stat"),
        ]
        sections: list[str] = []
        for label, *args in commands:
            output = await self._git(*args, cwd=path, check=False)
            sections.append(f"[{label}]\n{output.strip() or '(empty)'}")
        return "\n\n".join(sections)[:max_chars]

    async def _ensure_control_repo(self) -> None:
        if (self.control_repo / ".git").exists():
            await self._git(
                "remote", "set-url", "origin", self.config.clone_url, cwd=self.control_repo
            )
            return
        if self.control_repo.exists():
            raise WorkspaceError(
                f"control repository path exists but is not a git repository: {self.control_repo}"
            )
        self.control_repo.parent.mkdir(parents=True, exist_ok=True)
        await self._git(
            "clone", "--no-checkout", self.config.clone_url, str(self.control_repo), cwd=None
        )

    async def _is_worktree(self, path: Path) -> bool:
        output = await self._git("rev-parse", "--is-inside-work-tree", cwd=path, check=False)
        return output.strip().casefold() == "true"

    async def _git(
        self,
        *args: str,
        cwd: Path | None,
        check: bool = True,
        timeout_seconds: float = 180.0,
    ) -> str:
        try:
            process = await asyncio.create_subprocess_exec(
                "git",
                *args,
                cwd=str(cwd) if cwd else None,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except OSError as error:
            raise WorkspaceError(f"cannot launch git: {error}") from error
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout_seconds)
        except TimeoutError as error:
            process.kill()
            await process.wait()
            raise WorkspaceError(f"git command timed out: git {' '.join(args)}") from error
        output = stdout.decode("utf-8", errors="replace")
        error_output = stderr.decode("utf-8", errors="replace")
        if check and process.returncode != 0:
            raise WorkspaceError(
                f"git {' '.join(args[:3])} failed with exit {process.returncode}: "
                f"{error_output[-2000:]}"
            )
        return output if process.returncode == 0 else error_output
