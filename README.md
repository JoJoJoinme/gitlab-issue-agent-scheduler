# GitLab Issue Agent Scheduler

A long-running, issue-driven scheduler for coding-agent CLIs in internal GitLab environments.

This repository implements the P0 control plane: GitLab Issue authority, durable per-issue git worktrees, a backend-neutral `AgentBackend`, clean continuation, failure retry/backoff, active-run reconciliation and cancellation, cold-start recovery, and structured per-attempt events.

It deliberately stops at a **human review gate**. It does not merge branches or merge requests and the scheduler does not mutate GitLab issues.

## Why this exists

The scheduler makes a distinction that prompt-only loops usually blur:

| Observation | Scheduler meaning |
| --- | --- |
| Agent process exits `0`; issue is still active | Clean continuation after a short fixed delay |
| Agent process exits non-zero or times out; issue is still active | Failure retry with exponential backoff |
| Issue becomes non-active or terminal while an agent runs | Cancel the executor immediately during reconciliation |
| Scheduler restarts after a crash | Reap the recorded orphan process, re-read GitLab, and let tracker state override stale local state |
| Native session ID is available | Resume with the configured native CLI arguments |
| Native session ID is unavailable or native resume failed | Reconstruct from the current issue, worktree, git evidence, and bounded prior output |

A clean subprocess exit is never treated as proof that the objective is complete. The GitLab Issue remains authoritative.

## P0 scope

- Read-only GitLab v4 API adapter with configurable active/terminal states and labels.
- One deterministic, persistent git worktree and branch per issue.
- Generic, no-shell command backend. `custom-claude`, `csc`, Claude Code, and similar `-p` CLIs are configuration, not scheduler dependencies.
- Native resume and stateless reconstruction continuation paths.
- Separate counters and timers for clean continuation versus failures.
- Reconciliation before dispatch on every poll.
- Process birth identity (`pid` + creation time) and process-tree termination to contain orphan executors after an abrupt scheduler death.
- Atomic durable issue state and append-only JSONL event streams.
- Single-scheduler lock per state root.
- Integration tests for worktree reuse, continuation, backoff, cancellation, and crash recovery.

Not in P0: automatic merge, automatic issue transitions, a web UI, distributed leases, GitLab webhooks, or multi-project scheduling from one process.

## Requirements

- Python 3.11+
- Git 2.31+
- Network access from the scheduler host to the GitLab API and repository remote
- A non-interactive coding-agent command

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

On Windows PowerShell, activate with `.venv\Scripts\Activate.ps1`.

Copy the examples and set the token only in the environment:

```bash
cp examples/scheduler.yaml scheduler.yaml
cp examples/WORKFLOW.md WORKFLOW.md
export GITLAB_TOKEN='...'
gitlab-agent-scheduler --config scheduler.yaml --check
gitlab-agent-scheduler --config scheduler.yaml
```

The sample label policy is:

- `agent::ready`: active and dispatchable.
- `agent::human-review`: non-active; cancel/release scheduling but preserve the worktree for review.
- `agent::done` and `agent::cancelled`, or GitLab `closed`: terminal.

## Backend configuration

The default sample supports the `custom-claude`/`csc -p` shape without hard-coding Claude-specific state:

```yaml
agent:
  command: custom-claude
  args: ["-p", "{prompt}", "--output-format", "stream-json"]
  native_resume_args:
    ["--resume", "{session_id}", "-p", "{prompt}", "--output-format", "stream-json"]
  session_id_paths: ["session_id", "session.id"]
```

Use `command: csc` if that is the installed executable. If the internal CLI does not expose a resumable session ID, omit `native_resume_args`; the scheduler will always use stateless reconstruction.

Arguments are passed with `exec`, never through a shell. Supported placeholders are `{prompt}`, `{session_id}`, `{workspace}`, `{issue_id}`, and `{attempt_id}`. The launch event redacts an argument that consists of the complete prompt.

Tracker credentials are not inherited by the agent process. Add only explicitly required variables to `agent.pass_env`; prefer a narrowly scoped agent-side GitLab integration rather than passing the scheduler's read token.

## Runtime state and observability

The state root is deterministic and safe to inspect while the scheduler is stopped:

```text
.scheduler/
├── scheduler.lock
├── events.jsonl
├── repositories/<remote-hash>/
├── workspaces/<collision-safe-issue-key>/
└── issues/<collision-safe-issue-key>/
    ├── state.json
    ├── events.jsonl
    └── attempts/<attempt-uuid>/events.jsonl
```

Every event contains `event_id`, UTC `timestamp`, `event_type`, issue identity, optional `attempt_id`, and structured `details`. Important event types include:

- `attempt.started`, `attempt.process_started`, `agent.output`, `agent.exited`
- `continuation.scheduled`, `continuation.native_resume_abandoned`
- `retry.scheduled`
- `reconcile.cancel_requested`
- `cold_start.orphan_reaped`, `cold_start.reconciled`

Agent output can contain repository data. Protect the state root like build logs and set an appropriate retention policy.

## Verification

```bash
pip install -e '.[test]'
pytest -q
```

The cross-platform crash-recovery test writes the durable `RUNNING + process identity` boundary left by an abrupt scheduler death, launches a real orphan process, and verifies that cold-start reconciliation kills it before applying current tracker state. A separate POSIX end-to-end test starts the real CLI scheduler and fake GitLab, sends the scheduler `SIGKILL`, proves the executor survived, changes the issue to human review, then starts a replacement scheduler and verifies orphan cleanup plus tracker override.

## Design documents

- [Architecture and state machine](docs/ARCHITECTURE.md)
- [Example scheduler configuration](examples/scheduler.yaml)
- [Example repository workflow](examples/WORKFLOW.md)

## References and adaptation boundary

The control-plane semantics follow the [OpenAI Symphony specification](https://github.com/openai/symphony/blob/main/SPEC.md): reconciliation precedes dispatch, successful worker exit is not objective completion, active state is re-read between continuations, failure retry uses exponential backoff, and tracker/filesystem state drives restart recovery.

Implementation ideas were also compared with:

- [zxkane/autonomous-dev-team](https://github.com/zxkane/autonomous-dev-team) for GitLab provider seams, generic `-p` CLI routing, and explicit native-resume capability differences.
- [gherghett/ClaudeCodePSymphony](https://github.com/gherghett/ClaudeCodePSymphony) for a small Claude `-p` runner and cancellation flow.
- [manav03panchal/phonyhuman](https://github.com/manav03panchal/phonyhuman) for continuation on a live Claude-oriented session.

This code is not a mechanical port. It uses Python/asyncio, a backend-neutral command contract, built-in git worktrees, durable per-issue scheduler records, crash-orphan containment, GitLab authority, and a mandatory human review boundary.

## License

Apache-2.0. See [LICENSE](LICENSE).
