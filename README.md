# GitLab Issue Agent Scheduler

A long-running, issue-driven scheduler for coding-agent CLIs in internal GitLab environments.

This repository implements the P0 control plane: GitLab Issue authority, local or SSH execution targets, durable per-issue git worktrees on the selected host, a backend-neutral `AgentBackend`, clean continuation, failure retry/backoff, active-run reconciliation and cancellation, cold-start recovery, and structured per-attempt events.

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
| Issue has `agent-host::build-01` | Keep scheduling on the server; create/reuse the worktree and launch `csc` on `build-01` over SSH |
| SSH disconnects and remote stop cannot be proven | Enter `BLOCKED`; do not launch a duplicate executor |

A clean subprocess exit is never treated as proof that the objective is complete. The GitLab Issue remains authoritative.

## P0 scope

- Read-only GitLab v4 API adapter with configurable active/terminal states and labels.
- One deterministic, persistent git worktree and branch per issue.
- Generic, no-shell command backend. `custom-claude`, `csc`, Claude Code, and similar `-p` CLIs are configuration, not scheduler dependencies.
- Multiple local/SSH execution targets, Issue-label placement, and a per-target concurrency limit.
- A fixed JSON-over-stdin remote helper; Issue text and prompts are never interpolated into an SSH command string.
- Durable remote attempt leases and cancellation tombstones for restart-safe remote process reaping.
- Native resume and stateless reconstruction continuation paths.
- Separate counters and timers for clean continuation versus failures.
- Reconciliation before dispatch on every poll.
- Process birth identity (`target` + `attempt ID` + `pid` + creation time) and process-tree termination to contain orphan executors after an abrupt scheduler death.
- Atomic durable issue state and append-only JSONL event streams.
- Single-scheduler lock per state root.
- Integration tests for worktree reuse, continuation, backoff, local/remote cancellation, unreachable-host fail-closed behavior, crash recovery, and a real OpenSSH round trip in Linux CI.

Not in P0: automatic merge, automatic issue transitions, a web UI, active/active scheduler HA, GitLab webhooks, or multi-project scheduling from one process.

## Requirements

- Python 3.11+
- Git 2.31+
- Network access from the scheduler host to the GitLab API
- For local execution: repository credentials and a non-interactive coding-agent command on the scheduler host
- For SSH execution: Python, this package, Git, repository credentials, and `csc`/the configured agent command on each execution host
- OpenSSH client on the scheduler server and key-based, host-key-verified access to remote targets

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

## Server control plane and SSH execution hosts

Run exactly one scheduler process on the server. Install the package on that server and on every SSH execution host. The server owns GitLab polling and durable control-plane state; the selected execution host owns the repository clone, Issue worktree, `csc` process, and remote attempt lease.

```yaml
execution:
  default_target: scheduler-local
  label_prefix: "agent-host::"
  targets:
    scheduler-local:
      kind: local
      max_concurrent_agents: 1
    build-01:
      kind: ssh
      max_concurrent_agents: 2
      remote_state_root: /var/lib/gitlab-issue-agent
      ssh:
        host: build-01.internal.example
        user: issue-agent
        identity_file: /etc/gitlab-issue-agent/ssh/id_ed25519
        known_hosts_file: /etc/gitlab-issue-agent/ssh/known_hosts
        remote_command:
          ["/opt/gitlab-issue-agent/venv/bin/python", "-m", "gitlab_issue_agent.remote_runner"]
      agent:
        command: csc
```

An active Issue with `agent-host::build-01` runs on `build-01`; an active Issue with no placement label uses `scheduler-local`. An unknown target or multiple placement labels fail closed. Each target has its own concurrency cap in addition to the scheduler-wide cap.

Provision a dedicated, non-login automation account where practical. Populate `known_hosts` ahead of time; host-key checking is strict and non-interactive. Install repository and CSC credentials on the execution host itself. The scheduler's GitLab tracker token is not sent to the remote helper.

The SSH command contains only administrator-owned configuration. A versioned JSON request carrying the Issue, prompt, and typed operation is sent on stdin to `gitlab_issue_agent.remote_runner`, which launches Git and CSC with argument arrays rather than a shell. OpenSSH necessarily represents the fixed remote helper argv as a quoted command string, but no Issue-controlled data is included in it.

Changing the placement label cancels the current executor and then creates/reuses a workspace on the new host. Native session IDs are host-scoped and are cleared on that move. Uncommitted files are not copied between targets, so the workflow should push durable checkpoints before an operator deliberately reroutes an Issue.

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

Global `repository` and `agent` values are defaults. A target may override either mapping; this is useful when the server uses `custom-claude` but an execution host exposes the same backend as `csc`.

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

For an SSH target, its configured `remote_state_root` has the execution-plane subset:

```text
/var/lib/gitlab-issue-agent/
├── repositories/<remote-hash>/
├── workspaces/<collision-safe-issue-key>/
├── leases/<attempt-uuid>.json
└── cancellations/<attempt-uuid>.json
```

The attempt-scoped prepare/run helpers lease themselves before remote Git or CSC work. During `run`, that supervisor identity is atomically replaced with the CSC process birth identity immediately after spawn. A cancellation tombstone closes the race where cancellation reaches the host just before a request creates its lease.

Every event contains `event_id`, UTC `timestamp`, `event_type`, issue identity, optional `attempt_id`, and structured `details`. Important event types include:

- `attempt.started`, `attempt.process_started`, `agent.output`, `agent.exited`
- `continuation.scheduled`, `continuation.native_resume_abandoned`
- `retry.scheduled`
- `reconcile.cancel_requested`
- `cold_start.orphan_reaped`, `cold_start.orphan_reap_unconfirmed`, `cold_start.reconciled`
- `placement.changed`, `placement.blocked`, `attempt.executor_stop_unconfirmed`

Agent output can contain repository data. Protect the state root like build logs and set an appropriate retention policy.

## Verification

```bash
pip install -e '.[test]'
pytest -q
```

The cross-platform crash-recovery tests write the durable `RUNNING + process identity` boundary left by an abrupt scheduler death, launch real local and remote-shaped orphan processes, and verify that cold-start reconciliation kills them before applying current tracker state. A separate POSIX end-to-end test starts the real CLI scheduler and fake GitLab, sends the scheduler `SIGKILL`, proves the executor survived, changes the issue to human review, then starts a replacement scheduler and verifies orphan cleanup plus tracker override.

Linux CI also starts a loopback `sshd`. The integration suite prepares a worktree through real OpenSSH, launches the remote test agent, cancels it through a second SSH control request, and requires confirmed process death. Locally that test is skipped unless the `ISSUE_AGENT_TEST_SSH_*` variables used by the workflow are supplied.

## Design documents

- [Architecture and state machine](docs/ARCHITECTURE.md)
- [Example scheduler configuration](examples/scheduler.yaml)
- [Example repository workflow](examples/WORKFLOW.md)

## References and adaptation boundary

The control-plane semantics follow the [OpenAI Symphony specification](https://github.com/openai/symphony/blob/main/SPEC.md): reconciliation precedes dispatch, successful worker exit is not objective completion, active state is re-read between continuations, failure retry uses exponential backoff, and tracker/filesystem state drives restart recovery. The transport boundary was also compared with Symphony's [OpenSSH execution module](https://github.com/openai/symphony/blob/main/elixir/lib/symphony_elixir/ssh.ex); this project adds an explicit JSON protocol, durable remote attempt leases, and fail-closed redispatch.

Implementation ideas were also compared with:

- [zxkane/autonomous-dev-team](https://github.com/zxkane/autonomous-dev-team) for GitLab provider seams, generic `-p` CLI routing, explicit native-resume capability differences, and process-tree/orphan containment test cases.
- [gherghett/ClaudeCodePSymphony](https://github.com/gherghett/ClaudeCodePSymphony) for a small Claude `-p` runner and cancellation flow.
- [manav03panchal/phonyhuman](https://github.com/manav03panchal/phonyhuman) for continuation on a live Claude-oriented session.

This code is not a mechanical port. It uses Python/asyncio, a backend-neutral command contract, built-in git worktrees, durable per-issue scheduler records, crash-orphan containment, GitLab authority, and a mandatory human review boundary.

## License

Apache-2.0. See [LICENSE](LICENSE).
