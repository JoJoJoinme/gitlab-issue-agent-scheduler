# Architecture and control-plane semantics

## Invariants

1. **GitLab is authoritative.** A local `RUNNING`, `RETRY_WAIT`, or `RELEASED` value is only scheduler bookkeeping. Current issue state and labels decide whether work may run.
2. **One scheduling authority.** Only `Orchestrator` changes dispatch, retry, continuation, and release state. A filesystem lock prevents two scheduler processes from sharing a state root.
3. **One durable worktree per issue.** Agent execution is always rooted in the issue worktree. Reuse preserves uncommitted work and local evidence across attempts and restarts.
4. **Exit is not completion.** Exit code `0` is a clean turn boundary. If the issue remains active, the next action is continuation, not release.
5. **Continuation is not retry.** Clean continuations use a fixed short delay and reset failure count. Failures and timeouts increment a separate counter and use exponential backoff.
6. **Reconciliation cancels.** If a running issue becomes terminal, non-active, missing, or no longer routable, the executor is signalled immediately and its matching process tree is terminated.
7. **No automatic merge.** The workflow may ask the agent to push a branch and open/update an MR, but the scheduler never merges. `agent::human-review` is intentionally non-active rather than terminal, preserving the worktree.
8. **The server is the control plane.** GitLab polling, placement, retry, continuation, and durable Issue state remain on the scheduler server. Git, worktrees, and CSC may run locally or on a selected SSH execution host.
9. **Unknown executor state is not retryable.** SSH loss does not prove that CSC stopped. Redispatch is forbidden while remote reaping is unconfirmed; the Issue stays `BLOCKED` even if tracker state has become terminal.
10. **Placement is explicit and fail closed.** At most one `agent-host::<target>` label is allowed. Unknown or conflicting targets do not fall back silently.

## Components

```text
GitLab Issues (authority)
        │ list + exact refresh
        ▼
GitLabIssueTracker ── normalized Issue + disposition
        │
        ▼
Orchestrator ── owns running map, placement, continuation, retry, reconciliation
   │        │
   │        ├── StateStore ── atomic issue state
   │        └── EventSink ── global / issue / attempt JSONL
   │
   └── ExecutionRouter ── Issue label → target + per-target concurrency
          ├── local target
          │      ├── WorkspaceManager
          │      └── CommandAgentBackend
          └── SSH target
                 ├── OpenSSH transport ── fixed helper command + JSON stdin/stdout
                 └── remote_runner
                        ├── WorkspaceManager on execution host
                        ├── CommandAgentBackend / csc on execution host
                        └── attempt lease + cancellation tombstone
```

`IssueTracker` and `ExecutionRouter` are injected into the orchestrator. Each target still exposes the same workspace and `AgentBackend` seams, so local and SSH execution share scheduling semantics. Tests use those seams to drive deterministic control-plane races and Linux CI exercises the real OpenSSH transport.

## Attempt state machine

```text
READY
  │ dispatch
  ▼
RUNNING
  ├── tracker non-active/terminal/missing ── cancel ──► RELEASED
  ├── target label changes ── cancel old target ─────► READY on new target
  ├── clean exit + tracker active ───────────────────► CONTINUATION_WAIT
  ├── failure/timeout + tracker active ──────────────► RETRY_WAIT
  ├── remote stop unconfirmed ───────────────────────► BLOCKED
  └── any exit + tracker non-active/terminal ────────► RELEASED

CONTINUATION_WAIT ── fixed delay ──► RUNNING
RETRY_WAIT ── exponential backoff ─► RUNNING
RELEASED ── tracker reactivated ───► READY
BLOCKED ── reap confirmed + tracker active ──────────► READY
BLOCKED ── reap confirmed + tracker non-active ─────► RELEASED
```

The clean-continuation cap is a fairness yield, not a failure: every configured number of consecutive clean turns, the issue waits for `continuation.yield_seconds` before it may claim another slot.

## Continuation routes

The scheduler selects a route per attempt:

1. `first`: full workflow plus authoritative issue snapshot.
2. `native_resume`: used only when the backend reports a session ID and the configuration supplies `native_resume_args`. It sends concise continuation guidance to the existing session.
3. `stateless_reconstruction`: full workflow and refreshed issue plus bounded prior output and a fresh git snapshot. The prompt tells the agent to verify the worktree rather than trust the summary.

If a native-resume attempt fails, its saved session ID is cleared before retry so the next attempt takes the stateless path instead of looping on a corrupt or expired session.

Native sessions are scoped to an execution target. When an Issue moves from one target label to another, the scheduler cancels and confirms the old executor, clears the session ID, and takes the stateless route in the new target's durable worktree.

## Placement and transport boundary

`ExecutionRouter` resolves zero or one placement label before dispatch. Zero labels selects `execution.default_target`; one selects that exact configured target. Multiple or unknown labels produce `placement.blocked`. The scheduler-wide concurrency cap and the selected target's cap must both have capacity.

An SSH target uses the local OpenSSH executable in batch mode with strict host-key checking. Its remote command is fixed administrator configuration such as:

```text
/opt/gitlab-issue-agent/venv/bin/python -m gitlab_issue_agent.remote_runner
```

That fixed argv is quoted because the SSH protocol supplies a remote command string. Dynamic Issue text, prompts, paths, and attempt data never enter that string. They are a versioned JSON object on stdin. The helper emits structured event, process-identity, and final-response objects on stdout. It executes Git and the configured agent with `exec`-style argument arrays and no local shell.

The scheduler's GitLab token is not in this protocol. Repository and CSC authentication belong to the dedicated execution-host account. Configuration can explicitly supply an agent environment, but environment inheritance remains allowlisted.

## Reconciliation and cancellation

Every poll tick executes reconciliation before candidate dispatch. For every live executor, it fetches the exact current issue:

- active and routable: refresh the in-memory snapshot;
- terminal: request cancellation and release after process termination;
- non-active: request cancellation and preserve the worktree;
- missing (confirmed 404): request cancellation;
- active but placement changed/invalid: cancel the current target before allowing any new placement;
- tracker request error: keep the executor running and retry reconciliation next tick, because absence was not established.

For SSH execution, cancellation is a second remote-helper request keyed by target and attempt ID; it is not merely termination of the local `ssh` client. The helper writes a cancellation tombstone before inspecting the lease, closing the cancel-before-launch race. If the host cannot confirm that the recorded birth identity is gone, the local state becomes `BLOCKED` and no duplicate attempt may dispatch.

Cancellation detection is bounded by the poll interval. Confirmed termination additionally includes SSH connection and configured process grace time. P0 is polling-based; a future webhook can wake the same reconciliation path without changing authority semantics.

## Crash and cold-start boundary

Before launching an executor, the scheduler durably writes `RUNNING` and an attempt ID. Immediately after spawn it records:

```json
{"host_id": "build-01", "pid": 1234, "create_time": 1788000000.25, "attempt_id": "..."}
```

The creation timestamp prevents killing an unrelated process after PID reuse. An attempt-scoped remote helper writes a lease for its own supervisor identity before remote Git or CSC work, then atomically changes the run lease to the CSC identity. Thus a cold-start cancel can reap the helper tree during preparation/the narrow pre-spawn window or the exact CSC tree after spawn.

On cold start:

1. Load every durable issue record.
2. Resolve the durable execution target and attempt ID.
3. For local execution, verify PID birth identity and terminate its process tree if it still matches. For SSH execution, send `cancel(attempt_id)` and require a structured `confirmed_safe` response.
4. If stop cannot be confirmed, retain process/attempt evidence and mark `BLOCKED`; do not redispatch.
5. Fetch the exact current GitLab Issue.
6. If stop was confirmed and the Issue is active, mark it `READY` and reconstruct/resume from the durable worktree.
7. If stop was confirmed and the Issue is non-active, terminal, or missing, mark it `RELEASED`.
8. If GitLab is unavailable, do not dispatch from the stale snapshot; candidate polling must establish eligibility later.

Tracker state is authoritative for whether the objective should run, but it cannot overrule uncertain executor reality: a closed Issue triggers cancellation, not an unsafe assertion that the remote process is gone. This deliberately kills the old executor even when the Issue is still active before starting a replacement, preventing duplicate writers after a scheduler `kill -9`.

## Workspace model

Each execution target keeps its own non-agent control clone and creates worktrees from `origin/<default_branch>`:

```text
<target-state>/repositories/<remote-hash>       # control clone, never an agent cwd
<target-state>/workspaces/<issue-key>           # agent cwd
branch: agent/issue-<iid>
```

Workspace keys replace unsafe characters and append a 64-bit stable hash when sanitization changes the identifier, avoiding collisions such as `a/b#1` versus `a_b#1`.

Existing worktrees are never reset or cleaned automatically. Terminal cleanup is also manual in P0 so human review and forensic evidence remain available.

The control-plane state stores the selected target and opaque workspace path. It does not normalize a remote POSIX path through the scheduler server's local path implementation. Target changes do not copy uncommitted files; the target workflow is expected to push reviewable checkpoints before deliberate rerouting.

## Observability contract

Events are append-only JSON objects. Scheduler decisions and backend output share the same attempt ID. The global stream supports fleet operations; issue and attempt streams make one objective independently auditable.

The scheduler records the command shape but redacts the complete prompt argument. It does not automatically redact arbitrary secrets printed by the agent. The agent environment is allowlisted and excludes `GITLAB_TOKEN` by default, reducing accidental exposure at the source.

## Failure boundaries and future work

- P0 has one scheduler process, though execution spans hosts. The local file lock is not an active/active scheduler lease.
- Remote leases contain executor birth identity; they are not job-ownership leases for multiple schedulers.
- An unreachable execution host intentionally leaves its Issue `BLOCKED`. Operator recovery means restoring SSH/reaper reachability or proving and cleaning the execution state on that host, not deleting the local state record.
- P0 SSH execution targets are POSIX-like; the fixed remote helper argv uses POSIX quoting. Scheduler/local targets remain cross-platform, but Windows OpenSSH execution-host quoting needs a separate verified transport mode.
- Git operations can still be interrupted between directory creation and metadata persistence; reuse validation fails closed on a non-worktree path.
- Polling cannot cancel faster than the configured interval.
- Automatic MR creation is agent workflow behavior, not orchestrator behavior.
- Recommended next work: GitLab webhook wakeups, OpenTelemetry export, bounded tombstone retention/cleanup commands, multi-project config, execution-host health reporting, and a read-only operator status endpoint.
