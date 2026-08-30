# Architecture and control-plane semantics

## Invariants

1. **GitLab is authoritative.** A local `RUNNING`, `RETRY_WAIT`, or `RELEASED` value is only scheduler bookkeeping. Current issue state and labels decide whether work may run.
2. **One scheduling authority.** Only `Orchestrator` changes dispatch, retry, continuation, and release state. A filesystem lock prevents two scheduler processes from sharing a state root.
3. **One durable worktree per issue.** Agent execution is always rooted in the issue worktree. Reuse preserves uncommitted work and local evidence across attempts and restarts.
4. **Exit is not completion.** Exit code `0` is a clean turn boundary. If the issue remains active, the next action is continuation, not release.
5. **Continuation is not retry.** Clean continuations use a fixed short delay and reset failure count. Failures and timeouts increment a separate counter and use exponential backoff.
6. **Reconciliation cancels.** If a running issue becomes terminal, non-active, missing, or no longer routable, the executor is signalled immediately and its matching process tree is terminated.
7. **No automatic merge.** The workflow may ask the agent to push a branch and open/update an MR, but the scheduler never merges. `agent::human-review` is intentionally non-active rather than terminal, preserving the worktree.

## Components

```text
GitLab Issues (authority)
        │ list + exact refresh
        ▼
GitLabIssueTracker ── normalized Issue + disposition
        │
        ▼
Orchestrator ── owns running map, continuation, retry, reconciliation
   │        │
   │        ├── StateStore ── atomic issue state
   │        └── EventSink ── global / issue / attempt JSONL
   │
   ├── WorkspaceManager ── control clone + per-issue git worktree
   │
   └── AgentBackend
          └── CommandAgentBackend ── no-shell subprocess + structured output
```

`IssueTracker`, `WorkspaceManager`, and `AgentBackend` are injected into the orchestrator. The shipped P0 has one GitLab adapter and one generic command backend; tests use the same seams to drive deterministic control-plane races.

## Attempt state machine

```text
READY
  │ dispatch
  ▼
RUNNING
  ├── tracker non-active/terminal/missing ── cancel ──► RELEASED
  ├── clean exit + tracker active ───────────────────► CONTINUATION_WAIT
  ├── failure/timeout + tracker active ──────────────► RETRY_WAIT
  └── any exit + tracker non-active/terminal ────────► RELEASED

CONTINUATION_WAIT ── fixed delay ──► RUNNING
RETRY_WAIT ── exponential backoff ─► RUNNING
RELEASED ── tracker reactivated ───► READY
```

The clean-continuation cap is a fairness yield, not a failure: every configured number of consecutive clean turns, the issue waits for `continuation.yield_seconds` before it may claim another slot.

## Continuation routes

The scheduler selects a route per attempt:

1. `first`: full workflow plus authoritative issue snapshot.
2. `native_resume`: used only when the backend reports a session ID and the configuration supplies `native_resume_args`. It sends concise continuation guidance to the existing session.
3. `stateless_reconstruction`: full workflow and refreshed issue plus bounded prior output and a fresh git snapshot. The prompt tells the agent to verify the worktree rather than trust the summary.

If a native-resume attempt fails, its saved session ID is cleared before retry so the next attempt takes the stateless path instead of looping on a corrupt or expired session.

## Reconciliation and cancellation

Every poll tick executes reconciliation before candidate dispatch. For every live executor, it fetches the exact current issue:

- active and routable: refresh the in-memory snapshot;
- terminal: request cancellation and release after process termination;
- non-active: request cancellation and preserve the worktree;
- missing (confirmed 404): request cancellation;
- tracker request error: keep the executor running and retry reconciliation next tick, because absence was not established.

Cancellation latency is bounded by the poll interval plus the configured process termination grace. P0 is polling-based; a future webhook can wake the same reconciliation path without changing authority semantics.

## Crash and cold-start boundary

Before launching an executor, the scheduler durably writes `RUNNING` and an attempt ID. Immediately after spawn it records:

```json
{"pid": 1234, "create_time": 1788000000.25, "attempt_id": "..."}
```

The creation timestamp prevents killing an unrelated process after PID reuse. On cold start:

1. Load every durable issue record.
2. For a recorded process, verify PID birth identity and terminate its process tree if it still matches.
3. Fetch the exact current GitLab Issue.
4. If active, mark it `READY` and reconstruct/resume from the durable worktree.
5. If non-active, terminal, or missing, mark it `RELEASED` regardless of stale local phase.
6. If GitLab is unavailable, do not dispatch from the stale snapshot; candidate polling must establish eligibility later.

This deliberately kills the old executor even when the issue is still active before starting a replacement, preventing duplicate writers after a scheduler `kill -9`.

## Workspace model

The scheduler keeps one non-agent control clone and creates worktrees from `origin/<default_branch>`:

```text
state/repositories/<remote-hash>       # control clone, never an agent cwd
state/workspaces/<issue-key>           # agent cwd
branch: agent/issue-<iid>
```

Workspace keys replace unsafe characters and append a 64-bit stable hash when sanitization changes the identifier, avoiding collisions such as `a/b#1` versus `a_b#1`.

Existing worktrees are never reset or cleaned automatically. Terminal cleanup is also manual in P0 so human review and forensic evidence remain available.

## Observability contract

Events are append-only JSON objects. Scheduler decisions and backend output share the same attempt ID. The global stream supports fleet operations; issue and attempt streams make one objective independently auditable.

The scheduler records the command shape but redacts the complete prompt argument. It does not automatically redact arbitrary secrets printed by the agent. The agent environment is allowlisted and excludes `GITLAB_TOKEN` by default, reducing accidental exposure at the source.

## Failure boundaries and future work

- P0 is a single-host/single-process scheduler. The file lock is not a distributed lease.
- Git operations can still be interrupted between directory creation and metadata persistence; reuse validation fails closed on a non-worktree path.
- Polling cannot cancel faster than the configured interval.
- Automatic MR creation is agent workflow behavior, not orchestrator behavior.
- Recommended next work: GitLab webhook wakeups, OpenTelemetry export, retention/cleanup commands, multi-project config, and a read-only operator status endpoint.
