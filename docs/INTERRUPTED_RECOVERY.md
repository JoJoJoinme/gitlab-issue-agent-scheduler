# Interrupted execution recovery

## Motivation

A durable scheduler should distinguish **restoring control-plane state** from **continuing interrupted work**. OpenAI Codex made that distinction explicit in its September 16, 2026 managed-daemon recovery change: it records interrupted-turn metadata, restores the thread, rejects recovery when the saved turn has completed or been superseded, revalidates environment and permission context, and only then starts one continuation turn.

Reference: https://github.com/openai/codex/commit/f2b5b81f39fba7d1172e4a5e65a427f029a39479

This scheduler already has a stronger external authority boundary for its domain: GitLab decides whether an Issue may run, cold-start recovery first reaps the recorded executor, an execution-target change clears native session state, and automatic merge remains forbidden. The remaining gap was smaller but important: `state.json` is written before an executor starts, while the attempt number and a newly discovered backend session ID were previously committed only when the attempt finished. A hard scheduler crash during the first turn could therefore leave useful execution evidence in `events.jsonl` while `state.json` still looked like a never-completed first attempt.

## Minimal design

The append-only per-attempt event stream is used as a narrow recovery journal for two facts that the scheduler already emits:

- `attempt.started.details.attempt_number`
- `agent.session_observed.details.session_id` (and the same session ID in `agent.exited`)

When `StateStore` loads a state whose phase is `RUNNING`, or a non-placement `BLOCKED` state whose executor may still need reconciliation, it replays only the current `last_attempt_id` event file. It may advance `total_attempts` and recover the latest native session ID. `continuation.native_resume_abandoned` explicitly clears a recovered session so an earlier observation cannot revive a session that the control plane intentionally invalidated.

Recovery is deliberately scoped to interrupted phases. `READY`, continuation/retry waits, and `RELEASED` snapshots are not overlaid from old events. This prevents an old `agent.session_observed` record from undoing a later deliberate state change such as placement migration or native-resume abandonment.

Malformed trailing JSONL records are ignored. Earlier flushed records remain usable as recovery evidence.

## Cold-start flow

The resulting flow remains under the existing P0 authority model:

```text
load state.json
    |
    +-- RUNNING/BLOCKED? --> replay current attempt facts from events.jsonl
    |
    v
reap/confirm old executor is stopped
    |
    v
refresh exact GitLab Issue
    |
    +-- non-active/terminal/missing --> RELEASED
    |
    +-- active --> READY
                   |
                   +-- current target changed --> clear native session
                   |
                   +-- valid recovered session + backend supports resume --> native continuation
                   |
                   +-- otherwise --> stateless reconstruction from Issue + worktree evidence
```

The event journal never authorizes execution. It only repairs execution metadata. GitLab Issue state, execution-target placement, executor-safety proof, current backend capability, and the human-review gate remain authoritative.

## Deliberate boundaries

This change does **not** copy the full Codex daemon model. In particular it does not add a hosted thread store, backend-specific permission fingerprints, remote conversation snapshots, automatic user-message synthesis, or a new recovery daemon. Those would violate the current backend-neutral boundary without a stable protocol from `csc`/`custom-claude`.

OpenAI can revalidate a saved turn's exact permission profile and local environment because those are first-class runtime objects in Codex. This scheduler can currently revalidate the facts it owns: GitLab authority, execution target, process identity, worktree, and whether the selected backend still advertises native resume. A future backend protocol may add an opaque session/environment compatibility token; until then, the scheduler should not infer one from stdout text.

## Verification

Regression coverage exercises three properties:

1. A simulated hard-crash state with a live orphan, `total_attempts == 0`, and an observed session in the event journal is reaped, restored as attempt 1, and continued as attempt 2 through native resume.
2. A `continuation.native_resume_abandoned` event wins over an earlier session observation, including with a malformed trailing JSONL record.
3. A completed/non-interrupted snapshot does not replay an old session event.
