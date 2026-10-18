# NATSKit — independent task register

This is the **only current NATSKit backlog**. Do not import another application's tickets, historical unchecked boxes or broad feature wishlists.

## DONE — repository independence and handoff

- **NK-01 — Independent `.codex` structure.** Add self-contained start, context, architecture, decisions, status, tasks, session and next-chat documentation. Acceptance: a new chat can continue using this repository alone.
- **NK-02 — Safety and ownership boundaries.** Explicitly document read-only vs managed APIs, safe additive changes, non-destructive refusal, worker ACK/quarantine, local-only tests and version policy. Acceptance: no implicit migration permission.
- **NK-03 — Explicit integration preflight.** Add `task verify:strict` that **fails** before running tests if `nats-server` is absent, and then runs the current independent `task verify`. Acceptance: default task remains compatible; no new CI/operational infrastructure.

## DONE — NK-04: independent strict quality gate (owner-reported PASS, 2026-10-18)

- Owner executed `task verify:strict` with `nats-server: v2.14.6` available on PATH: `fmt:check`, `mod:check` (including `go mod tidy` and clean diff of `go.mod`/`go.sum`), `go:test` (`ok`, 15.546s), `go:race` (`ok`, 14.189s), and `go:vet` (no errors). The provided output ends normally at `go vet` with no failure.
- The missing-broker precondition is satisfied and the source's disposable-broker tests can run. The provided non-verbose log does **not** enumerate individual test names/skips or the workstation's `go version`; do not invent these details or claim broader production fault coverage. Do not rerun the gate for documentation-only commits.

## OPEN — NK-05: optional release policy decision

- Owner can separately decide whether this verified **pre-release** library needs a semver version/tag/release. **No tag/release has been requested or performed.** An immutable commit pin continues to work; release is not a build prerequisite.

## BLOCKED — not implementable safely without a cutover contract

- **NK-06 — Lossless subject replacement / Durable-filter migration.** Requires explicit external publisher/replica fencing, draining, durable cursor/message proof, rollback plan and operator approval. Current behavior **must fail closed**. Never implement delete/recreate/purge as a shortcut.

## DEFERRED — no current implementation authorization

- **NK-07 — Additional automatic reconnect/rebind, MaxDeliver reconciliation, long-haul HA, disaster recovery, performance tuning.** Consider only after an actual reliability requirement and measurable acceptance criteria; preserve independence and existing semantics.

## Test commands

```sh
# Standard source gate; integration tests may SKIP if the broker binary is absent:
task verify

# Required independent acceptance gate; demands a local nats-server binary:
task verify:strict
```

The library does not require a Docker Compose deployment, PostgreSQL, Protobuf, or an application-specific config to pass these gates.
