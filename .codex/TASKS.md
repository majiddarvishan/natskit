# NATSKit — independent task register

This is the **only current NATSKit backlog**. Do not import another application's tickets, historical unchecked boxes or broad feature wishlists.

## DONE — repository independence and handoff

- **NK-01 — Independent `.codex` structure.** Add self-contained start, context, architecture, decisions, status, tasks, session and next-chat documentation. Acceptance: a new chat can continue using this repository alone.
- **NK-02 — Safety and ownership boundaries.** Explicitly document read-only vs managed APIs, safe additive changes, non-destructive refusal, worker ACK/quarantine, local-only tests and version policy. Acceptance: no implicit migration permission.
- **NK-03 — Explicit integration preflight.** Add `task verify:strict` that **fails** before running tests if `nats-server` is absent, and then runs the current independent `task verify`. Acceptance: default task remains compatible; no new CI/operational infrastructure.

## OPEN — minimal final verification

- **NK-04 — One independent strict gate.** On a trusted development workstation using Go 1.26+, install a disposable-test `nats-server` binary (CI reference: v2.14.6), then execute `task verify:strict`. Acceptance: gofmt, clean go.mod/go.sum after tidy, Go unit, race and vet PASS; actual localhost/temp-storage tests **run**, not SKIP due to missing executable; record command output, Go/broker versions and remaining skips. Do not invoke customer NATS endpoints or data directories. If failed, fix only real failures, then rerun the relevant gate.
- **NK-05 — Final owner acceptance.** When NK-04 is complete, decide whether a future semver/tag/release is warranted. **No tag or release now.** A tag is never a prerequisite for using an immutable commit pin.

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
