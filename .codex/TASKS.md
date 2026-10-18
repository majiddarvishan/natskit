# NATSKit — independent task register

This is the **only current NATSKit backlog**. Do not import another application's tickets, historical unchecked boxes or broad feature wishlists.

## DONE — repository independence and handoff

- **NK-01 — Independent `.codex` structure.** Add self-contained start, context, architecture, decisions, status, tasks, session and next-chat documentation. Acceptance: a new chat can continue using this repository alone.
- **NK-02 — Safety and ownership boundaries.** Explicitly document read-only vs managed APIs, safe additive changes, non-destructive refusal, worker ACK/quarantine, local-only tests and version policy. Acceptance: no implicit migration permission.
- **NK-03 — Explicit integration preflight.** Add `task verify:strict` that **fails** before running tests if `nats-server` is absent, and then runs the current independent `task verify`. Acceptance: default task remains compatible; no new CI/operational infrastructure.

## DONE — NK-04: independent strict quality gate (owner-reported PASS, 2026-10-18)

- Owner executed `task verify:strict` with `nats-server: v2.14.6` available on PATH: `fmt:check`, `mod:check` (including `go mod tidy` and clean diff of `go.mod`/`go.sum`), `go:test` (`ok`, 15.546s), `go:race` (`ok`, 14.189s), and `go:vet` (no errors). The provided output ends normally at `go vet` with no failure.
- The missing-broker precondition is satisfied and the source's disposable-broker tests can run. The provided non-verbose log does **not** enumerate individual test names/skips or the workstation's `go version`; do not invent these details or claim broader production fault coverage. Do not rerun the gate for documentation-only commits.

## DONE — NK-05: official v0.1.0 release (GitHub verified 2026-10-18)

- Owner published the initial official release: [NATSKit v0.1.0](https://github.com/majiddarvishan/natskit/releases/tag/v0.1.0).
- **Annotated tag:** `refs/tags/v0.1.0` -> tag object `d232039c734e626b854669b62e82260c6fdd6a4d` -> commit **`0e27d162f0c43225e366da66d340b3f767d667d8`**. GitHub independently confirmed the exact immutable commit.
- **GitHub Release:** published at `2026-10-18T16:22:55Z`; `draft=false`, `prerelease=false`; [release notes](../docs/releases/v0.1.0.md) match its published content.
- Previous tag publication instructions are complete/historical. Do not re-create, move, or force-push this tag. Future versions require a separate explicit owner decision.
- Documentation-only changes made after the tagged commit are **not** part of the `v0.1.0` artifact. Library Go source/API and CRA pinned module are unchanged.

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

## IN PROGRESS — NK-08: explicit Subject replacement policy (2026-10-18)

- Owner authorized `jetstream.subject_update_policy = additive | replace`, default `additive`.
- Implemented source candidate: the strict normal `Connect` stays read-only; managed `Reconcile` checks all Streams/Consumers before replacing capture subjects only with `replace`; reject absent/nonpositive MaxAge on affected Stream; preserve all Stream/Consumer identities, messages, retention and ACK policies. No purge or delete; report replacements distinctly. Generic library contains no CRA-specific subject names.
- Regression added for default refusal, positive opt-in, unchanged Consumer filter, retention, stored message preservation, new capture and idempotence.
- **OPEN:** independent local `task verify:strict` on post-release commit, including real disposable NATS. `v0.1.0` published tag remains immutable; no new tag/release approved.
