# NATSKit — independent task register

This is the **only current NATSKit backlog**. Do not import another application's tickets, historical unchecked boxes or broad feature wishlists.

## DONE — repository independence and handoff

- **NK-01 — Independent `.codex` structure.** Add self-contained start, context, architecture, decisions, status, tasks, session and next-chat documentation. Acceptance: a new chat can continue using this repository alone.
- **NK-02 — Safety and ownership boundaries.** Explicitly document read-only vs managed APIs, safe additive changes, non-destructive refusal, worker ACK/quarantine, local-only tests and version policy. Acceptance: no implicit migration permission.
- **NK-03 — Explicit integration preflight.** Add `task verify:strict` that **fails** before running tests if `nats-server` is absent, and then runs the current independent `task verify`. Acceptance: default task remains compatible; no new CI/operational infrastructure.

## DONE — NK-04: independent strict quality gate (owner-reported PASS, 2026-10-18)

- Owner executed `task verify:strict` with `nats-server: v2.14.6` available on PATH: `fmt:check`, `mod:check` (including `go mod tidy` and clean diff of `go.mod`/`go.sum`), `go:test` (`ok`, 15.546s), `go:race` (`ok`, 14.189s), and `go:vet` (no errors). The provided output ends normally at `go vet` with no failure.
- The missing-broker precondition is satisfied and the source's disposable-broker tests can run. The provided non-verbose log does **not** enumerate individual test names/skips or the workstation's `go version`; do not invent these details or claim broader production fault coverage. Do not rerun the gate for documentation-only commits.

## IN PROGRESS — NK-05: owner-approved v0.1.0 release

- Owner explicitly authorized publishing **v0.1.0** on 2026-10-18, after the independent strict gate passed.
- Preflight on GitHub: no existing tags or GitHub Releases, including no v0.1.0 collision.
- Release notes: [docs/releases/v0.1.0.md](../docs/releases/v0.1.0.md). **Not yet published**: the connected GitHub actions in this chat cannot create a tag or GitHub Release. The prepared notes/commits alone are not a release.
- Finalization by a trusted local operator, in the standalone NATSKit repository (not CRA): verify clean local main matches the prepared release source commit, create exactly one annotated v0.1.0 tag on that commit, push ONLY that tag, then publish a non-draft GitHub Release using the prepared notes. Confirm the public tag and release links before marking NK-05 DONE.
- Exact commands after the release-preparation commit has been synced:

```sh
git fetch origin main --tags
git switch main
git pull --ff-only
git status --short                     # must be empty
git rev-parse HEAD                     # must equal the release-source SHA given in the handoff
git tag -a v0.1.0 HEAD -m "NATSKit v0.1.0"
git push origin refs/tags/v0.1.0
gh release create v0.1.0 --verify-tag --title "NATSKit v0.1.0" --notes-file docs/releases/v0.1.0.md
```

If GitHub CLI (`gh`) is unavailable, use the GitHub Releases web page after pushing the tag. Never retag or force-push a published version. Review existing GitHub Actions triggers before pushing a tag; the operator owns CI/billing impact. No CRA merge or live-data action is included.

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
