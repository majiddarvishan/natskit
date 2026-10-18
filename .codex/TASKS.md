# NATSKit — independent task register

This is the **only current NATSKit backlog**. Do not import another application's tickets, historical unchecked boxes or broad feature wishlists.

## DONE — Go 1.23.2 compatibility quality gate (2026-10-19)

The Go 1.23.2 module/dependency candidate was committed at `a3652d86`; formatting follow-up at `ba588d84`. Owner ran `GOTOOLCHAIN=local task verify:strict` with `nats-server v2.14.6`: clean gofmt, unchanged `go.mod`/`go.sum` after `go mod tidy`, `go test -count=1 ./...` PASS (20.098s), and `go test -race -count=1 ./...` PASS (20.601s). The owner then confirmed `echo $?` **0** immediately after the command, establishing the final vet/strict command **PASS**. Go's exact local version was not printed. No release approval; check dependency security before release.

## APPROVED NEW WORK — 2026-10-19: generic publish addressing independent of Stream ownership

**Authoritative design:** [PUBLISH_SUBJECT_INDEPENDENCE.md](PUBLISH_SUBJECT_INDEPENDENCE.md). These were new tasks, now **DONE after owner strict-gate acceptance**, distinct from completed \`NK-01\` through \`NK-08\`. In particular, keep completed \`NK-08\` explicit \`subject_update_policy=replace\`: it is a legitimate *managed capture* feature, not the solution for publisher address correction.

| Ticket | Priority | State | Acceptance |
| --- | --- | --- | --- |
| NK-PUB-01 | P0 | DONE — owner strict gate PASS | \`Client.PublishConfirmed\` publishes to any configured concrete outbound Subject even if the caller owns no matching Stream, assuming the JetStream broker captures the Subject; no provisioning side effects. Preserve Core NATS semantics. |
| NK-PUB-02 | P0 | DONE — owner strict gate PASS | Return genuine JetStream PubAck (Stream, sequence, duplicate), with optional expected-Stream assertion independent of ownership. Mismatch/unavailable capture and permissions fail clearly; no silent Core fallback, no imaginary business success. |
| NK-PUB-03 | P0 | DONE — owner strict gate PASS | Ensure \`Connect\`/\`Verify\` versus \`ConnectReconciled\`/\`Reconcile\` scope to **owned** Streams/Consumers; never auto-manage publisher destinations. Disposable A/B bidirectional Subject1/Subject2 -> X/Y, independent message histories, unchanged existing broker state, focused failure/regression tests. |

Do NATSKit first, record actual tests (focused then \`task verify:strict\` once). CRA later removes unverified \`cancel_external_stream\` and adopts generic API. **No live broker change, no implicit tag/release or GitHub Actions.**


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

- **NK-06 — Truly lossless external cutover and Durable-filter migration.** Explicit `subject_update_policy=replace` (NK-08 DONE) changes **Stream capture only** and makes no promise of preserving future traffic on an old subject. A lossless external cutover, Durable filter/cursor migration and exact cross-process continuity still need producer fencing/draining, cursor proof and operator approval. Consumer policy drift must continue to fail closed. Never implement delete/recreate/purge as a shortcut.

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

## DONE — NK-08: explicit Subject replacement policy (owner-reported strict gate, 2026-10-18)

- Owner authorized `jetstream.subject_update_policy = additive | replace`, default `additive`.
- Implemented source candidate: the strict normal `Connect` stays read-only; managed `Reconcile` checks all Streams/Consumers before replacing capture subjects only with `replace`; reject absent/nonpositive MaxAge on affected Stream; preserve all Stream/Consumer identities, messages, retention and ACK policies. No purge or delete; report replacements distinctly. Generic library contains no CRA-specific subject names.
- Regression added for default refusal, positive opt-in, unchanged Consumer filter, retention, stored message preservation, new capture and idempotence.
- **Quality gate: owner-reported PASS.** Owner initially encountered `fmt:check` errors in `reconcile.go` and then `config.go`. Both formatting fixes were committed; final `config.go` style fix was pushed in [`0960ffa252277ba976e4ff282c4f639fd7eddb77`](https://github.com/majiddarvishan/natskit/commit/0960ffa252277ba976e4ff282c4f639fd7eddb77). Following the exact suggested sequence (`gofmt -w config.go`, `gofmt -l *.go`, `task verify:strict`, commit/push after PASS), owner confirmed **"انجام شد"**. The final full log was not pasted; record this as developer-reported acceptance, not independently inspected per-test evidence. Remote GitHub verification confirms the final style Commit exists.
- **NK-08 DONE based on owner confirmation;** no new tag or release was requested. `v0.1.0` remains immutable. Downstream CRA `go.sum` and full application integration gate remain independently OPEN.
