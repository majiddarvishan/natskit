# NATSKit — session record

## 2026-10-19 — approved generic Publisher design and compact cross-repo handoff

- Owner wants two apps exchanging independent messages over two configured Subjects, with ordinary later remapping to X/Y. Publisher must not own the receiver's Stream; broker capture and receiver Durable provisioning belong to their owners.
- Existing NATSKit PubAck logic incorrectly couples external publish routing to managed Stream topology; this is a new generic task. Design and P0 tickets NK-PUB-01/02/03 are recorded; no code or tests changed in NATSKit for this approval.
- CRA's specific \`cancel_external_stream\` implementation on its feature branch is unverified and rejected as unnecessary special-casing; CRA will safely remove it after NATSKit generic implementation, with focused disposable tests.
- Preserve \`subject_update_policy=replace\` as a separate valid feature, no forced migrations or replay, no releases/Tags/CI/runtime mutations. Next work: NATSKit first, CRA second.

## 2026-10-18 — first independent engineering handoff

1. Inspected the repository's real remote `main` at `25f0db6b10c59b07228c6ed17b2c3db3026655cb` and the full repository tree. There was no pre-existing `.codex` directory.
2. Reviewed Go module identity/toolchain, config reference, README, origin, API source, safety-oriented reconcile/worker tests, Taskfile and existing CI configuration.
3. Created an **entirely self-contained** `.codex`; its context and task register are not maintained by any other repository.
4. Implemented a separate opt-in `task verify:strict` to reject a missing `nats-server` rather than accepting skipped JetStream integration tests. Standard `task verify` remains unchanged.
5. Kept application transports, broker subjects, data and credentials out of this change. No new runtime or package dependency, Go API change, deployment, tag, release or CI workflow edit.
6. Source edits were pushed via GitHub connector. **Local Go 1.26 / Task / disposable NATS execution was not available in this session.** Treat NK-04 as OPEN until owner supplies successful independent logs.

## Continuation rules

- Verify the *new* real `main` HEAD and current task status when resuming.
- Work from `TASKS.md`, not historical conversations or another repository's state.
- Do not run a live broker, reset state, or claim test success without evidence.
- Record any future validated release decision separately; this session authorized none.

## 2026-10-18 — NK-04 independent strict verification PASS (owner evidence)

- User ran `task verify:strict` on the independent NATSKit checkout. Broker version reported: `nats-server: v2.14.6`.
- Formatting, `go mod tidy`, `go.mod/go.sum` diff, `go test -count=1 ./...` (15.546s), race (14.189s) and `go vet ./...` all succeeded. No error was reported.
- The supplied output did not include `go version` or verbose per-test skip inventory; record those as unknown, not guessed. All tests run against local disposable broker resources in this repo; no external broker endpoint is configured by the quality gate.
- NK-04 marked DONE based on owner-provided results. NK-05 release/version choice remains OPEN, NK-06 remains BLOCKED; NK-07 deferred. This commit changes only docs and does not require retesting.

## 2026-10-18 — first release preparation

- Owner stated CRA Compose is connected to real data, that local runtime data/config must never enter Git, that `v0.1.0` publication is approved and that CRA branch merge must await a separate explicit request.
- GitHub preflight found zero existing NATSKit tags and Releases; checked the exact `v0.1.0` ref and release endpoint, both absent.
- Prepared `docs/releases/v0.1.0.md` and an explicit owner/operator release checklist. GitHub connector currently exposes commits but no tag/release creation: NK-05 remains pending publication. The exact release-source commit is the final main HEAD containing these docs, to be verified before tagging.
- No Go, broker configuration, runtime secret, CI workflow or dependency changes made; no production/live environment access, CRA merge or unapproved tag.

## 2026-10-18 — NK-05 official release confirmed

- Owner confirmed `v0.1.0` publication. Remote GitHub tag and Release independently verified; annotated tag SHA `d232039c734e626b854669b62e82260c6fdd6a4d` targets `0e27d162f0c43225e366da66d340b3f767d667d8`, published Release is not draft/prerelease.
- Marked `NK-05` DONE; `NK-06` and `NK-07` remain BLOCKED/DEFERRED. No Go source, tag, CI workflow, external NATS or application deployment modified.

## 2026-10-18 — Opt-in managed Subject replacement feature

- Owner authorized replacing Stream capture subjects automatically on explicit `jetstream.subject_update_policy=replace`, retaining `additive` as the default and refusing Consumer/Stream policy drift.
- Implemented Go source and independent disposable tests. Full preflight, positive MaxAge and unexpected concurrent subject update checks protect the replacement path. No purge/delete/force-recreate, and old messages remain under normal retention.
- No post-change local Go 1.26 + NATS-server test gate has been executed in this GitHub-only session; mark NK-08 pending owner verification. Existing `v0.1.0` tag remains immutable.

## 2026-10-18 — NK-08 reported PASS; final gofmt fix published

- User initially supplied `task verify:strict` failures at `fmt:check`, specifically `reconcile.go` then `config.go`; broker executable was confirmed `nats-server v2.14.6`.
- `reconcile.go` format fix committed at `1ca545f7e95f339c595decb4ad34f1a5ea49787d`. User ran `gofmt -w config.go`, reran strict gate, and pushed `0960ffa252277ba976e4ff282c4f639fd7eddb77` to `main`, subsequently saying **"انجام شد"**. Do not invent a pasted final strict stdout; acceptance is owner-reported.
- NK-08 marked DONE based on owner report. Leave public `v0.1.0` tag untouched. CRA integration tests, real `go.sum` and local subject replacement startup still need separate validation. Only documentation updated in this follow-up.
