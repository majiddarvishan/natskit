# NATSKit — current status

**Reviewed:** 2026-10-18. Source-of-truth is the actual remote `main`, not these historical SHA values.

## Audit baseline

- Before creation of this independent `.codex`: `main` HEAD was `25f0db6b10c59b07228c6ed17b2c3db3026655cb`.
- Module: `github.com/majiddarvishan/natskit`; declared Go `1.26.0`, `nats.go v1.51.0`.
- Managed reconciliation, fail-closed policy/subject checks and disposable regression tests are committed. `Connect` remains read-only.
- Last code fix (`25f0db6b10c5`) corrected passing `ResolvedConsumer` instead of `want.Config` to `consumerPolicySummary`.
- The earlier `consumerPolicySummary` compilation regression was fixed in `25f0db6b10c5`. On 2026-10-18 the owner **supplied the independent, post-fix `task verify:strict` all-pass output** with `nats-server: v2.14.6`. See the newest quality-gate evidence below.
- Existing `task verify` can silently skip disposable integration tests if the external `nats-server` binary is absent. A separate strict preflight gate is added for independent acceptance.
- Historical baseline: no NATSKit release/tag existed at initial audit. **Superseded:** v0.1.0 is now published and verified; no production migration was approved. Existing GitHub workflow was neither enabled nor modified by this documentation effort.

## Current gate status

| Gate | Status | Evidence |
| --- | --- | --- |
| Generic API, managed reconciliation source and regression tests | IMPLEMENTED | Git repository and current Go source |
| Independent engineering state in `.codex` | IMPLEMENTED | This directory |
| Strict disposable-broker quality command | IMPLEMENTED / PASS | `nats-server: v2.14.6` precondition satisfied in owner log |
| Independent `task verify:strict` on NATSKit main | **PASS (owner-supplied output)** | Format, module tidy/diff, Go unit, race and vet completed successfully on 2026-10-18; non-verbose log does not enumerate skip details or `go version` |
| Actual HA/fault behavior beyond existing disposable tests | DEFERRED | Not claimed as production verified |
| Destructive topic/Durable migration | BLOCKED | Needs operator-approved producer fencing/cursor continuity design |

## 2026-10-18 — Owner's final strict-gate evidence

- Command: `task verify:strict`, successful through `task go:vet`.
- Broker executable: `nats-server: v2.14.6` (**observed**).
- Formatting: PASS; `go mod tidy` and `git diff --exit-code -- go.mod go.sum`: PASS.
- `go test -count=1 ./...`: PASS, `ok github.com/majiddarvishan/natskit 15.546s`.
- `go test -race -count=1 ./...`: PASS, `ok github.com/majiddarvishan/natskit 14.189s`.
- `go vet ./...`: PASS, no diagnostics.
- Go toolchain version: **not in supplied log** (module declares Go 1.26.0).
- Individual skip inventory: **not in non-verbose log**. The precondition rules out missing `nats-server` as a reason for the suite skipping its disposable-broker tests, but it is not a complete audit of every `t.Skip` condition.
- This is **not** a production deployment, broker migration, customer acceptance or explicit release approval.

## 2026-10-18 — Initial v0.1.0 release approved; awaiting remote Tag/Release

- User authorized first NATSKit version `v0.1.0`. GitHub preflight: zero tags and releases, and no `v0.1.0` ref or GitHub Release found.
- Release notes and exact finalization checklist prepared. **The tag and GitHub Release do not exist yet.** This environment's connected GitHub operations can commit source but expose no create-tag or create-release command. NK-05 stays IN PROGRESS until the public tag and release are verified.
- The owner-reported `task verify:strict` PASS on 2026-10-18 continues to apply to unchanged Go code. All release-preparation changes are documentation-only; no re-run is necessary solely for those changes.
- Final tag must point to the last release-preparation commit on main, the SHA verified by a future `git rev-parse HEAD` and GitHub branch lookup. No additional features, runtime broker work or dependency changes are authorized.

## 2026-10-18 — Official v0.1.0 publication verified (NK-05 DONE)

- Owner confirmed completion of the release operation. Verified directly from GitHub: [release `v0.1.0`](https://github.com/majiddarvishan/natskit/releases/tag/v0.1.0) is **published**, not draft and not prerelease; published at `2026-10-18T16:22:55Z`.
- `refs/tags/v0.1.0` points to annotated tag object `d232039c734e626b854669b62e82260c6fdd6a4d`, which points to exact commit `0e27d162f0c43225e366da66d340b3f767d667d8`. The tag exists; prior historical claims of PENDING are superseded.
- No changes to the verified Go library source were required for publication. Later documentation updates to main are not included in this tag. `NK-04` and `NK-05` are DONE; `NK-06` remains BLOCKED, `NK-07` DEFERRED.

## 2026-10-18 — Opt-in post-v0.1.0 Stream capture replacement

- Owner explicitly approved `subject_update_policy=replace` with `additive` safe default to simplify changing application NATS wire subjects. Code and disposable tests are prepared for the *independent* NATSKit repository; the `v0.1.0` published artifact/tag is unchanged.
- A replacement does not purge existing messages or alter identity, Durable filters or policies; a positive existing MaxAge is required for automatic expiration. It can stop future capture from removed subjects. External clients own their changeover.
- This code change has **NOT yet passed an owner-run `task verify:strict`**. Do not treat prior `v0.1.0` gate as post-change verification; do not tag/release or alter any production broker as part of implementation.

## 2026-10-18 — NK-08 owner-reported strict acceptance after formatting fixes

- First post-feature `task verify:strict` attempt reached `nats-server v2.14.6` but stopped at `fmt:check`, first because of `reconcile.go` and then `config.go`. Targeted formatting commits `1ca545f7e95f339c595decb4ad34f1a5ea49787d` and final owner-pushed `0960ffa252277ba976e4ff282c4f639fd7eddb77` resolved the reported formatting blockers.
- Owner replied **"انجام شد"** to the instructions to run `gofmt -w config.go`, confirm no formatting output, execute `task verify:strict`, then commit/push after PASS. GitHub confirms final formatting commit on `main`. Mark **NK-08 DONE, developer-reported acceptance**; full successful final output, Go toolchain version, per-test results and skipped-test inventory were **not provided**, so do not claim independent verification of those specifics.
- Approved implementation retains `additive` default and explicit `replace` Stream capture updates; existing positive MaxAge required, no purge/delete/Consumer-policy updates. No new tag, semantic release, broker cutover or data mutation took place in this documentation update.
- Application follow-up belongs to CRA Bridge: `github.com/majiddarvishan/natskit v0.1.1-0.20261018193505-e3a7a86ccd10` is selected in CRA `go.mod`, but the new module sum is not committed and CRA focused tests/runtime startup are outstanding. Do not conflate NATSKit's successful quality gate with CRA readiness.
