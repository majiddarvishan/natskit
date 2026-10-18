# Continue NATSKit — independent project handoff

Act as a senior Go / NATS JetStream / reliability engineer. Continue the **standalone** project only:

- Repository: https://github.com/majiddarvishan/natskit
- Branch: `main`
- Go module: `github.com/majiddarvishan/natskit`
- Go version in module: `1.26.0`
- Base implementation commit reviewed on 2026-10-18: `25f0db6b10c59b07228c6ed17b2c3db3026655cb` (not necessarily the present HEAD).

First inspect the real Git HEAD, working tree, `go.mod`, `Taskfile.yml`, README, and **every current `.codex` file** in this order: `START_HERE.md`, `PROJECT_CONTEXT.md`, `ARCHITECTURE.md`, `DECISIONS.md`, `CURRENT_STATUS.md`, `TASKS.md`, `SESSION.md`.

Proceed only with **open tasks in NATSKit `.codex/TASKS.md`**. **NK-04 is DONE:** the owner ran `task verify:strict` with `nats-server v2.14.6`; gofmt, dependency tidiness, tests, race and vet all passed on 2026-10-18. Toolchain version and per-test skip inventory were not supplied, so do not invent them. Documentation-only changes do not reopen completed tests. **NK-05 DONE:** official [v0.1.0](https://github.com/majiddarvishan/natskit/releases/tag/v0.1.0) is published; GitHub verified the annotated tag targets `0e27d162f0c43225e366da66d340b3f767d667d8`. Do not retag, rerelease, or assume this grants permission for another version. The NATSKit runtime and all applications remain independently managed. **NK-06** lossless migration remains BLOCKED and **NK-07** reliability expansion deferred.

Critical invariants: generic library, no application dependencies; `Connect` verifies only; explicit `ConnectReconciled` may add missing resources/subjects; opt-in `subject_update_policy=replace` may change only existing Stream subject capture when broker's MaxAge is positive, while Stream/Consumer policy drift still fails closed; never purge retained messages; Quarantine PubAck precedes source ACK; database idempotency is application's responsibility; never delete/purge broker data or infer authorization for a production migration. No customer NATS, PostgreSQL, Docker stack, forced tag/release, GitHub Actions changes or extra feature work.

Fix only reproducible remaining problems. If the gate cannot run in this environment, provide the exact local commands and keep the gate OPEN. Keep `.codex` updated so a future chat can proceed without another project's context.

Newest code task: **NK-08** post-v0.1.0 optional Stream Subject replacement was implemented at NATSKit commit `e3a7a86ccd108f339ce43265c2fa0dfae2a25223`, owner-authorized 2026-10-18. Execute one local `task verify:strict` on new source; never mark PASS by relying on the older v0.1.0 logs. No new semantic tag/release was authorized. CRA application integration uses a post-v0.1.0 pseudo-version and requires the real Go sum checksum; this is a separate repository-level gate.

**Latest as of 2026-10-18:** NK-08 explicit `jetstream.subject_update_policy=replace` is **DONE (owner-reported `task verify:strict` acceptance)**; last confirmed NATSKit source commit `0960ffa252277ba976e4ff282c4f639fd7eddb77` fixed gofmt in `config.go`, after `1ca545f7e95f339c595decb4ad34f1a5ea49787d` fixed `reconcile.go`. The complete final PASS log was not shared, so avoid claims of independent per-test results. No post-v0.1.0 release approved. **Next work is in CRA, not NATSKit**: dependency checksum/targeted tests and local non-real-data JetStream smoke. Only revisit NATSKit for a concrete reproduced regression; do not create new tasks/tags.
