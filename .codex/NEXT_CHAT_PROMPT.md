# Continue NATSKit — independent project handoff

Act as a senior Go / NATS JetStream / reliability engineer. Continue the **standalone** project only:

- Repository: https://github.com/majiddarvishan/natskit
- Branch: `main`
- Go module: `github.com/majiddarvishan/natskit`
- Go version in module: `1.26.0`
- Base implementation commit reviewed on 2026-10-18: `25f0db6b10c59b07228c6ed17b2c3db3026655cb` (not necessarily the present HEAD).

First inspect the real Git HEAD, working tree, `go.mod`, `Taskfile.yml`, README, and **every current `.codex` file** in this order: `START_HERE.md`, `PROJECT_CONTEXT.md`, `ARCHITECTURE.md`, `DECISIONS.md`, `CURRENT_STATUS.md`, `TASKS.md`, `SESSION.md`.

Proceed only with **open tasks in NATSKit `.codex/TASKS.md`**. The next substantive task is **NK-04**: one full local `task verify:strict` run with an installed disposable-test `nats-server`, record Go/server versions, gofmt, clean module tidy, unit, race, vet, and any skips. Prior compilation failure was fixed but no independent post-fix full PASS was reported. Do not fabricate results.

Critical invariants: generic library, no application dependencies; `Connect` verifies only; explicit `ConnectReconciled` may add missing resources/subjects; unsafe subject/Durable changes refuse with typed diagnostics; Quarantine PubAck precedes source ACK; database idempotency is application's responsibility; never delete/purge broker data or infer authorization for a production migration. No customer NATS, PostgreSQL, Docker stack, forced tag/release, GitHub Actions changes or extra feature work.

Fix only reproducible remaining problems. If the gate cannot run in this environment, provide the exact local commands and keep the gate OPEN. Keep `.codex` updated so a future chat can proceed without another project's context.
