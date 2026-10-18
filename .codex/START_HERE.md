# NATSKit — start here

This is the **independent** engineering handoff for the Go module `github.com/majiddarvishan/natskit`. It is not an application deployment plan. Read these files in order:

1. [PROJECT_CONTEXT.md](PROJECT_CONTEXT.md) — scope, repository and version contract.
2. [ARCHITECTURE.md](ARCHITECTURE.md) — runtime, reconciliation, worker and safety boundaries.
3. [DECISIONS.md](DECISIONS.md) — decisions not to undo casually.
4. [CURRENT_STATUS.md](CURRENT_STATUS.md) — verified facts, missing evidence.
5. [TASKS.md](TASKS.md) — **only active NATSKit task register**.
6. [SESSION.md](SESSION.md) — recent actions and evidence.
7. [NEXT_CHAT_PROMPT.md](NEXT_CHAT_PROMPT.md) — cross-chat restart.

## Source of truth

At the start of **every** session inspect the real remote `main` HEAD, `go.mod`, `Taskfile.yml`, relevant implementation/tests and working tree. Historical commit IDs below are evidence, **not** a replacement for reading Git. Do not assume the local tree is clean or that all integration tests ran.

Quick independent gate (does not require a running external NATS):

```sh
task verify
```

**Required disposable-broker gate** (fails immediately when `nats-server` is not installed; tests start their own localhost/temp-data brokers):

```sh
task verify:strict
```

See [TASKS.md](TASKS.md) for gates and their evidence requirements. Neither command authorizes connecting to or modifying a customer broker.

## Hard constraints

- Generic Go module only: no app repositories, application schemas, database models, business subjects, Protobuf contracts or product-specific metrics/loggers.
- Default `Connect` is **read-only topology verification**. Only explicit `ConnectReconciled` can create missing resources or add Stream subjects. Never silently change that contract.
- Unsafe Stream policy/subject removal or Durable filter/policy drift must **fail closed**. No delete, purge, forced resubscription, replay, volume reset or guessed cursor migration.
- Successful PubAck is not a business commit; processing remains at-least-once, and the application owns idempotency and DB commit-before-ACK.
- Only the first `v0.1.0` release has explicit owner approval (2026-10-18); its Tag/GitHub Release publication is still pending. No additional tag, unrelated merge, dependency upgrade, CI change or new feature is authorized. Prefer one final test batch over repeated partial gates.
