# NATSKit — project context

## Identity and scope

- Repository: https://github.com/majiddarvishan/natskit
- Default development branch: `main`
- Go module: `github.com/majiddarvishan/natskit`
- Toolchain declared in `go.mod`: **Go 1.26.0**
- Direct NATS dependency at audit baseline: `github.com/nats-io/nats.go v1.51.0`
- Independent, importable NATS Core + JetStream transport, declarative broker topology validation, explicit provisioning/reconciliation, generic durable pull workers and quarantine transport.
- This is a **library**, not a daemon; it has no runtime Compose service, database, business subjects or application-specific configuration.
- Provenance is historical and documented in `ORIGIN.md`; the ongoing API and engineering tasks are defined **here**, independently of any origin project.

## Public contract

- `Decode` parses a strict `Config`; `Config.Resolve` validates configuration offline.
- `Connect(ctx, cfg)`: Core transport or read-only JetStream topology verification; no broker resource writes.
- `ConnectReconciled(ctx, cfg)`: explicit managed startup with a complete preflight, approved additive operations, final verification and a non-sensitive `ReconcileReport`.
- `ProvisionMissing(ctx, js, resolved)`: operator-oriented missing-resource creation with no update/delete.
- `Reconcile(ctx, js, resolved)`: explicit lower-level managed broker reconciliation; structured `ReconcileBlocked` errors and stable diagnostic codes.
- `Client.Publish` / `PublishConfirmed`: Core or JetStream publishing; any deduplication requires JetStream and a stable message ID.
- `Client.StartWorker` / `StartWorkerWithOptions`: bind existing durable pull consumers; no implicit consumer provisioning.
- `Worker.Stop` and `Client.Close`: controlled teardown.

## Compatibility and release policy

- The owner explicitly approved the initial `v0.1.0` release on 2026-10-18. Tag/Release publication is still pending; consumers should continue pinning immutable commits until the tag actually exists.
- Optional physical Stream/Durable names preserve *existing* names; they do not authorize migration.
- Documentation and Taskfile changes on `main` do not require an unrelated downstream application to change its pinned module version. Pin only after code/API changes warrant it.
- The library must be independently testable with disposable loopback `nats-server`; never require an external application's deployment or private credentials.

References: [README.md](../README.md), [CONFIG_REFERENCE.md](../CONFIG_REFERENCE.md), [config.example.json](../config.example.json), [ORIGIN.md](../ORIGIN.md).
