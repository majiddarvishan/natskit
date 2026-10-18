# natskit — portable NATS / JetStream Go library

Independent Go module: `github.com/majiddarvishan/natskit`. No import from CRA
Bridge, SARA, Telara, Protobuf, Postgres, fluxlog, or application-specific code.
Requires Go 1.26 or newer. See [ORIGIN.md](ORIGIN.md) for extraction provenance
and [the standalone engineering handoff](.codex/START_HERE.md) for current
architecture, decisions, status, task register and next-chat instructions.

The first release is [v0.1.0](https://github.com/majiddarvishan/natskit/releases/tag/v0.1.0), an annotated tag pinned to the verified library code:

```sh
go get github.com/majiddarvishan/natskit@v0.1.0
```

For experimental post-release changes, pin a separately reviewed immutable commit.

## Config model

Pass the JSON nested under the application's `nats` key to `natskit.Decode`.
See [`config.example.json`](config.example.json) and [configuration/reference guide](CONFIG_REFERENCE.md). Stream inheritance order:
`defaults.stream < profiles[stream.profile] < streams[NAME]`;
Consumer/Worker inheritance: `defaults.consumer < consumer[NAME]`,
`defaults.worker < consumer[NAME].worker`. Keys used under `subjects`, `streams`,
and `consumers` are aliases; no business roles are hardcoded.

`security` is fully optional; `security.tls` is independently optional.
`instance_id` is unique per process, while `jetstream.namespace`, Stream names,
and Durables are identical across replicas. Stream and Durable physical names
are `UPPERCASE_NAMESPACE_LOGICAL_NAME`. The connection's NATS name is generated
from instance ID. All replicas of one deployment **must** have identical
Stream/Consumer configs, and different independent deployments need distinct
actual wire subjects/accounts as well as different namespaces.

For compatibility with already-provisioned brokers, a Stream can optionally
specify `streams[KEY].name` and a Consumer can specify
`consumers[KEY].durable`. These fields retain the exact supplied physical
identity, rather than generating a namespaced one; they must be explicitly
configured and reviewed by the application adapter. Duplicate or malformed
physical names are rejected. Defaults remain namespace-derived. This is a
read-only naming capability, **not** permission for an automatic migration.

## Lifecycle

1. Use `cfg.Resolve()` for entirely offline, read-only validation.
2. In a separate operator process with NATS admin credentials, call
   `ProvisionMissing(ctx, js, resolved)` to create absent resources; it never
   updates or deletes existing resources. Never execute on a live broker without
   operator approval and a migration plan.
3. `Connect(ctx,cfg)` connects and calls `Verify` when JetStream is enabled;
   it fails closed if resources are missing or drifted. It never provisions.
   Apps explicitly opting into non-destructive managed startup use
   `ConnectReconciled(ctx,cfg)` instead.
4. `client.Publish(ctx, alias, bytes, stableMessageID)` and
   `client.StartWorker(ctx, logicalConsumerName, handler)` are generic.
   Return `nil` only after the application's database side effects have
   committed. Duplicate deliveries remain possible: implement idempotency.
5. `worker.Stop()` and `client.Close()` on shutdown.

## Scope and safety

- This library owns generic transport behavior only. Applications retain their
  database transactions, domain idempotency, wire contracts and compatibility
  adapters. Importing the module never authorizes a broker migration or a
  production transport switch.
- `pull` Durable with explicit ACK is implemented for multi-replica workers.
  Push, ACK-all, and ACK-none modes are rejected instead of silently degrading.
- `WorkQueuePolicy` forbids overlapping Consumer filters. Acks remove entries
  from workqueue storage; `LimitsPolicy` does not remove on ACK.
- Backoff applies to ACK timeouts, not NAK. One `in_progress_interval` must be
  less than the first backoff delay/ACK deadline; handler retries need idempotency.
- Quarantine stores poison payloads, which may contain sensitive information.
  Authorization, ACLs, quotas, redaction and retention must be assessed before
  a production deployment. A confirmed PubAck is required before origin ACK.
- MaxDeliver advisories are **best-effort**; the module exposes advisory and
  copy methods. Automatic lossless reconciliation is **not implemented**, and
  an optional reconciliation config does not activate an automatic scanner.
  Operators must reconcile the exhausted source positions separately.
- `instance_id` environment interpolation must resolve to a non-empty value.
  Local test: `NATS_INSTANCE_ID=dev-1 go test ./...`.

## Verification

Both `go.mod` and `go.sum` are committed and CI verifies that `go mod
tidy` leaves them unchanged. The worker's `Stop()` method is safe for
repeated and concurrent calls; it cancels the fetch loop and waits for
in-flight handlers once. It does not claim exactly-once processing.


```sh
go mod tidy
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
```

`task verify` runs the same formatting, dependency, unit, race and vet gates.
**For independent release-readiness evidence use `task verify:strict`: it first
requires `nats-server` on `PATH`, prints the broker version, and then runs
`task verify` unchanged.** This prevents reporting a complete disposable
JetStream gate when the broker executable is missing. Tests always use their
own loopback ports and temporary data directories; no running external broker
is required. Record any test skips separately.

CI installs `nats-server` v2.14.6 so disposable integration tests do not skip,
and repeats the controlled three-node failover drill three times under race.
The worker uses ordinary reconnect; transparent automatic pull-subscription
rebind and automatic recovery/replay are outside the supported scope.

Unit tests never use production resources. Integration tests launch a
`nats-server` executable bound to a random local port with a temporary data
directory. They cover two replicas sharing a Durable, handler failure with
redelivery before ACK, and quarantine PubAck failure without acknowledging the
origin. They skip if the binary is unavailable; the isolated CI job explicitly
installs it. These do **not** establish full crash/reconnect/fault readiness
or exactly-once delivery, and never act on operational brokers.

## Optional application-owned Worker delivery policy (2026-10-15)

`StartWorker(ctx, key, handler)` retains its original zero-option
behavior: retryable handler errors remain unacknowledged until the broker's
AckWait/BackOff. Applications requiring an explicit retry policy may invoke
`StartWorkerWithOptions(ctx, key, handler, natskit.WorkerOptions{...})`.

- `RetryDelay` enables `NakWithDelay` on retryable failures; zero
  preserves the original broker-controlled retry behavior.
- `BuildQuarantine(message, metadata, reason)` supplies application-owned
  replay headers and a stable NATS Msg-Id. NATS Kit rejects a changed body,
  destination subject, missing Msg-Id, builder errors and builder panics.
  Quarantine PubAck must confirm the dedicated configured Stream before the
  original can be acknowledged.
- `Observe(DeliveryEvent)` reports action, subject, byte count, broker
  stream, sequence and attempt number. The original body is never provided
  through this event. The callback may panic without affecting ACK handling;
  applications must avoid logging `event.Err` because it may contain PII.
- A failed Quarantine PubAck leaves the source unacknowledged. At the
  broker MaxDeliver ceiling, an operator still needs reliable advisory
  capture and reconciliation. This library has **no** exactly-once
  processing or lossless automatic replay guarantee.

Applications supply their own configuration mapping, NAK delay and replay
headers through the generic hooks. No application-specific adapter belongs
inside this repository.


## Managed startup reconciliation

Applications that own their NATS topology can opt in explicitly with
`ConnectReconciled(ctx, cfg)`. Ordinary `Connect` still performs **only
read-only verification** and retains its existing behavior.

`ConnectReconciled` preflights **every** configured Stream and Durable
before any mutation. It may create missing resources and **add** new Stream
subjects without removing the old capture patterns. It verifies the complete
topology after any operation, and handles concurrent create/expand races by
inspecting the actual winner. Since NATS cannot atomically update multiple
resources, interruptions may leave only those non-destructive additions
partially applied; a subsequent startup can converge safely.

A changed durable filter, removed/replaced Stream subject, or policy drift
fails closed via `ReconcileBlocked`, including a stable error code, resource
identity, reason, risk, and suggested recovery action. Consumer-filter errors
also contain observed pending counts. **No consumer deletion/recreation,
message purge, or volume cleanup is ever performed.** A zero pending count
alone cannot prove that all external producers and other application
replicas have stopped, so it does not authorize changing the filter.

`Reconcile(ctx, js, resolved)` is the lower-level generic API and returns
`ReconcileReport` with non-sensitive change counts. `ConnectReconciled`
exposes this report as `Client.Reconciliation`. The caller still decides
whether managed startup is appropriate and owns operator-facing diagnostics,
credentials, ACLs and cutover policy. There is no new configuration switch.

Automated subject replacement and durable-filter migration **are not
implemented** until a coordinated producer/consumer fence and a verifiable
lossless cursor handoff exists.

## Initial release

**v0.1.0 is officially published**, with [release notes](docs/releases/v0.1.0.md) and [GitHub Release](https://github.com/majiddarvishan/natskit/releases/tag/v0.1.0). The annotated tag points to the verified commit `0e27d162f0c43225e366da66d340b3f767d667d8`.
