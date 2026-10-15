# natskit — portable NATS / JetStream Go library (extraction candidate)

Independent Go module under `modules/natskit`. No import from CRA Bridge, SARA,
Telara, Protobuf, Postgres, fluxlog, or application-specific code. Designed to be
moved into its own Git repository with the same module path in a later release.

## Config model

Pass the JSON nested under the application's `nats` key to `natskit.Decode`.
See [`config.example.json`](config.example.json). Stream inheritance order:
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
4. `client.Publish(ctx, alias, bytes, stableMessageID)` and
   `client.StartWorker(ctx, logicalConsumerName, handler)` are generic.
   Return `nil` only after the application's database side effects have
   committed. Duplicate deliveries remain possible: implement idempotency.
5. `worker.Stop()` and `client.Close()` on shutdown.

## Scope and safety

- This is an **independent module foundation**, not yet a CRA runtime adapter.
  The old CRA JetStream path is left in place intentionally until an explicit,
  tested adapter and migration can preserve existing Stream/Durable identities.
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
cd modules/natskit
go mod tidy
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
```

No live resources are created or changed by tests in this initial foundation.
