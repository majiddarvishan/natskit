# NATS Kit — configuration and independent module reference

## Module boundary

Go import path: `github.com/majiddarvishan/natskit`. Source is maintained in
its own repository and does **not** import CRA, protobuf, PostgreSQL, business
models, product loggers or metrics. No release tag has been created.

The application may wrap the library JSON in `{"nats": {...}}`, but pass
**only** the nested `nats` JSON object to `natskit.Decode`. Unknown JSON
fields are errors. The reference manifest is
[`config.example.json`](config.example.json).

## Connection

- `instance_id`: nonempty, unique per process. `${NATS_INSTANCE_ID}` is
  expanded from the environment. The connection name becomes `nats-<id>`.
- `connection.urls`: nonempty NATS URL list. `connection.name` is not
  supported. Userinfo credentials in URLs are rejected.
- `connection.drain_timeout`: positive Go duration; default `30s`.
- `connection.security`: fully optional. `creds_file` and `tls` can be
  configured separately. `tls.ca_file` is optional; a client certificate
  needs **both** `cert_file` and `key_file`. Refer to external file paths,
  do not commit credentials into JSON.
- `subjects`: alias -> actual wire NATS subject/pattern. Publishing requires
  a concrete (non-wildcard) subject. Application-specific names belong in
  the adapter, never as defaults in the library.

## JetStream resource model

`jetstream.enabled=false` leaves the library using Core NATS, without
JetStream verification or resource creation. When enabled:

- `namespace` identifies the deployment, not the replica. Keep it stable
  across all replicas.
- `provisioning` must be `verify` or `operator_only`. **Neither** enables
  automatic provisioning on `Connect`.
- `profiles[NAME]` stores reusable Stream settings.
- `defaults.stream` -> selected `profiles[NAME]` -> `streams[KEY]`
  is the precedence chain for Stream fields.
- `defaults.consumer` -> `consumers[KEY]` is Consumer precedence;
  `defaults.worker` -> `consumers[KEY].worker` is Worker precedence.
- `streams[KEY].subjects` and `consumers[KEY].filter` are arrays of subject
  **aliases**, not arbitrary literal subjects. Consumer filters must be fully
  covered by their Stream.
- Stream physical name defaults to `UPPERCASE_NAMESPACE_KEY`; optional
  `streams[KEY].name` preserves an existing, explicitly supplied physical
  name **without renaming or migrating** it.
- Durable physical name defaults to `UPPERCASE_NAMESPACE_KEY`; optional
  `consumers[KEY].durable` preserves an existing Durable name.
  Duplicate physical names are rejected.
- Stream settings include `storage` (file/memory), `retention`
  (limits/workqueue/interest), `discard` (old/new), `max_age`,
  `duplicate_window`, `max_bytes`, `max_msg_size`, `replicas` (1/3/5).
- Consumer settings support pull, explicit ACK, deliver policy all/new,
  positive `max_deliver`, `max_ack_pending`, `ack_wait` or `backoff`
  (not both). Workqueue Consumer filter overlaps are rejected.
- Worker settings: `fetch_batch`, `max_concurrency`,
  `ack_sync_timeout`, `in_progress_interval` (shorter than ACK deadline).
  Maximum concurrency is **per process**; broker `max_ack_pending` is
  shared across processes on the same Durable.

## Runtime and operator boundary

1. Validate entirely offline with `cfg.Resolve()`.
2. An approved, separate operator command may invoke
   `natskit.ProvisionMissing(ctx, js, resolved)` using a dedicated
   administrative identity. It **never** updates or deletes existing
   Stream/Consumer resources; it refuses drift.
3. Application `natskit.Connect(ctx,cfg)` verifies existing topology,
   **never creates it**, then returns a Client.
4. Publish with `client.Publish(ctx, alias, payload, stableMessageID)`.
   A PubAck confirms storage only. In Core mode no broker deduplication
   is available.
5. Start pull Workers with `client.StartWorker(ctx, consumerKey, handler)`.
   Return nil **only** after all application side effects are durably
   committed. Stop workers and close the Client cleanly.
6. Permanent/decode failures may be sent to a dedicated, PubAck-confirmed
   Quarantine Stream before source ACK. Normal errors are not ACKed and
   redelivery can occur. Applications must persist their own idempotency.
   Lossless MaxDeliver advisory capture/reconciliation is **not implemented**:
   optional automatic reconciliation is rejected. A best-effort watcher and
   manual copy utility exist; manual copy is not source reconciliation.

A different namespace alone does not isolate physical wire subjects in one
account. Distinct deployments must avoid overlapping Stream subject captures,
or use separate NATS accounts/subjects. This library does not provide
exactly-once business execution, database transactions or an automatic
migration from existing broker topologies.

## Independent verification

```sh
go mod tidy
git diff --exit-code -- go.mod go.sum
test -z "$(gofmt -l *.go)"
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
```

Disposable broker integration tests run when `nats-server` is installed.
The GitHub Actions module workflow installs version 2.14.6 explicitly,
launches an isolated loopback server with temporary storage, and checks
shared Durable, redelivery, and Quarantine failure paths. Broader crash,
reconnect, sustained HA and production operations need separate evidence.

## Independent repository and version policy

Source is maintained at `https://github.com/majiddarvishan/natskit`.
Consumers pin immutable commits until the owner approves a semantic release.
No release tag is created by this extraction. API compatibility and a future
pre-1.0 version require an explicit release review. Database commits, business
idempotency and wire-contract adapters remain application responsibilities.
This repository does not authorize live provisioning, replay or deployment.

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

Application-specific configuration, NAK delay and replay headers belong
in consumer adapters, not in this generic transport library.

