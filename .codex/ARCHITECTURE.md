# NATSKit — architecture and invariants

## Config and isolation

```text
config.example.json / Config.Decode
            |
      Config.Resolve          (offline, no NATS connection)
            |
       Resolved topology
            |
     +------+----------------------------+
     |                                   |
Connect(ctx, cfg)             ConnectReconciled(ctx, cfg)
     |                                   |
read-only Verify              complete broker preflight
     |                                   |
bind existing resources       create missing Streams/Consumers
                                 or add Stream subjects only
                                         |
                                  read-only final Verify
                                         |
                               Client + ReconcileReport
```

The `jetstream.provisioning` field does **not** activate automatic provisioning. A caller must explicitly select `ConnectReconciled`. In Core mode there is no JetStream resource provisioning. Namespace identifies logical deployments; actual wire subjects/accounts must still be isolated. Physical Stream/Durable names and filter subjects must remain compatible across replicas.

## Managed reconciliation: safety contract

1. Inspect **all** desired Streams and Consumers before any mutation.
2. Refuse incompatible policy changes and subject removals/replacements with `ReconcileBlocked` (resource, current/desired policy summary, reason, risk, suggested operator action and stable code).
3. Only create missing resources or extend existing Stream subject sets. Never change an existing Durable filter, ACK policy or cursor; never delete/purge resources or broker storage.
4. Verify the full topology after operations. Concurrent creators/expanders may race; reinspection must confirm compatibility.
5. JetStream has no cross-resource transaction: a failed run can leave **non-destructive partial additions**. Retry/converge only under the same safety rules.

Relevant files: `topology.go`, `provision.go`, `reconcile.go`, `broker.go`, `reconcile_test.go`, `topology_test.go`.

## Worker delivery and quarantine

- Multiple process instances bind the same named pull Durable. `max_ack_pending` is broker-shared; concurrency limits are per worker/process.
- Business handlers own idempotency. Return `nil` only after durable application-side effects complete. Duplicate redelivery is possible.
- Retryable errors remain unacknowledged or use explicitly configured `NakWithDelay` via `WorkerOptions`. Unsupported ACK modes fail validation.
- Decode/permanent/exhausted failures may be copied to a dedicated Quarantine Stream. Confirm the quarantine PubAck **before** ACKing the source.
- Quarantine failures leave the original unacknowledged and require operator reconciliation once MaxDeliver is reached. Advisory notifications are best-effort, not proof of a lossless recovery.
- Delivery observation intentionally omits message bodies. Avoid logging `DeliveryEvent.Err` or quarantine payloads (possible sensitive data).

Relevant files: `worker.go`, `advisory.go`, `worker_options_test.go`, `worker_integration_test.go`.

## Isolation for testing

- Pure unit/config tests: no external NATS, no application database.
- Disposable integration: spawn the `nats-server` **binary** on localhost with temporary storage; two worker replicas, retries/ACK, quarantine failure, reconnect, and isolated three-node leader-failover coverage.
- Tests that cannot find `nats-server` call `t.Skip` in the original suite. `task verify:strict` first requires the binary, so a missing binary cannot be mistaken for a complete integration PASS.
- The strict gate does not cover deployed secrets, live production behavior, unknown external publisher fences, or cross-process cutover.
