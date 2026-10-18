# NATSKit — accepted decisions

| ID | Decision | Reason / guardrail |
| --- | --- | --- |
| NK-D01 | Keep this as an independent Go module with no application imports. | Library is reusable across projects and tested in isolation. |
| NK-D02 | Keep `Connect` read-only. Managed startup requires the explicit `ConnectReconciled` API. | Importing/connecting must not silently modify a broker. |
| NK-D03 | Preflight **every** Stream and Durable before any managed mutation. | Detect unsupported drift before beginning partial additions. |
| NK-D04 | Reconcile missing resources and **additive** Stream subjects only. | Preserve messages and existing consumer cursors. |
| NK-D05 | Block subject removal/replacement and Durable filter/policy changes. | Pending counts alone cannot establish a coordinated, lossless external producer and cursor handoff. |
| NK-D06 | Do not auto-delete/recreate Streams or Consumers, purge data, modify broker storage or replay poison records. | Such actions require a separate approved recovery/cutover design. |
| NK-D07 | Pull Durable + explicit ACK; Quarantine PubAck before source ACK; at-least-once semantics. | Application commits and idempotency cannot be provided by the transport library. |
| NK-D08 | Real integration tests must use disposable localhost servers and temp directories. | No customer broker, production volume, or external database required. |
| NK-D09 | Library consumers pin commits pending explicit semver review. | First `v0.1.0` is now owner-approved; tag/release creation and future versions still require explicit verified handling. |
| NK-D10 | No automatic pull subscription rebind/replay guarantee after all network/cluster failure classes. | Recovery outside tested paths remains a documented limitation, not an implied guarantee. |
| NK-D11 | `task verify` remains a fast standard source gate; `task verify:strict` requires a locally installed `nats-server`. | Distinguish missing-tool test skips from verified disposable-broker coverage without changing existing workflow. |

Changes to these decisions require an explicit design rationale, tests, and review; do not infer permission from a request to tidy documentation.

## 2026-10-18 — Release decision

The owner approved publishing exactly `v0.1.0` after the NATSKit isolated strict gate passed. This does **not** authorize unrelated source changes, CRA main merge, customer-data access, forced tags, unsafe broker migrations or additional version releases. The available GitHub connection cannot perform create-tag/create-release, so final publication requires a separate operator action and subsequent remote verification.

## Release completion (2026-10-18)

The official `v0.1.0` tag and GitHub Release have now been verified, with the annotated tag targeting commit `0e27d162f0c43225e366da66d340b3f767d667d8`. Historical statements about a pending publication are superseded. Tag immutability is mandatory; no automatic tag move, API changes or other version is approved.

## NK-D12 — Explicit managed Stream Subject replacement (2026-10-18)

Owner approved a new **post-v0.1.0** opt-in `subject_update_policy` with `additive` default and `replace` override. Its scope is exclusively the Stream capture Subject list, with positive existing MaxAge, full topology preflight and concurrency checks; no data purge, Stream/Consumer deletion, retention change, Durable filter or ACK policy migration. Removed subjects stop capturing *future* messages; clients/customers accept responsibility for external cutover. Earlier blanket no-replacement rules remain the `additive` default and must be read in light of this explicit new decision.
