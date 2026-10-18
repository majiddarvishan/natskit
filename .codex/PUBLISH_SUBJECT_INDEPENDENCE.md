# NATSKit — decouple publishing from resource ownership

**Owner-approved architecture:** 2026-10-19. **State:** DESIGN ACCEPTED; source changes and tests OPEN.

## Generic messaging rule

Application A publishes to configured subject1, B consumes subject1; B publishes to subject2, A consumes subject2. Later subject1/subject2 may be changed to X/Y via configuration and restart when the broker capture and receiver subscriptions are prepared. New and historical messages on different Subjects have no inherent relation; **never migrate, purge, replay, re-key or reset them simply because routing addresses changed**.

A publisher only needs a valid authorized NATS Subject and, for JetStream durability, an existing broker Stream that captures it and returns a genuine PubAck. **The publisher does NOT need to declare or own that Stream.** Receiver-owned Stream/Consumer policy and lifecycle remain outside the publisher's authority. Core NATS publish has no durable PubAck guarantee.

## Current source mismatch

At 2026-10-19 inspected \`broker.go\`: \`Client.PublishConfirmed\` resolves a Subject alias, loops over \`Client.Topology.Streams\` to find an owned/configured matching capture and rejects a Subject not covered by the current application's Streams. It then demands \`PubAck.Stream == target\`. \`topology.go\` validates configured managed Streams/Consumers together; this is appropriate for **managed resources**, not required for every outgoing route.

This accidentally conflates outbound addressing with resource ownership. CRA briefly introduced \`cancel_external_stream\` as an application-specific workaround; that is **not** the requested or approved generic architecture and is to be removed by CRA after the generic solution lands.

## Accepted contract and constraints

- Generic outgoing Subject/alias registry is independent of the subset of managed Streams. Client should be able to Publish/PublishConfirmed using a configured concrete outbound Subject absent from \`Resolved.Streams\`.
- **JetStream PubAck:** return actual stream name, sequence and duplicate status. If caller explicitly supplies an optional expected Stream, require broker-side \`ExpectStream\` or equivalent and verify the actual ack stream; mismatch/error fails closed. If no expected Stream is supplied, broker-confirmed success from whichever correctly configured Stream owns capture is sufficient. Never treat PubAck as receiver/business completion, fabricate ack, or fall back silently to Core NATS.
- **Ownership:** Publish never calls Add/Update/Delete/Purge/CreateConsumer; no implicit external Stream enrollment or ownership transfer. Existing read-only \`Connect\` / \`Verify\` and managed \`ConnectReconciled\` / \`Reconcile\` retain distinct responsibilities; only explicitly managed topology resources may be reconciled. Verify may validate owned Streams/Consumers, but should not require every outbound Subject to be covered by them.
- **Subscriber semantics:** Consumer filters, Durable identity, AckFloor and capture expectations belong to the receiver. Changing an inbound address does not silently mutate an existing Consumer filter/reset cursor; use controlled receiver-owned provisioning and fail with clear diagnostics when missing.
- **Compatibility:** Retain existing stable NATSKit publish API where possible, add a small generic extension/options only if required; preserve NATSKit's independent module, no CRA/Cancel-specific concepts, no speculative automatic migration logic. Keep existing \`subject_update_policy=replace\` feature for actual managed Stream Subject changes; it must not be the default fix for changing outgoing addresses.
- **Security:** Broker permissions decide whether a publisher can write to a Subject; read-only optional receiver-Stream inspection only if configured and authorized. Never use another app's Stream/Consumer update credentials for publishing.
- **Tests:** use disposable local NATS with A and B publishing to each other's Subjects on separately provisioned capture Streams, then reconfigure X/Y with appropriate pre-provisioned capture/consumers; assert correct PubAcks and negative paths, immutability of unrelated resources/messages/durables, no migration. Run focused unit/disposable integration, plus one independent \`task verify:strict\` after code settles.

## Backlog and sequence

The independent tasks live in [TASKS.md](TASKS.md) as \`NK-PUB-01\`, \`NK-PUB-02\`, and \`NK-PUB-03\`. Work on this repository **before CRA Bridge**, then allow CRA to adapt and remove its untested Cancel-specific workaround. CRA cross-repo plan: https://github.com/Shayan-Pardaz/cra-bridge/blob/feature/natskit-standalone-extraction/.codex/SUBJECT_ROUTING_DESIGN.md.

**No code or tests changed on approval day.** This approval does not authorize touching production/owner's local private broker state or creating a release/tag. Keep \`v0.1.0\` immutable.