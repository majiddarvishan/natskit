# NATSKit — current status

**Reviewed:** 2026-10-18. Source-of-truth is the actual remote `main`, not these historical SHA values.

## Audit baseline

- Before creation of this independent `.codex`: `main` HEAD was `25f0db6b10c59b07228c6ed17b2c3db3026655cb`.
- Module: `github.com/majiddarvishan/natskit`; declared Go `1.26.0`, `nats.go v1.51.0`.
- Managed reconciliation, fail-closed policy/subject checks and disposable regression tests are committed. `Connect` remains read-only.
- Last code fix (`25f0db6b10c5`) corrected passing `ResolvedConsumer` instead of `want.Config` to `consumerPolicySummary`.
- A previous owner `task verify` reached Go compilation and found that error; the correction was pushed. **A complete post-fix standalone `task verify` result has not been supplied**. Do not mark library tests passed on the basis of another application's successful tests.
- Existing `task verify` can silently skip disposable integration tests if the external `nats-server` binary is absent. A separate strict preflight gate is added for independent acceptance.
- No NATSKit release/tag or production migration has been approved. Existing GitHub workflow was neither enabled nor modified by this documentation effort.

## Current gate status

| Gate | Status | Evidence |
| --- | --- | --- |
| Generic API, managed reconciliation source and regression tests | IMPLEMENTED | Git repository and current Go source |
| Independent engineering state in `.codex` | IMPLEMENTED | This directory |
| Strict disposable-broker quality command | IMPLEMENTED / NOT YET EXECUTED | `task verify:strict` requires local `nats-server` |
| `task verify:strict` with Go 1.26+, installed broker, and no skips from missing broker | **OPEN** | No post-fix local log yet |
| Actual HA/fault behavior beyond existing disposable tests | DEFERRED | Not claimed as production verified |
| Destructive topic/Durable migration | BLOCKED | Needs operator-approved producer fencing/cursor continuity design |

After verification, add the **actual run date, `go version`, `nats-server -v`, result and skip information** in this file. Do not claim success from source review alone.
