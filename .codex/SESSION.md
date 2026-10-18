# NATSKit — session record

## 2026-10-18 — first independent engineering handoff

1. Inspected the repository's real remote `main` at `25f0db6b10c59b07228c6ed17b2c3db3026655cb` and the full repository tree. There was no pre-existing `.codex` directory.
2. Reviewed Go module identity/toolchain, config reference, README, origin, API source, safety-oriented reconcile/worker tests, Taskfile and existing CI configuration.
3. Created an **entirely self-contained** `.codex`; its context and task register are not maintained by any other repository.
4. Implemented a separate opt-in `task verify:strict` to reject a missing `nats-server` rather than accepting skipped JetStream integration tests. Standard `task verify` remains unchanged.
5. Kept application transports, broker subjects, data and credentials out of this change. No new runtime or package dependency, Go API change, deployment, tag, release or CI workflow edit.
6. Source edits were pushed via GitHub connector. **Local Go 1.26 / Task / disposable NATS execution was not available in this session.** Treat NK-04 as OPEN until owner supplies successful independent logs.

## Continuation rules

- Verify the *new* real `main` HEAD and current task status when resuming.
- Work from `TASKS.md`, not historical conversations or another repository's state.
- Do not run a live broker, reset state, or claim test success without evidence.
- Record any future validated release decision separately; this session authorized none.
