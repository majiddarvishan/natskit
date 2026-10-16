# Extraction provenance

This repository retains the 44 commits that changed `modules/natskit` in
`Shayan-Pardaz/cra-bridge`, projected to the repository root with original
authors, timestamps and commit messages. Projected commit hashes differ.
No CRA application source or unrelated repository history is included.

- CRA extraction checkpoint: `cd27312917ec143c899aac975fa015c4429b5cf7`.
- Source-matched CRA CI checkpoint: `a3c072ca421d61dfd1f65808b2740de9ea323f87`.
- Last library path commit: `4b5f5db` (controlled failover test).
- Destination authorized by the owner on 2026-10-16:
  `https://github.com/majiddarvishan/natskit`.

The extraction changes the module identity to
`github.com/majiddarvishan/natskit` and adds standalone documentation and CI.
Transport implementation and tests are preserved. No release tag, customer
deployment, automatic pull rebind or new recovery feature is part of this move.
