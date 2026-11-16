Stack, bottom to top. Each link shows only the commits of that PR.
1. #11748 Quint specs of the MVCC passive checkpoint
2. #11779 Replay of Quint traces on the engine ([new commits only](https://github.com/pereman2/limbo/compare/pere/formalize-mvcc-passive-checkpoint...pere/mvcc-trace-replay-harness))
3. #11794 Drop rootpage_gc_protected from GC ([new commits only](https://github.com/pereman2/limbo/compare/pere/mvcc-trace-replay-harness...pere/mvcc-one-gc-floor))

## Problem statement

The MVCC passive checkpoint has no written design. Its rules grew one bug fix at a time. The open data loss and corruption issues (#11499, #11500, #11501, #11502, #8004, #7960, #8077) come from the checkpoint running together with DDL, root page reuse and recovery. Nothing checks these interactions as a whole.

## Changes

This PR adds Quint specs of the passive checkpoint in `quint/mvcc-passive-checkpoint/`. There are three specs: the row protocol and GC, table ids with root pages and the schema cookie, and sequence compaction. TLC checks them with the `quint verify --backend tlc` command.

`DESIGN.md` states the design and maps each rule to the code. Each special case in the code has a variant that turns it off. The variant gives a counterexample, or it shows that the model does not need the special case at these bounds.

`HARNESS.md` describes a test that replays the traces on the engine. That test is not in this PR. PR #11779 builds it. `SIMPLIFY.md` is a plan to make the checkpoint simpler and keep the checked invariants.

This PR also adds `FINDINGS.md`, a "Scope and limits" section in `README.md`, and a small CI job (`.github/workflows/quint-spec.yml`).

### Suggested reading order

1. `README.md`: the tools, the make targets, and "Scope and limits".
2. `FINDINGS.md`: what the model found, what it only reproduced, and what a Rust run refuted.
3. `DESIGN.md`, then `DESIGN-rows.md`, `DESIGN-schema.md` and `DESIGN-sequences.md`.
4. `MvccPassiveCheckpoint.qnt` and its `Configs` file, then the other two specs.
5. `MODEL-GAPS.md` and `SIMPLIFY.md`.
6. `HARNESS.md` and `SYNC.md`, which are design notes and do not run.

### What the model found

`FINDINGS.md` has one row for each of 19 claims in the docs, with a verdict and its evidence. The verdicts are:

- 3 held up. The model showed the bug first, and a Rust test confirms it:
  - #11750: a `setval` during a checkpoint is lost, and `nextval` repeats values.
  - #11751: an `ALTER` during the first checkpoint of a table loses its rows after a crash.
  - #11749: a rollback resets `materialized_at`, so GC keeps the version in memory. It is a memory leak, not a correctness bug.
- 5 found by Rust runs or code reading. The model came later or not at all (#11831, #11835, #11836, #11833, and the dead publish drain bit).
- 1 refuted. The model said that `backfill_floor` alone is a safe GC floor. A Rust run showed that a checkpoint on the connection of an open reader breaks this (#11794).
- 5 unverified leads without a Rust test.
- 4 rows of known issues that the model only reproduced (11 issues in all). People had reported them before the model existed, for example #11499, #11500, #11501 and #11502.
- 1 row of simplification evidence (`rootpage_gc_protected`, #11794).

Some of the docs said that the model "caught" the known issues. That wording was too strong. The docs now say "reproduced" for them.

### Scope and limits

The bounds are small, for example 1 key and 2 readers in the row spec. The specs leave out mode B (the default), most of mode T, index rows, real cursor scans, the statement retry loop, and a checkpoint on its own connection. The full list is in `quint/mvcc-passive-checkpoint/README.md`, section "Scope and limits". These results are evidence within these bounds and this model, not proof.

### How to run

Run these in `quint/mvcc-passive-checkpoint/`. `README.md` has the install steps for Quint 0.33.1 and Java 17 or later.

```bash
make typecheck   # a few seconds
make check       # main configuration of each spec, 3.5 to 17 minutes each
make variants    # every variant, hours for the row spec
make variants ONLY="setvalRace issue11501"   # a few variants
make bug-traces  # one failing trace for each failing variant
```

### Follow-ups

- Replay the traces on the engine (#11779).
- Run more variants in CI. The new job runs 8 small variants only.
- Model mode B (the default) and mode T.
- Model index rows.
- Run larger bounds for the deletion of `rootpage_gc_protected` (#11794).
- Add a `PARITY.md` that maps the code to the specs. Today the map is manual `file:line` tables at commit 719fcc411.

<details><summary>Tests</summary>

- `make check`: the main configuration of each spec passes with the TLC backend (rows 16.8M states, schema 16.0M, sequences 4.0M).
- `make variants`: each variant gives the result in its comment.
- `make bug-traces`: writes a failing trace for 32 of 36 failing variants, every issue variant included.
- `make typecheck` and 8 small variants (`noSnapshotClamp noGcWait noBackfillStop setvalRace noDeferDeletes issue11501 issue11502 issue8004`) pass on a local machine in about 3 minutes with Quint 0.33.1 and Temurin 21. The CI job runs the same commands. It has not run on a GitHub runner yet.

</details>

🤖 Generated with [Claude Code](https://claude.com/claude-code)
