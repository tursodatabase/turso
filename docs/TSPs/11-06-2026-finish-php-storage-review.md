# Finish PHP storage review Technical Spec

> **For agentic workers:** REQUIRED SUB-SKILL: Use dmi-superpowers:subagent-driven-development or dmi-superpowers:executing-plans to implement this TSP task-by-task.

**Goal:** Complete PHP PR storage and compatibility review repairs, then resume independent worker concurrency.

**Architecture:** Replace eager full-file fingerprints with a single observed-read authority wrapping default main storage and its IO. Separate permission checks at open from activation before mutations, keep exclusivity through validation/publication and poison failed partial activations. Independently repair aggregate planning and serialize browser prepare.

**Tech Stack:** Existing Rust1.88 core/C API and TypeScript bindings; native filesystem IO, actual yielded I/O, PHP public API, Node24/browser checks. No new service or dependency.

## Global Constraints

Mixed opens include builtin unencrypted MVCC. Immutable connection policy survives shared authority promotion. Unknown identities/codecs/custom storage remain explicit unsupported cases. Do not remove the MVCC/multiprocess safety guards or alter memory-check thresholds. Root owns Cargo/Docker/GitHub mutations; agents edit isolated worktrees. Routine review gates are preapproved by the user's Projects AGENTS.

### Task 1: Valid external outer aggregates

**Blocked by:** None
**Files:** core/translate/subquery.rs; bindings/c/tests/compat/mod.rs; docs/sql-reference/statements/select.mdx
**Interfaces:** Resolve phase assignment through the existing Resolver; preserve translator guard and existing finalized aggregate register cache.
- [x] Reproduce valid no-FROM/empty-FROM/nonempty-FROM aggregate rejection with bounded C children while retaining invalid nested/WHERE controls.
- [x] Pass resolver into outer-aggregate detection and recognize external aggregates alongside builtins; propagate errors.
- [x] Execute `cargo test --locked -p turso_sqlite3 --test compat aggregate_misuse -- --test-threads=1`, reference SQLite controls and builtin outer-aggregate conformance; independent review and commit.

### Task 2: Browser native prepare serialization

**Blocked by:** None
**Files:** bindings/javascript/packages/common/promise.ts; bindings/javascript/sync/packages/wasm/promise.test.ts
**Interfaces:** `async prepare(sql: string): Promise<Statement>` connects before acquiring existing execution gate, then releases in finally.
- [x] Pause a completed actual native worker call before its gate releases, issue prepare, verify zero native entries during the pause and SELECT42 result afterward. Observed RED1/GREEN0.
- [x] Execute focused and full browser sync file plus native prepare/error controls; independent review and commit.

### Task 3: Observed storage authority

**Blocked by:** None
**Files:** core/database/readonly.rs
**Interfaces:** `observed_file`, `observed_io`, `raw_io`; `freeze_validate` returns one writer guard; guard `mark_mutating`/`commit`; `check_health`/`is_active`.
- [x] Run sparse32MB setup byte-volume regression red on existing full-scan implementation.
- [x] Capture read-count and 128-bit checksum observations keyed by file/offset/request, sizes, identities and missing sidecars; deduplicate identical repeats. Forward original completions before releasing read admission; propagate callback errors and conservatively refuse activation after canceled/failed reads.
- [x] Freeze only with no reads in flight; validate via raw retained descriptors under one exclusive main-file guard; mutation failure poisons, successful publication disables observations and retains writer exclusion.
- [x] Execute module tests for lifetime reads, changed data/EOF/size/sidecars, pending completions, guard retry/poison and post-activation pass-through; independent review and commit.

### Task 4: Deferred activation and SDK integration

**Blocked by:** Task3
**Files:** core/database.rs; core/connection.rs; core/statement.rs; core/vdbe/execute.rs; sdk-kit/src/rsapi.rs; tests/integration/storage/header_version.rs
**Interfaces:** `ensure_writable_open` only validates permission/capability; `ensure_writable_ready` activates; `writer_is_active` and `check_storage_health` use cheap atomic state on ordinary paths. Connection routes mutation gates to main or attached authority.
- [x] Change existing active-reader legacy case to require successful writer open and Busy only on first write; observed failure before repair.
- [ ] Install observed main+IO before first engine read; retain resolved default options across open_async yields and allow default SDK opens to use that resolver. Connect SDK with explicit readonly.
- [ ] Gate SQL write transaction start before state changes, explicit checkpoints and raw WAL insert begin. Skip close checkpoint while pending. Preserve TEMP behavior and reject unsafe own legacy-read upgrade as Busy.
- [ ] Prime private header bytes before freeze; validate under exclusion, mark mutation before first disk change, write+sync private header before publishing cache version; preserve WAL/log descriptor retention and MVCC replay metadata.
- [ ] Execute active-reader, stale activation, read upgrade, unused writer close and permission regression checks; update error timing expectations to first mutation and document them.

### Task 5: Activation failure, crash, and delivery

**Blocked by:** Tasks1,2,3,4
**Files:** tests/integration/storage/header_version.rs; matching user storage documentation; fork review evidence
- [ ] Extend real filesystem wrappers with deterministic after-header-write/next-sync error and child exit triggers, retaining file IDs and real locks. Check live handles reject work after partial activation and fresh reopen retains original rows.
- [ ] Execute whole focused C/PHP/storage suite, actual Shuttle where applicable, browser checks, lint/format and Linux/Windows runner matrix. Capture large sparse/read-real-file I/O volume and startup time with matched baseline/candidate.
- [ ] Independent integrated review, repair any defects through red/green loop, push a bounded fork PR and merge only verified source into fork main to update upstream11497. Report remaining upstream review scope requests with explicit user stack rationale.
- [ ] Merge verified PHP main into concurrency branch; continue issue7/PR8 full protocol implementation separately without presenting prerequisites as full concurrency.

Observation memory policy: retain count plus an existing-dependency XXH3-128 checksum for each distinct (descriptor, offset, requested length), not a copy of all read data. Repeated equal observations deduplicate. This is a noncryptographic accidental-change detector with a 128-bit collision limit, improving on the former whole-file 64-bit fingerprint; it does not claim protection against an adversary controlling filesystem contents. Validation still compares identities, sizes, EOF and all recorded ranges under exclusion. No dependency is added.

## Additional reviewed lifecycle checks

Default filesystem opens reserve the registry entry before opening a descriptor; another opener observing an in-progress open yields without creating an alias descriptor. Completed default options are released from the open state. The unobserved backend is passed to independent ATTACH/VACUUM databases so one database cannot keep another observation authority and file lock alive. Observed MVCC log verification never retains an observed wrapper inside its own storage authority. Failed activation checks include cached attached rows at every public step; a detached prepared database routes through ordinary schema reprepare instead of panicking.

All lifecycle cases were observed red before correction. The latest complete header/permission set passes, including the empty-file upgrade. A canonical 32 MiB blob fixture requests only 22016 main-file bytes for read-only startup and COUNT, below the 128 KiB regression bound. Broader checks and Windows execution remain required before publishing the storage repair.
