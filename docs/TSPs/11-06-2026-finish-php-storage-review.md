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
- [x] Install observed main+IO before first engine read; retain resolved default options across open_async yields and allow default SDK opens to use that resolver. Connect SDK with explicit readonly.
- [x] Gate SQL write transaction start before state changes, explicit checkpoints and raw WAL insert begin. Skip close checkpoint while pending. Preserve TEMP behavior and reject unsafe own legacy-read upgrade as Busy.
- [x] Prime private header bytes before freeze; validate under exclusion, mark mutation before first disk change, write+sync private header before publishing cache version; preserve WAL/log descriptor retention and MVCC replay metadata.
- [x] Execute active-reader, stale activation, read upgrade, unused writer close and permission regression checks; update error timing expectations to first mutation and document them.

### Task 5: Activation failure, crash, and delivery

**Blocked by:** Tasks1,2,3,4
**Files:** tests/integration/storage/header_version.rs; matching user storage documentation; fork review evidence
- [x] Extend real filesystem wrappers with deterministic after-header-write/next-sync error and child exit triggers, retaining file IDs and real locks. Check live handles reject work after partial activation and fresh reopen retains original rows.
- [x] Execute focused C/PHP public-API/storage coverage, actual Shuttle, lint/format and the Linux/Windows runner matrix. Capture large sparse/read-real-file I/O volume and startup time with matched baseline/candidate. Browser evidence and the unavailable full Mosaic harness are distinguished below.
- [x] Obtain independent integrated review, repair defects through red/green verification, document evidence and create bounded fork PR #15. Preserve the agreed dependency stack.
- [ ] Merge verified source into fork main to update upstream #11497; refresh that exact upstream head's CI and outstanding review requests.
- [ ] Merge verified PHP main into concurrency branch; continue issue7/PR8 full protocol implementation separately without presenting prerequisites as full concurrency.

Observation memory policy: retain count plus an existing-dependency XXH3-128 checksum for each distinct (descriptor, offset, requested length), not a copy of all read data. Repeated equal observations deduplicate. This is a noncryptographic accidental-change detector with a 128-bit collision limit, improving on the former whole-file 64-bit fingerprint; it does not claim protection against an adversary controlling filesystem contents. Validation still compares identities, sizes, EOF and all recorded ranges under exclusion. No dependency is added.

## Additional reviewed lifecycle checks

Default filesystem opens reserve the registry entry before opening a descriptor; another opener observing an in-progress open yields without creating an alias descriptor. Completed default options are released from the open state. The unobserved backend is passed to independent ATTACH/VACUUM databases so one database cannot keep another observation authority and file lock alive. Observed MVCC log verification never retains an observed wrapper inside its own storage authority. Failed activation checks include cached attached rows at every public step; a detached prepared database routes through ordinary schema reprepare instead of panicking.

All lifecycle cases were observed red before correction. The latest complete header/permission set passes, including the empty-file upgrade. A canonical 32 MiB blob fixture requests only 22016 main-file bytes for read-only startup and COUNT, below the 128 KiB regression bound. At handoff, broader checks and Windows execution still remained; the continuation evidence below records their results.


## Continuation verification on 2026-11-06

The continuation starts at `02994f36b`, preserving the upstream merge and the
original storage work. It keeps the agreed mixed read-only/read-write behavior,
builtin MVCC support and the #8498 dependency stack.

Fresh native verification exposed two obsolete assertions: the async-open test
still required supplied storage, and the C test still expected writable open to
return Busy while a legacy reader held a transaction. The replacements verify
default async storage with registry reuse, and successful C open followed by Busy
on the first write and successful retry on the same writer after the reader ends.

Independent review found that a transient validation I/O error permanently
invalidated the storage authority before any mutation. A regression reproduced
that failure. `eda0afbc4` preserves retry for read, size, descriptor identity, path
identity and missing-sidecar probe errors. Proven mismatches remain permanent,
including when changed bytes are later restored. Missing paths are confirmed by
a read-only open because the default metadata API does not preserve NotFound's
error type. Successful alias opens remain retained to preserve process locks.

Native macOS verification used Rust 1.88, four build jobs, disabled incremental
compilation and disabled dev/test debug symbols:

| Check | Result |
| --- | --- |
| Core library with `io_memory_yield` | 2,583 passed; 17 existing ignored |
| Native integration with `io_memory_yield`, no filters | 1,480 passed; 12 existing ignored |
| Strict core/C/SDK library Clippy | Passed; existing Rust 1.88 lint-expectation allowance retained |
| `cargo fmt --check` and `git diff --check` | Passed |

Before the final compact-checksum and validation-retry changes, a fresh native C
library passed all 113 compatibility cases; stock SQLite passed all 95 controls.
Main and deps dylibs had identical SHA-256 hashes, and `otool -L` confirmed each
compatibility executable linked its intended library. Linux/Windows validation
run [42232501718](https://github.com/HundredBillion/turso/actions/runs/42232501718)
passed the exact `f3655c4ac` runtime source: Linux storage 13/header 45/filesystem
1/PostgreSQL 1/sync 126/C 113; Windows storage 13/header 43/filesystem 2/PostgreSQL
1/sync 122/C 111. Each sync run had one existing ignored case. Activation failure
and crash-recovery cases executed on both platforms. Final-source expanded PHP
and Shuttle validation is tracked separately below.

Optimized comparisons use identical Rust 1.88 `lib-release` builds, unchanged
SQLite fixtures, alternating warm trials, binary hashes and live-process scan
RSS. An attempted FxHashMap replacement was rejected because it increased actual
scan time and memory despite faster isolated lookups. The retained change stores
all 128 checksum bits as bytes: each observation shrinks from 32 to 24 bytes.
This reduced the observation tree's measured allocation by 15.8% and live scan
RSS by about 0.88 MiB in the paired engine trial. The complete observation history
and checksum strength are unchanged. Read-only scans still incur a material
checksum/observation cost; the startup improvement is not a universal speedup.

The expanded PHP validation checks the public API and mixed-open lifecycle. The
previous full Mosaic application harness is not available in this checkout, so
these results must not be described as full Mosaic application acceptance.


### Handoff acceptance checklist

| Issue #14 remaining criterion | Evidence or remaining limit |
| --- | --- |
| Refresh upstream #11497 feedback and checks | Refreshed against `78a77aac1`: no newer actionable review beyond the known storage/scope findings; the existing upstream head had 207 successful checks and no failed checks. This is not CI for the later storage delta. |
| Matched optimized performance and memory comparison | Rust 1.88 `lib-release` baseline `78a77aac1`, original observation candidate and compact candidate; alternating unchanged fixtures and live RSS. Rejected the slower hash-table experiment; retained smaller checksum storage. Final-source measurements are recorded below. Full-scan overhead remains explicit. |
| Broader native integration | Final source: 1,480 passed, 12 existing ignored, no command-line exclusions. The historical Linux io_uring exclusion was not carried into the macOS invocation. |
| Full core library, strict lint, formatting | Final source: 2,583 passed, 17 existing ignored; strict core/C/SDK library Clippy, formatting and diff checks pass. |
| Fresh C library, SQLite controls, PHP/Mosaic/mixed-open/file preservation | Native fresh C/SQLite controls passed; exact final source runs in the linked platform validation. Expanded PHP probes cover legacy/WAL/MVCC, both close orders, snapshots, immutable read-only permissions, TEMP and file preservation. Full Mosaic application harness is unavailable and is not claimed. |
| PostgreSQL, sync/SDK, actual Shuttle | Native integration covers the Rust builder permission cases. Platform validation covers PostgreSQL and sync; final validation also uses actual `--cfg shuttle`, rather than filtering ordinary tests by name. |
| Exact PHP source on Windows | Linked final validation uses the candidate source plus isolated workflow/probe files; those files are not part of this repair branch. Failure/crash cases run on Windows, with Unix-only aliases excluded by their existing platform guards. |
| Independent review, fixes, documentation and bounded fork delivery | Parent review plus independent concurrency-agent review. The transient validation retry finding was reproduced and repaired, then re-reviewed without a source blocker. Fork [PR #15](https://github.com/HundredBillion/turso/pull/15) carries the bounded storage repair; merge and final CI status remain the parent task's responsibility. The agreed #8498 stack and contributor history remain preserved. |
| Continue concurrency only after verified PHP integration | No concurrency implementation was imported into this PHP branch. The separately reviewed bounded log reader is only one component of the wider independent-process MVCC protocol; it does not close issue #7. |

The published aggregate and browser prepare repairs remain intact. Earlier
browser execution and the existing upstream checks are historical evidence;
this continuation does not claim a fresh local browser suite or full Mosaic
application run for the storage delta.

### Final source performance

The final `eda0afbc4` runtime was rebuilt with
`cargo build --locked --profile lib-release --bin tursodb`, using the same Rust
1.88 toolchain, profile and features as baseline `78a77aac1`. No agent Cargo,
stress or test processes ran during measurement. Each median uses eight warm
trials after one warmup, alternating binary order. Main-file hashes were unchanged
after both experiments.

| Workload | Main baseline | Final storage repair |
| --- | ---: | ---: |
| Open + COUNT, 200,000 rows with 1,000-byte text payloads | 138.09 ms | 50.43 ms |
| Already-open scan, `SELECT sum(length(payload)) FROM t` | 49.28 ms | 57.33 ms |
| Live resident memory after that scan | 26.66 MiB | 31.03 MiB |
| Open + COUNT, one 32 MiB blob | 23.49 ms | 7.51 ms |

The scan remains **16.3% slower** and uses **4.375 MiB more live memory** than
main. Compared with the preserved original observation implementation, compact
checksums save **0.922 MiB** in this paired run; its scan timing changes only
slightly (57.63 to 57.33 ms). The memory saving is the reliable benefit of that
small representation change. Observations still scale with distinct read ranges.
The required hashing and bookkeeping remain a material cost for repeated large
read-only scans, even though the measured startup workloads improve substantially.
These local results do not establish a universal performance improvement.

The scan timer starts after opening and COUNT, so faster startup cannot conceal
slower scans. Scan memory is sampled from the live CLI using `ps` after the result;
startup peak memory comes from `/usr/bin/time -l`. The retained diagnostic
microbenchmark measured about 4.4–4.8 ms hashing 50,127 pages and 1.8–2.0 ms looking
up the corresponding tree entries; these isolated timings are attribution aids,
not replacements for the engine comparison.

Artifact SHA-256 values:

- Baseline CLI: `45ecc36746e0225b98ee7fb38a994acceb7989ea4d7953770cc2d3b37e0ee905`
- Final CLI: `7f178cb517da8f7171f0a2ba8d8ba45007108ddb36f7dc4f40389aa70c0cfe5f`
- 205,320,192-byte scan fixture: `cf6f5970bf4c18f528f7763d778e4418f4e63fadffc4985a83af33f4b38f8afa`
- 33,595,392-byte blob fixture: `22d717a4321c0daeefe73d090759a7076323f775e5c412af38dcf1c3ed3d2e18`

Detailed commands, trial samples, binaries and logs are retained locally under
`.local/php-verification/`, including `benchmark-final.py`,
`optimized-final-performance.json`, `benchmark-startup.py`,
`optimized-blob-startup.json`, `core-final.log`, `integration-final.log`,
`clippy-final.log` and the red/green validation-retry logs.


### Final platform and review evidence

[Validation run 42237110840](https://github.com/HundredBillion/turso/actions/runs/42237110840)
passed on **both Linux and Windows** against final runtime `eda0afbc4`. The
validation branch `dd487a40e` differs only in its temporary workflow and PHP probe;
those files are excluded from the storage repair branch.

| Final check | Linux | Windows |
| --- | ---: | ---: |
| Read-only storage module | 14 passed | 14 passed |
| Header/activation/failure cases | 45 passed | 43 passed |
| Fresh C compatibility library | 113 passed | 111 passed |
| Sync | 126 passed, 1 ignored | 122 passed, 1 ignored |
| PostgreSQL focused regression | 1 passed | 1 passed |
| Actual Shuttle (`--cfg shuttle`) | 71 passed, none ignored | Not selected |

Linux PHP verification passed six Turso mode/close-order variants and four stock
SQLite controls. It checks legacy, WAL and builtin MVCC databases, reader-first
and writer-first close, active legacy reader/first-write Busy/retry, MVCC snapshot
preservation, immutable read-only permissions, TEMP, file preservation and reopen.
The probe verifies the loaded Turso library identity and excludes accidentally
loading stock SQLite for the Turso cases. This supplements the fresh C suite; it
is not a substitute claim for the unavailable full Mosaic application harness.

The parent reviewed the integrated runtime delta, and the concurrency agent
independently reviewed observation admission, validation/exclusion, descriptor
retention, poisoning, mutation gates and SDK policy. The transient validation
retry finding was reproduced red, fixed, and independently re-reviewed with no
remaining source blocker. The final native suites and the exact platform run
cover that repair. Fork PR #15 is ready for review with the measured scan/memory
tradeoff and application/browser coverage limits stated explicitly; publication
into fork main and upstream #11497 follow in the parent task.
