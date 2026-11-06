# Finish PHP storage review

## Goal

Finish the remaining compatibility and storage review findings in upstream #11497 before resuming fork #8's independent-process concurrency protocol. Preserve mixed read-only/read-write opens, including builtin MVCC, and immutable per-connection permissions.

## Required behavior

- A writable open and connect succeed while a local legacy reader is active. The first persistent write or explicit maintenance operation acquires writer authority and may return Busy until the reader ends. Private TEMP operations do not activate persistent main storage.
- Read-only setup reads only bytes needed by normal engine initialization. It does not scan the complete main file for a possible later writer.
- Every disk-derived retained fact is validated under exclusive file authority before activation. Track successful reads with 128-bit per-range checksums, exact counts/EOF, sizes, identities and missing sidecars for the complete read-only lifetime; validation gates new observed I/O and waits for no in-flight reads without holding an I/O-blocking mutex.
- Main, WAL and logical-log observations share one authority. A header, timestamp or size alone cannot certify main pages. Repeated incompatible observations make activation ineligible; external changes return a clear close/reopen error before mutation.
- A transient bootstrap lock conflict retains observation capability. Supplied custom storage without verifiable file identity remains outside promotion; default SDK filesystem opens use the core resolver and every SDK connection receives its explicit read-only policy.
- Before mutation, failures release guards and permit retry. After possible mutation, errors poison the live authority, so subsequent public query operations fail clearly until handles close. Publish private legacy header changes to caches only after the write and sync succeed. A fresh reopen after the tested post-header I/O failure/crash must preserve existing rows.
- Empty unused writable-policy handles do not create sidecars or checkpoint on close. A legacy transaction already holding its own reader must end before promotion if safe upgrade cannot be proved; it must not restore legacy header bytes after promotion.
- The new external-aggregate scalar guard continues to reject nested/WHERE misuse while accepting valid outer-column aggregates in subqueries. Browser SQL prepare shares the existing native execution gate with sync workers.

## Verification

Minimized red/green cases cover valid/invalid external aggregates, browser native-prepare overlap, legacy active-reader open, read-to-write boundary, unused-writer close, sparse-large-file read volume, reads after bootstrap, stale page changes with unchanged header, sidecar/EOF changes, pending read completions, callback errors, and activation after abort. Inject failure and child-process exit immediately after a completed legacy header write and at its sync boundary; fresh-process readers must retain original rows and live poisoned handles must reject subsequent work. Repeat existing PHP/C compatibility, header/attachment/MVCC checks and focused Linux/Windows validation. Report actual startup I/O/time/memory and remaining platform limits.

## Scope boundaries

Keep existing #8498 stack and contributor credit per user instruction. No new multiprocess MVCC mode is claimed here. Concurrency imports remain on fork #8. These review repairs add no database daemon, SQLite replacement or unrelated language/framework work.

Observation memory policy: retain count plus an existing-dependency XXH3-128 checksum for each distinct (descriptor, offset, requested length), not a copy of all read data. Repeated equal observations deduplicate. This is a noncryptographic accidental-change detector with a 128-bit collision limit, improving on the former whole-file 64-bit fingerprint; it does not claim protection against an adversary controlling filesystem contents. Validation still compares identities, sizes, EOF and all recorded ranges under exclusion. No dependency is added.
