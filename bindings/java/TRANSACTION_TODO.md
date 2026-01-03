# Java Bindings RoadMap

## 1. Current Status (Minimal Support)
We have implemented minimal transaction support suitable for basic use cases.

- [x] Basic `commit()` and `rollback()`
- [x] `setAutoCommit()` toggling
- [x] Transaction state tracking (`inTransaction` flag)
- [x] Cleanup on connection close

---

## 2. Crucial Next Steps (Future Work)

Devs picking up this work should focus on the following critical areas:

### 2.1 MVCC & Concurrency (Experimental)
**Context**: Turso supports concurrency via MVCC, which requires a specific usage pattern different from standard SQLite.
- **Reference**: `docs/manual.md` (Turso Manual)
- **Goal**: Enable `PRAGMA journal_mode = experimental_mvcc` and support `BEGIN CONCURRENT`.

**Requirements**:
1.  **Multiple Connections**: Concurrency MUST be achieved via multiple `Connection` objects (e.g., pooling). Parallel statements on a single connection are NOT supported.
2.  **Special Pragmas**:
    -   `PRAGMA journal_mode = experimental_mvcc` (on connection open).
    -   `BEGIN CONCURRENT` (instead of standard `BEGIN`).
3.  **Snapshot Isolation**:
    -   Reads are repeatable (snapshot at start time).
    -   Write-write conflicts happen at `COMMIT` time.

**Tasks**:
- [ ] Add config option to enable MVCC (e.g. `jdbc:turso:db?mvcc=true`).
- [ ] When enabled, execute `PRAGMA journal_mode = experimental_mvcc`.
- [ ] When enabled, use `BEGIN CONCURRENT` for implementations of `setAutoCommit(false)`.

### 2.2 Robust Error Handling (SQLITE_BUSY)
**Context**: With correct MVCC or even standard WAL, `SQLITE_BUSY` (Code 5) errors are expected when transactions conflict.
- **Goal**: Allow applications to gracefully handle and retry transactions.

**Tasks**:
- [ ] Enhance exception throwing to ensure `SQLITE_BUSY` is easily identifiable (e.g. specific subclass or clear error code access).
- [ ] Implement/Document retry logic strategies (Exponential Backoff).
- [ ] Ensure Java-side `inTransaction` state remains consistent even if `commit()` fails. (Currently, if commit fails, we might still think we are in a transaction).

### 2.3 Thread Safety Documentation
**Context**: The underlying implementation is NOT thread-safe.
- **Tasks**: Explicitly document class-level warnings on `TursoConnection` and `JDBC4Connection`.