-------------------------------- MODULE SqliteTx --------------------------------
\*
\* TLA+ Specification for SQLite WAL Mode Transactions
\*
\* This specification models SQLite's concurrency control in WAL (Write-Ahead
\* Logging) mode, which implements single-writer, multiple-reader semantics.
\*
\* TRANSACTION MODEL
\*
\* The database maintains a version number (mxFrame in SQLite - the maximum
\* valid frame number in the WAL file) that increments with each committed
\* write. Multiple connections can access the database simultaneously:
\*
\* Readers:
\*   - Any number of connections can read concurrently
\*   - When a read begins, it captures a snapshot of the current mxFrame
\*     as its "end mark", giving it a consistent frozen view of the database
\*   - Readers never block other readers or writers
\*
\* Writers:
\*   - Only one connection can write at a time (enforced by write lock)
\*   - To write, a connection must acquire the exclusive write lock
\*   - On commit, new frames are appended to WAL and mxFrame advances
\*   - On rollback, the lock is released without advancing mxFrame
\*
\* Key guarantees:
\*   - No write conflicts (single writer)
\*   - Readers see consistent data (snapshot isolation)
\*   - Writers don't block readers, readers don't block writers
\*
EXTENDS Naturals, FiniteSets

CONSTANTS
    Connections,    \* Set of database connections
    MaxVersion,     \* Bounds version numbers
    NoWriter        \* Model value for "no write lock held"

VARIABLES
    txState,        \* Connections -> {Idle, Reading, Writing}
    writeLock,      \* NoWriter | connection holding write lock
    dbVersion,      \* Current committed database version
    txSnapshot      \* Connections -> version snapshot (what tx sees)

vars == <<txState, writeLock, dbVersion, txSnapshot>>

--------------------------------------------------------------------------------
\* Type invariant

TypeOK ==
    /\ txState \in [Connections -> {"Idle", "Reading", "Writing"}]
    /\ writeLock \in Connections \cup {NoWriter}
    /\ dbVersion \in 0..MaxVersion
    /\ txSnapshot \in [Connections -> 0..MaxVersion]

--------------------------------------------------------------------------------
\* Safety: Single Writer
\*
\* At most one connection can be in Writing state.

SingleWriter ==
    Cardinality({c \in Connections : txState[c] = "Writing"}) <= 1

--------------------------------------------------------------------------------
\* Safety: Write Lock Consistency
\*
\* A connection is in Writing state if and only if it holds the write lock.

WriteLockConsistency ==
    \A c \in Connections : txState[c] = "Writing" <=> writeLock = c

--------------------------------------------------------------------------------
\* Safety: Snapshot Validity
\*
\* Active transactions have snapshots that don't exceed the current version.

SnapshotValidity ==
    \A c \in Connections : txState[c] # "Idle" => txSnapshot[c] <= dbVersion

--------------------------------------------------------------------------------
\* Safety: No Future Reads
\*
\* Transactions can't see uncommitted versions.

NoFutureReads ==
    \A c \in Connections : txSnapshot[c] <= dbVersion

--------------------------------------------------------------------------------
\* Initial state

Init ==
    /\ txState = [c \in Connections |-> "Idle"]
    /\ writeLock = NoWriter
    /\ dbVersion = 0
    /\ txSnapshot = [c \in Connections |-> 0]

--------------------------------------------------------------------------------
\* Actions

\* Begin a read transaction - takes snapshot of current version
BeginRead(c) ==
    /\ txState[c] = "Idle"
    /\ txState' = [txState EXCEPT ![c] = "Reading"]
    /\ txSnapshot' = [txSnapshot EXCEPT ![c] = dbVersion]
    /\ UNCHANGED <<writeLock, dbVersion>>

\* Begin a write transaction directly - must acquire write lock
\* This models BEGIN IMMEDIATE / BEGIN EXCLUSIVE in SQLite
BeginWrite(c) ==
    /\ txState[c] = "Idle"
    /\ writeLock = NoWriter
    /\ txState' = [txState EXCEPT ![c] = "Writing"]
    /\ writeLock' = c
    /\ txSnapshot' = [txSnapshot EXCEPT ![c] = dbVersion]
    /\ UNCHANGED dbVersion

\* Upgrade from read to write transaction
\* This models the Turso implementation where begin_write_tx requires
\* an existing read transaction (wal.rs:1106-1108)
\* Precondition: snapshot must still be valid (not stale)
UpgradeToWrite(c) ==
    /\ txState[c] = "Reading"
    /\ writeLock = NoWriter
    /\ txSnapshot[c] = dbVersion  \* Snapshot must be current (wal.rs:1112-1123)
    /\ txState' = [txState EXCEPT ![c] = "Writing"]
    /\ writeLock' = c
    /\ UNCHANGED <<dbVersion, txSnapshot>>

\* Commit a read transaction
CommitRead(c) ==
    /\ txState[c] = "Reading"
    /\ txState' = [txState EXCEPT ![c] = "Idle"]
    /\ UNCHANGED <<writeLock, dbVersion, txSnapshot>>

\* Commit a write transaction - increments version, releases write lock
\* In Turso implementation, this returns to Reading state (read lock still held)
\* The connection must later call CommitRead/EndReadTx to release read lock
CommitWrite(c) ==
    /\ txState[c] = "Writing"
    /\ writeLock = c
    /\ dbVersion < MaxVersion
    /\ txState' = [txState EXCEPT ![c] = "Reading"]  \* Returns to Reading, not Idle
    /\ writeLock' = NoWriter
    /\ dbVersion' = dbVersion + 1
    /\ txSnapshot' = [txSnapshot EXCEPT ![c] = dbVersion + 1]  \* Update snapshot to new version

\* Rollback a read transaction - releases read lock, goes to Idle
RollbackRead(c) ==
    /\ txState[c] = "Reading"
    /\ txState' = [txState EXCEPT ![c] = "Idle"]
    /\ UNCHANGED <<writeLock, dbVersion, txSnapshot>>

\* Rollback a write transaction - releases write lock, keeps read lock
\* In Turso implementation, this returns to Reading state (read lock still held)
RollbackWrite(c) ==
    /\ txState[c] = "Writing"
    /\ writeLock = c
    /\ txState' = [txState EXCEPT ![c] = "Reading"]  \* Returns to Reading, not Idle
    /\ writeLock' = NoWriter
    /\ UNCHANGED <<dbVersion, txSnapshot>>

--------------------------------------------------------------------------------
\* Next state

Next ==
    \E c \in Connections :
        \/ BeginRead(c)
        \/ BeginWrite(c)
        \/ UpgradeToWrite(c)
        \/ CommitRead(c)
        \/ CommitWrite(c)
        \/ RollbackRead(c)
        \/ RollbackWrite(c)

Spec == Init /\ [][Next]_vars

================================================================================
