-------------------------------- MODULE SqliteTx --------------------------------
(*
   `^\textbf{\large SQLite WAL Mode Transaction Specification}^'

   This specification models SQLite's concurrency control in write-ahead log
   (WAL) mode, which provides single-writer, multiple-reader semantics with
   serializable isolation.

   The database maintains a version number called `mxFrame', which
   represents the maximum valid frame number in the WAL file. The
   `mxFrame' version number increments with each committed write
   transaction. Multiple connections may access the database simultaneously
   under the following rules:

   `^\textbf{Readers.}^' Any number of connections can read concurrently.
   When a read transaction begins, it captures a snapshot of the current
   `mxFrame' as its "end mark," giving it a consistent, frozen view of
   the database. Readers never block other readers or writers.

   `^\textbf{Writers.}^' Only one connection can write at a time, enforced
   by an exclusive write lock. On commit, new frames are appended to the
   WAL and `mxFrame' advances. On rollback, the lock is released without
   advancing `mxFrame'.

   Key guarantees: no write conflicts (single writer), readers see
   consistent data (snapshot isolation), and writers don't block readers
   nor readers block writers.

   `^\textbf{Transaction Lifecycle.}^' Connections transition through three
   states---Idle, Reading, and Writing. From Idle a connection may begin a
   read transaction (entering Reading) or begin an immediate write
   transaction (entering Writing directly). A reader may upgrade to a writer
   if no other writer exists and its snapshot is still current.

   On commit or rollback, a write transaction returns to Reading state, not
   Idle, because SQLite keeps the read lock held until explicitly released
   (`^{\tt pager\_end\_transaction}^' sets the pager state to
   `^{\tt PAGER\_READER}^'). The connection must then commit or rollback the
   read transaction to return to Idle.

   `^\textbf{References:}^'
   `^\texttt{https://www.sqlite.org/wal.html}^';
   SQLite source: `^{\tt wal.c}^', `^{\tt pager.c}^'.
*)

EXTENDS Naturals, FiniteSets

CONSTANTS
    Connections,    \* Set of database connections
    MaxVersion,     \* Upper bound on version numbers (for finite model checking)
    NoWriter        \* Sentinel value representing "no write lock held"

VARIABLES
    txState,        \* `txState'[c] \in {"Idle", "Reading", "Writing"}
    mxFrame,        \* Maximum valid WAL frame number (the committed version)
    txSnapshot,     \* `txSnapshot'[c] -- the `mxFrame' visible to connection c
    writeLock       \* `NoWriter' or the connection holding the exclusive write lock

vars == <<txState, mxFrame, txSnapshot, writeLock>>

--------------------------------------------------------------------------------
(* `^\textbf{Type Invariant.}^' Every variable stays within its expected domain. *)

TypeOK ==
    /\ txState \in [Connections -> {"Idle", "Reading", "Writing"}]
    /\ mxFrame \in 0..MaxVersion
    /\ txSnapshot \in [Connections -> 0..MaxVersion]
    /\ writeLock \in Connections \cup {NoWriter}
--------------------------------------------------------------------------------

(* `^\textbf{Single Writer Invariant.}^' At most one connection can be in Writing
   state at any time. This is the fundamental concurrency constraint of
   WAL mode, enforced by the exclusive write lock. *)
SingleWriter ==
    Cardinality({c \in Connections : txState[c] = "Writing"}) <= 1

(* `^\textbf{Write Lock Consistency Invariant.}^' A connection is in Writing state
   if and only if it holds the write lock. This ensures the write lock
   accurately reflects the system state---no "phantom" writers and no
   writers without the lock. *)
WriteLockConsistency ==
    \A c \in Connections : txState[c] = "Writing" <=> writeLock = c

(* `^\textbf{Snapshot Validity Invariant.}^' Every active (non-Idle) transaction has
   a snapshot that does not exceed the current committed `mxFrame'.
   Transactions see a consistent past, never the future. *)
SnapshotValidity ==
    \A c \in Connections : txState[c] # "Idle" => txSnapshot[c] <= mxFrame

(* `^\textbf{No Future Reads Invariant.}^' No connection---whether in a transaction
   or idle---has a snapshot that points beyond the current committed
   `mxFrame'. This is a stronger form of `SnapshotValidity' that also
   constrains idle connections. *)
NoFutureReads ==
    \A c \in Connections : txSnapshot[c] <= mxFrame

--------------------------------------------------------------------------------
(*
   `^\textbf{Initial State.}^' All connections start idle, no write lock
   is held, `mxFrame' is 0, and all snapshots point to frame 0.
*)

Init ==
    /\ txState = [c \in Connections |-> "Idle"]
    /\ mxFrame = 0
    /\ txSnapshot = [c \in Connections |-> 0]
    /\ writeLock = NoWriter

--------------------------------------------------------------------------------

(* `^\textbf{BeginRead}^' --- start a read transaction. The connection
   captures a snapshot of the current `mxFrame', giving it a frozen,
   consistent view. Any number of connections may be reading concurrently. *)
BeginRead(c) ==
    /\ txState[c] = "Idle"
    /\ txState' = [txState EXCEPT ![c] = "Reading"]
    /\ txSnapshot' = [txSnapshot EXCEPT ![c] = mxFrame]
    /\ UNCHANGED <<writeLock, mxFrame>>

(* `^\textbf{BeginWrite}^' --- start a write transaction directly from
   Idle. This models `^{\tt BEGIN IMMEDIATE}^' or `^{\tt BEGIN EXCLUSIVE}^'
   in SQLite: the connection acquires the write lock and takes a snapshot
   in one step. *)
BeginWrite(c) ==
    /\ txState[c] = "Idle"
    /\ writeLock = NoWriter
    /\ txState' = [txState EXCEPT ![c] = "Writing"]
    /\ writeLock' = c
    /\ txSnapshot' = [txSnapshot EXCEPT ![c] = mxFrame]
    /\ UNCHANGED mxFrame

(* `^\textbf{UpgradeToWrite}^' --- promote a read transaction to a write
   transaction. The connection must already be reading and the write lock
   must be free. Critically, the snapshot must still match the current
   `mxFrame'---if another writer has committed since this reader's snapshot
   was taken, the upgrade fails with `^{\tt SQLITE\_BUSY\_SNAPSHOT}^'
   (modeled by the precondition not being satisfied, so TLC simply does not
   explore that transition). See `^{\tt sqlite3WalBeginWriteTransaction}^'
   in `^{\tt wal.c}^'. *)
UpgradeToWrite(c) ==
    /\ txState[c] = "Reading"
    /\ writeLock = NoWriter
    /\ txSnapshot[c] = mxFrame
    /\ txState' = [txState EXCEPT ![c] = "Writing"]
    /\ writeLock' = c
    /\ UNCHANGED <<mxFrame, txSnapshot>>

(* `^\textbf{CommitRead}^' --- end a read transaction. The connection
   returns to Idle, releasing its snapshot. *)
CommitRead(c) ==
    /\ txState[c] = "Reading"
    /\ txState' = [txState EXCEPT ![c] = "Idle"]
    /\ UNCHANGED <<writeLock, mxFrame, txSnapshot>>

(* `^\textbf{CommitWrite}^' --- commit a write transaction. New frames are
   appended to the WAL, `mxFrame' advances, the write lock is released,
   and the connection's snapshot is updated to the new `mxFrame' so that
   the subsequent read transaction can see its own write. The connection
   returns to Reading state, not Idle, because SQLite keeps the read lock
   held until explicitly released. *)
CommitWrite(c) ==
    /\ txState[c] = "Writing"
    /\ writeLock = c
    /\ mxFrame < MaxVersion
    /\ txState' = [txState EXCEPT ![c] = "Reading"]
    /\ writeLock' = NoWriter
    /\ mxFrame' = mxFrame + 1
    /\ txSnapshot' = [txSnapshot EXCEPT ![c] = mxFrame + 1]

(* `^\textbf{RollbackRead}^' --- abort a read transaction. Identical to
   `CommitRead' in terms of state change---the connection returns to
   Idle. *)
RollbackRead(c) ==
    /\ txState[c] = "Reading"
    /\ txState' = [txState EXCEPT ![c] = "Idle"]
    /\ UNCHANGED <<writeLock, mxFrame, txSnapshot>>

(* `^\textbf{RollbackWrite}^' --- abort a write transaction. The write
   lock is released without advancing `mxFrame'. Like `CommitWrite', the
   connection returns to Reading state, keeping its read lock. *)
RollbackWrite(c) ==
    /\ txState[c] = "Writing"
    /\ writeLock = c
    /\ txState' = [txState EXCEPT ![c] = "Reading"]
    /\ writeLock' = NoWriter
    /\ UNCHANGED <<mxFrame, txSnapshot>>

--------------------------------------------------------------------------------

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
