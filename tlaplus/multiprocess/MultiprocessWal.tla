--------------------------- MODULE MultiprocessWal ---------------------------
(*
   `^\textbf{\large Turso Multiprocess WAL Coordination Specification}^'

   This specification models how Turso coordinates readers, the writer, and
   checkpoints when several processes open the same database in WAL mode.
   The coordination code is `^{\tt ShmWalCoordination}^' in
   `^{\tt core/storage/wal.rs}^' and `^{\tt MappedSharedWalCoordination}^'
   in `^{\tt core/storage/shared\_wal\_coordination.rs}^'.

   `^\textbf{Two layers of locks.}^' SQLite keeps all WAL locks in the
   `^{\tt -shm}^' file, so every process sees every lock. Turso keeps the
   read locks inside each process. Read lock 0 and the read marks are
   visible only to connections in the same process. Other processes learn
   about a reader only through the shared reader table in the
   `^{\tt -tshm}^' file. The writer lock and the checkpoint lock are shared
   by all processes.

   `^\textbf{Database file readers.}^' A read transaction that begins when
   the WAL is fully backfilled (`mxFrame' = `nBackfill') reads every page
   from the database file and holds read lock 0. It registers in the shared
   reader table without a frame number. While one is registered, a
   checkpoint in any process copies nothing past the current `nBackfill'.
   These readers do not stop a WAL restart, because they never read the WAL.

   `^\textbf{WAL readers.}^' A read transaction that begins when the WAL
   has frames that are not backfilled reads frames up to its snapshot's
   `mxFrame' from the WAL and the other pages from the database file. It
   holds a read mark in its process and registers its `mxFrame' in the
   shared reader table. Checkpoints in every process stop at that frame,
   and the WAL cannot restart until it ends.

   `^\textbf{Versions.}^' Every commit appends one frame and creates one new
   database version. `walBase' is the version the database file had when the
   current WAL generation started, so frame $i$ of the current generation
   holds version `walBase' $+\,i$. The database file holds version
   `walBase' $+$ `dbFrames'.

   `^\textbf{What is not modeled.}^' Process crashes and reclaiming slots of
   dead processes, running out of reader slots, the shared frame index and
   its overflow, RESTART and TRUNCATE checkpoints, and rollbacks.
*)

EXTENDS Naturals, FiniteSets

CONSTANTS
    Processes,                   \* Set of processes that open the database
    ConnsPerProcess,             \* Number of connections in each process
    MaxVersion,                  \* Upper bound on the number of commits
    NoConn                       \* Sentinel for "no connection"

Conns == Processes \X (1..ConnsPerProcess)

Proc(c) == c[1]

VARIABLES
    mode,       \* `mode'[c]: where connection c is in its transaction
    snap,       \* `snap'[c]: the WAL snapshot connection c loaded
    reg,        \* `reg'[c]: connection c's entry in the shared reader table
    walGen,     \* WAL generation, increased by every restart
    walBase,    \* Database version when the current WAL generation started
    mxFrame,    \* Last committed frame in the current WAL generation
    nBackfill,  \* Frames of the current generation published as backfilled
    dbFrames,   \* Frames of the current generation copied to the database file
    writer,     \* Connection holding the shared writer lock, or `NoConn'
    ckpt        \* The running checkpoint: its connection and safe frame

vars == <<mode, snap, reg, walGen, walBase, mxFrame, nBackfill, dbFrames,
          writer, ckpt>>

Modes == {"Idle", "Loaded", "Locked", "Registered", "Reading", "Writing",
          "Checkpointing"}

Snapshots == [gen : 0..MaxVersion, mx : 0..MaxVersion, nb : 0..MaxVersion,
              ver : 0..MaxVersion]

Registrations == [kind : {"none", "dbfile", "wal"}, frame : 0..MaxVersion]

NoRegistration == [kind |-> "none", frame |-> 0]

NoCheckpoint == [owner |-> NoConn, safe |-> 0]

Min(S) == CHOOSE x \in S : \A y \in S : x <= y

DbVersion == walBase + dbFrames

--------------------------------------------------------------------------------
(* `^\textbf{Local locks.}^' A connection holds a local read lock from the
   moment it takes one until its transaction ends. Which lock it holds
   depends on the snapshot it loaded. *)

HoldsLocalLock(c) == mode[c] \in {"Locked", "Registered", "Reading", "Writing"}

ReadsDatabaseFile(c) == snap[c].mx = snap[c].nb

HoldsReadLock0(c) == HoldsLocalLock(c) /\ ReadsDatabaseFile(c)

HoldsReadMark(c) == HoldsLocalLock(c) /\ ~ReadsDatabaseFile(c)

LocalCheckpointRunning(p) == ckpt.owner # NoConn /\ Proc(ckpt.owner) = p

(* `^\textbf{Shared reader table.}^' Registrations of connections in one
   process share a slot, which does not change what other processes see. *)

SharedWalReaderFrames == {reg[c].frame : c \in {d \in Conns : reg[d].kind = "wal"}}

HasDatabaseFileReader == \E c \in Conns : reg[c].kind = "dbfile"

--------------------------------------------------------------------------------
(* `^\textbf{Type Invariant.}^' *)

TypeOK ==
    /\ mode \in [Conns -> Modes]
    /\ snap \in [Conns -> Snapshots]
    /\ reg \in [Conns -> Registrations]
    /\ walGen \in 0..MaxVersion
    /\ walBase \in 0..MaxVersion
    /\ mxFrame \in 0..MaxVersion
    /\ nBackfill \in 0..mxFrame
    /\ dbFrames \in nBackfill..mxFrame
    /\ writer \in Conns \cup {NoConn}
    /\ ckpt.owner \in Conns \cup {NoConn}
    /\ ckpt.safe \in 0..MaxVersion

--------------------------------------------------------------------------------
(* `^\textbf{Database File Not Ahead Of Readers.}^' The database file never
   holds a version newer than the snapshot of an active transaction.
   Otherwise a reader that reads a page from the database file would see a
   page from a later commit next to pages from its own snapshot. *)
DatabaseFileNotAheadOfReaders ==
    \A c \in Conns :
        mode[c] \in {"Reading", "Writing"} => DbVersion <= snap[c].ver

(* `^\textbf{Database File Not Behind Readers.}^' The database file holds
   every frame that a transaction expects to find there instead of in the
   WAL. *)
DatabaseFileNotBehindReaders ==
    \A c \in Conns :
        mode[c] \in {"Reading", "Writing"} =>
            DbVersion >= snap[c].ver - snap[c].mx + snap[c].nb

(* `^\textbf{WAL Not Restarted Under WAL Readers.}^' A restart makes new
   commits overwrite the WAL from frame 1, so it must not happen while a
   transaction still reads frames of the old generation. *)
WalNotRestartedUnderWalReaders ==
    \A c \in Conns :
        mode[c] \in {"Reading", "Writing"} /\ ~ReadsDatabaseFile(c) =>
            snap[c].gen = walGen

(* `^\textbf{Single Writer.}^' Only the connection holding the writer lock
   writes. *)
SingleWriter ==
    \A c \in Conns : mode[c] = "Writing" <=> writer = c

(* `^\textbf{Registrations Match Transactions.}^' A connection has an entry
   in the shared reader table only while it is in a read transaction. *)
RegistrationsMatchTransactions ==
    \A c \in Conns :
        mode[c] \in {"Idle", "Loaded", "Locked", "Checkpointing"} =>
            reg[c] = NoRegistration

--------------------------------------------------------------------------------
(* `^\textbf{Restart Conditions.}^' A writer restarts the WAL before
   writing when its read transaction reads the database file and every
   frame is backfilled. In its own process it needs read lock 0 for itself
   and no read mark in use. In the shared reader table it needs no WAL
   reader. Database file readers, including the writer itself, do not block
   the restart. *)
CanRestart(c) ==
    /\ mode[c] = "Writing"
    /\ ReadsDatabaseFile(c)
    /\ nBackfill > 0
    /\ mxFrame = nBackfill
    /\ \A d \in Conns \ {c} :
           Proc(d) = Proc(c) => ~HoldsReadLock0(d) /\ ~HoldsReadMark(d)
    /\ \A d \in Conns : reg[d].kind # "wal"

(* `^\textbf{Writer Can Restart When Alone.}^' A writer that is the only
   connection in a transaction can always restart a fully backfilled WAL.
   If its own entry in the shared reader table blocked the restart, the WAL
   would never restart and would grow without bound. *)
WriterCanRestartWhenAlone ==
    \A c \in Conns :
        /\ mode[c] = "Writing"
        /\ ReadsDatabaseFile(c)
        /\ nBackfill > 0
        /\ mxFrame = nBackfill
        /\ \A d \in Conns \ {c} : mode[d] = "Idle"
        => CanRestart(c)

--------------------------------------------------------------------------------
(* `^\textbf{Checkpoint Safe Frame.}^' The last frame a checkpoint may copy.
   It stops at the read marks in its own process, at every WAL reader in the
   shared reader table, and at the current `nBackfill' while any database
   file reader is registered. *)
SafeFrame(c) ==
    Min({mxFrame}
        \cup {snap[d].mx : d \in {e \in Conns : Proc(e) = Proc(c) /\ HoldsReadMark(e)}}
        \cup SharedWalReaderFrames
        \cup (IF HasDatabaseFileReader THEN {nBackfill} ELSE {}))

--------------------------------------------------------------------------------
(* `^\textbf{Initial State.}^' An empty WAL and no transactions. *)

Init ==
    /\ mode = [c \in Conns |-> "Idle"]
    /\ snap = [c \in Conns |-> [gen |-> 0, mx |-> 0, nb |-> 0, ver |-> 0]]
    /\ reg = [c \in Conns |-> NoRegistration]
    /\ walGen = 0
    /\ walBase = 0
    /\ mxFrame = 0
    /\ nBackfill = 0
    /\ dbFrames = 0
    /\ writer = NoConn
    /\ ckpt = NoCheckpoint

--------------------------------------------------------------------------------
(* `^\textbf{Beginning a read transaction}^' takes four steps, and other
   connections may act between any two of them. *)

(* `^\textbf{LoadSnapshot}^' --- read the shared WAL snapshot. *)
LoadSnapshot(c) ==
    /\ mode[c] = "Idle"
    /\ mode' = [mode EXCEPT ![c] = "Loaded"]
    /\ snap' = [snap EXCEPT ![c] = [gen |-> walGen, mx |-> mxFrame,
                                   nb |-> nBackfill, ver |-> walBase + mxFrame]]
    /\ UNCHANGED <<reg, walGen, walBase, mxFrame, nBackfill, dbFrames, writer, ckpt>>

(* `^\textbf{TakeLocalLock}^' --- take read lock 0 for a database file
   reader, or a read mark for a WAL reader. Read lock 0 is not available
   while a checkpoint in the same process holds it exclusively. *)
TakeLocalLock(c) ==
    /\ mode[c] = "Loaded"
    /\ ReadsDatabaseFile(c) => ~LocalCheckpointRunning(Proc(c))
    /\ mode' = [mode EXCEPT ![c] = "Locked"]
    /\ UNCHANGED <<snap, reg, walGen, walBase, mxFrame, nBackfill, dbFrames, writer, ckpt>>

(* `^\textbf{Register}^' --- add the reader to the shared reader table. *)
Register(c) ==
    /\ mode[c] = "Locked"
    /\ mode' = [mode EXCEPT ![c] = "Registered"]
    /\ reg' = [reg EXCEPT ![c] =
                 IF ReadsDatabaseFile(c) THEN [kind |-> "dbfile", frame |-> 0]
                 ELSE [kind |-> "wal", frame |-> snap[c].mx]]
    /\ UNCHANGED <<snap, walGen, walBase, mxFrame, nBackfill, dbFrames, writer, ckpt>>

(* `^\textbf{Validate}^' --- check that the shared snapshot has not changed
   since it was loaded. A commit, a checkpoint, or a restart in between
   could have happened before the registration was visible, so the
   connection gives up its lock and registration and starts over. *)
Validate(c) ==
    /\ mode[c] = "Registered"
    /\ IF snap[c].gen = walGen /\ snap[c].mx = mxFrame /\ snap[c].nb = nBackfill
       THEN /\ mode' = [mode EXCEPT ![c] = "Reading"]
            /\ UNCHANGED reg
       ELSE /\ mode' = [mode EXCEPT ![c] = "Idle"]
            /\ reg' = [reg EXCEPT ![c] = NoRegistration]
    /\ UNCHANGED <<snap, walGen, walBase, mxFrame, nBackfill, dbFrames, writer, ckpt>>

(* `^\textbf{EndRead}^' --- end the read transaction. *)
EndRead(c) ==
    /\ mode[c] = "Reading"
    /\ mode' = [mode EXCEPT ![c] = "Idle"]
    /\ reg' = [reg EXCEPT ![c] = NoRegistration]
    /\ UNCHANGED <<snap, walGen, walBase, mxFrame, nBackfill, dbFrames, writer, ckpt>>

--------------------------------------------------------------------------------

(* `^\textbf{BeginWrite}^' --- upgrade a read transaction to a write
   transaction. The snapshot must still be the latest one. *)
BeginWrite(c) ==
    /\ mode[c] = "Reading"
    /\ writer = NoConn
    /\ snap[c].gen = walGen
    /\ snap[c].mx = mxFrame
    /\ mode' = [mode EXCEPT ![c] = "Writing"]
    /\ writer' = c
    /\ UNCHANGED <<snap, reg, walGen, walBase, mxFrame, nBackfill, dbFrames, ckpt>>

(* `^\textbf{RestartWal}^' --- start a new WAL generation before writing.
   The database file already holds every frame, so the new generation
   starts from the database file's version. The writer's snapshot moves to
   the new generation. *)
RestartWal(c) ==
    /\ CanRestart(c)
    /\ walGen' = walGen + 1
    /\ walBase' = walBase + mxFrame
    /\ mxFrame' = 0
    /\ nBackfill' = 0
    /\ dbFrames' = 0
    /\ snap' = [snap EXCEPT ![c] = [gen |-> walGen + 1, mx |-> 0, nb |-> 0,
                                   ver |-> snap[c].ver]]
    /\ UNCHANGED <<mode, reg, writer, ckpt>>

(* `^\textbf{Commit}^' --- append one frame and end the transaction. *)
Commit(c) ==
    /\ mode[c] = "Writing"
    /\ walBase + mxFrame < MaxVersion
    /\ mxFrame' = mxFrame + 1
    /\ mode' = [mode EXCEPT ![c] = "Idle"]
    /\ reg' = [reg EXCEPT ![c] = NoRegistration]
    /\ writer' = NoConn
    /\ UNCHANGED <<snap, walGen, walBase, nBackfill, dbFrames, ckpt>>

--------------------------------------------------------------------------------
(* `^\textbf{Checkpoint.}^' A PASSIVE checkpoint takes the shared checkpoint
   lock and read lock 0 in its own process, computes the safe frame once,
   copies frames one at a time, and publishes the new `nBackfill' at the
   end. Readers may register while it copies. *)

CheckpointBegin(c) ==
    /\ mode[c] = "Idle"
    /\ ckpt.owner = NoConn
    /\ mxFrame > nBackfill
    /\ \A d \in Conns : Proc(d) = Proc(c) => ~HoldsReadLock0(d)
    /\ mode' = [mode EXCEPT ![c] = "Checkpointing"]
    /\ ckpt' = [owner |-> c, safe |-> SafeFrame(c)]
    /\ UNCHANGED <<snap, reg, walGen, walBase, mxFrame, nBackfill, dbFrames, writer>>

CheckpointCopyFrame ==
    /\ ckpt.owner # NoConn
    /\ dbFrames < ckpt.safe
    /\ dbFrames' = dbFrames + 1
    /\ UNCHANGED <<mode, snap, reg, walGen, walBase, mxFrame, nBackfill, writer, ckpt>>

CheckpointEnd ==
    /\ ckpt.owner # NoConn
    /\ dbFrames >= ckpt.safe
    /\ nBackfill' = dbFrames
    /\ mode' = [mode EXCEPT ![ckpt.owner] = "Idle"]
    /\ ckpt' = NoCheckpoint
    /\ UNCHANGED <<snap, reg, walGen, walBase, mxFrame, dbFrames, writer>>

--------------------------------------------------------------------------------

Next ==
    \/ \E c \in Conns :
        \/ LoadSnapshot(c)
        \/ TakeLocalLock(c)
        \/ Register(c)
        \/ Validate(c)
        \/ EndRead(c)
        \/ BeginWrite(c)
        \/ RestartWal(c)
        \/ Commit(c)
        \/ CheckpointBegin(c)
    \/ CheckpointCopyFrame
    \/ CheckpointEnd

Spec == Init /\ [][Next]_vars

================================================================================
