// Configurations of MvccRecovery.qnt. Each module sets the constants of one
// configuration. The comment above a module gives the result to expect and
// the invariants to check. The Makefile reads these comments.

// Expect no error: the code today with the fix of PR #9770 off. The first
// process runs with the passive flag off. The user can open with the flag
// on or off, read-write or read-only. A checkpoint can fail at each modeled
// step from the pager commit to the WAL truncate. OpenSucceeds and NoLeak
// fail in the code today: see issue7480, issue9749, leadA, leadB, idea2 and
// idea2Passive.
// Invariants: TypeOK RecoveryCorrect
module main {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 1,
    MaxFails = 1,
    InitFlag = false,
    OpenFlags = Set(false, true),
    OpenReadOnly = Set(false, true),
    CheckpointKinds = Set("blocking", "passive", "truncflag"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = true,
    Fix9770 = false,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = false,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect no error: as main, but the first process runs with the passive flag on.
// Invariants: TypeOK RecoveryCorrect
module mainPassive {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 1,
    MaxFails = 1,
    InitFlag = true,
    OpenFlags = Set(false, true),
    OpenReadOnly = Set(false, true),
    CheckpointKinds = Set("blocking", "passive", "truncflag"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = true,
    Fix9770 = false,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = false,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect no error: PR #9770 and FixStampBelowBoundary are on. Read-only
// opens are left out (leadA). The torn header refusal is stated (leadB).
// Invariants: TypeOK RecoveryCorrect OpenSucceeds NoLeak
module fixed {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 1,
    MaxFails = 1,
    InitFlag = false,
    OpenFlags = Set(false, true),
    OpenReadOnly = Set(false),
    CheckpointKinds = Set("blocking", "passive", "truncflag"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = true,
    Fix9770 = true,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = true,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect no error: as fixed, but the first process runs with the passive flag on.
// Invariants: TypeOK RecoveryCorrect OpenSucceeds NoLeak
module fixedPassive {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 1,
    MaxFails = 1,
    InitFlag = true,
    OpenFlags = Set(false, true),
    OpenReadOnly = Set(false),
    CheckpointKinds = Set("blocking", "passive", "truncflag"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = true,
    Fix9770 = true,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = true,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect no error: two crashes, so a crash can stop recovery itself and a
// second crash can follow a successful open. The passive flag is on at the
// start, PR #9770 and FixStampBelowBoundary are on, MaxTs is 2.
// Invariants: TypeOK RecoveryCorrect OpenSucceeds
module twoCrashes {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 2,
    MaxCheckpoints = 2,
    MaxCrashes = 2,
    MaxFails = 0,
    InitFlag = true,
    OpenFlags = Set(false, true),
    OpenReadOnly = Set(false),
    CheckpointKinds = Set("passive"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = true,
    Fix9770 = true,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = true,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect OpenSucceeds to fail: issue #7480. A blocking checkpoint truncates
// the logical log, then a crash comes before the WAL truncate. The open
// refuses with "WAL has committed frames but logical log header is missing".
// Invariants: OpenSucceeds
module issue7480 {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 1,
    MaxFails = 0,
    InitFlag = false,
    OpenFlags = Set(false),
    OpenReadOnly = Set(false),
    CheckpointKinds = Set("blocking"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = true,
    Fix9770 = false,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = false,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect no error: issue7480 with the fix of PR #9770.
// Invariants: TypeOK RecoveryCorrect OpenSucceeds
module issue7480Fixed {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 1,
    MaxFails = 0,
    InitFlag = false,
    OpenFlags = Set(false),
    OpenReadOnly = Set(false),
    CheckpointKinds = Set("blocking"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = true,
    Fix9770 = true,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = false,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect OpenSucceeds to fail: issue #9749. A passive checkpoint leaves
// WAL frames and an empty log. An open with the flag off refuses.
// Invariants: OpenSucceeds
module issue9749 {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 1,
    MaxFails = 0,
    InitFlag = true,
    OpenFlags = Set(false),
    OpenReadOnly = Set(false),
    CheckpointKinds = Set("passive"),
    CrashOnlyWhenIdle = true,
    TornHeaderGarbage = true,
    Fix9770 = false,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = false,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect no error: issue9749 with the fix of PR #9770.
// Invariants: TypeOK RecoveryCorrect OpenSucceeds
module issue9749Fixed {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 1,
    MaxFails = 0,
    InitFlag = true,
    OpenFlags = Set(false),
    OpenReadOnly = Set(false),
    CheckpointKinds = Set("passive"),
    CrashOnlyWhenIdle = true,
    TornHeaderGarbage = true,
    Fix9770 = true,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = false,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect OpenSucceeds to fail: lead A. A passive database that closes
// cleanly keeps its WAL frames, because close does not checkpoint in MVCC.
// A read-only open refuses with "Cannot reconcile interrupted MVCC
// checkpoint in read-only mode". The fix of PR #9770 is on.
// Invariants: OpenSucceeds
module leadA {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 1,
    MaxFails = 0,
    InitFlag = true,
    OpenFlags = Set(true),
    OpenReadOnly = Set(true),
    CheckpointKinds = Set("passive"),
    CrashOnlyWhenIdle = true,
    TornHeaderGarbage = true,
    Fix9770 = true,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = false,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect OpenSucceedsWithWal to fail: lead B. A passive checkpoint truncates
// the log while the WAL keeps its frames. The next commit writes the log
// header with its frame, and a crash tears the header before the fsync.
// Every open refuses with "WAL has committed frames but logical log header
// is invalid", but the WAL holds every committed row. This needs a header
// write that tears into bytes that do not decode (not verified).
// Invariants: OpenSucceedsWithWal
module leadB {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 1,
    MaxFails = 0,
    InitFlag = true,
    OpenFlags = Set(false, true),
    OpenReadOnly = Set(false),
    CheckpointKinds = Set("passive"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = true,
    Fix9770 = true,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = false,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect no error: lead B with the refusals that RECOVERY_SEMANTICS.md
// states as intended (case 3 and case 5). The lead B refusal is case 3.
// Invariants: TypeOK RecoveryCorrect OpenSucceeds
module leadBStated {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 1,
    MaxFails = 0,
    InitFlag = true,
    OpenFlags = Set(false, true),
    OpenReadOnly = Set(false),
    CheckpointKinds = Set("passive"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = true,
    Fix9770 = true,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = false,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect no error: lead B when a torn header reads as zeros or a short
// file, with the flag on. The open sees NoLog and completes the checkpoint.
// Invariants: TypeOK RecoveryCorrect OpenSucceedsWithWal
module leadBZeros {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 1,
    MaxFails = 0,
    InitFlag = true,
    OpenFlags = Set(true),
    OpenReadOnly = Set(false),
    CheckpointKinds = Set("passive"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = false,
    Fix9770 = true,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = false,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect RecoveryCorrect to fail: idea 1. Replay gives every version one
// timestamp. A row inserted in one frame and deleted in the next comes
// back: the delete finds no version that began before it, so it adds a
// tombstone and leaves the insert current.
// Invariants: RecoveryCorrect
module idea1 {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 1,
    MaxFails = 1,
    InitFlag = false,
    OpenFlags = Set(false, true),
    OpenReadOnly = Set(false),
    CheckpointKinds = Set("blocking", "passive", "truncflag"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = true,
    Fix9770 = true,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = true,
    FixStampBelowBoundary = false,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect NoLeak to fail: idea 2. A blocking checkpoint fails after its
// publish, so its GC does not run. The next checkpoint collects nothing
// for those rows and does not stamp them, so GC keeps them while the key
// gets no new write.
// Invariants: NoLeak
module idea2 {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 0,
    MaxFails = 1,
    InitFlag = false,
    OpenFlags = Set(false),
    OpenReadOnly = Set(false),
    CheckpointKinds = Set("blocking"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = true,
    Fix9770 = false,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = false,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect NoLeak to fail: idea 2 with the passive flag on. A passive
// checkpoint fails after its publish, and the next one keeps the rows it
// wrote. probe_checkpoint_fails_after_publish.rs of the precondition audit
// shows this in Rust.
// Invariants: NoLeak
module idea2Passive {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 0,
    MaxFails = 1,
    InitFlag = true,
    OpenFlags = Set(true),
    OpenReadOnly = Set(false),
    CheckpointKinds = Set("passive", "truncflag"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = true,
    Fix9770 = false,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = false,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect no error: idea2 with FixBlockingStoreEmpty.
// Invariants: TypeOK RecoveryCorrect NoLeak
module idea2Fixed {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 0,
    MaxFails = 1,
    InitFlag = false,
    OpenFlags = Set(false),
    OpenReadOnly = Set(false),
    CheckpointKinds = Set("blocking"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = true,
    Fix9770 = false,
    FixBlockingStoreEmpty = true,
    RecoveryOneTs = false,
    FixStampBelowBoundary = false,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect no error: idea2Passive with FixStampBelowBoundary.
// Invariants: TypeOK RecoveryCorrect NoLeak
module idea2PassiveFixed {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 0,
    MaxFails = 1,
    InitFlag = true,
    OpenFlags = Set(true),
    OpenReadOnly = Set(false),
    CheckpointKinds = Set("passive", "truncflag"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = true,
    Fix9770 = false,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = true,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect RecoveryCorrect to fail: a hypothesis for an async I/O backend
// such as io_uring, not shown in Rust. The checkpoint resets the writer
// and submits the log truncate under the log lock. A commit appends at
// offset 0, fsyncs and is acknowledged. The backend then runs the truncate
// after the append and erases it, and a crash loses the commit.
// Invariants: RecoveryCorrect
module logTruncSplit {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 1,
    MaxFails = 0,
    InitFlag = true,
    OpenFlags = Set(true),
    OpenReadOnly = Set(false),
    CheckpointKinds = Set("passive", "truncflag"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = true,
    Fix9770 = true,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = false,
    AsyncLogTruncate = true,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect RecoveryCorrect to fail: the log truncate call of a blocking
// checkpoint fails (Unix returns the ftruncate error). The writer already
// restarted its running CRC from a new salt but keeps its offset. The next
// commit is acknowledged, but replay cannot chain its frame to the header,
// so a crash loses it. A LogicalLog unit test shows the lost frame in Rust.
// Invariants: RecoveryCorrect
module logTruncError {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 1,
    MaxFails = 1,
    InitFlag = false,
    OpenFlags = Set(false),
    OpenReadOnly = Set(false),
    CheckpointKinds = Set("blocking"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = true,
    Fix9770 = false,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = false,
    AsyncLogTruncate = false,
    LogTruncateErrors = true
  ).* from "./MvccRecovery"
}

// Expect NeverCrashBetweenLogAndWalTruncate to fail: a blocking checkpoint
// commits rows to the WAL and truncates the log, then a crash comes before
// the WAL truncate.
// Invariants: NeverCrashBetweenLogAndWalTruncate
module reachTruncWindow {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 1,
    MaxFails = 0,
    InitFlag = false,
    OpenFlags = Set(false),
    OpenReadOnly = Set(false),
    CheckpointKinds = Set("blocking"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = true,
    Fix9770 = false,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = false,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect NeverWalFramesNoLog to fail: a crash leaves WAL frames and no log
// header.
// Invariants: NeverWalFramesNoLog
module reachWalFramesNoLog {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 1,
    MaxFails = 0,
    InitFlag = true,
    OpenFlags = Set(false),
    OpenReadOnly = Set(false),
    CheckpointKinds = Set("passive"),
    CrashOnlyWhenIdle = true,
    TornHeaderGarbage = true,
    Fix9770 = false,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = false,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect NeverInvalidHeaderWithWal to fail: leadB and leadBStated reach
// files with WAL frames and a header that does not decode.
// Invariants: NeverInvalidHeaderWithWal
module reachInvalidHeaderWithWal {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 1,
    MaxFails = 0,
    InitFlag = true,
    OpenFlags = Set(false, true),
    OpenReadOnly = Set(false),
    CheckpointKinds = Set("passive"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = true,
    Fix9770 = true,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = false,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect NeverCleanCloseWithWal to fail: a clean close leaves WAL frames.
// Invariants: NeverCleanCloseWithWal
module reachCleanCloseWithWal {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 1,
    MaxFails = 0,
    InitFlag = true,
    OpenFlags = Set(true),
    OpenReadOnly = Set(true),
    CheckpointKinds = Set("passive"),
    CrashOnlyWhenIdle = true,
    TornHeaderGarbage = true,
    Fix9770 = true,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = false,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect NeverFailAfterPublishBlocking to fail: a blocking checkpoint with
// rows fails after its publish.
// Invariants: NeverFailAfterPublishBlocking
module reachFailAfterPublish {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 0,
    MaxFails = 1,
    InitFlag = false,
    OpenFlags = Set(false),
    OpenReadOnly = Set(false),
    CheckpointKinds = Set("blocking"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = true,
    Fix9770 = false,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = false,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect NeverFailAfterPublishFlagOn to fail: a passive checkpoint with
// rows fails after its publish.
// Invariants: NeverFailAfterPublishFlagOn
module reachFailAfterPublishFlagOn {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 0,
    MaxFails = 1,
    InitFlag = true,
    OpenFlags = Set(true),
    OpenReadOnly = Set(false),
    CheckpointKinds = Set("passive", "truncflag"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = true,
    Fix9770 = false,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = false,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect NeverFailBeforePublish to fail: mainPassive reaches a checkpoint
// that committed rows to the WAL and stops before its publish, with the
// boundary unchanged.
// Invariants: NeverFailBeforePublish
module reachFailBeforePublish {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 1,
    MaxFails = 1,
    InitFlag = true,
    OpenFlags = Set(false, true),
    OpenReadOnly = Set(false, true),
    CheckpointKinds = Set("blocking", "passive", "truncflag"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = true,
    Fix9770 = false,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = false,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect NeverCommitInPublishWindow to fail: in mainPassive a commit is
// acknowledged while a checkpoint with rows waits to publish.
// Invariants: NeverCommitInPublishWindow
module reachCommitInPublishWindow {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 1,
    MaxFails = 1,
    InitFlag = true,
    OpenFlags = Set(false, true),
    OpenReadOnly = Set(false, true),
    CheckpointKinds = Set("blocking", "passive", "truncflag"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = true,
    Fix9770 = false,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = false,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect NeverFsyncCoversUnacked to fail: in mainPassive the checkpoint log
// fsync makes an unacknowledged append durable.
// Invariants: NeverFsyncCoversUnacked
module reachFsyncCoversUnacked {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 1,
    MaxFails = 1,
    InitFlag = true,
    OpenFlags = Set(false, true),
    OpenReadOnly = Set(false, true),
    CheckpointKinds = Set("blocking", "passive", "truncflag"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = true,
    Fix9770 = false,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = false,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect NeverStampFixUsed to fail: in fixed, a checkpoint fails after its
// publish and a later checkpoint stamps those rows with the fix.
// Invariants: NeverStampFixUsed
module repairFixed {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 1,
    MaxFails = 1,
    InitFlag = false,
    OpenFlags = Set(false, true),
    OpenReadOnly = Set(false),
    CheckpointKinds = Set("blocking", "passive", "truncflag"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = true,
    Fix9770 = true,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = true,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect NeverOpenNoLogFlagOffOk to fail: in fixed, an open with the flag
// off starts on WAL frames with no log header and finishes recovery with the
// WAL removed and the committed rows.
// Invariants: NeverOpenNoLogFlagOffOk
module repairFixedOpen {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 1,
    MaxFails = 1,
    InitFlag = false,
    OpenFlags = Set(false, true),
    OpenReadOnly = Set(false),
    CheckpointKinds = Set("blocking", "passive", "truncflag"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = true,
    Fix9770 = true,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = true,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect NeverStampFixUsed to fail: in fixedPassive, a checkpoint fails
// after its publish and a later checkpoint stamps those rows with the fix.
// Invariants: NeverStampFixUsed
module repairFixedPassive {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 1,
    MaxFails = 1,
    InitFlag = true,
    OpenFlags = Set(false, true),
    OpenReadOnly = Set(false),
    CheckpointKinds = Set("blocking", "passive", "truncflag"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = true,
    Fix9770 = true,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = true,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect NeverOpenNoLogFlagOffOk to fail: in fixedPassive, an open with the
// flag off finishes recovery after a passive checkpoint, with the WAL
// removed and the committed rows.
// Invariants: NeverOpenNoLogFlagOffOk
module repairFixedPassiveOpen {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 1,
    MaxFails = 1,
    InitFlag = true,
    OpenFlags = Set(false, true),
    OpenReadOnly = Set(false),
    CheckpointKinds = Set("blocking", "passive", "truncflag"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = true,
    Fix9770 = true,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = true,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect NeverOpenNoLogFlagOffOk to fail: issue7480Fixed finishes recovery
// of the #7480 files with the flag off.
// Invariants: NeverOpenNoLogFlagOffOk
module repairIssue7480 {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 1,
    MaxFails = 0,
    InitFlag = false,
    OpenFlags = Set(false),
    OpenReadOnly = Set(false),
    CheckpointKinds = Set("blocking"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = true,
    Fix9770 = true,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = false,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect NeverOpenNoLogFlagOffOk to fail: issue9749Fixed finishes recovery
// of the #9749 files with the flag off.
// Invariants: NeverOpenNoLogFlagOffOk
module repairIssue9749 {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 1,
    MaxFails = 0,
    InitFlag = true,
    OpenFlags = Set(false),
    OpenReadOnly = Set(false),
    CheckpointKinds = Set("passive"),
    CrashOnlyWhenIdle = true,
    TornHeaderGarbage = true,
    Fix9770 = true,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = false,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect NeverOpenAfterTornFirstAppendOk to fail: in leadBZeros a crash
// tears the header of a first append while the WAL has frames. The header
// reads as no header, and the open finishes recovery with the WAL removed
// and the committed rows.
// Invariants: NeverOpenAfterTornFirstAppendOk
module repairLeadBZeros {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 1,
    MaxFails = 0,
    InitFlag = true,
    OpenFlags = Set(true),
    OpenReadOnly = Set(false),
    CheckpointKinds = Set("passive"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = false,
    Fix9770 = true,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = false,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect NeverStampFixUsed to fail: in idea2Fixed the fix stamps the rows
// of the failed checkpoint.
// Invariants: NeverStampFixUsed
module repairIdea2 {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 0,
    MaxFails = 1,
    InitFlag = false,
    OpenFlags = Set(false),
    OpenReadOnly = Set(false),
    CheckpointKinds = Set("blocking"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = true,
    Fix9770 = false,
    FixBlockingStoreEmpty = true,
    RecoveryOneTs = false,
    FixStampBelowBoundary = false,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect NeverStampFixUsed to fail: in idea2PassiveFixed the fix stamps the
// rows of the failed checkpoint.
// Invariants: NeverStampFixUsed
module repairIdea2Passive {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 0,
    MaxFails = 1,
    InitFlag = true,
    OpenFlags = Set(true),
    OpenReadOnly = Set(false),
    CheckpointKinds = Set("passive", "truncflag"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = true,
    Fix9770 = false,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = true,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect NeverReplayMany to fail: main replays two frames at one open.
// Invariants: NeverReplayMany
module reachReplayMany {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 1,
    MaxFails = 1,
    InitFlag = false,
    OpenFlags = Set(false, true),
    OpenReadOnly = Set(false, true),
    CheckpointKinds = Set("blocking", "passive", "truncflag"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = true,
    Fix9770 = false,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = false,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect NeverCkOfReplayedRows to fail: main checkpoints replayed rows
// after an open.
// Invariants: NeverCkOfReplayedRows
module reachCkOfReplayedRows {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 1,
    MaxFails = 1,
    InitFlag = false,
    OpenFlags = Set(false, true),
    OpenReadOnly = Set(false, true),
    CheckpointKinds = Set("blocking", "passive", "truncflag"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = true,
    Fix9770 = false,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = false,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect NeverLogRetained to fail: mainPassive keeps log frames in a
// checkpoint, because a commit is above the snapshot.
// Invariants: NeverLogRetained
module reachLogRetained {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 1,
    MaxFails = 1,
    InitFlag = true,
    OpenFlags = Set(false, true),
    OpenReadOnly = Set(false, true),
    CheckpointKinds = Set("blocking", "passive", "truncflag"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = true,
    Fix9770 = false,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = false,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect NeverTruncFlagDone to fail: mainPassive finishes a TRUNCATE
// checkpoint with rows and the flag on.
// Invariants: NeverTruncFlagDone
module reachTruncFlag {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 1,
    MaxFails = 1,
    InitFlag = true,
    OpenFlags = Set(false, true),
    OpenReadOnly = Set(false, true),
    CheckpointKinds = Set("blocking", "passive", "truncflag"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = true,
    Fix9770 = false,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = false,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect NeverCrashInRecovery to fail: twoCrashes crashes after the
// recovery header write and before the WAL truncate.
// Invariants: NeverCrashInRecovery
module reachCrashInRecovery {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 2,
    MaxCheckpoints = 2,
    MaxCrashes = 2,
    MaxFails = 0,
    InitFlag = true,
    OpenFlags = Set(false, true),
    OpenReadOnly = Set(false),
    CheckpointKinds = Set("passive"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = true,
    Fix9770 = true,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = true,
    AsyncLogTruncate = false,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}

// Expect NeverAppendErased to fail: an async truncate erases a newer append.
// Invariants: NeverAppendErased
module reachAppendErased {
  import MvccRecovery(
    Keys = Set("a"),
    MaxTs = 3,
    MaxCheckpoints = 2,
    MaxCrashes = 1,
    MaxFails = 0,
    InitFlag = true,
    OpenFlags = Set(true),
    OpenReadOnly = Set(false),
    CheckpointKinds = Set("passive", "truncflag"),
    CrashOnlyWhenIdle = false,
    TornHeaderGarbage = true,
    Fix9770 = true,
    FixBlockingStoreEmpty = false,
    RecoveryOneTs = false,
    FixStampBelowBoundary = false,
    AsyncLogTruncate = true,
    LogTruncateErrors = false
  ).* from "./MvccRecovery"
}
