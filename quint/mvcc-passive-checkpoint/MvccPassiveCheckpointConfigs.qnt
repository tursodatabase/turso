// Configurations of MvccPassiveCheckpoint.qnt. Each module sets the
// constants of one configuration. The comment above a module gives the
// result to expect and the invariants to check. The Makefile reads these
// comments.

// The main configuration has one key, so that TLC finishes in about 4
// minutes. DESIGN.md gives the result for two keys.
// Expect no error: every rule is on and the checkpoint refuses an open reader.
// Invariants: TypeOK ReadsCorrect RecoveryCorrect BackfillBelowReaders BackfillFloorBelowReaders GcKeepsVersionsAboveReaders NoStrandedVersions
module main {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1", "r2"),
    MaxTs = 5,
    FixRefuseCheckpointOwner = true,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
    CheckpointOnReaderConnection = "keepsMark",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// The main configuration with two keys, a longer run and the trace replay
// constants, for the traces of `make traces`. The Makefile does not model check
// it, because TLC needs hours for two keys.
// Expect no error: every rule of the design is on, as in the code today.
// Invariants: TypeOK ReadsCorrect RecoveryCorrect BackfillBelowReaders BackfillFloorBelowReaders
module mbt {
  import MvccPassiveCheckpoint(
    Keys = Set("k1", "k2"),
    Readers = Set("r1", "r2"),
    MaxTs = 8,
    FixRefuseCheckpointOwner = false,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = false,
    MaxCheckpoints = 3,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
    CheckpointOnReaderConnection = "none",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = true,
    LogAppendOneAtATime = true,
    WriterBeginTicks = true,
    WritersPinMarks = true
  ).* from "./MvccPassiveCheckpoint"
}

// Expect no error: rollback can keep materialized_at of the version it makes
// current again.
// Invariants: TypeOK ReadsCorrect RecoveryCorrect BackfillBelowReaders BackfillFloorBelowReaders CollectsEveryChange
module abortsKeepMaterialized {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1"),
    MaxTs = 5,
    FixRefuseCheckpointOwner = false,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
    CheckpointOnReaderConnection = "none",
    ReadLooksAtChainTwice = true,
    Aborts = true,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = true,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect ReadsCorrect to fail: a reader that read a write that rolled back does
// not abort.
// Invariants: ReadsCorrect
module abortsNoDependency {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1"),
    MaxTs = 5,
    FixRefuseCheckpointOwner = false,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
    CheckpointOnReaderConnection = "none",
    ReadLooksAtChainTwice = true,
    Aborts = true,
    GuardReadersAbortOnDependency = false,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect no error: writers can roll back before the log append, and readers
// that depended on them abort.
// Invariants: TypeOK ReadsCorrect RecoveryCorrect BackfillBelowReaders BackfillFloorBelowReaders CollectsEveryChange
module aborts {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1"),
    MaxTs = 5,
    FixRefuseCheckpointOwner = false,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
    CheckpointOnReaderConnection = "none",
    ReadLooksAtChainTwice = true,
    Aborts = true,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect CollectsEveryChange to fail: a full scan is cleared by a snapshot
// below the lost stamp.
// Invariants: CollectsEveryChange
module dirtyClearFullScanEarly {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set(),
    MaxTs = 5,
    FixRefuseCheckpointOwner = false,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = false,
    MaxCheckpoints = 3,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
    CheckpointOnReaderConnection = "none",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = true,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 1,
    GuardFullScanUntilStampCovered = false,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect no error: collection visits only dirty keys, with rollbacks and one
// failed re-mark.
// Invariants: TypeOK ReadsCorrect RecoveryCorrect BackfillBelowReaders BackfillFloorBelowReaders CollectsEveryChange
module dirtyKeys {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1"),
    MaxTs = 5,
    FixRefuseCheckpointOwner = false,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = false,
    MaxCheckpoints = 3,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
    CheckpointOnReaderConnection = "none",
    ReadLooksAtChainTwice = true,
    Aborts = true,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = true,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 1,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect CollectsEveryChange to fail: the dirty mark comes after CommitEnd, so
// a checkpoint can miss the key.
// Invariants: CollectsEveryChange
module dirtyMarkAfterCommit {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set(),
    MaxTs = 5,
    FixRefuseCheckpointOwner = false,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
    CheckpointOnReaderConnection = "none",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = true,
    GuardMarkBeforeCommitEnd = false,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect no error: the generation compare_exchange in
// clear_checkpoint_full_scan is not needed in the row model.
// Invariants: TypeOK ReadsCorrect RecoveryCorrect BackfillBelowReaders BackfillFloorBelowReaders CollectsEveryChange
module dirtyNoGenerationCas {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set(),
    MaxTs = 5,
    FixRefuseCheckpointOwner = false,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = false,
    MaxCheckpoints = 3,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
    CheckpointOnReaderConnection = "none",
    ReadLooksAtChainTwice = true,
    Aborts = true,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = true,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 2,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = false,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect no error: backfill_floor alone is a safe GC floor.
// Invariants: TypeOK ReadsCorrect RecoveryCorrect BackfillBelowReaders BackfillFloorBelowReaders
module floorBackfill {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1", "r2"),
    MaxTs = 5,
    FixRefuseCheckpointOwner = false,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "backfill",
    CheckpointOnReaderConnection = "none",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect no error: the lowest pinned read mark alone is a safe GC floor.
// Invariants: TypeOK ReadsCorrect RecoveryCorrect BackfillBelowReaders BackfillFloorBelowReaders
module floorPinned {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1", "r2"),
    MaxTs = 5,
    FixRefuseCheckpointOwner = false,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "pinned",
    CheckpointOnReaderConnection = "none",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect ReadsCorrect to fail: the GC floor leaves out pinned readers that are
// not in txs (rule R5).
// Invariants: ReadsCorrect
module floorRegistered {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1", "r2"),
    MaxTs = 5,
    FixRefuseCheckpointOwner = false,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "registered",
    CheckpointOnReaderConnection = "none",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect ReadsCorrect to fail: GC uses a stale lwm of no reader and Rule 3
// drops a row that an open reader needs.
// Invariants: ReadsCorrect
module gcSampleNoRecheck {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1", "r2"),
    MaxTs = 5,
    FixRefuseCheckpointOwner = false,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
    CheckpointOnReaderConnection = "none",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = true,
    GuardGcRechecksIdleLwm = false,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect no error: GC samples lwm and the floor before the chain lock and
// computes lwm again when it was no reader.
// Invariants: TypeOK ReadsCorrect RecoveryCorrect BackfillBelowReaders BackfillFloorBelowReaders CollectsEveryChange
module gcSample {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1", "r2"),
    MaxTs = 5,
    FixRefuseCheckpointOwner = false,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
    CheckpointOnReaderConnection = "none",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = true,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect BackfillBelowReaders to fail: rule R4 is off.
// Invariants: BackfillBelowReaders
module noBackfillStop {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1", "r2"),
    MaxTs = 5,
    FixRefuseCheckpointOwner = false,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = false,
    GcFloorRule = "code",
    CheckpointOnReaderConnection = "none",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect RecoveryCorrect to fail: rule R2 is off.
// Invariants: RecoveryCorrect
module noGcWait {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1", "r2"),
    MaxTs = 5,
    FixRefuseCheckpointOwner = false,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = false,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
    CheckpointOnReaderConnection = "none",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect RecoveryCorrect to fail: rule R1 is off.
// Invariants: RecoveryCorrect
module noSnapshotClamp {
  import MvccPassiveCheckpoint(
    Keys = Set("k1", "k2"),
    Readers = Set("r1"),
    MaxTs = 5,
    FixRefuseCheckpointOwner = false,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = false,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
    CheckpointOnReaderConnection = "none",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect no error: with one look at the chain per read, Rule 3 can run while
// readers are open.
// Invariants: TypeOK ReadsCorrect RecoveryCorrect BackfillBelowReaders BackfillFloorBelowReaders
module rule3UnderLoadOneLook {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1", "r2"),
    MaxTs = 5,
    FixRefuseCheckpointOwner = false,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = false,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "pinned",
    CheckpointOnReaderConnection = "none",
    ReadLooksAtChainTwice = false,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect ReadsCorrect to fail: rule R3 is off and reads look at the chain
// twice.
// Invariants: ReadsCorrect
module rule3UnderLoad {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1", "r2"),
    MaxTs = 5,
    FixRefuseCheckpointOwner = false,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = false,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
    CheckpointOnReaderConnection = "none",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect ReadsCorrect to fail: Connection::checkpoint runs on the connection
// of an open reader, the backfill passes the reader and its read mark moves.
// The reader reads a value that commits after it began. GC drops nothing.
// Invariants: ReadsCorrect
module ownCheckpoint {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1", "r2"),
    MaxTs = 5,
    FixRefuseCheckpointOwner = false,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
    CheckpointOnReaderConnection = "movesMark",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect ReadsCorrect to fail: the same as ownCheckpoint with backfill_floor
// as the only GC floor.
// Invariants: ReadsCorrect
module ownCheckpointBackfillFloor {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1", "r2"),
    MaxTs = 5,
    FixRefuseCheckpointOwner = false,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "backfill",
    CheckpointOnReaderConnection = "movesMark",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect ReadsCorrect to fail: the same as ownCheckpoint, but the reader keeps
// its read mark. A read of the database file finds a newer value.
// Invariants: ReadsCorrect
module ownCheckpointKeepsMark {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1", "r2"),
    MaxTs = 5,
    FixRefuseCheckpointOwner = false,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
    CheckpointOnReaderConnection = "keepsMark",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect ReadsCorrect to fail: the same as ownCheckpointKeepsMark with
// backfill_floor as the only GC floor.
// Invariants: ReadsCorrect
module ownCheckpointKeepsMarkBackfillFloor {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1", "r2"),
    MaxTs = 5,
    FixRefuseCheckpointOwner = false,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "backfill",
    CheckpointOnReaderConnection = "keepsMark",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect no error: the code floor still counts the owner's registered snapshot
// after the pager commit releases its pin.
// Invariants: GcKeepsVersionsAboveReaders
module ownCheckpointGc {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1", "r2"),
    MaxTs = 5,
    FixRefuseCheckpointOwner = false,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
    CheckpointOnReaderConnection = "keepsMark",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect GcKeepsVersionsAboveReaders to fail: the same path with
// backfill_floor as the only GC floor.
// Invariants: GcKeepsVersionsAboveReaders
module ownCheckpointGcBackfillFloor {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1", "r2"),
    MaxTs = 5,
    FixRefuseCheckpointOwner = false,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "backfill",
    CheckpointOnReaderConnection = "keepsMark",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect GcKeepsVersionsAboveReaders to fail: the pinned-only floor misses
// the owner's registered snapshot after its pager pin disappears.
// Invariants: GcKeepsVersionsAboveReaders
module ownCheckpointGcPinnedFloor {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1", "r2"),
    MaxTs = 5,
    FixRefuseCheckpointOwner = false,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "pinned",
    CheckpointOnReaderConnection = "keepsMark",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect NeverStampedAboveReader to fail: a pinned reader has a read mark
// below the stamp of a version.
// Invariants: NeverStampedAboveReader
module reachStampedAboveReader {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1", "r2"),
    MaxTs = 5,
    FixRefuseCheckpointOwner = false,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
    CheckpointOnReaderConnection = "none",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect NeverReaderMarkBelowBackfillFloor to fail: an owner's registered
// snapshot keeps a read mark below backfill_floor after its pager pin disappears.
// Invariants: NeverReaderMarkBelowBackfillFloor
module reachReaderMarkBelowFloor {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1", "r2"),
    MaxTs = 5,
    FixRefuseCheckpointOwner = false,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
    CheckpointOnReaderConnection = "keepsMark",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect NoStrandedVersions to fail: a failed publish pass leaves versions unstamped after a later pass.
// Invariants: NoStrandedVersions
module failedPublish {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set(),
    MaxTs = 3,
    FixRefuseCheckpointOwner = false,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = true,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
    CheckpointOnReaderConnection = "none",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect no error: a later pass collects and stamps versions missed after publish.
// Invariants: TypeOK ReadsCorrect RecoveryCorrect BackfillBelowReaders BackfillFloorBelowReaders GcKeepsVersionsAboveReaders NoStrandedVersions
module failedPublishFixed {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set(),
    MaxTs = 3,
    FixRefuseCheckpointOwner = false,
    Mode = "passive",
    FixRecollectUnstamped = true,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = true,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
    CheckpointOnReaderConnection = "none",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect NeverFailsAfterPublish to fail: a nonempty pager commit publishes and then fails before the stamp.
// Invariants: NeverFailsAfterPublish
module reachFailedPublish {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set(),
    MaxTs = 3,
    FixRefuseCheckpointOwner = false,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = true,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
    CheckpointOnReaderConnection = "none",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect NeverRepairsVersions to fail: a later pass stamps versions from a failed publish pass.
// Invariants: NeverRepairsVersions
module reachRepairedVersions {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set(),
    MaxTs = 3,
    FixRefuseCheckpointOwner = false,
    Mode = "passive",
    FixRecollectUnstamped = true,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = true,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
    CheckpointOnReaderConnection = "none",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect RecoveryCorrect to fail: an append completes between the truncate decision and completion.
// Invariants: RecoveryCorrect
module logTruncateRace {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set(),
    MaxTs = 3,
    FixRefuseCheckpointOwner = false,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = true,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = false,
    MaxCheckpoints = 1,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
    CheckpointOnReaderConnection = "none",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect NeverLosesAppendAtTruncate to fail: a nonempty pager commit precedes a truncate that loses a later append.
// Invariants: NeverLosesAppendAtTruncate
module reachTruncateAppend {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set(),
    MaxTs = 3,
    FixRefuseCheckpointOwner = false,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = true,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = false,
    MaxCheckpoints = 1,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
    CheckpointOnReaderConnection = "none",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect no error: an atomic truncate keeps later appends.
// Invariants: TypeOK ReadsCorrect RecoveryCorrect BackfillBelowReaders BackfillFloorBelowReaders GcKeepsVersionsAboveReaders NoStrandedVersions
module logTruncateAtomic {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set(),
    MaxTs = 3,
    FixRefuseCheckpointOwner = false,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = false,
    MaxCheckpoints = 1,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
    CheckpointOnReaderConnection = "none",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect NoStrandedVersions to fail: Busy leaves versions unstamped even after another pass completes.
// Invariants: NoStrandedVersions
module truncateFlagOn {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1"),
    MaxTs = 4,
    FixRefuseCheckpointOwner = false,
    Mode = "truncateFlagOn",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
    CheckpointOnReaderConnection = "none",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect no error: a later pass stamps versions after TRUNCATE fails with Busy.
// Invariants: TypeOK ReadsCorrect RecoveryCorrect BackfillBelowReaders BackfillFloorBelowReaders GcKeepsVersionsAboveReaders NoStrandedVersions
module truncateFlagOnFixed {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1"),
    MaxTs = 4,
    FixRefuseCheckpointOwner = false,
    Mode = "truncateFlagOn",
    FixRecollectUnstamped = true,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
    CheckpointOnReaderConnection = "none",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect NeverRepairsVersions to fail: a later pass repairs versions after a nonempty TRUNCATE publish fails with Busy.
// Invariants: NeverRepairsVersions
module reachTruncateRepair {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1"),
    MaxTs = 4,
    FixRefuseCheckpointOwner = false,
    Mode = "truncateFlagOn",
    FixRecollectUnstamped = true,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
    CheckpointOnReaderConnection = "none",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect no error: the checkpoint refuses a connection with an open snapshot.
// Invariants: TypeOK ReadsCorrect RecoveryCorrect BackfillBelowReaders BackfillFloorBelowReaders GcKeepsVersionsAboveReaders NoStrandedVersions
module refuseCheckpointOwner {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1"),
    MaxTs = 4,
    FixRefuseCheckpointOwner = true,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = false,
    FixCleanupPublishes = false,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
    CheckpointOnReaderConnection = "keepsMark",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect no error: the pager commit stamps versions and cleanup publishes a stopped pass.
// Invariants: TypeOK ReadsCorrect RecoveryCorrect BackfillBelowReaders BackfillFloorBelowReaders GcKeepsVersionsAboveReaders NoStrandedVersions
module rustFix {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1"),
    MaxTs = 4,
    FixRefuseCheckpointOwner = true,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = true,
    FixCleanupPublishes = true,
    CheckpointFailures = true,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
    CheckpointOnReaderConnection = "keepsMark",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect Safety to fail: the Rust fixes still allow an owner to lose its pager pin.
// Invariants: Safety
module rustFixOwner {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1"),
    MaxTs = 4,
    FixRefuseCheckpointOwner = false,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = true,
    FixCleanupPublishes = true,
    CheckpointFailures = true,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
    CheckpointOnReaderConnection = "keepsMark",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect no error: early stamps survive Busy after a TRUNCATE publish.
// Invariants: TypeOK ReadsCorrect RecoveryCorrect BackfillBelowReaders BackfillFloorBelowReaders GcKeepsVersionsAboveReaders NoStrandedVersions
module rustFixTruncate {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1"),
    MaxTs = 4,
    FixRefuseCheckpointOwner = true,
    Mode = "truncateFlagOn",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = true,
    FixCleanupPublishes = true,
    CheckpointFailures = true,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
    CheckpointOnReaderConnection = "keepsMark",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect NeverCleanupPublishes to fail: cleanup publishes a failed nonempty pager commit.
// Invariants: NeverCleanupPublishes
module reachCleanupPublish {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1"),
    MaxTs = 4,
    FixRefuseCheckpointOwner = true,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = true,
    FixCleanupPublishes = true,
    CheckpointFailures = true,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
    CheckpointOnReaderConnection = "keepsMark",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect no error: early stamps remain safe with the lowest pinned mark as the GC floor.
// Invariants: TypeOK ReadsCorrect RecoveryCorrect BackfillBelowReaders BackfillFloorBelowReaders GcKeepsVersionsAboveReaders NoStrandedVersions
module rustFixPinnedFloor {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1"),
    MaxTs = 4,
    FixRefuseCheckpointOwner = true,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = true,
    FixCleanupPublishes = true,
    CheckpointFailures = true,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "pinned",
    CheckpointOnReaderConnection = "keepsMark",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect NeverGcEarlyStampWithReader to fail: GC drops an early-stamped version before the late stamp while a reader is open.
// Invariants: NeverGcEarlyStampWithReader
module reachEarlyStampGc {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1"),
    MaxTs = 4,
    FixRefuseCheckpointOwner = true,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = true,
    FixCleanupPublishes = true,
    CheckpointFailures = true,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "pinned",
    CheckpointOnReaderConnection = "keepsMark",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}

// Expect NeverGcEarlyStampAfterFailure to fail: GC drops an early-stamped version after a failed pass and a later no-op pass.
// Invariants: NeverGcEarlyStampAfterFailure
module reachEarlyStampGcAfterFailure {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1"),
    MaxTs = 4,
    FixRefuseCheckpointOwner = true,
    Mode = "passive",
    FixRecollectUnstamped = false,
    SplitLogTruncate = false,
    FixStampAtPagerCommit = true,
    FixCleanupPublishes = true,
    CheckpointFailures = true,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
    CheckpointOnReaderConnection = "keepsMark",
    ReadLooksAtChainTwice = true,
    Aborts = false,
    GuardReadersAbortOnDependency = true,
    AbortKeepsMaterialized = false,
    DirtyKeys = false,
    GuardMarkBeforeCommitEnd = true,
    MaxMarkFailures = 0,
    GuardFullScanUntilStampCovered = true,
    GuardFullScanGenerationCas = true,
    GcSamplesFirst = false,
    GuardGcRechecksIdleLwm = true,
    BackfillToLimit = false,
    LogAppendOneAtATime = false,
    WriterBeginTicks = false,
    WritersPinMarks = false
  ).* from "./MvccPassiveCheckpoint"
}
