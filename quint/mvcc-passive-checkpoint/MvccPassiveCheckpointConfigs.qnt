// Configurations of MvccPassiveCheckpoint.qnt. Each module sets the
// constants of one configuration. The comment above a module gives the
// result to expect and the invariants to check. The Makefile reads these
// comments.

// The main configuration has one key, so that TLC finishes in about 4
// minutes. DESIGN.md gives the result for two keys.
// Expect no error: every rule of the design is on, as in the code today.
// Invariants: TypeOK ReadsCorrect RecoveryCorrect BackfillBelowReaders BackfillFloorBelowReaders
module main {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1", "r2"),
    MaxTs = 5,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
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
    GuardGcRechecksIdleLwm = true
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
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
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
    GuardGcRechecksIdleLwm = true
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
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
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
    GuardGcRechecksIdleLwm = true
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
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
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
    GuardGcRechecksIdleLwm = true
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
    MaxCheckpoints = 3,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
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
    GuardGcRechecksIdleLwm = true
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
    MaxCheckpoints = 3,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
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
    GuardGcRechecksIdleLwm = true
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
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
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
    GuardGcRechecksIdleLwm = true
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
    MaxCheckpoints = 3,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
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
    GuardGcRechecksIdleLwm = true
  ).* from "./MvccPassiveCheckpoint"
}

// Expect no error: backfill_floor alone is a safe GC floor.
// Invariants: TypeOK ReadsCorrect RecoveryCorrect BackfillBelowReaders BackfillFloorBelowReaders
module floorBackfill {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1", "r2"),
    MaxTs = 5,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "backfill",
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
    GuardGcRechecksIdleLwm = true
  ).* from "./MvccPassiveCheckpoint"
}

// Expect no error: the lowest pinned read mark alone is a safe GC floor.
// Invariants: TypeOK ReadsCorrect RecoveryCorrect BackfillBelowReaders BackfillFloorBelowReaders
module floorPinned {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1", "r2"),
    MaxTs = 5,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "pinned",
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
    GuardGcRechecksIdleLwm = true
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
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "registered",
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
    GuardGcRechecksIdleLwm = true
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
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
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
    GuardGcRechecksIdleLwm = false
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
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
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
    GuardGcRechecksIdleLwm = true
  ).* from "./MvccPassiveCheckpoint"
}

// Expect BackfillBelowReaders to fail: rule R4 is off.
// Invariants: BackfillBelowReaders
module noBackfillStop {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1", "r2"),
    MaxTs = 5,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = false,
    GcFloorRule = "code",
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
    GuardGcRechecksIdleLwm = true
  ).* from "./MvccPassiveCheckpoint"
}

// Expect RecoveryCorrect to fail: rule R2 is off.
// Invariants: RecoveryCorrect
module noGcWait {
  import MvccPassiveCheckpoint(
    Keys = Set("k1"),
    Readers = Set("r1", "r2"),
    MaxTs = 5,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = false,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
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
    GuardGcRechecksIdleLwm = true
  ).* from "./MvccPassiveCheckpoint"
}

// Expect RecoveryCorrect to fail: rule R1 is off.
// Invariants: RecoveryCorrect
module noSnapshotClamp {
  import MvccPassiveCheckpoint(
    Keys = Set("k1", "k2"),
    Readers = Set("r1"),
    MaxTs = 5,
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = false,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = true,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
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
    GuardGcRechecksIdleLwm = true
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
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = false,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "pinned",
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
    GuardGcRechecksIdleLwm = true
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
    MaxCheckpoints = 2,
    GuardSnapshotBelowPreparing = true,
    GuardGcWaitsForReaders = true,
    GuardDropCurrentOnlyWhenIdle = false,
    GuardBackfillStopsAtReaders = true,
    GcFloorRule = "code",
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
    GuardGcRechecksIdleLwm = true
  ).* from "./MvccPassiveCheckpoint"
}
