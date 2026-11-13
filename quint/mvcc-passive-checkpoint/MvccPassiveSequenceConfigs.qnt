// Configurations of MvccPassiveSequence.qnt. Each module sets the
// constants of one configuration. The comment above a module gives the
// result to expect and the invariants to check. The Makefile reads these
// comments.

// Expect no error: every special case is on, as in the code today. setval is
// off: with setval on, the code breaks CompactionKeepsCurrent
// (module setvalRace).
// Invariants: TypeOK ReadsCorrect CompactionKeepsCurrent UniqueValues RecoveryCorrect NoStrandedVersions
module main {
  import MvccPassiveSequence(
    MaxVal = 3,
    Txs = Set("t1", "t2"),
    MaxTs = 8,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    Ascending = true,
    Cycle = false,
    WithSetval = false,
    Mode = "passive",
    GuardWatermarkFollowsDirection = true,
    GuardDeferDeletes = true,
    GuardSkipCycle = true,
    GuardBlockingPurge = true,
    FixStampSnapshotVersion = false,
    DeleteTsRule = "clock"
  ).* from "./MvccPassiveSequence"
}

// Expect no error: rule S5 is off. The kept chains are below the watermark.
// Invariants: TypeOK ReadsCorrect CompactionKeepsCurrent UniqueValues RecoveryCorrect NoStrandedVersions
module blockingNoPurge {
  import MvccPassiveSequence(
    MaxVal = 3,
    Txs = Set("t1", "t2"),
    MaxTs = 8,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    Ascending = true,
    Cycle = false,
    WithSetval = false,
    Mode = "blocking",
    GuardWatermarkFollowsDirection = true,
    GuardDeferDeletes = true,
    GuardSkipCycle = true,
    GuardBlockingPurge = false,
    FixStampSnapshotVersion = false,
    DeleteTsRule = "clock"
  ).* from "./MvccPassiveSequence"
}

// Expect no error: the code with the blocking (TRUNCATE) checkpoint.
// Invariants: TypeOK ReadsCorrect CompactionKeepsCurrent UniqueValues RecoveryCorrect NoStrandedVersions
module blocking {
  import MvccPassiveSequence(
    MaxVal = 3,
    Txs = Set("t1", "t2"),
    MaxTs = 8,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    Ascending = true,
    Cycle = false,
    WithSetval = false,
    Mode = "blocking",
    GuardWatermarkFollowsDirection = true,
    GuardDeferDeletes = true,
    GuardSkipCycle = true,
    GuardBlockingPurge = true,
    FixStampSnapshotVersion = false,
    DeleteTsRule = "clock"
  ).* from "./MvccPassiveSequence"
}

// Expect no error: the code with a CYCLE sequence.
// Invariants: TypeOK ReadsCorrect CompactionKeepsCurrent UniqueValues RecoveryCorrect NoStrandedVersions
module cycle {
  import MvccPassiveSequence(
    MaxVal = 3,
    Txs = Set("t1", "t2"),
    MaxTs = 8,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    Ascending = true,
    Cycle = true,
    WithSetval = false,
    Mode = "passive",
    GuardWatermarkFollowsDirection = true,
    GuardDeferDeletes = true,
    GuardSkipCycle = true,
    GuardBlockingPurge = true,
    FixStampSnapshotVersion = false,
    DeleteTsRule = "clock"
  ).* from "./MvccPassiveSequence"
}

// Expect NoStrandedVersions to fail: rule S3 is off. Recorded deletes use the
// snapshot timestamp.
// Invariants: NoStrandedVersions
module deleteTsSnapshot {
  import MvccPassiveSequence(
    MaxVal = 3,
    Txs = Set("t1", "t2"),
    MaxTs = 8,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    Ascending = true,
    Cycle = false,
    WithSetval = false,
    Mode = "passive",
    GuardWatermarkFollowsDirection = true,
    GuardDeferDeletes = true,
    GuardSkipCycle = true,
    GuardBlockingPurge = true,
    FixStampSnapshotVersion = false,
    DeleteTsRule = "snapshot"
  ).* from "./MvccPassiveSequence"
}

// Expect no error: the code with a descending sequence.
// Invariants: TypeOK ReadsCorrect CompactionKeepsCurrent UniqueValues RecoveryCorrect NoStrandedVersions
module descending {
  import MvccPassiveSequence(
    MaxVal = 3,
    Txs = Set("t1", "t2"),
    MaxTs = 8,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    Ascending = false,
    Cycle = false,
    WithSetval = false,
    Mode = "passive",
    GuardWatermarkFollowsDirection = true,
    GuardDeferDeletes = true,
    GuardSkipCycle = true,
    GuardBlockingPurge = true,
    FixStampSnapshotVersion = false,
    DeleteTsRule = "clock"
  ).* from "./MvccPassiveSequence"
}

// Expect ReadsCorrect to fail: rule S2 is off. Passive deletes rows and purges
// chains inside the checkpoint.
// Invariants: ReadsCorrect
module noDeferDeletes {
  import MvccPassiveSequence(
    MaxVal = 3,
    Txs = Set("t1", "t2"),
    MaxTs = 7,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    Ascending = true,
    Cycle = false,
    WithSetval = false,
    Mode = "passive",
    GuardWatermarkFollowsDirection = true,
    GuardDeferDeletes = false,
    GuardSkipCycle = true,
    GuardBlockingPurge = true,
    FixStampSnapshotVersion = false,
    DeleteTsRule = "clock"
  ).* from "./MvccPassiveSequence"
}

// Expect no error: rule S4 is off. A CYCLE table has one row, so compaction has
// nothing to delete.
// Invariants: TypeOK ReadsCorrect CompactionKeepsCurrent UniqueValues RecoveryCorrect NoStrandedVersions
module noSkipCycle {
  import MvccPassiveSequence(
    MaxVal = 3,
    Txs = Set("t1", "t2"),
    MaxTs = 8,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    Ascending = true,
    Cycle = true,
    WithSetval = false,
    Mode = "passive",
    GuardWatermarkFollowsDirection = true,
    GuardDeferDeletes = true,
    GuardSkipCycle = false,
    GuardBlockingPurge = true,
    FixStampSnapshotVersion = false,
    DeleteTsRule = "clock"
  ).* from "./MvccPassiveSequence"
}

// Expect CompactionKeepsCurrent to fail: rule S1 is off. A descending sequence
// keeps its largest row and deletes its current value.
// Invariants: CompactionKeepsCurrent
module noWatermarkDirection {
  import MvccPassiveSequence(
    MaxVal = 3,
    Txs = Set("t1", "t2"),
    MaxTs = 6,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    Ascending = false,
    Cycle = false,
    WithSetval = false,
    Mode = "passive",
    GuardWatermarkFollowsDirection = false,
    GuardDeferDeletes = true,
    GuardSkipCycle = true,
    GuardBlockingPurge = true,
    FixStampSnapshotVersion = false,
    DeleteTsRule = "clock"
  ).* from "./MvccPassiveSequence"
}

// Expect no error: the proposed fix for finding 1. A recorded delete skips a
// version that began after the snapshot.
// Invariants: TypeOK ReadsCorrect CompactionKeepsCurrent RecoveryCorrect NoStrandedVersions
module setvalFixed {
  import MvccPassiveSequence(
    MaxVal = 2,
    Txs = Set("t1", "t2"),
    MaxTs = 6,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    Ascending = true,
    Cycle = false,
    WithSetval = true,
    Mode = "passive",
    GuardWatermarkFollowsDirection = true,
    GuardDeferDeletes = true,
    GuardSkipCycle = true,
    GuardBlockingPurge = true,
    FixStampSnapshotVersion = true,
    DeleteTsRule = "clock"
  ).* from "./MvccPassiveSequence"
}

// Expect CompactionKeepsCurrent to fail: finding 1. A setval that commits
// during the checkpoint loses its row.
// Invariants: CompactionKeepsCurrent
module setvalRace {
  import MvccPassiveSequence(
    MaxVal = 2,
    Txs = Set("t1", "t2"),
    MaxTs = 6,
    CheckpointFailures = false,
    MaxCheckpoints = 2,
    Ascending = true,
    Cycle = false,
    WithSetval = true,
    Mode = "passive",
    GuardWatermarkFollowsDirection = true,
    GuardDeferDeletes = true,
    GuardSkipCycle = true,
    GuardBlockingPurge = true,
    FixStampSnapshotVersion = false,
    DeleteTsRule = "clock"
  ).* from "./MvccPassiveSequence"
}
