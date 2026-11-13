// Configurations of MvccPassiveSchema.qnt. Each module sets the
// constants of one configuration. The comment above a module gives the
// result to expect and the invariants to check. The Makefile reads these
// comments.

// Expect no error: the design. Every special case of the code is on, and every
// Fix* constant applies the fix for one open issue.
// Invariants: TypeOK ReadsCorrect OpenCorrect NoSharedOwner RecoveryCorrect PagesAccounted CookieNotBehind KindsMatch NoLiveDestroy SchemaRowCurrent StampsAtOrAfterBinding GcNeverRunsUnderNewerBinding
module main {
  import MvccPassiveSchema(
    Objs = Set("t1", "t2"),
    Pages = Set(2, 3),
    Readers = Set("r1"),
    MaxTs = 4,
    MaxCheckpoints = 2,
    Indexes = Set(),
    IdxTable = "t1",
    Alters = false,
    GuardMatCheck = true,
    GuardCoversBegin = true,
    GuardCoversEnd = true,
    GuardRetireOnDestroy = true,
    GuardBindingGcLwm = true,
    GuardAllocRetiresStale = true,
    GuardOpenFallback = true,
    GuardBusyOnUnpublished = true,
    GuardGenerationGate = true,
    GuardCookieCheck = true,
    GuardPatchKeepsEnd = true,
    GuardEndedCreated = true,
    GuardFreedOnly = true,
    GuardRefreshRoot = true,
    GuardFrozenMark = true,
    FixDisjointIds = true,
    FixPersistIds = true,
    FixHeaderCookieMax = true,
    FixInnerTxChecks = true,
    FixDdlRebases = true,
    FixAlterPatch = true,
    FixAlterRecovery = true
  ).* from "./MvccPassiveSchema"
}

// Expect no error: the design with ALTER TABLE ADD COLUMN.
// Invariants: TypeOK ReadsCorrect OpenCorrect NoSharedOwner RecoveryCorrect PagesAccounted CookieNotBehind KindsMatch NoLiveDestroy SchemaRowCurrent StampsAtOrAfterBinding GcNeverRunsUnderNewerBinding
module alter {
  import MvccPassiveSchema(
    Objs = Set("t1", "t2"),
    Pages = Set(2, 3),
    Readers = Set("r1"),
    MaxTs = 4,
    MaxCheckpoints = 2,
    Indexes = Set(),
    IdxTable = "t1",
    Alters = true,
    GuardMatCheck = true,
    GuardCoversBegin = true,
    GuardCoversEnd = true,
    GuardRetireOnDestroy = true,
    GuardBindingGcLwm = true,
    GuardAllocRetiresStale = true,
    GuardOpenFallback = true,
    GuardBusyOnUnpublished = true,
    GuardGenerationGate = true,
    GuardCookieCheck = true,
    GuardPatchKeepsEnd = true,
    GuardEndedCreated = true,
    GuardFreedOnly = true,
    GuardRefreshRoot = true,
    GuardFrozenMark = true,
    FixDisjointIds = true,
    FixPersistIds = true,
    FixHeaderCookieMax = true,
    FixInnerTxChecks = true,
    FixDdlRebases = true,
    FixAlterPatch = true,
    FixAlterRecovery = true
  ).* from "./MvccPassiveSchema"
}

// Expect RecoveryCorrect to fail: issue #11501 or #11502. Every fix is off, as
// in the code today.
// Invariants: RecoveryCorrect
module codeToday {
  import MvccPassiveSchema(
    Objs = Set("t1", "t2"),
    Pages = Set(2, 3),
    Readers = Set("r1"),
    MaxTs = 4,
    MaxCheckpoints = 2,
    Indexes = Set(),
    IdxTable = "t1",
    Alters = false,
    GuardMatCheck = true,
    GuardCoversBegin = true,
    GuardCoversEnd = true,
    GuardRetireOnDestroy = true,
    GuardBindingGcLwm = true,
    GuardAllocRetiresStale = true,
    GuardOpenFallback = true,
    GuardBusyOnUnpublished = true,
    GuardGenerationGate = true,
    GuardCookieCheck = true,
    GuardPatchKeepsEnd = true,
    GuardEndedCreated = true,
    GuardFreedOnly = true,
    GuardRefreshRoot = true,
    GuardFrozenMark = true,
    FixDisjointIds = false,
    FixPersistIds = false,
    FixHeaderCookieMax = false,
    FixInnerTxChecks = false,
    FixDdlRebases = false,
    FixAlterPatch = false,
    FixAlterRecovery = false
  ).* from "./MvccPassiveSchema"
}

// Expect no error: the design with one table and one index on it.
// Invariants: TypeOK ReadsCorrect OpenCorrect NoSharedOwner RecoveryCorrect PagesAccounted CookieNotBehind KindsMatch NoLiveDestroy SchemaRowCurrent StampsAtOrAfterBinding GcNeverRunsUnderNewerBinding
module indexes {
  import MvccPassiveSchema(
    Objs = Set("t1", "i1"),
    Pages = Set(2, 3),
    Readers = Set("r1"),
    MaxTs = 5,
    MaxCheckpoints = 2,
    Indexes = Set("i1"),
    IdxTable = "t1",
    Alters = false,
    GuardMatCheck = true,
    GuardCoversBegin = true,
    GuardCoversEnd = true,
    GuardRetireOnDestroy = true,
    GuardBindingGcLwm = true,
    GuardAllocRetiresStale = true,
    GuardOpenFallback = true,
    GuardBusyOnUnpublished = true,
    GuardGenerationGate = true,
    GuardCookieCheck = true,
    GuardPatchKeepsEnd = true,
    GuardEndedCreated = true,
    GuardFreedOnly = true,
    GuardRefreshRoot = true,
    GuardFrozenMark = true,
    FixDisjointIds = true,
    FixPersistIds = true,
    FixHeaderCookieMax = true,
    FixInnerTxChecks = true,
    FixDdlRebases = true,
    FixAlterPatch = true,
    FixAlterRecovery = true
  ).* from "./MvccPassiveSchema"
}

// Expect CookieNotBehind to fail: issue #11499. The publish writes the header
// that it staged before a DDL commit.
// Invariants: CookieNotBehind
module issue11499 {
  import MvccPassiveSchema(
    Objs = Set("t1", "t2"),
    Pages = Set(2, 3),
    Readers = Set("r1"),
    MaxTs = 4,
    MaxCheckpoints = 2,
    Indexes = Set(),
    IdxTable = "t1",
    Alters = false,
    GuardMatCheck = true,
    GuardCoversBegin = true,
    GuardCoversEnd = true,
    GuardRetireOnDestroy = true,
    GuardBindingGcLwm = true,
    GuardAllocRetiresStale = true,
    GuardOpenFallback = true,
    GuardBusyOnUnpublished = true,
    GuardGenerationGate = true,
    GuardCookieCheck = true,
    GuardPatchKeepsEnd = true,
    GuardEndedCreated = true,
    GuardFreedOnly = true,
    GuardRefreshRoot = true,
    GuardFrozenMark = true,
    FixDisjointIds = true,
    FixPersistIds = true,
    FixHeaderCookieMax = false,
    FixInnerTxChecks = true,
    FixDdlRebases = true,
    FixAlterPatch = true,
    FixAlterRecovery = true
  ).* from "./MvccPassiveSchema"
}

// Expect ReadsCorrect to fail: issue #11500. An inner transaction resolves a
// stale compiled root page at a new snapshot, where the page belongs to another
// table.
// Invariants: ReadsCorrect
module issue11500 {
  import MvccPassiveSchema(
    Objs = Set("t1", "t2"),
    Pages = Set(2, 3),
    Readers = Set("r1"),
    MaxTs = 4,
    MaxCheckpoints = 2,
    Indexes = Set(),
    IdxTable = "t1",
    Alters = false,
    GuardMatCheck = true,
    GuardCoversBegin = true,
    GuardCoversEnd = true,
    GuardRetireOnDestroy = true,
    GuardBindingGcLwm = true,
    GuardAllocRetiresStale = true,
    GuardOpenFallback = true,
    GuardBusyOnUnpublished = true,
    GuardGenerationGate = true,
    GuardCookieCheck = true,
    GuardPatchKeepsEnd = true,
    GuardEndedCreated = true,
    GuardFreedOnly = true,
    GuardRefreshRoot = true,
    GuardFrozenMark = true,
    FixDisjointIds = true,
    FixPersistIds = true,
    FixHeaderCookieMax = true,
    FixInnerTxChecks = false,
    FixDdlRebases = true,
    FixAlterPatch = true,
    FixAlterRecovery = true
  ).* from "./MvccPassiveSchema"
}

// Expect RecoveryCorrect to fail: issue #11501. A temporary id -N equals the
// page N of another table, and recovery panics.
// Invariants: RecoveryCorrect
module issue11501 {
  import MvccPassiveSchema(
    Objs = Set("t1", "t2"),
    Pages = Set(2, 3),
    Readers = Set("r1"),
    MaxTs = 4,
    MaxCheckpoints = 2,
    Indexes = Set(),
    IdxTable = "t1",
    Alters = false,
    GuardMatCheck = true,
    GuardCoversBegin = true,
    GuardCoversEnd = true,
    GuardRetireOnDestroy = true,
    GuardBindingGcLwm = true,
    GuardAllocRetiresStale = true,
    GuardOpenFallback = true,
    GuardBusyOnUnpublished = true,
    GuardGenerationGate = true,
    GuardCookieCheck = true,
    GuardPatchKeepsEnd = true,
    GuardEndedCreated = true,
    GuardFreedOnly = true,
    GuardRefreshRoot = true,
    GuardFrozenMark = true,
    FixDisjointIds = false,
    FixPersistIds = true,
    FixHeaderCookieMax = true,
    FixInnerTxChecks = true,
    FixDdlRebases = true,
    FixAlterPatch = true,
    FixAlterRecovery = true
  ).* from "./MvccPassiveSchema"
}

// Expect RecoveryCorrect to fail: issue #11502. A row logged during the first
// checkpoint of its table keeps the temporary id, and recovery does not find
// the table.
// Invariants: RecoveryCorrect
module issue11502 {
  import MvccPassiveSchema(
    Objs = Set("t1", "t2"),
    Pages = Set(2, 3),
    Readers = Set("r1"),
    MaxTs = 4,
    MaxCheckpoints = 2,
    Indexes = Set(),
    IdxTable = "t1",
    Alters = false,
    GuardMatCheck = true,
    GuardCoversBegin = true,
    GuardCoversEnd = true,
    GuardRetireOnDestroy = true,
    GuardBindingGcLwm = true,
    GuardAllocRetiresStale = true,
    GuardOpenFallback = true,
    GuardBusyOnUnpublished = true,
    GuardGenerationGate = true,
    GuardCookieCheck = true,
    GuardPatchKeepsEnd = true,
    GuardEndedCreated = true,
    GuardFreedOnly = true,
    GuardRefreshRoot = true,
    GuardFrozenMark = true,
    FixDisjointIds = true,
    FixPersistIds = false,
    FixHeaderCookieMax = true,
    FixInnerTxChecks = true,
    FixDdlRebases = true,
    FixAlterPatch = true,
    FixAlterRecovery = true
  ).* from "./MvccPassiveSchema"
}

// Expect KindsMatch to fail: issue #7960. A table row logged with its temporary
// id -N lands in the index that got page N.
// Invariants: KindsMatch
module issue7960 {
  import MvccPassiveSchema(
    Objs = Set("t1", "i1"),
    Pages = Set(2, 3),
    Readers = Set("r1"),
    MaxTs = 4,
    MaxCheckpoints = 2,
    Indexes = Set("i1"),
    IdxTable = "t1",
    Alters = false,
    GuardMatCheck = true,
    GuardCoversBegin = true,
    GuardCoversEnd = true,
    GuardRetireOnDestroy = true,
    GuardBindingGcLwm = true,
    GuardAllocRetiresStale = true,
    GuardOpenFallback = true,
    GuardBusyOnUnpublished = true,
    GuardGenerationGate = true,
    GuardCookieCheck = true,
    GuardPatchKeepsEnd = true,
    GuardEndedCreated = true,
    GuardFreedOnly = true,
    GuardRefreshRoot = true,
    GuardFrozenMark = true,
    FixDisjointIds = false,
    FixPersistIds = false,
    FixHeaderCookieMax = true,
    FixInnerTxChecks = true,
    FixDdlRebases = true,
    FixAlterPatch = true,
    FixAlterRecovery = true
  ).* from "./MvccPassiveSchema"
}

// Expect PagesAccounted to fail: issue #8004. A DDL commit installs its schema
// copy and loses the dropped root that a checkpoint publish added.
// Invariants: PagesAccounted
module issue8004 {
  import MvccPassiveSchema(
    Objs = Set("t1", "t2"),
    Pages = Set(2, 3),
    Readers = Set("r1"),
    MaxTs = 4,
    MaxCheckpoints = 2,
    Indexes = Set(),
    IdxTable = "t1",
    Alters = false,
    GuardMatCheck = true,
    GuardCoversBegin = true,
    GuardCoversEnd = true,
    GuardRetireOnDestroy = true,
    GuardBindingGcLwm = true,
    GuardAllocRetiresStale = true,
    GuardOpenFallback = true,
    GuardBusyOnUnpublished = true,
    GuardGenerationGate = true,
    GuardCookieCheck = true,
    GuardPatchKeepsEnd = true,
    GuardEndedCreated = true,
    GuardFreedOnly = true,
    GuardRefreshRoot = true,
    GuardFrozenMark = true,
    FixDisjointIds = true,
    FixPersistIds = true,
    FixHeaderCookieMax = true,
    FixInnerTxChecks = true,
    FixDdlRebases = false,
    FixAlterPatch = true,
    FixAlterRecovery = true
  ).* from "./MvccPassiveSchema"
}

// Expect SchemaRowCurrent to fail: issue #8077. After the publish, the current
// schema row of the altered table still has the temporary id.
// Invariants: SchemaRowCurrent
module issue8077Row {
  import MvccPassiveSchema(
    Objs = Set("t1", "t2"),
    Pages = Set(2, 3),
    Readers = Set("r1"),
    MaxTs = 4,
    MaxCheckpoints = 2,
    Indexes = Set(),
    IdxTable = "t1",
    Alters = true,
    GuardMatCheck = true,
    GuardCoversBegin = true,
    GuardCoversEnd = true,
    GuardRetireOnDestroy = true,
    GuardBindingGcLwm = true,
    GuardAllocRetiresStale = true,
    GuardOpenFallback = true,
    GuardBusyOnUnpublished = true,
    GuardGenerationGate = true,
    GuardCookieCheck = true,
    GuardPatchKeepsEnd = true,
    GuardEndedCreated = true,
    GuardFreedOnly = true,
    GuardRefreshRoot = true,
    GuardFrozenMark = true,
    FixDisjointIds = true,
    FixPersistIds = true,
    FixHeaderCookieMax = true,
    FixInnerTxChecks = true,
    FixDdlRebases = true,
    FixAlterPatch = false,
    FixAlterRecovery = true
  ).* from "./MvccPassiveSchema"
}

// Expect NoLiveDestroy to fail: issue #8077. The publish patches only the old
// version of an altered schema row, and the next checkpoint destroys the live
// B-tree.
// Invariants: NoLiveDestroy
module issue8077 {
  import MvccPassiveSchema(
    Objs = Set("t1", "t2"),
    Pages = Set(2, 3),
    Readers = Set("r1"),
    MaxTs = 4,
    MaxCheckpoints = 2,
    Indexes = Set(),
    IdxTable = "t1",
    Alters = true,
    GuardMatCheck = true,
    GuardCoversBegin = true,
    GuardCoversEnd = true,
    GuardRetireOnDestroy = true,
    GuardBindingGcLwm = true,
    GuardAllocRetiresStale = true,
    GuardOpenFallback = true,
    GuardBusyOnUnpublished = true,
    GuardGenerationGate = true,
    GuardCookieCheck = true,
    GuardPatchKeepsEnd = true,
    GuardEndedCreated = true,
    GuardFreedOnly = true,
    GuardRefreshRoot = true,
    GuardFrozenMark = true,
    FixDisjointIds = true,
    FixPersistIds = true,
    FixHeaderCookieMax = true,
    FixInnerTxChecks = true,
    FixDdlRebases = true,
    FixAlterPatch = false,
    FixAlterRecovery = true
  ).* from "./MvccPassiveSchema"
}

// Expect OpenCorrect to fail: issue #8537. A cursor that opened before the
// publish seeks the negative root. Fixed by 367362427 (GuardRefreshRoot).
// Invariants: OpenCorrect
module issue8537 {
  import MvccPassiveSchema(
    Objs = Set("t1", "t2"),
    Pages = Set(2, 3),
    Readers = Set("r1"),
    MaxTs = 4,
    MaxCheckpoints = 2,
    Indexes = Set(),
    IdxTable = "t1",
    Alters = false,
    GuardMatCheck = true,
    GuardCoversBegin = true,
    GuardCoversEnd = true,
    GuardRetireOnDestroy = true,
    GuardBindingGcLwm = true,
    GuardAllocRetiresStale = true,
    GuardOpenFallback = true,
    GuardBusyOnUnpublished = true,
    GuardGenerationGate = true,
    GuardCookieCheck = true,
    GuardPatchKeepsEnd = true,
    GuardEndedCreated = true,
    GuardFreedOnly = true,
    GuardRefreshRoot = false,
    GuardFrozenMark = true,
    FixDisjointIds = true,
    FixPersistIds = true,
    FixHeaderCookieMax = true,
    FixInnerTxChecks = true,
    FixDdlRebases = true,
    FixAlterPatch = true,
    FixAlterRecovery = true
  ).* from "./MvccPassiveSchema"
}

// Expect no error: record_rootpage_alloc does not retire a stale live owner.
// The destroy already retired it.
// Invariants: TypeOK ReadsCorrect OpenCorrect NoSharedOwner RecoveryCorrect PagesAccounted CookieNotBehind
module noAllocRetiresStale {
  import MvccPassiveSchema(
    Objs = Set("t1", "t2"),
    Pages = Set(2, 3),
    Readers = Set("r1"),
    MaxTs = 4,
    MaxCheckpoints = 2,
    Indexes = Set(),
    IdxTable = "t1",
    Alters = false,
    GuardMatCheck = true,
    GuardCoversBegin = true,
    GuardCoversEnd = true,
    GuardRetireOnDestroy = true,
    GuardBindingGcLwm = true,
    GuardAllocRetiresStale = false,
    GuardOpenFallback = true,
    GuardBusyOnUnpublished = true,
    GuardGenerationGate = true,
    GuardCookieCheck = true,
    GuardPatchKeepsEnd = true,
    GuardEndedCreated = true,
    GuardFreedOnly = true,
    GuardRefreshRoot = true,
    GuardFrozenMark = true,
    FixDisjointIds = true,
    FixPersistIds = true,
    FixHeaderCookieMax = true,
    FixInnerTxChecks = true,
    FixDdlRebases = true,
    FixAlterPatch = true,
    FixAlterRecovery = true
  ).* from "./MvccPassiveSchema"
}

// Expect RecoveryCorrect to fail: an ALTER record with the temporary id above
// the boundary makes recovery panic or lose the table page.
// Invariants: RecoveryCorrect
module noAlterRecovery {
  import MvccPassiveSchema(
    Objs = Set("t1", "t2"),
    Pages = Set(2, 3),
    Readers = Set("r1"),
    MaxTs = 4,
    MaxCheckpoints = 2,
    Indexes = Set(),
    IdxTable = "t1",
    Alters = true,
    GuardMatCheck = true,
    GuardCoversBegin = true,
    GuardCoversEnd = true,
    GuardRetireOnDestroy = true,
    GuardBindingGcLwm = true,
    GuardAllocRetiresStale = true,
    GuardOpenFallback = true,
    GuardBusyOnUnpublished = true,
    GuardGenerationGate = true,
    GuardCookieCheck = true,
    GuardPatchKeepsEnd = true,
    GuardEndedCreated = true,
    GuardFreedOnly = true,
    GuardRefreshRoot = true,
    GuardFrozenMark = true,
    FixDisjointIds = true,
    FixPersistIds = true,
    FixHeaderCookieMax = true,
    FixInnerTxChecks = true,
    FixDdlRebases = true,
    FixAlterPatch = true,
    FixAlterRecovery = false
  ).* from "./MvccPassiveSchema"
}

// Expect ReadsCorrect to fail: gc_rootpage_entries drops a retired binding that
// a reader still needs.
// Invariants: ReadsCorrect
module noBindingGcLwm {
  import MvccPassiveSchema(
    Objs = Set("t1", "t2"),
    Pages = Set(2, 3),
    Readers = Set("r1"),
    MaxTs = 4,
    MaxCheckpoints = 2,
    Indexes = Set(),
    IdxTable = "t1",
    Alters = false,
    GuardMatCheck = true,
    GuardCoversBegin = true,
    GuardCoversEnd = true,
    GuardRetireOnDestroy = true,
    GuardBindingGcLwm = false,
    GuardAllocRetiresStale = true,
    GuardOpenFallback = true,
    GuardBusyOnUnpublished = true,
    GuardGenerationGate = true,
    GuardCookieCheck = true,
    GuardPatchKeepsEnd = true,
    GuardEndedCreated = true,
    GuardFreedOnly = true,
    GuardRefreshRoot = true,
    GuardFrozenMark = true,
    FixDisjointIds = true,
    FixPersistIds = true,
    FixHeaderCookieMax = true,
    FixInnerTxChecks = true,
    FixDdlRebases = true,
    FixAlterPatch = true,
    FixAlterRecovery = true
  ).* from "./MvccPassiveSchema"
}

// Expect ReadsCorrect to fail: begin_tx does not return Busy during an
// unpublished schema change (#9955).
// Invariants: ReadsCorrect
module noBusy {
  import MvccPassiveSchema(
    Objs = Set("t1", "t2"),
    Pages = Set(2, 3),
    Readers = Set("r1"),
    MaxTs = 4,
    MaxCheckpoints = 2,
    Indexes = Set(),
    IdxTable = "t1",
    Alters = false,
    GuardMatCheck = true,
    GuardCoversBegin = true,
    GuardCoversEnd = true,
    GuardRetireOnDestroy = true,
    GuardBindingGcLwm = true,
    GuardAllocRetiresStale = true,
    GuardOpenFallback = true,
    GuardBusyOnUnpublished = false,
    GuardGenerationGate = true,
    GuardCookieCheck = true,
    GuardPatchKeepsEnd = true,
    GuardEndedCreated = true,
    GuardFreedOnly = true,
    GuardRefreshRoot = true,
    GuardFrozenMark = true,
    FixDisjointIds = true,
    FixPersistIds = true,
    FixHeaderCookieMax = true,
    FixInnerTxChecks = true,
    FixDdlRebases = true,
    FixAlterPatch = true,
    FixAlterRecovery = true
  ).* from "./MvccPassiveSchema"
}

// Expect ReadsCorrect to fail: the Transaction opcode does not compare the
// schema cookie.
// Invariants: ReadsCorrect
module noCookieCheck {
  import MvccPassiveSchema(
    Objs = Set("t1", "t2"),
    Pages = Set(2, 3),
    Readers = Set("r1"),
    MaxTs = 4,
    MaxCheckpoints = 2,
    Indexes = Set(),
    IdxTable = "t1",
    Alters = false,
    GuardMatCheck = true,
    GuardCoversBegin = true,
    GuardCoversEnd = true,
    GuardRetireOnDestroy = true,
    GuardBindingGcLwm = true,
    GuardAllocRetiresStale = true,
    GuardOpenFallback = true,
    GuardBusyOnUnpublished = true,
    GuardGenerationGate = true,
    GuardCookieCheck = false,
    GuardPatchKeepsEnd = true,
    GuardEndedCreated = true,
    GuardFreedOnly = true,
    GuardRefreshRoot = true,
    GuardFrozenMark = true,
    FixDisjointIds = true,
    FixPersistIds = true,
    FixHeaderCookieMax = true,
    FixInnerTxChecks = true,
    FixDdlRebases = true,
    FixAlterPatch = true,
    FixAlterRecovery = true
  ).* from "./MvccPassiveSchema"
}

// Expect no error: is_btree_readable_at ignores binding.begin. materialized_at
// <= read_mark already implies it.
// Invariants: TypeOK ReadsCorrect OpenCorrect NoSharedOwner RecoveryCorrect PagesAccounted CookieNotBehind
module noCoversBegin {
  import MvccPassiveSchema(
    Objs = Set("t1", "t2"),
    Pages = Set(2, 3),
    Readers = Set("r1"),
    MaxTs = 4,
    MaxCheckpoints = 2,
    Indexes = Set(),
    IdxTable = "t1",
    Alters = false,
    GuardMatCheck = true,
    GuardCoversBegin = false,
    GuardCoversEnd = true,
    GuardRetireOnDestroy = true,
    GuardBindingGcLwm = true,
    GuardAllocRetiresStale = true,
    GuardOpenFallback = true,
    GuardBusyOnUnpublished = true,
    GuardGenerationGate = true,
    GuardCookieCheck = true,
    GuardPatchKeepsEnd = true,
    GuardEndedCreated = true,
    GuardFreedOnly = true,
    GuardRefreshRoot = true,
    GuardFrozenMark = true,
    FixDisjointIds = true,
    FixPersistIds = true,
    FixHeaderCookieMax = true,
    FixInnerTxChecks = true,
    FixDdlRebases = true,
    FixAlterPatch = true,
    FixAlterRecovery = true
  ).* from "./MvccPassiveSchema"
}

// Expect no error: is_btree_readable_at ignores binding.end. The cookie check
// already stops a reader of a dropped table.
// Invariants: TypeOK ReadsCorrect OpenCorrect NoSharedOwner RecoveryCorrect PagesAccounted CookieNotBehind
module noCoversEnd {
  import MvccPassiveSchema(
    Objs = Set("t1", "t2"),
    Pages = Set(2, 3),
    Readers = Set("r1"),
    MaxTs = 4,
    MaxCheckpoints = 2,
    Indexes = Set(),
    IdxTable = "t1",
    Alters = false,
    GuardMatCheck = true,
    GuardCoversBegin = true,
    GuardCoversEnd = false,
    GuardRetireOnDestroy = true,
    GuardBindingGcLwm = true,
    GuardAllocRetiresStale = true,
    GuardOpenFallback = true,
    GuardBusyOnUnpublished = true,
    GuardGenerationGate = true,
    GuardCookieCheck = true,
    GuardPatchKeepsEnd = true,
    GuardEndedCreated = true,
    GuardFreedOnly = true,
    GuardRefreshRoot = true,
    GuardFrozenMark = true,
    FixDisjointIds = true,
    FixPersistIds = true,
    FixHeaderCookieMax = true,
    FixInnerTxChecks = true,
    FixDdlRebases = true,
    FixAlterPatch = true,
    FixAlterRecovery = true
  ).* from "./MvccPassiveSchema"
}

// Expect PagesAccounted to fail: the publish does not keep a created root whose
// table is already dropped (#7956).
// Invariants: PagesAccounted
module noEndedCreated {
  import MvccPassiveSchema(
    Objs = Set("t1", "t2"),
    Pages = Set(2, 3),
    Readers = Set("r1"),
    MaxTs = 4,
    MaxCheckpoints = 2,
    Indexes = Set(),
    IdxTable = "t1",
    Alters = false,
    GuardMatCheck = true,
    GuardCoversBegin = true,
    GuardCoversEnd = true,
    GuardRetireOnDestroy = true,
    GuardBindingGcLwm = true,
    GuardAllocRetiresStale = true,
    GuardOpenFallback = true,
    GuardBusyOnUnpublished = true,
    GuardGenerationGate = true,
    GuardCookieCheck = true,
    GuardPatchKeepsEnd = true,
    GuardEndedCreated = false,
    GuardFreedOnly = true,
    GuardRefreshRoot = true,
    GuardFrozenMark = true,
    FixDisjointIds = true,
    FixPersistIds = true,
    FixHeaderCookieMax = true,
    FixInnerTxChecks = true,
    FixDdlRebases = true,
    FixAlterPatch = true,
    FixAlterRecovery = true
  ).* from "./MvccPassiveSchema"
}

// Expect PagesAccounted to fail: the publish clears every dropped root, also
// one that a later DROP added (#7957).
// Invariants: PagesAccounted
module noFreedOnly {
  import MvccPassiveSchema(
    Objs = Set("t1", "t2"),
    Pages = Set(2, 3),
    Readers = Set("r1"),
    MaxTs = 4,
    MaxCheckpoints = 2,
    Indexes = Set(),
    IdxTable = "t1",
    Alters = false,
    GuardMatCheck = true,
    GuardCoversBegin = true,
    GuardCoversEnd = true,
    GuardRetireOnDestroy = true,
    GuardBindingGcLwm = true,
    GuardAllocRetiresStale = true,
    GuardOpenFallback = true,
    GuardBusyOnUnpublished = true,
    GuardGenerationGate = true,
    GuardCookieCheck = true,
    GuardPatchKeepsEnd = true,
    GuardEndedCreated = true,
    GuardFreedOnly = false,
    GuardRefreshRoot = true,
    GuardFrozenMark = true,
    FixDisjointIds = true,
    FixPersistIds = true,
    FixHeaderCookieMax = true,
    FixInnerTxChecks = true,
    FixDdlRebases = true,
    FixAlterPatch = true,
    FixAlterRecovery = true
  ).* from "./MvccPassiveSchema"
}

// Expect ReadsCorrect to fail: a statement of a transaction reads pages from a
// newer WAL frame than its begin pinned (the symptom of #8087, #8091, #8092).
// Invariants: ReadsCorrect
module noFrozenMark {
  import MvccPassiveSchema(
    Objs = Set("t1", "t2"),
    Pages = Set(2, 3),
    Readers = Set("r1"),
    MaxTs = 4,
    MaxCheckpoints = 2,
    Indexes = Set(),
    IdxTable = "t1",
    Alters = false,
    GuardMatCheck = true,
    GuardCoversBegin = true,
    GuardCoversEnd = true,
    GuardRetireOnDestroy = true,
    GuardBindingGcLwm = true,
    GuardAllocRetiresStale = true,
    GuardOpenFallback = true,
    GuardBusyOnUnpublished = true,
    GuardGenerationGate = true,
    GuardCookieCheck = true,
    GuardPatchKeepsEnd = true,
    GuardEndedCreated = true,
    GuardFreedOnly = true,
    GuardRefreshRoot = true,
    GuardFrozenMark = false,
    FixDisjointIds = true,
    FixPersistIds = true,
    FixHeaderCookieMax = true,
    FixInnerTxChecks = true,
    FixDdlRebases = true,
    FixAlterPatch = true,
    FixAlterRecovery = true
  ).* from "./MvccPassiveSchema"
}

// Expect no error: begin_tx ignores schema_generation. A cursor resolves a
// negative root through its binding.
// Invariants: TypeOK ReadsCorrect OpenCorrect NoSharedOwner RecoveryCorrect PagesAccounted CookieNotBehind
module noGenerationGate {
  import MvccPassiveSchema(
    Objs = Set("t1", "t2"),
    Pages = Set(2, 3),
    Readers = Set("r1"),
    MaxTs = 4,
    MaxCheckpoints = 2,
    Indexes = Set(),
    IdxTable = "t1",
    Alters = false,
    GuardMatCheck = true,
    GuardCoversBegin = true,
    GuardCoversEnd = true,
    GuardRetireOnDestroy = true,
    GuardBindingGcLwm = true,
    GuardAllocRetiresStale = true,
    GuardOpenFallback = true,
    GuardBusyOnUnpublished = true,
    GuardGenerationGate = false,
    GuardCookieCheck = true,
    GuardPatchKeepsEnd = true,
    GuardEndedCreated = true,
    GuardFreedOnly = true,
    GuardRefreshRoot = true,
    GuardFrozenMark = true,
    FixDisjointIds = true,
    FixPersistIds = true,
    FixHeaderCookieMax = true,
    FixInnerTxChecks = true,
    FixDdlRebases = true,
    FixAlterPatch = true,
    FixAlterRecovery = true
  ).* from "./MvccPassiveSchema"
}

// Expect ReadsCorrect to fail: is_btree_readable_at ignores materialized_at.
// Invariants: ReadsCorrect
module noMatCheck {
  import MvccPassiveSchema(
    Objs = Set("t1", "t2"),
    Pages = Set(2, 3),
    Readers = Set("r1"),
    MaxTs = 4,
    MaxCheckpoints = 2,
    Indexes = Set(),
    IdxTable = "t1",
    Alters = false,
    GuardMatCheck = false,
    GuardCoversBegin = true,
    GuardCoversEnd = true,
    GuardRetireOnDestroy = true,
    GuardBindingGcLwm = true,
    GuardAllocRetiresStale = true,
    GuardOpenFallback = true,
    GuardBusyOnUnpublished = true,
    GuardGenerationGate = true,
    GuardCookieCheck = true,
    GuardPatchKeepsEnd = true,
    GuardEndedCreated = true,
    GuardFreedOnly = true,
    GuardRefreshRoot = true,
    GuardFrozenMark = true,
    FixDisjointIds = true,
    FixPersistIds = true,
    FixHeaderCookieMax = true,
    FixInnerTxChecks = true,
    FixDdlRebases = true,
    FixAlterPatch = true,
    FixAlterRecovery = true
  ).* from "./MvccPassiveSchema"
}

// Expect no error: MvccLazyCursor::new panics on an unmapped root page. The
// Busy and cookie checks already prevent it.
// Invariants: TypeOK ReadsCorrect OpenCorrect NoSharedOwner RecoveryCorrect PagesAccounted CookieNotBehind
module noOpenFallback {
  import MvccPassiveSchema(
    Objs = Set("t1", "t2"),
    Pages = Set(2, 3),
    Readers = Set("r1"),
    MaxTs = 4,
    MaxCheckpoints = 2,
    Indexes = Set(),
    IdxTable = "t1",
    Alters = false,
    GuardMatCheck = true,
    GuardCoversBegin = true,
    GuardCoversEnd = true,
    GuardRetireOnDestroy = true,
    GuardBindingGcLwm = true,
    GuardAllocRetiresStale = true,
    GuardOpenFallback = false,
    GuardBusyOnUnpublished = true,
    GuardGenerationGate = true,
    GuardCookieCheck = true,
    GuardPatchKeepsEnd = true,
    GuardEndedCreated = true,
    GuardFreedOnly = true,
    GuardRefreshRoot = true,
    GuardFrozenMark = true,
    FixDisjointIds = true,
    FixPersistIds = true,
    FixHeaderCookieMax = true,
    FixInnerTxChecks = true,
    FixDdlRebases = true,
    FixAlterPatch = true,
    FixAlterRecovery = true
  ).* from "./MvccPassiveSchema"
}

// Expect RecoveryCorrect to fail: the publish replaces the sqlite_schema row
// and drops a concurrent DROP's end (#7956).
// Invariants: RecoveryCorrect
module noPatchKeepsEnd {
  import MvccPassiveSchema(
    Objs = Set("t1", "t2"),
    Pages = Set(2, 3),
    Readers = Set("r1"),
    MaxTs = 4,
    MaxCheckpoints = 2,
    Indexes = Set(),
    IdxTable = "t1",
    Alters = false,
    GuardMatCheck = true,
    GuardCoversBegin = true,
    GuardCoversEnd = true,
    GuardRetireOnDestroy = true,
    GuardBindingGcLwm = true,
    GuardAllocRetiresStale = true,
    GuardOpenFallback = true,
    GuardBusyOnUnpublished = true,
    GuardGenerationGate = true,
    GuardCookieCheck = true,
    GuardPatchKeepsEnd = false,
    GuardEndedCreated = true,
    GuardFreedOnly = true,
    GuardRefreshRoot = true,
    GuardFrozenMark = true,
    FixDisjointIds = true,
    FixPersistIds = true,
    FixHeaderCookieMax = true,
    FixInnerTxChecks = true,
    FixDdlRebases = true,
    FixAlterPatch = true,
    FixAlterRecovery = true
  ).* from "./MvccPassiveSchema"
}

// Expect ReadsCorrect to fail: BTreeDestroy removes the binding instead of
// retiring it.
// Invariants: ReadsCorrect
module noRetire {
  import MvccPassiveSchema(
    Objs = Set("t1", "t2"),
    Pages = Set(2, 3),
    Readers = Set("r1"),
    MaxTs = 4,
    MaxCheckpoints = 2,
    Indexes = Set(),
    IdxTable = "t1",
    Alters = false,
    GuardMatCheck = true,
    GuardCoversBegin = true,
    GuardCoversEnd = true,
    GuardRetireOnDestroy = false,
    GuardBindingGcLwm = true,
    GuardAllocRetiresStale = true,
    GuardOpenFallback = true,
    GuardBusyOnUnpublished = true,
    GuardGenerationGate = true,
    GuardCookieCheck = true,
    GuardPatchKeepsEnd = true,
    GuardEndedCreated = true,
    GuardFreedOnly = true,
    GuardRefreshRoot = true,
    GuardFrozenMark = true,
    FixDisjointIds = true,
    FixPersistIds = true,
    FixHeaderCookieMax = true,
    FixInnerTxChecks = true,
    FixDdlRebases = true,
    FixAlterPatch = true,
    FixAlterRecovery = true
  ).* from "./MvccPassiveSchema"
}

// Expect NeverBusy to fail: a reader gets Busy during a schema change.
// Invariants: NeverBusy
module reachBusy {
  import MvccPassiveSchema(
    Objs = Set("t1", "t2"),
    Pages = Set(2, 3),
    Readers = Set("r1"),
    MaxTs = 4,
    MaxCheckpoints = 2,
    Indexes = Set(),
    IdxTable = "t1",
    Alters = false,
    GuardMatCheck = true,
    GuardCoversBegin = true,
    GuardCoversEnd = true,
    GuardRetireOnDestroy = true,
    GuardBindingGcLwm = true,
    GuardAllocRetiresStale = true,
    GuardOpenFallback = true,
    GuardBusyOnUnpublished = true,
    GuardGenerationGate = true,
    GuardCookieCheck = true,
    GuardPatchKeepsEnd = true,
    GuardEndedCreated = true,
    GuardFreedOnly = true,
    GuardRefreshRoot = true,
    GuardFrozenMark = true,
    FixDisjointIds = true,
    FixPersistIds = true,
    FixHeaderCookieMax = true,
    FixInnerTxChecks = true,
    FixDdlRebases = true,
    FixAlterPatch = true,
    FixAlterRecovery = true
  ).* from "./MvccPassiveSchema"
}

// Expect NeverReadsOldOwner to fail: a reader reads a reused page's old owner
// through an old read mark.
// Invariants: NeverReadsOldOwner
module reachOldOwner {
  import MvccPassiveSchema(
    Objs = Set("t1", "t2"),
    Pages = Set(2, 3),
    Readers = Set("r1"),
    MaxTs = 4,
    MaxCheckpoints = 2,
    Indexes = Set(),
    IdxTable = "t1",
    Alters = false,
    GuardMatCheck = true,
    GuardCoversBegin = true,
    GuardCoversEnd = true,
    GuardRetireOnDestroy = true,
    GuardBindingGcLwm = true,
    GuardAllocRetiresStale = true,
    GuardOpenFallback = true,
    GuardBusyOnUnpublished = true,
    GuardGenerationGate = true,
    GuardCookieCheck = true,
    GuardPatchKeepsEnd = true,
    GuardEndedCreated = true,
    GuardFreedOnly = true,
    GuardRefreshRoot = true,
    GuardFrozenMark = true,
    FixDisjointIds = true,
    FixPersistIds = true,
    FixHeaderCookieMax = true,
    FixInnerTxChecks = true,
    FixDdlRebases = true,
    FixAlterPatch = true,
    FixAlterRecovery = true
  ).* from "./MvccPassiveSchema"
}

// Expect NeverOwnedNegative to fail: with the code's DDL commit, a negative
// schema root with a root page outlives the publish. The old global TruncateWal
// assert fired here.
// Invariants: NeverOwnedNegative
module reachOwnedNegative {
  import MvccPassiveSchema(
    Objs = Set("t1", "t2"),
    Pages = Set(2, 3),
    Readers = Set("r1"),
    MaxTs = 4,
    MaxCheckpoints = 2,
    Indexes = Set(),
    IdxTable = "t1",
    Alters = false,
    GuardMatCheck = true,
    GuardCoversBegin = true,
    GuardCoversEnd = true,
    GuardRetireOnDestroy = true,
    GuardBindingGcLwm = true,
    GuardAllocRetiresStale = true,
    GuardOpenFallback = true,
    GuardBusyOnUnpublished = true,
    GuardGenerationGate = true,
    GuardCookieCheck = true,
    GuardPatchKeepsEnd = true,
    GuardEndedCreated = true,
    GuardFreedOnly = true,
    GuardRefreshRoot = true,
    GuardFrozenMark = true,
    FixDisjointIds = true,
    FixPersistIds = true,
    FixHeaderCookieMax = true,
    FixInnerTxChecks = true,
    FixDdlRebases = false,
    FixAlterPatch = true,
    FixAlterRecovery = true
  ).* from "./MvccPassiveSchema"
}

// Expect NeverReplaysRow to fail: recovery replays a row record above a
// checkpoint boundary.
// Invariants: NeverReplaysRow
module reachReplaysRow {
  import MvccPassiveSchema(
    Objs = Set("t1", "t2"),
    Pages = Set(2, 3),
    Readers = Set("r1"),
    MaxTs = 4,
    MaxCheckpoints = 2,
    Indexes = Set(),
    IdxTable = "t1",
    Alters = false,
    GuardMatCheck = true,
    GuardCoversBegin = true,
    GuardCoversEnd = true,
    GuardRetireOnDestroy = true,
    GuardBindingGcLwm = true,
    GuardAllocRetiresStale = true,
    GuardOpenFallback = true,
    GuardBusyOnUnpublished = true,
    GuardGenerationGate = true,
    GuardCookieCheck = true,
    GuardPatchKeepsEnd = true,
    GuardEndedCreated = true,
    GuardFreedOnly = true,
    GuardRefreshRoot = true,
    GuardFrozenMark = true,
    FixDisjointIds = true,
    FixPersistIds = true,
    FixHeaderCookieMax = true,
    FixInnerTxChecks = true,
    FixDdlRebases = true,
    FixAlterPatch = true,
    FixAlterRecovery = true
  ).* from "./MvccPassiveSchema"
}

// Expect NeverReusesPage to fail: a freed page becomes the root of another
// table.
// Invariants: NeverReusesPage
module reachReuse {
  import MvccPassiveSchema(
    Objs = Set("t1", "t2"),
    Pages = Set(2, 3),
    Readers = Set("r1"),
    MaxTs = 4,
    MaxCheckpoints = 2,
    Indexes = Set(),
    IdxTable = "t1",
    Alters = false,
    GuardMatCheck = true,
    GuardCoversBegin = true,
    GuardCoversEnd = true,
    GuardRetireOnDestroy = true,
    GuardBindingGcLwm = true,
    GuardAllocRetiresStale = true,
    GuardOpenFallback = true,
    GuardBusyOnUnpublished = true,
    GuardGenerationGate = true,
    GuardCookieCheck = true,
    GuardPatchKeepsEnd = true,
    GuardEndedCreated = true,
    GuardFreedOnly = true,
    GuardRefreshRoot = true,
    GuardFrozenMark = true,
    FixDisjointIds = true,
    FixPersistIds = true,
    FixHeaderCookieMax = true,
    FixInnerTxChecks = true,
    FixDdlRebases = true,
    FixAlterPatch = true,
    FixAlterRecovery = true
  ).* from "./MvccPassiveSchema"
}

// Expect NeverSchemaUpdatedCookie to fail: a reader gets SchemaUpdated from the
// cookie check.
// Invariants: NeverSchemaUpdatedCookie
module reachSchemaUpdatedCookie {
  import MvccPassiveSchema(
    Objs = Set("t1", "t2"),
    Pages = Set(2, 3),
    Readers = Set("r1"),
    MaxTs = 4,
    MaxCheckpoints = 2,
    Indexes = Set(),
    IdxTable = "t1",
    Alters = false,
    GuardMatCheck = true,
    GuardCoversBegin = true,
    GuardCoversEnd = true,
    GuardRetireOnDestroy = true,
    GuardBindingGcLwm = true,
    GuardAllocRetiresStale = true,
    GuardOpenFallback = true,
    GuardBusyOnUnpublished = true,
    GuardGenerationGate = true,
    GuardCookieCheck = true,
    GuardPatchKeepsEnd = true,
    GuardEndedCreated = true,
    GuardFreedOnly = true,
    GuardRefreshRoot = true,
    GuardFrozenMark = true,
    FixDisjointIds = true,
    FixPersistIds = true,
    FixHeaderCookieMax = true,
    FixInnerTxChecks = true,
    FixDdlRebases = true,
    FixAlterPatch = true,
    FixAlterRecovery = true
  ).* from "./MvccPassiveSchema"
}

// Expect NeverSchemaUpdatedGen to fail: a reader gets SchemaUpdated from the
// generation gate.
// Invariants: NeverSchemaUpdatedGen
module reachSchemaUpdatedGen {
  import MvccPassiveSchema(
    Objs = Set("t1", "t2"),
    Pages = Set(2, 3),
    Readers = Set("r1"),
    MaxTs = 4,
    MaxCheckpoints = 2,
    Indexes = Set(),
    IdxTable = "t1",
    Alters = false,
    GuardMatCheck = true,
    GuardCoversBegin = true,
    GuardCoversEnd = true,
    GuardRetireOnDestroy = true,
    GuardBindingGcLwm = true,
    GuardAllocRetiresStale = true,
    GuardOpenFallback = true,
    GuardBusyOnUnpublished = true,
    GuardGenerationGate = true,
    GuardCookieCheck = true,
    GuardPatchKeepsEnd = true,
    GuardEndedCreated = true,
    GuardFreedOnly = true,
    GuardRefreshRoot = true,
    GuardFrozenMark = true,
    FixDisjointIds = true,
    FixPersistIds = true,
    FixHeaderCookieMax = true,
    FixInnerTxChecks = true,
    FixDdlRebases = true,
    FixAlterPatch = true,
    FixAlterRecovery = true
  ).* from "./MvccPassiveSchema"
}

// Expect NeverRootGuardFires to fail: an object with versions has a binding
// above the GC floor.
// Invariants: NeverRootGuardFires
module reachRootGuardFires {
  import MvccPassiveSchema(
    Objs = Set("t1", "t2"),
    Pages = Set(2, 3),
    Readers = Set("r1"),
    MaxTs = 4,
    MaxCheckpoints = 2,
    Indexes = Set(),
    IdxTable = "t1",
    Alters = false,
    GuardMatCheck = true,
    GuardCoversBegin = true,
    GuardCoversEnd = true,
    GuardRetireOnDestroy = true,
    GuardBindingGcLwm = true,
    GuardAllocRetiresStale = true,
    GuardOpenFallback = true,
    GuardBusyOnUnpublished = true,
    GuardGenerationGate = true,
    GuardCookieCheck = true,
    GuardPatchKeepsEnd = true,
    GuardEndedCreated = true,
    GuardFreedOnly = true,
    GuardRefreshRoot = true,
    GuardFrozenMark = true,
    FixDisjointIds = true,
    FixPersistIds = true,
    FixHeaderCookieMax = true,
    FixInnerTxChecks = true,
    FixDdlRebases = true,
    FixAlterPatch = true,
    FixAlterRecovery = true
  ).* from "./MvccPassiveSchema"
}
