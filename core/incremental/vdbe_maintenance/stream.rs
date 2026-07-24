use super::*;

#[derive(Debug, Clone)]
pub(super) struct EphemeralDelta {
    pub(super) cursor_id: usize,
    pub(super) identity: DeltaIdentity,
    /// Physical metadata column carrying each logical binding's SQL rowid.
    /// `None` means this node intentionally does not expose rowid provenance
    /// for that namespace.
    pub(super) binding_rowid_columns: Arc<[Option<usize>]>,
    pub(super) value_start: usize,
    pub(super) width: usize,
    pub(super) weight_column: usize,
    pub(super) schema: Arc<dag::StreamSchema>,
    /// Applying negative join contributions before positive ones can
    /// transiently retract an as-yet-unknown group. Consumers that merge into
    /// state must process this stream positive-first.
    pub(super) requires_positive_first: bool,
}

/// Stable transport identity carried ahead of an ephemeral delta's relational
/// values. Encoding the identity kind as a sum type prevents consumers from
/// treating an operator-owned state rowid as a SQL binding rowid merely
/// because both occupy one physical slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DeltaIdentity {
    /// One SQL rowid per logical binding, in binding order.
    BindingRowids(usize),
    /// One opaque rowid owned by the emitting stateful operator.
    OperatorRowid,
    /// An opaque, fixed-width key owned by an operator. Consumers may carry
    /// it through linear edges or use it to build an arrangement, but must
    /// not interpret its slots as SQL binding rowids.
    OperatorKey(usize),
}

impl EphemeralDelta {
    pub(super) fn identity_width(&self) -> usize {
        self.identity.width()
    }

    pub(super) fn record_width(&self) -> usize {
        self.weight_column + 1
    }
}

impl DeltaIdentity {
    pub(super) fn width(self) -> usize {
        match self {
            Self::BindingRowids(width) => width,
            Self::OperatorRowid => 1,
            Self::OperatorKey(width) => width,
        }
    }
}

pub(super) fn binding_rowid_metadata_width(
    identity: DeltaIdentity,
    binding_rowids: &[bool],
) -> usize {
    if let DeltaIdentity::BindingRowids(width) = identity {
        turso_assert!(
            width == binding_rowids.len() && binding_rowids.iter().all(|available| *available),
            "binding-rowid transport identity must cover every logical binding"
        );
        0
    } else {
        binding_rowids
            .iter()
            .filter(|available| **available)
            .count()
    }
}

/// A maintained integral a stateful operator may probe.
///
/// Base tables, aggregate state, and explicit operator-output arrangements
/// share one logical contract: open one storage relation, map logical values
/// to physical columns, and optionally expose a signed multiplicity.
#[derive(Debug, Clone)]
pub(super) struct ArrangementHandle {
    table: Arc<BTreeTable>,
    identity: DeltaIdentity,
    identity_columns: Arc<[ArrangementIdentityColumn]>,
    /// Physical arrangement locations for the SQL rowid of each logical
    /// binding, independent of the arrangement's transport identity.
    binding_rowid_columns: Arc<[Option<ArrangementIdentityColumn>]>,
    /// Physical table columns corresponding to the operator's logical output
    /// schema. Base arrangements are identity-mapped; aggregate state skips
    /// accumulator payload columns and explicit output arrangements skip
    /// their source-identity prefix.
    value_columns: Arc<[usize]>,
    /// `None` when each physical row has multiplicity one.
    count_column: Option<usize>,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum ArrangementIdentityColumn {
    RowId,
    Column(usize),
}

impl ArrangementHandle {
    pub(super) fn table(&self) -> &Arc<BTreeTable> {
        &self.table
    }

    pub(super) fn value_columns(&self) -> &[usize] {
        &self.value_columns
    }

    pub(super) fn identity(&self) -> DeltaIdentity {
        self.identity
    }

    pub(super) fn identity_columns(&self) -> &[ArrangementIdentityColumn] {
        &self.identity_columns
    }

    pub(super) fn binding_rowid_columns(&self) -> &[Option<ArrangementIdentityColumn>] {
        &self.binding_rowid_columns
    }

    pub(super) fn count_column(&self) -> Option<usize> {
        self.count_column
    }
}

pub(super) fn btree_arrangement(
    table: Arc<BTreeTable>,
    identity: DeltaIdentity,
    identity_columns: Vec<ArrangementIdentityColumn>,
    binding_rowid_columns: Vec<Option<ArrangementIdentityColumn>>,
    value_columns: Vec<usize>,
    count_column: Option<usize>,
) -> ArrangementHandle {
    turso_assert!(
        identity.width() == identity_columns.len(),
        "arrangement identity layout must match its identity contract"
    );
    ArrangementHandle {
        table,
        identity,
        identity_columns: identity_columns.into(),
        binding_rowid_columns: binding_rowid_columns.into(),
        value_columns: value_columns.into(),
        count_column,
    }
}

pub(super) fn base_arrangement(
    table: Arc<BTreeTable>,
    count_column: Option<usize>,
) -> ArrangementHandle {
    let value_columns = (0..table.columns().len()).collect();
    btree_arrangement(
        table,
        DeltaIdentity::BindingRowids(1),
        vec![ArrangementIdentityColumn::RowId],
        vec![Some(ArrangementIdentityColumn::RowId)],
        value_columns,
        count_column,
    )
}

/// Persistent materialized-view table at the root of a maintenance circuit.
pub(super) struct ViewSink {
    pub(super) root_page: i64,
    pub(super) num_columns: usize,
}

pub(super) fn open_ephemeral_delta(
    program: &mut ProgramBuilder,
    name: &str,
    schema: dag::StreamSchema,
    identity: DeltaIdentity,
    binding_rowids: Arc<[bool]>,
    requires_positive_first: bool,
) -> EphemeralDelta {
    let schema = Arc::new(schema);
    let width = schema.len();
    let identity_width = identity.width();
    turso_assert!(
        binding_rowids.len() == schema.bindings.len(),
        "delta rowid provenance must be parallel to its logical bindings"
    );
    let binding_rowid_metadata_width =
        binding_rowid_metadata_width(identity, binding_rowids.as_ref());
    let mut next_metadata_column = identity_width;
    let binding_rowid_columns: Arc<[Option<usize>]> = if binding_rowid_metadata_width == 0
        && matches!(identity, DeltaIdentity::BindingRowids(_))
    {
        (0..binding_rowids.len())
            .map(Some)
            .collect::<Vec<_>>()
            .into()
    } else {
        binding_rowids
            .iter()
            .map(|available| {
                available.then(|| {
                    let column = next_metadata_column;
                    next_metadata_column += 1;
                    column
                })
            })
            .collect::<Vec<_>>()
            .into()
    };
    turso_assert!(
        next_metadata_column == identity_width + binding_rowid_metadata_width,
        "ephemeral rowid metadata layout must match its planned width"
    );
    let table = Arc::new(synthesized_view_table(
        name,
        0,
        next_metadata_column + width,
    ));
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table));
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id,
        is_table: true,
    });
    EphemeralDelta {
        cursor_id,
        identity,
        binding_rowid_columns,
        value_start: next_metadata_column,
        width,
        weight_column: next_metadata_column + width,
        schema,
        requires_positive_first,
    }
}
