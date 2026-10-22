//! VDBE bytecode maintenance for materialized views.
//!
//! This is the execution half of the IVM engine rewrite: the DBSP circuit
//! remains the logical description of a view, but all row-level evaluation is
//! compiled to a VDBE program so that view maintenance shares the exact
//! expression, comparison, affinity, and NULL semantics of regular query
//! execution instead of re-implementing them.
//!
//! A maintenance program emits each node of the view's operator DAG once, in
//! topological order. Every node reads its inputs' deltas (rows of identity,
//! values and a signed weight) and writes its own delta to an ephemeral
//! table. The program ends by merging the root's delta into the view's btree,
//! whose records are the row values followed by the row's weight.
//!
//! A delta is an unordered z-set. Before a delta is merged into any state
//! (the view, aggregate state, set-operation counts, or an arrangement), it
//! is consolidated: equal rows are summed and rows whose weights cancel are
//! dropped (see `consolidate_delta`). So no merge depends on the order in which
//! the transaction captured its changes.
//!
//! Programs are built with [`ProgramBuilder::new_for_subprogram`], so they
//! emit no transaction instructions and their `Halt` does not commit: they
//! always run inside an already-open write transaction — at statement
//! completion (or before an in-statement view read), or inside the CREATE
//! MATERIALIZED VIEW statement for initial population.

use rustc_hash::FxHashMap;
use turso_parser::ast;

use crate::schema::Schema;
use crate::sync::Arc;
use crate::translate::semantic::context::SemanticContext;
use crate::vdbe::{PreparedProgram, Program};
use crate::{Connection, LimboError, Result};

// The maintenance planner and code generators were written against the
// deleted legacy planner. They are being rebuilt over HIR; until then they
// do not compile and every entry point below reports that.
#[cfg(any())]
mod aggregate;
#[cfg(any())]
mod arrangement;
#[cfg(any())]
mod binding;
#[cfg(any())]
mod consolidate;
#[cfg(any())]
mod join;
#[cfg(any())]
mod linear;
#[cfg(any())]
mod members;
#[cfg(any())]
mod output;
#[cfg(any())]
mod plan;
#[cfg(any())]
mod set_op;
#[cfg(any())]
mod sink;
#[cfg(any())]
mod source;
#[cfg(any())]
mod stream;

mod delta_cursor;
pub use delta_cursor::DeltaCursor;

fn not_ported<T>() -> Result<T> {
    Err(LimboError::ParseError(
        "materialized views are not available until their maintenance is rebuilt over the HIR planner"
            .to_string(),
    ))
}

/// A hidden table that stores one operator's state.
#[derive(Debug, Clone)]
pub(crate) struct HiddenTableDef {
    pub(crate) table_name: String,
    pub(crate) create_sql: String,
    pub(crate) primary_key_index: bool,
}

/// The validated maintenance plan of a view: its operator graph and the
/// storage derived from it.
pub(crate) struct MaintenancePlan {
    hidden_tables: Vec<HiddenTableDef>,
    output_arity: usize,
}

impl MaintenancePlan {
    pub(crate) fn output_arity(&self) -> usize {
        self.output_arity
    }

    pub(crate) fn hidden_tables(&self) -> impl Iterator<Item = &HiddenTableDef> {
        self.hidden_tables.iter()
    }
}

#[cfg(feature = "test_helper")]
impl Connection {
    /// For each column of materialized view `view_name`: the name of the
    /// collation under which maintenance chose the value the column shows
    /// among several values that compare equal, or `None` when the value is
    /// exact.
    pub fn materialized_view_chosen_value_collations(
        self: &Arc<Self>,
        _view_name: &str,
    ) -> Result<Vec<Option<String>>> {
        not_ported()
    }
}

/// Validate a view and build its executable maintenance plan.
pub(crate) fn plan_view(
    _view_name: &str,
    _select: &ast::Select,
    _catalog: &SemanticContext<'_>,
    _connection: &Arc<Connection>,
) -> Result<MaintenancePlan> {
    not_ported()
}

/// What the maintenance program reads as its input relation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum MaintenanceInput {
    /// The transaction's captured delta for the view's base table:
    /// rows are (rowid, base column values..., weight).
    TransactionDelta,
    /// A full scan of the base table with an implicit weight of +1 per row.
    /// Used for initial population at CREATE MATERIALIZED VIEW time.
    BaseTable,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MaintenanceProgramCacheKey {
    view_name: String,
    schema_version: u32,
    root_page: i64,
    num_columns: usize,
    input: MaintenanceInput,
}

/// Per-connection prepared maintenance programs, invalidated by the schema
/// cookie and normal prepared-statement context generation.
pub(crate) struct MaintenanceProgramCache {
    entries: crate::sync::RwLock<FxHashMap<MaintenanceProgramCacheKey, Arc<PreparedProgram>>>,
}

impl MaintenanceProgramCache {
    pub(crate) fn new() -> Self {
        Self {
            entries: crate::sync::RwLock::new(FxHashMap::default()),
        }
    }

    fn get(
        &self,
        key: &MaintenanceProgramCacheKey,
        connection: &Connection,
    ) -> Option<Arc<PreparedProgram>> {
        self.entries
            .read()
            .get(key)
            .filter(|program| program.is_compatible_with(connection))
            .cloned()
    }

    /// Keeps a compiled program; used once maintenance compiles again.
    #[allow(dead_code)]
    fn insert(&self, key: MaintenanceProgramCacheKey, program: Arc<PreparedProgram>) {
        let mut entries = self.entries.write();
        entries.retain(|existing, _| existing.schema_version == key.schema_version);
        entries.insert(key, program);
    }
}

pub(crate) fn compile_maintenance_program(
    view_name: &str,
    _select: &ast::Select,
    view_root_page: i64,
    num_view_columns: usize,
    input: MaintenanceInput,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<Program> {
    let key = MaintenanceProgramCacheKey {
        view_name: view_name.to_string(),
        schema_version: schema.schema_version,
        root_page: view_root_page,
        num_columns: num_view_columns,
        input,
    };
    if let Some(prepared) = connection.maintenance_program_cache.get(&key, connection) {
        return Ok(prepared.bind(connection.clone()));
    }
    not_ported()
}
