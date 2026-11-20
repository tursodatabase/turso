//! Reading a schema from the database file: a list of steps, and one driver
//! that runs the statements of each step.

use crate::connection::{Connection, TransactionState};
use crate::namespace::NamespaceId;
use crate::schema::{
    Schema, Sequence, SequencesByNamespace, StoredMaterializedView, TURSO_NAMESPACES_TABLE_NAME,
    TURSO_TYPES_TABLE_NAME,
};
use crate::statement::StatementOrigin;
use crate::sync::Arc;
use crate::types::{IOResult, IOResultOr};
use crate::util::UnparsedFromSqlIndex;
use crate::vdbe::Row;
use crate::{turso_assert, LimboError, Result, Statement, Value};
use rustc_hash::FxHashMap as HashMap;
use std::collections::VecDeque;

/// One part of reading a schema from the database file. A step runs
/// statements against the schema read so far and puts their rows into it.
pub(crate) trait LoadStep: Send {
    /// The next statement of this step, or None when the step has no more.
    /// A step without statements does its work here.
    fn next_query(&mut self, load: &mut SchemaLoad) -> Result<Option<LoadQuery>>;

    /// One row of the statement that `next_query` returned last.
    fn row(&mut self, load: &mut SchemaLoad, row: &Row) -> Result<()>;

    /// Called after the last row of each statement.
    fn end_of_rows(&mut self, load: &mut SchemaLoad) -> Result<()> {
        let _ = load;
        Ok(())
    }

    /// The error to report when the statement that `next_query` returned
    /// last fails with `err`.
    fn query_failed(&self, failure: QueryFailure, err: LimboError) -> LimboError {
        let _ = failure;
        err
    }
}

/// A statement of a [`LoadStep`].
pub(crate) enum LoadQuery {
    /// `SELECT * FROM sqlite_schema`, as
    /// [`Connection::prepare_schema_table_read`] prepares it without parsing
    /// SQL.
    SchemaTableRead,
    Sql {
        sql: String,
        origin: StatementOrigin,
    },
}

impl LoadQuery {
    /// A read of an engine table, as
    /// [`Connection::prepare_engine_metadata_read`] prepares it.
    pub(crate) fn engine_metadata_read(sql: impl Into<String>) -> Self {
        Self::Sql {
            sql: sql.into(),
            origin: StatementOrigin::EngineMetadataRead,
        }
    }

    /// A helper statement, as [`Connection::prepare_internal`] prepares it.
    pub(crate) fn internal_helper(sql: impl Into<String>) -> Self {
        Self::Sql {
            sql: sql.into(),
            origin: StatementOrigin::InternalHelper,
        }
    }
}

/// Where a statement of a [`LoadStep`] failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueryFailure {
    Prepare,
    Read,
}

/// A schema that is being read.
pub(crate) struct SchemaLoad {
    pub(crate) schema: Schema,
    /// Indexes and materialized views of the schema rows. They are built
    /// after the types are loaded, and None after that.
    unbuilt: Option<UnbuiltViewsAndIndexes>,
    /// The CREATE TYPE statements of the custom types table.
    type_sqls: Vec<String>,
}

struct UnbuiltViewsAndIndexes {
    from_sql_indexes: crate::alloc::Vec<UnparsedFromSqlIndex>,
    automatic_indices: HashMap<String, crate::alloc::Vec<(String, i64)>>,
    dbsp_state_roots: HashMap<String, i64>,
    dbsp_state_index_roots: HashMap<String, i64>,
    materialized_view_info: HashMap<String, StoredMaterializedView>,
}

impl SchemaLoad {
    pub(crate) fn new(schema: Schema) -> Self {
        Self {
            schema,
            unbuilt: Some(UnbuiltViewsAndIndexes::new()),
            type_sqls: Vec::new(),
        }
    }
}

impl UnbuiltViewsAndIndexes {
    fn new() -> Self {
        Self {
            from_sql_indexes:
                <crate::alloc::Vec<_> as crate::alloc::TursoTryWithCapacityExt>::try_with_capacity_ext(10)
                    .expect(crate::alloc::ALLOC_ERR_MSG),
            automatic_indices: HashMap::with_capacity_and_hasher(10, Default::default()),
            dbsp_state_roots: HashMap::default(),
            dbsp_state_index_roots: HashMap::default(),
            materialized_view_info: HashMap::default(),
        }
    }
}

/// Runs the steps of a schema load one after the other. It holds what is
/// the same for every step: preparing each statement against the schema read
/// so far, yielding on I/O, and passing the rows to the step.
pub(crate) struct SchemaLoader {
    load: SchemaLoad,
    /// The steps not finished yet. The first one is the current step.
    steps: VecDeque<Box<dyn LoadStep>>,
    /// The statement of the current step that is being read.
    statement: Option<Box<Statement>>,
    /// Whether the connection's schema is the one that the current step
    /// started with.
    current_step_schema_installed: bool,
}

impl SchemaLoader {
    pub(crate) fn new(schema: Schema, steps: Vec<Box<dyn LoadStep>>) -> Self {
        Self {
            load: SchemaLoad::new(schema),
            steps: steps.into(),
            statement: None,
            current_step_schema_installed: false,
        }
    }

    /// Runs the steps until all are finished. Call it again with the same
    /// connection after each I/O yield.
    pub(crate) fn run(&mut self, conn: &Arc<Connection>) -> IOResultOr<()> {
        loop {
            if self.statement.is_none() && !self.prepare_next_statement(conn)? {
                return Ok(IOResult::Done(()));
            }
            let step = self
                .steps
                .front_mut()
                .expect("a prepared statement belongs to a step");
            let statement = self.statement.as_mut().expect("prepared above");
            let load = &mut self.load;
            match statement.run_with_row_callback_nonblock(|row| step.row(load, row)) {
                Ok(IOResult::IO(io)) => return Ok(IOResult::IO(io)),
                Ok(IOResult::Done(())) => {}
                Err(err) => return Err(step.query_failed(QueryFailure::Read, *err).into()),
            }
            step.end_of_rows(load)?;
            self.statement = None;
        }
    }

    /// Prepares the next statement of the steps. Returns false when no step
    /// has a statement left. The connection's schema becomes the schema read
    /// so far before the first statement of each step, so that the step's
    /// statements can name the tables read by the steps before it.
    pub(crate) fn prepare_next_statement(&mut self, conn: &Arc<Connection>) -> Result<bool> {
        turso_assert!(
            self.statement.is_none(),
            "the statement before must be read to its end"
        );
        loop {
            let Some(step) = self.steps.front_mut() else {
                return Ok(false);
            };
            let Some(query) = step.next_query(&mut self.load)? else {
                self.steps.pop_front();
                self.current_step_schema_installed = false;
                continue;
            };
            if !self.current_step_schema_installed {
                conn.install_schema_keeping_dialect_schema(&self.load.schema)?;
                self.current_step_schema_installed = true;
            }
            let statement = match query {
                LoadQuery::SchemaTableRead => conn.prepare_schema_table_read(),
                LoadQuery::Sql { sql, origin } => conn.prepare_with_origin(&sql, origin),
            }
            .map_err(|err| step.query_failed(QueryFailure::Prepare, err))?;
            self.statement = Some(Box::new(statement));
            return Ok(true);
        }
    }

    pub(crate) fn into_schema(self) -> Schema {
        self.load.schema
    }
}

/// Reads the schema rows: tables, views, triggers, and the indexes and
/// materialized views that [`ViewsAndIndexes`] builds later.
pub(crate) struct SchemaRows {
    conn: Arc<Connection>,
    queried: bool,
}

impl SchemaRows {
    pub(crate) fn new(conn: Arc<Connection>) -> Self {
        Self {
            conn,
            queried: false,
        }
    }
}

impl LoadStep for SchemaRows {
    fn next_query(&mut self, _load: &mut SchemaLoad) -> Result<Option<LoadQuery>> {
        if self.queried {
            return Ok(None);
        }
        self.queried = true;
        Ok(Some(LoadQuery::SchemaTableRead))
    }

    fn row(&mut self, load: &mut SchemaLoad, row: &Row) -> Result<()> {
        let unbuilt = load
            .unbuilt
            .as_mut()
            .expect("schema rows are read before views and indexes are built");
        // Maps the attached database that a temp trigger names to its index
        // on this connection.
        let attached_database_id = |name: &str| -> Option<usize> {
            self.conn
                .qualifiers
                .database_id(&crate::util::normalize_ident(name))
        };
        let ty = row.get::<&str>(0)?;
        let name = row.get::<&str>(1)?;
        let table_name = row.get::<&str>(2)?;
        let root_page = row.get::<i64>(3)?;
        let sql = row.get::<&str>(4).ok();
        load.schema.handle_schema_row(
            ty,
            name,
            table_name,
            root_page,
            sql,
            &self.conn.syms.read(),
            &mut unbuilt.from_sql_indexes,
            &mut unbuilt.automatic_indices,
            &mut unbuilt.dbsp_state_roots,
            &mut unbuilt.dbsp_state_index_roots,
            &mut unbuilt.materialized_view_info,
            &attached_database_id,
            self.conn.db.dialect().as_ref(),
        )
    }
}

/// Reads the names of the namespaces from the namespaces table.
pub(crate) struct NamespaceNames {
    conn: Arc<Connection>,
    queried: bool,
    rows: Vec<(i64, String)>,
}

impl NamespaceNames {
    pub(crate) fn new(conn: Arc<Connection>) -> Self {
        Self {
            conn,
            queried: false,
            rows: Vec::new(),
        }
    }
}

impl LoadStep for NamespaceNames {
    fn next_query(&mut self, load: &mut SchemaLoad) -> Result<Option<LoadQuery>> {
        if self.queried {
            return Ok(None);
        }
        self.queried = true;
        // Under a dialect without namespaces, a table of that name is an
        // ordinary table.
        if load.schema.supports_namespaces()
            && load
                .schema
                .has_table(NamespaceId::ROOT, TURSO_NAMESPACES_TABLE_NAME)
        {
            return Ok(Some(LoadQuery::internal_helper(format!(
                "SELECT id, name FROM {TURSO_NAMESPACES_TABLE_NAME}"
            ))));
        }
        load.schema
            .load_namespace_names(self.conn.db.dialect().as_ref(), &[])?;
        Ok(None)
    }

    fn row(&mut self, _load: &mut SchemaLoad, row: &Row) -> Result<()> {
        self.rows
            .push((row.get::<i64>(0)?, row.get::<&str>(1)?.to_string()));
        Ok(())
    }

    fn end_of_rows(&mut self, load: &mut SchemaLoad) -> Result<()> {
        load.schema
            .load_namespace_names(self.conn.db.dialect().as_ref(), &self.rows)
    }
}

/// Reads the description of every sequence from its backing table.
pub(crate) struct Sequences {
    conn: Arc<Connection>,
    /// The sequences that VACUUM kept from before it rebuilt the file. They
    /// replace reading the backing tables.
    preserved: Option<SequencesByNamespace>,
    /// `(backing table name, sequence name)` of each backing table, or None
    /// before the first call of `next_query`.
    backing_tables: Option<crate::alloc::Vec<(String, String)>>,
    /// Index in `backing_tables` of the backing table being read.
    current: usize,
    read: SequenceRead,
}

/// How far the read of one sequence backing table is.
enum SequenceRead {
    NotStarted,
    /// Reading the row `(start, inc, min, max, cycle)`.
    Descriptor {
        row: Option<(i64, i64, i64, i64, bool)>,
    },
    /// The descriptor row is read, and the watermark query is next.
    DescriptorRead {
        sequence: Sequence,
    },
    /// Reading the row `(value, is_called)`.
    Watermark {
        sequence: Sequence,
        row: Option<(i64, bool)>,
    },
}

impl Sequences {
    pub(crate) fn new(conn: Arc<Connection>, preserved: Option<SequencesByNamespace>) -> Self {
        Self {
            conn,
            preserved,
            backing_tables: None,
            current: 0,
            read: SequenceRead::NotStarted,
        }
    }

    fn current_backing_table(&self) -> (&str, &str) {
        let backing_tables = self
            .backing_tables
            .as_ref()
            .expect("set by the first call of next_query");
        let (backing_table_name, sequence_name) = &backing_tables[self.current];
        (backing_table_name, sequence_name)
    }
}

impl LoadStep for Sequences {
    fn next_query(&mut self, load: &mut SchemaLoad) -> Result<Option<LoadQuery>> {
        if self.backing_tables.is_none() {
            if let Some(sequences) = self.preserved.take() {
                load.schema.set_sequences_by_namespace(sequences);
                self.backing_tables = Some(crate::alloc::vec![]);
            } else {
                self.backing_tables = Some(load.schema.sequence_backing_table_names());
            }
        }
        let backing_tables = self.backing_tables.as_ref().expect("set above");
        if let SequenceRead::DescriptorRead { .. } = &self.read {
            let SequenceRead::DescriptorRead { sequence } =
                std::mem::replace(&mut self.read, SequenceRead::NotStarted)
            else {
                unreachable!("matched above");
            };
            let (backing_table_name, _) = &backing_tables[self.current];
            let sql = Connection::sequence_watermark_query(backing_table_name, &sequence);
            self.read = SequenceRead::Watermark {
                sequence,
                row: None,
            };
            return Ok(Some(LoadQuery::internal_helper(sql)));
        }
        turso_assert!(
            matches!(self.read, SequenceRead::NotStarted),
            "the statement before must be read to its end"
        );
        while let Some((backing_table_name, mangled_name)) = backing_tables.get(self.current) {
            let normalized = crate::util::normalize_ident(mangled_name);
            let (namespace, sequence_name) = load.schema.demangle_name(&normalized);
            if load.schema.has_sequence(namespace, sequence_name) {
                self.current += 1;
                continue;
            }
            self.read = SequenceRead::Descriptor { row: None };
            return Ok(Some(LoadQuery::internal_helper(
                Connection::sequence_descriptor_query(backing_table_name),
            )));
        }
        Ok(None)
    }

    fn row(&mut self, _load: &mut SchemaLoad, row: &Row) -> Result<()> {
        match &mut self.read {
            SequenceRead::Descriptor { row: descriptor } => {
                *descriptor = Some(Connection::sequence_descriptor_from_row(row)?);
            }
            SequenceRead::Watermark { row: watermark, .. } => {
                *watermark = Some(Connection::sequence_watermark_row(row)?);
            }
            SequenceRead::NotStarted | SequenceRead::DescriptorRead { .. } => {
                unreachable!("a row without a statement being read")
            }
        }
        Ok(())
    }

    fn end_of_rows(&mut self, load: &mut SchemaLoad) -> Result<()> {
        let backing_tables = self
            .backing_tables
            .as_ref()
            .expect("set by the first call of next_query");
        let (backing_table_name, mangled_name) = &backing_tables[self.current];
        let normalized = crate::util::normalize_ident(mangled_name);
        match std::mem::replace(&mut self.read, SequenceRead::NotStarted) {
            SequenceRead::Descriptor { row } => {
                let (_, sequence_name) = load.schema.demangle_name(&normalized);
                let sequence = Connection::sequence_from_descriptor_meta(
                    sequence_name,
                    backing_table_name,
                    row,
                )?;
                self.read = SequenceRead::DescriptorRead { sequence };
            }
            SequenceRead::Watermark { sequence, row } => {
                let watermark =
                    Connection::sequence_watermark_from_row(backing_table_name, &sequence, row)?;
                if let Some(mv_store) = self.conn.db.get_mv_store().as_ref() {
                    mv_store.set_sequence_watermark(&normalized, watermark);
                }
                load.schema
                    .insert_sequence_under_mangled_name(&normalized, Arc::new(sequence));
                self.current += 1;
            }
            SequenceRead::NotStarted | SequenceRead::DescriptorRead { .. } => {
                unreachable!("the end of rows without a statement being read")
            }
        }
        Ok(())
    }

    /// A backing table is the engine's own, so a statement on it fails only
    /// when the file is corrupt. Reporting that the sequence does not exist
    /// would hide it.
    fn query_failed(&self, failure: QueryFailure, err: LimboError) -> LimboError {
        let (backing_table_name, mangled_name) = self.current_backing_table();
        match (&self.read, failure) {
            (SequenceRead::Descriptor { .. }, QueryFailure::Prepare) => {
                Connection::sequence_backing_table_corrupt(
                    backing_table_name,
                    mangled_name,
                    "cannot prepare descriptor SELECT",
                    &err,
                )
            }
            (SequenceRead::Descriptor { .. }, QueryFailure::Read) => {
                Connection::sequence_backing_table_corrupt(
                    backing_table_name,
                    mangled_name,
                    "descriptor row read failed",
                    &err,
                )
            }
            (SequenceRead::Watermark { sequence, .. }, QueryFailure::Prepare) => {
                Connection::sequence_backing_table_corrupt(
                    backing_table_name,
                    &sequence.name,
                    "cannot prepare watermark SELECT",
                    &err,
                )
            }
            (SequenceRead::Watermark { sequence, .. }, QueryFailure::Read) => {
                Connection::sequence_backing_table_corrupt(
                    backing_table_name,
                    &sequence.name,
                    "watermark row read failed",
                    &err,
                )
            }
            (SequenceRead::NotStarted | SequenceRead::DescriptorRead { .. }, _) => err,
        }
    }
}

/// Reads the CREATE TYPE statements from the custom types table.
pub(crate) struct CustomTypes {
    conn: Arc<Connection>,
    queried: bool,
}

impl CustomTypes {
    pub(crate) fn new(conn: Arc<Connection>) -> Self {
        Self {
            conn,
            queried: false,
        }
    }
}

impl LoadStep for CustomTypes {
    fn next_query(&mut self, load: &mut SchemaLoad) -> Result<Option<LoadQuery>> {
        if self.queried {
            return Ok(None);
        }
        self.queried = true;
        if self.conn.experimental_custom_types_enabled()
            && load
                .schema
                .has_table(NamespaceId::ROOT, TURSO_TYPES_TABLE_NAME)
        {
            return Ok(Some(LoadQuery::internal_helper(format!(
                "SELECT name, sql FROM {TURSO_TYPES_TABLE_NAME}"
            ))));
        }
        Ok(None)
    }

    fn row(&mut self, load: &mut SchemaLoad, row: &Row) -> Result<()> {
        load.type_sqls.push(row.get::<&str>(1)?.to_string());
        Ok(())
    }
}

/// Loads the custom types that [`CustomTypes`] read, then builds the
/// materialized views and indexes that [`SchemaRows`] read. It has no
/// statement of its own.
pub(crate) struct ViewsAndIndexes {
    conn: Arc<Connection>,
}

impl ViewsAndIndexes {
    pub(crate) fn new(conn: Arc<Connection>) -> Self {
        Self { conn }
    }
}

impl LoadStep for ViewsAndIndexes {
    fn next_query(&mut self, load: &mut SchemaLoad) -> Result<Option<LoadQuery>> {
        let unbuilt = load
            .unbuilt
            .take()
            .expect("views and indexes are built once");
        load.schema.populate_types_views_then_indices(
            self.conn.db.dialect().as_ref(),
            &self.conn.syms.read(),
            &load.type_sqls,
            unbuilt.materialized_view_info,
            unbuilt.dbsp_state_roots,
            unbuilt.dbsp_state_index_roots,
            unbuilt.from_sql_indexes,
            unbuilt.automatic_indices,
            self.conn.mv_store().is_some(),
        )?;
        Ok(None)
    }

    fn row(&mut self, _load: &mut SchemaLoad, _row: &Row) -> Result<()> {
        unreachable!("building views and indexes has no statement")
    }
}

/// Reads the dialect's own schema state, such as the PostgreSQL frontend's
/// relation catalog, with the statements of
/// [`Dialect::schema_state_queries`](crate::dialect::Dialect::schema_state_queries),
/// and builds it with
/// [`Dialect::build_schema_state`](crate::dialect::Dialect::build_schema_state).
pub(crate) struct DialectState {
    conn: Arc<Connection>,
    /// The statements not started yet, or None before the first call of
    /// `next_query`.
    queries: Option<VecDeque<String>>,
    /// The rows so far of the statement being read.
    rows: Vec<Vec<Value>>,
    /// The rows of each finished statement.
    results: Vec<Vec<Vec<Value>>>,
}

impl DialectState {
    pub(crate) fn new(conn: Arc<Connection>) -> Self {
        Self {
            conn,
            queries: None,
            rows: Vec::new(),
            results: Vec::new(),
        }
    }
}

impl LoadStep for DialectState {
    fn next_query(&mut self, load: &mut SchemaLoad) -> Result<Option<LoadQuery>> {
        let queries = self.queries.get_or_insert_with(|| {
            self.conn
                .db
                .dialect()
                .schema_state_queries(&load.schema)
                .into()
        });
        Ok(queries.pop_front().map(LoadQuery::engine_metadata_read))
    }

    fn row(&mut self, _load: &mut SchemaLoad, row: &Row) -> Result<()> {
        turso_assert!(
            !matches!(self.conn.get_tx_state(), TransactionState::None),
            "dialect schema state must be read in a transaction"
        );
        self.rows.push(row.get_values().cloned().collect());
        Ok(())
    }

    fn end_of_rows(&mut self, load: &mut SchemaLoad) -> Result<()> {
        self.results.push(std::mem::take(&mut self.rows));
        let queries = self.queries.as_ref().expect("set by next_query");
        if queries.is_empty() {
            load.schema.dialect_schema = Some(
                self.conn
                    .db
                    .dialect()
                    .build_schema_state(&load.schema, &self.results)?,
            );
        }
        Ok(())
    }
}
