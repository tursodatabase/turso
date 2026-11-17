//! Reading a schema from the database file: a list of steps, and one driver
//! that runs the statements of each step.

use crate::connection::Connection;
use crate::namespace::NamespaceId;
use crate::schema::{Schema, StoredMaterializedView, TURSO_NAMESPACES_TABLE_NAME};
use crate::statement::StatementOrigin;
use crate::sync::Arc;
use crate::types::{IOResult, IOResultOr};
use crate::util::UnparsedFromSqlIndex;
use crate::vdbe::Row;
use crate::{turso_assert, LimboError, Result, Statement, SymbolTable};
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
pub(crate) struct LoadQuery {
    sql: String,
    origin: StatementOrigin,
}

impl LoadQuery {
    /// A read of an engine table, as
    /// [`Connection::prepare_engine_metadata_read`] prepares it.
    pub(crate) fn engine_metadata_read(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            origin: StatementOrigin::EngineMetadataRead,
        }
    }

    /// A helper statement, as [`Connection::prepare_internal`] prepares it.
    pub(crate) fn internal_helper(sql: impl Into<String>) -> Self {
        Self {
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
    pub(crate) type_sqls: Vec<String>,
}

#[derive(Default)]
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
            unbuilt: Some(UnbuiltViewsAndIndexes::default()),
            type_sqls: Vec::new(),
        }
    }

    /// Loads the custom types of `type_sqls`, then builds the materialized
    /// views and indexes that the schema rows step collected.
    pub(crate) fn load_types_then_build_views_and_indexes(
        &mut self,
        syms: &SymbolTable,
        mvcc_enabled: bool,
    ) -> Result<()> {
        let unbuilt = self
            .unbuilt
            .take()
            .expect("views and indexes are built once");
        self.schema.populate_types_views_then_indices(
            syms,
            &self.type_sqls,
            unbuilt.materialized_view_info,
            unbuilt.dbsp_state_roots,
            unbuilt.dbsp_state_index_roots,
            unbuilt.from_sql_indexes,
            unbuilt.automatic_indices,
            mvcc_enabled,
        )
    }
}

/// Runs the steps of a schema load one after the other. It holds what is
/// the same for every step: preparing each statement against the schema read
/// so far, yielding on I/O, and passing the rows to the step.
pub(crate) struct SchemaLoader {
    pub(crate) load: SchemaLoad,
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
            let statement = conn
                .prepare_with_origin(&query.sql, query.origin)
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
/// materialized views that are built once the custom types are loaded.
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
        Ok(Some(LoadQuery::engine_metadata_read(
            "SELECT * FROM sqlite_schema",
        )))
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
#[derive(Default)]
pub(crate) struct NamespaceNames {
    queried: bool,
    rows: Vec<(i64, String)>,
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
        load.schema.load_namespace_names(&[])?;
        Ok(None)
    }

    fn row(&mut self, _load: &mut SchemaLoad, row: &Row) -> Result<()> {
        self.rows
            .push((row.get::<i64>(0)?, row.get::<&str>(1)?.to_string()));
        Ok(())
    }

    fn end_of_rows(&mut self, load: &mut SchemaLoad) -> Result<()> {
        load.schema.load_namespace_names(&self.rows)
    }
}
