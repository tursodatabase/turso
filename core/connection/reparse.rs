use super::{Connection, SchemaReparseGuard, TransactionState};
use crate::alloc::TryClone;
use crate::namespace::NamespaceId;
use crate::schema::load::{NamespaceNames, SchemaLoader, SchemaRows, Sequences};
use crate::schema::SequencesByNamespace;
use crate::sync::Arc;
use crate::util::IOExt;
use crate::{turso_assert, Result, Schema, Statement, Value};
use std::collections::VecDeque;

/// Re-entrant state for [`Connection::reparse_schema_nonblock`] and the
/// VACUUM-only [`Connection::reparse_schema_with_cookie_keeping_sequences`].
/// `Start` is the fresh state (cookie not yet read / schema build not yet
/// begun); `Building` carries the half-built schema, captured table-valued
/// functions, the held reparse guard, and the current sub-phase across IO
/// yields.
#[derive(Default)]
pub enum ReparseSchemaState {
    #[default]
    Start,
    Building(Box<ReparseSchemaInner>),
}

pub struct ReparseSchemaInner {
    /// Held for the whole reparse so a concurrent reparse on this connection
    /// trips the recursion assert. Dropped when the schema is finalized.
    _guard: SchemaReparseGuard,
    /// Runs the steps converted to [`LoadStep`](crate::schema::load::LoadStep)
    /// and holds the schema that is being read.
    loader: SchemaLoader,
    phase: ReparsePhase,
}

enum ReparsePhase {
    /// Running the steps of the schema loader: the schema rows, the names
    /// of the namespaces and the sequences.
    RunLoadSteps,
    /// Loading custom type definitions from the internal types table.
    LoadTypes { stmt: Box<Statement> },
    /// Loading the CREATE TYPE statements, then building the materialized
    /// views and indexes that the schema rows step collected.
    BuildIndexes,
    /// Loading the dialect's own schema state, such as the PostgreSQL
    /// frontend's relation catalog: `stmt` reads one query into `rows`, the
    /// rows of the queries before it are in `results`, and `queries` are
    /// still to be read.
    LoadDialectSchema {
        stmt: Box<Statement>,
        rows: Vec<Vec<Value>>,
        results: Vec<Vec<Vec<Value>>>,
        queries: VecDeque<String>,
    },
    /// Best-effort ANALYZE-stats refresh before finalizing.
    RefreshStats {
        stats: crate::stats::RefreshAnalyzeStatsState,
    },
}

impl Connection {
    /// Blocking shim: drives [`Self::reparse_schema_nonblock`] to completion.
    /// Retained for the many synchronous callers (statement reprepare, attach,
    /// extension load, vacuum). The genuinely non-blocking callers (MVCC
    /// bootstrap, the open-db state machine) drive `*_nonblock` directly.
    pub(crate) fn reparse_schema(self: &Arc<Connection>) -> Result<()> {
        let io = self.pager.load().io.clone();
        let mut state = ReparseSchemaState::default();
        io.block(|| self.reparse_schema_nonblock(&mut state))
    }

    /// VACUUM-only reparse that grafts a caller-supplied sequence-
    /// descriptor map onto the freshly parsed schema rather than re-
    /// reading the backing tables from disk. VACUUM preserves every
    /// sequence's definition (start/inc/min/max/cycle) — only physical
    /// page locations change — so the source connection's pre-VACUUM
    /// sequences map is still valid for the post-VACUUM image.
    ///
    /// Blocking shim: VACUUM runs through the VDBE stepping layer (which
    /// does not thread IO out), so it drives the non-blocking reparse to
    /// completion here, seeding the `PopulateSequences` phase with the
    /// preserved map so it grafts rather than re-reading each backing table.
    pub(crate) fn reparse_schema_with_cookie_keeping_sequences(
        self: &Arc<Connection>,
        cookie: u32,
        sequences: SequencesByNamespace,
    ) -> Result<()> {
        let io = self.pager.load().io.clone();
        let mut state = ReparseSchemaState::default();
        // `init_reparse_building` consumes the preserved map exactly once, on
        // the first (Start) invocation; `io.block` re-invokes the closure on
        // every IO completion, so `take()` yields `Some` only that first time.
        let mut preserved = Some(sequences);
        io.block(|| {
            if matches!(state, ReparseSchemaState::Start) {
                state = ReparseSchemaState::Building(Box::new(
                    self.init_reparse_building(cookie, preserved.take())?,
                ));
            }
            self.drive_reparse_building(&mut state)
        })
    }

    /// Rebuilds this connection's schema, including dialect metadata, without
    /// blocking. Preserve `state` across I/O yields and resume on this connection.
    /// An active schema transaction publishes the rebuilt schema when it commits.
    pub fn reparse_schema_nonblock(
        self: &Arc<Connection>,
        state: &mut ReparseSchemaState,
    ) -> crate::types::IOResultOr<()> {
        use crate::types::IOResult;
        if matches!(state, ReparseSchemaState::Start) {
            // read cookie before consuming statement program - otherwise we can
            // end up reading cookie with closed transaction state
            let cookie = crate::return_if_io!(self.read_current_schema_cookie_nonblock());
            *state =
                ReparseSchemaState::Building(Box::new(self.init_reparse_building(cookie, None)?));
        }
        self.drive_reparse_building(state)
    }

    /// Synchronous setup shared by the reparse entry points: install the cookie,
    /// build a fresh schema, capture table-valued functions, install the empty
    /// schema (the reprepare hack), and prepare the sqlite_schema scan.
    /// `preserved_sequences` is `Some` only for VACUUM, which grafts the map in
    /// the `PopulateSequences` phase instead of re-reading the backing tables.
    fn init_reparse_building(
        self: &Arc<Connection>,
        cookie: u32,
        preserved_sequences: Option<SequencesByNamespace>,
    ) -> Result<ReparseSchemaInner> {
        let guard = self.schema_reparse_guard();
        self.pager.load().set_schema_cookie(Some(cookie));
        // create fresh schema as some objects can be deleted
        let mut fresh = self
            .db
            .dialect()
            .new_schema(self.experimental_custom_types_enabled())?;
        fresh.schema_version = cookie;

        fresh.copy_table_valued_functions(&self.schema.read());

        // TODO: this is hack to avoid a cyclical problem with schema reprepare
        // The problem here is that we prepare a statement here, but when the statement tries
        // to execute it, it first checks the schema cookie to see if it needs to reprepare the statement.
        // But in this occasion it will always reprepare, and we get an error. So we trick the statement:
        // the loader swaps our schema with a new clean schema that has the same header cookie
        // before it prepares its first statement.
        let mut loader = SchemaLoader::new(
            fresh,
            vec![
                Box::new(SchemaRows::new(self.clone())),
                Box::new(NamespaceNames::default()),
                Box::new(Sequences::new(self.clone(), preserved_sequences)),
            ],
        );
        loader.prepare_next_statement(self)?;

        // MVCC bootstrap connection gets the "baseline" from the DB file and ignores anything in MV store
        if self.is_mvcc_bootstrap_connection() {
            self.set_mv_tx(None);
        }
        Ok(ReparseSchemaInner {
            _guard: guard,
            loader,
            phase: ReparsePhase::RunLoadSteps,
        })
    }

    /// Drive the schema-rebuild phases held in `state` (must be `Building`).
    /// Yields IO; on completion installs the finished schema and resets `state`.
    fn drive_reparse_building(
        self: &Arc<Connection>,
        state: &mut ReparseSchemaState,
    ) -> crate::types::IOResultOr<()> {
        use crate::types::IOResult;
        loop {
            let ReparseSchemaState::Building(inner) = state else {
                unreachable!("drive_reparse_building requires Building state");
            };
            match &mut inner.phase {
                ReparsePhase::RunLoadSteps => {
                    crate::return_if_io!(inner.loader.run(self));

                    // Decide whether to load custom types next.
                    if self.experimental_custom_types_enabled()
                        && inner
                            .loader
                            .load
                            .schema
                            .has_table(NamespaceId::ROOT, crate::schema::TURSO_TYPES_TABLE_NAME)
                    {
                        // Temporarily install the schema so we can query against it.
                        self.install_schema_keeping_dialect_schema(&inner.loader.load.schema)?;
                        let stmt = self.prepare_internal(format!(
                            "SELECT name, sql FROM {}",
                            crate::schema::TURSO_TYPES_TABLE_NAME
                        ))?;
                        inner.phase = ReparsePhase::LoadTypes {
                            stmt: Box::new(stmt),
                        };
                    } else {
                        inner.phase = ReparsePhase::BuildIndexes;
                    }
                }
                ReparsePhase::LoadTypes { stmt } => {
                    let type_sqls = &mut inner.loader.load.type_sqls;
                    crate::return_if_io!(stmt.run_with_row_callback_nonblock(|row| {
                        type_sqls.push(row.get::<&str>(1)?.to_string());
                        Ok(())
                    }));
                    inner.phase = ReparsePhase::BuildIndexes;
                }
                ReparsePhase::BuildIndexes => {
                    inner.loader.load.load_types_then_build_views_and_indexes(
                        &self.syms.read(),
                        self.mv_store().is_some(),
                    )?;
                    inner.phase = self.load_dialect_schema_phase(&inner.loader.load.schema)?;
                }
                ReparsePhase::LoadDialectSchema {
                    stmt,
                    rows,
                    results,
                    queries,
                } => {
                    crate::return_if_io!(stmt.run_with_row_callback_nonblock(|row| {
                        turso_assert!(
                            !matches!(self.get_tx_state(), TransactionState::None),
                            "dialect schema state must be read in a transaction"
                        );
                        rows.push(row.get_values().cloned().collect());
                        Ok(())
                    }));
                    results.push(std::mem::take(rows));
                    if let Some(sql) = queries.pop_front() {
                        *stmt = Box::new(self.prepare_engine_metadata_read(&sql)?);
                        continue;
                    }
                    inner.loader.load.schema.dialect_schema = Some(
                        self.db
                            .dialect()
                            .build_schema_state(&inner.loader.load.schema, results)?,
                    );
                    inner.phase = ReparsePhase::RefreshStats {
                        stats: Default::default(),
                    };
                }
                ReparsePhase::RefreshStats { stats } => {
                    // Best-effort load stats if sqlite_stat1 is present.
                    crate::return_if_io!(crate::stats::refresh_analyze_stats_nonblock(self, stats));

                    // Finalize: install the rebuilt schema. Take ownership so the
                    // guard drops and `state` is reusable.
                    let ReparseSchemaState::Building(inner) = std::mem::take(state) else {
                        unreachable!("state is Building");
                    };
                    let fresh = inner.loader.into_schema();
                    tracing::debug!(
                        "reparse_schema: schema_version={}, tables={:?}",
                        fresh.schema_version,
                        fresh.table_names(NamespaceId::ROOT).collect::<Vec<_>>()
                    );
                    self.with_schema_mut(|schema| {
                        *schema = fresh;
                    })?;
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    /// Returns the phase that loads the dialect's own schema state into
    /// `fresh`, or the stats refresh phase if the dialect keeps none in this
    /// database. The fresh schema is installed first, because the current
    /// schema may not contain the dialect's tables yet.
    fn load_dialect_schema_phase(self: &Arc<Connection>, fresh: &Schema) -> Result<ReparsePhase> {
        let mut queries = VecDeque::from(self.db.dialect().schema_state_queries(fresh));
        let Some(sql) = queries.pop_front() else {
            return Ok(ReparsePhase::RefreshStats {
                stats: Default::default(),
            });
        };
        self.install_schema_keeping_dialect_schema(fresh)?;
        let stmt = self.prepare_engine_metadata_read(&sql)?;
        Ok(ReparsePhase::LoadDialectSchema {
            stmt: Box::new(stmt),
            rows: Vec::new(),
            results: Vec::new(),
            queries,
        })
    }

    /// Installs a copy of `fresh` as the connection's schema while the schema
    /// is still being read, keeping the dialect's schema state loaded before.
    /// It is empty in `fresh` until the dialect's tables are read, and a
    /// statement compiled in between must not see it empty: without it, no
    /// role exists and no table has an owner, privileges or row-level
    /// security.
    pub(crate) fn install_schema_keeping_dialect_schema(&self, fresh: &Schema) -> Result<()> {
        self.with_schema_mut(|schema| {
            let mut installed = fresh.try_clone()?;
            installed.dialect_schema.clone_from(&schema.dialect_schema);
            *schema = installed;
            Ok::<_, crate::alloc::TryReserveError>(())
        })??;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialect::EngineDialect;
    use crate::security::roles::SessionRole;
    use crate::{ConnectOpts, Database};

    /// The reparse of the schema installs a fresh schema before the
    /// dialect's schema state is read, and a statement compiled meanwhile
    /// must not see it empty.
    #[test]
    fn schema_installed_while_being_read_keeps_the_loaded_dialect_schema() {
        let db = Database::open_file(
            Arc::new(crate::MemoryIO::new()),
            ":memory:",
            Arc::new(EngineDialect),
        )
        .unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        conn.with_schema_mut(|schema| {
            schema.dialect_schema = Some(Arc::new(()));
        })
        .unwrap();
        let fresh = crate::schema::builtin::schema_builder(false)
            .unwrap()
            .build();
        assert!(fresh.dialect_schema.is_none());

        conn.install_schema_keeping_dialect_schema(&fresh).unwrap();

        assert!(conn.schema.read().dialect_schema.is_some());
    }
}
