//! ATTACH and DETACH: open a database on the connection and give it a
//! namespace, or remove both.

use crate::function::{Func, ScalarFunc};
#[cfg(feature = "fs")]
use crate::io::{MemoryIO, IO};
#[cfg(feature = "fs")]
use crate::storage::journal_mode;
#[cfg(feature = "fs")]
use crate::sync::atomic::Ordering;
use crate::sync::Arc;
use crate::translate::collate::CollationSeq;
use crate::translate::{emitter::Resolver, subquery::translate_expr_without_from_clause};
use crate::types::IOResultOr;
use crate::util::normalize_ident;
use crate::vdbe::builder::{ProgramBuilder, ProgramBuilderOpts};
use crate::vdbe::insn::Insn;
#[cfg(feature = "fs")]
use crate::{is_memory_like, DatabaseOpts, EncryptionKey, IOResult};
use crate::{
    turso_assert, Connection, Database, LimboError, Pager, Result, MAIN_DB_ID, TEMP_DB_ID,
};
use turso_parser::ast::Expr;

/// Translate ATTACH statement
pub fn translate_attach(
    expr: &Expr,
    resolver: &Resolver,
    db_name: &Expr,
    key: &Option<Box<Expr>>,
    program: &mut ProgramBuilder,
    connection: Arc<Connection>,
) -> Result<()> {
    if !connection.experimental_attach_enabled() {
        return Err(crate::LimboError::ParseError(
            "ATTACH is an experimental feature. Enable with --experimental-attach flag".to_string(),
        ));
    }

    // SQLite treats ATTACH as a function call to sqlite_attach(filename, dbname, key)
    // We'll allocate registers for the arguments and call the function
    program.extend(&ProgramBuilderOpts::new(0, 10, 0));

    let arg_reg = program.alloc_registers(4); // 3 for args + 1 for result

    translate_attach_arg(program, resolver, &connection, expr, arg_reg)?;
    translate_attach_arg(program, resolver, &connection, db_name, arg_reg + 1)?;
    if let Some(key_expr) = key {
        translate_attach_arg(program, resolver, &connection, key_expr, arg_reg + 2)?;
    } else {
        program.emit_insn(Insn::Null {
            dest: arg_reg + 2,
            dest_end: None,
        });
    }

    // Call sqlite_attach function
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: arg_reg,
        dest: arg_reg + 3, // Result register (not used but required)
        func: crate::function::FuncCtx {
            func: Func::Scalar(ScalarFunc::Attach),
            arg_count: 3,
            collation: CollationSeq::BINARY,
        },
    });

    Ok(())
}

/// Translate DETACH statement
pub fn translate_detach(
    expr: &Expr,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: Arc<Connection>,
) -> Result<()> {
    if !connection.experimental_attach_enabled() {
        return Err(crate::LimboError::ParseError(
            "DETACH is an experimental feature. Enable with --experimental-attach flag".to_string(),
        ));
    }
    // SQLite treats DETACH as a function call to sqlite_detach(dbname)
    program.extend(&ProgramBuilderOpts::new(0, 5, 0));

    let arg_reg = program.alloc_registers(2); // 1 for arg + 1 for result

    translate_attach_arg(program, resolver, &connection, expr, arg_reg)?;

    // Call sqlite_detach function
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: arg_reg,
        dest: arg_reg + 1, // Result register (not used but required)
        func: crate::function::FuncCtx {
            func: Func::Scalar(ScalarFunc::Detach),
            arg_count: 1,
            collation: CollationSeq::BINARY,
        },
    });

    Ok(())
}

/// Load an ATTACH or DETACH argument into `target_register`. As in SQLite, a
/// bare identifier is used as its own name and anything else is evaluated as an
/// expression, so `x.db` is a column reference and a subquery is run.
fn translate_attach_arg(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    connection: &Arc<Connection>,
    expr: &Expr,
    target_register: usize,
) -> Result<()> {
    if let Expr::Id(id) = expr {
        program.emit_insn(Insn::String8 {
            value: normalize_ident(id.as_str()),
            dest: target_register,
        });
        return Ok(());
    }
    translate_expr_without_from_clause(program, resolver, connection, expr, target_register)
}

#[cfg(not(feature = "fs"))]
#[derive(Default)]
pub(crate) enum AttachDatabaseState {
    #[default]
    Start,
}

#[cfg(feature = "fs")]
#[derive(Default)]
pub(crate) enum AttachDatabaseState {
    #[default]
    Start,
    Init(Box<AttachDatabaseInitState>),
    Bootstrap(Box<AttachDatabaseBootstrapState>),
    Publish {
        alias: String,
        db: Arc<Database>,
        pager: Arc<Pager>,
    },
    Done,
}

#[cfg(feature = "fs")]
pub(crate) struct AttachDatabaseInitState {
    alias: String,
    reserved_space: Option<u8>,
    db: Arc<Database>,
    attached_is_fresh: bool,
    encryption_key: Option<EncryptionKey>,
    init_st: crate::InitState,
}

#[cfg(feature = "fs")]
pub(crate) struct AttachDatabaseBootstrapState {
    alias: String,
    db: Arc<Database>,
    pager: Arc<Pager>,
    encryption_key: Option<EncryptionKey>,
    bootstrap_conn: Option<Arc<Connection>>,
    bootstrap_st: crate::mvcc::database::BootstrapState,
}

impl Connection {
    /// Returns the reserved-space value inherited from the main connection's pager.
    /// (This reads the main database pager, not the pager of db to be attached)
    fn inherited_reserved_space_for_fresh_attach(&self) -> u8 {
        let pager = self.pager.load();
        pager
            .get_reserved_space()
            .unwrap_or_else(|| pager.io_ctx.read().get_reserved_space_bytes())
    }

    /// Returns the minimum reserved space required by the attached pager's own IO context.
    /// This is used as a floor so inherited or explicit values cannot undercut the attached DB.
    fn minimum_reserved_space_for_fresh_attach(pager: &Pager) -> u8 {
        pager
            .get_reserved_space()
            .unwrap_or(0)
            .max(pager.io_ctx.read().get_reserved_space_bytes())
    }

    fn database_has_existing_wal_state(db: &Database) -> bool {
        let shared_wal = db.shared_wal.read();
        shared_wal.page_size() != 0 || shared_wal.last_checksum_and_max_frame().1 != 0
    }

    fn install_database_wal_on_pager(db: &Arc<Database>, pager: &mut Arc<Pager>) {
        let shared_wal = db.shared_wal.clone();
        let last_checksum_and_max_frame = shared_wal.read().last_checksum_and_max_frame();
        let wal = Arc::new(crate::storage::wal::WalFile::new(
            db.io.clone(),
            shared_wal,
            last_checksum_and_max_frame,
            db.buffer_pool.clone(),
        ));

        let pager = Arc::get_mut(pager)
            .expect("fresh attached pager must not be shared before bootstrap or publication");
        pager.set_wal(wal);
    }

    fn set_mvcc_journal_mode_fresh_db(pager: &Pager) -> Result<()> {
        turso_assert!(!pager.db_initialized());
        pager.set_initial_journal_version(crate::storage::sqlite3_ondisk::Version::Mvcc)
    }

    fn validate_attach_target(db: &Database, is_fresh: bool, alias: &str) -> Result<()> {
        if is_fresh && Self::database_has_existing_wal_state(db) {
            return Err(LimboError::InvalidArgument(format!(
                "cannot attach database '{alias}': main database file is uninitialized but WAL state exists"
            )));
        }

        if is_fresh && db.is_readonly() {
            return Err(LimboError::InvalidArgument(format!(
                "cannot attach database '{alias}': fresh read-only databases cannot be initialized during attach"
            )));
        }
        Ok(())
    }

    fn apply_page_layout_to_fresh_attach_db(
        &self,
        alias: &str,
        attached_db_pager: &Pager,
        reserved_space: Option<u8>,
    ) -> Result<()> {
        let target_page_size = self.get_page_size();
        let attached_min_reserved_space =
            Self::minimum_reserved_space_for_fresh_attach(attached_db_pager);
        let target_reserved_space = match reserved_space {
            Some(space) => {
                // this happens reserved_space is explicitly passed along with encryption or checksum
                if space < attached_min_reserved_space {
                    return Err(LimboError::InvalidArgument(format!(
                        "cannot attach database '{alias}': reserved space {space} is smaller than attached database minimum {attached_min_reserved_space}"
                    )));
                }
                Some(space)
            }
            None => Some(
                self.inherited_reserved_space_for_fresh_attach()
                    .max(attached_min_reserved_space),
            ),
        };

        attached_db_pager.set_initial_page_size(target_page_size)?;
        if let Some(reserved_space) = target_reserved_space {
            attached_db_pager.set_reserved_space_bytes(reserved_space);
        }
        Ok(())
    }

    fn reject_initialized_attach_mismatches(
        &self,
        alias: &str,
        db: &Database,
        pager: &Pager,
    ) -> Result<()> {
        // Reject incompatible journal modes for initialized attached databases:
        // we cannot silently convert the header (the user may have attached read-only).
        if self.mvcc_enabled() != db.mvcc_enabled() {
            let main_mode = if self.mvcc_enabled() { "MVCC" } else { "WAL" };
            let attached_mode = if db.mvcc_enabled() { "MVCC" } else { "WAL" };
            return Err(LimboError::InvalidArgument(format!(
                "cannot attach database '{alias}': main database uses {main_mode} journal mode \
                 but attached database uses {attached_mode}. Both must use the same journal mode."
            )));
        }

        // Reject mismatched page sizes: ephemeral tables and cross-database
        // operations assume a uniform page size across all attached databases.
        let main_pager = self.pager.load();
        if let (Some(main_ps), Some(attached_ps)) =
            (main_pager.get_page_size(), pager.get_page_size())
        {
            if main_ps != attached_ps {
                return Err(LimboError::InvalidArgument(format!(
                    "cannot attach database '{alias}': page size mismatch \
                     (main={main_ps:?}, attached={attached_ps:?})"
                )));
            }
        }

        Ok(())
    }

    fn reject_unsupported_fresh_mvcc_attach_durable_storage(
        &self,
        alias: &str,
        db: &Database,
        attached_is_fresh: bool,
    ) -> Result<()> {
        if attached_is_fresh
            && self.mvcc_enabled()
            && self.db.durable_storage.is_some()
            && db.durable_storage.is_none()
        {
            return Err(LimboError::InvalidArgument(format!(
                "cannot attach database '{alias}': fresh MVCC attach does not support inheriting custom durable storage"
            )));
        }

        Ok(())
    }

    /// Attach a database file with the given alias name
    #[cfg(not(feature = "fs"))]
    pub(crate) fn attach_database(
        &self,
        _path: &str,
        _alias: &str,
        _state: &mut AttachDatabaseState,
    ) -> IOResultOr<()> {
        Err(LimboError::InvalidArgument(
            "attach not available in this build (no-fs)".to_string(),
        ))
    }

    #[cfg(not(feature = "fs"))]
    pub(crate) fn attach_database_with_config(
        &self,
        _path: &str,
        _alias: &str,
        _reserved_space: Option<u8>,
        _state: &mut AttachDatabaseState,
    ) -> IOResultOr<()> {
        // File-backed ATTACH is unavailable without `fs`, so pre-initialization
        // page-layout overrides are also unsupported in this build.
        self.attach_database(_path, _alias, _state)
    }

    /// Attach a database file with the given alias name
    #[cfg(feature = "fs")]
    pub(crate) fn attach_database(
        &self,
        path: &str,
        alias: &str,
        state: &mut AttachDatabaseState,
    ) -> IOResultOr<()> {
        self.attach_database_with_config(path, alias, None, state)
    }

    /// Attach a database file with an optional pre-initialization reserved-space override.
    #[cfg(feature = "fs")]
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn attach_database_with_config(
        &self,
        path: &str,
        alias: &str,
        reserved_space: Option<u8>,
        state: &mut AttachDatabaseState,
    ) -> IOResultOr<()> {
        loop {
            match state {
                AttachDatabaseState::Start => {
                    if self.is_closed() {
                        return Err(
                            LimboError::InternalError("Connection closed".to_string()).into()
                        );
                    }

                    if self.is_attached(alias) {
                        return Err(LimboError::InvalidArgument(format!(
                            "database {alias} is already in use"
                        ))
                        .into());
                    }

                    if alias.eq_ignore_ascii_case("main") || alias.eq_ignore_ascii_case("temp") {
                        return Err(LimboError::InvalidArgument(format!(
                            "reserved name {alias} is already in use"
                        ))
                        .into());
                    }
                    if self.pager.load().has_external_page_codec() {
                        return Err(LimboError::InvalidArgument(
                            "ATTACH is unsupported for connections using an external page codec"
                                .to_string(),
                        )
                        .into());
                    }

                    let db_opts = DatabaseOpts::new()
                        .with_views(self.db.experimental_views_enabled())
                        .with_custom_types(self.db.experimental_custom_types_enabled())
                        .with_index_method(self.db.experimental_index_method_enabled())
                        .with_vacuum(self.db.experimental_vacuum_enabled())
                        .with_without_rowid(self.db.experimental_without_rowid_enabled());
                    let is_memory_db = is_memory_like(path);
                    let io: Arc<dyn IO> = if is_memory_db {
                        Arc::new(MemoryIO::new())
                    } else if self.db.is_in_memory_db() {
                        Database::io_for_path(path)?
                    } else {
                        self.db.io.clone()
                    };
                    let main_db_flags = self.db.open_flags;
                    let (db, encryption_opts) = Self::from_uri_attached(
                        path,
                        db_opts,
                        main_db_flags,
                        io,
                        self.db.dialect(),
                    )?;
                    let attached_is_fresh = !db.initialized();
                    if !is_memory_db {
                        Self::validate_attach_target(&db, attached_is_fresh, alias)?;
                    }
                    self.reject_unsupported_fresh_mvcc_attach_durable_storage(
                        alias,
                        &db,
                        attached_is_fresh,
                    )?;

                    let encryption_key = if let Some(ref enc) = encryption_opts {
                        Some(EncryptionKey::from_hex_string(&enc.hexkey)?)
                    } else {
                        None
                    };

                    *state = AttachDatabaseState::Init(Box::new(AttachDatabaseInitState {
                        alias: alias.to_string(),
                        reserved_space,
                        db,
                        attached_is_fresh,
                        encryption_key,
                        init_st: crate::InitState::default(),
                    }));
                }
                AttachDatabaseState::Init(init) => {
                    let mut pager = Arc::new(crate::return_if_io!(init.db._init_nonblock(
                        &mut init.init_st,
                        init.encryption_key.as_ref(),
                        None,
                    )));

                    if !init.attached_is_fresh {
                        self.reject_initialized_attach_mismatches(&init.alias, &init.db, &pager)?;
                        *state = AttachDatabaseState::Publish {
                            alias: init.alias.clone(),
                            db: init.db.clone(),
                            pager,
                        };
                        continue;
                    }

                    self.apply_page_layout_to_fresh_attach_db(
                        &init.alias,
                        &pager,
                        init.reserved_space,
                    )?;

                    if self.mvcc_enabled() && !init.db.mvcc_enabled() {
                        Self::set_mvcc_journal_mode_fresh_db(&pager)?;
                        Self::install_database_wal_on_pager(&init.db, &mut pager);
                        let enc_ctx = pager.io_ctx.read().encryption_context().cloned();
                        let mv_store = journal_mode::open_mv_store(
                            init.db.io.clone(),
                            &init.db.path,
                            init.db.open_flags,
                            init.db.durable_storage.clone(),
                            enc_ctx,
                            init.db.allocators.mv_store.clone(),
                            init.db.experimental_mvcc_passive_checkpoint_enabled(),
                        )?;
                        init.db.mv_store.store(Some(mv_store));
                        *state = AttachDatabaseState::Bootstrap(Box::new(
                            AttachDatabaseBootstrapState {
                                alias: init.alias.clone(),
                                db: init.db.clone(),
                                pager,
                                encryption_key: init.encryption_key.take(),
                                bootstrap_conn: None,
                                bootstrap_st: crate::mvcc::database::BootstrapState::default(),
                            },
                        ));
                    } else {
                        *state = AttachDatabaseState::Publish {
                            alias: init.alias.clone(),
                            db: init.db.clone(),
                            pager,
                        };
                    }
                }
                AttachDatabaseState::Bootstrap(bootstrap) => {
                    if bootstrap.bootstrap_conn.is_none() {
                        let default_cache_size = match bootstrap
                            .pager
                            .with_header(|header| header.default_page_cache_size)
                        {
                            Ok(IOResult::Done(default_cache_size)) => default_cache_size.get(),
                            Ok(IOResult::IO(io)) => return Ok(IOResult::IO(io)),
                            Err(_) => 0,
                        };
                        bootstrap.bootstrap_conn =
                            Some(bootstrap.db._connect_with_pager_and_default_cache_size(
                                true,
                                bootstrap.pager.clone(),
                                bootstrap.encryption_key.take(),
                                default_cache_size,
                            )?);
                    }

                    let mv_store_guard = bootstrap.db.get_mv_store();
                    let Some(mv_store) = mv_store_guard.as_ref() else {
                        return Err(LimboError::InternalError(
                            "fresh MVCC attach missing MV store".to_string(),
                        )
                        .into());
                    };
                    crate::return_if_io!(mv_store.bootstrap_nonblock(
                        bootstrap
                            .bootstrap_conn
                            .as_ref()
                            .expect("bootstrap connection initialized above"),
                        &mut bootstrap.bootstrap_st,
                    ));

                    *state = AttachDatabaseState::Publish {
                        alias: bootstrap.alias.clone(),
                        db: bootstrap.db.clone(),
                        pager: bootstrap.pager.clone(),
                    };
                }
                AttachDatabaseState::Publish { alias, db, pager } => {
                    self.has_non_main_pagers.store(true, Ordering::Release);
                    let database_id = self
                        .attached_databases
                        .write()
                        .insert(db.clone(), pager.clone());
                    self.add_qualifier(crate::Qualifier::database(alias, database_id));
                    *state = AttachDatabaseState::Done;
                    return Ok(IOResult::Done(()));
                }
                AttachDatabaseState::Done => {
                    return Err(LimboError::InternalError(
                        "attach_database called after completion".to_string(),
                    )
                    .into());
                }
            }
        }
    }

    // Detach a database by alias name
    pub(crate) fn detach_database(&self, alias: &str) -> Result<()> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }

        if alias == "main" || alias == "temp" {
            return Err(LimboError::InvalidArgument(format!(
                "cannot detach database: {alias}"
            )));
        }

        // Look up the database index first, then rollback any MVCC transaction
        // *before* removing the database from the catalog.  mv_store_for_db
        // and get_pager_from_database_index read `attached_databases`, so we
        // must not hold the write lock during the rollback.
        let Some(database_id) = self.qualifiers.database_id(alias) else {
            return Err(LimboError::InvalidArgument(format!(
                "no such database: {alias}"
            )));
        };
        if database_id == MAIN_DB_ID || database_id == TEMP_DB_ID {
            return Err(LimboError::InvalidArgument(format!(
                "cannot detach database: {alias}"
            )));
        }

        // Rollback any active transaction on this database before detaching.
        // After the Database is removed from the catalog, the MvStore / Pager
        // become unreachable and the transaction would leak forever.
        let pager = self
            .get_pager_from_database_index(&database_id)
            .expect("attached database should always have a pager");

        if pager.holds_read_lock() || pager.holds_write_lock() {
            return Err(LimboError::InvalidArgument(format!(
                "database {alias} is locked"
            )));
        }

        if let Some((tx_id, _mode)) = self.get_mv_tx_for_db(database_id) {
            if let Some(mv_store) = self.mv_store_for_db(database_id) {
                mv_store.rollback_tx(tx_id, pager.clone(), self, database_id);
                pager.end_read_tx();
            }
            self.set_mv_tx_for_db(database_id, None);
        } else {
            // Non-MVCC attached DB (e.g. :memory:) — rollback WAL state.
            pager.rollback_attached();
        }

        // Remove from catalog. The write lock must be released before
        // acquiring database_schemas.write() to maintain consistent lock
        // ordering (attached_databases before database_schemas).
        self.remove_qualifier(alias);
        self.attached_databases.write().remove(database_id);
        self.clear_sequence_currvals_of_database(database_id);

        // Invalidate the cached schema for this database index so that a future
        // ATTACH reusing the same index won't see stale schema entries.
        self.database_schemas.write().remove(&database_id);
        self.bump_prepare_context_generation();

        Ok(())
    }
}
