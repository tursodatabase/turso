use crate::sync::Arc;
use crate::types::IOResultOr;
use crate::{return_if_io, Connection, IOResult, LimboError, Statement};

const VERSIONS_TABLE: &str = "__turso_internal_frontends";

#[derive(Default)]
pub(crate) struct SetupState {
    phase: SetupPhase,
    statement: Option<Box<Statement>>,
    version: u32,
}

#[derive(Clone, Copy, Default)]
enum SetupPhase {
    #[default]
    Start,
    ReadVersion {
        locked: bool,
    },
    Begin,
    CreateVersions,
    Apply {
        migration: usize,
        statement: usize,
    },
    RecordVersion {
        migration: usize,
    },
    Commit,
    Done,
}

impl SetupState {
    pub(crate) fn step(&mut self, conn: &Arc<Connection>) -> IOResultOr<()> {
        let dialect = conn.dialect();
        let migrations = dialect.internal_migrations();
        let frontend = crate::util::escape_sql_string_literal(dialect.name());
        loop {
            match self.phase {
                SetupPhase::Start => {
                    for (index, migration) in migrations.iter().enumerate() {
                        assert_eq!(migration.version as usize, index + 1);
                    }
                    if migrations.is_empty() {
                        self.phase = SetupPhase::Done;
                    } else {
                        conn.maybe_update_schema();
                        self.phase = if conn
                            .current_schema()
                            .get_btree_table(VERSIONS_TABLE)
                            .is_some()
                        {
                            SetupPhase::ReadVersion { locked: false }
                        } else {
                            SetupPhase::Begin
                        };
                    }
                }
                SetupPhase::ReadVersion { locked } => {
                    if self.statement.is_none() {
                        self.version = 0;
                        self.statement = Some(Box::new(conn.prepare_internal_root(format!(
                            "SELECT version FROM {VERSIONS_TABLE} WHERE frontend = '{frontend}'"
                        ))?));
                    }
                    let mut found = self.version != 0;
                    let stmt = self.statement.as_mut().unwrap();
                    return_if_io!(stmt.run_with_row_callback_nonblock(|row| {
                        if found {
                            return Err(LimboError::Corrupt("duplicate frontend version".into()));
                        }
                        self.version = row
                            .get_value(0)
                            .as_int()
                            .and_then(|v| u32::try_from(v).ok())
                            .filter(|v| *v > 0)
                            .ok_or_else(|| {
                                LimboError::Corrupt("invalid frontend version".into())
                            })?;
                        found = true;
                        Ok(())
                    }));
                    self.statement = None;
                    if self.version as usize > migrations.len() {
                        return Err(LimboError::InvalidArgument(format!(
                            "frontend {} requires a newer version (stored version {})",
                            dialect.name(),
                            self.version
                        ))
                        .into());
                    }
                    self.phase = if self.version as usize == migrations.len() {
                        if locked {
                            SetupPhase::Commit
                        } else {
                            SetupPhase::Done
                        }
                    } else if locked {
                        SetupPhase::Apply {
                            migration: self.version as usize,
                            statement: 0,
                        }
                    } else {
                        SetupPhase::Begin
                    };
                }
                SetupPhase::Begin => {
                    if conn.db.is_readonly() {
                        return Err(LimboError::InvalidArgument(format!(
                            "frontend {} needs initialization on a writable database",
                            dialect.name()
                        ))
                        .into());
                    }
                    return_if_io!(self.run(conn, "BEGIN"));
                    self.phase = SetupPhase::CreateVersions;
                }
                SetupPhase::CreateVersions => {
                    return_if_io!(self.run(
                        conn,
                        "CREATE TABLE IF NOT EXISTS __turso_internal_frontends (
                            frontend TEXT PRIMARY KEY, version INTEGER NOT NULL
                        )"
                    ));
                    self.phase = SetupPhase::ReadVersion { locked: true };
                }
                SetupPhase::Apply {
                    migration,
                    statement,
                } => {
                    let statements = migrations[migration].statements;
                    if statement == statements.len() {
                        self.phase = SetupPhase::RecordVersion { migration };
                    } else {
                        return_if_io!(self.run(conn, statements[statement]));
                        self.phase = SetupPhase::Apply {
                            migration,
                            statement: statement + 1,
                        };
                    }
                }
                SetupPhase::RecordVersion { migration } => {
                    let sql = format!(
                        "INSERT INTO {VERSIONS_TABLE} (frontend, version) VALUES ('{frontend}', {})
                         ON CONFLICT (frontend) DO UPDATE SET version = excluded.version",
                        migrations[migration].version
                    );
                    return_if_io!(self.run(conn, &sql));
                    self.phase = if migration + 1 == migrations.len() {
                        SetupPhase::Commit
                    } else {
                        SetupPhase::Apply {
                            migration: migration + 1,
                            statement: 0,
                        }
                    };
                }
                SetupPhase::Commit => {
                    return_if_io!(self.run(conn, "COMMIT"));
                    self.phase = SetupPhase::Done;
                }
                SetupPhase::Done => return Ok(IOResult::Done(())),
            }
        }
    }

    fn run(&mut self, conn: &Arc<Connection>, sql: &str) -> IOResultOr<()> {
        if self.statement.is_none() {
            self.statement = Some(Box::new(conn.prepare_internal_root(sql)?));
        }
        return_if_io!(self.statement.as_mut().unwrap().run_ignore_rows_nonblock());
        self.statement = None;
        Ok(IOResult::Done(()))
    }

    pub(crate) fn cancel(&mut self) {
        self.statement = None;
    }
}
