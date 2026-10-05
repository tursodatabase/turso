use std::collections::VecDeque;
use std::sync::Arc;

use turso_core::{
    io::Buffer, types::IOResultOr, Completion, IOResult, LimboError, OpenFlags, PrepareOptions,
    Result, Statement, StatementExecution,
};
use turso_parser::ast;
use turso_pg_parser::translator::{PgCopyFromStmt, PgCreateSchemaStmt, PgDropSchemaStmt};

use crate::copy::parse_copy_text_format;
use crate::session::PgConnectionInner;

pub(crate) enum Plan {
    Prerequisites {
        statements: Vec<ast::Stmt>,
        options: PrepareOptions,
        main: Box<ast::Cmd>,
        input: String,
    },
    CreateSchema(PgCreateSchemaStmt),
    DropSchema(PgDropSchemaStmt),
    Copy(PgCopyFromStmt),
    SearchPath(Vec<String>),
}

pub(crate) fn with_execution(
    conn: Arc<PgConnectionInner>,
    mut stmt: Statement,
    plan: Option<Plan>,
) -> Statement {
    if let Some(plan) = plan {
        stmt.set_frontend_execution(Box::new(Execution {
            conn,
            plan,
            operation: None,
        }));
    }
    stmt
}

struct Execution {
    conn: Arc<PgConnectionInner>,
    plan: Plan,
    operation: Option<Operation>,
}

impl StatementExecution for Execution {
    fn step(&mut self) -> IOResultOr<Option<Statement>> {
        if self.operation.is_none() {
            self.operation = Some(Operation::new(&self.conn, &self.plan)?);
        }
        let result = self.operation.as_mut().unwrap().step(&self.conn)?;
        if matches!(result, IOResult::Done(_)) {
            self.operation = None;
        }
        Ok(result)
    }

    fn reset(&mut self) {
        self.operation = None;
    }
}

enum Operation {
    Batch {
        statements: VecDeque<Sql>,
        current: Option<Box<turso_core::Statement>>,
        main: Option<(Box<ast::Cmd>, String, PrepareOptions)>,
    },
    DropSchema {
        spec: PgDropSchemaStmt,
        query: Box<turso_core::Statement>,
        tables: Vec<String>,
    },
    CopyColumns {
        spec: PgCopyFromStmt,
        query: Box<turso_core::Statement>,
        columns: usize,
    },
    CopyRead {
        spec: PgCopyFromStmt,
        columns: usize,
        file: Arc<dyn turso_core::File>,
        completion: Completion,
    },
}

enum Sql {
    Translated(Box<ast::Stmt>, PrepareOptions),
    Sqlite(String),
}

impl Operation {
    fn new(pg_conn: &PgConnectionInner, plan: &Plan) -> Result<Self> {
        let conn = &pg_conn.conn;
        match plan {
            Plan::Prerequisites {
                statements,
                options,
                main,
                input,
            } => Ok(Self::Batch {
                statements: statements
                    .iter()
                    .map(|stmt| Sql::Translated(Box::new(stmt.clone()), options.clone()))
                    .collect(),
                current: None,
                main: Some((main.clone(), input.clone(), options.clone())),
            }),
            Plan::SearchPath(path) => {
                pg_conn.set_search_path(path.clone());
                Ok(Self::batch(VecDeque::new()))
            }
            Plan::CreateSchema(spec) => {
                let name = spec.name.to_lowercase();
                if schema_exists(conn, &name) {
                    if spec.if_not_exists {
                        return Ok(Self::batch(VecDeque::new()));
                    }
                    return Err(LimboError::ParseError(format!(
                        "schema \"{name}\" already exists"
                    )));
                }
                let path = schema_file_path(conn, &name);
                Ok(Self::batch(VecDeque::from([Sql::Sqlite(format!(
                    "ATTACH '{}' AS {}",
                    path.replace('\'', "''"),
                    quote_identifier(&name)
                ))])))
            }
            Plan::DropSchema(spec) => {
                let name = spec.name.to_lowercase();
                if !schema_exists(conn, &name) {
                    if spec.if_exists {
                        return Ok(Self::batch(VecDeque::new()));
                    }
                    return Err(LimboError::ParseError(format!(
                        "schema \"{name}\" does not exist"
                    )));
                }
                if name != "public" && !spec.cascade {
                    return Ok(Self::batch(VecDeque::from([Sql::Sqlite(format!(
                        "DETACH {}",
                        quote_identifier(&name)
                    ))])));
                }
                let schema = if name == "public" { "main" } else { &name };
                let sql = format!(
                    "SELECT name FROM {}.sqlite_schema WHERE type='table' \
                     AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__turso_internal_%'",
                    quote_identifier(schema)
                );
                Ok(Self::DropSchema {
                    spec: spec.clone(),
                    query: Box::new(conn.prepare_sqlite(sql)?),
                    tables: Vec::new(),
                })
            }
            Plan::Copy(spec) => {
                let schema = spec.schema_name.as_deref().unwrap_or("main");
                let sql = format!(
                    "PRAGMA {}.table_info('{}')",
                    quote_identifier(schema),
                    spec.table_name.replace('\'', "''")
                );
                Ok(Self::CopyColumns {
                    spec: spec.clone(),
                    query: Box::new(conn.prepare_sqlite(sql)?),
                    columns: 0,
                })
            }
        }
    }

    fn step(&mut self, pg_conn: &PgConnectionInner) -> IOResultOr<Option<turso_core::Statement>> {
        let conn = &pg_conn.conn;
        loop {
            match self {
                Self::Batch {
                    statements,
                    current,
                    main,
                } => {
                    if current.is_none() {
                        let Some(sql) = statements.front() else {
                            let stmt = main
                                .as_ref()
                                .map(|(cmd, input, options)| {
                                    conn.prepare_translated_cmd_with_options(
                                        cmd.as_ref().clone(),
                                        input,
                                        options,
                                    )
                                })
                                .transpose()?;
                            return Ok(IOResult::Done(stmt));
                        };
                        let stmt = match sql {
                            Sql::Translated(stmt, options) => conn
                                .prepare_translated_stmt_with_options(
                                    stmt.as_ref().clone(),
                                    &stmt.to_string(),
                                    options,
                                )?,
                            Sql::Sqlite(sql) => conn.prepare_sqlite(sql)?,
                        };
                        *current = Some(Box::new(stmt));
                    }
                    if let IOResult::IO(io) =
                        current.as_mut().unwrap().run_ignore_rows_nonblock()?
                    {
                        return Ok(IOResult::IO(io));
                    }
                    *current = None;
                    statements.pop_front();
                }
                Self::DropSchema {
                    spec,
                    query,
                    tables,
                } => {
                    if let IOResult::IO(io) = query.run_with_row_callback_nonblock(|row| {
                        tables.push(row.get::<String>(0)?);
                        Ok(())
                    })? {
                        return Ok(IOResult::IO(io));
                    }
                    let name = spec.name.to_lowercase();
                    if name == "public" && !spec.cascade && !tables.is_empty() {
                        return Err(LimboError::ParseError(
                            "cannot drop schema \"public\" because other objects depend on it"
                                .to_string(),
                        )
                        .into());
                    }
                    let schema = if name == "public" { "main" } else { &name };
                    let mut statements: VecDeque<_> = tables
                        .iter()
                        .map(|table| {
                            Sql::Sqlite(format!(
                                "DROP TABLE {}.{}",
                                quote_identifier(schema),
                                quote_identifier(table)
                            ))
                        })
                        .collect();
                    if name != "public" {
                        statements
                            .push_back(Sql::Sqlite(format!("DETACH {}", quote_identifier(&name))));
                    }
                    *self = Self::batch(statements);
                }
                Self::CopyColumns {
                    spec,
                    query,
                    columns,
                } => {
                    if let IOResult::IO(io) = query.run_with_row_callback_nonblock(|_| {
                        *columns += 1;
                        Ok(())
                    })? {
                        return Ok(IOResult::IO(io));
                    }
                    if *columns == 0 {
                        return Err(LimboError::ParseError(format!(
                            "COPY FROM: table '{}' not found or has no columns",
                            spec.table_name
                        ))
                        .into());
                    }
                    let io = conn.get_pager().io.clone();
                    let file = io
                        .open_file(&spec.filename, OpenFlags::ReadOnly, false)
                        .map_err(|err| {
                            LimboError::ParseError(format!(
                                "COPY FROM: cannot read '{}': {err}",
                                spec.filename
                            ))
                        })?;
                    let size = usize::try_from(file.size()?).map_err(|_| {
                        LimboError::ParseError("COPY FROM: file too large".to_string())
                    })?;
                    let buffer = Arc::new(Buffer::new_temporary(size));
                    let completion = file.pread(
                        0,
                        Completion::new_read(buffer, move |result| match result {
                            Ok((_, bytes)) if bytes as usize != size => {
                                Some(turso_core::CompletionError::IOError(
                                    std::io::ErrorKind::UnexpectedEof,
                                    "COPY file read",
                                ))
                            }
                            _ => None,
                        }),
                    )?;
                    *self = Self::CopyRead {
                        spec: spec.clone(),
                        columns: spec.columns.as_ref().map_or(*columns, Vec::len),
                        file,
                        completion,
                    };
                }
                Self::CopyRead {
                    spec,
                    columns,
                    file: _file,
                    completion,
                } => {
                    if !completion.finished() {
                        return Ok(IOResult::IO(turso_core::types::IOCompletions(
                            completion.clone(),
                        )));
                    }
                    if let Some(err) = completion.get_error() {
                        return Err(LimboError::ParseError(format!(
                            "COPY FROM: cannot read '{}': {err}",
                            spec.filename
                        ))
                        .into());
                    }
                    let buffer = completion.as_read().buf();
                    let data = std::str::from_utf8(buffer.as_slice()).map_err(|err| {
                        LimboError::ParseError(format!("COPY FROM: invalid UTF-8: {err}"))
                    })?;
                    let delimiter = spec
                        .delimiter
                        .as_ref()
                        .and_then(|d| d.chars().next())
                        .unwrap_or('\t');
                    let mut rows = parse_copy_text_format(
                        data,
                        delimiter,
                        spec.null_string.as_deref().unwrap_or("\\N"),
                        *columns,
                    )?;
                    if spec.header && !rows.is_empty() {
                        rows.remove(0);
                    }
                    if rows.is_empty() {
                        return Ok(IOResult::Done(Some(
                            conn.prepare_sqlite("SELECT 0 WHERE 0")?,
                        )));
                    }
                    let table = match &spec.schema_name {
                        Some(schema) => format!(
                            "{}.{}",
                            quote_identifier(schema),
                            quote_identifier(&spec.table_name)
                        ),
                        None => quote_identifier(&spec.table_name),
                    };
                    let columns = spec
                        .columns
                        .as_ref()
                        .map(|cols| {
                            format!(
                                " ({})",
                                cols.iter()
                                    .map(|c| quote_identifier(c))
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            )
                        })
                        .unwrap_or_default();
                    let values = rows
                        .iter()
                        .map(|row| {
                            format!(
                                "({})",
                                row.iter()
                                    .map(|value| match value {
                                        Some(value) => format!("'{}'", value.replace('\'', "''")),
                                        None => "NULL".to_string(),
                                    })
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    return Ok(IOResult::Done(Some(conn.prepare_sqlite(format!(
                        "INSERT INTO {table}{columns} VALUES {values}"
                    ))?)));
                }
            }
        }
    }

    fn batch(statements: VecDeque<Sql>) -> Self {
        Self::Batch {
            statements,
            current: None,
            main: None,
        }
    }
}

fn schema_exists(conn: &turso_core::Connection, name: &str) -> bool {
    name == "public"
        || conn
            .list_attached_databases()
            .iter()
            .any(|alias| alias == name)
}

fn schema_file_path(conn: &turso_core::Connection, schema_name: &str) -> String {
    let main_path = conn.db_file_path();
    let filename = format!("turso-postgres-schema-{schema_name}.db");
    if main_path == ":memory:" {
        filename
    } else {
        std::path::Path::new(&main_path)
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .join(filename)
            .to_string_lossy()
            .to_string()
    }
}

fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
