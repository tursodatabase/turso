use std::collections::HashMap;
use std::num::NonZero;
use std::sync::{Arc, Mutex};
use turso_core::{Connection, LimboError, Result, Statement, Value};
use turso_pg_parser::sql_function::{CreateFunction, SqlFunction};
use turso_pg_parser::{ParseError, ParseResult};

const FUNCTIONS_TABLE: &str = "pg_turso_sql_functions";

pub(crate) struct SqlFunctions {
    conn: Arc<Connection>,
    select_names: Mutex<Option<Statement>>,
    select_definition: Mutex<Option<Statement>>,
    parsed_definitions: Mutex<HashMap<String, (String, Arc<SqlFunction>)>>,
}

impl SqlFunctions {
    pub(crate) fn new(conn: Arc<Connection>) -> Self {
        Self {
            conn,
            select_names: Mutex::new(None),
            select_definition: Mutex::new(None),
            parsed_definitions: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn create(&self, sql: &str) -> Result<Statement> {
        let create = CreateFunction::parse(sql).map_err(limbo_error)?;
        check_schema(&self.conn, create.schema.as_deref())?;
        let function = create.function;
        check_no_built_in_function(&self.conn, &function)?;
        if let Some(existing) = self.find(&function.name)? {
            check_replacement(&existing, &function, create.replace)?;
        }
        self.check_inlining(&function)?;
        if !has_functions_table(&self.conn) {
            self.conn
                .prepare(format!(
                    "CREATE TABLE public.{FUNCTIONS_TABLE} (name text PRIMARY KEY, sql text NOT NULL)"
                ))?
                .run_ignore_rows()?;
        }
        let mut insert = self.conn.prepare(format!(
            "INSERT INTO public.{FUNCTIONS_TABLE} (name, sql) VALUES ($1, $2) \
             ON CONFLICT (name) DO UPDATE SET sql = excluded.sql"
        ))?;
        insert.bind_at(parameter(1), Value::build_text(function.name))?;
        insert.bind_at(parameter(2), Value::build_text(sql.to_string()))?;
        Ok(insert)
    }

    pub(crate) fn inline_calls(
        &self,
        sql: &str,
        parse_result: &ParseResult,
    ) -> Result<Option<String>> {
        let names = self.names_in(sql)?;
        if names.is_empty() {
            return Ok(None);
        }
        turso_pg_parser::sql_function::inline_calls(sql, parse_result, &mut |name| {
            if !names.iter().any(|candidate| candidate == name) {
                return Ok(None);
            }
            self.find(name).map_err(parse_error)
        })
        .map_err(limbo_error)
    }

    fn check_inlining(&self, function: &SqlFunction) -> Result<()> {
        let probe = format!(
            "SELECT 1 WHERE {} IS NULL",
            function.call_with_null_arguments()
        );
        let parse_result = turso_pg_parser::parse(&probe).map_err(limbo_error)?;
        let new_function = Arc::new(function.clone());
        let probe =
            turso_pg_parser::sql_function::inline_calls(&probe, &parse_result, &mut |name| {
                if name == new_function.name {
                    return Ok(Some(new_function.clone()));
                }
                self.find(name).map_err(parse_error)
            })
            .map_err(limbo_error)?
            .expect("the probe calls the new function");
        self.conn.prepare(probe).map_err(|e| {
            LimboError::ParseError(format!(
                "SQL function \"{}\" cannot be inlined: {}",
                function.name,
                message(&e)
            ))
        })?;
        Ok(())
    }

    fn names_in(&self, sql: &str) -> Result<Vec<String>> {
        if !has_functions_table(&self.conn) {
            return Ok(Vec::new());
        }
        let sql = sql.to_ascii_lowercase();
        let rows = run_cached(
            &self.conn,
            &self.select_names,
            &format!("SELECT name FROM public.{FUNCTIONS_TABLE}"),
            &[],
        )?;
        Ok(rows
            .into_iter()
            .filter_map(|row| match row.into_iter().next() {
                Some(Value::Text(name)) => Some(name.as_str().to_string()),
                _ => None,
            })
            .filter(|name| sql.contains(&name.to_ascii_lowercase()))
            .collect())
    }

    fn find(&self, name: &str) -> Result<Option<Arc<SqlFunction>>> {
        if !has_functions_table(&self.conn) {
            return Ok(None);
        }
        let rows = run_cached(
            &self.conn,
            &self.select_definition,
            &format!("SELECT sql FROM public.{FUNCTIONS_TABLE} WHERE name = $1"),
            &[Value::build_text(name.to_string())],
        )?;
        let row = match rows.as_slice() {
            [] => return Ok(None),
            [row] => row,
            _ => unreachable!("function names are the primary key of {FUNCTIONS_TABLE}"),
        };
        let Some(Value::Text(sql)) = row.first() else {
            return Err(LimboError::Corrupt(format!(
                "{FUNCTIONS_TABLE} has no SQL for function \"{name}\""
            )));
        };
        let mut parsed_definitions = self.parsed_definitions.lock().unwrap();
        if let Some((parsed_sql, function)) = parsed_definitions.get(name) {
            if parsed_sql == sql.as_str() {
                return Ok(Some(function.clone()));
            }
        }
        let function = Arc::new(
            CreateFunction::parse(sql.as_str())
                .map_err(limbo_error)?
                .function,
        );
        parsed_definitions.insert(
            name.to_string(),
            (sql.as_str().to_string(), function.clone()),
        );
        Ok(Some(function))
    }
}

fn check_schema(conn: &Connection, schema: Option<&str>) -> Result<()> {
    let schema = match schema {
        Some(schema) => Some(schema.to_string()),
        None => crate::session::current_schema(conn),
    };
    match schema.as_deref() {
        Some("public") => Ok(()),
        Some(schema) => Err(LimboError::ParseError(format!(
            "SQL functions can be created only in schema public, not in schema \"{schema}\""
        ))),
        None => Err(LimboError::ParseError(
            "no schema has been selected to create in".to_string(),
        )),
    }
}

fn check_no_built_in_function(conn: &Arc<Connection>, function: &SqlFunction) -> Result<()> {
    let call = function.call_with_null_arguments();
    for probe in [
        format!("SELECT {call}"),
        format!("SELECT {call} OVER ()"),
        format!("SELECT * FROM {call}"),
    ] {
        if conn.prepare(probe).is_ok() {
            return Err(LimboError::ParseError(format!(
                "function \"{}\" already exists as a built-in function",
                function.name
            )));
        }
    }
    Ok(())
}

fn check_replacement(existing: &SqlFunction, function: &SqlFunction, replace: bool) -> Result<()> {
    let same_argument_types = existing.parameters.len() == function.parameters.len()
        && existing
            .parameters
            .iter()
            .zip(&function.parameters)
            .all(|(old, new)| old.sql_type == new.sql_type);
    if !same_argument_types {
        return Err(LimboError::ParseError(format!(
            "function \"{}\" already exists with different argument types, \
             and overloading is not supported yet",
            function.name
        )));
    }
    if !replace {
        return Err(LimboError::ParseError(format!(
            "function \"{}\" already exists with same argument types",
            function.name
        )));
    }
    if existing.return_type != function.return_type {
        return Err(LimboError::ParseError(
            "cannot change return type of existing function".to_string(),
        ));
    }
    for (old, new) in existing.parameters.iter().zip(&function.parameters) {
        if let Some(old_name) = &old.name {
            if old.name != new.name {
                return Err(LimboError::ParseError(format!(
                    "cannot change name of input parameter \"{old_name}\""
                )));
            }
        }
    }
    Ok(())
}

fn has_functions_table(conn: &Connection) -> bool {
    conn.maybe_update_schema();
    conn.current_schema()
        .get_btree_table(FUNCTIONS_TABLE)
        .is_some()
}

fn run_cached(
    conn: &Arc<Connection>,
    cache: &Mutex<Option<Statement>>,
    sql: &str,
    parameters: &[Value],
) -> Result<Vec<Vec<Value>>> {
    let mut cache = cache.lock().unwrap();
    let statement = match cache.as_mut() {
        Some(statement) => {
            statement.reset()?;
            statement
        }
        None => cache.insert(conn.prepare(sql)?),
    };
    for (index, value) in parameters.iter().enumerate() {
        statement.bind_at(parameter(index + 1), value.clone())?;
    }
    statement.run_collect_rows()
}

fn parameter(index: usize) -> NonZero<usize> {
    NonZero::new(index).expect("parameter indexes start at 1")
}

fn limbo_error(error: ParseError) -> LimboError {
    LimboError::ParseError(error.to_string())
}

fn parse_error(error: LimboError) -> ParseError {
    ParseError::ParseError(message(&error))
}

fn message(error: &LimboError) -> String {
    match error {
        LimboError::ParseError(message) => message.clone(),
        error => error.to_string(),
    }
}
