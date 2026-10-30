//! Differential SQLSTATE fuzzer: generates a random schema, random data,
//! and random statements from seeded grammars, runs every generated
//! statement against a real PostgreSQL server and against embedded tursopg,
//! and fails on every disagreement — a different SQLSTATE, or an error only
//! one engine raises. Regressions it finds are fixed with a deterministic
//! test in pg-sqltests; the fuzzer itself keeps no corpus and no baseline.
//! The tests skip when PG_DSN is not set, so they run only when asked:
//!
//!   ./postgres/conformance/with-postgres.sh \
//!       cargo test -p turso_pg_server --test sqlstate_differential -- --nocapture
//!
//! Each run picks a fresh seed, prints it, and includes it in failures, so
//! SEED=<n> GEN_CASES=<n> reproduces a run exactly. GEN_CASES sets how many
//! statements are generated after the schema and data phases (default 256),
//! and BASIC_TYPES=1 keeps generated columns and casts to the types that do
//! not go through the custom type system, cutting known custom-type noise.
//! Divergences are reported grouped by the pair of codes the engines chose,
//! with example statements, so one root cause reads as one group.

use std::collections::BTreeMap;

use turso_pg_client::{BackendEvent, ConnParams, PgConn};

#[derive(Debug, Clone, PartialEq, Eq)]
enum Outcome {
    Ok,
    Error { sqlstate: String, message: String },
}

impl Outcome {
    fn code(&self) -> &str {
        match self {
            Outcome::Ok => "ok",
            Outcome::Error { sqlstate, .. } => sqlstate,
        }
    }

    fn message(&self) -> &str {
        match self {
            Outcome::Ok => "",
            Outcome::Error { message, .. } => message,
        }
    }
}

/// One compared execution: setup statements run first on both engines with
/// their errors tolerated (they drive a transaction into a state), then the
/// outcome of `stmt` is compared.
struct Case {
    setup: Vec<String>,
    stmt: String,
}

impl Case {
    fn of(stmt: String) -> Self {
        Self {
            setup: Vec::new(),
            stmt,
        }
    }
}

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }

    fn one_in(&mut self, n: u64) -> bool {
        self.next() % n == 0
    }

    fn ident(&mut self) -> String {
        format!("zz_{}", self.next() % 1_000_000)
    }
}

fn pick<'a>(r: &mut Lcg, options: &[&'a str]) -> &'a str {
    options[(r.next() as usize) % options.len()]
}

fn pick_owned(r: &mut Lcg, options: &[String]) -> String {
    options[(r.next() as usize) % options.len()].clone()
}

fn pick_owned_str<'a>(r: &mut Lcg, options: &[&'a str]) -> &'a str {
    options[(r.next() as usize) % options.len()]
}

const GEN_INTEGERS: &[&str] = &[
    "0",
    "1",
    "-1",
    "2",
    "10",
    "32767",
    "32768",
    "2147483647",
    "2147483648",
    "9223372036854775807",
    "-9223372036854775807",
];
const GEN_FLOATS: &[&str] = &["0.0", "0.5", "2.5", "-2.5", "1e10", "1e308", "1e-300"];
const GEN_STRINGS: &[&str] = &[
    "''",
    "'a'",
    "'1'",
    "'-5'",
    "' 7 '",
    "'1e999'",
    "'x9q'",
    "'2020-01-01'",
    "'2020-13-40'",
    "'12:30:00'",
    "'25:99:00'",
    "'true'",
    "'f'",
    "'{}'",
    "'[1,2,3]'",
    "'NaN'",
    "'inf'",
    "'0x10'",
];
/// The types whose tursopg implementation does not go through the custom
/// type system. BASIC_TYPES=1 restricts generation to these, cutting the
/// noise from the known-broken custom types.
/// FIXME: remove the flag and this split when custom types are production
/// ready.
const BASIC_CAST_TYPES: &[&str] = &[
    "int2", "int4", "int8", "float4", "float8", "numeric", "text",
];
const CUSTOM_CAST_TYPES: &[&str] = &[
    "boolean",
    "json",
    "date",
    "time",
    "timestamp",
    "uuid",
    "interval",
    "varchar",
];
const GEN_BINARY_OPERATORS: &[&str] = &[
    "+", "-", "*", "/", "%", "||", "=", "<", ">", "<=", ">=", "<>", "AND", "OR",
];
const GEN_ONE_ARG_FUNCTIONS: &[&str] = &[
    "abs", "sqrt", "ln", "exp", "floor", "ceil", "round", "length", "upper", "lower", "sign",
    "chr", "ascii",
];
const GEN_TWO_ARG_FUNCTIONS: &[&str] = &["power", "nullif", "coalesce", "mod"];

const TABLE_NAMES: &[&str] = &["ta", "tb", "tc", "td", "te", "tf"];
const COLUMN_NAMES: &[&str] = &["a", "b", "c", "x", "y", "z"];
const BASIC_COLUMN_TYPES: &[&str] = &["int4", "int8", "float8", "text"];
const CUSTOM_COLUMN_TYPES: &[&str] = &[
    "int2",
    "numeric",
    "varchar(3)",
    "boolean",
    "date",
    "timestamp",
    "json",
];

/// The type pools generation draws from: every type by default, only the
/// basic ones under BASIC_TYPES=1.
struct TypePools {
    cast_types: Vec<&'static str>,
    column_types: Vec<&'static str>,
}

impl TypePools {
    fn new(basic_only: bool) -> Self {
        let mut cast_types = BASIC_CAST_TYPES.to_vec();
        let mut column_types = BASIC_COLUMN_TYPES.to_vec();
        if !basic_only {
            cast_types.extend_from_slice(CUSTOM_CAST_TYPES);
            column_types.extend_from_slice(CUSTOM_COLUMN_TYPES);
        }
        Self {
            cast_types,
            column_types,
        }
    }
}

/// A table both engines agreed to create, usable in later statements.
struct GeneratedTable {
    name: String,
    columns: Vec<String>,
}

/// A random CREATE TABLE: names from small pools, so duplicate tables and
/// duplicate columns occur; constraints and foreign keys to earlier tables
/// with a small probability, referencing columns that may not be keys.
fn generated_create_table(
    r: &mut Lcg,
    pools: &TypePools,
    existing: &[GeneratedTable],
) -> (String, GeneratedTable) {
    let name = pick(r, TABLE_NAMES).to_string();
    let column_count = 1 + (r.next() as usize) % 4;
    let mut columns = Vec::new();
    let mut definitions = Vec::new();
    for _ in 0..column_count {
        let column = pick(r, COLUMN_NAMES).to_string();
        let ty = pick_owned_str(r, &pools.column_types);
        let constraint = match r.next() % 10 {
            0 => " NOT NULL".to_string(),
            1 => " UNIQUE".to_string(),
            2 => " PRIMARY KEY".to_string(),
            3 => format!(" CHECK ({column} > 0)"),
            4 => format!(" DEFAULT {}", generated_leaf(r, &[])),
            5 if !existing.is_empty() => {
                let target = &existing[(r.next() as usize) % existing.len()];
                format!(
                    " REFERENCES {}({})",
                    target.name,
                    pick_owned(r, &target.columns)
                )
            }
            _ => String::new(),
        };
        definitions.push(format!("{column} {ty}{constraint}"));
        columns.push(column);
    }
    (
        format!("CREATE TABLE {name} ({})", definitions.join(", ")),
        GeneratedTable { name, columns },
    )
}

/// A random INSERT; the arity is sometimes wrong on purpose.
fn generated_insert(r: &mut Lcg, table: &GeneratedTable) -> String {
    let mut arity = table.columns.len();
    if r.one_in(8) {
        arity += 1;
    } else if arity > 1 && r.one_in(8) {
        arity -= 1;
    }
    let values: Vec<String> = (0..arity).map(|_| generated_leaf(r, &[])).collect();
    format!("INSERT INTO {} VALUES ({})", table.name, values.join(", "))
}

/// A random statement over the generated schema: queries, writes, DDL, and
/// transaction sequences, referencing live tables and columns most of the
/// time and missing names some of the time.
fn generated_statement(r: &mut Lcg, pools: &TypePools, tables: &[GeneratedTable]) -> Case {
    let table = if tables.is_empty() {
        None
    } else {
        Some(&tables[(r.next() as usize) % tables.len()])
    };
    let table_name = match (&table, r.one_in(10)) {
        (Some(table), false) => table.name.clone(),
        _ => r.ident(),
    };
    let columns: Vec<String> = match (&table, r.one_in(6)) {
        (Some(table), false) => table.columns.clone(),
        _ => vec![pick(r, COLUMN_NAMES).to_string()],
    };
    match r.next() % 12 {
        0..=3 => {
            let mut stmt = if r.one_in(6) {
                format!("SELECT {}, count(*)", generated_expr(r, pools, 2, &columns))
            } else {
                format!("SELECT {}", generated_expr(r, pools, 3, &columns))
            };
            if table.is_some() || r.one_in(4) {
                stmt.push_str(&format!(" FROM {table_name}"));
                if r.one_in(2) {
                    stmt.push_str(&format!(" WHERE {}", generated_expr(r, pools, 2, &columns)));
                }
                if r.one_in(4) {
                    stmt.push_str(&format!(" GROUP BY {}", pick_owned(r, &columns)));
                }
                if r.one_in(4) {
                    stmt.push_str(&format!(
                        " ORDER BY {}",
                        generated_expr(r, pools, 1, &columns)
                    ));
                }
                if r.one_in(4) {
                    stmt.push_str(&format!(" LIMIT {}", generated_leaf(r, &[])));
                }
            }
            Case::of(stmt)
        }
        4 => Case::of(format!(
            "UPDATE {table_name} SET {} = {} WHERE {}",
            pick_owned(r, &columns),
            generated_expr(r, pools, 2, &columns),
            generated_expr(r, pools, 2, &columns),
        )),
        5 => Case::of(format!(
            "DELETE FROM {table_name} WHERE {}",
            generated_expr(r, pools, 2, &columns),
        )),
        6 => match table {
            Some(table) => Case::of(generated_insert(r, table)),
            None => Case::of(format!("INSERT INTO {table_name} VALUES (1)")),
        },
        7 => Case::of(match r.next() % 4 {
            0 => format!("DROP TABLE {table_name}"),
            1 => format!(
                "ALTER TABLE {table_name} ADD COLUMN {} {}",
                pick(r, COLUMN_NAMES),
                pick_owned_str(r, &pools.column_types)
            ),
            2 => format!(
                "ALTER TABLE {table_name} DROP COLUMN {}",
                pick_owned(r, &columns)
            ),
            _ => format!(
                "CREATE INDEX {} ON {table_name} ({})",
                r.ident(),
                pick_owned(r, &columns)
            ),
        }),
        _ => {
            let mut setup = vec![pick(r, &["BEGIN", "BEGIN READ ONLY"]).to_string()];
            if r.one_in(2) {
                setup.push(match r.next() % 4 {
                    0 => "SELECT 1/0".to_string(),
                    1 => "SAVEPOINT s1".to_string(),
                    2 => format!("SELECT count(*) FROM {table_name}"),
                    _ => format!("DELETE FROM {table_name} WHERE {}", generated_leaf(r, &[])),
                });
            }
            let stmt = match r.next() % 8 {
                0 => "COMMIT".to_string(),
                1 => "ROLLBACK".to_string(),
                2 => "SAVEPOINT s1".to_string(),
                3 => "RELEASE SAVEPOINT s1".to_string(),
                4 => "ROLLBACK TO SAVEPOINT s1".to_string(),
                5 => "SET TRANSACTION READ WRITE".to_string(),
                6 => format!("SELECT {}", generated_expr(r, pools, 2, &columns)),
                _ => match table {
                    Some(table) => generated_insert(r, table),
                    None => format!("INSERT INTO {table_name} VALUES (1)"),
                },
            };
            Case { setup, stmt }
        }
    }
}

fn generated_expr(r: &mut Lcg, pools: &TypePools, depth: u32, columns: &[String]) -> String {
    if depth == 0 || r.one_in(4) {
        return generated_leaf(r, columns);
    }
    match r.next() % 8 {
        0..=2 => format!(
            "({} {} {})",
            generated_expr(r, pools, depth - 1, columns),
            pick(r, GEN_BINARY_OPERATORS),
            generated_expr(r, pools, depth - 1, columns)
        ),
        3 => format!("(- {})", generated_expr(r, pools, depth - 1, columns)),
        4 => format!("(NOT {})", generated_expr(r, pools, depth - 1, columns)),
        5 => format!(
            "({})::{}",
            generated_expr(r, pools, depth - 1, columns),
            pick_owned_str(r, &pools.cast_types)
        ),
        6 => format!(
            "{}({})",
            pick(r, GEN_ONE_ARG_FUNCTIONS),
            generated_expr(r, pools, depth - 1, columns)
        ),
        _ => format!(
            "{}({}, {})",
            pick(r, GEN_TWO_ARG_FUNCTIONS),
            generated_expr(r, pools, depth - 1, columns),
            generated_expr(r, pools, depth - 1, columns)
        ),
    }
}

fn generated_leaf(r: &mut Lcg, columns: &[String]) -> String {
    match r.next() % if columns.is_empty() { 5 } else { 6 } {
        0 => pick(r, GEN_INTEGERS).to_string(),
        1 => pick(r, GEN_FLOATS).to_string(),
        2 => pick(r, GEN_STRINGS).to_string(),
        3 => "NULL".to_string(),
        4 => pick(r, &["TRUE", "FALSE"]).to_string(),
        _ => pick_owned(r, columns),
    }
}

struct PgSide {
    params: ConnParams,
}

impl PgSide {
    fn run_case(&self, c: &Case) -> Result<Outcome, String> {
        let mut conn = PgConn::connect(&self.params, &[]).map_err(|e| format!("{e:?}"))?;
        for s in &c.setup {
            let _ = run_pg(&mut conn, s)?;
        }
        run_pg(&mut conn, &c.stmt)
    }
}

fn run_pg(conn: &mut PgConn, sql: &str) -> Result<Outcome, String> {
    let events = conn.simple_query(sql).map_err(|e| format!("{e:?}"))?;
    for event in events {
        if let BackendEvent::ErrorResponse(fields) = event {
            return Ok(Outcome::Error {
                sqlstate: fields.get(&b'C').cloned().unwrap_or_default(),
                message: fields.get(&b'M').cloned().unwrap_or_default(),
            });
        }
    }
    Ok(Outcome::Ok)
}

struct TursoSide {
    db: std::sync::Arc<turso_pg::Database>,
}

impl TursoSide {
    fn run_case(&self, c: &Case) -> Result<Outcome, String> {
        let conn = turso_pg::PgConnection::connect(&self.db).map_err(|e| format!("{e:?}"))?;
        for s in &c.setup {
            let _ = conn.execute(s);
        }
        Ok(match conn.execute(&c.stmt) {
            Ok(()) => Outcome::Ok,
            Err(e) => Outcome::Error {
                sqlstate: e.sqlstate.to_string(),
                message: e.message.clone(),
            },
        })
    }
}

fn pick_seed() -> u64 {
    match std::env::var("SEED") {
        Ok(seed) => seed.parse().expect("SEED must be a number"),
        Err(_) => std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64,
    }
}

/// Divergences keyed by the pair of codes the engines chose, so one root
/// cause reads as one group.
#[derive(Default)]
struct Report {
    agree: usize,
    divergences: BTreeMap<(String, String), Vec<String>>,
}

impl Report {
    fn record(&mut self, case: &Case, pg: &Outcome, turso: &Outcome) -> bool {
        if pg.code() == turso.code() {
            self.agree += 1;
            return true;
        }
        self.divergences
            .entry((pg.code().to_string(), turso.code().to_string()))
            .or_default()
            .push(format!(
                "{}\n      postgres: {}\n      tursopg:  {}",
                case.stmt,
                pg.message(),
                turso.message()
            ));
        false
    }

    fn total_divergences(&self) -> usize {
        self.divergences.values().map(Vec::len).sum()
    }
}

#[test]
fn sqlstate_differential_sweep() {
    let Ok(dsn) = std::env::var("PG_DSN") else {
        eprintln!("PG_DSN not set; skipping differential sqlstate sweep");
        return;
    };
    let seed = pick_seed();
    let statement_count: usize = match std::env::var("GEN_CASES") {
        Ok(count) => count.parse().expect("GEN_CASES must be a number"),
        Err(_) => 256,
    };
    let basic_types = std::env::var("BASIC_TYPES").is_ok_and(|v| v == "1");
    let pools = TypePools::new(basic_types);
    println!("seed: {seed}");
    println!(
        "rerun: SEED={seed} GEN_CASES={statement_count} BASIC_TYPES={} \
         ./postgres/conformance/with-postgres.sh \
         cargo test -p turso_pg_server --test sqlstate_differential -- --nocapture",
        basic_types as u8
    );

    let mut params = ConnParams::parse(&dsn).unwrap();
    let mut admin = PgConn::connect(&params, &[]).unwrap();
    let _ = run_pg(&mut admin, "DROP DATABASE IF EXISTS sqlstate_sweep").unwrap();
    if let Outcome::Error { message, .. } =
        run_pg(&mut admin, "CREATE DATABASE sqlstate_sweep").unwrap()
    {
        panic!("could not create sweep database: {message}");
    }
    params.database = "sqlstate_sweep".to_string();
    let pg = PgSide { params };

    let (_io, db) = turso_pg::open_database(
        ":memory:",
        None,
        turso_pg::OpenFlags::default(),
        turso_pg::DatabaseOpts::new(),
    )
    .unwrap();
    let turso = TursoSide { db };

    let mut report = Report::default();
    let run_pair = |case: &Case, report: &mut Report| -> bool {
        let pg_out = case_outcome(pg.run_case(case), case, seed, "postgres");
        let turso_out = case_outcome(turso.run_case(case), case, seed, "tursopg");
        report.record(case, &pg_out, &turso_out)
    };

    let mut r = Lcg(seed);
    let mut tables: Vec<GeneratedTable> = Vec::new();
    for _ in 0..12 {
        let (sql, table) = generated_create_table(&mut r, &pools, &tables);
        if run_pair(&Case::of(sql), &mut report) {
            tables.push(table);
        }
    }
    let mut inserts = Vec::new();
    for table in &tables {
        for _ in 0..2 {
            inserts.push(generated_insert(&mut r, table));
        }
    }
    for insert in inserts {
        run_pair(&Case::of(insert), &mut report);
    }
    for _ in 0..statement_count {
        let case = generated_statement(&mut r, &pools, &tables);
        run_pair(&case, &mut report);
    }

    println!(
        "\n{} cases agree, {} tables usable",
        report.agree,
        tables.len()
    );
    if report.divergences.is_empty() {
        return;
    }
    let mut groups: Vec<_> = report.divergences.iter().collect();
    groups.sort_by_key(|(_, examples)| std::cmp::Reverse(examples.len()));
    println!("\ndivergences by (postgres, tursopg) code pair:");
    for ((pg_code, turso_code), examples) in &groups {
        println!(
            "\n{} x postgres={pg_code} tursopg={turso_code}",
            examples.len()
        );
        for example in examples.iter().take(3) {
            println!("    {example}");
        }
    }
    panic!(
        "{} divergence(s) from PostgreSQL in {} group(s); rerun with SEED={seed} \
         GEN_CASES={statement_count} BASIC_TYPES={}. Fix the root cause and pin it \
         with a test in pg-sqltests.",
        report.total_divergences(),
        groups.len(),
        basic_types as u8
    );
}

fn case_outcome(result: Result<Outcome, String>, case: &Case, seed: u64, engine: &str) -> Outcome {
    result.unwrap_or_else(|e| {
        panic!(
            "{engine} side failed on {:?}: {e} (seed: {seed})",
            case.stmt
        )
    })
}

mod raw_pg {
    use std::io::{Read, Write};
    use std::net::TcpStream;

    pub struct RawPg {
        stream: TcpStream,
    }

    #[derive(Debug)]
    pub enum Msg {
        ParseComplete,
        BindComplete,
        DataRow,
        PortalSuspended,
        CommandComplete(String),
        Error { code: String, message: String },
        Notice(String),
        ReadyForQuery,
        Other,
    }

    impl RawPg {
        pub fn connect(host: &str, port: u16, user: &str, database: &str) -> Self {
            let mut stream = TcpStream::connect((host, port)).unwrap();
            let mut params = Vec::new();
            params.extend_from_slice(&196608i32.to_be_bytes());
            for (k, v) in [("user", user), ("database", database)] {
                params.extend_from_slice(k.as_bytes());
                params.push(0);
                params.extend_from_slice(v.as_bytes());
                params.push(0);
            }
            params.push(0);
            stream
                .write_all(&((params.len() + 4) as i32).to_be_bytes())
                .unwrap();
            stream.write_all(&params).unwrap();
            let mut pg = Self { stream };
            loop {
                if let Msg::ReadyForQuery = pg.read() {
                    break;
                }
            }
            pg
        }

        pub fn send(&mut self, tag: u8, body: &[u8]) {
            self.stream.write_all(&[tag]).unwrap();
            self.stream
                .write_all(&((body.len() + 4) as i32).to_be_bytes())
                .unwrap();
            self.stream.write_all(body).unwrap();
            self.stream.flush().unwrap();
        }

        pub fn parse(&mut self, name: &str, sql: &str) {
            let mut b = Vec::new();
            b.extend_from_slice(name.as_bytes());
            b.push(0);
            b.extend_from_slice(sql.as_bytes());
            b.push(0);
            b.extend_from_slice(&0i16.to_be_bytes());
            self.send(b'P', &b);
        }

        pub fn bind(&mut self, portal: &str, stmt: &str) {
            let mut b = Vec::new();
            b.extend_from_slice(portal.as_bytes());
            b.push(0);
            b.extend_from_slice(stmt.as_bytes());
            b.push(0);
            b.extend_from_slice(&0i16.to_be_bytes());
            b.extend_from_slice(&0i16.to_be_bytes());
            b.extend_from_slice(&0i16.to_be_bytes());
            self.send(b'B', &b);
        }

        pub fn execute(&mut self, portal: &str, max_rows: i32) {
            let mut b = Vec::new();
            b.extend_from_slice(portal.as_bytes());
            b.push(0);
            b.extend_from_slice(&max_rows.to_be_bytes());
            self.send(b'E', &b);
        }

        pub fn flush(&mut self) {
            self.send(b'H', &[]);
        }

        pub fn sync(&mut self) {
            self.send(b'S', &[]);
        }

        pub fn read(&mut self) -> Msg {
            let mut header = [0u8; 5];
            self.stream.read_exact(&mut header).unwrap();
            let len = u32::from_be_bytes(header[1..5].try_into().unwrap()) as usize;
            let mut body = vec![0u8; len - 4];
            self.stream.read_exact(&mut body).unwrap();
            match header[0] {
                b'1' => Msg::ParseComplete,
                b'2' => Msg::BindComplete,
                b'D' => Msg::DataRow,
                b's' => Msg::PortalSuspended,
                b'C' => Msg::CommandComplete(
                    String::from_utf8_lossy(&body[..body.len() - 1]).to_string(),
                ),
                b'E' | b'N' => {
                    let mut code = String::new();
                    let mut message = String::new();
                    let mut i = 0;
                    while i < body.len() && body[i] != 0 {
                        let field = body[i];
                        let end = body[i + 1..].iter().position(|&b| b == 0).unwrap() + i + 1;
                        let val = String::from_utf8_lossy(&body[i + 1..end]).to_string();
                        if field == b'C' {
                            code = val;
                        } else if field == b'M' {
                            message = val;
                        }
                        i = end + 1;
                    }
                    if header[0] == b'E' {
                        Msg::Error { code, message }
                    } else {
                        Msg::Notice(message)
                    }
                }
                b'Z' => Msg::ReadyForQuery,
                _ => Msg::Other,
            }
        }

        /// Reads until the next CommandComplete, PortalSuspended, or Error.
        pub fn read_outcome(&mut self) -> Msg {
            loop {
                match self.read() {
                    Msg::CommandComplete(t) => return Msg::CommandComplete(t),
                    Msg::PortalSuspended => return Msg::PortalSuspended,
                    Msg::Error { code, message } => return Msg::Error { code, message },
                    Msg::Notice(n) => println!("  pg notice: {n}"),
                    _ => {}
                }
            }
        }
    }
}

/// PostgreSQL checks the access mode when a command starts executing, so a
/// write portal suspended mid-INSERT keeps writing after the transaction
/// becomes READ ONLY, and so does tursopg. The frontend's statement-start
/// enforcement and the VDBE write backstop's captured-at-start mode exist
/// to preserve this exact behavior.
#[test]
fn suspended_write_portal_keeps_writing_after_begin_read_only() {
    use raw_pg::{Msg, RawPg};

    let Ok(dsn) = std::env::var("PG_DSN") else {
        eprintln!("PG_DSN not set; skipping probe");
        return;
    };
    let params = ConnParams::parse(&dsn).unwrap();
    let mut admin = PgConn::connect(&params, &[]).unwrap();
    let _ = run_pg(&mut admin, "DROP DATABASE IF EXISTS absorb_probe").unwrap();
    run_pg(&mut admin, "CREATE DATABASE absorb_probe").unwrap();

    let mut pg = RawPg::connect(&params.host, params.port, &params.user, "absorb_probe");
    pg.parse("", "CREATE TABLE t9 (a int)");
    pg.bind("", "");
    pg.execute("", 0);
    pg.sync();
    loop {
        if let Msg::ReadyForQuery = pg.read() {
            break;
        }
    }

    pg.parse(
        "s1",
        "INSERT INTO t9 VALUES (1), (2), (3), (4), (5) RETURNING a",
    );
    pg.bind("p1", "s1");
    pg.execute("p1", 1);
    pg.flush();
    assert!(
        matches!(pg.read_outcome(), Msg::PortalSuspended),
        "INSERT portal must suspend after one row"
    );

    pg.parse("", "BEGIN READ ONLY");
    pg.bind("", "");
    pg.execute("", 0);
    pg.flush();
    assert!(
        matches!(pg.read_outcome(), Msg::CommandComplete(tag) if tag == "BEGIN"),
        "PostgreSQL accepts BEGIN READ ONLY while a write portal is suspended"
    );

    pg.execute("p1", 0);
    pg.flush();
    assert!(
        matches!(pg.read_outcome(), Msg::CommandComplete(tag) if tag == "INSERT 0 4"),
        "PostgreSQL keeps writing through the resumed portal"
    );

    pg.parse("", "COMMIT");
    pg.bind("", "");
    pg.execute("", 0);
    pg.sync();
    assert!(matches!(
        pg.read_outcome(),
        Msg::CommandComplete(tag) if tag == "COMMIT"
    ));
    loop {
        if let Msg::ReadyForQuery = pg.read() {
            break;
        }
    }
    let mut check = PgConn::connect(
        &ConnParams {
            database: "absorb_probe".to_string(),
            ..params
        },
        &[],
    )
    .unwrap();
    let mut pg_rows = None;
    for event in check.simple_query("SELECT count(*) FROM t9").unwrap() {
        if let BackendEvent::DataRow(cols) = event {
            pg_rows = cols[0].clone();
        }
    }
    assert_eq!(pg_rows.as_deref(), Some("5"));

    let (_io, db) = turso_pg::open_database(
        ":memory:",
        None,
        turso_pg::OpenFlags::default(),
        turso_pg::DatabaseOpts::new(),
    )
    .unwrap();
    let conn = turso_pg::PgConnection::connect(&db).unwrap();
    conn.execute("CREATE TABLE t9 (a int)").unwrap();
    let mut stmt = conn
        .prepare("INSERT INTO t9 VALUES (1), (2), (3), (4), (5) RETURNING a")
        .unwrap();
    assert!(
        stmt.run_one_step_blocking(|| Ok(()), || Ok(()))
            .unwrap()
            .is_some(),
        "INSERT must suspend after one returned row"
    );
    conn.execute("BEGIN READ ONLY")
        .expect("BEGIN READ ONLY must be accepted while a write statement is in flight");
    let mut resumed_rows = 0;
    loop {
        match stmt.run_one_step_blocking(|| Ok(()), || Ok(())) {
            Ok(Some(_)) => resumed_rows += 1,
            Ok(None) => break,
            Err(e) => panic!(
                "resumed INSERT must keep writing, got {} {}",
                e.sqlstate, e.message
            ),
        }
    }
    assert_eq!(resumed_rows, 4);
    drop(stmt);
    conn.execute("COMMIT").unwrap();
    let mut count = conn.prepare("SELECT count(*) FROM t9").unwrap();
    assert_eq!(
        count.run_collect_rows().unwrap(),
        vec![vec![turso_pg::types::Value::from_i64(5)]]
    );
}
