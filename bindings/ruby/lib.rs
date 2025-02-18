extern crate rutie;
extern crate limbo_core;

use rutie::{Class, Object, RString, VM};
use limbo_core::{Connection, Database, LimboError};
use std::rc::Rc;
use std::sync::Arc;

class!(RubyConnection);

methods!(
    RubyConnection,
    _itself,

    fn ruby_connect(path: RString) -> RubyConnection {
        let path = path.unwrap().to_str();
        let db_options = parse_query_str(path);
        if let Ok(io) = get_io(&db_options.path) {
            let db = Database::open_file(io.clone(), &db_options.path.to_string());
            match db {
                Ok(db) => {
                    let conn = db.connect();
                    RubyConnection::new(conn, io)
                }
                Err(e) => {
                    VM::raise(Class::from_existing("StandardError"), &format!("Error opening database: {}", e));
                    RubyConnection::new(Rc::new(Connection::default()), Arc::new(limbo_core::MemoryIO::new().unwrap()))
                }
            }
        } else {
            VM::raise(Class::from_existing("StandardError"), "Error getting IO");
            RubyConnection::new(Rc::new(Connection::default()), Arc::new(limbo_core::MemoryIO::new().unwrap()))
        }
    }

    fn ruby_execute(sql: RString) -> RString {
        let sql = sql.unwrap().to_str();
        let conn = _itself.get_data_mut::<RubyConnection>();
        let stmt = conn.conn.prepare(sql).unwrap();
        let mut result = String::new();
        loop {
            match stmt.step() {
                Ok(limbo_core::StepResult::Row) => {
                    let row = stmt.row().unwrap();
                    for value in row.get_values() {
                        result.push_str(&format!("{:?} ", value));
                    }
                    result.push('\n');
                }
                Ok(limbo_core::StepResult::Done) => break,
                Ok(limbo_core::StepResult::IO) => {
                    conn.io.run_once().unwrap();
                }
                Ok(limbo_core::StepResult::Interrupt) => break,
                Ok(limbo_core::StepResult::Busy) => break,
                Err(e) => {
                    VM::raise(Class::from_existing("StandardError"), &format!("Error executing statement: {}", e));
                    break;
                }
            }
        }
        RString::new_utf8(&result)
    }
);

impl RubyConnection {
    fn new(conn: Rc<Connection>, io: Arc<dyn limbo_core::IO>) -> Self {
        RubyConnection {
            conn,
            io,
        }
    }
}

fn get_io(db_location: &DbType) -> Result<Arc<dyn limbo_core::IO>, LimboError> {
    Ok(match db_location {
        DbType::Memory => Arc::new(limbo_core::MemoryIO::new()?),
        _ => {
            return Ok(Arc::new(limbo_core::PlatformIO::new()?));
        }
    })
}

struct DbOptions {
    path: DbType,
    params: Parameters,
}

#[derive(Default, Debug, Clone)]
enum DbType {
    File(String),
    #[default]
    Memory,
}

impl std::fmt::Display for DbType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbType::File(path) => write!(f, "{}", path),
            DbType::Memory => write!(f, ":memory:"),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct Parameters {
    mode: Mode,
    cache: Option<Cache>,
    vfs: Option<String>,
    nolock: bool,
    immutable: bool,
    modeof: Option<String>,
}

impl std::str::FromStr for Parameters {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if !s.contains('?') {
            return Ok(Parameters::default());
        }
        let mut params = Parameters::default();
        for param in s.split('?').nth(1).unwrap().split('&') {
            let mut kv = param.split('=');
            match kv.next() {
                Some("mode") => params.mode = kv.next().unwrap().parse().unwrap(),
                Some("cache") => params.cache = Some(kv.next().unwrap().parse().unwrap()),
                Some("vfs") => params.vfs = Some(kv.next().unwrap().to_string()),
                Some("nolock") => params.nolock = true,
                Some("immutable") => params.immutable = true,
                Some("modeof") => params.modeof = Some(kv.next().unwrap().to_string()),
                _ => {}
            }
        }
        Ok(params)
    }
}

#[derive(Default, Debug, Clone, Copy)]
enum Cache {
    Shared,
    #[default]
    Private,
}

impl std::str::FromStr for Cache {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "shared" => Ok(Cache::Shared),
            _ => Ok(Cache::Private),
        }
    }
}

#[derive(Default, Debug, Clone, Copy)]
enum Mode {
    ReadOnly,
    ReadWrite,
    #[default]
    ReadWriteCreate,
}

impl std::str::FromStr for Mode {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "readonly" | "ro" => Ok(Mode::ReadOnly),
            "readwrite" | "rw" => Ok(Mode::ReadWrite),
            "readwritecreate" | "rwc" => Ok(Mode::ReadWriteCreate),
            _ => Ok(Mode::default()),
        }
    }
}

fn parse_query_str(mut path: &str) -> DbOptions {
    if path == ":memory:" {
        return DbOptions {
            path: DbType::Memory,
            params: Parameters::default(),
        };
    }
    if path.starts_with("sqlite://") {
        path = &path[10..];
    }
    if path.contains('?') {
        let parameters = Parameters::from_str(path).unwrap();
        let path = &path[..path.find('?').unwrap()];
        DbOptions {
            path: DbType::File(path.to_string()),
            params: parameters,
        }
    } else {
        DbOptions {
            path: DbType::File(path.to_string()),
            params: Parameters::default(),
        }
    }
}
