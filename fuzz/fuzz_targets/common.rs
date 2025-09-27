use arbitrary::Arbitrary;
use rusqlite::fallible_iterator::FallibleIterator;
use std::{fmt, num::NonZero, sync::Arc};

macro_rules! str_enum {
    ($vis:vis enum $name:ident { $($variant:ident => $value:literal),*, }) => {
        #[derive(PartialEq, Debug, Copy, Clone, Arbitrary)]
        $vis enum $name {
            $($variant),*
        }

        impl $name {
            pub fn to_str(self) -> &'static str {
                match self {
                    $($name::$variant => $value),*
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.to_str())
            }
        }
    };
}

str_enum! {
    pub enum Binary {
        Equal => "=",
        Is => "IS",
        Concat => "||",
        NotEqual => "<>",
        GreaterThan => ">",
        GreaterThanOrEqual => ">=",
        LessThan => "<",
        LessThanOrEqual => "<=",
        RightShift => ">>",
        LeftShift => "<<",
        BitwiseAnd => "&",
        BitwiseOr => "|",
        And => "AND",
        Or => "OR",
        Add => "+",
        Subtract => "-",
        Multiply => "*",
        Divide => "/",
        Mod => "%",
    }
}

str_enum! {
    pub enum Unary {
        Not => "NOT",
        BitwiseNot => "~",
        Negative => "-",
        Positive => "+",
    }
}

#[derive(Arbitrary, Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl PartialEq<Value> for turso_core::Value {
    fn eq(&self, other: &Value) -> bool {
        *self == turso_core::Value::from(other)
    }
}

impl PartialEq<turso_core::Value> for Value {
    fn eq(&self, other: &turso_core::Value) -> bool {
        turso_core::Value::from(self) == *other
    }
}

impl From<Value> for turso_core::Value {
    fn from(value: Value) -> turso_core::Value {
        (&value).into()
    }
}

impl From<&turso_core::Value> for Value {
    fn from(value: &turso_core::Value) -> Value {
        match value {
            turso_core::Value::Null => Value::Null,
            turso_core::Value::Integer(v) => Value::Integer(*v),
            turso_core::Value::Float(v) => Value::Real(*v),
            turso_core::Value::Text(v) => Value::Text(v.to_string()),
            turso_core::Value::Blob(v) => Value::Blob(v.clone()),
        }
    }
}

impl From<&Value> for turso_core::Value {
    fn from(value: &Value) -> turso_core::Value {
        match value {
            Value::Null => turso_core::Value::Null,
            Value::Integer(v) => turso_core::Value::Integer(*v),
            Value::Real(v) => {
                if v.is_nan() {
                    turso_core::Value::Null
                } else {
                    turso_core::Value::Float(*v)
                }
            }
            Value::Text(v) => turso_core::Value::from_text(v),
            Value::Blob(v) => turso_core::Value::from_blob(v.to_owned()),
        }
    }
}

impl rusqlite::ToSql for Value {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        use rusqlite::types::ToSqlOutput;

        Ok(match self {
            Value::Null => ToSqlOutput::Owned(rusqlite::types::Value::Null),
            Value::Integer(v) => ToSqlOutput::Owned(rusqlite::types::Value::Integer(*v)),
            Value::Real(v) => ToSqlOutput::Owned(rusqlite::types::Value::Real(*v)),
            Value::Text(v) => ToSqlOutput::Owned(rusqlite::types::Value::Text(v.to_owned())),
            Value::Blob(v) => ToSqlOutput::Owned(rusqlite::types::Value::Blob(v.to_owned())),
        })
    }
}

impl rusqlite::types::FromSql for Value {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        Ok(match value {
            rusqlite::types::ValueRef::Null => Value::Null,
            rusqlite::types::ValueRef::Integer(v) => Value::Integer(v),
            rusqlite::types::ValueRef::Real(v) => Value::Real(v),
            rusqlite::types::ValueRef::Text(v) => {
                Value::Text(String::from_utf8_lossy(v).to_string())
            }
            rusqlite::types::ValueRef::Blob(v) => Value::Blob(v.to_vec()),
        })
    }
}

str_enum! {
    pub enum UnaryFunc {
        Round => "round",
        Hex => "hex",
        Unhex => "unhex",
        Abs => "abs",
        Lower => "lower",
        Upper => "upper",
        Sign => "sign",

        Ceil => "ceil",
        Floor => "floor",
        Trunc => "trunc",
        Radians => "radians",
        Degrees => "degrees",

        Sqrt => "sqrt",
        Exp => "exp",
        Ln => "ln",
        Log10 => "log10",
        Log2 => "log2",

        Sin => "sin",
        Sinh => "sinh",
        Asin => "asin",
        Asinh => "asinh",

        Cos => "cos",
        Cosh => "cosh",
        Acos => "acos",
        Acosh => "acosh",

        Tan => "tan",
        Tanh => "tanh",
        Atan => "atan",
        Atanh => "atanh",
    }
}

str_enum! {
    pub enum BinaryFunc {
        Round => "round",
        Power => "pow",
        Mod => "mod",
        Atan2 => "atan2",
        Log => "log",
    }
}

str_enum! {
    pub enum CastType {
        Text => "text",
        Real => "real",
        Integer => "integer",
        Numeric => "numeric",
    }
}

#[derive(Debug)]
pub struct Output {
    pub query: String,
    pub parameters: Vec<Value>,
    pub depth: usize,
}

pub struct FuzzDatabase {
    pub sqlite: rusqlite::Connection,
    pub turso: Arc<turso_core::Connection>,
}

impl FuzzDatabase {
    pub fn new() -> Self {
        let sqlite = rusqlite::Connection::open_in_memory().unwrap();

        let io = Arc::new(turso_core::MemoryIO::new());
        let turso_db =
            turso_core::Database::open_file(io.clone(), ":memory:", false, true).unwrap();
        let turso = turso_db.connect().unwrap();

        Self { sqlite, turso }
    }

    pub fn execute(&self, sql: &str) {
        self.sqlite.execute_batch(sql).unwrap();
        self.turso.execute(sql).unwrap();
    }

    pub fn assert(&self, sql: &str, parameters: &[Value]) {
        let expected = (|| -> Result<Vec<Vec<Value>>, Box<dyn std::error::Error>> {
            let mut stmt = self.sqlite.prepare(sql)?;

            let column_count = stmt.column_count();
            let rows = stmt.query(rusqlite::params_from_iter(parameters.iter()))?;

            Ok(rows
                .map(|row| {
                    Ok((0..column_count)
                        .map(|i| row.get_unwrap::<_, Value>(i))
                        .collect())
                })
                .collect()?)
        })();

        let found = (|| -> Result<Vec<Vec<Value>>, Box<dyn std::error::Error>> {
            let mut rows = vec![];

            let mut stmt = self.turso.prepare(sql)?;
            for (idx, value) in parameters.iter().enumerate() {
                stmt.bind_at(NonZero::new(idx + 1).unwrap(), value.clone().into())
            }

            loop {
                use turso_core::StepResult;
                match stmt.step()? {
                    StepResult::IO => stmt.run_once()?,
                    StepResult::Row => {
                        let row = stmt.row().unwrap();
                        rows.push(
                            (0..row.len())
                                .map(|i| Value::from(row.get_value(i)))
                                .collect::<Vec<_>>(),
                        );
                    }
                    StepResult::Done => break,
                    _ => unreachable!(),
                }
            }

            Ok(rows)
        })();

        match (expected, found) {
            (Ok(expected), Ok(found)) => assert_eq!(expected, found),
            (e1, e2) => {
                dbg!((e1, e2));
                panic!();
            }
        }
    }
}
