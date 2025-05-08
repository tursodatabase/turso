#![no_main]
use core::fmt;
use std::{error::Error, num::NonZero, sync::Arc};

use arbitrary::Arbitrary;
use libfuzzer_sys::{fuzz_target, Corpus};
use limbo_core::{Value, IO as _};

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
    enum Binary {
        // TODO: Not compatible yet
        // Equal => "=",
        // Is => "IS",
        // Concat => "||",
        // NotEqual => "<>",
        // GreaterThan => ">",
        // GreaterThanOrEqual => ">=",
        // LessThan => "<",
        // LessThanOrEqual => "<=",
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
    enum Unary {
        Not => "NOT",
        BitwiseNot => "~",
        Negative => "-",
        Positive => "+",
    }
}

#[derive(Arbitrary, Debug, Clone)]
enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl From<Value> for limbo_core::Value {
    fn from(value: Value) -> limbo_core::Value {
        match value {
            Value::Null => limbo_core::Value::Null,
            Value::Integer(v) => limbo_core::Value::Integer(v),
            Value::Real(v) => {
                if v.is_nan() {
                    limbo_core::Value::Null
                } else {
                    limbo_core::Value::Float(v)
                }
            }
            Value::Text(v) => limbo_core::Value::from_text(&v),
            Value::Blob(v) => limbo_core::Value::from_blob(v.to_owned()),
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

#[derive(Debug, Arbitrary)]
enum Expr {
    Value(Value),
    Binary(Binary, Box<Expr>, Box<Expr>),
    Unary(Unary, Box<Expr>),
}

#[derive(Debug)]
struct Output {
    query: String,
    parameters: Vec<Value>,
    depth: usize,
}

impl Expr {
    pub fn lower(&self) -> Output {
        match self {
            Expr::Value(value) => Output {
                query: "?".to_string(),
                parameters: vec![value.clone()],
                depth: 0,
            },
            Expr::Unary(op, expr) => {
                let expr = expr.lower();
                Output {
                    query: format!("{op} ({})", expr.query),
                    parameters: expr.parameters,
                    depth: expr.depth + 1,
                }
            }
            Expr::Binary(op, lhs, rhs) => {
                let mut lhs = lhs.lower();
                let mut rhs = rhs.lower();
                Output {
                    query: format!("({}) {op} ({})", lhs.query, rhs.query),
                    parameters: {
                        lhs.parameters.append(&mut rhs.parameters);
                        lhs.parameters
                    },
                    depth: lhs.depth.max(rhs.depth) + 1,
                }
            }
        }
    }
}

fn do_fuzz(expr: Expr) -> Result<Corpus, Box<dyn Error>> {
    let expr = expr.lower();
    let sql = format!("SELECT {}", expr.query);

    // FIX: `limbo_core::translate::expr::translate_expr` causes a overflow if this is any higher.
    if expr.depth > 140 {
        return Ok(Corpus::Reject);
    }

    let expected = {
        let conn = rusqlite::Connection::open_in_memory()?;
        conn.query_row(
            &sql,
            rusqlite::params_from_iter(expr.parameters.iter()),
            |row| row.get::<_, Value>(0),
        )?
    };

    let found = 'value: {
        let io = Arc::new(limbo_core::MemoryIO::new());
        let db = limbo_core::Database::open_file(io.clone(), ":memory:", true)?;
        let conn = db.connect()?;

        let mut stmt = conn.prepare(sql)?;
        for (idx, value) in expr.parameters.iter().enumerate() {
            stmt.bind_at(NonZero::new(idx + 1).unwrap(), value.clone().into())
        }
        loop {
            use limbo_core::StepResult;
            match stmt.step()? {
                StepResult::IO => io.run_once()?,
                StepResult::Row => {
                    let row = stmt.row().unwrap();
                    assert_eq!(row.len(), 1, "expr: {:?}", expr);
                    break 'value row.get_value(0).clone();
                }
                _ => unreachable!(),
            }
        }
    };

    assert_eq!(
        Value::from(expected.clone()),
        found.clone(),
        "with expression {:?}",
        expr,
    );

    Ok(Corpus::Keep)
}

fuzz_target!(|expr: Expr| -> Corpus { do_fuzz(expr).unwrap_or(Corpus::Keep) });
