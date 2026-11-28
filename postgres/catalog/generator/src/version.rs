//! The version of the built-in rows of a catalog.
//!
//! The version is the 32-bit FNV-1a hash of a text that has the
//! `CREATE TABLE` statement, each `CREATE INDEX` statement, and each value
//! of each row, in the order of the table:
//!
//! - the `CREATE TABLE` statement and a line feed,
//! - each `CREATE INDEX` statement and a line feed,
//! - for each row, its values and a line feed. A value is `N,` for NULL,
//!   `I` and the decimal integer and a comma for an integer, `R` and the
//!   `{:?}` text of the `f64` and a comma for a real, and `T`, the length
//!   of the text in bytes, a colon, the text and a comma for a text.
//!
//! The length before each text makes the encoding unambiguous, so two
//! different built-in rows give two different texts. The version thus changes when
//! a value, a row, a column or an index changes, unless two texts have the
//! same hash, which is unlikely. It does not change for a change of the
//! generator that gives the same statements and values.

use crate::SqlValue;

/// The version of the built-in rows of a table that has `create_sql` and `index_sql`, and
/// whose rows are `rows`.
pub(crate) fn version(create_sql: &str, index_sql: &[String], rows: &[Vec<SqlValue>]) -> u32 {
    fnv1a(canonical_text(create_sql, index_sql, rows).as_bytes())
}

/// The text that the version is the hash of. The module documentation
/// describes it.
fn canonical_text(create_sql: &str, index_sql: &[String], rows: &[Vec<SqlValue>]) -> String {
    let mut text = String::new();
    text.push_str(create_sql);
    text.push('\n');
    for sql in index_sql {
        text.push_str(sql);
        text.push('\n');
    }
    for row in rows {
        for value in row {
            match value {
                SqlValue::Null => text.push('N'),
                SqlValue::Integer(integer) => text.push_str(&format!("I{integer}")),
                SqlValue::Real(real) => text.push_str(&format!("R{real:?}")),
                SqlValue::Text(value) => text.push_str(&format!("T{}:{value}", value.len())),
            }
            text.push(',');
        }
        text.push('\n');
    }
    text
}

/// The 32-bit FNV-1a hash of `bytes`.
fn fnv1a(bytes: &[u8]) -> u32 {
    const OFFSET_BASIS: u32 = 0x811c_9dc5;
    const PRIME: u32 = 0x0100_0193;
    bytes.iter().fold(OFFSET_BASIS, |hash, byte| {
        (hash ^ u32::from(*byte)).wrapping_mul(PRIME)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows() -> Vec<Vec<SqlValue>> {
        vec![
            vec![
                SqlValue::Integer(16),
                SqlValue::Text("bool".to_string()),
                SqlValue::Real(0.01),
                SqlValue::Null,
            ],
            vec![
                SqlValue::Integer(17),
                SqlValue::Text("bytea".to_string()),
                SqlValue::Real(1.0),
                SqlValue::Text("{a,b}".to_string()),
            ],
        ]
    }

    const CREATE: &str = "CREATE TABLE t(oid INTEGER PRIMARY KEY, name TEXT, cost REAL, acl TEXT)";

    fn index_sql() -> Vec<String> {
        vec!["CREATE UNIQUE INDEX t_name ON t(name)".to_string()]
    }

    #[test]
    fn fnv1a_has_the_values_of_its_specification() {
        assert_eq!(fnv1a(b""), 0x811c_9dc5);
        assert_eq!(fnv1a(b"a"), 0xe40c_292c);
        assert_eq!(fnv1a(b"foobar"), 0xbf9c_f968);
    }

    #[test]
    fn the_canonical_text_has_each_value_with_its_type() {
        assert_eq!(
            canonical_text(CREATE, &index_sql(), &rows()),
            format!(
                "{CREATE}\nCREATE UNIQUE INDEX t_name ON t(name)\n\
                 I16,T4:bool,R0.01,N,\nI17,T5:bytea,R1.0,T5:{{a,b}},\n"
            )
        );
    }

    #[test]
    fn the_version_is_the_same_for_the_same_rows() {
        assert_eq!(
            version(CREATE, &index_sql(), &rows()),
            version(CREATE, &index_sql(), &rows())
        );
    }

    #[test]
    fn the_version_changes_when_the_rows_change() {
        let base = version(CREATE, &index_sql(), &rows());
        let mut changed_values = Vec::new();

        let mut changed = rows();
        changed[1][1] = SqlValue::Text("byteb".to_string());
        changed_values.push(changed);
        let mut changed = rows();
        changed[0][2] = SqlValue::Real(0.02);
        changed_values.push(changed);
        let mut changed = rows();
        changed[0][3] = SqlValue::Text(String::new());
        changed_values.push(changed);
        // The same value as an integer and as a real.
        let mut changed = rows();
        changed[1][2] = SqlValue::Integer(1);
        changed_values.push(changed);
        // The same values in two other rows.
        let mut changed = rows();
        changed.swap(0, 1);
        changed_values.push(changed);
        let mut changed = rows();
        changed.pop();
        changed_values.push(changed);
        for rows in changed_values {
            assert_ne!(version(CREATE, &index_sql(), &rows), base, "{rows:?}");
        }

        assert_ne!(version(CREATE, &[], &rows()), base);
        let other_create = CREATE.replace("cost REAL", "cost REAL NOT NULL");
        assert_ne!(version(&other_create, &index_sql(), &rows()), base);
    }
}
