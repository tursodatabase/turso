use asserting::prelude::*;
use asserting::spec::{DebugRepresentation, FailingStrategy, Spec};

#[derive(Debug, Clone, PartialEq)]
pub enum Cell {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

pub const NULL: Cell = Cell::Null;

impl From<i64> for Cell {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<f64> for Cell {
    fn from(value: f64) -> Self {
        Self::Real(value)
    }
}

impl From<&str> for Cell {
    fn from(value: &str) -> Self {
        Self::Text(value.to_owned())
    }
}

impl From<String> for Cell {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<Vec<u8>> for Cell {
    fn from(value: Vec<u8>) -> Self {
        Self::Blob(value)
    }
}

pub trait SqlValue {
    fn matches(&self, cell: &Cell) -> bool;
}

impl SqlValue for rusqlite::types::Value {
    fn matches(&self, cell: &Cell) -> bool {
        match (self, cell) {
            (Self::Null, Cell::Null) => true,
            (Self::Integer(a), Cell::Integer(e)) => a == e,
            (Self::Real(a), Cell::Real(e)) => a == e,
            (Self::Text(a), Cell::Text(e)) => a == e,
            (Self::Blob(a), Cell::Blob(e)) => a == e,
            _ => false,
        }
    }
}

impl SqlValue for turso_core::Value {
    fn matches(&self, cell: &Cell) -> bool {
        match (self, cell) {
            (Self::Null, Cell::Null) => true,
            (Self::Numeric(turso_core::Numeric::Integer(a)), Cell::Integer(e)) => a == e,
            (Self::Numeric(turso_core::Numeric::Float(a)), Cell::Real(e)) => f64::from(*a) == *e,
            (Self::Text(a), Cell::Text(e)) => a.as_str() == e,
            (Self::Blob(a), Cell::Blob(e)) => a.as_slice() == e.as_slice(),
            _ => false,
        }
    }
}

impl PartialEq<Cell> for rusqlite::types::Value {
    fn eq(&self, cell: &Cell) -> bool {
        self.matches(cell)
    }
}

impl PartialEq<Cell> for turso_core::Value {
    fn eq(&self, cell: &Cell) -> bool {
        self.matches(cell)
    }
}

#[macro_export]
macro_rules! row {
    ($($cell:expr),* $(,)?) => {
        vec![$($crate::assertions::Cell::from($cell)),*]
    };
}

pub trait AssertColumn<'a, V> {
    type Column;

    fn column(self, index: usize) -> Self::Column;
}

impl<'a, V, R> AssertColumn<'a, V> for Spec<'a, Vec<Vec<V>>, DebugRepresentation, R>
where
    V: Clone,
    R: FailingStrategy,
{
    type Column = Spec<'a, Vec<V>, DebugRepresentation, R>;

    fn column(mut self, index: usize) -> Self::Column {
        let rows = self.subject().clone();
        if let Some(short) = rows.iter().position(|r| index >= r.len()) {
            let expression = self.expression().to_string();
            self.do_fail_with_message(format!(
                "expected every row of {expression} to have a column at index {index}\n   but was: row {short} has {} columns",
                rows[short].len(),
            ));
        }
        let picked: Vec<V> = rows.iter().filter_map(|r| r.get(index).cloned()).collect();
        self.mapping(move |_| picked)
            .named(format!("column {index}"))
    }
}

pub trait PlanDetails {
    fn plan_details(&self) -> Vec<String>;
}

impl PlanDetails for Vec<Vec<rusqlite::types::Value>> {
    fn plan_details(&self) -> Vec<String> {
        self.iter()
            .filter_map(|row| match row.get(3) {
                Some(rusqlite::types::Value::Text(detail)) => Some(detail.clone()),
                _ => None,
            })
            .collect()
    }
}

impl<T: PlanDetails + ?Sized> PlanDetails for &T {
    fn plan_details(&self) -> Vec<String> {
        (**self).plan_details()
    }
}

impl PlanDetails for Vec<(i64, i64, i64, String)> {
    fn plan_details(&self) -> Vec<String> {
        self.iter()
            .map(|(_, _, _, detail)| detail.clone())
            .collect()
    }
}

pub trait AssertQueryPlan {
    fn uses_index(self, index_name: &str) -> Self;
    fn uses_no_index(self) -> Self;
    fn scans_table(self, table: &str) -> Self;
    fn searches_table(self, table: &str) -> Self;
    fn has_table_access_order(self, expected: impl IntoIterator<Item = &'static str>) -> Self;
    fn has_step_containing(self, text: &str) -> Self;
    fn has_no_step_containing(self, text: &str) -> Self;
}

impl<S, D, R> AssertQueryPlan for Spec<'_, S, D, R>
where
    S: PlanDetails,
    R: FailingStrategy,
{
    fn uses_index(mut self, index_name: &str) -> Self {
        let details = self.subject().plan_details();
        if !details
            .iter()
            .any(|d| d.contains("INDEX") && d.contains(index_name))
        {
            let expression = self.expression().to_string();
            self.do_fail_with_message(plan_failure(
                &expression,
                &format!("use index {index_name}"),
                &details,
            ));
        }
        self
    }

    fn uses_no_index(mut self) -> Self {
        let details = self.subject().plan_details();
        if details.iter().any(|d| d.contains("USING INDEX")) {
            let expression = self.expression().to_string();
            self.do_fail_with_message(plan_failure(&expression, "use no index", &details));
        }
        self
    }

    fn scans_table(mut self, table: &str) -> Self {
        let details = self.subject().plan_details();
        if !details
            .iter()
            .any(|d| plan_target(d, "SCAN ") == Some(table))
        {
            let expression = self.expression().to_string();
            self.do_fail_with_message(plan_failure(
                &expression,
                &format!("scan table {table}"),
                &details,
            ));
        }
        self
    }

    fn searches_table(mut self, table: &str) -> Self {
        let details = self.subject().plan_details();
        if !details
            .iter()
            .any(|d| plan_target(d, "SEARCH ") == Some(table))
        {
            let expression = self.expression().to_string();
            self.do_fail_with_message(plan_failure(
                &expression,
                &format!("search table {table}"),
                &details,
            ));
        }
        self
    }

    fn has_table_access_order(mut self, expected: impl IntoIterator<Item = &'static str>) -> Self {
        let details = self.subject().plan_details();
        let actual: Vec<&str> = details.iter().filter_map(|d| access_step(d)).collect();
        let expected: Vec<&str> = expected.into_iter().collect();
        if actual != expected {
            let expression = self.expression().to_string();
            self.do_fail_with_message(plan_failure(
                &expression,
                &format!("read tables in the order {expected:?}, but read {actual:?}"),
                &details,
            ));
        }
        self
    }

    fn has_step_containing(mut self, text: &str) -> Self {
        let details = self.subject().plan_details();
        if !details.iter().any(|d| d.contains(text)) {
            let expression = self.expression().to_string();
            self.do_fail_with_message(plan_failure(
                &expression,
                &format!("have a step that contains {text:?}"),
                &details,
            ));
        }
        self
    }

    fn has_no_step_containing(mut self, text: &str) -> Self {
        let details = self.subject().plan_details();
        if details.iter().any(|d| d.contains(text)) {
            let expression = self.expression().to_string();
            self.do_fail_with_message(plan_failure(
                &expression,
                &format!("have no step that contains {text:?}"),
                &details,
            ));
        }
        self
    }
}

fn plan_target<'d>(detail: &'d str, keyword: &str) -> Option<&'d str> {
    detail
        .strip_prefix(keyword)
        .and_then(|rest| rest.split_whitespace().next())
}

fn access_step(detail: &str) -> Option<&str> {
    plan_target(detail, "SCAN ")
        .or_else(|| plan_target(detail, "SEARCH "))
        .or_else(|| plan_target(detail, "QUERY INDEX METHOD "))
}

fn plan_failure(expression: &str, wanted: &str, details: &[String]) -> String {
    let plan = if details.is_empty() {
        "<empty plan>".to_owned()
    } else {
        details.join("\n            ")
    };
    format!("expected {expression} to {wanted}\n     plan: {plan}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{limbo_exec_rows, TempDatabase};

    fn seeded_db() -> TempDatabase {
        let db = TempDatabase::new_empty();
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, score REAL, note TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'alice', 1.5, NULL)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'bob', 2.5, 'hi')")
            .unwrap();
        conn.execute("CREATE INDEX idx_name ON t(name)").unwrap();
        db
    }

    #[test]
    fn rows_compare_against_literals() {
        let db = seeded_db();
        let conn = db.connect_limbo();
        let rows = limbo_exec_rows(&conn, "SELECT id, name, score, note FROM t ORDER BY id");

        assert_that!(rows.clone())
            .is_equal_to(vec![row![1, "alice", 1.5, NULL], row![2, "bob", 2.5, "hi"]]);
        assert_that!(rows.clone())
            .first_element()
            .is_equal_to(row![1, "alice", 1.5, NULL]);
        assert_that!(rows.clone())
            .nth_element(1)
            .is_equal_to(row![2, "bob", 2.5, "hi"]);
        assert_that!(rows.clone()).contains(row![2, "bob", 2.5, "hi"]);
        assert_that!(rows).has_length(2);
        assert_that!(limbo_exec_rows(&conn, "SELECT id FROM t WHERE id = 99")).is_empty();
    }

    #[test]
    fn blobs_compare_as_bytes() {
        let db = TempDatabase::new_empty();
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE b(v BLOB)").unwrap();
        conn.execute("INSERT INTO b VALUES (x'deadbeef')").unwrap();

        assert_that!(limbo_exec_rows(&conn, "SELECT v FROM b"))
            .is_equal_to(vec![row![vec![0xde_u8, 0xad, 0xbe, 0xef]]]);
    }

    #[test]
    fn a_column_can_be_taken_across_rows() {
        let db = seeded_db();
        let conn = db.connect_limbo();

        assert_that!(limbo_exec_rows(&conn, "SELECT id, name FROM t ORDER BY id"))
            .column(1)
            .is_equal_to(vec![Cell::from("alice"), Cell::from("bob")]);
    }

    #[test]
    fn query_plans_say_what_they_read() {
        let db = seeded_db();
        let conn = db.connect_limbo();

        assert_that!(limbo_exec_rows(
            &conn,
            "EXPLAIN QUERY PLAN SELECT id FROM t WHERE name = 'alice'"
        ))
        .uses_index("idx_name")
        .searches_table("t");

        assert_that!(limbo_exec_rows(
            &conn,
            "EXPLAIN QUERY PLAN SELECT id FROM t"
        ))
        .uses_no_index()
        .scans_table("t")
        .has_table_access_order(["t"])
        .has_step_containing("SCAN t")
        .has_no_step_containing("USING INDEX");
    }

    #[test]
    fn a_plan_that_reads_the_wrong_way_fails_both_step_assertions() {
        let db = seeded_db();
        let conn = db.connect_limbo();
        let plan = limbo_exec_rows(&conn, "EXPLAIN QUERY PLAN SELECT id FROM t");

        let failures = verify_that!(plan)
            .has_step_containing("SEARCH t")
            .has_no_step_containing("SCAN t")
            .failures();
        assert_that!(failures).has_length(2);
    }

    #[test]
    fn soft_assertions_report_every_problem_at_once() {
        let db = seeded_db();
        let conn = db.connect_limbo();
        let rows = limbo_exec_rows(&conn, "SELECT id, name FROM t ORDER BY id");

        let failures = verify_that!(rows)
            .has_length(3)
            .is_equal_to(vec![row![1, "alice"], row![2, "carol"]])
            .failures();
        assert_that!(failures).has_length(2);
    }
}
