//! The catalogs that have the built-in rows of PostgreSQL 18. Core serves
//! their rows from `turso_pg_catalog` as seeded tables, and stores nothing
//! for them when a database is created.

use turso_core::dialect::{HiddenColumn, SeededTable};
use turso_pg_catalog::{
    PG_AMOP_SEEDED_TABLE, PG_AMPROC_SEEDED_TABLE, PG_CAST_SEEDED_TABLE, PG_COLLATION_SEEDED_TABLE,
    PG_OPCLASS_SEEDED_TABLE, PG_OPERATOR_SEEDED_TABLE, PG_OPFAMILY_SEEDED_TABLE,
    PG_RANGE_SEEDED_TABLE,
};

/// The seeded catalogs of [`crate::classes::PostgresCatalog`]. Each has the
/// columns of PostgreSQL 18, in the order of `postgres.bki`, and the hidden
/// system column `tableoid`.
pub(crate) static SEEDED_CATALOGS: [SeededTable; 8] = [
    with_table_oid(PG_OPERATOR_SEEDED_TABLE, &[table_oid(2617)]),
    with_table_oid(PG_CAST_SEEDED_TABLE, &[table_oid(2605)]),
    with_table_oid(PG_OPCLASS_SEEDED_TABLE, &[table_oid(2616)]),
    with_table_oid(PG_OPFAMILY_SEEDED_TABLE, &[table_oid(2753)]),
    with_table_oid(PG_AMOP_SEEDED_TABLE, &[table_oid(2602)]),
    with_table_oid(PG_AMPROC_SEEDED_TABLE, &[table_oid(2603)]),
    with_table_oid(PG_RANGE_SEEDED_TABLE, &[table_oid(3541)]),
    with_table_oid(PG_COLLATION_SEEDED_TABLE, &[table_oid(3456)]),
];

/// `table` with the hidden column `tableoid`.
const fn with_table_oid(table: SeededTable, table_oid: &'static [HiddenColumn; 1]) -> SeededTable {
    SeededTable {
        hidden_columns: table_oid,
        ..table
    }
}

/// The system column `tableoid` of a catalog whose `pg_class` oid is `oid`:
/// the number of the `create` line of the catalog in `postgres.bki`.
const fn table_oid(oid: i64) -> HiddenColumn {
    HiddenColumn {
        name: "tableoid",
        value: oid,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The `tableoid` of each catalog is the oid of its `create` line in
    /// `postgres.bki`, such as `create pg_cast 2605`.
    #[test]
    fn table_oids_are_those_of_postgres_bki() {
        let bki = include_str!("../catalog/upstream/postgres.bki");
        for table in &SEEDED_CATALOGS {
            let [HiddenColumn { name, value }] = table.hidden_columns else {
                panic!("{} has one hidden column", table.name);
            };
            assert_eq!(*name, "tableoid");
            let create = format!("\ncreate {} {value}\n", table.name);
            assert!(bki.contains(&create), "postgres.bki has no line {create:?}");
        }
    }
}
