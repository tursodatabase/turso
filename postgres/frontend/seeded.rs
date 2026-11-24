//! The catalogs that have the built-in rows of PostgreSQL 18. Core serves
//! their rows from `turso_pg_catalog` as seeded tables, and stores nothing
//! for them when a database is created.

use turso_core::dialect::{HiddenColumn, SeededTable};
use turso_pg_catalog::{
    PG_AMOP_SEEDED_TABLE, PG_COLLATION_SEEDED_TABLE, PG_OPCLASS_SEEDED_TABLE,
    PG_OPFAMILY_SEEDED_TABLE,
};

/// FIXME(pg-catalog): This list leaves out pg_cast, pg_operator, pg_amproc
/// and pg_range, which stay empty native modules (see
/// `register_catalog_modules`). Their rows refer to functions by the oid of
/// PostgreSQL (castfunc; oprcode, oprrest and oprjoin; amproc; rngcanonical
/// and rngsubdiff), but the emulated pg_proc gives its rows other oids, so a
/// join with pg_proc finds another function. A pg_proc with the oids of
/// PostgreSQL replaces this: then these four catalogs join this list.
/// The seeded catalogs of [`crate::classes::PostgresCatalog`]. Each has the
/// columns of PostgreSQL 18, in the order of `postgres.bki`, and the hidden
/// system column `tableoid`.
pub(crate) static SEEDED_CATALOGS: [SeededTable; 4] = [
    with_table_oid(PG_OPCLASS_SEEDED_TABLE, &[table_oid(2616)]),
    with_table_oid(PG_OPFAMILY_SEEDED_TABLE, &[table_oid(2753)]),
    with_table_oid(PG_AMOP_SEEDED_TABLE, &[table_oid(2602)]),
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
    /// `postgres.bki`, such as `create pg_amop 2602`.
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
