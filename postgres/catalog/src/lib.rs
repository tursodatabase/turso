//! The built-in rows of PostgreSQL's system catalogs: the types, functions,
//! operators, casts and the other objects that exist in every PostgreSQL
//! database before a user creates anything.
//!
//! Each of the 64 catalogs has a struct with its columns and a `static`
//! table of its built-in rows. Most catalogs have no built-in rows, and
//! their table is empty. A catalog whose rows an oid identifies has its
//! table in the order of that oid and a lookup by it:
//!
//! ```
//! use turso_pg_catalog::{PgProc, PgType};
//!
//! let timestamptz = PgType::by_oid(1184).unwrap();
//! assert_eq!(timestamptz.typname, "timestamptz");
//! let input = PgProc::by_oid(timestamptz.typinput).unwrap();
//! assert_eq!(input.proname, "timestamptz_in");
//! ```
//!
//! A column that refers to a row of another catalog holds that row's oid,
//! as in PostgreSQL. 0 means that it refers to nothing.
//!
//! Each struct also has `CREATE_SQL` and `INDEX_SQL`: the `CREATE TABLE`
//! and `CREATE INDEX` statements that store the catalog in a SQLite table.
//! The documentation of `turso_pg_catalog_gen` gives the column types.
//!
//! The tables are generated from `upstream/postgres.bki`, which
//! PostgreSQL's own build script produced, so the rows and their oids are
//! PostgreSQL's. `upstream/README.md` says where the file comes from and
//! how to update it. A test fails when `src/generated.rs` is not what the
//! generator writes from that file.
//!
//! These rows say what PostgreSQL has, not what Turso implements: `pg_proc`
//! lists every built-in function of PostgreSQL.
//!
//! `PG_CLASS`, `PG_ATTRIBUTE` and `PG_DATABASE` are empty although
//! PostgreSQL has rows in them. The documentation of each says why.

/// The identifier of a row of a system catalog.
pub type Oid = u32;

// The generated file has one row per line, which rustfmt would split.
#[rustfmt::skip]
mod generated;

pub use generated::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_generated_file_is_what_the_generator_writes() {
        let expected =
            turso_pg_catalog_gen::generate(include_str!("../upstream/postgres.bki")).unwrap();
        assert!(
            expected == include_str!("generated.rs"),
            "src/generated.rs is out of date: run `cargo run -p turso_pg_catalog_gen`"
        );
    }

    #[test]
    fn known_rows_have_the_oids_of_postgresql() {
        let bool_type = PgType::by_oid(16).unwrap();
        assert_eq!(bool_type.typname, "bool");
        assert_eq!(bool_type.typarray, 1000);
        assert_eq!(
            PgProc::by_oid(bool_type.typinput).unwrap().proname,
            "boolin"
        );

        // genbki.pl makes the array type from the `array_type_oid` of bool.
        let bool_array = PgType::by_oid(1000).unwrap();
        assert_eq!(bool_array.typname, "_bool");
        assert_eq!(bool_array.typelem, 16);
        assert_eq!(bool_array.typcategory, 'A');

        let timestamptz = PgType::by_oid(1184).unwrap();
        assert_eq!(timestamptz.typcategory, 'D');
        assert!(timestamptz.typispreferred);

        let int4_eq = PG_OPERATOR
            .iter()
            .find(|op| op.oprname == "=" && op.oprleft == 23 && op.oprright == 23)
            .unwrap();
        assert_eq!(int4_eq.oid, 96);
        assert_eq!(PgProc::by_oid(int4_eq.oprcode).unwrap().proname, "int4eq");

        let int8_to_int2 = PG_CAST
            .iter()
            .find(|cast| cast.castsource == 20 && cast.casttarget == 21)
            .unwrap();
        assert_eq!(int8_to_int2.castcontext, 'a');
        let function = PgProc::by_oid(int8_to_int2.castfunc).unwrap();
        assert_eq!(function.proname, "int2");
        assert_eq!(function.proargtypes, [20]);
    }

    #[test]
    fn a_catalog_has_the_sql_of_a_sqlite_table() {
        assert_eq!(
            PgCast::CREATE_SQL,
            "CREATE TABLE pg_cast(oid INTEGER PRIMARY KEY, castsource INTEGER NOT NULL, \
             casttarget INTEGER NOT NULL, castfunc INTEGER NOT NULL, \
             castcontext TEXT NOT NULL, castmethod TEXT NOT NULL)"
        );
        assert_eq!(
            PgCast::INDEX_SQL,
            ["CREATE UNIQUE INDEX pg_cast_source_target_index ON pg_cast(castsource, casttarget)"]
        );
        // pg_depend has no oid and two indexes that are not unique.
        assert_eq!(
            PgDepend::INDEX_SQL,
            [
                "CREATE INDEX pg_depend_depender_index ON pg_depend(classid, objid, objsubid)",
                "CREATE INDEX pg_depend_reference_index ON pg_depend(refclassid, refobjid, refobjsubid)",
            ]
        );
        assert!(
            PgDepend::CREATE_SQL.starts_with("CREATE TABLE pg_depend(classid INTEGER NOT NULL,")
        );
        // pg_proc keeps the unique index on its name and argument types.
        assert_eq!(
            PgProc::INDEX_SQL,
            [
                "CREATE UNIQUE INDEX pg_proc_proname_args_nsp_index ON pg_proc(proname, proargtypes, pronamespace)"
            ]
        );
    }

    #[test]
    fn an_array_element_has_no_spaces_around_it() {
        // The file writes the names as '{string, pattern, replacement}'.
        let regexp_replace = PgProc::by_oid(2284).unwrap();
        assert_eq!(regexp_replace.proname, "regexp_replace");
        assert_eq!(
            regexp_replace.proargnames,
            Some(&["string", "pattern", "replacement"][..])
        );
    }

    #[test]
    fn a_symbol_of_the_file_has_the_value_of_a_64_bit_build() {
        // The file says NAMEDATALEN for the length of `name`.
        assert_eq!(PgType::by_oid(19).unwrap().typlen, 64);
    }

    #[test]
    fn a_catalog_outside_the_type_system_has_its_rows() {
        // The file says POSTGRES for the name of the role that owns each
        // built-in object.
        let owner = PgAuthid::by_oid(PgType::by_oid(16).unwrap().typowner).unwrap();
        assert_eq!(owner.rolname, "postgres");
        assert!(owner.rolsuper);

        // 1247 is the oid of pg_type itself.
        let comment = PG_DESCRIPTION
            .iter()
            .find(|row| row.objoid == 16 && row.classoid == 1247)
            .unwrap();
        assert_eq!(comment.description, "boolean, format 't'/'f'");
    }

    #[test]
    fn a_catalog_whose_rows_are_left_out_is_empty() {
        assert!(PG_CLASS.is_empty());
        assert!(PG_ATTRIBUTE.is_empty());
        assert!(PG_DATABASE.is_empty());
        assert_eq!(PgClass::by_oid(1259), None);
    }

    #[test]
    fn a_missing_key_finds_no_row() {
        assert_eq!(PgType::by_oid(0), None);
        assert_eq!(PgType::by_oid(16384), None);
    }

    /// The oids that a column may hold: those of the rows of the catalog
    /// it refers to, and 0 for no row.
    fn oids_or_zero(oids: impl Iterator<Item = Oid>) -> std::collections::HashSet<Oid> {
        oids.chain([0]).collect()
    }

    #[test]
    fn every_reference_names_a_row_that_exists() {
        let types = oids_or_zero(PG_TYPE.iter().map(|row| row.oid));
        let procs = oids_or_zero(PG_PROC.iter().map(|row| row.oid));
        let operators = oids_or_zero(PG_OPERATOR.iter().map(|row| row.oid));
        let classes = oids_or_zero(PG_OPCLASS.iter().map(|row| row.oid));
        let methods = oids_or_zero(PG_AM.iter().map(|row| row.oid));
        let namespaces = oids_or_zero(PG_NAMESPACE.iter().map(|row| row.oid));
        let collations = oids_or_zero(PG_COLLATION.iter().map(|row| row.oid));
        let languages = oids_or_zero(PG_LANGUAGE.iter().map(|row| row.oid));

        for row in &PG_TYPE {
            assert!(namespaces.contains(&row.typnamespace), "{row:?}");
            assert!(types.contains(&row.typelem), "{row:?}");
            assert!(types.contains(&row.typarray), "{row:?}");
            assert!(types.contains(&row.typbasetype), "{row:?}");
            assert!(collations.contains(&row.typcollation), "{row:?}");
            for function in [
                row.typinput,
                row.typoutput,
                row.typreceive,
                row.typsend,
                row.typmodin,
                row.typmodout,
                row.typanalyze,
                row.typsubscript,
            ] {
                assert!(procs.contains(&function), "{row:?}");
            }
        }
        for row in &PG_PROC {
            assert!(namespaces.contains(&row.pronamespace), "{row:?}");
            assert!(languages.contains(&row.prolang), "{row:?}");
            assert!(types.contains(&row.prorettype), "{row:?}");
            assert!(types.contains(&row.provariadic), "{row:?}");
            assert!(procs.contains(&row.prosupport), "{row:?}");
            assert_eq!(
                usize::try_from(row.pronargs).unwrap(),
                row.proargtypes.len()
            );
            for argument in row
                .proargtypes
                .iter()
                .chain(row.proallargtypes.unwrap_or(&[]))
            {
                assert!(types.contains(argument), "{row:?}");
            }
        }
        for row in &PG_AGGREGATE {
            assert!(PgProc::by_oid(row.aggfnoid).is_some(), "{row:?}");
            assert!(procs.contains(&row.aggtransfn), "{row:?}");
            assert!(procs.contains(&row.aggfinalfn), "{row:?}");
            assert!(types.contains(&row.aggtranstype), "{row:?}");
            assert!(operators.contains(&row.aggsortop), "{row:?}");
        }
        for row in &PG_OPERATOR {
            assert!(namespaces.contains(&row.oprnamespace), "{row:?}");
            assert!(types.contains(&row.oprleft), "{row:?}");
            assert!(types.contains(&row.oprright), "{row:?}");
            assert!(types.contains(&row.oprresult), "{row:?}");
            assert!(operators.contains(&row.oprcom), "{row:?}");
            assert!(operators.contains(&row.oprnegate), "{row:?}");
            assert!(procs.contains(&row.oprcode), "{row:?}");
        }
        for row in &PG_CAST {
            assert!(PgType::by_oid(row.castsource).is_some(), "{row:?}");
            assert!(PgType::by_oid(row.casttarget).is_some(), "{row:?}");
            assert!(procs.contains(&row.castfunc), "{row:?}");
        }
        for row in &PG_RANGE {
            assert!(PgType::by_oid(row.rngtypid).is_some(), "{row:?}");
            assert!(PgType::by_oid(row.rngsubtype).is_some(), "{row:?}");
            assert!(classes.contains(&row.rngsubopc), "{row:?}");
        }
        for row in &PG_OPFAMILY {
            assert!(methods.contains(&row.opfmethod), "{row:?}");
        }
        for row in &PG_OPCLASS {
            assert!(methods.contains(&row.opcmethod), "{row:?}");
            assert!(PgOpfamily::by_oid(row.opcfamily).is_some(), "{row:?}");
            assert!(PgType::by_oid(row.opcintype).is_some(), "{row:?}");
            assert!(types.contains(&row.opckeytype), "{row:?}");
        }
        for row in &PG_AMOP {
            assert!(PgOpfamily::by_oid(row.amopfamily).is_some(), "{row:?}");
            assert!(PgOperator::by_oid(row.amopopr).is_some(), "{row:?}");
            assert!(PgType::by_oid(row.amoplefttype).is_some(), "{row:?}");
            assert!(PgType::by_oid(row.amoprighttype).is_some(), "{row:?}");
            assert!(methods.contains(&row.amopmethod), "{row:?}");
        }
        for row in &PG_AMPROC {
            assert!(PgOpfamily::by_oid(row.amprocfamily).is_some(), "{row:?}");
            assert!(PgProc::by_oid(row.amproc).is_some(), "{row:?}");
            assert!(PgType::by_oid(row.amproclefttype).is_some(), "{row:?}");
            assert!(PgType::by_oid(row.amprocrighttype).is_some(), "{row:?}");
        }
    }
}
