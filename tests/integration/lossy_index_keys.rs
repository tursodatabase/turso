//! Keys on custom types whose index order can differ from their value order.
//!
//! An index stores a custom type's stored form, but queries compare the decoded
//! form. For a type with `DECODE lower(value)`, 'A' and 'a' are two index
//! entries but one value, so an index plan returns different rows than a scan.
//! The rule: such a key is refused with an error that names the type and
//! suggests `CAST(c AS <base>)`, and the planner never builds an automatic
//! index on such a column. This file checks the rule over a generated matrix.
//!
//! Rules checked (one finding per broken rule):
//!   1. Each declaration form on each type is exactly one of: allowed; refused
//!      by the new rule (the message names the type and `CAST(c AS <base>)`); or
//!      refused by a rule that already existed (the message contains that exact
//!      text). See `expectation` and `alter_expectation`.
//!   2. Every SELECT on a table gives the same rows as the same SELECT on a twin
//!      table that has no keys, with every key column wrapped in `+` so no
//!      index (declared or automatic) can be used. Rows are compared as sorted
//!      lists, or as exact sequences when the query has a total ORDER BY.
//!   3. UNIQUE and PRIMARY KEY accept a row exactly when no earlier row has an
//!      equal key under the type's `=` (checked with a scan of the twin table).
//!   4. UPDATE and DELETE select the same rows as they do on the twin table.
//!
//! Dimensions (all multiplied together):
//!   - type (see `types`): lossy DECODE shapes (lower(), round(), a CASE that
//!     merges values, abs(), -value, no OPERATOR '<'), domains over those,
//!     built-in types, types with no DECODE, DECODE exactly `value`, domains over
//!     those, and arrays. A key on an array is allowed exactly when the key on
//!     its element type is. Queries on arrays of a lossy type are not compared
//!     (see `TypeSpec::expressions_decode`).
//!   - declaration form (see `Form`)
//!   - index before data or data before index (CREATE INDEX forms)
//!   - ALTER COLUMN: the table is created with a key on a column of an allowed
//!     type, then the column's type is changed (see `alter_pairs`), with the
//!     rows in before or after the change. Both sides of the pair are allowed
//!     types, or the target is a lossy type.
//!   - statement shape (see `queries`)
//!   - data: rows that are equal after DECODE but stored differently, rows that
//!     are not, NULLs, and (full run) random picks from the pool.
//!
//! Every place that can make a key or an index or change a column's type, and
//! the form that exercises it:
//!
//! ```text
//! - core/translate/index.rs                CREATE INDEX (index_* forms)
//! - core/translate/schema.rs               UNIQUE and PRIMARY KEY in CREATE TABLE
//!                                          (col_*, tbl_* forms)
//! - core/schema.rs                         automatic_from_primary_key/unique, and
//!                                          loading an index from the schema table
//! - core/translate/alter.rs                ALTER TABLE ADD COLUMN with REFERENCES,
//!                                          UNIQUE or PRIMARY KEY (AddColumn* forms)
//! - core/translate/alter.rs                ALTER COLUMN that gives a column with no
//!                                          key a REFERENCES, UNIQUE or PRIMARY KEY
//!                                          (AlterColumn* forms)
//! - core/translate/alter.rs, vdbe/execute.rs   ALTER COLUMN ... TO on a column
//!                                          that already has a key (alter pairs)
//! - core/translate/fkeys.rs, insert.rs     FOREIGN KEY, both sides (fk_* forms)
//! - core/translate/upsert.rs               ON CONFLICT target (upsert_target)
//! - core/translate/optimizer/mod.rs        automatic index on a join
//! - core/translate/optimizer/access_method.rs   ephemeral index for a join
//! - core/translate/main_loop/init.rs       RIGHT JOIN matched rows (join-right)
//! - core/translate/main_loop/in_seek.rs    IN list (in-list)
//! - core/translate/subquery.rs             IN (SELECT ...) and EXISTS
//! - core/translate/compound_select.rs      UNION/INTERSECT/EXCEPT dedupe
//! - core/translate/order_by.rs             sorter index for ORDER BY
//! - core/translate/window.rs               partition index for window functions
//! - core/translate/recursive_cte.rs        recursive CTE queue and seen-row
//!                                          indexes (not reachable from a table
//!                                          column, so not generated)
//! - core/translate/pragma.rs               PRIMARY KEY index of a WITHOUT ROWID
//!                                          table (needs --experimental-without-rowid;
//!                                          not generated)
//! - core/incremental/view.rs               state index of a materialized view
//!                                          (experimental, internal; not generated)
//! ```
//!
//! Run (the default run takes a few seconds; the rest is `#[ignore]`):
//!   cargo test -p core_tester --test integration_tests lossy_index_keys
//!   cargo test -p core_tester --test integration_tests lossy_index_keys -- --ignored
//! Set LOSSY_INDEX_REPORT=<path> to also write the full report to a file.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Arc;

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use turso_core::{
    ConnectOpts, Connection, Database, DatabaseOpts, MemoryIO, Numeric, OpenFlags, SqliteDialect,
    Value, IO,
};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Debug)]
enum Kind {
    /// Index order can differ from value order. Every key on it must be refused.
    Lossy,
    /// Index order is value order. Every key on it must work.
    Allowed,
}

struct TypeSpec {
    /// Type name used for the column.
    name: &'static str,
    /// Statements that create the type (and domains over it).
    setup: &'static [&'static str],
    /// Type named in the hint `CAST(c AS <base>)`.
    base: &'static str,
    /// Names the refusal message may use for this type.
    names: &'static [&'static str],
    kind: Kind,
    /// ORDER BY and window functions on the type work today (it declares
    /// OPERATOR '<', has no DECODE, or is built in).
    sortable: bool,
    /// CREATE INDEX on the type is refused today with "type does not declare
    /// OPERATOR '<'" (it has a DECODE but no OPERATOR '<').
    index_needs_operator: bool,
    /// COLLATE on the type is refused today with this message.
    collation_error: Option<&'static str>,
    is_domain: bool,
    /// SQL literals. For lossy types the first two are equal after DECODE but
    /// stored differently.
    pool: &'static [&'static str],
    /// At least three SQL literals used as comparison values.
    probes: &'static [&'static str],
}

impl TypeSpec {
    /// False for an array of a lossy type. On main only a bare `c` is decoded;
    /// `+c`, `coalesce(c, c)` and `c = ...` see the stored form (`SELECT c` shows
    /// '{a}' where `SELECT +c` shows '{A}'). So the `+c` baseline does not answer
    /// the way `c` does, and comparing queries would only report that.
    fn expressions_decode(&self) -> bool {
        !(self.kind == Kind::Lossy && self.name.ends_with("[]"))
    }
}

const CI: &str =
    "CREATE TYPE ci(value text) BASE text ENCODE value DECODE lower(value) OPERATOR '<'";
const CI_PLAIN: &str =
    "CREATE TYPE ci_plain(value text) BASE text ENCODE value DECODE lower(value)";
const ROUND: &str =
    "CREATE TYPE round_t(value real) BASE real ENCODE value DECODE round(value) OPERATOR '<'";
const CASE_T: &str = "CREATE TYPE case_t(value integer) BASE integer ENCODE value DECODE CASE WHEN value < 0 THEN 0 ELSE value END OPERATOR '<'";
const ABS_T: &str =
    "CREATE TYPE abs_t(value integer) BASE integer ENCODE value DECODE abs(value) OPERATOR '<'";
const NEG_T: &str =
    "CREATE TYPE neg_t(value integer) BASE integer ENCODE value DECODE -value OPERATOR '<'";
const UPPER_LT: &str =
    "CREATE TYPE upper_lt(value text) BASE text ENCODE upper(value) OPERATOR '<'";
const UPPER_PLAIN: &str = "CREATE TYPE upper_plain(value text) BASE text ENCODE upper(value)";
const DECODE_VALUE_LT: &str =
    "CREATE TYPE decode_value_lt(value text) BASE text ENCODE upper(value) DECODE value OPERATOR '<'";
const DECODE_VALUE: &str =
    "CREATE TYPE decode_value(value text) BASE text ENCODE upper(value) DECODE value";

const UUID_POOL: &[&str] = &[
    "'01945ca0-3189-76c0-9a8f-caf310fc8b8e'",
    "'01945CA0-3189-76C0-9A8F-CAF310FC8B8E'",
    "'01945ca0-3189-76c0-9a8f-caf310fc8b8f'",
    "'11111111-2222-7333-8444-555555555555'",
];

const UUID_PROBES: &[&str] = &[UUID_POOL[0], UUID_POOL[2], UUID_POOL[3]];

fn types() -> Vec<TypeSpec> {
    let text_pool: &[&str] = &["'A'", "'a'", "'b'", "'B'", "'c'"];
    let text_probes: &[&str] = &["'a'", "'b'", "'z'"];
    vec![
        // ---- lossy ----
        TypeSpec {
            name: "ci",
            setup: &[CI],
            base: "text",
            names: &["ci"],
            kind: Kind::Lossy,
            sortable: true,
            index_needs_operator: false,
            collation_error: None,
            is_domain: false,
            pool: text_pool,
            probes: text_probes,
        },
        TypeSpec {
            name: "ci_plain",
            setup: &[CI_PLAIN],
            base: "text",
            names: &["ci_plain"],
            kind: Kind::Lossy,
            sortable: false,
            index_needs_operator: true,
            collation_error: None,
            is_domain: false,
            pool: text_pool,
            probes: text_probes,
        },
        TypeSpec {
            name: "round_t",
            setup: &[ROUND],
            base: "real",
            names: &["round_t"],
            kind: Kind::Lossy,
            sortable: true,
            index_needs_operator: false,
            collation_error: None,
            is_domain: false,
            pool: &["1.2", "1.4", "2.6", "3.0", "2.4"],
            probes: &["1.0", "3.0", "2.5"],
        },
        TypeSpec {
            name: "case_t",
            setup: &[CASE_T],
            base: "integer",
            names: &["case_t"],
            kind: Kind::Lossy,
            sortable: true,
            index_needs_operator: false,
            collation_error: None,
            is_domain: false,
            pool: &["-5", "-1", "0", "3", "7"],
            probes: &["0", "-9", "3"],
        },
        TypeSpec {
            name: "abs_t",
            setup: &[ABS_T],
            base: "integer",
            names: &["abs_t"],
            kind: Kind::Lossy,
            sortable: true,
            index_needs_operator: false,
            collation_error: None,
            is_domain: false,
            pool: &["-3", "3", "2", "-2", "5"],
            probes: &["3", "2", "4"],
        },
        TypeSpec {
            // No two values merge, but the index order is the reverse of the
            // value order, so every range seek is wrong.
            name: "neg_t",
            setup: &[NEG_T],
            base: "integer",
            names: &["neg_t"],
            kind: Kind::Lossy,
            sortable: true,
            index_needs_operator: false,
            collation_error: None,
            is_domain: false,
            pool: &["1", "2", "3", "4", "5"],
            probes: &["2", "4", "9"],
        },
        TypeSpec {
            name: "dom_ci",
            setup: &[CI, "CREATE DOMAIN dom_ci AS ci"],
            base: "text",
            names: &["dom_ci", "ci"],
            kind: Kind::Lossy,
            sortable: true,
            index_needs_operator: false,
            collation_error: None,
            is_domain: true,
            pool: text_pool,
            probes: text_probes,
        },
        TypeSpec {
            name: "dom_dom_ci",
            setup: &[
                CI,
                "CREATE DOMAIN dom_ci AS ci",
                "CREATE DOMAIN dom_dom_ci AS dom_ci",
            ],
            base: "text",
            names: &["dom_dom_ci", "dom_ci", "ci"],
            kind: Kind::Lossy,
            sortable: true,
            index_needs_operator: false,
            collation_error: None,
            is_domain: true,
            pool: text_pool,
            probes: text_probes,
        },
        // ---- allowed ----
        TypeSpec {
            name: "text",
            setup: &[],
            base: "text",
            names: &[],
            kind: Kind::Allowed,
            sortable: true,
            index_needs_operator: false,
            collation_error: None,
            is_domain: false,
            pool: text_pool,
            probes: text_probes,
        },
        TypeSpec {
            name: "integer",
            setup: &[],
            base: "integer",
            names: &[],
            kind: Kind::Allowed,
            sortable: true,
            index_needs_operator: false,
            collation_error: None,
            is_domain: false,
            pool: &["1", "2", "3", "10", "-4"],
            probes: &["2", "3", "99"],
        },
        TypeSpec {
            name: "uuid",
            setup: &[],
            base: "uuid",
            names: &[],
            kind: Kind::Allowed,
            sortable: true,
            index_needs_operator: false,
            collation_error: None,
            is_domain: false,
            pool: UUID_POOL,
            probes: UUID_PROBES,
        },
        TypeSpec {
            // No DECODE: the stored form is the value, so stored order is value order.
            name: "upper_lt",
            setup: &[UPPER_LT],
            base: "text",
            names: &[],
            kind: Kind::Allowed,
            sortable: true,
            index_needs_operator: false,
            collation_error: None,
            is_domain: false,
            pool: text_pool,
            probes: text_probes,
        },
        TypeSpec {
            name: "upper_plain",
            setup: &[UPPER_PLAIN],
            base: "text",
            names: &[],
            kind: Kind::Allowed,
            sortable: false,
            index_needs_operator: false,
            collation_error: None,
            is_domain: false,
            pool: text_pool,
            probes: text_probes,
        },
        TypeSpec {
            name: "decode_value_lt",
            setup: &[DECODE_VALUE_LT],
            base: "text",
            names: &[],
            kind: Kind::Allowed,
            sortable: true,
            index_needs_operator: false,
            collation_error: None,
            is_domain: false,
            pool: text_pool,
            probes: text_probes,
        },
        TypeSpec {
            name: "decode_value",
            setup: &[DECODE_VALUE],
            base: "text",
            names: &[],
            kind: Kind::Allowed,
            sortable: false,
            index_needs_operator: true,
            collation_error: None,
            is_domain: false,
            pool: text_pool,
            probes: text_probes,
        },
        TypeSpec {
            name: "dom_upper_lt",
            setup: &[UPPER_LT, "CREATE DOMAIN dom_upper_lt AS upper_lt"],
            base: "text",
            names: &[],
            kind: Kind::Allowed,
            sortable: true,
            index_needs_operator: false,
            collation_error: None,
            is_domain: true,
            pool: text_pool,
            probes: text_probes,
        },
        TypeSpec {
            name: "dom_positive",
            setup: &["CREATE DOMAIN dom_positive AS integer CHECK (value > 0)"],
            base: "integer",
            names: &[],
            kind: Kind::Allowed,
            sortable: true,
            index_needs_operator: false,
            collation_error: None,
            is_domain: true,
            pool: &["1", "2", "3", "10", "4"],
            probes: &["2", "3", "99"],
        },
        TypeSpec {
            name: "dom_uuid",
            setup: &["CREATE DOMAIN dom_uuid AS uuid"],
            base: "uuid",
            names: &[],
            kind: Kind::Allowed,
            sortable: true,
            index_needs_operator: false,
            collation_error: None,
            is_domain: true,
            pool: UUID_POOL,
            probes: UUID_PROBES,
        },
        // ---- arrays: a key is allowed exactly when the element type's key is ----
        TypeSpec {
            name: "integer[]",
            setup: &[],
            base: "integer[]",
            names: &[],
            kind: Kind::Allowed,
            sortable: true,
            index_needs_operator: false,
            collation_error: Some("collations are not supported by type array"),
            is_domain: false,
            pool: &["'{1,2}'", "'{1,3}'", "'{2}'", "'{1,2}'"],
            probes: &["'{1,2}'", "'{2}'", "'{9}'"],
        },
        TypeSpec {
            name: "ci[]",
            setup: &[CI],
            base: "text[]",
            names: &["ci[]", "ci"],
            kind: Kind::Lossy,
            sortable: true,
            index_needs_operator: false,
            collation_error: Some("collations are not supported by type text_array"),
            is_domain: false,
            pool: &["'{A}'", "'{a}'", "'{b}'", "'{B}'", "'{c}'"],
            probes: &["'{a}'", "'{b}'", "'{z}'"],
        },
        TypeSpec {
            // A domain cannot be declared over an array, so this is an array
            // whose element type is a domain.
            name: "dom_ci[]",
            setup: &[CI, "CREATE DOMAIN dom_ci AS ci"],
            base: "text[]",
            names: &["dom_ci[]", "dom_ci", "ci"],
            kind: Kind::Lossy,
            sortable: true,
            index_needs_operator: false,
            collation_error: Some("collations are not supported by type text_array"),
            is_domain: true,
            pool: &["'{A}'", "'{a}'", "'{b}'", "'{B}'", "'{c}'"],
            probes: &["'{a}'", "'{b}'", "'{z}'"],
        },
    ]
}

// ---------------------------------------------------------------------------
// Declaration forms
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Debug)]
enum Form {
    None,
    IndexPlain,
    IndexParen,
    IndexCollate,
    IndexDesc,
    IndexMulti,
    IndexUnique,
    IndexPartial,
    IndexExprPlus,
    IndexExprCastOwn,
    IndexExprCastBase,
    ColUnique,
    TblUnique,
    TblUniqueMulti,
    ColUniqueCollate,
    ColPk,
    TblPk,
    TblPkMulti,
    FkChildColumn,
    FkChildTable,
    FkParent,
    FkBoth,
    /// `t.c` is a PRIMARY KEY that another table's FOREIGN KEY refers to.
    FkReferenced,
    AddColumnUnique,
    AddColumnPrimaryKey,
    AddColumnReferences,
    /// `t.x` has the base type and no key, and ALTER COLUMN changes it to
    /// `c <type>` with the constraint.
    AlterColumnUnique,
    AlterColumnPrimaryKey,
    AlterColumnReferences,
    UpsertTarget,
}

const FORMS: &[Form] = &[
    Form::None,
    Form::IndexPlain,
    Form::IndexParen,
    Form::IndexCollate,
    Form::IndexDesc,
    Form::IndexMulti,
    Form::IndexUnique,
    Form::IndexPartial,
    Form::IndexExprPlus,
    Form::IndexExprCastOwn,
    Form::IndexExprCastBase,
    Form::ColUnique,
    Form::TblUnique,
    Form::TblUniqueMulti,
    Form::ColUniqueCollate,
    Form::ColPk,
    Form::TblPk,
    Form::TblPkMulti,
    Form::FkChildColumn,
    Form::FkChildTable,
    Form::FkParent,
    Form::FkBoth,
    Form::FkReferenced,
    Form::AddColumnUnique,
    Form::AddColumnPrimaryKey,
    Form::AddColumnReferences,
    Form::AlterColumnUnique,
    Form::AlterColumnPrimaryKey,
    Form::AlterColumnReferences,
    Form::UpsertTarget,
];

/// What a declaration must do. There is no "either": every case is one of these.
#[derive(Clone, PartialEq, Debug)]
enum Expect {
    /// The declaration succeeds.
    Allow,
    /// The new rule: refused with a message that names the type and suggests
    /// `CAST(c AS <base>)`.
    RefuseLossyKey,
    /// Refused by a rule that exists today. The message must contain this text.
    RefuseExisting(String),
}

/// How a unique key relates to the column `c`, for the unique-key check.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Unique {
    No,
    OnC,
    OnKC,
}

/// How the query text refers to `c`, so an expression index can be used.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Wrap {
    Plain,
    Plus,
    CastOwn,
    CastBase,
}

struct Script {
    /// Statements that must succeed (the table, parent tables).
    pre: Vec<String>,
    /// The statements that declare the key. The first error is the outcome.
    decl: Vec<String>,
    /// After `decl`, table `t(id, k, c)` exists and the data phase can run.
    data_phase: bool,
    unique: Unique,
    wrap: Wrap,
    /// A parent table `p(c <base> PRIMARY KEY)` exists and `t.c` references it.
    fk_child: bool,
    /// This form is a CREATE INDEX on an existing table, so the data can go in
    /// before the index.
    index_on_existing_table: bool,
    /// The key column is a PRIMARY KEY, so it cannot hold NULL.
    not_null: bool,
    /// Collation of the twin table column, when the key declares one on the column.
    twin_collation: &'static str,
}

fn base_table(ty: &TypeSpec, constraint_in_column: &str, table_constraint: &str) -> String {
    format!(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER, c {} {}{}) STRICT",
        ty.name, constraint_in_column, table_constraint
    )
}

fn script(form: Form, ty: &TypeSpec) -> Script {
    let plain_table = base_table(ty, "", "");
    let parent_base = format!("CREATE TABLE p(c {} PRIMARY KEY) STRICT", ty.base);
    let mut s = Script {
        pre: vec![],
        decl: vec![],
        data_phase: true,
        unique: Unique::No,
        wrap: Wrap::Plain,
        fk_child: false,
        index_on_existing_table: false,
        not_null: false,
        twin_collation: "",
    };
    match form {
        Form::None => s.pre.push(plain_table),
        Form::IndexPlain
        | Form::IndexParen
        | Form::IndexCollate
        | Form::IndexDesc
        | Form::IndexMulti
        | Form::IndexUnique
        | Form::IndexPartial
        | Form::IndexExprPlus
        | Form::IndexExprCastOwn
        | Form::IndexExprCastBase => {
            s.pre.push(plain_table);
            s.index_on_existing_table = true;
            let sql = match form {
                Form::IndexPlain => "CREATE INDEX i ON t(c)".to_string(),
                Form::IndexParen => "CREATE INDEX i ON t((c))".to_string(),
                Form::IndexCollate => "CREATE INDEX i ON t(c COLLATE nocase)".to_string(),
                Form::IndexDesc => "CREATE INDEX i ON t(c DESC)".to_string(),
                Form::IndexMulti => "CREATE INDEX i ON t(k, c)".to_string(),
                Form::IndexUnique => "CREATE UNIQUE INDEX i ON t(c)".to_string(),
                Form::IndexPartial => "CREATE INDEX i ON t(k) WHERE c IS NOT NULL".to_string(),
                Form::IndexExprPlus => "CREATE INDEX i ON t(+c)".to_string(),
                Form::IndexExprCastOwn => format!("CREATE INDEX i ON t(CAST(c AS {}))", ty.name),
                Form::IndexExprCastBase => format!("CREATE INDEX i ON t(CAST(c AS {}))", ty.base),
                _ => unreachable!(),
            };
            s.decl.push(sql);
            match form {
                Form::IndexExprCastOwn => s.wrap = Wrap::CastOwn,
                Form::IndexExprCastBase => s.wrap = Wrap::CastBase,
                Form::IndexExprPlus => s.wrap = Wrap::Plus,
                Form::IndexUnique => s.unique = Unique::OnC,
                _ => {}
            }
        }
        Form::ColUnique => {
            s.decl.push(base_table(ty, "UNIQUE", ""));
            s.unique = Unique::OnC;
        }
        Form::TblUnique => {
            s.decl.push(base_table(ty, "", ", UNIQUE(c)"));
            s.unique = Unique::OnC;
        }
        Form::TblUniqueMulti => {
            s.decl.push(base_table(ty, "", ", UNIQUE(k, c)"));
            s.unique = Unique::OnKC;
        }
        Form::ColUniqueCollate => {
            s.decl.push(base_table(ty, "COLLATE nocase UNIQUE", ""));
            s.unique = Unique::OnC;
            s.twin_collation = "COLLATE nocase";
        }
        Form::ColPk => {
            s.decl.push(format!(
                "CREATE TABLE t(id INTEGER, k INTEGER, c {} PRIMARY KEY) STRICT",
                ty.name
            ));
            s.unique = Unique::OnC;
            s.not_null = true;
        }
        Form::TblPk => {
            s.decl.push(format!(
                "CREATE TABLE t(id INTEGER, k INTEGER, c {}, PRIMARY KEY(c)) STRICT",
                ty.name
            ));
            s.unique = Unique::OnC;
            s.not_null = true;
        }
        Form::TblPkMulti => {
            s.decl.push(format!(
                "CREATE TABLE t(id INTEGER, k INTEGER, c {}, PRIMARY KEY(k, c)) STRICT",
                ty.name
            ));
            s.unique = Unique::OnKC;
            s.not_null = true;
        }
        Form::FkChildColumn => {
            s.pre.push(parent_base);
            s.decl.push(base_table(ty, "REFERENCES p(c)", ""));
            s.fk_child = true;
        }
        Form::FkChildTable => {
            s.pre.push(parent_base);
            s.decl
                .push(base_table(ty, "", ", FOREIGN KEY(c) REFERENCES p(c)"));
            s.fk_child = true;
        }
        Form::FkParent => {
            s.pre.push(format!(
                "CREATE TABLE p(id INTEGER PRIMARY KEY, c {}) STRICT",
                ty.name
            ));
            s.pre.push("PRAGMA foreign_keys = ON".to_string());
            s.decl.push(format!(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER, c {} REFERENCES p(c)) STRICT",
                ty.base
            ));
            // `p.c` has no unique key, so the first insert into `t` must fail.
            s.decl.push(format!(
                "INSERT INTO t(id, k, c) VALUES (1, 0, {})",
                ty.pool[0]
            ));
            // `t.c` has the base type here, not the custom type, so it cannot be
            // compared with the twin table.
            s.data_phase = false;
        }
        Form::FkBoth => {
            s.decl
                .push(format!("CREATE TABLE p(c {} PRIMARY KEY) STRICT", ty.name));
            s.decl.push(base_table(ty, "REFERENCES p(c)", ""));
        }
        Form::FkReferenced => {
            s.decl.push(format!(
                "CREATE TABLE t(id INTEGER, k INTEGER, c {}, PRIMARY KEY(c)) STRICT",
                ty.name
            ));
            s.decl.push(format!(
                "CREATE TABLE ch(id INTEGER PRIMARY KEY, c {} REFERENCES t(c)) STRICT",
                ty.base
            ));
            s.unique = Unique::OnC;
            s.not_null = true;
        }
        Form::AddColumnUnique => {
            s.pre
                .push("CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER) STRICT".to_string());
            s.decl
                .push(format!("ALTER TABLE t ADD COLUMN c {} UNIQUE", ty.name));
            s.data_phase = false;
        }
        Form::AddColumnPrimaryKey => {
            s.pre
                .push("CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER) STRICT".to_string());
            s.decl.push(format!(
                "ALTER TABLE t ADD COLUMN c {} PRIMARY KEY",
                ty.name
            ));
            s.data_phase = false;
        }
        Form::AddColumnReferences => {
            s.pre.push(parent_base);
            s.pre
                .push("CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER) STRICT".to_string());
            s.decl.push(format!(
                "ALTER TABLE t ADD COLUMN c {} REFERENCES p(c)",
                ty.name
            ));
            s.data_phase = false;
        }
        Form::AlterColumnUnique | Form::AlterColumnPrimaryKey | Form::AlterColumnReferences => {
            let constraint = match form {
                Form::AlterColumnUnique => "UNIQUE",
                Form::AlterColumnPrimaryKey => "PRIMARY KEY",
                Form::AlterColumnReferences => {
                    s.pre.push(parent_base);
                    "REFERENCES p(c)"
                }
                _ => unreachable!(),
            };
            s.pre.push(format!(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER, x {}) STRICT",
                ty.base
            ));
            s.decl.push(format!(
                "ALTER TABLE t ALTER COLUMN x TO c {} {constraint}",
                ty.name
            ));
            s.data_phase = false;
        }
        Form::UpsertTarget => {
            let unique = if ty.kind == Kind::Allowed {
                "UNIQUE"
            } else {
                ""
            };
            s.pre.push(base_table(ty, unique, ""));
            s.decl.push(format!(
                "INSERT INTO t(id, k, c) VALUES (1, 0, {}) ON CONFLICT(c) DO NOTHING",
                ty.pool[0]
            ));
            s.data_phase = false;
        }
    }
    s
}

/// Every (form, type) pair is exactly "allowed" or "refused with this message".
fn expectation(form: Form, ty: &TypeSpec) -> Expect {
    let lossy = ty.kind == Kind::Lossy;
    match form {
        // No key on `c`, or a key on an expression whose value is what the
        // query sees (explicit conversions stay allowed on every type). A
        // partial index keys on `k`; `c` only appears in its WHERE.
        Form::None
        | Form::IndexPartial
        | Form::IndexExprPlus
        | Form::IndexExprCastOwn
        | Form::IndexExprCastBase => Expect::Allow,
        // ADD COLUMN cannot add a UNIQUE or PRIMARY KEY column (SQLite's rule),
        // and ALTER COLUMN follows the same rule, for every type.
        Form::AddColumnUnique | Form::AlterColumnUnique => {
            Expect::RefuseExisting("Cannot add a UNIQUE column in ALTER TABLE statement".into())
        }
        Form::AddColumnPrimaryKey | Form::AlterColumnPrimaryKey => Expect::RefuseExisting(
            "Cannot add a PRIMARY KEY column in ALTER TABLE statement".into(),
        ),
        // `p.c` has no unique key, so there is nothing to refuse for the type.
        Form::FkParent => {
            Expect::RefuseExisting("foreign key mismatch - \"t\" referencing \"p\"".into())
        }
        // The lossy table has no unique key (the new rule refuses it), so the
        // conflict target matches nothing.
        Form::UpsertTarget if lossy => Expect::RefuseExisting(
            "ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint".into(),
        ),
        Form::UpsertTarget => Expect::Allow,
        // Arrays take no COLLATE. This is checked before any key rule.
        Form::IndexCollate | Form::ColUniqueCollate if ty.collation_error.is_some() => {
            Expect::RefuseExisting(ty.collation_error.unwrap().into())
        }
        // A type with a DECODE and no OPERATOR '<' cannot be indexed today. Keep
        // that exact message until OPERATOR '<' stops being required.
        Form::IndexPlain
        | Form::IndexParen
        | Form::IndexCollate
        | Form::IndexDesc
        | Form::IndexMulti
        | Form::IndexUnique
            if ty.index_needs_operator =>
        {
            Expect::RefuseExisting(format!(
                "cannot create index on column 'c' of type '{}': type does not declare OPERATOR '<'",
                ty.name
            ))
        }
        _ if lossy => Expect::RefuseLossyKey,
        _ => Expect::Allow,
    }
}

// ---------------------------------------------------------------------------
// ALTER COLUMN: changing the type of a column that already has a key
// ---------------------------------------------------------------------------

/// Forms where the table is created with its key on `c` (type `from`) and the
/// type is changed afterwards with `ALTER TABLE t ALTER COLUMN c TO d <type>`.
/// The other forms do not keep `c` as a key of `t` or are not a table created
/// with a key.
const ALTER_FORMS: &[Form] = &[
    Form::None,
    Form::IndexPlain,
    Form::IndexParen,
    Form::IndexCollate,
    Form::IndexDesc,
    Form::IndexMulti,
    Form::IndexUnique,
    Form::IndexPartial,
    Form::IndexExprPlus,
    Form::IndexExprCastOwn,
    Form::IndexExprCastBase,
    Form::ColUnique,
    Form::TblUnique,
    Form::TblUniqueMulti,
    Form::ColUniqueCollate,
    Form::ColPk,
    Form::TblPk,
    Form::TblPkMulti,
    Form::FkChildColumn,
    Form::FkChildTable,
    Form::FkReferenced,
];

/// The ALTER COLUMN forms the quick run keeps: no key, and one form for each
/// kind of key (index, expression index, UNIQUE, PRIMARY KEY, FK child, FK parent).
const QUICK_ALTER_FORMS: &[Form] = &[
    Form::None,
    Form::IndexPlain,
    Form::IndexExprCastOwn,
    Form::TblUnique,
    Form::TblPk,
    Form::FkChildColumn,
    Form::FkReferenced,
];

/// Every (from, to) type pair that gets an ALTER COLUMN. `from` always allows
/// every key, so the key can be declared first.
///   - from the built-in base type of `to` (so the data converts), for every `to`;
///   - between types that allow keys: a custom type, a domain over it, the same
///     type with a different ENCODE, and back to the built-in type.
fn alter_pairs<'a>(
    types: &'a [TypeSpec],
    source_only: &'a [TypeSpec],
) -> Vec<(&'a TypeSpec, &'a TypeSpec)> {
    let find = |name: &str| {
        types
            .iter()
            .chain(source_only)
            .find(|t| t.name == name)
            .unwrap()
    };
    let mut pairs = Vec::new();
    for to in types {
        pairs.push((find(to.base), to));
    }
    for (from, to) in [
        ("upper_lt", "text"),
        ("upper_lt", "decode_value_lt"),
        ("decode_value_lt", "upper_lt"),
        ("upper_lt", "dom_upper_lt"),
        ("dom_upper_lt", "upper_lt"),
        ("upper_plain", "text"),
        ("dom_positive", "integer"),
        ("dom_uuid", "uuid"),
    ] {
        pairs.push((find(from), find(to)));
    }
    pairs
}

/// A single-column PRIMARY KEY on this type is the rowid.
fn is_rowid_alias_type(ty: &TypeSpec) -> bool {
    ty.name == "integer"
}

/// ALTER COLUMN cases that cannot be set up: the starting type already refuses
/// the key (a COLLATE on an array, an index on a type with a DECODE and no
/// OPERATOR '<'), so the table cannot be created first.
fn alter_case_is_skipped(form: Form, from: &TypeSpec) -> bool {
    matches!(expectation(form, from), Expect::RefuseExisting(_))
}

/// Types that only appear as the starting type of an ALTER COLUMN: the built-in
/// base types that no custom type in `types` is named after.
fn source_only_types() -> Vec<TypeSpec> {
    vec![
        TypeSpec {
            name: "real",
            setup: &[],
            base: "real",
            names: &[],
            kind: Kind::Allowed,
            sortable: true,
            index_needs_operator: false,
            collation_error: None,
            is_domain: false,
            pool: &["1.2", "1.4", "2.6", "3.0", "2.4"],
            probes: &["1.0", "3.0", "2.5"],
        },
        TypeSpec {
            name: "text[]",
            setup: &[],
            base: "text[]",
            names: &[],
            kind: Kind::Allowed,
            sortable: true,
            index_needs_operator: false,
            collation_error: Some("collations are not supported by type text_array"),
            is_domain: false,
            pool: &["'{A}'", "'{a}'", "'{b}'", "'{B}'", "'{c}'"],
            probes: &["'{a}'", "'{b}'", "'{z}'"],
        },
    ]
}

/// The rows for an ALTER COLUMN from `from` to `to`: values both types accept.
/// `dom_positive` refuses the negative integer in the integer pool.
fn alter_rows(from: &TypeSpec, to: &TypeSpec, rows: usize, seed: u64) -> Vec<DataRow> {
    let pool_of = if from.name == "dom_positive" {
        from
    } else {
        to
    };
    let mut out = gen_rows(pool_of, rows, seed);
    if to.name == "round_t" {
        // After ALTER COLUMN from real to round_t, INSERT of an integral value
        // such as 3.0 fails with "cannot store INT value in REAL column", with
        // no key involved. 2.9 rounds to the same value, so use it instead.
        for row in out.iter_mut().filter(|r| r.v == "3.0") {
            row.v = "2.9".to_string();
        }
    }
    out
}

/// What ALTER COLUMN must do to a column that has the key `form` on type `from`.
/// Every case is one of the three outcomes, as in `expectation`.
fn alter_expectation(form: Form, from: &TypeSpec, to: &TypeSpec) -> Expect {
    let lossy = to.kind == Kind::Lossy;
    let primary_key_cannot_be_altered =
        || Expect::RefuseExisting("PRIMARY KEY constraint cannot be altered".into());
    match form {
        // The new column definition replaces the old one, so an inline UNIQUE
        // or PRIMARY KEY would be lost. ALTER COLUMN refuses that today, for
        // every type.
        Form::ColUnique | Form::ColUniqueCollate => {
            Expect::RefuseExisting("UNIQUE constraint cannot be altered".into())
        }
        Form::ColPk => primary_key_cannot_be_altered(),
        // A table-level PRIMARY KEY(c) on an INTEGER column is the rowid. It
        // cannot become an ordinary key or the other way round.
        Form::TblPk | Form::FkReferenced
            if is_rowid_alias_type(from) != is_rowid_alias_type(to) =>
        {
            primary_key_cannot_be_altered()
        }
        // No key on `c`, or a key on an expression whose value is what the
        // query sees.
        Form::None
        | Form::IndexPartial
        | Form::IndexExprPlus
        | Form::IndexExprCastOwn
        | Form::IndexExprCastBase => Expect::Allow,
        _ if lossy => Expect::RefuseLossyKey,
        _ => Expect::Allow,
    }
}

// ---------------------------------------------------------------------------
// Running SQL
// ---------------------------------------------------------------------------

type Rows = Vec<Vec<String>>;

struct Db {
    // Keeps the database alive for as long as the connection is used.
    _db: Arc<Database>,
    conn: Arc<Connection>,
}

fn open() -> Db {
    let io = Arc::new(MemoryIO::new()) as Arc<dyn IO>;
    let db = Database::open_file_with_flags(
        io,
        ":memory:",
        OpenFlags::default(),
        DatabaseOpts::new().with_custom_types(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect(ConnectOpts::new()).unwrap();
    Db { _db: db, conn }
}

fn show(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Numeric(Numeric::Integer(x)) => x.to_string(),
        Value::Numeric(Numeric::Float(x)) => format!("{:?}", f64::from(*x)),
        Value::Text(x) => format!("'{}'", x.as_str()),
        Value::Blob(x) => format!(
            "x'{}'",
            x.iter().map(|b| format!("{b:02x}")).collect::<String>()
        ),
    }
}

fn run(db: &Db, sql: &str) -> Result<Rows, String> {
    let mut stmt = db.conn.prepare(sql).map_err(|e| e.to_string())?;
    let mut rows = Vec::new();
    stmt.run_with_row_callback(|row| {
        rows.push(row.get_values().map(show).collect());
        Ok(())
    })
    .map_err(|e| e.to_string())?;
    Ok(rows)
}

// ---------------------------------------------------------------------------
// Data
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct DataRow {
    id: i64,
    k: i64,
    v: String,
}

fn gen_rows(ty: &TypeSpec, rows: usize, seed: u64) -> Vec<DataRow> {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let mut out = Vec::new();
    for i in 0..rows {
        // The first rows walk the pool in order so that the equal-after-DECODE
        // pair is always present, then picks are random.
        let v = if i < ty.pool.len() {
            ty.pool[i].to_string()
        } else {
            ty.pool[rng.random_range(0..ty.pool.len())].to_string()
        };
        out.push(DataRow {
            id: i as i64 + 1,
            k: (i % 3) as i64,
            v,
        });
    }
    out.push(DataRow {
        id: rows as i64 + 1,
        k: 0,
        v: "NULL".to_string(),
    });
    out
}

fn insert_sql(table: &str, r: &DataRow) -> String {
    format!(
        "INSERT INTO {table}(id, k, c) VALUES ({}, {}, {})",
        r.id, r.k, r.v
    )
}

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq)]
enum Compare {
    SortedRows,
    Sequence,
    /// Both statements change a table; compare the tables afterwards.
    TableAfter,
}

struct Query {
    label: String,
    test: String,
    baseline: String,
    compare: Compare,
}

/// `col` as the query text should spell it. `plus` is the baseline spelling that
/// stops every index from being usable.
fn spell(col: &str, wrap: Wrap, plus: bool, ty: &TypeSpec) -> String {
    let spelled = match wrap {
        Wrap::Plain => col.to_string(),
        Wrap::Plus => format!("+{col}"),
        Wrap::CastOwn => format!("CAST({col} AS {})", ty.name),
        Wrap::CastBase => format!("CAST({col} AS {})", ty.base),
    };
    if plus {
        format!("+({spelled})")
    } else {
        spelled
    }
}

/// The shapes the quick run keeps: one for each way the planner can use an
/// index or build one (seek, range, IN list, IN subquery, EXISTS, joins,
/// DISTINCT, GROUP BY, compound select).
const QUICK_SHAPES: &[&str] = &[
    "point",
    "range < ",
    "range >= ",
    "in-list",
    "in-subquery",
    "exists",
    "join-inner",
    "join-left",
    "join-right",
    "distinct",
    "group-by",
    "union",
];

/// `decoded_sort` selects the sorter-only shapes (ORDER BY and PARTITION BY),
/// compared with the `+c` spelling. Without it those shapes are compared with
/// the same spelling on the twin, so only an index can make them differ.
fn queries(ty: &TypeSpec, wrap: Wrap, scale: &Scale) -> Vec<Query> {
    let (full, decoded_sort) = (scale.full_probes, scale.decoded_sort);
    let mut out = Vec::new();
    // Each shape is written once with {T} for the table and {c:alias} for a
    // column reference, then spelled twice: for the test and for the baseline.
    let mut shape = |label: String, template: String, compare: Compare| {
        let sorter_shape = label.starts_with("order-by") || label == "window-partition";
        if decoded_sort && !sorter_shape {
            return;
        }
        if scale.quick && !QUICK_SHAPES.iter().any(|prefix| label.starts_with(prefix)) {
            return;
        }
        let plus_baseline = !sorter_shape || decoded_sort;
        let render = |table: &str, plus: bool| {
            let mut s = template.replace("{T}", table);
            for alias in ["", "a.", "b.", "d.", "s."] {
                let marker = format!("{{{alias}c}}");
                s = s.replace(&marker, &spell(&format!("{alias}c"), wrap, plus, ty));
            }
            s
        };
        out.push(Query {
            label,
            test: render("t", false),
            baseline: render("n", plus_baseline),
            compare,
        });
    };
    let probes: Vec<&str> = if full {
        ty.probes.to_vec()
    } else {
        ty.probes.iter().take(2).copied().collect()
    };
    let p0 = ty.probes[0];
    let p1 = ty.probes[1];

    for p in &probes {
        shape(
            format!("point {p}"),
            format!("SELECT id FROM {{T}} WHERE {{c}} = {p}"),
            Compare::SortedRows,
        );
        for op in ["<", "<=", ">", ">="] {
            shape(
                format!("range {op} {p}"),
                format!("SELECT id FROM {{T}} WHERE {{c}} {op} {p}"),
                Compare::SortedRows,
            );
        }
    }
    shape(
        format!("between {p0} {p1}"),
        format!("SELECT id FROM {{T}} WHERE {{c}} BETWEEN {p0} AND {p1}"),
        Compare::SortedRows,
    );
    shape(
        "in-list".to_string(),
        format!("SELECT id FROM {{T}} WHERE {{c}} IN ({p0}, {p1})"),
        Compare::SortedRows,
    );
    shape(
        "not-in-list".to_string(),
        format!("SELECT id FROM {{T}} WHERE {{c}} NOT IN ({p0}, {p1})"),
        Compare::SortedRows,
    );
    shape(
        "in-subquery".to_string(),
        "SELECT id FROM {T} WHERE {c} IN (SELECT {c} FROM {T} WHERE k >= 1)".to_string(),
        Compare::SortedRows,
    );
    shape(
        "exists".to_string(),
        "SELECT id FROM {T} a WHERE EXISTS (SELECT 1 FROM {T} b WHERE {b.c} = {a.c} AND b.id <> a.id)"
            .to_string(),
        Compare::SortedRows,
    );
    shape(
        "scalar-subquery".to_string(),
        "SELECT id, (SELECT count(*) FROM {T} b WHERE {b.c} = {a.c}) FROM {T} a".to_string(),
        Compare::SortedRows,
    );
    for (name, join) in [
        ("join-inner", "JOIN"),
        ("join-left", "LEFT JOIN"),
        ("join-right", "RIGHT JOIN"),
        ("join-cross-where", "CROSS JOIN"),
    ] {
        let template = if join == "CROSS JOIN" {
            "SELECT a.id, b.id FROM {T} a CROSS JOIN {T} b WHERE {a.c} = {b.c}".to_string()
        } else {
            format!("SELECT a.id, b.id FROM {{T}} a {join} {{T}} b ON {{a.c}} = {{b.c}}")
        };
        shape(name.to_string(), template, Compare::SortedRows);
    }
    shape(
        "join-extra-on".to_string(),
        "SELECT a.id, b.id FROM {T} a JOIN {T} b ON {a.c} = {b.c} AND a.k <= b.k".to_string(),
        Compare::SortedRows,
    );
    shape(
        "join-subquery".to_string(),
        "SELECT a.id, s.id FROM {T} a JOIN (SELECT id, c FROM {T} WHERE k < 2) s ON {a.c} = {s.c}"
            .to_string(),
        Compare::SortedRows,
    );
    shape(
        "join-union-subquery".to_string(),
        "SELECT a.id, s.id FROM {T} a JOIN (SELECT id, c FROM {T} UNION ALL SELECT id, c FROM {T}) s ON {a.c} = {s.c}"
            .to_string(),
        Compare::SortedRows,
    );
    shape(
        "join-3way".to_string(),
        "SELECT a.id, b.id, d.id FROM {T} a JOIN {T} b ON {a.c} = {b.c} JOIN {T} d ON {b.c} = {d.c}"
            .to_string(),
        Compare::SortedRows,
    );
    shape(
        "join-where-filter".to_string(),
        format!(
            "SELECT a.id, b.id FROM {{T}} a JOIN {{T}} b ON {{a.c}} = {{b.c}} WHERE {{a.c}} >= {p0}"
        ),
        Compare::SortedRows,
    );
    shape(
        "distinct".to_string(),
        "SELECT DISTINCT {c} FROM {T}".to_string(),
        Compare::SortedRows,
    );
    shape(
        "group-by".to_string(),
        "SELECT {c}, count(*) FROM {T} GROUP BY {c}".to_string(),
        Compare::SortedRows,
    );
    shape(
        "count-distinct".to_string(),
        "SELECT count(DISTINCT {c}) FROM {T}".to_string(),
        Compare::SortedRows,
    );
    shape(
        "min-max".to_string(),
        "SELECT min({c}), max({c}) FROM {T}".to_string(),
        Compare::SortedRows,
    );
    shape(
        "order-by".to_string(),
        "SELECT id FROM {T} ORDER BY {c}, id".to_string(),
        Compare::Sequence,
    );
    shape(
        "order-by-desc".to_string(),
        "SELECT id FROM {T} ORDER BY {c} DESC, id".to_string(),
        Compare::Sequence,
    );
    shape(
        "order-by-range".to_string(),
        format!("SELECT id FROM {{T}} WHERE {{c}} >= {p0} ORDER BY {{c}}, id"),
        Compare::Sequence,
    );
    shape(
        "union".to_string(),
        "SELECT {c} FROM {T} UNION SELECT {c} FROM {T} WHERE k = 0".to_string(),
        Compare::SortedRows,
    );
    shape(
        "intersect".to_string(),
        "SELECT {c} FROM {T} INTERSECT SELECT {c} FROM {T} WHERE k = 0".to_string(),
        Compare::SortedRows,
    );
    shape(
        "except".to_string(),
        "SELECT {c} FROM {T} EXCEPT SELECT {c} FROM {T} WHERE k = 0".to_string(),
        Compare::SortedRows,
    );
    shape(
        "window-partition".to_string(),
        "SELECT id, count(*) OVER (PARTITION BY {c}) FROM {T}".to_string(),
        Compare::SortedRows,
    );
    out
}

/// UPDATE and DELETE shapes. Each is run on both tables, then the tables are compared.
fn dml(ty: &TypeSpec, wrap: Wrap) -> Vec<(String, String, String)> {
    let p0 = ty.probes[0];
    let p1 = ty.probes[1];
    let mut out = Vec::new();
    let mut add = |label: &str, template: String| {
        let render = |table: &str, plus: bool| {
            template
                .replace("{T}", table)
                .replace("{c}", &spell("c", wrap, plus, ty))
        };
        out.push((label.to_string(), render("t", false), render("n", true)));
    };
    add(
        "update-by-key",
        format!("UPDATE {{T}} SET k = k + 10 WHERE {{c}} = {p0}"),
    );
    add(
        "update-by-range",
        format!("UPDATE {{T}} SET k = k + 100 WHERE {{c}} >= {p1}"),
    );
    add(
        "delete-by-key",
        format!("DELETE FROM {{T}} WHERE {{c}} = {p1}"),
    );
    add(
        "delete-by-in",
        format!("DELETE FROM {{T}} WHERE {{c}} IN ({p0}, {p1})"),
    );
    out
}
// ---------------------------------------------------------------------------
// Findings
// ---------------------------------------------------------------------------

/// Two statements that must agree. `pre` is run on both sides first.
#[derive(Clone)]
struct Check {
    pre: Vec<(String, String)>,
    test: String,
    baseline: String,
    compare: Compare,
}

enum Outcome {
    Same,
    /// The baseline cannot answer, so there is nothing to compare with.
    Skipped,
    TestError(String),
    Different {
        expected: String,
        got: String,
    },
}

impl Outcome {
    fn is_failure(&self) -> bool {
        matches!(self, Outcome::TestError(_) | Outcome::Different { .. })
    }

    fn same_kind(&self, other: &Outcome) -> bool {
        matches!(
            (self, other),
            (Outcome::TestError(_), Outcome::TestError(_))
                | (Outcome::Different { .. }, Outcome::Different { .. })
        )
    }
}

struct Finding {
    /// The rule that broke, for example "accepted but should refuse".
    cause: String,
    ty: &'static str,
    form: String,
    label: String,
    detail: String,
    /// Statements that built the state, in order.
    script: Vec<String>,
    check: Option<Check>,
    form_id: Form,
    data_first: bool,
    rows: Vec<DataRow>,
    alter_from: Option<&'static str>,
    outcome_failure: Option<Outcome>,
}

#[derive(Default)]
struct Stats {
    cases: usize,
    by_type_kind: BTreeMap<String, usize>,
    by_form: BTreeMap<String, usize>,
    declaration_checks: usize,
    expect_allow: usize,
    expect_refuse_new_rule: usize,
    expect_refuse_existing_rule: usize,
    comparisons: usize,
    skipped_baseline_error: usize,
    skipped_operator_rule: usize,
    skipped_no_baseline: usize,
    alter_skipped: usize,
    dml_comparisons: usize,
    unique_checks: usize,
    statements: usize,
}

struct Scale {
    rows: usize,
    seeds: Vec<u64>,
    full_probes: bool,
    /// Only ORDER BY and PARTITION BY shapes, compared in decoded order.
    decoded_sort: bool,
    /// Fewer query shapes and UPDATE/DELETE statements, and no query comparison
    /// for a key that was refused (the table has no key then, so only the form
    /// `None` is needed for the automatic-index cases). For the default run.
    quick: bool,
}

struct Case<'a> {
    /// The type of column `c` that the queries run on. For an ALTER COLUMN case
    /// this is the type the column is changed to.
    ty: &'a TypeSpec,
    form: Form,
    /// For a CREATE INDEX form: the rows go in before the index. For an ALTER
    /// COLUMN case: the rows go in before the type change.
    data_first: bool,
    rows: Vec<DataRow>,
    /// Set for an ALTER COLUMN case: the table is created with this type for `c`
    /// and the key `form`, then `c` is changed to `ty`.
    alter_from: Option<&'a TypeSpec>,
}

impl Case<'_> {
    fn form_name(&self) -> String {
        let mut name = format!("{:?}", self.form);
        if let Some(from) = self.alter_from {
            name = format!("{name} after ALTER COLUMN from {}", from.name);
        }
        if self.data_first {
            name.push_str("/data-first");
        }
        name
    }

    /// The type the declaration `script` is written for.
    fn declared_ty(&self) -> &TypeSpec {
        self.alter_from.unwrap_or(self.ty)
    }

    fn expectation(&self) -> Expect {
        match self.alter_from {
            Some(from) => alter_expectation(self.form, from, self.ty),
            None => expectation(self.form, self.ty),
        }
    }

    fn finding(&self, cause: &str, label: &str, detail: String, built: &Built) -> Finding {
        Finding {
            cause: cause.to_string(),
            ty: self.ty.name,
            form: self.form_name(),
            label: label.to_string(),
            detail,
            script: built.statements.clone(),
            check: None,
            form_id: self.form,
            data_first: self.data_first,
            rows: self.rows.clone(),
            alter_from: self.alter_from.map(|t| t.name),
            outcome_failure: None,
        }
    }
}

fn tag(kind: Kind) -> &'static str {
    match kind {
        Kind::Lossy => "lossy",
        Kind::Allowed => "allowed",
    }
}

// ---------------------------------------------------------------------------
// Building one case
// ---------------------------------------------------------------------------

/// The database after the type, the tables, the key and the data are created.
struct Built {
    db: Db,
    decl_error: Option<String>,
    /// Every statement that was run, in order.
    statements: Vec<String>,
    /// Problems found while loading the data (unique key checks).
    load_problems: Vec<(String, String, String)>,
    has_t: bool,
}

fn build(case: &Case, stats: &mut Stats) -> Built {
    match case.alter_from {
        Some(from) => build_altered(case, from, stats),
        None => build_declared(case, stats),
    }
}

/// Runs `sql` and records it in `statements`.
fn exec(db: &Db, sql: &str, statements: &mut Vec<String>) -> Result<Rows, String> {
    statements.push(sql.to_string());
    run(db, sql)
}

fn build_declared(case: &Case, stats: &mut Stats) -> Built {
    let ty = case.ty;
    let sc = script(case.form, ty);
    let db = open();
    let mut statements: Vec<String> = Vec::new();
    let mut load_problems = Vec::new();
    for s in ty.setup.iter().map(|s| s.to_string()).chain(sc.pre.clone()) {
        if let Err(e) = exec(&db, &s, &mut statements) {
            panic!("harness bug: setup statement failed: {s}\n{e}");
        }
    }
    // The twin never has a key. Its column type is the same.
    let twin = format!(
        "CREATE TABLE n(id INTEGER PRIMARY KEY, k INTEGER, c {} {}) STRICT",
        ty.name, sc.twin_collation
    );
    if exec(&db, &twin, &mut statements).is_err() {
        // Some types (arrays) take no collation. Then the key cannot declare
        // one either, so the twin goes without.
        statements.pop();
        let twin = twin.replace(sc.twin_collation, "");
        exec(&db, &twin, &mut statements).unwrap();
    }

    let rows: Vec<&DataRow> = case
        .rows
        .iter()
        .filter(|r| !(sc.not_null && r.v == "NULL"))
        .collect();

    if case.data_first {
        // The table already exists. Load the rows, then declare the key.
        for r in &rows {
            exec(&db, &insert_sql("t", r), &mut statements).unwrap();
            exec(&db, &insert_sql("n", r), &mut statements).unwrap();
        }
    }

    let mut decl_error = None;
    for s in &sc.decl {
        if let Err(e) = exec(&db, s, &mut statements) {
            decl_error = Some(e);
            break;
        }
    }
    let has_t = sc.data_phase && run(&db, "SELECT count(*) FROM t").is_ok();

    if has_t && !case.data_first {
        let unique = if decl_error.is_none() {
            sc.unique
        } else {
            Unique::No
        };
        load_problems = load_rows(&db, &rows, unique, stats, &mut statements);
    }
    stats.statements += statements.len();
    Built {
        db,
        decl_error,
        statements,
        load_problems,
        has_t,
    }
}

/// An ALTER COLUMN case: `t` is created with the key `case.form` on a column of
/// type `from`, then the column's type is changed to `case.ty`. The twin `n`
/// has no key and goes through the same type change, so both tables hold the
/// same values and differ only in the key.
fn build_altered(case: &Case, from: &TypeSpec, stats: &mut Stats) -> Built {
    let ty = case.ty;
    let sc = script(case.form, from);
    let db = open();
    let mut statements: Vec<String> = Vec::new();
    let mut load_problems = Vec::new();

    let mut setup: Vec<String> = Vec::new();
    for s in from.setup.iter().chain(ty.setup) {
        if !setup.iter().any(|done| done == s) {
            setup.push(s.to_string());
        }
    }
    for s in setup.iter().chain(&sc.pre).chain(&sc.decl) {
        if let Err(e) = exec(&db, s, &mut statements) {
            panic!("harness bug: statement for the starting table failed: {s}\n{e}");
        }
    }
    exec(
        &db,
        &format!(
            "CREATE TABLE n(id INTEGER PRIMARY KEY, k INTEGER, c {}) STRICT",
            from.name
        ),
        &mut statements,
    )
    .unwrap();

    let rows: Vec<&DataRow> = case
        .rows
        .iter()
        .filter(|r| !(sc.not_null && r.v == "NULL"))
        .collect();
    if case.data_first {
        for r in &rows {
            exec(&db, &insert_sql("n", r), &mut statements).unwrap();
            if exec(&db, &insert_sql("t", r), &mut statements).is_err() {
                // The starting key refuses this row (an equal key). Neither table keeps it.
                exec(
                    &db,
                    &format!("DELETE FROM n WHERE id = {}", r.id),
                    &mut statements,
                )
                .unwrap();
            }
        }
    }

    // Change the twin first. Rows that become equal under the new type's `=`
    // cannot stay in a table with a unique key, so drop them from both tables.
    let alter = |table: &str| format!("ALTER TABLE {table} ALTER COLUMN c TO d {}", ty.name);
    let rename = |table: &str| format!("ALTER TABLE {table} RENAME COLUMN d TO c");
    for sql in [alter("n"), rename("n")] {
        if let Err(e) = exec(&db, &sql, &mut statements) {
            panic!("harness bug: the keyless twin cannot change type: {sql}\n{e}");
        }
    }
    if case.data_first && sc.unique != Unique::No {
        let same_k = if sc.unique == Unique::OnKC {
            " AND a.k = b.k"
        } else {
            ""
        };
        let merged = run(
            &db,
            &format!(
                "SELECT a.id FROM n a WHERE EXISTS (SELECT 1 FROM n b WHERE b.id < a.id \
                 AND +a.c = +b.c{same_k})"
            ),
        )
        .unwrap();
        for id in merged.iter().map(|r| r[0].clone()) {
            for table in ["n", "t"] {
                exec(
                    &db,
                    &format!("DELETE FROM {table} WHERE id = {id}"),
                    &mut statements,
                )
                .unwrap();
            }
        }
    }

    // When the change is refused, `t` and `n` have different types, so nothing is compared.
    let decl_error = exec(&db, &alter("t"), &mut statements).err();
    let mut has_t = false;
    if decl_error.is_none() {
        match exec(&db, &rename("t"), &mut statements) {
            Ok(_) => has_t = true,
            Err(e) => load_problems.push((
                "rename after ALTER COLUMN fails".to_string(),
                "rename".to_string(),
                e,
            )),
        }
    }
    if has_t && !case.data_first {
        load_problems.extend(load_rows(&db, &rows, sc.unique, stats, &mut statements));
    }
    stats.statements += statements.len();
    Built {
        db,
        decl_error,
        statements,
        load_problems,
        has_t,
    }
}

/// Inserts `rows` into the twin `n` (no key, so it accepts everything) and into
/// `t`, and reports what `t` does wrong. A row must be rejected exactly when
/// `unique` says another row has an equal key under the type's `=` (compared
/// column to column, with `+` so no index is used). NULLs are never equal.
fn load_rows(
    db: &Db,
    rows: &[&DataRow],
    unique: Unique,
    stats: &mut Stats,
    statements: &mut Vec<String>,
) -> Vec<(String, String, String)> {
    let mut problems = Vec::new();
    for r in rows {
        exec(db, &insert_sql("n", r), statements).unwrap();
        let model_dup = match unique {
            Unique::No => false,
            _ if r.v == "NULL" => false,
            unique => {
                stats.unique_checks += 1;
                let same_k = if unique == Unique::OnKC {
                    " AND a.k = b.k"
                } else {
                    ""
                };
                let q = format!(
                    "SELECT count(*) FROM n a, n b WHERE a.id = {} AND b.id <> a.id \
                     AND +a.c = +b.c{same_k}",
                    r.id
                );
                run(db, &q).map(|rows| rows[0][0] != "0").unwrap_or(false)
            }
        };
        match exec(db, &insert_sql("t", r), statements) {
            Ok(_) => {
                if model_dup {
                    problems.push((
                        "unique key accepts equal keys".to_string(),
                        format!("insert {}", r.v),
                        format!("row {} was accepted but a stored row is equal under =", r.v),
                    ));
                }
            }
            Err(e) => {
                exec(
                    db,
                    &format!("DELETE FROM n WHERE id = {}", r.id),
                    statements,
                )
                .unwrap();
                let unique_error = e.contains("UNIQUE") || e.contains("PRIMARY KEY");
                if !(model_dup && unique_error) {
                    let cause = if unique_error {
                        "unique key rejects distinct keys"
                    } else {
                        "insert fails"
                    };
                    problems.push((cause.to_string(), format!("insert {}", r.v), e));
                }
            }
        }
    }
    problems
}

fn check_declaration(case: &Case, built: &Built, stats: &mut Stats) -> Vec<Finding> {
    let ty = case.ty;
    let expect = case.expectation();
    stats.declaration_checks += 1;
    match expect {
        Expect::Allow => stats.expect_allow += 1,
        Expect::RefuseLossyKey => stats.expect_refuse_new_rule += 1,
        Expect::RefuseExisting(_) => stats.expect_refuse_existing_rule += 1,
    }
    let mut out = Vec::new();
    match (&expect, &built.decl_error) {
        (Expect::RefuseLossyKey, None) => out.push(case.finding(
            "accepted but should refuse",
            "declaration",
            "no error".to_string(),
            built,
        )),
        (Expect::RefuseLossyKey, Some(msg)) => {
            // After an ALTER COLUMN the key column is `d` while the statement
            // runs, and the message may call it either `c` or `d`.
            let hint_ok = ["c", "d"]
                .iter()
                .any(|col| msg.contains(&format!("CAST({col} AS {})", ty.base)));
            let names_type = ty.names.iter().any(|n| msg.contains(n));
            if !hint_ok || !names_type {
                out.push(case.finding(
                    "refused without the type name and CAST hint",
                    "declaration",
                    msg.clone(),
                    built,
                ));
            }
        }
        (Expect::RefuseExisting(want), None) => out.push(case.finding(
            "accepted but an existing rule refuses it",
            "declaration",
            format!("no error, expected: {want}"),
            built,
        )),
        (Expect::RefuseExisting(want), Some(msg)) => {
            if !msg.contains(want.as_str()) {
                out.push(case.finding(
                    "refused with a different message than today's",
                    "declaration",
                    format!("got: {msg}\n    expected: {want}"),
                    built,
                ));
            }
        }
        (Expect::Allow, Some(msg)) => out.push(case.finding(
            "refused but should allow",
            "declaration",
            msg.clone(),
            built,
        )),
        (Expect::Allow, None) => {}
    }
    out
}

// ---------------------------------------------------------------------------
// Comparing queries
// ---------------------------------------------------------------------------

fn normalized(mut rows: Rows, compare: Compare) -> Rows {
    if compare == Compare::SortedRows {
        rows.sort();
    }
    rows
}

/// Runs the test and baseline statements of `check` (not `pre`).
fn evaluate(db: &Db, check: &Check) -> Outcome {
    let baseline = run(db, &check.baseline);
    let Ok(baseline) = baseline else {
        return Outcome::Skipped;
    };
    let test = match run(db, &check.test) {
        Ok(r) => r,
        Err(e) => return Outcome::TestError(e),
    };
    let (expected, got) = if check.compare == Compare::TableAfter {
        // The statements were UPDATE/DELETE; compare what is left in the tables.
        (
            run(db, "SELECT id, k, c FROM n ORDER BY id").unwrap(),
            run(db, "SELECT id, k, c FROM t ORDER BY id").unwrap(),
        )
    } else {
        (
            normalized(baseline, check.compare),
            normalized(test, check.compare),
        )
    };
    if expected == got {
        Outcome::Same
    } else {
        Outcome::Different {
            expected: format!("{expected:?}"),
            got: format!("{got:?}"),
        }
    }
}

fn replay(db: &Db, check: &Check) -> Outcome {
    for (t, n) in &check.pre {
        let _ = run(db, t);
        let _ = run(db, n);
    }
    evaluate(db, check)
}

fn plan_class(db: &Db, sql: &str) -> String {
    let Ok(rows) = run(db, &format!("EXPLAIN QUERY PLAN {sql}")) else {
        return "no plan".to_string();
    };
    let text = rows
        .iter()
        .map(|r| r.join(" "))
        .collect::<Vec<_>>()
        .join(" | ")
        .to_uppercase();
    if text.contains("AUTOMATIC") || text.contains("EPHEMERAL") {
        "automatic index"
    } else if text.contains("USING INDEX")
        || text.contains("USING COVERING INDEX")
        || text.contains("SQLITE_AUTOINDEX")
    {
        "declared key"
    } else if text.contains("HASH") {
        "hash join"
    } else if text.contains("SORTER") || text.contains("TEMP B-TREE") {
        "sorter or temp b-tree"
    } else {
        "no index in the plan"
    }
    .to_string()
}

fn compare_queries(case: &Case, built: &Built, stats: &mut Stats, scale: &Scale) -> Vec<Finding> {
    let ty = case.ty;
    let sc = script(case.form, case.declared_ty());
    let mut out = Vec::new();
    if !ty.expressions_decode() {
        stats.skipped_no_baseline += 1;
        return out;
    }
    let mut checks: Vec<(String, Check)> = Vec::new();
    for q in queries(ty, sc.wrap, scale) {
        checks.push((
            q.label,
            Check {
                pre: vec![],
                test: q.test,
                baseline: q.baseline,
                compare: q.compare,
            },
        ));
    }
    // UPDATE and DELETE run in order on the live tables, so each check
    // records the earlier ones in `pre`.
    let mut applied: Vec<(String, String)> = Vec::new();
    let mut dml_checks = Vec::new();
    let dml_shapes = if scale.decoded_sort {
        vec![]
    } else {
        dml(ty, sc.wrap)
            .into_iter()
            .filter(|(label, ..)| {
                !scale.quick || label == "update-by-key" || label == "delete-by-in"
            })
            .collect()
    };
    for (label, t_sql, n_sql) in dml_shapes {
        dml_checks.push((
            label,
            Check {
                pre: applied.clone(),
                test: t_sql.clone(),
                baseline: n_sql.clone(),
                compare: Compare::TableAfter,
            },
        ));
        applied.push((t_sql, n_sql));
    }
    let n_selects = checks.len();
    checks.extend(dml_checks);

    for (i, (label, check)) in checks.into_iter().enumerate() {
        let is_dml = i >= n_selects;
        stats.statements += 2;
        let outcome = evaluate(&built.db, &check);
        if is_dml {
            stats.dml_comparisons += 1;
        }
        match &outcome {
            Outcome::Skipped => {
                stats.skipped_baseline_error += 1;
                continue;
            }
            Outcome::Same => {
                stats.comparisons += 1;
                continue;
            }
            // ORDER BY and window functions on a type without OPERATOR '<' are
            // refused by a rule that already exists and is not part of this one.
            Outcome::TestError(e) if !ty.sortable && e.contains("does not declare OPERATOR") => {
                stats.skipped_operator_rule += 1;
                continue;
            }
            _ => stats.comparisons += 1,
        }
        let (cause, detail) = match &outcome {
            Outcome::TestError(e) => (
                "query fails where the baseline works".to_string(),
                e.clone(),
            ),
            Outcome::Different { expected, got } => {
                let class = if is_dml {
                    "UPDATE/DELETE".to_string()
                } else {
                    plan_class(&built.db, &check.test)
                };
                (
                    format!("wrong rows ({class})"),
                    format!("expected (baseline) {expected}\n      got                {got}"),
                )
            }
            _ => unreachable!(),
        };
        let mut f = case.finding(&cause, &label, detail, built);
        f.check = Some(check);
        f.outcome_failure = Some(outcome);
        out.push(f);
        if is_dml {
            // The two tables now differ, so later steps would only repeat this.
            break;
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Minimizer
// ---------------------------------------------------------------------------

/// Removes data rows while the same failure keeps happening, then returns the
/// script that shows it.
fn minimize(finding: &Finding) -> Option<Vec<String>> {
    let check = finding.check.as_ref()?;
    let want = finding.outcome_failure.as_ref()?;
    let all_types: Vec<TypeSpec> = types().into_iter().chain(source_only_types()).collect();
    let find = |name: &str| all_types.iter().find(|t| t.name == name).unwrap();
    let ty = find(finding.ty);
    let alter_from = finding.alter_from.map(find);
    let still_fails = |rows: &[DataRow]| -> Option<Built> {
        let case = Case {
            ty,
            form: finding.form_id,
            data_first: finding.data_first,
            rows: rows.to_vec(),
            alter_from,
        };
        let built = build(&case, &mut Stats::default());
        if !built.has_t {
            return None;
        }
        let outcome = replay(&built.db, check);
        (outcome.is_failure() && outcome.same_kind(want)).then_some(built)
    };
    let mut rows = finding.rows.clone();
    let mut i = 0;
    while i < rows.len() {
        let mut trial = rows.clone();
        trial.remove(i);
        if still_fails(&trial).is_some() {
            rows = trial;
        } else {
            i += 1;
        }
    }
    let built = still_fails(&rows)?;
    let mut script = built.statements;
    for (t, n) in &check.pre {
        script.push(format!("{t} -- test side"));
        script.push(format!("{n} -- baseline side"));
    }
    script.push(format!("{} -- test", check.test));
    script.push(format!(
        "{} -- baseline; must give the same {}",
        check.baseline,
        if check.compare == Compare::TableAfter {
            "table contents (compare SELECT id, k, c FROM t and FROM n)"
        } else {
            "rows"
        }
    ));
    Some(script)
}

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

fn run_matrix(scale: &Scale) -> (Stats, Vec<Finding>) {
    let mut stats = Stats::default();
    let mut all: Vec<Finding> = Vec::new();
    let types = types();
    for ty in &types {
        for form in FORMS
            .iter()
            .filter(|f| !scale.decoded_sort || **f == Form::None)
        {
            let orders: &[bool] =
                if script(*form, ty).index_on_existing_table && *form != Form::IndexUnique {
                    &[false, true]
                } else {
                    &[false]
                };
            for &data_first in orders {
                for &seed in &scale.seeds {
                    let case = Case {
                        ty,
                        form: *form,
                        data_first,
                        rows: gen_rows(ty, scale.rows, seed),
                        alter_from: None,
                    };
                    run_case(&case, seed == scale.seeds[0], scale, &mut stats, &mut all);
                }
            }
        }
    }
    // ALTER COLUMN on a table that already has a key. The sorter-only shapes
    // (`decoded_sort`) do not depend on keys, so that run skips these.
    if !scale.decoded_sort {
        let source_only = source_only_types();
        for (from, to) in alter_pairs(&types, &source_only) {
            for form in ALTER_FORMS
                .iter()
                .filter(|f| !scale.quick || QUICK_ALTER_FORMS.contains(f))
            {
                if alter_case_is_skipped(*form, from) {
                    stats.alter_skipped += 1;
                    continue;
                }
                for data_first in [false, true] {
                    for &seed in &scale.seeds {
                        let case = Case {
                            ty: to,
                            form: *form,
                            data_first,
                            rows: alter_rows(from, to, scale.rows, seed),
                            alter_from: Some(from),
                        };
                        run_case(&case, seed == scale.seeds[0], scale, &mut stats, &mut all);
                    }
                }
            }
        }
    }
    (stats, all)
}

/// Builds one case and checks the declaration (once per seed group), the
/// loading of rows, and the queries.
fn run_case(
    case: &Case,
    check_the_declaration: bool,
    scale: &Scale,
    stats: &mut Stats,
    all: &mut Vec<Finding>,
) {
    stats.cases += 1;
    let kind = format!(
        "{}{}{}",
        if case.alter_from.is_some() {
            "alter column to "
        } else {
            ""
        },
        tag(case.ty.kind),
        if case.ty.is_domain { " (domain)" } else { "" }
    );
    *stats.by_type_kind.entry(kind).or_default() += 1;
    let form = match case.alter_from {
        Some(_) => format!("alter:{:?}", case.form),
        None => case.form_name(),
    };
    *stats.by_form.entry(form).or_default() += 1;

    let built = build(case, stats);
    // The declaration does not depend on the data; report it once.
    if check_the_declaration {
        all.extend(check_declaration(case, &built, stats));
    }
    for (cause, label, detail) in &built.load_problems {
        all.push(case.finding(cause, label, detail.clone(), &built));
    }
    // A refused key leaves a table without it, which behaves like `None`.
    let key_was_refused = built.decl_error.is_some() && case.form != Form::None;
    if built.has_t && !(scale.quick && key_was_refused) {
        all.extend(compare_queries(case, &built, stats, scale));
    }
}

/// How many findings get a minimized repro. The first finding of every cause
/// always gets one.
const MINIMIZE_LIMIT: usize = 40;

fn report(stats: &Stats, findings: &[Finding]) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "lossy_index_keys matrix");
    let _ = writeln!(out, "  cases (type x form x order x seed): {}", stats.cases);
    for (k, v) in &stats.by_type_kind {
        let _ = writeln!(out, "    type kind {k}: {v}");
    }
    let forms: Vec<String> = stats
        .by_form
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect();
    let _ = writeln!(out, "  forms: {}", forms.join(", "));
    let _ = writeln!(
        out,
        "  declaration checks: {} (allow {}, refuse by the new rule {}, refuse by an existing rule {})",
        stats.declaration_checks,
        stats.expect_allow,
        stats.expect_refuse_new_rule,
        stats.expect_refuse_existing_rule
    );
    let _ = writeln!(
        out,
        "  query comparisons: {} select, {} update/delete; skipped {} (baseline cannot answer) \
         and {} (ORDER BY or window on a type without OPERATOR '<')",
        stats.comparisons - stats.dml_comparisons,
        stats.dml_comparisons,
        stats.skipped_baseline_error,
        stats.skipped_operator_rule
    );
    let _ = writeln!(
        out,
        "  cases with no query comparison (array of a lossy type, no sound baseline): {}",
        stats.skipped_no_baseline
    );
    let _ = writeln!(
        out,
        "  ALTER COLUMN cases not run (starting type refuses the key): {}",
        stats.alter_skipped
    );
    let _ = writeln!(out, "  unique-key model checks: {}", stats.unique_checks);
    let _ = writeln!(out, "  statements executed: {}", stats.statements);
    let _ = writeln!(out, "  findings: {}", findings.len());

    // cause -> type -> findings
    let mut groups: BTreeMap<&str, BTreeMap<&str, Vec<&Finding>>> = BTreeMap::new();
    for f in findings {
        groups
            .entry(&f.cause)
            .or_default()
            .entry(f.ty)
            .or_default()
            .push(f);
    }
    let mut minimized = 0;
    for (cause, by_type) in &groups {
        let total: usize = by_type.values().map(|v| v.len()).sum();
        let _ = writeln!(out, "\n== {cause}: {total} finding(s)");
        for (i, (ty, list)) in by_type.iter().enumerate() {
            let mut forms: BTreeMap<&str, usize> = BTreeMap::new();
            let mut labels: BTreeMap<&str, usize> = BTreeMap::new();
            for f in list {
                *forms.entry(f.form.as_str()).or_default() += 1;
                *labels.entry(f.label.as_str()).or_default() += 1;
            }
            let join = |m: &BTreeMap<&str, usize>| {
                let mut s = m.keys().take(8).copied().collect::<Vec<_>>().join(", ");
                if m.len() > 8 {
                    let _ = write!(s, ", ... ({} in all)", m.len());
                }
                s
            };
            let _ = writeln!(out, "  [{ty}] forms: {}", join(&forms));
            let _ = writeln!(out, "         shapes: {}", join(&labels));
            let first = list[0];
            let _ = writeln!(
                out,
                "    example {} / {}: {}",
                first.form, first.label, first.detail
            );
            let want_repro = i == 0 || minimized < MINIMIZE_LIMIT;
            let repro = if first.label == "declaration" {
                Some(first.script.clone())
            } else if want_repro {
                minimized += 1;
                minimize(first)
            } else {
                None
            };
            if let Some(lines) = repro {
                let _ = writeln!(out, "    repro:");
                for line in lines {
                    let _ = writeln!(out, "      {};", line.trim_end_matches(';'));
                }
            }
        }
    }
    out
}

fn run_and_report(scale: Scale) {
    let started = std::time::Instant::now();
    let (stats, findings) = run_matrix(&scale);
    let matrix_time = started.elapsed();
    let mut text = report(&stats, &findings);
    let _ = writeln!(
        text,
        "\nmatrix: {matrix_time:?}, with report and minimizing: {:?}",
        started.elapsed()
    );
    if let Ok(path) = std::env::var("LOSSY_INDEX_REPORT") {
        std::fs::write(path, &text).unwrap();
    }
    if !findings.is_empty() {
        panic!("{text}");
    }
    println!("{text}");
}

#[test]
fn lossy_index_keys_default() {
    run_and_report(Scale {
        rows: 8,
        seeds: vec![1],
        full_probes: false,
        decoded_sort: false,
        quick: true,
    });
}

/// Larger run: more rows, many seeds, every probe value.
#[test]
#[ignore]
fn lossy_index_keys_full() {
    run_and_report(Scale {
        rows: 16,
        seeds: (1..=4).collect(),
        full_probes: true,
        decoded_sort: false,
        quick: false,
    });
}

/// Not about keys. ORDER BY c and PARTITION BY c on a lossy type use the stored
/// form (so 'B' sorts before 'a'), while `ORDER BY +c` and `PARTITION BY +c` use
/// the decoded form. This is wrong rows with no index involved, so refusing keys
/// does not fix it. It stays ignored so it does not hide the key results; run it
/// to see the current state.
#[test]
#[ignore]
fn lossy_sort_and_partition_use_decoded_value() {
    run_and_report(Scale {
        rows: 8,
        seeds: vec![1],
        full_probes: false,
        decoded_sort: true,
        quick: false,
    });
}

/// Prints what every declaration form and every ALTER COLUMN does on every type
/// today. Use it to see the exact refusal messages that `expectation` and
/// `alter_expectation` pin.
#[test]
#[ignore]
fn print_declaration_outcomes() {
    let mut stats = Stats::default();
    let types = types();
    let source_only = source_only_types();
    let mut cases = Vec::new();
    for ty in &types {
        for form in FORMS {
            let rows = gen_rows(ty, 2, 1);
            cases.push(Case {
                ty,
                form: *form,
                data_first: false,
                rows,
                alter_from: None,
            });
        }
    }
    for (from, to) in alter_pairs(&types, &source_only) {
        for form in ALTER_FORMS
            .iter()
            .filter(|f| !alter_case_is_skipped(**f, from))
        {
            cases.push(Case {
                ty: to,
                form: *form,
                data_first: true,
                rows: alter_rows(from, to, 4, 1),
                alter_from: Some(from),
            });
        }
    }
    for case in &cases {
        let built = build(case, &mut stats);
        println!(
            "{:<16} {:<48} {}",
            case.ty.name,
            case.form_name(),
            built.decl_error.as_deref().unwrap_or("ok")
        );
    }
}
