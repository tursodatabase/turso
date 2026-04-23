//! Custom types fuzzer (structs, unions, arrays, custom scalars).
//!
//! Generates random STRUCT and UNION type hierarchies, tables with custom-type
//! columns (including arrays), expression indexes over those columns, and DML
//! (INSERT / UPDATE / DELETE / SELECT) — then checks for panics, internal
//! errors, and self-consistency violations.
//!
//! Deterministic: same seed ⇒ same run.
//!
//! ```text
//! cargo run -p differential-fuzzer --bin custom_types_fuzzer -- --seed 42 -n 500
//! ```

use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use parking_lot::Mutex;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use differential_fuzzer::memory::MemorySimIO;
use turso_core::Database;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

/// Custom types fuzzer for Turso (structs, unions, arrays, custom scalars).
#[derive(Parser, Debug)]
#[command(name = "custom_types_fuzzer")]
struct Args {
    /// Random seed (omit for random).
    #[arg(long)]
    seed: Option<u64>,

    /// Number of DML statements to generate.
    #[arg(short = 'n', long, default_value_t = 500)]
    statements: usize,

    /// Number of tables to create.
    #[arg(short = 't', long, default_value_t = 3)]
    tables: usize,

    /// Print every generated statement.
    #[arg(long)]
    verbose: bool,
}

// ---------------------------------------------------------------------------
// Type model
// ---------------------------------------------------------------------------

/// A named STRUCT definition: `CREATE TYPE <name> AS STRUCT(f1 T1, f2 T2, …)`.
#[derive(Debug, Clone)]
struct StructDef {
    name: String,
    fields: Vec<(String, FieldType)>,
}

/// A named UNION definition: `CREATE TYPE <name> AS UNION(a T1, b T2, …)`.
#[derive(Debug, Clone)]
struct UnionDef {
    name: String,
    variants: Vec<(String, VariantType)>,
}

/// Type of a union variant.
#[derive(Debug, Clone)]
enum VariantType {
    Scalar(FieldType),
    Struct(String),
    Array(String), // element type: "INTEGER", "REAL", "TEXT"
    Union(String), // nested union type name
}

impl std::fmt::Display for VariantType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VariantType::Scalar(ft) => write!(f, "{ft}"),
            VariantType::Struct(name) => write!(f, "{name}"),
            VariantType::Array(elem) => write!(f, "{elem}[]"),
            VariantType::Union(name) => write!(f, "{name}"),
        }
    }
}

/// Leaf or nested type for struct fields.
#[derive(Debug, Clone)]
enum FieldType {
    Int,
    Text,
    Real,
    Struct(String),
    Array(String), // element type: "INTEGER", "REAL", "TEXT"
    Boolean,
    Varchar(u32),
    Date,
    Time,
    Timestamp,
    Smallint,
    Bigint,
    Numeric(u32, u32),
    Inet,
    Bytea,
    Domain(String),
    CustomType(String),
}

impl std::fmt::Display for FieldType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldType::Int => write!(f, "INT"),
            FieldType::Text => write!(f, "TEXT"),
            FieldType::Real => write!(f, "REAL"),
            FieldType::Struct(name) => write!(f, "{name}"),
            FieldType::Array(elem) => write!(f, "{elem}[]"),
            FieldType::Boolean => write!(f, "boolean"),
            FieldType::Varchar(n) => write!(f, "varchar({n})"),
            FieldType::Date => write!(f, "date"),
            FieldType::Time => write!(f, "time"),
            FieldType::Timestamp => write!(f, "timestamp"),
            FieldType::Smallint => write!(f, "smallint"),
            FieldType::Bigint => write!(f, "bigint"),
            FieldType::Numeric(p, s) => write!(f, "numeric({p},{s})"),
            FieldType::Inet => write!(f, "inet"),
            FieldType::Bytea => write!(f, "bytea"),
            FieldType::Domain(name) | FieldType::CustomType(name) => write!(f, "{name}"),
        }
    }
}

/// Metadata about a created table so we can generate DML against it.
#[derive(Debug, Clone)]
struct TableDef {
    name: String,
    /// (column_name, type_str, is_custom_type)
    columns: Vec<(String, String, bool)>,
    /// Type name for each custom-type column (index into columns vec)
    custom_type_columns: Vec<(usize, CustomColKind)>,
    /// Column indices that have DEFAULT expressions (col_idx, default_sql)
    defaults: Vec<(usize, String)>,
}

/// Tracks the last union variant used per (table_idx, row_id, col_idx).
type VariantTracker = HashMap<(usize, i64, usize), String>;

#[derive(Debug, Clone)]
enum CustomColKind {
    Struct(String),
    Union(String),
    Array(String), // element type: "INTEGER", "REAL", "TEXT"
    Domain(String),
    CustomType(String),
}

/// An expression index we created, so we can verify it via SELECT.
#[derive(Debug, Clone)]
struct ExprIndexDef {
    index_name: String,
    table_name: String,
    /// The SQL expression in the index, e.g. `union_tag(v)`
    expr_sql: String,
    /// The column the expression references
    #[allow(dead_code)]
    col_name: String,
}

/// A domain definition: `CREATE DOMAIN <name> AS <base> [CHECK...] [NOT NULL] [DEFAULT...]`
#[derive(Debug, Clone)]
struct DomainDef {
    name: String,
    base: String,
    checks: Vec<String>,
    not_null: bool,
    default: Option<String>,
    /// Resolved bounds for value generation
    int_min: i64,
    int_max: i64,
    text_max_len: usize,
}

/// A custom type with ENCODE/DECODE: `CREATE TYPE <name> BASE <base> ENCODE ... DECODE ...`
#[derive(Debug, Clone)]
struct CustomTypeDef {
    name: String,
    base: String,
    encode: String,
    decode: String,
    has_order: bool,
    default: Option<String>,
}

/// Bundles all user-defined type definitions. Passed to generators to avoid
/// threading individual slices through every function.
#[derive(Debug, Clone, Default)]
struct TypeDefs {
    structs: Vec<StructDef>,
    unions: Vec<UnionDef>,
    domains: Vec<DomainDef>,
    custom_types: Vec<CustomTypeDef>,
}

// ---------------------------------------------------------------------------
// Schema generator
// ---------------------------------------------------------------------------

/// Pick a random custom scalar type.
fn gen_random_custom_scalar(rng: &mut StdRng) -> FieldType {
    match rng.random_range(0..10u32) {
        0 => FieldType::Boolean,
        1 => FieldType::Varchar(rng.random_range(1..=255u32)),
        2 => FieldType::Date,
        3 => FieldType::Time,
        4 => FieldType::Timestamp,
        5 => FieldType::Smallint,
        6 => FieldType::Bigint,
        7 => {
            let p = rng.random_range(1..=18u32);
            let s = rng.random_range(0..=p.min(6));
            FieldType::Numeric(p, s)
        }
        8 => FieldType::Inet,
        _ => FieldType::Bytea,
    }
}

/// Generate `n` struct definitions with increasing nesting depth.
/// `types` may already contain domains and custom_types for field references.
fn gen_structs(rng: &mut StdRng, types: &TypeDefs, n: usize) -> Vec<StructDef> {
    let mut structs: Vec<StructDef> = Vec::new();

    for i in 0..n {
        let name = format!("s{i}");
        let num_fields = rng.random_range(1..=4usize);
        let mut fields = Vec::new();
        for fi in 0..num_fields {
            let fname = format!("f{fi}");
            let choice = rng.random_range(0..100u32);
            let ftype = if i > 0 && choice < 20 {
                // ~20% nested struct
                let ref_idx = rng.random_range(0..i);
                FieldType::Struct(structs[ref_idx].name.clone())
            } else if choice < 33 {
                // ~13% array type
                let elem = match rng.random_range(0..3u32) {
                    0 => "INTEGER",
                    1 => "REAL",
                    _ => "TEXT",
                };
                FieldType::Array(elem.to_string())
            } else if choice < 43 {
                // ~10% custom scalar
                gen_random_custom_scalar(rng)
            } else if choice < 48 && !types.domains.is_empty() {
                // ~5% domain field
                let d = &types.domains[rng.random_range(0..types.domains.len())];
                FieldType::Domain(d.name.clone())
            } else if choice < 53 && !types.custom_types.is_empty() {
                // ~5% custom type field
                let ct = &types.custom_types[rng.random_range(0..types.custom_types.len())];
                FieldType::CustomType(ct.name.clone())
            } else {
                // ~47-57% basic scalar
                match rng.random_range(0..3u32) {
                    0 => FieldType::Int,
                    1 => FieldType::Text,
                    _ => FieldType::Real,
                }
            };
            fields.push((fname, ftype));
        }
        structs.push(StructDef { name, fields });
    }

    structs
}

/// Generate `n` union definitions with mixed variant types.
/// Variant names are globally unique across all unions (u0_v0, u0_v1, u1_v0, …)
/// so that `union_value('tag', …)` is never ambiguous.
fn gen_unions(rng: &mut StdRng, types: &TypeDefs, n: usize) -> Vec<UnionDef> {
    let structs = &types.structs;
    let mut unions: Vec<UnionDef> = Vec::new();

    for i in 0..n {
        let name = format!("u{i}");
        let num_variants = rng.random_range(2..=4usize.min(structs.len().max(2)));
        let mut variants = Vec::new();
        let mut used_structs = std::collections::HashSet::new();

        for vi in 0..num_variants {
            let vname = format!("u{i}_v{vi}");
            let choice = rng.random_range(0..100u32);
            let vtype = if choice < 35 {
                // ~35% struct variant
                let mut attempts = 0;
                let sname = loop {
                    let idx = rng.random_range(0..structs.len());
                    let candidate = &structs[idx].name;
                    if !used_structs.contains(candidate) || attempts > 20 {
                        used_structs.insert(candidate.clone());
                        break candidate.clone();
                    }
                    attempts += 1;
                };
                VariantType::Struct(sname)
            } else if choice < 55 {
                // ~20% scalar variant
                let ft = match rng.random_range(0..8u32) {
                    0 => FieldType::Int,
                    1 => FieldType::Text,
                    2 => FieldType::Real,
                    3..=5 => gen_random_custom_scalar(rng),
                    6 if !types.domains.is_empty() => {
                        let d = &types.domains[rng.random_range(0..types.domains.len())];
                        FieldType::Domain(d.name.clone())
                    }
                    _ if !types.custom_types.is_empty() => {
                        let ct = &types.custom_types[rng.random_range(0..types.custom_types.len())];
                        FieldType::CustomType(ct.name.clone())
                    }
                    _ => FieldType::Int,
                };
                VariantType::Scalar(ft)
            } else if choice < 70 {
                // ~15% array variant
                let elem = match rng.random_range(0..3u32) {
                    0 => "INTEGER",
                    1 => "REAL",
                    _ => "TEXT",
                };
                VariantType::Array(elem.to_string())
            } else if !unions.is_empty() && choice < 85 {
                // ~15% nested union variant (referencing an already-defined union)
                let idx = rng.random_range(0..unions.len());
                VariantType::Union(unions[idx].name.clone())
            } else {
                // ~15% (or ~30% when no unions yet) → struct fallback
                let mut attempts = 0;
                let sname = loop {
                    let idx = rng.random_range(0..structs.len());
                    let candidate = &structs[idx].name;
                    if !used_structs.contains(candidate) || attempts > 20 {
                        used_structs.insert(candidate.clone());
                        break candidate.clone();
                    }
                    attempts += 1;
                };
                VariantType::Struct(sname)
            };
            variants.push((vname, vtype));
        }

        unions.push(UnionDef { name, variants });
    }

    unions
}

fn struct_def_sql(s: &StructDef) -> String {
    let fields: Vec<String> = s.fields.iter().map(|(n, t)| format!("{n} {t}")).collect();
    format!("CREATE TYPE {} AS STRUCT({})", s.name, fields.join(", "))
}

fn union_def_sql(u: &UnionDef) -> String {
    let variants: Vec<String> = u
        .variants
        .iter()
        .map(|(vn, vt)| format!("{vn} {vt}"))
        .collect();
    format!("CREATE TYPE {} AS UNION({})", u.name, variants.join(", "))
}

fn domain_def_sql(d: &DomainDef) -> String {
    let mut sql = format!("CREATE DOMAIN {} AS {}", d.name, d.base);
    if d.not_null {
        sql.push_str(" NOT NULL");
    }
    for check in &d.checks {
        sql.push_str(&format!(" CHECK ({check})"));
    }
    if let Some(default) = &d.default {
        sql.push_str(&format!(" DEFAULT ({default})"));
    }
    sql
}

fn custom_type_def_sql(ct: &CustomTypeDef) -> String {
    let mut sql = format!(
        "CREATE TYPE {} BASE {} ENCODE ({}) DECODE ({})",
        ct.name, ct.base, ct.encode, ct.decode
    );
    if ct.has_order {
        sql.push_str(" OPERATOR '<'");
    }
    if let Some(default) = &ct.default {
        sql.push_str(&format!(" DEFAULT ({default})"));
    }
    sql
}

/// Generate custom type definitions (ENCODE/DECODE types).
fn gen_custom_type_defs(rng: &mut StdRng, n: usize) -> Vec<CustomTypeDef> {
    let mut defs = Vec::new();
    for i in 0..n {
        let name = format!("ct{i}");
        let pattern = rng.random_range(0..4u32);
        let (base, encode, decode) = match pattern {
            0 => (
                "integer".to_string(),
                "value".to_string(),
                "value".to_string(),
            ),
            1 => {
                let factor = rng.random_range(2..=10i64);
                (
                    "integer".to_string(),
                    format!("value * {factor}"),
                    format!("value / {factor}"),
                )
            }
            2 => (
                "text".to_string(),
                "lower(value)".to_string(),
                "value".to_string(),
            ),
            _ => (
                "integer".to_string(),
                format!(
                    "CASE WHEN value >= 0 THEN value ELSE RAISE(ABORT, '{name}: negative') END"
                ),
                "value".to_string(),
            ),
        };
        let has_order = rng.random_bool(0.5);
        let default = if rng.random_bool(0.3) {
            match base.as_str() {
                "integer" => Some(rng.random_range(0..=100i64).to_string()),
                "text" => Some("'default'".to_string()),
                _ => None,
            }
        } else {
            None
        };
        defs.push(CustomTypeDef {
            name,
            base,
            encode,
            decode,
            has_order,
            default,
        });
    }
    defs
}

/// Generate domain definitions, possibly chaining on other domains or custom types.
fn gen_domains(rng: &mut StdRng, custom_types: &[CustomTypeDef], n: usize) -> Vec<DomainDef> {
    let mut domains: Vec<DomainDef> = Vec::new();
    for i in 0..n {
        let name = format!("d{i}");
        let choice = rng.random_range(0..100u32);
        let base = if choice < 30 {
            "integer".to_string()
        } else if choice < 50 {
            "text".to_string()
        } else if choice < 65 {
            "real".to_string()
        } else if choice < 80 && i > 0 {
            // chain on a previous domain
            domains[rng.random_range(0..i)].name.clone()
        } else if !custom_types.is_empty() {
            // domain wrapping a custom type
            custom_types[rng.random_range(0..custom_types.len())]
                .name
                .clone()
        } else {
            "integer".to_string()
        };

        let is_int_based = base == "integer" || base == "real";
        let is_text_based = base == "text";

        let mut checks = Vec::new();
        let n_checks = rng.random_range(0..=2usize);
        let mut int_min = -1000i64;
        let mut int_max = 1000i64;
        let mut text_max_len = 50usize;
        for _ in 0..n_checks {
            if is_int_based {
                match rng.random_range(0..3u32) {
                    0 => {
                        let lo = rng.random_range(0..=50i64);
                        checks.push(format!("value > {lo}"));
                        int_min = int_min.max(lo + 1);
                    }
                    1 => {
                        let hi = rng.random_range(100..=1000i64);
                        checks.push(format!("value < {hi}"));
                        int_max = int_max.min(hi - 1);
                    }
                    _ => {
                        let lo = rng.random_range(0..=50i64);
                        let hi = rng.random_range(51..=200i64);
                        checks.push(format!("value >= {lo} AND value <= {hi}"));
                        int_min = int_min.max(lo);
                        int_max = int_max.min(hi);
                    }
                }
            } else if is_text_based {
                let max_len = rng.random_range(3..=30usize);
                checks.push(format!("length(value) <= {max_len}"));
                text_max_len = text_max_len.min(max_len);
            }
        }
        // Ensure int_min <= int_max
        if int_min > int_max {
            int_max = int_min + 10;
        }

        let not_null = rng.random_bool(0.4);
        let default = if rng.random_bool(0.3) {
            if is_int_based {
                Some(rng.random_range(int_min..=int_max).to_string())
            } else if is_text_based {
                Some("'default'".to_string())
            } else {
                None
            }
        } else {
            None
        };

        domains.push(DomainDef {
            name,
            base,
            checks,
            not_null,
            default,
            int_min,
            int_max,
            text_max_len,
        });
    }
    domains
}

/// Generate a value that satisfies a domain's constraints.
fn gen_domain_value(rng: &mut StdRng, domain: &DomainDef, types: &TypeDefs) -> String {
    // Walk the chain: if base is another domain, delegate
    if let Some(base_domain) = types.domains.iter().find(|d| d.name == domain.base) {
        return gen_domain_value(rng, base_domain, types);
    }
    // If base is a custom type, generate for that
    if let Some(ct) = types.custom_types.iter().find(|c| c.name == domain.base) {
        return gen_custom_type_value(rng, ct);
    }
    // Generate based on primitive base type
    match domain.base.as_str() {
        "integer" => rng
            .random_range(domain.int_min..=domain.int_max)
            .to_string(),
        "real" => format!(
            "{:.2}",
            rng.random_range(domain.int_min as f64..=domain.int_max as f64)
        ),
        "text" => {
            let len = rng.random_range(1..=domain.text_max_len.max(1));
            let s: String = (0..len)
                .map(|_| (b'a' + rng.random_range(0..26u8)) as char)
                .collect();
            format!("'{s}'")
        }
        _ => rng
            .random_range(domain.int_min..=domain.int_max)
            .to_string(),
    }
}

/// Generate a value for a custom type (ENCODE/DECODE). We pass the raw input value;
/// the server applies ENCODE on INSERT.
fn gen_custom_type_value(rng: &mut StdRng, ct: &CustomTypeDef) -> String {
    match ct.base.as_str() {
        "integer" => rng.random_range(0..=100i64).to_string(),
        "text" => {
            let len = rng.random_range(1..=8usize);
            let s: String = (0..len)
                .map(|_| (b'a' + rng.random_range(0..26u8)) as char)
                .collect();
            format!("'{s}'")
        }
        "real" => format!("{:.2}", rng.random_range(0.0..100.0f64)),
        "blob" => {
            let n_bytes = rng.random_range(1..=4usize);
            let hex: String = (0..n_bytes)
                .map(|_| format!("{:02x}", rng.random_range(0..=255u8)))
                .collect();
            format!("X'{hex}'")
        }
        _ => rng.random_range(0..=100i64).to_string(),
    }
}

/// Generate a default value expression for a custom-type column.
fn gen_default_value(rng: &mut StdRng, kind: &CustomColKind, types: &TypeDefs) -> String {
    match kind {
        CustomColKind::Struct(sname) => gen_struct_value(rng, sname, types),
        CustomColKind::Union(uname) => gen_union_value(rng, uname, types),
        CustomColKind::Array(elem) => gen_array_value(rng, elem),
        CustomColKind::Domain(dname) => {
            if let Some(d) = types.domains.iter().find(|d| &d.name == dname) {
                gen_domain_value(rng, d, types)
            } else {
                "NULL".to_string()
            }
        }
        CustomColKind::CustomType(ctname) => {
            if let Some(ct) = types.custom_types.iter().find(|c| &c.name == ctname) {
                gen_custom_type_value(rng, ct)
            } else {
                "NULL".to_string()
            }
        }
    }
}

/// Generate a table with a mix of scalar and custom-type columns.
fn gen_table(rng: &mut StdRng, table_idx: usize, types: &TypeDefs) -> (String, TableDef) {
    let name = format!("t{table_idx}");
    let mut cols: Vec<(String, String, bool)> = Vec::new();
    let mut custom_cols: Vec<(usize, CustomColKind)> = Vec::new();
    let mut defaults: Vec<(usize, String)> = Vec::new();

    // Always start with an integer PK
    cols.push(("id".to_string(), "INT".to_string(), false));

    // 1-3 scalar columns
    let n_scalar = rng.random_range(1..=3usize);
    for si in 0..n_scalar {
        let cname = format!("c{si}");
        let ctype = match rng.random_range(0..3u32) {
            0 => "INT",
            1 => "TEXT",
            _ => "REAL",
        };
        cols.push((cname, ctype.to_string(), false));
    }

    // 1-3 custom-type columns (union, struct, array, domain, or custom type)
    let n_custom = rng.random_range(1..=3usize);
    for ci in 0..n_custom {
        let cname = format!("x{ci}");
        let choice = rng.random_range(0..100u32);
        let (ty_str, kind) = if !types.unions.is_empty() && choice < 30 {
            // ~30% union
            let u = &types.unions[rng.random_range(0..types.unions.len())];
            (u.name.clone(), CustomColKind::Union(u.name.clone()))
        } else if choice < 50 {
            // ~20% struct
            let s = &types.structs[rng.random_range(0..types.structs.len())];
            (s.name.clone(), CustomColKind::Struct(s.name.clone()))
        } else if choice < 65 && !types.domains.is_empty() {
            // ~15% domain
            let d = &types.domains[rng.random_range(0..types.domains.len())];
            (d.name.clone(), CustomColKind::Domain(d.name.clone()))
        } else if choice < 75 && !types.custom_types.is_empty() {
            // ~10% custom type (ENCODE/DECODE)
            let ct = &types.custom_types[rng.random_range(0..types.custom_types.len())];
            (ct.name.clone(), CustomColKind::CustomType(ct.name.clone()))
        } else {
            // ~25-40% array
            let elem = match rng.random_range(0..3u32) {
                0 => "INTEGER",
                1 => "REAL",
                _ => "TEXT",
            };
            (format!("{elem}[]"), CustomColKind::Array(elem.to_string()))
        };
        let idx = cols.len();
        cols.push((cname, ty_str, true));
        // ~30% of custom-type columns get a DEFAULT expression
        if rng.random_bool(0.3) {
            let default_val = gen_default_value(rng, &kind, types);
            defaults.push((idx, default_val));
        }
        custom_cols.push((idx, kind));
    }

    let col_defs: Vec<String> = cols
        .iter()
        .enumerate()
        .map(|(i, (n, t, _))| {
            if i == 0 {
                format!("{n} {t} PRIMARY KEY")
            } else if let Some((_, default_sql)) = defaults.iter().find(|(di, _)| *di == i) {
                format!("{n} {t} DEFAULT ({default_sql})")
            } else {
                format!("{n} {t}")
            }
        })
        .collect();

    let sql = format!("CREATE TABLE {name}({}) STRICT", col_defs.join(", "));
    let tdef = TableDef {
        name,
        columns: cols,
        custom_type_columns: custom_cols,
        defaults,
    };
    (sql, tdef)
}

// ---------------------------------------------------------------------------
// Expression index generator
// ---------------------------------------------------------------------------

/// Possible expression index patterns for a union column.
#[derive(Debug, Clone, Copy)]
enum UnionExprKind {
    Tag,                // union_tag(col)
    ExtractField,       // struct_extract(union_extract(col, 'variant'), 'field')
    ExtractFieldNested, // deeper nesting
    CastTag,            // CAST(union_tag(col) AS TEXT) — compound
    ExtractFieldArith,  // struct_extract(..., 'field') + 0  — arithmetic on extracted
    LowerTag,           // LOWER(union_tag(col))
    CastExtractField,   // CAST(struct_extract(union_extract(col,'v'),'f') AS TEXT)
}

/// Possible expression index patterns for a struct column.
#[derive(Debug, Clone, Copy)]
enum StructExprKind {
    Field,       // struct_extract(col, 'field')
    NestedField, // struct_extract(struct_extract(col, 'f'), 'nested_f')
    CastField,   // CAST(struct_extract(col, 'field') AS TEXT)
    AbsField,    // ABS(struct_extract(col, 'int_field'))
    DotField,    // col.field  (dot notation in expression index)
}

/// Generate expression index SQL for a custom-type column.
/// Returns (CREATE INDEX sql, ExprIndexDef) or None if we can't generate one.
fn gen_expr_index(
    rng: &mut StdRng,
    table: &TableDef,
    col_idx: usize,
    kind: &CustomColKind,
    idx_counter: &mut usize,
    types: &TypeDefs,
) -> Option<(String, ExprIndexDef)> {
    let col_name = &table.columns[col_idx].0;
    let ix_name = format!("ix{}", *idx_counter);
    *idx_counter += 1;

    match kind {
        CustomColKind::Union(uname) => {
            let udef = types.unions.iter().find(|u| &u.name == uname)?;
            let expr_kind = match rng.random_range(0..14u32) {
                0..=2 => UnionExprKind::Tag,
                3..=5 => UnionExprKind::ExtractField,
                6..=7 => UnionExprKind::ExtractFieldNested,
                8 => UnionExprKind::CastTag,
                9 => UnionExprKind::ExtractFieldArith,
                10..=11 => UnionExprKind::LowerTag,
                _ => UnionExprKind::CastExtractField,
            };

            let expr_sql = match expr_kind {
                UnionExprKind::Tag => format!("union_tag({col_name})"),
                UnionExprKind::CastTag => {
                    format!("CAST(union_tag({col_name}) AS TEXT)")
                }
                UnionExprKind::LowerTag => {
                    format!("LOWER(union_tag({col_name}))")
                }
                UnionExprKind::CastExtractField => {
                    // Only works on struct variants
                    let struct_variants: Vec<_> = udef
                        .variants
                        .iter()
                        .filter_map(|(vn, vt)| match vt {
                            VariantType::Struct(sn) => Some((vn.as_str(), sn.as_str())),
                            _ => None,
                        })
                        .collect();
                    if struct_variants.is_empty() {
                        format!("CAST(union_tag({col_name}) AS TEXT)")
                    } else {
                        let (vname, sname) =
                            struct_variants[rng.random_range(0..struct_variants.len())];
                        let sdef = types.structs.iter().find(|s| s.name == sname)?;
                        let fname = &sdef.fields[rng.random_range(0..sdef.fields.len())].0;
                        format!(
                            "CAST(struct_extract(union_extract({col_name}, '{vname}'), '{fname}') AS TEXT)"
                        )
                    }
                }
                UnionExprKind::ExtractField
                | UnionExprKind::ExtractFieldNested
                | UnionExprKind::ExtractFieldArith => {
                    // Only works on struct variants
                    let struct_variants: Vec<_> = udef
                        .variants
                        .iter()
                        .filter_map(|(vn, vt)| match vt {
                            VariantType::Struct(sn) => Some((vn.as_str(), sn.as_str())),
                            _ => None,
                        })
                        .collect();
                    if struct_variants.is_empty() {
                        // Fall back to tag expression
                        format!("union_tag({col_name})")
                    } else {
                        let (vname, sname) =
                            struct_variants[rng.random_range(0..struct_variants.len())];
                        let sdef = types.structs.iter().find(|s| s.name == sname)?;

                        if matches!(expr_kind, UnionExprKind::ExtractFieldNested) {
                            // Try to find a nested struct field for deeper nesting
                            let nested_fields: Vec<_> = sdef
                                .fields
                                .iter()
                                .filter(|(_, ft)| matches!(ft, FieldType::Struct(_)))
                                .collect();

                            if let Some((fname, FieldType::Struct(inner_sname))) =
                                nested_fields.first()
                            {
                                let inner_sdef =
                                    types.structs.iter().find(|s| &s.name == inner_sname)?;
                                let inner_fname = &inner_sdef.fields
                                    [rng.random_range(0..inner_sdef.fields.len())]
                                .0;
                                format!(
                                    "struct_extract(struct_extract(union_extract({col_name}, '{vname}'), '{fname}'), '{inner_fname}')"
                                )
                            } else {
                                // Fall back to simple field extraction
                                let fname = &sdef.fields[rng.random_range(0..sdef.fields.len())].0;
                                format!(
                                    "struct_extract(union_extract({col_name}, '{vname}'), '{fname}')"
                                )
                            }
                        } else if matches!(expr_kind, UnionExprKind::ExtractFieldArith) {
                            // Only use arithmetic on INT fields
                            let int_fields: Vec<_> = sdef
                                .fields
                                .iter()
                                .filter(|(_, ft)| matches!(ft, FieldType::Int))
                                .collect();
                            if let Some((fname, _)) = int_fields.first() {
                                format!(
                                    "struct_extract(union_extract({col_name}, '{vname}'), '{fname}') + 0"
                                )
                            } else {
                                let fname = &sdef.fields[rng.random_range(0..sdef.fields.len())].0;
                                format!(
                                    "struct_extract(union_extract({col_name}, '{vname}'), '{fname}')"
                                )
                            }
                        } else {
                            let fname = &sdef.fields[rng.random_range(0..sdef.fields.len())].0;
                            format!(
                                "struct_extract(union_extract({col_name}, '{vname}'), '{fname}')"
                            )
                        }
                    } // close else block for struct_variants
                }
            };

            let sql = format!("CREATE INDEX {ix_name} ON {}({expr_sql})", table.name);
            Some((
                sql,
                ExprIndexDef {
                    index_name: ix_name,
                    table_name: table.name.clone(),
                    expr_sql,
                    col_name: col_name.clone(),
                },
            ))
        }
        CustomColKind::Struct(sname) => {
            let sdef = types.structs.iter().find(|s| &s.name == sname)?;
            let expr_kind = match rng.random_range(0..10u32) {
                0..=2 => StructExprKind::Field,
                3..=4 => StructExprKind::NestedField,
                5..=6 => StructExprKind::CastField,
                7..=8 => StructExprKind::AbsField,
                _ => StructExprKind::DotField,
            };

            let expr_sql = match expr_kind {
                StructExprKind::Field => {
                    let fname = &sdef.fields[rng.random_range(0..sdef.fields.len())].0;
                    format!("struct_extract({col_name}, '{fname}')")
                }
                StructExprKind::CastField => {
                    let fname = &sdef.fields[rng.random_range(0..sdef.fields.len())].0;
                    format!("CAST(struct_extract({col_name}, '{fname}') AS TEXT)")
                }
                StructExprKind::AbsField => {
                    // ABS only makes sense on numeric fields
                    let int_fields: Vec<_> = sdef
                        .fields
                        .iter()
                        .filter(|(_, ft)| matches!(ft, FieldType::Int | FieldType::Real))
                        .collect();
                    if let Some((fname, _)) = int_fields.first() {
                        format!("ABS(struct_extract({col_name}, '{fname}'))")
                    } else {
                        // Fall back to simple field extraction
                        let fname = &sdef.fields[rng.random_range(0..sdef.fields.len())].0;
                        format!("struct_extract({col_name}, '{fname}')")
                    }
                }
                StructExprKind::DotField => {
                    // Dot notation: col.field — only scalar fields
                    let scalar_fields: Vec<_> = sdef
                        .fields
                        .iter()
                        .filter(|(_, ft)| !matches!(ft, FieldType::Struct(_) | FieldType::Array(_)))
                        .collect();
                    if let Some((fname, _)) = scalar_fields.first() {
                        format!("{col_name}.{fname}")
                    } else {
                        let fname = &sdef.fields[rng.random_range(0..sdef.fields.len())].0;
                        format!("struct_extract({col_name}, '{fname}')")
                    }
                }
                StructExprKind::NestedField => {
                    let nested_fields: Vec<_> = sdef
                        .fields
                        .iter()
                        .filter(|(_, ft)| matches!(ft, FieldType::Struct(_)))
                        .collect();

                    if let Some((fname, FieldType::Struct(inner_sname))) = nested_fields.first() {
                        let inner_sdef = types.structs.iter().find(|s| &s.name == inner_sname)?;
                        let inner_fname =
                            &inner_sdef.fields[rng.random_range(0..inner_sdef.fields.len())].0;
                        format!(
                            "struct_extract(struct_extract({col_name}, '{fname}'), '{inner_fname}')"
                        )
                    } else {
                        let fname = &sdef.fields[rng.random_range(0..sdef.fields.len())].0;
                        format!("struct_extract({col_name}, '{fname}')")
                    }
                }
            };

            let sql = format!("CREATE INDEX {ix_name} ON {}({expr_sql})", table.name);
            Some((
                sql,
                ExprIndexDef {
                    index_name: ix_name,
                    table_name: table.name.clone(),
                    expr_sql,
                    col_name: col_name.clone(),
                },
            ))
        }
        CustomColKind::Array(elem_type) => {
            let expr_kind = match rng.random_range(0..6u32) {
                0..=1 => ArrayExprKind::Length,
                2 => ArrayExprKind::Element,
                3 => ArrayExprKind::Contains,
                4 => ArrayExprKind::ArrayToString,
                _ => ArrayExprKind::CastLength,
            };

            let expr_sql = match expr_kind {
                ArrayExprKind::Length => format!("array_length({col_name})"),
                ArrayExprKind::Element => {
                    let idx = rng.random_range(1..=3u32);
                    format!("{col_name}[{idx}]")
                }
                ArrayExprKind::Contains => {
                    let val = gen_array_element(rng, elem_type);
                    format!("array_contains({col_name}, {val})")
                }
                ArrayExprKind::Position => {
                    let val = gen_array_element(rng, elem_type);
                    format!("array_position({col_name}, {val})")
                }
                ArrayExprKind::ArrayToString => {
                    format!("array_to_string({col_name}, ',')")
                }
                ArrayExprKind::CastLength => {
                    format!("CAST(array_length({col_name}) AS TEXT)")
                }
            };

            let sql = format!("CREATE INDEX {ix_name} ON {}({expr_sql})", table.name);
            Some((
                sql,
                ExprIndexDef {
                    index_name: ix_name,
                    table_name: table.name.clone(),
                    expr_sql,
                    col_name: col_name.clone(),
                },
            ))
        }
        CustomColKind::Domain(_) | CustomColKind::CustomType(_) => {
            // Skip expression indexes on domain/custom type columns for now.
            // The pre-encoded default fix handles NULLs correctly, but the
            // consistency check (scan vs index) can't easily verify custom type
            // expression indexes since the CAST semantics depend on the type.
            None
        }
    }
}

/// Possible expression index patterns for an array column.
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
enum ArrayExprKind {
    Length,        // array_length(col)
    Element,       // col[N]
    Contains,      // array_contains(col, val)
    Position,      // array_position(col, val)
    ArrayToString, // array_to_string(col, ',')
    CastLength,    // CAST(array_length(col) AS TEXT)
}

// ---------------------------------------------------------------------------
// Value generator — produces valid struct_pack / union_value literals
// ---------------------------------------------------------------------------

fn gen_scalar_value(rng: &mut StdRng, ft: &FieldType) -> String {
    match ft {
        FieldType::Int => rng.random_range(-100..=100i64).to_string(),
        FieldType::Real => format!("{:.2}", rng.random_range(-100.0..100.0f64)),
        FieldType::Text => {
            let len = rng.random_range(1..=8usize);
            let s: String = (0..len)
                .map(|_| (b'a' + rng.random_range(0..26u8)) as char)
                .collect();
            format!("'{s}'")
        }
        FieldType::Struct(_) => unreachable!("call gen_struct_value for nested structs"),
        FieldType::Array(_) => unreachable!("call gen_array_value for arrays"),
        FieldType::Domain(_) => unreachable!("call gen_domain_value for domains"),
        FieldType::CustomType(_) => unreachable!("call gen_custom_type_value for custom types"),
        FieldType::Boolean => {
            if rng.random_bool(0.5) {
                "1".to_string()
            } else {
                "0".to_string()
            }
        }
        FieldType::Varchar(maxlen) => {
            let len = rng.random_range(1..=(*maxlen as usize).min(20));
            let s: String = (0..len)
                .map(|_| (b'a' + rng.random_range(0..26u8)) as char)
                .collect();
            format!("'{s}'")
        }
        FieldType::Date => {
            let y = rng.random_range(2000..=2025u32);
            let m = rng.random_range(1..=12u32);
            let d = rng.random_range(1..=28u32);
            format!("'{y:04}-{m:02}-{d:02}'")
        }
        FieldType::Time => {
            let h = rng.random_range(0..=23u32);
            let m = rng.random_range(0..=59u32);
            let s = rng.random_range(0..=59u32);
            format!("'{h:02}:{m:02}:{s:02}'")
        }
        FieldType::Timestamp => {
            let y = rng.random_range(2000..=2025u32);
            let mo = rng.random_range(1..=12u32);
            let d = rng.random_range(1..=28u32);
            let h = rng.random_range(0..=23u32);
            let mi = rng.random_range(0..=59u32);
            let s = rng.random_range(0..=59u32);
            format!("'{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}'")
        }
        FieldType::Smallint => rng.random_range(-32768..=32767i32).to_string(),
        FieldType::Bigint => rng.random_range(-1_000_000..=1_000_000i64).to_string(),
        FieldType::Numeric(p, s) => {
            let max_int_digits = (*p as i32 - *s as i32).max(1) as u32;
            let int_bound = 10i64.pow(max_int_digits.min(9));
            let int_part = rng.random_range(0..int_bound);
            if *s > 0 {
                let frac_bound = 10i64.pow((*s).min(6));
                let frac_part = rng.random_range(0..frac_bound);
                format!("'{int_part}.{frac_part:0>width$}'", width = *s as usize)
            } else {
                format!("'{int_part}'")
            }
        }
        FieldType::Inet => {
            let a = rng.random_range(1..=254u8);
            let b = rng.random_range(0..=255u8);
            let c = rng.random_range(0..=255u8);
            let d = rng.random_range(1..=254u8);
            format!("'{a}.{b}.{c}.{d}'")
        }
        FieldType::Bytea => {
            let n_bytes = rng.random_range(1..=8usize);
            let hex: String = (0..n_bytes)
                .map(|_| format!("{:02x}", rng.random_range(0..=255u8)))
                .collect();
            format!("X'{hex}'")
        }
    }
}

fn gen_struct_value(rng: &mut StdRng, sname: &str, types: &TypeDefs) -> String {
    let sdef = types.structs.iter().find(|s| s.name == sname).unwrap();
    let args: Vec<String> = sdef
        .fields
        .iter()
        .map(|(_, ft)| match ft {
            FieldType::Struct(inner) => gen_struct_value(rng, inner, types),
            FieldType::Array(elem) => gen_array_value(rng, elem),
            FieldType::Domain(dname) => {
                if let Some(d) = types.domains.iter().find(|d| &d.name == dname) {
                    gen_domain_value(rng, d, types)
                } else {
                    "NULL".to_string()
                }
            }
            FieldType::CustomType(ctname) => {
                if let Some(ct) = types.custom_types.iter().find(|c| &c.name == ctname) {
                    gen_custom_type_value(rng, ct)
                } else {
                    "NULL".to_string()
                }
            }
            other => gen_scalar_value(rng, other),
        })
        .collect();
    format!("struct_pack({})", args.join(", "))
}

fn gen_union_value(rng: &mut StdRng, uname: &str, types: &TypeDefs) -> String {
    let udef = types.unions.iter().find(|u| u.name == uname).unwrap();
    let (vname, vtype) = &udef.variants[rng.random_range(0..udef.variants.len())];
    let val = match vtype {
        VariantType::Struct(sname) => gen_struct_value(rng, sname, types),
        VariantType::Scalar(FieldType::Domain(dname)) => {
            if let Some(d) = types.domains.iter().find(|d| &d.name == dname) {
                gen_domain_value(rng, d, types)
            } else {
                "NULL".to_string()
            }
        }
        VariantType::Scalar(FieldType::CustomType(ctname)) => {
            if let Some(ct) = types.custom_types.iter().find(|c| &c.name == ctname) {
                gen_custom_type_value(rng, ct)
            } else {
                "NULL".to_string()
            }
        }
        VariantType::Scalar(ft) => gen_scalar_value(rng, ft),
        VariantType::Array(elem) => gen_array_value(rng, elem),
        VariantType::Union(inner_uname) => gen_union_value(rng, inner_uname, types),
    };
    format!("union_value('{vname}', {val})")
}

/// Returns true if this column kind is a NOT NULL domain (walks the chain).
fn is_not_null_domain(kind: &CustomColKind, types: &TypeDefs) -> bool {
    let dname = match kind {
        CustomColKind::Domain(dname) => dname,
        _ => return false,
    };
    domain_chain_has_not_null(dname, types)
}

/// Walk the domain chain to check if any domain in the chain has NOT NULL.
fn domain_chain_has_not_null(dname: &str, types: &TypeDefs) -> bool {
    let mut current = dname;
    for _ in 0..10 {
        if let Some(d) = types.domains.iter().find(|d| d.name == current) {
            if d.not_null {
                return true;
            }
            // Check if base is another domain
            if types.domains.iter().any(|d2| d2.name == d.base) {
                current = &d.base;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }
    false
}

/// Generate a value for a custom-type column. Sometimes NULL (15% chance).
fn gen_custom_value_or_null(rng: &mut StdRng, kind: &CustomColKind, types: &TypeDefs) -> String {
    if !is_not_null_domain(kind, types) && rng.random_bool(0.15) {
        return "NULL".to_string();
    }
    match kind {
        CustomColKind::Struct(sname) => gen_struct_value(rng, sname, types),
        CustomColKind::Union(uname) => gen_union_value(rng, uname, types),
        CustomColKind::Array(elem) => gen_array_value(rng, elem),
        CustomColKind::Domain(dname) => {
            if let Some(d) = types.domains.iter().find(|d| &d.name == dname) {
                gen_domain_value(rng, d, types)
            } else {
                "NULL".to_string()
            }
        }
        CustomColKind::CustomType(ctname) => {
            if let Some(ct) = types.custom_types.iter().find(|c| &c.name == ctname) {
                gen_custom_type_value(rng, ct)
            } else {
                "NULL".to_string()
            }
        }
    }
}

/// Generate an array literal: `ARRAY[elem1, elem2, ...]` with 0-6 elements.
fn gen_array_value(rng: &mut StdRng, elem_type: &str) -> String {
    let n = rng.random_range(0..=6usize);
    let elems: Vec<String> = (0..n).map(|_| gen_array_element(rng, elem_type)).collect();
    format!("ARRAY[{}]", elems.join(", "))
}

/// Generate a single array element value.
fn gen_array_element(rng: &mut StdRng, elem_type: &str) -> String {
    match elem_type {
        "INTEGER" => rng.random_range(-100..=100i64).to_string(),
        "REAL" => format!("{:.2}", rng.random_range(-100.0..100.0f64)),
        "TEXT" => {
            let len = rng.random_range(1..=6usize);
            let s: String = (0..len)
                .map(|_| (b'a' + rng.random_range(0..26u8)) as char)
                .collect();
            format!("'{s}'")
        }
        _ => "NULL".to_string(),
    }
}

// ---------------------------------------------------------------------------
// DML generators
// ---------------------------------------------------------------------------

fn gen_insert(rng: &mut StdRng, table: &TableDef, row_id: i64, types: &TypeDefs) -> String {
    let mut vals: Vec<String> = Vec::new();
    // id
    vals.push(row_id.to_string());

    // scalar + custom columns
    for (col_i, (_, ty, is_custom)) in table.columns[1..].iter().enumerate() {
        let col_idx = col_i + 1; // adjust for skipping id
        if *is_custom {
            let kind = table
                .custom_type_columns
                .iter()
                .find(|(i, _)| *i == col_idx)
                .map(|(_, k)| k)
                .unwrap();
            vals.push(gen_custom_value_or_null(rng, kind, types));
        } else {
            vals.push(gen_scalar_literal(rng, ty));
        }
    }

    format!("INSERT INTO {} VALUES ({})", table.name, vals.join(", "))
}

/// Generate a scalar literal for a given type string.
fn gen_scalar_literal(rng: &mut StdRng, ty: &str) -> String {
    match ty {
        "INT" => rng.random_range(-100..=100i64).to_string(),
        "TEXT" => {
            let len = rng.random_range(1..=6usize);
            let s: String = (0..len)
                .map(|_| (b'a' + rng.random_range(0..26u8)) as char)
                .collect();
            format!("'{s}'")
        }
        "REAL" => format!("{:.2}", rng.random_range(-100.0..100.0f64)),
        _ => "NULL".to_string(),
    }
}

fn gen_update(
    rng: &mut StdRng,
    table: &TableDef,
    max_id: i64,
    types: &TypeDefs,
    expr_indexes: &[ExprIndexDef],
) -> String {
    // 30% chance: update multiple columns at once
    let multi_col = rng.random_bool(0.3);
    let updatable: Vec<usize> = (1..table.columns.len()).collect();

    let set_clauses: Vec<String> = if multi_col {
        let n = rng.random_range(2..=updatable.len().min(4));
        let mut chosen = Vec::new();
        let mut used = std::collections::HashSet::new();
        for _ in 0..n {
            let mut attempts = 0;
            loop {
                let idx = updatable[rng.random_range(0..updatable.len())];
                if used.insert(idx) || attempts > 20 {
                    chosen.push(idx);
                    break;
                }
                attempts += 1;
            }
        }
        chosen
            .iter()
            .map(|&col_idx| gen_set_clause(rng, table, col_idx, types))
            .collect()
    } else {
        let col_idx = updatable[rng.random_range(0..updatable.len())];
        vec![gen_set_clause(rng, table, col_idx, types)]
    };

    let where_clause = gen_where_clause(rng, table, max_id, expr_indexes, types);

    format!(
        "UPDATE {} SET {} WHERE {}",
        table.name,
        set_clauses.join(", "),
        where_clause
    )
}

/// Generate a single `col = value` SET clause.
fn gen_set_clause(rng: &mut StdRng, table: &TableDef, col_idx: usize, types: &TypeDefs) -> String {
    let (col_name, _, is_custom) = &table.columns[col_idx];
    let new_val = if *is_custom {
        let kind = table
            .custom_type_columns
            .iter()
            .find(|(i, _)| *i == col_idx)
            .map(|(_, k)| k)
            .unwrap();
        gen_custom_value_or_null(rng, kind, types)
    } else {
        let ty = &table.columns[col_idx].1;
        gen_scalar_literal(rng, ty)
    };
    format!("{col_name} = {new_val}")
}

/// Generate a WHERE clause — sometimes by id, sometimes by custom type expression.
fn gen_where_clause(
    rng: &mut StdRng,
    table: &TableDef,
    max_id: i64,
    expr_indexes: &[ExprIndexDef],
    types: &TypeDefs,
) -> String {
    // 45% simple id, 20% equality, 8% IS NULL, 7% IN list, 5% BETWEEN,
    // 5% COALESCE, 5% CASE WHEN, 5% compound id range
    let choice = rng.random_range(0..100u32);
    if choice < 45 {
        let target_id = rng.random_range(1..=max_id);
        return format!("id = {target_id}");
    }

    // Try to use an expression index expression in the WHERE clause
    let table_indexes: Vec<_> = expr_indexes
        .iter()
        .filter(|ei| ei.table_name == table.name)
        .collect();

    if table_indexes.is_empty() {
        let target_id = rng.random_range(1..=max_id);
        return format!("id = {target_id}");
    }

    let eidx = table_indexes[rng.random_range(0..table_indexes.len())];

    if choice < 65 {
        // WHERE expr = value
        if eidx.expr_sql.contains("union_tag(") || eidx.expr_sql.contains("LOWER(union_tag(") {
            if let Some(vname) = random_variant_name(rng, table, &eidx.col_name, types) {
                let cmp_val = if eidx.expr_sql.contains("LOWER(") {
                    vname.to_lowercase()
                } else {
                    vname
                };
                format!("{} = '{cmp_val}'", eidx.expr_sql)
            } else {
                format!("id = {}", rng.random_range(1..=max_id))
            }
        } else {
            format!("{} = {}", eidx.expr_sql, rng.random_range(-50..=50i64))
        }
    } else if choice < 73 {
        // IS NULL / IS NOT NULL
        if rng.random_bool(0.5) {
            format!("{} IS NULL", eidx.expr_sql)
        } else {
            format!("{} IS NOT NULL", eidx.expr_sql)
        }
    } else if choice < 80 {
        // IN list — exercises multi-value index seek
        if eidx.expr_sql.contains("union_tag(") || eidx.expr_sql.contains("LOWER(union_tag(") {
            // IN with variant names
            let col_name = &eidx.col_name;
            let col_idx = table.columns.iter().position(|c| c.0 == *col_name);
            let kind = col_idx.and_then(|ci| {
                table
                    .custom_type_columns
                    .iter()
                    .find(|(i, _)| *i == ci)
                    .map(|(_, k)| k)
            });
            if let Some(CustomColKind::Union(uname)) = kind {
                let udef = types.unions.iter().find(|u| &u.name == uname);
                if let Some(udef) = udef {
                    let n = rng.random_range(2..=udef.variants.len().min(4));
                    let names: Vec<String> = udef
                        .variants
                        .iter()
                        .take(n)
                        .map(|(vn, _)| {
                            if eidx.expr_sql.contains("LOWER(") {
                                format!("'{}'", vn.to_lowercase())
                            } else {
                                format!("'{vn}'")
                            }
                        })
                        .collect();
                    format!("{} IN ({})", eidx.expr_sql, names.join(", "))
                } else {
                    format!("id = {}", rng.random_range(1..=max_id))
                }
            } else {
                format!("id = {}", rng.random_range(1..=max_id))
            }
        } else {
            // IN with numeric values
            let n = rng.random_range(2..=4usize);
            let vals: Vec<String> = (0..n)
                .map(|_| rng.random_range(-50..=50i64).to_string())
                .collect();
            format!("{} IN ({})", eidx.expr_sql, vals.join(", "))
        }
    } else if choice < 85 {
        // BETWEEN — range scan on expression index
        let lo = rng.random_range(-50..=25i64);
        let hi = lo + rng.random_range(1..=50i64);
        format!("{} BETWEEN {lo} AND {hi}", eidx.expr_sql)
    } else if choice < 90 {
        // COALESCE — tests NULL handling in expression indexes
        format!("COALESCE({}, -999) != -999", eidx.expr_sql)
    } else if choice < 95 {
        // CASE WHEN on expression — tests conditional logic with expression indexes
        if eidx.expr_sql.contains("union_tag(") {
            if let Some(vname) = random_variant_name(rng, table, &eidx.col_name, types) {
                format!(
                    "CASE WHEN {} = '{vname}' THEN 1 ELSE 0 END = 1",
                    eidx.expr_sql
                )
            } else {
                format!("id = {}", rng.random_range(1..=max_id))
            }
        } else {
            format!("CASE WHEN {} > 0 THEN 1 ELSE 0 END = 1", eidx.expr_sql)
        }
    } else {
        // Compound: id range
        let lo = rng.random_range(1..=max_id);
        let hi = (lo + rng.random_range(1..=5)).min(max_id);
        format!("id BETWEEN {lo} AND {hi}")
    }
}

/// Pick a random variant name for a union column.
fn random_variant_name(
    rng: &mut StdRng,
    table: &TableDef,
    col_name: &str,
    types: &TypeDefs,
) -> Option<String> {
    let col_idx = table.columns.iter().position(|c| c.0 == *col_name)?;
    let kind = table
        .custom_type_columns
        .iter()
        .find(|(i, _)| *i == col_idx)
        .map(|(_, k)| k)?;
    if let CustomColKind::Union(uname) = kind {
        let udef = types.unions.iter().find(|u| &u.name == uname)?;
        let (vname, _) = &udef.variants[rng.random_range(0..udef.variants.len())];
        Some(vname.clone())
    } else {
        None
    }
}

fn gen_delete(
    rng: &mut StdRng,
    table: &TableDef,
    max_id: i64,
    expr_indexes: &[ExprIndexDef],
    types: &TypeDefs,
) -> String {
    let where_clause = gen_where_clause(rng, table, max_id, expr_indexes, types);
    format!("DELETE FROM {} WHERE {where_clause}", table.name)
}

/// Generate a SELECT that exercises expression index expressions.
fn gen_select_expr_index(
    rng: &mut StdRng,
    expr_indexes: &[ExprIndexDef],
    tables: &[TableDef],
    types: &TypeDefs,
) -> Option<String> {
    if expr_indexes.is_empty() {
        return None;
    }
    let eidx = &expr_indexes[rng.random_range(0..expr_indexes.len())];
    let table = tables.iter().find(|t| t.name == eidx.table_name)?;

    let query_kind = rng.random_range(0..12u32);
    match query_kind {
        0 => {
            // COUNT WHERE expr = val
            let cmp_val = gen_expr_cmp_value(rng, eidx, table, types);
            Some(format!(
                "SELECT COUNT(*) FROM {} WHERE {} = {cmp_val}",
                eidx.table_name, eidx.expr_sql,
            ))
        }
        1 => {
            // Range query: WHERE expr > val
            let val = rng.random_range(-50..=50i64);
            Some(format!(
                "SELECT COUNT(*) FROM {} WHERE {} > {val}",
                eidx.table_name, eidx.expr_sql,
            ))
        }
        2 => {
            // IS NULL check
            Some(format!(
                "SELECT COUNT(*) FROM {} WHERE {} IS NULL",
                eidx.table_name, eidx.expr_sql,
            ))
        }
        3 => {
            // IS NOT NULL check
            Some(format!(
                "SELECT COUNT(*) FROM {} WHERE {} IS NOT NULL",
                eidx.table_name, eidx.expr_sql,
            ))
        }
        4 => {
            // ORDER BY the indexed expression
            Some(format!(
                "SELECT id, ({}) FROM {} ORDER BY {} LIMIT 10",
                eidx.expr_sql, eidx.table_name, eidx.expr_sql,
            ))
        }
        5 => {
            // GROUP BY + HAVING
            Some(format!(
                "SELECT ({}), COUNT(*) FROM {} GROUP BY ({}) HAVING COUNT(*) > 1",
                eidx.expr_sql, eidx.table_name, eidx.expr_sql,
            ))
        }
        6 => {
            // DISTINCT
            Some(format!(
                "SELECT DISTINCT ({}) FROM {} ORDER BY ({}) LIMIT 20",
                eidx.expr_sql, eidx.table_name, eidx.expr_sql,
            ))
        }
        7 => {
            // Aggregate: MIN / MAX / COUNT
            let agg = match rng.random_range(0..3u32) {
                0 => "MIN",
                1 => "MAX",
                _ => "COUNT",
            };
            Some(format!(
                "SELECT {}({}) FROM {}",
                agg, eidx.expr_sql, eidx.table_name,
            ))
        }
        8 => {
            // TYPEOF on expression index — tests type introspection
            Some(format!(
                "SELECT TYPEOF({}), COUNT(*) FROM {} GROUP BY TYPEOF({})",
                eidx.expr_sql, eidx.table_name, eidx.expr_sql,
            ))
        }
        9 => {
            // COALESCE with expression index
            Some(format!(
                "SELECT id, COALESCE({}, -999) FROM {} ORDER BY id LIMIT 10",
                eidx.expr_sql, eidx.table_name,
            ))
        }
        10 => {
            // IN list query — exercises multi-value seek
            let cmp_val1 = gen_expr_cmp_value(rng, eidx, table, types);
            let cmp_val2 = gen_expr_cmp_value(rng, eidx, table, types);
            Some(format!(
                "SELECT id FROM {} WHERE {} IN ({cmp_val1}, {cmp_val2}) ORDER BY id",
                eidx.table_name, eidx.expr_sql,
            ))
        }
        _ => {
            // BETWEEN range query
            let lo = rng.random_range(-50..=25i64);
            let hi = lo + rng.random_range(1..=50i64);
            Some(format!(
                "SELECT id FROM {} WHERE {} BETWEEN {lo} AND {hi} ORDER BY id",
                eidx.table_name, eidx.expr_sql,
            ))
        }
    }
}

/// Generate a comparison value for an expression index expression.
fn gen_expr_cmp_value(
    rng: &mut StdRng,
    eidx: &ExprIndexDef,
    table: &TableDef,
    types: &TypeDefs,
) -> String {
    if eidx.expr_sql.contains("union_tag(") || eidx.expr_sql.contains("LOWER(union_tag(") {
        if let Some(vname) = random_variant_name(rng, table, &eidx.col_name, types) {
            if eidx.expr_sql.contains("LOWER(") {
                format!("'{}'", vname.to_lowercase())
            } else {
                format!("'{vname}'")
            }
        } else {
            "'unknown'".to_string()
        }
    } else if eidx.expr_sql.starts_with("CAST(") && eidx.expr_sql.ends_with("AS TEXT)") {
        // CAST(... AS TEXT) returns text — use text comparison values sometimes
        if rng.random_bool(0.5) {
            format!("'{}'", rng.random_range(-50..=50i64))
        } else {
            rng.random_range(-50..=50i64).to_string()
        }
    } else {
        rng.random_range(-50..=50i64).to_string()
    }
}

/// Generate a SELECT with a JOIN between two tables.
fn gen_join_select(
    rng: &mut StdRng,
    tables: &[TableDef],
    expr_indexes: &[ExprIndexDef],
) -> Option<String> {
    if tables.len() < 2 {
        return None;
    }
    let t1 = &tables[rng.random_range(0..tables.len())];
    let t2_candidates: Vec<_> = tables.iter().filter(|t| t.name != t1.name).collect();
    if t2_candidates.is_empty() {
        return None;
    }
    let t2 = t2_candidates[rng.random_range(0..t2_candidates.len())];

    let join_kind = match rng.random_range(0..3u32) {
        0 => "INNER JOIN",
        1 => "LEFT JOIN",
        _ => "CROSS JOIN",
    };

    let on_clause = if join_kind == "CROSS JOIN" {
        String::new()
    } else {
        format!(" ON {}.id = {}.id", t1.name, t2.name)
    };

    // 40% chance: join on custom type expression instead of id
    let join_expr = if rng.random_bool(0.4) {
        let t1_indexes: Vec<_> = expr_indexes
            .iter()
            .filter(|ei| ei.table_name == t1.name)
            .collect();
        let t2_indexes: Vec<_> = expr_indexes
            .iter()
            .filter(|ei| ei.table_name == t2.name)
            .collect();
        if !t1_indexes.is_empty() && !t2_indexes.is_empty() && join_kind != "CROSS JOIN" {
            let e1 = t1_indexes[rng.random_range(0..t1_indexes.len())];
            let e2 = t2_indexes[rng.random_range(0..t2_indexes.len())];
            let e1_sql = e1
                .expr_sql
                .replace(&e1.col_name, &format!("{}.{}", t1.name, e1.col_name));
            let e2_sql = e2
                .expr_sql
                .replace(&e2.col_name, &format!("{}.{}", t2.name, e2.col_name));
            Some(format!(" ON {e1_sql} = {e2_sql}"))
        } else {
            None
        }
    } else {
        None
    };

    let final_on = join_expr.unwrap_or(on_clause);

    // Select some columns from both
    let sel_col1 = &t1.columns[rng.random_range(0..t1.columns.len())].0;
    let sel_col2 = &t2.columns[rng.random_range(0..t2.columns.len())].0;

    Some(format!(
        "SELECT {}.{sel_col1}, {}.{sel_col2} FROM {} {join_kind} {}{final_on} LIMIT 10",
        t1.name, t2.name, t1.name, t2.name
    ))
}

/// Generate various SELECT queries on custom type columns.
fn gen_custom_type_select(rng: &mut StdRng, table: &TableDef, types: &TypeDefs) -> Option<String> {
    if table.custom_type_columns.is_empty() {
        return None;
    }
    let (col_idx, kind) =
        &table.custom_type_columns[rng.random_range(0..table.custom_type_columns.len())];
    let col_name = &table.columns[*col_idx].0;

    let query = match kind {
        CustomColKind::Union(uname) => {
            let udef = types.unions.iter().find(|u| &u.name == uname)?;
            match rng.random_range(0..12u32) {
                0 => format!("SELECT union_tag({col_name}) FROM {} LIMIT 5", table.name),
                1 => {
                    // struct_extract only works on struct variants
                    let struct_variants: Vec<_> = udef
                        .variants
                        .iter()
                        .filter_map(|(vn, vt)| match vt {
                            VariantType::Struct(sn) => Some((vn.as_str(), sn.as_str())),
                            _ => None,
                        })
                        .collect();
                    if let Some((vname, sname)) = struct_variants.first() {
                        let sdef = types.structs.iter().find(|s| s.name == *sname)?;
                        let fname = &sdef.fields[rng.random_range(0..sdef.fields.len())].0;
                        format!(
                            "SELECT struct_extract(union_extract({col_name}, '{vname}'), '{fname}') FROM {} LIMIT 5",
                            table.name
                        )
                    } else {
                        format!("SELECT union_tag({col_name}) FROM {} LIMIT 5", table.name)
                    }
                }
                2 => {
                    format!(
                        "SELECT union_tag({col_name}), COUNT(*) FROM {} GROUP BY union_tag({col_name})",
                        table.name
                    )
                }
                3 => {
                    format!(
                        "SELECT id, union_tag({col_name}) FROM {} ORDER BY union_tag({col_name}) LIMIT 10",
                        table.name
                    )
                }
                4 => {
                    // DISTINCT union_tag
                    format!(
                        "SELECT DISTINCT union_tag({col_name}) FROM {} ORDER BY union_tag({col_name})",
                        table.name
                    )
                }
                5 => {
                    // Aggregate on extracted field (struct variants only)
                    let struct_variants: Vec<_> = udef
                        .variants
                        .iter()
                        .filter_map(|(vn, vt)| match vt {
                            VariantType::Struct(sn) => Some((vn.as_str(), sn.as_str())),
                            _ => None,
                        })
                        .collect();
                    if let Some((vname, sname)) = struct_variants.first() {
                        let sdef = types.structs.iter().find(|s| s.name == *sname)?;
                        let int_fields: Vec<_> = sdef
                            .fields
                            .iter()
                            .filter(|(_, ft)| matches!(ft, FieldType::Int))
                            .collect();
                        if let Some((fname, _)) = int_fields.first() {
                            let agg = match rng.random_range(0..3u32) {
                                0 => "SUM",
                                1 => "MIN",
                                _ => "MAX",
                            };
                            format!(
                                "SELECT {agg}(struct_extract(union_extract({col_name}, '{vname}'), '{fname}')) FROM {}",
                                table.name
                            )
                        } else {
                            format!(
                                "SELECT COUNT(*) FROM {} WHERE union_tag({col_name}) IS NOT NULL",
                                table.name
                            )
                        }
                    } else {
                        format!(
                            "SELECT COUNT(*) FROM {} WHERE union_tag({col_name}) IS NOT NULL",
                            table.name
                        )
                    }
                }
                6 => {
                    // Subquery: WHERE col IN (SELECT ...)
                    format!(
                        "SELECT id FROM {} WHERE union_tag({col_name}) IN (SELECT DISTINCT union_tag({col_name}) FROM {} LIMIT 2)",
                        table.name, table.name
                    )
                }
                7 => {
                    // WHERE IS NULL / IS NOT NULL
                    if rng.random_bool(0.5) {
                        format!("SELECT id FROM {} WHERE {col_name} IS NULL", table.name)
                    } else {
                        format!("SELECT id FROM {} WHERE {col_name} IS NOT NULL", table.name)
                    }
                }
                8 => {
                    // TYPEOF on union_tag
                    format!(
                        "SELECT TYPEOF(union_tag({col_name})), COUNT(*) FROM {} GROUP BY TYPEOF(union_tag({col_name}))",
                        table.name
                    )
                }
                9 => {
                    // LOWER/UPPER on union_tag
                    let func = if rng.random_bool(0.5) {
                        "LOWER"
                    } else {
                        "UPPER"
                    };
                    format!(
                        "SELECT {func}(union_tag({col_name})) FROM {} ORDER BY 1 LIMIT 10",
                        table.name
                    )
                }
                10 => {
                    // COALESCE on extracted field (struct variants only)
                    let struct_variants: Vec<_> = udef
                        .variants
                        .iter()
                        .filter_map(|(vn, vt)| match vt {
                            VariantType::Struct(sn) => Some((vn.as_str(), sn.as_str())),
                            _ => None,
                        })
                        .collect();
                    if struct_variants.is_empty() {
                        return Some(format!(
                            "SELECT union_tag({col_name}) FROM {} LIMIT 10",
                            table.name
                        ));
                    }
                    let (vname, sname) =
                        struct_variants[rng.random_range(0..struct_variants.len())];
                    let sdef = types.structs.iter().find(|s| s.name == sname)?;
                    let fname = &sdef.fields[rng.random_range(0..sdef.fields.len())].0;
                    format!(
                        "SELECT COALESCE(struct_extract(union_extract({col_name}, '{vname}'), '{fname}'), -1) FROM {} LIMIT 10",
                        table.name
                    )
                }
                _ => {
                    // CASE WHEN on union_tag
                    let (vname, _) = &udef.variants[rng.random_range(0..udef.variants.len())];
                    format!(
                        "SELECT CASE WHEN union_tag({col_name}) = '{vname}' THEN 'yes' ELSE 'no' END FROM {} LIMIT 10",
                        table.name
                    )
                }
            }
        }
        CustomColKind::Struct(sname) => {
            let sdef = types.structs.iter().find(|s| &s.name == sname)?;
            let fname = &sdef.fields[rng.random_range(0..sdef.fields.len())].0;
            match rng.random_range(0..10u32) {
                0 => format!(
                    "SELECT struct_extract({col_name}, '{fname}') FROM {} LIMIT 5",
                    table.name
                ),
                1 => format!("SELECT {col_name}.{fname} FROM {} LIMIT 5", table.name),
                2 => {
                    format!(
                        "SELECT * FROM {} WHERE struct_extract({col_name}, '{fname}') IS NOT NULL LIMIT 5",
                        table.name
                    )
                }
                3 => {
                    // DISTINCT on struct field
                    format!(
                        "SELECT DISTINCT struct_extract({col_name}, '{fname}') FROM {}",
                        table.name
                    )
                }
                4 => {
                    // GROUP BY struct field
                    format!(
                        "SELECT struct_extract({col_name}, '{fname}'), COUNT(*) FROM {} GROUP BY struct_extract({col_name}, '{fname}')",
                        table.name
                    )
                }
                5 => {
                    // ORDER BY struct field
                    format!(
                        "SELECT id, struct_extract({col_name}, '{fname}') FROM {} ORDER BY struct_extract({col_name}, '{fname}') LIMIT 10",
                        table.name
                    )
                }
                6 => {
                    // TYPEOF on struct field
                    format!(
                        "SELECT TYPEOF(struct_extract({col_name}, '{fname}')), COUNT(*) FROM {} GROUP BY TYPEOF(struct_extract({col_name}, '{fname}'))",
                        table.name
                    )
                }
                7 => {
                    // COALESCE on struct field
                    format!(
                        "SELECT COALESCE(struct_extract({col_name}, '{fname}'), -1) FROM {} LIMIT 10",
                        table.name
                    )
                }
                8 => {
                    // ABS on struct field (numeric)
                    format!(
                        "SELECT ABS(struct_extract({col_name}, '{fname}')) FROM {} LIMIT 10",
                        table.name
                    )
                }
                _ => {
                    // Dot notation in WHERE
                    format!(
                        "SELECT id FROM {} WHERE {col_name}.{fname} IS NOT NULL LIMIT 10",
                        table.name
                    )
                }
            }
        }
        CustomColKind::Array(elem_type) => match rng.random_range(0..10u32) {
            0 => format!(
                "SELECT array_length({col_name}) FROM {} LIMIT 10",
                table.name
            ),
            1 => {
                let idx = rng.random_range(1..=3u32);
                format!("SELECT {col_name}[{idx}] FROM {} LIMIT 10", table.name)
            }
            2 => format!(
                "SELECT array_to_string({col_name}, ',') FROM {} LIMIT 10",
                table.name
            ),
            3 => {
                let val = gen_array_element(rng, elem_type);
                format!(
                    "SELECT COUNT(*) FROM {} WHERE array_contains({col_name}, {val}) = 1",
                    table.name
                )
            }
            4 => {
                let val = gen_array_element(rng, elem_type);
                format!(
                    "SELECT COUNT(*) FROM {} WHERE {col_name} @> ARRAY[{val}]",
                    table.name
                )
            }
            5 => format!(
                "SELECT array_length({col_name}), COUNT(*) FROM {} GROUP BY array_length({col_name})",
                table.name
            ),
            6 => {
                if rng.random_bool(0.5) {
                    format!("SELECT id FROM {} WHERE {col_name} IS NULL", table.name)
                } else {
                    format!("SELECT id FROM {} WHERE {col_name} IS NOT NULL", table.name)
                }
            }
            7 => format!(
                "SELECT id, {col_name} FROM {} ORDER BY array_length({col_name}) LIMIT 10",
                table.name
            ),
            8 => format!("SELECT {col_name} || ARRAY[1] FROM {} LIMIT 5", table.name),
            _ => format!(
                "SELECT TYPEOF({col_name}), COUNT(*) FROM {} GROUP BY TYPEOF({col_name})",
                table.name
            ),
        },
        CustomColKind::Domain(_) | CustomColKind::CustomType(_) => {
            match rng.random_range(0..4u32) {
                0 => format!(
                    "SELECT {col_name} FROM {} ORDER BY {col_name} LIMIT 10",
                    table.name
                ),
                1 => format!(
                    "SELECT TYPEOF({col_name}), COUNT(*) FROM {} GROUP BY TYPEOF({col_name})",
                    table.name
                ),
                2 => format!(
                    "SELECT id FROM {} WHERE {col_name} IS NOT NULL LIMIT 10",
                    table.name
                ),
                _ => format!(
                    "SELECT CAST({col_name} AS TEXT), COUNT(*) FROM {} GROUP BY CAST({col_name} AS TEXT)",
                    table.name
                ),
            }
        }
    };

    Some(query)
}

/// Generate INSERT ... SELECT (copy rows from one table to another with id offset).
fn gen_insert_select(rng: &mut StdRng, tables: &[TableDef], max_ids: &[i64]) -> Option<String> {
    if tables.len() < 2 {
        return None;
    }
    // Find two tables with compatible custom type columns
    let dst_idx = rng.random_range(0..tables.len());
    let dst = &tables[dst_idx];
    let src_candidates: Vec<usize> = (0..tables.len())
        .filter(|&i| {
            i != dst_idx
                && tables[i].columns.len() == dst.columns.len()
                && tables[i]
                    .columns
                    .iter()
                    .zip(dst.columns.iter())
                    .all(|(a, b)| a.1 == b.1)
        })
        .collect();

    if src_candidates.is_empty() {
        return None;
    }

    let src_idx = src_candidates[rng.random_range(0..src_candidates.len())];
    let src = &tables[src_idx];
    let id_offset = max_ids[dst_idx] + 1;

    // Build column list (offset the id)
    let cols: Vec<String> = dst
        .columns
        .iter()
        .enumerate()
        .map(|(i, (name, _, _))| {
            if i == 0 {
                format!("{name} + {id_offset}")
            } else {
                name.clone()
            }
        })
        .collect();

    Some(format!(
        "INSERT INTO {} SELECT {} FROM {} LIMIT 5",
        dst.name,
        cols.join(", "),
        src.name
    ))
}

/// Generate upsert (INSERT OR REPLACE / ON CONFLICT DO UPDATE) statements.
fn gen_upsert(rng: &mut StdRng, table: &TableDef, max_id: i64, types: &TypeDefs) -> String {
    // Use an existing id to trigger the conflict
    let target_id = rng.random_range(1..=max_id);
    let mut vals: Vec<String> = Vec::new();
    vals.push(target_id.to_string());

    for (col_i, (_, ty, is_custom)) in table.columns[1..].iter().enumerate() {
        let col_idx = col_i + 1;
        if *is_custom {
            let kind = table
                .custom_type_columns
                .iter()
                .find(|(i, _)| *i == col_idx)
                .map(|(_, k)| k)
                .unwrap();
            vals.push(gen_custom_value_or_null(rng, kind, types));
        } else {
            vals.push(gen_scalar_literal(rng, ty));
        }
    }

    match rng.random_range(0..5u32) {
        0 => {
            format!(
                "INSERT OR REPLACE INTO {} VALUES ({})",
                table.name,
                vals.join(", ")
            )
        }
        1 => {
            // ON CONFLICT DO UPDATE single column
            let updatable: Vec<usize> = (1..table.columns.len()).collect();
            let col_idx = updatable[rng.random_range(0..updatable.len())];
            let set_clause = gen_set_clause(rng, table, col_idx, types);
            format!(
                "INSERT INTO {} VALUES ({}) ON CONFLICT(id) DO UPDATE SET {set_clause}",
                table.name,
                vals.join(", ")
            )
        }
        2 => {
            // ON CONFLICT DO UPDATE SET col = excluded.col
            let updatable: Vec<usize> = (1..table.columns.len()).collect();
            let col_idx = updatable[rng.random_range(0..updatable.len())];
            let col_name = &table.columns[col_idx].0;
            format!(
                "INSERT INTO {} VALUES ({}) ON CONFLICT(id) DO UPDATE SET {col_name} = excluded.{col_name}",
                table.name,
                vals.join(", ")
            )
        }
        3 => {
            // ON CONFLICT DO UPDATE multiple columns
            let updatable: Vec<usize> = (1..table.columns.len()).collect();
            let n = rng.random_range(2..=updatable.len().min(4));
            let mut chosen = Vec::new();
            let mut used = std::collections::HashSet::new();
            for _ in 0..n {
                let mut attempts = 0;
                loop {
                    let idx = updatable[rng.random_range(0..updatable.len())];
                    if used.insert(idx) || attempts > 20 {
                        chosen.push(idx);
                        break;
                    }
                    attempts += 1;
                }
            }
            let set_clauses: Vec<String> = chosen
                .iter()
                .map(|&col_idx| gen_set_clause(rng, table, col_idx, types))
                .collect();
            format!(
                "INSERT INTO {} VALUES ({}) ON CONFLICT(id) DO UPDATE SET {}",
                table.name,
                vals.join(", "),
                set_clauses.join(", ")
            )
        }
        _ => {
            // ON CONFLICT DO UPDATE multiple excluded columns
            let updatable: Vec<usize> = (1..table.columns.len()).collect();
            let n = rng.random_range(2..=updatable.len().min(4));
            let mut chosen = Vec::new();
            let mut used = std::collections::HashSet::new();
            for _ in 0..n {
                let mut attempts = 0;
                loop {
                    let idx = updatable[rng.random_range(0..updatable.len())];
                    if used.insert(idx) || attempts > 20 {
                        chosen.push(idx);
                        break;
                    }
                    attempts += 1;
                }
            }
            let set_clauses: Vec<String> = chosen
                .iter()
                .map(|&col_idx| {
                    let col_name = &table.columns[col_idx].0;
                    format!("{col_name} = excluded.{col_name}")
                })
                .collect();
            format!(
                "INSERT INTO {} VALUES ({}) ON CONFLICT(id) DO UPDATE SET {}",
                table.name,
                vals.join(", "),
                set_clauses.join(", ")
            )
        }
    }
}

/// Generate a transaction/savepoint operation.
fn gen_transaction_op(rng: &mut StdRng) -> &'static str {
    match rng.random_range(0..6u32) {
        0 => "BEGIN",
        1 => "COMMIT",
        2 => "SAVEPOINT sp1",
        3 => "RELEASE sp1",
        4 => "ROLLBACK TO sp1",
        _ => "ROLLBACK",
    }
}

// ---------------------------------------------------------------------------
// Aggressive DML generators
// ---------------------------------------------------------------------------

/// Generate a "row torture" sequence: 3-5 sequential mutations on the same row.
/// INSERT → UPDATE custom col → UPDATE to NULL → UPDATE to different variant → DELETE
fn gen_row_torture(
    rng: &mut StdRng,
    table: &TableDef,
    table_idx: usize,
    max_id: &mut i64,
    types: &TypeDefs,
    variant_tracker: &mut VariantTracker,
) -> Vec<String> {
    let mut stmts = Vec::new();
    *max_id += 1;
    let row_id = *max_id;

    // Step 1: INSERT
    stmts.push(gen_insert(rng, table, row_id, types));

    // Track variants for this insert
    for (col_idx, kind) in &table.custom_type_columns {
        if let CustomColKind::Union(uname) = kind {
            let udef = types.unions.iter().find(|u| &u.name == uname);
            if let Some(udef) = udef {
                let (vname, _) = &udef.variants[rng.random_range(0..udef.variants.len())];
                variant_tracker.insert((table_idx, row_id, *col_idx), vname.clone());
            }
        }
    }

    let n_ops = rng.random_range(3..=5usize);
    for op in 0..n_ops {
        if table.custom_type_columns.is_empty() {
            break;
        }
        let (col_idx, kind) =
            &table.custom_type_columns[rng.random_range(0..table.custom_type_columns.len())];
        let col_name = &table.columns[*col_idx].0;

        match op % 4 {
            0 => {
                // UPDATE custom col to new value
                let new_val = gen_custom_value_or_null(rng, kind, types);
                stmts.push(format!(
                    "UPDATE {} SET {col_name} = {new_val} WHERE id = {row_id}",
                    table.name
                ));
            }
            1 => {
                // UPDATE to NULL (skip for NOT NULL domain columns)
                if is_not_null_domain(kind, types) {
                    let new_val = gen_custom_value_or_null(rng, kind, types);
                    stmts.push(format!(
                        "UPDATE {} SET {col_name} = {new_val} WHERE id = {row_id}",
                        table.name
                    ));
                } else {
                    stmts.push(format!(
                        "UPDATE {} SET {col_name} = NULL WHERE id = {row_id}",
                        table.name
                    ));
                }
            }
            2 => {
                // UPDATE to different variant (for unions)
                if let CustomColKind::Union(uname) = kind {
                    if let Some(val) = gen_different_variant_value(
                        rng,
                        table_idx,
                        row_id,
                        *col_idx,
                        uname,
                        types,
                        variant_tracker,
                    ) {
                        stmts.push(format!(
                            "UPDATE {} SET {col_name} = {val} WHERE id = {row_id}",
                            table.name
                        ));
                    }
                } else {
                    let new_val = gen_custom_value_or_null(rng, kind, types);
                    stmts.push(format!(
                        "UPDATE {} SET {col_name} = {new_val} WHERE id = {row_id}",
                        table.name
                    ));
                }
            }
            _ => {
                // Self-referencing update (no-op)
                stmts.push(format!(
                    "UPDATE {} SET {col_name} = {col_name} WHERE id = {row_id}",
                    table.name
                ));
            }
        }
    }

    // Final step: DELETE (50% chance) or leave the row
    if rng.random_bool(0.5) {
        stmts.push(format!("DELETE FROM {} WHERE id = {row_id}", table.name));
    }

    stmts
}

/// Pick a variant different from the one last used for this (table, row, col).
#[allow(clippy::too_many_arguments)]
fn gen_different_variant_value(
    rng: &mut StdRng,
    table_idx: usize,
    row_id: i64,
    col_idx: usize,
    uname: &str,
    types: &TypeDefs,
    variant_tracker: &mut VariantTracker,
) -> Option<String> {
    let udef = types.unions.iter().find(|u| u.name == uname)?;
    if udef.variants.len() < 2 {
        return None;
    }
    let last_variant = variant_tracker.get(&(table_idx, row_id, col_idx));
    let candidates: Vec<_> = udef
        .variants
        .iter()
        .filter(|(vn, _)| last_variant != Some(vn))
        .collect();
    let (vname, vtype) = if candidates.is_empty() {
        &udef.variants[0]
    } else {
        candidates[rng.random_range(0..candidates.len())]
    };
    let val = match vtype {
        VariantType::Struct(sname) => gen_struct_value(rng, sname, types),
        VariantType::Scalar(FieldType::Domain(dname)) => {
            if let Some(d) = types.domains.iter().find(|d| &d.name == dname) {
                gen_domain_value(rng, d, types)
            } else {
                "NULL".to_string()
            }
        }
        VariantType::Scalar(FieldType::CustomType(ctname)) => {
            if let Some(ct) = types.custom_types.iter().find(|c| &c.name == ctname) {
                gen_custom_type_value(rng, ct)
            } else {
                "NULL".to_string()
            }
        }
        VariantType::Scalar(ft) => gen_scalar_value(rng, ft),
        VariantType::Array(elem) => gen_array_value(rng, elem),
        VariantType::Union(inner_uname) => gen_union_value(rng, inner_uname, types),
    };
    variant_tracker.insert((table_idx, row_id, col_idx), vname.clone());
    Some(format!("union_value('{vname}', {val})"))
}

/// Generate an UPDATE that specifically switches a union column to a different variant.
fn gen_variant_switch_update(
    rng: &mut StdRng,
    table: &TableDef,
    table_idx: usize,
    max_id: i64,
    types: &TypeDefs,
    variant_tracker: &mut VariantTracker,
) -> Option<String> {
    // Find a union column
    let union_cols: Vec<_> = table
        .custom_type_columns
        .iter()
        .filter(|(_, k)| matches!(k, CustomColKind::Union(_)))
        .collect();
    if union_cols.is_empty() {
        return None;
    }
    let (col_idx, kind) = union_cols[rng.random_range(0..union_cols.len())];
    let col_name = &table.columns[*col_idx].0;
    let uname = match kind {
        CustomColKind::Union(u) => u,
        _ => return None,
    };

    let target_id = rng.random_range(1..=max_id);
    let val = gen_different_variant_value(
        rng,
        table_idx,
        target_id,
        *col_idx,
        uname,
        types,
        variant_tracker,
    )?;
    Some(format!(
        "UPDATE {} SET {col_name} = {val} WHERE id = {target_id}",
        table.name
    ))
}

/// Generate a self-referencing UPDATE: `UPDATE t SET x0 = x0 WHERE id = N`.
fn gen_self_ref_update(rng: &mut StdRng, table: &TableDef, max_id: i64) -> Option<String> {
    if table.custom_type_columns.is_empty() {
        return None;
    }
    let (col_idx, _) =
        &table.custom_type_columns[rng.random_range(0..table.custom_type_columns.len())];
    let col_name = &table.columns[*col_idx].0;
    let target_id = rng.random_range(1..=max_id);
    Some(format!(
        "UPDATE {} SET {col_name} = {col_name} WHERE id = {target_id}",
        table.name
    ))
}

/// Generate NULL↔Value transition updates.
fn gen_null_transition_update(
    rng: &mut StdRng,
    table: &TableDef,
    max_id: i64,
    types: &TypeDefs,
) -> Option<String> {
    if table.custom_type_columns.is_empty() {
        return None;
    }
    let (col_idx, kind) =
        &table.custom_type_columns[rng.random_range(0..table.custom_type_columns.len())];
    let col_name = &table.columns[*col_idx].0;
    let target_id = rng.random_range(1..=max_id);

    if rng.random_bool(0.5) && !is_not_null_domain(kind, types) {
        // Value → NULL (skip for NOT NULL domain columns)
        Some(format!(
            "UPDATE {} SET {col_name} = NULL WHERE {col_name} IS NOT NULL AND id = {target_id}",
            table.name
        ))
    } else {
        // NULL → Value
        let new_val = gen_custom_value_or_null(rng, kind, types);
        Some(format!(
            "UPDATE {} SET {col_name} = {new_val} WHERE {col_name} IS NULL AND id = {target_id}",
            table.name
        ))
    }
}

/// Generate multi-row DML that hits many rows at once.
fn gen_multi_row_dml(
    rng: &mut StdRng,
    table: &TableDef,
    max_id: i64,
    types: &TypeDefs,
) -> Option<String> {
    if table.custom_type_columns.is_empty() {
        return None;
    }

    let (col_idx, kind) =
        &table.custom_type_columns[rng.random_range(0..table.custom_type_columns.len())];
    let col_name = &table.columns[*col_idx].0;

    if rng.random_bool(0.5) {
        // DELETE all rows of one variant
        if let CustomColKind::Union(uname) = kind {
            let udef = types.unions.iter().find(|u| &u.name == uname)?;
            let (vname, _) = &udef.variants[rng.random_range(0..udef.variants.len())];
            Some(format!(
                "DELETE FROM {} WHERE union_tag({col_name}) = '{vname}'",
                table.name
            ))
        } else {
            // For structs, delete rows where a field matches
            let threshold = rng.random_range(1..=max_id);
            Some(format!("DELETE FROM {} WHERE id > {threshold}", table.name))
        }
    } else {
        // UPDATE many rows at once
        let threshold = rng.random_range(1..=max_id);
        let new_val = gen_custom_value_or_null(rng, kind, types);
        Some(format!(
            "UPDATE {} SET {col_name} = {new_val} WHERE id > {threshold}",
            table.name
        ))
    }
}

/// Generate a DELETE + re-INSERT cycle: delete a row then re-insert with same ID
/// but different custom type value.
fn gen_delete_reinsert(
    rng: &mut StdRng,
    table: &TableDef,
    max_id: i64,
    types: &TypeDefs,
) -> Vec<String> {
    let target_id = rng.random_range(1..=max_id);
    let delete_sql = format!("DELETE FROM {} WHERE id = {target_id}", table.name);
    let insert_sql = gen_insert(rng, table, target_id, types);
    vec![delete_sql, insert_sql]
}

/// Generate INSERT statements that use the DEFAULT keyword for some columns.
fn gen_insert_with_defaults(
    rng: &mut StdRng,
    table: &TableDef,
    row_id: i64,
    types: &TypeDefs,
) -> Option<String> {
    if table.defaults.is_empty() {
        return None;
    }

    let choice = rng.random_range(0..3u32);
    match choice {
        0 => {
            // INSERT INTO t DEFAULT VALUES — all columns get defaults
            Some(format!("INSERT INTO {} DEFAULT VALUES", table.name))
        }
        1 => {
            // Single-row INSERT with DEFAULT in specific positions
            let mut vals: Vec<String> = Vec::new();
            vals.push(row_id.to_string());

            let default_col_indices: std::collections::HashSet<usize> =
                table.defaults.iter().map(|(idx, _)| *idx).collect();

            for (col_i, (_, ty, is_custom)) in table.columns[1..].iter().enumerate() {
                let col_idx = col_i + 1;
                if default_col_indices.contains(&col_idx) && rng.random_bool(0.5) {
                    vals.push("DEFAULT".to_string());
                } else if *is_custom {
                    let kind = table
                        .custom_type_columns
                        .iter()
                        .find(|(i, _)| *i == col_idx)
                        .map(|(_, k)| k)
                        .unwrap();
                    vals.push(gen_custom_value_or_null(rng, kind, types));
                } else {
                    vals.push(gen_scalar_literal(rng, ty));
                }
            }
            Some(format!(
                "INSERT INTO {} VALUES ({})",
                table.name,
                vals.join(", ")
            ))
        }
        _ => {
            // Multi-row INSERT mixing DEFAULT and explicit values
            let default_col_indices: std::collections::HashSet<usize> =
                table.defaults.iter().map(|(idx, _)| *idx).collect();

            let mut all_rows: Vec<String> = Vec::new();
            for r in 0..rng.random_range(2..=3u32) {
                let rid = row_id + r as i64;
                let mut vals: Vec<String> = Vec::new();
                vals.push(rid.to_string());

                for (col_i, (_, ty, is_custom)) in table.columns[1..].iter().enumerate() {
                    let col_idx = col_i + 1;
                    // First row uses DEFAULT, second uses explicit
                    if default_col_indices.contains(&col_idx) && r == 0 {
                        vals.push("DEFAULT".to_string());
                    } else if *is_custom {
                        let kind = table
                            .custom_type_columns
                            .iter()
                            .find(|(i, _)| *i == col_idx)
                            .map(|(_, k)| k)
                            .unwrap();
                        vals.push(gen_custom_value_or_null(rng, kind, types));
                    } else {
                        vals.push(gen_scalar_literal(rng, ty));
                    }
                }
                all_rows.push(format!("({})", vals.join(", ")));
            }
            Some(format!(
                "INSERT INTO {} VALUES {}",
                table.name,
                all_rows.join(", ")
            ))
        }
    }
}

/// Generate an ALTER TABLE ADD COLUMN statement with a random type.
/// Returns the SQL and updated TableDef info (new column name, kind, optional default).
fn gen_alter_add_column(
    rng: &mut StdRng,
    table: &mut TableDef,
    types: &TypeDefs,
    alt_counter: &mut usize,
) -> String {
    let col_name = format!("alt{}", *alt_counter);
    *alt_counter += 1;

    let choice = rng.random_range(0..100u32);
    let (ty_str, kind) = if !types.domains.is_empty() && choice < 30 {
        let d = &types.domains[rng.random_range(0..types.domains.len())];
        (d.name.clone(), Some(CustomColKind::Domain(d.name.clone())))
    } else if !types.custom_types.is_empty() && choice < 50 {
        let ct = &types.custom_types[rng.random_range(0..types.custom_types.len())];
        (
            ct.name.clone(),
            Some(CustomColKind::CustomType(ct.name.clone())),
        )
    } else if choice < 70 {
        let s = &types.structs[rng.random_range(0..types.structs.len())];
        (s.name.clone(), Some(CustomColKind::Struct(s.name.clone())))
    } else {
        let scalar = match rng.random_range(0..3u32) {
            0 => "INT",
            1 => "TEXT",
            _ => "REAL",
        };
        (scalar.to_string(), None)
    };

    let is_custom = kind.is_some();

    // Check if the column requires NOT NULL (walks domain chain)
    let needs_default = match &kind {
        Some(k) => is_not_null_domain(k, types),
        _ => false,
    };

    // Optional DEFAULT (mandatory when domain has NOT NULL)
    let default_sql = if needs_default || rng.random_bool(0.3) {
        if let Some(ref k) = kind {
            Some(gen_default_value(rng, k, types))
        } else {
            match ty_str.as_str() {
                "INT" => Some(rng.random_range(0..=100i64).to_string()),
                "TEXT" => Some("'default'".to_string()),
                "REAL" => Some(format!("{:.2}", rng.random_range(0.0..100.0f64))),
                _ => None,
            }
        }
    } else {
        None
    };

    let sql = if let Some(ref def) = default_sql {
        format!(
            "ALTER TABLE {} ADD COLUMN {col_name} {ty_str} DEFAULT ({def})",
            table.name
        )
    } else {
        format!("ALTER TABLE {} ADD COLUMN {col_name} {ty_str}", table.name)
    };

    // Update the in-memory TableDef
    let idx = table.columns.len();
    table.columns.push((col_name, ty_str, is_custom));
    if let Some(k) = kind {
        table.custom_type_columns.push((idx, k));
    }
    if let Some(def) = default_sql {
        table.defaults.push((idx, def));
    }

    sql
}

// ---------------------------------------------------------------------------
// Array DML generators
// ---------------------------------------------------------------------------

/// Generate an array subscript update: `UPDATE t SET col[i] = val WHERE id = N`
fn gen_array_subscript_update(rng: &mut StdRng, table: &TableDef, max_id: i64) -> Option<String> {
    let array_cols: Vec<_> = table
        .custom_type_columns
        .iter()
        .filter(|(_, k)| matches!(k, CustomColKind::Array(_)))
        .collect();
    if array_cols.is_empty() {
        return None;
    }
    let (col_idx, kind) = array_cols[rng.random_range(0..array_cols.len())];
    let col_name = &table.columns[*col_idx].0;
    let elem_type = match kind {
        CustomColKind::Array(e) => e.as_str(),
        _ => unreachable!(),
    };
    let idx = rng.random_range(1..=4u32);
    let val = gen_array_element(rng, elem_type);
    let target_id = rng.random_range(1..=max_id);
    Some(format!(
        "UPDATE {} SET {col_name}[{idx}] = {val} WHERE id = {target_id}",
        table.name
    ))
}

/// Generate an array mutation: array_append/prepend/remove/cat/slice.
fn gen_array_mutation(rng: &mut StdRng, table: &TableDef, max_id: i64) -> Option<String> {
    let array_cols: Vec<_> = table
        .custom_type_columns
        .iter()
        .filter(|(_, k)| matches!(k, CustomColKind::Array(_)))
        .collect();
    if array_cols.is_empty() {
        return None;
    }
    let (col_idx, kind) = array_cols[rng.random_range(0..array_cols.len())];
    let col_name = &table.columns[*col_idx].0;
    let elem_type = match kind {
        CustomColKind::Array(e) => e.as_str(),
        _ => unreachable!(),
    };
    let target_id = rng.random_range(1..=max_id);
    let val = gen_array_element(rng, elem_type);

    let expr = match rng.random_range(0..5u32) {
        0 => format!("array_append({col_name}, {val})"),
        1 => format!("array_prepend({val}, {col_name})"),
        2 => format!("array_remove({col_name}, {val})"),
        3 => {
            let val2 = gen_array_element(rng, elem_type);
            format!("{col_name} || ARRAY[{val}, {val2}]")
        }
        _ => format!("array_slice({col_name}, 1, 3)"),
    };

    Some(format!(
        "UPDATE {} SET {col_name} = {expr} WHERE id = {target_id}",
        table.name
    ))
}

/// Generate 3-5 sequential array mutations on the same row.
fn gen_array_torture(rng: &mut StdRng, table: &TableDef, max_id: i64) -> Vec<String> {
    let array_cols: Vec<_> = table
        .custom_type_columns
        .iter()
        .filter(|(_, k)| matches!(k, CustomColKind::Array(_)))
        .collect();
    if array_cols.is_empty() {
        return vec![];
    }
    let (col_idx, kind) = array_cols[rng.random_range(0..array_cols.len())];
    let col_name = &table.columns[*col_idx].0;
    let elem_type = match kind {
        CustomColKind::Array(e) => e.as_str(),
        _ => unreachable!(),
    };
    let target_id = rng.random_range(1..=max_id);
    let n_ops = rng.random_range(3..=5usize);
    let mut stmts = Vec::new();

    for _ in 0..n_ops {
        let val = gen_array_element(rng, elem_type);
        let expr = match rng.random_range(0..4u32) {
            0 => format!("array_append({col_name}, {val})"),
            1 => format!("array_prepend({val}, {col_name})"),
            2 => format!("array_remove({col_name}, {val})"),
            _ => format!("{col_name} || ARRAY[{val}]"),
        };
        stmts.push(format!(
            "UPDATE {} SET {col_name} = {expr} WHERE id = {target_id}",
            table.name
        ));
    }
    stmts
}

// ---------------------------------------------------------------------------
// Trigger generator
// ---------------------------------------------------------------------------

/// A trigger we created, so we can track the sink table for consistency checks.
#[derive(Debug, Clone)]
struct TriggerDef {
    #[allow(dead_code)]
    trigger_name: String,
    sink_table: String,
    source_table: String,
    /// Column names in the sink table (for SELECT verification).
    #[allow(dead_code)]
    sink_columns: Vec<String>,
}

// ---------------------------------------------------------------------------
// Advanced SQL pattern generators (Group A: read-only, Group B: DML, Group C: DDL)
// ---------------------------------------------------------------------------

/// A1. Comparison ops on custom type columns.
fn gen_comparison_select(rng: &mut StdRng, table: &TableDef, types: &TypeDefs) -> Option<String> {
    if table.custom_type_columns.is_empty() {
        return None;
    }
    let (col_idx, kind) =
        &table.custom_type_columns[rng.random_range(0..table.custom_type_columns.len())];
    let col_name = &table.columns[*col_idx].0;

    let query = match kind {
        CustomColKind::Union(uname) => {
            let udef = types.unions.iter().find(|u| &u.name == uname)?;
            let (vname, _) = &udef.variants[rng.random_range(0..udef.variants.len())];
            match rng.random_range(0..4u32) {
                0 => format!(
                    "SELECT id FROM {} WHERE union_tag({col_name}) = '{vname}' ORDER BY id LIMIT 10",
                    table.name
                ),
                1 => format!(
                    "SELECT id FROM {} WHERE union_tag({col_name}) != '{vname}' ORDER BY id LIMIT 10",
                    table.name
                ),
                2 => format!(
                    "SELECT id FROM {} WHERE union_tag({col_name}) >= '{vname}' ORDER BY id LIMIT 10",
                    table.name
                ),
                _ => format!(
                    "SELECT id FROM {} WHERE {col_name} IS NOT NULL AND union_tag({col_name}) < '{vname}' ORDER BY id LIMIT 10",
                    table.name
                ),
            }
        }
        CustomColKind::Struct(sname) => {
            let sdef = types.structs.iter().find(|s| &s.name == sname)?;
            let (fname, _) = &sdef.fields[rng.random_range(0..sdef.fields.len())];
            match rng.random_range(0..3u32) {
                0 => format!(
                    "SELECT id FROM {} WHERE struct_extract({col_name}, '{fname}') IS NOT NULL ORDER BY id LIMIT 10",
                    table.name
                ),
                1 => format!(
                    "SELECT id FROM {} WHERE struct_extract({col_name}, '{fname}') > 0 ORDER BY id LIMIT 10",
                    table.name
                ),
                _ => format!(
                    "SELECT id FROM {} WHERE CAST(struct_extract({col_name}, '{fname}') AS TEXT) BETWEEN 'a' AND 'z' ORDER BY id LIMIT 10",
                    table.name
                ),
            }
        }
        CustomColKind::Array(_) => match rng.random_range(0..2u32) {
            0 => format!(
                "SELECT id FROM {} WHERE {col_name} IS NOT NULL ORDER BY id LIMIT 10",
                table.name
            ),
            _ => format!(
                "SELECT id FROM {} WHERE {col_name} IS NULL ORDER BY id LIMIT 10",
                table.name
            ),
        },
        _ => {
            format!(
                "SELECT id FROM {} WHERE {col_name} IS NOT NULL ORDER BY id LIMIT 10",
                table.name
            )
        }
    };
    Some(query)
}

/// A2. Scalar subquery in WHERE clause.
fn gen_scalar_subquery_select(rng: &mut StdRng, tables: &[TableDef]) -> Option<String> {
    if tables.is_empty() {
        return None;
    }
    let t = &tables[rng.random_range(0..tables.len())];
    // Find a scalar (non-custom) column
    let scalar_cols: Vec<usize> = t
        .columns
        .iter()
        .enumerate()
        .filter(|(i, (_, _, is_custom))| *i > 0 && !*is_custom)
        .map(|(i, _)| i)
        .collect();

    let query = if scalar_cols.is_empty() {
        match rng.random_range(0..2u32) {
            0 => format!(
                "SELECT id FROM {} WHERE id > (SELECT AVG(id) FROM {}) ORDER BY id LIMIT 10",
                t.name, t.name
            ),
            _ => format!(
                "SELECT id, (SELECT COUNT(*) FROM {} AS sub WHERE sub.id <= {}.id) AS cnt FROM {} ORDER BY id LIMIT 10",
                t.name, t.name, t.name
            ),
        }
    } else {
        let col_idx = scalar_cols[rng.random_range(0..scalar_cols.len())];
        let col_name = &t.columns[col_idx].0;
        let t2 = &tables[rng.random_range(0..tables.len())];
        match rng.random_range(0..3u32) {
            0 => format!(
                "SELECT id FROM {} WHERE id > (SELECT AVG(id) FROM {}) ORDER BY id LIMIT 10",
                t.name, t2.name
            ),
            1 => format!(
                "SELECT id, (SELECT COUNT(*) FROM {} AS sub WHERE sub.id <= {}.id) AS cnt FROM {} ORDER BY id LIMIT 10",
                t.name, t.name, t.name
            ),
            _ => format!(
                "SELECT id FROM {} WHERE {col_name} > (SELECT MIN({col_name}) FROM {}) ORDER BY id LIMIT 10",
                t.name, t.name
            ),
        }
    };
    Some(query)
}

/// A3. CTE (WITH clause) queries.
fn gen_cte_select(
    rng: &mut StdRng,
    tables: &[TableDef],
    expr_indexes: &[ExprIndexDef],
    types: &TypeDefs,
) -> Option<String> {
    if tables.is_empty() {
        return None;
    }
    let t = &tables[rng.random_range(0..tables.len())];

    match rng.random_range(0..4u32) {
        0 => {
            // CTE wrapping a simple select with GROUP BY
            if let Some((col_idx, CustomColKind::Union(uname))) = t
                .custom_type_columns
                .iter()
                .find(|(_, k)| matches!(k, CustomColKind::Union(_)))
            {
                let col_name = &t.columns[*col_idx].0;
                let _ = types.unions.iter().find(|u| &u.name == uname)?;
                Some(format!(
                    "WITH cte AS (SELECT id, union_tag({col_name}) AS tag FROM {}) SELECT tag, COUNT(*) AS cnt FROM cte GROUP BY tag ORDER BY tag",
                    t.name
                ))
            } else {
                Some(format!(
                    "WITH cte AS (SELECT id FROM {}) SELECT COUNT(*) FROM cte",
                    t.name
                ))
            }
        }
        1 => {
            // CTE with expression index
            let t_indexes: Vec<_> = expr_indexes
                .iter()
                .filter(|ei| ei.table_name == t.name)
                .collect();
            if let Some(eidx) = t_indexes.first() {
                Some(format!(
                    "WITH cte AS (SELECT id, {} AS expr_val FROM {}) SELECT expr_val, COUNT(*) FROM cte GROUP BY expr_val ORDER BY 1 LIMIT 20",
                    eidx.expr_sql, t.name
                ))
            } else {
                Some(format!(
                    "WITH cte AS (SELECT id FROM {}) SELECT COUNT(*) FROM cte",
                    t.name
                ))
            }
        }
        2 => {
            // Two CTEs joined
            if tables.len() >= 2 {
                let t2 = &tables[rng.random_range(0..tables.len())];
                Some(format!(
                    "WITH a AS (SELECT id FROM {}), b AS (SELECT id FROM {}) SELECT a.id FROM a INNER JOIN b ON a.id = b.id ORDER BY a.id LIMIT 10",
                    t.name, t2.name
                ))
            } else {
                Some(format!(
                    "WITH cte AS (SELECT id FROM {}) SELECT COUNT(*) FROM cte",
                    t.name
                ))
            }
        }
        _ => {
            // Recursive CTE (simple counter)
            Some("WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 10) SELECT x FROM cnt".to_string())
        }
    }
}

/// A4. Window function queries.
fn gen_window_function_select(
    rng: &mut StdRng,
    table: &TableDef,
    types: &TypeDefs,
) -> Option<String> {
    let query = match rng.random_range(0..5u32) {
        0 => format!(
            "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM {} LIMIT 10",
            table.name
        ),
        1 => format!(
            "SELECT id, LAG(id, 1) OVER (ORDER BY id) AS prev_id, LEAD(id, 1) OVER (ORDER BY id) AS next_id FROM {} LIMIT 10",
            table.name
        ),
        2 => {
            // RANK with custom type partition
            if let Some((col_idx, CustomColKind::Union(uname))) = table
                .custom_type_columns
                .iter()
                .find(|(_, k)| matches!(k, CustomColKind::Union(_)))
            {
                let col_name = &table.columns[*col_idx].0;
                let _ = types.unions.iter().find(|u| &u.name == uname)?;
                format!(
                    "SELECT id, RANK() OVER (PARTITION BY union_tag({col_name}) ORDER BY id) AS rnk FROM {} LIMIT 20",
                    table.name
                )
            } else {
                format!(
                    "SELECT id, RANK() OVER (ORDER BY id) AS rnk FROM {} LIMIT 10",
                    table.name
                )
            }
        }
        3 => {
            // SUM window
            format!(
                "SELECT id, SUM(id) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum FROM {} LIMIT 10",
                table.name
            )
        }
        _ => {
            // NTILE
            format!(
                "SELECT id, NTILE(3) OVER (ORDER BY id) AS bucket FROM {} LIMIT 10",
                table.name
            )
        }
    };
    Some(query)
}

/// A5. UNION/EXCEPT/INTERSECT compound queries.
fn gen_compound_select(rng: &mut StdRng, tables: &[TableDef]) -> Option<String> {
    if tables.len() < 2 {
        return None;
    }
    let t1 = &tables[rng.random_range(0..tables.len())];
    let t2 = &tables[rng.random_range(0..tables.len())];

    let query = match rng.random_range(0..4u32) {
        0 => format!(
            "SELECT id FROM {} UNION ALL SELECT id FROM {} ORDER BY 1 LIMIT 20",
            t1.name, t2.name
        ),
        1 => format!(
            "SELECT id FROM {} UNION SELECT id FROM {} ORDER BY 1 LIMIT 20",
            t1.name, t2.name
        ),
        2 => format!(
            "SELECT id FROM {} INTERSECT SELECT id FROM {} ORDER BY 1 LIMIT 20",
            t1.name, t2.name
        ),
        _ => format!(
            "SELECT id FROM {} EXCEPT SELECT id FROM {} ORDER BY 1 LIMIT 20",
            t1.name, t2.name
        ),
    };
    Some(query)
}

/// A6. Multi-table JOIN (3+ tables).
fn gen_multi_join_select(
    rng: &mut StdRng,
    tables: &[TableDef],
    types: &TypeDefs,
) -> Option<String> {
    if tables.len() < 3 {
        return None;
    }
    let indices: Vec<usize> = {
        let mut v: Vec<usize> = (0..tables.len()).collect();
        // Shuffle and take 3
        for i in (1..v.len()).rev() {
            let j = rng.random_range(0..=i);
            v.swap(i, j);
        }
        v.into_iter().take(3).collect()
    };
    let t0 = &tables[indices[0]];
    let t1 = &tables[indices[1]];
    let t2 = &tables[indices[2]];

    let join2_kind = match rng.random_range(0..3u32) {
        0 => "INNER JOIN",
        1 => "LEFT JOIN",
        _ => "LEFT JOIN",
    };
    let join3_kind = match rng.random_range(0..3u32) {
        0 => "INNER JOIN",
        1 => "LEFT JOIN",
        _ => "LEFT JOIN",
    };

    // Optionally add a WHERE on custom types
    let where_clause = if rng.random_bool(0.4) {
        if let Some((col_idx, CustomColKind::Union(uname))) = t0
            .custom_type_columns
            .iter()
            .find(|(_, k)| matches!(k, CustomColKind::Union(_)))
        {
            let col_name = &t0.columns[*col_idx].0;
            if let Some(udef) = types.unions.iter().find(|u| &u.name == uname) {
                let (vname, _) = &udef.variants[rng.random_range(0..udef.variants.len())];
                format!(" WHERE union_tag({}.{col_name}) = '{vname}'", t0.name)
            } else {
                String::new()
            }
        } else {
            String::new()
        }
    } else {
        String::new()
    };

    Some(format!(
        "SELECT {t0}.id, {t1}.id, {t2}.id FROM {t0} {join2_kind} {t1} ON {t0}.id = {t1}.id {join3_kind} {t2} ON {t1}.id = {t2}.id{where_clause} ORDER BY {t0}.id LIMIT 10",
        t0 = t0.name,
        t1 = t1.name,
        t2 = t2.name
    ))
}

/// B1. INSERT OR IGNORE (50% existing id, 50% new id).
fn gen_insert_or_ignore(
    rng: &mut StdRng,
    table: &TableDef,
    max_id: i64,
    types: &TypeDefs,
) -> (String, bool) {
    let use_existing = rng.random_bool(0.5) && max_id > 0;
    let row_id = if use_existing {
        rng.random_range(1..=max_id)
    } else {
        max_id + 1
    };

    let mut vals: Vec<String> = Vec::new();
    vals.push(row_id.to_string());
    for (col_i, (_, ty, is_custom)) in table.columns[1..].iter().enumerate() {
        let col_idx = col_i + 1;
        if *is_custom {
            let kind = table
                .custom_type_columns
                .iter()
                .find(|(i, _)| *i == col_idx)
                .map(|(_, k)| k)
                .unwrap();
            vals.push(gen_custom_value_or_null(rng, kind, types));
        } else {
            vals.push(gen_scalar_literal(rng, ty));
        }
    }

    let sql = format!(
        "INSERT OR IGNORE INTO {} VALUES ({})",
        table.name,
        vals.join(", ")
    );
    // Returns whether this was a new id (caller should bump max_id)
    (sql, !use_existing)
}

/// B2. INSERT...SELECT cross-table.
fn gen_insert_select_cross(
    rng: &mut StdRng,
    tables: &[TableDef],
    max_ids: &[i64],
) -> Option<(String, usize, i64)> {
    if tables.len() < 2 {
        return None;
    }
    let dst_idx = rng.random_range(0..tables.len());
    let dst = &tables[dst_idx];

    // Find a compatible source
    let src_candidates: Vec<usize> = (0..tables.len())
        .filter(|&i| {
            i != dst_idx
                && tables[i].columns.len() == dst.columns.len()
                && tables[i]
                    .columns
                    .iter()
                    .zip(dst.columns.iter())
                    .all(|(a, b)| a.1 == b.1)
        })
        .collect();

    if src_candidates.is_empty() {
        return None;
    }

    let src_idx = src_candidates[rng.random_range(0..src_candidates.len())];
    let src = &tables[src_idx];
    let id_offset = max_ids[dst_idx] + 1;
    let limit = rng.random_range(2..=5i64);

    let cols: Vec<String> = dst
        .columns
        .iter()
        .enumerate()
        .map(|(i, (name, _, _))| {
            if i == 0 {
                format!("{name} + {id_offset}")
            } else {
                name.clone()
            }
        })
        .collect();

    let sql = format!(
        "INSERT INTO {} SELECT {} FROM {} ORDER BY id LIMIT {limit}",
        dst.name,
        cols.join(", "),
        src.name
    );
    Some((sql, dst_idx, limit))
}

/// B3. DELETE with correlated subquery.
fn gen_delete_correlated(rng: &mut StdRng, tables: &[TableDef]) -> Option<String> {
    if tables.len() < 2 {
        return None;
    }
    let t1_idx = rng.random_range(0..tables.len());
    let t1 = &tables[t1_idx];
    let t2_candidates: Vec<_> = tables.iter().filter(|t| t.name != t1.name).collect();
    if t2_candidates.is_empty() {
        return None;
    }
    let t2 = t2_candidates[rng.random_range(0..t2_candidates.len())];

    let query = match rng.random_range(0..3u32) {
        0 => format!(
            "DELETE FROM {} WHERE EXISTS (SELECT 1 FROM {} WHERE {}.id = {}.id)",
            t1.name, t2.name, t2.name, t1.name
        ),
        1 => format!(
            "DELETE FROM {} WHERE id IN (SELECT id FROM {} LIMIT 3)",
            t1.name, t2.name
        ),
        _ => format!(
            "DELETE FROM {} WHERE id > (SELECT AVG(id) FROM {})",
            t1.name, t2.name
        ),
    };
    Some(query)
}

/// B4. UPDATE with subquery in SET clause (scalar columns only).
fn gen_update_subquery(rng: &mut StdRng, tables: &[TableDef], max_id: i64) -> Option<String> {
    if tables.is_empty() || max_id <= 0 {
        return None;
    }
    let t = &tables[rng.random_range(0..tables.len())];
    // Find scalar columns
    let scalar_cols: Vec<usize> = t
        .columns
        .iter()
        .enumerate()
        .filter(|(i, (_, _, is_custom))| *i > 0 && !*is_custom)
        .map(|(i, _)| i)
        .collect();

    if scalar_cols.is_empty() {
        return None;
    }
    let col_idx = scalar_cols[rng.random_range(0..scalar_cols.len())];
    let col_name = &t.columns[col_idx].0;
    let target_id = rng.random_range(1..=max_id);

    let t2 = &tables[rng.random_range(0..tables.len())];
    // Find a scalar column in t2 for the subquery
    let t2_scalar_cols: Vec<usize> = t2
        .columns
        .iter()
        .enumerate()
        .filter(|(i, (_, _, is_custom))| *i > 0 && !*is_custom)
        .map(|(i, _)| i)
        .collect();

    let subquery = if t2_scalar_cols.is_empty() {
        format!("(SELECT MAX(id) FROM {})", t2.name)
    } else {
        let t2_col_idx = t2_scalar_cols[rng.random_range(0..t2_scalar_cols.len())];
        let t2_col = &t2.columns[t2_col_idx].0;
        match rng.random_range(0..2u32) {
            0 => format!("(SELECT MAX({t2_col}) FROM {})", t2.name),
            _ => format!("(SELECT MIN({t2_col}) FROM {})", t2.name),
        }
    };

    Some(format!(
        "UPDATE {} SET {col_name} = {subquery} WHERE id = {target_id}",
        t.name
    ))
}

/// C1. Partial index creation.
fn gen_partial_index(
    rng: &mut StdRng,
    table: &TableDef,
    types: &TypeDefs,
    counter: &mut usize,
) -> Option<String> {
    let idx_name = format!("pidx_{counter}");
    *counter += 1;

    let query = if let Some((col_idx, kind)) = table
        .custom_type_columns
        .iter()
        .find(|(_, k)| matches!(k, CustomColKind::Union(_)))
    {
        let col_name = &table.columns[*col_idx].0;
        match kind {
            CustomColKind::Union(uname) => {
                let udef = types.unions.iter().find(|u| &u.name == uname)?;
                let (vname, _) = &udef.variants[rng.random_range(0..udef.variants.len())];
                match rng.random_range(0..2u32) {
                    0 => format!(
                        "CREATE INDEX {idx_name} ON {}(CAST({col_name} AS TEXT)) WHERE id > {}",
                        table.name,
                        rng.random_range(1..=10i64)
                    ),
                    _ => format!(
                        "CREATE INDEX {idx_name} ON {}(id) WHERE union_tag({col_name}) = '{vname}'",
                        table.name
                    ),
                }
            }
            _ => unreachable!(),
        }
    } else {
        // Scalar column partial index
        let scalar_cols: Vec<usize> = table
            .columns
            .iter()
            .enumerate()
            .filter(|(i, (_, _, is_custom))| *i > 0 && !*is_custom)
            .map(|(i, _)| i)
            .collect();
        if scalar_cols.is_empty() {
            return None;
        }
        let col_idx = scalar_cols[rng.random_range(0..scalar_cols.len())];
        let col_name = &table.columns[col_idx].0;
        format!(
            "CREATE INDEX {idx_name} ON {}({col_name}) WHERE {col_name} IS NOT NULL",
            table.name
        )
    };
    Some(query)
}

/// C2. CREATE TABLE AS SELECT (ephemeral: create, verify, drop).
fn gen_ctas(rng: &mut StdRng, tables: &[TableDef], counter: &mut usize) -> Option<Vec<String>> {
    if tables.is_empty() {
        return None;
    }
    let t = &tables[rng.random_range(0..tables.len())];
    let tname = format!("ctas_{counter}");
    *counter += 1;

    let create = format!(
        "CREATE TABLE {tname} AS SELECT * FROM {} ORDER BY id LIMIT 10",
        t.name
    );
    let verify = format!("SELECT COUNT(*) FROM {tname}");
    let drop = format!("DROP TABLE {tname}");
    Some(vec![create, verify, drop])
}

/// C3. CREATE VIEW + query + DROP VIEW.
fn gen_view_and_query(
    rng: &mut StdRng,
    tables: &[TableDef],
    types: &TypeDefs,
    counter: &mut usize,
) -> Option<Vec<String>> {
    if tables.is_empty() {
        return None;
    }
    let t = &tables[rng.random_range(0..tables.len())];
    let vname = format!("vw_{counter}");
    *counter += 1;

    let view_body = if let Some((col_idx, CustomColKind::Union(uname))) = t
        .custom_type_columns
        .iter()
        .find(|(_, k)| matches!(k, CustomColKind::Union(_)))
    {
        let col_name = &t.columns[*col_idx].0;
        let _ = types.unions.iter().find(|u| &u.name == uname)?;
        format!("SELECT id, union_tag({col_name}) AS tag FROM {}", t.name)
    } else if let Some((col_idx, CustomColKind::Struct(sname))) = t
        .custom_type_columns
        .iter()
        .find(|(_, k)| matches!(k, CustomColKind::Struct(_)))
    {
        let col_name = &t.columns[*col_idx].0;
        let sdef = types.structs.iter().find(|s| &s.name == sname)?;
        let fname = &sdef.fields[0].0;
        format!(
            "SELECT id, struct_extract({col_name}, '{fname}') AS f FROM {}",
            t.name
        )
    } else {
        format!("SELECT id FROM {}", t.name)
    };

    let create = format!("CREATE VIEW {vname} AS {view_body}");
    let query = format!("SELECT * FROM {vname} ORDER BY id LIMIT 10");
    let drop = format!("DROP VIEW {vname}");
    Some(vec![create, query, drop])
}

/// Generate triggers that decompose struct/union columns via
/// `struct_extract(union_extract(NEW.col, ...), ...)` and `union_tag(NEW.col)`.
/// Returns the DDL statements to execute and the trigger definitions.
fn gen_triggers(
    rng: &mut StdRng,
    tables: &[TableDef],
    types: &TypeDefs,
    trigger_counter: &mut usize,
) -> Vec<(Vec<String>, TriggerDef)> {
    let mut results = Vec::new();

    for table in tables {
        // Only create triggers on tables with union columns that have struct variants
        for (col_idx, kind) in &table.custom_type_columns {
            let uname = match kind {
                CustomColKind::Union(u) => u,
                _ => continue,
            };
            let udef = match types.unions.iter().find(|u| &u.name == uname) {
                Some(u) => u,
                None => continue,
            };
            // Find struct variants
            let struct_variants: Vec<(&str, &str)> = udef
                .variants
                .iter()
                .filter_map(|(vn, vt)| match vt {
                    VariantType::Struct(sn) => Some((vn.as_str(), sn.as_str())),
                    _ => None,
                })
                .collect();
            if struct_variants.is_empty() {
                continue;
            }

            // 50% chance to create a trigger for this column
            if !rng.random_bool(0.5) {
                continue;
            }

            let col_name = &table.columns[*col_idx].0;
            let idx = *trigger_counter;
            *trigger_counter += 1;

            let sink_name = format!("trigger_sink_{idx}");
            let trigger_name = format!("trg_{idx}");

            // Pick a random struct variant to decompose
            let (vname, sname) = struct_variants[rng.random_range(0..struct_variants.len())];
            let sdef = match types.structs.iter().find(|s| s.name == sname) {
                Some(s) => s,
                None => continue,
            };

            // Build sink table columns: id, tag, then one column per struct field
            let mut sink_cols = vec!["id INTEGER PRIMARY KEY".to_string(), "tag TEXT".to_string()];
            let mut sink_col_names = vec!["id".to_string(), "tag".to_string()];
            let mut insert_exprs = vec![format!("NEW.id"), format!("union_tag(NEW.{col_name})")];

            for (fi, (fname, ftype)) in sdef.fields.iter().enumerate() {
                let sink_col = format!("f{fi}");
                let type_str = match ftype {
                    FieldType::Int | FieldType::Smallint | FieldType::Bigint => "INTEGER",
                    FieldType::Real | FieldType::Numeric(_, _) => "REAL",
                    FieldType::Text
                    | FieldType::Varchar(_)
                    | FieldType::Date
                    | FieldType::Time
                    | FieldType::Timestamp
                    | FieldType::Inet
                    | FieldType::Domain(_)
                    | FieldType::CustomType(_) => "TEXT",
                    FieldType::Boolean => "INTEGER",
                    FieldType::Bytea | FieldType::Struct(_) | FieldType::Array(_) => "BLOB",
                };
                sink_cols.push(format!("{sink_col} {type_str}"));
                sink_col_names.push(sink_col);
                insert_exprs.push(format!(
                    "struct_extract(union_extract(NEW.{col_name}, '{vname}'), '{fname}')"
                ));
            }

            let create_sink = format!("CREATE TABLE {sink_name}({}) STRICT", sink_cols.join(", "));
            let create_trigger = format!(
                "CREATE TRIGGER {trigger_name} AFTER INSERT ON {} BEGIN \
                 INSERT INTO {sink_name}({}) VALUES ({}); \
                 END",
                table.name,
                sink_col_names.join(", "),
                insert_exprs.join(", "),
            );

            let tdef = TriggerDef {
                trigger_name,
                sink_table: sink_name,
                source_table: table.name.clone(),
                sink_columns: sink_col_names,
            };
            results.push((vec![create_sink, create_trigger], tdef));
        }
    }
    results
}

// ---------------------------------------------------------------------------
// Consistency checks
// ---------------------------------------------------------------------------

/// For each expression index, verify that scanning the table and evaluating the
/// expression produces the same result as using the index.
#[allow(clippy::too_many_arguments)]
fn run_consistency_checks(
    conn: &Arc<turso_core::Connection>,
    tables: &[TableDef],
    expr_indexes: &[ExprIndexDef],
    triggers: &[TriggerDef],
    types: &TypeDefs,
    stats: &mut FuzzerStats,
    executed_sql: &mut Vec<String>,
    verbose: bool,
) -> Result<()> {
    // Check 1: for each expression index, compare indexed vs full-scan results
    for eidx in expr_indexes {
        let scan_sql = format!(
            "SELECT id, ({}) FROM {} ORDER BY id",
            eidx.expr_sql, eidx.table_name
        );
        let idx_sql = format!(
            "SELECT id, ({}) FROM {} INDEXED BY {} ORDER BY id",
            eidx.expr_sql, eidx.table_name, eidx.index_name
        );

        if verbose {
            tracing::debug!("Consistency: {}", scan_sql);
        }

        let scan_rows = match execute_turso_rows(conn, &scan_sql) {
            Ok(r) => r,
            Err(e) => {
                if verbose {
                    tracing::debug!("Scan query failed (ok): {e}");
                }
                continue;
            }
        };

        if let Ok(idx_rows) = execute_turso_rows(conn, &idx_sql) {
            if scan_rows != idx_rows {
                stats.consistency_failures += 1;
                tracing::error!(
                    "CONSISTENCY: scan vs index mismatch for {}\n  scan: {:?}\n  idx:  {:?}",
                    eidx.expr_sql,
                    scan_rows,
                    idx_rows,
                );
                executed_sql.push(format!("-- CONSISTENCY FAIL: {}", eidx.expr_sql));
            }
        }
    }

    // Check 2: union_tag round-trip — tag of inserted value should match
    for table in tables {
        for (col_idx, kind) in &table.custom_type_columns {
            if let CustomColKind::Union(_) = kind {
                let col_name = &table.columns[*col_idx].0;
                // Verify union_tag is never NULL for non-NULL union values
                let check_sql = format!(
                    "SELECT COUNT(*) FROM {} WHERE {col_name} IS NOT NULL AND union_tag({col_name}) IS NULL",
                    table.name
                );
                if let Ok(rows) = execute_turso_rows(conn, &check_sql) {
                    if rows.len() == 1 && rows[0].len() == 1 && rows[0][0] != "0" {
                        stats.consistency_failures += 1;
                        tracing::error!(
                            "CONSISTENCY: union_tag returned NULL for non-NULL union values in {}.{col_name} ({} rows)",
                            table.name,
                            rows[0][0]
                        );
                        executed_sql.push(format!(
                            "-- CONSISTENCY FAIL: NULL union_tag in {}.{col_name}",
                            table.name
                        ));
                    }
                }
            }
        }
    }

    // Check 3: COUNT aggregates match between scan and indexed paths
    for eidx in expr_indexes {
        let scan_count = format!(
            "SELECT COUNT(*) FROM {} WHERE ({}) IS NOT NULL",
            eidx.table_name, eidx.expr_sql
        );
        let idx_count = format!(
            "SELECT COUNT(*) FROM {} INDEXED BY {} WHERE ({}) IS NOT NULL",
            eidx.table_name, eidx.index_name, eidx.expr_sql
        );

        if let (Ok(scan_rows), Ok(idx_rows)) = (
            execute_turso_rows(conn, &scan_count),
            execute_turso_rows(conn, &idx_count),
        ) {
            if scan_rows != idx_rows {
                stats.consistency_failures += 1;
                tracing::error!(
                    "CONSISTENCY: COUNT mismatch for {} IS NOT NULL\n  scan: {:?}\n  idx:  {:?}",
                    eidx.expr_sql,
                    scan_rows,
                    idx_rows,
                );
            }
        }
    }

    // Check 4: Specific value lookups via index — catches stale/wrong index entries.
    // For each expression index, get the distinct values via full scan, then verify
    // that filtering by each value produces the same results via scan and index.
    for eidx in expr_indexes {
        let distinct_sql = format!(
            "SELECT DISTINCT ({}) FROM {} WHERE ({}) IS NOT NULL",
            eidx.expr_sql, eidx.table_name, eidx.expr_sql
        );
        let distinct_values = match execute_turso_rows(conn, &distinct_sql) {
            Ok(rows) => rows,
            Err(_) => continue,
        };

        for row in &distinct_values {
            if row.is_empty() {
                continue;
            }
            let val = &row[0];
            // Quote the value for use in WHERE clause
            let where_val = if val.parse::<f64>().is_ok() {
                val.clone()
            } else {
                format!("'{}'", val.replace('\'', "''"))
            };
            let scan_sql = format!(
                "SELECT id FROM {} WHERE ({}) = {} ORDER BY id",
                eidx.table_name, eidx.expr_sql, where_val
            );
            let idx_sql = format!(
                "SELECT id FROM {} INDEXED BY {} WHERE ({}) = {} ORDER BY id",
                eidx.table_name, eidx.index_name, eidx.expr_sql, where_val
            );
            if let (Ok(scan_rows), Ok(idx_rows)) = (
                execute_turso_rows(conn, &scan_sql),
                execute_turso_rows(conn, &idx_sql),
            ) {
                if scan_rows != idx_rows {
                    stats.consistency_failures += 1;
                    tracing::error!(
                        "CONSISTENCY: value lookup mismatch for {} = {}\n  scan: {:?}\n  idx:  {:?}",
                        eidx.expr_sql,
                        where_val,
                        scan_rows,
                        idx_rows,
                    );
                    executed_sql.push(format!(
                        "-- CONSISTENCY FAIL: {} = {} lookup mismatch",
                        eidx.expr_sql, where_val
                    ));
                }
            }
        }
    }

    // Check 5: Aggregate SUM consistency — catches wrong-value entries that
    // individual lookups might miss if they happen to be in the distinct set.
    for eidx in expr_indexes {
        // Only run SUM check on numeric expressions (skip tag/text ones)
        if eidx.expr_sql.contains("union_tag(")
            || eidx.expr_sql.contains("LOWER(")
            || eidx.expr_sql.contains("UPPER(")
            || eidx.expr_sql.starts_with("CAST(")
        {
            continue;
        }

        let scan_sum = format!("SELECT SUM({}) FROM {}", eidx.expr_sql, eidx.table_name);
        let idx_sum = format!(
            "SELECT SUM({}) FROM {} INDEXED BY {}",
            eidx.expr_sql, eidx.table_name, eidx.index_name
        );

        if let (Ok(scan_rows), Ok(idx_rows)) = (
            execute_turso_rows(conn, &scan_sum),
            execute_turso_rows(conn, &idx_sum),
        ) {
            if scan_rows != idx_rows {
                stats.consistency_failures += 1;
                tracing::error!(
                    "CONSISTENCY: SUM mismatch for {}\n  scan: {:?}\n  idx:  {:?}",
                    eidx.expr_sql,
                    scan_rows,
                    idx_rows,
                );
                executed_sql.push(format!(
                    "-- CONSISTENCY FAIL: SUM({}) mismatch",
                    eidx.expr_sql
                ));
            }
        }
    }

    // Check 6: array_length(array_append(col, 99)) = array_length(col) + 1
    for table in tables {
        for (col_idx, kind) in &table.custom_type_columns {
            if let CustomColKind::Array(_) = kind {
                let col_name = &table.columns[*col_idx].0;
                let check_sql = format!(
                    "SELECT COUNT(*) FROM {} WHERE {col_name} IS NOT NULL AND \
                     array_length(array_append({col_name}, 99)) != array_length({col_name}) + 1",
                    table.name
                );
                if let Ok(rows) = execute_turso_rows(conn, &check_sql) {
                    if rows.len() == 1 && rows[0].len() == 1 && rows[0][0] != "0" {
                        stats.consistency_failures += 1;
                        tracing::error!(
                            "CONSISTENCY: array append identity violated for {}.{col_name}: {} rows",
                            table.name,
                            rows[0][0]
                        );
                        executed_sql.push(format!(
                            "-- CONSISTENCY FAIL: array append identity {}.{col_name}",
                            table.name
                        ));
                    }
                }
            }
        }
    }

    // Check 7: array_length is non-negative for non-NULL arrays
    for table in tables {
        for (col_idx, kind) in &table.custom_type_columns {
            if let CustomColKind::Array(_) = kind {
                let col_name = &table.columns[*col_idx].0;
                let check_sql = format!(
                    "SELECT COUNT(*) FROM {} WHERE {col_name} IS NOT NULL AND array_length({col_name}) < 0",
                    table.name
                );
                if let Ok(rows) = execute_turso_rows(conn, &check_sql) {
                    if rows.len() == 1 && rows[0].len() == 1 && rows[0][0] != "0" {
                        stats.consistency_failures += 1;
                        tracing::error!(
                            "CONSISTENCY: negative array_length for {}.{col_name}: {} rows",
                            table.name,
                            rows[0][0]
                        );
                    }
                }
            }
        }
    }

    // Check 8: domain NOT NULL columns should have no NULLs
    // Skip ALTER-added columns ("alt*") since existing rows may have NULL
    // from before the ALTER TABLE ADD COLUMN.
    for table in tables {
        for (col_idx, kind) in &table.custom_type_columns {
            if let CustomColKind::Domain(dname) = kind {
                if let Some(d) = types.domains.iter().find(|d| &d.name == dname) {
                    if d.not_null {
                        let col_name = &table.columns[*col_idx].0;
                        if col_name.starts_with("alt") {
                            continue;
                        }
                        let check_sql = format!(
                            "SELECT COUNT(*) FROM {} WHERE {col_name} IS NULL",
                            table.name
                        );
                        if let Ok(rows) = execute_turso_rows(conn, &check_sql) {
                            if rows.len() == 1 && rows[0].len() == 1 && rows[0][0] != "0" {
                                stats.consistency_failures += 1;
                                tracing::error!(
                                    "CONSISTENCY: NOT NULL domain {dname} has NULL values in {}.{col_name}: {} rows",
                                    table.name,
                                    rows[0][0]
                                );
                                executed_sql.push(format!(
                                    "-- CONSISTENCY FAIL: NOT NULL domain {dname} has NULLs in {}.{col_name}",
                                    table.name
                                ));
                            }
                        }
                    }
                }
            }
        }
    }

    // Check 9: domain CHECK constraints hold for all rows
    for table in tables {
        for (col_idx, kind) in &table.custom_type_columns {
            if let CustomColKind::Domain(dname) = kind {
                if let Some(d) = types.domains.iter().find(|d| &d.name == dname) {
                    let col_name = &table.columns[*col_idx].0;
                    for check_expr in &d.checks {
                        let where_expr = check_expr.replace("value", col_name);
                        let check_sql = format!(
                            "SELECT COUNT(*) FROM {} WHERE {col_name} IS NOT NULL AND NOT ({where_expr})",
                            table.name
                        );
                        if let Ok(rows) = execute_turso_rows(conn, &check_sql) {
                            if rows.len() == 1 && rows[0].len() == 1 && rows[0][0] != "0" {
                                stats.consistency_failures += 1;
                                tracing::error!(
                                    "CONSISTENCY: domain CHECK({check_expr}) violated in {}.{col_name}: {} rows",
                                    table.name,
                                    rows[0][0]
                                );
                                executed_sql.push(format!(
                                    "-- CONSISTENCY FAIL: domain CHECK violated in {}.{col_name}",
                                    table.name
                                ));
                            }
                        }
                    }
                }
            }
        }
    }

    // Check 10: trigger sink tables — sink count should be >= source count.
    // The trigger fires AFTER INSERT, so REPLACE (DELETE+INSERT) and upsert
    // operations cause the sink to accumulate more rows than the source.
    for trigger in triggers {
        let source_count = format!("SELECT COUNT(*) FROM {}", trigger.source_table);
        let sink_count = format!("SELECT COUNT(*) FROM {}", trigger.sink_table);

        if let (Ok(src), Ok(snk)) = (
            execute_turso_rows(conn, &source_count),
            execute_turso_rows(conn, &sink_count),
        ) {
            if verbose {
                tracing::debug!(
                    "Trigger sink {}: source={:?} sink={:?}",
                    trigger.sink_table,
                    src,
                    snk
                );
            }
            let src_n: i64 = src
                .first()
                .and_then(|r| r.first())
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            let snk_n: i64 = snk
                .first()
                .and_then(|r| r.first())
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            if snk_n < src_n {
                stats.consistency_failures += 1;
                tracing::error!(
                    "CONSISTENCY: trigger sink row count too low for {}: source {} vs sink {}",
                    trigger.sink_table,
                    src_n,
                    snk_n,
                );
                executed_sql.push(format!(
                    "-- CONSISTENCY FAIL: trigger sink {} count too low",
                    trigger.sink_table
                ));
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// SQL execution helpers
// ---------------------------------------------------------------------------

fn execute_turso(conn: &Arc<turso_core::Connection>, sql: &str) -> Result<()> {
    conn.execute(sql)
        .map_err(|e| anyhow::anyhow!("Turso error: {e}"))
}

fn execute_turso_rows(conn: &Arc<turso_core::Connection>, sql: &str) -> Result<Vec<Vec<String>>> {
    let mut rows_result = conn
        .query(sql)
        .map_err(|e| anyhow::anyhow!("Turso query error: {e}"))?
        .ok_or_else(|| anyhow::anyhow!("No result from query"))?;

    let mut rows = Vec::new();
    rows_result
        .run_with_row_callback(|row| {
            let mut values = Vec::with_capacity(row.len());
            for col in 0..row.len() {
                let val = row.get_value(col);
                match val {
                    turso_core::Value::Null => values.push("null".to_string()),
                    turso_core::Value::Numeric(turso_core::Numeric::Integer(i)) => {
                        values.push(i.to_string())
                    }
                    turso_core::Value::Numeric(turso_core::Numeric::Float(f)) => {
                        values.push(f.to_string())
                    }
                    turso_core::Value::Text(s) => values.push(s.as_str().to_string()),
                    turso_core::Value::Blob(b) => {
                        let hex: String = b.iter().map(|byte| format!("{byte:02x}")).collect();
                        values.push(format!("x'{hex}'"));
                    }
                }
            }
            rows.push(values);
            Ok(())
        })
        .map_err(|e| anyhow::anyhow!("Row callback error: {e}"))?;

    Ok(rows)
}

// ---------------------------------------------------------------------------
// Stats & helpers
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct FuzzerStats {
    statements_executed: usize,
    errors: usize,
    panics: usize,
    consistency_failures: usize,
    internal_errors: usize,
}

fn is_internal_error(msg: &str) -> bool {
    let msg_lower = msg.to_lowercase();
    msg_lower.contains("internal error")
        || msg_lower.contains("assertion failed")
        || msg_lower.contains("unreachable")
        || msg_lower.contains("unwrap() on none")
        || msg_lower.contains("called `option::unwrap()` on a `none`")
        || msg_lower.contains("index out of bounds")
        || msg_lower.contains("idxdelete: no matching index entry")
        // Parse errors during DML execution indicate a trigger compilation bug
        || msg_lower.contains("cannot resolve struct field")
        || msg_lower.contains("cannot resolve union variant")
        || msg_lower.contains("unknown variant")
}

fn write_sql_file(out_dir: &std::path::Path, statements: &[String]) -> Result<()> {
    let path = out_dir.join("custom_types_test.sql");
    let mut file = std::fs::File::create(&path)?;
    for sql in statements {
        writeln!(file, "{sql};")?;
    }
    tracing::info!(
        "Wrote {} statements to {}",
        statements.len(),
        path.display()
    );
    Ok(())
}

fn print_stats(stats: &FuzzerStats, seed: u64) {
    println!("\n--- Custom Types Fuzzer Results ---");
    println!("Seed:                  {seed}");
    println!("Statements executed:   {}", stats.statements_executed);
    println!("Execution errors:      {}", stats.errors);
    println!("Panics:                {}", stats.panics);
    println!("Consistency failures:  {}", stats.consistency_failures);
    println!("Internal errors:       {}", stats.internal_errors);
    if stats.panics == 0 && stats.consistency_failures == 0 && stats.internal_errors == 0 {
        println!("Status:                PASSED");
    } else {
        println!("Status:                FAILED");
    }
    println!("----------------------------------");
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let seed = args.seed.unwrap_or_else(|| rand::rng().random());
    tracing::info!(
        "Custom types fuzzer: seed={seed}, statements={}, tables={}",
        args.statements,
        args.tables,
    );

    let out_dir: PathBuf = "custom-types-fuzzer-output".into();
    if !out_dir.exists() {
        std::fs::create_dir_all(&out_dir)?;
    }

    let mut rng = StdRng::seed_from_u64(seed);

    // Open Turso with custom types enabled
    let io = Arc::new(MemorySimIO::new(seed));
    let turso_db = Database::open_file_with_flags(
        io,
        out_dir.join("custom_types_test.db").to_str().unwrap(),
        turso_core::OpenFlags::default(),
        turso_core::DatabaseOpts::default().with_custom_types(true),
        None,
    )?;
    let conn = turso_db.connect()?;

    let panic_context: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let mut executed_sql: Vec<String> = Vec::new();
    let mut stats = FuzzerStats::default();

    // -----------------------------------------------------------------------
    // Phase 1: Create type hierarchy
    // -----------------------------------------------------------------------
    let num_structs = rng.random_range(3..=6usize);
    let num_unions = rng.random_range(2..=3usize);
    let num_custom_types = rng.random_range(1..=3usize);
    let num_domains = rng.random_range(2..=4usize);
    tracing::info!(
        "Phase 1: Creating type hierarchy ({num_custom_types} custom types, {num_domains} domains, {num_structs} structs, {num_unions} unions)"
    );

    // Build TypeDefs incrementally: custom types → domains → structs → unions
    let mut types = TypeDefs::default();
    types.custom_types = gen_custom_type_defs(&mut rng, num_custom_types);
    types.domains = gen_domains(&mut rng, &types.custom_types, num_domains);
    types.structs = gen_structs(&mut rng, &types, num_structs);
    types.unions = gen_unions(&mut rng, &types, num_unions);

    // Execute type creation DDL in dependency order
    for ct in &types.custom_types {
        let sql = custom_type_def_sql(ct);
        if args.verbose {
            tracing::info!("DDL: {sql}");
        }
        execute_turso(&conn, &sql).context(format!("Failed: {sql}"))?;
        executed_sql.push(sql);
    }
    for d in &types.domains {
        let sql = domain_def_sql(d);
        if args.verbose {
            tracing::info!("DDL: {sql}");
        }
        execute_turso(&conn, &sql).context(format!("Failed: {sql}"))?;
        executed_sql.push(sql);
    }
    for s in &types.structs {
        let sql = struct_def_sql(s);
        if args.verbose {
            tracing::info!("DDL: {sql}");
        }
        execute_turso(&conn, &sql).context(format!("Failed: {sql}"))?;
        executed_sql.push(sql);
    }
    for u in &types.unions {
        let sql = union_def_sql(u);
        if args.verbose {
            tracing::info!("DDL: {sql}");
        }
        execute_turso(&conn, &sql).context(format!("Failed: {sql}"))?;
        executed_sql.push(sql);
    }

    // -----------------------------------------------------------------------
    // Phase 2: Create tables
    // -----------------------------------------------------------------------
    tracing::info!("Phase 2: Creating {} tables", args.tables);
    let mut tables = Vec::new();
    for i in 0..args.tables {
        let (sql, tdef) = gen_table(&mut rng, i, &types);
        if args.verbose {
            tracing::info!("DDL: {sql}");
        }
        execute_turso(&conn, &sql).context(format!("Failed: {sql}"))?;
        executed_sql.push(sql);
        tables.push(tdef);
    }

    // -----------------------------------------------------------------------
    // Phase 3: Create expression indexes
    // -----------------------------------------------------------------------
    tracing::info!("Phase 3: Creating expression indexes");
    let mut expr_indexes: Vec<ExprIndexDef> = Vec::new();
    let mut idx_counter = 0usize;

    for table in &tables {
        for (col_idx, kind) in &table.custom_type_columns {
            // 1-3 indexes per custom column
            let n_indexes = rng.random_range(1..=3usize);
            for _ in 0..n_indexes {
                if let Some((sql, eidx)) =
                    gen_expr_index(&mut rng, table, *col_idx, kind, &mut idx_counter, &types)
                {
                    if args.verbose {
                        tracing::info!("DDL: {sql}");
                    }
                    match execute_turso(&conn, &sql) {
                        Ok(_) => {
                            executed_sql.push(sql);
                            expr_indexes.push(eidx);
                        }
                        Err(e) => {
                            tracing::warn!("Index creation failed (ok): {e}");
                            executed_sql.push(format!("-- ERROR: {sql} ({e})"));
                        }
                    }
                }
            }
        }
    }
    tracing::info!("Created {} expression indexes", expr_indexes.len());

    // -----------------------------------------------------------------------
    // Phase 3.5: Triggers that decompose union columns with struct variants
    // -----------------------------------------------------------------------
    let mut trigger_counter = 0usize;
    let trigger_defs = gen_triggers(&mut rng, &tables, &types, &mut trigger_counter);
    tracing::info!(
        "Phase 3.5: Creating {} triggers on union/struct columns",
        trigger_defs.len()
    );
    let mut triggers: Vec<TriggerDef> = Vec::new();
    for (ddl_stmts, tdef) in trigger_defs {
        let mut ok = true;
        for ddl in &ddl_stmts {
            if args.verbose {
                tracing::info!("TRIGGER DDL: {ddl}");
            }
            match execute_turso(&conn, ddl) {
                Ok(_) => {
                    executed_sql.push(ddl.clone());
                    stats.statements_executed += 1;
                }
                Err(e) => {
                    tracing::warn!("Trigger DDL failed: {e} -- {ddl}");
                    ok = false;
                    break;
                }
            }
        }
        if ok {
            triggers.push(tdef);
        }
    }
    tracing::info!("Created {} triggers", triggers.len());

    // -----------------------------------------------------------------------
    // Phase 4: Seed data
    // -----------------------------------------------------------------------
    let rows_per_table = rng.random_range(10..=30usize);
    tracing::info!("Phase 4: Inserting {rows_per_table} rows per table");
    let mut max_ids: Vec<i64> = Vec::new();

    for table in &tables {
        let mut max_id = 0i64;
        for row in 0..rows_per_table {
            let row_id = (row + 1) as i64;
            let sql = gen_insert(&mut rng, table, row_id, &types);
            if args.verbose {
                tracing::info!("INSERT: {sql}");
            }
            match execute_turso(&conn, &sql) {
                Ok(_) => {
                    executed_sql.push(sql);
                    stats.statements_executed += 1;
                    max_id = row_id;
                }
                Err(e) => {
                    stats.errors += 1;
                    executed_sql.push(format!("-- ERROR: {sql} ({e})"));
                    if is_internal_error(&e.to_string()) {
                        stats.internal_errors += 1;
                        tracing::error!("INTERNAL ERROR during INSERT: {e}");
                        tracing::error!("SQL: {sql}");
                    }
                }
            }
        }
        max_ids.push(max_id);
    }

    // -----------------------------------------------------------------------
    // Phase 5: Mixed DML workload (aggressive)
    // -----------------------------------------------------------------------
    tracing::info!("Phase 5: Executing {} DML statements", args.statements);
    let mut in_transaction = false;
    let mut has_savepoint = false;
    let mut variant_tracker: VariantTracker = HashMap::new();
    // Track which indexes are currently active (for DROP/recreate)
    let mut active_index_indices: Vec<usize> = (0..expr_indexes.len()).collect();
    let mut alt_counter = 0usize;
    let mut pidx_counter = 0usize;
    let mut ctas_counter = 0usize;
    let mut view_counter = 0usize;

    // Helper closure: execute a single SQL with panic capture, stats tracking, etc.
    // We use a macro to avoid borrow issues with the closure capturing &mut stats etc.
    macro_rules! exec_one {
        ($sql:expr, $i:expr) => {{
            let sql: String = $sql;
            if args.verbose {
                tracing::info!("Statement {}: {sql}", $i);
            }

            let ctx_clone = Arc::clone(&panic_context);
            let prev_hook = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |info| {
                let bt = std::backtrace::Backtrace::force_capture();
                *ctx_clone.lock() = Some(format!("{info}\n{bt}"));
            }));

            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                execute_turso(&conn, &sql)
            }));

            std::panic::set_hook(prev_hook);

            match result {
                Ok(Ok(_)) => {
                    stats.statements_executed += 1;
                    executed_sql.push(sql.clone());
                }
                Ok(Err(e)) => {
                    stats.errors += 1;
                    let err_msg = e.to_string();
                    if is_internal_error(&err_msg) {
                        stats.internal_errors += 1;
                        tracing::error!("INTERNAL ERROR at statement {}: {err_msg}", $i);
                        tracing::error!("SQL: {sql}");
                    } else if args.verbose {
                        tracing::debug!("Statement {} error: {e}", $i);
                    }
                    executed_sql.push(format!("-- ERROR: {sql} ({e})"));
                }
                Err(panic) => {
                    let msg = panic
                        .downcast_ref::<&str>()
                        .map(|s| s.to_string())
                        .or_else(|| panic.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "Unknown panic".to_string());
                    let bt = panic_context.lock().take().unwrap_or_default();
                    stats.panics += 1;
                    tracing::error!("PANIC at statement {}: {msg}", $i);
                    tracing::error!("Panicking SQL: {sql}");
                    tracing::error!("Backtrace:\n{bt}");
                    executed_sql.push(format!("-- PANIC: {sql}"));
                    write_sql_file(&out_dir, &executed_sql).expect("Failed to write SQL file");
                    print_stats(&stats, seed);
                    anyhow::bail!("Panic during statement {}: {msg}\n  SQL: {sql}\n{bt}", $i);
                }
            }
        }};
    }

    let mut i = 0usize;
    while i < args.statements {
        let table_idx = rng.random_range(0..tables.len());
        let table = &tables[table_idx];
        let max_id = max_ids[table_idx].max(1);

        // Weighted choice (280 range for finer control):
        //   INSERT=20, UPDATE=44, DELETE=12, SELECT_EXPR=24, SELECT_JOIN=12,
        //   SELECT_CUSTOM=16, UPSERT=12, INSERT_SELECT=4, TRANSACTION=14,
        //   ROW_TORTURE=10, VARIANT_SWITCH=8, SELF_REF_UPDATE=6,
        //   NULL_TRANSITION=6, MULTI_ROW_DML=6, DELETE_REINSERT=3,
        //   INSERT_WITH_DEFAULTS=2, DROP_INDEX_RECREATE=7,
        //   CTE=7, WINDOW_FN=6, COMPOUND_SELECT=6, SCALAR_SUBQUERY=5,
        //   MULTI_JOIN=5, COMPARISON_OPS=5, INSERT_OR_IGNORE=3,
        //   DELETE_CORRELATED=3, UPDATE_SUBQUERY=3, INSERT_SELECT_CROSS=4,
        //   PARTIAL_INDEX=2, CTAS=2, VIEW=1, SAVEPOINT_ENHANCED=1
        let action = rng.random_range(0..280u32);

        if action < 20 {
            // INSERT
            max_ids[table_idx] += 1;
            let sql = gen_insert(&mut rng, table, max_ids[table_idx], &types);
            exec_one!(sql, i);
        } else if action < 64 {
            // UPDATE
            let sql = gen_update(&mut rng, table, max_id, &types, &expr_indexes);
            exec_one!(sql, i);
        } else if action < 76 {
            // DELETE
            let sql = gen_delete(&mut rng, table, max_id, &expr_indexes, &types);
            exec_one!(sql, i);
        } else if action < 100 {
            // SELECT using expression index
            let sql = gen_select_expr_index(&mut rng, &expr_indexes, &tables, &types)
                .unwrap_or_else(|| format!("SELECT COUNT(*) FROM {}", table.name));
            exec_one!(sql, i);
        } else if action < 112 {
            // JOIN between tables
            let sql = gen_join_select(&mut rng, &tables, &expr_indexes)
                .unwrap_or_else(|| format!("SELECT COUNT(*) FROM {}", table.name));
            exec_one!(sql, i);
        } else if action < 128 {
            // Custom type function SELECT
            let sql = gen_custom_type_select(&mut rng, table, &types)
                .unwrap_or_else(|| format!("SELECT COUNT(*) FROM {}", table.name));
            exec_one!(sql, i);
        } else if action < 140 {
            // UPSERT (with multi-column DO UPDATE)
            let sql = gen_upsert(&mut rng, table, max_id, &types);
            exec_one!(sql, i);
        } else if action < 144 {
            // INSERT ... SELECT
            let sql = gen_insert_select(&mut rng, &tables, &max_ids).unwrap_or_else(|| {
                max_ids[table_idx] += 1;
                gen_insert(&mut rng, table, max_ids[table_idx], &types)
            });
            exec_one!(sql, i);
        } else if action < 158 {
            // Transaction / savepoint (with ROLLBACK)
            let txn_sql = gen_transaction_op(&mut rng);
            let sql = match txn_sql {
                "BEGIN" if !in_transaction => {
                    in_transaction = true;
                    has_savepoint = false;
                    txn_sql.to_string()
                }
                "COMMIT" if in_transaction => {
                    in_transaction = false;
                    has_savepoint = false;
                    txn_sql.to_string()
                }
                "SAVEPOINT sp1" if in_transaction => {
                    has_savepoint = true;
                    txn_sql.to_string()
                }
                "RELEASE sp1" if in_transaction && has_savepoint => {
                    has_savepoint = false;
                    txn_sql.to_string()
                }
                "ROLLBACK TO sp1" if in_transaction && has_savepoint => {
                    // ROLLBACK TO keeps the savepoint active
                    txn_sql.to_string()
                }
                "ROLLBACK" if in_transaction => {
                    in_transaction = false;
                    has_savepoint = false;
                    txn_sql.to_string()
                }
                _ => {
                    // Invalid txn state, do a SELECT instead
                    format!("SELECT COUNT(*) FROM {}", table.name)
                }
            };
            let is_rollback = sql.starts_with("ROLLBACK");
            exec_one!(sql, i);
            // After ROLLBACK, run an immediate consistency check
            if is_rollback && !in_transaction {
                run_consistency_checks(
                    &conn,
                    &tables,
                    &expr_indexes,
                    &triggers,
                    &types,
                    &mut stats,
                    &mut executed_sql,
                    args.verbose,
                )?;
            }
        } else if action < 168 {
            // Row torture: 3-5 sequential mutations on same row
            let stmts = gen_row_torture(
                &mut rng,
                table,
                table_idx,
                &mut max_ids[table_idx],
                &types,
                &mut variant_tracker,
            );
            for s in stmts {
                exec_one!(s, i);
                i += 1;
                if i >= args.statements {
                    break;
                }
            }
            // Don't double-increment i at the bottom
            i = i.wrapping_sub(1);
        } else if action < 176 {
            // Variant-switching update
            let sql = gen_variant_switch_update(
                &mut rng,
                table,
                table_idx,
                max_id,
                &types,
                &mut variant_tracker,
            )
            .unwrap_or_else(|| gen_update(&mut rng, table, max_id, &types, &expr_indexes));
            exec_one!(sql, i);
        } else if action < 182 {
            // Self-referencing update (no-op: SET x0 = x0)
            let sql = gen_self_ref_update(&mut rng, table, max_id)
                .unwrap_or_else(|| format!("SELECT COUNT(*) FROM {}", table.name));
            exec_one!(sql, i);
        } else if action < 188 {
            // NULL↔Value transition update
            let sql = gen_null_transition_update(&mut rng, table, max_id, &types)
                .unwrap_or_else(|| format!("SELECT COUNT(*) FROM {}", table.name));
            exec_one!(sql, i);
        } else if action < 194 {
            // Multi-row DML (DELETE WHERE tag = ... or UPDATE WHERE id > N)
            let sql = gen_multi_row_dml(&mut rng, table, max_id, &types)
                .unwrap_or_else(|| format!("SELECT COUNT(*) FROM {}", table.name));
            exec_one!(sql, i);
        } else if action < 197 {
            // DELETE + re-INSERT cycle (same id, different value)
            let stmts = gen_delete_reinsert(&mut rng, table, max_id, &types);
            for s in stmts {
                exec_one!(s, i);
                i += 1;
                if i >= args.statements {
                    break;
                }
            }
            i = i.wrapping_sub(1);
        } else if action < 199 {
            // INSERT with DEFAULT keyword
            max_ids[table_idx] += 1;
            let sql = gen_insert_with_defaults(&mut rng, table, max_ids[table_idx], &types)
                .unwrap_or_else(|| gen_insert(&mut rng, table, max_ids[table_idx], &types));
            exec_one!(sql, i);
        } else if action < 206 {
            // Array subscript update
            let sql = gen_array_subscript_update(&mut rng, table, max_id)
                .unwrap_or_else(|| format!("SELECT COUNT(*) FROM {}", table.name));
            exec_one!(sql, i);
        } else if action < 214 {
            // Array mutation (append/prepend/remove/cat/slice)
            let sql = gen_array_mutation(&mut rng, table, max_id)
                .unwrap_or_else(|| format!("SELECT COUNT(*) FROM {}", table.name));
            exec_one!(sql, i);
        } else if action < 220 {
            // Array torture: 3-5 sequential array mutations on same row
            let stmts = gen_array_torture(&mut rng, table, max_id);
            if stmts.is_empty() {
                let sql = format!("SELECT COUNT(*) FROM {}", table.name);
                exec_one!(sql, i);
            } else {
                for s in stmts {
                    exec_one!(s, i);
                    i += 1;
                    if i >= args.statements {
                        break;
                    }
                }
                i = i.wrapping_sub(1);
            }
        } else if action < 227 && !in_transaction {
            // ALTER TABLE ADD COLUMN
            let table_mut = &mut tables[table_idx];
            let sql = gen_alter_add_column(&mut rng, table_mut, &types, &mut alt_counter);
            exec_one!(sql, i);
        } else if action < 234 {
            // CTE query
            let sql = gen_cte_select(&mut rng, &tables, &expr_indexes, &types)
                .unwrap_or_else(|| format!("SELECT COUNT(*) FROM {}", table.name));
            exec_one!(sql, i);
        } else if action < 240 {
            // Window function query
            let sql = gen_window_function_select(&mut rng, table, &types)
                .unwrap_or_else(|| format!("SELECT COUNT(*) FROM {}", table.name));
            exec_one!(sql, i);
        } else if action < 246 {
            // UNION/EXCEPT/INTERSECT
            let sql = gen_compound_select(&mut rng, &tables)
                .unwrap_or_else(|| format!("SELECT COUNT(*) FROM {}", table.name));
            exec_one!(sql, i);
        } else if action < 251 {
            // Scalar subquery in WHERE
            let sql = gen_scalar_subquery_select(&mut rng, &tables)
                .unwrap_or_else(|| format!("SELECT COUNT(*) FROM {}", table.name));
            exec_one!(sql, i);
        } else if action < 256 {
            // Multi-table JOIN (3+)
            let sql = gen_multi_join_select(&mut rng, &tables, &types)
                .unwrap_or_else(|| format!("SELECT COUNT(*) FROM {}", table.name));
            exec_one!(sql, i);
        } else if action < 261 {
            // Comparison ops on custom type columns
            let sql = gen_comparison_select(&mut rng, table, &types)
                .unwrap_or_else(|| format!("SELECT COUNT(*) FROM {}", table.name));
            exec_one!(sql, i);
        } else if action < 265 {
            // INSERT...SELECT cross-table
            if let Some((sql, dst_idx, bump)) = gen_insert_select_cross(&mut rng, &tables, &max_ids)
            {
                exec_one!(sql, i);
                max_ids[dst_idx] += bump;
            } else {
                max_ids[table_idx] += 1;
                let sql = gen_insert(&mut rng, table, max_ids[table_idx], &types);
                exec_one!(sql, i);
            }
        } else if action < 268 {
            // DELETE with correlated subquery
            let sql = gen_delete_correlated(&mut rng, &tables)
                .unwrap_or_else(|| format!("SELECT COUNT(*) FROM {}", table.name));
            exec_one!(sql, i);
        } else if action < 271 {
            // UPDATE with subquery in SET
            let sql = gen_update_subquery(&mut rng, &tables, max_id)
                .unwrap_or_else(|| format!("SELECT COUNT(*) FROM {}", table.name));
            exec_one!(sql, i);
        } else if action < 274 {
            // INSERT OR IGNORE
            let (sql, is_new) = gen_insert_or_ignore(&mut rng, table, max_id, &types);
            exec_one!(sql, i);
            if is_new {
                max_ids[table_idx] += 1;
            }
        } else if action < 276 && !in_transaction {
            // Partial index create
            let sql = gen_partial_index(&mut rng, table, &types, &mut pidx_counter)
                .unwrap_or_else(|| format!("SELECT COUNT(*) FROM {}", table.name));
            exec_one!(sql, i);
        } else if action < 278 && !in_transaction {
            // CREATE TABLE AS SELECT (ephemeral)
            if let Some(stmts) = gen_ctas(&mut rng, &tables, &mut ctas_counter) {
                for s in stmts {
                    exec_one!(s, i);
                    i += 1;
                    if i >= args.statements {
                        break;
                    }
                }
                i = i.wrapping_sub(1);
            } else {
                let sql = format!("SELECT COUNT(*) FROM {}", table.name);
                exec_one!(sql, i);
            }
        } else if action < 279 && !in_transaction {
            // View create + query + drop
            if let Some(stmts) = gen_view_and_query(&mut rng, &tables, &types, &mut view_counter) {
                for s in stmts {
                    exec_one!(s, i);
                    i += 1;
                    if i >= args.statements {
                        break;
                    }
                }
                i = i.wrapping_sub(1);
            } else {
                let sql = format!("SELECT COUNT(*) FROM {}", table.name);
                exec_one!(sql, i);
            }
        } else if action < 280 {
            // Enhanced SAVEPOINT/ROLLBACK with max_ids snapshot
            let txn_sql = gen_transaction_op(&mut rng);
            let sql = match txn_sql {
                "BEGIN" if !in_transaction => {
                    in_transaction = true;
                    has_savepoint = false;
                    txn_sql.to_string()
                }
                "COMMIT" if in_transaction => {
                    in_transaction = false;
                    has_savepoint = false;
                    txn_sql.to_string()
                }
                "SAVEPOINT sp1" if in_transaction => {
                    has_savepoint = true;
                    txn_sql.to_string()
                }
                "RELEASE sp1" if in_transaction && has_savepoint => {
                    has_savepoint = false;
                    txn_sql.to_string()
                }
                "ROLLBACK TO sp1" if in_transaction && has_savepoint => txn_sql.to_string(),
                "ROLLBACK" if in_transaction => {
                    in_transaction = false;
                    has_savepoint = false;
                    txn_sql.to_string()
                }
                _ => format!("SELECT COUNT(*) FROM {}", table.name),
            };
            exec_one!(sql, i);
        } else {
            // DROP INDEX + recreate
            if !active_index_indices.is_empty() && !in_transaction {
                let pick = rng.random_range(0..active_index_indices.len());
                let eidx_pos = active_index_indices[pick];
                let eidx = &expr_indexes[eidx_pos];

                // DROP the index
                let drop_sql = format!("DROP INDEX {}", eidx.index_name);
                exec_one!(drop_sql, i);
                active_index_indices.remove(pick);

                // Do a few DML ops without the index
                let n_dml = rng.random_range(2..=5u32);
                for _ in 0..n_dml {
                    i += 1;
                    if i >= args.statements {
                        break;
                    }
                    max_ids[table_idx] += 1;
                    let s = gen_insert(&mut rng, table, max_ids[table_idx], &types);
                    exec_one!(s, i);
                }

                // Recreate the index
                i += 1;
                if i < args.statements {
                    let recreate_sql = format!(
                        "CREATE INDEX {} ON {}({})",
                        eidx.index_name, eidx.table_name, eidx.expr_sql
                    );
                    exec_one!(recreate_sql, i);
                    active_index_indices.push(eidx_pos);
                }
            } else {
                let sql = format!("SELECT COUNT(*) FROM {}", table.name);
                exec_one!(sql, i);
            }
        }

        i += 1;

        // Periodic consistency checks (only outside transactions)
        if i % 50 == 0 && !in_transaction {
            run_consistency_checks(
                &conn,
                &tables,
                &expr_indexes,
                &triggers,
                &types,
                &mut stats,
                &mut executed_sql,
                args.verbose,
            )?;
        }
    }

    // Make sure we end any open transaction
    if in_transaction {
        let _ = execute_turso(&conn, "COMMIT");
    }

    // -----------------------------------------------------------------------
    // Phase 6: Final checks
    // -----------------------------------------------------------------------
    tracing::info!("Phase 6: Final consistency checks");
    run_consistency_checks(
        &conn,
        &tables,
        &expr_indexes,
        &triggers,
        &types,
        &mut stats,
        &mut executed_sql,
        args.verbose,
    )?;

    // Integrity check
    let integrity_sql = "PRAGMA integrity_check";
    executed_sql.push(integrity_sql.to_string());
    match execute_turso_rows(&conn, integrity_sql) {
        Ok(rows) => {
            if rows.len() == 1 && rows[0].len() == 1 && rows[0][0] == "ok" {
                tracing::info!("PRAGMA integrity_check: ok");
            } else {
                stats.consistency_failures += 1;
                tracing::error!("PRAGMA integrity_check failed: {:?}", rows);
            }
        }
        Err(e) => {
            stats.consistency_failures += 1;
            tracing::error!("PRAGMA integrity_check error: {e}");
        }
    }

    write_sql_file(&out_dir, &executed_sql)?;
    print_stats(&stats, seed);

    if stats.panics > 0 || stats.consistency_failures > 0 || stats.internal_errors > 0 {
        anyhow::bail!(
            "Custom types fuzzer found {} panics, {} consistency failures, {} internal errors",
            stats.panics,
            stats.consistency_failures,
            stats.internal_errors,
        );
    }
    tracing::info!("Custom types fuzzer completed successfully");
    std::fs::remove_dir_all(&out_dir)?;
    Ok(())
}
