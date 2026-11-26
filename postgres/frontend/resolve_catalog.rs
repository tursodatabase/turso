//! The rows of the frontend's catalog tables, as the resolver reads them.

use crate::casts::{self, PG_CASTS};
use crate::functions::format_type;
use crate::operators::OPERATORS;
use crate::pg_type::BUILT_IN_TYPES;
use crate::procs::{Proc, PROCS};
use crate::resolve::{
    CastMethod, CastRow, Catalog, OperatorRow, ProcRow, RangeRow, TypeKind, TypeRow,
};
use rustc_hash::FxHashMap;
use std::sync::LazyLock;

/// `pg_type`, `pg_cast`, `pg_proc` and `pg_operator` of the frontend.
pub(crate) struct FrontendCatalog;

impl FrontendCatalog {
    pub(crate) fn has_function(name: &str) -> bool {
        TABLES.procs.contains_key(name)
    }

    /// The array type over the type `element`.
    pub(crate) fn array_type(element: i64) -> Option<i64> {
        let array = TABLES.types.get(&element)?.row.array_type;
        (array != 0).then_some(array)
    }

    /// The `typname` of the type `oid`, or of its element type for an array
    /// type, and whether it is an array type. `None` for a pseudo-type and for
    /// a type that has no row here.
    pub(crate) fn type_name(oid: i64) -> Option<(&'static str, bool)> {
        let ty = TABLES.types.get(&oid)?;
        if ty.row.kind == TypeKind::Pseudo {
            return None;
        }
        if ty.row.array_element != 0 {
            let (element, _) = Self::type_name(ty.row.array_element)?;
            return Some((element, true));
        }
        Some((ty.name.as_str(), false))
    }
}

impl Catalog for FrontendCatalog {
    fn pg_type(&self, oid: i64) -> Option<TypeRow> {
        TABLES.types.get(&oid).map(|ty| ty.row)
    }

    fn pg_type_oid(&self, name: &str) -> Option<i64> {
        TABLES.type_oids.get(name).copied()
    }

    fn pg_cast(&self, source: i64, target: i64) -> Option<CastRow> {
        TABLES.casts.get(&(source, target)).copied()
    }

    fn pg_range(&self, _range_type: i64) -> Option<RangeRow> {
        None
    }

    fn pg_range_of_multirange(&self, _multirange_type: i64) -> Option<RangeRow> {
        None
    }

    fn pg_proc(&self, name: &str) -> Vec<ProcRow<'_>> {
        TABLES
            .procs
            .get(name)
            .map(|procs| procs.iter().map(|proc| proc_row(proc)).collect())
            .unwrap_or_default()
    }

    fn pg_operator(&self, name: &str) -> Vec<OperatorRow> {
        TABLES.operators.get(name).cloned().unwrap_or_default()
    }

    fn format_type_name(&self, oid: i64) -> String {
        let Some(ty) = TABLES.types.get(&oid) else {
            return "???".to_string();
        };
        if ty.row.array_element != 0 {
            return format!("{}[]", self.format_type_name(ty.row.array_element));
        }
        match ty.name.as_str() {
            "any" => "\"any\"".to_string(),
            "unknown" => "unknown".to_string(),
            name => match format_type(oid, -1).as_str() {
                "unknown" => name.to_string(),
                formatted => formatted.to_string(),
            },
        }
    }
}

struct Tables {
    types: FxHashMap<i64, Type>,
    type_oids: FxHashMap<String, i64>,
    casts: FxHashMap<(i64, i64), CastRow>,
    procs: FxHashMap<&'static str, Vec<&'static Proc>>,
    operators: FxHashMap<&'static str, Vec<OperatorRow>>,
}

struct Type {
    name: String,
    row: TypeRow,
}

static TABLES: LazyLock<Tables> = LazyLock::new(|| {
    let base = BUILT_IN_TYPES.iter().map(|ty| {
        let row = TypeRow {
            category: ty.category.chars().next().expect("a typcategory letter"),
            is_preferred: ty.is_preferred,
            kind: TypeKind::Base,
            array_element: 0,
            array_type: ty.array_oid,
        };
        (ty.oid, ty.name.to_string(), row)
    });
    let arrays = BUILT_IN_TYPES
        .iter()
        .filter(|ty| ty.array_oid != 0)
        .map(|ty| {
            let row = TypeRow {
                category: 'A',
                is_preferred: false,
                kind: TypeKind::Base,
                array_element: ty.oid,
                array_type: 0,
            };
            (ty.array_oid, format!("_{}", ty.name), row)
        });
    let pseudo = PSEUDO_TYPES.iter().map(|&(oid, name)| {
        let row = TypeRow {
            category: if name == "unknown" { 'X' } else { 'P' },
            is_preferred: false,
            kind: TypeKind::Pseudo,
            array_element: 0,
            array_type: 0,
        };
        (oid, name.to_string(), row)
    });
    let all_types: Vec<_> = base.chain(arrays).chain(pseudo).collect();
    let mut procs: FxHashMap<&'static str, Vec<&'static Proc>> = FxHashMap::default();
    for proc in PROCS {
        procs.entry(proc.name).or_default().push(proc);
    }
    let mut operators: FxHashMap<&'static str, Vec<OperatorRow>> = FxHashMap::default();
    for operator in OPERATORS {
        operators
            .entry(operator.name)
            .or_default()
            .push(OperatorRow {
                oid: operator.oid,
                kind: if operator.left == 0 { 'l' } else { 'b' },
                left: operator.left,
                right: operator.right,
                result: operator.result,
                function: 0,
            });
    }
    Tables {
        types: all_types
            .iter()
            .map(|(oid, name, row)| {
                let ty = Type {
                    name: name.clone(),
                    row: *row,
                };
                (*oid, ty)
            })
            .collect(),
        type_oids: all_types
            .iter()
            .map(|(oid, name, _)| (name.clone(), *oid))
            .collect(),
        casts: PG_CASTS
            .iter()
            .map(|cast| {
                let method = match cast.method {
                    casts::CastMethod::Function => CastMethod::Function(cast.function),
                    casts::CastMethod::InputOutput => CastMethod::InputOutput,
                    casts::CastMethod::Binary => CastMethod::Binary,
                };
                let row = CastRow {
                    context: cast.context,
                    method,
                };
                ((cast.source, cast.target), row)
            })
            .collect(),
        procs,
        operators,
    }
});

fn proc_row(proc: &'static Proc) -> ProcRow<'static> {
    ProcRow {
        oid: proc.oid,
        kind: proc.kind.chars().next().expect("a prokind letter"),
        arg_types: proc.arg_types,
        result: proc.result,
        variadic: proc.variadic_type,
        default_count: 0,
    }
}

/// The pseudo-types of PostgreSQL that a row of `PROCS` or the resolver names.
const PSEUDO_TYPES: &[(i64, &str)] = &[
    (705, "unknown"),
    (2249, "record"),
    (2275, "cstring"),
    (2276, "any"),
    (2277, "anyarray"),
    (2278, "void"),
    (2281, "internal"),
    (2283, "anyelement"),
    (2776, "anynonarray"),
    (3500, "anyenum"),
    (3831, "anyrange"),
    (4537, "anymultirange"),
    (4538, "anycompatiblemultirange"),
    (5077, "anycompatible"),
    (5078, "anycompatiblearray"),
    (5079, "anycompatiblenonarray"),
    (5080, "anycompatiblerange"),
];
