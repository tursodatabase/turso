// Translated from PostgreSQL 18.6, src/backend/utils/cache/lsyscache.c, src/backend/parser/parse_type.c and src/include/catalog/pg_type.h. See licenses/postgres-license.md.

use super::Reason;
use turso_core::dialect::CastContext;

pub(crate) const INVALID_OID: i64 = 0;
pub(crate) const INT2VECTOROID: i64 = 22;
pub(crate) const TEXTOID: i64 = 25;
pub(crate) const OIDVECTOROID: i64 = 30;
pub(crate) const UNKNOWNOID: i64 = 705;
pub(crate) const TEXTARRAYOID: i64 = 1009;
pub(crate) const RECORDOID: i64 = 2249;
pub(crate) const ANYOID: i64 = 2276;
pub(crate) const ANYARRAYOID: i64 = 2277;
pub(crate) const INTERNALOID: i64 = 2281;
pub(crate) const ANYELEMENTOID: i64 = 2283;
pub(crate) const RECORDARRAYOID: i64 = 2287;
pub(crate) const ANYNONARRAYOID: i64 = 2776;
pub(crate) const ANYENUMOID: i64 = 3500;
pub(crate) const ANYRANGEOID: i64 = 3831;
pub(crate) const ANYMULTIRANGEOID: i64 = 4537;
pub(crate) const ANYCOMPATIBLEMULTIRANGEOID: i64 = 4538;
pub(crate) const ANYCOMPATIBLEOID: i64 = 5077;
pub(crate) const ANYCOMPATIBLEARRAYOID: i64 = 5078;
pub(crate) const ANYCOMPATIBLENONARRAYOID: i64 = 5079;
pub(crate) const ANYCOMPATIBLERANGEOID: i64 = 5080;

pub(crate) const TYPCATEGORY_INVALID: char = '\0';
pub(crate) const TYPCATEGORY_STRING: char = 'S';

pub(crate) trait Catalog {
    fn pg_type(&self, oid: i64) -> Option<TypeRow>;

    /// The oid of the `pg_type` row with this `typname`, unless the type is only a shell.
    fn pg_type_oid(&self, name: &str) -> Option<i64>;

    fn pg_cast(&self, source: i64, target: i64) -> Option<CastRow>;

    /// The `pg_range` row of a range type.
    fn pg_range(&self, range_type: i64) -> Option<RangeRow>;

    /// The `pg_range` row of the range type that has this multirange type.
    fn pg_range_of_multirange(&self, multirange_type: i64) -> Option<RangeRow>;

    fn pg_proc(&self, name: &str) -> Vec<ProcRow<'_>>;

    fn pg_operator(&self, name: &str) -> Vec<OperatorRow>;

    /// The name that PostgreSQL's `format_type` gives the type.
    fn format_type_name(&self, oid: i64) -> String;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TypeRow {
    pub category: char,
    pub is_preferred: bool,
    pub kind: TypeKind,
    /// `typelem` when the type subscripts as an array, 0 for any other type.
    pub array_element: i64,
    /// `typarray`: the array type over this type, 0 without one.
    pub array_type: i64,
}

/// `pg_type.typtype`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TypeKind {
    Base,
    Composite,
    Domain,
    Enum,
    Pseudo,
    Range,
    Multirange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CastRow {
    pub context: CastContext,
    pub method: CastMethod,
}

/// `pg_cast.castmethod`, with `castfunc` for a cast that calls a function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CastMethod {
    Function(i64),
    InputOutput,
    Binary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RangeRow {
    pub range_type: i64,
    pub subtype: i64,
    pub multirange_type: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProcRow<'a> {
    pub oid: i64,
    /// `f` for a function, `a` for an aggregate.
    pub kind: char,
    pub arg_types: &'a [i64],
    pub result: i64,
    /// The element type of a variadic last parameter, 0 without one.
    pub variadic: i64,
    pub default_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OperatorRow {
    pub oid: i64,
    /// `b` for an operator between two operands, `l` for one before its operand.
    pub kind: char,
    pub left: i64,
    pub right: i64,
    pub result: i64,
    pub function: i64,
}

pub(crate) fn get_base_type(catalog: &dyn Catalog, typid: i64) -> Result<i64, Reason> {
    let typ_tup = catalog.pg_type(typid).ok_or(Reason::MissingType)?;
    if typ_tup.kind != TypeKind::Domain {
        return Ok(typid);
    }
    Err(Reason::Domain)
}

pub(crate) fn get_typtype(catalog: &dyn Catalog, typid: i64) -> Option<TypeKind> {
    catalog.pg_type(typid).map(|typtup| typtup.kind)
}

pub(crate) fn type_is_enum(catalog: &dyn Catalog, typid: i64) -> bool {
    get_typtype(catalog, typid) == Some(TypeKind::Enum)
}

pub(crate) fn type_is_range(catalog: &dyn Catalog, typid: i64) -> bool {
    get_typtype(catalog, typid) == Some(TypeKind::Range)
}

pub(crate) fn type_is_multirange(catalog: &dyn Catalog, typid: i64) -> bool {
    get_typtype(catalog, typid) == Some(TypeKind::Multirange)
}

pub(crate) fn get_type_category_preferred(
    catalog: &dyn Catalog,
    typid: i64,
) -> Result<(char, bool), Reason> {
    let typtup = catalog.pg_type(typid).ok_or(Reason::MissingType)?;
    Ok((typtup.category, typtup.is_preferred))
}

pub(crate) fn type_is_array(catalog: &dyn Catalog, typid: i64) -> bool {
    get_element_type(catalog, typid) != INVALID_OID
}

pub(crate) fn get_element_type(catalog: &dyn Catalog, typid: i64) -> i64 {
    match catalog.pg_type(typid) {
        Some(typtup) => typtup.array_element,
        None => INVALID_OID,
    }
}

pub(crate) fn get_array_type(catalog: &dyn Catalog, typid: i64) -> i64 {
    match catalog.pg_type(typid) {
        Some(tp) => tp.array_type,
        None => INVALID_OID,
    }
}

pub(crate) fn type_is_array_domain(catalog: &dyn Catalog, typid: i64) -> Result<bool, Reason> {
    Ok(get_base_element_type(catalog, typid)? != INVALID_OID)
}

pub(crate) fn get_base_element_type(catalog: &dyn Catalog, typid: i64) -> Result<i64, Reason> {
    let Some(typ_tup) = catalog.pg_type(typid) else {
        return Ok(INVALID_OID);
    };
    if typ_tup.kind != TypeKind::Domain {
        return Ok(typ_tup.array_element);
    }
    Err(Reason::Domain)
}

pub(crate) fn get_range_subtype(catalog: &dyn Catalog, range_oid: i64) -> Result<i64, Reason> {
    match catalog.pg_range(range_oid) {
        Some(rngtup) => Ok(rngtup.subtype),
        None if type_is_range(catalog, range_oid) => Err(Reason::Range),
        None => Ok(INVALID_OID),
    }
}

pub(crate) fn get_range_multirange(catalog: &dyn Catalog, range_oid: i64) -> Result<i64, Reason> {
    match catalog.pg_range(range_oid) {
        Some(rngtup) => Ok(rngtup.multirange_type),
        None if type_is_range(catalog, range_oid) => Err(Reason::Range),
        None => Ok(INVALID_OID),
    }
}

pub(crate) fn get_multirange_range(
    catalog: &dyn Catalog,
    multirange_oid: i64,
) -> Result<i64, Reason> {
    match catalog.pg_range_of_multirange(multirange_oid) {
        Some(rngtup) => Ok(rngtup.range_type),
        None if type_is_multirange(catalog, multirange_oid) => Err(Reason::Range),
        None => Ok(INVALID_OID),
    }
}

pub(crate) fn is_complex(catalog: &dyn Catalog, type_id: i64) -> Result<bool, Reason> {
    let type_tuple = catalog.pg_type(type_id).ok_or(Reason::MissingType)?;
    if type_tuple.kind == TypeKind::Domain {
        return Err(Reason::Domain);
    }
    Ok(type_tuple.kind == TypeKind::Composite)
}

pub(crate) fn typeid_type_is_composite(
    catalog: &dyn Catalog,
    type_id: i64,
) -> Result<bool, Reason> {
    let type_tuple = catalog.pg_type(type_id).ok_or(Reason::MissingType)?;
    Ok(type_tuple.kind == TypeKind::Composite)
}

pub(crate) fn is_polymorphic_type(typid: i64) -> bool {
    is_polymorphic_type_family1(typid) || is_polymorphic_type_family2(typid)
}

pub(crate) fn is_polymorphic_type_family1(typid: i64) -> bool {
    typid == ANYELEMENTOID
        || typid == ANYARRAYOID
        || typid == ANYNONARRAYOID
        || typid == ANYENUMOID
        || typid == ANYRANGEOID
        || typid == ANYMULTIRANGEOID
}

fn is_polymorphic_type_family2(typid: i64) -> bool {
    typid == ANYCOMPATIBLEOID
        || typid == ANYCOMPATIBLEARRAYOID
        || typid == ANYCOMPATIBLENONARRAYOID
        || typid == ANYCOMPATIBLERANGEOID
        || typid == ANYCOMPATIBLEMULTIRANGEOID
}

#[cfg(test)]
pub(crate) mod fixtures {
    use super::*;
    use rustc_hash::FxHashMap;
    use serde_json::Value;
    use std::sync::LazyLock;

    pub(crate) struct FixtureCatalog {
        types: FxHashMap<i64, FixtureType>,
        types_by_name: FxHashMap<String, i64>,
        casts: FxHashMap<(i64, i64), CastRow>,
        ranges: Vec<RangeRow>,
        procs: Vec<FixtureProc>,
        operators: Vec<(String, OperatorRow)>,
    }

    struct FixtureType {
        name: String,
        row: TypeRow,
    }

    pub(crate) struct FixtureProc {
        pub oid: i64,
        pub name: String,
        pub kind: char,
        pub arg_types: Vec<i64>,
        pub result: i64,
        pub variadic: i64,
        pub default_count: usize,
    }

    static FIXTURES: LazyLock<FixtureCatalog> = LazyLock::new(|| FixtureCatalog {
        types: rows(include_str!("testdata/pg_type.json"))
            .iter()
            .map(|row| (int(row, "oid"), fixture_type(row)))
            .collect(),
        types_by_name: rows(include_str!("testdata/pg_type.json"))
            .iter()
            .map(|row| (text(row, "typname").to_string(), int(row, "oid")))
            .collect(),
        casts: rows(include_str!("testdata/pg_cast.json"))
            .iter()
            .map(|row| ((int(row, "castsource"), int(row, "casttarget")), cast(row)))
            .collect(),
        ranges: rows(include_str!("testdata/pg_range.json"))
            .iter()
            .map(range)
            .collect(),
        procs: rows(include_str!("testdata/pg_proc.json"))
            .iter()
            .map(fixture_proc)
            .collect(),
        operators: rows(include_str!("testdata/pg_operator.json"))
            .iter()
            .map(|row| (text(row, "oprname").to_string(), operator(row)))
            .collect(),
    });

    impl FixtureCatalog {
        pub(crate) fn get() -> &'static FixtureCatalog {
            &FIXTURES
        }

        pub(crate) fn type_oids(&self) -> Vec<i64> {
            let mut oids: Vec<i64> = self.types.keys().copied().collect();
            oids.sort_unstable();
            oids
        }

        pub(crate) fn type_name(&self, oid: i64) -> Option<&str> {
            self.types.get(&oid).map(|fixture| fixture.name.as_str())
        }

        pub(crate) fn proc_by_oid(&self, oid: i64) -> Option<&FixtureProc> {
            self.procs.iter().find(|proc| proc.oid == oid)
        }

        pub(crate) fn operator_by_oid(&self, oid: i64) -> Option<OperatorRow> {
            self.operators
                .iter()
                .map(|(_, row)| *row)
                .find(|row| row.oid == oid)
        }
    }

    impl Catalog for FixtureCatalog {
        fn pg_type(&self, oid: i64) -> Option<TypeRow> {
            self.types.get(&oid).map(|fixture| fixture.row)
        }

        fn pg_type_oid(&self, name: &str) -> Option<i64> {
            self.types_by_name.get(name).copied()
        }

        fn pg_cast(&self, source: i64, target: i64) -> Option<CastRow> {
            self.casts.get(&(source, target)).copied()
        }

        fn pg_range(&self, range_type: i64) -> Option<RangeRow> {
            self.ranges
                .iter()
                .copied()
                .find(|row| row.range_type == range_type)
        }

        fn pg_range_of_multirange(&self, multirange_type: i64) -> Option<RangeRow> {
            self.ranges
                .iter()
                .copied()
                .find(|row| row.multirange_type == multirange_type)
        }

        fn pg_proc(&self, name: &str) -> Vec<ProcRow<'_>> {
            self.procs
                .iter()
                .filter(|proc| proc.name == name)
                .map(|proc| ProcRow {
                    oid: proc.oid,
                    kind: proc.kind,
                    arg_types: &proc.arg_types,
                    result: proc.result,
                    variadic: proc.variadic,
                    default_count: proc.default_count,
                })
                .collect()
        }

        fn pg_operator(&self, name: &str) -> Vec<OperatorRow> {
            self.operators
                .iter()
                .filter(|(operator_name, _)| operator_name == name)
                .map(|(_, row)| *row)
                .collect()
        }

        fn format_type_name(&self, oid: i64) -> String {
            let Some(fixture) = self.types.get(&oid) else {
                return "???".to_string();
            };
            let prints_as_array = fixture.row.array_element != INVALID_OID
                && oid != OIDVECTOROID
                && oid != INT2VECTOROID;
            if prints_as_array {
                return format!("{}[]", self.format_type_name(fixture.row.array_element));
            }
            match oid {
                16 => "boolean",
                18 => "\"char\"",
                20 => "bigint",
                21 => "smallint",
                23 => "integer",
                700 => "real",
                701 => "double precision",
                1042 => "character",
                1043 => "character varying",
                1083 => "time without time zone",
                1114 => "timestamp without time zone",
                1184 => "timestamp with time zone",
                1266 => "time with time zone",
                1562 => "bit varying",
                ANYOID => "\"any\"",
                _ => &fixture.name,
            }
            .to_string()
        }
    }

    fn rows(json: &str) -> Vec<Value> {
        serde_json::from_str(json).expect("a fixture is a JSON array of rows")
    }

    fn fixture_type(row: &Value) -> FixtureType {
        let subscripts_as_array = text(row, "typsubscript") == "array_subscript_handler";
        FixtureType {
            name: text(row, "typname").to_string(),
            row: TypeRow {
                category: letter(row, "typcategory"),
                is_preferred: row["typispreferred"]
                    .as_bool()
                    .expect("typispreferred is a boolean"),
                kind: match letter(row, "typtype") {
                    'b' => TypeKind::Base,
                    'c' => TypeKind::Composite,
                    'd' => TypeKind::Domain,
                    'e' => TypeKind::Enum,
                    'p' => TypeKind::Pseudo,
                    'r' => TypeKind::Range,
                    'm' => TypeKind::Multirange,
                    other => panic!("unrecognized typtype: {other}"),
                },
                array_element: if subscripts_as_array {
                    int(row, "typelem")
                } else {
                    INVALID_OID
                },
                array_type: int(row, "typarray"),
            },
        }
    }

    fn cast(row: &Value) -> CastRow {
        CastRow {
            context: match letter(row, "castcontext") {
                'i' => CastContext::Implicit,
                'a' => CastContext::Assignment,
                'e' => CastContext::Explicit,
                other => panic!("unrecognized castcontext: {other}"),
            },
            method: match letter(row, "castmethod") {
                'f' => CastMethod::Function(int(row, "castfunc")),
                'i' => CastMethod::InputOutput,
                'b' => CastMethod::Binary,
                other => panic!("unrecognized castmethod: {other}"),
            },
        }
    }

    fn range(row: &Value) -> RangeRow {
        RangeRow {
            range_type: int(row, "rngtypid"),
            subtype: int(row, "rngsubtype"),
            multirange_type: int(row, "rngmultitypid"),
        }
    }

    fn fixture_proc(row: &Value) -> FixtureProc {
        FixtureProc {
            oid: int(row, "oid"),
            name: text(row, "proname").to_string(),
            kind: letter(row, "prokind"),
            arg_types: row["proargtypes"]
                .as_array()
                .expect("proargtypes is an array")
                .iter()
                .map(|oid| oid.as_i64().expect("an argument type is an oid"))
                .collect(),
            result: int(row, "prorettype"),
            variadic: int(row, "provariadic"),
            default_count: usize::try_from(int(row, "pronargdefaults"))
                .expect("pronargdefaults is not negative"),
        }
    }

    fn operator(row: &Value) -> OperatorRow {
        OperatorRow {
            oid: int(row, "oid"),
            kind: letter(row, "oprkind"),
            left: int(row, "oprleft"),
            right: int(row, "oprright"),
            result: int(row, "oprresult"),
            function: int(row, "oprcode"),
        }
    }

    fn int(row: &Value, field: &str) -> i64 {
        row[field]
            .as_i64()
            .unwrap_or_else(|| panic!("{field} is an integer"))
    }

    fn text<'a>(row: &'a Value, field: &str) -> &'a str {
        row[field]
            .as_str()
            .unwrap_or_else(|| panic!("{field} is a string"))
    }

    fn letter(row: &Value, field: &str) -> char {
        let mut letters = text(row, field).chars();
        match (letters.next(), letters.next()) {
            (Some(letter), None) => letter,
            _ => panic!("{field} is one letter"),
        }
    }

    #[test]
    fn a_function_name_gives_every_overload() {
        let mut oids: Vec<i64> = FixtureCatalog::get()
            .pg_proc("abs")
            .iter()
            .map(|proc| proc.oid)
            .collect();
        oids.sort_unstable();
        assert_eq!(oids, [1394, 1395, 1396, 1397, 1398, 1705]);
        let abs_of_bigint = FixtureCatalog::get()
            .pg_proc("abs")
            .into_iter()
            .find(|proc| proc.oid == 1396)
            .expect("abs(bigint) is in the fixtures");
        assert_eq!(
            abs_of_bigint,
            ProcRow {
                oid: 1396,
                kind: 'f',
                arg_types: &[20],
                result: 20,
                variadic: 0,
                default_count: 0,
            }
        );
    }

    #[test]
    fn an_operator_name_gives_every_pair_of_operand_types() {
        let equals = FixtureCatalog::get().pg_operator("=");
        assert!(equals.contains(&OperatorRow {
            oid: 15,
            kind: 'b',
            left: 23,
            right: 20,
            result: 16,
            function: 852,
        }));
        assert!(equals.len() > 50);
        assert!(FixtureCatalog::get().pg_operator("no such").is_empty());
    }

    #[test]
    fn type_names_are_spelled_as_format_type_spells_them() {
        let catalog = FixtureCatalog::get();
        for (oid, name) in [
            (16, "boolean"),
            (18, "\"char\""),
            (23, "integer"),
            (25, "text"),
            (30, "oidvector"),
            (701, "double precision"),
            (705, "unknown"),
            (1007, "integer[]"),
            (1015, "character varying[]"),
            (1043, "character varying"),
            (1184, "timestamp with time zone"),
            (1560, "bit"),
            (1700, "numeric"),
            (2276, "\"any\""),
            (2287, "record[]"),
        ] {
            assert_eq!(catalog.format_type_name(oid), name);
        }
    }

    #[test]
    fn only_a_type_that_subscripts_as_an_array_has_an_element_type() {
        let catalog = FixtureCatalog::get();
        assert_eq!(get_element_type(catalog, 1007), 23);
        assert_eq!(get_element_type(catalog, 30), 26);
        assert_eq!(get_element_type(catalog, RECORDARRAYOID), RECORDOID);
        assert_eq!(get_element_type(catalog, 19), INVALID_OID);
        assert_eq!(get_element_type(catalog, 600), INVALID_OID);
        assert_eq!(get_element_type(catalog, 23), INVALID_OID);
        assert_eq!(get_element_type(catalog, 999_999), INVALID_OID);
    }

    #[test]
    fn a_type_has_the_array_type_of_its_typarray() {
        let catalog = FixtureCatalog::get();
        assert_eq!(get_array_type(catalog, 23), 1007);
        assert_eq!(get_array_type(catalog, TEXTOID), TEXTARRAYOID);
        assert_eq!(get_array_type(catalog, 1007), INVALID_OID);
        assert_eq!(get_array_type(catalog, ANYELEMENTOID), INVALID_OID);
        assert_eq!(get_array_type(catalog, 999_999), INVALID_OID);
        assert_eq!(type_is_array_domain(catalog, 1007), Ok(true));
        assert_eq!(type_is_array_domain(catalog, 23), Ok(false));
        assert_eq!(type_is_array_domain(catalog, INVALID_OID), Ok(false));
    }

    #[test]
    fn a_range_has_the_subtype_and_the_multirange_of_its_pg_range_row() {
        let catalog = FixtureCatalog::get();
        let (int4range, int4multirange, numrange) = (3904, 4451, 3906);
        assert_eq!(get_range_subtype(catalog, int4range), Ok(23));
        assert_eq!(get_range_subtype(catalog, numrange), Ok(1700));
        assert_eq!(get_range_subtype(catalog, 23), Ok(INVALID_OID));
        assert_eq!(get_range_subtype(catalog, int4multirange), Ok(INVALID_OID));
        assert_eq!(get_range_subtype(catalog, ANYRANGEOID), Ok(INVALID_OID));
        assert_eq!(get_range_multirange(catalog, int4range), Ok(int4multirange));
        assert_eq!(get_range_multirange(catalog, 23), Ok(INVALID_OID));
        assert_eq!(get_multirange_range(catalog, int4multirange), Ok(int4range));
        assert_eq!(get_multirange_range(catalog, int4range), Ok(INVALID_OID));
    }

    #[test]
    fn a_range_without_a_pg_range_row_is_not_supported() {
        struct NoRanges;
        impl Catalog for NoRanges {
            fn pg_type(&self, oid: i64) -> Option<TypeRow> {
                FixtureCatalog::get().pg_type(oid)
            }
            fn pg_type_oid(&self, name: &str) -> Option<i64> {
                FixtureCatalog::get().pg_type_oid(name)
            }
            fn pg_cast(&self, source: i64, target: i64) -> Option<CastRow> {
                FixtureCatalog::get().pg_cast(source, target)
            }
            fn pg_range(&self, _range_type: i64) -> Option<RangeRow> {
                None
            }
            fn pg_range_of_multirange(&self, _multirange_type: i64) -> Option<RangeRow> {
                None
            }
            fn pg_proc(&self, name: &str) -> Vec<ProcRow<'_>> {
                FixtureCatalog::get().pg_proc(name)
            }
            fn pg_operator(&self, name: &str) -> Vec<OperatorRow> {
                FixtureCatalog::get().pg_operator(name)
            }
            fn format_type_name(&self, oid: i64) -> String {
                FixtureCatalog::get().format_type_name(oid)
            }
        }
        let (int4range, int4multirange) = (3904, 4451);
        assert_eq!(get_range_subtype(&NoRanges, int4range), Err(Reason::Range));
        assert_eq!(get_range_subtype(&NoRanges, 23), Ok(INVALID_OID));
        assert_eq!(
            get_range_multirange(&NoRanges, int4range),
            Err(Reason::Range)
        );
        assert_eq!(
            get_multirange_range(&NoRanges, int4multirange),
            Err(Reason::Range)
        );
        assert_eq!(get_multirange_range(&NoRanges, int4range), Ok(INVALID_OID));
    }

    #[test]
    fn a_type_without_a_row_cannot_be_resolved() {
        let catalog = FixtureCatalog::get();
        assert_eq!(get_base_type(catalog, 999_999), Err(Reason::MissingType));
        assert_eq!(
            get_type_category_preferred(catalog, 999_999),
            Err(Reason::MissingType)
        );
        assert_eq!(get_typtype(catalog, 999_999), None);
    }
}
