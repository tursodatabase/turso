// Translated from PostgreSQL 18.6, src/backend/parser/parse_coerce.c and src/backend/catalog/pg_inherits.c. See licenses/postgres-license.md.

use super::catalog::{
    get_base_type, get_element_type, get_type_category_preferred, is_complex, is_polymorphic_type,
    type_is_array, type_is_enum, type_is_multirange, type_is_range, typeid_type_is_composite,
    CastMethod, Catalog, ANYARRAYOID, ANYCOMPATIBLEARRAYOID, ANYCOMPATIBLEMULTIRANGEOID,
    ANYCOMPATIBLENONARRAYOID, ANYCOMPATIBLEOID, ANYCOMPATIBLERANGEOID, ANYELEMENTOID, ANYENUMOID,
    ANYMULTIRANGEOID, ANYNONARRAYOID, ANYOID, ANYRANGEOID, INT2VECTOROID, INTERNALOID, INVALID_OID,
    OIDVECTOROID, RECORDARRAYOID, RECORDOID, TEXTOID, TYPCATEGORY_INVALID, TYPCATEGORY_STRING,
    UNKNOWNOID,
};
use super::Reason;
use turso_core::dialect::CastContext as CoercionContext;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CoercionPathType {
    None,
    Func(i64),
    RelabelType,
    CoerceViaIo,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommonType {
    Type(i64),
    /// The two types are of different categories.
    CannotBeMatched(i64, i64),
}

pub(crate) fn select_common_type(
    catalog: &dyn Catalog,
    typeids: &[i64],
) -> Result<CommonType, Reason> {
    assert!(!typeids.is_empty(), "a common type needs one type or more");
    let nargs = typeids.len();
    let mut i = 1;
    let mut ptype = typeids[0];

    if ptype != UNKNOWNOID {
        while i < nargs {
            if typeids[i] != ptype {
                break;
            }
            i += 1;
        }
        if i == nargs {
            return Ok(CommonType::Type(ptype));
        }
    }

    ptype = get_base_type(catalog, ptype)?;
    let (mut pcategory, mut pispreferred) = get_type_category_preferred(catalog, ptype)?;

    for &typeid in &typeids[i..] {
        let ntype = get_base_type(catalog, typeid)?;

        if ntype != UNKNOWNOID && ntype != ptype {
            let (ncategory, nispreferred) = get_type_category_preferred(catalog, ntype)?;
            if ptype == UNKNOWNOID {
                ptype = ntype;
                pcategory = ncategory;
                pispreferred = nispreferred;
            } else if ncategory != pcategory {
                return Ok(CommonType::CannotBeMatched(ptype, ntype));
            } else if !pispreferred
                && can_coerce_type(catalog, &[ptype], &[ntype], CoercionContext::Implicit)?
                && !can_coerce_type(catalog, &[ntype], &[ptype], CoercionContext::Implicit)?
            {
                ptype = ntype;
                pcategory = ncategory;
                pispreferred = nispreferred;
            }
        }
    }

    if ptype == UNKNOWNOID {
        ptype = TEXTOID;
    }

    Ok(CommonType::Type(ptype))
}

pub(crate) fn can_coerce_type(
    catalog: &dyn Catalog,
    input_typeids: &[i64],
    target_typeids: &[i64],
    ccontext: CoercionContext,
) -> Result<bool, Reason> {
    assert_eq!(
        input_typeids.len(),
        target_typeids.len(),
        "every input type has a target type"
    );
    let mut have_generics = false;

    for (&input_type_id, &target_type_id) in input_typeids.iter().zip(target_typeids) {
        if input_type_id == target_type_id {
            continue;
        }

        if input_type_id == INTERNALOID || target_type_id == INTERNALOID {
            return Ok(false);
        }

        if target_type_id == ANYOID {
            return Err(Reason::Polymorphic);
        }

        if is_polymorphic_type(target_type_id) {
            have_generics = true;
            continue;
        }

        if input_type_id == UNKNOWNOID {
            continue;
        }

        let pathtype = find_coercion_pathway(catalog, target_type_id, input_type_id, ccontext)?;
        if pathtype != CoercionPathType::None {
            continue;
        }

        if input_type_id == RECORDOID && is_complex(catalog, target_type_id)? {
            return Err(Reason::Composite);
        }

        if target_type_id == RECORDOID && is_complex(catalog, input_type_id)? {
            return Err(Reason::Composite);
        }

        if target_type_id == RECORDARRAYOID && is_complex_array(catalog, input_type_id)? {
            return Err(Reason::Composite);
        }

        if type_inherits_from(catalog, input_type_id, target_type_id)?
            || type_is_of_typed_table(catalog, input_type_id)?
        {
            continue;
        }

        return Ok(false);
    }

    if have_generics {
        return Err(Reason::Polymorphic);
    }

    Ok(true)
}

pub(crate) fn type_category(catalog: &dyn Catalog, type_id: i64) -> Result<char, Reason> {
    let (typcategory, _) = get_type_category_preferred(catalog, type_id)?;
    assert_ne!(typcategory, TYPCATEGORY_INVALID);
    Ok(typcategory)
}

pub(crate) fn is_preferred_type(
    catalog: &dyn Catalog,
    category: char,
    type_id: i64,
) -> Result<bool, Reason> {
    let (typcategory, typispreferred) = get_type_category_preferred(catalog, type_id)?;
    if category == typcategory || category == TYPCATEGORY_INVALID {
        Ok(typispreferred)
    } else {
        Ok(false)
    }
}

pub(crate) fn is_binary_coercible(
    catalog: &dyn Catalog,
    srctype: i64,
    targettype: i64,
) -> Result<bool, Reason> {
    if srctype == targettype {
        return Ok(true);
    }

    if targettype == ANYOID || targettype == ANYELEMENTOID || targettype == ANYCOMPATIBLEOID {
        return Ok(true);
    }

    let srctype = if srctype != INVALID_OID {
        get_base_type(catalog, srctype)?
    } else {
        srctype
    };

    if srctype == targettype {
        return Ok(true);
    }

    if (targettype == ANYARRAYOID || targettype == ANYCOMPATIBLEARRAYOID)
        && type_is_array(catalog, srctype)
    {
        return Ok(true);
    }

    if (targettype == ANYNONARRAYOID || targettype == ANYCOMPATIBLENONARRAYOID)
        && !type_is_array(catalog, srctype)
    {
        return Ok(true);
    }

    if targettype == ANYENUMOID && type_is_enum(catalog, srctype) {
        return Ok(true);
    }

    if (targettype == ANYRANGEOID || targettype == ANYCOMPATIBLERANGEOID)
        && type_is_range(catalog, srctype)
    {
        return Ok(true);
    }

    if (targettype == ANYMULTIRANGEOID || targettype == ANYCOMPATIBLEMULTIRANGEOID)
        && type_is_multirange(catalog, srctype)
    {
        return Ok(true);
    }

    if targettype == RECORDOID && is_complex(catalog, srctype)? {
        return Ok(true);
    }

    if targettype == RECORDARRAYOID && is_complex_array(catalog, srctype)? {
        return Ok(true);
    }

    let Some(cast_form) = catalog.pg_cast(srctype, targettype) else {
        return Ok(false);
    };

    Ok(cast_form.method == CastMethod::Binary && cast_form.context == CoercionContext::Implicit)
}

pub(crate) fn find_coercion_pathway(
    catalog: &dyn Catalog,
    target_type_id: i64,
    source_type_id: i64,
    ccontext: CoercionContext,
) -> Result<CoercionPathType, Reason> {
    let mut result = CoercionPathType::None;

    let source_type_id = if source_type_id != INVALID_OID {
        get_base_type(catalog, source_type_id)?
    } else {
        source_type_id
    };
    let target_type_id = if target_type_id != INVALID_OID {
        get_base_type(catalog, target_type_id)?
    } else {
        target_type_id
    };

    if source_type_id == target_type_id {
        return Ok(CoercionPathType::RelabelType);
    }

    if source_type_id == INTERNALOID || target_type_id == INTERNALOID {
        return Ok(CoercionPathType::None);
    }

    if let Some(cast_form) = catalog.pg_cast(source_type_id, target_type_id) {
        if ccontext >= cast_form.context {
            result = match cast_form.method {
                CastMethod::Function(castfunc) => CoercionPathType::Func(castfunc),
                CastMethod::InputOutput => CoercionPathType::CoerceViaIo,
                CastMethod::Binary => CoercionPathType::RelabelType,
            };
        }
    } else {
        if target_type_id != OIDVECTOROID && target_type_id != INT2VECTOROID {
            let target_elem = get_element_type(catalog, target_type_id);
            let source_elem = get_element_type(catalog, source_type_id);
            if target_elem != INVALID_OID && source_elem != INVALID_OID {
                let elempathtype =
                    find_coercion_pathway(catalog, target_elem, source_elem, ccontext)?;
                if elempathtype != CoercionPathType::None {
                    return Err(Reason::Array);
                }
            }
        }

        let to_string = ccontext >= CoercionContext::Assignment
            && type_category(catalog, target_type_id)? == TYPCATEGORY_STRING;
        if to_string
            || (ccontext >= CoercionContext::Explicit
                && type_category(catalog, source_type_id)? == TYPCATEGORY_STRING)
        {
            result = CoercionPathType::CoerceViaIo;
        }
    }

    Ok(result)
}

fn is_complex_array(catalog: &dyn Catalog, typid: i64) -> Result<bool, Reason> {
    let elemtype = get_element_type(catalog, typid);

    Ok(elemtype != INVALID_OID && is_complex(catalog, elemtype)?)
}

fn type_is_of_typed_table(catalog: &dyn Catalog, reltype_id: i64) -> Result<bool, Reason> {
    if is_complex(catalog, reltype_id)? {
        return Err(Reason::Composite);
    }

    Ok(false)
}

fn type_inherits_from(
    catalog: &dyn Catalog,
    subclass_type_id: i64,
    superclass_type_id: i64,
) -> Result<bool, Reason> {
    if !is_complex(catalog, subclass_type_id)? {
        return Ok(false);
    }
    if !typeid_type_is_composite(catalog, superclass_type_id)? {
        return Ok(false);
    }

    Err(Reason::Composite)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolve::catalog::fixtures::FixtureCatalog;
    use serde_json::Value;
    use std::collections::BTreeSet;
    use CoercionContext::{Assignment, Explicit, Implicit};

    const BOOL: i64 = 16;
    const NAME: i64 = 19;
    const INT8: i64 = 20;
    const INT2: i64 = 21;
    const INT4: i64 = 23;
    const OID: i64 = 26;
    const PG_TYPE_ROW: i64 = 71;
    const JSON: i64 = 114;
    const FLOAT4: i64 = 700;
    const FLOAT8: i64 = 701;
    const INT4_ARRAY: i64 = 1007;
    const INT8_ARRAY: i64 = 1016;
    const BPCHAR: i64 = 1042;
    const VARCHAR: i64 = 1043;
    const DATE: i64 = 1082;
    const TIMESTAMP: i64 = 1114;
    const TIMESTAMPTZ: i64 = 1184;
    const NUMERIC: i64 = 1700;
    const REGCLASS: i64 = 2205;
    const JSONB: i64 = 3802;
    const INT4RANGE: i64 = 3904;
    const INT4MULTIRANGE: i64 = 4451;

    struct OracleCase {
        kind: String,
        name: String,
        args: Vec<i64>,
        chosen: Option<i64>,
        coercions: Vec<String>,
        node: Option<String>,
        sqlstate: Option<String>,
    }

    fn oracle_cases() -> Vec<OracleCase> {
        let oracle: Value = serde_json::from_str(include_str!("testdata/oracle.json"))
            .expect("oracle.json is JSON");
        let cases = oracle["cases"].as_array().expect("the oracle has cases");
        cases
            .iter()
            .map(|case| OracleCase {
                kind: string(&case["kind"]),
                name: string(&case["name"]),
                args: case["args"]
                    .as_array()
                    .expect("a case has arguments")
                    .iter()
                    .map(|arg| arg.as_i64().unwrap_or(UNKNOWNOID))
                    .collect(),
                chosen: case["chosen"].as_i64(),
                coercions: case["coercions"]
                    .as_array()
                    .map(|coercions| coercions.iter().map(string).collect())
                    .unwrap_or_default(),
                node: case["node"].as_str().map(str::to_string),
                sqlstate: case["error"]["sqlstate"].as_str().map(str::to_string),
            })
            .collect()
    }

    fn string(value: &Value) -> String {
        value.as_str().expect("a string").to_string()
    }

    fn oracle_types() -> BTreeSet<i64> {
        oracle_cases()
            .iter()
            .flat_map(|case| case.args.clone())
            .filter(|&oid| oid != UNKNOWNOID)
            .collect()
    }

    fn parameter_types(catalog: &FixtureCatalog, case: &OracleCase) -> Option<Vec<i64>> {
        let chosen = case.chosen?;
        if case.kind == "operator" {
            let operator = catalog
                .operator_by_oid(chosen)
                .expect("the chosen operator is in the fixtures");
            return Some(match operator.kind {
                'l' => vec![operator.right],
                _ => vec![operator.left, operator.right],
            });
        }
        let proc = catalog
            .proc_by_oid(chosen)
            .expect("the chosen function is in the fixtures");
        (proc.variadic == INVALID_OID && proc.arg_types.len() == case.args.len())
            .then(|| proc.arg_types.clone())
    }

    fn takes_any_type(parameter: i64) -> bool {
        parameter == ANYOID || is_polymorphic_type(parameter)
    }

    #[test]
    fn a_cast_in_pg_cast_is_allowed_in_its_context_and_in_wider_ones_only() {
        let catalog = FixtureCatalog::get();
        let types = oracle_types();
        let mut casts = 0;
        for &source in &types {
            for &target in &types {
                let Some(cast) = catalog.pg_cast(source, target) else {
                    continue;
                };
                if source == target {
                    continue;
                }
                casts += 1;
                let expected = match cast.method {
                    CastMethod::Function(oid) => CoercionPathType::Func(oid),
                    CastMethod::InputOutput => CoercionPathType::CoerceViaIo,
                    CastMethod::Binary => CoercionPathType::RelabelType,
                };
                let narrowest = [Implicit, Assignment, Explicit]
                    .into_iter()
                    .find(|&context| {
                        find_coercion_pathway(catalog, target, source, context) == Ok(expected)
                    });
                assert_eq!(narrowest, Some(cast.context), "{source} to {target}");
                for context in [Implicit, Assignment, Explicit] {
                    if context < cast.context {
                        assert_eq!(
                            find_coercion_pathway(catalog, target, source, context),
                            Ok(CoercionPathType::None),
                            "{source} to {target} in {context:?}"
                        );
                    }
                }
            }
        }
        assert!(casts > 150, "only {casts} casts checked");
    }

    #[test]
    fn without_a_pg_cast_row_only_a_string_type_converts_and_not_implicitly() {
        let catalog = FixtureCatalog::get();
        let types = oracle_types();
        let mut pairs = 0;
        for &source in &types {
            for &target in &types {
                let both_arrays = type_is_array(catalog, source) && type_is_array(catalog, target);
                if source == target || both_arrays || catalog.pg_cast(source, target).is_some() {
                    continue;
                }
                pairs += 1;
                let to_string = type_category(catalog, target) == Ok(TYPCATEGORY_STRING);
                let from_string = type_category(catalog, source) == Ok(TYPCATEGORY_STRING);
                let through_io = |allowed| {
                    Ok(if allowed {
                        CoercionPathType::CoerceViaIo
                    } else {
                        CoercionPathType::None
                    })
                };
                assert_eq!(
                    find_coercion_pathway(catalog, target, source, Implicit),
                    Ok(CoercionPathType::None),
                    "{source} to {target}"
                );
                assert_eq!(
                    find_coercion_pathway(catalog, target, source, Assignment),
                    through_io(to_string),
                    "{source} to {target}"
                );
                assert_eq!(
                    find_coercion_pathway(catalog, target, source, Explicit),
                    through_io(to_string || from_string),
                    "{source} to {target}"
                );
            }
        }
        assert!(pairs > 7000, "only {pairs} pairs checked");
    }

    #[test]
    fn each_argument_gets_the_coercion_postgresql_inserted() {
        let catalog = FixtureCatalog::get();
        let mut checked = 0;
        for case in oracle_cases() {
            let Some(parameters) = parameter_types(catalog, &case) else {
                continue;
            };
            for ((&argument, &parameter), coercion) in
                case.args.iter().zip(&parameters).zip(&case.coercions)
            {
                if argument == UNKNOWNOID || takes_any_type(parameter) {
                    continue;
                }
                let expected = if coercion == "none" {
                    assert_eq!(argument, parameter, "{} {:?}", case.name, case.args);
                    continue;
                } else if let Some(function) = coercion.strip_prefix("fn:") {
                    let (function, result) = function.split_once('>').expect("fn:<oid>><type oid>");
                    assert_eq!(result.parse(), Ok(parameter));
                    CoercionPathType::Func(function.parse().expect("a function oid"))
                } else if let Some(result) = coercion.strip_prefix("relabel>") {
                    assert_eq!(result.parse(), Ok(parameter));
                    CoercionPathType::RelabelType
                } else {
                    continue;
                };
                assert_eq!(
                    find_coercion_pathway(catalog, parameter, argument, Implicit),
                    Ok(expected),
                    "{} {:?}",
                    case.name,
                    case.args
                );
                checked += 1;
            }
        }
        assert!(checked > 700, "only {checked} coercions checked");
    }

    #[test]
    fn the_arguments_can_be_coerced_to_the_parameters_of_the_chosen_row() {
        let catalog = FixtureCatalog::get();
        let (mut coercible, mut unsupported) = (0, 0);
        for case in oracle_cases() {
            let Some(parameters) = parameter_types(catalog, &case) else {
                continue;
            };
            match can_coerce_type(catalog, &case.args, &parameters, Implicit) {
                Ok(true) => coercible += 1,
                Ok(false) => panic!("{} {:?} is not coercible", case.name, case.args),
                Err(_) => unsupported += 1,
            }
        }
        assert!(coercible > 7000, "only {coercible} calls checked");
        assert!(unsupported < 1500, "{unsupported} calls are not supported");
    }

    #[test]
    fn a_call_postgresql_refuses_has_no_row_its_arguments_can_be_coerced_to() {
        let catalog = FixtureCatalog::get();
        let mut refused = 0;
        for case in oracle_cases() {
            if case.kind != "function" || case.sqlstate.as_deref() != Some("42883") {
                continue;
            }
            let candidates = catalog.pg_proc(&case.name);
            if candidates
                .iter()
                .any(|proc| proc.variadic != INVALID_OID || proc.default_count > 0)
            {
                continue;
            }
            for proc in candidates {
                if proc.arg_types.len() != case.args.len() {
                    continue;
                }
                assert_ne!(
                    can_coerce_type(catalog, &case.args, proc.arg_types, Implicit),
                    Ok(true),
                    "{} {:?} can be coerced to row {}",
                    case.name,
                    case.args,
                    proc.oid
                );
            }
            refused += 1;
        }
        assert!(refused > 8000, "only {refused} calls checked");
    }

    #[test]
    fn a_cast_written_as_a_function_call_takes_the_explicit_pathway() {
        let catalog = FixtureCatalog::get();
        let mut checked = 0;
        for case in oracle_cases() {
            let (Some(node), [source]) = (&case.node, case.args.as_slice()) else {
                continue;
            };
            if *source == UNKNOWNOID {
                continue;
            }
            let target = catalog
                .type_oids()
                .into_iter()
                .find(|&oid| catalog.type_name(oid) == Some(case.name.as_str()))
                .expect("the cast names a type");
            let expected = match node.as_str() {
                "CONST" => {
                    assert_eq!(*source, target);
                    continue;
                }
                "COERCEVIAIO" => CoercionPathType::CoerceViaIo,
                "RELABELTYPE" => CoercionPathType::RelabelType,
                other => panic!("unexpected node {other}"),
            };
            assert_eq!(
                find_coercion_pathway(catalog, target, *source, Explicit),
                Ok(expected),
                "{}({source})",
                case.name
            );
            checked += 1;
        }
        assert!(checked > 30, "only {checked} casts checked");
    }

    #[test]
    fn a_type_converts_to_text_on_assignment_and_from_text_only_when_asked() {
        let catalog = FixtureCatalog::get();
        let none = Ok(CoercionPathType::None);
        let io = Ok(CoercionPathType::CoerceViaIo);
        assert_eq!(
            find_coercion_pathway(catalog, TEXTOID, INT4, Implicit),
            none
        );
        assert_eq!(
            find_coercion_pathway(catalog, TEXTOID, INT4, Assignment),
            io
        );
        assert_eq!(find_coercion_pathway(catalog, TEXTOID, INT4, Explicit), io);
        assert_eq!(
            find_coercion_pathway(catalog, INT4, TEXTOID, Implicit),
            none
        );
        assert_eq!(
            find_coercion_pathway(catalog, INT4, TEXTOID, Assignment),
            none
        );
        assert_eq!(find_coercion_pathway(catalog, INT4, TEXTOID, Explicit), io);
        assert_eq!(find_coercion_pathway(catalog, DATE, INT4, Explicit), none);
        assert_eq!(
            find_coercion_pathway(catalog, TEXTOID, INT4_ARRAY, Explicit),
            io
        );
        assert_eq!(
            find_coercion_pathway(catalog, INT4_ARRAY, TEXTOID, Explicit),
            io
        );
        assert_eq!(
            find_coercion_pathway(catalog, TEXTOID, PG_TYPE_ROW, Explicit),
            io
        );
    }

    #[test]
    fn a_pg_cast_row_gives_the_method_from_its_context_on() {
        let catalog = FixtureCatalog::get();
        let none = Ok(CoercionPathType::None);
        assert_eq!(
            find_coercion_pathway(catalog, INT8, INT4, Implicit),
            Ok(CoercionPathType::Func(481))
        );
        assert_eq!(find_coercion_pathway(catalog, INT4, INT8, Implicit), none);
        assert_eq!(
            find_coercion_pathway(catalog, INT4, INT8, Assignment),
            Ok(CoercionPathType::Func(480))
        );
        assert_eq!(find_coercion_pathway(catalog, INT4, BOOL, Assignment), none);
        assert_eq!(
            find_coercion_pathway(catalog, INT4, BOOL, Explicit),
            Ok(CoercionPathType::Func(2558))
        );
        assert_eq!(
            find_coercion_pathway(catalog, TEXTOID, VARCHAR, Implicit),
            Ok(CoercionPathType::RelabelType)
        );
        assert_eq!(find_coercion_pathway(catalog, JSONB, JSON, Implicit), none);
        assert_eq!(
            find_coercion_pathway(catalog, JSONB, JSON, Assignment),
            Ok(CoercionPathType::CoerceViaIo)
        );
        assert_eq!(
            find_coercion_pathway(catalog, INT4, INT4, Implicit),
            Ok(CoercionPathType::RelabelType)
        );
    }

    #[test]
    fn nothing_converts_to_or_from_internal() {
        let catalog = FixtureCatalog::get();
        let none = Ok(CoercionPathType::None);
        assert_eq!(
            find_coercion_pathway(catalog, TEXTOID, INTERNALOID, Explicit),
            none
        );
        assert_eq!(
            find_coercion_pathway(catalog, INTERNALOID, TEXTOID, Explicit),
            none
        );
        assert_eq!(
            can_coerce_type(catalog, &[UNKNOWNOID], &[INTERNALOID], Implicit),
            Ok(false)
        );
        assert_eq!(
            can_coerce_type(catalog, &[INTERNALOID], &[INTERNALOID], Implicit),
            Ok(true)
        );
    }

    #[test]
    fn a_cast_between_arrays_whose_elements_convert_is_not_supported() {
        let catalog = FixtureCatalog::get();
        assert_eq!(
            find_coercion_pathway(catalog, INT8_ARRAY, INT4_ARRAY, Implicit),
            Err(Reason::Array)
        );
        assert_eq!(
            find_coercion_pathway(catalog, INT4_ARRAY, INT2VECTOROID, Explicit),
            Err(Reason::Array)
        );
        assert_eq!(
            find_coercion_pathway(catalog, INT4_ARRAY, INT8_ARRAY, Implicit),
            Ok(CoercionPathType::None)
        );
        assert_eq!(
            find_coercion_pathway(catalog, INT2VECTOROID, INT4_ARRAY, Explicit),
            Ok(CoercionPathType::None)
        );
    }

    #[test]
    fn an_untyped_literal_can_be_coerced_to_any_type_but_internal() {
        let catalog = FixtureCatalog::get();
        for target in catalog.type_oids() {
            let expected = match target {
                INTERNALOID => Ok(false),
                ANYOID => Err(Reason::Polymorphic),
                target if is_polymorphic_type(target) => Err(Reason::Polymorphic),
                _ => Ok(true),
            };
            assert_eq!(
                can_coerce_type(catalog, &[UNKNOWNOID], &[target], Implicit),
                expected,
                "unknown to {target}"
            );
        }
    }

    #[test]
    fn every_argument_must_be_coercible() {
        let catalog = FixtureCatalog::get();
        assert_eq!(can_coerce_type(catalog, &[], &[], Implicit), Ok(true));
        assert_eq!(
            can_coerce_type(catalog, &[INT4, INT2], &[INT8, FLOAT8], Implicit),
            Ok(true)
        );
        assert_eq!(
            can_coerce_type(catalog, &[INT4, TEXTOID], &[INT8, FLOAT8], Implicit),
            Ok(false)
        );
        assert_eq!(
            can_coerce_type(catalog, &[INT8, INT4], &[INT4, INT4], Implicit),
            Ok(false)
        );
        assert_eq!(
            can_coerce_type(catalog, &[INT8, INT4], &[INT4, INT4], Assignment),
            Ok(true)
        );
    }

    #[test]
    fn a_polymorphic_parameter_is_not_supported_when_the_other_arguments_fit() {
        let catalog = FixtureCatalog::get();
        assert_eq!(
            can_coerce_type(catalog, &[INT4, INT4], &[ANYELEMENTOID, INT8], Implicit),
            Err(Reason::Polymorphic)
        );
        assert_eq!(
            can_coerce_type(catalog, &[INT4, TEXTOID], &[ANYELEMENTOID, INT8], Implicit),
            Ok(false)
        );
        assert_eq!(
            can_coerce_type(catalog, &[INT4], &[ANYOID], Implicit),
            Err(Reason::Polymorphic)
        );
        assert_eq!(
            can_coerce_type(catalog, &[ANYELEMENTOID], &[ANYELEMENTOID], Implicit),
            Ok(true)
        );
    }

    #[test]
    fn a_composite_argument_is_not_supported() {
        let catalog = FixtureCatalog::get();
        assert_eq!(
            can_coerce_type(catalog, &[PG_TYPE_ROW], &[RECORDOID], Implicit),
            Err(Reason::Composite)
        );
        assert_eq!(
            can_coerce_type(catalog, &[RECORDOID], &[PG_TYPE_ROW], Implicit),
            Err(Reason::Composite)
        );
        assert_eq!(
            can_coerce_type(catalog, &[PG_TYPE_ROW], &[INT4], Implicit),
            Err(Reason::Composite)
        );
        assert_eq!(
            can_coerce_type(catalog, &[INT4], &[PG_TYPE_ROW], Implicit),
            Ok(false)
        );
        assert_eq!(
            can_coerce_type(catalog, &[RECORDOID], &[INT4], Implicit),
            Ok(false)
        );
    }

    #[test]
    fn a_type_the_catalog_does_not_have_is_not_supported() {
        let catalog = FixtureCatalog::get();
        assert_eq!(
            find_coercion_pathway(catalog, INT4, 999_999, Implicit),
            Err(Reason::MissingType)
        );
        assert_eq!(
            can_coerce_type(catalog, &[INT4], &[999_999], Implicit),
            Err(Reason::MissingType)
        );
        assert_eq!(type_category(catalog, 999_999), Err(Reason::MissingType));
    }

    #[test]
    fn binary_coercible_means_an_implicit_cast_that_calls_no_function() {
        let catalog = FixtureCatalog::get();
        assert_eq!(is_binary_coercible(catalog, INT4, INT4), Ok(true));
        assert_eq!(is_binary_coercible(catalog, VARCHAR, TEXTOID), Ok(true));
        assert_eq!(is_binary_coercible(catalog, TEXTOID, VARCHAR), Ok(true));
        assert_eq!(is_binary_coercible(catalog, INT4, OID), Ok(true));
        assert_eq!(is_binary_coercible(catalog, INT4, REGCLASS), Ok(true));
        assert_eq!(is_binary_coercible(catalog, OID, INT4), Ok(false));
        assert_eq!(is_binary_coercible(catalog, INT4, INT8), Ok(false));
        assert_eq!(is_binary_coercible(catalog, BPCHAR, TEXTOID), Ok(false));
        assert_eq!(is_binary_coercible(catalog, INT4, TEXTOID), Ok(false));
        assert_eq!(
            is_binary_coercible(catalog, 999_999, INT4),
            Err(Reason::MissingType)
        );
    }

    #[test]
    fn a_type_is_binary_coercible_to_the_polymorphic_type_of_its_kind() {
        let catalog = FixtureCatalog::get();
        for source in [INT4, INT4_ARRAY, INT4RANGE, PG_TYPE_ROW] {
            assert_eq!(is_binary_coercible(catalog, source, ANYOID), Ok(true));
            assert_eq!(
                is_binary_coercible(catalog, source, ANYELEMENTOID),
                Ok(true)
            );
            assert_eq!(
                is_binary_coercible(catalog, source, ANYCOMPATIBLEOID),
                Ok(true)
            );
        }
        let accepted = [
            (INT4_ARRAY, ANYARRAYOID),
            (INT4_ARRAY, ANYCOMPATIBLEARRAYOID),
            (INT4, ANYNONARRAYOID),
            (INT4, ANYCOMPATIBLENONARRAYOID),
            (INT4RANGE, ANYRANGEOID),
            (INT4RANGE, ANYCOMPATIBLERANGEOID),
            (INT4MULTIRANGE, ANYMULTIRANGEOID),
            (INT4MULTIRANGE, ANYCOMPATIBLEMULTIRANGEOID),
            (PG_TYPE_ROW, RECORDOID),
        ];
        for (source, target) in accepted {
            assert_eq!(
                is_binary_coercible(catalog, source, target),
                Ok(true),
                "{source} to {target}"
            );
        }
        let refused = [
            (INT4, ANYARRAYOID),
            (INT4_ARRAY, ANYNONARRAYOID),
            (INT4, ANYENUMOID),
            (INT4, ANYRANGEOID),
            (INT4RANGE, ANYMULTIRANGEOID),
            (INT4, RECORDOID),
            (INT4_ARRAY, RECORDARRAYOID),
        ];
        for (source, target) in refused {
            assert_eq!(
                is_binary_coercible(catalog, source, target),
                Ok(false),
                "{source} to {target}"
            );
        }
    }

    #[test]
    fn each_category_has_its_preferred_types() {
        let catalog = FixtureCatalog::get();
        assert_eq!(type_category(catalog, INT4), Ok('N'));
        assert_eq!(type_category(catalog, VARCHAR), Ok(TYPCATEGORY_STRING));
        assert_eq!(type_category(catalog, UNKNOWNOID), Ok('X'));
        assert_eq!(type_category(catalog, INT4_ARRAY), Ok('A'));
        assert_eq!(is_preferred_type(catalog, 'N', FLOAT8), Ok(true));
        assert_eq!(is_preferred_type(catalog, 'N', OID), Ok(true));
        assert_eq!(is_preferred_type(catalog, 'N', NUMERIC), Ok(false));
        assert_eq!(is_preferred_type(catalog, 'S', TEXTOID), Ok(true));
        assert_eq!(is_preferred_type(catalog, 'S', VARCHAR), Ok(false));
        assert_eq!(is_preferred_type(catalog, 'D', TIMESTAMPTZ), Ok(true));
        assert_eq!(is_preferred_type(catalog, 'D', TIMESTAMP), Ok(false));
        assert_eq!(is_preferred_type(catalog, 'S', FLOAT8), Ok(false));
        assert_eq!(
            is_preferred_type(catalog, TYPCATEGORY_INVALID, FLOAT8),
            Ok(true)
        );
        assert_eq!(
            is_preferred_type(catalog, TYPCATEGORY_INVALID, INT4),
            Ok(false)
        );
    }

    #[test]
    fn the_common_type_is_the_one_the_others_convert_to_implicitly() {
        let catalog = FixtureCatalog::get();
        let common = |types: &[i64]| select_common_type(catalog, types);
        assert_eq!(common(&[INT4]), Ok(CommonType::Type(INT4)));
        assert_eq!(common(&[INT4, INT4, INT4]), Ok(CommonType::Type(INT4)));
        assert_eq!(common(&[INT8, INT4]), Ok(CommonType::Type(INT8)));
        assert_eq!(common(&[INT4, INT8]), Ok(CommonType::Type(INT8)));
        assert_eq!(common(&[INT4, INT2, INT8]), Ok(CommonType::Type(INT8)));
        assert_eq!(common(&[NUMERIC, INT4]), Ok(CommonType::Type(NUMERIC)));
        assert_eq!(common(&[FLOAT4, NUMERIC]), Ok(CommonType::Type(FLOAT4)));
        assert_eq!(common(&[NUMERIC, FLOAT8]), Ok(CommonType::Type(FLOAT8)));
        assert_eq!(common(&[INT8, FLOAT4]), Ok(CommonType::Type(FLOAT4)));
        assert_eq!(common(&[TIMESTAMP, DATE]), Ok(CommonType::Type(TIMESTAMP)));
        assert_eq!(
            common(&[DATE, TIMESTAMPTZ]),
            Ok(CommonType::Type(TIMESTAMPTZ))
        );
        assert_eq!(common(&[NAME, VARCHAR]), Ok(CommonType::Type(NAME)));
        assert_eq!(common(&[VARCHAR, NAME]), Ok(CommonType::Type(NAME)));
    }

    #[test]
    fn the_common_type_stays_on_the_first_type_when_both_convert_or_it_is_preferred() {
        let catalog = FixtureCatalog::get();
        let common = |types: &[i64]| select_common_type(catalog, types);
        assert_eq!(common(&[TEXTOID, VARCHAR]), Ok(CommonType::Type(TEXTOID)));
        assert_eq!(common(&[VARCHAR, TEXTOID]), Ok(CommonType::Type(VARCHAR)));
        assert_eq!(common(&[VARCHAR, BPCHAR]), Ok(CommonType::Type(VARCHAR)));
        assert_eq!(common(&[OID, INT4]), Ok(CommonType::Type(OID)));
        assert_eq!(common(&[INT4, OID]), Ok(CommonType::Type(OID)));
    }

    #[test]
    fn untyped_literals_take_the_type_of_the_others_or_text() {
        let catalog = FixtureCatalog::get();
        let common = |types: &[i64]| select_common_type(catalog, types);
        assert_eq!(common(&[UNKNOWNOID]), Ok(CommonType::Type(TEXTOID)));
        assert_eq!(
            common(&[UNKNOWNOID, UNKNOWNOID]),
            Ok(CommonType::Type(TEXTOID))
        );
        assert_eq!(common(&[DATE, UNKNOWNOID]), Ok(CommonType::Type(DATE)));
        assert_eq!(common(&[UNKNOWNOID, INT4]), Ok(CommonType::Type(INT4)));
        assert_eq!(
            common(&[UNKNOWNOID, INT4, UNKNOWNOID, INT8]),
            Ok(CommonType::Type(INT8))
        );
    }

    #[test]
    fn types_of_different_categories_have_no_common_type() {
        let catalog = FixtureCatalog::get();
        assert_eq!(
            select_common_type(catalog, &[TEXTOID, INT4]),
            Ok(CommonType::CannotBeMatched(TEXTOID, INT4))
        );
        assert_eq!(
            select_common_type(catalog, &[INT4, BOOL]),
            Ok(CommonType::CannotBeMatched(INT4, BOOL))
        );
        assert_eq!(
            select_common_type(catalog, &[UNKNOWNOID, INT4, INT8, BOOL]),
            Ok(CommonType::CannotBeMatched(INT8, BOOL))
        );
        assert_eq!(
            select_common_type(catalog, &[INT4, 999_999]),
            Err(Reason::MissingType)
        );
    }
}
