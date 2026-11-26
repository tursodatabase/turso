// Translated from PostgreSQL 18.6, src/backend/parser/parse_func.c. See licenses/postgres-license.md.

use super::candidates::{funcname_get_candidates, FuncCandidate};
use super::catalog::{
    get_base_type, get_type_category_preferred, is_complex, typeid_type_is_composite, Catalog,
    INTERNALOID, INVALID_OID, RECORDOID, TYPCATEGORY_INVALID, TYPCATEGORY_STRING, UNKNOWNOID,
};
use super::coerce::{
    can_coerce_type, coerce_type, enforce_generic_type_consistency, find_coercion_pathway,
    is_preferred_type, type_category, CoercionPathType,
};
use super::error::{function_does_not_exist, function_is_not_unique};
use super::{ArgCoercion, ArgType, Reason, ResolveError, ResolvedCall};
use turso_core::dialect::CastContext as CoercionContext;

const FUNC_MAX_ARGS: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FuncDetailCode {
    NotFound,
    Multiple,
    Normal,
    Procedure,
    Aggregate,
    WindowFunc,
    Coercion,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FuncDetail {
    pub code: FuncDetailCode,
    pub funcid: i64,
    pub rettype: i64,
    pub true_typeids: Vec<i64>,
}

pub(crate) fn parse_func_or_column(
    catalog: &dyn Catalog,
    funcname: &str,
    fargs: &[ArgType],
) -> Result<Result<ResolvedCall, ResolveError>, Reason> {
    if fargs.len() > FUNC_MAX_ARGS {
        return Err(Reason::TooManyArguments);
    }

    let actual_arg_types: Vec<i64> = fargs
        .iter()
        .map(|arg| match arg {
            ArgType::Known(argtype) => *argtype,
            ArgType::Unknown => UNKNOWNOID,
        })
        .collect();
    let nargs = actual_arg_types.len();

    let could_be_projection = nargs == 1
        && (actual_arg_types[0] == RECORDOID || is_complex(catalog, actual_arg_types[0])?);

    let fdresult = func_get_detail(catalog, funcname, &actual_arg_types)?;

    match fdresult.code {
        FuncDetailCode::Normal => {}
        FuncDetailCode::Procedure => return Err(Reason::Procedure),
        FuncDetailCode::Aggregate => return Err(Reason::Aggregate),
        FuncDetailCode::WindowFunc => return Err(Reason::Window),
        FuncDetailCode::Coercion => return Err(Reason::Cast),
        FuncDetailCode::Multiple => {
            return Ok(Err(function_is_not_unique(&func_signature_string(
                catalog,
                funcname,
                &actual_arg_types,
            ))));
        }
        FuncDetailCode::NotFound => {
            if could_be_projection {
                return Err(Reason::Composite);
            }
            return Ok(Err(function_does_not_exist(&func_signature_string(
                catalog,
                funcname,
                &actual_arg_types,
            ))));
        }
    }

    let FuncDetail {
        funcid,
        rettype,
        true_typeids: mut declared_arg_types,
        ..
    } = fdresult;

    let rettype = match enforce_generic_type_consistency(
        catalog,
        &actual_arg_types,
        &mut declared_arg_types,
        rettype,
        false,
    )? {
        Ok(rettype) => rettype,
        Err(error) => return Ok(Err(error)),
    };

    if declared_arg_types.contains(&INTERNALOID) || rettype == INTERNALOID {
        return Err(Reason::Internal);
    }

    let coercions = make_fn_arguments(catalog, &actual_arg_types, &declared_arg_types)?;

    Ok(Ok(ResolvedCall {
        oid: funcid,
        parameter_types: declared_arg_types,
        coercions,
        result_type: rettype,
    }))
}

pub(crate) fn func_get_detail(
    catalog: &dyn Catalog,
    funcname: &str,
    argtypes: &[i64],
) -> Result<FuncDetail, Reason> {
    let nargs = argtypes.len();
    let without_function = |code| FuncDetail {
        code,
        funcid: INVALID_OID,
        rettype: INVALID_OID,
        true_typeids: Vec::new(),
    };

    let raw_candidates = funcname_get_candidates(catalog, funcname, nargs)?;

    let mut best_candidate = raw_candidates
        .iter()
        .find(|candidate| candidate.args == argtypes)
        .cloned();

    if best_candidate.is_none() {
        if nargs == 1 {
            if let Some(target_type) = func_name_as_type(catalog, funcname)? {
                let source_type = argtypes[0];

                let iscoercion = if source_type == UNKNOWNOID {
                    true
                } else {
                    let cpathtype = find_coercion_pathway(
                        catalog,
                        target_type,
                        source_type,
                        CoercionContext::Explicit,
                    )?;
                    match cpathtype {
                        CoercionPathType::RelabelType => true,
                        CoercionPathType::CoerceViaIo => {
                            !((source_type == RECORDOID || is_complex(catalog, source_type)?)
                                && type_category(catalog, target_type)? == TYPCATEGORY_STRING)
                        }
                        _ => false,
                    }
                };

                if iscoercion {
                    return Ok(FuncDetail {
                        code: FuncDetailCode::Coercion,
                        funcid: INVALID_OID,
                        rettype: target_type,
                        true_typeids: argtypes.to_vec(),
                    });
                }
            }
        }

        if !raw_candidates.is_empty() {
            let mut current_candidates = func_match_argtypes(catalog, argtypes, raw_candidates)?;

            if current_candidates.len() == 1 {
                best_candidate = current_candidates.pop();
            } else if current_candidates.len() > 1 {
                best_candidate = func_select_candidate(catalog, argtypes, current_candidates)?;

                if best_candidate.is_none() {
                    return Ok(without_function(FuncDetailCode::Multiple));
                }
            }
        }
    }

    let Some(best_candidate) = best_candidate else {
        return Ok(without_function(FuncDetailCode::NotFound));
    };

    if best_candidate.oid == INVALID_OID {
        return Ok(without_function(FuncDetailCode::Multiple));
    }

    let pform = catalog
        .pg_proc(funcname)
        .into_iter()
        .find(|pform| pform.oid == best_candidate.oid)
        .unwrap_or_else(|| panic!("cache lookup failed for function {}", best_candidate.oid));

    let code = match pform.kind {
        'a' => FuncDetailCode::Aggregate,
        'f' => FuncDetailCode::Normal,
        'p' => FuncDetailCode::Procedure,
        'w' => FuncDetailCode::WindowFunc,
        prokind => panic!("unrecognized prokind: {prokind}"),
    };

    Ok(FuncDetail {
        code,
        funcid: best_candidate.oid,
        rettype: pform.result,
        true_typeids: best_candidate.args,
    })
}

pub(crate) fn func_match_argtypes(
    catalog: &dyn Catalog,
    input_typeids: &[i64],
    raw_candidates: Vec<FuncCandidate>,
) -> Result<Vec<FuncCandidate>, Reason> {
    let mut candidates = Vec::new();

    for current_candidate in raw_candidates {
        if can_coerce_type(
            catalog,
            input_typeids,
            &current_candidate.args,
            CoercionContext::Implicit,
        )? {
            candidates.insert(0, current_candidate);
        }
    }

    Ok(candidates)
}

pub(crate) fn func_select_candidate(
    catalog: &dyn Catalog,
    input_typeids: &[i64],
    mut candidates: Vec<FuncCandidate>,
) -> Result<Option<FuncCandidate>, Reason> {
    let nargs = input_typeids.len();

    if nargs > FUNC_MAX_ARGS {
        return Err(Reason::TooManyArguments);
    }

    let mut nunknowns = 0;
    let mut input_base_typeids = Vec::with_capacity(nargs);
    for &input_typeid in input_typeids {
        if input_typeid != UNKNOWNOID {
            input_base_typeids.push(get_base_type(catalog, input_typeid)?);
        } else {
            input_base_typeids.push(UNKNOWNOID);
            nunknowns += 1;
        }
    }

    candidates = keep_candidates_with_most_matches(candidates, |current_typeids| {
        let mut nmatch = 0;
        for i in 0..nargs {
            if input_base_typeids[i] != UNKNOWNOID && current_typeids[i] == input_base_typeids[i] {
                nmatch += 1;
            }
        }
        Ok(nmatch)
    })?;

    if candidates.len() == 1 {
        return Ok(candidates.pop());
    }

    let mut slot_category = Vec::with_capacity(nargs);
    for &input_base_typeid in &input_base_typeids {
        slot_category.push(type_category(catalog, input_base_typeid)?);
    }
    candidates = keep_candidates_with_most_matches(candidates, |current_typeids| {
        let mut nmatch = 0;
        for i in 0..nargs {
            if input_base_typeids[i] != UNKNOWNOID
                && (current_typeids[i] == input_base_typeids[i]
                    || is_preferred_type(catalog, slot_category[i], current_typeids[i])?)
            {
                nmatch += 1;
            }
        }
        Ok(nmatch)
    })?;

    if candidates.len() == 1 {
        return Ok(candidates.pop());
    }

    if nunknowns == 0 {
        return Ok(None);
    }

    let mut slot_has_preferred_type = vec![false; nargs];
    let mut resolved_unknowns = false;
    for i in 0..nargs {
        if input_base_typeids[i] != UNKNOWNOID {
            continue;
        }
        resolved_unknowns = true;
        slot_category[i] = TYPCATEGORY_INVALID;
        slot_has_preferred_type[i] = false;
        let mut have_conflict = false;
        for current_candidate in &candidates {
            let current_type = current_candidate.args[i];
            let (current_category, current_is_preferred) =
                get_type_category_preferred(catalog, current_type)?;
            if slot_category[i] == TYPCATEGORY_INVALID {
                slot_category[i] = current_category;
                slot_has_preferred_type[i] = current_is_preferred;
            } else if current_category == slot_category[i] {
                slot_has_preferred_type[i] |= current_is_preferred;
            } else if current_category == TYPCATEGORY_STRING {
                slot_category[i] = current_category;
                slot_has_preferred_type[i] = current_is_preferred;
            } else {
                have_conflict = true;
            }
        }
        if have_conflict && slot_category[i] != TYPCATEGORY_STRING {
            resolved_unknowns = false;
            break;
        }
    }

    if resolved_unknowns {
        let mut kept = Vec::new();
        for current_candidate in &candidates {
            let mut keepit = true;
            for i in 0..nargs {
                if input_base_typeids[i] != UNKNOWNOID {
                    continue;
                }
                let current_type = current_candidate.args[i];
                let (current_category, current_is_preferred) =
                    get_type_category_preferred(catalog, current_type)?;
                if current_category != slot_category[i] {
                    keepit = false;
                    break;
                }
                if slot_has_preferred_type[i] && !current_is_preferred {
                    keepit = false;
                    break;
                }
            }
            if keepit {
                kept.push(current_candidate.clone());
            }
        }

        if !kept.is_empty() {
            candidates = kept;
        }

        if candidates.len() == 1 {
            return Ok(candidates.pop());
        }
    }

    if nunknowns < nargs {
        let mut known_type = UNKNOWNOID;

        for &input_base_typeid in &input_base_typeids {
            if input_base_typeid == UNKNOWNOID {
                continue;
            }
            if known_type == UNKNOWNOID {
                known_type = input_base_typeid;
            } else if known_type != input_base_typeid {
                known_type = UNKNOWNOID;
                break;
            }
        }

        if known_type != UNKNOWNOID {
            let input_base_typeids = vec![known_type; nargs];
            let mut ncandidates = 0;
            let mut last_candidate = None;
            for current_candidate in &candidates {
                if can_coerce_type(
                    catalog,
                    &input_base_typeids,
                    &current_candidate.args,
                    CoercionContext::Implicit,
                )? {
                    ncandidates += 1;
                    if ncandidates > 1 {
                        break;
                    }
                    last_candidate = Some(current_candidate);
                }
            }
            if ncandidates == 1 {
                return Ok(last_candidate.cloned());
            }
        }
    }

    Ok(None)
}

fn keep_candidates_with_most_matches(
    candidates: Vec<FuncCandidate>,
    count_matches: impl Fn(&[i64]) -> Result<usize, Reason>,
) -> Result<Vec<FuncCandidate>, Reason> {
    let mut nbest_match = 0;
    let mut kept: Vec<FuncCandidate> = Vec::new();

    for current_candidate in candidates {
        let nmatch = count_matches(&current_candidate.args)?;

        if nmatch > nbest_match || kept.is_empty() {
            nbest_match = nmatch;
            kept.clear();
            kept.push(current_candidate);
        } else if nmatch == nbest_match {
            kept.push(current_candidate);
        }
    }

    Ok(kept)
}

fn make_fn_arguments(
    catalog: &dyn Catalog,
    actual_arg_types: &[i64],
    declared_arg_types: &[i64],
) -> Result<Vec<ArgCoercion>, Reason> {
    let mut coercions = Vec::with_capacity(actual_arg_types.len());

    for (&actual_arg_type, &declared_arg_type) in actual_arg_types.iter().zip(declared_arg_types) {
        if actual_arg_type != declared_arg_type {
            coercions.push(coerce_type(
                catalog,
                actual_arg_type,
                declared_arg_type,
                CoercionContext::Implicit,
            )?);
        } else {
            coercions.push(ArgCoercion::None);
        }
    }

    Ok(coercions)
}

fn func_name_as_type(catalog: &dyn Catalog, funcname: &str) -> Result<Option<i64>, Reason> {
    let Some(typid) = catalog.pg_type_oid(funcname) else {
        return Ok(None);
    };

    if typeid_type_is_composite(catalog, typid)? {
        return Ok(None);
    }

    Ok(Some(typid))
}

pub(crate) fn func_signature_string(
    catalog: &dyn Catalog,
    funcname: &str,
    argtypes: &[i64],
) -> String {
    let mut argbuf = format!("{funcname}(");

    for (i, &argtype) in argtypes.iter().enumerate() {
        if i > 0 {
            argbuf.push_str(", ");
        }
        argbuf.push_str(&catalog.format_type_name(argtype));
    }

    argbuf.push(')');

    argbuf
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolve::catalog::fixtures::FixtureCatalog;
    use crate::resolve::oracle::{oracle_cases, OracleCase};
    use crate::resolve::{resolve_function, Resolution};
    use std::collections::BTreeMap;

    fn arguments(case: &OracleCase) -> Vec<ArgType> {
        case.args
            .iter()
            .map(|&arg| match arg {
                UNKNOWNOID => ArgType::Unknown,
                known => ArgType::Known(known),
            })
            .collect()
    }

    fn coercions_as_the_oracle_writes_them(case: &OracleCase, call: &ResolvedCall) -> Vec<String> {
        case.args
            .iter()
            .zip(&call.parameter_types)
            .zip(&call.coercions)
            .map(|((&argument, parameter), coercion)| match coercion {
                ArgCoercion::None if argument == UNKNOWNOID => format!("unknown>{UNKNOWNOID}"),
                ArgCoercion::None => "none".to_string(),
                ArgCoercion::Literal => format!("unknown>{parameter}"),
                ArgCoercion::Relabel => format!("relabel>{parameter}"),
                ArgCoercion::Function(function) => format!("fn:{function}>{parameter}"),
                ArgCoercion::ViaInputOutput => format!("io>{parameter}"),
            })
            .collect()
    }

    fn difference_from_the_oracle(case: &OracleCase, resolution: &Resolution) -> Option<String> {
        let expected_error = case.sqlstate.as_deref();
        let of_internal = case.message.as_deref().is_some_and(|message| {
            message.contains("type \"internal\" cannot be called explicitly")
        });
        match (resolution, case.chosen, expected_error) {
            (Resolution::Unsupported(Reason::Cast | Reason::Variadic), None, None) => None,
            (_, None, None) => Some("a cast must be left as not supported".to_string()),
            (Resolution::Unsupported(Reason::Cast), _, _) => {
                Some("PostgreSQL does not read it as a cast".to_string())
            }
            (Resolution::Unsupported(_), _, Some("42725" | "42883" | "42809")) => None,
            (Resolution::Unsupported(Reason::Aggregate), None, Some("42804")) => None,
            (Resolution::Unsupported(Reason::Aggregate), Some(_), _) => {
                (case.kind != "aggregate").then(|| "it is not an aggregate".to_string())
            }
            (Resolution::Unsupported(_), Some(_), _) => None,
            (Resolution::Resolved(call), Some(chosen), _) => {
                let ours = (
                    call.oid,
                    Some(call.result_type),
                    coercions_as_the_oracle_writes_them(case, call),
                );
                let theirs = (chosen, case.result, case.coercions.clone());
                (ours != theirs).then(|| format!("resolved to {ours:?}, expected {theirs:?}"))
            }
            (Resolution::Unsupported(Reason::Internal), None, Some("0A000")) if of_internal => None,
            (Resolution::Resolved(_), None, Some("0A000" | "XX000")) if !of_internal => None,
            (Resolution::Error(error), None, Some(sqlstate)) => {
                let ours = (error.sqlstate(), error.message(), error.hint());
                let theirs = (
                    sqlstate,
                    case.message.as_deref().unwrap_or(""),
                    case.hint.as_deref(),
                );
                (ours != theirs).then(|| format!("failed with {ours:?}, expected {theirs:?}"))
            }
            (ours, _, _) => Some(format!("got {ours:?}")),
        }
    }

    #[test]
    fn every_function_call_of_the_oracle_gets_the_answer_of_postgresql() {
        let catalog = FixtureCatalog::get();
        let mut differences = Vec::new();
        let mut outcomes: BTreeMap<String, usize> = BTreeMap::new();
        let mut cases = 0;
        for case in oracle_cases() {
            if case.kind == "operator" {
                continue;
            }
            cases += 1;
            let resolution = resolve_function(catalog, &case.name, &arguments(case));
            if let Some(difference) = difference_from_the_oracle(case, &resolution) {
                differences.push(format!("{}{:?}: {difference}", case.name, case.args));
            }
            let outcome = match &resolution {
                Resolution::Resolved(_) => "resolved".to_string(),
                Resolution::Error(error) => format!("error {}", error.sqlstate()),
                Resolution::Unsupported(reason) => format!("not supported: {reason:?}"),
            };
            *outcomes.entry(outcome).or_default() += 1;
        }
        println!("{cases} calls: {outcomes:#?}");
        assert!(
            differences.is_empty(),
            "{} differences, the first ones: {:#?}",
            differences.len(),
            &differences[..differences.len().min(40)]
        );
        let expected = [
            ("error 42704", 10),
            ("error 42725", 60),
            ("error 42804", 156),
            ("error 42883", 11708),
            ("not supported: Aggregate", 413),
            ("not supported: Array", 127),
            ("not supported: Cast", 58),
            ("not supported: Internal", 1),
            ("not supported: Variadic", 343),
            ("resolved", 8105),
        ];
        assert_eq!(
            outcomes,
            expected
                .into_iter()
                .map(|(outcome, count)| (outcome.to_string(), count))
                .collect()
        );
    }

    #[test]
    fn an_aggregate_is_found_with_the_rules_of_a_function() {
        let catalog = FixtureCatalog::get();
        let (mut found, mut unsupported) = (0, 0);
        for case in oracle_cases() {
            if case.kind != "aggregate" {
                continue;
            }
            match func_get_detail(catalog, &case.name, &case.args) {
                Ok(detail) => {
                    assert_eq!(
                        (detail.code, Some(detail.funcid)),
                        (FuncDetailCode::Aggregate, case.chosen),
                        "{}{:?}",
                        case.name,
                        case.args
                    );
                    found += 1;
                }
                Err(_) => unsupported += 1,
            }
            assert!(
                matches!(
                    resolve_function(catalog, &case.name, &arguments(case)),
                    Resolution::Unsupported(_)
                ),
                "{}{:?}",
                case.name,
                case.args
            );
        }
        assert!(found > 190, "only {found} aggregates found");
        assert!(
            unsupported < 50,
            "{unsupported} aggregates are not supported"
        );
    }

    const BOOL: i64 = 16;
    const INT8: i64 = 20;
    const INT2: i64 = 21;
    const INT4: i64 = 23;
    const TEXT: i64 = 25;
    const FLOAT4: i64 = 700;
    const FLOAT8: i64 = 701;
    const VARCHAR: i64 = 1043;
    const DATE: i64 = 1082;
    const TIMESTAMP: i64 = 1114;
    const TIMESTAMPTZ: i64 = 1184;
    const INTERVAL: i64 = 1186;
    const NUMERIC: i64 = 1700;
    const REGTYPE: i64 = 2206;
    const INT4_ARRAY: i64 = 1007;
    const INT8_ARRAY: i64 = 1016;
    const TEXT_ARRAY: i64 = 1009;
    const INT4RANGE: i64 = 3904;
    const ANYOID: i64 = 2276;
    const ANYARRAYOID: i64 = 2277;
    const ANYELEMENTOID: i64 = 2283;
    const ANYRANGEOID: i64 = 3831;
    const ANYMULTIRANGEOID: i64 = 4537;

    fn resolve(name: &str, arguments: &[ArgType]) -> Resolution {
        resolve_function(FixtureCatalog::get(), name, arguments)
    }

    fn resolved(
        oid: i64,
        parameter_types: &[i64],
        coercions: &[ArgCoercion],
        result_type: i64,
    ) -> Resolution {
        Resolution::Resolved(ResolvedCall {
            oid,
            parameter_types: parameter_types.to_vec(),
            coercions: coercions.to_vec(),
            result_type,
        })
    }

    use ArgType::{Known, Unknown};

    #[test]
    fn a_row_whose_parameter_types_are_the_argument_types_is_chosen_first() {
        assert_eq!(
            resolve("abs", &[Known(INT8)]),
            resolved(1396, &[INT8], &[ArgCoercion::None], INT8)
        );
        assert_eq!(
            resolve("round", &[Known(NUMERIC), Known(INT4)]),
            resolved(
                1707,
                &[NUMERIC, INT4],
                &[ArgCoercion::None, ArgCoercion::None],
                NUMERIC
            )
        );
        assert_eq!(resolve("pi", &[]), resolved(1610, &[], &[], FLOAT8));
    }

    #[test]
    fn the_only_row_that_takes_the_arguments_by_implicit_cast_is_chosen() {
        assert_eq!(
            resolve("acos", &[Known(FLOAT4)]),
            resolved(1601, &[FLOAT8], &[ArgCoercion::Function(311)], FLOAT8)
        );
        assert_eq!(
            resolve("lpad", &[Known(VARCHAR), Known(INT4)]),
            resolved(
                879,
                &[TEXT, INT4],
                &[ArgCoercion::Relabel, ArgCoercion::None],
                TEXT
            )
        );
    }

    #[test]
    fn no_row_is_chosen_when_an_argument_has_no_implicit_cast_to_any_row() {
        let hint = "No function matches the given name and argument types. You might need to add explicit type casts.";
        for (name, arguments, message) in [
            (
                "abs",
                vec![Known(TEXT)],
                "function abs(text) does not exist",
            ),
            (
                "md5",
                vec![Known(INT4)],
                "function md5(integer) does not exist",
            ),
            (
                "round",
                vec![Known(TEXT), Known(INT4)],
                "function round(text, integer) does not exist",
            ),
            (
                "substr",
                vec![Known(TEXT), Known(INT8)],
                "function substr(text, bigint) does not exist",
            ),
            ("no_such", vec![], "function no_such() does not exist"),
            (
                "abs",
                vec![Known(INT4), Unknown],
                "function abs(integer, unknown) does not exist",
            ),
        ] {
            let Resolution::Error(error) = resolve(name, &arguments) else {
                panic!("{message}");
            };
            assert_eq!(
                (error.sqlstate(), error.message(), error.hint()),
                ("42883", message, Some(hint))
            );
        }
    }

    #[test]
    fn the_rows_with_the_most_parameters_of_the_exact_argument_type_are_kept() {
        assert_eq!(
            resolve("age", &[Unknown, Known(TIMESTAMP)]),
            resolved(
                2058,
                &[TIMESTAMP, TIMESTAMP],
                &[ArgCoercion::Literal, ArgCoercion::None],
                INTERVAL
            )
        );
        assert_eq!(
            resolve("mod", &[Unknown, Known(INT8)]),
            resolved(
                947,
                &[INT8, INT8],
                &[ArgCoercion::Literal, ArgCoercion::None],
                INT8
            )
        );
    }

    #[test]
    fn a_row_that_takes_the_preferred_type_of_the_category_of_the_argument_wins() {
        assert_eq!(
            resolve("ceil", &[Known(INT8)]),
            resolved(2308, &[FLOAT8], &[ArgCoercion::Function(482)], FLOAT8)
        );
        assert_eq!(
            resolve("sqrt", &[Known(INT4)]),
            resolved(1344, &[FLOAT8], &[ArgCoercion::Function(316)], FLOAT8)
        );
        assert_eq!(
            resolve("length", &[Known(VARCHAR)]),
            resolved(1317, &[TEXT], &[ArgCoercion::Relabel], INT4)
        );
        assert_eq!(
            resolve("date_trunc", &[Unknown, Known(DATE)]),
            resolved(
                1217,
                &[TEXT, TIMESTAMPTZ],
                &[ArgCoercion::Literal, ArgCoercion::Function(1174)],
                TIMESTAMPTZ
            )
        );
    }

    #[test]
    fn typed_arguments_that_fit_several_rows_equally_well_are_ambiguous() {
        let hint =
            "Could not choose a best candidate function. You might need to add explicit type casts.";
        for (name, message) in [
            (
                "pg_size_pretty",
                "function pg_size_pretty(smallint) is not unique",
            ),
            ("money", "function money(smallint) is not unique"),
        ] {
            let Resolution::Error(error) = resolve(name, &[Known(INT2)]) else {
                panic!("{message}");
            };
            assert_eq!(
                (error.sqlstate(), error.message(), error.hint()),
                ("42725", message, Some(hint))
            );
        }
    }

    #[test]
    fn a_quoted_literal_takes_the_preferred_type_of_its_category() {
        assert_eq!(
            resolve("abs", &[Unknown]),
            resolved(1395, &[FLOAT8], &[ArgCoercion::Literal], FLOAT8)
        );
        assert_eq!(
            resolve("char_length", &[Unknown]),
            resolved(1381, &[TEXT], &[ArgCoercion::Literal], INT4)
        );
    }

    #[test]
    fn a_quoted_literal_is_a_string_when_a_row_takes_a_string() {
        assert_eq!(
            resolve("length", &[Unknown]),
            resolved(1317, &[TEXT], &[ArgCoercion::Literal], INT4)
        );
        assert_eq!(
            resolve("to_char", &[Known(INT4), Unknown]),
            resolved(
                1773,
                &[INT4, TEXT],
                &[ArgCoercion::None, ArgCoercion::Literal],
                TEXT
            )
        );
    }

    #[test]
    fn a_quoted_literal_is_ambiguous_when_the_rows_take_different_categories_and_no_string() {
        for (name, arguments, message) in [
            ("age", vec![Unknown], "function age(unknown) is not unique"),
            (
                "to_char",
                vec![Unknown, Unknown],
                "function to_char(unknown, unknown) is not unique",
            ),
        ] {
            let Resolution::Error(error) = resolve(name, &arguments) else {
                panic!("{message}");
            };
            assert_eq!((error.sqlstate(), error.message()), ("42725", message));
        }
    }

    #[test]
    fn a_quoted_literal_takes_the_type_of_the_typed_arguments_as_the_last_rule() {
        let detail = func_get_detail(
            FixtureCatalog::get(),
            "percentile_cont",
            &[UNKNOWNOID, FLOAT8],
        );
        assert_eq!(
            detail,
            Ok(FuncDetail {
                code: FuncDetailCode::Aggregate,
                funcid: 3974,
                rettype: FLOAT8,
                true_typeids: vec![FLOAT8, FLOAT8],
            })
        );
    }

    #[test]
    fn a_type_name_called_with_one_argument_is_a_cast_unless_a_row_takes_the_argument_type() {
        for (name, argument) in [
            ("int4", Unknown),
            ("text", Known(INT4)),
            ("text", Known(VARCHAR)),
            ("int4", Known(TEXT)),
            ("bpchar", Known(INT4)),
        ] {
            assert_eq!(
                resolve(name, &[argument]),
                Resolution::Unsupported(Reason::Cast),
                "{name}({argument:?})"
            );
        }
        assert_eq!(
            resolve("int4", &[Known(INT8)]),
            resolved(480, &[INT8], &[ArgCoercion::None], INT4)
        );
        assert_eq!(
            resolve("text", &[Known(BOOL)]),
            resolved(2971, &[BOOL], &[ArgCoercion::None], TEXT)
        );
        assert_eq!(
            resolve("float8", &[Known(INT2)]),
            resolved(235, &[INT2], &[ArgCoercion::None], FLOAT8)
        );
    }

    #[test]
    fn a_call_that_needs_a_rule_not_translated_yet_is_not_supported() {
        for (name, arguments, reason) in [
            ("concat", vec![Known(TEXT), Known(TEXT)], Reason::Variadic),
            ("pg_terminate_backend", vec![Known(INT4)], Reason::Defaults),
            (
                "array_append",
                vec![Known(INT4_ARRAY), Known(INT8)],
                Reason::Array,
            ),
            (
                "width_bucket",
                vec![Known(NUMERIC), Known(INT4_ARRAY)],
                Reason::Array,
            ),
            ("count", vec![Known(INT4)], Reason::Aggregate),
            ("any_value", vec![Unknown], Reason::Aggregate),
            ("sum", vec![Known(INT4)], Reason::Aggregate),
            ("internal_in", vec![Unknown], Reason::Internal),
            ("abs", vec![Known(RECORDOID)], Reason::Composite),
            ("abs", vec![Known(999_999)], Reason::MissingType),
            ("abs", vec![Known(INT4); 101], Reason::TooManyArguments),
        ] {
            assert_eq!(
                resolve(name, &arguments),
                Resolution::Unsupported(reason),
                "{name}"
            );
        }
    }

    #[test]
    fn a_range_argument_fills_a_polymorphic_range_parameter() {
        let int4multirange = 4451;
        let numrange = 3906;
        assert_eq!(
            resolve("lower", &[Known(INT4RANGE)]),
            resolved(3848, &[ANYRANGEOID], &[ArgCoercion::None], INT4)
        );
        assert_eq!(
            resolve("range_merge", &[Known(INT4RANGE), Known(INT4RANGE)]),
            resolved(
                4057,
                &[ANYRANGEOID, ANYRANGEOID],
                &[ArgCoercion::None, ArgCoercion::None],
                INT4RANGE
            )
        );
        assert_eq!(
            resolve("multirange", &[Known(INT4RANGE)]),
            resolved(4298, &[ANYRANGEOID], &[ArgCoercion::None], int4multirange)
        );
        assert_eq!(
            resolve("range_merge", &[Known(int4multirange)]),
            resolved(4228, &[ANYMULTIRANGEOID], &[ArgCoercion::None], INT4RANGE)
        );
        assert!(matches!(
            resolve("range_merge", &[Known(INT4RANGE), Known(numrange)]),
            Resolution::Error(ResolveError::NoMatch { .. })
        ));
    }

    #[test]
    fn a_polymorphic_row_that_cannot_take_the_arguments_is_refused() {
        for (name, arguments, message) in [
            (
                "lower",
                vec![Known(INT4)],
                "function lower(integer) does not exist",
            ),
            (
                "enum_first",
                vec![Known(INT4)],
                "function enum_first(integer) does not exist",
            ),
            (
                "enum_range",
                vec![Unknown],
                "function enum_range(unknown) does not exist",
            ),
            (
                "anytextcat",
                vec![Known(INT4_ARRAY), Known(TEXT)],
                "function anytextcat(integer[], text) does not exist",
            ),
            (
                "array_ndims",
                vec![Known(INT4)],
                "function array_ndims(integer) does not exist",
            ),
            (
                "array_cat",
                vec![Known(INT4_ARRAY), Known(TEXT_ARRAY)],
                "function array_cat(integer[], text[]) does not exist",
            ),
            (
                "array_append",
                vec![Known(INT4_ARRAY), Known(TEXT)],
                "function array_append(integer[], text) does not exist",
            ),
            (
                "array_larger",
                vec![Known(INT4_ARRAY), Known(INT8_ARRAY)],
                "function array_larger(integer[], bigint[]) does not exist",
            ),
            (
                "array_append",
                vec![Known(ANYARRAYOID), Known(INT4)],
                "function array_append(anyarray, integer) does not exist",
            ),
        ] {
            let Resolution::Error(error) = resolve(name, &arguments) else {
                panic!("{message}");
            };
            assert_eq!((error.sqlstate(), error.message()), ("42883", message));
        }
    }

    #[test]
    fn a_polymorphic_row_keeps_its_parameter_types_and_takes_its_result_from_the_arguments() {
        assert_eq!(
            resolve("array_length", &[Known(INT4_ARRAY), Known(INT4)]),
            resolved(
                2176,
                &[ANYARRAYOID, INT4],
                &[ArgCoercion::None, ArgCoercion::None],
                INT4
            )
        );
        assert_eq!(
            resolve("array_fill", &[Known(INT4), Known(INT4_ARRAY)]),
            resolved(
                1193,
                &[ANYELEMENTOID, INT4_ARRAY],
                &[ArgCoercion::None, ArgCoercion::None],
                INT4_ARRAY
            )
        );
        assert_eq!(
            resolve("unnest", &[Known(INT4_ARRAY)]),
            resolved(2331, &[ANYARRAYOID], &[ArgCoercion::None], INT4)
        );
        assert_eq!(
            resolve("quote_literal", &[Known(INT4)]),
            resolved(1285, &[ANYELEMENTOID], &[ArgCoercion::None], TEXT)
        );
        assert_eq!(
            resolve("array_to_string", &[Known(ANYARRAYOID), Known(TEXT)]),
            resolved(
                395,
                &[ANYARRAYOID, TEXT],
                &[ArgCoercion::None, ArgCoercion::None],
                TEXT
            )
        );
    }

    #[test]
    fn a_quoted_literal_of_a_polymorphic_parameter_takes_the_type_the_other_arguments_give() {
        assert_eq!(
            resolve("array_larger", &[Known(INT4_ARRAY), Unknown]),
            resolved(
                515,
                &[ANYARRAYOID, INT4_ARRAY],
                &[ArgCoercion::None, ArgCoercion::Literal],
                INT4_ARRAY
            )
        );
        assert_eq!(
            resolve("array_position", &[Known(INT4_ARRAY), Unknown]),
            resolved(
                3277,
                &[INT4_ARRAY, INT4],
                &[ArgCoercion::None, ArgCoercion::Literal],
                INT4
            )
        );
        assert_eq!(
            resolve("array_append", &[Unknown, Known(INT4)]),
            resolved(
                378,
                &[INT4_ARRAY, INT4],
                &[ArgCoercion::Literal, ArgCoercion::None],
                INT4_ARRAY
            )
        );
        assert_eq!(
            resolve("quote_literal", &[Unknown]),
            resolved(1283, &[TEXT], &[ArgCoercion::Literal], TEXT)
        );
    }

    #[test]
    fn the_arguments_of_anycompatible_parameters_are_cast_to_their_common_type() {
        assert_eq!(
            resolve("array_append", &[Known(INT4_ARRAY), Known(INT4)]),
            resolved(
                378,
                &[INT4_ARRAY, INT4],
                &[ArgCoercion::None, ArgCoercion::None],
                INT4_ARRAY
            )
        );
        assert_eq!(
            resolve("array_position", &[Known(INT8_ARRAY), Known(INT4)]),
            resolved(
                3277,
                &[INT8_ARRAY, INT8],
                &[ArgCoercion::None, ArgCoercion::Function(481)],
                INT4
            )
        );
        assert_eq!(
            resolve("width_bucket", &[Known(INT4), Known(INT4_ARRAY)]),
            resolved(
                3218,
                &[INT4, INT4_ARRAY],
                &[ArgCoercion::None, ArgCoercion::None],
                INT4
            )
        );
    }

    #[test]
    fn a_parameter_of_type_any_leaves_its_argument_as_it_is() {
        for argument in [Known(INT4), Unknown] {
            assert_eq!(
                resolve("pg_typeof", &[argument]),
                resolved(1619, &[ANYOID], &[ArgCoercion::None], REGTYPE)
            );
        }
    }

    #[test]
    fn a_polymorphic_row_fails_when_its_arguments_do_not_give_the_polymorphic_type() {
        for (name, arguments, message) in [
            (
                "to_json",
                vec![Unknown],
                "could not determine polymorphic type because input has type unknown",
            ),
            (
                "array_ndims",
                vec![Unknown],
                "could not determine polymorphic type because input has type unknown",
            ),
            (
                "array_length",
                vec![Unknown, Known(INT4)],
                "could not determine polymorphic type because input has type unknown",
            ),
            (
                "arraycontains",
                vec![Known(ANYARRAYOID), Known(ANYARRAYOID)],
                "cannot determine element type of \"anyarray\" argument",
            ),
        ] {
            let Resolution::Error(error) = resolve(name, &arguments) else {
                panic!("{name}");
            };
            assert_eq!(
                (error.sqlstate(), error.message(), error.hint()),
                ("42804", message, None),
                "{name}"
            );
        }
    }

    #[test]
    fn only_the_rows_that_take_the_arguments_by_implicit_cast_stay_candidates() {
        let catalog = FixtureCatalog::get();
        let candidates = |argument| {
            let raw_candidates =
                funcname_get_candidates(catalog, "abs", 1).expect("abs is supported");
            let mut oids: Vec<i64> = func_match_argtypes(catalog, &[argument], raw_candidates)
                .expect("the types of abs are supported")
                .iter()
                .map(|candidate| candidate.oid)
                .collect();
            oids.sort_unstable();
            oids
        };
        assert_eq!(candidates(UNKNOWNOID), [1394, 1395, 1396, 1397, 1398, 1705]);
        assert_eq!(candidates(INT2), [1394, 1395, 1396, 1397, 1398, 1705]);
        assert_eq!(candidates(INT8), [1394, 1395, 1396, 1705]);
        assert_eq!(candidates(NUMERIC), [1394, 1395, 1705]);
        assert_eq!(candidates(FLOAT8), [1395]);
        assert_eq!(candidates(TEXT), Vec::<i64>::new());
    }

    #[test]
    fn a_signature_names_the_types_as_format_type_does() {
        let catalog = FixtureCatalog::get();
        assert_eq!(func_signature_string(catalog, "now", &[]), "now()");
        assert_eq!(
            func_signature_string(
                catalog,
                "f",
                &[UNKNOWNOID, VARCHAR, TIMESTAMPTZ, 1007, FLOAT8]
            ),
            "f(unknown, character varying, timestamp with time zone, integer[], double precision)"
        );
    }
}
