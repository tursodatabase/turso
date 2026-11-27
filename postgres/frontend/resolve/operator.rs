// Translated from PostgreSQL 18.6, src/backend/parser/parse_oper.c. See licenses/postgres-license.md.

use super::candidates::{opername_get_candidates, opername_get_oprid, FuncCandidate};
use super::catalog::{get_base_type, Catalog, OperatorRow, INTERNALOID, INVALID_OID, UNKNOWNOID};
use super::coerce::enforce_generic_type_consistency;
use super::error::{operator_does_not_exist, operator_is_not_unique, operator_is_only_a_shell};
use super::function::{
    func_match_argtypes, func_select_candidate, make_fn_arguments, FuncDetailCode,
};
use super::{ArgType, Reason, ResolveError, ResolvedOperator};

pub(crate) fn make_op(
    catalog: &dyn Catalog,
    opname: &str,
    ltree: Option<ArgType>,
    rtree: ArgType,
) -> Result<Result<ResolvedOperator, ResolveError>, Reason> {
    let rtype_id = expr_type(rtree);
    let (ltype_id, tup) = match ltree {
        None => (INVALID_OID, left_oper(catalog, opname, rtype_id)?),
        Some(ltree) => {
            let ltype_id = expr_type(ltree);
            (ltype_id, oper(catalog, opname, ltype_id, rtype_id)?)
        }
    };

    let opform = match tup {
        Ok(opform) => opform,
        Err(error) => return Ok(Err(error)),
    };

    if opform.function == INVALID_OID {
        return Ok(Err(operator_is_only_a_shell(&op_signature_string(
            catalog,
            opname,
            opform.left,
            opform.right,
        ))));
    }

    let (actual_arg_types, mut declared_arg_types) = if ltree.is_none() {
        (vec![rtype_id], vec![opform.right])
    } else {
        (vec![ltype_id, rtype_id], vec![opform.left, opform.right])
    };

    let rettype = match enforce_generic_type_consistency(
        catalog,
        &actual_arg_types,
        &mut declared_arg_types,
        opform.result,
        false,
    )? {
        Ok(rettype) => rettype,
        Err(error) => return Ok(Err(error)),
    };

    if declared_arg_types.contains(&INTERNALOID) || rettype == INTERNALOID {
        return Err(Reason::Internal);
    }

    let coercions = make_fn_arguments(catalog, &actual_arg_types, &declared_arg_types)?;

    Ok(Ok(ResolvedOperator {
        oid: opform.oid,
        function: opform.function,
        operand_types: declared_arg_types,
        coercions,
        result_type: rettype,
    }))
}

fn expr_type(expr: ArgType) -> i64 {
    match expr {
        ArgType::Known(type_id) => type_id,
        ArgType::Unknown => UNKNOWNOID,
    }
}

pub(crate) fn oper(
    catalog: &dyn Catalog,
    opname: &str,
    mut ltype_id: i64,
    mut rtype_id: i64,
) -> Result<Result<OperatorRow, ResolveError>, Reason> {
    let mut fdresult = FuncDetailCode::NotFound;

    let mut oper_oid = binary_oper_exact(catalog, opname, ltype_id, rtype_id)?;
    if oper_oid == INVALID_OID {
        let clist = opername_get_candidates(catalog, opname, 'b');

        if !clist.is_empty() {
            if rtype_id == INVALID_OID {
                rtype_id = ltype_id;
            } else if ltype_id == INVALID_OID {
                ltype_id = rtype_id;
            }
            let input_oids = [ltype_id, rtype_id];
            (fdresult, oper_oid) = oper_select_candidate(catalog, &input_oids, clist)?;
        }
    }

    if oper_oid != INVALID_OID {
        return Ok(Ok(operator_with_oid(catalog, opname, oper_oid)));
    }

    Ok(Err(op_error(catalog, opname, ltype_id, rtype_id, fdresult)))
}

pub(crate) fn left_oper(
    catalog: &dyn Catalog,
    op: &str,
    arg: i64,
) -> Result<Result<OperatorRow, ResolveError>, Reason> {
    let mut fdresult = FuncDetailCode::NotFound;

    let mut oper_oid = opername_get_oprid(catalog, op, INVALID_OID, arg);
    if oper_oid == INVALID_OID {
        let mut clist = opername_get_candidates(catalog, op, 'l');

        if !clist.is_empty() {
            for clisti in &mut clist {
                clisti.args = vec![clisti.args[1]];
            }

            (fdresult, oper_oid) = oper_select_candidate(catalog, &[arg], clist)?;
        }
    }

    if oper_oid != INVALID_OID {
        return Ok(Ok(operator_with_oid(catalog, op, oper_oid)));
    }

    Ok(Err(op_error(catalog, op, INVALID_OID, arg, fdresult)))
}

fn binary_oper_exact(
    catalog: &dyn Catalog,
    opname: &str,
    mut arg1: i64,
    mut arg2: i64,
) -> Result<i64, Reason> {
    let mut was_unknown = false;

    if arg1 == UNKNOWNOID && arg2 != INVALID_OID {
        arg1 = arg2;
        was_unknown = true;
    } else if arg2 == UNKNOWNOID && arg1 != INVALID_OID {
        arg2 = arg1;
        was_unknown = true;
    }

    let result = opername_get_oprid(catalog, opname, arg1, arg2);
    if result != INVALID_OID {
        return Ok(result);
    }

    if was_unknown {
        let basetype = get_base_type(catalog, arg1)?;

        if basetype != arg1 {
            let result = opername_get_oprid(catalog, opname, basetype, basetype);
            if result != INVALID_OID {
                return Ok(result);
            }
        }
    }

    Ok(INVALID_OID)
}

fn oper_select_candidate(
    catalog: &dyn Catalog,
    input_typeids: &[i64],
    candidates: Vec<FuncCandidate>,
) -> Result<(FuncDetailCode, i64), Reason> {
    let mut candidates = func_match_argtypes(catalog, input_typeids, candidates)?;

    if candidates.is_empty() {
        return Ok((FuncDetailCode::NotFound, INVALID_OID));
    }
    if candidates.len() == 1 {
        return Ok((FuncDetailCode::Normal, candidates.remove(0).oid));
    }

    let candidate = func_select_candidate(catalog, input_typeids, candidates)?;

    if let Some(candidate) = candidate {
        return Ok((FuncDetailCode::Normal, candidate.oid));
    }

    Ok((FuncDetailCode::Multiple, INVALID_OID))
}

fn operator_with_oid(catalog: &dyn Catalog, opname: &str, oper_oid: i64) -> OperatorRow {
    catalog
        .pg_operator(opname)
        .into_iter()
        .find(|opform| opform.oid == oper_oid)
        .unwrap_or_else(|| panic!("cache lookup failed for operator {oper_oid}"))
}

fn op_error(
    catalog: &dyn Catalog,
    op: &str,
    arg1: i64,
    arg2: i64,
    fdresult: FuncDetailCode,
) -> ResolveError {
    let signature = op_signature_string(catalog, op, arg1, arg2);
    if fdresult == FuncDetailCode::Multiple {
        operator_is_not_unique(&signature)
    } else {
        operator_does_not_exist(&signature, arg1 != INVALID_OID && arg2 != INVALID_OID)
    }
}

pub(crate) fn op_signature_string(catalog: &dyn Catalog, op: &str, arg1: i64, arg2: i64) -> String {
    let mut argbuf = String::new();

    if arg1 != INVALID_OID {
        argbuf.push_str(&catalog.format_type_name(arg1));
        argbuf.push(' ');
    }

    argbuf.push_str(op);

    argbuf.push(' ');
    argbuf.push_str(&catalog.format_type_name(arg2));

    argbuf
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolve::catalog::fixtures::FixtureCatalog;
    use crate::resolve::oracle::{oracle_cases, OracleCase};
    use crate::resolve::{resolve_operator, ArgCoercion, Resolution};
    use std::collections::BTreeMap;

    fn operand(type_id: i64) -> ArgType {
        match type_id {
            UNKNOWNOID => ArgType::Unknown,
            known => ArgType::Known(known),
        }
    }

    fn resolve_case(case: &OracleCase) -> Resolution<ResolvedOperator> {
        let catalog = FixtureCatalog::get();
        match case.args.as_slice() {
            [right] => resolve_operator(catalog, &case.name, None, operand(*right)),
            [left, right] => {
                resolve_operator(catalog, &case.name, Some(operand(*left)), operand(*right))
            }
            operands => panic!("an operator with {} operands", operands.len()),
        }
    }

    fn coercions_as_the_oracle_writes_them(
        case: &OracleCase,
        operator: &ResolvedOperator,
    ) -> Vec<String> {
        case.args
            .iter()
            .zip(&operator.operand_types)
            .zip(&operator.coercions)
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

    fn difference_from_the_oracle(
        case: &OracleCase,
        resolution: &Resolution<ResolvedOperator>,
    ) -> Option<String> {
        match (resolution, case.chosen, case.sqlstate.as_deref()) {
            (Resolution::Unsupported(_), _, _) => None,
            (Resolution::Resolved(operator), Some(chosen), None) => {
                let ours = (
                    operator.oid,
                    Some(operator.function),
                    Some(operator.result_type),
                    coercions_as_the_oracle_writes_them(case, operator),
                );
                let theirs = (chosen, case.function, case.result, case.coercions.clone());
                (ours != theirs).then(|| format!("resolved to {ours:?}, expected {theirs:?}"))
            }
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
    fn every_operator_of_the_oracle_gets_the_answer_of_postgresql() {
        let mut differences = Vec::new();
        let mut outcomes: BTreeMap<String, usize> = BTreeMap::new();
        let mut cases = 0;
        for case in oracle_cases() {
            if case.kind != "operator" {
                continue;
            }
            cases += 1;
            let resolution = resolve_case(case);
            if let Some(difference) = difference_from_the_oracle(case, &resolution) {
                differences.push(format!("{} {:?}: {difference}", case.name, case.args));
            }
            let outcome = match &resolution {
                Resolution::Resolved(_) => "resolved".to_string(),
                Resolution::Error(error) => format!("error {}", error.sqlstate()),
                Resolution::Unsupported(reason) => format!("not supported: {reason:?}"),
            };
            *outcomes.entry(outcome).or_default() += 1;
        }
        println!("{cases} operators: {outcomes:#?}");
        assert!(
            differences.is_empty(),
            "{} differences, the first ones: {:#?}",
            differences.len(),
            &differences[..differences.len().min(40)]
        );
        let expected = [
            ("error 42725", 61),
            ("error 42883", 936),
            ("not supported: Array", 16),
            ("resolved", 2006),
        ];
        assert_eq!(
            outcomes,
            expected
                .into_iter()
                .map(|(outcome, count)| (outcome.to_string(), count))
                .collect()
        );
    }

    const BOOL: i64 = 16;
    const NAME: i64 = 19;
    const INT2: i64 = 21;
    const INT4: i64 = 23;
    const TEXT: i64 = 25;
    const OID: i64 = 26;
    const FLOAT4: i64 = 700;
    const FLOAT8: i64 = 701;
    const BPCHAR: i64 = 1042;
    const VARCHAR: i64 = 1043;
    const DATE: i64 = 1082;
    const TIMESTAMP: i64 = 1114;
    const NUMERIC: i64 = 1700;
    const REGCLASS: i64 = 2205;
    const UUID: i64 = 2950;
    const JSONB: i64 = 3802;
    const INT4_ARRAY: i64 = 1007;
    const INT8_ARRAY: i64 = 1016;
    const INT8: i64 = 20;
    const INT4RANGE: i64 = 3904;
    const INT4MULTIRANGE: i64 = 4451;
    const ANYARRAYOID: i64 = 2277;
    const ANYNONARRAYOID: i64 = 2776;

    use ArgCoercion::{Function, Literal, Relabel};
    use ArgType::{Known, Unknown};

    fn binary(name: &str, left: ArgType, right: ArgType) -> Resolution<ResolvedOperator> {
        resolve_operator(FixtureCatalog::get(), name, Some(left), right)
    }

    fn prefix(name: &str, right: ArgType) -> Resolution<ResolvedOperator> {
        resolve_operator(FixtureCatalog::get(), name, None, right)
    }

    fn resolved(
        oid: i64,
        function: i64,
        operand_types: &[i64],
        coercions: &[ArgCoercion],
        result_type: i64,
    ) -> Resolution<ResolvedOperator> {
        Resolution::Resolved(ResolvedOperator {
            oid,
            function,
            operand_types: operand_types.to_vec(),
            coercions: coercions.to_vec(),
            result_type,
        })
    }

    fn error_of(
        resolution: Resolution<ResolvedOperator>,
    ) -> (&'static str, String, Option<&'static str>) {
        let Resolution::Error(error) = resolution else {
            panic!("{resolution:?} is not an error");
        };
        (error.sqlstate(), error.message().to_string(), error.hint())
    }

    const NO_MATCH_FOR_TWO: &str =
        "No operator matches the given name and argument types. You might need to add explicit type casts.";
    const NO_MATCH_FOR_ONE: &str =
        "No operator matches the given name and argument type. You might need to add an explicit type cast.";
    const NOT_UNIQUE: &str =
        "Could not choose a best candidate operator. You might need to add explicit type casts.";

    #[test]
    fn an_operator_whose_operand_types_are_the_types_of_the_operands_is_chosen_first() {
        assert_eq!(
            binary("+", Known(INT4), Known(INT4)),
            resolved(551, 177, &[INT4, INT4], &[ArgCoercion::None; 2], INT4)
        );
        assert_eq!(
            binary("=", Known(NAME), Known(TEXT)),
            resolved(254, 240, &[NAME, TEXT], &[ArgCoercion::None; 2], BOOL)
        );
        assert_eq!(
            binary("=", Known(TIMESTAMP), Known(DATE)),
            resolved(
                2373,
                2366,
                &[TIMESTAMP, DATE],
                &[ArgCoercion::None; 2],
                BOOL
            )
        );
    }

    #[test]
    fn a_quoted_literal_next_to_a_typed_operand_takes_the_type_of_that_operand() {
        assert_eq!(
            binary("=", Known(INT4), Unknown),
            resolved(96, 65, &[INT4, INT4], &[ArgCoercion::None, Literal], BOOL)
        );
        assert_eq!(
            binary("-", Unknown, Known(INT2)),
            resolved(554, 180, &[INT2, INT2], &[Literal, ArgCoercion::None], INT2)
        );
        assert_eq!(
            binary("=", Unknown, Known(UUID)),
            resolved(
                2972,
                2956,
                &[UUID, UUID],
                &[Literal, ArgCoercion::None],
                BOOL
            )
        );
    }

    #[test]
    fn a_quoted_literal_next_to_a_type_without_its_own_operator_follows_the_rules_of_a_function() {
        assert_eq!(
            binary("~~", Known(VARCHAR), Unknown),
            resolved(1209, 850, &[TEXT, TEXT], &[Relabel, Literal], BOOL)
        );
        assert_eq!(
            binary("->", Known(JSONB), Unknown),
            resolved(
                3211,
                3478,
                &[JSONB, TEXT],
                &[ArgCoercion::None, Literal],
                JSONB
            )
        );
    }

    #[test]
    fn two_quoted_literals_are_strings_when_an_operator_takes_strings() {
        assert_eq!(
            binary("=", Unknown, Unknown),
            resolved(98, 67, &[TEXT, TEXT], &[Literal, Literal], BOOL)
        );
        assert_eq!(
            binary("||", Unknown, Unknown),
            resolved(654, 1258, &[TEXT, TEXT], &[Literal, Literal], TEXT)
        );
    }

    #[test]
    fn operands_are_cast_implicitly_to_the_operand_types_of_the_best_operator() {
        assert_eq!(
            binary("+", Known(INT2), Known(FLOAT8)),
            resolved(
                591,
                218,
                &[FLOAT8, FLOAT8],
                &[Function(235), ArgCoercion::None],
                FLOAT8
            )
        );
        assert_eq!(
            binary("*", Known(FLOAT4), Known(INT4)),
            resolved(
                1119,
                279,
                &[FLOAT4, FLOAT8],
                &[ArgCoercion::None, Function(316)],
                FLOAT8
            )
        );
        assert_eq!(
            binary("=", Known(INT4), Known(NUMERIC)),
            resolved(
                1752,
                1718,
                &[NUMERIC, NUMERIC],
                &[Function(1740), ArgCoercion::None],
                BOOL
            )
        );
        assert_eq!(
            binary("+", Known(DATE), Known(INT2)),
            resolved(
                1100,
                1141,
                &[DATE, INT4],
                &[ArgCoercion::None, Function(313)],
                DATE
            )
        );
        assert_eq!(
            binary("||", Known(VARCHAR), Known(VARCHAR)),
            resolved(654, 1258, &[TEXT, TEXT], &[Relabel, Relabel], TEXT)
        );
        assert_eq!(
            binary("=", Known(BPCHAR), Known(VARCHAR)),
            resolved(
                1054,
                1048,
                &[BPCHAR, BPCHAR],
                &[ArgCoercion::None, Relabel],
                BOOL
            )
        );
        assert_eq!(
            binary("=", Known(REGCLASS), Known(INT4)),
            resolved(607, 184, &[OID, OID], &[Relabel, Relabel], BOOL)
        );
    }

    #[test]
    fn no_operator_is_chosen_when_an_operand_has_no_implicit_cast_to_any_operator() {
        for (name, left, right, message) in [
            ("=", TEXT, INT4, "operator does not exist: text = integer"),
            ("+", TEXT, INT4, "operator does not exist: text + integer"),
            ("=", UUID, TEXT, "operator does not exist: uuid = text"),
            (
                "=",
                BOOL,
                INT4,
                "operator does not exist: boolean = integer",
            ),
            (
                "||",
                INT4,
                INT4,
                "operator does not exist: integer || integer",
            ),
            (
                "no such",
                INT4,
                INT4,
                "operator does not exist: integer no such integer",
            ),
        ] {
            assert_eq!(
                error_of(binary(name, Known(left), Known(right))),
                ("42883", message.to_string(), Some(NO_MATCH_FOR_TWO))
            );
        }
    }

    #[test]
    fn operands_that_fit_several_operators_equally_well_are_ambiguous() {
        assert_eq!(
            error_of(binary("+", Unknown, Unknown)),
            (
                "42725",
                "operator is not unique: unknown + unknown".to_string(),
                Some(NOT_UNIQUE)
            )
        );
    }

    #[test]
    fn a_prefix_operator_is_chosen_from_the_type_of_its_one_operand() {
        assert_eq!(
            prefix("-", Known(INT4)),
            resolved(558, 212, &[INT4], &[ArgCoercion::None], INT4)
        );
        assert_eq!(
            prefix("-", Known(INT2)),
            resolved(559, 213, &[INT2], &[ArgCoercion::None], INT2)
        );
        assert_eq!(
            prefix("@", Known(INT2)),
            resolved(682, 1253, &[INT2], &[ArgCoercion::None], INT2)
        );
        assert_eq!(
            error_of(prefix("-", Known(TEXT))),
            (
                "42883",
                "operator does not exist: - text".to_string(),
                Some(NO_MATCH_FOR_ONE)
            )
        );
        assert_eq!(
            error_of(prefix("-", Unknown)),
            (
                "42725",
                "operator is not unique: - unknown".to_string(),
                Some(NOT_UNIQUE)
            )
        );
        assert_eq!(
            error_of(prefix("||", Known(TEXT))),
            (
                "42883",
                "operator does not exist: || text".to_string(),
                Some(NO_MATCH_FOR_ONE)
            )
        );
    }

    #[test]
    fn a_polymorphic_operator_keeps_its_operand_types_and_takes_its_result_from_the_operands() {
        assert_eq!(
            binary("=", Known(INT4_ARRAY), Known(INT4_ARRAY)),
            resolved(
                1070,
                744,
                &[ANYARRAYOID, ANYARRAYOID],
                &[ArgCoercion::None; 2],
                BOOL
            )
        );
        assert_eq!(
            binary("&&", Known(INT4_ARRAY), Known(INT4_ARRAY)),
            resolved(
                2750,
                2747,
                &[ANYARRAYOID, ANYARRAYOID],
                &[ArgCoercion::None; 2],
                BOOL
            )
        );
        assert_eq!(
            binary("||", Known(TEXT), Known(INT4)),
            resolved(
                2779,
                2003,
                &[TEXT, ANYNONARRAYOID],
                &[ArgCoercion::None; 2],
                TEXT
            )
        );
        assert_eq!(
            binary("||", Unknown, Known(INT4)),
            resolved(
                2779,
                2003,
                &[TEXT, ANYNONARRAYOID],
                &[Literal, ArgCoercion::None],
                TEXT
            )
        );
    }

    #[test]
    fn a_quoted_literal_of_a_polymorphic_operand_takes_the_type_of_the_other_operand() {
        assert_eq!(
            binary("=", Known(INT4_ARRAY), Unknown),
            resolved(
                1070,
                744,
                &[ANYARRAYOID, INT4_ARRAY],
                &[ArgCoercion::None, Literal],
                BOOL
            )
        );
        assert_eq!(
            binary("@>", Known(INT4_ARRAY), Unknown),
            resolved(
                2751,
                2748,
                &[ANYARRAYOID, INT4_ARRAY],
                &[ArgCoercion::None, Literal],
                BOOL
            )
        );
        assert_eq!(
            binary("||", Known(INT4_ARRAY), Unknown),
            resolved(
                375,
                383,
                &[INT4_ARRAY, INT4_ARRAY],
                &[ArgCoercion::None, Literal],
                INT4_ARRAY
            )
        );
    }

    #[test]
    fn the_operands_of_anycompatible_operand_types_are_cast_to_their_common_type() {
        assert_eq!(
            binary("||", Known(INT4_ARRAY), Known(INT4)),
            resolved(
                349,
                378,
                &[INT4_ARRAY, INT4],
                &[ArgCoercion::None; 2],
                INT4_ARRAY
            )
        );
        assert_eq!(
            binary("||", Known(INT8_ARRAY), Known(INT4)),
            resolved(
                349,
                378,
                &[INT8_ARRAY, INT8],
                &[ArgCoercion::None, Function(481)],
                INT8_ARRAY
            )
        );
        assert_eq!(
            binary("||", Known(INT4), Known(INT4_ARRAY)),
            resolved(
                374,
                379,
                &[INT4, INT4_ARRAY],
                &[ArgCoercion::None; 2],
                INT4_ARRAY
            )
        );
    }

    #[test]
    fn a_polymorphic_operator_that_cannot_take_the_operands_is_refused() {
        for (name, left, right, message) in [
            (
                "=",
                INT4_ARRAY,
                INT8_ARRAY,
                "operator does not exist: integer[] = bigint[]",
            ),
            (
                "@>",
                INT4_ARRAY,
                INT8_ARRAY,
                "operator does not exist: integer[] @> bigint[]",
            ),
            (
                "=",
                INT4,
                INT4_ARRAY,
                "operator does not exist: integer = integer[]",
            ),
            (
                "||",
                INT4_ARRAY,
                TEXT,
                "operator does not exist: integer[] || text",
            ),
        ] {
            assert_eq!(
                error_of(binary(name, Known(left), Known(right))),
                ("42883", message.to_string(), Some(NO_MATCH_FOR_TWO))
            );
        }
    }

    #[test]
    fn an_operator_that_needs_a_rule_not_translated_yet_is_not_supported() {
        for (name, left, right, reason) in [
            ("||", INT4_ARRAY, INT8, Reason::Array),
            ("=", 999_999, INT4, Reason::MissingType),
        ] {
            assert_eq!(
                binary(name, Known(left), Known(right)),
                Resolution::Unsupported(reason),
                "{name}"
            );
        }
    }

    #[test]
    fn a_range_operand_fills_the_range_parameter_of_an_operator() {
        let anyrange = 3831;
        let anyelement = 2283;
        let anymultirange = 4537;
        for (name, left, right, oid, function, operand_types) in [
            ("=", INT4RANGE, INT4RANGE, 3882, 3855, [anyrange, anyrange]),
            ("@>", INT4RANGE, INT4, 3889, 3858, [anyrange, anyelement]),
            (
                "@>",
                INT4MULTIRANGE,
                INT4RANGE,
                2870,
                4250,
                [anymultirange, anyrange],
            ),
        ] {
            assert_eq!(
                binary(name, Known(left), Known(right)),
                Resolution::Resolved(ResolvedOperator {
                    oid,
                    function,
                    operand_types: operand_types.to_vec(),
                    coercions: vec![ArgCoercion::None, ArgCoercion::None],
                    result_type: BOOL,
                }),
                "{name}"
            );
        }
        assert!(matches!(
            binary("@>", Known(INT4RANGE), Known(INT8)),
            Resolution::Error(ResolveError::NoMatch { .. })
        ));
    }

    struct OneShellOperator;

    impl Catalog for OneShellOperator {
        fn pg_type(&self, oid: i64) -> Option<crate::resolve::catalog::TypeRow> {
            FixtureCatalog::get().pg_type(oid)
        }

        fn pg_type_oid(&self, name: &str) -> Option<i64> {
            FixtureCatalog::get().pg_type_oid(name)
        }

        fn pg_cast(&self, source: i64, target: i64) -> Option<crate::resolve::catalog::CastRow> {
            FixtureCatalog::get().pg_cast(source, target)
        }

        fn pg_range(&self, range_type: i64) -> Option<crate::resolve::catalog::RangeRow> {
            FixtureCatalog::get().pg_range(range_type)
        }

        fn pg_range_of_multirange(
            &self,
            multirange_type: i64,
        ) -> Option<crate::resolve::catalog::RangeRow> {
            FixtureCatalog::get().pg_range_of_multirange(multirange_type)
        }

        fn pg_proc(&self, _name: &str) -> Vec<crate::resolve::catalog::ProcRow<'_>> {
            Vec::new()
        }

        fn pg_operator(&self, _name: &str) -> Vec<OperatorRow> {
            vec![OperatorRow {
                oid: 1,
                kind: 'b',
                left: INT4,
                right: TEXT,
                result: INVALID_OID,
                function: INVALID_OID,
            }]
        }

        fn format_type_name(&self, oid: i64) -> String {
            FixtureCatalog::get().format_type_name(oid)
        }
    }

    #[test]
    fn an_operator_without_a_function_is_refused_as_a_shell() {
        let Resolution::Error(error) =
            resolve_operator(&OneShellOperator, "===", Some(Known(INT2)), Unknown)
        else {
            panic!("a shell operator must be refused");
        };
        assert_eq!(
            (error.sqlstate(), error.message(), error.hint()),
            ("42883", "operator is only a shell: integer === text", None)
        );
    }

    #[test]
    fn a_signature_names_the_operand_types_as_format_type_does() {
        let catalog = FixtureCatalog::get();
        assert_eq!(
            op_signature_string(catalog, "<=", VARCHAR, UNKNOWNOID),
            "character varying <= unknown"
        );
        assert_eq!(
            op_signature_string(catalog, "@", INVALID_OID, FLOAT8),
            "@ double precision"
        );
    }
}
