// Translated from PostgreSQL 18.6, src/backend/catalog/namespace.c. See licenses/postgres-license.md.

use super::catalog::{Catalog, INVALID_OID};
use super::Reason;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FuncCandidate {
    /// 0 when several rows take these argument types.
    pub oid: i64,
    pub args: Vec<i64>,
}

pub(crate) fn funcname_get_candidates(
    catalog: &dyn Catalog,
    funcname: &str,
    nargs: usize,
) -> Result<Vec<FuncCandidate>, Reason> {
    let mut result_list: Vec<FuncCandidate> = Vec::new();

    for procform in catalog.pg_proc(funcname) {
        let proargtypes = procform.arg_types;
        let pronargs = proargtypes.len();

        if pronargs <= nargs && procform.variadic != INVALID_OID {
            return Err(Reason::Variadic);
        }

        if pronargs > nargs {
            if nargs + procform.default_count < pronargs {
                continue;
            }
            return Err(Reason::Defaults);
        }

        if pronargs != nargs {
            continue;
        }

        let prev_result = result_list
            .iter_mut()
            .find(|prev_result| prev_result.args == proargtypes);
        if let Some(prev_result) = prev_result {
            prev_result.oid = INVALID_OID;
            continue;
        }

        result_list.insert(
            0,
            FuncCandidate {
                oid: procform.oid,
                args: proargtypes.to_vec(),
            },
        );
    }

    Ok(result_list)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolve::catalog::fixtures::FixtureCatalog;
    use crate::resolve::catalog::{CastRow, OperatorRow, ProcRow, RangeRow, TypeRow};

    fn oids(candidates: &[FuncCandidate]) -> Vec<i64> {
        let mut oids: Vec<i64> = candidates.iter().map(|candidate| candidate.oid).collect();
        oids.sort_unstable();
        oids
    }

    #[test]
    fn the_candidates_are_the_rows_with_the_name_and_the_argument_count() {
        let catalog = FixtureCatalog::get();
        let one_argument = funcname_get_candidates(catalog, "abs", 1).expect("abs is supported");
        assert_eq!(oids(&one_argument), [1394, 1395, 1396, 1397, 1398, 1705]);
        assert!(one_argument.contains(&FuncCandidate {
            oid: 1396,
            args: vec![20],
        }));
        assert_eq!(funcname_get_candidates(catalog, "abs", 2), Ok(vec![]));
        assert_eq!(funcname_get_candidates(catalog, "abs", 0), Ok(vec![]));
        assert_eq!(funcname_get_candidates(catalog, "no_such", 1), Ok(vec![]));
        let round = funcname_get_candidates(catalog, "round", 2).expect("round is supported");
        assert_eq!(oids(&round), [1707]);
    }

    #[test]
    fn a_function_without_parameters_is_a_candidate_of_a_call_without_arguments() {
        let catalog = FixtureCatalog::get();
        assert_eq!(
            funcname_get_candidates(catalog, "pi", 0),
            Ok(vec![FuncCandidate {
                oid: 1610,
                args: vec![],
            }])
        );
    }

    #[test]
    fn a_variadic_row_that_the_arguments_could_fill_is_not_supported() {
        let catalog = FixtureCatalog::get();
        assert_eq!(
            funcname_get_candidates(catalog, "concat", 1),
            Err(Reason::Variadic)
        );
        assert_eq!(
            funcname_get_candidates(catalog, "concat", 3),
            Err(Reason::Variadic)
        );
        assert_eq!(funcname_get_candidates(catalog, "concat", 0), Ok(vec![]));
        assert_eq!(funcname_get_candidates(catalog, "concat_ws", 1), Ok(vec![]));
        assert_eq!(
            funcname_get_candidates(catalog, "concat_ws", 2),
            Err(Reason::Variadic)
        );
    }

    #[test]
    fn a_row_that_needs_default_arguments_is_not_supported() {
        let catalog = FixtureCatalog::get();
        let with_defaults = catalog
            .pg_proc("pg_terminate_backend")
            .into_iter()
            .find(|proc| proc.default_count == 1 && proc.arg_types.len() == 2)
            .expect("pg_terminate_backend(integer, bigint DEFAULT 0) is in the fixtures");
        assert_eq!(
            funcname_get_candidates(catalog, "pg_terminate_backend", 2),
            Ok(vec![FuncCandidate {
                oid: with_defaults.oid,
                args: vec![23, 20],
            }])
        );
        assert_eq!(
            funcname_get_candidates(catalog, "pg_terminate_backend", 1),
            Err(Reason::Defaults)
        );
        assert_eq!(
            funcname_get_candidates(catalog, "pg_terminate_backend", 0),
            Ok(vec![])
        );
    }

    struct TwoRowsWithTheSameArguments;

    impl Catalog for TwoRowsWithTheSameArguments {
        fn pg_type(&self, _oid: i64) -> Option<TypeRow> {
            None
        }

        fn pg_type_oid(&self, _name: &str) -> Option<i64> {
            None
        }

        fn pg_cast(&self, _source: i64, _target: i64) -> Option<CastRow> {
            None
        }

        fn pg_range(&self, _range_type: i64) -> Option<RangeRow> {
            None
        }

        fn pg_range_of_multirange(&self, _multirange_type: i64) -> Option<RangeRow> {
            None
        }

        fn pg_proc(&self, _name: &str) -> Vec<ProcRow<'_>> {
            let row = |oid, arg_types| ProcRow {
                oid,
                kind: 'f',
                arg_types,
                result: 23,
                variadic: INVALID_OID,
                default_count: 0,
            };
            vec![row(1, &[23]), row(2, &[20]), row(3, &[23])]
        }

        fn pg_operator(&self, _name: &str) -> Vec<OperatorRow> {
            Vec::new()
        }

        fn format_type_name(&self, _oid: i64) -> String {
            String::new()
        }
    }

    #[test]
    fn rows_with_the_same_argument_types_become_one_candidate_without_an_oid() {
        assert_eq!(
            funcname_get_candidates(&TwoRowsWithTheSameArguments, "f", 1),
            Ok(vec![
                FuncCandidate {
                    oid: 2,
                    args: vec![20],
                },
                FuncCandidate {
                    oid: INVALID_OID,
                    args: vec![23],
                },
            ])
        );
    }
}
