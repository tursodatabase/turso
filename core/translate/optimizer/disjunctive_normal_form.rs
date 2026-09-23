use crate::translate::expr::{unwrap_parens, walk_expr, WalkControl};
use turso_parser::ast::{Expr, Operator, UnaryOperator};

const MAX_CONJUNCTIONS_PER_AND: usize = 16;

#[derive(Clone, Copy)]
pub(super) struct Literal<'a> {
    expr: &'a Expr,
    negated: bool,
}

impl Literal<'_> {
    pub(super) fn to_expr(self) -> Expr {
        if self.negated {
            Expr::Unary(UnaryOperator::Not, Box::new(self.expr.clone()))
        } else {
            self.expr.clone()
        }
    }
}

pub(super) fn disjunctive_normal_form(expr: &Expr) -> Vec<Vec<Literal<'_>>> {
    conjunctions(expr, false, MAX_CONJUNCTIONS_PER_AND)
}

pub(super) fn disjunctive_normal_form_without_distribution(expr: &Expr) -> Vec<Vec<Literal<'_>>> {
    conjunctions(expr, false, 1)
}

fn conjunctions(
    expr: &Expr,
    negated: bool,
    max_conjunctions_per_and: usize,
) -> Vec<Vec<Literal<'_>>> {
    let expr = unwrap_parens(expr).unwrap_or(expr);
    match (expr, negated) {
        (Expr::Unary(UnaryOperator::Not, operand), _) => {
            conjunctions(operand, !negated, max_conjunctions_per_and)
        }
        (Expr::Binary(lhs, Operator::Or, rhs), false)
        | (Expr::Binary(lhs, Operator::And, rhs), true) => {
            let mut result = conjunctions(lhs, negated, max_conjunctions_per_and);
            result.extend(conjunctions(rhs, negated, max_conjunctions_per_and));
            result
        }
        (Expr::Binary(lhs, Operator::And, rhs), false)
        | (Expr::Binary(lhs, Operator::Or, rhs), true) => {
            let left = conjunctions(lhs, negated, max_conjunctions_per_and);
            let right = conjunctions(rhs, negated, max_conjunctions_per_and);
            if can_distribute(&left, &right, max_conjunctions_per_and) {
                distribute(&left, &right)
            } else {
                vec![[
                    one_conjunction(left, lhs, negated),
                    one_conjunction(right, rhs, negated),
                ]
                .concat()]
            }
        }
        _ => vec![vec![Literal { expr, negated }]],
    }
}

fn can_distribute(
    left: &[Vec<Literal>],
    right: &[Vec<Literal>],
    max_conjunctions_per_and: usize,
) -> bool {
    left.len().saturating_mul(right.len()) <= max_conjunctions_per_and
        && (right.len() == 1 || all_can_be_copied(left))
        && (left.len() == 1 || all_can_be_copied(right))
}

fn all_can_be_copied(conjunctions: &[Vec<Literal>]) -> bool {
    conjunctions
        .iter()
        .flatten()
        .all(|literal| can_be_copied(literal.expr))
}

fn can_be_copied(expr: &Expr) -> bool {
    let mut can_be_copied = true;
    walk_expr(expr, &mut |expr| {
        if matches!(
            expr,
            Expr::FunctionCall { .. } | Expr::FunctionCallStar { .. } | Expr::SubqueryResult { .. }
        ) {
            can_be_copied = false;
            return Ok(WalkControl::SkipChildren);
        }
        Ok(WalkControl::Continue)
    })
    .expect("walking an expression cannot fail");
    can_be_copied
}

fn distribute<'a>(left: &[Vec<Literal<'a>>], right: &[Vec<Literal<'a>>]) -> Vec<Vec<Literal<'a>>> {
    left.iter()
        .flat_map(|left| {
            right
                .iter()
                .map(move |right| [left.as_slice(), right.as_slice()].concat())
        })
        .collect()
}

fn one_conjunction<'a>(
    mut conjunctions: Vec<Vec<Literal<'a>>>,
    expr: &'a Expr,
    negated: bool,
) -> Vec<Literal<'a>> {
    if conjunctions.len() == 1 {
        conjunctions.pop().expect("length is 1")
    } else {
        vec![Literal { expr, negated }]
    }
}

#[cfg(test)]
mod tests {
    use super::{disjunctive_normal_form, disjunctive_normal_form_without_distribution, Literal};
    use turso_parser::{ast, parser::Parser};

    fn dnf(condition: &str) -> Vec<Vec<String>> {
        to_strings(disjunctive_normal_form(&parse_condition(condition)))
    }

    fn dnf_without_distribution(condition: &str) -> Vec<Vec<String>> {
        to_strings(disjunctive_normal_form_without_distribution(
            &parse_condition(condition),
        ))
    }

    fn to_strings(conjunctions: Vec<Vec<Literal>>) -> Vec<Vec<String>> {
        conjunctions
            .into_iter()
            .map(|conjunction| {
                conjunction
                    .into_iter()
                    .map(|literal| literal.to_expr().to_string())
                    .collect()
            })
            .collect()
    }

    fn parse_condition(condition: &str) -> ast::Expr {
        let sql = format!("SELECT 1 WHERE {condition}");
        let cmd = Parser::new(sql.as_bytes())
            .next_cmd()
            .expect("test SQL must parse")
            .expect("test SQL must contain a statement");
        let ast::Cmd::Stmt(ast::Stmt::Select(select)) = cmd else {
            panic!("expected SELECT statement");
        };
        let ast::OneSelect::Select {
            where_clause: Some(where_clause),
            ..
        } = select.body.select
        else {
            panic!("expected SELECT with WHERE");
        };
        *where_clause
    }

    #[test]
    fn literal_is_one_conjunction() {
        assert_eq!(dnf("a = 1"), [["a = 1"]]);
    }

    #[test]
    fn double_negation_is_removed() {
        assert_eq!(dnf("NOT NOT a"), [["a"]]);
        assert_eq!(dnf("NOT (NOT (a = 1))"), [["a = 1"]]);
    }

    #[test]
    fn not_or_becomes_and_of_nots() {
        assert_eq!(dnf("NOT (a OR b)"), [["NOT a", "NOT b"]]);
    }

    #[test]
    fn not_and_becomes_or_of_nots() {
        assert_eq!(dnf("NOT (a AND b)"), [["NOT a"], ["NOT b"]]);
    }

    #[test]
    fn and_distributes_over_or_on_the_right() {
        assert_eq!(dnf("a AND (b OR c)"), [["a", "b"], ["a", "c"]]);
    }

    #[test]
    fn and_distributes_over_or_on_the_left() {
        assert_eq!(dnf("(a OR b) AND c"), [["a", "c"], ["b", "c"]]);
    }

    #[test]
    fn and_of_two_ors_has_four_conjunctions() {
        assert_eq!(
            dnf("(a OR b) AND (c OR d)"),
            [["a", "c"], ["a", "d"], ["b", "c"], ["b", "d"]]
        );
    }

    #[test]
    fn parentheses_do_not_hide_nested_ors() {
        assert_eq!(dnf("(a OR b) OR c"), [["a"], ["b"], ["c"]]);
        assert_eq!(dnf("a OR ((b OR c))"), [["a"], ["b"], ["c"]]);
    }

    #[test]
    fn rules_apply_at_every_depth() {
        assert_eq!(
            dnf("NOT (NOT a AND NOT (b AND (c OR d)))"),
            vec![vec!["a"], vec!["b", "c"], vec!["b", "d"]]
        );
    }

    #[test]
    fn and_with_too_many_conjunctions_is_not_distributed() {
        assert_eq!(
            dnf("(a OR b) AND (c OR d) AND (e OR f) AND (g OR h) AND (i OR j)"),
            [[
                "(a OR b) AND (c OR d) AND (e OR f) AND (g OR h)",
                "(i OR j)"
            ]]
        );
    }

    #[test]
    fn without_distribution_only_moves_not_to_the_literals() {
        assert_eq!(
            dnf_without_distribution("a AND (b OR c)"),
            [["a", "(b OR c)"]]
        );
        assert_eq!(
            dnf_without_distribution("NOT (NOT a AND NOT (b AND (c OR d)))"),
            vec![vec!["a"], vec!["b", "(c OR d)"]]
        );
    }

    #[test]
    fn function_call_is_not_copied() {
        assert_eq!(
            dnf("random() > 0 AND (a OR b)"),
            [["random () > 0", "(a OR b)"]]
        );
        assert_eq!(
            dnf("(a OR b) AND random() > 0"),
            [["(a OR b)", "random () > 0"]]
        );
    }

    #[test]
    fn function_call_that_stays_in_one_conjunction_is_distributed() {
        assert_eq!(
            dnf("a AND (random() > 0 OR b)"),
            [["a", "random () > 0"], ["a", "b"]]
        );
    }

    #[test]
    fn subquery_is_not_copied() {
        let subquery = ast::Expr::SubqueryResult {
            subquery_id: ast::TableInternalId::from(1),
            lhs: None,
            not_in: false,
            query_type: ast::SubqueryType::Exists { result_reg: 1 },
        };
        let condition = ast::Expr::Binary(
            Box::new(subquery),
            ast::Operator::And,
            Box::new(parse_condition("a OR b")),
        );
        assert_eq!(disjunctive_normal_form(&condition).len(), 1);
    }
}
