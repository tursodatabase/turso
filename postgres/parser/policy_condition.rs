//! The text of a row-level security condition as `CREATE POLICY` wrote it.
//! The catalogs show it when they cannot print the condition as
//! PostgreSQL's `pg_get_expr` does.

use pg_query::protobuf::{Node, SelectStmt};
use pg_query::NodeEnum;

use crate::ParseError;

/// The text of `condition`, the `USING` or `WITH CHECK` condition of a
/// policy, with its columns named without their table, as `pg_get_expr`
/// prints them. A condition can only read the columns of the policy's
/// table, and without the table's name the text stays valid after the
/// table is renamed. A condition whose text does not parse back is refused,
/// because pg_query does not print every expression as valid SQL.
pub(crate) fn written_condition(condition: &Node) -> Result<String, ParseError> {
    let mut condition = condition.clone();
    for_each_node(&mut condition, &mut |node| {
        if let NodeEnum::ColumnRef(column) = node {
            let names_a_column = matches!(
                column.fields.last().and_then(|field| field.node.as_ref()),
                Some(NodeEnum::String(_))
            );
            if names_a_column && column.fields.len() > 1 {
                column.fields.drain(..column.fields.len() - 1);
            }
        }
    })?;
    let text = deparse_expr(&condition)?;
    if crate::parse(&format!("SELECT {text}")).is_err() {
        return Err(ParseError::not_supported(
            "this expression in a policy condition",
        ));
    }
    Ok(text)
}

/// Calls `visit` with `node` and every node in it. These are the nodes the
/// translator accepts in an expression. A subquery's FROM is not visited,
/// because the engine refuses a policy whose subquery reads a table.
fn for_each_node(node: &mut Node, visit: &mut impl FnMut(&mut NodeEnum)) -> Result<(), ParseError> {
    let Some(node) = node.node.as_mut() else {
        return Ok(());
    };
    visit(node);
    let mut children = Vec::new();
    match node {
        NodeEnum::AConst(_)
        | NodeEnum::ColumnRef(_)
        | NodeEnum::ParamRef(_)
        | NodeEnum::SqlvalueFunction(_)
        | NodeEnum::String(_)
        | NodeEnum::AStar(_)
        | NodeEnum::SetToDefault(_) => {}
        NodeEnum::AExpr(expr) => {
            children.extend(boxed(&mut expr.lexpr).chain(boxed(&mut expr.rexpr)))
        }
        NodeEnum::BoolExpr(expr) => children.extend(&mut expr.args),
        NodeEnum::NullTest(test) => children.extend(boxed(&mut test.arg)),
        NodeEnum::BooleanTest(test) => children.extend(boxed(&mut test.arg)),
        NodeEnum::CaseExpr(case) => children.extend(
            boxed(&mut case.arg)
                .chain(&mut case.args)
                .chain(boxed(&mut case.defresult)),
        ),
        NodeEnum::CaseWhen(when) => {
            children.extend(boxed(&mut when.expr).chain(boxed(&mut when.result)))
        }
        NodeEnum::CoalesceExpr(expr) => children.extend(&mut expr.args),
        NodeEnum::MinMaxExpr(expr) => children.extend(&mut expr.args),
        NodeEnum::CollateClause(collate) => children.extend(boxed(&mut collate.arg)),
        NodeEnum::TypeCast(cast) => children.extend(boxed(&mut cast.arg)),
        NodeEnum::FuncCall(call) => children.extend(
            call.args
                .iter_mut()
                .chain(&mut call.agg_order)
                .chain(boxed(&mut call.agg_filter)),
        ),
        NodeEnum::SortBy(sort) => children.extend(boxed(&mut sort.node)),
        NodeEnum::SubLink(link) => {
            children.extend(boxed(&mut link.testexpr).chain(boxed(&mut link.subselect)))
        }
        NodeEnum::SelectStmt(select) => return for_each_node_of_select(select, visit),
        NodeEnum::ResTarget(target) => {
            children.extend(target.indirection.iter_mut().chain(boxed(&mut target.val)))
        }
        NodeEnum::RowExpr(row) => children.extend(&mut row.args),
        NodeEnum::AArrayExpr(array) => children.extend(&mut array.elements),
        NodeEnum::AIndirection(indirection) => {
            children.extend(boxed(&mut indirection.arg).chain(&mut indirection.indirection))
        }
        NodeEnum::AIndices(indices) => {
            children.extend(boxed(&mut indices.lidx).chain(boxed(&mut indices.uidx)))
        }
        NodeEnum::NamedArgExpr(arg) => children.extend(boxed(&mut arg.arg)),
        NodeEnum::List(list) => children.extend(&mut list.items),
        _ => {
            return Err(ParseError::not_supported(
                "this expression in a policy condition",
            ))
        }
    }
    for child in children {
        for_each_node(child, visit)?;
    }
    Ok(())
}

fn for_each_node_of_select(
    select: &mut SelectStmt,
    visit: &mut impl FnMut(&mut NodeEnum),
) -> Result<(), ParseError> {
    let children = select
        .distinct_clause
        .iter_mut()
        .chain(&mut select.target_list)
        .chain(boxed(&mut select.where_clause))
        .chain(&mut select.group_clause)
        .chain(boxed(&mut select.having_clause))
        .chain(&mut select.values_lists)
        .chain(&mut select.sort_clause)
        .chain(boxed(&mut select.limit_offset))
        .chain(boxed(&mut select.limit_count));
    for child in children {
        for_each_node(child, visit)?;
    }
    for side in [&mut select.larg, &mut select.rarg].into_iter().flatten() {
        for_each_node_of_select(side, visit)?;
    }
    Ok(())
}

fn boxed(node: &mut Option<Box<Node>>) -> impl Iterator<Item = &mut Node> {
    node.as_deref_mut().into_iter()
}

/// The text pg_query prints for `expr`.
fn deparse_expr(expr: &Node) -> Result<String, ParseError> {
    use pg_query::protobuf::{LimitOption, ResTarget, SetOperation};

    let select = NodeEnum::SelectStmt(Box::new(SelectStmt {
        target_list: vec![Node {
            node: Some(NodeEnum::ResTarget(Box::new(ResTarget {
                val: Some(Box::new(expr.clone())),
                ..Default::default()
            }))),
        }],
        op: SetOperation::SetopNone as i32,
        limit_option: LimitOption::Default as i32,
        ..Default::default()
    }));
    let sql = select
        .deparse(pg_query::DeparseOptions::default())
        // pg_query fails to deparse only a node that it does not support.
        .map_err(|e| {
            ParseError::sqlstate(
                crate::sqlstate::SQLSTATE_FEATURE_NOT_SUPPORTED,
                e.to_string(),
            )
        })?;
    sql.strip_prefix("SELECT ")
        .map(str::to_string)
        .ok_or_else(|| {
            ParseError::internal_error(format!("unexpected deparse of an expression: {sql}"))
        })
}
