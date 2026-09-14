use std::collections::BTreeMap;

use turso_parser::ast::{self, Expr};

use crate::translate::{emitter::Resolver, eqp::JsonBuilder, plan::Plan};
use crate::Result;

use super::{
    binding::bind_query, rewrite, BindError, Column, ColumnId, JoinKind, LogicalPlan, Relation,
    Scalar, Scope,
};

pub(crate) fn inspect_plan(plan: &Plan, resolver: &Resolver, rewrite: bool) -> Result<String> {
    let bound = bind_query(plan, resolver);
    let mut out = String::new();
    match bound {
        Ok(mut plan) => {
            #[cfg(feature = "simulator")]
            let rewrite = rewrite
                && resolver.subquery_unnesting_mode() != crate::SubqueryUnnestingMode::Disabled;
            let report = if rewrite {
                Some(rewrite::normalize(&mut plan)?)
            } else {
                None
            };
            plan.validate()?;
            plan.write_json(&mut out, report.as_ref())?;
        }
        Err(BindError::Unsupported(reason)) => {
            let mut json = JsonBuilder::new(&mut out);
            json.str("status", "legacy");
            json.str("reason", reason);
            json.finish();
        }
        Err(BindError::Error(error)) => return Err(error),
    }
    Ok(out)
}

#[derive(Default)]
struct RuleDeclines {
    dependency: BTreeMap<&'static str, BTreeMap<&'static str, usize>>,
    normalization: BTreeMap<&'static str, BTreeMap<&'static str, usize>>,
}

impl LogicalPlan {
    fn write_json(&self, out: &mut String, report: Option<&rewrite::RewriteReport>) -> Result<()> {
        let mut json = JsonBuilder::new(out);
        json.str("status", "bound");
        if let Some(report) = report {
            let mut diagnostics = JsonBuilder::new(json.key("rewrites"));
            diagnostics.num("pull_dependent_filter", report.dependent_filters_pulled());
            let mut rules = JsonBuilder::new(diagnostics.key("applied_rules"));
            for (name, count) in report.rules() {
                rules.num(name, count);
            }
            rules.finish();
            diagnostics.num("visited", report.visited);
            diagnostics.num("added_nodes", report.added_nodes);
            diagnostics.bool("budget_exhausted", report.exhausted);
            diagnostics.finish();
        }
        let bindings = json.key("bindings");
        bindings.push('[');
        for (index, binding) in self.bindings.iter().enumerate() {
            comma(bindings, index);
            let mut binding_json = JsonBuilder::new(bindings);
            binding_json.num("id", binding.id.into());
            binding_json.str("name", &binding.name);
            let columns = binding_json.key("columns");
            columns.push('[');
            for (index, id) in binding.column_ids().enumerate() {
                comma(columns, index);
                write_column(columns, &binding.column(id));
            }
            columns.push(']');
            let keys = binding_json.key("unique_keys");
            keys.push('[');
            for (index, key) in binding.unique_keys().iter().enumerate() {
                comma(keys, index);
                write_columns(keys, key.iter().copied());
            }
            keys.push(']');
            binding_json.finish();
        }
        bindings.push(']');
        write_columns(
            json.key("outer_references"),
            self.outer_columns.iter().copied(),
        );
        json.num_array(
            "retained_parameters",
            self.parameters
                .iter()
                .map(|parameter| parameter.index.get() as usize),
        );
        let shared = json.key("shared_inputs");
        shared.push('[');
        let mut declines = RuleDeclines::default();
        for (index, input) in self.shared_inputs.iter().enumerate() {
            comma(shared, index);
            let mut source = JsonBuilder::new(shared);
            source.num("id", input.id);
            write_columns(source.key("output_columns"), input.columns.iter().copied());
            self.write_relation(&input.input, source.key("root"), &mut 0, &mut declines)?;
            source.finish();
        }
        shared.push(']');
        self.write_relation(&self.root, json.key("root"), &mut 0, &mut declines)?;
        json.num("dependent_joins", self.dependent_join_count());
        for (field, declines) in [
            ("dependency_declines", declines.dependency),
            ("normalization_declines", declines.normalization),
        ] {
            let mut counts = JsonBuilder::new(json.key(field));
            for (rule, reasons) in declines {
                let mut rule = JsonBuilder::new(counts.key(rule));
                for (reason, count) in reasons {
                    rule.num(reason, count);
                }
                rule.finish();
            }
            counts.finish();
        }
        json.finish();
        Ok(())
    }

    fn write_relation(
        &self,
        relation: &Relation,
        out: &mut String,
        next_id: &mut usize,
        declines: &mut RuleDeclines,
    ) -> Result<()> {
        rewrite::normalization_declines(relation, self, |rule, precondition| {
            *declines
                .normalization
                .entry(rule)
                .or_default()
                .entry(precondition)
                .or_default() += 1;
        })?;
        let mut node = JsonBuilder::new(out);
        node.num("id", *next_id);
        *next_id += 1;
        let properties = self.properties(relation)?;
        write_columns(node.key("output_columns"), self.output_columns(relation)?);
        write_columns(node.key("outer_references"), properties.outer.iter());
        let mut inputs = Vec::new();
        match relation {
            Relation::OneRow => node.str("type", "one_row"),
            Relation::Values(values) => {
                node.str("type", "values");
                let columns = node.key("columns");
                columns.push('[');
                for (index, column) in values.columns.iter().enumerate() {
                    comma(columns, index);
                    write_column(columns, column);
                }
                columns.push(']');
                let rows = node.key("rows");
                rows.push('[');
                for (index, row) in values.rows.iter().enumerate() {
                    comma(rows, index);
                    write_scalars(rows, row);
                }
                rows.push(']');
            }
            Relation::Scan(id) => {
                node.str("type", "scan");
                node.num("relation", (*id).into());
            }
            Relation::SharedRef { binding, input } => {
                node.str("type", "shared_ref");
                node.num("relation", (*binding).into());
                node.num("shared_input", *input);
            }
            Relation::Subquery {
                binding,
                input,
                columns,
            } => {
                node.str("type", "subquery");
                node.num("relation", (*binding).into());
                write_columns(node.key("input_columns"), columns.iter().copied());
                inputs.push(input.as_ref());
            }
            Relation::Filter { input, predicates } => {
                node.str("type", "filter");
                write_scalars(node.key("predicates"), predicates);
                inputs.push(input.as_ref());
            }
            Relation::Project { input, outputs } => {
                node.str("type", "project");
                write_outputs(node.key("expressions"), outputs);
                inputs.push(input.as_ref());
            }
            Relation::Distinct { input } => {
                node.str("type", "distinct");
                inputs.push(input.as_ref());
            }
            Relation::Aggregate { input, aggregation } => {
                node.str("type", "aggregate");
                let grouped = aggregation
                    .keys
                    .as_ref()
                    .is_some_and(|keys| !keys.is_empty());
                node.bool("grouped", grouped);
                node.bool("empty_input_row", !grouped);
                write_scalars(
                    node.key("group_keys"),
                    aggregation.keys.as_deref().unwrap_or(&[]),
                );
                write_scalars(
                    node.key("having"),
                    aggregation.having.as_deref().unwrap_or(&[]),
                );
                write_outputs(node.key("expressions"), &aggregation.outputs);
                let functions = node.key("aggregates");
                functions.push('[');
                for (index, function) in aggregation.functions.iter().enumerate() {
                    comma(functions, index);
                    let mut json = JsonBuilder::new(functions);
                    json.str("function", function.func.as_str());
                    json.bool("distinct", function.distinct);
                    write_scalars(json.key("arguments"), &function.args);
                    write_scalar(json.key("expression"), &function.expr);
                    if let Some(filter) = &function.filter {
                        write_scalar(json.key("filter"), filter);
                    }
                    json.finish();
                }
                functions.push(']');
                inputs.push(input.as_ref());
            }
            Relation::Set {
                left,
                right,
                operation,
            } => {
                node.str("type", "set");
                node.str(
                    "operation",
                    match operation.operator {
                        ast::CompoundOperator::Union => "union",
                        ast::CompoundOperator::UnionAll => "union_all",
                        ast::CompoundOperator::Except => "except",
                        ast::CompoundOperator::Intersect => "intersect",
                    },
                );
                let columns = node.key("columns");
                columns.push('[');
                for (index, column) in operation.outputs.iter().enumerate() {
                    comma(columns, index);
                    write_column(columns, column);
                }
                columns.push(']');
                node.str_array(
                    "comparison_collations",
                    operation
                        .comparison_collations
                        .iter()
                        .map(|collation| collation.name()),
                );
                inputs.extend([left.as_ref(), right.as_ref()]);
            }
            Relation::Join {
                left,
                right,
                kind,
                predicates,
            } => {
                node.str("type", "join");
                node.str("kind", join_name(*kind));
                if *kind == JoinKind::Left {
                    write_columns(
                        node.key("null_extended_columns"),
                        self.output_columns(right)?,
                    );
                }
                write_scalars(node.key("predicates"), predicates);
                inputs.extend([left.as_ref(), right.as_ref()]);
            }
            Relation::MarkJoin {
                left,
                right,
                subquery,
                kind,
                column,
            } => {
                node.str("type", "mark_join");
                node.num("subquery", (*subquery).into());
                node.str("evaluation", "dependent");
                match kind.as_ref() {
                    super::MarkKind::Exists { negated } => {
                        node.str("kind", if *negated { "not_exists" } else { "exists" });
                    }
                    super::MarkKind::Membership { lhs, negated } => {
                        node.str("kind", if *negated { "not_in" } else { "in" });
                        write_scalars(node.key("lhs"), lhs);
                    }
                }
                write_column(node.key("result_column"), column);
                inputs.extend([left.as_ref(), right.as_ref()]);
            }
            Relation::ScalarJoin {
                left,
                right,
                subquery,
                column,
            } => {
                node.str("type", "scalar_join");
                node.num("subquery", (*subquery).into());
                node.str("row_selection", "first");
                node.str("empty_result", "null");
                write_column(node.key("result_column"), column);
                inputs.extend([left.as_ref(), right.as_ref()]);
            }
            Relation::Membership {
                left,
                right,
                lhs,
                negated,
                subquery,
            } => {
                node.str("type", "membership");
                node.str("kind", if *negated { "not_in" } else { "in" });
                node.num("subquery", (*subquery).into());
                node.bool("null_aware", true);
                write_scalars(node.key("lhs"), lhs);
                let decline = super::membership::decline(left, right, lhs, *negated, self)?;
                node.bool("unnesting_applicable", decline.is_none());
                if let Some(reason) = decline {
                    node.str("decline_reason", reason);
                    *declines
                        .dependency
                        .entry("UnnestMembership")
                        .or_default()
                        .entry(reason)
                        .or_default() += 1;
                }
                inputs.extend([left.as_ref(), right.as_ref()]);
            }
            Relation::DependentJoin {
                left,
                right,
                kind,
                subquery,
            } => {
                node.str("type", "dependent_join");
                node.str("kind", join_name(*kind));
                node.num("subquery", (*subquery).into());
                self.write_dependency_rules(
                    left,
                    right,
                    kind,
                    node.key("unnesting_rules"),
                    &mut declines.dependency,
                )?;
                inputs.extend([left.as_ref(), right.as_ref()]);
            }
            Relation::Sort { input, keys } => {
                node.str("type", "sort");
                let sort = node.key("keys");
                sort.push('[');
                for (index, (expr, direction, nulls)) in keys.iter().enumerate() {
                    comma(sort, index);
                    let mut key = JsonBuilder::new(sort);
                    write_scalar(key.key("scalar"), expr);
                    key.str(
                        "direction",
                        match direction {
                            ast::SortOrder::Asc => "asc",
                            ast::SortOrder::Desc => "desc",
                        },
                    );
                    key.str(
                        "nulls",
                        match nulls {
                            Some(ast::NullsOrder::First) => "first",
                            Some(ast::NullsOrder::Last) => "last",
                            None => "default",
                        },
                    );
                    key.finish();
                }
                sort.push(']');
                inputs.push(input.as_ref());
            }
            Relation::Limit {
                input,
                limit,
                offset,
            } => {
                node.str("type", "limit");
                if let Some(expr) = limit {
                    write_scalar(node.key("limit"), expr);
                }
                if let Some(expr) = offset {
                    write_scalar(node.key("offset"), expr);
                }
                inputs.push(input.as_ref());
            }
        }
        let children = node.key("inputs");
        children.push('[');
        for (index, input) in inputs.iter().enumerate() {
            comma(children, index);
            self.write_relation(input, children, next_id, declines)?;
        }
        children.push(']');
        node.finish();
        Ok(())
    }

    fn write_dependency_rules(
        &self,
        left: &Relation,
        right: &Relation,
        kind: &JoinKind,
        out: &mut String,
        declines: &mut BTreeMap<&'static str, BTreeMap<&'static str, usize>>,
    ) -> Result<()> {
        out.push('[');
        for (index, (name, reason)) in rewrite::dependent_filter_rules(left, right, kind, self)?
            .into_iter()
            .enumerate()
        {
            comma(out, index);
            let mut rule = JsonBuilder::new(out);
            rule.str("rule", name);
            rule.bool("applicable", reason.is_none());
            if let Some(reason) = reason {
                rule.str("decline_reason", reason);
                *declines.entry(name).or_default().entry(reason).or_default() += 1;
            }
            rule.finish();
        }
        out.push(']');
        Ok(())
    }
}

fn join_name(kind: JoinKind) -> &'static str {
    match kind {
        JoinKind::Inner => "inner",
        JoinKind::Left => "left",
        JoinKind::Semi => "semi",
        JoinKind::Anti => "anti",
    }
}

fn write_outputs(out: &mut String, outputs: &[super::Output]) {
    out.push('[');
    for (index, output) in outputs.iter().enumerate() {
        comma(out, index);
        let mut expr = JsonBuilder::new(out);
        write_column(expr.key("output"), &output.column);
        write_scalar(expr.key("scalar"), &output.expr);
        expr.finish();
    }
    out.push(']');
}

fn write_columns(out: &mut String, columns: impl IntoIterator<Item = ColumnId>) {
    out.push('[');
    for (index, column) in columns.into_iter().enumerate() {
        comma(out, index);
        write_column_id(out, column);
    }
    out.push(']');
}

fn write_column_id(out: &mut String, column: ColumnId) {
    let mut json = JsonBuilder::new(out);
    json.num("relation", column.relation.into());
    match column.position {
        Some(position) => json.num("column", position),
        None => json.str("column", "rowid"),
    }
    json.finish();
}

fn write_column(out: &mut String, column: &Column) {
    let mut json = JsonBuilder::new(out);
    write_column_id(json.key("id"), column.id);
    json.str("name", &column.name);
    json.bool("nullable", column.nullable);
    json.str("affinity", column.affinity.short_type_name());
    json.str("collation", &column.collation.to_string());
    json.finish();
}

fn write_scalars(out: &mut String, expressions: &[Scalar]) {
    out.push('[');
    for (index, expr) in expressions.iter().enumerate() {
        comma(out, index);
        write_scalar(out, expr);
    }
    out.push(']');
}

fn write_scalar(out: &mut String, scalar: &Scalar) {
    let mut json = JsonBuilder::new(out);
    json.str("affinity", scalar.affinity.short_type_name());
    json.str("collation", &scalar.collation.to_string());
    json.bool("nullable", scalar.nullable);
    json.bool("can_fail", scalar.can_fail);
    json.bool("volatile", scalar.volatile);
    write_expr(json.key("expression"), scalar.ast(), scalar);
    json.finish();
}

fn write_expr(out: &mut String, expr: &Expr, scalar: &Scalar) {
    let mut json = JsonBuilder::new(out);
    let mut children = Vec::new();
    match expr {
        Expr::Column { table, .. } | Expr::RowId { table, .. } => {
            let position = match expr {
                Expr::Column { column, .. } => Some(*column),
                _ => None,
            };
            let id = ColumnId {
                relation: *table,
                position,
            };
            let reference = scalar
                .references
                .iter()
                .find(|reference| reference.column == id)
                .expect("bound column reference");
            json.str(
                "type",
                if reference.scope == Scope::Local {
                    "column"
                } else {
                    "outer_column"
                },
            );
            write_column_id(json.key("id"), id);
            if let Scope::Outer(depth) = reference.scope {
                json.num("depth", depth);
            }
        }
        Expr::Literal(literal) => {
            json.str("type", "literal");
            json.str("sql", &literal.to_string());
        }
        Expr::Variable(variable) => {
            json.str("type", "parameter");
            json.num("slot", variable.index.get() as usize);
            json.opt_str("name", variable.name.as_deref());
        }
        Expr::Binary(left, op, right) => {
            json.str("type", "binary");
            json.str("operator", &op.to_string());
            children.extend([left.as_ref(), right.as_ref()]);
        }
        Expr::Unary(op, expr) => {
            json.str("type", "unary");
            json.str("operator", &op.to_string());
            children.push(expr.as_ref());
        }
        Expr::IsNull(child) | Expr::NotNull(child) => {
            json.str(
                "type",
                if matches!(expr, Expr::IsNull(_)) {
                    "is_null"
                } else {
                    "not_null"
                },
            );
            children.push(child.as_ref());
        }
        Expr::Collate(expr, name) => {
            json.str("type", "collate");
            json.str("name", name.as_str());
            children.push(expr.as_ref());
        }
        Expr::Cast { expr, type_name } => {
            json.str("type", "cast");
            if let Some(name) = type_name {
                json.str("name", &name.name);
            }
            children.push(expr.as_ref());
        }
        Expr::Between {
            lhs,
            not,
            start,
            end,
        } => {
            json.str("type", "between");
            json.bool("not", *not);
            children.extend([lhs.as_ref(), start.as_ref(), end.as_ref()]);
        }
        Expr::InList { lhs, not, rhs } => {
            json.str("type", "in_list");
            json.bool("not", *not);
            children.push(lhs.as_ref());
            children.extend(rhs.iter().map(|expr| expr.as_ref()));
        }
        Expr::Parenthesized(exprs) | Expr::Array { elements: exprs } => {
            json.str(
                "type",
                if matches!(expr, Expr::Array { .. }) {
                    "array"
                } else {
                    "row"
                },
            );
            children.extend(exprs.iter().map(|expr| expr.as_ref()));
        }
        Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            json.str("type", "case");
            json.bool("has_base", base.is_some());
            json.bool("has_else", else_expr.is_some());
            children.extend(base.iter().map(|expr| expr.as_ref()));
            for (when, then) in when_then_pairs {
                children.extend([when.as_ref(), then.as_ref()]);
            }
            children.extend(else_expr.iter().map(|expr| expr.as_ref()));
        }
        Expr::FunctionCall { name, args, .. } => {
            json.str("type", "call");
            json.str("name", name.as_str());
            children.extend(args.iter().map(|expr| expr.as_ref()));
        }
        Expr::FunctionCallStar { name, .. } => {
            json.str("type", "call_star");
            json.str("name", name.as_str());
        }
        Expr::Like {
            lhs,
            not,
            op,
            rhs,
            escape,
        } => {
            json.str("type", "like");
            json.str("operator", &op.to_string());
            json.bool("not", *not);
            children.extend([lhs.as_ref(), rhs.as_ref()]);
            children.extend(escape.iter().map(|expr| expr.as_ref()));
        }
        Expr::FieldAccess { base, field, .. } => {
            json.str("type", "field");
            json.str("name", field.as_str());
            children.push(base.as_ref());
        }
        Expr::Subscript { base, index } => {
            json.str("type", "subscript");
            children.extend([base.as_ref(), index.as_ref()]);
        }
        Expr::Raise(resolve, expr) => {
            json.str("type", "raise");
            json.str("action", &resolve.to_string());
            children.extend(expr.iter().map(|expr| expr.as_ref()));
        }
        Expr::Register(_)
        | Expr::SubqueryResult { .. }
        | Expr::Id(_)
        | Expr::Name(_)
        | Expr::Qualified(_, _)
        | Expr::DoublyQualified(_, _, _)
        | Expr::Exists(_)
        | Expr::Subquery(_)
        | Expr::InSelect { .. }
        | Expr::InTable { .. }
        | Expr::Default => unreachable!("unbound or physical scalar in logical inspection"),
    }
    let args = json.key("children");
    args.push('[');
    for (index, child) in children.into_iter().enumerate() {
        comma(args, index);
        write_expr(args, child, scalar);
    }
    args.push(']');
    json.finish();
}

fn comma(out: &mut String, index: usize) {
    if index != 0 {
        out.push(',');
    }
}
