use super::*;

/// Extract a string literal value from an expression that has already been
/// validated as `Expr::Literal(Literal::String(_))` during bind-time checks.
pub(super) fn extract_string_literal(expr: &ast::Expr) -> crate::Result<String> {
    match expr {
        ast::Expr::Literal(ast::Literal::String(s)) => Ok(s.trim_matches('\'').to_string()),
        _ => crate::bail_parse_error!("expected a string literal argument"),
    }
}

/// Resolve the UnionDef for a column expression. Returns the variant names list
/// and optionally resolves a tag name to its numeric index.
/// Used by union_value, union_tag, union_extract function translation.
///
/// In the DML index-maintenance path (INSERT with expression indexes),
/// `referenced_tables` is `None` and columns use `SELF_TABLE`. We fall back
/// to the Resolver's `SelfTableContext::ForDML` to obtain column metadata.
/// Resolve the TypeDef for a column expression (Column or DML self-table column).
pub(super) fn resolve_typedef_from_column(
    expr: &ast::Expr,
    referenced_tables: Option<&TableReferences>,
    resolver: &Resolver,
) -> Option<Arc<TypeDef>> {
    let ty_str = match expr {
        ast::Expr::Column { table, column, .. } => {
            resolve_column_type_str(*table, *column, referenced_tables, resolver)?
        }
        ast::Expr::Variable(var) => var.col_type.as_ref()?.to_string(),
        _ => return None,
    };
    let td = resolver.schema().get_type_def_unchecked(&ty_str)?;
    Some(Arc::clone(td))
}

pub(super) fn resolve_union_from_column(
    expr: &ast::Expr,
    referenced_tables: Option<&TableReferences>,
    resolver: &Resolver,
) -> Option<Arc<TypeDef>> {
    resolve_typedef_from_column(expr, referenced_tables, resolver).filter(|td| td.is_union())
}

/// Resolve the struct TypeDef that an expression evaluates to.
///
/// Handles column references (direct struct column),
/// `union_extract(...)` (variant's struct type), and
/// `struct_extract(...)` (field's struct type for nested extraction).
pub(super) fn resolve_struct_from_expr(
    expr: &ast::Expr,
    referenced_tables: Option<&TableReferences>,
    resolver: &Resolver,
) -> Option<Arc<TypeDef>> {
    match expr {
        ast::Expr::Column { .. } => resolve_typedef_from_column(expr, referenced_tables, resolver)
            .filter(|td| td.is_struct()),
        ast::Expr::FunctionCall { name, args, .. } => {
            let normalized = crate::util::normalize_ident(name.as_str());
            match normalized.as_str() {
                // union_extract(col, 'tag') → variant's type
                "union_extract" if args.len() == 2 => {
                    let tag_name = extract_string_literal(&args[1]).ok()?;
                    let union_td =
                        resolve_union_from_column(&args[0], referenced_tables, resolver)?;
                    let (_, variant) = union_td.find_union_variant(&tag_name)?;
                    let struct_td = resolver
                        .schema()
                        .get_type_def_unchecked(&variant.type_name)?;
                    if struct_td.is_struct() {
                        Some(Arc::clone(struct_td))
                    } else {
                        None
                    }
                }
                // struct_extract(expr, 'field') → field's type (if it's a struct)
                "struct_extract" if args.len() == 2 => {
                    let field_name = extract_string_literal(&args[1]).ok()?;
                    let parent_td =
                        resolve_struct_from_expr(&args[0], referenced_tables, resolver)?;
                    let (_, field_def) = parent_td.find_struct_field(&field_name)?;
                    let field_td = resolver
                        .schema()
                        .get_type_def_unchecked(&field_def.type_name)?;
                    if field_td.is_struct() {
                        Some(Arc::clone(field_td))
                    } else {
                        None
                    }
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// Get the type string for a column
pub(super) fn resolve_column_type_str(
    table: ast::TableInternalId,
    column: usize,
    referenced_tables: Option<&TableReferences>,
    resolver: &Resolver,
) -> Option<String> {
    if let Some(rt) = referenced_tables {
        if let Some((_, tbl)) = rt.find_table_by_internal_id(table) {
            return Some(tbl.columns().get(column)?.ty_str.clone());
        }
    }
    if table.is_self_table() {
        return resolver.self_table_column_type_str(column);
    }
    None
}

/// Build an `Expr::FieldAccess { base: Expr::Column { ... }, field, resolved }` node,
/// pre-resolving the field index via `resolve_field_access`.
pub(crate) fn make_field_access_expr(
    table_id: TableInternalId,
    col_idx: usize,
    is_rowid_alias: bool,
    field_name: &str,
    td: &crate::schema::TypeDef,
) -> Expr {
    let resolved = resolve_field_access(td, field_name);
    Expr::FieldAccess {
        base: Box::new(Expr::Column {
            database: None,
            table: table_id,
            column: col_idx,
            is_rowid_alias,
        }),
        field: ast::Name::from_bytes(field_name.as_bytes()),
        resolved,
    }
}

/// Resolve a field/variant name against a TypeDef to produce a FieldAccessResolution.
pub(super) fn resolve_field_access(
    td: &crate::schema::TypeDef,
    field_name: &str,
) -> Option<ast::FieldAccessResolution> {
    if let Some((idx, _)) = td.find_struct_field(field_name) {
        Some(ast::FieldAccessResolution::StructField { field_index: idx })
    } else if let Some((tag_idx, _)) = td.find_union_variant(field_name) {
        Some(ast::FieldAccessResolution::UnionVariant { tag_index: tag_idx })
    } else {
        None
    }
}

/// Recursively resolve the output TypeDef of an expression.
///
/// For `Expr::Column`, returns the column's declared custom type.
/// For `Expr::FieldAccess`, recurses into the base to find the parent type,
/// then looks up what type the accessed field/variant produces.
/// Returns `None` for expressions that don't produce a known custom type.
pub(super) fn resolve_expr_output_type<'a>(
    expr: &ast::Expr,
    referenced_tables: Option<&TableReferences>,
    resolver: &'a Resolver<'a>,
) -> crate::Result<&'a crate::schema::TypeDef> {
    match expr {
        ast::Expr::Column { table, column, .. } => {
            let Some(referenced_tables) = referenced_tables else {
                crate::bail_parse_error!("cannot resolve type: no table context");
            };
            let Some((_is_outer, tbl)) = referenced_tables.find_table_by_internal_id(*table) else {
                crate::bail_parse_error!("cannot resolve type: table not found");
            };
            let col = &tbl.columns()[*column];
            let Some(td) = resolver.schema().get_type_def_unchecked(&col.ty_str) else {
                crate::bail_parse_error!(
                    "column '{}' has type '{}' which is not a known struct or union type",
                    col.name.as_deref().unwrap_or("?"),
                    col.ty_str
                );
            };
            Ok(td)
        }
        ast::Expr::FieldAccess { base, field, .. } => {
            let parent_td = resolve_expr_output_type(base, referenced_tables, resolver)?;
            let field_name = normalize_ident(field.as_str());
            // Find what type this field/variant produces
            let inner_type_name =
                if let Some((_, variant)) = parent_td.find_union_variant(&field_name) {
                    &variant.type_name
                } else if let Some((_, f)) = parent_td.find_struct_field(&field_name) {
                    &f.type_name
                } else {
                    let kind = if parent_td.is_union() {
                        "variant"
                    } else {
                        "field"
                    };
                    crate::bail_parse_error!("no such {} '{}' in type", kind, field_name);
                };
            let Some(td) = resolver.schema().get_type_def_unchecked(inner_type_name) else {
                crate::bail_parse_error!(
                    "'{}' resolves to type '{}' which is not a known type",
                    field_name,
                    inner_type_name
                );
            };
            Ok(td)
        }
        _ => {
            crate::bail_parse_error!("expression does not produce a known custom type");
        }
    }
}

/// Validates custom-type function calls (arrays, structs, unions) at bind time.
///
/// Compile-time checks belong in the earliest phase that has enough context.
/// Binding has the resolver (for the custom-types gate) and the raw AST args
/// (for arity and literal checks). Catching errors here avoids wasting
/// optimizer and translation cycles on invalid queries, and keeps the
/// translate_expr match arms focused purely on register allocation and codegen.
pub(crate) fn validate_custom_type_function_call(
    name: &str,
    args: &[Box<ast::Expr>],
    resolver: &Resolver<'_>,
) -> Result<()> {
    let normalized = crate::util::normalize_ident(name);
    match normalized.as_str() {
        // Arrays
        "array" | "array_element" | "array_set_element" | "array_length" | "array_append"
        | "array_prepend" | "array_cat" | "array_remove" | "array_contains" | "array_position"
        | "array_slice" | "string_to_array" | "array_to_string" | "array_overlap"
        | "array_contains_all" => {
            resolver.require_custom_types("Array features")?;
        }
        // Structs
        "struct_pack" => {
            resolver.require_custom_types("Struct features")?;
        }
        "struct_extract" => {
            resolver.require_custom_types("Struct features")?;
            if args.len() != 2 {
                crate::bail_parse_error!("struct_extract() requires exactly 2 arguments");
            }
            if !matches!(&*args[1], ast::Expr::Literal(ast::Literal::String(_))) {
                crate::bail_parse_error!(
                    "struct_extract() second argument must be a string literal"
                );
            }
        }
        // Unions
        "union_value" => {
            resolver.require_custom_types("Union features")?;
            if args.len() != 2 {
                crate::bail_parse_error!("union_value() requires exactly 2 arguments");
            }
            if !matches!(&*args[0], ast::Expr::Literal(ast::Literal::String(_))) {
                crate::bail_parse_error!("union_value() first argument must be a string literal");
            }
        }
        "union_tag" => {
            resolver.require_custom_types("Union features")?;
            if args.len() != 1 {
                crate::bail_parse_error!("union_tag() requires exactly 1 argument");
            }
        }
        "union_extract" => {
            resolver.require_custom_types("Union features")?;
            if args.len() != 2 {
                crate::bail_parse_error!("union_extract() requires exactly 2 arguments");
            }
            if !matches!(&*args[1], ast::Expr::Literal(ast::Literal::String(_))) {
                crate::bail_parse_error!(
                    "union_extract() second argument must be a string literal"
                );
            }
        }
        _ => {}
    }
    Ok(())
}
