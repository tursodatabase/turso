//! Property tests for stored-expression name, position, and profile rules.

use hegel::generators;
use turso_parser::{
    ast::{self, OneSelect, ResultColumn},
    parser::Parser,
};

use super::{
    BuiltinSchemaExprResolver, DropRemap, ResolutionMode, SchemaColumn, SchemaExpr,
    SchemaExprContext, SchemaExprNode, SchemaExprProfile, SchemaTable, SchemaTypeParameter,
};

fn parse_expr(sql: &str) -> ast::Expr {
    let statement = format!("SELECT {sql}");
    let command = Parser::new(statement.as_bytes())
        .next()
        .expect("generated SQL contains a statement")
        .expect("generated SQL parses");
    let ast::Cmd::Stmt(ast::Stmt::Select(select)) = command else {
        panic!("generated SQL is a SELECT");
    };
    let OneSelect::Select { mut columns, .. } = select.body.select else {
        panic!("generated SQL has a SELECT body");
    };
    let ResultColumn::Expr(expression, _) = columns.remove(0) else {
        panic!("generated SQL selects one expression");
    };
    *expression
}

fn generated_column_count(tc: &hegel::TestCase) -> usize {
    usize::from(tc.draw(generators::integers::<u8>())) + 1
}

fn generated_position(tc: &hegel::TestCase, len: usize) -> usize {
    tc.draw(generators::integers::<usize>().max_value(len - 1))
}

fn column_name(position: usize) -> String {
    format!("c{position}")
}

fn table_with_columns(count: usize, rowid_alias: Option<usize>, has_rowid: bool) -> SchemaTable {
    SchemaTable::new(
        "items",
        (0..count)
            .map(|position| {
                SchemaColumn::new(
                    column_name(position),
                    rowid_alias == Some(position),
                    Some("INTEGER".to_string()),
                )
            })
            .collect(),
        has_rowid,
    )
}

fn column_names(count: usize) -> Vec<String> {
    (0..count).map(column_name).collect()
}

fn generated_profile(tc: &hegel::TestCase) -> SchemaExprProfile {
    match tc.draw(generators::integers::<u8>().max_value(6)) {
        0 => SchemaExprProfile::Default,
        1 => SchemaExprProfile::Check {
            strict_types: tc.draw(generators::booleans()),
        },
        2 => SchemaExprProfile::GeneratedColumn,
        3 => SchemaExprProfile::IndexKey,
        4 => SchemaExprProfile::PartialIndexPredicate,
        5 => SchemaExprProfile::DomainCheck,
        6 => SchemaExprProfile::TypeTransform,
        _ => unreachable!("generated profile is bounded"),
    }
}

fn generated_table_profile(tc: &hegel::TestCase) -> SchemaExprProfile {
    match tc.draw(generators::integers::<u8>().max_value(3)) {
        0 => SchemaExprProfile::Check {
            strict_types: tc.draw(generators::booleans()),
        },
        1 => SchemaExprProfile::GeneratedColumn,
        2 => SchemaExprProfile::IndexKey,
        3 => SchemaExprProfile::PartialIndexPredicate,
        _ => unreachable!("generated profile is bounded"),
    }
}

fn profiles(strict_types: bool) -> [SchemaExprProfile; 7] {
    [
        SchemaExprProfile::Default,
        SchemaExprProfile::Check { strict_types },
        SchemaExprProfile::GeneratedColumn,
        SchemaExprProfile::IndexKey,
        SchemaExprProfile::PartialIndexPredicate,
        SchemaExprProfile::DomainCheck,
        SchemaExprProfile::TypeTransform,
    ]
}

fn table_profiles(strict_types: bool) -> [SchemaExprProfile; 4] {
    [
        SchemaExprProfile::Check { strict_types },
        SchemaExprProfile::GeneratedColumn,
        SchemaExprProfile::IndexKey,
        SchemaExprProfile::PartialIndexPredicate,
    ]
}

fn type_parameters() -> Vec<SchemaTypeParameter> {
    vec![
        SchemaTypeParameter::new("value", Some("INTEGER".to_string())),
        SchemaTypeParameter::new("argument", Some("INTEGER".to_string())),
    ]
}

fn resolve(
    syntax: &ast::Expr,
    profile: SchemaExprProfile,
    table: &SchemaTable,
    parameters: &[SchemaTypeParameter],
    mode: ResolutionMode,
) -> crate::Result<SchemaExpr> {
    let context = match profile {
        SchemaExprProfile::Check { .. }
        | SchemaExprProfile::GeneratedColumn
        | SchemaExprProfile::IndexKey
        | SchemaExprProfile::PartialIndexPredicate => SchemaExprContext::for_table(table),
        SchemaExprProfile::TypeTransform => {
            SchemaExprContext::without_table().with_type_parameters(parameters)
        }
        SchemaExprProfile::Default | SchemaExprProfile::DomainCheck => {
            SchemaExprContext::without_table()
        }
    };
    SchemaExpr::resolve(syntax, profile, context, &BuiltinSchemaExprResolver, mode)
}

fn valid_sql(tc: &hegel::TestCase, profile: SchemaExprProfile, count: usize) -> String {
    let number = tc.draw(generators::integers::<i64>());
    match profile {
        SchemaExprProfile::Default => match tc.draw(generators::integers::<u8>().max_value(2)) {
            0 => number.to_string(),
            1 => format!("abs({number})"),
            2 => format!("CASE WHEN {number} IS NULL THEN 0 ELSE {number} END"),
            _ => unreachable!("generated expression shape is bounded"),
        },
        SchemaExprProfile::DomainCheck => {
            match tc.draw(generators::integers::<u8>().max_value(2)) {
                0 => format!("value > {number}"),
                1 => "abs(value)".to_string(),
                2 => "value IS NOT NULL".to_string(),
                _ => unreachable!("generated expression shape is bounded"),
            }
        }
        SchemaExprProfile::TypeTransform => {
            match tc.draw(generators::integers::<u8>().max_value(2)) {
                0 => format!("value + {number}"),
                1 => "coalesce(value, argument)".to_string(),
                2 => "CASE WHEN value IS NULL THEN argument ELSE value END".to_string(),
                _ => unreachable!("generated expression shape is bounded"),
            }
        }
        SchemaExprProfile::Check { .. }
        | SchemaExprProfile::GeneratedColumn
        | SchemaExprProfile::IndexKey
        | SchemaExprProfile::PartialIndexPredicate => {
            let first = generated_position(tc, count);
            let second = generated_position(tc, count);
            match tc.draw(generators::integers::<u8>().max_value(5)) {
                0 => column_name(first),
                1 => format!("{} + {number}", column_name(first)),
                2 => format!("abs({})", column_name(first)),
                3 => format!(
                    "CASE WHEN {} IS NULL THEN {} ELSE {} END",
                    column_name(first),
                    column_name(second),
                    column_name(first)
                ),
                4 => format!("{} COLLATE BINARY", column_name(first)),
                5 => format!("{} IN ({number}, 0)", column_name(first)),
                _ => unreachable!("generated expression shape is bounded"),
            }
        }
    }
}

fn profile_allows_table_columns(profile: SchemaExprProfile) -> bool {
    matches!(
        profile,
        SchemaExprProfile::Check { .. }
            | SchemaExprProfile::GeneratedColumn
            | SchemaExprProfile::IndexKey
            | SchemaExprProfile::PartialIndexPredicate
    )
}

fn profile_allows_rowid(profile: SchemaExprProfile) -> bool {
    matches!(
        profile,
        SchemaExprProfile::Check { .. } | SchemaExprProfile::PartialIndexPredicate
    )
}

fn profile_allows_current_time(profile: SchemaExprProfile) -> bool {
    matches!(
        profile,
        SchemaExprProfile::Default
            | SchemaExprProfile::Check { .. }
            | SchemaExprProfile::DomainCheck
    )
}

fn profile_allows_nondeterministic_functions(profile: SchemaExprProfile) -> bool {
    matches!(
        profile,
        SchemaExprProfile::Default | SchemaExprProfile::Check { .. }
    )
}

// Example: in `CHECK(c2 > 0)` or `CREATE INDEX ... ON items(c2)`, `c2`
// resolves to schema position two even when the identifier uses different case.
#[hegel::test]
fn table_column_names_bind_to_exact_positions(tc: hegel::TestCase) {
    let count = generated_column_count(&tc);
    let position = generated_position(&tc, count);
    let is_rowid_alias = tc.draw(generators::booleans());
    let table = table_with_columns(count, is_rowid_alias.then_some(position), true);
    let parameters = type_parameters();
    let profile = generated_table_profile(&tc);
    let syntax = parse_expr(&column_name(position).to_ascii_uppercase());

    let expression = resolve(
        &syntax,
        profile,
        &table,
        &parameters,
        ResolutionMode::Strict,
    )
    .expect("a generated table-column expression resolves");

    let SchemaExprNode::SelfColumn(column) = expression
        .as_valid()
        .expect("strict resolution returned a valid expression")
        .root()
    else {
        panic!("a table column resolves to a positional reference");
    };
    assert_eq!(column.position(), position);
    assert_eq!(column.is_rowid_alias(), is_rowid_alias);
}

// Example: `CHECK(c3 + c1 + c3)` reports positional dependencies `{1, 3}`
// once, in schema order, regardless of reference order or repetition.
#[hegel::test]
fn dependencies_agree_with_a_position_set_model(tc: hegel::TestCase) {
    let count = generated_column_count(&tc);
    let positions =
        tc.draw(generators::vecs(generators::integers::<usize>().max_value(count - 1)).min_size(1));
    let sql = positions
        .iter()
        .map(|position| column_name(*position))
        .collect::<Vec<_>>()
        .join(" + ");
    let table = table_with_columns(count, None, true);
    let parameters = type_parameters();
    let expression = resolve(
        &parse_expr(&sql),
        generated_table_profile(&tc),
        &table,
        &parameters,
        ResolutionMode::Strict,
    )
    .expect("generated stored expression resolves");
    let mut expected = positions;
    expected.sort_unstable();
    expected.dedup();

    assert_eq!(expression.dependencies().unwrap().columns(), expected);
}

// Example: `CHECK(c1 + abs(c3) COLLATE BINARY)` can be rendered, parsed, and
// resolved again without changing its positional dependencies or canonical SQL.
#[hegel::test]
fn resolve_render_parse_resolve_keeps_stored_meaning(tc: hegel::TestCase) {
    let count = generated_column_count(&tc);
    let table = table_with_columns(count, None, true);
    let parameters = type_parameters();
    let profile = generated_profile(&tc);
    let syntax = parse_expr(&valid_sql(&tc, profile, count));
    let first = resolve(
        &syntax,
        profile,
        &table,
        &parameters,
        ResolutionMode::Strict,
    )
    .expect("generated stored expression resolves");
    let first_valid = first.as_valid().expect("strict mode returns valid syntax");
    let first_dependencies = first_valid.dependencies();
    let first_render = first_valid
        .render(&column_names(count))
        .expect("valid stored expression renders");

    let reparsed = parse_expr(&first_render);
    let second = resolve(
        &reparsed,
        profile,
        &table,
        &parameters,
        ResolutionMode::Strict,
    )
    .expect("rendered stored expression resolves again");
    let second_valid = second.as_valid().expect("strict mode returns valid syntax");

    assert_eq!(second_valid.dependencies(), first_dependencies);
    assert_eq!(
        second_valid
            .render(&column_names(count))
            .expect("re-resolved stored expression renders"),
        first_render
    );
}

// Example: renaming `items.c2` to `items.total` changes rendered SQL from
// `c2 + 1` to `total + 1`, while the stored dependency remains position two.
#[hegel::test]
fn column_rename_changes_spelling_not_position(tc: hegel::TestCase) {
    let count = generated_column_count(&tc);
    let position = generated_position(&tc, count);
    let table = table_with_columns(count, None, true);
    let parameters = type_parameters();
    let profile = generated_table_profile(&tc);
    let syntax = parse_expr(&format!("{} + 1", column_name(position)));
    let expression = resolve(
        &syntax,
        profile,
        &table,
        &parameters,
        ResolutionMode::Strict,
    )
    .expect("generated stored expression resolves");
    let dependencies = expression.dependencies().expect("expression is valid");

    let mut renamed_names = column_names(count);
    renamed_names[position] = format!("renamed_{position}");
    let renamed_sql = expression
        .render(&renamed_names)
        .expect("positional expression renders after rename");
    let renamed_table = SchemaTable::new(
        "items",
        renamed_names
            .iter()
            .map(|name| SchemaColumn::new(name, false, Some("INTEGER".to_string())))
            .collect(),
        true,
    );
    let re_resolved = resolve(
        &parse_expr(&renamed_sql),
        profile,
        &renamed_table,
        &parameters,
        ResolutionMode::Strict,
    )
    .expect("rendered expression resolves against the renamed table");

    assert_eq!(dependencies.columns(), &[position]);
    assert_eq!(re_resolved.dependencies().unwrap(), dependencies);
}

// Example: in `b AS (a * 2), c AS (b + a)`, renaming `a` to `x` renders
// both generated expressions with `x` while their dependency positions stay fixed.
#[hegel::test]
fn column_rename_renders_every_repeated_dependency(tc: hegel::TestCase) {
    let count = generated_column_count(&tc).max(2);
    let renamed = generated_position(&tc, count);
    let other = if renamed + 1 == count { 0 } else { renamed + 1 };
    let table = table_with_columns(count, None, true);
    let parameters = type_parameters();
    let syntax = parse_expr(&format!(
        "{} + {} + {}",
        column_name(renamed),
        column_name(other),
        column_name(renamed)
    ));
    let expression = resolve(
        &syntax,
        SchemaExprProfile::GeneratedColumn,
        &table,
        &parameters,
        ResolutionMode::Strict,
    )
    .expect("generated stored expression resolves");
    let dependencies = expression.dependencies().unwrap();

    let mut names = column_names(count);
    let new_name = format!("renamed_{renamed}");
    names[renamed].clone_from(&new_name);
    let rendered = expression.render_syntax(&names).unwrap().to_string();

    assert_eq!(rendered.matches(&new_name).count(), 2);
    assert!(!rendered.contains(&column_name(renamed)));
    assert_eq!(
        dependencies.columns(),
        &[renamed.min(other), renamed.max(other)]
    );
}

// Example: after `ALTER TABLE items DROP COLUMN c1`, a stored reference to old
// position three moves to position two and still renders as the same column name.
#[hegel::test]
fn dropping_an_earlier_column_shifts_every_later_position(tc: hegel::TestCase) {
    let count = generated_column_count(&tc).max(2);
    let dropped = generated_position(&tc, count);
    let new_position = generated_position(&tc, count - 1);
    let old_position = if new_position >= dropped {
        new_position + 1
    } else {
        new_position
    };
    let table = table_with_columns(count, None, true);
    let parameters = type_parameters();
    let profile = generated_table_profile(&tc);
    let mut expression = resolve(
        &parse_expr(&column_name(old_position)),
        profile,
        &table,
        &parameters,
        ResolutionMode::Strict,
    )
    .expect("generated stored expression resolves");

    assert_eq!(
        expression
            .remap_after_drop(dropped)
            .expect("expression does not use the dropped column"),
        DropRemap::Remapped
    );
    assert_eq!(
        expression.dependencies().unwrap().columns(),
        &[new_position]
    );

    let mut names = column_names(count);
    names.remove(dropped);
    assert_eq!(
        expression.render(&names).unwrap(),
        column_name(old_position)
    );
}

// Example: `ALTER TABLE items DROP COLUMN c2` is rejected while an index or
// generated expression still directly depends on position two.
#[hegel::test]
fn dropping_a_referenced_column_is_rejected(tc: hegel::TestCase) {
    let count = generated_column_count(&tc);
    let dropped = generated_position(&tc, count);
    let table = table_with_columns(count, None, true);
    let parameters = type_parameters();
    let profile = generated_table_profile(&tc);
    let mut expression = resolve(
        &parse_expr(&column_name(dropped)),
        profile,
        &table,
        &parameters,
        ResolutionMode::Strict,
    )
    .expect("generated stored expression resolves");

    assert!(expression.remap_after_drop(dropped).is_err());
}

// Example: `c0 + 1` is valid in CHECK, generated-column, expression-index,
// and partial-index expressions, but not in DEFAULT, domain, or type templates.
#[hegel::test]
fn each_profile_enforces_table_column_visibility(tc: hegel::TestCase) {
    let table = table_with_columns(1, None, true);
    let parameters = type_parameters();
    let syntax = parse_expr("c0 + 1");

    for profile in profiles(tc.draw(generators::booleans())) {
        let result = resolve(
            &syntax,
            profile,
            &table,
            &parameters,
            ResolutionMode::Strict,
        );
        assert_eq!(
            result.is_ok(),
            profile_allows_table_columns(profile),
            "profile {profile:?}"
        );
    }
}

// Example: `CHECK(rowid > 0)` and `CREATE INDEX ... WHERE rowid > 0` may use
// rowid, while generated columns, index keys, and tableless profiles may not.
#[hegel::test]
fn each_profile_enforces_rowid_visibility(tc: hegel::TestCase) {
    let table = table_with_columns(1, None, true);
    let parameters = type_parameters();
    let syntax = parse_expr("(rowid)");

    for profile in profiles(tc.draw(generators::booleans())) {
        let result = resolve(
            &syntax,
            profile,
            &table,
            &parameters,
            ResolutionMode::Strict,
        );
        assert_eq!(
            result.is_ok(),
            profile_allows_rowid(profile),
            "profile {profile:?}"
        );
    }
}

// Example: `CHECK(rowid > 0)` is invalid for `CREATE TABLE ... WITHOUT ROWID`
// because that table occurrence has no hidden rowid source.
#[hegel::test]
fn without_rowid_tables_never_expose_a_pseudo_column(tc: hegel::TestCase) {
    let table = table_with_columns(1, None, false);
    let parameters = type_parameters();
    let syntax = parse_expr("(rowid)");

    for profile in profiles(tc.draw(generators::booleans())) {
        assert!(
            resolve(
                &syntax,
                profile,
                &table,
                &parameters,
                ResolutionMode::Strict,
            )
            .is_err(),
            "WITHOUT ROWID unexpectedly exposed rowid for profile {profile:?}"
        );
    }
}

// Example: if a table declares a real column named `rowid`, generated columns
// and index keys bind that column at position zero instead of the hidden rowid.
#[hegel::test]
fn a_real_rowid_column_shadows_the_pseudo_column(tc: hegel::TestCase) {
    let table = SchemaTable::new(
        "items",
        vec![SchemaColumn::new(
            "rowid",
            false,
            Some("INTEGER".to_string()),
        )],
        true,
    );
    let parameters = type_parameters();
    let syntax = parse_expr("rowid");

    for profile in table_profiles(tc.draw(generators::booleans())) {
        let expression = resolve(
            &syntax,
            profile,
            &table,
            &parameters,
            ResolutionMode::Strict,
        )
        .expect("the declared rowid column is visible to every table profile");
        let SchemaExprNode::SelfColumn(column) = expression.as_valid().unwrap().root() else {
            panic!("the declared rowid name binds as a table column");
        };
        assert_eq!(column.position(), 0, "profile {profile:?}");
    }
}

// Example: `items.c0` is valid in CHECK and index expressions, while generated
// columns reject the dot operator and store only unqualified column positions.
#[hegel::test]
fn table_qualification_follows_each_profile(tc: hegel::TestCase) {
    let table = table_with_columns(1, None, true);
    let parameters = type_parameters();
    let syntax = parse_expr("items.c0");
    let strict_types = tc.draw(generators::booleans());

    for profile in table_profiles(strict_types) {
        let result = resolve(
            &syntax,
            profile,
            &table,
            &parameters,
            ResolutionMode::Strict,
        );
        let expected = !matches!(profile, SchemaExprProfile::GeneratedColumn);
        assert_eq!(result.is_ok(), expected, "profile {profile:?}");
    }
}

// Example: `main.items.c0` is accepted for expression and partial indexes,
// while CHECK expressions reject database qualification and generated columns reject dots.
#[hegel::test]
fn database_qualification_is_limited_to_index_profiles(tc: hegel::TestCase) {
    let table = table_with_columns(1, None, true);
    let parameters = type_parameters();
    let syntax = parse_expr("main.items.c0");

    for profile in profiles(tc.draw(generators::booleans())) {
        let result = resolve(
            &syntax,
            profile,
            &table,
            &parameters,
            ResolutionMode::Strict,
        );
        let expected = matches!(
            profile,
            SchemaExprProfile::IndexKey | SchemaExprProfile::PartialIndexPredicate
        );
        assert_eq!(result.is_ok(), expected, "profile {profile:?}");
    }
}

// Example: `DEFAULT CURRENT_TIMESTAMP` and `CHECK(CURRENT_TIMESTAMP)` retain
// SQLite's time literals, while generated and index expressions reject them.
#[hegel::test]
fn each_profile_enforces_current_time_rules(tc: hegel::TestCase) {
    let table = table_with_columns(1, None, true);
    let parameters = type_parameters();
    let syntax = parse_expr("CURRENT_TIMESTAMP");

    for profile in profiles(tc.draw(generators::booleans())) {
        let result = resolve(
            &syntax,
            profile,
            &table,
            &parameters,
            ResolutionMode::Strict,
        );
        assert_eq!(
            result.is_ok(),
            profile_allows_current_time(profile),
            "profile {profile:?}"
        );
    }
}

// Example: `DEFAULT(random())` and `CHECK(random())` are accepted for SQLite
// compatibility, while persistent generated/index/type expressions reject it.
#[hegel::test]
fn each_profile_enforces_function_determinism(tc: hegel::TestCase) {
    let table = table_with_columns(1, None, true);
    let parameters = type_parameters();
    let syntax = parse_expr("random()");

    for profile in profiles(tc.draw(generators::booleans())) {
        let result = resolve(
            &syntax,
            profile,
            &table,
            &parameters,
            ResolutionMode::Strict,
        );
        assert_eq!(
            result.is_ok(),
            profile_allows_nondeterministic_functions(profile),
            "profile {profile:?}"
        );
    }
}

// Example: `CREATE TYPE ... ENCODE WITH (RAISE(ABORT, 'bad'))` may signal a
// transform failure, but `RAISE()` is forbidden in every other stored profile.
#[hegel::test]
fn only_type_transforms_allow_raise(tc: hegel::TestCase) {
    let table = table_with_columns(1, None, true);
    let parameters = type_parameters();
    let syntax = parse_expr("raise(ABORT, 'bad')");

    for profile in profiles(tc.draw(generators::booleans())) {
        let result = resolve(
            &syntax,
            profile,
            &table,
            &parameters,
            ResolutionMode::Strict,
        );
        assert_eq!(
            result.is_ok(),
            profile == SchemaExprProfile::TypeTransform,
            "profile {profile:?}"
        );
    }
}

// Example: `?1`, `(SELECT 1)`, `sum(1)`, and `row_number() OVER ()` all depend
// on runtime or query state and are rejected from every stored expression profile.
#[hegel::test]
fn stored_expressions_reject_runtime_and_query_state(tc: hegel::TestCase) {
    let sql = tc.draw(generators::sampled_from(vec![
        "?1",
        "(SELECT 1)",
        "sum(1)",
        "row_number() OVER ()",
    ]));
    let syntax = parse_expr(sql);
    let table = table_with_columns(1, None, true);
    let parameters = type_parameters();

    for profile in profiles(tc.draw(generators::booleans())) {
        assert!(
            resolve(
                &syntax,
                profile,
                &table,
                &parameters,
                ResolutionMode::Strict,
            )
            .is_err(),
            "{sql} unexpectedly resolved for profile {profile:?}"
        );
    }
}

// Example: SQLite treats bare `DEFAULT legacy_name` as the string
// `'legacy_name'`, but `(legacy_name)` is a non-constant column reference.
#[hegel::test]
fn only_a_bare_default_identifier_gets_string_compatibility(tc: hegel::TestCase) {
    let position = tc.draw(generators::integers::<u16>());
    let name = format!("legacy_{position}");
    let table = table_with_columns(1, None, true);
    let parameters = type_parameters();
    let bare = resolve(
        &parse_expr(&name),
        SchemaExprProfile::Default,
        &table,
        &parameters,
        ResolutionMode::Strict,
    )
    .expect("bare default identifier keeps SQLite compatibility");
    let nested = resolve(
        &parse_expr(&format!("({name})")),
        SchemaExprProfile::Default,
        &table,
        &parameters,
        ResolutionMode::Strict,
    );

    assert!(matches!(
        bare.as_valid().unwrap().root(),
        SchemaExprNode::Literal(ast::Literal::String(_))
    ));
    assert!(nested.is_err());
}

// Example: domain `CHECK(value > 0)` binds `value` as a domain input rather
// than looking for a table column with that spelling.
#[hegel::test]
fn domain_value_has_its_own_identity(tc: hegel::TestCase) {
    let spelling = if tc.draw(generators::booleans()) {
        "value"
    } else {
        "VALUE"
    };
    let table = table_with_columns(1, None, true);
    let parameters = type_parameters();
    let expression = resolve(
        &parse_expr(spelling),
        SchemaExprProfile::DomainCheck,
        &table,
        &parameters,
        ResolutionMode::Strict,
    )
    .expect("domain value resolves");
    let dependencies = expression.dependencies().unwrap();

    assert!(dependencies.uses_domain_value());
    assert!(dependencies.columns().is_empty());
}

// Example: in `ENCODE(value + argument)`, `value` is input zero and `argument`
// is input one regardless of identifier case.
#[hegel::test]
fn type_transform_names_bind_to_declared_input_positions(tc: hegel::TestCase) {
    let position = usize::from(tc.draw(generators::booleans()));
    let parameters = type_parameters();
    let spelling = if tc.draw(generators::booleans()) {
        parameters[position].name().to_ascii_uppercase()
    } else {
        parameters[position].name().to_string()
    };
    let table = table_with_columns(1, None, true);
    let expression = resolve(
        &parse_expr(&spelling),
        SchemaExprProfile::TypeTransform,
        &table,
        &parameters,
        ResolutionMode::Strict,
    )
    .expect("declared transform input resolves");

    let SchemaExprNode::TypeParameter {
        position: actual, ..
    } = expression.as_valid().unwrap().root()
    else {
        panic!("a transform input resolves to a positional parameter");
    };
    assert_eq!(*actual, position);
}

// Example: resolving `c0 + 1` in strict and schema-repair modes yields the
// same valid stored form because there is no semantic failure to preserve.
#[hegel::test]
fn strict_and_preserve_modes_agree_when_resolution_succeeds(tc: hegel::TestCase) {
    let count = generated_column_count(&tc);
    let profile = generated_profile(&tc);
    let table = table_with_columns(count, None, true);
    let parameters = type_parameters();
    let syntax = parse_expr(&valid_sql(&tc, profile, count));
    let strict = resolve(
        &syntax,
        profile,
        &table,
        &parameters,
        ResolutionMode::Strict,
    )
    .expect("generated expression resolves strictly");
    let preserved = resolve(
        &syntax,
        profile,
        &table,
        &parameters,
        ResolutionMode::PreserveUnresolved,
    )
    .expect("generated expression resolves in preserve mode");

    assert_eq!(
        strict.dependencies().unwrap(),
        preserved.dependencies().unwrap()
    );
    assert_eq!(
        strict.render(&column_names(count)).unwrap(),
        preserved.render(&column_names(count)).unwrap()
    );
}

// Example: `missing + 1` returns an error during strict CREATE processing but
// remains explicit unresolved syntax during lenient schema repair.
#[hegel::test]
fn preserve_mode_keeps_parse_failures_explicit(tc: hegel::TestCase) {
    let profile = generated_profile(&tc);
    let table = table_with_columns(1, None, true);
    let parameters = type_parameters();
    let syntax = parse_expr("missing + 1");

    assert!(resolve(
        &syntax,
        profile,
        &table,
        &parameters,
        ResolutionMode::Strict,
    )
    .is_err());
    let preserved = resolve(
        &syntax,
        profile,
        &table,
        &parameters,
        ResolutionMode::PreserveUnresolved,
    )
    .expect("parse failure is retained for schema repair");

    assert!(preserved.as_unresolved().is_some());
    assert!(preserved.as_valid().is_err());
    assert_eq!(preserved.render(&["c0"]).unwrap(), syntax.to_string());
}
