//! Property tests for CTE visibility, dependency, and recursion rules.

use std::collections::HashSet;

use hegel::generators;
use turso_parser::{ast, parser::Parser};

use super::{
    cte_bindings::{CteBinding, CteBindings, CteState},
    cte_rules::{cte_self_reference_info, validate_recursive_cte_structure},
    expect_column,
    hir::{CteId, SourceKind},
    scope::{NamePrecedence, Scope},
    source, source_columns,
};

fn parse_select(sql: &str) -> ast::Select {
    let command = Parser::new(sql.as_bytes())
        .next()
        .expect("generated SQL contains a statement")
        .expect("generated SQL parses");
    let ast::Cmd::Stmt(ast::Stmt::Select(select)) = command else {
        panic!("generated SQL is a SELECT");
    };
    select
}

fn add_with_frame(bindings: &CteBindings, sql: &str) -> CteBindings {
    let select = parse_select(sql);
    bindings
        .with_clause(select.with.as_ref(), None)
        .expect("generated WITH clause is valid")
}

fn cte_body(sql: &str, name: &str) -> ast::Select {
    let select = parse_select(sql);
    select
        .with
        .expect("generated SELECT has a WITH clause")
        .ctes
        .into_iter()
        .find(|cte| cte.tbl_name.as_str().eq_ignore_ascii_case(name))
        .expect("generated WITH clause contains the named CTE")
        .select
}

fn parsed_definition(name: &str, value: usize) -> ast::CommonTableExpr {
    parse_select(&format!("WITH {} SELECT 1", definition(name, value)))
        .with
        .expect("generated SELECT has a WITH clause")
        .ctes
        .into_iter()
        .next()
        .expect("generated WITH clause has one definition")
}

fn generated_count(tc: &hegel::TestCase) -> usize {
    usize::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(16)))
}

fn generated_position(tc: &hegel::TestCase, len: usize) -> usize {
    tc.draw(generators::integers::<usize>().max_value(len - 1))
}

fn definition(name: &str, value: usize) -> String {
    format!("{name} AS (SELECT {value})")
}

fn require_binding(bindings: &CteBindings, name: &str) -> CteBinding {
    bindings.find(name).expect("generated CTE is visible")
}

// Example: `WITH first AS (...), second AS (...) SELECT * FROM SECOND` keeps
// every declaration visible and resolves identifiers without ASCII case sensitivity.
#[hegel::test]
fn one_with_frame_exposes_every_normalized_name(tc: hegel::TestCase) {
    let count = generated_count(&tc);
    let chosen = generated_position(&tc, count);
    let definitions = (0..count)
        .map(|position| definition(&format!("cte_{position}"), position))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("WITH {definitions} SELECT 1");
    let bindings = add_with_frame(&CteBindings::default(), &sql);

    for position in 0..count {
        assert_eq!(
            require_binding(&bindings, &format!("CTE_{position}")).name(),
            format!("cte_{position}")
        );
    }
    assert_eq!(
        require_binding(&bindings, &format!("CtE_{chosen}")).name(),
        format!("cte_{chosen}")
    );
}

// Example: `WITH item AS (...), ITEM AS (...) SELECT 1` is rejected because
// CTE names are unique after SQLite's ASCII identifier folding.
#[hegel::test]
fn duplicate_names_in_one_with_frame_are_rejected(tc: hegel::TestCase) {
    let suffix = tc.draw(generators::integers::<u16>());
    let lower = format!("item_{suffix}");
    let upper = lower.to_ascii_uppercase();
    let with = ast::With {
        recursive: false,
        ctes: vec![parsed_definition(&lower, 1), parsed_definition(&upper, 2)],
    };

    assert!(CteBindings::default()
        .with_clause(Some(&with), None)
        .is_err());
}

// Example: `WITH shared AS (...)` inside another query shadows the outer
// `shared`, while unrelated outer CTEs remain visible.
#[hegel::test]
fn inner_with_frames_shadow_only_matching_outer_names(tc: hegel::TestCase) {
    let suffix = tc.draw(generators::integers::<u16>());
    let outer_name = format!("outer_{suffix}");
    let outer = add_with_frame(
        &CteBindings::default(),
        &format!(
            "WITH {}, {} SELECT 1",
            definition("shared", 1),
            definition(&outer_name, 2)
        ),
    );
    let outer_shared = require_binding(&outer, "shared");
    let outer_unique = require_binding(&outer, &outer_name);
    let inner = add_with_frame(
        &outer,
        &format!(
            "WITH {}, {} SELECT 1",
            definition("SHARED", 3),
            definition("inner", 4)
        ),
    );

    let inner_shared = require_binding(&inner, "shared");
    assert!(!inner_shared.is_same_definition(&outer_shared));
    assert!(require_binding(&inner, &outer_name).is_same_definition(&outer_unique));
}

// Example: after `chosen AS (SELECT * FROM base)` is declared, a deeper
// `WITH base AS (...)` cannot change which `base` its lazily analyzed body sees.
#[hegel::test]
fn lazy_cte_dependencies_keep_the_declaration_site_environment(tc: hegel::TestCase) {
    let value = tc.draw(generators::integers::<u16>());
    let outer = add_with_frame(
        &CteBindings::default(),
        &format!("WITH {} SELECT 1", definition("base", usize::from(value))),
    );
    let original_base = require_binding(&outer, "base");
    let declaring_frame = add_with_frame(&outer, "WITH chosen AS (SELECT * FROM base) SELECT 1");
    let chosen = require_binding(&declaring_frame, "chosen");
    let deeper = add_with_frame(&declaring_frame, "WITH base AS (SELECT 999) SELECT 1");
    let shadowing_base = require_binding(&deeper, "base");
    let dependencies = chosen.dependencies();

    assert_eq!(dependencies.len(), 1);
    assert!(dependencies[0].is_same_definition(&original_base));
    assert!(!dependencies[0].is_same_definition(&shadowing_base));
}

// Example: `target AS (SELECT 1 FROM used_1, used_3)` records only `used_1`
// and `used_3`; unreferenced WITH bodies stay dormant even when declared first.
#[hegel::test]
fn dependency_discovery_matches_the_referenced_cte_names(tc: hegel::TestCase) {
    let count = generated_count(&tc);
    let selected: Vec<bool> = tc.draw(
        generators::vecs(generators::booleans())
            .min_size(count)
            .max_size(count),
    );
    let expected = selected
        .iter()
        .enumerate()
        .filter(|(_, selected)| **selected)
        .map(|(position, _)| format!("candidate_{position}"))
        .collect::<HashSet<_>>();
    let from = if expected.is_empty() {
        String::new()
    } else {
        format!(
            " FROM {}",
            (0..count)
                .filter(|position| selected[*position])
                .map(|position| format!("candidate_{position}"))
                .collect::<Vec<_>>()
                .join(" CROSS JOIN ")
        )
    };
    let mut definitions = vec![format!("target AS (SELECT 1{from})")];
    definitions
        .extend((0..count).map(|position| definition(&format!("candidate_{position}"), position)));
    let bindings = add_with_frame(
        &CteBindings::default(),
        &format!("WITH {} SELECT 1", definitions.join(", ")),
    );
    let actual = require_binding(&bindings, "target")
        .dependencies()
        .into_iter()
        .map(|binding| binding.name().to_string())
        .collect::<HashSet<_>>();

    assert_eq!(actual, expected);
}

// Example: `WITH item AS (...) SELECT * FROM item a, item b` analyzes `item`
// once; every occurrence sees the same completed CTE arena identity.
#[hegel::test]
fn repeated_bindings_share_lazy_analysis_state(tc: hegel::TestCase) {
    let cte_id = CteId::new(usize::from(tc.draw(generators::integers::<u8>())));
    let bindings = add_with_frame(&CteBindings::default(), "WITH item AS (SELECT 1) SELECT 1");
    let first = require_binding(&bindings, "item");
    let second = require_binding(&bindings, "ITEM");

    first.set_state(CteState::Complete(cte_id));

    assert!(first.is_same_definition(&second));
    assert!(matches!(second.state(), CteState::Complete(id) if id == cte_id));
}

// Example: `SELECT 1 IN candidate` is SQLite shorthand for a table-backed
// subquery, so `candidate` is a real dependency of the containing CTE.
#[hegel::test]
fn in_table_shorthand_counts_as_a_cte_dependency(tc: hegel::TestCase) {
    let suffix = tc.draw(generators::integers::<u16>());
    let name = format!("candidate_{suffix}");
    let bindings = add_with_frame(
        &CteBindings::default(),
        &format!(
            "WITH target AS (SELECT 1 IN {name}), {} SELECT 1",
            definition(&name, 1)
        ),
    );
    let dependencies = require_binding(&bindings, "target").dependencies();

    assert_eq!(dependencies.len(), 1);
    assert_eq!(dependencies[0].name(), name);
}

// Example: `SELECT * FROM candidate(1)` is a table-function call, not a read
// from the CTE named `candidate`, so it must not force that CTE body to run.
#[hegel::test]
fn table_function_calls_do_not_count_as_cte_dependencies(tc: hegel::TestCase) {
    let suffix = tc.draw(generators::integers::<u16>());
    let name = format!("candidate_{suffix}");
    let bindings = add_with_frame(
        &CteBindings::default(),
        &format!(
            "WITH target AS (SELECT * FROM {name}(1)), {} SELECT 1",
            definition(&name, 1)
        ),
    );

    assert!(require_binding(&bindings, "target")
        .dependencies()
        .is_empty());
}

// Example: `WITH item(a, b) AS (...) SELECT b FROM item` binds `b` to CTE
// source position one, regardless of the number of columns around it.
#[hegel::test]
fn cte_columns_bind_to_their_hir_source_positions(tc: hegel::TestCase) {
    let count = generated_count(&tc);
    let position = generated_position(&tc, count);
    let mut cte_source = source(
        0,
        "item",
        None,
        None,
        source_columns(count, None, None),
        false,
    );
    cte_source.kind = SourceKind::Cte(CteId::new(0));
    let mut scope = Scope::default();
    scope.add_source(&cte_source, true);

    let resolved = scope
        .resolve_unqualified(&format!("c{position}"), NamePrecedence::SourcesOnly)
        .expect("one CTE source is unambiguous")
        .expect("generated CTE column is visible");

    expect_column(resolved.expr, cte_source.id, position);
}

// Example: `WITH RECURSIVE walk AS (SELECT 0 UNION ALL SELECT n FROM walk)`
// accepts any non-empty seed prefix followed by one or more recursive arms.
#[hegel::test]
fn recursive_arms_may_follow_any_nonempty_seed_prefix(tc: hegel::TestCase) {
    let seed_count = generated_count(&tc);
    let recursive_count = generated_count(&tc);
    let mut body = "SELECT 0 AS n".to_string();
    for seed in 1..seed_count {
        body.push_str(&format!(" UNION ALL SELECT {seed}"));
    }
    for increment in 1..=recursive_count {
        body.push_str(&format!(" UNION ALL SELECT n + {increment} FROM walk"));
    }
    let body = cte_body(
        &format!("WITH RECURSIVE walk(n) AS ({body}) SELECT * FROM walk"),
        "walk",
    );

    assert_eq!(cte_self_reference_info("walk", &body), (true, false));
    assert_eq!(
        validate_recursive_cte_structure("walk", &body)
            .expect("generated recursive structure is valid"),
        seed_count
    );
}

// Example: `WITH RECURSIVE walk AS (SELECT n FROM walk UNION ALL SELECT 1)`
// is circular because the initial query reads the recursive table.
#[hegel::test]
fn recursive_seed_references_are_rejected(tc: hegel::TestCase) {
    let suffix = tc.draw(generators::integers::<u16>());
    let name = format!("walk_{suffix}");
    let body = cte_body(
        &format!(
            "WITH RECURSIVE {name}(n) AS (SELECT n FROM {name} UNION ALL SELECT 1) \
             SELECT * FROM {name}"
        ),
        &name,
    );

    assert_eq!(cte_self_reference_info(&name, &body), (true, true));
    assert!(validate_recursive_cte_structure(&name, &body).is_err());
}

#[derive(Clone, Copy, Debug)]
enum BrokenRecursiveArm {
    NestedOnly,
    TwoTopLevelReferences,
    TopLevelAndNestedReference,
}

// Example: after `SELECT 0 UNION ALL`, `SELECT * FROM walk` is valid, but a
// nested-only reference, two FROM references, or another nested read is rejected.
#[hegel::test]
fn each_recursive_arm_requires_exactly_one_top_level_reference(tc: hegel::TestCase) {
    let broken = tc.draw(generators::sampled_from(vec![
        BrokenRecursiveArm::NestedOnly,
        BrokenRecursiveArm::TwoTopLevelReferences,
        BrokenRecursiveArm::TopLevelAndNestedReference,
    ]));
    let arm = match broken {
        BrokenRecursiveArm::NestedOnly => "SELECT (SELECT n FROM walk)",
        BrokenRecursiveArm::TwoTopLevelReferences => {
            "SELECT left_side.n FROM walk AS left_side JOIN walk AS right_side"
        }
        BrokenRecursiveArm::TopLevelAndNestedReference => {
            "SELECT (SELECT nested.n FROM walk AS nested) FROM walk"
        }
    };
    let body = cte_body(
        &format!("WITH RECURSIVE walk(n) AS (SELECT 0 UNION ALL {arm}) SELECT * FROM walk"),
        "walk",
    );

    assert!(validate_recursive_cte_structure("walk", &body).is_err());
}

// Example: `SELECT 0 UNION ALL SELECT n FROM walk UNION ALL SELECT 2` is
// rejected because every compound arm after recursion begins must also recurse.
#[hegel::test]
fn nonrecursive_arms_cannot_follow_recursive_arms(tc: hegel::TestCase) {
    let recursive_arms = generated_count(&tc);
    let mut body = "SELECT 0 AS n".to_string();
    for increment in 1..=recursive_arms {
        body.push_str(&format!(" UNION ALL SELECT n + {increment} FROM walk"));
    }
    body.push_str(" UNION ALL SELECT 999");
    let body = cte_body(
        &format!("WITH RECURSIVE walk(n) AS ({body}) SELECT * FROM walk"),
        "walk",
    );

    assert!(validate_recursive_cte_structure("walk", &body).is_err());
}

// Example: inside a recursive arm, `WITH alias AS (SELECT * FROM walk),
// walk AS (SELECT 1)` makes the local `walk` shadow the outer recursive CTE
// throughout that WITH clause, including in the definition written before it.
#[hegel::test]
fn nested_with_names_shadow_the_recursive_cte_through_aliases(tc: hegel::TestCase) {
    let aliases = generated_count(&tc);
    let local_first = tc.draw(generators::booleans());
    let mut chain = vec!["alias_0 AS (SELECT * FROM walk)".to_string()];
    for position in 1..aliases {
        chain.push(format!(
            "alias_{position} AS (SELECT * FROM alias_{})",
            position - 1
        ));
    }
    let local = "walk AS (SELECT 1)".to_string();
    if local_first {
        chain.insert(0, local);
    } else {
        chain.push(local);
    }
    let body = cte_body(
        &format!(
            "WITH RECURSIVE walk(n) AS (SELECT 0 UNION ALL \
             SELECT * FROM (WITH {} SELECT * FROM alias_{})) SELECT * FROM walk",
            chain.join(", "),
            aliases - 1
        ),
        "walk",
    );

    assert_eq!(cte_self_reference_info("walk", &body), (false, false));
}

// Example: `WITH alias AS (SELECT * FROM walk) SELECT * FROM alias` inside a
// recursive arm still reaches the outer `walk`, but only through a nested query,
// so it is rejected instead of being treated as the required top-level read.
#[hegel::test]
fn nested_aliases_do_not_hide_indirect_recursive_references(tc: hegel::TestCase) {
    let aliases = generated_count(&tc);
    let mut chain = vec!["alias_0 AS (SELECT * FROM walk)".to_string()];
    for position in 1..aliases {
        chain.push(format!(
            "alias_{position} AS (SELECT * FROM alias_{})",
            position - 1
        ));
    }
    let body = cte_body(
        &format!(
            "WITH RECURSIVE walk(n) AS (SELECT 0 UNION ALL \
             SELECT * FROM (WITH {} SELECT * FROM alias_{})) SELECT * FROM walk",
            chain.join(", "),
            aliases - 1
        ),
        "walk",
    );

    assert_eq!(cte_self_reference_info("walk", &body), (true, false));
    assert!(validate_recursive_cte_structure("walk", &body).is_err());
}

// Example: `SELECT * FROM main.walk` names a catalog object, not the unqualified
// recursive CTE `walk`, so it does not turn a query into a recursive arm.
#[hegel::test]
fn database_qualified_names_do_not_reference_the_recursive_cte(tc: hegel::TestCase) {
    let database = if tc.draw(generators::booleans()) {
        "main"
    } else {
        "temp"
    };
    let body = cte_body(
        &format!(
            "WITH RECURSIVE walk(n) AS (SELECT 0 UNION ALL SELECT * FROM {database}.walk) \
             SELECT * FROM walk"
        ),
        "walk",
    );

    assert_eq!(cte_self_reference_info("walk", &body), (false, false));
}
