//! Properties for the catalog-free HIR-to-runtime binding boundary.

use hegel::generators;
use turso_parser::ast;

use super::*;
use crate::{
    schema::Type,
    translate::semantic::hir::{
        BoundCastPrograms, BoundSchemaCall, BoundSchemaProgram, CatalogSnapshot,
        ColumnReadExpression, Expr, From, HirDocument, HirRoot, IndexCoverage, IndexHint, Join,
        JoinConstraint, JoinKind, Output, OutputId, OutputNameKind, Query, QueryBlock,
        QueryBlockBody, QueryBlockId, QueryId, QueryRoot, SchemaProgramId, Source, SourceColumn,
        SourceId, SourceKind, SourceOwner, SubqueryExpr, TypeFact, TypeName,
    },
    vdbe::affinity::Affinity,
};

fn generated_width(tc: &hegel::TestCase) -> usize {
    usize::from(tc.draw(generators::integers::<u8>().max_value(15))) + 1
}

fn source(id: SourceId, owner: SourceOwner, width: usize) -> Source {
    Source {
        id,
        owner,
        database: None,
        name: format!("source_{}", id.index()),
        alias: None,
        kind: SourceKind::SchemaExpression,
        columns: (0..width)
            .map(|position| SourceColumn {
                name: format!("c{position}"),
                type_fact: TypeFact::known(Type::Integer),
                affinity: Affinity::Integer,
                has_affinity: true,
                collation: None,
                hidden: false,
                rowid_alias: false,
            })
            .collect(),
        generated_expressions: vec![ColumnReadExpression::Absent; width],
        default_expressions: vec![ColumnReadExpression::Absent; width],
        column_type_programs: vec![None; width],
        check_constraints: None,
        rowid_available: false,
        index_hint: IndexHint::None,
        index_expressions: Vec::new(),
        index_coverage: IndexCoverage::Selective,
        index_method_patterns: Vec::new(),
    }
}

fn output(id: OutputId, expression: Expr) -> Output {
    Output {
        id,
        name: format!("output_{}", id.index),
        expr: expression,
        type_fact: TypeFact::known(Type::Integer),
        affinity: Affinity::Integer,
        schema_affinity: Affinity::Integer,
        has_affinity: true,
        collation: None,
        collation_is_explicit: false,
        name_kind: OutputNameKind::Inferred,
    }
}

/// `q1` is the scalar subquery in
/// `SELECT (SELECT outer_left.c0) FROM left AS outer_left, right`.
fn correlated_document(width: usize) -> HirDocument {
    let outer_query = QueryId::new(0);
    let outer_block = QueryBlockId::new(outer_query, 0);
    let inner_query = QueryId::new(1);
    let inner_block = QueryBlockId::new(inner_query, 0);
    let captured_source = SourceId::new(0);
    let uncaptured_source = SourceId::new(1);
    let outer_output = OutputId::query(outer_block, 0);
    let inner_output = OutputId::query(inner_block, 0);

    HirDocument {
        snapshot: CatalogSnapshot::from_id(7),
        databases: Vec::new(),
        root: HirRoot::Query(QueryRoot {
            query: outer_query,
            trigger: None,
        }),
        queries: vec![
            Query {
                id: outer_query,
                parent: None,
                captures: Vec::new(),
                reachable_ctes: Vec::new(),
                blocks: vec![QueryBlock {
                    id: outer_block,
                    from: Some(From {
                        first: captured_source,
                        joins: vec![Join {
                            right: uncaptured_source,
                            kind: JoinKind::Comma,
                            constraint: JoinConstraint::None,
                        }],
                    }),
                    outputs: vec![output(
                        outer_output,
                        Expr::Subquery(SubqueryExpr::Scalar {
                            query: inner_query,
                            output: 0,
                        }),
                    )],
                    aggregate_count: 0,
                    window_function_count: 0,
                    body: QueryBlockBody::Select {
                        distinctness: None,
                        filter: None,
                        grouping: None,
                        windows: Vec::new(),
                    },
                }],
                first: outer_block,
                compounds: Vec::new(),
                order_by: Vec::new(),
                limit: None,
                output: vec![outer_output],
            },
            Query {
                id: inner_query,
                parent: Some(outer_query),
                captures: vec![captured_source],
                reachable_ctes: Vec::new(),
                blocks: vec![QueryBlock {
                    id: inner_block,
                    from: None,
                    outputs: vec![output(inner_output, Expr::column(captured_source, 0))],
                    aggregate_count: 0,
                    window_function_count: 0,
                    body: QueryBlockBody::Select {
                        distinctness: None,
                        filter: None,
                        grouping: None,
                        windows: Vec::new(),
                    },
                }],
                first: inner_block,
                compounds: Vec::new(),
                order_by: Vec::new(),
                limit: None,
                output: vec![inner_output],
            },
        ],
        sources: vec![
            source(captured_source, SourceOwner::QueryBlock(outer_block), width),
            source(
                uncaptured_source,
                SourceOwner::QueryBlock(outer_block),
                width,
            ),
        ],
        ctes: Vec::new(),
        schema_programs: Vec::new(),
        cdc: None,
    }
}

// Example: in `SELECT (SELECT o.c0) FROM items AS o, other`, the scalar
// subquery may reuse `o`'s cursor because q1 captures it, but it may not see
// `other` merely because that cursor exists in an outer runtime frame.
#[hegel::test]
fn nested_runtime_scopes_expose_only_exact_hir_captures(tc: hegel::TestCase) {
    let width = generated_width(&tc);
    let document = correlated_document(width);
    let outer_query = QueryId::new(0);
    let inner_query = QueryId::new(1);
    let captured_source = SourceId::new(0);
    let uncaptured_source = SourceId::new(1);
    let captured_runtime = if tc.draw(generators::booleans()) {
        SourceRuntime::Cursor(CursorId(11))
    } else {
        SourceRuntime::Registers {
            columns: RegisterRange::new(20, width),
            rowid: Some(RegisterId(19)),
        }
    };
    let mut bindings = RuntimeBindings::new(&document, document.snapshot)
        .expect("the generated HIR document is closed");

    bindings
        .enter_query(outer_query)
        .expect("the root query belongs to the root runtime frame");
    bindings
        .bind_source(captured_source, captured_runtime)
        .expect("the captured source belongs to q0");
    bindings
        .bind_source(uncaptured_source, SourceRuntime::Cursor(CursorId(12)))
        .expect("the second source also belongs to q0");
    let inner_runtime = match tc.draw(generators::integers::<u8>().max_value(2)) {
        0 => QueryRuntime::Registers(RegisterRange::new(40, 1)),
        1 => QueryRuntime::Exists(RegisterId(40)),
        _ => QueryRuntime::RowSet(CursorId(40)),
    };
    bindings
        .bind_query(inner_query, inner_runtime)
        .expect("q1 is a direct child of q0");
    bindings
        .enter_query(inner_query)
        .expect("q1 follows its recorded lexical parent");

    assert_eq!(bindings.source(captured_source), Ok(captured_runtime));
    assert_eq!(
        bindings.source(uncaptured_source),
        Err(RuntimeBindingError::SourceNotCaptured(uncaptured_source))
    );
    assert_eq!(bindings.query(inner_query), Ok(inner_runtime));
    assert_eq!(bindings.leave_query(), Ok(inner_query));
    assert_eq!(
        bindings.source(uncaptured_source),
        Ok(SourceRuntime::Cursor(CursorId(12)))
    );
    assert_eq!(bindings.leave_query(), Ok(outer_query));
    assert_eq!(
        bindings.leave_query(),
        Err(RuntimeBindingError::CannotLeaveRoot)
    );
}

// Example: q1 from `(SELECT o.c0)` cannot be entered before its q0 parent,
// and q0's result slot cannot be read as though it were one of q1's outputs.
#[hegel::test]
fn query_parents_and_output_slots_define_runtime_scope(tc: hegel::TestCase) {
    let document = correlated_document(generated_width(&tc));
    let outer_query = QueryId::new(0);
    let inner_query = QueryId::new(1);
    let outer_output = OutputId::query(QueryBlockId::new(outer_query, 0), 0);
    let inner_output = OutputId::query(QueryBlockId::new(inner_query, 0), 0);
    let mut bindings = RuntimeBindings::new(&document, document.snapshot)
        .expect("the generated HIR document is closed");

    assert_eq!(
        bindings.enter_query(inner_query),
        Err(RuntimeBindingError::WrongScope("query"))
    );
    bindings
        .enter_query(outer_query)
        .expect("q0 is a root query");
    let outer_runtime = OutputRuntime {
        register: RegisterId(50),
    };
    bindings
        .bind_output(outer_output, outer_runtime)
        .expect("the q0 output belongs to q0");
    bindings.enter_query(inner_query).expect("q1 follows q0");
    assert_eq!(
        bindings.output(outer_output),
        Err(RuntimeBindingError::WrongScope("output"))
    );
    let inner_runtime = OutputRuntime {
        register: RegisterId(51),
    };
    bindings
        .bind_output(inner_output, inner_runtime)
        .expect("the q1 output belongs to q1");
    assert_eq!(bindings.output(inner_output), Ok(inner_runtime));
}

// Example: `SELECT * FROM saved_view` may enter the view's independent root
// query while the caller query is active. Because that view query has no HIR
// parent or captures, it still cannot read any caller source by accident.
#[hegel::test]
fn independent_queries_can_nest_without_inheriting_the_caller_scope(tc: hegel::TestCase) {
    let width = generated_width(&tc);
    let mut document = correlated_document(width);
    let outer_query = QueryId::new(0);
    let independent_query = QueryId::new(1);
    let outer_source = SourceId::new(0);
    document.queries[0].blocks[0].outputs[0].expr = Expr::column(outer_source, 0);
    document.queries[1].parent = None;
    document.queries[1].captures.clear();
    document.queries[1].blocks[0].outputs[0].expr = Expr::Literal(ast::Literal::Null);
    document.sources[outer_source.index()].kind = SourceKind::Derived(independent_query);
    document.sources[outer_source.index()].columns.truncate(1);
    document.sources[outer_source.index()]
        .generated_expressions
        .truncate(1);
    document.sources[outer_source.index()]
        .default_expressions
        .truncate(1);
    document.sources[outer_source.index()]
        .column_type_programs
        .truncate(1);
    document
        .validate()
        .expect("the generated queries are independent closed roots");
    let mut bindings = RuntimeBindings::new(&document, document.snapshot)
        .expect("the generated HIR document is closed");

    bindings
        .enter_query(outer_query)
        .expect("the caller query enters from the root frame");
    bindings
        .bind_source(outer_source, SourceRuntime::Cursor(CursorId(11)))
        .expect("the caller owns its source");
    bindings
        .enter_query(independent_query)
        .expect("an independent view query may execute inside its caller");
    assert_eq!(
        bindings.source(outer_source),
        Err(RuntimeBindingError::SourceNotCaptured(outer_source))
    );
}

// Example: a three-column OLD/NEW row image must map to three contiguous
// registers. A two-register binding is rejected, and rebinding the source does
// not silently replace the first physical location.
#[hegel::test]
fn register_rows_have_exact_width_and_bind_once(tc: hegel::TestCase) {
    let width = generated_width(&tc);
    let document = correlated_document(width);
    let source = SourceId::new(0);
    let mut bindings = RuntimeBindings::new(&document, document.snapshot)
        .expect("the generated HIR document is closed");
    bindings
        .enter_query(QueryId::new(0))
        .expect("q0 is a root query");
    let wrong_width = if width == 1 { 2 } else { width - 1 };
    assert_eq!(
        bindings.bind_source(
            source,
            SourceRuntime::Registers {
                columns: RegisterRange::new(60, wrong_width),
                rowid: None,
            },
        ),
        Err(RuntimeBindingError::SourceWidth {
            source,
            expected: width,
            actual: wrong_width,
        })
    );
    let rowid = RegisterId(69);
    let first = SourceRuntime::Registers {
        columns: RegisterRange::new(70, width),
        rowid: Some(rowid),
    };
    bindings
        .bind_source(source, first)
        .expect("the exact row width is accepted");
    assert_eq!(
        bindings.bind_source(source, SourceRuntime::Cursor(CursorId(99))),
        Err(RuntimeBindingError::Duplicate("source"))
    );
    assert_eq!(bindings.source(source), Ok(first));
    if let SourceRuntime::Registers {
        columns: registers,
        rowid: runtime_rowid,
    } = first
    {
        let position = tc.draw(generators::integers::<usize>().max_value(width - 1));
        assert_eq!(
            registers.register(position),
            Some(RegisterId(70 + position))
        );
        assert_eq!(registers.register(width), None);
        assert_eq!(runtime_rowid, Some(rowid));
    }
}

// Example: evaluating `ENCODE (value + precision)` for a custom column binds
// only `[value, precision]` while that schema program runs. Afterwards its
// synthetic input source is gone, while the surrounding query's row remains.
#[hegel::test]
fn schema_program_inputs_have_exact_width_and_restore_the_outer_frame(tc: hegel::TestCase) {
    let outer_width = generated_width(&tc);
    let argument_count = usize::from(tc.draw(generators::integers::<u8>().max_value(7)));
    let input_width = argument_count + 1;
    let mut document = correlated_document(outer_width);
    let input_source = SourceId::new(document.sources.len());
    document
        .sources
        .push(source(input_source, SourceOwner::Root, input_width));
    let program = SchemaProgramId::new(0);
    document.schema_programs.push(BoundSchemaProgram {
        input_source,
        body: Expr::column(input_source, 0),
    });
    document.queries[1].blocks[0].outputs[0].expr = Expr::Cast {
        expr: Box::new(Expr::column(SourceId::new(0), 0)),
        target: TypeName {
            name: "property_type".to_string(),
            parameters: Vec::new(),
            array_dimensions: 0,
            type_fact: TypeFact::known(Type::Integer),
            affinity: Affinity::Integer,
            programs: BoundCastPrograms {
                encode: vec![BoundSchemaCall {
                    program,
                    arguments: vec![
                        Expr::Literal(turso_parser::ast::Literal::Null);
                        argument_count
                    ],
                }],
                domain: None,
                apply_builtin_affinity: false,
            },
        },
    };
    document
        .validate()
        .expect("the schema-program document is closed");

    let outer_source = SourceId::new(0);
    let outer_runtime = SourceRuntime::Registers {
        columns: RegisterRange::new(10, outer_width),
        rowid: Some(RegisterId(9)),
    };
    let mut bindings = RuntimeBindings::new(&document, document.snapshot)
        .expect("the generated HIR document is closed");
    bindings
        .enter_query(QueryId::new(0))
        .expect("q0 is the root query");
    bindings
        .bind_source(outer_source, outer_runtime)
        .expect("the outer row belongs to q0");

    assert_eq!(
        bindings.enter_schema_program(program, RegisterRange::new(100, input_width + 1)),
        Err(RuntimeBindingError::SourceWidth {
            source: input_source,
            expected: input_width,
            actual: input_width + 1,
        })
    );
    bindings
        .enter_schema_program(program, RegisterRange::new(100, input_width))
        .expect("value and every user argument are present");
    assert_eq!(
        bindings.source(input_source),
        Ok(SourceRuntime::Registers {
            columns: RegisterRange::new(100, input_width),
            rowid: None,
        })
    );
    assert_eq!(bindings.source(outer_source), Ok(outer_runtime));
    assert_eq!(bindings.leave_schema_program(), Ok(program));
    assert_eq!(
        bindings.source(input_source),
        Err(RuntimeBindingError::SourceNotCaptured(input_source))
    );
    assert_eq!(bindings.source(outer_source), Ok(outer_runtime));
}

// Example: a plan built for schema snapshot 7 cannot be paired with runtime
// mappings for snapshot 8, even when every document-local ID is numerically the
// same.
#[hegel::test]
fn runtime_bindings_reject_another_catalog_snapshot(tc: hegel::TestCase) {
    let document = correlated_document(generated_width(&tc));
    let other = CatalogSnapshot::from_id(document.snapshot.id() + 1);

    assert!(matches!(
        RuntimeBindings::new(&document, other),
        Err(RuntimeBindingError::SnapshotMismatch)
    ));
}

// Example: `SELECT (SELECT o.c0) FROM left AS o, right` produces a scan-only
// outer block in written source order. Its physical output points at the exact
// `hir::Expr::Subquery` owned by HIR; planning does not clone or translate it
// into parser expressions.
#[hegel::test]
fn scan_plans_preserve_hir_expressions_and_source_order(tc: hegel::TestCase) {
    let document = correlated_document(generated_width(&tc));

    let plan = PhysicalPlan::new(&document).expect("every generated closed HIR document plans");
    assert!(matches!(
        plan.root,
        PhysicalRoot::Query(query) if query == QueryId::new(0)
    ));
    assert!(std::ptr::eq(plan.document, &document));
    let outer = plan.query(QueryId::new(0)).expect("the outer plan exists");
    assert!(std::ptr::eq(outer.hir, &document.queries[0]));
    assert_eq!(
        outer.blocks[0].source_order,
        [SourceId::new(0), SourceId::new(1)]
    );
    assert!(std::ptr::eq(
        &outer.blocks[0].outputs[0].expr,
        &document.queries[0].blocks[0].outputs[0].expr
    ));
    for source in [SourceId::new(0), SourceId::new(1)] {
        assert!(matches!(
            plan.source(source).map(|source| &source.kind),
            Some(PhysicalSourceKind::SchemaExpression)
        ));
    }
}

// Example: if q1 says it no longer captures `o` while its output still reads
// `o.c0`, physical planning refuses the document instead of guessing an outer
// cursor or doing another name-resolution pass.
#[hegel::test]
fn physical_planning_rejects_incomplete_hir_instead_of_resolving_again(tc: hegel::TestCase) {
    let mut document = correlated_document(generated_width(&tc));
    document.queries[1].captures.clear();

    assert!(matches!(
        PhysicalPlan::new(&document),
        Err(PhysicalPlanError::InvalidDocument(_))
    ));
}
