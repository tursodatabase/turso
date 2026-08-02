//! Property tests for closed HIR document structure.

use hegel::generators;
use turso_parser::ast::{Literal, Materialized};

use super::*;
use crate::{schema::Type, vdbe::affinity::Affinity};

fn generated_count(tc: &hegel::TestCase) -> usize {
    usize::from(tc.draw(generators::integers::<u8>())) + 1
}

fn generated_position(tc: &hegel::TestCase, width: usize) -> usize {
    tc.draw(generators::integers::<usize>().max_value(width - 1))
}

fn source_column(position: usize) -> SourceColumn {
    SourceColumn {
        name: format!("c{position}"),
        type_fact: TypeFact::known(Type::Integer),
        affinity: Affinity::Integer,
        has_affinity: true,
        collation: None,
        hidden: false,
        rowid_alias: false,
    }
}

fn source(id: SourceId, owner: SourceOwner, width: usize, kind: SourceKind) -> Source {
    Source {
        id,
        owner,
        database: None,
        name: format!("source_{}", id.index()),
        alias: None,
        kind,
        columns: (0..width).map(source_column).collect(),
        generated_expressions: vec![ColumnReadExpression::Absent; width],
        default_expressions: vec![ColumnReadExpression::Absent; width],
        column_type_programs: vec![None; width],
        check_constraints: Vec::new(),
        rowid_available: false,
        index_hint: IndexHint::None,
        index_expressions: Vec::new(),
        index_method_patterns: Vec::new(),
    }
}

fn output(block: QueryBlockId, index: usize, expression: Expr) -> Output {
    Output {
        id: OutputId::query(block, index),
        name: format!("output_{index}"),
        expr: expression,
        type_fact: TypeFact::known(Type::Integer),
        affinity: Affinity::Integer,
        has_affinity: true,
        collation: None,
        name_kind: OutputNameKind::Inferred,
    }
}

fn generated_query_document(tc: &hegel::TestCase) -> HirDocument {
    let query_id = QueryId::new(0);
    let block_id = QueryBlockId::new(query_id, 0);
    let source_id = SourceId::new(0);
    let column_count = generated_count(tc);
    let output_count = generated_count(tc);
    let outputs = (0..output_count)
        .map(|index| {
            output(
                block_id,
                index,
                Expr::column(source_id, generated_position(tc, column_count)),
            )
        })
        .collect::<Vec<_>>();
    let query_outputs = outputs.iter().map(|output| output.id).collect();
    let block = QueryBlock {
        id: block_id,
        from: Some(From {
            first: source_id,
            joins: Vec::new(),
        }),
        outputs,
        body: QueryBlockBody::Select {
            distinctness: None,
            filter: None,
            grouping: None,
            windows: Vec::new(),
        },
    };

    HirDocument {
        snapshot: CatalogSnapshot::from_id(1),
        root: HirRoot::Query(QueryRoot {
            query: query_id,
            trigger: None,
        }),
        queries: vec![Query {
            id: query_id,
            reachable_ctes: Vec::new(),
            blocks: vec![block],
            first: block_id,
            compounds: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            output: query_outputs,
        }],
        sources: vec![source(
            source_id,
            SourceOwner::QueryBlock(block_id),
            column_count,
            SourceKind::SchemaExpression,
        )],
        ctes: Vec::new(),
        schema_programs: Vec::new(),
    }
}

fn schema_call(program: SchemaProgramId) -> BoundSchemaCall {
    BoundSchemaCall {
        program,
        arguments: vec![Expr::Literal(Literal::Null)],
    }
}

fn expression_with_schema_call(program: SchemaProgramId) -> Expr {
    Expr::Cast {
        expr: Box::new(Expr::Literal(Literal::Null)),
        target: TypeName {
            name: "domain_value".to_string(),
            parameters: Vec::new(),
            array_dimensions: 0,
            type_fact: TypeFact::known(Type::Integer),
            programs: BoundCastPrograms {
                encode: vec![schema_call(program)],
                domain: None,
                apply_builtin_affinity: false,
            },
        },
    }
}

fn generated_schema_program_document(tc: &hegel::TestCase) -> HirDocument {
    let mut document = generated_query_document(tc);
    let program_id = SchemaProgramId::new(0);
    let input_source = SourceId::new(document.sources.len());
    let input_width = generated_count(tc);
    let input_position = generated_position(tc, input_width);
    document.sources.push(source(
        input_source,
        SourceOwner::Root,
        input_width,
        SourceKind::SchemaExpression,
    ));
    document.schema_programs.push(BoundSchemaProgram {
        input_source,
        body: Expr::column(input_source, input_position),
    });
    document.queries[0].blocks[0].outputs[0].expr = expression_with_schema_call(program_id);
    document
}

fn generated_cte_document(tc: &hegel::TestCase) -> HirDocument {
    let root_query = QueryId::new(0);
    let root_block = QueryBlockId::new(root_query, 0);
    let body_query = QueryId::new(1);
    let body_block = QueryBlockId::new(body_query, 0);
    let cte_id = CteId::new(0);
    let source_id = SourceId::new(0);
    let width = generated_count(tc);
    let root_outputs = (0..width)
        .map(|position| output(root_block, position, Expr::column(source_id, position)))
        .collect::<Vec<_>>();
    let body_outputs = (0..width)
        .map(|position| {
            output(
                body_block,
                position,
                Expr::Literal(Literal::Numeric(position.to_string())),
            )
        })
        .collect::<Vec<_>>();
    let root_output_ids = root_outputs.iter().map(|output| output.id).collect();
    let body_output_ids = body_outputs.iter().map(|output| output.id).collect();

    HirDocument {
        snapshot: CatalogSnapshot::from_id(1),
        root: HirRoot::Query(QueryRoot {
            query: root_query,
            trigger: None,
        }),
        queries: vec![
            Query {
                id: root_query,
                reachable_ctes: vec![cte_id],
                blocks: vec![QueryBlock {
                    id: root_block,
                    from: Some(From {
                        first: source_id,
                        joins: Vec::new(),
                    }),
                    outputs: root_outputs,
                    body: QueryBlockBody::Select {
                        distinctness: None,
                        filter: None,
                        grouping: None,
                        windows: Vec::new(),
                    },
                }],
                first: root_block,
                compounds: Vec::new(),
                order_by: Vec::new(),
                limit: None,
                output: root_output_ids,
            },
            Query {
                id: body_query,
                reachable_ctes: Vec::new(),
                blocks: vec![QueryBlock {
                    id: body_block,
                    from: None,
                    outputs: body_outputs,
                    body: QueryBlockBody::Select {
                        distinctness: None,
                        filter: None,
                        grouping: None,
                        windows: Vec::new(),
                    },
                }],
                first: body_block,
                compounds: Vec::new(),
                order_by: Vec::new(),
                limit: None,
                output: body_output_ids,
            },
        ],
        sources: vec![source(
            source_id,
            SourceOwner::QueryBlock(root_block),
            width,
            SourceKind::Cte(cte_id),
        )],
        ctes: vec![Cte {
            id: cte_id,
            name: "generated_cte".to_string(),
            columns: (0..width)
                .map(|position| CteColumn {
                    name: format!("c{position}"),
                    type_fact: TypeFact::known(Type::Integer),
                    affinity: Affinity::Integer,
                    has_affinity: true,
                    collation: None,
                })
                .collect(),
            materialized: Materialized::Any,
            body: CteBody::Query(body_query),
        }],
        schema_programs: Vec::new(),
    }
}

fn first_output_mut(document: &mut HirDocument) -> &mut Output {
    &mut document.queries[0].blocks[0].outputs[0]
}

// Example: a generated `SELECT c2 FROM items` has matching query, block,
// source, output, and column identities and therefore forms a closed document.
#[hegel::test]
fn generated_closed_documents_validate(tc: hegel::TestCase) {
    let document = generated_query_document(&tc);

    document.validate().expect("generated document is closed");
}

#[derive(Clone, Copy, Debug)]
enum MissingReference {
    RootQuery,
    QueryOutput,
    ExpressionSource,
    ExpressionColumn,
}

// Example: changing `c2` to source `s99`, column 99, or a nonexistent output
// makes validation fail instead of leaving a dangling document-local identity.
#[hegel::test]
fn out_of_range_references_are_rejected(tc: hegel::TestCase) {
    let mut document = generated_query_document(&tc);
    let corruption = tc.draw(generators::sampled_from(vec![
        MissingReference::RootQuery,
        MissingReference::QueryOutput,
        MissingReference::ExpressionSource,
        MissingReference::ExpressionColumn,
    ]));
    match corruption {
        MissingReference::RootQuery => {
            let HirRoot::Query(root) = &mut document.root else {
                unreachable!("the generator always creates a query root");
            };
            root.query = QueryId::new(document.queries.len());
        }
        MissingReference::QueryOutput => {
            let block = document.queries[0].blocks[0].id;
            document.queries[0].output[0] =
                OutputId::query(block, document.queries[0].blocks[0].outputs.len());
        }
        MissingReference::ExpressionSource => {
            first_output_mut(&mut document).expr =
                Expr::column(SourceId::new(document.sources.len()), 0);
        }
        MissingReference::ExpressionColumn => {
            let width = document.sources[0].columns.len();
            first_output_mut(&mut document).expr = Expr::column(SourceId::new(0), width);
        }
    }

    assert!(document.validate().is_err());
}

#[derive(Clone, Copy, Debug)]
enum WrongIdentity {
    Query,
    QueryBlock,
    Source,
    SourceOwner,
    OutputOwner,
}

// Example: an output stored in query block `q0:0` cannot claim to belong to
// the root, and a FROM source cannot claim a different owner.
#[hegel::test]
fn arena_identity_and_owner_disagreements_are_rejected(tc: hegel::TestCase) {
    let mut document = generated_query_document(&tc);
    let corruption = tc.draw(generators::sampled_from(vec![
        WrongIdentity::Query,
        WrongIdentity::QueryBlock,
        WrongIdentity::Source,
        WrongIdentity::SourceOwner,
        WrongIdentity::OutputOwner,
    ]));
    match corruption {
        WrongIdentity::Query => document.queries[0].id = QueryId::new(1),
        WrongIdentity::QueryBlock => {
            document.queries[0].blocks[0].id = QueryBlockId::new(QueryId::new(0), 1);
        }
        WrongIdentity::Source => document.sources[0].id = SourceId::new(1),
        WrongIdentity::SourceOwner => document.sources[0].owner = SourceOwner::Root,
        WrongIdentity::OutputOwner => {
            first_output_mut(&mut document).id = OutputId::root(0);
        }
    }

    assert!(document.validate().is_err());
}

// Example: appending a bound source that no root, query, expression, CTE, or
// schema program can reach is rejected as leftover analysis state.
#[hegel::test]
fn unreachable_arena_entries_are_rejected(tc: hegel::TestCase) {
    let mut document = generated_query_document(&tc);
    let source_id = SourceId::new(document.sources.len());
    document.sources.push(source(
        source_id,
        SourceOwner::Root,
        generated_count(&tc),
        SourceKind::SchemaExpression,
    ));

    assert!(document.validate().is_err());
}

#[derive(Clone, Copy, Debug)]
enum MisalignedSourceState {
    GeneratedExpressions,
    DefaultExpressions,
    TypePrograms,
}

// Example: a three-column source must have exactly three generated-expression,
// default-expression, and type-program slots, even when every slot is empty.
#[hegel::test]
fn source_column_state_widths_must_stay_aligned(tc: hegel::TestCase) {
    let mut document = generated_query_document(&tc);
    let corruption = tc.draw(generators::sampled_from(vec![
        MisalignedSourceState::GeneratedExpressions,
        MisalignedSourceState::DefaultExpressions,
        MisalignedSourceState::TypePrograms,
    ]));
    match corruption {
        MisalignedSourceState::GeneratedExpressions => {
            document.sources[0].generated_expressions.pop();
        }
        MisalignedSourceState::DefaultExpressions => {
            document.sources[0].default_expressions.pop();
        }
        MisalignedSourceState::TypePrograms => {
            document.sources[0].column_type_programs.pop();
        }
    }

    assert!(document.validate().is_err());
}

// Example: a cast whose encoder points to a completed schema program with a
// schema-expression input source is a closed part of the document.
#[hegel::test]
fn generated_schema_programs_validate(tc: hegel::TestCase) {
    let document = generated_schema_program_document(&tc);

    document
        .validate()
        .expect("generated schema program is closed");
}

// Example: `WITH generated_cte AS (SELECT 0, 1) SELECT * FROM generated_cte`
// reaches the CTE body query and keeps the CTE, source, and output widths aligned.
#[hegel::test]
fn generated_cte_documents_validate(tc: hegel::TestCase) {
    let document = generated_cte_document(&tc);

    document
        .validate()
        .expect("generated CTE document is closed");
}

#[derive(Clone, Copy, Debug)]
enum BrokenCte {
    MissingCte,
    WrongCteIdentity,
    MissingBodyQuery,
    WrongWidth,
    WrongReachabilitySummary,
}

// Example: a `FROM generated_cte` source cannot point to `c99`, disagree with
// the CTE output width, lose its body query, or disappear from the query summary.
#[hegel::test]
fn broken_cte_structure_is_rejected(tc: hegel::TestCase) {
    let mut document = generated_cte_document(&tc);
    let corruption = tc.draw(generators::sampled_from(vec![
        BrokenCte::MissingCte,
        BrokenCte::WrongCteIdentity,
        BrokenCte::MissingBodyQuery,
        BrokenCte::WrongWidth,
        BrokenCte::WrongReachabilitySummary,
    ]));
    match corruption {
        BrokenCte::MissingCte => document.sources[0].kind = SourceKind::Cte(CteId::new(1)),
        BrokenCte::WrongCteIdentity => document.ctes[0].id = CteId::new(1),
        BrokenCte::MissingBodyQuery => {
            document.ctes[0].body = CteBody::Query(QueryId::new(document.queries.len()));
        }
        BrokenCte::WrongWidth => {
            document.ctes[0].columns.pop();
        }
        BrokenCte::WrongReachabilitySummary => {
            document.queries[0].reachable_ctes.clear();
        }
    }

    assert!(document.validate().is_err());
}

#[derive(Clone, Copy, Debug)]
enum BrokenSchemaProgram {
    MissingProgram,
    MissingInputSource,
    RecursiveProgram,
    UnreachableProgram,
}

// Example: an encoder may not call `schema_program99`, use a missing input
// source, recursively call itself, or remain unreferenced by the document.
#[hegel::test]
fn unfinished_schema_program_state_is_rejected(tc: hegel::TestCase) {
    let mut document = generated_schema_program_document(&tc);
    let corruption = tc.draw(generators::sampled_from(vec![
        BrokenSchemaProgram::MissingProgram,
        BrokenSchemaProgram::MissingInputSource,
        BrokenSchemaProgram::RecursiveProgram,
        BrokenSchemaProgram::UnreachableProgram,
    ]));
    match corruption {
        BrokenSchemaProgram::MissingProgram => {
            first_output_mut(&mut document).expr =
                expression_with_schema_call(SchemaProgramId::new(1));
        }
        BrokenSchemaProgram::MissingInputSource => {
            document.schema_programs[0].input_source = SourceId::new(document.sources.len());
        }
        BrokenSchemaProgram::RecursiveProgram => {
            document.schema_programs[0].body = expression_with_schema_call(SchemaProgramId::new(0));
        }
        BrokenSchemaProgram::UnreachableProgram => {
            first_output_mut(&mut document).expr = Expr::Literal(Literal::Null);
        }
    }

    assert!(document.validate().is_err());
}
