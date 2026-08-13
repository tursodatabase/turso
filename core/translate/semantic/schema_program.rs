//! Binding stored custom-type expressions into document-owned HIR programs.

use super::{
    analyze::Analyzer,
    expr::ExprPolicy,
    hir::{
        self, BoundDomainCheck, BoundDomainConstraints, BoundSchemaCall, BoundSchemaProgram,
        ColumnReadExpression, DatabaseId, IndexCoverage, IndexHint, ResolvedType, Source,
        SourceColumn, SourceKind, SourceOwner, TypeFact,
    },
    scope::{ResolvedScopeExpr, Scope},
};
use crate::{schema::Type, vdbe::affinity::Affinity, LimboError, Result};

struct SchemaInput {
    name: String,
    type_fact: TypeFact,
}

#[derive(Clone, Copy)]
pub(super) enum TypeTransform {
    Encode,
    Decode,
}

impl Analyzer<'_, '_, '_> {
    pub(super) fn bind_type_encoder(
        &mut self,
        definition: &ResolvedType,
        arguments: &[ResolvedScopeExpr],
    ) -> Result<Option<BoundSchemaCall>> {
        self.bind_type_transform(definition, arguments, TypeTransform::Encode)
    }

    pub(super) fn bind_type_transform(
        &mut self,
        definition: &ResolvedType,
        arguments: &[ResolvedScopeExpr],
        transform: TypeTransform,
    ) -> Result<Option<BoundSchemaCall>> {
        let (expression, value_type, description) = match transform {
            TypeTransform::Encode => (
                definition.value().encode(),
                definition.value().value_input_type(),
                "encode",
            ),
            TypeTransform::Decode => (
                definition.value().decode(),
                definition.value().base(),
                "decode",
            ),
        };
        let Some(expression) = expression else {
            return Ok(None);
        };
        let expected = definition.value().user_params().count();
        let arguments = if expected == 0 {
            &[][..]
        } else if expected == arguments.len() {
            arguments
        } else {
            return Err(LimboError::InternalError(format!(
                "custom type '{}' {description} program expects {expected} arguments, got {}",
                definition.value().name,
                arguments.len()
            )));
        };

        let mut inputs = Vec::with_capacity(arguments.len() + 1);
        inputs.push(SchemaInput {
            name: "value".to_string(),
            type_fact: declared_input_fact(value_type),
        });
        inputs.extend(definition.value().user_params().zip(arguments).map(
            |(parameter, argument)| SchemaInput {
                name: parameter.name.clone(),
                type_fact: argument.type_fact.clone(),
            },
        ));

        if !self.enter_schema_program_binding(definition.id()) {
            crate::bail_parse_error!(
                "recursive {description} program for custom type '{}'",
                definition.value().name
            );
        }
        let result = self.bind_schema_program(expression, definition.database(), &inputs);
        self.leave_schema_program_binding(definition.id());
        let program = result?;
        Ok(Some(BoundSchemaCall {
            program,
            arguments: arguments
                .iter()
                .map(|argument| argument.expr.clone())
                .collect(),
        }))
    }

    pub(super) fn bind_domain_constraints(
        &mut self,
        value_type: &TypeFact,
        chain: &[ResolvedType],
    ) -> Result<BoundDomainConstraints> {
        let domain_name = chain
            .first()
            .expect("a resolved domain has at least one definition")
            .value()
            .name
            .as_str();
        let not_null_description = chain
            .iter()
            .any(|definition| definition.value().not_null)
            .then(|| format!("domain {domain_name} does not allow null values"));
        let input = [SchemaInput {
            name: "value".to_string(),
            type_fact: value_type
                .storage
                .map_or_else(TypeFact::dynamic, TypeFact::known),
        }];
        let mut checks = Vec::new();
        for definition in chain {
            for (index, constraint) in definition.value().domain_checks.iter().enumerate() {
                if !self.enter_schema_program_binding(definition.id()) {
                    crate::bail_parse_error!(
                        "recursive domain CHECK program for domain '{}'",
                        definition.value().name
                    );
                }
                let result = self.bind_schema_program(
                    constraint.check.as_ref(),
                    definition.database(),
                    &input,
                );
                self.leave_schema_program_binding(definition.id());
                let program = result?;
                let name = constraint
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("{}_{}", definition.value().name, index));
                checks.push(BoundDomainCheck {
                    call: BoundSchemaCall {
                        program,
                        arguments: Vec::new(),
                    },
                    failure_description: format!(
                        "value for domain {} violates check constraint \"{}\"",
                        definition.value().name,
                        name
                    ),
                });
            }
        }
        Ok(BoundDomainConstraints {
            not_null_description,
            checks,
        })
    }

    fn bind_schema_program(
        &mut self,
        expression: &turso_parser::ast::Expr,
        database: Option<DatabaseId>,
        inputs: &[SchemaInput],
    ) -> Result<hir::SchemaProgramId> {
        let source_id = self.reserve_source();
        let columns = inputs
            .iter()
            .map(|input| SourceColumn {
                name: input.name.clone(),
                affinity: input_affinity(&input.type_fact),
                has_affinity: true,
                type_fact: input.type_fact.clone(),
                collation: None,
                hidden: false,
                rowid_alias: false,
            })
            .collect::<Vec<_>>();
        let width = columns.len();
        let source = Source {
            id: source_id,
            owner: SourceOwner::Root,
            database,
            name: "schema expression inputs".to_string(),
            alias: None,
            kind: SourceKind::SchemaExpression,
            columns,
            generated_expressions: vec![ColumnReadExpression::Absent; width],
            default_expressions: vec![ColumnReadExpression::Absent; width],
            column_type_programs: vec![None; width],
            check_constraints: None,
            rowid_available: false,
            index_hint: IndexHint::None,
            index_expressions: Vec::new(),
            index_coverage: IndexCoverage::Selective,
            index_method_patterns: Vec::new(),
        };
        let mut scope = Scope::default();
        scope.add_source(&source, true);
        self.insert_source(source_id, source)?;
        let body = self.analyze_expr(expression, &scope, ExprPolicy::schema_expression())?;
        let program = self.reserve_schema_program();
        self.insert_schema_program(
            program,
            BoundSchemaProgram {
                input_source: source_id,
                body,
            },
        )?;
        Ok(program)
    }
}

fn declared_input_fact(name: &str) -> TypeFact {
    if name.eq_ignore_ascii_case("any") {
        return TypeFact::dynamic();
    }
    TypeFact::declared(hir::DeclaredType {
        name: name.to_string(),
        storage: Affinity::affinity(name).to_type(),
        custom_chain: Vec::new(),
        array_dimensions: 0,
    })
}

fn input_affinity(type_fact: &TypeFact) -> Affinity {
    if let Some(declared) = &type_fact.declared {
        return Affinity::affinity(&declared.name);
    }
    match type_fact.storage {
        Some(Type::Integer) => Affinity::Integer,
        Some(Type::Real) => Affinity::Real,
        Some(Type::Text) => Affinity::Text,
        Some(Type::Numeric) => Affinity::Numeric,
        Some(Type::Null | Type::Blob) | None => Affinity::Blob,
    }
}
