//! Binding of custom-type schema expressions into one HIR document.

use super::{
    hir::{
        self, BoundArrayStorage, BoundCastPrograms, BoundColumnTypePrograms, BoundDomainCheck,
        BoundDomainConstraints, BoundSchemaCall, BoundSchemaProgram, CatalogObjectId, ResolvedType,
        SchemaProgramId, TypeFact,
    },
    schema_expr::SchemaExprInput,
    scope::Scope,
    Analyzer,
};
use crate::{
    schema::Column,
    schema_expr::{SchemaExprProfile, SelfColumn, ValidSchemaExpr},
    vdbe::affinity::Affinity,
    LimboError, Result, MAIN_DB_ID,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct SchemaProgramKey {
    definition: CatalogObjectId,
    kind: SchemaProgramKind,
    profile: BoundSchemaProfile,
    inputs: Vec<TypeFactKey>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum SchemaProgramKind {
    Encode,
    Decode,
    DomainCheck(usize),
}

impl SchemaProgramKind {
    const fn description(self) -> &'static str {
        match self {
            Self::Encode => "encode",
            Self::Decode => "decode",
            Self::DomainCheck(_) => "domain CHECK",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum BoundSchemaProfile {
    TypeTransform,
    DomainCheck,
}

impl BoundSchemaProfile {
    fn from_expression(expression: &ValidSchemaExpr) -> Result<Self> {
        match expression.profile() {
            SchemaExprProfile::TypeTransform => Ok(Self::TypeTransform),
            SchemaExprProfile::DomainCheck => Ok(Self::DomainCheck),
            profile => Err(LimboError::InternalError(format!(
                "{} cannot be bound as a custom-type program",
                profile.description()
            ))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct TypeFactKey {
    storage: Option<u8>,
    declared: Option<DeclaredTypeKey>,
    array_dimensions: u32,
    array_rank_unbounded: bool,
}

impl From<&TypeFact> for TypeFactKey {
    fn from(fact: &TypeFact) -> Self {
        Self {
            storage: fact.storage.map(|storage| storage as u8),
            array_dimensions: fact.array_dimensions,
            array_rank_unbounded: fact.array_rank_unbounded,
            declared: fact.declared.as_ref().map(|declared| DeclaredTypeKey {
                name: declared.name.clone(),
                storage: declared.storage as u8,
                custom_chain: declared
                    .custom_chain
                    .iter()
                    .map(|definition| definition.id())
                    .collect(),
                array_dimensions: declared.array_dimensions,
            }),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct DeclaredTypeKey {
    name: String,
    storage: u8,
    custom_chain: Vec<CatalogObjectId>,
    array_dimensions: u32,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum SchemaProgramBindingState {
    Binding,
    Bound(SchemaProgramId),
}

#[derive(Clone, Debug)]
struct SchemaProgramArgument {
    expression: hir::Expr,
    type_fact: TypeFact,
}

impl Analyzer<'_, '_> {
    /// Bind the transforms needed when reading or storing one concrete table
    /// column. Unused type arguments stay unresolved when no transform needs
    /// them.
    pub(crate) fn bind_column_type_programs(
        &mut self,
        column: &Column,
        type_fact: &TypeFact,
        table_name: &str,
    ) -> Result<Option<BoundColumnTypePrograms>> {
        let declared = type_fact.declared.as_ref();
        let chain = declared.map_or(&[][..], |declared| declared.custom_chain.as_slice());
        let dimensions = declared.map_or(column.array_dimensions(), |declared| {
            declared.array_dimensions
        });
        let needs_arguments = chain.iter().any(|definition| {
            definition.value().encode().is_some() || definition.value().decode().is_some()
        });
        let database = chain
            .first()
            .and_then(|definition| definition.database())
            .map_or(MAIN_DB_ID, |database| database.index());
        let arguments = if needs_arguments {
            self.bind_column_arguments(column, database)?
        } else {
            Vec::new()
        };

        let mut encode = Vec::new();
        for definition in chain {
            if let Some(call) =
                self.bind_type_transform(definition, SchemaProgramKind::Encode, &arguments)?
            {
                encode.push(call);
            }
        }

        let mut decode = Vec::new();
        for definition in chain.iter().rev() {
            if let Some(call) =
                self.bind_type_transform(definition, SchemaProgramKind::Decode, &arguments)?
            {
                decode.push(call);
            }
        }

        let array = (dimensions > 0).then(|| {
            let any = column.ty_str.eq_ignore_ascii_case("ANY");
            let multidimensional = dimensions > 1;
            BoundArrayStorage {
                element_affinity: if any || multidimensional {
                    Affinity::Blob
                } else {
                    Affinity::affinity(&column.ty_str)
                },
                element_type: if any || multidimensional {
                    "ANY".to_string()
                } else {
                    column.ty_str.to_uppercase()
                },
                table_name: table_name.to_string(),
                column_name: column.name.clone().unwrap_or_default(),
                dimensions,
            }
        });

        if chain.is_empty() && array.is_none() {
            return Ok(None);
        }
        Ok(Some(BoundColumnTypePrograms {
            encode,
            decode,
            array,
            encode_nulls: dimensions == 0
                && chain.iter().any(|definition| definition.value().not_null),
        }))
    }

    /// Bind the custom work for one CAST target. The returned bundle is
    /// concrete even for built-in fallback so code generation cannot confuse
    /// a no-op custom cast with ordinary affinity handling.
    pub(crate) fn bind_cast_programs(
        &mut self,
        target_type: &TypeFact,
        parameters: &[hir::Expr],
        scope: &Scope,
    ) -> Result<BoundCastPrograms> {
        let arguments = parameters
            .iter()
            .map(|expression| SchemaProgramArgument {
                expression: expression.clone(),
                type_fact: self.expression_type_fact(expression, scope),
            })
            .collect::<Vec<_>>();
        self.require_schema_program_arguments(&arguments);

        let Some(declared) = &target_type.declared else {
            return Ok(Self::builtin_cast_programs());
        };
        let Some(leaf) = declared.custom() else {
            return Ok(Self::builtin_cast_programs());
        };
        let leaf_parameter_count = leaf.value().user_params().count();
        if leaf_parameter_count != 0 && leaf_parameter_count != arguments.len() {
            return Ok(Self::builtin_cast_programs());
        }

        let mut encode = Vec::new();
        for definition in &declared.custom_chain {
            if let Some(call) =
                self.bind_type_transform(definition, SchemaProgramKind::Encode, &arguments)?
            {
                encode.push(call);
            }
        }
        let domain = leaf
            .value()
            .is_domain
            .then(|| self.bind_domain_constraints(target_type, &declared.custom_chain))
            .transpose()?;
        Ok(BoundCastPrograms {
            encode,
            domain,
            apply_builtin_affinity: false,
        })
    }

    /// Bind all inherited checks for a resolved domain value.
    pub(crate) fn bind_domain_constraints(
        &mut self,
        value_type: &TypeFact,
        chain: &[ResolvedType],
    ) -> Result<BoundDomainConstraints> {
        let not_null_description = chain
            .iter()
            .any(|definition| definition.value().not_null)
            .then(|| {
                format!(
                    "domain {} does not allow null values",
                    chain
                        .first()
                        .map(|definition| definition.value().name.as_str())
                        .unwrap_or("?")
                )
            });
        let mut checks = Vec::new();
        for definition in chain {
            for (index, constraint) in definition.value().domain_checks.iter().enumerate() {
                let database = definition
                    .database()
                    .map_or(MAIN_DB_ID, |database| database.index());
                let visible_types = [definition.handle()];
                let stored = self.resolve_standalone_schema_syntax(
                    &constraint.check,
                    SchemaExprProfile::DomainCheck,
                    database,
                    Some(&definition.value().name),
                    &[],
                    &visible_types,
                )?;
                let expression = stored
                    .as_valid()?
                    .specialize_domain_value(SelfColumn::new(0, false))?;
                let input = SchemaExprInput {
                    name: "value".to_string(),
                    declared_type: Some(definition.value().name.clone()),
                    array_dimensions: 0,
                    type_fact: Some(value_type.clone()),
                };
                let call = self.bind_schema_program(
                    definition,
                    SchemaProgramKind::DomainCheck(index),
                    expression,
                    vec![input],
                    &[],
                )?;
                let name = constraint
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("{}_{}", definition.value().name, index));
                checks.push(BoundDomainCheck {
                    call,
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

    /// Bind the one leaf encoder used to convert a literal operand before a
    /// custom binary operator call.
    pub(crate) fn bind_literal_encoder(
        &mut self,
        column: Option<&Column>,
        definition: &ResolvedType,
    ) -> Result<Option<BoundSchemaCall>> {
        if definition.value().encode().is_none() {
            return Ok(None);
        }
        let database = definition
            .database()
            .map_or(MAIN_DB_ID, |database| database.index());
        let arguments = match column {
            Some(column) => self.bind_column_arguments(column, database)?,
            None if definition.value().user_params().next().is_none() => Vec::new(),
            None => {
                return Err(LimboError::ParseError(format!(
                    "cannot encode a literal as custom type '{}' because this expression does not retain its type parameters",
                    definition.value().name
                )))
            }
        };
        self.bind_type_transform(definition, SchemaProgramKind::Encode, &arguments)
    }

    fn builtin_cast_programs() -> BoundCastPrograms {
        BoundCastPrograms {
            encode: Vec::new(),
            domain: None,
            apply_builtin_affinity: true,
        }
    }

    fn bind_column_arguments(
        &mut self,
        column: &Column,
        database: usize,
    ) -> Result<Vec<SchemaProgramArgument>> {
        if column.ty_params.is_empty() {
            return Ok(Vec::new());
        }
        let source = self.create_schema_expression_source(database, &[])?;
        let scope = Scope::default();
        column
            .ty_params
            .iter()
            .map(|argument| {
                let stored = self.resolve_standalone_schema_syntax(
                    argument,
                    SchemaExprProfile::Default,
                    database,
                    Some(&column.ty_str),
                    &[],
                    &[],
                )?;
                let expression = self.instantiate_schema_expr(stored.as_valid()?, source)?;
                let type_fact = self.expression_type_fact(&expression, &scope);
                Ok(SchemaProgramArgument {
                    expression,
                    type_fact,
                })
            })
            .collect()
    }

    fn bind_type_transform(
        &mut self,
        definition: &ResolvedType,
        kind: SchemaProgramKind,
        arguments: &[SchemaProgramArgument],
    ) -> Result<Option<BoundSchemaCall>> {
        let stored = match kind {
            SchemaProgramKind::Encode => definition.value().encode(),
            SchemaProgramKind::Decode => definition.value().decode(),
            SchemaProgramKind::DomainCheck(_) => {
                return Err(LimboError::InternalError(
                    "a domain CHECK cannot be bound as a type transform".to_string(),
                ));
            }
        };
        let Some(stored) = stored else {
            return Ok(None);
        };
        let arguments = Self::arguments_for_definition(definition, arguments)?;
        let database = definition
            .database()
            .map_or(MAIN_DB_ID, |database| database.index());
        let visible_types = [definition.handle()];
        let stored = self.resolve_standalone_schema_syntax(
            stored,
            SchemaExprProfile::TypeTransform,
            database,
            Some(&definition.value().name),
            definition.value().params(),
            &visible_types,
        )?;
        let expression = stored.as_valid()?.specialize_type_parameters()?;
        let value_fact = self.resolve_declared_type_fact_in_database(
            definition.value().value_input_type(),
            0,
            database,
        )?;
        let mut inputs = Vec::with_capacity(arguments.len() + 1);
        inputs.push(SchemaExprInput {
            name: "value".to_string(),
            declared_type: Some(definition.value().value_input_type().to_string()),
            array_dimensions: 0,
            type_fact: Some(value_fact),
        });
        for (parameter, argument) in definition.value().user_params().zip(arguments) {
            inputs.push(SchemaExprInput {
                name: parameter.name.clone(),
                declared_type: parameter.ty.clone(),
                array_dimensions: 0,
                type_fact: Some(argument.type_fact.clone()),
            });
        }
        self.bind_schema_program(definition, kind, expression, inputs, arguments)
            .map(Some)
    }

    fn arguments_for_definition<'a>(
        definition: &ResolvedType,
        arguments: &'a [SchemaProgramArgument],
    ) -> Result<&'a [SchemaProgramArgument]> {
        let expected = definition.value().user_params().count();
        if expected == 0 {
            return Ok(&[]);
        }
        if expected != arguments.len() {
            return Err(LimboError::InternalError(format!(
                "custom type {} expects {expected} parameters but its application has {}",
                definition.value().name,
                arguments.len()
            )));
        }
        Ok(arguments)
    }

    fn bind_schema_program(
        &mut self,
        definition: &ResolvedType,
        kind: SchemaProgramKind,
        expression: ValidSchemaExpr,
        inputs: Vec<SchemaExprInput>,
        arguments: &[SchemaProgramArgument],
    ) -> Result<BoundSchemaCall> {
        self.require_schema_program_arguments(arguments);
        let profile = BoundSchemaProfile::from_expression(&expression)?;
        let input_facts = inputs
            .iter()
            .map(|input| {
                input
                    .type_fact
                    .as_ref()
                    .map(TypeFactKey::from)
                    .ok_or_else(|| {
                        LimboError::InternalError(
                            "bound schema program input has no resolved type fact".to_string(),
                        )
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let key = SchemaProgramKey {
            definition: definition.id(),
            kind,
            profile,
            inputs: input_facts,
        };
        match self.schema_program_bindings.get(&key).copied() {
            Some(SchemaProgramBindingState::Bound(program)) => {
                return Ok(BoundSchemaCall {
                    program,
                    arguments: arguments
                        .iter()
                        .map(|argument| argument.expression.clone())
                        .collect(),
                });
            }
            Some(SchemaProgramBindingState::Binding) => {
                return Err(LimboError::ParseError(format!(
                    "recursive {} program for custom type '{}'",
                    kind.description(),
                    definition.value().name
                )));
            }
            None => {}
        }

        self.schema_program_bindings
            .insert(key.clone(), SchemaProgramBindingState::Binding);
        let result: Result<SchemaProgramId> = (|| {
            let database = definition
                .database()
                .map_or(MAIN_DB_ID, |database| database.index());
            let (input_source, body) =
                self.instantiate_synthetic_schema_expr(database, &expression, &inputs)?;
            let program = self.reserve_schema_program();
            self.insert_schema_program(program, BoundSchemaProgram { input_source, body })?;
            Ok(program)
        })();

        let program = match result {
            Ok(program) => program,
            Err(error) => {
                self.schema_program_bindings.remove(&key);
                return Err(error);
            }
        };
        let previous = self
            .schema_program_bindings
            .insert(key, SchemaProgramBindingState::Bound(program));
        assert!(
            matches!(previous, Some(SchemaProgramBindingState::Binding)),
            "schema program binding must transition from Binding to Bound"
        );
        Ok(BoundSchemaCall {
            program,
            arguments: arguments
                .iter()
                .map(|argument| argument.expression.clone())
                .collect(),
        })
    }

    fn require_schema_program_arguments(&mut self, arguments: &[SchemaProgramArgument]) {
        for argument in arguments {
            self.require_source_columns_in_expr(&argument.expression);
        }
    }
}
