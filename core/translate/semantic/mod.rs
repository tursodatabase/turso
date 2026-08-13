//! Semantic analysis from parser AST into resolved HIR.

mod analyze;
pub(crate) mod catalog;
pub(crate) mod context;
mod cte;
mod delete;
mod dml;
mod expr;
pub(crate) mod hir;
mod insert;
mod query;
mod schema_program;
mod scope;
mod trigger;
mod update;

pub(crate) struct SemanticOptions {
    pub(crate) dialect: crate::sync::Arc<dyn crate::dialect::Dialect>,
    pub(crate) custom_types_enabled: bool,
    pub(crate) dqs_dml: context::DoubleQuotedDml,
}

pub(crate) enum SemanticRootInput<'ast> {
    Statement(&'ast turso_parser::ast::Stmt),
    TriggerProgram {
        database: hir::DatabaseId,
        table_name: &'ast str,
        event: turso_parser::ast::TriggerEvent,
        predicate: Option<&'ast turso_parser::ast::Expr>,
        commands: &'ast [turso_parser::ast::TriggerCmd],
        conflict_override: Option<turso_parser::ast::ResolveType>,
    },
}

pub(crate) fn analyze_root(
    catalog: &catalog::SemanticCatalog,
    symbols: &crate::SymbolTable,
    options: SemanticOptions,
    input: SemanticRootInput<'_>,
) -> crate::Result<hir::HirDocument> {
    let context = context::SemanticContext::for_catalog(
        catalog,
        symbols,
        options.custom_types_enabled,
        options.dialect,
        options.dqs_dml,
    )?;
    let input = match input {
        SemanticRootInput::Statement(statement) => AnalyzeInput::Statement(statement),
        SemanticRootInput::TriggerProgram {
            database,
            table_name,
            event,
            predicate,
            commands,
            conflict_override,
        } => {
            let normalized_name = crate::util::normalize_ident(table_name);
            let table = context
                .schema(database)?
                .get_table(&normalized_name)
                .ok_or_else(|| {
                    crate::LimboError::ParseError(format!("no such table: {normalized_name}"))
                })?;
            AnalyzeInput::TriggerProgram(TriggerProgramInput {
                context: TriggerAnalysis {
                    database,
                    table,
                    event,
                },
                predicate,
                commands,
                conflict_override,
            })
        }
    };
    analyze::analyze(&context, input)
}

pub(crate) enum AnalyzeInput<'ast> {
    Statement(&'ast turso_parser::ast::Stmt),
    TriggerPredicate {
        context: TriggerAnalysis,
        expression: &'ast turso_parser::ast::Expr,
    },
    TriggerSelect {
        context: TriggerAnalysis,
        select: &'ast turso_parser::ast::Select,
    },
    TriggerInsert {
        context: TriggerAnalysis,
        insert: TriggerInsert<'ast>,
    },
    TriggerUpdate {
        context: TriggerAnalysis,
        update: TriggerUpdate<'ast>,
    },
    TriggerDelete {
        context: TriggerAnalysis,
        delete: TriggerDelete<'ast>,
    },
    TriggerProgram(TriggerProgramInput<'ast>),
}

pub(crate) struct TriggerProgramInput<'ast> {
    pub(crate) context: TriggerAnalysis,
    pub(crate) predicate: Option<&'ast turso_parser::ast::Expr>,
    pub(crate) commands: &'ast [turso_parser::ast::TriggerCmd],
    pub(crate) conflict_override: Option<turso_parser::ast::ResolveType>,
}

pub(crate) struct TriggerInsert<'ast> {
    pub(crate) command_conflict: Option<turso_parser::ast::ResolveType>,
    pub(crate) conflict_override: Option<turso_parser::ast::ResolveType>,
    pub(crate) table: &'ast turso_parser::ast::Name,
    pub(crate) columns: &'ast [turso_parser::ast::Name],
    pub(crate) select: &'ast turso_parser::ast::Select,
    pub(crate) upsert: Option<&'ast turso_parser::ast::Upsert>,
    pub(crate) returning: &'ast [turso_parser::ast::ResultColumn],
}

pub(crate) struct TriggerUpdate<'ast> {
    pub(crate) command_conflict: Option<turso_parser::ast::ResolveType>,
    pub(crate) conflict_override: Option<turso_parser::ast::ResolveType>,
    pub(crate) table: &'ast turso_parser::ast::Name,
    pub(crate) assignments: &'ast [turso_parser::ast::Set],
    pub(crate) from: Option<&'ast turso_parser::ast::FromClause>,
    pub(crate) predicate: Option<&'ast turso_parser::ast::Expr>,
}

pub(crate) struct TriggerDelete<'ast> {
    pub(crate) table: &'ast turso_parser::ast::Name,
    pub(crate) predicate: Option<&'ast turso_parser::ast::Expr>,
}

pub(crate) struct TriggerAnalysis {
    pub(crate) database: hir::DatabaseId,
    pub(crate) table: crate::sync::Arc<crate::schema::Table>,
    pub(crate) event: turso_parser::ast::TriggerEvent,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        dialect::SqliteDialect,
        schema::{BTreeTable, Schema},
        sync::Arc,
        translate::semantic::context::DoubleQuotedDml,
        DatabaseCatalog, RwLock, SymbolTable, MAIN_DB_ID, TEMP_DB_ID,
    };
    use rustc_hash::FxHashMap as HashMap;
    use turso_parser::{ast, parser::Parser};

    #[test]
    fn owned_catalog_produces_validated_hir() {
        let catalog = {
            let mut main_schema = Schema::new();
            main_schema
                .add_btree_table(Arc::new(
                    BTreeTable::from_sql("CREATE TABLE items(value TEXT)", 2)
                        .expect("table SQL parses"),
                ))
                .expect("table name is unique");
            let main_schema = Arc::new(main_schema);
            let database_schemas = RwLock::new(HashMap::default());
            let temp_database = RwLock::new(None);
            let attached_databases = RwLock::new(DatabaseCatalog::new());
            catalog::SemanticCatalog::capture(
                main_schema,
                &database_schemas,
                &temp_database,
                &attached_databases,
                true,
                &SqliteDialect,
                None,
            )
            .expect("catalog capture succeeds")
        };
        let symbols = SymbolTable::new();
        let ast::Cmd::Stmt(statement) = Parser::new(b"SELECT value FROM items")
            .next_cmd()
            .expect("SQL parses")
            .expect("SQL contains statement")
        else {
            panic!("SQL contains ordinary statement");
        };

        let options = || SemanticOptions {
            dialect: Arc::new(SqliteDialect),
            custom_types_enabled: true,
            dqs_dml: DoubleQuotedDml::Disabled,
        };
        let document = analyze_root(
            &catalog,
            &symbols,
            options(),
            SemanticRootInput::Statement(&statement),
        )
        .expect("owned catalog produces HIR");
        assert_eq!(
            document
                .databases
                .iter()
                .map(|database| database.database.index())
                .collect::<Vec<_>>(),
            [MAIN_DB_ID, TEMP_DB_ID]
        );
        document.validate().expect("document validates again");

        let ast::Cmd::Stmt(ast::Stmt::CreateTrigger {
            event,
            when_clause,
            commands,
            ..
        }) = Parser::new(
            b"CREATE TRIGGER read_row AFTER UPDATE ON items \
              WHEN new.value <> old.value BEGIN SELECT new.value; END",
        )
        .next_cmd()
        .expect("trigger SQL parses")
        .expect("trigger SQL contains statement")
        else {
            panic!("SQL contains CREATE TRIGGER");
        };
        let document = analyze_root(
            &catalog,
            &symbols,
            options(),
            SemanticRootInput::TriggerProgram {
                database: hir::DatabaseId::new(MAIN_DB_ID),
                table_name: "items",
                event: event.clone(),
                predicate: when_clause.as_deref(),
                commands: &commands,
                conflict_override: None,
            },
        )
        .expect("catalog resolves trigger target and produces HIR");
        let hir::HirRoot::Trigger(root) = &document.root else {
            panic!("trigger program produces trigger root");
        };
        let hir::TriggerBody::Program(program) = &root.body else {
            panic!("trigger root contains whole program");
        };
        assert!(program.predicate.is_some());
        assert_eq!(program.commands.len(), 1);

        let error = analyze_root(
            &catalog,
            &symbols,
            options(),
            SemanticRootInput::TriggerProgram {
                database: hir::DatabaseId::new(MAIN_DB_ID),
                table_name: "missing",
                event,
                predicate: None,
                commands: &commands,
                conflict_override: None,
            },
        )
        .expect_err("trigger target must belong to catalog database");
        assert_eq!(error.to_string(), "Parse error: no such table: missing");
    }
}
