use pg_query::protobuf::CreateFunctionStmt;
use pg_query::NodeRef;
use turso_core::schema::FunctionDef;
use turso_core::{LimboError, Result};
use turso_parser::ast;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Language {
    Sql,
    Plpgsql,
}

pub(crate) struct UserFunction {
    pub(crate) statement: CreateFunctionStmt,
    pub(crate) language: Language,
}

impl UserFunction {
    pub(crate) fn parse(function: &FunctionDef) -> Result<Self> {
        let parsed = turso_pg_parser::parse(&function.definition)
            .map_err(|e| LimboError::Corrupt(format!("malformed function definition: {e}")))?;
        let node = parsed.protobuf.nodes().into_iter().next();
        let Some((NodeRef::CreateFunctionStmt(statement), ..)) = node else {
            return Err(LimboError::Corrupt(format!(
                "malformed function definition of {}",
                function.name
            )));
        };
        let language = match turso_pg_parser::function_language(statement)
            .map_err(|e| LimboError::Corrupt(e.to_string()))?
            .as_str()
        {
            "sql" => Language::Sql,
            "plpgsql" => Language::Plpgsql,
            language => {
                return Err(LimboError::Corrupt(format!(
                    "function {} has unsupported language {language}",
                    function.name
                )))
            }
        };
        Ok(Self {
            statement: statement.clone(),
            language,
        })
    }

    pub(crate) fn is_procedure(&self) -> bool {
        self.statement.is_procedure
    }
}

pub(crate) fn compile_function_call(
    function: &FunctionDef,
    _args: &[Box<ast::Expr>],
) -> Result<ast::Expr> {
    let user_function = UserFunction::parse(function)?;
    if user_function.is_procedure() {
        return Err(LimboError::ParseError(format!(
            "{}() is a procedure",
            function.name
        )));
    }
    let reason = match user_function.language {
        Language::Sql => "SQL functions are not supported yet",
        Language::Plpgsql => "PL/pgSQL functions can run only as triggers",
    };
    Err(LimboError::ParseError(format!(
        "cannot execute function {}(): {reason}",
        function.name
    )))
}
