use pg_query::protobuf::node::Node;
use pg_query::protobuf::{CreateFunctionStmt, FunctionParameter, FunctionParameterMode, TypeName};
use pg_query::NodeRef;
use turso_core::dialect::FunctionCatalog;
use turso_core::schema::FunctionDef;
use turso_core::{LimboError, Result};
use turso_parser::ast;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Volatility {
    Immutable,
    Stable,
    Volatile,
}

impl Volatility {
    pub(crate) fn catalog_code(self) -> &'static str {
        match self {
            Self::Immutable => "i",
            Self::Stable => "s",
            Self::Volatile => "v",
        }
    }

    pub(crate) fn keyword(self) -> &'static str {
        match self {
            Self::Immutable => "IMMUTABLE",
            Self::Stable => "STABLE",
            Self::Volatile => "VOLATILE",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Language {
    Sql,
    Plpgsql,
}

impl Language {
    pub(crate) const SQL_OID: i64 = 14;
    pub(crate) const PLPGSQL_OID: i64 = 13612;

    pub(crate) fn oid(self) -> i64 {
        match self {
            Self::Sql => Self::SQL_OID,
            Self::Plpgsql => Self::PLPGSQL_OID,
        }
    }
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

    pub(crate) fn volatility(&self) -> Volatility {
        match self.option_text("volatility").as_deref() {
            Some("immutable") => Volatility::Immutable,
            Some("stable") => Volatility::Stable,
            _ => Volatility::Volatile,
        }
    }

    pub(crate) fn is_strict(&self) -> bool {
        self.option_boolean("strict")
    }

    pub(crate) fn is_security_definer(&self) -> bool {
        self.option_boolean("security")
    }

    pub(crate) fn returns_set(&self) -> bool {
        self.statement
            .return_type
            .as_ref()
            .is_some_and(|return_type| return_type.setof)
    }

    pub(crate) fn return_type(&self) -> Option<&TypeName> {
        self.statement.return_type.as_ref()
    }

    pub(crate) fn input_parameters(&self) -> impl Iterator<Item = &FunctionParameter> {
        self.statement
            .parameters
            .iter()
            .filter_map(|parameter| match &parameter.node {
                Some(Node::FunctionParameter(parameter)) => Some(parameter.as_ref()),
                _ => None,
            })
            .filter(|parameter| {
                !matches!(
                    parameter.mode(),
                    FunctionParameterMode::FuncParamOut | FunctionParameterMode::FuncParamTable
                )
            })
    }

    pub(crate) fn body_text(&self) -> Option<String> {
        let option = self.option("as")?;
        match option {
            Node::List(list) => list.items.first().and_then(|item| match &item.node {
                Some(Node::String(body)) => Some(body.sval.clone()),
                _ => None,
            }),
            Node::String(body) => Some(body.sval.clone()),
            _ => None,
        }
    }

    fn option(&self, name: &str) -> Option<&Node> {
        self.statement
            .options
            .iter()
            .find_map(|option| match &option.node {
                Some(Node::DefElem(def)) if def.defname == name => {
                    def.arg.as_ref().and_then(|arg| arg.node.as_ref())
                }
                _ => None,
            })
    }

    fn option_text(&self, name: &str) -> Option<String> {
        match self.option(name)? {
            Node::String(value) => Some(value.sval.to_ascii_lowercase()),
            _ => None,
        }
    }

    fn option_boolean(&self, name: &str) -> bool {
        matches!(self.option(name), Some(Node::Boolean(value)) if value.boolval)
    }
}

pub(crate) fn compile_function_call(
    function: &FunctionDef,
    args: &[Box<ast::Expr>],
    functions: &dyn FunctionCatalog,
) -> Result<ast::Expr> {
    let user_function = UserFunction::parse(function)?;
    if user_function.is_procedure() {
        return Err(LimboError::ParseError(format!(
            "{}() is a procedure",
            function.name
        )));
    }
    match user_function.language {
        Language::Sql => {
            crate::sql_function::inline_sql_function(function, &user_function, args, functions)
        }
        Language::Plpgsql => Err(LimboError::ParseError(format!(
            "cannot execute function {}(): PL/pgSQL functions can run only as triggers",
            function.name
        ))),
    }
}
