use crate::coerce::{common_type, PgType};
use crate::sqlstate::SQLSTATE_FEATURE_NOT_SUPPORTED;
use crate::translator::{
    alias_column_names, derived_column_name, extract_key_columns, last_string,
    non_decimal_literal_as_decimal,
};
use crate::ParseError;
use pg_query::protobuf::a_const::Val;
use pg_query::protobuf::node::Node as NodeKind;
use pg_query::protobuf::{
    AExpr, AExprKind, Alias, CommonTableExpr, CreateStmt, DeleteStmt, FuncCall, IndexStmt,
    InsertStmt, MinMaxOp, Node, RangeFunction, RangeVar, SelectStmt, SetOperation,
    SqlValueFunctionOp, SubLinkType, TypeName, UpdateStmt, WithClause,
};
use std::rc::Rc;

/// Gives the columns (name, type) of the table or view `name` in `schema` (None: found through the search path), or None when it is not known.
pub type ColumnLookup = Box<dyn Fn(Option<&str>, &str) -> Option<Vec<(String, PgType)>>>;

/// The type of an expression, or why it is not known.
pub type ExprType = Result<PgType, Untyped>;

/// The names and types of the columns that a SELECT returns.
pub type OutputColumns = Vec<(Option<String>, ExprType)>;

/// The names and types of the columns of a table.
pub type TargetColumns = Vec<(String, Option<PgType>)>;

/// Why the type of an expression is not known: a name in it does not resolve (the engine reports that), or typing does not cover it (the text says what in plain words).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Untyped {
    NameNotFound,
    Cause(String),
}

/// The stack of name scopes of the statement being translated, innermost last.
#[derive(Clone, Default)]
pub struct Scopes {
    frames: Vec<Rc<Frame>>,
    lookup: Option<Rc<ColumnLookup>>,
}

#[derive(Clone, Default)]
struct Frame {
    ctes: Vec<Cte>,
    relations: Vec<Relation>,
}

#[derive(Clone)]
struct Cte {
    name: String,
    columns: Columns,
}

#[derive(Clone)]
struct Relation {
    name: String,
    schema: Option<String>,
    columns: Columns,
}

#[derive(Clone)]
enum Columns {
    Known(Vec<Column>),
    NotFound,
    Hidden(String),
}

#[derive(Clone)]
struct Column {
    name: String,
    ty: ExprType,
}

impl Columns {
    fn renamed(self, names: &[String]) -> Columns {
        match self {
            Columns::Known(columns) => Columns::Known(rename_columns(columns, names)),
            other => other,
        }
    }
}

impl Scopes {
    /// Starts with no scopes; `lookup` finds the columns of tables that are not defined in the statement.
    pub fn new(lookup: Option<ColumnLookup>) -> Self {
        Scopes {
            frames: Vec::new(),
            lookup: lookup.map(Rc::new),
        }
    }

    /// Adds the scope of a SELECT: its WITH items and FROM items.
    pub fn enter_select(&mut self, select: &SelectStmt) {
        let frame = select_frame(select, self);
        self.frames.push(Rc::new(frame));
    }

    /// Adds the scope of an INSERT: the target table, and `excluded` when there is an ON CONFLICT clause.
    pub fn enter_insert(&mut self, insert: &InsertStmt) {
        let mut leading = Vec::new();
        if let Some(target) = &insert.relation {
            let relation = target_relation(target, self);
            if insert.on_conflict_clause.is_some() {
                leading.push(Relation {
                    name: "excluded".to_string(),
                    ..relation.clone()
                });
            }
            leading.push(relation);
        }
        let frame = statement_frame(insert.with_clause.as_ref(), leading, &[], self);
        self.frames.push(Rc::new(frame));
    }

    /// Adds the scope of an UPDATE: the target table and the FROM items.
    pub fn enter_update(&mut self, update: &UpdateStmt) {
        let leading = update
            .relation
            .iter()
            .map(|target| target_relation(target, self))
            .collect();
        let frame = statement_frame(
            update.with_clause.as_ref(),
            leading,
            &update.from_clause,
            self,
        );
        self.frames.push(Rc::new(frame));
    }

    /// Adds the scope of a DELETE: the target table and the USING items.
    pub fn enter_delete(&mut self, delete: &DeleteStmt) {
        let leading = delete
            .relation
            .iter()
            .map(|target| target_relation(target, self))
            .collect();
        let frame = statement_frame(
            delete.with_clause.as_ref(),
            leading,
            &delete.using_clause,
            self,
        );
        self.frames.push(Rc::new(frame));
    }

    /// Adds the scope of a CREATE TABLE: the new table with the columns the statement defines.
    pub fn enter_create_table(&mut self, create: &CreateStmt) {
        let Some(table) = &create.relation else {
            self.frames.push(Rc::new(Frame::default()));
            return;
        };
        let columns = create
            .table_elts
            .iter()
            .filter_map(|element| match &element.node {
                Some(NodeKind::ColumnDef(definition)) => Some(Column {
                    name: definition.colname.clone(),
                    ty: definition
                        .type_name
                        .as_ref()
                        .and_then(|type_name| declared_type(type_name, true))
                        .ok_or_else(|| cause("a column with no declared type")),
                }),
                _ => None,
            })
            .collect();
        let relation = Relation {
            name: table.relname.clone(),
            schema: non_empty(&table.schemaname),
            columns: Columns::Known(columns),
        };
        self.frames.push(Rc::new(Frame {
            ctes: Vec::new(),
            relations: vec![relation],
        }));
    }

    /// Adds the scope of a CREATE INDEX: the indexed table.
    pub fn enter_create_index(&mut self, index: &IndexStmt) {
        let relations = index
            .relation
            .iter()
            .map(|target| target_relation(target, self))
            .collect();
        self.frames.push(Rc::new(Frame {
            ctes: Vec::new(),
            relations,
        }));
    }

    /// The columns (name, type) of the table `table`, in order, or None when they are not known.
    pub fn target_columns(&self, table: &RangeVar) -> Option<TargetColumns> {
        let Columns::Known(columns) =
            self.table_columns(non_empty(&table.schemaname).as_deref(), &table.relname)
        else {
            return None;
        };
        Some(columns.into_iter().map(|c| (c.name, c.ty.ok())).collect())
    }

    /// Removes the innermost scope.
    pub fn leave(&mut self) {
        self.frames.pop();
    }

    fn with_frame(&self, frame: Frame) -> Scopes {
        let mut scopes = self.clone();
        scopes.frames.push(Rc::new(frame));
        scopes
    }

    fn table_columns(&self, schema: Option<&str>, name: &str) -> Columns {
        let Some(columns) = self.lookup.as_ref().and_then(|lookup| lookup(schema, name)) else {
            return Columns::NotFound;
        };
        Columns::Known(
            columns
                .into_iter()
                .map(|(name, ty)| Column { name, ty: Ok(ty) })
                .collect(),
        )
    }

    fn visible_cte(&self, name: &str) -> Option<&Cte> {
        self.frames
            .iter()
            .rev()
            .find_map(|frame| frame.ctes.iter().rev().find(|cte| cte.name == name))
    }

    fn bare_column_type(&self, column: &str) -> ExprType {
        for frame in self.frames.iter().rev() {
            let mut found: Vec<&ExprType> = Vec::new();
            let mut has_missing_relation = false;
            let mut hidden: Option<&String> = None;
            for relation in &frame.relations {
                match &relation.columns {
                    Columns::Known(columns) => {
                        found.extend(columns.iter().filter(|c| c.name == column).map(|c| &c.ty))
                    }
                    Columns::NotFound => has_missing_relation = true,
                    Columns::Hidden(description) => hidden = hidden.or(Some(description)),
                }
            }
            if let Some((first, rest)) = found.split_first() {
                return if rest.iter().all(|ty| ty == first) {
                    (*first).clone()
                } else {
                    Err(Untyped::NameNotFound)
                };
            }
            if has_missing_relation {
                return Err(Untyped::NameNotFound);
            }
            if let Some(description) = hidden {
                return Err(cause(description));
            }
        }
        Err(Untyped::NameNotFound)
    }

    fn qualified_column_type(&self, schema: Option<&str>, table: &str, column: &str) -> ExprType {
        let relation = self.frames.iter().rev().find_map(|frame| {
            frame.relations.iter().find(|relation| {
                relation.name == table
                    && match (schema, &relation.schema) {
                        (Some(wanted), Some(actual)) => wanted == actual,
                        _ => true,
                    }
            })
        });
        match relation.map(|relation| &relation.columns) {
            Some(Columns::Known(columns)) => columns
                .iter()
                .find(|c| c.name == column)
                .map_or(Err(Untyped::NameNotFound), |c| c.ty.clone()),
            Some(Columns::Hidden(description)) => Err(cause(description)),
            Some(Columns::NotFound) | None => Err(Untyped::NameNotFound),
        }
    }
}

/// The PostgreSQL type of `node`, or why it is not known.
// FIXME: the engine works out expression types again for the translated statement, in
// core/translate/expr/expr_type.rs. Keep this one small until there is only one.
pub fn type_of(node: &Node, scopes: &Scopes) -> ExprType {
    let Some(kind) = node.node.as_ref() else {
        return Err(cause("an empty expression"));
    };
    match kind {
        NodeKind::AConst(constant) => constant_type(constant),
        NodeKind::ParamRef(_) => Ok(PgType::Unknown),
        NodeKind::TypeCast(cast) => cast
            .type_name
            .as_ref()
            .and_then(|type_name| declared_type(type_name, false))
            .ok_or_else(|| cause("a cast to a type with no name")),
        NodeKind::ColumnRef(column) => column_type(&column.fields, scopes),
        NodeKind::AExpr(expression) => operator_type(expression, scopes),
        NodeKind::BoolExpr(_) | NodeKind::NullTest(_) | NodeKind::BooleanTest(_) => {
            Ok(PgType::Bool)
        }
        NodeKind::SubLink(link) => match SubLinkType::try_from(link.sub_link_type) {
            Ok(SubLinkType::ExistsSublink | SubLinkType::AnySublink | SubLinkType::AllSublink) => {
                Ok(PgType::Bool)
            }
            Ok(SubLinkType::ExprSublink) => match link.subselect.as_deref() {
                Some(subselect) => subquery_column_type(subselect, scopes),
                None => Err(cause("a subquery with no query")),
            },
            Ok(SubLinkType::ArraySublink) => Err(cause("an ARRAY subquery")),
            _ => Err(cause("this kind of subquery")),
        },
        NodeKind::CaseExpr(case) => {
            let mut results = vec![match case.defresult.as_deref() {
                Some(default) => type_of(default, scopes),
                None => Ok(PgType::Unknown),
            }];
            results.extend(case.args.iter().map(|arm| match &arm.node {
                Some(NodeKind::CaseWhen(when)) => match when.result.as_deref() {
                    Some(result) => type_of(result, scopes),
                    None => Err(cause("a CASE branch with no result")),
                },
                _ => Err(cause("a CASE branch with no result")),
            }));
            matching_type(results, "CASE")
        }
        NodeKind::CoalesceExpr(coalesce) => {
            matching_type(types_of(&coalesce.args, scopes), "COALESCE")
        }
        NodeKind::MinMaxExpr(min_max) => matching_type(
            types_of(&min_max.args, scopes),
            min_max_construct(min_max.op),
        ),
        NodeKind::FuncCall(call) => function_call_type(call, scopes),
        NodeKind::SqlvalueFunction(function) => sql_value_function_type(function.op),
        NodeKind::AArrayExpr(array) => {
            not_covered("an ARRAY constructor", &types_of(&array.elements, scopes))
        }
        NodeKind::RowExpr(row) => not_covered("a row value", &types_of(&row.args, scopes)),
        NodeKind::AIndirection(indirection) => {
            let operand = indirection.arg.as_deref().map(|arg| type_of(arg, scopes));
            if is_one_subscript(&indirection.indirection) {
                match &operand {
                    Some(Ok(PgType::Text)) => return Ok(PgType::Char),
                    Some(Ok(PgType::Array(element))) => return Ok((**element).clone()),
                    _ => {}
                }
            }
            not_covered(
                "a subscript or field selection",
                &operand.into_iter().collect::<Vec<_>>(),
            )
        }
        NodeKind::CollateClause(_) => Err(cause("a COLLATE clause")),
        NodeKind::GroupingFunc(_) => Err(cause("a GROUPING call")),
        NodeKind::XmlExpr(_) => Err(cause("an XML expression")),
        NodeKind::SetToDefault(_) => Err(cause("DEFAULT")),
        _ => Err(cause("this kind of expression")),
    }
}

/// The error for a value whose type is needed but is not known, or None when the engine reports the name that does not resolve.
pub fn require_type(typed: ExprType) -> Result<Option<PgType>, ParseError> {
    match typed {
        Ok(ty) => Ok(Some(ty)),
        Err(Untyped::NameNotFound) => Ok(None),
        Err(Untyped::Cause(description)) => Err(type_not_known(&description)),
    }
}

/// Like `require_type` for the values of one construct: a name that does not resolve anywhere among them takes precedence.
pub fn require_types(typed: Vec<ExprType>) -> Result<Vec<Option<PgType>>, ParseError> {
    if typed
        .iter()
        .any(|t| matches!(t, Err(Untyped::NameNotFound)))
    {
        return Ok(typed.into_iter().map(|t| t.ok()).collect());
    }
    typed.into_iter().map(require_type).collect()
}

/// The names and types of the columns `select` returns, or why they are not known.
pub fn select_output_columns(
    select: &SelectStmt,
    scopes: &Scopes,
) -> Result<OutputColumns, Untyped> {
    match SetOperation::try_from(select.op) {
        Ok(SetOperation::SetopUnion | SetOperation::SetopIntersect | SetOperation::SetopExcept) => {
            set_operation_columns(select, scopes)
        }
        _ => {
            let inner = scopes.with_frame(select_frame(select, scopes));
            if select.values_lists.is_empty() {
                target_list_columns(select, &inner)
            } else {
                values_columns(select, &inner)
            }
        }
    }
}

/// The name PostgreSQL gives GREATEST or LEAST in error messages.
pub(crate) fn min_max_construct(operation: i32) -> &'static str {
    match MinMaxOp::try_from(operation) {
        Ok(MinMaxOp::IsLeast) => "LEAST",
        _ => "GREATEST",
    }
}

/// Checks that each column of a UNION, INTERSECT or EXCEPT has one type all its operands convert to, or gives the error PostgreSQL gives.
pub fn check_set_operation_columns(select: &SelectStmt, scopes: &Scopes) -> Result<(), ParseError> {
    let construct = match SetOperation::try_from(select.op) {
        Ok(SetOperation::SetopUnion) => "UNION",
        Ok(SetOperation::SetopIntersect) => "INTERSECT",
        Ok(SetOperation::SetopExcept) => "EXCEPT",
        _ => return Ok(()),
    };
    let (Some(left), Some(right)) = (select.larg.as_deref(), select.rarg.as_deref()) else {
        return Ok(());
    };
    let inner = scopes.with_frame(Frame {
        ctes: with_items(select.with_clause.as_ref(), scopes),
        relations: Vec::new(),
    });
    check_set_operation_columns(left, &inner)?;
    check_set_operation_columns(right, &inner)?;
    let (Ok(left), Ok(right)) = (
        select_output_columns(left, &inner),
        select_output_columns(right, &inner),
    ) else {
        return Ok(());
    };
    if left.len() != right.len() {
        return Ok(());
    }
    for ((_, left_type), (_, right_type)) in left.into_iter().zip(right) {
        if let (Ok(left_type), Ok(right_type)) = (left_type, right_type) {
            common_type(&[left_type, right_type], construct)?;
        }
    }
    Ok(())
}

/// Checks that each column of a VALUES with several rows has one type all its rows convert to, or gives the error PostgreSQL gives.
pub fn check_values_columns(select: &SelectStmt, scopes: &Scopes) -> Result<(), ParseError> {
    let inner = scopes.with_frame(select_frame(select, scopes));
    let Ok(columns) = values_column_types(select, &inner) else {
        return Ok(());
    };
    for column in columns {
        if let Ok(types) = column.into_iter().collect::<Result<Vec<PgType>, Untyped>>() {
            common_type(&types, "VALUES")?;
        }
    }
    Ok(())
}

fn is_one_subscript(steps: &[Node]) -> bool {
    matches!(
        steps,
        [Node { node: Some(NodeKind::AIndices(indices)) }] if !indices.is_slice
    )
}

fn type_not_known(description: &str) -> ParseError {
    ParseError::Sqlstate {
        sqlstate: SQLSTATE_FEATURE_NOT_SUPPORTED,
        message: format!("the type of {description} is not known"),
    }
}

fn cause(description: impl Into<String>) -> Untyped {
    Untyped::Cause(description.into())
}

fn types_of(nodes: &[Node], scopes: &Scopes) -> Vec<ExprType> {
    nodes.iter().map(|node| type_of(node, scopes)).collect()
}

fn not_covered(description: &str, parts: &[ExprType]) -> ExprType {
    Err(prefer_name_error(cause(description), parts))
}

fn prefer_name_error(failure: Untyped, parts: &[ExprType]) -> Untyped {
    if parts
        .iter()
        .any(|part| matches!(part, Err(Untyped::NameNotFound)))
    {
        Untyped::NameNotFound
    } else {
        failure
    }
}

fn all_types(types: Vec<ExprType>) -> Result<Vec<PgType>, Untyped> {
    let name_not_found = types
        .iter()
        .any(|ty| matches!(ty, Err(Untyped::NameNotFound)));
    let mut known = Vec::new();
    for ty in types {
        match ty {
            Ok(ty) => known.push(ty),
            Err(_) if name_not_found => return Err(Untyped::NameNotFound),
            Err(failure) => return Err(failure),
        }
    }
    Ok(known)
}

fn matching_type(types: Vec<ExprType>, construct: &str) -> ExprType {
    let known = all_types(types)?;
    common_type(&known, construct)
        .map_err(|_| cause(format!("a {construct} whose types do not match")))
}

fn constant_type(constant: &pg_query::protobuf::AConst) -> ExprType {
    if constant.isnull {
        return Ok(PgType::Unknown);
    }
    match constant.val.as_ref() {
        None => Err(cause("a constant with no value")),
        Some(Val::Ival(_)) => Ok(PgType::Int4),
        Some(Val::Fval(float)) => {
            let text = non_decimal_literal_as_decimal(&float.fval)
                .unwrap_or_else(|| float.fval.replace('_', ""));
            let digits = text.strip_prefix('-').unwrap_or(&text);
            let is_integer = !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit());
            if is_integer && text.parse::<i64>().is_ok() {
                Ok(PgType::Int8)
            } else {
                Ok(PgType::Numeric)
            }
        }
        Some(Val::Sval(_)) => Ok(PgType::Unknown),
        Some(Val::Boolval(_)) => Ok(PgType::Bool),
        Some(Val::Bsval(_)) => Err(cause("a bit string constant")),
    }
}

pub(crate) fn declared_type(type_name: &TypeName, with_serial_types: bool) -> Option<PgType> {
    let name = last_string(&type_name.names)?.to_lowercase();
    if type_name.typmods.is_empty() && type_name.array_bounds.is_empty() {
        if with_serial_types {
            match name.as_str() {
                "serial" | "serial4" => return Some(PgType::Int4),
                "bigserial" | "serial8" => return Some(PgType::Int8),
                "smallserial" | "serial2" => return Some(PgType::from_name("smallint")),
                _ => {}
            }
        }
        return Some(PgType::from_name(&name));
    }
    let modifiers: Vec<String> = type_name.typmods.iter().filter_map(modifier_text).collect();
    let mut readable = name;
    if !type_name.typmods.is_empty() {
        readable.push_str(&format!("({})", modifiers.join(",")));
    }
    for _ in &type_name.array_bounds {
        readable.push_str("[]");
    }
    Some(PgType::from_name(&readable))
}

fn modifier_text(modifier: &Node) -> Option<String> {
    match modifier.node.as_ref()? {
        NodeKind::AConst(constant) => match constant.val.as_ref()? {
            Val::Ival(integer) => Some(integer.ival.to_string()),
            Val::Sval(string) => Some(string.sval.clone()),
            _ => None,
        },
        NodeKind::Integer(integer) => Some(integer.ival.to_string()),
        _ => None,
    }
}

fn column_type(fields: &[Node], scopes: &Scopes) -> ExprType {
    let Ok(names) = extract_key_columns(fields) else {
        return Err(cause("a row value"));
    };
    match names.as_slice() {
        [column] => scopes.bare_column_type(column),
        [table, column] => scopes.qualified_column_type(None, table, column),
        [schema, table, column] => scopes.qualified_column_type(Some(schema), table, column),
        _ => Err(cause("a column of another database")),
    }
}

fn operator_type(expression: &AExpr, scopes: &Scopes) -> ExprType {
    let Ok(kind) = AExprKind::try_from(expression.kind) else {
        return Err(cause("an operator"));
    };
    match kind {
        AExprKind::AexprOp => {
            let Some(operator) = last_string(&expression.name) else {
                return Err(cause("an operator"));
            };
            let operands: Vec<ExprType> = expression
                .lexpr
                .iter()
                .chain(expression.rexpr.iter())
                .map(|operand| type_of(operand, scopes))
                .collect();
            let result = match operands.as_slice() {
                [operand] => unary_operator_type(&operator, operand),
                [left, right] => binary_operator_type(&operator, left, right),
                _ => Err(cause(format!("the operator {operator}"))),
            };
            result.map_err(|failure| prefer_name_error(failure, &operands))
        }
        AExprKind::AexprNullif => match expression.lexpr.as_deref() {
            Some(first) => type_of(first, scopes),
            None => Err(cause("NULLIF with no arguments")),
        },
        AExprKind::AexprOpAny
        | AExprKind::AexprOpAll
        | AExprKind::AexprDistinct
        | AExprKind::AexprNotDistinct
        | AExprKind::AexprIn
        | AExprKind::AexprLike
        | AExprKind::AexprIlike
        | AExprKind::AexprSimilar
        | AExprKind::AexprBetween
        | AExprKind::AexprNotBetween
        | AExprKind::AexprBetweenSym
        | AExprKind::AexprNotBetweenSym => Ok(PgType::Bool),
        AExprKind::Undefined => Err(cause("an operator")),
    }
}

fn unary_operator_type(operator: &str, operand: &ExprType) -> ExprType {
    match operator {
        "-" | "+" => match operand {
            Ok(ty) if number_rank(ty).is_some() => Ok(ty.clone()),
            Ok(_) => Err(cause(format!("the operator {operator}"))),
            Err(failure) => Err(failure.clone()),
        },
        _ => Err(cause(format!("the operator {operator}"))),
    }
}

fn binary_operator_type(operator: &str, left: &ExprType, right: &ExprType) -> ExprType {
    match operator {
        "=" | "<>" | "!=" | "<" | "<=" | ">" | ">=" | "~" | "~*" | "!~" | "!~*" | "~~" | "~~*"
        | "!~~" | "!~~*" => Ok(PgType::Bool),
        "+" | "-" | "*" | "/" | "%" => {
            let (left, right) = (left.clone()?, right.clone()?);
            arithmetic_type(left, right).ok_or_else(|| cause(format!("the operator {operator}")))
        }
        "||" => {
            let (left, right) = (left.clone()?, right.clone()?);
            let is_one_of =
                |found: &PgType, wanted: &PgType| found == wanted || *found == PgType::Unknown;
            let is_text = |found: &PgType| matches!(found, PgType::Text | PgType::Unknown);
            let is_plain_value = |found: &PgType| {
                matches!(
                    found,
                    PgType::Text
                        | PgType::Unknown
                        | PgType::Int2
                        | PgType::Int4
                        | PgType::Int8
                        | PgType::Numeric
                        | PgType::Float8
                        | PgType::Oid
                        | PgType::Bool
                )
            };
            if is_text(&left) && is_text(&right) {
                Ok(PgType::Text)
            } else if is_one_of(&left, &PgType::Bytea) && is_one_of(&right, &PgType::Bytea) {
                Ok(PgType::Bytea)
            } else if (is_text(&left) && is_plain_value(&right))
                || (is_plain_value(&left) && is_text(&right))
            {
                Ok(PgType::Text)
            } else {
                Err(cause(format!("the operator {operator}")))
            }
        }
        _ => Err(cause(format!("the operator {operator}"))),
    }
}

fn arithmetic_type(left: PgType, right: PgType) -> Option<PgType> {
    match (&left, &right) {
        (PgType::Unknown, PgType::Unknown) => None,
        (PgType::Unknown, other) | (other, PgType::Unknown) => {
            number_rank(other).map(|_| other.clone())
        }
        _ => {
            let (left_rank, right_rank) = (number_rank(&left)?, number_rank(&right)?);
            Some(if left_rank >= right_rank { left } else { right })
        }
    }
}

fn number_rank(ty: &PgType) -> Option<u8> {
    match ty {
        PgType::Int2 => Some(0),
        PgType::Int4 => Some(1),
        PgType::Int8 => Some(2),
        PgType::Numeric => Some(3),
        PgType::Float8 => Some(4),
        _ => None,
    }
}

/// The type of the only column that the subquery `subquery` returns, or why it is not known.
pub fn subquery_column_type(subquery: &Node, scopes: &Scopes) -> ExprType {
    let Some(NodeKind::SelectStmt(select)) = &subquery.node else {
        return Err(cause("a subquery that is not a SELECT"));
    };
    let columns = select_output_columns(select, scopes)?;
    match columns.as_slice() {
        [(_, ty)] => ty.clone(),
        _ => Err(cause("a subquery that does not return one column")),
    }
}

fn function_call_type(call: &FuncCall, scopes: &Scopes) -> ExprType {
    let Ok(names) = extract_key_columns(&call.funcname) else {
        return Err(cause("a function with a name that is not plain"));
    };
    let name = match names.as_slice() {
        [name] => name,
        [schema, name] if schema == "pg_catalog" => name,
        _ => return Err(cause(format!("the function {}", names.join(".")))),
    };
    let arguments = if call.agg_star {
        Vec::new()
    } else {
        types_of(&call.args, scopes)
    };
    function_result_type(&name.to_lowercase(), &arguments)
        .map_err(|failure| prefer_name_error(failure, &arguments))
}

fn function_result_type(name: &str, arguments: &[ExprType]) -> ExprType {
    match name {
        "count" => Ok(PgType::Int8),
        "sum" => match first_argument(name, arguments)? {
            PgType::Int4 => Ok(PgType::Int8),
            PgType::Int8 | PgType::Numeric => Ok(PgType::Numeric),
            PgType::Float8 => Ok(PgType::Float8),
            other => Err(function_on(name, &other)),
        },
        "avg" => match first_argument(name, arguments)? {
            PgType::Int4 | PgType::Int8 | PgType::Numeric => Ok(PgType::Numeric),
            PgType::Float8 => Ok(PgType::Float8),
            other => Err(function_on(name, &other)),
        },
        "min" | "max" if arguments.len() == 1 => match first_argument(name, arguments)? {
            PgType::Unknown => Err(function_on(name, &PgType::Unknown)),
            other => Ok(other),
        },
        "abs" => match first_argument(name, arguments)? {
            PgType::Unknown => Err(function_on(name, &PgType::Unknown)),
            other => Ok(other),
        },
        "length" | "char_length" | "character_length" | "octet_length" | "strpos" | "position"
        | "ascii" => Ok(PgType::Int4),
        "lower" | "upper" | "trim" | "btrim" | "ltrim" | "rtrim" | "substr" | "substring"
        | "replace" | "concat" | "concat_ws" | "left" | "right" | "repeat" | "reverse" | "md5"
        | "lpad" | "rpad" | "initcap" | "string_agg" | "version" | "format" => Ok(PgType::Text),
        "round" | "ceil" | "ceiling" | "floor" | "trunc" => match arguments.len() {
            1 => match first_argument(name, arguments)? {
                PgType::Float8 => Ok(PgType::Float8),
                PgType::Int4 | PgType::Int8 | PgType::Numeric => Ok(PgType::Numeric),
                other => Err(function_on(name, &other)),
            },
            2 if name == "round" || name == "trunc" => Ok(PgType::Numeric),
            _ => Err(cause(format!("the function {name}"))),
        },
        "random" | "sqrt" | "exp" | "ln" | "pi" => Ok(PgType::Float8),
        "nextval" | "currval" | "lastval" | "setval" | "row_number" | "rank" | "dense_rank" => {
            Ok(PgType::Int8)
        }
        "bool_and" | "bool_or" | "every" | "booleq" | "boolne" | "pg_type_is_visible"
        | "pg_input_is_valid" => Ok(PgType::Bool),
        "current_setting"
        | "set_config"
        | "current_schema"
        | "pg_get_userbyid"
        | "pg_get_expr"
        | "pg_get_constraintdef"
        | "pg_get_indexdef"
        | "chr" => Ok(PgType::Text),
        "pg_backend_pid" | "array_length" | "int4" => Ok(PgType::Int4),
        "int8" => Ok(PgType::Int8),
        "float8" => Ok(PgType::Float8),
        "now" | "transaction_timestamp" | "statement_timestamp" | "clock_timestamp" => {
            Ok(PgType::from_name("timestamp with time zone"))
        }
        _ => Err(cause(format!("the function {name}"))),
    }
}

fn first_argument(name: &str, arguments: &[ExprType]) -> ExprType {
    arguments
        .first()
        .cloned()
        .unwrap_or_else(|| Err(cause(format!("the function {name}"))))
}

fn function_on(name: &str, ty: &PgType) -> Untyped {
    cause(format!("the function {name} on a value of type {ty}"))
}

fn sql_value_function_type(operation: i32) -> ExprType {
    let Ok(operation) = SqlValueFunctionOp::try_from(operation) else {
        return Err(cause("a SQL value function"));
    };
    match operation {
        SqlValueFunctionOp::SvfopCurrentDate => Ok(PgType::from_name("date")),
        SqlValueFunctionOp::SvfopCurrentTimestamp => {
            Ok(PgType::from_name("timestamp with time zone"))
        }
        SqlValueFunctionOp::SvfopLocaltimestamp => {
            Ok(PgType::from_name("timestamp without time zone"))
        }
        SqlValueFunctionOp::SvfopCurrentUser
        | SqlValueFunctionOp::SvfopSessionUser
        | SqlValueFunctionOp::SvfopUser
        | SqlValueFunctionOp::SvfopCurrentRole
        | SqlValueFunctionOp::SvfopCurrentSchema
        | SqlValueFunctionOp::SvfopCurrentCatalog => Ok(PgType::Text),
        SqlValueFunctionOp::SvfopCurrentTime | SqlValueFunctionOp::SvfopCurrentTimeN => {
            Err(cause("CURRENT_TIME"))
        }
        SqlValueFunctionOp::SvfopCurrentTimestampN => {
            Err(cause("CURRENT_TIMESTAMP with a precision"))
        }
        SqlValueFunctionOp::SvfopLocaltime | SqlValueFunctionOp::SvfopLocaltimeN => {
            Err(cause("LOCALTIME"))
        }
        SqlValueFunctionOp::SvfopLocaltimestampN => Err(cause("LOCALTIMESTAMP with a precision")),
        _ => Err(cause("a SQL value function")),
    }
}

fn select_frame(select: &SelectStmt, outer: &Scopes) -> Frame {
    statement_frame(
        select.with_clause.as_ref(),
        Vec::new(),
        &select.from_clause,
        outer,
    )
}

fn statement_frame(
    with_clause: Option<&WithClause>,
    mut relations: Vec<Relation>,
    from_items: &[Node],
    outer: &Scopes,
) -> Frame {
    let ctes = with_items(with_clause, outer);
    let base = outer.with_frame(Frame {
        ctes: ctes.clone(),
        relations: Vec::new(),
    });
    for item in from_items {
        add_from_item(item, &base, &mut relations);
    }
    Frame { ctes, relations }
}

fn with_items(with_clause: Option<&WithClause>, outer: &Scopes) -> Vec<Cte> {
    let mut ctes: Vec<Cte> = Vec::new();
    let Some(with_clause) = with_clause else {
        return ctes;
    };
    for item in &with_clause.ctes {
        let Some(NodeKind::CommonTableExpr(cte)) = &item.node else {
            continue;
        };
        let visible = outer.with_frame(Frame {
            ctes: ctes.clone(),
            relations: Vec::new(),
        });
        ctes.push(Cte {
            name: cte.ctename.clone(),
            columns: cte_columns(cte, with_clause.recursive, &visible),
        });
    }
    ctes
}

fn cte_columns(cte: &CommonTableExpr, recursive: bool, visible: &Scopes) -> Columns {
    let hidden = |kind: &str| Columns::Hidden(format!("a column of the {kind}{}", cte.ctename));
    let Some(query) = cte.ctequery.as_deref() else {
        return hidden("WITH query ");
    };
    let Some(NodeKind::SelectStmt(select)) = &query.node else {
        return hidden("WITH query ");
    };
    let output = if recursive && refers_to_table(query, &cte.ctename) {
        match non_recursive_term(select, &cte.ctename) {
            Some(term) => select_output_columns(term, visible),
            None => return hidden("recursive WITH query "),
        }
    } else {
        select_output_columns(select, visible)
    };
    match output {
        Ok(output) => {
            Columns::Known(output_as_columns(output)).renamed(&names_of(&cte.aliascolnames))
        }
        Err(Untyped::NameNotFound) => Columns::NotFound,
        Err(Untyped::Cause(_)) => hidden("WITH query "),
    }
}

fn non_recursive_term<'a>(select: &'a SelectStmt, name: &str) -> Option<&'a SelectStmt> {
    if !matches!(
        SetOperation::try_from(select.op),
        Ok(SetOperation::SetopUnion)
    ) {
        return None;
    }
    let term = select.larg.as_deref()?;
    let as_node = Node {
        node: Some(NodeKind::SelectStmt(Box::new(term.clone()))),
    };
    (!refers_to_table(&as_node, name)).then_some(term)
}

fn refers_to_table(query: &Node, name: &str) -> bool {
    let Some(kind) = &query.node else {
        return false;
    };
    kind.nodes().iter().any(|(node, _, _, _)| {
        matches!(node, pg_query::NodeRef::RangeVar(table) if table.relname == name)
    })
}

fn add_from_item(item: &Node, base: &Scopes, relations: &mut Vec<Relation>) {
    match &item.node {
        Some(NodeKind::RangeVar(table)) => relations.push(from_table_relation(table, base)),
        Some(NodeKind::RangeSubselect(subselect)) => {
            let visible = if subselect.lateral {
                base.with_frame(Frame {
                    ctes: Vec::new(),
                    relations: relations.clone(),
                })
            } else {
                base.clone()
            };
            let output = match subselect.subquery.as_deref().and_then(|n| n.node.as_ref()) {
                Some(NodeKind::SelectStmt(select)) => select_output_columns(select, &visible),
                _ => Err(cause("a subquery that is not a SELECT")),
            };
            let columns = match output {
                Ok(output) => Columns::Known(output_as_columns(output)),
                Err(Untyped::NameNotFound) => Columns::NotFound,
                Err(Untyped::Cause(_)) => {
                    Columns::Hidden(columns_of("the subquery", subselect.alias.as_ref()))
                }
            };
            relations.push(aliased_relation(subselect.alias.as_ref(), columns));
        }
        Some(NodeKind::JoinExpr(join)) => match &join.alias {
            Some(alias) => relations.push(aliased_relation(
                Some(alias),
                Columns::Hidden(columns_of("the join", Some(alias))),
            )),
            None => {
                for side in [&join.larg, &join.rarg].into_iter().flatten() {
                    add_from_item(side, base, relations);
                }
            }
        },
        Some(NodeKind::RangeFunction(function)) => {
            let visible = base.with_frame(Frame {
                ctes: Vec::new(),
                relations: relations.clone(),
            });
            let columns = function_columns(function, &visible).unwrap_or_else(|| {
                Columns::Hidden(format!("a column of {}", function_in_from(function)))
            });
            relations.push(aliased_relation(function.alias.as_ref(), columns));
        }
        _ => relations.push(aliased_relation(
            None,
            Columns::Hidden("a column of this kind of FROM item".to_string()),
        )),
    }
}

fn columns_of(what: &str, alias: Option<&Alias>) -> String {
    match alias {
        Some(alias) => format!("a column of {what} {}", alias.aliasname),
        None => format!("a column of {what}"),
    }
}

fn function_columns(function: &RangeFunction, scopes: &Scopes) -> Option<Columns> {
    if function.ordinality {
        return None;
    }
    if !function.coldeflist.is_empty() {
        return Some(Columns::Known(
            function
                .coldeflist
                .iter()
                .filter_map(|element| match &element.node {
                    Some(NodeKind::ColumnDef(definition)) => Some(Column {
                        name: definition.colname.clone(),
                        ty: definition
                            .type_name
                            .as_ref()
                            .and_then(|type_name| declared_type(type_name, false))
                            .ok_or_else(|| cause("a column with no declared type")),
                    }),
                    _ => None,
                })
                .collect(),
        ));
    }
    let [item] = function.functions.as_slice() else {
        return None;
    };
    let Some(NodeKind::List(list)) = &item.node else {
        return None;
    };
    let Some(NodeKind::FuncCall(call)) = list.items.first()?.node.as_ref() else {
        return None;
    };
    let names = extract_key_columns(&call.funcname).ok()?;
    let name = match names.as_slice() {
        [name] => name.to_lowercase(),
        [schema, name] if schema == "pg_catalog" => name.to_lowercase(),
        _ => return None,
    };
    let arguments = types_of(&call.args, scopes)
        .into_iter()
        .collect::<Result<Vec<PgType>, Untyped>>()
        .ok()?;
    let ty = match (name.as_str(), arguments.as_slice()) {
        ("unnest", [PgType::Array(element)]) => (**element).clone(),
        ("generate_series", [_, _] | [_, _, _]) => series_type(&arguments)?,
        _ => return None,
    };
    let column_name = match &function.alias {
        Some(alias) => alias.aliasname.clone(),
        None => name,
    };
    Some(Columns::Known(vec![Column {
        name: column_name,
        ty: Ok(ty),
    }]))
}

fn series_type(arguments: &[PgType]) -> Option<PgType> {
    match common_type(arguments, "generate_series").ok()? {
        ty @ (PgType::Int4 | PgType::Int8 | PgType::Numeric) => Some(ty),
        _ => None,
    }
}

fn function_in_from(function: &RangeFunction) -> String {
    function
        .functions
        .iter()
        .find_map(|item| match &item.node {
            Some(NodeKind::List(list)) => list.items.first(),
            _ => None,
        })
        .and_then(|call| match &call.node {
            Some(NodeKind::FuncCall(call)) => last_string(&call.funcname),
            _ => None,
        })
        .unwrap_or_else(|| "a function in FROM".to_string())
}

fn from_table_relation(table: &RangeVar, base: &Scopes) -> Relation {
    let schema = non_empty(&table.schemaname);
    let cte_columns = match schema {
        None => base
            .visible_cte(&table.relname)
            .map(|cte| cte.columns.clone()),
        Some(_) => None,
    };
    let columns = match cte_columns {
        Some(columns) => columns,
        None => base.table_columns(schema.as_deref(), &table.relname),
    };
    let mut relation = Relation {
        name: table.relname.clone(),
        schema,
        columns,
    };
    if let Some(alias) = &table.alias {
        relation.name.clone_from(&alias.aliasname);
        relation.columns = relation.columns.renamed(&names_of_alias(alias));
    }
    relation
}

fn target_relation(target: &RangeVar, scopes: &Scopes) -> Relation {
    let schema = non_empty(&target.schemaname);
    let name = match &target.alias {
        Some(alias) => alias.aliasname.clone(),
        None => target.relname.clone(),
    };
    Relation {
        name,
        columns: scopes.table_columns(schema.as_deref(), &target.relname),
        schema,
    }
}

fn aliased_relation(alias: Option<&Alias>, columns: Columns) -> Relation {
    let columns = match alias {
        Some(alias) => columns.renamed(&names_of_alias(alias)),
        None => columns,
    };
    Relation {
        name: alias.map(|a| a.aliasname.clone()).unwrap_or_default(),
        schema: None,
        columns,
    }
}

fn rename_columns(mut columns: Vec<Column>, names: &[String]) -> Vec<Column> {
    for (column, name) in columns.iter_mut().zip(names) {
        column.name.clone_from(name);
    }
    columns
}

fn names_of_alias(alias: &Alias) -> Vec<String> {
    alias_column_names(alias).unwrap_or_default()
}

fn names_of(nodes: &[Node]) -> Vec<String> {
    nodes
        .iter()
        .filter_map(|node| match &node.node {
            Some(NodeKind::String(name)) => Some(name.sval.clone()),
            _ => None,
        })
        .collect()
}

fn output_as_columns(output: OutputColumns) -> Vec<Column> {
    output
        .into_iter()
        .map(|(name, ty)| Column {
            name: name.unwrap_or_else(|| "?column?".to_string()),
            ty,
        })
        .collect()
}

fn non_empty(text: &str) -> Option<String> {
    (!text.is_empty()).then(|| text.to_string())
}

fn set_operation_columns(select: &SelectStmt, scopes: &Scopes) -> Result<OutputColumns, Untyped> {
    let ctes = with_items(select.with_clause.as_ref(), scopes);
    let inner = scopes.with_frame(Frame {
        ctes,
        relations: Vec::new(),
    });
    let (Some(left), Some(right)) = (select.larg.as_deref(), select.rarg.as_deref()) else {
        return Err(cause("a set operation with a missing side"));
    };
    let left = select_output_columns(left, &inner)?;
    let right = select_output_columns(right, &inner)?;
    if left.len() != right.len() {
        return Err(cause(
            "a set operation whose sides have different column counts",
        ));
    }
    Ok(left
        .into_iter()
        .zip(right)
        .map(|((name, left_type), (_, right_type))| {
            let ty = matching_type(vec![left_type, right_type], "UNION");
            (name, ty)
        })
        .collect())
}

fn values_columns(select: &SelectStmt, scopes: &Scopes) -> Result<OutputColumns, Untyped> {
    let columns = values_column_types(select, scopes)?;
    Ok(columns
        .into_iter()
        .enumerate()
        .map(|(index, types)| {
            (
                Some(format!("column{}", index + 1)),
                matching_type(types, "VALUES"),
            )
        })
        .collect())
}

fn values_column_types(
    select: &SelectStmt,
    scopes: &Scopes,
) -> Result<Vec<Vec<ExprType>>, Untyped> {
    let mut rows: Vec<Vec<ExprType>> = Vec::new();
    for row in &select.values_lists {
        let Some(NodeKind::List(list)) = &row.node else {
            return Err(cause("a VALUES row that is not a list"));
        };
        rows.push(types_of(&list.items, scopes));
    }
    let Some(first) = rows.first() else {
        return Err(cause("a VALUES with no rows"));
    };
    let width = first.len();
    if rows.iter().any(|row| row.len() != width) {
        return Err(cause("VALUES rows of different lengths"));
    }
    Ok((0..width)
        .map(|index| rows.iter().map(|row| row[index].clone()).collect())
        .collect())
}

fn target_list_columns(select: &SelectStmt, scopes: &Scopes) -> Result<OutputColumns, Untyped> {
    let Some(frame) = scopes.frames.last() else {
        return Err(cause("a SELECT with no scope"));
    };
    let own_relations = &frame.relations;
    let mut output = Vec::new();
    for target in &select.target_list {
        let Some(NodeKind::ResTarget(target)) = &target.node else {
            return Err(cause("a SELECT item that is not a value"));
        };
        let Some(value) = target.val.as_deref() else {
            return Err(cause("a SELECT item with no value"));
        };
        match star_qualifier(value) {
            Some(qualifier) => {
                output.extend(expand_star(qualifier.as_deref(), own_relations)?);
            }
            None => {
                let name = non_empty(&target.name).or_else(|| derived_column_name(value));
                output.push((name, type_of(value, scopes)));
            }
        }
    }
    Ok(output)
}

fn star_qualifier(node: &Node) -> Option<Option<String>> {
    let Some(NodeKind::ColumnRef(column)) = &node.node else {
        return None;
    };
    let (last, qualifier) = column.fields.split_last()?;
    if !matches!(last.node, Some(NodeKind::AStar(_))) {
        return None;
    }
    Some(last_string(qualifier))
}

fn expand_star(qualifier: Option<&str>, relations: &[Relation]) -> Result<OutputColumns, Untyped> {
    let selected: Vec<&Relation> = relations
        .iter()
        .filter(|relation| qualifier.is_none_or(|wanted| wanted == relation.name))
        .collect();
    if qualifier.is_some() && selected.is_empty() {
        return Err(Untyped::NameNotFound);
    }
    let mut output = Vec::new();
    for relation in selected {
        match &relation.columns {
            Columns::Known(columns) => {
                for column in columns {
                    output.push((Some(column.name.clone()), column.ty.clone()));
                }
            }
            Columns::NotFound => return Err(Untyped::NameNotFound),
            Columns::Hidden(description) => return Err(cause(description)),
        }
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg_query::NodeEnum;

    fn int4() -> Option<PgType> {
        Some(PgType::Int4)
    }

    fn other(name: &str) -> Option<PgType> {
        Some(PgType::Other(name.to_string()))
    }

    fn fake_lookup() -> ColumnLookup {
        Box::new(|_schema, name| {
            let columns: Vec<(&str, PgType)> = match name {
                "t" => vec![
                    ("a", PgType::Int4),
                    ("b", PgType::Text),
                    ("c", PgType::Float8),
                    ("d", PgType::Bool),
                    ("e", PgType::Int8),
                    ("n", PgType::Numeric),
                    ("ts", PgType::Other("timestamp".to_string())),
                ],
                "u" => vec![("a", PgType::Int4), ("x", PgType::Text)],
                "w" => vec![("a", PgType::Text)],
                _ => return None,
            };
            Some(
                columns
                    .into_iter()
                    .map(|(name, ty)| (name.to_string(), ty))
                    .collect(),
            )
        })
    }

    fn statement(sql: &str) -> NodeEnum {
        let parsed = crate::parse(sql).unwrap();
        parsed.protobuf.stmts[0]
            .stmt
            .as_ref()
            .unwrap()
            .node
            .clone()
            .unwrap()
    }

    fn select_of(sql: &str) -> SelectStmt {
        match statement(sql) {
            NodeEnum::SelectStmt(select) => *select,
            other => panic!("not a select: {other:?}"),
        }
    }

    fn expression(sql: &str) -> Node {
        let select = select_of(&format!("SELECT {sql}"));
        match select.target_list[0].node.clone() {
            Some(NodeKind::ResTarget(target)) => *target.val.unwrap(),
            other => panic!("not a target: {other:?}"),
        }
    }

    fn known(node: &Node, scopes: &Scopes) -> Option<PgType> {
        type_of(node, scopes).ok()
    }

    fn columns(sql: &str) -> Option<Vec<(Option<String>, Option<PgType>)>> {
        select_output_columns(&select_of(sql), &Scopes::new(Some(fake_lookup())))
            .ok()
            .map(|output| {
                output
                    .into_iter()
                    .map(|(name, ty)| (name, ty.ok()))
                    .collect()
            })
    }

    fn types(sql: &str) -> Vec<Option<PgType>> {
        columns(sql)
            .unwrap()
            .into_iter()
            .map(|(_, ty)| ty)
            .collect()
    }

    fn names(sql: &str) -> Vec<Option<String>> {
        columns(sql)
            .unwrap()
            .into_iter()
            .map(|(name, _)| name)
            .collect()
    }

    fn type_from_t(expr: &str) -> Option<PgType> {
        types(&format!("SELECT {expr} FROM t")).remove(0)
    }

    #[test]
    fn integer_constants_are_int4() {
        assert_eq!(type_from_t("1"), int4());
        assert_eq!(type_from_t("-5"), int4());
    }

    #[test]
    fn big_integer_constants_are_int8_until_they_do_not_fit() {
        assert_eq!(type_from_t("3000000000"), Some(PgType::Int8));
        assert_eq!(type_from_t("9223372036854775807"), Some(PgType::Int8));
        assert_eq!(type_from_t("9223372036854775808"), Some(PgType::Numeric));
    }

    #[test]
    fn hex_octal_binary_and_underscored_integers_are_typed_by_value() {
        assert_eq!(type_from_t("0x7fffffffffffffff"), Some(PgType::Int8));
        assert_eq!(type_from_t("0o777777777777"), Some(PgType::Int8));
        assert_eq!(
            type_from_t("0b1111111111111111111111111111111111"),
            Some(PgType::Int8)
        );
        assert_eq!(type_from_t("(-0x8000000000000000)"), Some(PgType::Int8));
        assert_eq!(type_from_t("1_000_000_000_000"), Some(PgType::Int8));
        assert_eq!(type_from_t("0x8000000000000000"), Some(PgType::Numeric));
        assert_eq!(
            type_from_t("9_223_372_036_854_775_808"),
            Some(PgType::Numeric)
        );
    }

    #[test]
    fn decimal_constants_are_numeric() {
        assert_eq!(type_from_t("1.5"), Some(PgType::Numeric));
        assert_eq!(type_from_t("1e3"), Some(PgType::Numeric));
    }

    #[test]
    fn string_null_and_parameter_are_unknown() {
        assert_eq!(type_from_t("'x'"), Some(PgType::Unknown));
        assert_eq!(type_from_t("NULL"), Some(PgType::Unknown));
        assert_eq!(type_from_t("$1"), Some(PgType::Unknown));
    }

    #[test]
    fn boolean_constants_are_bool() {
        assert_eq!(type_from_t("true"), Some(PgType::Bool));
        assert_eq!(type_from_t("false"), Some(PgType::Bool));
    }

    #[test]
    fn casts_give_the_named_type() {
        assert_eq!(type_from_t("a::integer"), int4());
        assert_eq!(type_from_t("a::bigint"), Some(PgType::Int8));
        assert_eq!(type_from_t("a::double precision"), Some(PgType::Float8));
        assert_eq!(type_from_t("a::boolean"), Some(PgType::Bool));
        assert_eq!(type_from_t("a::text"), Some(PgType::Text));
        assert_eq!(type_from_t("CAST(a AS numeric)"), Some(PgType::Numeric));
        assert_eq!(type_from_t("'x'::date"), other("date"));
    }

    #[test]
    fn casts_with_modifiers_or_array_bounds_are_other_types() {
        assert_eq!(type_from_t("a::numeric(10,2)"), other("numeric(10,2)"));
        assert_eq!(
            type_from_t("a::int4[]"),
            Some(PgType::Array(Box::new(PgType::Int4)))
        );
        assert_eq!(type_from_t("a::integer[][]"), other("int4[][]"));
        assert_eq!(type_from_t("a::date[]"), other("date[]"));
    }

    #[test]
    fn bare_column_gets_the_type_of_the_table_column() {
        assert_eq!(
            types("SELECT a, b, c, d, e, n, ts FROM t"),
            vec![
                int4(),
                Some(PgType::Text),
                Some(PgType::Float8),
                Some(PgType::Bool),
                Some(PgType::Int8),
                Some(PgType::Numeric),
                other("timestamp"),
            ]
        );
    }

    #[test]
    fn qualified_columns_use_the_table_name_or_alias() {
        assert_eq!(type_from_t("t.b"), Some(PgType::Text));
        assert_eq!(type_from_t("public.t.b"), Some(PgType::Text));
        assert_eq!(types("SELECT s.b FROM t AS s"), vec![Some(PgType::Text)]);
        assert_eq!(types("SELECT t.b FROM t AS s"), vec![None]);
    }

    #[test]
    fn column_in_two_tables_has_a_type_only_when_they_agree() {
        assert_eq!(types("SELECT a FROM t, u"), vec![int4()]);
        assert_eq!(types("SELECT a FROM t, w"), vec![None]);
        assert_eq!(
            types("SELECT u.a, w.a FROM u, w"),
            vec![int4(), Some(PgType::Text)]
        );
    }

    #[test]
    fn missing_column_and_star_have_no_type() {
        assert_eq!(type_from_t("nosuch"), None);
        assert_eq!(type_from_t("t.nosuch"), None);
        assert_eq!(type_from_t("nosuch.a"), None);
        assert_eq!(type_from_t("a.b.c.d"), None);
    }

    #[test]
    fn column_of_an_unknown_table_has_no_type_and_stops_the_search() {
        assert_eq!(types("SELECT a FROM nosuch"), vec![None]);
        assert_eq!(types("SELECT x FROM nosuch, u"), vec![Some(PgType::Text)]);
        assert_eq!(types("SELECT z FROM nosuch, u"), vec![None]);
        assert_eq!(types("SELECT (SELECT a FROM nosuch) FROM t"), vec![None]);
    }

    #[test]
    fn correlated_subquery_sees_columns_of_the_outer_query() {
        assert_eq!(
            types("SELECT (SELECT x || b FROM u LIMIT 1) FROM t"),
            vec![Some(PgType::Text)]
        );
        assert_eq!(
            types("SELECT (SELECT t.e FROM u LIMIT 1) FROM t"),
            vec![Some(PgType::Int8)]
        );
    }

    #[test]
    fn inner_column_hides_outer_column_with_the_same_name() {
        assert_eq!(
            types("SELECT (SELECT a FROM w LIMIT 1) FROM t"),
            vec![Some(PgType::Text)]
        );
    }

    #[test]
    fn arithmetic_gives_the_widest_number_type() {
        assert_eq!(type_from_t("a + a"), int4());
        assert_eq!(type_from_t("a + e"), Some(PgType::Int8));
        assert_eq!(type_from_t("e * n"), Some(PgType::Numeric));
        assert_eq!(type_from_t("n - c"), Some(PgType::Float8));
        assert_eq!(type_from_t("a / 2"), int4());
        assert_eq!(type_from_t("a % 2"), int4());
    }

    #[test]
    fn arithmetic_with_an_unknown_operand_takes_the_other_type() {
        assert_eq!(type_from_t("a + '1'"), int4());
        assert_eq!(type_from_t("'1' + c"), Some(PgType::Float8));
        assert_eq!(type_from_t("b + '1'"), None);
        assert_eq!(type_from_t("'1' + '2'"), None);
    }

    #[test]
    fn arithmetic_with_a_missing_or_non_number_operand_has_no_type() {
        assert_eq!(type_from_t("a + nosuch"), None);
        assert_eq!(type_from_t("a + b"), None);
        assert_eq!(type_from_t("d * d"), None);
    }

    #[test]
    fn unary_sign_keeps_number_types() {
        assert_eq!(type_from_t("-e"), Some(PgType::Int8));
        assert_eq!(type_from_t("+n"), Some(PgType::Numeric));
        assert_eq!(type_from_t("-b"), None);
        assert_eq!(type_from_t("-'1'"), None);
    }

    #[test]
    fn comparisons_and_pattern_matches_are_bool() {
        for expr in [
            "a = 1",
            "a <> 1",
            "a != 1",
            "a < 1",
            "a <= 1",
            "a > 1",
            "a >= 1",
            "a IS DISTINCT FROM 1",
            "a IS NOT DISTINCT FROM 1",
            "a IN (1, 2)",
            "a NOT IN (1, 2)",
            "b LIKE 'x%'",
            "b ILIKE 'x%'",
            "b SIMILAR TO 'x%'",
            "a BETWEEN 1 AND 2",
            "a NOT BETWEEN 1 AND 2",
            "a BETWEEN SYMMETRIC 1 AND 2",
            "a NOT BETWEEN SYMMETRIC 1 AND 2",
            "a = ANY (ARRAY[1, 2])",
            "a > ALL (ARRAY[1, 2])",
        ] {
            assert_eq!(type_from_t(expr), Some(PgType::Bool), "{expr}");
        }
    }

    #[test]
    fn concatenation_is_text_when_one_side_is_text_or_unknown() {
        assert_eq!(type_from_t("b || 'x'"), Some(PgType::Text));
        assert_eq!(type_from_t("'x' || 'y'"), Some(PgType::Text));
        assert_eq!(type_from_t("b || a"), Some(PgType::Text));
        assert_eq!(type_from_t("b || nosuch"), None);
        assert_eq!(type_from_t("a || e"), None);
    }

    #[test]
    fn nullif_has_the_type_of_its_first_argument() {
        assert_eq!(type_from_t("NULLIF(e, 0)"), Some(PgType::Int8));
        assert_eq!(type_from_t("NULLIF(b, 'x')"), Some(PgType::Text));
    }

    #[test]
    fn other_operators_have_no_type() {
        assert_eq!(type_from_t("a & 1"), None);
        assert_eq!(type_from_t("a ^ 2"), None);
    }

    #[test]
    fn boolean_operators_and_tests_are_bool() {
        for expr in [
            "d AND d",
            "d OR d",
            "NOT d",
            "a IS NULL",
            "a IS NOT NULL",
            "d IS TRUE",
            "d IS NOT FALSE",
            "d IS UNKNOWN",
        ] {
            assert_eq!(type_from_t(expr), Some(PgType::Bool), "{expr}");
        }
    }

    #[test]
    fn exists_in_and_all_subqueries_are_bool() {
        assert_eq!(type_from_t("EXISTS (SELECT 1 FROM u)"), Some(PgType::Bool));
        assert_eq!(type_from_t("a IN (SELECT a FROM u)"), Some(PgType::Bool));
        assert_eq!(type_from_t("a = ANY (SELECT a FROM u)"), Some(PgType::Bool));
        assert_eq!(type_from_t("a > ALL (SELECT a FROM u)"), Some(PgType::Bool));
    }

    #[test]
    fn scalar_subquery_has_the_type_of_its_one_column() {
        assert_eq!(type_from_t("(SELECT x FROM u)"), Some(PgType::Text));
        assert_eq!(type_from_t("(SELECT max(e) FROM t)"), Some(PgType::Int8));
        assert_eq!(type_from_t("(SELECT a, x FROM u)"), None);
        assert_eq!(type_from_t("ARRAY(SELECT a FROM u)"), None);
    }

    #[test]
    fn case_gives_the_common_type_of_its_results() {
        assert_eq!(type_from_t("CASE WHEN d THEN a ELSE a END"), int4());
        assert_eq!(type_from_t("CASE WHEN d THEN a END"), int4());
        assert_eq!(
            type_from_t("CASE WHEN d THEN a ELSE e END"),
            Some(PgType::Int8)
        );
        assert_eq!(
            type_from_t("CASE WHEN d THEN 'x' ELSE b END"),
            Some(PgType::Text)
        );
        assert_eq!(type_from_t("CASE WHEN d THEN 'x' END"), Some(PgType::Text));
        assert_eq!(
            type_from_t("CASE a WHEN 1 THEN n ELSE c END"),
            Some(PgType::Float8)
        );
    }

    #[test]
    fn case_with_mismatching_or_unknown_results_has_no_type() {
        assert_eq!(type_from_t("CASE WHEN d THEN a ELSE b END"), None);
        assert_eq!(type_from_t("CASE WHEN d THEN a ELSE nosuch END"), None);
    }

    #[test]
    fn coalesce_and_greatest_give_the_common_type() {
        assert_eq!(type_from_t("COALESCE(a, 1)"), int4());
        assert_eq!(type_from_t("COALESCE(a, e)"), Some(PgType::Int8));
        assert_eq!(type_from_t("COALESCE(NULL, 'x')"), Some(PgType::Text));
        assert_eq!(type_from_t("COALESCE(a, b)"), None);
        assert_eq!(type_from_t("COALESCE(a, nosuch)"), None);
        assert_eq!(type_from_t("GREATEST(a, n)"), Some(PgType::Numeric));
        assert_eq!(type_from_t("LEAST(a, 1)"), int4());
        assert_eq!(type_from_t("GREATEST(a, b)"), None);
        assert_eq!(type_from_t("GREATEST(a, nosuch)"), None);
    }

    #[test]
    fn aggregate_functions_follow_the_argument_type() {
        assert_eq!(type_from_t("count(*)"), Some(PgType::Int8));
        assert_eq!(type_from_t("count(a)"), Some(PgType::Int8));
        assert_eq!(type_from_t("sum(a)"), Some(PgType::Int8));
        assert_eq!(type_from_t("sum(e)"), Some(PgType::Numeric));
        assert_eq!(type_from_t("sum(n)"), Some(PgType::Numeric));
        assert_eq!(type_from_t("sum(c)"), Some(PgType::Float8));
        assert_eq!(type_from_t("sum(b)"), None);
        assert_eq!(type_from_t("avg(a)"), Some(PgType::Numeric));
        assert_eq!(type_from_t("avg(c)"), Some(PgType::Float8));
        assert_eq!(type_from_t("min(b)"), Some(PgType::Text));
        assert_eq!(type_from_t("max(e)"), Some(PgType::Int8));
        assert_eq!(type_from_t("max('x')"), None);
        assert_eq!(type_from_t("min(a, e)"), None);
        assert_eq!(type_from_t("abs(c)"), Some(PgType::Float8));
        assert_eq!(type_from_t("abs('1')"), None);
        assert_eq!(type_from_t("bool_and(d)"), Some(PgType::Bool));
        assert_eq!(type_from_t("string_agg(b, ',')"), Some(PgType::Text));
    }

    #[test]
    fn string_functions_return_text_or_integers() {
        for expr in [
            "lower(b)",
            "upper(b)",
            "trim(b)",
            "ltrim(b)",
            "substr(b, 1)",
            "substring(b, 1, 2)",
            "replace(b, 'x', 'y')",
            "concat(b, 'x')",
            "md5(b)",
            "version()",
            "pg_catalog.lower(b)",
            "LOWER(b)",
        ] {
            assert_eq!(type_from_t(expr), Some(PgType::Text), "{expr}");
        }
        for expr in ["length(b)", "strpos(b, 'x')", "ascii(b)", "octet_length(b)"] {
            assert_eq!(type_from_t(expr), int4(), "{expr}");
        }
    }

    #[test]
    fn rounding_functions_follow_the_argument_type() {
        assert_eq!(type_from_t("round(c)"), Some(PgType::Float8));
        assert_eq!(type_from_t("round(n)"), Some(PgType::Numeric));
        assert_eq!(type_from_t("floor(a)"), Some(PgType::Numeric));
        assert_eq!(type_from_t("ceil(c)"), Some(PgType::Float8));
        assert_eq!(type_from_t("round(c, 2)"), Some(PgType::Numeric));
        assert_eq!(type_from_t("trunc(n, 1)"), Some(PgType::Numeric));
        assert_eq!(type_from_t("floor(n, 1)"), None);
        assert_eq!(type_from_t("round(b)"), None);
    }

    #[test]
    fn number_sequence_and_time_functions_have_fixed_types() {
        assert_eq!(type_from_t("random()"), Some(PgType::Float8));
        assert_eq!(type_from_t("sqrt(c)"), Some(PgType::Float8));
        assert_eq!(type_from_t("nextval('s')"), Some(PgType::Int8));
        assert_eq!(type_from_t("row_number() OVER ()"), Some(PgType::Int8));
        assert_eq!(type_from_t("now()"), other("timestamptz"));
        assert_eq!(type_from_t("clock_timestamp()"), other("timestamptz"));
    }

    #[test]
    fn function_that_is_not_listed_has_no_type() {
        assert_eq!(type_from_t("my_function(a)"), None);
        assert_eq!(type_from_t("other_schema.lower(b)"), None);
    }

    #[test]
    fn sql_value_functions_have_their_types() {
        assert_eq!(type_from_t("CURRENT_DATE"), other("date"));
        assert_eq!(type_from_t("CURRENT_TIMESTAMP"), other("timestamptz"));
        assert_eq!(type_from_t("LOCALTIMESTAMP"), other("timestamp"));
        for expr in [
            "CURRENT_USER",
            "SESSION_USER",
            "USER",
            "CURRENT_ROLE",
            "CURRENT_SCHEMA",
            "CURRENT_CATALOG",
        ] {
            assert_eq!(type_from_t(expr), Some(PgType::Text), "{expr}");
        }
        assert_eq!(type_from_t("CURRENT_TIMESTAMP(3)"), None);
        assert_eq!(type_from_t("CURRENT_TIME"), None);
    }

    #[test]
    fn arrays_rows_and_subscripts_have_no_type() {
        assert_eq!(type_from_t("ARRAY[1, 2]"), None);
        assert_eq!(type_from_t("ROW(1, 2)"), None);
        assert_eq!(type_from_t("(ARRAY[1, 2])[1]"), None);
    }

    #[test]
    fn output_columns_are_named_by_alias_column_or_function() {
        assert_eq!(
            names("SELECT a AS z, b, count(*), 1, a + 1, a::text FROM t"),
            vec![
                Some("z".to_string()),
                Some("b".to_string()),
                Some("count".to_string()),
                None,
                None,
                Some("a".to_string()),
            ]
        );
    }

    #[test]
    fn star_expands_to_every_column_of_every_table() {
        assert_eq!(
            names("SELECT * FROM u, w"),
            vec![
                Some("a".to_string()),
                Some("x".to_string()),
                Some("a".to_string())
            ]
        );
        assert_eq!(
            types("SELECT * FROM u, w"),
            vec![int4(), Some(PgType::Text), Some(PgType::Text)]
        );
    }

    #[test]
    fn qualified_star_expands_to_one_table() {
        assert_eq!(
            types("SELECT w.*, u.* FROM u, w"),
            vec![Some(PgType::Text), int4(), Some(PgType::Text)]
        );
        assert_eq!(
            types("SELECT s.* FROM u AS s"),
            vec![int4(), Some(PgType::Text)]
        );
        assert_eq!(columns("SELECT nosuch.* FROM u"), None);
    }

    #[test]
    fn star_over_a_table_with_unknown_columns_has_no_result() {
        assert_eq!(columns("SELECT * FROM nosuch"), None);
        assert_eq!(columns("SELECT * FROM u, nosuch"), None);
        assert_eq!(columns("SELECT u.* FROM u, nosuch").unwrap().len(), 2);
    }

    #[test]
    fn table_alias_can_rename_columns() {
        assert_eq!(
            names("SELECT * FROM u AS s(p)"),
            vec![Some("p".to_string()), Some("x".to_string())]
        );
        assert_eq!(types("SELECT p FROM u AS s(p)"), vec![int4()]);
        assert_eq!(types("SELECT a FROM u AS s(p)"), vec![None]);
    }

    #[test]
    fn subquery_in_from_gives_its_columns_under_its_alias() {
        assert_eq!(
            types("SELECT s.m, s.l FROM (SELECT max(e) AS m, lower(b) AS l FROM t) AS s"),
            vec![Some(PgType::Int8), Some(PgType::Text)]
        );
        assert_eq!(
            types("SELECT q, x FROM (SELECT a, x FROM u) AS s(q)"),
            vec![int4(), Some(PgType::Text)]
        );
        assert_eq!(
            columns("SELECT * FROM (SELECT 1 AS k) AS s").unwrap().len(),
            1
        );
    }

    #[test]
    fn subquery_in_from_sees_outer_columns_only_when_it_is_lateral() {
        assert_eq!(
            types("SELECT s.m FROM t, LATERAL (SELECT e AS m) AS s"),
            vec![Some(PgType::Int8)]
        );
        assert_eq!(types("SELECT s.m FROM t, (SELECT e AS m) AS s"), vec![None]);
    }

    #[test]
    fn cte_gives_its_columns() {
        assert_eq!(
            types("WITH c AS (SELECT a, b AS bb FROM t) SELECT a, bb FROM c"),
            vec![int4(), Some(PgType::Text)]
        );
        assert_eq!(
            types("WITH c AS (SELECT a FROM t) SELECT c.a FROM c AS k"),
            vec![None]
        );
    }

    #[test]
    fn cte_column_list_renames_columns() {
        assert_eq!(
            types("WITH c(p, q) AS (SELECT a, b FROM t) SELECT p, q FROM c"),
            vec![int4(), Some(PgType::Text)]
        );
        assert_eq!(
            types("WITH c(p) AS (SELECT a, b FROM t) SELECT p, b FROM c"),
            vec![int4(), Some(PgType::Text)]
        );
    }

    #[test]
    fn later_cte_sees_earlier_cte_and_cte_hides_table() {
        assert_eq!(
            types("WITH c AS (SELECT e AS k FROM t), d AS (SELECT k FROM c) SELECT k FROM d"),
            vec![Some(PgType::Int8)]
        );
        assert_eq!(
            types("WITH t AS (SELECT 'x'::text AS solo) SELECT solo FROM t"),
            vec![Some(PgType::Text)]
        );
    }

    #[test]
    fn recursive_cte_that_refers_to_itself_has_the_types_of_its_first_part() {
        assert_eq!(
            types(
                "WITH RECURSIVE c AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM c WHERE n < 3) \
                 SELECT n FROM c"
            ),
            vec![int4()]
        );
        assert_eq!(
            types(
                "WITH RECURSIVE c(m) AS (SELECT 5000000000 UNION ALL SELECT m FROM c) \
                 SELECT m FROM c"
            ),
            vec![Some(PgType::Int8)]
        );
        assert_eq!(
            types(
                "WITH RECURSIVE c AS (SELECT n FROM c UNION ALL SELECT 1 FROM c) SELECT n FROM c"
            ),
            vec![None]
        );
        assert_eq!(
            types("WITH RECURSIVE c AS (SELECT 1 AS n FROM c) SELECT n FROM c"),
            vec![None]
        );
        assert_eq!(
            types("WITH RECURSIVE c AS (SELECT a FROM t) SELECT a FROM c"),
            vec![int4()]
        );
    }

    #[test]
    fn cte_that_is_not_a_select_has_unknown_columns() {
        assert_eq!(
            types("WITH c AS (INSERT INTO t (a) VALUES (1) RETURNING a) SELECT a FROM c"),
            vec![None]
        );
    }

    #[test]
    fn join_exposes_the_columns_of_both_sides() {
        assert_eq!(
            types("SELECT x, e FROM t JOIN u ON t.a = u.a"),
            vec![Some(PgType::Text), Some(PgType::Int8)]
        );
        assert_eq!(
            types("SELECT x, e FROM t LEFT JOIN u USING (a)"),
            vec![Some(PgType::Text), Some(PgType::Int8)]
        );
        assert_eq!(
            types("SELECT x FROM t NATURAL JOIN u CROSS JOIN w"),
            vec![Some(PgType::Text)]
        );
    }

    #[test]
    fn join_with_an_alias_has_unknown_columns() {
        assert_eq!(types("SELECT j.a FROM (t JOIN u ON true) AS j"), vec![None]);
    }

    #[test]
    fn function_in_from_has_unknown_columns() {
        assert_eq!(
            types("SELECT g, x FROM generate_series(1.5::float8, 3) AS g, u"),
            vec![None, Some(PgType::Text)]
        );
        assert_eq!(
            types("SELECT x FROM generate_series(1.5::float8, 3) AS g, u"),
            vec![Some(PgType::Text)]
        );
        assert_eq!(
            types("SELECT g.g FROM generate_series(1.5::float8, 3) AS g"),
            vec![None]
        );
    }

    #[test]
    fn union_gives_the_left_names_and_common_types() {
        assert_eq!(
            names("SELECT a AS k, b FROM t UNION ALL SELECT e, x FROM t, u"),
            vec![Some("k".to_string()), Some("b".to_string())]
        );
        assert_eq!(
            types("SELECT a, b FROM t UNION ALL SELECT e, 'x' FROM t"),
            vec![Some(PgType::Int8), Some(PgType::Text)]
        );
        assert_eq!(
            types("SELECT a FROM t INTERSECT SELECT e FROM t"),
            vec![Some(PgType::Int8)]
        );
        assert_eq!(
            types("SELECT a FROM t EXCEPT SELECT a FROM u"),
            vec![int4()]
        );
    }

    #[test]
    fn union_with_mismatching_unknown_or_uneven_sides_has_no_type() {
        assert_eq!(types("SELECT a FROM t UNION SELECT b FROM t"), vec![None]);
        assert_eq!(
            types("SELECT a FROM t UNION SELECT nosuch FROM t"),
            vec![None]
        );
        assert_eq!(columns("SELECT a, b FROM t UNION SELECT a FROM t"), None);
    }

    #[test]
    fn union_sides_see_the_with_items_of_the_whole_statement() {
        assert_eq!(
            types("WITH c AS (SELECT e AS k FROM t) SELECT k FROM c UNION SELECT k FROM c"),
            vec![Some(PgType::Int8)]
        );
    }

    #[test]
    fn values_columns_are_named_and_typed_across_rows() {
        assert_eq!(
            names("VALUES (1, 'x'), (2, 'y')"),
            vec![Some("column1".to_string()), Some("column2".to_string())]
        );
        assert_eq!(
            types("VALUES (1, 'x'), (2, 'y')"),
            vec![int4(), Some(PgType::Text)]
        );
        assert_eq!(types("VALUES (1), (3000000000)"), vec![Some(PgType::Int8)]);
        assert_eq!(types("VALUES (NULL), ('a')"), vec![Some(PgType::Text)]);
        assert_eq!(types("VALUES (1), ('a'::date)"), vec![None]);
        assert_eq!(types("VALUES (1), (true)"), vec![None]);
        assert_eq!(types("VALUES (nosuch)"), vec![None]);
    }

    #[test]
    fn values_in_from_is_typed_through_its_alias() {
        assert_eq!(
            types("SELECT p, column2 FROM (VALUES (1, 'x')) AS v(p)"),
            vec![int4(), Some(PgType::Text)]
        );
    }

    #[test]
    fn without_a_lookup_no_table_columns_are_known() {
        let scopes = Scopes::new(None);
        let select = select_of("SELECT a, 1, 'x'::text FROM t");
        let output = select_output_columns(&select, &scopes).unwrap();
        let types: Vec<_> = output.into_iter().map(|(_, ty)| ty).collect();
        assert_eq!(
            types,
            vec![
                Err(Untyped::NameNotFound),
                Ok(PgType::Int4),
                Ok(PgType::Text)
            ]
        );
        assert_eq!(
            select_output_columns(&select_of("SELECT * FROM t"), &scopes),
            Err(Untyped::NameNotFound)
        );
    }

    #[test]
    fn type_of_does_not_change_the_scopes_it_reads() {
        let mut scopes = Scopes::new(Some(fake_lookup()));
        scopes.enter_select(&select_of("SELECT 1 FROM u"));
        let depth = scopes.frames.len();
        let node = expression("(SELECT max(e) FROM t) + x");
        assert_eq!(known(&node, &scopes), None);
        assert_eq!(scopes.frames.len(), depth);
        assert_eq!(known(&expression("x"), &scopes), Some(PgType::Text));
    }

    #[test]
    fn entered_select_gives_its_columns_until_it_is_left() {
        let mut scopes = Scopes::new(Some(fake_lookup()));
        assert_eq!(known(&expression("x"), &scopes), None);
        scopes.enter_select(&select_of("SELECT 1 FROM u"));
        assert_eq!(known(&expression("x"), &scopes), Some(PgType::Text));
        scopes.enter_select(&select_of("SELECT 1 FROM t"));
        assert_eq!(known(&expression("x"), &scopes), Some(PgType::Text));
        assert_eq!(known(&expression("b"), &scopes), Some(PgType::Text));
        scopes.leave();
        assert_eq!(known(&expression("b"), &scopes), None);
        scopes.leave();
        assert_eq!(known(&expression("x"), &scopes), None);
    }

    #[test]
    fn insert_scope_has_the_target_table_and_excluded() {
        let NodeEnum::InsertStmt(insert) = statement(
            "INSERT INTO u AS target (a) VALUES (1) ON CONFLICT (a) DO UPDATE SET x = excluded.x",
        ) else {
            panic!("not an insert");
        };
        let mut scopes = Scopes::new(Some(fake_lookup()));
        scopes.enter_insert(&insert);
        assert_eq!(known(&expression("target.a"), &scopes), int4());
        assert_eq!(
            known(&expression("excluded.x"), &scopes),
            Some(PgType::Text)
        );
        assert_eq!(known(&expression("x"), &scopes), Some(PgType::Text));
        assert_eq!(known(&expression("u.x"), &scopes), None);
    }

    #[test]
    fn insert_without_on_conflict_has_no_excluded() {
        let NodeEnum::InsertStmt(insert) = statement("INSERT INTO u (a) VALUES (1)") else {
            panic!("not an insert");
        };
        let mut scopes = Scopes::new(Some(fake_lookup()));
        scopes.enter_insert(&insert);
        assert_eq!(known(&expression("u.a"), &scopes), int4());
        assert_eq!(known(&expression("excluded.x"), &scopes), None);
    }

    #[test]
    fn update_scope_has_the_target_and_the_from_items() {
        let NodeEnum::UpdateStmt(update) = statement(
            "UPDATE t SET a = 1 FROM u AS v, (SELECT 'q'::text AS z) AS s WHERE t.a = v.a",
        ) else {
            panic!("not an update");
        };
        let mut scopes = Scopes::new(Some(fake_lookup()));
        scopes.enter_update(&update);
        assert_eq!(known(&expression("t.e"), &scopes), Some(PgType::Int8));
        assert_eq!(known(&expression("v.x"), &scopes), Some(PgType::Text));
        assert_eq!(known(&expression("s.z"), &scopes), Some(PgType::Text));
        assert_eq!(known(&expression("e + 1"), &scopes), Some(PgType::Int8));
    }

    #[test]
    fn delete_scope_has_the_target_and_the_using_items() {
        let NodeEnum::DeleteStmt(delete) =
            statement("DELETE FROM t AS target USING u WHERE target.a = u.a")
        else {
            panic!("not a delete");
        };
        let mut scopes = Scopes::new(Some(fake_lookup()));
        scopes.enter_delete(&delete);
        assert_eq!(
            known(&expression("target.c"), &scopes),
            Some(PgType::Float8)
        );
        assert_eq!(known(&expression("u.x"), &scopes), Some(PgType::Text));
        assert_eq!(known(&expression("t.c"), &scopes), None);
    }

    #[test]
    fn dml_scope_sees_its_with_items() {
        let NodeEnum::DeleteStmt(delete) =
            statement("WITH c AS (SELECT e AS k FROM t) DELETE FROM u USING c WHERE u.a = c.k")
        else {
            panic!("not a delete");
        };
        let mut scopes = Scopes::new(Some(fake_lookup()));
        scopes.enter_delete(&delete);
        assert_eq!(known(&expression("c.k"), &scopes), Some(PgType::Int8));
    }

    #[test]
    fn create_index_scope_has_the_indexed_table() {
        let NodeEnum::IndexStmt(index) = statement("CREATE INDEX i ON t (a) WHERE ts > ts AND d")
        else {
            panic!("not a create index");
        };
        let mut scopes = Scopes::new(Some(fake_lookup()));
        scopes.enter_create_index(&index);
        assert_eq!(known(&expression("d"), &scopes), Some(PgType::Bool));
        assert_eq!(known(&expression("t.e"), &scopes), Some(PgType::Int8));
        assert_eq!(known(&expression("x"), &scopes), None);
        scopes.leave();
        assert_eq!(known(&expression("d"), &scopes), None);
    }

    #[test]
    fn subquery_column_type_is_the_type_of_the_only_column() {
        let select =
            select_of("SELECT 1 WHERE a IN (SELECT e FROM t) AND a IN (SELECT a, e FROM t)");
        let Some(NodeKind::BoolExpr(conditions)) = select.where_clause.unwrap().node else {
            panic!("not a boolean expression");
        };
        let scopes = Scopes::new(Some(fake_lookup()));
        let subquery = |node: &Node| match &node.node {
            Some(NodeKind::SubLink(link)) => *link.subselect.clone().unwrap(),
            other => panic!("not a sublink: {other:?}"),
        };
        assert_eq!(
            subquery_column_type(&subquery(&conditions.args[0]), &scopes),
            Ok(PgType::Int8)
        );
        assert_eq!(
            subquery_column_type(&subquery(&conditions.args[1]), &scopes),
            Err(Untyped::Cause(
                "a subquery that does not return one column".to_string()
            ))
        );
    }

    #[test]
    fn create_table_scope_has_the_new_columns_without_a_lookup() {
        let NodeEnum::CreateStmt(create) = statement(
            "CREATE TABLE made (i serial, j bigserial, k smallserial, l serial4, m serial8, \
             n smallint, o numeric(8,2), p text, q boolean, r int[], s double precision, \
             CHECK (i > 0))",
        ) else {
            panic!("not a create table");
        };
        let mut scopes = Scopes::new(None);
        scopes.enter_create_table(&create);
        let expected = [
            ("i", int4()),
            ("j", Some(PgType::Int8)),
            ("k", Some(PgType::Int2)),
            ("l", int4()),
            ("m", Some(PgType::Int8)),
            ("n", Some(PgType::Int2)),
            ("o", other("numeric(8,2)")),
            ("p", Some(PgType::Text)),
            ("q", Some(PgType::Bool)),
            ("r", Some(PgType::Array(Box::new(PgType::Int4)))),
            ("s", Some(PgType::Float8)),
        ];
        for (column, ty) in expected {
            assert_eq!(known(&expression(column), &scopes), ty, "{column}");
            assert_eq!(
                known(&expression(&format!("made.{column}")), &scopes),
                ty,
                "made.{column}"
            );
        }
        assert_eq!(known(&expression("i + 1"), &scopes), int4());
        assert_eq!(known(&expression("j * i"), &scopes), Some(PgType::Int8));
        assert_eq!(known(&expression("nosuch"), &scopes), None);
    }

    fn check_set_operation(sql: &str) -> Result<(), ParseError> {
        check_set_operation_columns(&select_of(sql), &Scopes::new(Some(fake_lookup())))
    }

    fn check_values(sql: &str) -> Result<(), ParseError> {
        check_values_columns(&select_of(sql), &Scopes::new(Some(fake_lookup())))
    }

    fn assert_check_error(result: Result<(), ParseError>, code: &str, text: &str) {
        match result {
            Err(ParseError::Sqlstate { sqlstate, message }) => {
                assert_eq!(sqlstate, code);
                assert_eq!(message, text);
            }
            other => panic!("expected error {code} {text:?}, got {other:?}"),
        }
    }

    #[test]
    fn concatenation_of_bytea_is_bytea() {
        assert_eq!(
            type_from_t("'\\x01'::bytea || '\\x02'::bytea"),
            Some(PgType::Bytea)
        );
        assert_eq!(
            type_from_t("'\\x01'::bytea || '\\x02'"),
            Some(PgType::Bytea)
        );
        assert_eq!(type_from_t("'a' || 'b'"), Some(PgType::Text));
        assert_eq!(type_from_t("'\\x01'::bytea || b"), None);
    }

    #[test]
    fn greatest_and_least_have_the_common_type_their_conversion_uses() {
        assert_eq!(type_from_t("GREATEST(a, e)"), Some(PgType::Int8));
        assert_eq!(type_from_t("LEAST(a, c, '1')"), Some(PgType::Float8));
        assert_eq!(type_from_t("COALESCE(a, e)"), Some(PgType::Int8));
        assert_eq!(type_from_t("COALESCE(a, '0')"), int4());
        assert_eq!(
            type_from_t("CASE WHEN d THEN a ELSE e END"),
            Some(PgType::Int8)
        );
        assert_eq!(type_from_t("GREATEST(a, b)"), None);
    }

    #[test]
    fn the_names_of_greatest_and_least_in_messages() {
        assert_eq!(min_max_construct(MinMaxOp::IsGreatest as i32), "GREATEST");
        assert_eq!(min_max_construct(MinMaxOp::IsLeast as i32), "LEAST");
    }

    #[test]
    fn target_columns_are_the_columns_of_the_table_in_order() {
        let scopes = Scopes::new(Some(fake_lookup()));
        let NodeEnum::InsertStmt(insert) = statement("INSERT INTO u VALUES (1, 'x')") else {
            panic!("not an insert");
        };
        let relation = insert.relation.unwrap();
        assert_eq!(
            scopes.target_columns(&relation),
            Some(vec![
                ("a".to_string(), int4()),
                ("x".to_string(), Some(PgType::Text)),
            ])
        );
        let NodeEnum::InsertStmt(insert) = statement("INSERT INTO missing VALUES (1)") else {
            panic!("not an insert");
        };
        assert_eq!(scopes.target_columns(&insert.relation.unwrap()), None);
    }

    #[test]
    fn set_operation_of_columns_that_convert_to_one_type_passes() {
        for sql in [
            "SELECT a FROM t UNION SELECT e FROM t",
            "SELECT a FROM t UNION SELECT '1'",
            "SELECT 1 INTERSECT SELECT 2",
            "SELECT a FROM t EXCEPT SELECT NULL",
            "SELECT a FROM t UNION SELECT e FROM t UNION SELECT c FROM t",
        ] {
            assert!(check_set_operation(sql).is_ok(), "{sql}");
        }
    }

    #[test]
    fn set_operation_of_columns_that_do_not_match_names_the_operation() {
        for (sql, text) in [
            (
                "SELECT a FROM t UNION SELECT b FROM t",
                "UNION types integer and text cannot be matched",
            ),
            (
                "SELECT a FROM t UNION ALL SELECT b FROM t",
                "UNION types integer and text cannot be matched",
            ),
            (
                "SELECT a FROM t INTERSECT SELECT b FROM t",
                "INTERSECT types integer and text cannot be matched",
            ),
            (
                "SELECT d FROM t EXCEPT SELECT a FROM t",
                "EXCEPT types boolean and integer cannot be matched",
            ),
            (
                "SELECT 1 UNION SELECT 2 UNION SELECT b FROM t",
                "UNION types integer and text cannot be matched",
            ),
            (
                "SELECT a, b FROM t UNION SELECT a, a FROM t",
                "UNION types text and integer cannot be matched",
            ),
        ] {
            assert_check_error(check_set_operation(sql), "42804", text);
        }
    }

    #[test]
    fn set_operation_with_a_type_that_has_no_supported_casts_is_not_supported() {
        assert_check_error(
            check_set_operation("SELECT a FROM t UNION SELECT ts FROM t"),
            "0A000",
            "UNION with types integer and timestamp is not supported",
        );
    }

    #[test]
    fn set_operation_with_columns_that_are_not_known_passes() {
        for sql in [
            "SELECT a FROM missing UNION SELECT b FROM t",
            "SELECT a FROM t UNION SELECT a, b FROM t",
        ] {
            assert!(check_set_operation(sql).is_ok(), "{sql}");
        }
    }

    #[test]
    fn values_rows_that_convert_to_one_type_pass() {
        for sql in [
            "VALUES (1), (2)",
            "VALUES (1, 'a'), (2, 'b')",
            "VALUES (1), ('2'), (NULL)",
            "VALUES (1), (3000000000)",
        ] {
            assert!(check_values(sql).is_ok(), "{sql}");
        }
    }

    #[test]
    fn values_rows_that_do_not_match_are_an_error() {
        assert_check_error(
            check_values("VALUES (1), ('x'::text)"),
            "42804",
            "VALUES types integer and text cannot be matched",
        );
        assert_check_error(
            check_values("VALUES (1, true), (2, 3)"),
            "42804",
            "VALUES types boolean and integer cannot be matched",
        );
    }

    fn why(sql: &str) -> ExprType {
        let mut output = select_output_columns(&select_of(sql), &Scopes::new(Some(fake_lookup())))
            .expect("the select has known columns");
        output.remove(0).1
    }

    fn not_covered_because(description: &str) -> ExprType {
        Err(Untyped::Cause(description.to_string()))
    }

    #[test]
    fn a_function_that_is_not_listed_names_the_function() {
        assert_eq!(
            why("SELECT json_extract(b, 'x') FROM t"),
            not_covered_because("the function json_extract")
        );
        assert_eq!(
            why("SELECT auth.uid() FROM t"),
            not_covered_because("the function auth.uid")
        );
    }

    #[test]
    fn a_listed_function_on_a_type_it_does_not_cover_names_the_type() {
        assert_eq!(
            why("SELECT sum(b) FROM t"),
            not_covered_because("the function sum on a value of type text")
        );
        assert_eq!(
            why("SELECT round(b) FROM t"),
            not_covered_because("the function round on a value of type text")
        );
        assert_eq!(
            why("SELECT abs('x') FROM t"),
            not_covered_because("the function abs on a value of type unknown")
        );
    }

    #[test]
    fn a_function_whose_argument_has_no_type_gives_that_argument_as_the_cause() {
        assert_eq!(
            why("SELECT abs(json_extract(b, 'x')) FROM t"),
            not_covered_because("the function json_extract")
        );
        assert_eq!(
            why("SELECT count(json_extract(b, 'x')) FROM t"),
            Ok(PgType::Int8)
        );
    }

    #[test]
    fn an_operator_that_is_not_listed_names_the_operator() {
        assert_eq!(
            why("SELECT b ->> 'x' FROM t"),
            not_covered_because("the operator ->>")
        );
        assert_eq!(
            why("SELECT -b FROM t"),
            not_covered_because("the operator -")
        );
        assert_eq!(
            why("SELECT ts + 1 FROM t"),
            not_covered_because("the operator +")
        );
        assert_eq!(
            why("SELECT d || a FROM t"),
            not_covered_because("the operator ||")
        );
    }

    #[test]
    fn concatenation_of_a_value_with_text_is_text() {
        for expr in [
            "a || b", "b || a", "e || 'x'", "'x' || c", "d || b", "n || 'x'",
        ] {
            assert_eq!(
                why(&format!("SELECT {expr} FROM t")),
                Ok(PgType::Text),
                "{expr}"
            );
        }
        assert_eq!(
            why("SELECT a || e FROM t"),
            not_covered_because("the operator ||")
        );
        assert_eq!(
            why("SELECT ts || b FROM t"),
            not_covered_because("the operator ||")
        );
    }

    #[test]
    fn regular_expression_and_pattern_operators_are_boolean() {
        for operator in ["~", "~*", "!~", "!~*", "~~", "~~*", "!~~", "!~~*"] {
            assert_eq!(
                why(&format!("SELECT b {operator} 'x' FROM t")),
                Ok(PgType::Bool),
                "{operator}"
            );
        }
    }

    #[test]
    fn an_operand_with_no_type_gives_its_cause_for_arithmetic() {
        assert_eq!(
            why("SELECT a + json_extract(b, 'x') FROM t"),
            not_covered_because("the function json_extract")
        );
    }

    #[test]
    fn array_row_and_subscript_say_what_they_are() {
        assert_eq!(
            why("SELECT ARRAY[1, 2] FROM t"),
            not_covered_because("an ARRAY constructor")
        );
        assert_eq!(
            why("SELECT ROW(1, 2) FROM t"),
            not_covered_because("a row value")
        );
        assert_eq!(
            why("SELECT (a, b) FROM t"),
            not_covered_because("a row value")
        );
        assert_eq!(
            why("SELECT a[1] FROM t"),
            not_covered_because("a subscript or field selection")
        );
        assert_eq!(
            why("SELECT ARRAY(SELECT a FROM t) FROM t"),
            not_covered_because("an ARRAY subquery")
        );
    }

    #[test]
    fn a_whole_row_reference_is_a_row_value() {
        assert_eq!(why("SELECT count(t.*) FROM t"), Ok(PgType::Int8));
        assert_eq!(
            why("SELECT abs(t.*) FROM t"),
            not_covered_because("a row value")
        );
    }

    #[test]
    fn a_bit_string_constant_and_a_decimal_current_time_say_what_they_are() {
        assert_eq!(
            why("SELECT B'101' FROM t"),
            not_covered_because("a bit string constant")
        );
        assert_eq!(
            why("SELECT CURRENT_TIME FROM t"),
            not_covered_because("CURRENT_TIME")
        );
        assert_eq!(
            why("SELECT LOCALTIME FROM t"),
            not_covered_because("LOCALTIME")
        );
        assert_eq!(
            why("SELECT CURRENT_TIMESTAMP(3) FROM t"),
            not_covered_because("CURRENT_TIMESTAMP with a precision")
        );
        assert_eq!(
            why("SELECT LOCALTIMESTAMP(3) FROM t"),
            not_covered_because("LOCALTIMESTAMP with a precision")
        );
    }

    #[test]
    fn a_construct_whose_types_do_not_match_says_so() {
        assert_eq!(
            why("SELECT COALESCE(a, b) FROM t"),
            not_covered_because("a COALESCE whose types do not match")
        );
        assert_eq!(
            why("SELECT CASE WHEN d THEN a ELSE b END FROM t"),
            not_covered_because("a CASE whose types do not match")
        );
        assert_eq!(
            why("SELECT GREATEST(a, b) FROM t"),
            not_covered_because("a GREATEST whose types do not match")
        );
    }

    #[test]
    fn a_column_of_a_relation_with_hidden_columns_names_the_relation() {
        assert_eq!(
            why("SELECT g FROM generate_series(1.5::float8, 3) AS g"),
            not_covered_because("a column of generate_series")
        );
        assert_eq!(
            why("SELECT s.g FROM generate_series(1.5::float8, 3) AS s(g)"),
            not_covered_because("a column of generate_series")
        );
        assert_eq!(
            why("SELECT j.a FROM (t JOIN u ON true) AS j"),
            not_covered_because("a column of the join j")
        );
        assert_eq!(
            why("WITH RECURSIVE c AS (SELECT n FROM c) SELECT n FROM c"),
            not_covered_because("a column of the recursive WITH query c")
        );
        assert_eq!(
            why("WITH c AS (INSERT INTO t (a) VALUES (1) RETURNING a) SELECT a FROM c"),
            not_covered_because("a column of the WITH query c")
        );
        assert_eq!(
            why("SELECT s.k FROM (SELECT * FROM generate_series(1.5::float8, 3)) AS s"),
            not_covered_because("a column of the subquery s")
        );
    }

    #[test]
    fn a_column_that_matches_no_column_is_a_name_that_does_not_resolve() {
        for sql in [
            "SELECT nosuch FROM t",
            "SELECT t.nosuch FROM t",
            "SELECT s.a FROM t",
            "SELECT nosuch FROM t, u",
            "SELECT a FROM t, w",
        ] {
            assert_eq!(why(sql), Err(Untyped::NameNotFound), "{sql}");
        }
    }

    #[test]
    fn a_column_of_a_table_that_is_not_found_is_a_name_that_does_not_resolve() {
        assert_eq!(why("SELECT x FROM nosuch"), Err(Untyped::NameNotFound));
        assert_eq!(
            why("SELECT nosuch.x FROM nosuch"),
            Err(Untyped::NameNotFound)
        );
        assert_eq!(
            why("SELECT x FROM nosuch, generate_series(1, 3)"),
            Err(Untyped::NameNotFound)
        );
        assert_eq!(
            why("SELECT x FROM u, nosuch"),
            Ok(PgType::Text),
            "a column that a known table has still has its type"
        );
    }

    #[test]
    fn without_a_lookup_every_column_is_a_name_that_does_not_resolve() {
        let scopes = Scopes::new(None);
        let select = select_of("SELECT a, t.a FROM t");
        let output = select_output_columns(&select, &scopes).unwrap();
        for (_, ty) in output {
            assert_eq!(ty, Err(Untyped::NameNotFound));
        }
        assert_eq!(
            type_of(&expression("x"), &scopes),
            Err(Untyped::NameNotFound)
        );
    }

    #[test]
    fn a_name_that_does_not_resolve_wins_over_a_cause_inside_the_same_expression() {
        for sql in [
            "SELECT json_extract(nosuch, 'x') FROM t",
            "SELECT nosuch ->> 'x' FROM t",
            "SELECT json_extract(b, 'x') ->> nosuch FROM t",
            "SELECT abs(json_extract(b, nosuch)) FROM t",
            "SELECT ARRAY[nosuch] FROM t",
            "SELECT ROW(a, nosuch) FROM t",
            "SELECT nosuch[1] FROM t",
            "SELECT COALESCE(nosuch, json_extract(b, 'x')) FROM t",
        ] {
            assert_eq!(why(sql), Err(Untyped::NameNotFound), "{sql}");
        }
    }

    #[test]
    fn a_column_whose_own_value_has_no_type_gives_that_cause_through_a_subquery() {
        assert_eq!(
            why("SELECT s.v FROM (SELECT json_extract(b, 'x') AS v FROM t) AS s"),
            not_covered_because("the function json_extract")
        );
    }

    #[test]
    fn functions_with_one_result_type_are_typed() {
        for (call, expected) in [
            ("current_setting('x')", PgType::Text),
            ("set_config('x', 'y', false)", PgType::Text),
            ("current_schema()", PgType::Text),
            ("pg_get_userbyid(a)", PgType::Text),
            ("pg_get_expr(b, a)", PgType::Text),
            ("pg_get_constraintdef(a)", PgType::Text),
            ("pg_get_indexdef(a)", PgType::Text),
            ("chr(a)", PgType::Text),
            ("pg_backend_pid()", PgType::Int4),
            ("array_length(b, 1)", PgType::Int4),
            ("int4(a)", PgType::Int4),
            ("int8(a)", PgType::Int8),
            ("float8(a)", PgType::Float8),
            ("booleq(d, d)", PgType::Bool),
            ("boolne(d, d)", PgType::Bool),
            ("pg_type_is_visible(a)", PgType::Bool),
            ("pg_input_is_valid(b, b)", PgType::Bool),
        ] {
            assert_eq!(
                why(&format!("SELECT {call} FROM t")),
                Ok(expected),
                "{call}"
            );
        }
    }

    #[test]
    fn require_type_passes_a_name_that_does_not_resolve_and_rejects_a_cause() {
        assert_eq!(require_type(Ok(PgType::Int4)).unwrap(), Some(PgType::Int4));
        assert_eq!(require_type(Err(Untyped::NameNotFound)).unwrap(), None);
        match require_type(not_covered_because("the function f")) {
            Err(ParseError::Sqlstate { sqlstate, message }) => {
                assert_eq!(sqlstate, "0A000");
                assert_eq!(message, "the type of the function f is not known");
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn require_types_lets_every_value_through_when_one_name_does_not_resolve() {
        let typed = vec![
            Ok(PgType::Int4),
            not_covered_because("the function f"),
            Err(Untyped::NameNotFound),
        ];
        assert_eq!(
            require_types(typed).unwrap(),
            vec![Some(PgType::Int4), None, None]
        );
        let typed = vec![Ok(PgType::Int4), not_covered_because("the function f")];
        assert!(require_types(typed).is_err());
    }

    #[test]
    fn a_cast_to_oid_or_regclass_has_that_type() {
        for (expr, expected) in [
            ("a::oid", PgType::Oid),
            ("a::pg_catalog.oid", PgType::Oid),
            ("'1'::OID", PgType::Oid),
            ("a::regclass", PgType::Regclass),
            ("a::oid::regclass", PgType::Regclass),
            ("a::regclass::oid", PgType::Oid),
        ] {
            assert_eq!(type_from_t(expr), Some(expected), "{expr}");
        }
    }

    #[test]
    fn comparisons_of_oid_are_boolean() {
        assert_eq!(type_from_t("a::oid = 5"), Some(PgType::Bool));
        assert_eq!(type_from_t("a = 'x'::regclass"), Some(PgType::Bool));
    }

    #[test]
    fn concatenating_an_oid_with_text_is_text() {
        assert_eq!(type_from_t("a::oid || b"), Some(PgType::Text));
        assert_eq!(type_from_t("b || a::oid"), Some(PgType::Text));
    }

    #[test]
    fn coalesce_and_case_of_oid_and_integers_are_oid() {
        assert_eq!(type_from_t("COALESCE(a, a::oid)"), Some(PgType::Oid));
        assert_eq!(type_from_t("COALESCE(e, a::oid, NULL)"), Some(PgType::Oid));
        assert_eq!(
            type_from_t("CASE WHEN d THEN a ELSE 5::oid END"),
            Some(PgType::Oid)
        );
    }

    #[test]
    fn coalesce_of_oid_and_a_float_is_not_typed() {
        assert_eq!(type_from_t("COALESCE(a::oid, c)"), None);
    }

    #[test]
    fn oid_has_no_unary_sign_so_it_is_not_typed() {
        assert_eq!(type_from_t("-a::oid"), None);
    }

    #[test]
    fn a_subscript_of_a_name_is_a_char() {
        assert_eq!(type_from_t("b[0]"), Some(PgType::Char));
        assert_eq!(type_from_t("(b)[1]"), Some(PgType::Char));
        assert_eq!(type_from_t("b[0] = '_'"), Some(PgType::Bool));
    }

    #[test]
    fn a_slice_or_several_subscripts_of_a_name_are_not_typed() {
        assert_eq!(type_from_t("b[0:1]"), None);
        assert_eq!(type_from_t("b[0][1]"), None);
    }

    fn array_of(element: PgType) -> PgType {
        PgType::Array(Box::new(element))
    }

    #[test]
    fn unnest_of_an_array_has_one_column_of_the_element_type() {
        assert_eq!(
            types("SELECT unnest FROM unnest('{1,2}'::pg_catalog.oid[])"),
            vec![Some(PgType::Oid)]
        );
        assert_eq!(
            types("SELECT src.tbloid FROM unnest('{1}'::oid[]) AS src(tbloid)"),
            vec![Some(PgType::Oid)]
        );
        assert_eq!(
            types("SELECT s FROM unnest('{a}'::text[]) AS s"),
            vec![Some(PgType::Text)]
        );
    }

    #[test]
    fn unnest_of_anything_else_has_unknown_columns() {
        for sql in [
            "SELECT unnest FROM unnest(ARRAY[1, 2])",
            "SELECT unnest FROM unnest('{1}'::oid[], '{2}'::oid[])",
            "SELECT unnest FROM unnest('{1}'::oid[]) WITH ORDINALITY",
            "SELECT unnest FROM unnest(a) ",
        ] {
            assert_eq!(types(sql), vec![None], "{sql}");
        }
    }

    #[test]
    fn unnest_can_read_a_column_of_a_relation_on_its_left() {
        assert_eq!(
            types("SELECT unnest FROM u, unnest(x::text[])"),
            vec![Some(PgType::Text)]
        );
    }

    #[test]
    fn generate_series_of_integers_has_one_integer_column() {
        for (sql, expected) in [
            ("SELECT g FROM generate_series(1, 3) AS g", PgType::Int4),
            (
                "SELECT generate_series FROM generate_series(1, 3)",
                PgType::Int4,
            ),
            (
                "SELECT g.n FROM generate_series(1, 3) AS g(n)",
                PgType::Int4,
            ),
            ("SELECT g FROM generate_series(1, 3, 2) AS g", PgType::Int4),
            (
                "SELECT g FROM generate_series(1, 3::int8) AS g",
                PgType::Int8,
            ),
            (
                "SELECT g FROM generate_series(1, 3.5) AS g",
                PgType::Numeric,
            ),
            ("SELECT g FROM t, generate_series(a, 3) AS g", PgType::Int4),
        ] {
            assert_eq!(types(sql), vec![Some(expected)], "{sql}");
        }
    }

    #[test]
    fn generate_series_of_other_types_has_unknown_columns() {
        for sql in [
            "SELECT g FROM generate_series(1.5::float8, 3) AS g",
            "SELECT g FROM generate_series('2024-01-01'::date, '2024-01-03'::date, '1 day'::interval) AS g",
            "SELECT g FROM generate_series(1) AS g",
        ] {
            assert_eq!(types(sql), vec![None], "{sql}");
        }
    }

    #[test]
    fn a_subscript_of_an_array_has_the_element_type() {
        assert_eq!(type_from_t("('{1,2}'::int2[])[1]"), Some(PgType::Int2));
        assert_eq!(type_from_t("('{1,2}'::oid[])[1] = 1"), Some(PgType::Bool));
    }

    #[test]
    fn casts_to_regnamespace_regproc_smallint_and_char_have_those_types() {
        assert_eq!(
            type_from_t("'pg_catalog'::regnamespace"),
            Some(PgType::Regnamespace)
        );
        assert_eq!(type_from_t("a::pg_catalog.regproc"), Some(PgType::Regproc));
        assert_eq!(type_from_t("0::pg_catalog.int2"), Some(PgType::Int2));
        assert_eq!(type_from_t("'s'::\"char\""), Some(PgType::Char));
    }

    #[test]
    fn smallint_is_the_narrowest_number_for_arithmetic() {
        assert_eq!(type_from_t("0::int2 + 1::int2"), Some(PgType::Int2));
        assert_eq!(type_from_t("0::int2 + 1"), Some(PgType::Int4));
        assert_eq!(type_from_t("-(0::int2)"), Some(PgType::Int2));
    }
}
