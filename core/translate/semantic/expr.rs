//! Parser-expression conversion into resolved HIR expressions.

use smallvec::SmallVec;
use turso_parser::ast;

use super::{
    analyze::{Analyzer, CatalogObjectKind},
    context::DoubleQuotedDml,
    hir,
    scope::{ExprCollation, NamePrecedence, ResolvedScopeExpr, Scope},
};
use crate::{
    function::{AggFunc, Func, ScalarFunc, WindowFunc},
    schema::{Schema, Type, TypeDef, AUTOINCREMENT_SEQ_PREFIX, SQLITE_SEQUENCE_TABLE_NAME},
    sync::Arc,
    translate::collate::CollationSeq,
    util::normalize_ident,
    vdbe::affinity::Affinity,
    LimboError, Result,
};

#[cfg(feature = "json")]
use crate::function::JsonFunc;

/// Clause rules that change expression name visibility.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ExprPolicy {
    precedence: NamePrecedence,
    allow_dqs_fallback: bool,
    aggregates: AggregatePolicy,
    allow_windows: bool,
    raise: RaisePolicy,
    self_source: Option<hir::SourceId>,
    policies: ExprPolicies,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ExprPolicies {
    dqs_dml: DoubleQuotedDml,
    trigger: Option<TriggerExprContext>,
    row_values: RowValueChecks,
}

/// Which row value widths are checked. SQLite checks some of them only
/// while generating code, so an ORDER BY term it drops skips them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RowValueChecks {
    All,
    /// Only the width of scalar subqueries, as in the ORDER BY terms SQLite
    /// drops after a rowid.
    ScalarSubqueries,
    /// None, as in the ORDER BY SQLite drops from an aggregate query
    /// without GROUP BY.
    None,
}

#[derive(Clone, Copy, Debug)]
struct TriggerExprContext {
    new_source: Option<hir::SourceId>,
    old_source: Option<hir::SourceId>,
}

#[derive(Clone, Copy, Debug)]
enum AggregatePolicy {
    Allow,
    RejectFunction,
    RejectMisuse,
}

#[derive(Clone, Copy, Debug)]
enum RaisePolicy {
    AbortOnly,
    Trigger,
}

struct ResolvedCustomMember {
    container_type: hir::ResolvedType,
    result_type: hir::TypeFact,
    kind: hir::FieldAccessKind,
}

struct CustomOperatorColumn {
    declared_name: String,
    definition: hir::ResolvedType,
    encoder: Option<hir::BoundSchemaCall>,
}

struct CustomOperatorDefinition {
    function_name: String,
    swap_args: bool,
    negate: bool,
}

impl ExprPolicy {
    pub(crate) const fn select(dqs_dml: DoubleQuotedDml) -> Self {
        Self {
            precedence: NamePrecedence::SourcesOnly,
            allow_dqs_fallback: dqs_dml.is_enabled(),
            aggregates: AggregatePolicy::Allow,
            allow_windows: true,
            raise: RaisePolicy::AbortOnly,
            self_source: None,
            policies: ExprPolicies::statement(dqs_dml),
        }
    }

    pub(crate) const fn where_clause(dqs_dml: DoubleQuotedDml) -> Self {
        Self {
            precedence: NamePrecedence::SourceThenOutput,
            allow_dqs_fallback: dqs_dml.is_enabled(),
            aggregates: AggregatePolicy::RejectFunction,
            allow_windows: false,
            raise: RaisePolicy::AbortOnly,
            self_source: None,
            policies: ExprPolicies::statement(dqs_dml),
        }
    }

    pub(crate) const fn group_by(dqs_dml: DoubleQuotedDml) -> Self {
        Self {
            precedence: NamePrecedence::SourceThenOutput,
            allow_dqs_fallback: dqs_dml.is_enabled(),
            aggregates: AggregatePolicy::RejectFunction,
            allow_windows: false,
            raise: RaisePolicy::AbortOnly,
            self_source: None,
            policies: ExprPolicies::statement(dqs_dml),
        }
    }

    pub(crate) const fn having(dqs_dml: DoubleQuotedDml) -> Self {
        Self {
            precedence: NamePrecedence::OutputThenSource,
            allow_dqs_fallback: dqs_dml.is_enabled(),
            aggregates: AggregatePolicy::Allow,
            allow_windows: false,
            raise: RaisePolicy::AbortOnly,
            self_source: None,
            policies: ExprPolicies::statement(dqs_dml),
        }
    }

    pub(crate) const fn order_by(dqs_dml: DoubleQuotedDml, aggregate_query: bool) -> Self {
        Self {
            precedence: NamePrecedence::OutputThenSource,
            allow_dqs_fallback: dqs_dml.is_enabled(),
            aggregates: if aggregate_query {
                AggregatePolicy::Allow
            } else {
                AggregatePolicy::RejectMisuse
            },
            allow_windows: true,
            raise: RaisePolicy::AbortOnly,
            self_source: None,
            policies: ExprPolicies::statement(dqs_dml),
        }
    }

    pub(crate) const fn limit(dqs_dml: DoubleQuotedDml) -> Self {
        Self {
            precedence: NamePrecedence::SourcesOnly,
            allow_dqs_fallback: dqs_dml.is_enabled(),
            aggregates: AggregatePolicy::RejectFunction,
            allow_windows: false,
            raise: RaisePolicy::AbortOnly,
            self_source: None,
            policies: ExprPolicies::statement(dqs_dml),
        }
    }

    pub(crate) const fn table_function(dqs_dml: DoubleQuotedDml) -> Self {
        Self {
            precedence: NamePrecedence::SourcesOnly,
            allow_dqs_fallback: dqs_dml.is_enabled(),
            aggregates: AggregatePolicy::RejectFunction,
            allow_windows: false,
            raise: RaisePolicy::AbortOnly,
            self_source: None,
            policies: ExprPolicies::statement(dqs_dml),
        }
    }

    pub(super) const fn insert_values(dqs_dml: DoubleQuotedDml) -> Self {
        Self {
            precedence: NamePrecedence::SourcesOnly,
            allow_dqs_fallback: dqs_dml.is_enabled(),
            aggregates: AggregatePolicy::RejectFunction,
            allow_windows: false,
            raise: RaisePolicy::AbortOnly,
            self_source: None,
            policies: ExprPolicies::statement(dqs_dml),
        }
    }

    pub(super) const fn returning(dqs_dml: DoubleQuotedDml) -> Self {
        Self::insert_values(dqs_dml)
    }

    pub(super) const fn update(dqs_dml: DoubleQuotedDml) -> Self {
        Self::insert_values(dqs_dml)
    }

    const fn without_windows(mut self) -> Self {
        self.allow_windows = false;
        self
    }

    pub(super) const fn schema_expression() -> Self {
        Self {
            precedence: NamePrecedence::SourcesOnly,
            allow_dqs_fallback: false,
            aggregates: AggregatePolicy::RejectFunction,
            allow_windows: false,
            raise: RaisePolicy::AbortOnly,
            self_source: None,
            policies: ExprPolicies::statement(DoubleQuotedDml::Disabled),
        }
    }

    pub(super) const fn with_self_source(mut self, source: hir::SourceId) -> Self {
        self.self_source = Some(source);
        self
    }
}

impl ExprPolicies {
    pub(crate) const fn statement(dqs_dml: DoubleQuotedDml) -> Self {
        Self {
            dqs_dml,
            trigger: None,
            row_values: RowValueChecks::All,
        }
    }

    pub(super) const fn with_row_value_checks(mut self, checks: RowValueChecks) -> Self {
        self.row_values = checks;
        self
    }

    pub(super) fn trigger(dqs_dml: DoubleQuotedDml, environment: &hir::TriggerEnvironment) -> Self {
        Self {
            dqs_dml,
            trigger: Some(TriggerExprContext {
                new_source: environment.new_source,
                old_source: environment.old_source,
            }),
            row_values: RowValueChecks::All,
        }
    }

    fn apply(self, mut policy: ExprPolicy) -> ExprPolicy {
        policy.policies = self;
        if self.trigger.is_some() {
            policy.raise = RaisePolicy::Trigger;
        }
        policy
    }

    pub(crate) fn select(self) -> ExprPolicy {
        self.apply(ExprPolicy::select(self.dqs_dml))
    }

    pub(crate) fn where_clause(self) -> ExprPolicy {
        self.apply(ExprPolicy::where_clause(self.dqs_dml))
    }

    pub(crate) fn group_by(self) -> ExprPolicy {
        self.apply(ExprPolicy::group_by(self.dqs_dml))
    }

    pub(crate) fn having(self) -> ExprPolicy {
        self.apply(ExprPolicy::having(self.dqs_dml))
    }

    pub(crate) fn order_by(self, aggregate_query: bool) -> ExprPolicy {
        self.apply(ExprPolicy::order_by(self.dqs_dml, aggregate_query))
    }

    pub(crate) fn limit(self) -> ExprPolicy {
        self.apply(ExprPolicy::limit(self.dqs_dml))
    }

    pub(crate) fn table_function(self) -> ExprPolicy {
        self.apply(ExprPolicy::table_function(self.dqs_dml))
    }

    pub(super) fn insert_values(self) -> ExprPolicy {
        self.apply(ExprPolicy::insert_values(self.dqs_dml))
    }

    pub(super) fn returning(self) -> ExprPolicy {
        self.apply(ExprPolicy::returning(self.dqs_dml))
    }

    pub(super) fn update(self) -> ExprPolicy {
        self.apply(ExprPolicy::update(self.dqs_dml))
    }

    pub(super) fn schema_expression(self) -> ExprPolicy {
        self.apply(ExprPolicy::schema_expression())
    }
}

type ExprChildren = SmallVec<[ResolvedScopeExpr; 3]>;
type FrameChildren = SmallVec<[FrameValue; 3]>;

#[allow(clippy::large_enum_variant)]
enum FrameValue {
    Scalar(ResolvedScopeExpr),
    RowSubquery {
        query: hir::QueryId,
        /// Boxed: a row subquery is rare, and every frame holds a few of
        /// these values inline.
        components: Box<ExprChildren>,
    },
}

impl FrameValue {
    /// A value standing for this one in an expression that is never
    /// evaluated: the first column of a row subquery.
    fn into_any_value(self) -> ResolvedScopeExpr {
        match self {
            Self::Scalar(value) => value,
            Self::RowSubquery { mut components, .. } => components.swap_remove(0),
        }
    }

    fn into_scalar(self) -> Result<ResolvedScopeExpr> {
        match self {
            Self::Scalar(value) => Ok(value),
            Self::RowSubquery { components, .. } => crate::bail_parse_error!(
                "sub-select returns {} columns - expected 1",
                components.len()
            ),
        }
    }

    fn into_operand(self) -> ExprOperand {
        match self {
            Self::Scalar(value) => ExprOperand::Scalar(value),
            Self::RowSubquery { query, components } => {
                ExprOperand::RowSubquery { query, components }
            }
        }
    }
}

enum ExprOperand {
    Scalar(ResolvedScopeExpr),
    RowSubquery {
        query: hir::QueryId,
        components: Box<ExprChildren>,
    },
    RowValue(Box<ExprChildren>),
}

impl ExprOperand {
    fn components(&self) -> &[ResolvedScopeExpr] {
        match self {
            Self::Scalar(value) => std::slice::from_ref(value),
            Self::RowSubquery { components, .. } | Self::RowValue(components) => components,
        }
    }

    fn into_expr(self) -> hir::Expr {
        match self {
            Self::Scalar(value) => value.expr,
            Self::RowSubquery { query, .. } => {
                hir::Expr::Subquery(hir::SubqueryExpr::Row { query })
            }
            Self::RowValue(components) => hir::Expr::Row(
                components
                    .into_iter()
                    .map(|component| component.expr)
                    .collect(),
            ),
        }
    }
}

#[derive(Clone)]
pub(super) struct QueryFunctionState<'ast> {
    block: hir::QueryBlockId,
    window_function_count: usize,
    named_windows: Vec<NamedFunctionWindow<'ast>>,
    windows: Vec<hir::ResolvedWindow>,
    /// The `OVER name` of each window in `windows`, if any.
    window_names: Vec<Option<String>>,
}

impl<'ast> QueryFunctionState<'ast> {
    pub(super) const fn new(block: hir::QueryBlockId) -> Self {
        Self {
            block,
            window_function_count: 0,
            named_windows: Vec::new(),
            windows: Vec::new(),
            window_names: Vec::new(),
        }
    }

    pub(super) const fn block(&self) -> hir::QueryBlockId {
        self.block
    }

    fn allocate_window(&mut self) -> hir::WindowFunctionId {
        let id = hir::WindowFunctionId::new(self.block, self.window_function_count);
        self.window_function_count += 1;
        id
    }

    pub(super) const fn window_function_count(&self) -> usize {
        self.window_function_count
    }

    pub(super) fn take_windows(&mut self) -> Vec<hir::ResolvedWindow> {
        self.window_names.clear();
        std::mem::take(&mut self.windows)
    }

    /// Return the block window for one call, sharing an earlier window when
    /// it can: a call with an inline OVER clause reuses any
    /// window with the same frame, PARTITION BY, and ORDER BY; a call naming
    /// its window reuses the latest window with that name and frame.
    fn allocate_resolved_window(
        &mut self,
        window: FunctionWindow,
        function: &Func,
    ) -> Result<hir::WindowId> {
        let name = window.name.clone();
        let id = hir::WindowId::new(self.block, self.windows.len());
        let resolved = window.into_hir(id, function)?;
        let existing = match &name {
            Some(name) => self.windows.iter().zip(&self.window_names).rposition(
                |(existing, existing_name)| {
                    existing_name.as_ref() == Some(name)
                        && frames_are_equivalent(&existing.frame, &resolved.frame)
                },
            ),
            None => self
                .windows
                .iter()
                .position(|existing| windows_are_equivalent(existing, &resolved)),
        };
        if let Some(index) = existing {
            return Ok(self.windows[index].id);
        }
        self.windows.push(resolved);
        self.window_names.push(name);
        Ok(id)
    }
}

fn windows_are_equivalent(left: &hir::ResolvedWindow, right: &hir::ResolvedWindow) -> bool {
    frames_are_equivalent(&left.frame, &right.frame)
        && left.partition_by.len() == right.partition_by.len()
        && left
            .partition_by
            .iter()
            .zip(&right.partition_by)
            .all(|(left, right)| left.expr.equivalent(&right.expr))
        && left.order_by.len() == right.order_by.len()
        && left
            .order_by
            .iter()
            .zip(&right.order_by)
            .all(|(left, right)| {
                left.expr.equivalent(&right.expr)
                    && left.order == right.order
                    && left.nulls == right.nulls
            })
}

fn frames_are_equivalent(left: &hir::WindowFrame, right: &hir::WindowFrame) -> bool {
    let bounds_are_equivalent = |left: &hir::WindowFrameBound, right: &hir::WindowFrameBound| {
        use hir::WindowFrameBound::*;
        match (left, right) {
            (CurrentRow, CurrentRow)
            | (UnboundedPreceding, UnboundedPreceding)
            | (UnboundedFollowing, UnboundedFollowing) => true,
            (Preceding(left), Preceding(right)) | (Following(left), Following(right)) => {
                left.equivalent(right)
            }
            _ => false,
        }
    };
    left.mode == right.mode
        && left.exclude == right.exclude
        && bounds_are_equivalent(&left.start, &right.start)
        && match (&left.end, &right.end) {
            (Some(left), Some(right)) => bounds_are_equivalent(left, right),
            (None, None) => true,
            _ => false,
        }
}

enum FunctionContext<'state, 'ast> {
    ScalarOnly,
    Query(&'state mut QueryFunctionState<'ast>),
}

#[derive(Clone, Copy)]
enum ExprOwner {
    Root,
    Query(hir::QueryId),
}

impl ExprOwner {
    const fn query(self) -> Option<hir::QueryId> {
        match self {
            Self::Root => None,
            Self::Query(query) => Some(query),
        }
    }
}

enum FunctionBinding {
    Scalar,
    Aggregate(AggregateBinding),
    Window(WindowBinding),
}

struct AggregateBinding {
    /// The block whose expression holds the call.
    block: hir::QueryBlockId,
}

struct WindowBinding {
    id: hir::WindowFunctionId,
}

#[derive(Clone)]
struct FunctionOrderTerm {
    value: ResolvedScopeExpr,
    order: ast::SortOrder,
    nulls: Option<ast::NullsOrder>,
}

#[derive(Clone)]
struct FunctionWindow {
    partition_by: ExprChildren,
    order_by: Vec<FunctionOrderTerm>,
    frame: Option<FunctionWindowFrame>,
    /// Set when the call names its window (`OVER name`). Calls naming a
    /// window share it only with calls using the same name and frame.
    name: Option<String>,
}

#[derive(Clone)]
struct FunctionWindowFrame {
    mode: ast::FrameMode,
    start: hir::WindowFrameBound,
    end: Option<hir::WindowFrameBound>,
    exclude: Option<ast::FrameExclude>,
}

/// A WINDOW clause definition, with what it inherits from its base window.
/// Like SQLite, its expressions are analyzed where a window function uses
/// it, so they may read the block's aggregates, and a definition no
/// function uses is never analyzed.
#[derive(Clone)]
struct NamedFunctionWindow<'ast> {
    name: String,
    partition_by: &'ast [Box<ast::Expr>],
    order_by: &'ast [ast::SortedColumn],
    frame: Option<&'ast ast::FrameClause>,
}

#[allow(clippy::large_enum_variant)]
enum FunctionOver {
    Named(String),
    Inline {
        base: Option<String>,
        window: FunctionWindow,
    },
}

enum NestedFunction {
    Aggregate(String),
    Window(String),
}

impl FunctionWindow {
    fn into_hir(self, id: hir::WindowId, function: &Func) -> Result<hir::ResolvedWindow> {
        let frame = match function {
            Func::Window(window) => match coerced_window_frame(window) {
                Some(frame) => frame,
                None => effective_window_frame(self.frame, self.order_by.len(), function)?,
            },
            _ => effective_window_frame(self.frame, self.order_by.len(), function)?,
        };
        Ok(hir::ResolvedWindow {
            id,
            partition_by: self
                .partition_by
                .into_iter()
                .map(|value| hir::WindowPartitionTerm {
                    collation: value.collation.value().cloned(),
                    type_fact: value.type_fact,
                    expr: value.expr,
                })
                .collect(),
            order_by: self
                .order_by
                .into_iter()
                .map(FunctionOrderTerm::into_hir)
                .collect(),
            frame,
        })
    }

    fn nested_window(&self) -> Option<NestedFunction> {
        let nested = NestedSearch::WindowsOnly;
        nested_function_iter(&self.partition_by, nested)
            .or_else(|| nested_function_iter(self.order_by.iter().map(|term| &term.value), nested))
            .or_else(|| {
                let frame = self.frame.as_ref()?;
                nested_function_in_window_bound(&frame.start, nested).or_else(|| {
                    frame
                        .end
                        .as_ref()
                        .and_then(|bound| nested_function_in_window_bound(bound, nested))
                })
            })
    }
}

/// SQLite's rules for a window that names a base window: it may only add
/// an ORDER BY the base lacks, and a frame.
fn check_window_chain(
    has_partition: bool,
    both_have_order: bool,
    base_has_frame: bool,
    base_name: &str,
) -> Result<()> {
    if has_partition {
        crate::bail_parse_error!("cannot override PARTITION clause of window: {}", base_name);
    }
    if both_have_order {
        crate::bail_parse_error!("cannot override ORDER BY clause of window: {}", base_name);
    }
    if base_has_frame {
        crate::bail_parse_error!(
            "cannot override frame specification of window: {}",
            base_name
        );
    }
    Ok(())
}

fn chain_function_window(
    mut window: FunctionWindow,
    base: &FunctionWindow,
    base_has_frame: bool,
    base_name: &str,
) -> Result<FunctionWindow> {
    check_window_chain(
        !window.partition_by.is_empty(),
        !base.order_by.is_empty() && !window.order_by.is_empty(),
        base_has_frame,
        base_name,
    )?;
    window.partition_by.clone_from(&base.partition_by);
    if window.order_by.is_empty() {
        window.order_by.clone_from(&base.order_by);
    }
    Ok(window)
}

impl FunctionOrderTerm {
    fn into_hir(self) -> hir::OrderTerm {
        hir::OrderTerm {
            collation: self.value.collation.value().cloned(),
            type_fact: self.value.type_fact,
            expr: self.value.expr,
            order: self.order,
            nulls: self.nulls,
        }
    }
}

#[allow(clippy::large_enum_variant)]
enum FunctionInput {
    Star,
    Expressions {
        distinctness: Option<ast::Distinctness>,
        values: ExprChildren,
        order_by: Vec<FunctionOrderTerm>,
    },
    OrderedSet {
        function: AggFunc,
        direct: ExprChildren,
        order_by: FunctionOrderTerm,
    },
}

impl FunctionInput {
    fn argument_count(&self) -> usize {
        match self {
            Self::Star => 0,
            Self::Expressions { values, .. } => values.len(),
            Self::OrderedSet { direct, .. } => direct.len() + 1,
        }
    }

    fn facts(&self) -> &[ResolvedScopeExpr] {
        match self {
            Self::Star => &[],
            Self::Expressions { values, .. } => values,
            Self::OrderedSet { direct, .. } => direct,
        }
    }

    fn distinctness(&self) -> Option<ast::Distinctness> {
        match self {
            Self::Star => None,
            Self::Expressions { distinctness, .. } => *distinctness,
            Self::OrderedSet { .. } => None,
        }
    }

    fn order_terms(&self) -> &[FunctionOrderTerm] {
        match self {
            Self::Star => &[],
            Self::Expressions { order_by, .. } => order_by,
            Self::OrderedSet { order_by, .. } => std::slice::from_ref(order_by),
        }
    }

    fn result_facts(&self) -> &[ResolvedScopeExpr] {
        match self {
            Self::OrderedSet { order_by, .. } => std::slice::from_ref(&order_by.value),
            _ => self.facts(),
        }
    }

    fn into_hir(self) -> hir::FunctionArguments {
        match self {
            Self::Star => hir::FunctionArguments::Star,
            Self::Expressions {
                distinctness,
                values,
                order_by,
            } => {
                let mut expressions = Vec::with_capacity(values.len());
                let mut facts = Vec::with_capacity(values.len());
                for value in values {
                    expressions.push(value.expr);
                    facts.push(hir::FunctionArgumentFacts {
                        type_fact: value.type_fact,
                        collation: value.collation.value().cloned(),
                    });
                }
                hir::FunctionArguments::Expressions {
                    values: expressions,
                    facts,
                    distinctness,
                    order_by: order_by
                        .into_iter()
                        .map(FunctionOrderTerm::into_hir)
                        .collect(),
                }
            }
            Self::OrderedSet {
                direct, order_by, ..
            } => hir::FunctionArguments::OrderedSet {
                direct: direct.into_iter().map(|value| value.expr).collect(),
                order_by: Box::new(order_by.into_hir()),
            },
        }
    }
}

fn normalize_scalar_input(
    name: &ast::Name,
    function: &Func,
    input: FunctionInput,
    scope: &Scope,
) -> Result<FunctionInput> {
    match input {
        FunctionInput::Expressions {
            values, order_by, ..
        } => {
            if !order_by.is_empty() {
                crate::bail_parse_error!(
                    "ORDER BY may not be used with non-aggregate {}()",
                    name.as_str()
                );
            }
            Ok(FunctionInput::Expressions {
                distinctness: None,
                values,
                order_by: Vec::new(),
            })
        }
        FunctionInput::Star if function.needs_star_expansion() => {
            if !scope.has_sources() {
                crate::bail_parse_error!("{}(*) requires a FROM clause", name.as_str());
            }
            let expanded = scope.expand_star()?;
            let mut values = ExprChildren::with_capacity(expanded.len() * 2);
            for column in expanded {
                values.push(computed_expr(
                    hir::Expr::Literal(ast::Literal::String(format!("'{}'", column.name))),
                    hir::TypeFact::known(Type::Text),
                    ExprCollation::Absent,
                ));
                values.push(column.resolved);
            }
            Ok(FunctionInput::Expressions {
                distinctness: None,
                values,
                order_by: Vec::new(),
            })
        }
        FunctionInput::Star if function.supports_star_syntax() => Ok(FunctionInput::Expressions {
            distinctness: None,
            values: ExprChildren::new(),
            order_by: Vec::new(),
        }),
        FunctionInput::Star => {
            crate::bail_parse_error!("wrong number of arguments to function {}()", name.as_str())
        }
        FunctionInput::OrderedSet { .. } => {
            unreachable!("ordered-set functions are aggregate functions")
        }
    }
}

#[cfg(feature = "json")]
fn validate_json_arguments(function: &JsonFunc, count: usize) -> Result<()> {
    let require_exact = |expected| {
        if count == 0 {
            crate::bail_parse_error!("{} function with no arguments", function);
        }
        if count != expected {
            crate::bail_parse_error!(
                "{} function called with not exactly {} arguments",
                function,
                expected
            );
        }
        Ok(())
    };
    let require_max = |maximum| {
        if count == 0 {
            crate::bail_parse_error!("{} function with no arguments", function);
        }
        if count > maximum {
            crate::bail_parse_error!(
                "{} function called with more than {} arguments",
                function,
                maximum
            );
        }
        Ok(())
    };

    match function {
        JsonFunc::Json | JsonFunc::Jsonb | JsonFunc::JsonQuote => require_exact(1),
        JsonFunc::JsonErrorPosition if count != 1 => {
            crate::bail_parse_error!("{} function with not exactly 1 argument", function)
        }
        JsonFunc::JsonErrorPosition => Ok(()),
        JsonFunc::JsonPatch | JsonFunc::JsonbPatch => require_exact(2),
        JsonFunc::JsonArrayLength
        | JsonFunc::JsonType
        | JsonFunc::JsonPretty
        | JsonFunc::JsonValid => require_max(2),
        JsonFunc::JsonObject | JsonFunc::JsonbObject if count % 2 != 0 => {
            crate::bail_parse_error!("{} function requires an even number of arguments", function)
        }
        _ => Ok(()),
    }
}

fn require_exact_arguments(function: &Func, count: usize, expected: usize) -> Result<()> {
    if count == 0 {
        crate::bail_parse_error!("{} function with no arguments", function);
    }
    if count != expected {
        crate::bail_parse_error!(
            "{} function called with not exactly {} arguments",
            function,
            expected
        );
    }
    Ok(())
}

fn require_at_most_arguments(function: &Func, count: usize, maximum: usize) -> Result<()> {
    if count == 0 {
        crate::bail_parse_error!("{} function with no arguments", function);
    }
    if count > maximum {
        crate::bail_parse_error!(
            "{} function called with more than {} arguments",
            function,
            maximum
        );
    }
    Ok(())
}

fn require_at_least_arguments(function: &Func, count: usize, minimum: usize) -> Result<()> {
    if count == 0 {
        crate::bail_parse_error!("{} function with no arguments", function);
    }
    if count < minimum {
        crate::bail_parse_error!("{} function with less than {} arguments", function, minimum);
    }
    Ok(())
}

fn validate_likelihood_arguments(input: &FunctionInput) -> Result<()> {
    if input.argument_count() != 2 {
        crate::bail_parse_error!("likelihood() function must have exactly 2 arguments");
    }
    let hir::Expr::Literal(ast::Literal::Numeric(value)) = &input.facts()[1].expr else {
        crate::bail_parse_error!(
            "second argument to likelihood() must be a constant between 0.0 and 1.0"
        );
    };
    let Ok(probability) = value.parse::<f64>() else {
        crate::bail_parse_error!(
            "second argument to likelihood() must be a floating point constant"
        );
    };
    if !(0.0..=1.0).contains(&probability) {
        crate::bail_parse_error!(
            "second argument to likelihood() must be a constant between 0.0 and 1.0"
        );
    }
    if !value.contains('.') {
        crate::bail_parse_error!(
            "second argument to likelihood() must be a floating point number with decimal point"
        );
    }
    Ok(())
}

fn validate_scalar_arguments(function: &Func, input: &FunctionInput) -> Result<()> {
    let count = input.argument_count();
    match function {
        Func::Scalar(
            ScalarFunc::Abs
            | ScalarFunc::Lower
            | ScalarFunc::Upper
            | ScalarFunc::Length
            | ScalarFunc::OctetLength
            | ScalarFunc::Typeof
            | ScalarFunc::Unicode
            | ScalarFunc::Unistr
            | ScalarFunc::UnistrQuote
            | ScalarFunc::Quote
            | ScalarFunc::RandomBlob
            | ScalarFunc::Sign
            | ScalarFunc::Soundex
            | ScalarFunc::ZeroBlob
            | ScalarFunc::SequenceWatermark,
        ) => require_exact_arguments(function, count, 1),
        Func::Scalar(ScalarFunc::Hex) if count != 1 => {
            crate::bail_parse_error!("hex function must have exactly 1 argument")
        }
        Func::Scalar(ScalarFunc::Instr) if count != 2 => {
            crate::bail_parse_error!("{} function must have two argument", function)
        }
        Func::Scalar(ScalarFunc::Replace) if count != 3 => {
            crate::bail_parse_error!("wrong number of arguments to function {}()", function)
        }
        Func::Scalar(ScalarFunc::IfNull) if count != 2 => {
            crate::bail_parse_error!("{} function requires exactly 2 arguments", function)
        }
        Func::Scalar(ScalarFunc::Iif | ScalarFunc::Coalesce) => {
            require_at_least_arguments(function, count, 2)
        }
        Func::Scalar(ScalarFunc::Like | ScalarFunc::Glob) if count < 2 => {
            crate::bail_parse_error!("{} function with less than 2 arguments", function)
        }
        Func::Scalar(ScalarFunc::Likely) if count != 1 => {
            crate::bail_parse_error!("likely function must have exactly 1 argument")
        }
        Func::Scalar(ScalarFunc::Unlikely) if count != 1 => {
            crate::bail_parse_error!("Unlikely function must have exactly 1 argument")
        }
        Func::Scalar(ScalarFunc::Likelihood) => validate_likelihood_arguments(input),
        Func::Scalar(ScalarFunc::TableColumnsJsonArray) if count != 1 => {
            crate::bail_parse_error!(
                "table_columns_json_array() function must have exactly 1 argument"
            )
        }
        Func::Scalar(ScalarFunc::BinRecordJsonObject) if count != 2 => {
            crate::bail_parse_error!(
                "bin_record_json_object() function must have exactly 2 arguments"
            )
        }
        Func::Scalar(ScalarFunc::Attach) => {
            crate::bail_parse_error!(
                "ATTACH should be handled at statement level, not as expression"
            )
        }
        Func::Scalar(ScalarFunc::Detach) => {
            crate::bail_parse_error!(
                "DETACH should be handled at statement level, not as expression"
            )
        }
        Func::Scalar(ScalarFunc::StatInit | ScalarFunc::StatPush | ScalarFunc::StatGet) => {
            crate::bail_parse_error!("{} is an internal function used by ANALYZE", function)
        }
        Func::Scalar(ScalarFunc::ConnTxnId | ScalarFunc::IsAutocommit) => {
            crate::bail_parse_error!("{} is an internal function used by CDC", function)
        }
        Func::Scalar(
            ScalarFunc::Trim
            | ScalarFunc::LTrim
            | ScalarFunc::RTrim
            | ScalarFunc::Round
            | ScalarFunc::Unhex,
        ) => require_at_most_arguments(function, count, 2),
        Func::Scalar(ScalarFunc::Changes | ScalarFunc::TotalChanges) if count != 0 => {
            crate::bail_parse_error!("{} function with more than 0 arguments", function)
        }
        Func::Scalar(ScalarFunc::Random) if count != 0 => {
            crate::bail_parse_error!("{} function with arguments", function)
        }
        Func::Scalar(
            ScalarFunc::SqliteVersion | ScalarFunc::TursoVersion | ScalarFunc::SqliteSourceId,
        ) if count != 0 => {
            crate::bail_parse_error!("sqlite_version function with arguments")
        }
        #[cfg(feature = "test_helper")]
        Func::Scalar(ScalarFunc::TestNondetCounter) if count != 0 => {
            crate::bail_parse_error!("{} function with arguments", function)
        }
        #[cfg(all(feature = "fs", not(target_family = "wasm")))]
        Func::Scalar(ScalarFunc::LoadExtension) => require_exact_arguments(function, count, 1),
        #[cfg(feature = "json")]
        Func::Json(function) => validate_json_arguments(function, count),
        _ => Ok(()),
    }
}

struct ExprFrame<'a> {
    syntax: &'a ast::Expr,
    expected_type: Option<Arc<TypeDef>>,
    next_child: usize,
    first_value: usize,
    /// For an IN list: the operand the next child is in, and the child's
    /// position in it. Finding the child by counting operand widths from
    /// the start would read the whole list for every child.
    in_list: OperandCursor,
}

#[derive(Default)]
struct OperandCursor {
    operand: usize,
    component: usize,
}

struct ExprChild<'a> {
    syntax: &'a ast::Expr,
    expected_type: Option<Arc<TypeDef>>,
}

impl<'a> ExprFrame<'a> {
    fn new(
        syntax: &'a ast::Expr,
        expected_type: Option<Arc<TypeDef>>,
        first_value: usize,
    ) -> Result<Self> {
        validate_special_function_syntax(syntax)?;
        Ok(Self {
            syntax,
            expected_type,
            next_child: 0,
            first_value,
            in_list: OperandCursor::default(),
        })
    }

    fn next_child(&mut self, schema: &Schema) -> Result<Option<ExprChild<'a>>> {
        let child_index = self.next_child;
        let child = match self.syntax {
            ast::Expr::Parenthesized(expressions) if expressions.len() == 1 => {
                (self.next_child == 0).then(|| expressions[0].as_ref())
            }
            ast::Expr::Unary(_, expression)
            | ast::Expr::Collate(expression, _)
            | ast::Expr::IsNull(expression)
            | ast::Expr::NotNull(expression)
            | ast::Expr::FieldAccess {
                base: expression, ..
            } => (self.next_child == 0).then(|| expression.as_ref()),
            ast::Expr::Binary(lhs, _, rhs) => {
                expression_operand_child([lhs.as_ref(), rhs.as_ref()], self.next_child)
            }
            ast::Expr::Array { elements } => elements.get(self.next_child).map(Box::as_ref),
            ast::Expr::Subscript { base, index } => match self.next_child {
                0 => Some(base.as_ref()),
                1 => Some(index.as_ref()),
                _ => None,
            },
            ast::Expr::Between {
                lhs, start, end, ..
            } => expression_operand_child(
                [lhs.as_ref(), start.as_ref(), end.as_ref()],
                self.next_child,
            ),
            ast::Expr::InList { lhs, rhs, .. } => {
                in_list_operand_child(lhs, rhs, &mut self.in_list)
            }
            ast::Expr::InSelect { lhs, .. } | ast::Expr::InTable { lhs, .. } => {
                expression_operand_child([lhs.as_ref()], self.next_child)
            }
            ast::Expr::Like {
                lhs, rhs, escape, ..
            } => {
                let lhs_count = expression_operand_width(lhs);
                if self.next_child < lhs_count {
                    expression_operand_child([lhs.as_ref()], self.next_child)
                } else if self.next_child == lhs_count {
                    Some(rhs.as_ref())
                } else if self.next_child == lhs_count + 1 {
                    escape.as_deref()
                } else {
                    None
                }
            }
            ast::Expr::Case {
                base,
                when_then_pairs,
                else_expr,
            } => match (base.as_deref(), self.next_child) {
                (Some(base), 0) => Some(base),
                _ => {
                    let index = self.next_child - usize::from(base.is_some());
                    match when_then_pairs.get(index / 2) {
                        Some((when, _)) if index % 2 == 0 => Some(when.as_ref()),
                        Some((_, then)) => Some(then.as_ref()),
                        None if index == when_then_pairs.len() * 2 => else_expr.as_deref(),
                        None => None,
                    }
                }
            },
            ast::Expr::Cast { expr, type_name } => {
                match type_name
                    .as_ref()
                    .and_then(|type_name| type_name.size.as_ref())
                {
                    Some(ast::TypeSize::MaxSize(parameter)) => match self.next_child {
                        0 => Some(parameter.as_ref()),
                        1 => Some(expr.as_ref()),
                        _ => None,
                    },
                    Some(ast::TypeSize::TypeSize(first, second)) => match self.next_child {
                        0 => Some(first.as_ref()),
                        1 => Some(second.as_ref()),
                        2 => Some(expr.as_ref()),
                        _ => None,
                    },
                    None => (self.next_child == 0).then(|| expr.as_ref()),
                }
            }
            ast::Expr::FunctionCall {
                name,
                args,
                order_by,
                within_group,
                filter_over,
                ..
            } => {
                let argument_order_end = args.len() + order_by.len();
                let within_group_end = argument_order_end + within_group.len();
                let filter_end =
                    within_group_end + usize::from(filter_over.filter_clause.is_some());
                if self.next_child < args.len() {
                    args.get(self.next_child).map(Box::as_ref)
                } else if self.next_child < argument_order_end {
                    order_by
                        .get(self.next_child - args.len())
                        .map(|term| term.expr.as_ref())
                } else if self.next_child < within_group_end {
                    within_group
                        .get(self.next_child - argument_order_end)
                        .map(|term| term.expr.as_ref())
                } else if self.next_child == within_group_end && filter_over.filter_clause.is_some()
                {
                    filter_over.filter_clause.as_deref()
                } else {
                    window_child(
                        name,
                        args.len(),
                        filter_over.over_clause.as_deref(),
                        self.next_child - filter_end,
                    )
                }
            }
            ast::Expr::FunctionCallStar { name, filter_over } => {
                let filter_end = usize::from(filter_over.filter_clause.is_some());
                if self.next_child == 0 && filter_end == 1 {
                    filter_over.filter_clause.as_deref()
                } else {
                    window_child(
                        name,
                        0,
                        filter_over.over_clause.as_deref(),
                        self.next_child - filter_end,
                    )
                }
            }
            ast::Expr::Raise(_, message) => (self.next_child == 0)
                .then_some(message.as_deref())
                .flatten(),
            _ => None,
        };
        if child.is_some() {
            self.next_child += 1;
        }
        let Some(syntax) = child else {
            return Ok(None);
        };
        let expected_type = self.child_expected_type(child_index, schema)?;
        Ok(Some(ExprChild {
            syntax,
            expected_type,
        }))
    }

    fn child_expected_type(
        &self,
        child_index: usize,
        schema: &Schema,
    ) -> Result<Option<Arc<TypeDef>>> {
        let ast::Expr::FunctionCall { name, args, .. } = self.syntax else {
            return Ok(self.expected_type.clone());
        };
        if normalize_ident(name.as_str()) != "union_value" {
            return Ok(self.expected_type.clone());
        }
        let union = self
            .expected_type
            .as_ref()
            .filter(|definition| definition.is_union())
            .ok_or_else(|| {
                LimboError::ParseError(
                    "union_value() can only be used in INSERT/UPDATE targeting a union-typed column"
                        .to_string(),
                )
            })?;
        let ast::Expr::Literal(ast::Literal::String(tag_name)) = args[0].as_ref() else {
            unreachable!("union_value syntax validation requires a string tag")
        };
        let tag_name = tag_name.trim_matches('\'');
        let (_, variant) = union.find_union_variant(tag_name).ok_or_else(|| {
            LimboError::ParseError(format!(
                "unknown variant '{}' in union type '{}'",
                tag_name, union.name
            ))
        })?;
        match child_index {
            0 => Ok(None),
            1 => Ok(schema.get_type_def_unchecked(&variant.type_name).cloned()),
            _ => unreachable!("union_value has exactly two arguments"),
        }
    }
}

fn expression_operand_width(syntax: &ast::Expr) -> usize {
    #[cfg(test)]
    tests::OPERAND_WIDTH_CALLS.with(|calls| calls.set(calls.get() + 1));
    match syntax {
        ast::Expr::Parenthesized(expressions) if expressions.len() == 1 => {
            expression_operand_width(&expressions[0])
        }
        ast::Expr::Parenthesized(expressions) => expressions.len(),
        _ => 1,
    }
}

fn expression_operand_component(syntax: &ast::Expr, index: usize) -> Option<&ast::Expr> {
    match syntax {
        ast::Expr::Parenthesized(expressions) if expressions.len() == 1 => {
            expression_operand_component(&expressions[0], index)
        }
        ast::Expr::Parenthesized(expressions) => expressions.get(index).map(Box::as_ref),
        _ => (index == 0).then_some(syntax),
    }
}

/// The child at `cursor` among `lhs` and the elements of `rhs`, moving
/// the cursor past it: the children of each operand in turn.
fn in_list_operand_child<'a>(
    lhs: &'a ast::Expr,
    rhs: &'a [Box<ast::Expr>],
    cursor: &mut OperandCursor,
) -> Option<&'a ast::Expr> {
    loop {
        let syntax = match cursor.operand {
            0 => lhs,
            position => rhs.get(position - 1)?.as_ref(),
        };
        let width = expression_operand_width(syntax);
        if cursor.component < width {
            let child = expression_operand_component(syntax, cursor.component);
            cursor.component += 1;
            if cursor.component == width {
                cursor.operand += 1;
                cursor.component = 0;
            }
            return child;
        }
        cursor.operand += 1;
        cursor.component = 0;
    }
}

fn expression_operand_child<'a>(
    syntaxes: impl IntoIterator<Item = &'a ast::Expr>,
    mut index: usize,
) -> Option<&'a ast::Expr> {
    for syntax in syntaxes {
        let width = expression_operand_width(syntax);
        if index < width {
            return expression_operand_component(syntax, index);
        }
        index -= width;
    }
    None
}

fn take_expression_operands<'a>(
    syntaxes: impl IntoIterator<Item = &'a ast::Expr>,
    children: FrameChildren,
) -> Result<SmallVec<[ExprOperand; 3]>> {
    let syntaxes = syntaxes.into_iter().collect::<SmallVec<[_; 3]>>();
    let expected = syntaxes
        .iter()
        .map(|syntax| expression_operand_width(syntax))
        .sum::<usize>();
    if expected != children.len() {
        return Err(LimboError::InternalError(format!(
            "expression operands expected {expected} child values, got {}",
            children.len()
        )));
    }
    if expected == 0 {
        crate::bail_parse_error!("row value misused");
    }

    let mut children = children.into_iter();
    let mut operands = SmallVec::with_capacity(syntaxes.len());
    for syntax in syntaxes {
        let width = expression_operand_width(syntax);
        if width == 1 {
            let child = children.next().expect("operand contains one frame value");
            operands.push(child.into_operand());
            continue;
        }
        let components = children
            .by_ref()
            .take(width)
            .map(FrameValue::into_scalar)
            .collect::<Result<ExprChildren>>()?;
        operands.push(ExprOperand::RowValue(Box::new(components)));
    }
    Ok(operands)
}

fn build_in_list_expr(
    lhs_syntax: &ast::Expr,
    negated: bool,
    rhs_syntax: &[Box<ast::Expr>],
    children: FrameChildren,
) -> Result<ResolvedScopeExpr> {
    let operands = take_expression_operands(
        std::iter::once(lhs_syntax).chain(rhs_syntax.iter().map(Box::as_ref)),
        children,
    )?;
    let (lhs, values) = operands.split_first().expect("IN has a left operand");
    let lhs_width = lhs.components().len();
    for value in values {
        let width = value.components().len();
        if lhs_width != width {
            if lhs_width == 1 {
                crate::bail_parse_error!("row value misused");
            }
            crate::bail_parse_error!(
                "IN(...) element has {width} term{} - expected {lhs_width}",
                if width == 1 { "" } else { "s" },
            );
        }
    }
    let comparisons = values
        .iter()
        .map(|value| in_operand_comparison(lhs.components(), value.components()))
        .collect();
    let mut operands = operands.into_iter().map(ExprOperand::into_expr);
    let lhs = operands.next().expect("IN has a left operand");
    Ok(computed_expr(
        hir::Expr::InList {
            lhs: Box::new(lhs),
            negated,
            values: operands.collect(),
            comparisons,
        },
        hir::TypeFact::known(Type::Integer),
        ExprCollation::Absent,
    ))
}

fn scalarize_frame_values(children: &mut FrameChildren, row_values: RowValueChecks) -> Result<()> {
    for child in children.iter_mut() {
        let FrameValue::RowSubquery { components, .. } = child else {
            continue;
        };
        if row_values != RowValueChecks::None {
            crate::bail_parse_error!(
                "sub-select returns {} columns - expected 1",
                components.len()
            );
        }
        let value = components.swap_remove(0);
        *child = FrameValue::Scalar(value);
    }
    Ok(())
}

fn take_scalar_operand(operand: ExprOperand) -> Result<ResolvedScopeExpr> {
    match operand {
        ExprOperand::Scalar(value) => Ok(value),
        ExprOperand::RowSubquery { mut components, .. } | ExprOperand::RowValue(mut components)
            if components.len() == 1 =>
        {
            Ok(components.pop().expect("scalar operand contains one value"))
        }
        _ => crate::bail_parse_error!("row value misused"),
    }
}

/// The first explicit COLLATE among `facts`, if any: the collation an
/// operator or call over them carries.
fn explicit_collation<'a>(facts: impl IntoIterator<Item = &'a ResolvedScopeExpr>) -> ExprCollation {
    facts
        .into_iter()
        .find_map(|fact| match &fact.collation {
            ExprCollation::Explicit(collation) => Some(ExprCollation::Explicit(collation.clone())),
            _ => None,
        })
        .unwrap_or(ExprCollation::Absent)
}

fn expression_operands_collation<'a>(
    operands: impl IntoIterator<Item = &'a ExprOperand>,
) -> ExprCollation {
    operands
        .into_iter()
        .flat_map(|operand| operand.components())
        .fold(ExprCollation::Absent, |current, value| {
            expression_collation(&current, &value.collation)
        })
}

fn window_child<'a>(
    name: &ast::Name,
    argument_count: usize,
    over: Option<&'a ast::Over>,
    index: usize,
) -> Option<&'a ast::Expr> {
    let ast::Over::Window(window) = over? else {
        return None;
    };
    if let Some(expression) = window.partition_by.get(index) {
        return Some(expression.as_ref());
    }
    let index = index - window.partition_by.len();
    if let Some(term) = window.order_by.get(index) {
        return Some(term.expr.as_ref());
    }
    if window_function_ignores_frame(name, argument_count) {
        return None;
    }
    let index = index - window.order_by.len();
    let frame = window.frame_clause.as_ref()?;
    [
        frame_bound_expr(&frame.start),
        frame.end.as_ref().and_then(frame_bound_expr),
    ]
    .into_iter()
    .flatten()
    .nth(index)
}

fn frame_bound_expr(bound: &ast::FrameBound) -> Option<&ast::Expr> {
    match bound {
        ast::FrameBound::Following(expression) | ast::FrameBound::Preceding(expression) => {
            Some(expression.as_ref())
        }
        ast::FrameBound::CurrentRow
        | ast::FrameBound::UnboundedFollowing
        | ast::FrameBound::UnboundedPreceding => None,
    }
}

fn window_function_ignores_frame(name: &ast::Name, argument_count: usize) -> bool {
    matches!(
        (normalize_ident(name.as_str()).as_str(), argument_count),
        (
            "row_number" | "rank" | "dense_rank" | "percent_rank" | "cume_dist",
            0
        ) | ("ntile", 1)
            | ("lag" | "lead", 1..=3)
    )
}

fn function_ignores_user_frame(function: &Func) -> bool {
    matches!(function, Func::Window(window) if coerced_window_frame(window).is_some())
}

fn window_expression_count(
    name: &ast::Name,
    argument_count: usize,
    over: Option<&ast::Over>,
) -> usize {
    let Some(ast::Over::Window(window)) = over else {
        return 0;
    };
    let frame_offsets = if window_function_ignores_frame(name, argument_count) {
        0
    } else {
        window.frame_clause.as_ref().map_or(0, |frame| {
            usize::from(frame_bound_expr(&frame.start).is_some())
                + usize::from(frame.end.as_ref().and_then(frame_bound_expr).is_some())
        })
    };
    window.partition_by.len() + window.order_by.len() + frame_offsets
}

fn take_function_window(
    name: &ast::Name,
    argument_count: usize,
    over: Option<&ast::Over>,
    children: &mut impl Iterator<Item = ResolvedScopeExpr>,
) -> Result<Option<FunctionOver>> {
    let Some(over) = over else {
        return Ok(None);
    };
    let ast::Over::Window(window) = over else {
        let ast::Over::Name(name) = over else {
            unreachable!("OVER has only named and inline forms")
        };
        return Ok(Some(FunctionOver::Named(normalize_ident(name.as_str()))));
    };
    let partition_by = children.take(window.partition_by.len()).collect();
    let order_by = window
        .order_by
        .iter()
        .map(|syntax| FunctionOrderTerm {
            value: children
                .next()
                .expect("window child count includes ORDER BY expression"),
            order: syntax.order.unwrap_or(ast::SortOrder::Asc),
            nulls: syntax.nulls,
        })
        .collect();
    let frame = if window_function_ignores_frame(name, argument_count) {
        None
    } else {
        window
            .frame_clause
            .as_ref()
            .map(|frame| take_function_window_frame(frame, children))
            .transpose()?
    };
    Ok(Some(FunctionOver::Inline {
        base: window
            .base
            .as_ref()
            .map(|name| normalize_ident(name.as_str())),
        window: FunctionWindow {
            partition_by,
            order_by,
            frame,
            name: None,
        },
    }))
}

fn take_function_window_frame(
    frame: &ast::FrameClause,
    children: &mut impl Iterator<Item = ResolvedScopeExpr>,
) -> Result<FunctionWindowFrame> {
    Ok(FunctionWindowFrame {
        mode: frame.mode,
        start: take_window_frame_bound(&frame.start, children),
        end: frame
            .end
            .as_ref()
            .map(|bound| take_window_frame_bound(bound, children)),
        exclude: frame.exclude.clone(),
    })
}

fn take_window_frame_bound(
    bound: &ast::FrameBound,
    children: &mut impl Iterator<Item = ResolvedScopeExpr>,
) -> hir::WindowFrameBound {
    match bound {
        ast::FrameBound::CurrentRow => hir::WindowFrameBound::CurrentRow,
        ast::FrameBound::Following(_) => hir::WindowFrameBound::Following(Box::new(
            children
                .next()
                .expect("window child count includes FOLLOWING offset")
                .expr,
        )),
        ast::FrameBound::Preceding(_) => hir::WindowFrameBound::Preceding(Box::new(
            children
                .next()
                .expect("window child count includes PRECEDING offset")
                .expr,
        )),
        ast::FrameBound::UnboundedFollowing => hir::WindowFrameBound::UnboundedFollowing,
        ast::FrameBound::UnboundedPreceding => hir::WindowFrameBound::UnboundedPreceding,
    }
}

impl<'context, 'catalog, 'ast> Analyzer<'context, 'catalog, 'ast> {
    pub(crate) fn analyze_expr(
        &mut self,
        syntax: &ast::Expr,
        scope: &Scope,
        policy: ExprPolicy,
    ) -> Result<hir::Expr> {
        Ok(self.analyze_resolved_expr(syntax, scope, policy)?.expr)
    }

    pub(super) fn analyze_expr_with_expected_type(
        &mut self,
        syntax: &ast::Expr,
        scope: &Scope,
        policy: ExprPolicy,
        expected_type: Option<Arc<TypeDef>>,
    ) -> Result<hir::Expr> {
        Ok(self
            .analyze_resolved_expr_with_functions(
                syntax,
                scope,
                policy,
                &mut FunctionContext::ScalarOnly,
                expected_type,
            )?
            .expr)
    }

    pub(super) fn analyze_resolved_expr(
        &mut self,
        syntax: &ast::Expr,
        scope: &Scope,
        policy: ExprPolicy,
    ) -> Result<ResolvedScopeExpr> {
        self.analyze_resolved_expr_with_functions(
            syntax,
            scope,
            policy,
            &mut FunctionContext::ScalarOnly,
            None,
        )
    }

    pub(super) fn analyze_query_expr(
        &mut self,
        syntax: &'ast ast::Expr,
        scope: &Scope,
        policy: ExprPolicy,
        functions: &mut QueryFunctionState<'ast>,
    ) -> Result<ResolvedScopeExpr> {
        self.analyze_query_expr_with_expected_type(syntax, scope, policy, functions, None)
    }

    pub(super) fn analyze_query_expr_with_expected_type(
        &mut self,
        syntax: &'ast ast::Expr,
        scope: &Scope,
        policy: ExprPolicy,
        functions: &mut QueryFunctionState<'ast>,
        expected_type: Option<Arc<TypeDef>>,
    ) -> Result<ResolvedScopeExpr> {
        let owner = ExprOwner::Query(functions.block.query);
        let mut functions = FunctionContext::Query(functions);
        self.analyze_owned_expr(syntax, scope, policy, owner, &mut functions, expected_type)
    }

    pub(super) fn analyze_query_scalar_expr(
        &mut self,
        syntax: &'ast ast::Expr,
        scope: &Scope,
        policy: ExprPolicy,
        parent: hir::QueryId,
    ) -> Result<ResolvedScopeExpr> {
        self.analyze_owned_expr(
            syntax,
            scope,
            policy,
            ExprOwner::Query(parent),
            &mut FunctionContext::ScalarOnly,
            None,
        )
    }

    pub(super) fn analyze_root_expr(
        &mut self,
        syntax: &'ast ast::Expr,
        scope: &Scope,
        policy: ExprPolicy,
    ) -> Result<ResolvedScopeExpr> {
        self.analyze_root_expr_with_expected_type(syntax, scope, policy, None)
    }

    pub(super) fn analyze_root_expr_with_expected_type(
        &mut self,
        syntax: &'ast ast::Expr,
        scope: &Scope,
        policy: ExprPolicy,
        expected_type: Option<Arc<TypeDef>>,
    ) -> Result<ResolvedScopeExpr> {
        self.analyze_owned_expr(
            syntax,
            scope,
            policy,
            ExprOwner::Root,
            &mut FunctionContext::ScalarOnly,
            expected_type,
        )
    }

    fn analyze_owned_expr(
        &mut self,
        syntax: &'ast ast::Expr,
        scope: &Scope,
        policy: ExprPolicy,
        owner: ExprOwner,
        functions: &mut FunctionContext<'_, 'ast>,
        expected_type: Option<Arc<TypeDef>>,
    ) -> Result<ResolvedScopeExpr> {
        let mut frames = vec![ExprFrame::new(syntax, expected_type, 0)?];
        let mut values = Vec::<FrameValue>::new();
        loop {
            if let Some(child) = frames
                .last_mut()
                .expect("root expression frame exists")
                .next_child(self.context().main_schema())?
            {
                frames.push(ExprFrame::new(
                    child.syntax,
                    child.expected_type,
                    values.len(),
                )?);
                continue;
            }

            let frame = frames.pop().expect("completed expression frame exists");
            let children = values.drain(frame.first_value..).collect::<FrameChildren>();
            let resolved = match frame.syntax {
                ast::Expr::Subquery(_)
                | ast::Expr::Exists(_)
                | ast::Expr::InSelect { .. }
                | ast::Expr::InTable { .. } => {
                    // An aggregate in the subquery of only this block's
                    // columns belongs to this block, where aggregates are
                    // allowed here.
                    let host = match (&functions, policy.aggregates) {
                        (FunctionContext::Query(state), AggregatePolicy::Allow) => {
                            Some(state.block)
                        }
                        _ => None,
                    };
                    self.aggregate_hosts.push(host);
                    let value = self.build_subquery_expr(
                        frame.syntax,
                        children,
                        scope,
                        owner,
                        policy.policies,
                    );
                    self.aggregate_hosts.pop();
                    value?
                }
                _ => FrameValue::Scalar(self.build_expr_from_frames(
                    frame.syntax,
                    children,
                    scope,
                    policy,
                    functions,
                    frame.expected_type.as_ref(),
                )?),
            };
            match frames.last_mut() {
                Some(_) => values.push(resolved),
                None if policy.policies.row_values == RowValueChecks::None => {
                    return Ok(resolved.into_any_value());
                }
                None => return resolved.into_scalar(),
            }
        }
    }

    /// Whether an aggregate written in `block` reads columns of other query
    /// blocks only, so that, as in SQLite, it is an aggregate of the
    /// enclosing query: a misuse where that query allows no aggregates. An
    /// aggregate that reads any column of `block`, in its arguments, their
    /// ORDER BY or its FILTER, belongs to `block`.
    fn aggregate_reads_outside(
        &self,
        block: hir::QueryBlockId,
        input: &FunctionInput,
        filter: Option<&ResolvedScopeExpr>,
    ) -> bool {
        let mut reads_block = false;
        let mut reads_outside = false;
        let mut visit = |expression: &hir::Expr| {
            expression.for_each(&mut |expression| {
                let source = match expression {
                    hir::Expr::Column(column) => column.source,
                    hir::Expr::RowId(source) => *source,
                    _ => return,
                };
                match self.source(source).map(|source| source.owner) {
                    Some(hir::SourceOwner::QueryBlock(owner)) if owner == block => {
                        reads_block = true
                    }
                    _ => reads_outside = true,
                }
            });
        };
        for value in input.facts() {
            visit(&value.expr);
        }
        for term in input.order_terms() {
            visit(&term.value.expr);
        }
        if let Some(filter) = filter {
            visit(&filter.expr);
        }
        reads_outside && !reads_block
    }

    /// The block an aggregate call belongs to. Like SQLite, a call in a
    /// subquery whose arguments read only columns of the enclosing block
    /// is an aggregate of that block, which then runs as an aggregate query;
    /// otherwise it belongs to `block`, the block it is written in.
    fn aggregate_owner(
        &self,
        block: hir::QueryBlockId,
        input: &FunctionInput,
        filter: Option<&ResolvedScopeExpr>,
    ) -> hir::QueryBlockId {
        let Some(Some(host)) = self.aggregate_hosts.last().copied() else {
            return block;
        };
        if host == block {
            return block;
        }
        let mut reads_host = false;
        let mut reads_other = false;
        let mut visit = |expression: &hir::Expr| {
            expression.for_each(&mut |expression| {
                let source = match expression {
                    hir::Expr::Column(column) => column.source,
                    hir::Expr::RowId(source) => *source,
                    hir::Expr::Subquery(_) => {
                        reads_other = true;
                        return;
                    }
                    _ => return,
                };
                match self.source(source).map(|source| source.owner) {
                    Some(hir::SourceOwner::QueryBlock(owner)) if owner == host => reads_host = true,
                    _ => reads_other = true,
                }
            });
        };
        for value in input.facts() {
            visit(&value.expr);
        }
        for term in input.order_terms() {
            visit(&term.value.expr);
        }
        if let Some(filter) = filter {
            visit(&filter.expr);
        }
        if reads_host && !reads_other {
            host
        } else {
            block
        }
    }

    fn build_subquery_expr(
        &mut self,
        syntax: &'ast ast::Expr,
        children: FrameChildren,
        scope: &Scope,
        owner: ExprOwner,
        policies: ExprPolicies,
    ) -> Result<FrameValue> {
        match syntax {
            ast::Expr::Subquery(select) => {
                expect_no_frame_children(children)?;
                let query = self.analyze_subquery(select, owner.query(), scope, policies)?;
                self.resolve_query_value(query)
            }
            ast::Expr::Exists(select) => {
                expect_no_frame_children(children)?;
                let query = self.analyze_subquery(select, owner.query(), scope, policies)?;
                Ok(FrameValue::Scalar(computed_expr(
                    hir::Expr::Subquery(hir::SubqueryExpr::Exists(query)),
                    hir::TypeFact::known(Type::Integer),
                    ExprCollation::Absent,
                )))
            }
            ast::Expr::InSelect {
                not, rhs: select, ..
            } => self
                .build_in_subquery(
                    scalar_frame_children(children)?,
                    select,
                    *not,
                    scope,
                    owner,
                    policies,
                )
                .map(FrameValue::Scalar),
            ast::Expr::InTable { not, rhs, args, .. } => {
                let query =
                    self.analyze_named_relation_query(rhs, args, owner.query(), scope, policies)?;
                self.build_in_query(
                    scalar_frame_children(children)?,
                    query,
                    *not,
                    policies.row_values,
                )
                .map(FrameValue::Scalar)
            }
            _ => Err(LimboError::InternalError(
                "subquery expression builder received a non-subquery".to_string(),
            )),
        }
    }

    fn build_in_subquery(
        &mut self,
        lhs: ExprChildren,
        select: &'ast ast::Select,
        negated: bool,
        scope: &Scope,
        owner: ExprOwner,
        policies: ExprPolicies,
    ) -> Result<ResolvedScopeExpr> {
        let query = self.analyze_subquery(select, owner.query(), scope, policies)?;
        self.build_in_query(lhs, query, negated, policies.row_values)
    }

    fn build_in_query(
        &self,
        lhs: ExprChildren,
        query: hir::QueryId,
        negated: bool,
        row_values: RowValueChecks,
    ) -> Result<ResolvedScopeExpr> {
        if lhs.is_empty() {
            return Err(LimboError::InternalError(
                "IN query expression has no left value".to_string(),
            ));
        }
        let output_width = self
            .query(query)
            .ok_or_else(|| LimboError::InternalError(format!("missing semantic query {query}")))?
            .output
            .len();
        // SQLite checks the widths only while generating code.
        if output_width != lhs.len() && row_values != RowValueChecks::All {
            return Ok(lhs.into_iter().next().expect("IN query has a left value"));
        }
        if output_width != lhs.len() {
            crate::bail_parse_error!(
                "sub-select returns {output_width} columns - expected {}",
                lhs.len()
            );
        }
        let rhs = (0..output_width)
            .map(|output| self.query_output_fact(query, output))
            .collect::<Result<Vec<_>>>()?;
        let comparison = hir::ComparisonSemantics {
            components: lhs
                .iter()
                .zip(&rhs)
                .map(|(lhs, rhs)| comparison_component(lhs, rhs))
                .collect(),
        };
        let collation = ExprCollation::Absent;
        let lhs = if lhs.len() == 1 {
            lhs.into_iter()
                .next()
                .expect("IN query left side has one value")
                .expr
        } else {
            hir::Expr::Row(lhs.into_iter().map(|value| value.expr).collect())
        };
        Ok(computed_expr(
            hir::Expr::Subquery(hir::SubqueryExpr::In {
                lhs: Box::new(lhs),
                query,
                negated,
                comparison,
            }),
            hir::TypeFact::known(Type::Integer),
            collation,
        ))
    }

    fn resolve_query_output(
        &self,
        query: hir::QueryId,
        output: usize,
    ) -> Result<ResolvedScopeExpr> {
        let definition = self
            .query(query)
            .ok_or_else(|| LimboError::InternalError(format!("missing semantic query {query}")))?;
        let width = definition.output.len();
        if width != 1 {
            crate::bail_parse_error!("sub-select returns {width} columns - expected 1");
        }
        let mut resolved = self.query_output_fact(query, output)?;
        resolved.expr = hir::Expr::Subquery(hir::SubqueryExpr::Scalar { query, output });
        Ok(resolved)
    }

    fn resolve_query_value(&self, query: hir::QueryId) -> Result<FrameValue> {
        let width = self
            .query(query)
            .ok_or_else(|| LimboError::InternalError(format!("missing semantic query {query}")))?
            .output
            .len();
        if width == 1 {
            return self.resolve_query_output(query, 0).map(FrameValue::Scalar);
        }
        let components = (0..width)
            .map(|output| self.query_output_fact(query, output))
            .collect::<Result<ExprChildren>>()?;
        Ok(FrameValue::RowSubquery {
            query,
            components: Box::new(components),
        })
    }

    fn query_output_fact(&self, query: hir::QueryId, output: usize) -> Result<ResolvedScopeExpr> {
        let definition = self
            .query(query)
            .ok_or_else(|| LimboError::InternalError(format!("missing semantic query {query}")))?;
        let facts = definition
            .blocks
            .first()
            .and_then(|block| block.outputs.get(output))
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "semantic query {} has no output {output}",
                    definition.id
                ))
            })?;
        Ok(ResolvedScopeExpr {
            expr: hir::Expr::Output(facts.id),
            type_fact: facts.type_fact.clone(),
            affinity: facts.affinity,
            has_affinity: facts.has_affinity,
            collation: ExprCollation::output(facts.collation.clone(), facts.collation_is_explicit),
        })
    }

    pub(super) fn analyze_named_windows(
        &mut self,
        definitions: &'ast [ast::WindowDef],
        functions: &mut QueryFunctionState<'ast>,
    ) -> Result<()> {
        for definition in definitions {
            let window = &definition.window;
            let mut named = NamedFunctionWindow {
                name: normalize_ident(definition.name.as_str()),
                partition_by: &window.partition_by,
                order_by: &window.order_by,
                frame: window.frame_clause.as_ref(),
            };
            if let Some(base) = &window.base {
                // SQLite ignores the base on the first WINDOW definition.
                if !functions.named_windows.is_empty() {
                    let base = normalize_ident(base.as_str());
                    let inherited = functions
                        .named_windows
                        .iter()
                        .rfind(|window| window.name == base)
                        .ok_or_else(|| LimboError::ParseError(format!("no such window: {base}")))?;
                    check_window_chain(
                        !named.partition_by.is_empty(),
                        !inherited.order_by.is_empty() && !named.order_by.is_empty(),
                        inherited.frame.is_some(),
                        &base,
                    )?;
                    named.partition_by = inherited.partition_by;
                    if named.order_by.is_empty() {
                        named.order_by = inherited.order_by;
                    }
                }
            }
            functions.named_windows.push(named);
        }
        Ok(())
    }

    /// The PARTITION BY and ORDER BY of a named window, analyzed where a
    /// window function uses it. They are analyzed as expressions of the
    /// block's query, so they may hold subqueries.
    fn analyze_named_window(
        &mut self,
        name: &str,
        scope: &Scope,
        policy: ExprPolicy,
        functions: &mut QueryFunctionState<'ast>,
    ) -> Result<(FunctionWindow, NamedFunctionWindow<'ast>)> {
        let named = functions
            .named_windows
            .iter()
            .rfind(|window| window.name == name)
            .cloned()
            .ok_or_else(|| LimboError::ParseError(format!("no such window: {name}")))?;
        let policy = policy.without_windows();
        let mut partition_by = ExprChildren::new();
        for expression in named.partition_by {
            partition_by.push(self.analyze_query_expr(expression, scope, policy, functions)?);
        }
        let mut order_by = Vec::with_capacity(named.order_by.len());
        for term in named.order_by {
            order_by.push(FunctionOrderTerm {
                value: self.analyze_query_expr(&term.expr, scope, policy, functions)?,
                order: term.order.unwrap_or(ast::SortOrder::Asc),
                nulls: term.nulls,
            });
        }
        let window = FunctionWindow {
            partition_by,
            order_by,
            frame: None,
            name: None,
        };
        Ok((window, named))
    }

    fn analyze_window_frame(
        &mut self,
        frame: &ast::FrameClause,
        scope: &Scope,
        policy: ExprPolicy,
    ) -> Result<FunctionWindowFrame> {
        Ok(FunctionWindowFrame {
            mode: frame.mode,
            start: self.analyze_window_bound(&frame.start, scope, policy)?,
            end: frame
                .end
                .as_ref()
                .map(|bound| self.analyze_window_bound(bound, scope, policy))
                .transpose()?,
            exclude: frame.exclude.clone(),
        })
    }

    fn analyze_window_bound(
        &mut self,
        bound: &ast::FrameBound,
        scope: &Scope,
        policy: ExprPolicy,
    ) -> Result<hir::WindowFrameBound> {
        Ok(match bound {
            ast::FrameBound::CurrentRow => hir::WindowFrameBound::CurrentRow,
            ast::FrameBound::Following(expression) => hir::WindowFrameBound::Following(Box::new(
                self.analyze_resolved_expr(expression, scope, policy)?.expr,
            )),
            ast::FrameBound::Preceding(expression) => hir::WindowFrameBound::Preceding(Box::new(
                self.analyze_resolved_expr(expression, scope, policy)?.expr,
            )),
            ast::FrameBound::UnboundedFollowing => hir::WindowFrameBound::UnboundedFollowing,
            ast::FrameBound::UnboundedPreceding => hir::WindowFrameBound::UnboundedPreceding,
        })
    }

    fn resolve_function_over(
        &mut self,
        over: FunctionOver,
        function: &Func,
        scope: &Scope,
        policy: ExprPolicy,
        functions: &mut QueryFunctionState<'ast>,
    ) -> Result<FunctionWindow> {
        match over {
            FunctionOver::Named(name) => {
                let (mut window, named) =
                    self.analyze_named_window(&name, scope, policy, functions)?;
                window.name = Some(name);
                if !function_ignores_user_frame(function) {
                    window.frame = named
                        .frame
                        .map(|frame| self.analyze_window_frame(frame, scope, policy))
                        .transpose()?;
                }
                Ok(window)
            }
            FunctionOver::Inline { base: None, window } => Ok(window),
            FunctionOver::Inline {
                base: Some(base),
                window,
            } => {
                let (base_window, named) =
                    self.analyze_named_window(&base, scope, policy, functions)?;
                chain_function_window(window, &base_window, named.frame.is_some(), &base)
            }
        }
    }

    fn analyze_resolved_expr_with_functions(
        &mut self,
        syntax: &ast::Expr,
        scope: &Scope,
        policy: ExprPolicy,
        functions: &mut FunctionContext<'_, 'ast>,
        expected_type: Option<Arc<TypeDef>>,
    ) -> Result<ResolvedScopeExpr> {
        let mut frames = vec![ExprFrame::new(syntax, expected_type, 0)?];
        let mut values = Vec::<FrameValue>::new();
        loop {
            if let Some(child) = frames
                .last_mut()
                .expect("root expression frame exists")
                .next_child(self.context().main_schema())?
            {
                frames.push(ExprFrame::new(
                    child.syntax,
                    child.expected_type,
                    values.len(),
                )?);
                continue;
            }

            let frame = frames.pop().expect("completed expression frame exists");
            let children = values.drain(frame.first_value..).collect::<FrameChildren>();
            let resolved = self.build_expr_from_frames(
                frame.syntax,
                children,
                scope,
                policy,
                functions,
                frame.expected_type.as_ref(),
            )?;
            match frames.last_mut() {
                Some(_) => values.push(FrameValue::Scalar(resolved)),
                None => return Ok(resolved),
            }
        }
    }

    fn resolve_trigger_qualified(
        &self,
        policy: ExprPolicy,
        namespace: &str,
        column: &str,
    ) -> Result<Option<ResolvedScopeExpr>> {
        let Some(source) = trigger_source(policy, namespace)? else {
            return Ok(None);
        };
        let source = self.source(source).ok_or_else(|| {
            LimboError::InternalError(format!("missing trigger row source {source}"))
        })?;
        let mut scope = Scope::default();
        scope.add_source(source, false);
        scope.resolve_qualified(namespace, column)
    }

    fn build_expr_from_frames(
        &mut self,
        syntax: &ast::Expr,
        children: FrameChildren,
        scope: &Scope,
        policy: ExprPolicy,
        functions: &mut FunctionContext<'_, 'ast>,
        expected_type: Option<&Arc<TypeDef>>,
    ) -> Result<ResolvedScopeExpr> {
        match syntax {
            ast::Expr::Binary(lhs, operator, rhs) => {
                self.build_binary_expr(lhs, *operator, rhs, children)
            }
            ast::Expr::Between {
                lhs,
                start,
                end,
                not,
                ..
            } => self.build_between_expr(lhs, start, end, *not, children),
            ast::Expr::InList { lhs, not, rhs, .. } => {
                let mut children = children;
                scalarize_frame_values(&mut children, policy.policies.row_values)?;
                build_in_list_expr(lhs, *not, rhs, children)
            }
            _ => {
                let children = if policy.policies.row_values == RowValueChecks::None {
                    children
                        .into_iter()
                        .map(FrameValue::into_any_value)
                        .collect()
                } else {
                    scalar_frame_children(children)?
                };
                self.build_expr(syntax, children, scope, policy, functions, expected_type)
            }
        }
    }

    fn build_binary_expr(
        &mut self,
        lhs_syntax: &ast::Expr,
        operator: ast::Operator,
        rhs_syntax: &ast::Expr,
        children: FrameChildren,
    ) -> Result<ResolvedScopeExpr> {
        let mut operands = take_expression_operands([lhs_syntax, rhs_syntax], children)?;
        let rhs = operands
            .pop()
            .expect("binary expression has a right operand");
        let lhs = operands
            .pop()
            .expect("binary expression has a left operand");
        if matches!(rhs_syntax, ast::Expr::Literal(ast::Literal::Null))
            && matches!(operator, ast::Operator::Is | ast::Operator::IsNot)
        {
            return Ok(null_test_expr(
                take_scalar_operand(lhs)?,
                operator == ast::Operator::Is,
            ));
        }
        if matches!(lhs_syntax, ast::Expr::Literal(ast::Literal::Null))
            && matches!(operator, ast::Operator::Is | ast::Operator::IsNot)
        {
            return Ok(null_test_expr(
                take_scalar_operand(rhs)?,
                operator == ast::Operator::Is,
            ));
        }
        if matches!(operator, ast::Operator::Is | ast::Operator::IsNot) {
            if let Some(is_true) = truth_test_rhs(rhs_syntax) {
                let collation = ExprCollation::Absent;
                return Ok(truth_test_expr(
                    take_scalar_operand(lhs)?,
                    is_true,
                    operator == ast::Operator::IsNot,
                    collation,
                ));
            }
        }
        if lhs.components().len() != rhs.components().len()
            || lhs.components().len() > 1 && !supports_row_value_binary_comparison(operator)
        {
            crate::bail_parse_error!("row value misused");
        }
        if lhs.components().len() > 1 {
            let comparison = expression_operand_comparison(lhs.components(), rhs.components());
            let collation = expression_operands_collation([&lhs, &rhs]);
            return Ok(computed_expr(
                hir::Expr::Binary {
                    lhs: Box::new(lhs.into_expr()),
                    operator,
                    rhs: Box::new(rhs.into_expr()),
                    array_concat: false,
                    custom: None,
                    comparison: Some(comparison),
                },
                hir::TypeFact::known(Type::Integer),
                collation,
            ));
        }
        let lhs = take_scalar_operand(lhs)?;
        let rhs = take_scalar_operand(rhs)?;
        let written_literals = [
            matches!(lhs_syntax, ast::Expr::Literal(_)),
            matches!(rhs_syntax, ast::Expr::Literal(_)),
        ];
        let (lhs, rhs) = if operator.is_comparison() {
            read_other_side_as_array(lhs, rhs)
        } else {
            (lhs, rhs)
        };
        self.build_binary_expr_of_written_operands(lhs, operator, rhs, written_literals)
    }

    pub(super) fn build_scalar_binary_expr(
        &mut self,
        lhs: ResolvedScopeExpr,
        operator: ast::Operator,
        rhs: ResolvedScopeExpr,
    ) -> Result<ResolvedScopeExpr> {
        let (lhs, rhs) = if operator.is_comparison() {
            read_other_side_as_array(lhs, rhs)
        } else {
            (lhs, rhs)
        };
        self.build_binary_expr_of_operands(lhs, operator, rhs)
    }

    /// Like [`Self::build_scalar_binary_expr`], but a scalar operand compared
    /// with an array is not read as an array. A table function's hidden
    /// argument column receives the argument's value as is.
    pub(super) fn build_binary_expr_of_operands(
        &mut self,
        lhs: ResolvedScopeExpr,
        operator: ast::Operator,
        rhs: ResolvedScopeExpr,
    ) -> Result<ResolvedScopeExpr> {
        self.build_binary_expr_of_written_operands(lhs, operator, rhs, [true, true])
    }

    /// `written_literals` says whether each operand is written as a literal.
    /// A custom type's operator encodes such an operand with the type; a
    /// literal in parentheses or a computed value is passed as is.
    fn build_binary_expr_of_written_operands(
        &mut self,
        lhs: ResolvedScopeExpr,
        operator: ast::Operator,
        rhs: ResolvedScopeExpr,
        written_literals: [bool; 2],
    ) -> Result<ResolvedScopeExpr> {
        let custom = self.resolve_custom_binary_operator(operator, &lhs, &rhs, written_literals)?;
        let type_fact = binary_type_fact(operator, &lhs.type_fact, &rhs.type_fact);
        let array_concat = operator == ast::Operator::Concat
            && (lhs.type_fact.is_array() || rhs.type_fact.is_array());
        let comparison = operator
            .is_comparison()
            .then(|| comparison_semantics(&lhs, &rhs));
        // The value of an operator keeps an explicit COLLATE of either
        // operand, as SQLite propagates one through operators, but not an
        // operand's column collation; a comparison's operands decide the
        // comparison's own collation above.
        let collation = explicit_collation([&lhs, &rhs]);
        Ok(computed_expr(
            hir::Expr::Binary {
                lhs: Box::new(lhs.expr),
                operator,
                rhs: Box::new(rhs.expr),
                array_concat,
                custom: custom.map(Box::new),
                comparison,
            },
            type_fact,
            collation,
        ))
    }

    fn build_between_expr(
        &mut self,
        lhs_syntax: &ast::Expr,
        start_syntax: &ast::Expr,
        end_syntax: &ast::Expr,
        negated: bool,
        children: FrameChildren,
    ) -> Result<ResolvedScopeExpr> {
        let mut operands =
            take_expression_operands([lhs_syntax, start_syntax, end_syntax], children)?.into_iter();
        let expr = operands.next().expect("BETWEEN has a tested operand");
        let start = operands.next().expect("BETWEEN has a lower bound");
        let end = operands.next().expect("BETWEEN has an upper bound");
        if expr.components().len() != start.components().len()
            || expr.components().len() != end.components().len()
        {
            crate::bail_parse_error!("row value misused");
        }
        // BETWEEN on a custom type is `x >= a AND x <= b` with
        // the type's own comparison operators.
        if expr.components().len() == 1 {
            let (tested, low, high) = (
                &expr.components()[0],
                &start.components()[0],
                &end.components()[0],
            );
            let custom = self
                .resolve_custom_binary_operator(
                    ast::Operator::GreaterEquals,
                    tested,
                    low,
                    [true, true],
                )?
                .is_some()
                || self
                    .resolve_custom_binary_operator(
                        ast::Operator::LessEquals,
                        tested,
                        high,
                        [true, true],
                    )?
                    .is_some();
            if custom {
                let lower = self.build_scalar_binary_expr(
                    tested.clone(),
                    ast::Operator::GreaterEquals,
                    low.clone(),
                )?;
                let upper = self.build_scalar_binary_expr(
                    tested.clone(),
                    ast::Operator::LessEquals,
                    high.clone(),
                )?;
                let both = self.build_scalar_binary_expr(lower, ast::Operator::And, upper)?;
                if !negated {
                    return Ok(both);
                }
                return Ok(computed_expr(
                    hir::Expr::Unary {
                        operator: ast::UnaryOperator::Not,
                        expr: Box::new(both.expr),
                    },
                    hir::TypeFact::known(Type::Integer),
                    ExprCollation::Absent,
                ));
            }
        }
        let start_comparison = expression_operand_comparison(expr.components(), start.components());
        let end_comparison = expression_operand_comparison(expr.components(), end.components());
        let collation = ExprCollation::Absent;
        Ok(computed_expr(
            hir::Expr::Between {
                expr: Box::new(expr.into_expr()),
                negated,
                start: Box::new(start.into_expr()),
                end: Box::new(end.into_expr()),
                start_comparison,
                end_comparison,
            },
            hir::TypeFact::known(Type::Integer),
            collation,
        ))
    }

    fn build_expr(
        &mut self,
        syntax: &ast::Expr,
        children: ExprChildren,
        scope: &Scope,
        policy: ExprPolicy,
        functions: &mut FunctionContext<'_, 'ast>,
        expected_type: Option<&Arc<TypeDef>>,
    ) -> Result<ResolvedScopeExpr> {
        match syntax {
            ast::Expr::Literal(literal) => {
                expect_no_expr_children(children)?;
                self.resolve_atomic_expr(hir::Expr::Literal(literal.clone()), scope)
            }
            ast::Expr::Id(name) | ast::Expr::Name(name) => {
                expect_no_expr_children(children)?;
                let expression =
                    match scope.resolve_unqualified(name.as_str(), policy.precedence)? {
                        Some(resolved) => resolved.expr,
                        None if policy.allow_dqs_fallback && name.quoted_with('"') => {
                            hir::Expr::Literal(ast::Literal::String(name.as_literal()))
                        }
                        None => crate::bail_parse_error!("no such column: {}", name.as_str()),
                    };
                self.resolve_atomic_expr(expression, scope)
            }
            ast::Expr::Qualified(table, column) => {
                expect_no_expr_children(children)?;
                if let Some(resolved) =
                    self.resolve_trigger_qualified(policy, table.as_str(), column.as_str())?
                {
                    return Ok(resolved);
                }
                if let Some(resolved) = scope.resolve_qualified(table.as_str(), column.as_str())? {
                    return self.resolve_atomic_expr(resolved.expr, scope);
                }
                // `data.x` reads field `x` of a struct or union column `data`.
                let base = scope
                    .resolve_unqualified(table.as_str(), NamePrecedence::SourcesOnly)
                    .map_err(|error| match error {
                        LimboError::ParseError(message)
                            if message.starts_with("ambiguous column name") =>
                        {
                            LimboError::ParseError(format!(
                                "ambiguous column reference: '{}' — multiple tables have a \
                                 struct/union column with this name",
                                table.as_str()
                            ))
                        }
                        error => error,
                    })?;
                if let Some(base) = base {
                    if custom_argument_type(&base, |definition| {
                        definition.is_struct() || definition.is_union()
                    })
                    .is_some()
                    {
                        return self.build_field_access(base, column.as_str());
                    }
                }
                // In a query, no visible source named that way means the
                // table is missing, not the column. A CTE defined for the
                // query but not in its FROM is a known name, so only its
                // column is missing. The expressions of a DML statement
                // outside a query, like an upsert's SET, report the column.
                let normalized_table = normalize_ident(table.as_str());
                if matches!(functions, FunctionContext::Query(_))
                    && !scope.has_qualifier(&normalized_table)
                    && !self.cte_is_defined(&normalized_table)
                {
                    crate::bail_parse_error!("no such table: {normalized_table}");
                }
                crate::bail_parse_error!("no such column: {}.{}", table.as_str(), column.as_str());
            }
            ast::Expr::DoublyQualified(database, table, column) => {
                expect_no_expr_children(children)?;
                if let Some(base) =
                    self.resolve_trigger_qualified(policy, database.as_str(), table.as_str())?
                {
                    if custom_argument_type(&base, |definition| {
                        definition.is_struct() || definition.is_union()
                    })
                    .is_some()
                    {
                        return self.build_field_access(base, column.as_str());
                    }
                    crate::bail_parse_error!(
                        "column '{}' is not a STRUCT or UNION type; cannot access field '{}'",
                        normalize_ident(table.as_str()),
                        normalize_ident(column.as_str())
                    );
                }
                if let Some(resolved) =
                    self.resolve_trigger_qualified(policy, table.as_str(), column.as_str())?
                {
                    return Ok(resolved);
                }
                if let Some(database_id) = self.context().database(database.as_str()) {
                    if let Some(resolved) = scope.resolve_database_qualified(
                        database_id,
                        table.as_str(),
                        column.as_str(),
                    )? {
                        return self.resolve_atomic_expr(resolved.expr, scope);
                    }
                }

                if let Some(base) = scope.resolve_qualified(database.as_str(), table.as_str())? {
                    if custom_argument_type(&base, |definition| {
                        definition.is_struct() || definition.is_union()
                    })
                    .is_some()
                    {
                        return self.build_field_access(base, column.as_str());
                    }
                    crate::bail_parse_error!(
                        "column '{}' is not a STRUCT or UNION type; cannot access field '{}'",
                        normalize_ident(table.as_str()),
                        normalize_ident(column.as_str())
                    );
                }

                if let Some(base) =
                    scope.resolve_unqualified(database.as_str(), NamePrecedence::SourcesOnly)?
                {
                    if let Some(first) = self.try_build_field_access(base, table.as_str())? {
                        if custom_argument_type(&first, |definition| {
                            definition.is_struct() || definition.is_union()
                        })
                        .is_some()
                        {
                            return self.build_field_access(first, column.as_str());
                        }
                    }
                }

                crate::bail_parse_error!(
                    "no such column: {}.{}.{}",
                    database.as_str(),
                    table.as_str(),
                    column.as_str()
                );
            }
            ast::Expr::Column { table, column, .. } if table.is_self_table() => {
                expect_no_expr_children(children)?;
                let source = policy.self_source.ok_or_else(|| {
                    LimboError::InternalError(
                        "SELF_TABLE column appeared outside a stored schema expression".to_string(),
                    )
                })?;
                self.resolve_atomic_expr(hir::Expr::column(source, *column), scope)
            }
            ast::Expr::RowId { table, .. } if table.is_self_table() => {
                expect_no_expr_children(children)?;
                let source = policy.self_source.ok_or_else(|| {
                    LimboError::InternalError(
                        "SELF_TABLE rowid appeared outside a stored schema expression".to_string(),
                    )
                })?;
                self.resolve_atomic_expr(hir::Expr::rowid(source), scope)
            }
            ast::Expr::FieldAccess { field, .. } => {
                let [base] = expect_expr_children(children)?;
                self.build_field_access(base, field.as_str())
            }
            ast::Expr::Array { .. } => Ok(array_expr(children)),
            ast::Expr::Subscript { .. } => {
                let [base, index] = expect_expr_children(children)?;
                Ok(subscript_expr(base, index))
            }
            ast::Expr::Parenthesized(expressions) if expressions.len() == 1 => {
                let [inner] = expect_expr_children(children)?;
                Ok(inner)
            }
            // A row value that is never evaluated is not checked, as in
            // SQLite, which only rejects it while generating code.
            ast::Expr::Parenthesized(expressions)
                if policy.policies.row_values != RowValueChecks::All =>
            {
                expect_no_expr_children(children)?;
                let mut first = None;
                for expression in expressions {
                    let value = self.analyze_resolved_expr_with_functions(
                        expression, scope, policy, functions, None,
                    )?;
                    first.get_or_insert(value);
                }
                first.ok_or_else(|| {
                    LimboError::InternalError("row value has no components".to_string())
                })
            }
            ast::Expr::Parenthesized(_) => crate::bail_parse_error!("row value misused"),
            ast::Expr::Variable(variable) => {
                expect_no_expr_children(children)?;
                if variable.col_type.is_some() {
                    return super::analyze::unsupported_select();
                }
                self.resolve_atomic_expr(
                    hir::Expr::Parameter(hir::Parameter {
                        index: variable.index,
                        spelling: match variable.name.as_deref() {
                            Some(name) => {
                                crate::parameters::ParameterSpelling::Named(name.to_owned())
                            }
                            None if variable.numbered => {
                                crate::parameters::ParameterSpelling::Numbered
                            }
                            None => crate::parameters::ParameterSpelling::Anonymous,
                        },
                        type_fact: hir::TypeFact::dynamic(),
                    }),
                    scope,
                )
            }
            ast::Expr::Unary(operator, _) => {
                let [inner] = expect_expr_children(children)?;
                let type_fact = match operator {
                    ast::UnaryOperator::Positive | ast::UnaryOperator::Negative => {
                        hir::TypeFact::arithmetic_result(
                            &inner.type_fact,
                            &hir::TypeFact::known(Type::Integer),
                        )
                    }
                    ast::UnaryOperator::BitwiseNot | ast::UnaryOperator::Not => {
                        hir::TypeFact::known(Type::Integer)
                    }
                };
                Ok(computed_expr(
                    hir::Expr::Unary {
                        operator: *operator,
                        expr: Box::new(inner.expr),
                    },
                    type_fact,
                    inner.collation,
                ))
            }
            ast::Expr::Binary(..) | ast::Expr::Between { .. } | ast::Expr::InList { .. } => {
                unreachable!("operand expressions are built from shaped frame values")
            }
            ast::Expr::Like {
                lhs,
                not,
                op,
                escape,
                ..
            } => self.build_like(lhs, *not, *op, escape.is_some(), children),
            ast::Expr::Case {
                base: base_syntax,
                when_then_pairs,
                else_expr: else_syntax,
            } => {
                let expected_children = usize::from(base_syntax.is_some())
                    + when_then_pairs.len() * 2
                    + usize::from(else_syntax.is_some());
                if children.len() != expected_children {
                    return Err(LimboError::InternalError(format!(
                        "CASE expression expected {expected_children} child values, got {}",
                        children.len()
                    )));
                }
                // A CASE value carries no collation, whatever its arms read.
                let collation = ExprCollation::Absent;
                let mut children = children.into_iter();
                let base = base_syntax.as_ref().map(|_| {
                    children
                        .next()
                        .expect("CASE child count includes base expression")
                });
                let when_then = when_then_pairs
                    .iter()
                    .map(|_| {
                        (
                            children.next().expect("CASE child count includes WHEN"),
                            children.next().expect("CASE child count includes THEN"),
                        )
                    })
                    .collect::<Vec<_>>();
                let else_expr = else_syntax.as_ref().map(|_| {
                    children
                        .next()
                        .expect("CASE child count includes ELSE expression")
                });
                let base_comparisons = base.as_ref().map_or_else(Vec::new, |base| {
                    when_then
                        .iter()
                        .map(|(when, _)| comparison_semantics(base, when))
                        .collect()
                });
                let type_fact = hir::TypeFact::selected_value_result(
                    when_then
                        .iter()
                        .map(|(_, then)| &then.type_fact)
                        .chain(else_expr.iter().map(|value| &value.type_fact)),
                );
                Ok(computed_expr(
                    hir::Expr::Case {
                        base: base.map(|value| Box::new(value.expr)),
                        when_then: when_then
                            .into_iter()
                            .map(|(when, then)| (when.expr, then.expr))
                            .collect(),
                        else_expr: else_expr.map(|value| Box::new(value.expr)),
                        base_comparisons,
                    },
                    type_fact,
                    collation,
                ))
            }
            ast::Expr::Cast { type_name, .. } => {
                let parameter_count = cast_parameter_count(type_name.as_ref());
                if children.len() != parameter_count + 1 {
                    return Err(LimboError::InternalError(format!(
                        "CAST expression expected {} child values, got {}",
                        parameter_count + 1,
                        children.len()
                    )));
                }
                let mut children = children.into_iter();
                let parameters = children.by_ref().take(parameter_count).collect::<Vec<_>>();
                let input = children
                    .next()
                    .expect("CAST child count includes input expression");
                let collation = parameters
                    .iter()
                    .fold(input.collation.clone(), |current, value| {
                        expression_collation(&current, &value.collation)
                    });
                let target = self.resolve_cast_target(type_name.as_ref(), parameters)?;
                let type_fact = target.type_fact.clone();
                let affinity = target.affinity;
                Ok(ResolvedScopeExpr {
                    expr: hir::Expr::Cast {
                        expr: Box::new(input.expr),
                        target: Box::new(target),
                    },
                    type_fact,
                    affinity,
                    has_affinity: true,
                    collation,
                })
            }
            ast::Expr::FunctionCall {
                name,
                distinctness,
                args,
                order_by,
                within_group,
                filter_over,
            } => {
                let ordered_set = if within_group.is_empty() {
                    if normalize_ident(name.as_str()) == "mode" {
                        crate::bail_parse_error!(
                            "mode() requires a WITHIN GROUP (ORDER BY ...) clause"
                        );
                    }
                    None
                } else {
                    Some(resolve_ordered_set_function(
                        name,
                        args,
                        distinctness.as_ref(),
                        order_by,
                        within_group,
                        filter_over,
                    )?)
                };
                let expected_children = args.len()
                    + order_by.len()
                    + within_group.len()
                    + usize::from(filter_over.filter_clause.is_some())
                    + window_expression_count(name, args.len(), filter_over.over_clause.as_deref());
                if children.len() != expected_children {
                    return Err(LimboError::InternalError(format!(
                        "function expression expected {} child values, got {}",
                        expected_children,
                        children.len()
                    )));
                }
                let mut children = children.into_iter();
                let values = children.by_ref().take(args.len()).collect();
                let argument_order = order_by
                    .iter()
                    .zip(children.by_ref())
                    .map(|(syntax, value)| FunctionOrderTerm {
                        value,
                        order: syntax.order.unwrap_or(ast::SortOrder::Asc),
                        nulls: syntax.nulls,
                    })
                    .collect::<Vec<_>>();
                let within_group = within_group
                    .iter()
                    .zip(children.by_ref())
                    .map(|(syntax, value)| FunctionOrderTerm {
                        value,
                        order: syntax.order.unwrap_or(ast::SortOrder::Asc),
                        nulls: syntax.nulls,
                    })
                    .collect::<Vec<_>>();
                let filter = filter_over.filter_clause.as_ref().map(|_| {
                    children
                        .next()
                        .expect("function child count includes FILTER")
                });
                let window = take_function_window(
                    name,
                    args.len(),
                    filter_over.over_clause.as_deref(),
                    &mut children,
                )?;
                let input = match ordered_set {
                    Some(function) => FunctionInput::OrderedSet {
                        function,
                        direct: values,
                        order_by: within_group
                            .into_iter()
                            .next()
                            .expect("ordered-set syntax requires one ordering expression"),
                    },
                    None => FunctionInput::Expressions {
                        distinctness: *distinctness,
                        values,
                        order_by: argument_order,
                    },
                };
                self.build_function_call(
                    name,
                    input,
                    filter,
                    window,
                    scope,
                    policy,
                    functions,
                    expected_type,
                )
            }
            ast::Expr::FunctionCallStar { name, filter_over } => {
                let expected_children = usize::from(filter_over.filter_clause.is_some())
                    + window_expression_count(name, 0, filter_over.over_clause.as_deref());
                if children.len() != expected_children {
                    return Err(LimboError::InternalError(format!(
                        "star function expression expected {expected_children} child values, got {}",
                        children.len()
                    )));
                }
                let mut children = children.into_iter();
                let filter = filter_over.filter_clause.as_ref().map(|_| {
                    children
                        .next()
                        .expect("star function child count includes FILTER")
                });
                let window = take_function_window(
                    name,
                    0,
                    filter_over.over_clause.as_deref(),
                    &mut children,
                )?;
                self.build_function_call(
                    name,
                    FunctionInput::Star,
                    filter,
                    window,
                    scope,
                    policy,
                    functions,
                    expected_type,
                )
            }
            ast::Expr::Collate(_, name) => {
                let [inner] = expect_expr_children(children)?;
                let collation = self.resolve_collation(name)?;
                Ok(ResolvedScopeExpr {
                    expr: hir::Expr::Collate {
                        expr: Box::new(inner.expr),
                        collation: collation.clone(),
                    },
                    type_fact: inner.type_fact,
                    affinity: inner.affinity,
                    has_affinity: inner.has_affinity,
                    collation: ExprCollation::Explicit(collation),
                })
            }
            ast::Expr::IsNull(_) => {
                let [inner] = expect_expr_children(children)?;
                Ok(null_test_expr(inner, true))
            }
            ast::Expr::NotNull(_) => {
                let [inner] = expect_expr_children(children)?;
                Ok(null_test_expr(inner, false))
            }
            ast::Expr::Register(index) => {
                expect_no_expr_children(children)?;
                let Some(source) = self.register_source else {
                    crate::bail_parse_error!(
                        "a register reference is only allowed in an expression over register inputs"
                    );
                };
                let width = self.source(source).map_or(0, |source| source.columns.len());
                if *index >= width {
                    crate::bail_parse_error!(
                        "register {index} is outside the {width} inputs of the expression"
                    );
                }
                self.resolve_atomic_expr(hir::Expr::column(source, *index), scope)
            }
            ast::Expr::Raise(action, message) => {
                validate_raise(*action, policy.raise)?;
                let message = match message {
                    Some(_) => {
                        let [message] = expect_expr_children(children)?;
                        Some(Box::new(message.expr))
                    }
                    None => {
                        expect_no_expr_children(children)?;
                        None
                    }
                };
                Ok(computed_expr(
                    hir::Expr::Raise {
                        action: *action,
                        message,
                    },
                    hir::TypeFact::dynamic(),
                    ExprCollation::Absent,
                ))
            }
            _ => super::analyze::unsupported_select(),
        }
    }

    fn build_like(
        &mut self,
        lhs_syntax: &ast::Expr,
        negated: bool,
        operator: ast::LikeOperator,
        has_escape: bool,
        children: ExprChildren,
    ) -> Result<ResolvedScopeExpr> {
        let lhs_count = expression_operand_width(lhs_syntax);
        let expected_children = lhs_count + 1 + usize::from(has_escape);
        if children.len() != expected_children {
            return Err(LimboError::InternalError(format!(
                "{operator} expression expected {expected_children} child values, got {}",
                children.len()
            )));
        }
        if lhs_count > 1 && operator != ast::LikeOperator::Match {
            crate::bail_parse_error!("row value misused");
        }
        if has_escape && operator != ast::LikeOperator::Like {
            crate::bail_parse_error!("wrong number of arguments to function {operator}()");
        }

        let mut children = children.into_iter();
        let lhs = if lhs_count == 1 {
            children
                .next()
                .expect("LIKE child count includes left expression")
                .expr
        } else {
            hir::Expr::Row(
                children
                    .by_ref()
                    .take(lhs_count)
                    .map(|value| value.expr)
                    .collect(),
            )
        };
        let rhs = children
            .next()
            .expect("LIKE child count includes right expression")
            .expr;
        let escape = has_escape.then(|| {
            Box::new(
                children
                    .next()
                    .expect("LIKE child count includes ESCAPE expression")
                    .expr,
            )
        });
        debug_assert!(children.next().is_none());

        let function_name = match operator {
            ast::LikeOperator::Like => "like",
            ast::LikeOperator::Glob => "glob",
            ast::LikeOperator::Regexp => "regexp",
            ast::LikeOperator::Match => {
                #[cfg(all(feature = "fts", not(target_family = "wasm")))]
                {
                    "fts_match"
                }
                #[cfg(any(not(feature = "fts"), target_family = "wasm"))]
                {
                    crate::bail_parse_error!("MATCH requires the 'fts' feature to be enabled")
                }
            }
        };
        let argument_count = lhs_count + 1 + usize::from(has_escape);
        let Some(function) = self
            .context()
            .resolve_function(function_name, argument_count)?
        else {
            crate::bail_parse_error!("no such function: {function_name}");
        };
        let id = self.catalog_object_id(
            None,
            CatalogObjectKind::Function { argument_count },
            function_name,
        );
        let function =
            hir::CatalogObject::new(id, self.context().snapshot(), None, Arc::new(function));

        Ok(computed_expr(
            hir::Expr::Like {
                lhs: Box::new(lhs),
                negated,
                operator,
                function,
                argument_count,
                rhs: Box::new(rhs),
                escape,
            },
            hir::TypeFact::known(Type::Integer),
            ExprCollation::Absent,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn build_function_call(
        &mut self,
        name: &ast::Name,
        input: FunctionInput,
        filter: Option<ResolvedScopeExpr>,
        window: Option<FunctionOver>,
        scope: &Scope,
        policy: ExprPolicy,
        functions: &mut FunctionContext<'_, 'ast>,
        expected_type: Option<&Arc<TypeDef>>,
    ) -> Result<ResolvedScopeExpr> {
        let parsed_argument_count = input.argument_count();
        let function_name = normalize_ident(name.as_str());
        let function = match &input {
            FunctionInput::OrderedSet { function, .. } => Func::Agg(function.clone()),
            _ => {
                let Some(function) = self
                    .context()
                    .resolve_function(name.as_str(), parsed_argument_count)?
                else {
                    crate::bail_parse_error!("no such function: {}", name.as_str());
                };
                function
            }
        };
        let binding = bind_function(
            &function,
            window.is_some(),
            matches!(input, FunctionInput::Star),
            name,
            policy,
            functions,
        )?;
        if matches!(binding, FunctionBinding::Scalar) && filter.is_some() {
            crate::bail_parse_error!(
                "FILTER may not be used with non-aggregate {}()",
                function_name
            );
        }
        let aggregate = matches!(binding, FunctionBinding::Aggregate(_));
        let window_evaluation = matches!(binding, FunctionBinding::Window(_));
        let window = match (window_evaluation, window) {
            (true, Some(over)) => Some(match functions {
                FunctionContext::Query(state) => {
                    self.resolve_function_over(over, &function, scope, policy, state)?
                }
                FunctionContext::ScalarOnly => {
                    unreachable!("window binding rejects scalar-only expression contexts")
                }
            }),
            (false, None) => None,
            _ => unreachable!("window binding and OVER clause must agree"),
        };
        if window_evaluation && !input.order_terms().is_empty() {
            crate::bail_parse_error!(
                "ORDER BY may not be used with non-aggregate {}()",
                name.as_str()
            );
        }
        if window_evaluation && input.distinctness().is_some() {
            crate::bail_parse_error!("DISTINCT is not supported for window functions");
        }
        if window_evaluation && filter.is_some() && matches!(&function, Func::Window(_)) {
            crate::bail_parse_error!(
                "FILTER clause may only be used with aggregate window functions"
            );
        }
        let (input, filter) = if matches!(binding, FunctionBinding::Scalar) {
            (normalize_scalar_input(name, &function, input, scope)?, None)
        } else {
            (input, filter)
        };
        if matches!(binding, FunctionBinding::Scalar) {
            validate_scalar_arguments(&function, &input)?;
        }
        let special_operation =
            match self.resolve_custom_type_operation(&function, &input, expected_type)? {
                Some((operation, result_type)) => {
                    Some((hir::FunctionOperation::CustomType(operation), result_type))
                }
                None => self.resolve_sequence_operation(&function, &input)?,
            };
        if aggregate || window_evaluation {
            // A window call runs over the rows an aggregate query produces, so
            // its operands may contain aggregates but not other windows.
            let search = if window_evaluation {
                NestedSearch::WindowsOnly
            } else {
                NestedSearch::AggregatesAndWindows
            };
            if let Some(nested) = nested_function_iter(input.facts(), search)
                .or_else(|| {
                    nested_function_iter(input.order_terms().iter().map(|term| &term.value), search)
                })
                .or_else(|| {
                    filter
                        .as_ref()
                        .and_then(|filter| nested_function_iter([filter], search))
                })
                .or_else(|| window.as_ref().and_then(FunctionWindow::nested_window))
            {
                match nested {
                    NestedFunction::Aggregate(name) => {
                        crate::bail_parse_error!("misuse of aggregate function {name}()")
                    }
                    NestedFunction::Window(name) => {
                        crate::bail_parse_error!("misuse of window function {name}()")
                    }
                }
            }
        }
        if matches!(binding, FunctionBinding::Scalar) {
            match &function {
                Func::Scalar(ScalarFunc::Array) => {
                    let FunctionInput::Expressions { values, .. } = input else {
                        unreachable!("array() only accepts ordinary expression arguments")
                    };
                    return Ok(array_expr(values));
                }
                Func::Scalar(ScalarFunc::ArrayElement) => {
                    let FunctionInput::Expressions { values, .. } = input else {
                        unreachable!("array_element() only accepts ordinary expression arguments")
                    };
                    let [base, index] = expect_expr_children(values)?;
                    return Ok(subscript_expr(base, index));
                }
                _ => {}
            }
        }
        let (operation, result_type) = special_operation.map_or_else(
            || {
                (
                    hir::FunctionOperation::Ordinary,
                    function_result_type(&function, input.result_facts()),
                )
            },
            |resolved| resolved,
        );
        let argument_count = input.argument_count();
        let id = self.catalog_object_id(
            None,
            CatalogObjectKind::Function { argument_count },
            function_name.clone(),
        );
        let function =
            hir::CatalogObject::new(id, self.context().snapshot(), None, Arc::new(function));
        let evaluation = match binding {
            FunctionBinding::Scalar => hir::FunctionEvaluation::Scalar,
            FunctionBinding::Aggregate(binding) => {
                if self.aggregate_hosts.last() == Some(&None)
                    && self.aggregate_reads_outside(binding.block, &input, filter.as_ref())
                {
                    crate::bail_parse_error!("misuse of aggregate: {}()", function_name);
                }
                let block = self.aggregate_owner(binding.block, &input, filter.as_ref());
                hir::FunctionEvaluation::Aggregate {
                    id: self.allocate_aggregate(block),
                    filter: filter.map(|filter| Box::new(filter.expr)),
                }
            }
            FunctionBinding::Window(binding) => hir::FunctionEvaluation::Window {
                id: binding.id,
                window: match functions {
                    FunctionContext::Query(state) => state.allocate_resolved_window(
                        window.expect("window binding requires an OVER clause"),
                        function.value(),
                    )?,
                    FunctionContext::ScalarOnly => {
                        unreachable!("window binding rejects scalar-only expression contexts")
                    }
                },
                filter: filter.map(|filter| Box::new(filter.expr)),
            },
        };
        // A function's value keeps an explicit COLLATE of an argument, as
        // SQLite propagates one through calls, but not a column collation.
        let collation = explicit_collation(input.facts());
        let arguments = input.into_hir();
        Ok(computed_expr(
            hir::Expr::Function(Box::new(hir::FunctionCall {
                function,
                evaluation,
                arguments,
                result_type: result_type.clone(),
                operation,
            })),
            result_type,
            collation,
        ))
    }

    fn resolve_custom_type_operation(
        &mut self,
        function: &Func,
        input: &FunctionInput,
        expected_type: Option<&Arc<TypeDef>>,
    ) -> Result<Option<(hir::CustomTypeOperation, hir::TypeFact)>> {
        let scalar = match function {
            Func::Scalar(
                scalar @ (ScalarFunc::UnionValueFunc
                | ScalarFunc::UnionTagFunc
                | ScalarFunc::UnionExtractFunc
                | ScalarFunc::StructExtractFunc),
            ) => scalar,
            _ => return Ok(None),
        };
        let arguments = input.facts();
        match scalar {
            ScalarFunc::UnionValueFunc => {
                let union = expected_type
                    .filter(|definition| definition.is_union())
                    .ok_or_else(|| {
                        LimboError::ParseError(
                            "union_value() can only be used in INSERT/UPDATE targeting a union-typed column"
                                .to_string(),
                        )
                    })?;
                let tag_name = string_literal_argument(
                    &arguments[0],
                    "union_value() first argument must be a string literal",
                )?;
                let (tag_index, _) = union.find_union_variant(&tag_name).ok_or_else(|| {
                    LimboError::ParseError(format!(
                        "unknown variant '{}' in union type '{}'",
                        tag_name, union.name
                    ))
                })?;
                let resolved = self
                    .context()
                    .main_schema()
                    .resolve_type_unchecked(&union.name)?
                    .ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "expected INSERT type '{}' is missing from the schema",
                            union.name
                        ))
                    })?;
                let (result_type, _) = self.freeze_type_fact(&union.name, resolved, 0);
                let union_type = result_type
                    .declared
                    .as_ref()
                    .and_then(|declared| declared.custom())
                    .cloned()
                    .ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "expected INSERT union '{}' has no frozen type definition",
                            union.name
                        ))
                    })?;
                Ok(Some((
                    hir::CustomTypeOperation::UnionValue {
                        union_type,
                        tag_index,
                    },
                    result_type,
                )))
            }
            ScalarFunc::UnionTagFunc => {
                let union_type =
                    custom_argument_type(&arguments[0], |definition| definition.is_union())
                        .ok_or_else(|| {
                            LimboError::ParseError(
                                "union_tag() argument must have a known union type".to_string(),
                            )
                        })?;
                let tag_names = Arc::clone(
                    &union_type
                        .value()
                        .union_def()
                        .expect("resolved union type has a union definition")
                        .tag_names,
                );
                Ok(Some((
                    hir::CustomTypeOperation::UnionTag {
                        union_type,
                        tag_names,
                    },
                    hir::TypeFact::known(Type::Text),
                )))
            }
            ScalarFunc::UnionExtractFunc => {
                let tag_name = string_literal_argument(
                    &arguments[1],
                    "union_extract() second argument must be a string literal",
                )?;
                let union_type =
                    custom_argument_type(&arguments[0], |definition| definition.is_union())
                        .ok_or_else(|| {
                            LimboError::ParseError(
                                "union_extract() first argument must have a known union type"
                                    .to_string(),
                            )
                        })?;
                let member = self
                    .resolve_custom_member(&arguments[0], &tag_name)?
                    .ok_or_else(|| {
                        LimboError::ParseError(format!(
                            "unknown variant '{}' in union type '{}'",
                            tag_name,
                            union_type.value().name
                        ))
                    })?;
                let hir::FieldAccessKind::Union { tag_index } = member.kind else {
                    unreachable!("union member resolution returns a union variant")
                };
                Ok(Some((
                    hir::CustomTypeOperation::UnionExtract {
                        union_type,
                        tag_index,
                    },
                    member.result_type,
                )))
            }
            ScalarFunc::StructExtractFunc => {
                let field_name = string_literal_argument(
                    &arguments[1],
                    "struct_extract() second argument must be a string literal",
                )?;
                // An unknown type and an unknown field report the same error.
                let unresolved = || {
                    LimboError::ParseError(format!(
                        "cannot resolve struct field '{field_name}' for struct_extract"
                    ))
                };
                let struct_type =
                    custom_argument_type(&arguments[0], |definition| definition.is_struct())
                        .ok_or_else(unresolved)?;
                let member = self
                    .resolve_custom_member(&arguments[0], &field_name)?
                    .ok_or_else(unresolved)?;
                let hir::FieldAccessKind::Struct { field_index } = member.kind else {
                    unreachable!("struct member resolution returns a struct field")
                };
                Ok(Some((
                    hir::CustomTypeOperation::StructExtract {
                        struct_type,
                        field_index,
                    },
                    member.result_type,
                )))
            }
            _ => unreachable!("custom-type read function match is exhaustive"),
        }
    }

    fn try_build_field_access(
        &mut self,
        base: ResolvedScopeExpr,
        field_name: &str,
    ) -> Result<Option<ResolvedScopeExpr>> {
        let Some(member) = self.resolve_custom_member(&base, field_name)? else {
            return Ok(None);
        };
        Ok(Some(field_access_expr(base, field_name, member)))
    }

    fn build_field_access(
        &mut self,
        base: ResolvedScopeExpr,
        field_name: &str,
    ) -> Result<ResolvedScopeExpr> {
        if let Some(member) = self.resolve_custom_member(&base, field_name)? {
            return Ok(field_access_expr(base, field_name, member));
        }

        let normalized = normalize_ident(field_name);
        let Some(container) = custom_argument_type(&base, |_| true) else {
            crate::bail_parse_error!(
                "cannot access field '{}' on a value without a known struct or union type",
                normalized
            );
        };
        if container.value().is_struct() {
            crate::bail_parse_error!(
                "no such field '{}' in struct type '{}'",
                normalized,
                container.value().name
            );
        }
        if container.value().is_union() {
            crate::bail_parse_error!(
                "no such variant '{}' in union type '{}'",
                normalized,
                container.value().name
            );
        }
        crate::bail_parse_error!(
            "type '{}' is not a struct or union type",
            container.value().name
        );
    }

    fn resolve_custom_member(
        &mut self,
        base: &ResolvedScopeExpr,
        field_name: &str,
    ) -> Result<Option<ResolvedCustomMember>> {
        let Some(container_type) = custom_argument_type(base, |definition| {
            definition.is_struct() || definition.is_union()
        }) else {
            return Ok(None);
        };
        let normalized = normalize_ident(field_name);
        let (kind, result_name) = if let Some((field_index, field)) =
            container_type.value().find_struct_field(&normalized)
        {
            (
                hir::FieldAccessKind::Struct { field_index },
                field.type_name.clone(),
            )
        } else if let Some((tag_index, variant)) =
            container_type.value().find_union_variant(&normalized)
        {
            (
                hir::FieldAccessKind::Union { tag_index },
                variant.type_name.clone(),
            )
        } else {
            return Ok(None);
        };
        let result_type = self.resolve_named_type_fact(&result_name)?;
        Ok(Some(ResolvedCustomMember {
            container_type,
            result_type,
            kind,
        }))
    }

    fn resolve_custom_binary_operator(
        &mut self,
        operator: ast::Operator,
        lhs: &ResolvedScopeExpr,
        rhs: &ResolvedScopeExpr,
        written_literals: [bool; 2],
    ) -> Result<Option<hir::CustomBinaryOperator>> {
        if self.standard_operators_only || custom_operator_symbol(operator).is_none() {
            return Ok(None);
        }
        let lhs_column = self.custom_operator_column(lhs)?;
        let rhs_column = self.custom_operator_column(rhs)?;

        // With one custom column, the other operand is encoded when it is a
        // literal the type accepts as input. Otherwise an arithmetic operator
        // of a type whose stored values read back unchanged still gets the
        // operand as it is, so `date + 1`, `date + int_col` and `date + ?`
        // add days; the operator function checks the operand's type. A
        // comparison such as `int_col = '10'` keeps the standard operator.
        let one_custom_column = |column: &CustomOperatorColumn,
                                 other: &ResolvedScopeExpr,
                                 operand: hir::BinaryOperand| {
            let other_is_written_literal = match operand {
                hir::BinaryOperand::Left => written_literals[0],
                hir::BinaryOperand::Right => written_literals[1],
            };
            let encode_other = other_is_written_literal
                && literal_matches_custom_type(other, column.definition.value());
            let pass_other_as_is = !operator.is_comparison()
                && column.definition.value().decode_returns_value_unchanged();
            if !encode_other && !pass_other_as_is {
                return None;
            }
            Some(encode_other.then(|| hir::CustomBinaryLiteralEncoding {
                operand,
                encoder: column.encoder.clone(),
            }))
        };
        let (column, literal_encoding) = match (&lhs_column, &rhs_column) {
            (Some(lhs), Some(rhs)) => {
                if !lhs.declared_name.eq_ignore_ascii_case(&rhs.declared_name) {
                    return Ok(None);
                }
                (lhs, None)
            }
            (Some(lhs), None) => {
                let Some(encoding) = one_custom_column(lhs, rhs, hir::BinaryOperand::Right) else {
                    return Ok(None);
                };
                (lhs, encoding)
            }
            (None, Some(rhs)) => {
                let Some(encoding) = one_custom_column(rhs, lhs, hir::BinaryOperand::Left) else {
                    return Ok(None);
                };
                (rhs, encoding)
            }
            (None, None) => return Ok(None),
        };

        let Some(resolved) = resolve_custom_operator(column.definition.value(), operator) else {
            return Ok(None);
        };
        let function_name = normalize_ident(&resolved.function_name);
        let function = self
            .context()
            .resolve_function(&function_name, 2)?
            .ok_or_else(|| {
                LimboError::InternalError(format!("function not found: {function_name}"))
            })?;
        let id = self.catalog_object_id(
            None,
            CatalogObjectKind::Function { argument_count: 2 },
            function_name,
        );
        Ok(Some(hir::CustomBinaryOperator {
            function: hir::CatalogObject::new(
                id,
                self.context().snapshot(),
                None,
                Arc::new(function),
            ),
            swap_args: resolved.swap_args,
            negate: resolved.negate,
            literal_encoding,
        }))
    }

    fn custom_operator_column(
        &self,
        expression: &ResolvedScopeExpr,
    ) -> Result<Option<CustomOperatorColumn>> {
        let hir::Expr::Column(reference) = &expression.expr else {
            return Ok(None);
        };
        let Some(declared) = expression.type_fact.declared.as_ref() else {
            return Ok(None);
        };
        let Some(definition) = declared.custom().cloned() else {
            return Ok(None);
        };
        let source = self.source(reference.source).ok_or_else(|| {
            LimboError::InternalError(format!(
                "custom operator refers to missing source {}",
                reference.source
            ))
        })?;
        // The row a write builds (NEW, or the row an UPDATE writes) has the
        // table's columns, so its custom types keep their operators there.
        if !matches!(
            &source.kind,
            hir::SourceKind::Table(_)
                | hir::SourceKind::TableFunction { .. }
                | hir::SourceKind::Pseudo { .. }
        ) {
            return Ok(None);
        }
        let encoder = if definition.value().encode().is_some() {
            let programs = source
                .column_type_programs
                .get(reference.column)
                .and_then(Option::as_ref)
                .ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "custom column {}.{} has no bound type programs",
                        reference.source, reference.column
                    ))
                })?;
            Some(programs.encode.first().cloned().ok_or_else(|| {
                LimboError::InternalError(format!(
                    "custom column {}.{} has no bound leaf encoder",
                    reference.source, reference.column
                ))
            })?)
        } else {
            None
        };
        Ok(Some(CustomOperatorColumn {
            declared_name: declared.name.clone(),
            definition,
            encoder,
        }))
    }

    fn resolve_sequence_operation(
        &mut self,
        function: &Func,
        input: &FunctionInput,
    ) -> Result<Option<(hir::FunctionOperation, hir::TypeFact)>> {
        let kind = match function {
            Func::Scalar(ScalarFunc::NextVal) => hir::SequenceOperationKind::NextValue,
            Func::Scalar(ScalarFunc::SetVal) => hir::SequenceOperationKind::SetValue,
            _ => return Ok(None),
        };
        let user_name =
            string_literal_argument(&input.facts()[0], "expected a string literal argument")?;
        let (database, normalized_name) = match user_name.split_once('.') {
            Some((schema, name)) => {
                let schema = normalize_ident(schema);
                let database = self.context().database(&schema).ok_or_else(|| {
                    LimboError::InvalidArgument(format!("no such database: {schema}"))
                })?;
                (database, normalize_ident(name))
            }
            None => {
                let name = normalize_ident(&user_name);
                let database = self
                    .context()
                    .resolve_existing_sequence_database_id(&name)
                    .unwrap_or(crate::MAIN_DB_ID);
                (hir::DatabaseId::new(database), name)
            }
        };
        let backing_table_name =
            crate::translate::sequence::sequence_backing_table_name(&normalized_name);
        let (backing_table, sequence, sqlite_sequence) = {
            let schema = self.context().schema(database)?;
            let backing_table = schema.get_table(&backing_table_name).ok_or_else(|| {
                LimboError::ParseError(format!("sequence \"{user_name}\" does not exist"))
            })?;
            let sequence = schema
                .get_sequence(&normalized_name)
                .cloned()
                .ok_or_else(|| {
                    LimboError::ParseError(format!("sequence \"{user_name}\" does not exist"))
                })?;
            let sqlite_sequence = normalized_name
                .starts_with(AUTOINCREMENT_SEQ_PREFIX)
                .then(|| schema.get_table(SQLITE_SEQUENCE_TABLE_NAME))
                .flatten();
            (backing_table, sequence, sqlite_sequence)
        };
        let backing_table_id =
            self.catalog_object_id(Some(database), CatalogObjectKind::Table, backing_table_name);
        let backing_table = hir::CatalogObject::new(
            backing_table_id,
            self.context().snapshot(),
            Some(database),
            backing_table,
        );
        let sequence_id = self.catalog_object_id(
            Some(database),
            CatalogObjectKind::Sequence,
            normalized_name.clone(),
        );
        let sequence = hir::CatalogObject::new(
            sequence_id,
            self.context().snapshot(),
            Some(database),
            sequence,
        );
        let sqlite_sequence = sqlite_sequence.map(|table| {
            let id = self.catalog_object_id(
                Some(database),
                CatalogObjectKind::Table,
                SQLITE_SEQUENCE_TABLE_NAME,
            );
            hir::CatalogObject::new(id, self.context().snapshot(), Some(database), table)
        });
        Ok(Some((
            hir::FunctionOperation::Sequence(hir::SequenceOperation {
                kind,
                normalized_name,
                sequence,
                backing_table,
                sqlite_sequence,
            }),
            hir::TypeFact::known(Type::Integer),
        )))
    }

    fn resolve_named_type_fact(&mut self, name: &str) -> Result<hir::TypeFact> {
        if self.context().custom_types_enabled() {
            if let Some(resolved) = self.context().main_schema().resolve_type_unchecked(name)? {
                return Ok(self.freeze_type_fact(name, resolved, 0).0);
            }
        }
        let affinity = Affinity::affinity(name);
        Ok(hir::TypeFact::declared(hir::DeclaredType {
            name: name.to_string(),
            storage: affinity.to_type(),
            custom_chain: Vec::new(),
            array_dimensions: 0,
        }))
    }

    pub(super) fn resolve_atomic_expr(
        &self,
        expression: hir::Expr,
        scope: &Scope,
    ) -> Result<ResolvedScopeExpr> {
        match expression {
            hir::Expr::Literal(literal) => Ok(ResolvedScopeExpr {
                type_fact: super::analyze::literal_type_fact(&literal)?,
                expr: hir::Expr::Literal(literal),
                affinity: Affinity::Blob,
                has_affinity: false,
                collation: ExprCollation::Absent,
            }),
            hir::Expr::Parameter(parameter) => Ok(ResolvedScopeExpr {
                type_fact: parameter.type_fact.clone(),
                expr: hir::Expr::Parameter(parameter),
                affinity: Affinity::Blob,
                has_affinity: false,
                collation: ExprCollation::Absent,
            }),
            hir::Expr::Column(reference) => {
                let source = self.source(reference.source).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "missing semantic source {}",
                        reference.source
                    ))
                })?;
                let column = source.columns.get(reference.column).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "missing semantic column {}.{}",
                        reference.source, reference.column
                    ))
                })?;
                Ok(ResolvedScopeExpr {
                    expr: hir::Expr::Column(reference),
                    type_fact: column.type_fact().into_owned(),
                    affinity: column.affinity(),
                    has_affinity: column.has_affinity(),
                    collation: ExprCollation::column(
                        column.collation().cloned(),
                        &self.binary_collation,
                    ),
                })
            }
            hir::Expr::MergedColumn(merged) => Ok(ResolvedScopeExpr {
                type_fact: merged.type_fact.clone(),
                affinity: merged.affinity,
                has_affinity: merged.has_affinity,
                collation: ExprCollation::column(merged.collation.clone(), &self.binary_collation),
                expr: hir::Expr::MergedColumn(merged),
            }),
            hir::Expr::RowId(source) => Ok(ResolvedScopeExpr {
                expr: hir::Expr::RowId(source),
                type_fact: hir::TypeFact::known(Type::Integer),
                affinity: Affinity::Integer,
                has_affinity: true,
                collation: ExprCollation::Absent,
            }),
            hir::Expr::Output(output) => {
                let type_fact = scope.output_type(output).cloned().ok_or_else(|| {
                    LimboError::InternalError(format!("missing output facts for {output:?}"))
                })?;
                let affinity = scope.output_affinity(output).ok_or_else(|| {
                    LimboError::InternalError(format!("missing output affinity for {output:?}"))
                })?;
                let has_affinity = scope.output_has_affinity(output).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "missing output affinity state for {output:?}"
                    ))
                })?;
                let collation = scope.output_collation(output).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "missing output collation state for {output:?}"
                    ))
                })?;
                Ok(ResolvedScopeExpr {
                    expr: hir::Expr::Output(output),
                    type_fact,
                    affinity,
                    has_affinity,
                    collation,
                })
            }
            _ => super::analyze::unsupported_select(),
        }
    }

    pub(super) fn resolve_collation(&mut self, name: &ast::Name) -> Result<hir::ResolvedCollation> {
        let collation = match self.context().symbols().resolve_collation(name.as_str()) {
            Some(collation) => collation,
            None => CollationSeq::new(name.as_str())?,
        };
        let id = self.catalog_object_id(None, CatalogObjectKind::Collation, collation.to_string());
        Ok(hir::CatalogObject::new(
            id,
            self.context().snapshot(),
            None,
            Arc::new(collation),
        ))
    }

    fn resolve_cast_target(
        &mut self,
        syntax: Option<&ast::Type>,
        parameters: Vec<ResolvedScopeExpr>,
    ) -> Result<hir::TypeName> {
        let parameter_expressions = parameters
            .iter()
            .map(|parameter| parameter.expr.clone())
            .collect();
        let Some(syntax) = syntax else {
            return Ok(hir::TypeName {
                name: String::new(),
                parameters: parameter_expressions,
                array_dimensions: 0,
                type_fact: hir::TypeFact::dynamic(),
                affinity: Affinity::Numeric,
                programs: builtin_cast_programs(),
            });
        };
        // SQLite dequotes the CAST type name before reading its affinity, so
        // `CAST(x AS "BLOB" INTEGER)` casts to the type named BLOB.
        let name = crate::util::dequote(&syntax.name);
        if self.context().custom_types_enabled() {
            if let Some(resolved) = self.context().main_schema().resolve_type_unchecked(&name)? {
                if resolved.leaf().user_params().count() != parameters.len() {
                    return Ok(builtin_cast_target(
                        &name,
                        syntax.array_dimensions,
                        parameter_expressions,
                    ));
                }
                let (type_fact, affinity) =
                    self.freeze_type_fact(&name, resolved, syntax.array_dimensions);
                let custom_chain = type_fact
                    .declared
                    .as_ref()
                    .expect("resolved custom type has a declaration")
                    .custom_chain
                    .clone();
                let mut encode = Vec::new();
                for definition in &custom_chain {
                    if let Some(call) = self.bind_type_encoder(definition, &parameters)? {
                        encode.push(call);
                    }
                }
                let domain = custom_chain
                    .first()
                    .filter(|definition| definition.value().is_domain)
                    .map(|_| self.bind_domain_constraints(&type_fact, &custom_chain))
                    .transpose()?;
                return Ok(hir::TypeName {
                    name,
                    parameters: parameter_expressions,
                    array_dimensions: syntax.array_dimensions,
                    type_fact,
                    affinity,
                    programs: hir::BoundCastPrograms {
                        encode,
                        domain,
                        apply_builtin_affinity: false,
                    },
                });
            }
        }
        Ok(builtin_cast_target(
            &name,
            syntax.array_dimensions,
            parameter_expressions,
        ))
    }

    pub(super) fn freeze_type_fact(
        &mut self,
        name: &str,
        resolved: crate::schema::ResolvedType,
        array_dimensions: u32,
    ) -> (hir::TypeFact, Affinity) {
        let database = hir::DatabaseId::new(crate::MAIN_DB_ID);
        let affinity = Affinity::affinity(&resolved.primitive);
        let custom_chain = resolved
            .chain
            .into_iter()
            .map(|definition| {
                let id = self.catalog_object_id(
                    Some(database),
                    CatalogObjectKind::Type,
                    crate::util::normalize_ident(&definition.name),
                );
                hir::CatalogObject::new(id, self.context().snapshot(), Some(database), definition)
            })
            .collect();
        (
            hir::TypeFact::declared(hir::DeclaredType {
                name: name.to_string(),
                storage: affinity.to_type(),
                custom_chain,
                array_dimensions,
            }),
            affinity,
        )
    }
}

/// When one side of a comparison is declared as an array, such as a `text[]`
/// column, and the other side is not, the other side is read as an array of
/// the same element type, the way a write to that column reads it: a text
/// literal is parsed, a record blob is checked. The two sides then compare
/// as arrays, so `tags = '{z}'` matches the row whose `tags` holds `{z}`,
/// with or without an index. A NULL literal is left alone, so `IS NULL`
/// keeps its own plan. Arrays of a custom type encode their elements on
/// write, which a comparison does not do, so they are left alone too.
fn read_other_side_as_array(
    lhs: ResolvedScopeExpr,
    rhs: ResolvedScopeExpr,
) -> (ResolvedScopeExpr, ResolvedScopeExpr) {
    fn declared_array(operand: &ResolvedScopeExpr) -> Option<Arc<hir::DeclaredType>> {
        operand
            .type_fact
            .declared
            .as_ref()
            .filter(|declared| declared.array_dimensions > 0 && declared.custom_chain.is_empty())
            .cloned()
    }
    fn is_plain_scalar(operand: &ResolvedScopeExpr) -> bool {
        !operand.type_fact.is_array()
            && !operand.type_fact.array_rank_unbounded
            && !matches!(operand.expr, hir::Expr::Literal(ast::Literal::Null))
    }
    match (declared_array(&lhs), declared_array(&rhs)) {
        (Some(declared), None) if is_plain_scalar(&rhs) => {
            let rhs = read_as_array(rhs, &declared);
            (lhs, rhs)
        }
        (None, Some(declared)) if is_plain_scalar(&lhs) => {
            let lhs = read_as_array(lhs, &declared);
            (lhs, rhs)
        }
        _ => (lhs, rhs),
    }
}

/// `value` cast to the array type `declared` names.
fn read_as_array(value: ResolvedScopeExpr, declared: &hir::DeclaredType) -> ResolvedScopeExpr {
    let target = builtin_cast_target(&declared.name, declared.array_dimensions, Vec::new());
    let type_fact = target.type_fact.clone();
    computed_expr(
        hir::Expr::Cast {
            expr: Box::new(value.expr),
            target: Box::new(target),
        },
        type_fact,
        value.collation,
    )
}

/// The CAST target for the built-in type `name`, already dequoted.
fn builtin_cast_target(
    name: &str,
    array_dimensions: u32,
    parameters: Vec<hir::Expr>,
) -> hir::TypeName {
    let affinity = Affinity::of_written_type(name);
    let type_fact = hir::TypeFact::declared(hir::DeclaredType {
        name: name.to_string(),
        storage: affinity.to_type(),
        custom_chain: Vec::new(),
        array_dimensions,
    });
    hir::TypeName {
        name: name.to_string(),
        parameters,
        array_dimensions,
        type_fact,
        affinity,
        programs: builtin_cast_programs(),
    }
}

fn custom_argument_type(
    argument: &ResolvedScopeExpr,
    expected: impl FnOnce(&crate::schema::TypeDef) -> bool,
) -> Option<hir::ResolvedType> {
    argument
        .type_fact
        .declared
        .as_ref()?
        .custom()
        .filter(|definition| expected(definition.value()))
        .cloned()
}

fn custom_operator_symbol(operator: ast::Operator) -> Option<&'static str> {
    match operator {
        ast::Operator::Add => Some("+"),
        ast::Operator::Subtract => Some("-"),
        ast::Operator::Multiply => Some("*"),
        ast::Operator::Divide => Some("/"),
        ast::Operator::Modulus => Some("%"),
        ast::Operator::Less => Some("<"),
        ast::Operator::LessEquals => Some("<="),
        ast::Operator::Greater => Some(">"),
        ast::Operator::GreaterEquals => Some(">="),
        ast::Operator::Equals => Some("="),
        ast::Operator::NotEquals => Some("!="),
        _ => None,
    }
}

fn resolve_custom_operator(
    definition: &crate::schema::TypeDef,
    operator: ast::Operator,
) -> Option<CustomOperatorDefinition> {
    let symbol = custom_operator_symbol(operator)?;
    if let Some(direct) = definition
        .operators()
        .iter()
        .find(|candidate| candidate.op == symbol)
    {
        return direct
            .func_name
            .as_ref()
            .map(|function_name| CustomOperatorDefinition {
                function_name: function_name.clone(),
                swap_args: false,
                negate: false,
            });
    }

    let derived = match operator {
        ast::Operator::Greater => ("<", true, false),
        ast::Operator::GreaterEquals => ("<", false, true),
        ast::Operator::LessEquals => ("<", true, true),
        ast::Operator::NotEquals => ("=", false, true),
        _ => return None,
    };
    let function_name = definition
        .operators()
        .iter()
        .find(|candidate| candidate.op == derived.0)?
        .func_name
        .clone()?;
    Some(CustomOperatorDefinition {
        function_name,
        swap_args: derived.1,
        negate: derived.2,
    })
}

fn literal_matches_custom_type(
    expression: &ResolvedScopeExpr,
    definition: &crate::schema::TypeDef,
) -> bool {
    let hir::Expr::Literal(literal) = &expression.expr else {
        return false;
    };
    let literal_type = match literal {
        ast::Literal::Numeric(value) => {
            if value
                .chars()
                .any(|character| matches!(character, '.' | 'e' | 'E'))
            {
                "real"
            } else {
                "integer"
            }
        }
        ast::Literal::String(_) => "text",
        ast::Literal::Blob(_) => "blob",
        ast::Literal::True | ast::Literal::False => "integer",
        _ => return false,
    };
    let input_type = definition.value_input_type();
    input_type.eq_ignore_ascii_case("any") || input_type.eq_ignore_ascii_case(literal_type)
}

fn string_literal_argument(argument: &ResolvedScopeExpr, error: &str) -> Result<String> {
    match &argument.expr {
        hir::Expr::Literal(ast::Literal::String(value)) => Ok(value.trim_matches('\'').to_string()),
        _ => Err(LimboError::ParseError(error.to_string())),
    }
}

fn validate_special_function_syntax(syntax: &ast::Expr) -> Result<()> {
    let ast::Expr::FunctionCall {
        name,
        args,
        order_by,
        within_group,
        filter_over,
        ..
    } = syntax
    else {
        return Ok(());
    };
    let function = normalize_ident(name.as_str());
    let valid_sequence_arity = match function.as_str() {
        "nextval" => args.len() == 1,
        "setval" => matches!(args.len(), 2 | 3),
        _ => false,
    };
    if valid_sequence_arity {
        if !matches!(
            args[0].as_ref(),
            ast::Expr::Literal(ast::Literal::String(_))
        ) {
            crate::bail_parse_error!("expected a string literal argument");
        }
        return Ok(());
    }
    let expected_arguments = match function.as_str() {
        "union_tag" => 1,
        "union_value" | "union_extract" | "struct_extract" => 2,
        _ => return Ok(()),
    };
    if args.len() != expected_arguments {
        crate::bail_parse_error!(
            "{}() requires exactly {} argument{}",
            function,
            expected_arguments,
            if expected_arguments == 1 { "" } else { "s" }
        );
    }
    if !order_by.is_empty() || !within_group.is_empty() {
        crate::bail_parse_error!("ORDER BY is not allowed for scalar function {}()", function);
    }
    if filter_over.filter_clause.is_some() || filter_over.over_clause.is_some() {
        crate::bail_parse_error!(
            "{}() may not be used as an aggregate or window function",
            function
        );
    }
    match function.as_str() {
        "union_value"
            if !matches!(
                args[0].as_ref(),
                ast::Expr::Literal(ast::Literal::String(_))
            ) =>
        {
            crate::bail_parse_error!("union_value() first argument must be a string literal");
        }
        "union_extract" | "struct_extract"
            if !matches!(
                args[1].as_ref(),
                ast::Expr::Literal(ast::Literal::String(_))
            ) =>
        {
            crate::bail_parse_error!("{}() second argument must be a string literal", function);
        }
        _ => {}
    }
    Ok(())
}

fn cast_parameter_count(type_name: Option<&ast::Type>) -> usize {
    match type_name.and_then(|type_name| type_name.size.as_ref()) {
        Some(ast::TypeSize::MaxSize(_)) => 1,
        Some(ast::TypeSize::TypeSize(_, _)) => 2,
        None => 0,
    }
}

fn builtin_cast_programs() -> hir::BoundCastPrograms {
    hir::BoundCastPrograms {
        encode: Vec::new(),
        domain: None,
        apply_builtin_affinity: true,
    }
}

fn expect_no_expr_children(children: ExprChildren) -> Result<()> {
    if children.is_empty() {
        Ok(())
    } else {
        Err(LimboError::InternalError(format!(
            "leaf expression produced {} child values",
            children.len()
        )))
    }
}

fn expect_no_frame_children(children: FrameChildren) -> Result<()> {
    if children.is_empty() {
        Ok(())
    } else {
        Err(LimboError::InternalError(format!(
            "leaf expression produced {} child values",
            children.len()
        )))
    }
}

fn scalar_frame_children(children: FrameChildren) -> Result<ExprChildren> {
    let mut scalars = ExprChildren::with_capacity(children.len());
    for child in children {
        scalars.push(child.into_scalar()?);
    }
    Ok(scalars)
}

fn expect_expr_children<const N: usize>(children: ExprChildren) -> Result<[ResolvedScopeExpr; N]> {
    if children.len() != N {
        return Err(LimboError::InternalError(format!(
            "expression expected {N} child values, got {}",
            children.len()
        )));
    }
    let mut children = children.into_iter();
    Ok(std::array::from_fn(|_| {
        children.next().expect("expression child count was checked")
    }))
}

fn null_test_expr(inner: ResolvedScopeExpr, is_null: bool) -> ResolvedScopeExpr {
    let expression = if is_null {
        hir::Expr::IsNull(Box::new(inner.expr))
    } else {
        hir::Expr::NotNull(Box::new(inner.expr))
    };
    computed_expr(
        expression,
        hir::TypeFact::known(Type::Integer),
        inner.collation,
    )
}

/// Return TRUE/FALSE only when SQLite treats this syntax as a truth-test
/// keyword. Parentheses and COLLATE do not block recognition; unary `+` does.
fn truth_test_rhs(expression: &ast::Expr) -> Option<bool> {
    match expression {
        ast::Expr::Literal(ast::Literal::True) => Some(true),
        ast::Expr::Literal(ast::Literal::False) => Some(false),
        ast::Expr::Parenthesized(expressions) if expressions.len() == 1 => {
            truth_test_rhs(&expressions[0])
        }
        ast::Expr::Collate(inner, _) => truth_test_rhs(inner),
        _ => None,
    }
}

fn truth_test_expr(
    inner: ResolvedScopeExpr,
    is_true: bool,
    negated: bool,
    collation: ExprCollation,
) -> ResolvedScopeExpr {
    computed_expr(
        hir::Expr::TruthTest {
            expr: Box::new(inner.expr),
            is_true,
            negated,
        },
        hir::TypeFact::known(Type::Integer),
        collation,
    )
}

fn computed_expr(
    expr: hir::Expr,
    type_fact: hir::TypeFact,
    collation: ExprCollation,
) -> ResolvedScopeExpr {
    ResolvedScopeExpr {
        expr,
        type_fact,
        affinity: Affinity::Blob,
        has_affinity: false,
        collation,
    }
}

fn array_expr(elements: ExprChildren) -> ResolvedScopeExpr {
    let type_fact = hir::TypeFact::array_literal_result(
        elements.iter().map(|element| element.type_fact.clone()),
    );
    computed_expr(
        hir::Expr::Array(elements.into_iter().map(|element| element.expr).collect()),
        type_fact,
        ExprCollation::Absent,
    )
}

fn subscript_expr(base: ResolvedScopeExpr, index: ResolvedScopeExpr) -> ResolvedScopeExpr {
    let type_fact = base
        .type_fact
        .array_element()
        .unwrap_or_else(hir::TypeFact::dynamic);
    computed_expr(
        hir::Expr::Subscript {
            base: Box::new(base.expr),
            index: Box::new(index.expr),
        },
        type_fact,
        ExprCollation::Absent,
    )
}

fn field_access_expr(
    base: ResolvedScopeExpr,
    field_name: &str,
    member: ResolvedCustomMember,
) -> ResolvedScopeExpr {
    let result_type = member.result_type;
    computed_expr(
        hir::Expr::FieldAccess(Box::new(hir::FieldAccess {
            base: Box::new(base.expr),
            field_name: normalize_ident(field_name),
            kind: member.kind,
            container_type: member.container_type,
            result_type: result_type.clone(),
        })),
        result_type,
        ExprCollation::Absent,
    )
}

fn effective_window_frame(
    frame: Option<FunctionWindowFrame>,
    order_by_len: usize,
    function: &Func,
) -> Result<hir::WindowFrame> {
    let Some(frame) = frame else {
        return Ok(hir::WindowFrame {
            mode: ast::FrameMode::Range,
            start: hir::WindowFrameBound::UnboundedPreceding,
            end: Some(hir::WindowFrameBound::CurrentRow),
            exclude: None,
        });
    };
    let end = frame.end.unwrap_or(hir::WindowFrameBound::CurrentRow);
    let illegal = matches!(
        (&frame.start, &end),
        (hir::WindowFrameBound::UnboundedFollowing, _)
            | (_, hir::WindowFrameBound::UnboundedPreceding)
            | (
                hir::WindowFrameBound::CurrentRow,
                hir::WindowFrameBound::Preceding(_)
            )
            | (
                hir::WindowFrameBound::Following(_),
                hir::WindowFrameBound::Preceding(_) | hir::WindowFrameBound::CurrentRow
            )
    );
    if illegal {
        crate::bail_parse_error!("unsupported frame specification");
    }
    if frame.mode == ast::FrameMode::Range
        && (matches!(
            &frame.start,
            hir::WindowFrameBound::Preceding(_) | hir::WindowFrameBound::Following(_)
        ) || matches!(
            &end,
            hir::WindowFrameBound::Preceding(_) | hir::WindowFrameBound::Following(_)
        ))
        && order_by_len != 1
    {
        crate::bail_parse_error!(
            "RANGE with offset PRECEDING/FOLLOWING requires one ORDER BY expression"
        );
    }
    let moving_start = !matches!(&frame.start, hir::WindowFrameBound::UnboundedPreceding);
    if moving_start && frame.exclude.is_none() && !function_supports_sliding_frame(function) {
        crate::bail_parse_error!(
            "{}() does not yet support window frames with a moving start; use a frame with UNBOUNDED PRECEDING start",
            function
        );
    }
    Ok(hir::WindowFrame {
        mode: frame.mode,
        start: frame.start,
        end: Some(end),
        exclude: frame.exclude,
    })
}

fn function_supports_sliding_frame(function: &Func) -> bool {
    let supported = matches!(
        function,
        Func::Agg(
            AggFunc::Sum
                | AggFunc::Total
                | AggFunc::Count
                | AggFunc::Count0
                | AggFunc::Avg
                | AggFunc::Min
                | AggFunc::Max
                | AggFunc::GroupConcat
                | AggFunc::StringAgg
        ) | Func::Window(WindowFunc::FirstValue | WindowFunc::NthValue | WindowFunc::LastValue)
    );
    #[cfg(feature = "json")]
    let supported = supported
        || matches!(
            function,
            Func::Agg(
                AggFunc::JsonGroupObject
                    | AggFunc::JsonbGroupObject
                    | AggFunc::JsonGroupArray
                    | AggFunc::JsonbGroupArray
            )
        );
    supported
}

fn coerced_window_frame(function: &WindowFunc) -> Option<hir::WindowFrame> {
    use hir::WindowFrameBound::{CurrentRow, Following, UnboundedFollowing, UnboundedPreceding};
    let (mode, start, end) = match function {
        WindowFunc::RowNumber | WindowFunc::Lag => {
            (ast::FrameMode::Rows, UnboundedPreceding, CurrentRow)
        }
        WindowFunc::Rank | WindowFunc::DenseRank => {
            (ast::FrameMode::Range, UnboundedPreceding, CurrentRow)
        }
        WindowFunc::PercentRank => (ast::FrameMode::Groups, CurrentRow, UnboundedFollowing),
        WindowFunc::CumeDist => (
            ast::FrameMode::Groups,
            Following(Box::new(hir::Expr::Literal(ast::Literal::Numeric(
                "1".to_string(),
            )))),
            UnboundedFollowing,
        ),
        WindowFunc::Ntile => (ast::FrameMode::Rows, CurrentRow, UnboundedFollowing),
        WindowFunc::Lead => (ast::FrameMode::Rows, UnboundedPreceding, UnboundedFollowing),
        WindowFunc::FirstValue | WindowFunc::LastValue | WindowFunc::NthValue => return None,
        WindowFunc::External(_) => {
            unreachable!("WindowFunc::External is not constructible: ExtFunc has no Window variant")
        }
    };
    Some(hir::WindowFrame {
        mode,
        start,
        end: Some(end),
        exclude: None,
    })
}

fn bind_function(
    function: &Func,
    has_over: bool,
    star: bool,
    name: &ast::Name,
    policy: ExprPolicy,
    context: &mut FunctionContext<'_, '_>,
) -> Result<FunctionBinding> {
    // A call written `count(*)` is named that way in its error.
    let arguments = if star { "*" } else { "" };
    let aggregate = matches!(function, Func::Agg(_))
        || matches!(function, Func::External(external) if external.func.is_aggregate());
    if aggregate && !has_over {
        match policy.aggregates {
            AggregatePolicy::Allow => {}
            AggregatePolicy::RejectFunction => {
                crate::bail_parse_error!("misuse of aggregate function {function}({arguments})")
            }
            AggregatePolicy::RejectMisuse => {
                crate::bail_parse_error!("misuse of aggregate: {function}({arguments})")
            }
        }
    }
    if has_over && !policy.allow_windows {
        match function {
            Func::Agg(_) | Func::Window(_) => {
                crate::bail_parse_error!("misuse of window function {function}({arguments})")
            }
            Func::External(external) if external.func.is_aggregate() => {
                crate::bail_parse_error!("misuse of window function {}()", function)
            }
            _ => {
                crate::bail_parse_error!("{} may not be used as a window function", name.as_str())
            }
        }
    }
    match function {
        Func::Agg(_) if has_over => bind_window(function, context),
        Func::Agg(_) => bind_aggregate(function, context),
        Func::External(external) if external.func.is_aggregate() => {
            if has_over {
                crate::bail_parse_error!(
                    "{}() is an extension aggregate and cannot be used as a window function",
                    name.as_str()
                )
            }
            bind_aggregate(function, context)
        }
        Func::Window(_) if has_over => bind_window(function, context),
        Func::Window(window) => {
            crate::bail_parse_error!("misuse of window function {}()", window)
        }
        _ if has_over => {
            crate::bail_parse_error!("{} may not be used as a window function", name.as_str())
        }
        Func::AlterTable(_) => super::analyze::unsupported_select(),
        _ => Ok(FunctionBinding::Scalar),
    }
}

fn bind_window(function: &Func, context: &mut FunctionContext<'_, '_>) -> Result<FunctionBinding> {
    match context {
        FunctionContext::ScalarOnly => {
            crate::bail_parse_error!("misuse of window function {}()", function)
        }
        FunctionContext::Query(state) => Ok(FunctionBinding::Window(WindowBinding {
            id: state.allocate_window(),
        })),
    }
}

fn bind_aggregate(
    function: &Func,
    context: &mut FunctionContext<'_, '_>,
) -> Result<FunctionBinding> {
    match context {
        FunctionContext::ScalarOnly => {
            crate::bail_parse_error!("misuse of aggregate function {}()", function)
        }
        FunctionContext::Query(state) => Ok(FunctionBinding::Aggregate(AggregateBinding {
            block: state.block,
        })),
    }
}

fn resolve_ordered_set_function(
    name: &ast::Name,
    args: &[Box<ast::Expr>],
    distinctness: Option<&ast::Distinctness>,
    argument_order: &[ast::SortedColumn],
    within_group: &[ast::SortedColumn],
    tail: &ast::FunctionTail,
) -> Result<AggFunc> {
    let function = match normalize_ident(name.as_str()).as_str() {
        "mode" => AggFunc::Mode,
        "percentile_cont" => AggFunc::PercentileCont,
        "percentile_disc" => AggFunc::PercentileDisc,
        _ => {
            crate::bail_parse_error!(
                "WITHIN GROUP is not supported for function {}()",
                name.as_str()
            )
        }
    };
    if tail.over_clause.is_some() {
        crate::bail_parse_error!(
            "ordered-set aggregate {}() may not be used as a window function",
            name.as_str()
        );
    }
    if distinctness.is_some() {
        crate::bail_parse_error!(
            "DISTINCT is not supported for ordered-set aggregate {}()",
            name.as_str()
        );
    }
    if !argument_order.is_empty() {
        crate::bail_parse_error!(
            "{}() does not accept an argument ORDER BY together with WITHIN GROUP",
            name.as_str()
        );
    }
    if within_group.len() != 1 {
        crate::bail_parse_error!(
            "WITHIN GROUP for {}() must specify exactly one ORDER BY expression",
            name.as_str()
        );
    }
    let order_by = &within_group[0];
    if matches!(order_by.order, Some(ast::SortOrder::Desc)) || order_by.nulls.is_some() {
        crate::bail_parse_error!(
            "DESC and NULLS ordering inside WITHIN GROUP are not supported yet"
        );
    }
    let expected_direct = match function {
        AggFunc::Mode => 0,
        AggFunc::PercentileCont | AggFunc::PercentileDisc => 1,
        _ => unreachable!("ordered-set function list is exhaustive"),
    };
    if args.len() != expected_direct {
        crate::bail_parse_error!("wrong number of arguments to function {}()", name.as_str());
    }
    Ok(function)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum NestedSearch {
    AggregatesAndWindows,
    WindowsOnly,
}

fn nested_function_iter<'a>(
    arguments: impl IntoIterator<Item = &'a ResolvedScopeExpr>,
    search: NestedSearch,
) -> Option<NestedFunction> {
    arguments
        .into_iter()
        .find_map(|argument| nested_function_in_expr(&argument.expr, search))
}

fn nested_function_in_expr(expression: &hir::Expr, search: NestedSearch) -> Option<NestedFunction> {
    let mut nested = None;
    expression.for_each(&mut |expression| {
        if nested.is_some() {
            return;
        }
        let hir::Expr::Function(call) = expression else {
            return;
        };
        nested = match call.evaluation {
            hir::FunctionEvaluation::Aggregate { .. }
                if search == NestedSearch::AggregatesAndWindows =>
            {
                Some(NestedFunction::Aggregate(call.function.value().to_string()))
            }
            hir::FunctionEvaluation::Aggregate { .. } => None,
            hir::FunctionEvaluation::Window { .. } => {
                Some(NestedFunction::Window(call.function.value().to_string()))
            }
            hir::FunctionEvaluation::Scalar => None,
        };
    });
    nested
}

fn nested_function_in_window_bound(
    bound: &hir::WindowFrameBound,
    search: NestedSearch,
) -> Option<NestedFunction> {
    match bound {
        hir::WindowFrameBound::Following(expression)
        | hir::WindowFrameBound::Preceding(expression) => {
            nested_function_in_expr(expression, search)
        }
        hir::WindowFrameBound::CurrentRow
        | hir::WindowFrameBound::UnboundedFollowing
        | hir::WindowFrameBound::UnboundedPreceding => None,
    }
}

fn validate_raise(action: ast::ResolveType, policy: RaisePolicy) -> Result<()> {
    match (policy, action) {
        (RaisePolicy::AbortOnly, ast::ResolveType::Abort) | (RaisePolicy::Trigger, _) => Ok(()),
        (RaisePolicy::AbortOnly, _) => {
            crate::bail_parse_error!("RAISE() may only be used within a trigger-program")
        }
    }
}

fn trigger_source(policy: ExprPolicy, namespace: &str) -> Result<Option<hir::SourceId>> {
    let Some(trigger) = policy.policies.trigger else {
        return Ok(None);
    };
    let namespace = normalize_ident(namespace);
    match namespace.as_str() {
        "new" => trigger.new_source.map(Some).ok_or_else(|| {
            LimboError::ParseError(
                "NEW references are only valid in INSERT and UPDATE triggers".to_string(),
            )
        }),
        "old" => trigger.old_source.map(Some).ok_or_else(|| {
            LimboError::ParseError(
                "OLD references are only valid in UPDATE and DELETE triggers".to_string(),
            )
        }),
        _ => Ok(None),
    }
}

fn function_result_type(function: &Func, arguments: &[ResolvedScopeExpr]) -> hir::TypeFact {
    match function {
        Func::Scalar(ScalarFunc::ArraySetElement) => hir::TypeFact::array_with_element_result(
            &arguments[0].type_fact,
            &arguments[2].type_fact,
        ),
        Func::Scalar(ScalarFunc::ArrayAppend) => hir::TypeFact::array_with_element_result(
            &arguments[0].type_fact,
            &arguments[1].type_fact,
        ),
        Func::Scalar(ScalarFunc::ArrayPrepend) => hir::TypeFact::array_with_element_result(
            &arguments[1].type_fact,
            &arguments[0].type_fact,
        ),
        Func::Scalar(ScalarFunc::ArrayCat) => {
            hir::TypeFact::array_concat_result(&arguments[0].type_fact, &arguments[1].type_fact)
        }
        Func::Scalar(ScalarFunc::ArrayRemove | ScalarFunc::ArraySlice) => {
            hir::TypeFact::array_transform_result(&arguments[0].type_fact)
        }
        Func::Scalar(ScalarFunc::StringToArray) => hir::TypeFact::known_array(1),
        Func::Scalar(
            ScalarFunc::ArrayLength
            | ScalarFunc::ArrayContains
            | ScalarFunc::ArrayPosition
            | ScalarFunc::ArrayOverlap
            | ScalarFunc::ArrayContainsAll,
        ) => hir::TypeFact::known(Type::Integer),
        Func::Scalar(ScalarFunc::ArrayToString) => hir::TypeFact::known(Type::Text),
        Func::Scalar(
            ScalarFunc::Length | ScalarFunc::NextVal | ScalarFunc::CurrVal | ScalarFunc::SetVal,
        ) => hir::TypeFact::known(Type::Integer),
        Func::Scalar(ScalarFunc::Abs) => {
            arguments
                .first()
                .map_or_else(hir::TypeFact::dynamic, |argument| {
                    match argument.type_fact.storage {
                        Some(Type::Null) => hir::TypeFact::known(Type::Null),
                        Some(Type::Integer) => hir::TypeFact::known(Type::Integer),
                        Some(Type::Real) | Some(Type::Text) | Some(Type::Blob) => {
                            hir::TypeFact::known(Type::Real)
                        }
                        Some(Type::Numeric) => hir::TypeFact::known(Type::Numeric),
                        None => hir::TypeFact::dynamic(),
                    }
                })
        }
        Func::Agg(AggFunc::Count | AggFunc::Count0) => hir::TypeFact::known(Type::Integer),
        Func::Agg(AggFunc::Avg | AggFunc::Total | AggFunc::PercentileCont) => {
            hir::TypeFact::known(Type::Real)
        }
        Func::Agg(AggFunc::Sum) => hir::TypeFact::known(Type::Numeric),
        Func::Agg(AggFunc::Min | AggFunc::Max | AggFunc::Mode | AggFunc::PercentileDisc) => {
            arguments
                .first()
                .map_or_else(hir::TypeFact::dynamic, |argument| {
                    argument.type_fact.clone()
                })
        }
        Func::Agg(AggFunc::GroupConcat | AggFunc::StringAgg) => hir::TypeFact::known(Type::Text),
        Func::Agg(AggFunc::ArrayAgg) => arguments.first().map_or_else(
            || hir::TypeFact::known_array(1),
            |argument| hir::TypeFact::array_literal_result([argument.type_fact.clone()]),
        ),
        Func::Window(
            WindowFunc::RowNumber | WindowFunc::Rank | WindowFunc::DenseRank | WindowFunc::Ntile,
        ) => hir::TypeFact::known(Type::Integer),
        Func::Window(WindowFunc::PercentRank | WindowFunc::CumeDist) => {
            hir::TypeFact::known(Type::Real)
        }
        Func::Window(
            WindowFunc::Lag
            | WindowFunc::Lead
            | WindowFunc::FirstValue
            | WindowFunc::LastValue
            | WindowFunc::NthValue,
        ) => arguments
            .first()
            .map_or_else(hir::TypeFact::dynamic, |argument| {
                argument.type_fact.clone()
            }),
        _ => hir::TypeFact::dynamic(),
    }
}

/// The collation of a comparison of `lhs` with `rhs`: an explicit COLLATE
/// on either side, the left one first; else the column collation of either
/// side, the left one first, where a column without a declared collation
/// counts as BINARY; else none.
fn expression_collation(lhs: &ExprCollation, rhs: &ExprCollation) -> ExprCollation {
    match (lhs, rhs) {
        (ExprCollation::Explicit(collation), _) => ExprCollation::Explicit(collation.clone()),
        (_, ExprCollation::Explicit(collation)) => ExprCollation::Explicit(collation.clone()),
        (ExprCollation::Inherited(collation), _) => ExprCollation::Inherited(collation.clone()),
        (ExprCollation::InheritedBinary(binary), _) => {
            ExprCollation::InheritedBinary(binary.clone())
        }
        (_, ExprCollation::Inherited(collation)) => ExprCollation::Inherited(collation.clone()),
        (_, ExprCollation::InheritedBinary(binary)) => {
            ExprCollation::InheritedBinary(binary.clone())
        }
        (ExprCollation::Absent, ExprCollation::Absent) => ExprCollation::Absent,
    }
}

fn binary_type_fact(
    operator: ast::Operator,
    lhs: &hir::TypeFact,
    rhs: &hir::TypeFact,
) -> hir::TypeFact {
    use ast::Operator;

    match operator {
        Operator::Add | Operator::Subtract | Operator::Multiply | Operator::Divide => {
            hir::TypeFact::arithmetic_result(lhs, rhs)
        }
        Operator::Concat => hir::TypeFact::concat_result(lhs, rhs),
        Operator::ArrowRight | Operator::ArrowRightShift => hir::TypeFact::dynamic(),
        Operator::Modulus
        | Operator::And
        | Operator::Or
        | Operator::BitwiseAnd
        | Operator::BitwiseOr
        | Operator::BitwiseNot
        | Operator::LeftShift
        | Operator::RightShift
        | Operator::Equals
        | Operator::NotEquals
        | Operator::Less
        | Operator::LessEquals
        | Operator::Greater
        | Operator::GreaterEquals
        | Operator::Is
        | Operator::IsNot
        | Operator::ArrayContains
        | Operator::ArrayOverlap => hir::TypeFact::known(Type::Integer),
    }
}

fn comparison_semantics(
    lhs: &ResolvedScopeExpr,
    rhs: &ResolvedScopeExpr,
) -> hir::ComparisonSemantics {
    expression_operand_comparison(std::slice::from_ref(lhs), std::slice::from_ref(rhs))
}

fn expression_operand_comparison(
    lhs: &[ResolvedScopeExpr],
    rhs: &[ResolvedScopeExpr],
) -> hir::ComparisonSemantics {
    assert_eq!(
        lhs.len(),
        rhs.len(),
        "resolved comparison operands have equal widths"
    );
    hir::ComparisonSemantics {
        components: lhs
            .iter()
            .zip(rhs)
            .map(|(lhs, rhs)| comparison_component(lhs, rhs))
            .collect(),
    }
}

fn comparison_component(
    lhs: &ResolvedScopeExpr,
    rhs: &ResolvedScopeExpr,
) -> hir::ComparisonComponent {
    // Like SQLite's sqlite3CompareAffinity, a numeric affinity on
    // either side compares as NUMERIC, even alone: REAL affinity would turn a
    // large integer into a float and lose its exact value.
    let lone = |affinity: Affinity| {
        if affinity.is_numeric() {
            Affinity::Numeric
        } else {
            affinity
        }
    };
    let affinity = match (lhs.has_affinity, rhs.has_affinity) {
        (true, true) if lhs.affinity.is_numeric() || rhs.affinity.is_numeric() => Affinity::Numeric,
        (true, true) => Affinity::Blob,
        (true, false) => lone(lhs.affinity),
        (false, true) => lone(rhs.affinity),
        (false, false) => Affinity::Blob,
    };
    hir::ComparisonComponent {
        affinity,
        collation: expression_collation(&lhs.collation, &rhs.collation)
            .comparison_value()
            .cloned(),
        array: lhs.type_fact.is_array() && rhs.type_fact.is_array(),
    }
}

pub(super) fn build_using_column(
    name: String,
    left: ResolvedScopeExpr,
    right: ResolvedScopeExpr,
    value: hir::MergedColumnValue,
) -> Result<hir::UsingColumn> {
    let hir::Expr::Column(right_reference) = &right.expr else {
        return Err(LimboError::InternalError(
            "USING right side is not a source column".to_string(),
        ));
    };
    let right_reference = *right_reference;
    let comparison = comparison_semantics(&left, &right);
    let (type_fact, affinity, has_affinity, collation) = match value {
        hir::MergedColumnValue::Left => (
            left.type_fact.clone(),
            left.affinity,
            left.has_affinity,
            left.collation.value().cloned(),
        ),
        hir::MergedColumnValue::Right => (
            right.type_fact.clone(),
            right.affinity,
            right.has_affinity,
            right.collation.value().cloned(),
        ),
        hir::MergedColumnValue::Coalesce => (
            hir::TypeFact::selected_value_result([&left.type_fact, &right.type_fact]),
            Affinity::Blob,
            false,
            None,
        ),
    };

    Ok(hir::UsingColumn {
        name,
        left: Box::new(left.expr),
        right: right_reference,
        value,
        type_fact,
        affinity,
        has_affinity,
        collation,
        comparison,
    })
}

fn in_operand_comparison(
    lhs: &[ResolvedScopeExpr],
    rhs: &[ResolvedScopeExpr],
) -> hir::ComparisonSemantics {
    assert_eq!(
        lhs.len(),
        rhs.len(),
        "resolved IN operands have equal widths"
    );
    hir::ComparisonSemantics {
        components: lhs
            .iter()
            .zip(rhs)
            .map(|(lhs, rhs)| hir::ComparisonComponent {
                affinity: lhs.affinity,
                collation: expression_collation(&lhs.collation, &rhs.collation)
                    .comparison_value()
                    .cloned(),
                array: lhs.type_fact.is_array() && rhs.type_fact.is_array(),
            })
            .collect(),
    }
}

pub(crate) fn supports_row_value_binary_comparison(operator: ast::Operator) -> bool {
    matches!(
        operator,
        ast::Operator::Equals
            | ast::Operator::NotEquals
            | ast::Operator::Less
            | ast::Operator::LessEquals
            | ast::Operator::Greater
            | ast::Operator::GreaterEquals
            | ast::Operator::Is
            | ast::Operator::IsNot
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        dialect::SqliteDialect,
        schema::{Schema, Type},
        sync::Arc,
        translate::semantic::{
            context::{DoubleQuotedDml, SemanticContext},
            hir::{
                DatabaseId, Expr, IndexCoverage, IndexHint, Source, SourceColumn, SourceId,
                SourceKind, SourceOwner, TypeFact,
            },
            scope::Scope,
        },
        vdbe::affinity::Affinity,
        SymbolTable, MAIN_DB_ID,
    };
    use turso_parser::parser::Parser;

    #[test]
    fn expression_values_stay_small() {
        // Every expression is moved by value while it is built, planned and
        // lowered, and a frame of the expression builder holds a few of
        // them inline. A call, a field access, a merged column, a cast's
        // type name and a custom operator are boxed to keep them small.
        assert!(std::mem::size_of::<hir::Expr>() <= 96);
        assert!(std::mem::size_of::<ResolvedScopeExpr>() <= 216);
        assert!(std::mem::size_of::<FrameValue>() <= 216);
        assert!(std::mem::size_of::<ExprFrame<'static>>() <= 696);
    }

    thread_local! {
        /// How many operand widths were computed on this thread.
        pub(super) static OPERAND_WIDTH_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }

    #[test]
    fn in_list_frame_finds_each_child_without_reading_the_whole_list() {
        let literal =
            |value: usize| Box::new(ast::Expr::Literal(ast::Literal::Numeric(value.to_string())));
        let elements = 1_000_usize;
        let syntax = ast::Expr::InList {
            lhs: literal(0),
            not: false,
            rhs: (1..=elements).map(literal).collect(),
        };
        let schema = Schema::new();
        let mut frame = ExprFrame::new(&syntax, None, 0).expect("IN list frame");
        OPERAND_WIDTH_CALLS.with(|calls| calls.set(0));
        let mut children = Vec::new();
        while let Some(child) = frame.next_child(&schema).expect("IN list children") {
            children.push(child.syntax.to_string());
        }
        let width_calls = OPERAND_WIDTH_CALLS.with(|calls| calls.get());

        assert_eq!(children.len(), elements + 1);
        assert_eq!(children[0], "0");
        assert_eq!(children[elements], elements.to_string());
        // Each child costs a bounded amount of work, not a walk of the list
        // from its start.
        assert!(
            width_calls <= 4 * (elements + 1),
            "finding {} children computed {width_calls} operand widths",
            elements + 1
        );
    }

    fn expression(sql: &str) -> ast::Expr {
        let ast::Cmd::Stmt(ast::Stmt::Select(select)) = Parser::new(sql.as_bytes())
            .next_cmd()
            .expect("SQL parses")
            .expect("SQL contains statement")
        else {
            panic!("SQL contains SELECT");
        };
        let ast::OneSelect::Select { columns, .. } = select.body.select else {
            panic!("SQL contains SELECT body");
        };
        let ast::ResultColumn::Expr(expression, _) = &columns[0] else {
            panic!("SELECT has expression output");
        };
        expression.as_ref().clone()
    }

    fn source() -> Source {
        Source {
            id: SourceId::new(0),
            owner: SourceOwner::Root,
            database: Some(DatabaseId::new(MAIN_DB_ID)),
            name: "items".to_string(),
            alias: None,
            kind: SourceKind::SchemaExpression,
            columns: vec![SourceColumn {
                name: "value".into(),
                type_fact: TypeFact::known(Type::Text),
                affinity: Affinity::Text,
                has_affinity: true,
                collation: None,
                hidden: false,
                rowid_alias: false,
            }]
            .into(),
            generated_expressions: hir::ColumnSlots::empty(1),
            default_expressions: hir::ColumnSlots::empty(1),
            column_type_programs: hir::ColumnSlots::empty(1),
            check_constraints: None,
            rowid_available: true,
            index_hint: IndexHint::None,
            index_expressions: Vec::new(),
            index_coverage: IndexCoverage::Selective,
            index_method_patterns: Vec::new(),
        }
    }

    fn analyze_expression(syntax: &ast::Expr, scope: &Scope, policy: ExprPolicy) -> Result<Expr> {
        let schema = Schema::new();
        let symbols = SymbolTable::new();
        let context = SemanticContext::for_main_schema_object(
            &schema,
            &symbols,
            true,
            Arc::new(SqliteDialect),
        );
        let mut analyzer = Analyzer::new(&context);
        let source_id = analyzer.reserve_source();
        analyzer.insert_source(source_id, source())?;
        analyzer.analyze_expr(syntax, scope, policy)
    }

    fn expect_column(expression: Expr) {
        assert!(matches!(
            expression,
            Expr::Column(column) if column.source == SourceId::new(0) && column.column == 0
        ));
    }

    #[test]
    fn empty_in_lists_keep_negation_without_comparisons() {
        for negated in [false, true] {
            let syntax = ast::Expr::InList {
                lhs: Box::new(expression("SELECT 1")),
                not: negated,
                rhs: Vec::new(),
            };
            let expression = analyze_expression(
                &syntax,
                &Scope::default(),
                ExprPolicy::select(DoubleQuotedDml::Enabled),
            )
            .expect("empty IN list binds");
            let Expr::InList {
                negated: actual,
                values,
                comparisons,
                ..
            } = expression
            else {
                panic!("empty IN list becomes HIR");
            };
            assert_eq!(actual, negated);
            assert!(values.is_empty());
            assert!(comparisons.is_empty());
        }
    }

    #[test]
    fn cast_parameters_bind_before_input_expression() {
        let syntax = ast::Expr::Cast {
            expr: Box::new(ast::Expr::Id(ast::Name::exact("missing_input".to_string()))),
            type_name: Some(ast::Type {
                name: "DECIMAL".to_string(),
                size: Some(ast::TypeSize::MaxSize(Box::new(ast::Expr::Id(
                    ast::Name::exact("missing_parameter".to_string()),
                )))),
                array_dimensions: 0,
            }),
        };
        let error = analyze_expression(
            &syntax,
            &Scope::default(),
            ExprPolicy::select(DoubleQuotedDml::Enabled),
        )
        .expect_err("first unresolved CAST child fails");
        assert_eq!(
            error.to_string(),
            "Parse error: no such column: missing_parameter"
        );
    }

    #[test]
    fn is_true_and_false_become_explicit_hir_truth_tests() {
        for (sql, expected_is_true, expected_negated) in [
            ("SELECT 2 IS TRUE", true, false),
            ("SELECT 2 IS FALSE", false, false),
            ("SELECT 2 IS NOT TRUE", true, true),
            ("SELECT 2 IS NOT FALSE", false, true),
            ("SELECT 2 IS (TRUE)", true, false),
        ] {
            let analyzed = analyze_expression(
                &expression(sql),
                &Scope::default(),
                ExprPolicy::select(DoubleQuotedDml::Enabled),
            )
            .expect("truth test binds");
            assert!(matches!(
                analyzed,
                Expr::TruthTest {
                    expr,
                    is_true,
                    negated,
                } if matches!(*expr, Expr::Literal(ast::Literal::Numeric(ref value)) if value == "2")
                    && is_true == expected_is_true
                    && negated == expected_negated
            ));
        }

        let unary_true = analyze_expression(
            &expression("SELECT 2 IS +TRUE"),
            &Scope::default(),
            ExprPolicy::select(DoubleQuotedDml::Enabled),
        )
        .expect("unary plus blocks truth-test recognition");
        assert!(matches!(
            unary_true,
            Expr::Binary {
                operator: ast::Operator::Is,
                ..
            }
        ));
    }

    #[test]
    fn identifier_forms_become_hir_column_references() {
        let source = source();
        let mut scope = Scope::default();
        scope.add_source(&source, true);

        for sql in [
            "SELECT value",
            "SELECT items.value",
            "SELECT main.items.value",
        ] {
            expect_column(
                analyze_expression(
                    &expression(sql),
                    &scope,
                    ExprPolicy::select(DoubleQuotedDml::Enabled),
                )
                .expect("name resolves"),
            );
        }

        expect_column(
            analyze_expression(
                &ast::Expr::Name(ast::Name::exact("value".to_string())),
                &scope,
                ExprPolicy::select(DoubleQuotedDml::Enabled),
            )
            .expect("Name node follows identifier rules"),
        );
    }

    #[test]
    fn qualified_names_report_the_missing_column_as_sqlite_does() {
        let source = source();
        let mut scope = Scope::default();
        scope.add_source(&source, true);

        let missing_table = analyze_expression(
            &expression("SELECT absent.value"),
            &scope,
            ExprPolicy::select(DoubleQuotedDml::Enabled),
        )
        .expect_err("unknown qualifier fails");
        assert_eq!(
            missing_table.to_string(),
            "Parse error: no such column: absent.value"
        );

        let missing_column = analyze_expression(
            &expression("SELECT items.absent"),
            &scope,
            ExprPolicy::select(DoubleQuotedDml::Enabled),
        )
        .expect_err("known qualifier with unknown column fails");
        assert_eq!(
            missing_column.to_string(),
            "Parse error: no such column: items.absent"
        );
    }

    #[test]
    fn scalar_only_context_rejects_aggregate_functions() {
        let error = analyze_expression(
            &expression("SELECT sum(1)"),
            &Scope::default(),
            ExprPolicy::select(DoubleQuotedDml::Enabled),
        )
        .expect_err("schema-style expression context rejects aggregates");
        assert_eq!(
            error.to_string(),
            "Parse error: misuse of aggregate function sum()"
        );
    }

    #[test]
    fn statement_and_internal_functions_are_rejected_during_semantic_analysis() {
        let input = FunctionInput::Expressions {
            distinctness: None,
            values: ExprChildren::new(),
            order_by: Vec::new(),
        };
        for (function, expected) in [
            (
                ScalarFunc::Attach,
                "ATTACH should be handled at statement level, not as expression",
            ),
            (
                ScalarFunc::Detach,
                "DETACH should be handled at statement level, not as expression",
            ),
            (
                ScalarFunc::StatInit,
                "stat_init is an internal function used by ANALYZE",
            ),
            (
                ScalarFunc::StatPush,
                "stat_push is an internal function used by ANALYZE",
            ),
            (
                ScalarFunc::StatGet,
                "stat_get is an internal function used by ANALYZE",
            ),
            (
                ScalarFunc::ConnTxnId,
                "conn_txn_id is an internal function used by CDC",
            ),
            (
                ScalarFunc::IsAutocommit,
                "is_autocommit is an internal function used by CDC",
            ),
        ] {
            let error = validate_scalar_arguments(&Func::Scalar(function), &input)
                .expect_err("statement-only or internal function is rejected");
            assert_eq!(error.to_string(), format!("Parse error: {expected}"));
        }

        for (sql, expected) in [
            (
                "SELECT conn_txn_id()",
                "Parse error: conn_txn_id is an internal function used by CDC",
            ),
            (
                "SELECT is_autocommit()",
                "Parse error: is_autocommit is an internal function used by CDC",
            ),
        ] {
            let error = analyze_expression(
                &expression(sql),
                &Scope::default(),
                ExprPolicy::select(DoubleQuotedDml::Enabled),
            )
            .expect_err("publicly resolvable internal function is rejected");
            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn fixed_unary_argument_rules_are_checked_during_semantic_analysis() {
        for function in [
            "abs",
            "lower",
            "upper",
            "length",
            "octet_length",
            "typeof",
            "unicode",
            "unistr",
            "unistr_quote",
            "quote",
            "randomblob",
            "sign",
            "soundex",
            "zeroblob",
            "sequence_watermark_experimental",
        ] {
            for (arguments, expected) in [
                ("", format!("{function} function with no arguments")),
                (
                    "1, 2",
                    format!("{function} function called with not exactly 1 arguments"),
                ),
            ] {
                let error = analyze_expression(
                    &expression(&format!("SELECT {function}({arguments})")),
                    &Scope::default(),
                    ExprPolicy::select(DoubleQuotedDml::Enabled),
                )
                .expect_err("invalid unary call fails during semantic analysis");
                assert_eq!(error.to_string(), format!("Parse error: {expected}"));
            }
        }

        let analyzed = analyze_expression(
            &expression("SELECT abs(1)"),
            &Scope::default(),
            ExprPolicy::select(DoubleQuotedDml::Enabled),
        )
        .expect("valid unary call binds");
        assert!(matches!(analyzed, Expr::Function(_)));
    }

    #[test]
    fn fixed_ordinary_argument_rules_are_checked_during_semantic_analysis() {
        for (sql, expected) in [
            (
                "SELECT timediff()",
                "wrong number of arguments to function timediff()",
            ),
            (
                "SELECT timediff(1)",
                "wrong number of arguments to function timediff()",
            ),
            ("SELECT hex()", "hex function must have exactly 1 argument"),
            ("SELECT nullif(1)", "no such function: nullif"),
            (
                "SELECT instr(1, 2, 3)",
                "instr function must have two argument",
            ),
            (
                "SELECT replace(1, 2)",
                "wrong number of arguments to function replace()",
            ),
            (
                "SELECT ifnull()",
                "ifnull function requires exactly 2 arguments",
            ),
            (
                "SELECT ifnull(1)",
                "ifnull function requires exactly 2 arguments",
            ),
            (
                "SELECT ifnull(1, 2, 3)",
                "ifnull function requires exactly 2 arguments",
            ),
            (
                "SELECT table_columns_json_array()",
                "table_columns_json_array() function must have exactly 1 argument",
            ),
            (
                "SELECT table_columns_json_array(1, 2)",
                "table_columns_json_array() function must have exactly 1 argument",
            ),
            (
                "SELECT bin_record_json_object(1)",
                "bin_record_json_object() function must have exactly 2 arguments",
            ),
            (
                "SELECT bin_record_json_object(1, 2, 3)",
                "bin_record_json_object() function must have exactly 2 arguments",
            ),
        ] {
            let error = analyze_expression(
                &expression(sql),
                &Scope::default(),
                ExprPolicy::select(DoubleQuotedDml::Enabled),
            )
            .expect_err("invalid fixed-arity call fails during semantic analysis");
            assert_eq!(error.to_string(), format!("Parse error: {expected}"));
        }

        for sql in [
            "SELECT timediff(1, 2)",
            "SELECT hex(1)",
            "SELECT nullif(1, 2)",
            "SELECT instr(1, 2)",
            "SELECT replace(1, 2, 3)",
            "SELECT ifnull(1, 2)",
            "SELECT table_columns_json_array(1)",
            "SELECT bin_record_json_object(1, 2)",
        ] {
            let analyzed = analyze_expression(
                &expression(sql),
                &Scope::default(),
                ExprPolicy::select(DoubleQuotedDml::Enabled),
            )
            .expect("valid fixed-arity call binds");
            assert!(matches!(analyzed, Expr::Function(_)));
        }
    }

    #[test]
    fn minimum_argument_rules_are_checked_during_semantic_analysis() {
        for (sql, expected) in [
            ("SELECT iif()", "iif function with no arguments"),
            ("SELECT iif(1)", "iif function with less than 2 arguments"),
            ("SELECT coalesce()", "coalesce function with no arguments"),
            (
                "SELECT coalesce(1)",
                "coalesce function with less than 2 arguments",
            ),
            ("SELECT like()", "like function with less than 2 arguments"),
            ("SELECT like(1)", "like function with less than 2 arguments"),
            ("SELECT glob()", "glob function with less than 2 arguments"),
            ("SELECT glob(1)", "glob function with less than 2 arguments"),
        ] {
            let error = analyze_expression(
                &expression(sql),
                &Scope::default(),
                ExprPolicy::select(DoubleQuotedDml::Enabled),
            )
            .expect_err("invalid minimum-arity call fails during semantic analysis");
            assert_eq!(error.to_string(), format!("Parse error: {expected}"));
        }

        for sql in [
            "SELECT iif(1, 2)",
            "SELECT iif(1, 2, 3)",
            "SELECT coalesce(1, 2)",
            "SELECT coalesce(1, 2, 3)",
            "SELECT like(1, 2)",
            "SELECT like(1, 2, 3)",
            "SELECT glob(1, 2)",
            "SELECT glob(1, 2, 3)",
        ] {
            let analyzed = analyze_expression(
                &expression(sql),
                &Scope::default(),
                ExprPolicy::select(DoubleQuotedDml::Enabled),
            )
            .expect("valid minimum-arity call binds");
            assert!(matches!(analyzed, Expr::Function(_)));
        }
    }

    #[test]
    fn planner_hint_rules_are_checked_during_semantic_analysis() {
        for (sql, expected) in [
            (
                "SELECT likely()",
                "likely function must have exactly 1 argument",
            ),
            (
                "SELECT likely(1, 2)",
                "likely function must have exactly 1 argument",
            ),
            (
                "SELECT unlikely()",
                "Unlikely function must have exactly 1 argument",
            ),
            (
                "SELECT unlikely(1, 2)",
                "Unlikely function must have exactly 1 argument",
            ),
            (
                "SELECT likelihood(1)",
                "likelihood() function must have exactly 2 arguments",
            ),
            (
                "SELECT likelihood(1, 0.5, 2)",
                "likelihood() function must have exactly 2 arguments",
            ),
            (
                "SELECT likelihood(1, 0)",
                "second argument to likelihood() must be a floating point number with decimal point",
            ),
            (
                "SELECT likelihood(1, 1.5)",
                "second argument to likelihood() must be a constant between 0.0 and 1.0",
            ),
            (
                "SELECT likelihood(1, '0.5')",
                "second argument to likelihood() must be a constant between 0.0 and 1.0",
            ),
        ] {
            let error = analyze_expression(
                &expression(sql),
                &Scope::default(),
                ExprPolicy::select(DoubleQuotedDml::Enabled),
            )
            .expect_err("invalid planner hint fails during semantic analysis");
            assert_eq!(error.to_string(), format!("Parse error: {expected}"));
        }

        for sql in [
            "SELECT likely(1)",
            "SELECT unlikely(1)",
            "SELECT likelihood(1, 0.5)",
        ] {
            let analyzed = analyze_expression(
                &expression(sql),
                &Scope::default(),
                ExprPolicy::select(DoubleQuotedDml::Enabled),
            )
            .expect("valid planner hint binds");
            assert!(matches!(analyzed, Expr::Function(_)));
        }
    }

    #[test]
    fn one_or_two_argument_rules_are_checked_during_semantic_analysis() {
        for function in ["trim", "ltrim", "rtrim", "round", "unhex"] {
            for (arguments, expected) in [
                ("", format!("{function} function with no arguments")),
                (
                    "1, 2, 3",
                    format!("{function} function called with more than 2 arguments"),
                ),
            ] {
                let error = analyze_expression(
                    &expression(&format!("SELECT {function}({arguments})")),
                    &Scope::default(),
                    ExprPolicy::select(DoubleQuotedDml::Enabled),
                )
                .expect_err("invalid one-or-two argument call fails during semantic analysis");
                assert_eq!(error.to_string(), format!("Parse error: {expected}"));
            }

            for arguments in ["1", "1, 2"] {
                let analyzed = analyze_expression(
                    &expression(&format!("SELECT {function}({arguments})")),
                    &Scope::default(),
                    ExprPolicy::select(DoubleQuotedDml::Enabled),
                )
                .expect("valid one-or-two argument call binds");
                assert!(matches!(analyzed, Expr::Function(_)));
            }
        }
    }

    #[test]
    fn nullary_argument_rules_are_checked_during_semantic_analysis() {
        for (function, expected) in [
            ("changes", "changes function with more than 0 arguments"),
            (
                "total_changes",
                "total_changes function with more than 0 arguments",
            ),
            ("random", "random function with arguments"),
            ("sqlite_version", "sqlite_version function with arguments"),
            ("turso_version", "sqlite_version function with arguments"),
            ("sqlite_source_id", "sqlite_version function with arguments"),
        ] {
            let error = analyze_expression(
                &expression(&format!("SELECT {function}(1)")),
                &Scope::default(),
                ExprPolicy::select(DoubleQuotedDml::Enabled),
            )
            .expect_err("invalid nullary call fails during semantic analysis");
            assert_eq!(error.to_string(), format!("Parse error: {expected}"));

            let analyzed = analyze_expression(
                &expression(&format!("SELECT {function}()")),
                &Scope::default(),
                ExprPolicy::select(DoubleQuotedDml::Enabled),
            )
            .expect("valid nullary call binds");
            assert!(matches!(analyzed, Expr::Function(_)));
        }

        #[cfg(feature = "test_helper")]
        {
            let error = analyze_expression(
                &expression("SELECT test_nondet_counter(1)"),
                &Scope::default(),
                ExprPolicy::select(DoubleQuotedDml::Enabled),
            )
            .expect_err("test counter rejects arguments during semantic analysis");
            assert_eq!(
                error.to_string(),
                "Parse error: test_nondet_counter function with arguments"
            );

            let analyzed = analyze_expression(
                &expression("SELECT test_nondet_counter()"),
                &Scope::default(),
                ExprPolicy::select(DoubleQuotedDml::Enabled),
            )
            .expect("nullary test counter binds");
            assert!(matches!(analyzed, Expr::Function(_)));
        }
    }

    #[cfg(feature = "json")]
    #[test]
    fn json_argument_rules_are_checked_during_semantic_analysis() {
        for sql in [
            "SELECT json_object()",
            "SELECT json_object('key', 1)",
            "SELECT json_pretty('{}')",
            "SELECT json_pretty('{}', '  ')",
            "SELECT json_array()",
            "SELECT json_valid('{}', 6)",
        ] {
            let analyzed = analyze_expression(
                &expression(sql),
                &Scope::default(),
                ExprPolicy::select(DoubleQuotedDml::Enabled),
            )
            .expect("valid JSON call binds");
            assert!(matches!(analyzed, Expr::Function(_)));
        }

        for (sql, expected) in [
            ("SELECT json()", "json function with no arguments"),
            (
                "SELECT json(1, 2)",
                "json function called with not exactly 1 arguments",
            ),
            (
                "SELECT json_error_position()",
                "json_error_position function with not exactly 1 argument",
            ),
            (
                "SELECT json_patch(1)",
                "json_patch function called with not exactly 2 arguments",
            ),
            (
                "SELECT json_array_length()",
                "json_array_length function with no arguments",
            ),
            (
                "SELECT json_pretty(1, 2, 3)",
                "json_pretty function called with more than 2 arguments",
            ),
            (
                "SELECT json_object('key')",
                "json_object function requires an even number of arguments",
            ),
            (
                "SELECT json_valid('{}', 6, 1)",
                "json_valid function called with more than 2 arguments",
            ),
        ] {
            let error = analyze_expression(
                &expression(sql),
                &Scope::default(),
                ExprPolicy::select(DoubleQuotedDml::Enabled),
            )
            .expect_err("invalid JSON call fails during semantic analysis");
            assert_eq!(error.to_string(), format!("Parse error: {expected}"));
        }
    }

    #[test]
    fn like_family_becomes_resolved_hir() {
        for (sql, expected_operator, expected_negated, expected_arguments) in [
            (
                "SELECT 'alphabet' LIKE 'alpha%'",
                ast::LikeOperator::Like,
                false,
                2,
            ),
            (
                "SELECT 'alphabet' NOT GLOB 'z*'",
                ast::LikeOperator::Glob,
                true,
                2,
            ),
            (
                "SELECT 'a_b' LIKE 'a!_b' ESCAPE '!'",
                ast::LikeOperator::Like,
                false,
                3,
            ),
        ] {
            let analyzed = analyze_expression(
                &expression(sql),
                &Scope::default(),
                ExprPolicy::select(DoubleQuotedDml::Enabled),
            )
            .expect("LIKE-family expression binds");
            let Expr::Like {
                operator,
                negated,
                argument_count,
                function,
                escape,
                ..
            } = analyzed
            else {
                panic!("LIKE-family syntax becomes LIKE HIR");
            };
            assert_eq!(operator, expected_operator);
            assert_eq!(negated, expected_negated);
            assert_eq!(argument_count, expected_arguments);
            assert_eq!(escape.is_some(), expected_arguments == 3);
            assert!(matches!(
                (operator, function.value()),
                (ast::LikeOperator::Like, Func::Scalar(ScalarFunc::Like))
                    | (ast::LikeOperator::Glob, Func::Scalar(ScalarFunc::Glob))
            ));
        }

        for operator in ["GLOB", "REGEXP", "MATCH"] {
            let error = analyze_expression(
                &expression(&format!("SELECT 'value' {operator} 'pattern' ESCAPE '!'")),
                &Scope::default(),
                ExprPolicy::select(DoubleQuotedDml::Enabled),
            )
            .expect_err("only LIKE accepts ESCAPE");
            assert_eq!(
                error.to_string(),
                format!("Parse error: wrong number of arguments to function {operator}()")
            );
        }

        let error = analyze_expression(
            &expression("SELECT 'value' REGEXP 'pattern'"),
            &Scope::default(),
            ExprPolicy::select(DoubleQuotedDml::Enabled),
        )
        .expect_err("REGEXP requires a registered function");
        assert_eq!(error.to_string(), "Parse error: no such function: regexp");
    }

    #[cfg(all(feature = "fts", not(target_family = "wasm")))]
    #[test]
    fn match_accepts_a_row_valued_left_side() {
        let analyzed = analyze_expression(
            &expression("SELECT ('one', 'two') MATCH 'query'"),
            &Scope::default(),
            ExprPolicy::select(DoubleQuotedDml::Enabled),
        )
        .expect("MATCH accepts multiple document columns");
        let Expr::Like {
            lhs,
            operator: ast::LikeOperator::Match,
            function,
            argument_count: 3,
            ..
        } = analyzed
        else {
            panic!("MATCH becomes resolved LIKE-family HIR");
        };
        assert!(matches!(*lhs, Expr::Row(values) if values.len() == 2));
        assert!(matches!(
            function.value(),
            Func::Fts(crate::function::FtsFunc::Match)
        ));
    }

    #[cfg(any(not(feature = "fts"), target_family = "wasm"))]
    #[test]
    fn match_requires_fts_support() {
        let error = analyze_expression(
            &expression("SELECT 'document' MATCH 'query'"),
            &Scope::default(),
            ExprPolicy::select(DoubleQuotedDml::Enabled),
        )
        .expect_err("MATCH requires FTS support");
        assert_eq!(
            error.to_string(),
            "Parse error: MATCH requires the 'fts' feature to be enabled"
        );
    }

    #[test]
    fn deeply_nested_like_expressions_use_expression_frames() {
        const DEPTH: usize = 10_000;

        let mut syntax = ast::Expr::Literal(ast::Literal::String("value".to_string()));
        for _ in 0..DEPTH {
            syntax = ast::Expr::Like {
                lhs: Box::new(syntax),
                not: false,
                op: ast::LikeOperator::Like,
                rhs: Box::new(ast::Expr::Literal(ast::Literal::String("%".to_string()))),
                escape: None,
            };
        }

        let mut analyzed = analyze_expression(
            &syntax,
            &Scope::default(),
            ExprPolicy::select(DoubleQuotedDml::Enabled),
        )
        .expect("deep LIKE expression binds without using the call stack");
        for _ in 0..DEPTH {
            let Expr::Like { lhs, .. } = analyzed else {
                panic!("expected nested LIKE HIR expression");
            };
            analyzed = *lhs;
        }
        assert!(matches!(
            analyzed,
            Expr::Literal(ast::Literal::String(value)) if value == "value"
        ));

        for _ in 0..DEPTH {
            let ast::Expr::Like { lhs, .. } = syntax else {
                panic!("expected nested LIKE parser expression");
            };
            syntax = *lhs;
        }
        assert!(matches!(
            syntax,
            ast::Expr::Literal(ast::Literal::String(value)) if value == "value"
        ));
    }

    #[test]
    fn deeply_nested_expressions_use_expression_frames() {
        const DEPTH: usize = 20_000;

        let mut syntax = ast::Expr::Literal(ast::Literal::Numeric("1".to_string()));
        for _ in 0..DEPTH {
            syntax = ast::Expr::Unary(ast::UnaryOperator::Positive, Box::new(syntax));
        }

        let mut analyzed = analyze_expression(
            &syntax,
            &Scope::default(),
            ExprPolicy::select(DoubleQuotedDml::Enabled),
        )
        .expect("deep expression binds without using the call stack");

        for _ in 0..DEPTH {
            let Expr::Unary {
                operator: ast::UnaryOperator::Positive,
                expr,
            } = analyzed
            else {
                panic!("expected nested unary HIR expression");
            };
            analyzed = *expr;
        }
        assert!(matches!(
            analyzed,
            Expr::Literal(ast::Literal::Numeric(value)) if value == "1"
        ));

        // Take the parser tree apart iteratively too, so dropping the test input
        // does not hide analyzer behavior behind the recursive enum destructor.
        for _ in 0..DEPTH {
            let ast::Expr::Unary(ast::UnaryOperator::Positive, inner) = syntax else {
                panic!("expected nested unary parser expression");
            };
            syntax = *inner;
        }
        assert!(matches!(
            syntax,
            ast::Expr::Literal(ast::Literal::Numeric(value)) if value == "1"
        ));
    }
}
