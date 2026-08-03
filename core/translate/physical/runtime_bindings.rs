//! Scoped mappings from HIR identities to runtime locations.

use rustc_hash::FxHashMap;
use std::fmt;

use crate::translate::semantic::hir::{
    AggregateId, CatalogSnapshot, HirDocument, OutputId, OutputOwner, QueryId, SchemaProgramId,
    SourceId, SourceOwner, WindowFunctionId,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct CursorId(pub(crate) usize);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct RegisterId(pub(crate) usize);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RegisterRange {
    pub(crate) first: RegisterId,
    pub(crate) width: usize,
}

impl RegisterRange {
    pub(crate) const fn new(first: usize, width: usize) -> Self {
        Self {
            first: RegisterId(first),
            width,
        }
    }

    pub(crate) fn register(self, position: usize) -> Option<RegisterId> {
        (position < self.width).then(|| RegisterId(self.first.0 + position))
    }
}

/// Where emission reads the current row for one semantic source.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SourceRuntime {
    Cursor(CursorId),
    Registers {
        columns: RegisterRange,
        /// Separate because rowid is not one of a table's stored column slots.
        rowid: Option<RegisterId>,
    },
}

/// Runtime rows supplied by the caller of one HIR root.
///
/// Ordinary statements have no inputs. Trigger commands and predicates use
/// this map for their explicit OLD and NEW pseudo-sources. Keeping the map in
/// physical terms means HIR remains independent of registers and parameters.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RootRuntimeInputs {
    sources: Vec<(SourceId, SourceRuntime)>,
}

impl RootRuntimeInputs {
    pub(crate) fn bind_source(&mut self, source: SourceId, runtime: SourceRuntime) {
        self.sources.push((source, runtime));
    }

    pub(crate) fn apply<'document>(
        &self,
        bindings: &mut RuntimeBindings<'document>,
    ) -> BindingResult<()> {
        for (source, runtime) in &self.sources {
            bindings.bind_source(*source, *runtime)?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OutputRuntime {
    pub(crate) register: RegisterId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct AggregateRuntime {
    pub(crate) register: RegisterId,
    /// Per-aggregate duplicate set. This is runtime state selected by physical
    /// emission; the HIR only records whether DISTINCT was written.
    pub(crate) distinct_hash_table: Option<usize>,
    pub(crate) ordered_sorter: Option<OrderedAggregateRuntime>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OrderedAggregateRuntime {
    pub(crate) cursor: usize,
    pub(crate) record: RegisterId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WindowFunctionRuntime {
    pub(crate) register: RegisterId,
}

/// Destination already chosen for a query by physical planning.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QueryRuntime {
    Registers(RegisterRange),
    Exists(RegisterId),
    RowSet(CursorId),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RuntimeBindingError {
    InvalidDocument(String),
    SnapshotMismatch,
    UnknownSource(SourceId),
    UnknownOutput(OutputId),
    UnknownAggregate(AggregateId),
    UnknownWindowFunction(WindowFunctionId),
    UnknownQuery(QueryId),
    UnknownSchemaProgram(SchemaProgramId),
    WrongScope(&'static str),
    Duplicate(&'static str),
    SourceNotCaptured(SourceId),
    SourceWidth {
        source: SourceId,
        expected: usize,
        actual: usize,
    },
    CannotLeaveRoot,
}

impl fmt::Display for RuntimeBindingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidDocument(message) => write!(formatter, "invalid HIR document: {message}"),
            Self::SnapshotMismatch => {
                formatter.write_str("runtime bindings use a different catalog snapshot")
            }
            Self::UnknownSource(source) => write!(formatter, "unknown HIR source {source}"),
            Self::UnknownOutput(output) => write!(formatter, "unknown HIR output {output:?}"),
            Self::UnknownAggregate(aggregate) => {
                write!(formatter, "unknown HIR aggregate {aggregate:?}")
            }
            Self::UnknownWindowFunction(function) => {
                write!(formatter, "unknown HIR window function {function:?}")
            }
            Self::UnknownQuery(query) => write!(formatter, "unknown HIR query {query}"),
            Self::UnknownSchemaProgram(program) => {
                write!(formatter, "unknown HIR schema program {program}")
            }
            Self::WrongScope(kind) => {
                write!(formatter, "{kind} does not belong to this runtime scope")
            }
            Self::Duplicate(kind) => write!(formatter, "{kind} already has a runtime binding"),
            Self::SourceNotCaptured(source) => {
                write!(formatter, "query does not capture runtime source {source}")
            }
            Self::SourceWidth {
                source,
                expected,
                actual,
            } => write!(
                formatter,
                "runtime source {source} has width {actual}, expected {expected}"
            ),
            Self::CannotLeaveRoot => formatter.write_str("cannot leave the root runtime scope"),
        }
    }
}

impl std::error::Error for RuntimeBindingError {}

type BindingResult<T> = std::result::Result<T, RuntimeBindingError>;

#[derive(Default)]
struct RuntimeFrame {
    query: Option<QueryId>,
    sources: FxHashMap<SourceId, SourceRuntime>,
    outputs: FxHashMap<OutputId, OutputRuntime>,
    aggregates: FxHashMap<AggregateId, AggregateRuntime>,
    window_functions: FxHashMap<WindowFunctionId, WindowFunctionRuntime>,
    queries: FxHashMap<QueryId, QueryRuntime>,
}

/// Runtime locations visible while one HIR document is planned or emitted.
///
/// Frames follow lexical query parents. A nested query can see an outer source
/// only when that exact `SourceId` appears in its HIR capture list.
pub(crate) struct RuntimeBindings<'document> {
    document: &'document HirDocument,
    frames: Vec<RuntimeFrame>,
    /// Bound schema programs are expressions evaluated inside the current
    /// query/DML frame. Their positional input must shadow no unrelated source
    /// and must disappear as soon as the call finishes.
    schema_inputs: Vec<(SchemaProgramId, SourceId, SourceRuntime)>,
}

impl<'document> RuntimeBindings<'document> {
    pub(crate) fn new(
        document: &'document HirDocument,
        snapshot: CatalogSnapshot,
    ) -> BindingResult<Self> {
        document
            .validate()
            .map_err(|error| RuntimeBindingError::InvalidDocument(error.to_string()))?;
        if document.snapshot != snapshot {
            return Err(RuntimeBindingError::SnapshotMismatch);
        }
        Ok(Self {
            document,
            frames: vec![RuntimeFrame::default()],
            schema_inputs: Vec::new(),
        })
    }

    pub(crate) fn current_query(&self) -> Option<QueryId> {
        self.frames.last().and_then(|frame| frame.query)
    }

    pub(crate) const fn document(&self) -> &'document HirDocument {
        self.document
    }

    pub(crate) fn enter_query(&mut self, query: QueryId) -> BindingResult<()> {
        let definition = self
            .document
            .query(query)
            .ok_or(RuntimeBindingError::UnknownQuery(query))?;
        if definition.parent.is_some() && definition.parent != self.current_query() {
            return Err(RuntimeBindingError::WrongScope("query"));
        }
        self.frames.push(RuntimeFrame {
            query: Some(query),
            ..RuntimeFrame::default()
        });
        Ok(())
    }

    pub(crate) fn leave_query(&mut self) -> BindingResult<QueryId> {
        if self.frames.len() == 1 {
            return Err(RuntimeBindingError::CannotLeaveRoot);
        }
        Ok(self
            .frames
            .pop()
            .and_then(|frame| frame.query)
            .expect("every non-root runtime frame belongs to a query"))
    }

    pub(crate) fn bind_source(
        &mut self,
        source: SourceId,
        runtime: SourceRuntime,
    ) -> BindingResult<()> {
        let definition = self
            .document
            .source(source)
            .ok_or(RuntimeBindingError::UnknownSource(source))?;
        if !self.source_owner_is_current(definition.owner) {
            return Err(RuntimeBindingError::WrongScope("source"));
        }
        if let SourceRuntime::Registers { columns, .. } = runtime {
            let expected = definition.columns.len();
            if columns.width != expected {
                return Err(RuntimeBindingError::SourceWidth {
                    source,
                    expected,
                    actual: columns.width,
                });
            }
        }
        let frame = self
            .frames
            .last_mut()
            .expect("the root frame always exists");
        if frame.sources.contains_key(&source) {
            return Err(RuntimeBindingError::Duplicate("source"));
        }
        frame.sources.insert(source, runtime);
        Ok(())
    }

    pub(crate) fn source(&self, source: SourceId) -> BindingResult<SourceRuntime> {
        let definition = self
            .document
            .source(source)
            .ok_or(RuntimeBindingError::UnknownSource(source))?;
        if let Some((_, _, runtime)) = self
            .schema_inputs
            .iter()
            .rev()
            .find(|(_, input, _)| *input == source)
        {
            return Ok(*runtime);
        }
        let current = self.frames.last().expect("the root frame always exists");
        if let Some(runtime) = current.sources.get(&source) {
            return Ok(*runtime);
        }
        if self.source_owner_is_current(definition.owner) {
            return Err(RuntimeBindingError::WrongScope("unbound source"));
        }
        let Some(query) = current.query else {
            return Err(RuntimeBindingError::WrongScope("source"));
        };
        let query = self
            .document
            .query(query)
            .expect("a runtime query frame was checked before insertion");
        if !query.captures.contains(&source) {
            return Err(RuntimeBindingError::SourceNotCaptured(source));
        }
        self.frames[..self.frames.len() - 1]
            .iter()
            .rev()
            .find_map(|frame| frame.sources.get(&source).copied())
            .ok_or(RuntimeBindingError::WrongScope("captured source"))
    }

    /// Replace the current physical location for one source while preserving
    /// its semantic identity. Grouped and window phases use this when rows
    /// move from base cursors into a sorter-owned register record.
    pub(crate) fn replace_source(
        &mut self,
        source: SourceId,
        runtime: SourceRuntime,
    ) -> BindingResult<SourceRuntime> {
        let definition = self
            .document
            .source(source)
            .ok_or(RuntimeBindingError::UnknownSource(source))?;
        if !self.source_owner_is_current(definition.owner) {
            return Err(RuntimeBindingError::WrongScope("source"));
        }
        if let SourceRuntime::Registers { columns, .. } = runtime {
            let expected = definition.columns.len();
            if columns.width != expected {
                return Err(RuntimeBindingError::SourceWidth {
                    source,
                    expected,
                    actual: columns.width,
                });
            }
        }
        self.frames
            .last_mut()
            .expect("the root frame always exists")
            .sources
            .insert(source, runtime)
            .ok_or(RuntimeBindingError::WrongScope("unbound source"))
    }

    /// Bind `[value, user arguments...]` while emitting one stored schema
    /// expression. This does not open a query scope and cannot expose any
    /// other root-owned synthetic source.
    pub(crate) fn enter_schema_program(
        &mut self,
        program: SchemaProgramId,
        inputs: RegisterRange,
    ) -> BindingResult<()> {
        let definition = self
            .document
            .schema_program(program)
            .ok_or(RuntimeBindingError::UnknownSchemaProgram(program))?;
        let source = self
            .document
            .source(definition.input_source)
            .ok_or(RuntimeBindingError::UnknownSource(definition.input_source))?;
        if inputs.width != source.columns.len() {
            return Err(RuntimeBindingError::SourceWidth {
                source: source.id,
                expected: source.columns.len(),
                actual: inputs.width,
            });
        }
        self.schema_inputs.push((
            program,
            source.id,
            SourceRuntime::Registers {
                columns: inputs,
                rowid: None,
            },
        ));
        Ok(())
    }

    pub(crate) fn leave_schema_program(&mut self) -> BindingResult<SchemaProgramId> {
        self.schema_inputs
            .pop()
            .map(|(program, _, _)| program)
            .ok_or(RuntimeBindingError::WrongScope("schema program"))
    }

    pub(crate) fn bind_output(
        &mut self,
        output: OutputId,
        runtime: OutputRuntime,
    ) -> BindingResult<()> {
        self.document
            .output(output)
            .ok_or(RuntimeBindingError::UnknownOutput(output))?;
        if !self.output_owner_is_current(output.owner) {
            return Err(RuntimeBindingError::WrongScope("output"));
        }
        let frame = self
            .frames
            .last_mut()
            .expect("the root frame always exists");
        if frame.outputs.contains_key(&output) {
            return Err(RuntimeBindingError::Duplicate("output"));
        }
        frame.outputs.insert(output, runtime);
        Ok(())
    }

    pub(crate) fn output(&self, output: OutputId) -> BindingResult<OutputRuntime> {
        self.document
            .output(output)
            .ok_or(RuntimeBindingError::UnknownOutput(output))?;
        if !self.output_owner_is_current(output.owner) {
            return Err(RuntimeBindingError::WrongScope("output"));
        }
        self.frames
            .last()
            .and_then(|frame| frame.outputs.get(&output).copied())
            .ok_or(RuntimeBindingError::WrongScope("unbound output"))
    }

    pub(crate) fn bind_aggregate(
        &mut self,
        aggregate: AggregateId,
        runtime: AggregateRuntime,
    ) -> BindingResult<()> {
        self.require_aggregate(aggregate)?;
        let frame = self
            .frames
            .last_mut()
            .expect("the root frame always exists");
        if frame.aggregates.contains_key(&aggregate) {
            return Err(RuntimeBindingError::Duplicate("aggregate"));
        }
        frame.aggregates.insert(aggregate, runtime);
        Ok(())
    }

    pub(crate) fn aggregate(&self, aggregate: AggregateId) -> BindingResult<AggregateRuntime> {
        self.require_aggregate(aggregate)?;
        self.frames
            .last()
            .and_then(|frame| frame.aggregates.get(&aggregate).copied())
            .ok_or(RuntimeBindingError::WrongScope("unbound aggregate"))
    }

    pub(crate) fn replace_aggregate(
        &mut self,
        aggregate: AggregateId,
        runtime: AggregateRuntime,
    ) -> BindingResult<AggregateRuntime> {
        self.require_aggregate(aggregate)?;
        self.frames
            .last_mut()
            .expect("the root frame always exists")
            .aggregates
            .insert(aggregate, runtime)
            .ok_or(RuntimeBindingError::WrongScope("unbound aggregate"))
    }

    pub(crate) fn bind_window_function(
        &mut self,
        function: WindowFunctionId,
        runtime: WindowFunctionRuntime,
    ) -> BindingResult<()> {
        self.require_window_function(function)?;
        let frame = self
            .frames
            .last_mut()
            .expect("the root frame always exists");
        if frame.window_functions.contains_key(&function) {
            return Err(RuntimeBindingError::Duplicate("window function"));
        }
        frame.window_functions.insert(function, runtime);
        Ok(())
    }

    pub(crate) fn window_function(
        &self,
        function: WindowFunctionId,
    ) -> BindingResult<WindowFunctionRuntime> {
        self.require_window_function(function)?;
        self.frames
            .last()
            .and_then(|frame| frame.window_functions.get(&function).copied())
            .ok_or(RuntimeBindingError::WrongScope("unbound window function"))
    }

    pub(crate) fn bind_query(
        &mut self,
        query: QueryId,
        runtime: QueryRuntime,
    ) -> BindingResult<()> {
        let definition = self
            .document
            .query(query)
            .ok_or(RuntimeBindingError::UnknownQuery(query))?;
        if definition.parent != self.current_query() {
            return Err(RuntimeBindingError::WrongScope("query"));
        }
        let frame = self
            .frames
            .last_mut()
            .expect("the root frame always exists");
        if frame.queries.contains_key(&query) {
            return Err(RuntimeBindingError::Duplicate("query"));
        }
        frame.queries.insert(query, runtime);
        Ok(())
    }

    pub(crate) fn query(&self, query: QueryId) -> BindingResult<QueryRuntime> {
        let definition = self
            .document
            .query(query)
            .ok_or(RuntimeBindingError::UnknownQuery(query))?;
        let current = self.current_query();
        if definition.parent != current && current != Some(query) {
            return Err(RuntimeBindingError::WrongScope("query"));
        }
        self.frames
            .iter()
            .rev()
            .find_map(|frame| frame.queries.get(&query).copied())
            .ok_or(RuntimeBindingError::WrongScope("unbound query"))
    }

    fn source_owner_is_current(&self, owner: SourceOwner) -> bool {
        match (owner, self.current_query()) {
            (SourceOwner::Root, None) => true,
            (SourceOwner::QueryBlock(block), Some(query)) => block.query == query,
            (SourceOwner::Cte(_), _)
            | (SourceOwner::Root, Some(_))
            | (SourceOwner::QueryBlock(_), None) => false,
        }
    }

    fn require_aggregate(&self, aggregate: AggregateId) -> BindingResult<()> {
        let Some(block) = self.document.query_block(aggregate.block) else {
            return Err(RuntimeBindingError::UnknownAggregate(aggregate));
        };
        if aggregate.index >= block.aggregate_count {
            return Err(RuntimeBindingError::UnknownAggregate(aggregate));
        }
        if self.current_query() != Some(aggregate.block.query) {
            return Err(RuntimeBindingError::WrongScope("aggregate"));
        }
        Ok(())
    }

    fn require_window_function(&self, function: WindowFunctionId) -> BindingResult<()> {
        let Some(block) = self.document.query_block(function.block) else {
            return Err(RuntimeBindingError::UnknownWindowFunction(function));
        };
        if function.index >= block.window_function_count {
            return Err(RuntimeBindingError::UnknownWindowFunction(function));
        }
        if self.current_query() != Some(function.block.query) {
            return Err(RuntimeBindingError::WrongScope("window function"));
        }
        Ok(())
    }

    fn output_owner_is_current(&self, owner: OutputOwner) -> bool {
        match (owner, self.current_query()) {
            (OutputOwner::Root, None) => true,
            (OutputOwner::QueryBlock(block), Some(query)) => block.query == query,
            (OutputOwner::IndexMethodPattern(pattern), _) => self
                .document
                .source(pattern.source)
                .is_some_and(|source| self.source_owner_is_current(source.owner)),
            (OutputOwner::Root, Some(_)) | (OutputOwner::QueryBlock(_), None) => false,
        }
    }
}
