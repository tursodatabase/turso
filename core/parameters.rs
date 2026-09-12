use rustc_hash::FxHashMap as HashMap;
use std::num::NonZero;

#[derive(Clone, Debug)]
pub enum Parameter {
    Indexed(NonZero<usize>),
    Named(String, NonZero<usize>),
}

impl PartialEq for Parameter {
    fn eq(&self, other: &Self) -> bool {
        self.index() == other.index()
    }
}

impl Parameter {
    pub fn index(&self) -> NonZero<usize> {
        match self {
            Parameter::Indexed(index) => *index,
            Parameter::Named(_, index) => *index,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Parameters {
    next_index: NonZero<usize>,
    pub list: Vec<Parameter>,
    /// Every index currently present in `list`, with true for indices
    /// written as an explicit `?N` marker (their "?N" name is derived from
    /// the index on demand). One map serves both membership checks and
    /// numbered-ness, so registering a marker costs a single insert and
    /// `has_index` stays O(1) instead of scanning `list`, which otherwise
    /// makes preparing a statement with N parameters O(N^2) (e.g. a large
    /// multi-row `INSERT ... VALUES` with bound parameters).
    present: HashMap<NonZero<usize>, bool>,
    /// Named-parameter name -> index, for O(1) name dedup/lookup.
    name_to_index: HashMap<String, NonZero<usize>>,
    /// Index -> name for indices whose slot is a `Named` parameter. Lets us
    /// distinguish `Named` from `Indexed` slots (and recover the name) in O(1).
    index_to_name: HashMap<NonZero<usize>, String>,
}

impl Default for Parameters {
    fn default() -> Self {
        Self::new()
    }
}

impl Parameters {
    pub fn new() -> Self {
        Self {
            next_index: 1.try_into().unwrap(),
            list: vec![],
            present: HashMap::default(),
            name_to_index: HashMap::default(),
            index_to_name: HashMap::default(),
        }
    }

    pub fn count(&self) -> usize {
        self.next_index.get() - 1
    }

    pub fn has_slot(&self, index: NonZero<usize>) -> bool {
        index < self.next_index
    }

    pub fn has_index(&self, index: NonZero<usize>) -> bool {
        self.present.contains_key(&index)
    }

    pub fn is_indexed(&self, index: NonZero<usize>) -> bool {
        self.present.contains_key(&index) && !self.index_to_name.contains_key(&index)
    }

    /// The name of the parameter at `index`: the ":name"/"@name"/"$name"
    /// text for named parameters, "?N" for explicit numbered ones, and None
    /// for anonymous `?` slots, matching sqlite3_bind_parameter_name.
    pub fn name(&self, index: NonZero<usize>) -> Option<String> {
        if let Some(name) = self.index_to_name.get(&index) {
            Some(name.clone())
        } else if self.present.get(&index).copied().unwrap_or(false) {
            Some(format!("?{index}"))
        } else {
            None
        }
    }

    pub fn index(&self, name: impl AsRef<str>) -> Option<NonZero<usize>> {
        let name = name.as_ref();
        if let Some(index) = self.name_to_index.get(name) {
            return Some(*index);
        }
        // "?N" resolves to N when that numbered marker exists, as
        // sqlite3_bind_parameter_index does.
        let index: NonZero<usize> = name.strip_prefix('?')?.parse().ok()?;
        self.present
            .get(&index)
            .copied()
            .unwrap_or(false)
            .then_some(index)
    }

    pub fn next_index(&self) -> NonZero<usize> {
        self.next_index
    }

    fn allocate_new_index(&mut self) -> NonZero<usize> {
        let index = self.next_index;
        self.next_index = self.next_index.checked_add(1).unwrap();
        index
    }

    /// Register an explicit `?N` marker: the same single insert as
    /// push_index, carrying the numbered bit; the "?N" name is derived on
    /// demand by `name`/`index`.
    pub fn push_numbered(&mut self, index: NonZero<usize>) -> NonZero<usize> {
        self.push_positional(index, true)
    }

    pub fn push_index(&mut self, index: NonZero<usize>) -> NonZero<usize> {
        self.push_positional(index, false)
    }

    fn push_positional(&mut self, index: NonZero<usize>, numbered: bool) -> NonZero<usize> {
        if index >= self.next_index {
            self.next_index = index.checked_add(1).unwrap();
        }
        // First spelling wins, as SQLite assigns variable names: a Named
        // slot keeps its name — a later ?N spelling of the same index
        // neither renames it nor makes "?N" resolvable — and a numbered
        // slot keeps "?N" across later bare-? occurrences.
        let numbered = numbered && !self.index_to_name.contains_key(&index);
        match self.present.insert(index, numbered) {
            None => self.list.push(Parameter::Indexed(index)),
            Some(was_numbered) => {
                if was_numbered && !numbered {
                    self.present.insert(index, true);
                }
            }
        }
        tracing::trace!("indexed parameter at {index}");
        index
    }

    pub fn push_named_at(
        &mut self,
        name: impl Into<String>,
        index: NonZero<usize>,
    ) -> NonZero<usize> {
        let name = name.into();
        if index >= self.next_index {
            self.next_index = index.checked_add(1).unwrap();
        }

        // A `Named` slot already exists at this index: keep it (matching the
        // first name encountered), as the original list-scan did.
        if let Some(existing_name) = self.index_to_name.get(&index) {
            if existing_name != &name {
                tracing::trace!(
                    "named parameter alias at {index} as {name}; keeping existing name {existing_name}"
                );
            }
            return index;
        }

        // An `Indexed` slot occupies this index: replace it with the named one.
        if self.present.contains_key(&index) {
            self.list.retain(|parameter| parameter.index() != index);
        }

        tracing::trace!("named parameter at {index} as {name}");
        self.present.insert(index, false);
        self.name_to_index.entry(name.clone()).or_insert(index);
        self.index_to_name.insert(index, name.clone());
        self.list.push(Parameter::Named(name, index));
        index
    }

    pub fn push(&mut self, name: impl AsRef<str>) -> NonZero<usize> {
        match name.as_ref() {
            name if name.starts_with(['$', ':', '@', '#']) => match self.name_to_index.get(name) {
                Some(index) => {
                    let index = *index;
                    tracing::trace!("named parameter at {index} as {name}");
                    index
                }
                None => {
                    let index = self.allocate_new_index();
                    self.push_named_at(name, index)
                }
            },
            index => {
                // SAFETY: Guaranteed from parser that the index is bigger than 0.
                let index: NonZero<usize> = index.parse().unwrap();
                self.push_index(index)
            }
        }
    }
}

#[cfg(test)]
#[path = "tests/unit/parameters/tests.rs"]
mod tests;
