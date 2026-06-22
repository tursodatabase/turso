use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
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
    /// Every index currently present in `list`. Keeps membership checks
    /// (`has_index`) O(1) instead of scanning `list`, which otherwise makes
    /// preparing a statement with N parameters O(N^2) (e.g. a large multi-row
    /// `INSERT ... VALUES` with bound parameters).
    present: HashSet<NonZero<usize>>,
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
            present: HashSet::default(),
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
        self.present.contains(&index)
    }

    pub fn is_indexed(&self, index: NonZero<usize>) -> bool {
        self.present.contains(&index) && !self.index_to_name.contains_key(&index)
    }

    pub fn name(&self, index: NonZero<usize>) -> Option<String> {
        if let Some(name) = self.index_to_name.get(&index) {
            Some(name.clone())
        } else if self.present.contains(&index) {
            Some(format!("?{index}"))
        } else {
            None
        }
    }

    pub fn index(&self, name: impl AsRef<str>) -> Option<NonZero<usize>> {
        self.name_to_index.get(name.as_ref()).copied()
    }

    pub fn next_index(&self) -> NonZero<usize> {
        self.next_index
    }

    fn allocate_new_index(&mut self) -> NonZero<usize> {
        let index = self.next_index;
        self.next_index = self.next_index.checked_add(1).unwrap();
        index
    }

    pub fn push_index(&mut self, index: NonZero<usize>) -> NonZero<usize> {
        if index >= self.next_index {
            self.next_index = index.checked_add(1).unwrap();
        }
        if self.present.insert(index) {
            self.list.push(Parameter::Indexed(index));
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
        if self.present.contains(&index) {
            self.list.retain(|parameter| parameter.index() != index);
        }

        tracing::trace!("named parameter at {index} as {name}");
        self.present.insert(index);
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
mod tests {
    use super::Parameters;
    use std::num::NonZero;

    #[test]
    fn count_and_name_are_stable_with_reused_parameters() {
        let mut parameters = Parameters::new();

        parameters.push(":a");
        parameters.push(":a");
        parameters.push("1");
        parameters.push("1");
        parameters.push(":b");
        parameters.push("2");

        assert_eq!(parameters.count(), 2);

        let idx1 = 1usize.try_into().unwrap();
        let idx2 = 2usize.try_into().unwrap();
        let idx3 = 3usize.try_into().unwrap();

        assert!(parameters.has_index(idx1));
        assert!(parameters.has_index(idx2));
        assert!(!parameters.has_index(idx3));
        assert!(!parameters.is_indexed(idx1));
        assert!(!parameters.is_indexed(idx2));

        assert_eq!(parameters.name(idx1).as_deref(), Some(":a"));
        assert_eq!(parameters.name(idx2).as_deref(), Some(":b"));
    }

    #[test]
    fn is_indexed_is_true_only_for_indexed_parameters() {
        let mut parameters = Parameters::new();
        parameters.push("1");
        parameters.push(":b");

        let idx1 = 1usize.try_into().unwrap();
        let idx2 = 2usize.try_into().unwrap();

        assert!(parameters.is_indexed(idx1));
        assert!(!parameters.is_indexed(idx2));
    }

    #[test]
    fn many_distinct_named_params_lookup_round_trips() {
        // Exercises the O(1) name<->index maps with many distinct names,
        // the case that was previously O(n^2) via a linear list scan.
        let mut parameters = Parameters::new();
        let n = 1000usize;
        for i in 0..n {
            let idx = parameters.push(format!(":p{i}"));
            // Distinct names get consecutive indices in encounter order.
            assert_eq!(idx.get(), i + 1);
        }
        assert_eq!(parameters.count(), n);
        for i in 0..n {
            let expected: NonZero<usize> = (i + 1).try_into().unwrap();
            assert_eq!(parameters.index(format!(":p{i}")), Some(expected));
            assert_eq!(parameters.name(expected).as_deref(), Some(format!(":p{i}").as_str()));
            assert!(parameters.has_index(expected));
            assert!(!parameters.is_indexed(expected));
        }
        assert_eq!(parameters.list.len(), n, "no duplicate slots");
    }

    #[test]
    fn reused_named_param_returns_same_index() {
        let mut parameters = Parameters::new();
        let a1 = parameters.push(":a");
        let b = parameters.push(":b");
        let a2 = parameters.push(":a");
        assert_eq!(a1, a2);
        assert_ne!(a1, b);
        assert_eq!(parameters.count(), 2);
        assert_eq!(parameters.list.len(), 2);
    }

    #[test]
    fn push_named_at_replaces_existing_indexed_slot() {
        // push_index then push_named_at at the same index: the slot becomes Named.
        let mut parameters = Parameters::new();
        let idx: NonZero<usize> = 1.try_into().unwrap();
        parameters.push_index(idx);
        assert!(parameters.is_indexed(idx));
        parameters.push_named_at(":x", idx);
        assert!(parameters.has_index(idx));
        assert!(!parameters.is_indexed(idx));
        assert_eq!(parameters.name(idx).as_deref(), Some(":x"));
        assert_eq!(parameters.index(":x"), Some(idx));
        assert_eq!(parameters.list.len(), 1, "indexed slot replaced, not duplicated");
    }

    #[test]
    fn repeated_indexed_param_dedups_to_single_slot() {
        let mut parameters = Parameters::new();
        let idx: NonZero<usize> = 5.try_into().unwrap();
        for _ in 0..100 {
            assert_eq!(parameters.push_index(idx), idx);
        }
        assert_eq!(parameters.list.len(), 1);
        assert_eq!(parameters.count(), 5);
        assert!(parameters.has_index(idx));
    }

    #[test]
    fn count_tracks_highest_index_for_sparse_parameters() {
        let mut parameters = Parameters::new();
        parameters.push("3");

        let idx1 = 1usize.try_into().unwrap();
        let idx2 = 2usize.try_into().unwrap();
        let idx3 = 3usize.try_into().unwrap();
        let idx4 = 4usize.try_into().unwrap();

        assert_eq!(parameters.count(), 3);
        assert!(parameters.has_slot(idx1));
        assert!(parameters.has_slot(idx2));
        assert!(parameters.has_slot(idx3));
        assert!(!parameters.has_slot(idx4));

        assert!(!parameters.has_index(idx1));
        assert!(!parameters.has_index(idx2));
        assert!(parameters.has_index(idx3));
    }
}
