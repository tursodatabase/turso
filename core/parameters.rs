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
        }
    }

    pub fn count(&self) -> usize {
        self.next_index.get() - 1
    }

    pub fn has_slot(&self, index: NonZero<usize>) -> bool {
        index < self.next_index
    }

    pub fn has_index(&self, index: NonZero<usize>) -> bool {
        self.list.iter().any(|p| p.index() == index)
    }

    pub fn is_indexed(&self, index: NonZero<usize>) -> bool {
        self.list
            .iter()
            .any(|p| matches!(p, Parameter::Indexed(i) if *i == index))
    }

    pub fn name(&self, index: NonZero<usize>) -> Option<String> {
        self.list.iter().find_map(|p| match p {
            Parameter::Indexed(i) if *i == index => Some(format!("?{i}")),
            Parameter::Named(name, i) if *i == index => Some(name.to_owned()),
            _ => None,
        })
    }

    pub fn index(&self, name: impl AsRef<str>) -> Option<NonZero<usize>> {
        self.list
            .iter()
            .find_map(|p| match p {
                Parameter::Named(n, index) if n == name.as_ref() => Some(index),
                _ => None,
            })
            .copied()
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
        if !self.has_index(index) {
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

        if let Some(Parameter::Named(existing_name, _)) = self
            .list
            .iter_mut()
            .find(|parameter| parameter.index() == index)
        {
            if existing_name != &name {
                tracing::trace!(
                    "named parameter alias at {index} as {name}; keeping existing name {existing_name}"
                );
            }
            return index;
        }

        if self.has_index(index) {
            self.list.retain(|parameter| parameter.index() != index);
        }

        tracing::trace!("named parameter at {index} as {name}");
        self.list.push(Parameter::Named(name, index));
        index
    }

    pub fn push(&mut self, name: impl AsRef<str>) -> NonZero<usize> {
        match name.as_ref() {
            name if name.starts_with(['$', ':', '@', '#']) => {
                match self
                    .list
                    .iter()
                    .find(|p| matches!(p, Parameter::Named(n, _) if name == n))
                {
                    Some(t) => {
                        let index = t.index();
                        tracing::trace!("named parameter at {index} as {name}");
                        index
                    }
                    None => {
                        let index = self.allocate_new_index();
                        self.push_named_at(name, index)
                    }
                }
            }
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
