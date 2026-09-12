use smallvec::SmallVec;

use super::ColumnId;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ColumnSet(SmallVec<[ColumnId; 8]>);

impl ColumnSet {
    pub(crate) fn contains(&self, column: &ColumnId) -> bool {
        self.0.binary_search(column).is_ok()
    }

    pub(crate) fn insert(&mut self, column: ColumnId) {
        if let Err(position) = self.0.binary_search(&column) {
            self.0.insert(position, column);
        }
    }

    pub(crate) fn is_disjoint(&self, other: &Self) -> bool {
        self.iter().all(|column| !other.contains(column))
    }

    pub(crate) fn difference<'a>(&'a self, other: &'a Self) -> impl Iterator<Item = ColumnId> + 'a {
        self.iter()
            .copied()
            .filter(|column| !other.contains(column))
    }
}

impl std::ops::Deref for ColumnSet {
    type Target = [ColumnId];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl IntoIterator for ColumnSet {
    type Item = ColumnId;
    type IntoIter = smallvec::IntoIter<[ColumnId; 8]>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl FromIterator<ColumnId> for ColumnSet {
    fn from_iter<T: IntoIterator<Item = ColumnId>>(iter: T) -> Self {
        let mut columns: SmallVec<_> = iter.into_iter().collect();
        columns.sort_unstable();
        columns.dedup();
        Self(columns)
    }
}

impl Extend<ColumnId> for ColumnSet {
    fn extend<T: IntoIterator<Item = ColumnId>>(&mut self, iter: T) {
        self.0.extend(iter);
        self.0.sort_unstable();
        self.0.dedup();
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn column_sets_preserve_rowids_and_wide_relation_outputs() {
        let input: Vec<_> = (0..5)
            .flat_map(|relation| {
                (0..100)
                    .map(Some)
                    .chain([None])
                    .map(move |position| ColumnId {
                        relation: relation.into(),
                        position,
                    })
            })
            .collect();
        for stride in [1, 2, 3, 7, 101] {
            let mut actual: ColumnSet = input.iter().rev().step_by(stride).copied().collect();
            let mut expected: BTreeSet<_> = input.iter().rev().step_by(stride).copied().collect();
            for column in input.iter().step_by(2) {
                actual.insert(*column);
                expected.insert(*column);
            }
            assert_eq!(
                actual.as_ref(),
                expected.iter().copied().collect::<Vec<_>>()
            );
            let other: ColumnSet = input.iter().step_by(3).copied().collect();
            let expected_other: BTreeSet<_> = other.iter().copied().collect();
            assert_eq!(
                actual.is_disjoint(&other),
                expected.is_disjoint(&expected_other)
            );
            assert_eq!(
                actual.difference(&other).collect::<Vec<_>>(),
                expected
                    .difference(&expected_other)
                    .copied()
                    .collect::<Vec<_>>()
            );
            actual.extend(other);
            expected.extend(expected_other);
            assert_eq!(
                actual.as_ref(),
                expected.iter().copied().collect::<Vec<_>>()
            );
        }
    }
}
