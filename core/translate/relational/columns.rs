use smallvec::SmallVec;
use turso_parser::ast::TableInternalId;

use crate::translate::plan::BitSet;
use crate::Result;

use super::ColumnId;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ColumnSet(SmallVec<[(TableInternalId, BitSet); 1]>);

impl ColumnSet {
    pub(crate) fn for_relation(
        relation: TableInternalId,
        columns: usize,
        rowid: bool,
    ) -> Result<Self> {
        if columns == 0 && !rowid {
            return Ok(Self::default());
        }
        let mut bits = BitSet::default();
        if rowid {
            bits.set(0)?;
        }
        for column in 0..columns {
            bits.set(column + 1)?;
        }
        Ok(Self(smallvec::smallvec![(relation, bits)]))
    }

    pub(crate) fn from_columns(columns: impl IntoIterator<Item = ColumnId>) -> Result<Self> {
        let mut set = Self::default();
        for column in columns {
            set.insert(column)?;
        }
        Ok(set)
    }

    pub(crate) fn contains(&self, column: &ColumnId) -> bool {
        self.0
            .iter()
            .find(|(relation, _)| *relation == column.relation)
            .is_some_and(|(_, bits)| bits.get(column_bit(column.position)))
    }

    pub(crate) fn insert(&mut self, column: ColumnId) -> Result<()> {
        let index = match self.0.binary_search_by_key(&column.relation, |(id, _)| *id) {
            Ok(index) => index,
            Err(index) => {
                self.0.insert(index, (column.relation, BitSet::default()));
                index
            }
        };
        self.0[index].1.set(column_bit(column.position))?;
        Ok(())
    }

    pub(crate) fn is_disjoint(&self, other: &Self) -> bool {
        self.0.iter().all(|(relation, bits)| {
            other
                .0
                .iter()
                .find(|(other, _)| other == relation)
                .is_none_or(|(_, other)| !bits.intersects(other))
        })
    }

    pub(crate) fn difference(mut self, other: &Self) -> Self {
        self.0.retain_mut(|(relation, bits)| {
            if let Some((_, other)) = other.0.iter().find(|(other, _)| other == relation) {
                bits.subtract(other);
            }
            bits.count() != 0
        });
        self
    }

    pub(crate) fn union_with(&mut self, other: Self) -> Result<()> {
        for (relation, bits) in other.0 {
            match self.0.binary_search_by_key(&relation, |(id, _)| *id) {
                Ok(index) => self.0[index].1.union_with(&bits)?,
                Err(index) => self.0.insert(index, (relation, bits)),
            }
        }
        Ok(())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.0.iter().map(|(_, bits)| bits.count()).sum()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = ColumnId> + '_ {
        self.0.iter().flat_map(|(relation, bits)| {
            bits.iter().map(|bit| ColumnId {
                relation: *relation,
                position: bit.checked_sub(1),
            })
        })
    }
}

fn column_bit(position: Option<usize>) -> usize {
    position.map_or(0, |column| {
        column.checked_add(1).expect("valid column index")
    })
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
            let mut actual =
                ColumnSet::from_columns(input.iter().rev().step_by(stride).copied()).unwrap();
            let mut expected: BTreeSet<_> = input.iter().rev().step_by(stride).copied().collect();
            for column in input.iter().step_by(2) {
                actual.insert(*column).unwrap();
                expected.insert(*column);
            }
            assert_eq!(
                actual.iter().collect::<Vec<_>>(),
                expected.iter().copied().collect::<Vec<_>>()
            );
            let other = ColumnSet::from_columns(input.iter().step_by(3).copied()).unwrap();
            let expected_other: BTreeSet<_> = other.iter().collect();
            assert_eq!(
                actual.is_disjoint(&other),
                expected.is_disjoint(&expected_other)
            );
            assert_eq!(
                actual.clone().difference(&other).iter().collect::<Vec<_>>(),
                expected
                    .difference(&expected_other)
                    .copied()
                    .collect::<Vec<_>>()
            );
            actual.union_with(other).unwrap();
            expected.extend(expected_other);
            assert_eq!(
                actual.iter().collect::<Vec<_>>(),
                expected.iter().copied().collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn relation_columns_preserve_rowids_and_word_boundaries() {
        for columns in [0, 1, 62, 63, 64, 65, 127, 128, 4096] {
            for rowid in [false, true] {
                let actual = ColumnSet::for_relation(5.into(), columns, rowid).unwrap();
                let expected: BTreeSet<_> = (0..columns)
                    .map(Some)
                    .chain(rowid.then_some(None))
                    .map(|position| ColumnId {
                        relation: 5.into(),
                        position,
                    })
                    .collect();
                assert_eq!(actual.len(), expected.len());
                assert_eq!(actual.is_empty(), expected.is_empty());
                assert_eq!(
                    actual.iter().collect::<Vec<_>>(),
                    expected.into_iter().collect::<Vec<_>>()
                );
                assert!(actual.clone().difference(&actual).is_empty());
            }
        }
    }
}
