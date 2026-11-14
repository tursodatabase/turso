use crate::alloc::{TryReserveError, TursoFromIterator, TursoTryWithCapacityExt, TursoVecInExt};
use crate::mvcc::database::RowVersion;
use std::ops::{Deref, DerefMut};

/// The versions of one row, oldest first.
///
/// Most chains hold one version: the row of an insert, or the tombstone of a
/// delete of a B-tree row. Thus a chain keeps one version inline, in the
/// allocation of its `Arc`, and allocates a buffer only when it holds two or
/// more versions. When one version or none remains, the chain frees the
/// buffer.
#[derive(Clone, Debug, Default)]
pub enum VersionChain {
    #[default]
    Empty,
    One(RowVersion),
    Many(Vec<RowVersion>),
}

impl VersionChain {
    pub fn as_slice(&self) -> &[RowVersion] {
        match self {
            Self::Empty => &[],
            Self::One(version) => std::slice::from_ref(version),
            Self::Many(versions) => versions,
        }
    }

    pub fn as_mut_slice(&mut self) -> &mut [RowVersion] {
        match self {
            Self::Empty => &mut [],
            Self::One(version) => std::slice::from_mut(version),
            Self::Many(versions) => versions,
        }
    }

    /// The number of versions that the chain holds without a new allocation.
    /// The inline slot holds one.
    pub fn capacity(&self) -> usize {
        match self {
            Self::Empty | Self::One(_) => 1,
            Self::Many(versions) => versions.capacity(),
        }
    }

    pub fn try_reserve(&mut self, additional: usize) -> Result<(), TryReserveError> {
        self.reserve_buffer(additional, |versions, additional| {
            versions.try_reserve(additional)
        })
    }

    pub fn try_reserve_exact(&mut self, additional: usize) -> Result<(), TryReserveError> {
        self.reserve_buffer(additional, |versions, additional| {
            versions.try_reserve_exact(additional)
        })
    }

    /// Makes room for `additional` more versions. A chain that needs more than the
    /// inline slot moves its inline version into a buffer, so that a later insert
    /// does not allocate.
    fn reserve_buffer(
        &mut self,
        additional: usize,
        reserve: impl FnOnce(
            &mut Vec<RowVersion>,
            usize,
        ) -> Result<(), std::collections::TryReserveError>,
    ) -> Result<(), TryReserveError> {
        let len = self.len();
        if let Self::Many(versions) = self {
            reserve(versions, additional)?;
            return Ok(());
        }
        if len + additional <= 1 {
            return Ok(());
        }
        let mut versions = Vec::new();
        reserve(&mut versions, len + additional)?;
        if let Self::One(version) = std::mem::take(self) {
            versions.push(version);
        }
        *self = Self::Many(versions);
        Ok(())
    }

    pub fn insert(&mut self, index: usize, version: RowVersion) {
        match self {
            Self::Empty => {
                assert_eq!(index, 0, "insert index out of bounds for an empty chain");
                *self = Self::One(version);
            }
            Self::One(_) => {
                let Self::One(first) = std::mem::take(self) else {
                    unreachable!("the chain held one version")
                };
                let mut versions = Vec::with_capacity(2);
                versions.push(first);
                versions.insert(index, version);
                *self = Self::Many(versions);
            }
            Self::Many(versions) => versions.insert(index, version),
        }
    }

    pub fn push(&mut self, version: RowVersion) {
        self.insert(self.len(), version);
    }

    pub fn retain(&mut self, mut keep: impl FnMut(&RowVersion) -> bool) {
        self.retain_mut(|version| keep(version));
    }

    pub fn retain_mut(&mut self, mut keep: impl FnMut(&mut RowVersion) -> bool) {
        match self {
            Self::Empty => {}
            Self::One(version) => {
                if !keep(version) {
                    *self = Self::Empty;
                }
            }
            Self::Many(versions) => {
                versions.retain_mut(keep);
                self.free_buffer_of_short_chain();
            }
        }
    }

    pub fn clear(&mut self) {
        *self = Self::Empty;
    }

    pub fn shrink_to(&mut self, min_capacity: usize) {
        if let Self::Many(versions) = self {
            versions.shrink_to(min_capacity);
        }
        self.free_buffer_of_short_chain();
    }

    /// A chain with one version or none keeps it inline and frees the buffer.
    fn free_buffer_of_short_chain(&mut self) {
        let Self::Many(versions) = self else {
            return;
        };
        match versions.len() {
            0 => *self = Self::Empty,
            1 => {
                let version = versions.pop().expect("the chain held one version");
                *self = Self::One(version);
            }
            _ => {}
        }
    }
}

impl Deref for VersionChain {
    type Target = [RowVersion];

    fn deref(&self) -> &[RowVersion] {
        self.as_slice()
    }
}

impl DerefMut for VersionChain {
    fn deref_mut(&mut self) -> &mut [RowVersion] {
        self.as_mut_slice()
    }
}

impl<A> TursoVecInExt<RowVersion, A> for VersionChain {
    fn new_in(_alloc: A) -> Self {
        Self::Empty
    }

    fn with_capacity_in(capacity: usize, _alloc: A) -> Self {
        if capacity <= 1 {
            Self::Empty
        } else {
            Self::Many(Vec::with_capacity(capacity))
        }
    }

    fn try_with_capacity_in(capacity: usize, _alloc: A) -> Result<Self, TryReserveError> {
        let mut chain = Self::Empty;
        chain.try_reserve_exact(capacity)?;
        Ok(chain)
    }
}

impl TursoTryWithCapacityExt for VersionChain {
    fn try_with_capacity_ext(capacity: usize) -> Result<Self, TryReserveError> {
        let mut chain = Self::Empty;
        chain.try_reserve_exact(capacity)?;
        Ok(chain)
    }
}

impl TursoFromIterator<RowVersion> for VersionChain {
    fn try_from_iter<I>(iter: I) -> Result<Self, TryReserveError>
    where
        I: IntoIterator<Item = RowVersion>,
    {
        let mut chain = Self::Empty;
        chain.try_extend(iter)?;
        Ok(chain)
    }

    fn try_extend<I>(&mut self, iter: I) -> Result<(), TryReserveError>
    where
        I: IntoIterator<Item = RowVersion>,
    {
        for version in iter {
            self.try_reserve(1)?;
            self.push(version);
        }
        Ok(())
    }
}

impl From<Vec<RowVersion>> for VersionChain {
    fn from(mut versions: Vec<RowVersion>) -> Self {
        match versions.len() {
            0 => Self::Empty,
            1 => Self::One(versions.pop().expect("the vector held one version")),
            _ => Self::Many(versions),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::VersionChain;
    use crate::mvcc::database::{RowVersion, TxTimestampOrID};

    fn version(id: u64) -> RowVersion {
        RowVersion::new(id, Some(TxTimestampOrID::Timestamp(id)), None, None, false)
    }

    fn ids(chain: &VersionChain) -> Vec<u64> {
        chain.iter().map(|version| version.id).collect()
    }

    #[test]
    fn a_chain_is_no_bigger_than_its_inline_version() {
        assert_eq!(
            std::mem::size_of::<VersionChain>(),
            std::mem::size_of::<RowVersion>()
        );
    }

    #[test]
    fn one_version_stays_inline_and_two_move_into_a_buffer() {
        let mut chain = VersionChain::default();
        chain.push(version(1));
        assert!(matches!(chain, VersionChain::One(_)));
        assert_eq!(chain.capacity(), 1);

        chain.insert(0, version(0));
        assert!(matches!(chain, VersionChain::Many(_)));
        assert_eq!(ids(&chain), [0, 1]);
    }

    #[test]
    fn reserving_space_moves_the_inline_version_before_the_insert() {
        let mut chain = VersionChain::default();
        chain.try_reserve_exact(1).unwrap();
        assert!(matches!(chain, VersionChain::Empty));
        chain.push(version(1));

        chain.try_reserve_exact(1).unwrap();
        assert!(matches!(chain, VersionChain::Many(_)));
        assert_eq!((chain.len(), chain.capacity()), (1, 2));
        chain.push(version(2));
        assert_eq!((chain.len(), chain.capacity()), (2, 2));
        assert_eq!(ids(&chain), [1, 2]);
    }

    #[test]
    fn a_chain_frees_its_buffer_when_one_version_or_none_remains() {
        let mut chain = VersionChain::from(vec![version(1), version(2), version(3)]);
        chain.retain(|version| version.id == 2);
        assert!(matches!(chain, VersionChain::One(_)));
        assert_eq!(ids(&chain), [2]);

        chain.retain(|_| false);
        assert!(matches!(chain, VersionChain::Empty));
        assert!(chain.is_empty());
    }
}
