use crate::storage::pager::CreateBTreeFlags;
use crate::sync::Arc;
use crate::types::IndexInfo;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BtreeDescriptor {
    Table,
    Index(Arc<IndexInfo>),
}

impl BtreeDescriptor {
    pub fn flags(&self) -> CreateBTreeFlags {
        match self {
            Self::Table => CreateBTreeFlags::new_table(),
            Self::Index(_) => CreateBTreeFlags::new_index(),
        }
    }
}

impl BtreeDescriptor {
    pub(crate) fn encode(&self) -> crate::Result<Vec<u8>> {
        use crate::translate::collate::{CollationSeq, TextCollation};
        use turso_parser::ast::{NullsOrder, SortOrder};
        let Self::Index(info) = self else {
            return Ok(vec![1, 0]);
        };
        assert_eq!(info.key_info.len(), info.num_cols);
        if info.num_cols == 0 {
            return Err(invalid_descriptor());
        }
        let mut bytes = vec![1, 1, u8::from(info.has_rowid), u8::from(info.is_unique)];
        bytes.extend_from_slice(
            &u32::try_from(info.num_cols)
                .map_err(|_| invalid_descriptor())?
                .to_le_bytes(),
        );
        for key in &info.key_info {
            bytes.push(match key.sort_order {
                SortOrder::Asc => 0,
                SortOrder::Desc => 1,
            });
            bytes.push(match key.nulls_order {
                None => 0,
                Some(NullsOrder::First) => 1,
                Some(NullsOrder::Last) => 2,
            });
            let (kind, name) = match key.collation {
                CollationSeq::Text(TextCollation::Custom(_)) => {
                    return Err(crate::LimboError::ParseError(
                        "custom collations cannot be stored in a B-tree descriptor".into(),
                    ))
                }
                CollationSeq::Text(text) => (0, text.name()),
                CollationSeq::Type(order) => (1, order.name().to_string()),
            };
            bytes.push(kind);
            bytes.extend_from_slice(
                &u32::try_from(name.len())
                    .map_err(|_| invalid_descriptor())?
                    .to_le_bytes(),
            );
            bytes.extend_from_slice(name.as_bytes());
        }
        Ok(bytes)
    }

    pub(crate) fn decode(mut bytes: &[u8]) -> crate::Result<Self> {
        use crate::translate::collate::{CollationSeq, TextCollation};
        use crate::type_ordering::TypeOrdering;
        use crate::types::KeyInfo;
        use turso_parser::ast::{NullsOrder, SortOrder};
        if take(&mut bytes, 1)? != [1] {
            return Err(invalid_descriptor());
        }
        match take(&mut bytes, 1)? {
            [0] if bytes.is_empty() => Ok(Self::Table),
            [1] => {
                let has_rowid = boolean(&mut bytes)?;
                let is_unique = boolean(&mut bytes)?;
                let count = number(&mut bytes)?;
                if count == 0 || count > bytes.len() / 7 {
                    return Err(invalid_descriptor());
                }
                let mut keys = Vec::with_capacity(count);
                for _ in 0..count {
                    let sort_order = match take(&mut bytes, 1)? {
                        [0] => SortOrder::Asc,
                        [1] => SortOrder::Desc,
                        _ => return Err(invalid_descriptor()),
                    };
                    let nulls_order = match take(&mut bytes, 1)? {
                        [0] => None,
                        [1] => Some(NullsOrder::First),
                        [2] => Some(NullsOrder::Last),
                        _ => return Err(invalid_descriptor()),
                    };
                    let kind = take(&mut bytes, 1)?[0];
                    let len = number(&mut bytes)?;
                    let name = std::str::from_utf8(take(&mut bytes, len)?)
                        .map_err(|_| invalid_descriptor())?;
                    let collation = match kind {
                        0 => CollationSeq::Text(
                            TextCollation::new(name).map_err(|_| invalid_descriptor())?,
                        ),
                        1 => CollationSeq::Type(
                            *TypeOrdering::ALL
                                .iter()
                                .find(|order| order.name() == name)
                                .ok_or_else(invalid_descriptor)?,
                        ),
                        _ => return Err(invalid_descriptor()),
                    };
                    keys.push(KeyInfo {
                        sort_order,
                        nulls_order,
                        collation,
                    });
                }
                if !bytes.is_empty() {
                    return Err(invalid_descriptor());
                }
                Ok(Self::Index(Arc::new(IndexInfo::new(
                    keys, has_rowid, count, is_unique,
                )?)))
            }
            _ => Err(invalid_descriptor()),
        }
    }
}

fn boolean(bytes: &mut &[u8]) -> crate::Result<bool> {
    match take(bytes, 1)? {
        [0] => Ok(false),
        [1] => Ok(true),
        _ => Err(invalid_descriptor()),
    }
}

fn number(bytes: &mut &[u8]) -> crate::Result<usize> {
    Ok(u32::from_le_bytes(take(bytes, 4)?.try_into().unwrap()) as usize)
}

fn take<'a>(bytes: &mut &'a [u8], count: usize) -> crate::Result<&'a [u8]> {
    if count > bytes.len() {
        return Err(invalid_descriptor());
    }
    let (head, rest) = bytes.split_at(count);
    *bytes = rest;
    Ok(head)
}

fn invalid_descriptor() -> crate::LimboError {
    crate::LimboError::Corrupt("invalid B-tree descriptor".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::collate::CollationSeq;
    use crate::type_ordering::TypeOrdering;
    use crate::types::KeyInfo;
    use quickcheck_macros::quickcheck;
    use turso_parser::ast::{NullsOrder, SortOrder};

    #[quickcheck]
    fn descriptor_roundtrip(keys: Vec<(bool, u8, u8)>, has_rowid: bool, unique: bool) -> bool {
        let mut keys: Vec<_> = keys
            .into_iter()
            .take(64)
            .map(|(descending, nulls, collation)| KeyInfo {
                sort_order: if descending {
                    SortOrder::Desc
                } else {
                    SortOrder::Asc
                },
                nulls_order: match nulls % 3 {
                    0 => None,
                    1 => Some(NullsOrder::First),
                    _ => Some(NullsOrder::Last),
                },
                collation: match collation % 6 {
                    0 => CollationSeq::BINARY,
                    1 => CollationSeq::NOCASE,
                    2 => CollationSeq::RTRIM,
                    3 => CollationSeq::Type(TypeOrdering::Numeric),
                    4 => CollationSeq::Type(TypeOrdering::Array),
                    _ => CollationSeq::Type(TypeOrdering::TextArray),
                },
            })
            .collect();
        if has_rowid || keys.is_empty() {
            keys.push(KeyInfo {
                sort_order: SortOrder::Asc,
                nulls_order: None,
                collation: CollationSeq::BINARY,
            });
        }
        let count = keys.len();
        let descriptor = BtreeDescriptor::Index(Arc::new(
            IndexInfo::new(keys, has_rowid, count, unique).unwrap(),
        ));
        let snapshot = vec![
            BtreeSnapshotEntry {
                logical_id: -10,
                root_page: 2,
                descriptor: BtreeDescriptor::Table,
            },
            BtreeSnapshotEntry {
                logical_id: -11,
                root_page: 3,
                descriptor: descriptor.clone(),
            },
        ];
        BtreeDescriptor::decode(&descriptor.encode().unwrap()).unwrap() == descriptor
            && decode_snapshot(&encode_snapshot(&snapshot).unwrap()).unwrap() == snapshot
    }

    #[test]
    fn snapshot_rejects_invalid_roots_and_incomplete_records() {
        let bytes = encode_snapshot(&[BtreeSnapshotEntry {
            logical_id: -10,
            root_page: 2,
            descriptor: BtreeDescriptor::Table,
        }])
        .unwrap();
        for len in 0..bytes.len() {
            assert!(decode_snapshot(&bytes[..len]).is_err());
        }
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(decode_snapshot(&trailing).is_err());
        for root in [0i64, -1, i64::MIN, i64::MAX] {
            let mut invalid = bytes.clone();
            invalid[13..21].copy_from_slice(&root.to_le_bytes());
            assert!(decode_snapshot(&invalid).is_err());
            assert!(encode_snapshot(&[BtreeSnapshotEntry {
                logical_id: -10,
                root_page: root,
                descriptor: BtreeDescriptor::Table
            }])
            .is_err());
        }
        let mut duplicate = bytes.clone();
        duplicate[1..5].copy_from_slice(&2u32.to_le_bytes());
        duplicate.extend_from_slice(&bytes[5..]);
        assert!(decode_snapshot(&duplicate).is_err());
    }

    #[test]
    fn descriptor_rejects_truncation_and_trailing_bytes() {
        let info = IndexInfo::new(
            [KeyInfo {
                sort_order: SortOrder::Desc,
                nulls_order: Some(NullsOrder::Last),
                collation: CollationSeq::new("en-US").unwrap(),
            }],
            false,
            1,
            true,
        )
        .unwrap();
        let descriptor = BtreeDescriptor::Index(Arc::new(info));
        let bytes = descriptor.encode().unwrap();
        assert_eq!(BtreeDescriptor::decode(&bytes).unwrap(), descriptor);
        for end in 0..bytes.len() {
            assert!(BtreeDescriptor::decode(&bytes[..end]).is_err());
        }
        let mut trailing = bytes;
        trailing.push(0);
        assert!(BtreeDescriptor::decode(&trailing).is_err());
        assert_eq!(
            BtreeDescriptor::decode(&BtreeDescriptor::Table.encode().unwrap()).unwrap(),
            BtreeDescriptor::Table
        );
        assert!(BtreeDescriptor::decode(&[2, 0]).is_err());
        assert!(BtreeDescriptor::decode(&[1, 0, 0]).is_err());
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BtreeSnapshotEntry {
    pub(crate) logical_id: i64,
    pub(crate) root_page: i64,
    pub(crate) descriptor: BtreeDescriptor,
}

pub(crate) fn encode_snapshot(objects: &[BtreeSnapshotEntry]) -> crate::Result<Vec<u8>> {
    let mut bytes = vec![1];
    bytes.extend_from_slice(
        &u32::try_from(objects.len())
            .map_err(|_| invalid_descriptor())?
            .to_le_bytes(),
    );
    let mut ids = rustc_hash::FxHashSet::default();
    let mut roots = rustc_hash::FxHashSet::default();
    for object in objects {
        validate_snapshot_entry(object)?;
        if !ids.insert(object.logical_id) || !roots.insert(object.root_page) {
            return Err(invalid_descriptor());
        }
        let descriptor = object.descriptor.encode()?;
        bytes.extend_from_slice(&object.logical_id.to_le_bytes());
        bytes.extend_from_slice(&object.root_page.to_le_bytes());
        bytes.extend_from_slice(
            &u32::try_from(descriptor.len())
                .map_err(|_| invalid_descriptor())?
                .to_le_bytes(),
        );
        bytes.extend_from_slice(&descriptor);
    }
    Ok(bytes)
}

pub(crate) fn decode_snapshot(mut bytes: &[u8]) -> crate::Result<Vec<BtreeSnapshotEntry>> {
    if take(&mut bytes, 1)? != [1] {
        return Err(invalid_descriptor());
    }
    let count = number(&mut bytes)?;
    if count > bytes.len() / 22 {
        return Err(invalid_descriptor());
    }
    let mut objects = Vec::with_capacity(count);
    let mut ids = rustc_hash::FxHashSet::default();
    let mut roots = rustc_hash::FxHashSet::default();
    for _ in 0..count {
        let logical_id = i64::from_le_bytes(take(&mut bytes, 8)?.try_into().unwrap());
        let root_page = i64::from_le_bytes(take(&mut bytes, 8)?.try_into().unwrap());
        let len = number(&mut bytes)?;
        let descriptor = BtreeDescriptor::decode(take(&mut bytes, len)?)?;
        let object = BtreeSnapshotEntry {
            logical_id,
            root_page,
            descriptor,
        };
        validate_snapshot_entry(&object)?;
        if !ids.insert(logical_id) || !roots.insert(root_page) {
            return Err(invalid_descriptor());
        }
        objects.push(object);
    }
    if !bytes.is_empty() {
        return Err(invalid_descriptor());
    }
    Ok(objects)
}

fn validate_snapshot_entry(object: &BtreeSnapshotEntry) -> crate::Result<()> {
    if object.logical_id >= 0
        || i32::try_from(object.logical_id).is_err()
        || object.root_page <= 0
        || u32::try_from(object.root_page).is_err()
        || (object.logical_id == -1) != (object.root_page == 1)
        || (object.root_page == 1 && !matches!(object.descriptor, BtreeDescriptor::Table))
    {
        return Err(invalid_descriptor());
    }
    Ok(())
}
