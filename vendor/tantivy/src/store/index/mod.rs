const CHECKPOINT_PERIOD: usize = 8;

use std::fmt;
use std::ops::Range;
mod block;
mod skip_index;
mod skip_index_builder;

pub use self::skip_index::SkipIndex;
pub use self::skip_index_builder::SkipIndexBuilder;
use crate::DocId;

/// A checkpoint contains meta-information about
/// a block. Either a block of documents, or another block
/// of checkpoints.
///
/// All of the intervals here defined are semi-open.
/// The checkpoint describes that the block within the `byte_range`
/// and spans over the `doc_range`.
#[derive(Clone, Eq, PartialEq, Default)]
pub struct Checkpoint {
    pub doc_range: Range<DocId>,
    pub byte_range: Range<usize>,
}

impl Checkpoint {
    pub(crate) fn follows(&self, other: &Checkpoint) -> bool {
        (self.doc_range.start == other.doc_range.end)
            && (self.byte_range.start == other.byte_range.end)
    }
}

impl fmt::Debug for Checkpoint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "(doc={:?}, bytes={:?})", self.doc_range, self.byte_range)
    }
}

#[cfg(test)]
mod tests {

    use std::io;

    use proptest::prelude::*;

    use super::{SkipIndex, SkipIndexBuilder};
    use crate::directory::OwnedBytes;
    use crate::indexer::NoMergePolicy;
    use crate::schema::{SchemaBuilder, STORED, TEXT};
    use crate::store::index::Checkpoint;
    use crate::{DocAddress, DocId, Index, IndexWriter, TantivyDocument, Term};

    #[test]
    fn test_skip_index_empty() -> io::Result<()> {
        let mut output: Vec<u8> = Vec::new();
        let skip_index_builder: SkipIndexBuilder = SkipIndexBuilder::new();
        skip_index_builder.serialize_into(&mut output)?;
        let skip_index: SkipIndex = SkipIndex::open(OwnedBytes::new(output));
        let mut skip_cursor = skip_index.checkpoints();
        assert!(skip_cursor.next().is_none());
        Ok(())
    }

    #[test]
    fn test_skip_index_single_el() -> io::Result<()> {
        let mut output: Vec<u8> = Vec::new();
        let mut skip_index_builder: SkipIndexBuilder = SkipIndexBuilder::new();
        let checkpoint = Checkpoint {
            doc_range: 0..2,
            byte_range: 0..3,
        };
        skip_index_builder.insert(checkpoint.clone());
        skip_index_builder.serialize_into(&mut output)?;
        let skip_index: SkipIndex = SkipIndex::open(OwnedBytes::new(output));
        let mut skip_cursor = skip_index.checkpoints();
        assert_eq!(skip_cursor.next(), Some(checkpoint));
        assert_eq!(skip_cursor.next(), None);
        Ok(())
    }

    #[test]
    fn test_skip_index() -> io::Result<()> {
        let mut output: Vec<u8> = Vec::new();
        let checkpoints = vec![
            Checkpoint {
                doc_range: 0..3,
                byte_range: 0..9,
            },
            Checkpoint {
                doc_range: 3..4,
                byte_range: 9..25,
            },
            Checkpoint {
                doc_range: 4..6,
                byte_range: 25..49,
            },
            Checkpoint {
                doc_range: 6..8,
                byte_range: 49..81,
            },
            Checkpoint {
                doc_range: 8..10,
                byte_range: 81..100,
            },
        ];

        let mut skip_index_builder: SkipIndexBuilder = SkipIndexBuilder::new();
        for checkpoint in &checkpoints {
            skip_index_builder.insert(checkpoint.clone());
        }
        skip_index_builder.serialize_into(&mut output)?;

        let skip_index: SkipIndex = SkipIndex::open(OwnedBytes::new(output));
        assert_eq!(
            &skip_index.checkpoints().collect::<Vec<_>>()[..],
            &checkpoints[..]
        );
        Ok(())
    }

    fn offset_test(doc: DocId) -> usize {
        (doc as usize) * (doc as usize)
    }

    #[test]
    fn test_merge_store_with_stacking_reproducing_issue969() -> crate::Result<()> {
        let mut schema_builder = SchemaBuilder::default();
        let text = schema_builder.add_text_field("text", STORED | TEXT);
        let body = schema_builder.add_text_field("body", STORED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests()?;
        index_writer.set_merge_policy(Box::new(NoMergePolicy));
        let long_text: String = "abcdefghijklmnopqrstuvwxyz".repeat(1_000);
        for _ in 0..20 {
            index_writer.add_document(doc!(body=>long_text.clone()))?;
        }
        index_writer.commit()?;
        index_writer.add_document(doc!(text=>"testb"))?;
        for _ in 0..10 {
            index_writer.add_document(doc!(text=>"testd", body=>long_text.clone()))?;
        }
        index_writer.commit()?;
        index_writer.delete_term(Term::from_field_text(text, "testb"));
        index_writer.commit()?;
        let segment_ids = index.searchable_segment_ids()?;
        index_writer.merge(&segment_ids).wait().unwrap();
        let reader = index.reader()?;
        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 30);
        for i in 0..searcher.num_docs() as u32 {
            let _doc = searcher.doc::<TantivyDocument>(DocAddress::new(0u32, i))?;
        }
        Ok(())
    }

    #[test]
    fn test_skip_index_long() -> io::Result<()> {
        let mut output: Vec<u8> = Vec::new();
        let checkpoints: Vec<Checkpoint> = (0..1000)
            .map(|i| Checkpoint {
                doc_range: i..(i + 1),
                byte_range: offset_test(i)..offset_test(i + 1),
            })
            .collect();
        let mut skip_index_builder = SkipIndexBuilder::new();
        for checkpoint in &checkpoints {
            skip_index_builder.insert(checkpoint.clone());
        }
        skip_index_builder.serialize_into(&mut output)?;
        assert_eq!(output.len(), 4035);
        let resulting_checkpoints: Vec<Checkpoint> = SkipIndex::open(OwnedBytes::new(output))
            .checkpoints()
            .collect();
        assert_eq!(&resulting_checkpoints, &checkpoints);
        Ok(())
    }

    fn integrate_delta(vals: Vec<usize>) -> Vec<usize> {
        let mut output = Vec::with_capacity(vals.len() + 1);
        output.push(0);
        let mut prev = 0;
        for val in vals {
            let new_val = val + prev;
            prev = new_val;
            output.push(new_val);
        }
        output
    }

    // Generates a sequence of n valid checkpoints, with n < max_len.
    fn monotonic_checkpoints(max_len: usize) -> BoxedStrategy<Vec<Checkpoint>> {
        (0..max_len)
            .prop_flat_map(move |len: usize| {
                (
                    proptest::collection::vec(1usize..20, len).prop_map(integrate_delta),
                    proptest::collection::vec(1usize..26, len).prop_map(integrate_delta),
                )
                    .prop_map(|(docs, offsets)| {
                        (0..docs.len() - 1)
                            .map(move |i| Checkpoint {
                                doc_range: docs[i] as DocId..docs[i + 1] as DocId,
                                byte_range: offsets[i]..offsets[i + 1],
                            })
                            .collect::<Vec<Checkpoint>>()
                    })
            })
            .boxed()
    }

    fn seek_manual<I: Iterator<Item = Checkpoint>>(
        checkpoints: I,
        target: DocId,
    ) -> Option<Checkpoint> {
        checkpoints
            .into_iter()
            .find(|checkpoint| checkpoint.doc_range.end > target)
    }

    fn test_skip_index_aux(skip_index: SkipIndex, checkpoints: &[Checkpoint]) {
        if let Some(last_checkpoint) = checkpoints.last() {
            for doc in 0u32..last_checkpoint.doc_range.end {
                let expected = seek_manual(skip_index.checkpoints(), doc);
                assert_eq!(expected, skip_index.seek(doc), "Doc {doc}");
            }
            assert!(skip_index.seek(last_checkpoint.doc_range.end).is_none());
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(20))]
        #[test]
        fn test_proptest_skip(checkpoints in monotonic_checkpoints(100)) {
             let mut skip_index_builder = SkipIndexBuilder::new();
             for checkpoint in checkpoints.iter().cloned() {
                 skip_index_builder.insert(checkpoint);
             }
             let mut buffer = Vec::new();
             skip_index_builder.serialize_into(&mut buffer).unwrap();
             let skip_index = SkipIndex::open(OwnedBytes::new(buffer));
             let iter_checkpoints: Vec<Checkpoint> = skip_index.checkpoints().collect();
             assert_eq!(&checkpoints[..], &iter_checkpoints[..]);
             test_skip_index_aux(skip_index, &checkpoints[..]);
         }
    }
}
