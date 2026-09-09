use tantivy_fst::map::{OpBuilder, Union};
use tantivy_fst::raw::IndexedValue;
use tantivy_fst::Streamer;

use super::termdict::TermDictionary;
use crate::postings::TermInfo;
use crate::termdict::{TermOrdinal, TermStreamer};

/// Given a list of sorted term streams,
/// returns an iterator over sorted unique terms.
///
/// The item yielded is actually a pair with
/// - the term
/// - a slice with the ordinal of the segments containing the term.
pub struct TermMerger<'a> {
    dictionaries: Vec<&'a TermDictionary>,
    union: Union<'a>,
    current_key: Vec<u8>,
    current_segment_and_term_ordinals: Vec<IndexedValue>,
}

impl<'a> TermMerger<'a> {
    /// Stream of merged term dictionary
    pub fn new(streams: Vec<TermStreamer<'a>>) -> TermMerger<'a> {
        let mut op_builder = OpBuilder::new();
        let mut dictionaries = vec![];
        for streamer in streams {
            op_builder.push(streamer.stream);
            dictionaries.push(streamer.fst_map);
        }
        TermMerger {
            dictionaries,
            union: op_builder.union(),
            current_key: vec![],
            current_segment_and_term_ordinals: vec![],
        }
    }

    /// Iterator over `(segment ordinal, TermOrdinal)` pairs sorted by segment ordinal
    ///
    /// This method may be called
    /// if [`Self::advance`] has been called before
    /// and `true` was returned.
    pub fn matching_segments<'b: 'a>(&'b self) -> impl 'b + Iterator<Item = (usize, TermOrdinal)> {
        self.current_segment_and_term_ordinals
            .iter()
            .map(|iv| (iv.index, iv.value))
    }

    /// Advance the term iterator to the next term.
    /// Returns `true` if there is indeed another term
    /// `false` if there is none.
    pub fn advance(&mut self) -> bool {
        let (key, values) = if let Some((key, values)) = self.union.next() {
            (key, values)
        } else {
            return false;
        };
        self.current_key.clear();
        self.current_key.extend_from_slice(key);
        self.current_segment_and_term_ordinals.clear();
        self.current_segment_and_term_ordinals
            .extend_from_slice(values);
        self.current_segment_and_term_ordinals
            .sort_by_key(|iv| iv.index);
        true
    }

    /// Returns the current term.
    ///
    /// This method may be called if [`Self::advance`] has been called before
    /// and `true` was returned.
    pub fn key(&self) -> &[u8] {
        &self.current_key
    }

    /// Iterator over `(segment ordinal, TermInfo)` pairs sorted by the ordinal.
    ///
    /// This method may be called if [`Self::advance`] has been called before
    /// and `true` was returned.
    pub fn current_segment_ords_and_term_infos<'b: 'a>(
        &'b self,
    ) -> impl 'b + Iterator<Item = (usize, TermInfo)> {
        self.current_segment_and_term_ordinals
            .iter()
            .map(move |iv| {
                (
                    iv.index,
                    self.dictionaries[iv.index].term_info_from_ord(iv.value),
                )
            })
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {
    use rand::distributions::Alphanumeric;
    use rand::{rng, Rng};
    use test::{self, Bencher};

    use super::TermMerger;
    use crate::directory::FileSlice;
    use crate::postings::TermInfo;
    use crate::termdict::{TermDictionary, TermDictionaryBuilder};

    fn make_term_info(term_ord: u64) -> TermInfo {
        let offset = |term_ord: u64| (term_ord * 100 + term_ord * term_ord) as usize;
        TermInfo {
            doc_freq: term_ord as u32,
            postings_range: offset(term_ord)..offset(term_ord + 1),
            positions_range: offset(term_ord)..offset(term_ord + 1),
        }
    }

    /// Create a dictionary of random strings.
    fn rand_dict(num_terms: usize) -> std::io::Result<TermDictionary> {
        let buffer: Vec<u8> = {
            let mut terms = vec![];
            for _i in 0..num_terms {
                let rand_string: String = rng()
                    .sample_iter(&Alphanumeric)
                    .take(rng().random_range(30..42))
                    .map(char::from)
                    .collect();
                terms.push(rand_string);
            }
            terms.sort();

            let mut term_dictionary_builder = TermDictionaryBuilder::create(Vec::new())?;
            for i in 0..num_terms {
                term_dictionary_builder.insert(terms[i].as_bytes(), &make_term_info(i as u64))?;
            }
            term_dictionary_builder.finish()?
        };
        let file = FileSlice::from(buffer);
        TermDictionary::open(file)
    }

    #[bench]
    fn bench_termmerger(b: &mut Bencher) -> crate::Result<()> {
        let dict1 = rand_dict(100_000)?;
        let dict2 = rand_dict(100_000)?;
        b.iter(|| -> crate::Result<u32> {
            let stream1 = dict1.stream()?;
            let stream2 = dict2.stream()?;
            let mut merger = TermMerger::new(vec![stream1, stream2]);
            let mut count = 0;
            while merger.advance() {
                count += 1;
            }
            Ok(count)
        });
        Ok(())
    }
}
