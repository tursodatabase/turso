use std::cmp::Ordering;
use std::collections::{btree_map, BTreeMap, BTreeSet, BinaryHeap};
use std::io;
use std::ops::Bound;

use crate::collector::{Collector, SegmentCollector};
use crate::fastfield::FacetReader;
use crate::schema::Facet;
use crate::{DocId, Score, SegmentOrdinal, SegmentReader};

struct Hit<'a> {
    count: u64,
    facet: &'a Facet,
}

impl Eq for Hit<'_> {}

impl<'a> PartialEq<Hit<'a>> for Hit<'a> {
    fn eq(&self, other: &Hit<'_>) -> bool {
        self.count == other.count
    }
}

impl<'a> PartialOrd<Hit<'a>> for Hit<'a> {
    fn partial_cmp(&self, other: &Hit<'_>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Hit<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .count
            .cmp(&self.count)
            .then(self.facet.cmp(other.facet))
    }
}

fn facet_depth(facet_bytes: &[u8]) -> usize {
    if facet_bytes.is_empty() {
        0
    } else {
        facet_bytes.iter().cloned().filter(|b| *b == 0u8).count() + 1
    }
}

/// Collector for faceting
///
/// The collector collects all facets. You need to configure it
/// beforehand with the facet you want to extract.
///
/// This is done by calling `.add_facet(...)` with the root of the
/// facet you want to extract as argument.
///
/// Facet counts will only be computed for the facet that are direct children
/// of such a root facet.
///
/// For instance, if your index represents books, your hierarchy of facets
/// may contain `category`, `language`.
///
/// The category facet may include `subcategories`. For instance, a book
/// could belong to `/category/fiction/fantasy`.
///
/// If you request the facet counts for `/category`, the result will be
/// the breakdown of counts for the direct children of `/category`
/// (e.g. `/category/fiction`, `/category/biography`, `/category/personal_development`).
///
/// Once collection is finished, you can harvest its results in the form
/// of a [`FacetCounts`] object, and extract your facet counts from it.
///
/// This implementation assumes you are working with a number of facets that
/// is many hundreds of times smaller than your number of documents.
///
///
/// ```rust
/// use tantivy::collector::FacetCollector;
/// use tantivy::query::AllQuery;
/// use tantivy::schema::{Facet, Schema, FacetOptions, TEXT};
/// use tantivy::{doc, Index};
///
/// fn example() -> tantivy::Result<()> {
///     let mut schema_builder = Schema::builder();
///
///     // Facet have their own specific type.
///     // It is not a bad practise to put all of your
///     // facet information in the same field.
///     let facet = schema_builder.add_facet_field("facet", FacetOptions::default());
///     let title = schema_builder.add_text_field("title", TEXT);
///     let schema = schema_builder.build();
///     let index = Index::create_in_ram(schema);
///     {
///         let mut index_writer = index.writer(15_000_000)?;
///         // a document can be associated with any number of facets
///         index_writer.add_document(doc!(
///             title => "The Name of the Wind",
///             facet => Facet::from("/lang/en"),
///             facet => Facet::from("/category/fiction/fantasy")
///         ))?;
///         index_writer.add_document(doc!(
///             title => "Dune",
///             facet => Facet::from("/lang/en"),
///             facet => Facet::from("/category/fiction/sci-fi")
///         ))?;
///         index_writer.add_document(doc!(
///             title => "La Vénus d'Ille",
///             facet => Facet::from("/lang/fr"),
///             facet => Facet::from("/category/fiction/fantasy"),
///             facet => Facet::from("/category/fiction/horror")
///         ))?;
///         index_writer.add_document(doc!(
///             title => "The Diary of a Young Girl",
///             facet => Facet::from("/lang/en"),
///             facet => Facet::from("/category/biography")
///         ))?;
///         index_writer.commit()?;
///     }
///     let reader = index.reader()?;
///     let searcher = reader.searcher();
///
///     {
///         let mut facet_collector = FacetCollector::for_field("facet");
///         facet_collector.add_facet("/lang");
///         facet_collector.add_facet("/category");
///         let facet_counts = searcher.search(&AllQuery, &facet_collector)?;
///
///         // This lists all of the facet counts
///         let facets: Vec<(&Facet, u64)> = facet_counts
///             .get("/category")
///             .collect();
///         assert_eq!(facets, vec![
///             (&Facet::from("/category/biography"), 1),
///             (&Facet::from("/category/fiction"), 3)
///         ]);
///     }
///
///     {
///         let mut facet_collector = FacetCollector::for_field("facet");
///         facet_collector.add_facet("/category/fiction");
///         let facet_counts = searcher.search(&AllQuery, &facet_collector)?;
///
///         // This lists all of the facet counts
///         let facets: Vec<(&Facet, u64)> = facet_counts
///             .get("/category/fiction")
///             .collect();
///         assert_eq!(facets, vec![
///             (&Facet::from("/category/fiction/fantasy"), 2),
///             (&Facet::from("/category/fiction/horror"), 1),
///             (&Facet::from("/category/fiction/sci-fi"), 1)
///         ]);
///     }
///
///     {
///         let mut facet_collector = FacetCollector::for_field("facet");
///         facet_collector.add_facet("/category/fiction");
///         let facet_counts = searcher.search(&AllQuery, &facet_collector)?;
///
///         // This lists all of the facet counts
///         let facets: Vec<(&Facet, u64)> = facet_counts.top_k("/category/fiction", 1);
///         assert_eq!(facets, vec![
///             (&Facet::from("/category/fiction/fantasy"), 2)
///         ]);
///     }
///
///     {
///         let mut facet_collector = FacetCollector::for_field("facet");
///         facet_collector.add_facet("/");
///         let facet_counts = searcher.search(&AllQuery, &facet_collector)?;
///
///         // This lists all of the facet counts
///         let facets: Vec<(&Facet, u64)> = facet_counts
///             .get("/")
///             .collect();
///         assert_eq!(facets, vec![
///             (&Facet::from("/category"), 4),
///             (&Facet::from("/lang"), 4)
///         ]);
///     }
///
///     Ok(())
/// }
/// # assert!(example().is_ok());
/// ```
pub struct FacetCollector {
    field_name: String,
    facets: BTreeSet<Facet>,
}

pub struct FacetSegmentCollector {
    reader: FacetReader,
    // collapse facet_id -> count
    counts: Vec<u64>,
    // facet_ord -> compressed collapse facet_id
    compressed_collapse_mapping: Vec<usize>,
    // compressed collapse facet_id -> facet_ord
    unique_facet_ords: Vec<(u64, usize)>,
}

impl FacetCollector {
    /// Create a facet collector to collect the facets
    /// from a specific facet `Field`.
    ///
    /// This function does not check whether the field
    /// is of the proper type.
    pub fn for_field(field_name: impl ToString) -> FacetCollector {
        FacetCollector {
            field_name: field_name.to_string(),
            facets: BTreeSet::default(),
        }
    }

    /// Adds a facet that we want to record counts
    ///
    /// Adding facet `Facet::from("/country")` for instance,
    /// will record the counts of all of the direct children of the facet country
    /// (e.g. `/country/FR`, `/country/UK`).
    ///
    /// Adding two facets within which one is the prefix of the other is forbidden.
    /// If you need the correct number of unique documents for two such facets,
    /// just add them in a separate `FacetCollector`.
    pub fn add_facet<T>(&mut self, facet_from: T)
    where Facet: From<T> {
        let facet = Facet::from(facet_from);
        for old_facet in &self.facets {
            assert!(
                !old_facet.is_prefix_of(&facet),
                "Tried to add a facet which is a descendant of an already added facet."
            );
            assert!(
                !facet.is_prefix_of(old_facet),
                "Tried to add a facet which is an ancestor of an already added facet."
            );
        }
        self.facets.insert(facet);
    }
}

fn compress_mapping(mapping: &[(u64, usize)]) -> (Vec<usize>, Vec<(u64, usize)>) {
    // facet_ord -> collapse facet_id
    let mut compressed_collapse_mapping: Vec<usize> = Vec::with_capacity(mapping.len());
    // collapse facet_id -> facet_ord
    let mut unique_facet_ords: Vec<(u64, usize)> = Vec::new();
    if mapping.is_empty() {
        return (Vec::new(), Vec::new());
    }
    compressed_collapse_mapping.push(0);
    unique_facet_ords.push(mapping[0]);
    let mut last_facet_ord = mapping[0];
    let mut last_facet_id = 0;
    for &facet_ord in &mapping[1..] {
        if facet_ord != last_facet_ord {
            last_facet_id += 1;
            last_facet_ord = facet_ord;
            unique_facet_ords.push(facet_ord);
        }
        compressed_collapse_mapping.push(last_facet_id);
    }
    (compressed_collapse_mapping, unique_facet_ords)
}

impl Collector for FacetCollector {
    type Fruit = FacetCounts;

    type Child = FacetSegmentCollector;

    fn for_segment(
        &self,
        _: SegmentOrdinal,
        reader: &SegmentReader,
    ) -> crate::Result<FacetSegmentCollector> {
        let facet_reader = reader.facet_reader(&self.field_name)?;
        let facet_dict = facet_reader.facet_dict();
        let collapse_mapping: Vec<(u64, usize)> =
            compute_collapse_mapping(facet_dict, &self.facets)?;
        let (compressed_collapse_mapping, unique_facet_ords) = compress_mapping(&collapse_mapping);
        let counts = vec![0u64; unique_facet_ords.len()];
        Ok(FacetSegmentCollector {
            reader: facet_reader,
            compressed_collapse_mapping,
            counts,
            unique_facet_ords,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, segments_facet_counts: Vec<FacetCounts>) -> crate::Result<FacetCounts> {
        let mut facet_counts: BTreeMap<Facet, u64> = BTreeMap::new();
        for segment_facet_counts in segments_facet_counts {
            for (facet, count) in segment_facet_counts.facet_counts {
                *(facet_counts.entry(facet).or_insert(0)) += count;
            }
        }
        Ok(FacetCounts { facet_counts })
    }
}

fn is_child_facet(parent_facet: &[u8], possible_child_facet: &[u8]) -> bool {
    if !possible_child_facet.starts_with(parent_facet) {
        return false;
    }
    if parent_facet.is_empty() {
        return true;
    }
    possible_child_facet.get(parent_facet.len()).copied() == Some(0u8)
}

fn compute_collapse_mapping_one(
    facet_terms: &mut columnar::Streamer,
    facet_bytes: &[u8],
    collapsed: &mut [(u64, usize)],
) -> io::Result<bool> {
    let mut facet_child: Vec<u8> = Vec::new();
    let mut term_ord = 0;
    let offset = facet_bytes.len() + 1;
    let depth = facet_depth(facet_bytes);
    loop {
        match facet_terms.key().cmp(facet_bytes) {
            Ordering::Less | Ordering::Equal => {}
            Ordering::Greater => {
                if !is_child_facet(facet_bytes, facet_terms.key()) {
                    return Ok(true);
                }
                let suffix = &facet_terms.key()[offset..];
                if facet_child.is_empty() || !is_child_facet(&facet_child, suffix) {
                    facet_child.clear();
                    term_ord = facet_terms.term_ord();
                    let end = suffix
                        .iter()
                        .position(|b| *b == 0u8)
                        .unwrap_or(suffix.len());
                    facet_child.extend(&suffix[..end]);
                }
                collapsed[facet_terms.term_ord() as usize] = (term_ord, depth);
            }
        }
        if !facet_terms.advance() {
            return Ok(false);
        }
    }
}

fn compute_collapse_mapping(
    facet_dict: &columnar::Dictionary,
    facets: &BTreeSet<Facet>,
) -> io::Result<Vec<(u64, usize)>> {
    let mut collapsed = vec![(u64::MAX, 0); facet_dict.num_terms()];
    if facets.is_empty() {
        return Ok(collapsed);
    }
    let mut facet_terms: columnar::Streamer = facet_dict.range().into_stream()?;
    if !facet_terms.advance() {
        return Ok(collapsed);
    }
    let mut facet_bytes = Vec::new();
    for facet in facets {
        facet_bytes.clear();
        facet_bytes.extend(facet.encoded_str().as_bytes());
        if !compute_collapse_mapping_one(&mut facet_terms, &facet_bytes, &mut collapsed[..])? {
            break;
        }
    }
    Ok(collapsed)
}

impl SegmentCollector for FacetSegmentCollector {
    type Fruit = FacetCounts;

    fn collect(&mut self, doc: DocId, _: Score) {
        let mut previous_collapsed_ord: usize = usize::MAX;
        for facet_ord in self.reader.facet_ords(doc) {
            let collapsed_ord = self.compressed_collapse_mapping[facet_ord as usize];
            self.counts[collapsed_ord] += u64::from(collapsed_ord != previous_collapsed_ord);
            previous_collapsed_ord = collapsed_ord;
        }
    }

    /// Returns the results of the collection.
    ///
    /// This method does not just return the counters,
    /// it also translates the facet ordinals of the last segment.
    fn harvest(self) -> FacetCounts {
        let mut facet_counts = BTreeMap::new();
        let facet_dict = self.reader.facet_dict();
        for (collapsed_facet_ord, count) in self.counts.iter().cloned().enumerate() {
            if count == 0 {
                continue;
            }
            let mut facet = vec![];
            let (facet_ord, facet_depth) = self.unique_facet_ords[collapsed_facet_ord];
            // TODO handle errors.
            if facet_dict.ord_to_term(facet_ord, &mut facet).is_ok() {
                if let Some((end_collapsed_facet, _)) = facet
                    .iter()
                    .enumerate()
                    .filter(|(_pos, &b)| b == 0u8)
                    .nth(facet_depth)
                {
                    facet.truncate(end_collapsed_facet);
                }
                if let Ok(facet) = Facet::from_encoded(facet) {
                    facet_counts.insert(facet, count);
                }
            }
        }
        FacetCounts { facet_counts }
    }
}

/// Intermediary result of the `FacetCollector` that stores
/// the facet counts for all the segments.
#[derive(Default, Clone)]
pub struct FacetCounts {
    facet_counts: BTreeMap<Facet, u64>,
}

pub struct FacetChildIterator<'a> {
    underlying: btree_map::Range<'a, Facet, u64>,
}

impl<'a> Iterator for FacetChildIterator<'a> {
    type Item = (&'a Facet, u64);

    fn next(&mut self) -> Option<Self::Item> {
        self.underlying.next().map(|(facet, count)| (facet, *count))
    }
}

impl FacetCounts {
    /// Returns an iterator over all of the facet count pairs inside this result.
    /// See the documentation for [`FacetCollector`] for a usage example.
    pub fn get<T>(&self, facet_from: T) -> FacetChildIterator<'_>
    where Facet: From<T> {
        let facet = Facet::from(facet_from);
        let lower_bound = Bound::Excluded(facet.clone());
        let upper_bound = if facet.is_root() {
            Bound::Unbounded
        } else {
            let mut facet_after_bytes: String = facet.encoded_str().to_owned();
            facet_after_bytes.push('\u{1}');
            let facet_after = Facet::from_encoded_string(facet_after_bytes);
            Bound::Excluded(facet_after)
        };
        let underlying: btree_map::Range<'_, _, _> =
            self.facet_counts.range((lower_bound, upper_bound));
        FacetChildIterator { underlying }
    }

    /// Returns a vector of top `k` facets with their counts, sorted highest-to-lowest by counts.
    /// See the documentation for [`FacetCollector`] for a usage example.
    pub fn top_k<T>(&self, facet: T, k: usize) -> Vec<(&Facet, u64)>
    where Facet: From<T> {
        let mut heap = BinaryHeap::with_capacity(k);
        let mut it = self.get(facet);

        // push the first k elements to first bring the heap
        // to capacity
        for (facet, count) in (&mut it).take(k) {
            heap.push(Hit { count, facet });
        }

        let mut lowest_count: u64 = heap.peek().map(|hit| hit.count).unwrap_or(u64::MIN); //< the `unwrap_or` case may be triggered but the value
                                                                                          // is never used in that case.

        for (facet, count) in it {
            if count > lowest_count {
                if let Some(mut head) = heap.peek_mut() {
                    *head = Hit { count, facet };
                }
                // the heap gets reconstructed at this point
                if let Some(head) = heap.peek() {
                    lowest_count = head.count;
                }
            }
        }
        heap.into_sorted_vec()
            .into_iter()
            .map(|hit| (hit.facet, hit.count))
            .collect::<Vec<_>>()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use columnar::Dictionary;
    use rand::distr::Uniform;
    use rand::prelude::SliceRandom;
    use rand::{rng, Rng};

    use super::{FacetCollector, FacetCounts};
    use crate::collector::facet_collector::compress_mapping;
    use crate::collector::Count;
    use crate::index::Index;
    use crate::query::{AllQuery, QueryParser, TermQuery};
    use crate::schema::{Facet, FacetOptions, IndexRecordOption, Schema, TantivyDocument};
    use crate::{IndexWriter, Term};

    fn test_collapse_mapping_aux(
        facet_terms: &[&str],
        facet_params: &[&str],
        expected_collapsed_mapping: &[(u64, usize)],
    ) {
        let mut facets: Vec<Facet> = facet_terms.iter().map(Facet::from).collect();
        facets.sort();
        let facet_terms: Vec<&str> = facets.iter().map(|facet| facet.encoded_str()).collect();
        let dictionary = Dictionary::build_for_tests(&facet_terms);
        let facet_params: BTreeSet<Facet> = facet_params.iter().map(Facet::from).collect();
        let collapse_mapping = super::compute_collapse_mapping(&dictionary, &facet_params).unwrap();
        assert_eq!(&collapse_mapping[..], expected_collapsed_mapping);
    }

    #[test]
    fn test_collapse_simple() {
        test_collapse_mapping_aux(&["/facet/a", "/facet/b"], &["/facet"], &[(0, 1), (1, 1)]);
        test_collapse_mapping_aux(
            &["/facet/a", "/facet/a2", "/facet/b"],
            &["/facet"],
            &[(0, 1), (1, 1), (2, 1)],
        );
        test_collapse_mapping_aux(&["/facet/a", "/facet/a/2"], &["/facet"], &[(0, 1), (0, 1)]);
        test_collapse_mapping_aux(
            &["/facet/a", "/facet/a/2", "/facet/b"],
            &["/facet"],
            &[(0, 1), (0, 1), (2, 1)],
        );
    }

    fn test_compress_mapping_aux(
        collapsed_mapping: &[(u64, usize)],
        expected_compressed_collapsed_mapping: &[usize],
        expected_unique_facet_ords: &[(u64, usize)],
    ) {
        let (compressed_collapsed_mapping, unique_facet_ords) = compress_mapping(collapsed_mapping);
        assert_eq!(
            compressed_collapsed_mapping,
            expected_compressed_collapsed_mapping
        );
        assert_eq!(unique_facet_ords, expected_unique_facet_ords);
    }

    #[test]
    fn test_compress_mapping() {
        test_compress_mapping_aux(&[], &[], &[]);
        test_compress_mapping_aux(&[(1, 2)], &[0], &[(1, 2)]);
        test_compress_mapping_aux(&[(1, 2), (1, 2)], &[0, 0], &[(1, 2)]);
        test_compress_mapping_aux(
            &[(1, 2), (5, 2), (5, 2), (6, 3), (8, 3)],
            &[0, 1, 1, 2, 3],
            &[(1, 2), (5, 2), (6, 3), (8, 3)],
        );
    }

    #[test]
    fn test_facet_collector_simple() {
        let mut schema_builder = Schema::builder();
        let facet_field = schema_builder.add_facet_field("facet", FacetOptions::default());
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        index_writer
            .add_document(doc!(facet_field=>Facet::from("/facet/a")))
            .unwrap();
        index_writer
            .add_document(doc!(facet_field=>Facet::from("/facet/b")))
            .unwrap();
        index_writer
            .add_document(doc!(facet_field=>Facet::from("/facet/b")))
            .unwrap();
        index_writer
            .add_document(doc!(facet_field=>Facet::from("/facet/c")))
            .unwrap();
        index_writer.commit().unwrap();
        let searcher = index.reader().unwrap().searcher();
        let mut facet_collector = FacetCollector::for_field("facet");
        facet_collector.add_facet("/facet");
        let counts: FacetCounts = searcher.search(&AllQuery, &facet_collector).unwrap();
        let facets: Vec<(&Facet, u64)> = counts.top_k("/facet", 1);
        assert_eq!(facets, vec![(&Facet::from("/facet/b"), 2)]);
    }

    #[test]
    fn test_facet_collector_drilldown() {
        let mut schema_builder = Schema::builder();
        let facet_field = schema_builder.add_facet_field("facet", FacetOptions::default());
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        let num_facets: usize = 3 * 4 * 5;
        let facets: Vec<Facet> = (0..num_facets)
            .map(|mut n| {
                let top = n % 3;
                n /= 3;
                let mid = n % 4;
                n /= 4;
                let leaf = n % 5;
                Facet::from(&format!("/top{top}/mid{mid}/leaf{leaf}"))
            })
            .collect();
        for i in 0..num_facets * 10 {
            let mut doc = TantivyDocument::new();
            doc.add_facet(facet_field, facets[i % num_facets].clone());
            index_writer.add_document(doc).unwrap();
        }
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let mut facet_collector = FacetCollector::for_field("facet");
        facet_collector.add_facet(Facet::from("/top1"));
        let counts = searcher.search(&AllQuery, &facet_collector).unwrap();

        {
            let facets: Vec<(String, u64)> = counts
                .get("/top1")
                .map(|(facet, count)| (facet.to_string(), count))
                .collect();
            assert_eq!(
                facets,
                [
                    ("/top1/mid0", 50),
                    ("/top1/mid1", 50),
                    ("/top1/mid2", 50),
                    ("/top1/mid3", 50),
                ]
                .iter()
                .map(|&(facet_str, count)| (String::from(facet_str), count))
                .collect::<Vec<_>>()
            );
        }
    }

    #[test]
    #[should_panic(
        expected = "Tried to add a facet which is a descendant of an already added facet."
    )]
    fn test_misused_facet_collector() {
        let mut facet_collector = FacetCollector::for_field("facet");
        facet_collector.add_facet(Facet::from("/country"));
        facet_collector.add_facet(Facet::from("/country/europe"));
    }

    #[test]
    fn test_doc_unsorted_multifacet() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let facet_field = schema_builder.add_facet_field("facets", FacetOptions::default());
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests()?;
        index_writer.add_document(doc!(
            facet_field => Facet::from_text(&"/subjects/A/a").unwrap(),
            facet_field => Facet::from_text(&"/subjects/B/a").unwrap(),
            facet_field => Facet::from_text(&"/subjects/A/b").unwrap(),
            facet_field => Facet::from_text(&"/subjects/B/b").unwrap(),
        ))?;
        index_writer.commit()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 1);
        let mut facet_collector = FacetCollector::for_field("facets");
        facet_collector.add_facet("/subjects");
        let counts = searcher.search(&AllQuery, &facet_collector)?;
        let facets: Vec<(&Facet, u64)> = counts.get("/subjects").collect();
        assert_eq!(facets[0].1, 1);
        Ok(())
    }

    #[test]
    fn test_doc_search_by_facet() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let facet_field = schema_builder.add_facet_field("facet", FacetOptions::default());
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests()?;
        index_writer.add_document(doc!(
            facet_field => Facet::from_text(&"/A/A").unwrap(),
        ))?;
        index_writer.add_document(doc!(
            facet_field => Facet::from_text(&"/A/B").unwrap(),
        ))?;
        index_writer.add_document(doc!(
            facet_field => Facet::from_text(&"/A/C/A").unwrap(),
        ))?;
        index_writer.add_document(doc!(
            facet_field => Facet::from_text(&"/D/C/A").unwrap(),
        ))?;
        index_writer.commit()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 4);

        let count_facet = |facet_str: &str| {
            let term = Term::from_facet(facet_field, &Facet::from_text(facet_str).unwrap());
            searcher
                .search(&TermQuery::new(term, IndexRecordOption::Basic), &Count)
                .unwrap()
        };

        assert_eq!(count_facet("/"), 4);
        assert_eq!(count_facet("/A"), 3);
        assert_eq!(count_facet("/A/B"), 1);
        assert_eq!(count_facet("/A/C"), 1);
        assert_eq!(count_facet("/A/C/A"), 1);
        assert_eq!(count_facet("/C/A"), 0);

        let query_parser = QueryParser::for_index(&index, vec![]);
        {
            let query = query_parser.parse_query("facet:/A/B")?;
            assert_eq!(1, searcher.search(&query, &Count).unwrap());
        }
        {
            let query = query_parser.parse_query("facet:/A")?;
            assert_eq!(3, searcher.search(&query, &Count)?);
        }
        Ok(())
    }

    #[test]
    fn test_non_used_facet_collector() {
        let mut facet_collector = FacetCollector::for_field("facet");
        facet_collector.add_facet(Facet::from("/country"));
        facet_collector.add_facet(Facet::from("/countryeurope"));
    }

    #[test]
    fn test_facet_collector_topk() {
        let mut schema_builder = Schema::builder();
        let facet_field = schema_builder.add_facet_field("facet", FacetOptions::default());
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        let uniform = Uniform::new_inclusive(1, 100_000).unwrap();
        let mut docs: Vec<TantivyDocument> =
            vec![("a", 10), ("b", 100), ("c", 7), ("d", 12), ("e", 21)]
                .into_iter()
                .flat_map(|(c, count)| {
                    let facet = Facet::from(&format!("/facet/{c}"));
                    let doc = doc!(facet_field => facet);
                    std::iter::repeat_n(doc, count)
                })
                .map(|mut doc| {
                    doc.add_facet(facet_field, &format!("/facet/{}", rng().sample(uniform)));
                    doc
                })
                .collect();
        docs[..].shuffle(&mut rng());

        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        for doc in docs {
            index_writer.add_document(doc).unwrap();
        }
        index_writer.commit().unwrap();
        let searcher = index.reader().unwrap().searcher();

        let mut facet_collector = FacetCollector::for_field("facet");
        facet_collector.add_facet("/facet");
        let counts: FacetCounts = searcher.search(&AllQuery, &facet_collector).unwrap();

        {
            let facets: Vec<(&Facet, u64)> = counts.top_k("/facet", 3);
            assert_eq!(
                facets,
                vec![
                    (&Facet::from("/facet/b"), 100),
                    (&Facet::from("/facet/e"), 21),
                    (&Facet::from("/facet/d"), 12),
                ]
            );
        }
    }

    #[test]
    fn test_facet_collector_topk_tie_break() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let facet_field = schema_builder.add_facet_field("facet", FacetOptions::default());
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        let docs: Vec<TantivyDocument> = vec![("b", 2), ("a", 2), ("c", 4)]
            .into_iter()
            .flat_map(|(c, count)| {
                let facet = Facet::from(&format!("/facet/{c}"));
                let doc = doc!(facet_field => facet);
                std::iter::repeat_n(doc, count)
            })
            .collect();

        let mut index_writer = index.writer_for_tests()?;
        for doc in docs {
            index_writer.add_document(doc)?;
        }
        index_writer.commit()?;

        let searcher = index.reader()?.searcher();
        let mut facet_collector = FacetCollector::for_field("facet");
        facet_collector.add_facet("/facet");
        let counts: FacetCounts = searcher.search(&AllQuery, &facet_collector)?;

        let facets: Vec<(&Facet, u64)> = counts.top_k("/facet", 2);
        assert_eq!(
            facets,
            vec![(&Facet::from("/facet/c"), 4), (&Facet::from("/facet/a"), 2)]
        );
        Ok(())
    }

    #[test]
    fn is_child_facet() {
        assert!(super::is_child_facet(&b"foo"[..], &b"foo\0bar"[..]));
        assert!(super::is_child_facet(&b""[..], &b"foo\0bar"[..]));
        assert!(super::is_child_facet(&b""[..], &b"foo"[..]));
        assert!(!super::is_child_facet(&b"foo\0bar"[..], &b"foo"[..]));
        assert!(!super::is_child_facet(&b"foo"[..], &b"foobar\0baz"[..]));
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {

    use rand::rng;
    use rand::seq::SliceRandom;
    use test::Bencher;

    use crate::collector::FacetCollector;
    use crate::query::AllQuery;
    use crate::schema::{Facet, Schema, INDEXED};
    use crate::{Index, IndexWriter};

    #[bench]
    fn bench_facet_collector(b: &mut Bencher) {
        let mut schema_builder = Schema::builder();
        let facet_field = schema_builder.add_facet_field("facet", INDEXED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        let mut docs = vec![];
        for val in 0..50 {
            let facet = Facet::from(&format!("/facet_{val}"));
            for _ in 0..val * val {
                docs.push(doc!(facet_field=>facet.clone()));
            }
        }
        // 40425 docs
        docs[..].shuffle(&mut rng());

        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        for doc in docs {
            index_writer.add_document(doc).unwrap();
        }
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();
        b.iter(|| {
            let searcher = reader.searcher();
            let facet_collector = FacetCollector::for_field("facet");
            searcher.search(&AllQuery, &facet_collector).unwrap();
        });
    }
}
