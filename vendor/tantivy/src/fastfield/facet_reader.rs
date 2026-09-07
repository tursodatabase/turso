use columnar::StrColumn;

use crate::schema::Facet;
use crate::termdict::TermOrdinal;
use crate::DocId;

/// The facet reader makes it possible to access the list of
/// facets associated with a given document in a specific
/// segment.
///
/// Rather than manipulating `Facet` object directly, the API
/// exposes those in the form of list of `Facet` ordinal.
///
/// A segment ordinal can then be translated into a facet via
/// `.facet_from_ord(...)`.
///
/// Facet ordinals are defined as their position in the sorted
/// list of facets. This ordinal is segment local and
/// only makes sense for a given segment.
pub struct FacetReader {
    facet_column: StrColumn,
}

impl FacetReader {
    /// Creates a new `FacetReader`.
    ///
    /// A facet reader just wraps :
    /// - a `MultiValuedFastFieldReader` that makes it possible to access the list of facet ords for
    ///   a given document.
    /// - a `TermDictionary` that helps associating a facet to an ordinal and vice versa.
    pub fn new(facet_column: StrColumn) -> FacetReader {
        FacetReader { facet_column }
    }

    /// Returns the size of the sets of facets in the segment.
    /// This does not take in account the documents that may be marked
    /// as deleted.
    ///
    /// `Facet` ordinals range from `0` to `num_facets() - 1`.
    pub fn num_facets(&self) -> usize {
        self.facet_column.num_terms()
    }

    /// Given a term ordinal returns the term associated with it.
    pub fn facet_from_ord(&self, facet_ord: TermOrdinal, output: &mut Facet) -> crate::Result<()> {
        let found_term = self.facet_column.ord_to_str(facet_ord, &mut output.0)?;
        assert!(found_term, "Term ordinal {facet_ord} no found.");
        Ok(())
    }

    /// Return the list of facet ordinals associated with a document.
    pub fn facet_ords(&self, doc: DocId) -> impl Iterator<Item = u64> + '_ {
        self.facet_column.ords().values_for_doc(doc)
    }

    /// Accessor to the facet dictionary.
    pub fn facet_dict(&self) -> &columnar::Dictionary {
        self.facet_column.dictionary()
    }
}

#[cfg(test)]
mod tests {
    use crate::schema::{Facet, FacetOptions, SchemaBuilder, Value, STORED};
    use crate::{DocAddress, Index, IndexWriter, TantivyDocument};

    #[test]
    fn test_facet_only_indexed() {
        let mut schema_builder = SchemaBuilder::default();
        let facet_field = schema_builder.add_facet_field("facet", FacetOptions::default());
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        index_writer
            .add_document(doc!(facet_field=>Facet::from_text("/a/b").unwrap()))
            .unwrap();
        index_writer.commit().unwrap();
        let searcher = index.reader().unwrap().searcher();
        let facet_reader = searcher.segment_reader(0u32).facet_reader("facet").unwrap();
        let mut facet_ords = Vec::new();
        facet_ords.extend(facet_reader.facet_ords(0u32));
        assert_eq!(&facet_ords, &[0u64]);
        assert_eq!(facet_reader.num_facets(), 1);
        let mut facet = Facet::default();
        facet_reader.facet_from_ord(0, &mut facet).unwrap();
        assert_eq!(facet.to_path_string(), "/a/b");
        let doc = searcher
            .doc::<TantivyDocument>(DocAddress::new(0u32, 0u32))
            .unwrap();
        let value = doc
            .get_first(facet_field)
            .and_then(|v| v.as_value().as_facet());
        assert_eq!(value, None);
    }

    #[test]
    fn test_facet_several_facets_sorted() {
        let mut schema_builder = SchemaBuilder::default();
        let facet_field = schema_builder.add_facet_field("facet", FacetOptions::default());
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        index_writer
            .add_document(doc!(facet_field=>Facet::from_text("/parent/child1").unwrap()))
            .unwrap();
        index_writer
            .add_document(doc!(
                facet_field=>Facet::from_text("/parent/child2").unwrap(),
                facet_field=>Facet::from_text("/parent/child1/blop").unwrap(),
            ))
            .unwrap();
        index_writer.commit().unwrap();
        let searcher = index.reader().unwrap().searcher();
        let facet_reader = searcher.segment_reader(0u32).facet_reader("facet").unwrap();
        let mut facet_ords = Vec::new();

        facet_ords.extend(facet_reader.facet_ords(0u32));
        assert_eq!(&facet_ords, &[0u64]);

        facet_ords.clear();
        facet_ords.extend(facet_reader.facet_ords(1u32));
        assert_eq!(&facet_ords, &[1u64, 2u64]);

        assert_eq!(facet_reader.num_facets(), 3);
        let mut facet = Facet::default();
        facet_reader.facet_from_ord(0, &mut facet).unwrap();
        assert_eq!(facet.to_path_string(), "/parent/child1");
        facet_reader.facet_from_ord(1, &mut facet).unwrap();
        assert_eq!(facet.to_path_string(), "/parent/child1/blop");
        facet_reader.facet_from_ord(2, &mut facet).unwrap();
        assert_eq!(facet.to_path_string(), "/parent/child2");
    }

    #[test]
    fn test_facet_stored_and_indexed() -> crate::Result<()> {
        let mut schema_builder = SchemaBuilder::default();
        let facet_field = schema_builder.add_facet_field("facet", STORED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests()?;
        index_writer.add_document(doc!(facet_field=>Facet::from_text("/a/b").unwrap()))?;
        index_writer.commit()?;
        let searcher = index.reader()?.searcher();
        let facet_reader = searcher.segment_reader(0u32).facet_reader("facet").unwrap();
        let mut facet_ords = Vec::new();
        facet_ords.extend(facet_reader.facet_ords(0u32));
        assert_eq!(&facet_ords, &[0u64]);
        let doc = searcher.doc::<TantivyDocument>(DocAddress::new(0u32, 0u32))?;
        let value: Option<Facet> = doc
            .get_first(facet_field)
            .and_then(|v| v.as_facet())
            .map(|facet| Facet::from_encoded_string(facet.to_string()));
        assert_eq!(value, Facet::from_text("/a/b").ok());
        Ok(())
    }

    #[test]
    fn test_facet_not_populated_for_all_docs() -> crate::Result<()> {
        let mut schema_builder = SchemaBuilder::default();
        let facet_field = schema_builder.add_facet_field("facet", FacetOptions::default());
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests()?;
        index_writer.add_document(doc!(facet_field=>Facet::from_text("/a/b").unwrap()))?;
        index_writer.add_document(TantivyDocument::default())?;
        index_writer.commit()?;
        let searcher = index.reader()?.searcher();
        let facet_reader = searcher.segment_reader(0u32).facet_reader("facet").unwrap();
        let mut facet_ords = Vec::new();
        facet_ords.extend(facet_reader.facet_ords(0u32));
        assert_eq!(&facet_ords, &[0u64]);
        facet_ords.clear();
        facet_ords.extend(facet_reader.facet_ords(1u32));
        assert!(facet_ords.is_empty());
        Ok(())
    }

    #[test]
    fn test_facet_not_populated_for_any_docs() -> crate::Result<()> {
        let mut schema_builder = SchemaBuilder::default();
        schema_builder.add_facet_field("facet", FacetOptions::default());
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests()?;
        index_writer.add_document(TantivyDocument::default())?;
        index_writer.add_document(TantivyDocument::default())?;
        index_writer.commit()?;
        let searcher = index.reader()?.searcher();
        let facet_reader = searcher.segment_reader(0u32).facet_reader("facet").unwrap();
        assert!(facet_reader.facet_ords(0u32).next().is_none());
        assert!(facet_reader.facet_ords(1u32).next().is_none());
        Ok(())
    }
}
