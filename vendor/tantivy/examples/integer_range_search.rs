use std::ops::Bound;

// # Searching a range on an indexed int field.
//
// Below is an example of creating an indexed integer field in your schema
// You can use RangeQuery to get a Count of all occurrences in a given range.
use tantivy::collector::Count;
use tantivy::query::RangeQuery;
use tantivy::schema::{Schema, INDEXED};
use tantivy::{doc, Index, IndexWriter, Result, Term};

fn main() -> Result<()> {
    // For the sake of simplicity, this schema will only have 1 field
    let mut schema_builder = Schema::builder();

    // `INDEXED` is a short-hand to indicate that our field should be "searchable".
    let year_field = schema_builder.add_u64_field("year", INDEXED);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema);
    let reader = index.reader()?;
    {
        let mut index_writer: IndexWriter = index.writer_with_num_threads(1, 6_000_000)?;
        for year in 1950u64..2019u64 {
            index_writer.add_document(doc!(year_field => year))?;
        }
        index_writer.commit()?;
        // The index will be a range of years
    }
    reader.reload()?;
    let searcher = reader.searcher();
    // The end is excluded i.e. here we are searching up to 1969
    let docs_in_the_sixties = RangeQuery::new(
        Bound::Included(Term::from_field_u64(year_field, 1960)),
        Bound::Excluded(Term::from_field_u64(year_field, 1970)),
    );
    // Uses a Count collector to sum the total number of docs in the range
    let num_60s_books = searcher.search(&docs_in_the_sixties, &Count)?;
    assert_eq!(num_60s_books, 10);
    Ok(())
}
