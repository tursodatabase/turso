use binggan::plugins::PeakMemAllocPlugin;
use binggan::{black_box, InputGroup, PeakMemAlloc, INSTRUMENTED_SYSTEM};
use serde_json::json;
use tantivy::collector::Count;
use tantivy::query::ExistsQuery;
use tantivy::schema::{Schema, FAST, TEXT};
use tantivy::{doc, Index};

#[global_allocator]
pub static GLOBAL: &PeakMemAlloc<std::alloc::System> = &INSTRUMENTED_SYSTEM;

fn main() {
    let doc_count: usize = 500_000;
    let subfield_counts: &[usize] = &[1, 2, 3, 4, 5, 6, 7, 8, 16, 256, 4096, 65536, 262144];

    let indices: Vec<(String, Index)> = subfield_counts
        .iter()
        .map(|&sub_fields| {
            (
                format!("subfields={sub_fields}"),
                build_index_with_json_subfields(doc_count, sub_fields),
            )
        })
        .collect();

    let mut group = InputGroup::new_with_inputs(indices);
    group.add_plugin(PeakMemAllocPlugin::new(GLOBAL));

    group.config().num_iter_group = Some(1);
    group.config().num_iter_bench = Some(1);
    group.register("exists_json", exists_json_union);

    group.run();
}

fn exists_json_union(index: &Index) {
    let reader = index.reader().expect("reader");
    let searcher = reader.searcher();
    let query = ExistsQuery::new("json".to_string(), true);
    let count = searcher.search(&query, &Count).expect("exists search");
    // Prevents optimizer from eliding the search
    black_box(count);
}

fn build_index_with_json_subfields(num_docs: usize, num_subfields: usize) -> Index {
    // Schema: single JSON field stored as FAST to support ExistsQuery.
    let mut schema_builder = Schema::builder();
    let json_field = schema_builder.add_json_field("json", TEXT | FAST);
    let schema = schema_builder.build();

    let index = Index::create_from_tempdir(schema).expect("create index");
    {
        let mut index_writer = index
            .writer_with_num_threads(1, 200_000_000)
            .expect("writer");
        for i in 0..num_docs {
            let sub = i % num_subfields;
            // Only one subpath set per document; rotate subpaths so that
            // no single subpath is full, but the union covers all docs.
            let v = json!({ format!("field_{sub}"): i as u64 });
            index_writer
                .add_document(doc!(json_field => v))
                .expect("add_document");
        }
        index_writer.commit().expect("commit");
    }

    index
}
