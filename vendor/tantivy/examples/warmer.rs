use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock, Weak};

use tantivy::collector::TopDocs;
use tantivy::index::SegmentId;
use tantivy::query::QueryParser;
use tantivy::schema::{Schema, FAST, TEXT};
use tantivy::{
    doc, DocAddress, DocId, Index, IndexWriter, Opstamp, Searcher, SearcherGeneration,
    SegmentReader, Warmer,
};

// This example shows how warmers can be used to
// load values from an external sources and
// tie their lifecycle to that of the index segments
// using the Warmer API.
//
// In this example, we assume an e-commerce search engine.

type ProductId = u64;

type Price = u32;

pub trait PriceFetcher: Send + Sync + 'static {
    fn fetch_prices(&self, product_ids: &[ProductId]) -> Vec<Price>;
}

type SegmentKey = (SegmentId, Option<Opstamp>);

struct DynamicPriceColumn {
    field: String,
    price_cache: RwLock<HashMap<SegmentKey, Arc<Vec<Price>>>>,
    price_fetcher: Box<dyn PriceFetcher>,
}

impl DynamicPriceColumn {
    pub fn with_product_id_field<T: PriceFetcher>(field: String, price_fetcher: T) -> Self {
        DynamicPriceColumn {
            field,
            price_cache: Default::default(),
            price_fetcher: Box::new(price_fetcher),
        }
    }

    pub fn price_for_segment(&self, segment_reader: &SegmentReader) -> Option<Arc<Vec<Price>>> {
        let segment_key = (segment_reader.segment_id(), segment_reader.delete_opstamp());
        self.price_cache.read().unwrap().get(&segment_key).cloned()
    }
}
impl Warmer for DynamicPriceColumn {
    fn warm(&self, searcher: &Searcher) -> tantivy::Result<()> {
        for segment in searcher.segment_readers() {
            let product_id_reader = segment
                .fast_fields()
                .u64(&self.field)?
                .first_or_default_col(0);
            let product_ids: Vec<ProductId> = segment
                .doc_ids_alive()
                .map(|doc| product_id_reader.get_val(doc))
                .collect();

            let mut prices = self.price_fetcher.fetch_prices(&product_ids).into_iter();

            let prices: Vec<Price> = (0..segment.max_doc())
                .map(|doc| {
                    if !segment.is_deleted(doc) {
                        prices.next().unwrap()
                    } else {
                        0
                    }
                })
                .collect();

            let key = (segment.segment_id(), segment.delete_opstamp());
            self.price_cache
                .write()
                .unwrap()
                .insert(key, Arc::new(prices));
        }

        Ok(())
    }

    fn garbage_collect(&self, live_generations: &[&SearcherGeneration]) {
        let live_keys: HashSet<SegmentKey> = live_generations
            .iter()
            .flat_map(|gen| gen.segments())
            .map(|(&segment_id, &opstamp)| (segment_id, opstamp))
            .collect();

        self.price_cache
            .write()
            .unwrap()
            .retain(|key, _| live_keys.contains(key));
    }
}

// For the sake of this example, the table is just an editable HashMap behind a RwLock.
// This map represents a map (ProductId -> Price)
//
// In practise, it could be fetching things from an external service, like a SQL table.
#[derive(Default, Clone)]
pub struct ExternalPriceTable {
    prices: Arc<RwLock<HashMap<ProductId, Price>>>,
}

impl ExternalPriceTable {
    pub fn update_price(&self, product_id: ProductId, price: Price) {
        self.prices.write().unwrap().insert(product_id, price);
    }
}

impl PriceFetcher for ExternalPriceTable {
    fn fetch_prices(&self, product_ids: &[ProductId]) -> Vec<Price> {
        let prices = self.prices.read().unwrap();

        product_ids
            .iter()
            .map(|product_id| prices.get(product_id).cloned().unwrap_or(0))
            .collect()
    }
}

fn main() -> tantivy::Result<()> {
    // Declaring our schema.
    let mut schema_builder = Schema::builder();
    // The product id is assumed to be a primary id for our external price source.
    let product_id = schema_builder.add_u64_field("product_id", FAST);
    let text = schema_builder.add_text_field("text", TEXT);
    let schema: Schema = schema_builder.build();

    let price_table = ExternalPriceTable::default();
    let price_dynamic_column = Arc::new(DynamicPriceColumn::with_product_id_field(
        "product_id".to_string(),
        price_table.clone(),
    ));
    price_table.update_price(OLIVE_OIL, 12);
    price_table.update_price(GLOVES, 13);
    price_table.update_price(SNEAKERS, 80);

    const OLIVE_OIL: ProductId = 323423;
    const GLOVES: ProductId = 3966623;
    const SNEAKERS: ProductId = 23222;

    let index = Index::create_in_ram(schema);
    let mut writer: IndexWriter = index.writer_with_num_threads(1, 15_000_000)?;
    writer.add_document(doc!(product_id=>OLIVE_OIL, text=>"cooking olive oil from greece"))?;
    writer.add_document(doc!(product_id=>GLOVES, text=>"kitchen gloves, perfect for cooking"))?;
    writer.add_document(doc!(product_id=>SNEAKERS, text=>"uber sweet sneakers"))?;
    writer.commit()?;

    let warmers = vec![Arc::downgrade(&price_dynamic_column) as Weak<dyn Warmer>];
    let reader = index.reader_builder().warmers(warmers).try_into()?;

    let query_parser = QueryParser::for_index(&index, vec![text]);
    let query = query_parser.parse_query("cooking")?;

    let searcher = reader.searcher();
    let score_by_price = move |segment_reader: &SegmentReader| {
        let price = price_dynamic_column
            .price_for_segment(segment_reader)
            .unwrap();
        move |doc_id: DocId| Reverse(price[doc_id as usize])
    };

    let most_expensive_first = TopDocs::with_limit(10).order_by(score_by_price);

    let hits = searcher.search(&query, &most_expensive_first)?;
    assert_eq!(
        &hits,
        &[
            (
                Reverse(12u32),
                DocAddress {
                    segment_ord: 0,
                    doc_id: 0u32
                }
            ),
            (
                Reverse(13u32),
                DocAddress {
                    segment_ord: 0,
                    doc_id: 1u32
                }
            ),
        ]
    );

    // Olive oil just got more expensive!
    price_table.update_price(OLIVE_OIL, 15);

    // The price update are directly reflected on `reload`.
    //
    // Be careful here though!...
    // You may have spotted that we are still using the same `Searcher`.
    //
    // It is up to the `Warmer` implementer to decide how
    // to control this behavior.

    reader.reload()?;

    let hits_with_new_prices = searcher.search(&query, &most_expensive_first)?;
    assert_eq!(
        &hits_with_new_prices,
        &[
            (
                Reverse(13u32),
                DocAddress {
                    segment_ord: 0,
                    doc_id: 1u32
                }
            ),
            (
                Reverse(15u32),
                DocAddress {
                    segment_ord: 0,
                    doc_id: 0u32
                }
            ),
        ]
    );

    Ok(())
}
