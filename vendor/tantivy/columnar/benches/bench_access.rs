use binggan::{InputGroup, black_box};
use common::*;
use tantivy_columnar::Column;

pub mod common;

const NUM_DOCS: u32 = 2_000_000;

pub fn generate_columnar_and_open(card: Card, num_docs: u32) -> Column {
    let reader = generate_columnar_with_name(card, num_docs, "price");
    reader.read_columns("price").unwrap()[0]
        .open_u64_lenient()
        .unwrap()
        .unwrap()
}

fn main() {
    let mut inputs = Vec::new();

    let mut add_card = |card1: Card| {
        inputs.push((
            card1.to_string(),
            generate_columnar_and_open(card1, NUM_DOCS),
        ));
    };

    add_card(Card::MultiSparse);
    add_card(Card::Multi);
    add_card(Card::Sparse);
    add_card(Card::Dense);
    add_card(Card::Full);

    bench_group(InputGroup::new_with_inputs(inputs));
}

fn bench_group(mut runner: InputGroup<Column>) {
    runner.register("access_values_for_doc", |column| {
        let mut sum = 0;
        for i in 0..NUM_DOCS {
            for value in column.values_for_doc(i) {
                sum += value;
            }
        }
        black_box(sum);
    });
    runner.register("access_first_vals", |column| {
        let mut sum = 0;
        const BLOCK_SIZE: usize = 32;
        let mut docs = vec![0; BLOCK_SIZE];
        let mut buffer = vec![None; BLOCK_SIZE];
        for i in (0..NUM_DOCS).step_by(BLOCK_SIZE) {
            // fill docs
            #[allow(clippy::needless_range_loop)]
            for idx in 0..BLOCK_SIZE {
                docs[idx] = idx as u32 + i;
            }

            column.first_vals(&docs, &mut buffer);
            for val in buffer.iter() {
                let Some(val) = val else { continue };
                sum += *val;
            }
        }

        black_box(sum);
    });
    runner.run();
}
