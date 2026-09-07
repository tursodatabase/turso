pub mod common;

use binggan::BenchRunner;
use common::{Card, generate_columnar_with_name};
use tantivy_columnar::*;

const NUM_DOCS: u32 = 100_000;

fn main() {
    let mut inputs = Vec::new();

    let mut add_combo = |card1: Card, card2: Card| {
        inputs.push((
            format!("merge_{card1}_and_{card2}"),
            vec![
                generate_columnar_with_name(card1, NUM_DOCS, "price"),
                generate_columnar_with_name(card2, NUM_DOCS, "price"),
            ],
        ));
    };

    add_combo(Card::Multi, Card::Multi);
    add_combo(Card::MultiSparse, Card::MultiSparse);
    add_combo(Card::Dense, Card::Dense);
    add_combo(Card::Sparse, Card::Sparse);
    add_combo(Card::Sparse, Card::Dense);
    add_combo(Card::MultiSparse, Card::Dense);
    add_combo(Card::MultiSparse, Card::Sparse);
    add_combo(Card::Multi, Card::Dense);
    add_combo(Card::Multi, Card::Sparse);

    let mut runner: BenchRunner = BenchRunner::new();
    let mut group = runner.new_group();
    for (input_name, columnar_readers) in inputs.iter() {
        group.register_with_input(
            input_name,
            columnar_readers,
            move |columnar_readers: &Vec<ColumnarReader>| {
                let mut out = Vec::new();
                let columnar_readers = columnar_readers.iter().collect::<Vec<_>>();
                let merge_row_order = StackMergeOrder::stack(&columnar_readers[..]);

                merge_columnar(&columnar_readers, &[], merge_row_order.into(), &mut out).unwrap();
                Some(out.len() as u64)
            },
        );
    }
    group.run();
}
