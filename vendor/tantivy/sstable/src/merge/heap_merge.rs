use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::collections::binary_heap::PeekMut;
use std::io;

use super::{SingleValueMerger, ValueMerger};
use crate::{Reader, SSTable, Writer};

struct HeapItem<B: AsRef<[u8]>>(B);

impl<B: AsRef<[u8]>> Ord for HeapItem<B> {
    fn cmp(&self, other: &Self) -> Ordering {
        other.0.as_ref().cmp(self.0.as_ref())
    }
}
impl<B: AsRef<[u8]>> PartialOrd for HeapItem<B> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<B: AsRef<[u8]>> Eq for HeapItem<B> {}
impl<B: AsRef<[u8]>> PartialEq for HeapItem<B> {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_ref() == other.0.as_ref()
    }
}

pub fn merge_sstable<SST: SSTable, W: io::Write, M: ValueMerger<SST::Value>>(
    readers: Vec<Reader<SST::ValueReader>>,
    mut writer: Writer<W, SST::ValueWriter>,
    mut merger: M,
) -> io::Result<()> {
    let mut heap: BinaryHeap<HeapItem<Reader<SST::ValueReader>>> =
        BinaryHeap::with_capacity(readers.len());
    for mut reader in readers {
        if reader.advance()? {
            heap.push(HeapItem(reader));
        }
    }
    loop {
        let len = heap.len();
        let mut value_merger;
        match heap.peek_mut() {
            Some(mut head) => {
                writer.insert_key(head.0.key()).unwrap();
                value_merger = merger.new_value(head.0.value());
                if !head.0.advance()? {
                    PeekMut::pop(head);
                }
            }
            _ => {
                break;
            }
        }
        for _ in 0..len - 1 {
            if let Some(mut head) = heap.peek_mut()
                && head.0.key() == writer.last_inserted_key()
            {
                value_merger.add(head.0.value());
                if !head.0.advance()? {
                    PeekMut::pop(head);
                }
                continue;
            }
            break;
        }
        let value = value_merger.finish();
        writer.insert_value(&value)?;
        writer.flush_block_if_required()?;
    }
    writer.finish()?;
    Ok(())
}
