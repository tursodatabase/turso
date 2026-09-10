// Benchmarks segment merging
//
// Notes:
// - Input segments are kept intact (no deletes / no IndexWriter merge).
// - Output is written to a `NullDirectory` that discards all files except
//  fieldnorms (needed for merging).

use std::collections::HashMap;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use binggan::{black_box, BenchRunner};
use rand::prelude::*;
use rand::rngs::StdRng;
use rand::SeedableRng;
use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use tantivy::directory::{
    AntiCallToken, Directory, FileHandle, OwnedBytes, TerminatingWrite, WatchCallback, WatchHandle,
    WritePtr,
};
use tantivy::indexer::{merge_filtered_segments, NoMergePolicy};
use tantivy::schema::{Schema, TEXT};
use tantivy::{doc, HasLen, Index, IndexSettings, Segment};

#[derive(Clone, Default, Debug)]
struct NullDirectory {
    blobs: Arc<RwLock<HashMap<PathBuf, OwnedBytes>>>,
}

struct NullWriter;

impl Write for NullWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl TerminatingWrite for NullWriter {
    fn terminate_ref(&mut self, _token: AntiCallToken) -> io::Result<()> {
        Ok(())
    }
}

struct InMemoryWriter {
    path: PathBuf,
    buffer: Vec<u8>,
    blobs: Arc<RwLock<HashMap<PathBuf, OwnedBytes>>>,
}

impl Write for InMemoryWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl TerminatingWrite for InMemoryWriter {
    fn terminate_ref(&mut self, _token: AntiCallToken) -> io::Result<()> {
        let bytes = OwnedBytes::new(std::mem::take(&mut self.buffer));
        self.blobs.write().unwrap().insert(self.path.clone(), bytes);
        Ok(())
    }
}

#[derive(Debug, Default)]
struct NullFileHandle;
impl HasLen for NullFileHandle {
    fn len(&self) -> usize {
        0
    }
}
impl FileHandle for NullFileHandle {
    fn read_bytes(&self, _range: std::ops::Range<usize>) -> io::Result<OwnedBytes> {
        unimplemented!()
    }
}

impl Directory for NullDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        if let Some(bytes) = self.blobs.read().unwrap().get(path) {
            return Ok(Arc::new(bytes.clone()));
        }
        Ok(Arc::new(NullFileHandle))
    }

    fn delete(&self, _path: &Path) -> Result<(), DeleteError> {
        Ok(())
    }

    fn exists(&self, _path: &Path) -> Result<bool, OpenReadError> {
        Ok(true)
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        let path_buf = path.to_path_buf();
        if path.to_string_lossy().ends_with(".fieldnorm") {
            let writer = InMemoryWriter {
                path: path_buf,
                buffer: Vec::new(),
                blobs: Arc::clone(&self.blobs),
            };
            Ok(io::BufWriter::new(Box::new(writer)))
        } else {
            Ok(io::BufWriter::new(Box::new(NullWriter)))
        }
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        if let Some(bytes) = self.blobs.read().unwrap().get(path) {
            return Ok(bytes.as_slice().to_vec());
        }
        Err(OpenReadError::FileDoesNotExist(path.to_path_buf()))
    }

    fn atomic_write(&self, _path: &Path, _data: &[u8]) -> io::Result<()> {
        Ok(())
    }

    fn sync_directory(&self) -> io::Result<()> {
        Ok(())
    }

    fn watch(&self, _watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        Ok(WatchHandle::empty())
    }
}

struct MergeScenario {
    #[allow(dead_code)]
    index: Index,
    segments: Vec<Segment>,
    settings: IndexSettings,
    label: String,
}

fn build_index(
    num_segments: usize,
    docs_per_segment: usize,
    tokens_per_doc: usize,
    vocab_size: usize,
) -> MergeScenario {
    let mut schema_builder = Schema::builder();
    let body = schema_builder.add_text_field("body", TEXT);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());

    assert!(vocab_size > 0);
    let total_tokens = num_segments * docs_per_segment * tokens_per_doc;
    let use_unique_terms = vocab_size >= total_tokens;
    let mut rng = StdRng::from_seed([7u8; 32]);
    let mut next_token_id: u64 = 0;

    {
        let mut writer = index.writer_with_num_threads(1, 256_000_000).unwrap();
        writer.set_merge_policy(Box::new(NoMergePolicy));
        for _ in 0..num_segments {
            for _ in 0..docs_per_segment {
                let mut tokens = Vec::with_capacity(tokens_per_doc);
                for _ in 0..tokens_per_doc {
                    let token_id = if use_unique_terms {
                        let id = next_token_id;
                        next_token_id += 1;
                        id
                    } else {
                        rng.random_range(0..vocab_size as u64)
                    };
                    tokens.push(format!("term_{token_id}"));
                }
                writer.add_document(doc!(body => tokens.join(" "))).unwrap();
            }
            writer.commit().unwrap();
        }
    }

    let segments = index.searchable_segments().unwrap();
    let settings = index.settings().clone();
    let label = format!(
        "segments={}, docs/seg={}, tokens/doc={}, vocab={}",
        num_segments, docs_per_segment, tokens_per_doc, vocab_size
    );

    MergeScenario {
        index,
        segments,
        settings,
        label,
    }
}

fn main() {
    let scenarios = vec![
        build_index(8, 50_000, 12, 8),
        build_index(16, 50_000, 12, 8),
        build_index(16, 100_000, 12, 8),
        build_index(8, 50_000, 8, 8 * 50_000 * 8),
    ];

    let mut runner = BenchRunner::new();
    for scenario in scenarios {
        let mut group = runner.new_group();
        group.set_name(format!("merge_segments inv_index — {}", scenario.label));
        let segments = scenario.segments.clone();
        let settings = scenario.settings.clone();
        group.register("merge", move |_| {
            let output_dir = NullDirectory::default();
            let filter_doc_ids = vec![None; segments.len()];
            let merged_index =
                merge_filtered_segments(&segments, settings.clone(), filter_doc_ids, output_dir)
                    .unwrap();
            black_box(merged_index);
        });

        group.run();
    }
}
