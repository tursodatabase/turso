// # Indexing from different threads.
//
// It is fairly common to have to index from different threads.
// Tantivy forbids to create more than one `IndexWriter` at a time.
//
// This `IndexWriter` itself has its own multithreaded layer, so managing your own
// indexing threads will not help. However, it can still be useful for some applications.
//
// For instance, if preparing documents to send to tantivy before indexing is the bottleneck of
// your application, it is reasonable to have multiple threads.
//
// Another very common reason to want to index from multiple threads, is implementing a webserver
// with CRUD capabilities. The server framework will most likely handle request from
// different threads.
//
// The recommended way to address both of these use case is to wrap your `IndexWriter` into a
// `Arc<RwLock<IndexWriter>>`.
//
// While this is counterintuitive, adding and deleting documents do not require mutability
// over the `IndexWriter`, so several threads will be able to do this operation concurrently.
//
// The example below does not represent an actual real-life use case (who would spawn thread to
// index a single document?), but aims at demonstrating the mechanism that makes indexing
// from several threads possible.

// ---
// Importing tantivy...
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

use tantivy::schema::{Schema, STORED, TEXT};
use tantivy::{doc, Index, IndexWriter, Opstamp, TantivyError};

fn main() -> tantivy::Result<()> {
    // # Defining the schema
    let mut schema_builder = Schema::builder();
    let title = schema_builder.add_text_field("title", TEXT | STORED);
    let body = schema_builder.add_text_field("body", TEXT);
    let schema = schema_builder.build();

    let index = Index::create_in_ram(schema);
    let index_writer: Arc<RwLock<IndexWriter>> = Arc::new(RwLock::new(index.writer(50_000_000)?));

    // # First indexing thread.
    let index_writer_clone_1 = index_writer.clone();
    thread::spawn(move || {
        // we index 100 times the document... for the sake of the example.
        for i in 0..100 {
            let opstamp = index_writer_clone_1
                .read().unwrap() //< A read lock is sufficient here.
                .add_document(
                    doc!(
                        title => "Of Mice and Men",
                        body => "A few miles south of Soledad, the Salinas River drops in close to the hillside \
                        bank and runs deep and green. The water is warm too, for it has slipped twinkling \
                        over the yellow sands in the sunlight before reaching the narrow pool. On one \
                        side of the river the golden foothill slopes curve up to the strong and rocky \
                        Gabilan Mountains, but on the valley side the water is lined with trees—willows \
                        fresh and green with every spring, carrying in their lower leaf junctures the \
                        debris of the winter’s flooding; and sycamores with mottled, white, recumbent \
                        limbs and branches that arch over the pool"
                    ))?;
            println!("add doc {i} from thread 1 - opstamp {opstamp}");
            thread::sleep(Duration::from_millis(20));
        }
        Result::<(), TantivyError>::Ok(())
    });

    // # Second indexing thread.
    let index_writer_clone_2 = index_writer.clone();
    // For convenience, tantivy also comes with a macro to
    // reduce the boilerplate above.
    thread::spawn(move || {
        // we index 100 times the document... for the sake of the example.
        for i in 0..100 {
            // A read lock is sufficient here.
            let opstamp = {
                let index_writer_rlock = index_writer_clone_2.read().unwrap();
                index_writer_rlock.add_document(doc!(
                    title => "Manufacturing consent",
                    body => "Some great book description..."
                ))?
            };
            println!("add doc {i} from thread 2 - opstamp {opstamp}");
            thread::sleep(Duration::from_millis(10));
        }
        Result::<(), TantivyError>::Ok(())
    });

    // # In the main thread, we commit 10 times, once every 500ms.
    for _ in 0..10 {
        let opstamp: Opstamp = {
            // Committing or rollbacking on the other hand requires write lock. This will block
            // other threads.
            let mut index_writer_wlock = index_writer.write().unwrap();
            index_writer_wlock.commit()?
        };
        println!("committed with opstamp {opstamp}");
        thread::sleep(Duration::from_millis(500));
    }

    Ok(())
}
