# Full-Text Search in tursodb

This proposal is to use the https://github.com/quickwit-oss/tantivy library to provide full-text search capabilities in tursodb. 

# Tantivy overview

*introduction and terminology*:

**Term:** a normalized token extracted from text during indexing.

**Posting list:** (also referred to as an *inverted list*) is the list of all documents that contain a given term, along with metadata needed for scoring the results.

**Document:** Tantivy’s unit of indexing and retrieval, analogous to a single row in a db table.

**Field:** Analogous to a column in a table

**Position:** The sequential offset/token index of a term within a field.

**Segment:**  self-contained and immutable chunk of the index. Each commit writes one or more new segments to disk, searches query all active segments in parallel.

Inside each segment:

- A term dictionary lists every term in that segment.
- For each term, the posting list maps to documents.
- Stored fields (original text) are compressed for retrieval.
- Deletes are stored as a separate bitmap until the next merge. Merges rewrite new segments without the deleted docs.

More details on everything here: https://fulmicoton.gitbooks.io/tantivy-doc/content/step-by-step.html

### Note: this blog post here is a bit old but is a very interesting read. https://fulmicoton.com/posts/behold-tantivy-part2/

At a high level, a Tantivy index is a self-contained directory containing immutable “segments” of inverted index data file. 

*“Your index is in fact divided into smaller independent indexes called **segments**. The reason for this division is explained in [Incremental Indexing](https://fulmicoton.gitbooks.io/tantivy-doc/content/incremental-indexing.html). The UUID part stands as the segment name, while the extension express which data-structure is stored in the file.*

- ***.info** contains some meta information about the segment*
- ***.term** contains the term dictionary*
- ***.idx** contains the inverted lists*
- ***.fieldnorm** contains the field norms*
- ***.pos** contains the positions information*
- ***.store** contains the stored documents*
- ***.fas**t contains the fast terms*

*The last file, meta.json, contains in a JSON format :*

- *The schema of your index*
- *The name of the segments that were committed and are available for search.”*

source: https://fulmicoton.gitbooks.io/tantivy-doc/content/index-files.html

### Writing / indexing

- Applications obtain a single `IndexWriter`, which owns the memory arena and background worker threads.
- Each call to `add_document()` tokenizes and indexes the text fields into in-memory postings.
- When the writer’s memory budget is full or `commit()` is called, Tantivy serializes a new segment (posting lists, term dictionary, stored field data) and updates the `meta.json` (all these segments are Write-once, Read-many, with the exception of the per-index meta.json).
- `commit()` is atomic: either the new segment is visible or not at all.
- `delete_term()` records tombstones that are applied at merge time (all updates done are just “delete + insert”).

### Storage abstraction

Tantivy’s I/O is implemented through the `Directory` trait (https://github.com/quickwit-oss/tantivy/blob/dabcaa58093a3f7f10e98a5a3b06cfe2370482f9/src/directory/directory.rs#L107), which is kinda similar to our own `IO` / `File` traits.

Tantivy includes `MmapDirectory` (mmap’d file based segments, the default option), and `RamDirectory` (in-memory, usually for testing). Obviously the default will not work for our use case, as we cannot have a whole directory created for each index with a bunch of files.

### Threading and concurrency

- One write per index at a time, but internally multi-threaded for indexing throughput.
- Multiple readers can coexist and search concurrently on immutable segments.
- Writer commits are visible to new readers only after `reader.reload()` or automatic reload.

### Typical workflow

1. Build a `Schema` describing indexed fields.
2. Create or open an `Index` using a `Directory`.
3. Acquire an `IndexWriter`, call `add_document()` or `delete_term()`.
4. Call `commit()` to persist and publish new data.
5. Use `IndexReader`/`Searcher` + `QueryParser` + `TopDocs` to execute searches.
6. Fetch stored fields for displayed results.

### ===========================================================

## Usage:

From the user perspective, the syntax for using full text search features in tursodb will look like the following:

## DDL:

```sql
CREATE INDEX idx_posts
ON posts USING fts (title, body);
```

### Tokenizer Configuration

You can configure the tokenizer used for text processing via the `WITH` clause:

```sql
-- Use raw tokenizer for exact-match fields (IDs, tags)
CREATE INDEX idx_tags ON articles USING fts (tag) WITH (tokenizer = 'raw');

-- Use ngram tokenizer for autocomplete/substring matching
CREATE INDEX idx_products ON products USING fts (name) WITH (tokenizer = 'ngram');
```

#### Available Tokenizers

| Tokenizer | Description | Use Case |
|-----------|-------------|----------|
| `default` | Lowercase, punctuation split, 40 char limit | General English text |
| `raw` | No tokenization - exact match only | IDs, UUIDs, tags, categories |
| `simple` | Basic whitespace/punctuation split | Simple text without lowercase |
| `whitespace` | Split on whitespace only | Space-separated tokens |
| `ngram` | 2-3 character n-grams | Autocomplete, substring matching |

### Field Weights

You can configure relative importance of indexed columns using the `weights` parameter:

```sql
-- Title matches are 2x more important than body matches
CREATE INDEX idx_articles ON articles USING fts (title, body)
WITH (weights = 'title=2.0,body=1.0');

-- Combined with tokenizer
CREATE INDEX idx_docs ON docs USING fts (name, description)
WITH (tokenizer = 'simple', weights = 'name=3.0,description=1.0');
```

**Weight behavior:**
- Default weight is `1.0` for all fields
- Weights affect the BM25 relevance score from `fts_score()`
- Higher weights increase the score contribution from that field
- Weights must be positive numbers

#### Tokenizer Examples

```sql
-- Default tokenizer: "Hello World" → ["hello", "world"]
-- Searches for "hello" or "HELLO" will match

-- Raw tokenizer: "user-123" → ["user-123"]
-- Only exact match "user-123" will work, "user" won't match

-- Ngram tokenizer: "iPhone" → ["iP", "iPh", "Ph", "Pho", "ho", "hon", "on", "one", "ne"]
-- Search for "Pho" will match documents containing "iPhone"
```

## DQL:

Three FTS functions are provided:

- **`fts_score(col1, col2, ..., 'query')`** - Returns the BM25 relevance score for each matching row
- **`fts_match(col1, col2, ..., 'query')`** - Returns a boolean indicating if the row matches (used in WHERE clause)
- **`fts_highlight(col1, col2, ..., before_tag, after_tag, 'query')`** - Returns text with matching terms wrapped in tags

### Basic Query Examples

```sql
-- Get scores for matching documents, ordered by relevance
SELECT fts_score(title, body, 'database') as score, id, title
FROM articles
ORDER BY score DESC
LIMIT 10;

-- Simple match filter
SELECT id, title FROM articles WHERE fts_match(body, 'science') LIMIT 10;
```

### Highlighting Search Results

The `fts_highlight` function wraps matching query terms with custom tags for display:

```sql
-- Basic highlighting (single column)
SELECT fts_highlight('Learn about database optimization', '<b>', '</b>', 'database');
-- Returns: "Learn about <b>database</b> optimization"

-- Multiple columns - text is concatenated with spaces
SELECT fts_highlight(title, body, '<mark>', '</mark>', 'database') as highlighted
FROM articles
WHERE fts_match(title, body, 'database');
-- If title='Database Design' and body='Learn about optimization',
-- Returns: "<mark>Database</mark> Design Learn about optimization"

-- Use with FTS queries to highlight matched content
SELECT
    id,
    title,
    fts_highlight(body, '<mark>', '</mark>', 'database') as highlighted_body
FROM articles
WHERE fts_match(title, body, 'database')
ORDER BY fts_score(title, body, 'database') DESC;

-- Multiple terms are highlighted
SELECT fts_highlight('The quick brown fox', '<em>', '</em>', 'quick fox');
-- Returns: "The <em>quick</em> brown <em>fox</em>"
```

**Features:**
- Supports multiple text columns (concatenated with spaces)
- Case-insensitive matching (uses the default tokenizer)
- Highlights all occurrences of matching terms
- Works as a standalone function (doesn't require an FTS index)
- Returns original text if no matches found
- Returns NULL if query, before_tag, or after_tag is NULL
- NULL text columns are skipped (not returned as NULL)

### Supported Query Patterns

The FTS index recognizes and optimizes these query patterns:

| Pattern | Description |
|---------|-------------|
| `SELECT fts_score(...) as score FROM t ORDER BY score DESC LIMIT ?` | Score with ORDER BY and LIMIT |
| `SELECT fts_score(...) as score FROM t WHERE fts_match(...) ORDER BY score DESC LIMIT ?` | Combined score+match with ORDER BY and LIMIT |
| `SELECT fts_score(...) as score FROM t WHERE fts_match(...) ORDER BY score DESC` | Combined without LIMIT |
| `SELECT fts_score(...) as score FROM t WHERE fts_match(...) LIMIT ?` | Combined with LIMIT only |
| `SELECT fts_score(...) as score FROM t WHERE fts_match(...)` | Combined without ORDER BY or LIMIT |
| `SELECT * FROM t WHERE fts_match(...) LIMIT ?` | Match filter with LIMIT |
| `SELECT * FROM t WHERE fts_match(...)` | Match filter only |

### Function Recognition Mode

Queries that don't exactly match the predefined patterns still work via **function recognition**. When `fts_match()` or `fts_score()` functions are detected in a query, the FTS index is used even with:
- Additional SELECT columns
- Extra WHERE conditions (e.g., `AND category = 'tech'`)
- ORDER BY non-score columns
- Computed expressions

```sql
-- Complex query with extra columns and WHERE conditions
SELECT id, author, title, category, views, fts_score(title, body, 'Rust') as score
FROM articles
WHERE fts_match(title, body, 'Rust')
  AND category = 'tech'
  AND views > 100
ORDER BY score DESC;

-- ORDER BY non-score column
SELECT id, title FROM docs WHERE fts_match(title, body, 'Rust') ORDER BY created_at DESC;
```

### Query Syntax

The query string passed to `fts_match`/`fts_score` supports Tantivy's QueryParser syntax:

| Syntax | Example | Description |
|--------|---------|-------------|
| Single term | `database` | Match documents containing "database" |
| Multiple terms (OR) | `database sql` | Match documents with "database" OR "sql" |
| AND operator | `database AND sql` | Match documents with both terms |
| NOT operator | `database NOT nosql` | Match "database" but exclude "nosql" |
| Phrase search | `"full text search"` | Match exact phrase |
| Prefix search | `data*` | Match terms starting with "data" |
| Column filter | `title:database` | Match "database" only in title field |
| Boosting | `title:database^2 body:database` | Boost title matches |

This syntax can be improved on in the future, and maybe eventually we can support some fancy elasticsearch/paradeDB syntax.

### DML Operations

DML statements (INSERT, UPDATE, DELETE) work automatically with FTS indexes:

- **INSERT**: Documents are indexed immediately but batched for efficiency. Tantivy commits after every 1000 documents (`BATCH_COMMIT_SIZE`).
- **UPDATE**: Implemented as DELETE + INSERT internally.
- **DELETE**: Uses term-based tombstones that are cleaned up at merge time.

```sql
-- These trigger automatic FTS index updates
INSERT INTO articles VALUES (1, 'Title', 'Body text');
UPDATE articles SET body = 'New body' WHERE id = 1;
DELETE FROM articles WHERE id = 1;
```

# Planning

For the above SELECT query, we would look up which FTS index handles `t.name` from the in-memory schema representation, construct a Tantivy query for this index with `QueryParser::parse_query("tursodb")`. Tantivy then returns a list of `(score, doc_address)` pairs, which we translate each result into `(rowid, rating)` because we stored the table’s PK or rowid as a field during the indexing. Then an index lookup/SeekRowID for each of those result rows from the FtsQuery internal function fetches the `t.*` columns and no extra sort is required. 

If there are additional filters on `t` (`WHERE t.created_at > ...`) we should probably prefer ‘fts-first’, then filter out results from that. Run the Tantivy query, materialize results into an ephemeral table of some sort and then discard rows failing the post-filter. TODO: expand on this a bit more

# Metadata

CREATE INDEX statements are already stored in the schema table, when we parse the sqlite_schema table which contains the DDL statement that created the index, we build the in-memory schema representation that allows us to send these queries to Tantivy.

## Storage

We implement Tantivy’s `Directory` over our pager/B-tree but just as a regular table. We do not reinterpret Tantivy’s files, we store them exactly as Tantivy names and writes them. We should probably store the files as chunks of 256-512 kb blobs.

One table, and one index: per each FTS index

- **Data table:**
    
    ```sql
    CREATE TABLE fts_dir_{idx_id} ( 
      path TEXT NOT NULL, 
      chunk_no INTEGER NOT NULL,
      bytes BLOB NOT NULL
    );
    ```
    
- **Unique index:**
    
    ```sql
    CREATE UNIQUE INDEX fts_dir_{idx_id}_key ON fts_dir_{idx_id} 
    (path, chunk_no ASC);
    ```
    

This way we can use an index cursor to `SeekGE` (path, chunk_no) where chunk_no is just computed from the offset requested by `read_bytes` on the file handle.

# Current Architecture

## Problem Statement

The original `CachedBTreeDirectory` implementation loaded ALL Tantivy files into memory at startup. This doesn't scale for medium to large indexes (100MB - 1GB+), causing significant memory pressure.

Additionally, Tantivy's synchronous `Directory` trait needed careful integration with our async BTree storage layer.

## New Architecture: HybridBTreeDirectory

The redesigned architecture uses a hybrid approach that balances memory usage and performance:

```
┌─────────────────────────────────────────────────────────────┐
│                    HybridBTreeDirectory                     │
├─────────────────────────────────────────────────────────────┤
│  File Catalog (always in memory)                            │
│  ├── path -> FileMetadata { size, num_chunks, category }    │
│                                                             │
│  Hot Cache (metadata + term dictionaries)                   │
│  ├── meta.json, .managed.json (always loaded)               │
│  ├── .term files (loaded on first access)                   │
│  ├── .fast, .fieldnorm (small, frequently accessed)         │
│                                                             │
│  Chunk LRU Cache (lazy-loaded segment data)                 │
│  ├── .idx, .pos, .store chunks                              │
│  └── Eviction when over memory budget                       │
└─────────────────────────────────────────────────────────────┘
```

### File Categories

Files are classified based on their role in Tantivy operations:

| Category | Files | Behavior |
|----------|-------|----------|
| Metadata | `meta.json`, `.managed.json`, `.lock` | Always in hot cache |
| TermDictionary | `*.term` | Hot, loaded on first access |
| FastFields | `*.fast`, `*.fieldnorm` | Hot, small and frequent |
| SegmentData | `*.idx`, `*.pos`, `*.store` | Lazy-loaded on demand |

### Loading Flow (Catalog-First)

The FTS cursor uses an async state machine to load the index:

1. **LoadingCatalog**: Scan BTree records, building file metadata (path, max_chunk, size)
2. **PreloadingEssentials**: Load hot files (metadata, term dicts) into memory
3. **CreatingIndex**: Open/create Tantivy index using HybridBTreeDirectory
4. **Ready**: Index is usable; segment data lazy-loaded on query

Additional states for write operations:
- **FlushingWrites**: Persisting pending writes to BTree
- **Querying**: Executing a Tantivy search

The state machine is driven by `FtsCursor` which handles the async IO integration with our pager.

### Lazy Loading with Blocking Reads

Since blocking reads are acceptable in the query path:

```rust
impl FileHandle for LazyFileHandle {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        // 1. Check hot cache
        // 2. Calculate required chunks from byte range
        // 3. For each chunk: check LRU cache, or blocking BTree fetch
        // 4. Assemble and return result
    }
}
```

The `get_chunk_blocking` method creates a temporary BTree cursor and loops on `pager.io.step()` until the chunk is fetched.

### Memory Management

```rust
pub const DEFAULT_CHUNK_CACHE_BYTES: usize = 128 * 1024 * 1024; // 128MB
pub const DEFAULT_HOT_CACHE_BYTES: usize = 64 * 1024 * 1024;    // 64MB
```

The `ChunkLruCache` evicts least-recently-used chunks when over capacity. Each FTS index has its own cache for isolation.

### Key Components

| Component | Purpose |
|-----------|---------|
| `FileCategory` | Classifies files for caching decisions |
| `FileMetadata` | Stores file size, chunk count, category |
| `ChunkLruCache` | Memory-bounded LRU cache for segment chunks |
| `HybridBTreeDirectory` | Implements Tantivy's Directory trait |
| `LazyFileHandle` | Fetches chunks on demand |

### Memory Usage Comparison

| Index Size | Old (CachedBTreeDirectory) | New (HybridBTreeDirectory) |
|------------|---------------------------|---------------------------|
| 100MB | ~100MB | ~25MB |
| 500MB | ~500MB | ~80MB |
| 1GB | ~1GB | ~150MB |

### Configuration Constants

```rust
DEFAULT_MEMORY_BUDGET_BYTES  = 64 MB   // Tantivy IndexWriter memory budget
DEFAULT_CHUNK_SIZE           = 1 MB    // BTree blob chunk size
DEFAULT_HOT_CACHE_BYTES      = 64 MB   // Bounded LRU cache for metadata/term dicts
DEFAULT_CHUNK_CACHE_BYTES    = 128 MB  // Bounded LRU cache for segment chunks
BATCH_COMMIT_SIZE            = 1000    // Documents per Tantivy commit
```

Both the hot cache and chunk cache use approximate LRU eviction to stay within their memory budgets, preventing unbounded memory growth with many FTS indexes.

---

# Index Maintenance

## OPTIMIZE INDEX

The `OPTIMIZE INDEX` command merges all Tantivy segments into a single segment, which can improve query performance and reduce storage overhead. This is especially useful after bulk inserts that create many small segments.

```sql
-- Optimize a specific FTS index
OPTIMIZE INDEX fts_articles;

-- Optimize all FTS indexes in the database
OPTIMIZE INDEX;
```

**When to use:**
- After bulk data imports (many INSERTs)
- When you notice query performance degradation
- During maintenance windows

**What it does:**
- Merges all segments into one optimized segment
- Flushes any pending documents first
- Invalidates caches to ensure fresh reads
- Works within normal transaction semantics

**Note:** Optimization can take time on large indexes. For very large indexes with millions of documents, consider running this during off-peak hours.

---

# Current Limitations

The FTS implementation has some known limitations that may be addressed in future versions:

| Limitation | Description |
|------------|-------------|
| No `snippet()` function | Cannot return context snippets around matches (highlight is available) |
| No automatic segment merging | Uses `NoMergePolicy` - use `OPTIMIZE INDEX` for manual segment merging |
| No read-your-writes in transaction | FTS changes within a transaction aren't visible to queries until COMMIT |
| No MATCH operator syntax | Must use `fts_match()` function instead of `WHERE table MATCH 'query'` |

**Note on transactions:** ROLLBACK works correctly - FTS data is stored in BTrees that participate in the same WAL transaction as table data. When a transaction is rolled back, both table changes and FTS index changes are discarded together.

# Comparison with SQLite FTS5

| Feature | SQLite FTS5 | tursodb FTS |
|---------|-------------|-------------|
| MATCH operator | `WHERE t MATCH 'query'` | `WHERE fts_match(col, 'query')` |
| Ranking | `bm25(t)`, `rank` column | `fts_score(col, 'query')` |
| Highlighting | `highlight(t, ...)` | `fts_highlight(text, query, before, after)` ✓ |
| Snippets | `snippet(t, ...)` | Not implemented |
| Boolean operators | AND, OR, NOT | AND, OR, NOT ✓ |
| Phrase search | `"exact phrase"` | `"exact phrase"` ✓ |
| Prefix search | `word*` | `word*` ✓ |
| Column filter | `col:term` | `col:term` ✓ |
| Tokenizers | unicode61, ascii, porter | default, raw, simple, whitespace, ngram ✓ |
| External content | contentless tables | Not supported |
