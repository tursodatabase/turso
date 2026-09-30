---
display_name: "vector search"
---

# Vector Search

## Overview

Turso supports vector operations for building similarity search and semantic search applications. Vectors are stored as BLOBs and can be searched using distance functions to find similar items.

**Important:** Vector indexes are not yet supported. All vector searches currently use brute-force scanning, which means searching scales linearly with the number of rows.

## Vector Types

Turso supports two vector formats:

- **`vector32`** - 32-bit floating-point vectors (4 bytes per dimension)
- **`vector64`** - 64-bit floating-point vectors (8 bytes per dimension)

## Creating and Storing Vectors

Vectors are stored in vector columns as-is, and represented on-disk as BLOBs. Embeddings are interpreted and validated at runtime. In order for embedding to be valid, it must be either JSON array of float values OR binary blob created with turso vector functiosn `vector32` / `vector64`. 

### Basic Example

```sql
-- Create a table with vector embeddings
CREATE TABLE documents (
    id INTEGER PRIMARY KEY,
    content TEXT,
    embedding BLOB  -- Store vector as BLOB
);

-- Insert vectors using vector32() or vector64()
INSERT INTO documents VALUES
    (1, 'Introduction to databases', vector32('[0.1, 0.2, 0.3, 0.4]')),
    (2, 'SQL query optimization', vector32('[0.2, 0.1, 0.4, 0.3]')),
    (3, 'Vector similarity search', vector32('[0.4, 0.3, 0.2, 0.1]'));
```

### Working with Higher Dimensions

Real embeddings typically have hundreds or thousands of dimensions:

```sql
-- Example with 1536-dimensional embeddings (like OpenAI's ada-002)
CREATE TABLE embeddings (
    id INTEGER PRIMARY KEY,
    text TEXT,
    vector BLOB
);

-- Insert a 1536-dimensional vector
INSERT INTO embeddings VALUES
    (1, 'Sample text', vector32('[0.001, 0.002, ..., 0.1536]'));
```

## Vector Functions

### Creation Functions

- **`vector32(text)`** - Create a 32-bit float vector from JSON array
- **`vector64(text)`** - Create a 64-bit float vector from JSON array

### Distance Functions

- **`vector_distance_l2(v1, v2)`** - Euclidean (L2) distance between vectors
- **`vector_distance_cos(v1, v2)`** - Cosine distance (1 - cosine similarity)

### Utility Functions

- **`vector_extract(blob)`** - Convert vector BLOB back to JSON text
- **`vector_concat(v1, v2)`** - Concatenate two vectors
- **`vector_slice(v, start, end)`** - Extract a portion of a vector

## Similarity Search Examples

### Finding Similar Documents

```sql
-- Find documents similar to a query vector
WITH query AS (
    SELECT vector32('[0.15, 0.25, 0.35, 0.45]') AS query_vector
)
SELECT
    id,
    content,
    vector_distance_l2(embedding, query_vector) AS distance
FROM documents, query
ORDER BY distance
LIMIT 5;
```

### Cosine Similarity Search

Cosine similarity is often preferred for text embeddings:

```sql
-- Find semantically similar documents using cosine distance
WITH query AS (
    SELECT vector32('[0.15, 0.25, 0.35, 0.45]') AS query_vector
)
SELECT
    id,
    content,
    vector_distance_cos(embedding, query_vector) AS cosine_distance
FROM documents, query
ORDER BY cosine_distance
LIMIT 5;
```

### Threshold-Based Search

Find all vectors within a certain distance:

```sql
-- Find all documents within distance threshold
WITH query AS (
    SELECT vector32('[0.15, 0.25, 0.35, 0.45]') AS query_vector
)
SELECT
    id,
    content,
    vector_distance_l2(embedding, query_vector) AS distance
FROM documents, query
WHERE vector_distance_l2(embedding, query_vector) < 0.5
ORDER BY distance;
```

## Working with Vector Data

### Inspecting Vectors

```sql
-- Extract and view vector data as JSON
SELECT id, vector_extract(embedding) AS vector_json
FROM documents
LIMIT 3;
```

### Vector Operations

```sql
-- Concatenate two vectors
SELECT vector_concat(
    vector32('[1.0, 2.0]'),
    vector32('[3.0, 4.0]')
) AS concatenated;

-- Slice a vector (extract dimensions 2-4)
SELECT vector_slice(
    vector32('[1.0, 2.0, 3.0, 4.0, 5.0]'),
    2, 4
) AS sliced;
```

## Building a Semantic Search Application

Here's a complete example of a semantic search application:

```sql
-- 1. Create schema
CREATE TABLE articles (
    id INTEGER PRIMARY KEY,
    title TEXT,
    content TEXT,
    embedding BLOB
);

-- 2. Insert pre-computed embeddings
INSERT INTO articles VALUES
    (1, 'Database Fundamentals', 'An introduction to relational databases...',
     vector32('[0.12, -0.34, 0.56, ...]')),
    (2, 'Machine Learning Basics', 'Understanding neural networks and deep learning...',
     vector32('[0.23, 0.45, -0.67, ...]')),
    (3, 'Web Development Guide', 'Modern web applications with JavaScript...',
     vector32('[0.34, -0.12, 0.78, ...]'));

-- 3. Search for similar articles
WITH search_embedding AS (
    -- This would come from your embedding model for the search query
    SELECT vector32('[0.15, -0.30, 0.60, ...]') AS query_vec
)
SELECT
    a.id,
    a.title,
    vector_distance_cos(a.embedding, s.query_vec) AS similarity_score
FROM articles a, search_embedding s
ORDER BY similarity_score
LIMIT 10;
```

## Performance Considerations

Since vector indexes are not yet implemented, keep in mind:

- **Linear scan**: Every search examines all rows in the table
- **Memory usage**: Vectors consume significant space (4 bytes × dimensions for vector32)
- **Optimization tips**:
  - Use smaller dimensions when possible
  - Pre-filter data with WHERE clauses before distance calculations
  - Consider partitioning large datasets
  - Use vector32 instead of vector64 unless high precision is needed

## Common Use Cases

- **Semantic search**: Find documents by meaning rather than keywords
- **Recommendation systems**: Find similar items based on embeddings
- **Duplicate detection**: Identify near-duplicate content
- **Image similarity**: Search for similar images using visual embeddings
- **Anomaly detection**: Find outliers in high-dimensional data
