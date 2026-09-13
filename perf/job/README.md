# Join Order Benchmark (JOB)

The Join Order Benchmark from "How Good Are Query Optimizers, Really?"
(Leis et al., VLDB 2015): 113 analytical queries over the IMDB schema,
designed to stress join-order planning with realistic correlated data.

Files are vendored from <https://github.com/gregrahn/join-order-benchmark>:

- `schema.sql` - IMDB table definitions (21 tables)
- `fkindexes.sql` - foreign-key indexes the benchmark assumes
- `queries/` - the 113 queries (33 templates, variants `a`-`f`)

The queries are currently used by `core/benches/prepare_benchmark.rs` to
track statement preparation (parse + plan + codegen) cost; no IMDB data is
needed for that. To run the queries against real data, load an IMDB dump as
described in the upstream repository.
