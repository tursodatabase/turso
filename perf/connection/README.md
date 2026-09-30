# Connection benchmarks

Run the following in `rusqlite` and `limbo` directories:

```
./gen-databases
./run-benchmark.sh
```

This benchmark tests the time it takes to open a connection to databases with varying numbers of tables (1,000, 10,000, and 100,000 tables).