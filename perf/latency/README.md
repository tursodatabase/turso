# Latency benchmarks

`rusqlite/` and `limbo/` measure SELECT latency versus tenant count. In each
directory:

```
./gen-databases
./run-benchmark.sh
```

INSERT+COMMIT latency is `./perf/throughput/run.sh`. That runner samples
BEGIN→COMMIT on a closed loop. Occupancy is the worker count, not tenant
count. p50 and p99 land in the throughput CSV. An ECDF JSON dump is optional.
