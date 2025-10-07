#!/bin/sh

cargo build --release

echo "system,threads,batch_size,compute,throughput"

for threads in 1 2 4 8; do
  for compute in 0 100 500 1000; do
      rm -f write_throughput_test.db*
      ../../../target/release/write-throughput-sqlite --threads ${threads} --batch-size 100 --compute ${compute} -i 1000
  done
done
