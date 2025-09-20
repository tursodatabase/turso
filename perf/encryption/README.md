# Encryption Throughput Benchmarking

```shell
$ cargo run --release -- --help

Usage: encryption-throughput [OPTIONS]

Options:
  -t, --threads <THREADS>          [default: 1]
  -b, --batch-size <BATCH_SIZE>    [default: 100]
  -i, --iterations <ITERATIONS>    [default: 10]
  -r, --read-ratio <READ_RATIO>    Percentage of operations that should be reads (0-100)
  -w, --write-ratio <WRITE_RATIO>  Percentage of operations that should be writes (0-100)
      --encryption                 Enable database encryption
      --cipher <CIPHER>            Encryption cipher to use (only relevant if --encryption is set) [default: aegis-256]
      --think <THINK>              Per transaction think time (ms) [default: 0]
      --timeout <TIMEOUT>          Busy timeout in milliseconds [default: 30000]
      --seed <SEED>                Random seed for reproducible workloads [default: 2167532792061351037]
  -h, --help                       Print help
```

```shell
# try these:

cargo run --release -- -b 100 -i 25000 --read-ratio 75

cargo run --release -- -b 100 -i 25000 --read-ratio 75 --encryption
```