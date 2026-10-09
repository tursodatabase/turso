# SQLancer Testing

Run [SQLancer](https://github.com/sqlancer/sqlancer) against Limbo to find bugs.

## Usage

```bash
./scripts/run-sqlancer.sh              # 60s default
./scripts/run-sqlancer.sh --timeout 300  # 5 minutes
./scripts/run-sqlancer.sh --clean      # force rebuild
```

## Oracles

`--oracle` takes the SQLite oracles of SQLancer (default `NoREC`) and `LATERAL_JSON`.

`LATERAL_JSON` compares a query that has LATERAL joins with the same query where some of the joins read
`json_each` over `json_group_array`. It is a port of the `LateralMatchesJsonEach` property of the simulator,
and it needs a Turso build that supports LATERAL joins.

```bash
./scripts/run-sqlancer.sh --oracle LATERAL_JSON --timeout 300
```

## Requirements

- Java 11+
- Rust toolchain

## Logs

`/tmp/sqlancer-limbo/logs/limbo/` - one file per database with all executed SQL.

## Updating

Edit `patches/LimboProvider.java`:
- Remove from `LIMBO_EXPECTED_ERRORS` when features are implemented
- Add to `DEFAULT_PRAGMAS` when new pragmas are supported

`patches/src/` has the oracles that upstream SQLancer does not have, in the directory layout of SQLancer.
The scripts copy it into the SQLancer clone, and `patches/SQLite3OracleFactory.patch` adds `LATERAL_JSON`
to the oracle list of SQLancer.
