# SQLancer Testing

Run [SQLancer](https://github.com/sqlancer/sqlancer) against Limbo to find bugs.

## Usage

```bash
./scripts/run-sqlancer.sh              # 60s default
./scripts/run-sqlancer.sh --timeout 300  # 5 minutes
./scripts/run-sqlancer.sh --clean      # force rebuild
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
