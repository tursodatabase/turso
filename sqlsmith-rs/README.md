# sqlsmith-rs

[![CircleCI](https://dl.circleci.com/status-badge/img/gh/cyw0ng95/sqlsmith-rs/tree/main.svg?style=svg)](https://dl.circleci.com/status-badge/redirect/gh/cyw0ng95/sqlsmith-rs/tree/main)

`sqlsmith-rs` is a random SQL testing tool written in Rust. It automatically generates and executes a wide variety of SQL statements to uncover potential bugs in database engines. The tool supports both SQLite in-memory and Limbo databases, and allows users to configure the probability of generating different SQL statement types via a profile. It features modular statement generators, unified driver interfaces, and can be easily extended to support new SQL dialects or database backends. Typical use cases include database engine fuzzing, regression testing, and SQL compatibility validation.

## Project Structure
The project is organized as follows:
```plaintext
sqlsmith-rs/
├── profile.json        # Main configuration file for SQL generation probabilities and options
├── src/                # (Legacy) main source code directory
├── common/             # Shared utilities: random number generator, profile parsing, logging
│   ├── lib.rs
│   ├── profile.rs
│   └── rand_by_seed.rs
├── drivers/            # Database driver abstraction and implementations
│   ├── lib.rs
│   ├── limbo_in_mem.rs
│   └── sqlite_in_mem.rs
├── executor/           # SQL statement generation and execution engine
│   ├── main.rs         # CLI entry point
│   ├── engines/        # Engine logic for Limbo/SQLite
│   ├── generators/     # Modular SQL statement generators
│   │   ├── common/     # Shared statement generators (select, insert, update, ...)
│   │   ├── limbo/      # Limbo-specific schema and logic
│   │   └── sqlite/     # SQLite-specific schema and logic
│   └── ...
├── server/             # (Optional) Web server and web UI for interactive testing
│   ├── main.rs
│   └── fork_server/
├── assets/             # Example schemas and SQL files for Limbo/SQLite
│   ├── limbo/
│   └── sqlite/
├── build.sh            # Build helper script
├── runenv.sh           # Runtime environment setup
├── runtime.json        # Runtime configuration
├── sonar-project.properties # SonarCloud config
└── ...                 # Other files (logs, lockfiles, etc)
```