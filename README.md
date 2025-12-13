<p align="center">
  <img src="assets/turso.png" alt="Turso Database" width="800"/>
  <h1 align="center">Turso Database</h1>
</p>

<p align="center">
  An in-process SQL database, compatible with SQLite.
</p>

<p align="center">
  <a title="Build Status" target="_blank" href="https://github.com/tursodatabase/turso/actions/workflows/rust.yml"><img src="https://img.shields.io/github/actions/workflow/status/tursodatabase/turso/rust.yml?style=flat-square"></a>
  <a title="Releases" target="_blank" href="https://github.com/tursodatabase/turso/releases"><img src="https://img.shields.io/github/release/tursodatabase/turso?style=flat-square&color=9CF"></a>
  <a title="Rust" target="_blank" href="https://crates.io/crates/turso"><img alt="Crate" src="https://img.shields.io/crates/v/turso"></a>
  <a title="JavaScript" target="_blank" href="https://www.npmjs.com/package/@tursodatabase/database"><img alt="NPM" src="https://img.shields.io/npm/v/@tursodatabase/database"></a>
  <a title="Python" target="_blank" href="https://pypi.org/project/pyturso/"><img alt="PyPI" src="https://img.shields.io/pypi/v/pyturso"></a>
  <a title="Java" target="_blank" href="https://central.sonatype.com/artifact/tech.turso/turso"><img alt="Maven Central" src="https://img.shields.io/maven-central/v/tech.turso/turso"></a>
  <a title="MIT" target="_blank" href="https://github.com/tursodatabase/turso/blob/main/LICENSE.md"><img src="http://img.shields.io/badge/license-MIT-orange.svg?style=flat-square"></a>
  <br>
  <a title="GitHub Pull Requests" target="_blank" href="https://github.com/tursodatabase/turso/pulls"><img src="https://img.shields.io/github/issues-pr-closed/tursodatabase/turso.svg?style=flat-square&color=FF9966"></a>
  <a title="GitHub Commits" target="_blank" href="https://github.com/tursodatabase/turso/commits/main"><img src="https://img.shields.io/github/commit-activity/m/tursodatabase/turso.svg?style=flat-square"></a>
  <a title="Last Commit" target="_blank" href="https://github.com/tursodatabase/turso/commits/main"><img src="https://img.shields.io/github/last-commit/tursodatabase/turso.svg?style=flat-square&color=FF9900"></a>
</p>
<p align="center">
  <a title="Developer's Discord" target="_blank" href="https://discord.gg/jgjmyYgHwB"><img alt="Chat with the Core Developers on Discord" src="https://img.shields.io/discord/1258658826257961020?label=Discord&logo=Discord&style=social&label=Core%20Developers"></a>
</p>
<p align="center">
  <a title="Users's Discord" target="_blank" href="https://tur.so/discord"><img alt="Chat with other users of Turso (and Turso Cloud) on Discord" src="https://img.shields.io/discord/933071162680958986?label=Discord&logo=Discord&style=social&label=Users"></a>
</p>

---

## About

Turso Database is an in-process SQL database written in Rust, compatible with SQLite.

> **⚠️ Warning:** This software is in BETA. It may still contain bugs and unexpected behavior. Use caution with production data and ensure you have backups.

## Features and Roadmap

* **SQLite compatibility** for SQL dialect, file formats, and the C API [see [document](COMPAT.md) for details]
* **Change data capture (CDC)** for real-time tracking of database changes.
* **Multi-language support** for
  * [Go](https://github.com/tursodatabase/turso-go)
  * [JavaScript](bindings/javascript)
  * [Java](bindings/java)
  * [Python](bindings/python)
  * [Rust](bindings/rust)
  * [WebAssembly](bindings/javascript)
* **Asynchronous I/O** support on Linux with `io_uring`
* **Cross-platform** support for Linux, macOS, Windows and browsers (through WebAssembly)
* **Vector support** support including exact search and vector manipulation
* **Improved schema management** including extended `ALTER` support and faster schema changes.

The database has the following experimental features:

* **`BEGIN CONCURRENT`** for improved write throughput using multi-version concurrency control (MVCC).
* **Encryption at rest** for protecting the data locally.
* **Incremental computation** using DBSP for incremental view maintenance and query subscriptions.

The following features are on our current roadmap:

* **Vector indexing** for fast approximate vector search, similar to [libSQL vector search](https://turso.tech/vector).

## Getting Started

Please see the [Turso Database Manual](docs/manual.md) for more information.

<details>
<summary>💻 Command Line</summary>
<br>
You can install the latest `turso` release with:

```shell
curl --proto '=https' --tlsv1.2 -LsSf \
  https://github.com/tursodatabase/turso/releases/latest/download/turso_cli-installer.sh | sh
```

Then launch the interactive shell:

```shell
$ tursodb
```

This will start the Turso interactive shell where you can execute SQL statements:

```console
Turso
Enter ".help" for usage hints.
Connected to a transient in-memory database.
Use ".open FILENAME" to reopen on a persistent database
turso> CREATE TABLE users (id INT, username TEXT);
turso> INSERT INTO users VALUES (1, 'alice');
turso> INSERT INTO users VALUES (2, 'bob');
turso> SELECT * FROM users;
1|alice
2|bob
```

You can also build and run the latest development version with:

```shell
cargo run
```

If you like docker, we got you covered. Simply run this in the root folder:

```bash
make docker-cli-build && \
make docker-cli-run
```

</details>

<details>
<summary>🦀 Rust</summary>
<br>

```console
cargo add turso
```

Example usage:

```rust
let db = Builder::new_local("sqlite.db").build().await?;
let conn = db.connect()?;

let res = conn.query("SELECT * FROM users", ()).await?;
```
</details>

<details>
<summary>✨ JavaScript</summary>
<br>

```console
npm i @tursodatabase/database
```

Example usage:

```js
import { connect } from '@tursodatabase/database';

const db = await connect('sqlite.db');
const stmt = db.prepare('SELECT * FROM users');
const users = stmt.all();
console.log(users);
```
</details>

<details>
<summary>🐍 Python</summary>
<br>

```console
uv pip install pyturso
```

Example usage:

```python
import turso

con = turso.connect("sqlite.db")
cur = con.cursor()
res = cur.execute("SELECT * FROM users")
print(res.fetchone())
```
</details>

<details>
<summary>🦫 Go</summary>
<br>

```console
go get github.com/tursodatabase/turso-go
go install github.com/tursodatabase/turso-go
```

Example usage:
```go
import (
    "database/sql"
    _ "github.com/tursodatabase/turso-go"
)

conn, _ = sql.Open("turso", "sqlite.db")
defer conn.Close()

stmt, _ := conn.Prepare("select * from users")
defer stmt.Close()

rows, _ = stmt.Query()
for rows.Next() {
    var id int
    var username string
    _ := rows.Scan(&id, &username)
    fmt.Printf("User: ID: %d, Username: %s\n", id, username)
}
```
</details>

<details>

<summary>☕️ Java</summary>
<br>

We integrated Turso Database into JDBC. For detailed instructions on how to use Turso Database with java, please refer to
the [README.md under bindings/java](bindings/java/README.md).
</details>

<details>
<summary>🤖 MCP Server Mode</summary>
<br>


The Turso CLI includes a built-in [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server that allows AI assistants to interact with your databases.

Start the MCP server with:

```shell
tursodb your_database.db --mcp
```

### Configuration

Add Turso to your MCP client configuration:

```json
{
  "mcpServers": {
    "turso": {
      "command": "/path/to/.turso/tursodb",
      "args": ["/path/to/your/database.db", "--mcp"]
    }
  }
}
```

### Available Tools

The MCP server provides nine tools for database interaction:

1. **`open_database`** - Open a new database
2. **`current_database`** - Describe the current database
3. **`list_tables`** - List all tables in the database
4. **`describe_table`** - Describe the structure of a specific table
5. **`execute_query`** - Execute read-only SELECT queries
6. **`insert_data`** - Insert new data into tables
7. **`update_data`** - Update existing data in tables
8. **`delete_data`** - Delete data from tables
9. **`schema_change`** - Execute schema modification statements (CREATE TABLE, ALTER TABLE, DROP TABLE)

Once connected, you can ask your AI assistant:

- "Show me all tables in the database"
- "What's the schema for the users table?"
- "Find all posts with more than 100 upvotes"
- "Insert a new user with name 'Alice' and email 'alice@example.com'"

### MCP Clients

<details>
<summary>Claude Code</summary>

If you're using [Claude Code](https://claude.ai/code), you can easily connect to your Turso MCP server using the built-in MCP management commands:

#### Quick Setup

1. **Add the MCP server** to Claude Code:

   ```bash
   claude mcp add my-database -- tursodb ./path/to/your/database.db --mcp
   ```

2. **Restart Claude Code** to activate the connection

3. **Start querying** your database through natural language!

#### Command Breakdown

```bash
claude mcp add my-database -- tursodb ./path/to/your/database.db --mcp
#              ↑            ↑       ↑                           ↑
#              |            |       |                           |
#              Name         |       Database path               MCP flag
#                          Separator
```

- **`my-database`** - Choose any name for your MCP server
- **`--`** - Required separator between Claude options and your command
- **`tursodb`** - The Turso database CLI
- **`./path/to/your/database.db`** - Path to your SQLite database file
- **`--mcp`** - Enables MCP server mode

#### Example Usage

```bash
# For a local project database
cd /your/project
claude mcp add my-project-db -- tursodb ./data/app.db --mcp

# For an absolute path
claude mcp add analytics-db -- tursodb /Users/you/databases/analytics.db --mcp

# For a specific project (local scope)
claude mcp add project-db --local -- tursodb ./database.db --mcp
```

#### Managing MCP Servers

```bash
# List all configured MCP servers
claude mcp list

# Get details about a specific server
claude mcp get my-database

# Remove an MCP server
claude mcp remove my-database
```

</details>

<details>
<summary>Claude Desktop</summary>

For Claude Desktop, add the configuration to your `claude_desktop_config.json` file:

```json
{
  "mcpServers": {
    "turso": {
      "command": "/path/to/.turso/tursodb",
      "args": ["./path/to/your/database.db.db", "--mcp"]
    }
  }
}
```

</details>

<details>
<summary>Cursor</summary>

For Cursor, configure MCP in your settings:

1. Open Cursor settings
2. Navigate to Extensions → MCP
3. Add a new server with:
   - **Name**: `turso`
   - **Command**: `/path/to/.turso/tursodb`
   - **Args**: `["./path/to/your/database.db.db", "--mcp"]`

Alternatively, you can add it to your Cursor configuration file directly.

</details>

### Direct JSON-RPC Usage

The MCP server runs as a single process that handles multiple JSON-RPC requests over stdin/stdout. Here's how to interact with it directly:

#### Example with In-Memory Database

```bash
cat << 'EOF' | tursodb --mcp
{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name": "client", "version": "1.0"}}}
{"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": "schema_change", "arguments": {"query": "CREATE TABLE users (id INTEGER, name TEXT, email TEXT)"}}}
{"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "list_tables", "arguments": {}}}
{"jsonrpc": "2.0", "id": 4, "method": "tools/call", "params": {"name": "insert_data", "arguments": {"query": "INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')"}}}
{"jsonrpc": "2.0", "id": 5, "method": "tools/call", "params": {"name": "execute_query", "arguments": {"query": "SELECT * FROM users"}}}
EOF
```

#### Example with Existing Database

```bash
# Working with an existing database file
cat << 'EOF' | tursodb mydb.db --mcp
{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name": "client", "version": "1.0"}}}
{"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": "list_tables", "arguments": {}}}
EOF
```

</details>

## Contributing

We'd love to have you contribute to Turso Database! Please check out the [contribution guide] to get started.

### Found a data corruption bug? Get up to $1,000.00

SQLite is loved because it is the most reliable database in the world. The next evolution of SQLite has
to match or surpass this level of reliability. Turso is built with [Deterministic Simulation Testing](simulator/)
from the ground up, and is also tested by [Antithesis](https://antithesis.com).

Even during Alpha, if you find a bug that leads to a data corruption and demonstrate
how our simulator failed to catch it, you can get up to $1,000.00. As the project matures we will
increase the size of the prize, and the scope of the bugs.

List of rewarded cases:

* B-Tree interior cell replacement issue in btrees with depth >=3 ([#2106](https://github.com/tursodatabase/turso/issues/2106))
* Don't allow autovacuum to be flipped on non-empty databases ([#3830](https://github.com/tursodatabase/turso/pull/3830))

More details [here](https://turso.algora.io).

Turso core staff are not eligible.

## FAQ

### Is Turso Database ready for production use?

Turso Database is currently under heavy development and is **not** ready for production use.

### How is Turso Database different from Turso's libSQL?

Turso Database is a project to build the next evolution of SQLite in Rust, with a strong open contribution focus and features like native async support, vector search, and more. The libSQL project is also an attempt to evolve SQLite in a similar direction, but through a fork rather than a rewrite.

Rewriting SQLite in Rust started as an unassuming experiment, and due to its incredible success, replaces libSQL as our intended direction. At this point, libSQL is production ready, Turso Database is not - although it is evolving rapidly. More details [here](https://turso.tech/blog/we-will-rewrite-sqlite-and-we-are-going-all-in).

## Publications

* Pekka Enberg, Sasu Tarkoma, Jon Crowcroft Ashwin Rao (2024). Serverless Runtime / Database Co-Design With Asynchronous I/O. In _EdgeSys ‘24_. [[PDF]](https://penberg.org/papers/penberg-edgesys24.pdf)
* Pekka Enberg, Sasu Tarkoma, and Ashwin Rao (2023). Towards Database and Serverless Runtime Co-Design. In _CoNEXT-SW ’23_. [[PDF](https://penberg.org/papers/penberg-conext-sw-23.pdf)] [[Slides](https://penberg.org/papers/penberg-conext-sw-23-slides.pdf)]

## License

This project is licensed under the [MIT license].

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in Turso Database by you, shall be licensed as MIT, without any additional
terms or conditions.

[contribution guide]: CONTRIBUTING.md
[MIT license]: LICENSE.md

## Partners

Thanks to all the partners of Turso!

<a href="https://antithesis.com/"><img src="assets/antithesis.jpg" width="400"></a>

<a href="https://blacksmith.sh"><img src="assets/blacksmith.svg" width="400"></a>

<a href="https://nyrkio.com/"><img src="assets/turso-nyrkio.png" width="400"></a>

## Contributors

Thanks to all the contributors to Turso Database!

<a href="https://github.com/tursodatabase/turso/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=tursodatabase/turso" />
</a>
