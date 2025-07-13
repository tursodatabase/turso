<p align="center">
  <img src="turso.png" alt="Turso Database" width="800"/>
  <h1 align="center">Turso Database</h1>
</p>

<p align="center">
  <i>Turso Database</i> is an in-process SQL database, compatible with SQLite.
</p>

<p align="center">
  <a title="Build Status" target="_blank" href="https://github.com/tursodatabase/turso/actions/workflows/rust.yml"><img src="https://img.shields.io/github/actions/workflow/status/tursodatabase/turso/rust.yml?style=flat-square"></a>
  <a title="Releases" target="_blank" href="https://github.com/tursodatabase/turso/releases"><img src="https://img.shields.io/github/release/tursodatabase/turso?style=flat-square&color=9CF"></a>
  <a title="Rust" target="_blank" href="https://crates.io/crates/turso"><img alt="PyPI" src="https://img.shields.io/crates/v/turso"></a>
  <a title="JavaScript" target="_blank" href="https://www.npmjs.com/package/@tursodatabase/turso"><img alt="PyPI" src="https://img.shields.io/npm/v/@tursodatabase/turso"></a>
  <a title="Python" target="_blank" href="https://pypi.org/project/pyturso/"><img alt="PyPI" src="https://img.shields.io/pypi/v/pyturso"></a>
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

## Features and Roadmap

Turso Database is a _work-in-progress_, in-process OLTP database engine library written in Rust that has:

* **SQLite compatibility** [[doc](COMPAT.md)] for SQL dialect, file formats, and the C API
* **Language bindings** for JavaScript/WebAssembly, Rust, Go, Python, and [Java](bindings/java)
* **Asynchronous I/O** support on Linux with `io_uring`
* **OS support** for Linux, macOS, and Windows

In the future, we will be also working on:

* **`BEGIN CONCURRENT`** for improved write throughput.
* **Indexing for vector search**.
* **Improved schema management** including better `ALTER` support and strict column types by default.

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

Then launch the shell to execute SQL statements:

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
npm i @tursodatabase/turso
```

Example usage:

```js
import { Database } from '@tursodatabase/turso';

const db = new Database('sqlite.db');
const stmt = db.prepare('SELECT * FROM users');
const users = stmt.all();
console.log(users);
```
</details>

<details>
<summary>🐍 Python</summary>
<br>

```console
pip install pyturso
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

1. Clone the repository
2. Build the library and set your LD_LIBRARY_PATH to include turso's target directory
```console
cargo build --package limbo-go
export LD_LIBRARY_PATH=/path/to/limbo/target/debug:$LD_LIBRARY_PATH
```
3. Use the driver

```console
go get github.com/tursodatabase/turso
go install github.com/tursodatabase/turso
```

Example usage:
```go
import (
    "database/sql"
    _ "github.com/tursodatabase/turso"
)

conn, _ = sql.Open("sqlite3", "sqlite.db")
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

## Contributing

We'd love to have you contribute to Turso Database! Please check out the [contribution guide] to get started.

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

[contribution guide]: https://github.com/tursodatabase/turso/blob/main/CONTRIBUTING.md
[MIT license]: https://github.com/tursodatabase/turso/blob/main/LICENSE.md

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
