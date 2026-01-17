<p align="center">
  <h1 align="center">Turso Database for Go</h1>
</p>

<p align="center">
  <a title="Go" target="_blank" href="https://pkg.go.dev/turso.tech/database/tursogo"><img src="https://pkg.go.dev/badge/turso.tech/database/tursogo.svg" alt="Go Reference"></a>
  <a title="MIT" target="_blank" href="https://github.com/tursodatabase/turso/blob/main/LICENSE.md"><img src="http://img.shields.io/badge/license-MIT-orange.svg?style=flat-square"></a>
</p>
<p align="center">
  <a title="Users Discord" target="_blank" href="https://tur.so/discord"><img alt="Chat with other users of Turso on Discord" src="https://img.shields.io/discord/933071162680958986?label=Discord&logo=Discord&style=social"></a>
</p>

---

## About

> **⚠️ Warning:** This software is in BETA. It may still contain bugs and unexpected behavior. Use caution with production data and ensure you have backups.

## Features

- **SQLite compatible:** SQLite query language and file format support ([status](https://github.com/tursodatabase/turso/blob/main/COMPAT.md)).
- **In-process**: No network overhead, runs directly in your Go process
- **Cross-platform**: Supports Linux, macOS, Windows
- **No CGO**: This driver uses the awesome [purego](https://github.com/ebitengine/purego) library to call C (in this case Rust with C ABI) functions from Go.
- **Remote partial sync**: Bootstrap from a remote database, pull remote changes, and push local changes when online &mdash; all while enjoying a fully operational database offline.

## Installation

```bash
go get turso.tech/database/tursogo
```

## Get Started

```go
package main

import (
	"database/sql"
	"fmt"
	"os"

	_ "turso.tech/database/tursogo"
)

func main() {
	conn, err := sql.Open("turso", ":memory:")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
	sql := "CREATE table go_turso (foo INTEGER, bar TEXT)"
	_, _ = conn.Exec(sql)

	sql = "INSERT INTO go_turso (foo, bar) values (?, ?)"
	stmt, _ := conn.Prepare(sql)
	defer stmt.Close()
	_, _ = stmt.Exec(42, "turso")
	rows, _ := conn.Query("SELECT * from go_turso")
	defer rows.Close()
	for rows.Next() {
		var a int
		var b string
		_ = rows.Scan(&a, &b)
		fmt.Printf("%d, %s\n", a, b) // 42, turso
	}
}

```

## License

This project is licensed under the [MIT license](../../LICENSE.md).

## Support

- [GitHub Issues](https://github.com/tursodatabase/turso/issues)
- [Documentation](https://docs.turso.tech)
- [Discord Community](https://tur.so/discord)
