module stress-go

go 1.24.0

toolchain go1.24.10

require (
	github.com/tursodatabase/turso-go-platform-libs v0.0.0-20251210190052-57d6c2f7db38
	gorm.io/driver/sqlite v1.5.6
	gorm.io/gorm v1.25.12
	turso.tech/database/tursogo v0.0.0
)

require (
	github.com/ebitengine/purego v0.9.1 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/mattn/go-sqlite3 v1.14.22 // indirect
	golang.org/x/sys v0.38.0 // indirect
	golang.org/x/text v0.14.0 // indirect
)

replace turso.tech/database/tursogo => ../bindings/go
