module example/encryption

go 1.24.0

require (
	github.com/tursodatabase/turso-go-platform-libs v0.0.0-20251210190052-57d6c2f7db38
	turso.tech/database/tursogo v0.0.0-00010101000000-000000000000
)

require (
	github.com/ebitengine/purego v0.9.1 // indirect
	golang.org/x/sys v0.38.0 // indirect
)

replace turso.tech/database/tursogo => ../../bindings/go
