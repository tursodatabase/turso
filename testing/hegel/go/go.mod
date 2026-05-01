module turso.tech/testing/hegel-go

go 1.25.0

require (
	github.com/tursodatabase/turso-go-platform-libs v0.0.0-20251210190052-57d6c2f7db38
	hegel.dev/go/hegel v0.3.2
	turso.tech/database/tursogo v0.0.0
	turso.tech/database/tursogo-serverless v0.0.0
)

require (
	github.com/ebitengine/purego v0.9.1 // indirect
	github.com/fxamacker/cbor/v2 v2.9.0 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	golang.org/x/sys v0.41.0 // indirect
)

replace (
	turso.tech/database/tursogo => ../../../bindings/go
	turso.tech/database/tursogo-serverless => ../../../serverless/go
)
