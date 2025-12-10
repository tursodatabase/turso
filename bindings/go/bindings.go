package turso

import (
	"fmt"
	"sync"

	turso_libs "github.com/tursodatabase/turso-go-platform-libs"
)

var initLibrary sync.Once

func InitLibrary(strategy turso_libs.LoadTursoLibraryConfig) {
	initLibrary.Do(func() {
		library, err := turso_libs.LoadTursoLibrary(strategy)
		if err != nil {
			panic(fmt.Errorf("unable to load turso library: %w", err))
		}
		registerTursoDb(library)
		registerTursoSync(library)
	})
}
