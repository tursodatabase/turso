package turso

import "fmt"

func init() {
	library, err := loadLibrary("turso_sdk_kit")
	if err != nil {
		panic(fmt.Errorf("unable to load turso library: %w", err))
	}
	register_turso_db(library)
}
