//go:build linux || darwin

package turso

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/ebitengine/purego"
)

func loadLibrary(name string) (uintptr, error) {
	// Try to extract embedded library first
	libPath, err := extractEmbeddedLibrary(name)
	if err == nil {
		// Successfully extracted embedded library, try to load it
		slib, dlerr := purego.Dlopen(libPath, purego.RTLD_NOW|purego.RTLD_GLOBAL)
		if dlerr == nil {
			return slib, nil
		}
		// If loading failed, log the error and fall back to system paths
		fmt.Printf("Warning: Failed to load embedded library: %v\n", dlerr)
	} else {
		fmt.Printf("Warning: Failed to extract embedded library: %v\n", err)
	}

	// Fall back to original behavior for compatibility
	var libraryName string
	switch runtime.GOOS {
	case "darwin":
		libraryName = fmt.Sprintf("lib%s.dylib", name)
	case "linux":
		libraryName = fmt.Sprintf("lib%s.so", name)
	default:
		return 0, fmt.Errorf("GOOS=%s is not supported", runtime.GOOS)
	}

	libPath = os.Getenv("LD_LIBRARY_PATH")
	paths := strings.Split(libPath, ":")
	cwd, err := os.Getwd()
	if err != nil {
		return 0, err
	}
	paths = append(paths, cwd)

	for _, path := range paths {
		libPath := filepath.Join(path, libraryName)
		if _, err := os.Stat(libPath); err == nil {
			slib, dlerr := purego.Dlopen(libPath, purego.RTLD_NOW|purego.RTLD_GLOBAL)
			if dlerr != nil {
				return 0, fmt.Errorf("failed to load library at %s: %w", libPath, dlerr)
			}
			return slib, nil
		}
	}
	return 0, fmt.Errorf("%s library not found in LD_LIBRARY_PATH or CWD", name)
}
