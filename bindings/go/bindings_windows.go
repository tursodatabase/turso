//go:build windows

package turso

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/windows"
)

func loadLibrary(name string) (uintptr, error) {
	// Try to extract embedded library first
	libPath, err := extractEmbeddedLibrary(name)
	if err == nil {
		// Successfully extracted embedded library, try to load it
		slib, dlerr := windows.LoadLibrary(libPath)
		if dlerr == nil {
			return uintptr(slib), nil
		}
		// If loading failed, log the error and fall back to system paths
		fmt.Printf("Warning: Failed to load embedded library: %v\n", dlerr)
	} else {
		fmt.Printf("Warning: Failed to extract embedded library: %v\n", err)
	}

	// Fall back to original behavior
	libraryName := fmt.Sprintf("%s.dll", name)

	pathEnv := os.Getenv("PATH")
	paths := strings.Split(pathEnv, ";")
	cwd, err := os.Getwd()
	if err != nil {
		return 0, err
	}
	paths = append(paths, cwd)

	for _, path := range paths {
		dllPath := filepath.Join(path, libraryName)
		if _, err := os.Stat(dllPath); err == nil {
			slib, loadErr := windows.LoadLibrary(dllPath)
			if loadErr != nil {
				return 0, fmt.Errorf("failed to load library at %s: %w", dllPath, loadErr)
			}
			return uintptr(slib), nil
		}
	}

	return 0, fmt.Errorf("library %s not found in PATH or CWD", libraryName)
}
