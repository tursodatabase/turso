// Go bindings for the turso database.
//
// This file implements library embedding and extraction at runtime, a pattern
// also used in several other Go projects that need to distribute native binaries:
//
//   - github.com/kluctl/go-embed-python: Embeds a full Python distribution in Go
//     binaries, extracting to temporary directories at runtime. The approach used here
//     was directly inspired by its embed_util implementation.
//
//   - github.com/kluctl/go-jinja2: Uses the same pattern to embed Jinja2 and related
//     Python libraries, allowing Go applications to use Jinja2 templates without
//     external dependencies.
//
// This approach has several advantages:
// - Allows distribution of a single, self-contained binary
// - Eliminates the need for users to set LD_LIBRARY_PATH or other environment variables
// - Works cross-platform with the same codebase
// - Preserves backward compatibility with existing methods
// - Extracts libraries only once per execution via sync.Once
//
// The embedded library is extracted to a user-specific temporary directory and
// loaded dynamically. If extraction fails, the code falls back to the traditional
// method of searching system paths.
package turso

import (
	"embed"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"
)

//go:embed libs/*
var embeddedLibs embed.FS

//go:embed VERSION
var embeddedVersion string

// isMuslLibc detects if the system is using musl libc (Alpine Linux, Void Linux, etc.)
func isMuslLibc() bool {
	// Check for Alpine release file
	if _, err := os.Stat("/etc/alpine-release"); err == nil {
		return true
	}

	// Check ldd output for musl - more reliable for detecting any musl-based system
	cmd := exec.Command("ldd", "--version")
	if output, err := cmd.CombinedOutput(); err == nil {
		if strings.Contains(strings.ToLower(string(output)), "musl") {
			return true
		}
	}

	return false
}

// extractEmbeddedLibrary extracts the library for the current platform
// to a temporary directory and returns the path to the extracted library
func extractEmbeddedLibrary(name string) (string, error) {
	// Determine platform-specific details
	var libName string
	var platformDir string

	switch runtime.GOOS {
	case "darwin":
		libName = fmt.Sprintf("lib%v.dylib", name)
	case "linux":
		libName = fmt.Sprintf("lib%v.so", name)
	case "windows":
		libName = fmt.Sprintf("%v.dll", name)
	default:
		return "", fmt.Errorf("unsupported operating system: %s", runtime.GOOS)
	}

	// Determine architecture suffix
	var archSuffix string
	switch runtime.GOARCH {
	case "amd64":
		archSuffix = "amd64"
	case "arm64":
		archSuffix = "arm64"
	case "386":
		archSuffix = "386"
	default:
		return "", fmt.Errorf("unsupported architecture: %s", runtime.GOARCH)
	}

	// Detect libc variant on Linux (musl for Alpine)
	libcVariant := ""
	if runtime.GOOS == "linux" {
		if isMuslLibc() {
			libcVariant = "_musl"
		}
	}

	// Create platform directory string
	platformDir = fmt.Sprintf("%s%s_%s", runtime.GOOS, libcVariant, archSuffix)

	// Try to find the embedded library - first with detected platform,
	// then fallback to glibc variant on Linux if musl is not available
	embedPath := path.Join("libs", platformDir, libName)
	fallbackPaths := []string{embedPath}

	// If we're on musl Linux but the musl library isn't available, try glibc
	if runtime.GOOS == "linux" && libcVariant == "_musl" {
		glibcPlatform := fmt.Sprintf("%s_%s", runtime.GOOS, archSuffix)
		fallbackPaths = append(fallbackPaths, path.Join("libs", glibcPlatform, libName))
	}

	cacheRoot := os.Getenv("TURSO_GO_CACHE_DIR")
	if cacheRoot == "" {
		if d, err := os.UserCacheDir(); err == nil {
			cacheRoot = d
		} else {
			cacheRoot = os.TempDir()
		}
	}
	destDir := filepath.Join(cacheRoot, name, embeddedVersion, platformDir)

	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return "", fmt.Errorf("mkdir %s: %w", destDir, err)
	}
	extractedPath := filepath.Join(destDir, libName)

	if fi, err := os.Stat(extractedPath); err == nil && fi.Size() > 0 {
		return extractedPath, nil
	}

	// Try each path until we find one that exists
	var in fs.File
	var err error
	foundPath := ""
	for _, tryPath := range fallbackPaths {
		in, err = embeddedLibs.Open(tryPath)
		if err == nil {
			foundPath = tryPath
			break
		}
	}
	if foundPath == "" {
		return "", fmt.Errorf("open embedded library (tried %v): %w", fallbackPaths, err)
	}
	defer in.Close()

	out, err := os.Create(extractedPath)
	if err != nil {
		return "", fmt.Errorf("create %s: %w", extractedPath, err)
	}
	defer out.Close()

	if _, err := io.Copy(out, in); err != nil {
		return "", fmt.Errorf("copy: %w", err)
	}
	if runtime.GOOS != "windows" {
		_ = os.Chmod(extractedPath, 0o755)
	}
	return extractedPath, nil
}
