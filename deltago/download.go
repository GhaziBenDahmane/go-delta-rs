package deltago

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
)

const fallbackVersion = "0.3.2"

const releaseBaseURL = "https://github.com/ghazibendahmane/go-delta-rs/releases/download"

// moduleVersion returns the version of this module as recorded in the build
// info, falling back to fallbackVersion when running from source (go run / tests).
func moduleVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return fallbackVersion
	}
	// When used as a dependency the module appears in info.Deps.
	for _, dep := range info.Deps {
		if dep.Path == "github.com/ghazibendahmane/go-delta-rs" {
			if v := dep.Version; v != "" && v != "(devel)" {
				return strings.TrimPrefix(v, "v")
			}
		}
	}
	// When this is the main module itself.
	if v := info.Main.Version; v != "" && v != "(devel)" {
		return strings.TrimPrefix(v, "v")
	}
	return fallbackVersion
}

// platformArtifact returns the release asset name for the current OS/arch.
func platformArtifact() (string, error) {
	goos := runtime.GOOS
	arch := runtime.GOARCH
	switch goos {
	case "linux":
		switch arch {
		case "amd64":
			return "delta-server-linux-amd64", nil
		case "arm64":
			return "delta-server-linux-arm64", nil
		}
	case "darwin":
		switch arch {
		case "amd64":
			return "delta-server-darwin-amd64", nil
		case "arm64":
			return "delta-server-darwin-arm64", nil
		}
	case "windows":
		if arch == "amd64" {
			return "delta-server-windows-amd64.exe", nil
		}
	}
	return "", fmt.Errorf("no pre-built binary for %s/%s — build from source: cd delta-server && cargo build --release", goos, arch)
}

// cachedBinaryPath returns the path where the binary for the current version
// is (or will be) stored on disk.
//
//	Linux:   ~/.cache/go-delta-rs/<version>/delta-server
//	macOS:   ~/Library/Caches/go-delta-rs/<version>/delta-server
//	Windows: %LOCALAPPDATA%/go-delta-rs/<version>/delta-server.exe
func cachedBinaryPath() (string, error) {
	dir, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("locate user cache dir: %w", err)
	}
	name := "delta-server"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	return filepath.Join(dir, "go-delta-rs", moduleVersion(), name), nil
}

// EnsureBinary returns the path to the delta-server binary using this resolution order:
//
//  1. $DELTA_SERVER_PATH env var — explicit override (useful in Docker/CI)
//  2. "delta-server" on $PATH — picks up system-installed binary automatically
//  3. OS cache directory — binary downloaded in a previous run
//  4. GitHub Releases download — first run; verified with SHA-256, then cached
func EnsureBinary() (string, error) {
	// 1. Explicit env var override.
	if p := os.Getenv("DELTA_SERVER_PATH"); p != "" {
		return p, nil
	}

	// 2. Binary already on PATH (e.g. /usr/local/bin/delta-server in Docker).
	if p, err := exec.LookPath("delta-server"); err == nil {
		return p, nil
	}

	// 3. Previously cached download.
	dest, err := cachedBinaryPath()
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(dest); err == nil {
		return dest, nil
	}

	artifact, err := platformArtifact()
	if err != nil {
		return "", err
	}

	ver := moduleVersion()
	binaryURL := fmt.Sprintf("%s/v%s/%s", releaseBaseURL, ver, artifact)
	checksumURL := binaryURL + ".sha256"

	fmt.Fprintf(os.Stderr, "go-delta-rs: downloading delta-server v%s for %s/%s...\n",
		ver, runtime.GOOS, runtime.GOARCH)

	// Download and verify in one streaming pass.
	if err := streamDownload(binaryURL, checksumURL, dest); err != nil {
		return "", fmt.Errorf("download delta-server: %w", err)
	}

	fmt.Fprintf(os.Stderr, "go-delta-rs: cached at %s\n", dest)
	return dest, nil
}

// streamDownload fetches binaryURL, streams it to dest while computing
// SHA-256, then compares against the checksum from checksumURL.
// Uses a .tmp file and atomic rename so a partial download is never left
// as a valid binary.
func streamDownload(binaryURL, checksumURL, dest string) error {
	// Fetch the checksum file first (small).
	expected, err := fetchChecksum(checksumURL)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return fmt.Errorf("create cache dir: %w", err)
	}

	tmp := dest + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}

	resp, err := http.Get(binaryURL) //nolint:noctx
	if err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("GET %s: %w", binaryURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("GET %s: HTTP %d", binaryURL, resp.StatusCode)
	}

	h := sha256.New()
	if _, err := io.Copy(io.MultiWriter(f, h), resp.Body); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("write binary: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}

	actual := hex.EncodeToString(h.Sum(nil))
	if actual != expected {
		os.Remove(tmp)
		return fmt.Errorf("checksum mismatch (expected %s, got %s) — release asset may be corrupt", expected, actual)
	}

	return os.Rename(tmp, dest)
}

// fetchChecksum downloads a .sha256 file and returns the hex digest string.
func fetchChecksum(url string) (string, error) {
	resp, err := http.Get(url) //nolint:noctx
	if err != nil {
		return "", fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GET %s: HTTP %d", url, resp.StatusCode)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 256))
	if err != nil {
		return "", err
	}
	// Format: "<hex>  <filename>\n" — take the first field.
	fields := strings.Fields(string(b))
	if len(fields) == 0 {
		return "", fmt.Errorf("empty checksum file at %s", url)
	}
	return fields[0], nil
}
