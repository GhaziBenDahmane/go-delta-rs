// Command setup downloads and caches the delta-server binary for the current platform.
//
// Run this during your Docker build to ensure the binary is available at container
// startup without any network access:
//
//	RUN go run github.com/ghazibendahmane/go-delta-rs/cmd/setup@v0.1.0
//
// The binary is written to the OS cache directory and reused on subsequent runs.
// Exit code is non-zero if the download or checksum verification fails.
package main

import (
	"fmt"
	"os"

	"github.com/ghazibendahmane/go-delta-rs/deltago"
)

func main() {
	path, err := deltago.EnsureBinary()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(path)
}
