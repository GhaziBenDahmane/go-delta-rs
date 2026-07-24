# go-delta-rs

A Go client for [Delta Lake](https://delta.io/), backed by
[delta-rs](https://github.com/delta-io/delta-rs) in a Rust sidecar.

The goal is to let Go services and jobs work with Delta tables without CGo, a
JVM, or a new implementation of the Delta transaction protocol. The Go package
manages the sidecar and exposes table operations over a local gRPC connection.

This project is pre-1.0. It is not a pure-Go implementation or a general SQL
engine.

## What it supports

- local files, S3-compatible storage, GCS, and Azure;
- table creation, append and overwrite writes;
- current and historical reads with filters and limits;
- idempotent writes and predicate deletes;
- history, vacuum, compaction, Z-order, and checkpoint maintenance;
- storage capability checks and sidecar memory controls.

It does not currently expose `MERGE`, row updates, schema evolution, change
data feed, or Arrow streaming.

## Install

Go 1.24 or newer is required.

```bash
go get github.com/ghazibendahmane/go-delta-rs/deltago@latest
```

On first use, the package downloads the matching `delta-server` release,
verifies its checksum, and caches it. Prebuilt binaries are available for Linux
and macOS on amd64 and arm64, and Windows on amd64.

Set `DELTA_SERVER_PATH` or `SidecarOptions.BinaryPath` to provide the binary
yourself.

## Example

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/ghazibendahmane/go-delta-rs/deltago"
)

func main() {
	ctx := context.Background()
	sidecar := deltago.NewSidecar(deltago.SidecarOptions{})
	if err := sidecar.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() { _ = sidecar.Stop() }()

	client := sidecar.Client()
	uri := "file:///tmp/go-delta-rs-events"
	schema := []deltago.Column{
		{Name: "id", Type: "int64", Nullable: false},
		{Name: "name", Type: "string", Nullable: false},
		{Name: "score", Type: "float64", Nullable: true},
	}

	if _, err := client.EnsureTable(ctx, uri, schema, nil); err != nil {
		log.Fatal(err)
	}
	rows := []deltago.Row{
		{"id": 1, "name": "alice", "score": 9.5},
		{"id": 2, "name": "bob", "score": 7.0},
	}
	if err := client.Write(ctx, uri, deltago.WriteOverwrite, rows, schema); err != nil {
		log.Fatal(err)
	}

	result, err := client.Read(ctx, uri, &deltago.ReadOptions{
		Filter: "score >= 9",
		Limit:  100,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(result)
}
```

## S3-compatible storage

The sidecar uses the standard AWS credential chain. Configure custom endpoints
through `StorageConfig`:

```go
sidecar := deltago.NewSidecar(deltago.SidecarOptions{
	Storage: deltago.StorageConfig{
		S3Endpoint:        "http://localhost:9000",
		S3AllowHTTP:       true,
		S3ForcePathStyle:  true,
		S3Region:          "us-east-1",
		S3CommitMode:      deltago.S3CommitModeConditionalPutETag,
	},
})
```

Choose a commit mode supported by the store. Use
`CheckStorageCapabilities` before enabling concurrent writers. Unsafe rename is
only suitable as a last resort for single-writer workloads.

For retryable writes, use `WriteResult` with a stable
`AppTransactionID` and a monotonically increasing `AppTransactionVersion`.

## Limits

Rows are `map[string]any` values sent as JSON over gRPC. Messages are limited to
256 MiB, so filter reads and batch large writes. Supported column types are
string, 32- and 64-bit integers and floats, boolean, timestamp, and date.

The externally run sidecar has no built-in authentication or TLS. Do not expose
its port to an untrusted network.

## Build and test

Building the sidecar requires Rust. Regenerating gRPC code requires `protoc`.

```bash
make build-go
make build-server
make test-unit
make test-integration
make generate
```

The protobuf contract is [proto/delta.proto](proto/delta.proto). A complete
local example is in [example/main.go](example/main.go).

## License

MIT. See [LICENSE](LICENSE).
