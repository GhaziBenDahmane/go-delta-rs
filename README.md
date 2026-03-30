# go-delta-rs

Go bindings for [Delta Lake](https://delta.io) powered by [delta-rs](https://github.com/delta-io/delta-rs), using a gRPC sidecar.

```go
import "github.com/ghazibendahmane/go-delta-rs/deltago"

sidecar := deltago.NewSidecar(deltago.SidecarOptions{
    BinaryPath: "./delta-server",
})
sidecar.Start(ctx)
defer sidecar.Stop()

c := sidecar.Client()
c.CreateTable(ctx, "s3://my-bucket/events", schema, []string{"date"})
c.Write(ctx, "s3://my-bucket/events", deltago.WriteAppend, rows, schema)
rows, _ := c.Read(ctx, "s3://my-bucket/events", &deltago.ReadOptions{Filter: "user_id = 42"})
```

---

## Why this exists

Go is the dominant language for data infrastructure — stream processors, API servers, CDC pipelines, Kubernetes operators. Delta Lake is the dominant open table format. Yet there is no reliable, production-grade way to read and write Delta tables from Go.

This project fills that gap.

### The problems it solves

**Delta Lake is not just Parquet.** A Delta table is a set of Parquet files governed by a JSON transaction log (`_delta_log/`). Reading `.parquet` files directly gives you stale, potentially corrupt data — no snapshot isolation, no handling of concurrent writes, no awareness of deletions or updates. Any correct Delta reader must implement the full [Delta Protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md), which covers checkpoint files, schema evolution, partition pruning, and more. Implementing this correctly from scratch in Go is a substantial, ongoing engineering effort.

**delta-rs is the reference implementation.** Maintained by the Linux Foundation under the Delta.io project, delta-rs is the non-JVM reference implementation of Delta Lake. It powers the official Python, Rust, and Java bindings. It handles the full protocol, including writes with ACID semantics, time travel, vacuuming, and optimisation. By wrapping delta-rs rather than reimplementing Delta Lake in Go, this project gets correctness for free and automatically inherits upstream improvements.

---

## Comparison to alternatives

### 1. Pure Go reimplementations

Several projects have attempted a pure Go Delta Lake implementation. The pattern is consistent: they support basic reads on simple tables, then stall when they hit the full complexity of the protocol — checkpoint files, schema evolution, `MERGE` operations, deletion vectors. As of 2025 none are production-ready for writes.

| | go-delta-rs | Pure Go |
|---|---|---|
| Full Delta Protocol | ✅ (via delta-rs) | ⚠ Partial |
| Writes / ACID | ✅ | ❌ |
| Time travel | ✅ | ❌ |
| Actively maintained | ✅ | ❌ Most abandoned |
| Pure Go client | ✅ | ✅ |

### 2. DuckDB (`go-duckdb` + Delta extension)

DuckDB has a Delta extension that can read Delta tables, and [go-duckdb](https://github.com/marcboeker/go-duckdb) provides Go bindings. This is a viable read path for analytics but has significant limitations:

- **Read-only for Delta.** DuckDB's Delta extension supports `delta_scan()` for reading but does not write data back as a Delta table.
- **CGo dependency.** `go-duckdb` is a CGo binding, which breaks cross-compilation and adds deployment complexity.
- **Not Delta-native.** DuckDB operates on a copy of the data; it has no awareness of Delta transaction semantics when other writers are active.

| | go-delta-rs | DuckDB |
|---|---|---|
| Delta writes | ✅ | ❌ Read-only |
| ACID guarantees | ✅ | ❌ |
| Pure Go client | ✅ | ❌ CGo |
| Cross-compilation | ✅ | ❌ |
| Good for ad-hoc queries | ⚠ SQL via DataFusion | ✅ |

### 3. CGo bindings to delta-rs

The conceptually obvious approach: compile delta-rs as a C shared library and call it from Go via `cgo`. This works in a proof-of-concept but breaks down in practice:

- **Broken cross-compilation.** `CGO_ENABLED=1` disables `GOOS`/`GOARCH` cross-compilation.
- **CGo overhead.** Every call transitions from a goroutine to an OS thread, adding latency and bypassing Go's scheduler.
- **Deployment complexity.** The shared library must be present at runtime. Statically linking it is possible but fragile across glibc versions.
- **Memory safety.** Ownership across a C ABI is manual and error-prone.

| | go-delta-rs | CGo bindings |
|---|---|---|
| Pure Go client | ✅ | ❌ |
| Cross-compilation | ✅ | ❌ |
| `go get` works | ✅ | ❌ |
| Process isolation | ✅ | ❌ |

### 4. JVM (delta-io/delta, delta-rs Java bindings)

Both the official Spark Delta and delta-rs Java bindings require a JVM at runtime — not acceptable for most Go infrastructure tooling, CLI utilities, or serverless functions.

### 5. Python subprocess

Some teams run a Python process using the `deltalake` Python package and communicate over stdin/stdout or a Unix socket. This requires a Python runtime and virtualenv everywhere the Go binary is deployed.

### 6. Direct Parquet reading

Libraries like `github.com/xitongsys/parquet-go` or the Apache Arrow Go library let you read `.parquet` files directly. This deliberately ignores the Delta transaction log — no ACID guarantees, no correct handling of deletes or updates, stale reads during writes.

### Summary

| Approach | Delta writes | ACID | Pure Go client | No JVM | Maintained |
|---|:---:|:---:|:---:|:---:|:---:|
| **go-delta-rs (this)** | ✅ | ✅ | ✅ | ✅ | ✅ |
| DuckDB (go-duckdb) | ❌ read-only | ❌ | ❌ CGo | ✅ | ✅ |
| Pure Go reimpl | ❌ | ❌ | ✅ | ✅ | ❌ |
| CGo bindings | ✅ | ✅ | ❌ | ✅ | ❌ |
| JVM / Spark | ✅ | ✅ | ❌ | ❌ | ✅ |
| Python subprocess | ✅ | ✅ | ❌ | ✅ | ✅ |
| Direct Parquet | ❌ | ❌ | ✅ | ✅ | ✅ |

---

## Installation

### 1. Download the sidecar binary

```bash
VERSION=v0.1.0 curl -fsSL \
  https://raw.githubusercontent.com/ghazibendahmane/go-delta-rs/main/install.sh | bash
```

Or download manually from the [releases page](https://github.com/ghazibendahmane/go-delta-rs/releases) and make it executable:

```bash
# Linux amd64
curl -L https://github.com/ghazibendahmane/go-delta-rs/releases/latest/download/delta-server-linux-amd64 \
  -o delta-server && chmod +x delta-server

# macOS Apple Silicon
curl -L https://github.com/ghazibendahmane/go-delta-rs/releases/latest/download/delta-server-darwin-arm64 \
  -o delta-server && chmod +x delta-server
```

### 2. Add the Go package

```bash
go get github.com/ghazibendahmane/go-delta-rs/deltago
```

### 3. (Optional) Build from source

Requires a [Rust toolchain](https://rustup.rs):

```bash
git clone https://github.com/ghazibendahmane/go-delta-rs
cd go-delta-rs/delta-server && cargo build --release
```

---

## Usage

### Basic example

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

    sidecar := deltago.NewSidecar(deltago.SidecarOptions{
        BinaryPath: "./delta-server",
    })
    if err := sidecar.Start(ctx); err != nil {
        log.Fatal(err)
    }
    defer sidecar.Stop()

    c := sidecar.Client()

    schema := []deltago.Column{
        {Name: "id",    Type: "int64",   Nullable: false},
        {Name: "email", Type: "string",  Nullable: true},
        {Name: "score", Type: "float64", Nullable: true},
    }

    // Create
    c.CreateTable(ctx, "file:///tmp/users", schema, nil)

    // Write
    rows := []deltago.Row{
        {"id": 1, "email": "alice@example.com", "score": 9.5},
        {"id": 2, "email": "bob@example.com",   "score": 7.2},
    }
    c.Write(ctx, "file:///tmp/users", deltago.WriteAppend, rows, schema)

    // Read all
    all, _ := c.Read(ctx, "file:///tmp/users", nil)

    // Filter
    high, _ := c.Read(ctx, "file:///tmp/users", &deltago.ReadOptions{
        Filter: "score > 8.0",
        Limit:  100,
    })

    // Time travel
    old, _ := c.ReadAtVersion(ctx, "file:///tmp/users", 0)

    // Metadata
    info, _ := c.GetTableInfo(ctx, "file:///tmp/users")
    fmt.Printf("version=%d  files=%d\n", info.Version, info.NumFiles)

    // History
    history, _ := c.History(ctx, "file:///tmp/users", 10)
    for _, commit := range history {
        fmt.Printf("v%d  %s  %s\n", commit.Version, commit.Timestamp, commit.Operation)
    }

    // Vacuum
    c.Vacuum(ctx, "file:///tmp/users", 168, false)
}
```

### Amazon S3

```go
sidecar := deltago.NewSidecar(deltago.SidecarOptions{
    BinaryPath: "./delta-server",
    // Credentials fall back to the standard AWS credential chain
    // (env vars, ~/.aws/credentials, EC2 instance role, ECS task role, etc.)
})
sidecar.Start(ctx)
c := sidecar.Client()
c.Write(ctx, "s3://my-bucket/events", deltago.WriteAppend, rows, schema)
```

### S3-compatible storage (MinIO, Localstack, Tigris, Ceph, …)

```go
sidecar := deltago.NewSidecar(deltago.SidecarOptions{
    BinaryPath: "./delta-server",
    Storage: deltago.StorageConfig{
        S3Endpoint:        "http://localhost:9000",  // MinIO
        S3AccessKeyID:     "minioadmin",
        S3SecretAccessKey: "minioadmin",
        S3Region:          "us-east-1",
        S3AllowHTTP:       true, // required when TLS is not configured
        S3ForcePathStyle:  true, // required for MinIO and most self-hosted stores
    },
})
sidecar.Start(ctx)
c := sidecar.Client()
c.Write(ctx, "s3://my-bucket/events", deltago.WriteAppend, rows, schema)
```

### Google Cloud Storage

```go
// Credentials read from GOOGLE_APPLICATION_CREDENTIALS or the GCE metadata server.
c.Write(ctx, "gs://my-bucket/events", deltago.WriteAppend, rows, schema)
```

### Azure Data Lake Storage

```go
// Credentials read from AZURE_STORAGE_ACCOUNT_NAME + AZURE_STORAGE_ACCOUNT_KEY,
// or from the Azure DefaultCredential chain.
c.Write(ctx, "az://my-container/events", deltago.WriteAppend, rows, schema)
```

### External sidecar (separate container / service)

```go
import (
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
    deltapb "github.com/ghazibendahmane/go-delta-rs/gen/go/delta"
    "github.com/ghazibendahmane/go-delta-rs/deltago"
)

conn, _ := grpc.NewClient("sidecar:50051",
    grpc.WithTransportCredentials(insecure.NewCredentials()))
c := deltago.NewDeltaClient(deltapb.NewDeltaServiceClient(conn))
```

---

## Column types

| Type string | Arrow type | Delta primitive |
|---|---|---|
| `string` | `Utf8` | `string` |
| `int32` / `integer` | `Int32` | `integer` |
| `int64` / `long` | `Int64` | `long` |
| `float32` / `float` | `Float32` | `float` |
| `float64` / `double` | `Float64` | `double` |
| `boolean` / `bool` | `Boolean` | `boolean` |
| `timestamp` | `Timestamp(µs)` | `timestamp` |
| `date` | `Date32` | `date` |

---

## Architecture

```
┌──────────────────────────────────┐
│  Your Go application             │
│                                  │
│  deltago.Sidecar                 │  manages process lifecycle
│  deltago.DeltaClient             │  ergonomic Go API
│      │                           │
│      │ gRPC (HTTP/2)             │
│      ▼                           │
│  delta-server (Rust binary)      │
│  ├── tonic gRPC server           │
│  ├── delta-rs (DeltaOps)         │  ACID writes, time travel
│  └── Apache DataFusion           │  SQL predicate pushdown
└──────────────────────────────────┘
         │
         ▼
   Storage (local / S3 / GCS / Azure)
```

Data is serialised as JSON over gRPC. This keeps the Go client dependency-free (no Arrow or Parquet in Go) and makes the protocol easy to inspect and debug. An Arrow IPC streaming mode for bulk reads is planned.

---

## Project structure

```
go-delta-rs/
├── proto/delta.proto          # gRPC service definition (source of truth)
├── delta-server/              # Rust gRPC sidecar
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs            # server entry point ($DELTA_SERVER_PORT)
│       └── service.rs         # RPC handlers
├── deltago/                   # Go client package
│   ├── doc.go
│   ├── sidecar.go             # Sidecar + StorageConfig
│   ├── client.go              # DeltaClient API
│   └── types.go               # Column, Row, WriteMode, …
├── gen/go/delta/              # generated protobuf stubs (committed)
├── example/main.go
└── Makefile
```

---

## Building from source

```bash
make build-server   # cargo build --release
make build-go       # go build ./...
make test           # unit + integration tests
make generate       # regenerate proto stubs (requires protoc)
```

---

## Contributing

Contributions are welcome. Please open an issue before starting significant work.

- **Bug reports:** include the delta-rs version, Go version, storage backend, and a minimal reproducer.
- **New RPC operations:** add to `proto/delta.proto` first, implement in `service.rs`, expose in `client.go`, add tests.
- **Arrow IPC streaming:** tracked as a future milestone — high-value contribution for bulk read performance.

---

## License

MIT. See [LICENSE](LICENSE).

delta-rs is licensed under Apache 2.0. Apache Arrow and Apache DataFusion are licensed under Apache 2.0.
