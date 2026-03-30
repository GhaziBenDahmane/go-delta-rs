# go-delta-rs

Go bindings for [Delta Lake](https://delta.io) powered by [delta-rs](https://github.com/delta-io/delta-rs), using a gRPC sidecar.

```go
sidecar := deltago.NewSidecar(deltago.SidecarOptions{
    BinaryPath: "./delta-server/target/release/delta-server",
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

**delta-rs is the reference implementation.** Maintained by the Linux Foundation under the Delta.io project, delta-rs is the non-JVM reference implementation of Delta Lake. It powers the official Python, Rust, and (partially) Java bindings. It handles the full protocol, including writes with ACID semantics, time travel, vacuuming, and optimisation. By wrapping delta-rs rather than reimplementing Delta Lake in Go, this project gets correctness for free and automatically inherits upstream improvements.

---

## Comparison to alternatives

### 1. Pure Go reimplementations (`go-delta`, etc.)

Several projects have attempted a pure Go Delta Lake implementation. The pattern is consistent: they support basic reads on simple tables, then stall when they hit the full complexity of the protocol — checkpoint files, schema evolution, `MERGE` operations, deletion vectors. As of 2025 none are production-ready for writes.

| | go-delta-rs | Pure Go |
|---|---|---|
| Full Delta Protocol | ✅ (via delta-rs) | ⚠ Partial |
| Writes / ACID | ✅ | ❌ |
| Time travel | ✅ | ❌ |
| Actively maintained | ✅ | ❌ Most abandoned |
| Pure Go client | ✅ | ✅ |

### 2. CGo bindings to delta-rs

The conceptually obvious approach: compile delta-rs as a C shared library (`cdylib`), expose a C ABI, and call it from Go via `cgo`. This works in a proof-of-concept but breaks down in practice:

- **Broken cross-compilation.** `CGO_ENABLED=1` disables `GOOS`/`GOARCH` cross-compilation. You cannot `go build` for `linux/arm64` from a Mac without a full cross-compilation toolchain.
- **CGo overhead.** Every call across the CGo boundary transitions from a goroutine to an OS thread, adding latency and bypassing Go's scheduler. High-frequency calls are measurably slower.
- **Deployment complexity.** The shared library (`.so` / `.dylib`) must be present at runtime. Statically linking it into the Go binary is possible but fragile across glibc versions.
- **Memory safety.** Memory allocated in Rust must be freed by Rust, and vice versa. Ownership across a C ABI is manual and error-prone.
- **Build friction.** `go get` stops working out of the box. Users need a Rust toolchain at build time *and* a matching shared library at run time.

| | go-delta-rs | CGo bindings |
|---|---|---|
| Pure Go client | ✅ | ❌ |
| Cross-compilation | ✅ | ❌ |
| `go get` works | ✅ | ❌ |
| Process isolation | ✅ | ❌ |
| Deployment | Single binary + sidecar | Shared lib dependency |

### 3. JVM (delta-io/delta, delta-rs Java bindings)

The official Apache Spark Delta Lake implementation is JVM-only. delta-rs also ships Java bindings. Both require a JVM at runtime — not acceptable for most Go infrastructure tooling, CLI utilities, or serverless functions.

### 4. Python subprocess

Some teams run a Python process that uses the `deltalake` Python package and communicate over stdin/stdout or a Unix socket. This requires a Python runtime and a virtualenv wherever the Go binary is deployed. Per-call process spawning is prohibitively slow; a persistent subprocess is effectively reinventing the sidecar pattern with worse ergonomics.

### 5. Direct Parquet reading

Libraries like `github.com/xitongsys/parquet-go` or the Apache Arrow Go library let you read `.parquet` files directly. This deliberately ignores the Delta transaction log and therefore provides no ACID guarantees, no correct handling of deletes or updates, and stale reads when another writer is active. Acceptable for one-off data exploration; not acceptable for production pipelines.

### Summary

| Approach | Writes | ACID | Pure Go client | No JVM | Maintained |
|---|:---:|:---:|:---:|:---:|:---:|
| **go-delta-rs (this)** | ✅ | ✅ | ✅ | ✅ | ✅ |
| Pure Go reimpl | ❌ | ❌ | ✅ | ✅ | ❌ |
| CGo bindings | ✅ | ✅ | ❌ | ✅ | ❌ |
| JVM / Spark | ✅ | ✅ | ❌ | ❌ | ✅ |
| Python subprocess | ✅ | ✅ | ❌ | ✅ | ✅ |
| Direct Parquet | ❌ | ❌ | ✅ | ✅ | ✅ |

---

## Prerequisites

**To build the sidecar:**
- [Rust](https://rustup.rs) 1.75 or later
- `cargo` (included with Rust)

**To use the Go package:**
- Go 1.21 or later

**To regenerate protobuf stubs** (only needed when modifying `proto/delta.proto`):
- `protoc` (Protocol Buffers compiler)
- `protoc-gen-go` and `protoc-gen-go-grpc` (installed automatically by `make generate`)

---

## Building

### 1. Build the Rust sidecar

```bash
cd delta-server
cargo build --release
```

The binary is written to `delta-server/target/release/delta-server`.

This is a one-time step. The resulting binary is self-contained and has no shared library dependencies beyond libc.

### 2. Get the Go package

```bash
go get github.com/YOUR_USERNAME/go-delta-rs/deltago
```

Or clone the repo and use a `replace` directive in your `go.mod` during development.

### 3. (Optional) Regenerate protobuf stubs

Only needed if you change `proto/delta.proto`:

```bash
make generate
```

---

## Usage

### Managed sidecar (recommended)

The `Sidecar` type launches and manages the `delta-server` process for you.

```go
package main

import (
    "context"
    "log"

    "github.com/YOUR_USERNAME/go-delta-rs/deltago"
)

func main() {
    ctx := context.Background()

    sidecar := deltago.NewSidecar(deltago.SidecarOptions{
        BinaryPath: "./delta-server/target/release/delta-server",
        // Port: 50051  // omit to pick a free port automatically
    })
    if err := sidecar.Start(ctx); err != nil {
        log.Fatal(err)
    }
    defer sidecar.Stop()

    c := sidecar.Client()

    // --- Create a table ---
    schema := []deltago.Column{
        {Name: "id",    Type: "int64",   Nullable: false},
        {Name: "email", Type: "string",  Nullable: true},
        {Name: "score", Type: "float64", Nullable: true},
    }
    err := c.CreateTable(ctx, "file:///tmp/users", schema, nil)

    // --- Write rows ---
    rows := []deltago.Row{
        {"id": 1, "email": "alice@example.com", "score": 9.5},
        {"id": 2, "email": "bob@example.com",   "score": 7.2},
    }
    err = c.Write(ctx, "file:///tmp/users", deltago.WriteAppend, rows, schema)

    // --- Read all rows ---
    all, err := c.Read(ctx, "file:///tmp/users", nil)

    // --- Filter with a SQL predicate ---
    high, err := c.Read(ctx, "file:///tmp/users", &deltago.ReadOptions{
        Filter: "score > 8.0",
        Limit:  100,
    })

    // --- Read a specific version (time travel) ---
    old, err := c.ReadAtVersion(ctx, "file:///tmp/users", 0)

    // --- Schema and metadata ---
    info, err := c.GetTableInfo(ctx, "file:///tmp/users")
    fmt.Printf("version=%d  files=%d\n", info.Version, info.NumFiles)

    // --- Commit history ---
    history, err := c.History(ctx, "file:///tmp/users", 10)

    // --- Vacuum (remove old files) ---
    deleted, err := c.Vacuum(ctx, "file:///tmp/users", 168, false)
}
```

### External sidecar

If you want to run `delta-server` as a separate container or system service, connect directly:

```go
import (
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
    deltapb "github.com/YOUR_USERNAME/go-delta-rs/gen/go/delta"
    "github.com/YOUR_USERNAME/go-delta-rs/deltago"
)

conn, _ := grpc.NewClient("sidecar:50051",
    grpc.WithTransportCredentials(insecure.NewCredentials()))
c := deltago.NewDeltaClient(deltapb.NewDeltaServiceClient(conn))
```

Start the server manually:

```bash
DELTA_SERVER_PORT=50051 ./delta-server/target/release/delta-server
```

### Storage backends

Supported URIs follow the delta-rs conventions:

| Storage | URI format |
|---|---|
| Local filesystem | `file:///absolute/path/to/table` |
| Amazon S3 | `s3://bucket/prefix` |
| Google Cloud Storage | `gs://bucket/prefix` |
| Azure Data Lake | `az://container/path` |

Cloud credentials are read from the environment using the standard SDK conventions for each provider (AWS credential chain, `GOOGLE_APPLICATION_CREDENTIALS`, Azure environment variables).

---

## Column types

The following type strings are accepted in `Column.Type` and `ColumnDef.data_type`:

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

Data is serialised as JSON over gRPC. This is intentional — it keeps the Go client dependency-free (no Arrow/Parquet in Go) and makes the protocol easy to inspect and debug. For workloads moving very large result sets, a future release will add an Arrow IPC streaming mode.

---

## Project structure

```
go-delta-rs/
├── proto/delta.proto          # gRPC service definition
├── delta-server/              # Rust gRPC sidecar
│   ├── Cargo.toml
│   ├── build.rs
│   └── src/
│       ├── main.rs            # server entry point
│       └── service.rs         # RPC handlers
├── deltago/                   # Go client package
│   ├── sidecar.go             # process lifecycle
│   ├── client.go              # DeltaClient API
│   └── types.go               # Column, Row, WriteMode, …
├── gen/go/delta/              # generated protobuf stubs
│   ├── delta.pb.go
│   └── delta_grpc.pb.go
├── example/main.go
└── Makefile
```

---

## Makefile targets

```bash
make generate      # regenerate Go protobuf stubs from proto/delta.proto
make build-server  # cargo build --release
make build-go      # go build ./...
make all           # generate + build-server + build-go
make run-example   # build everything and run example/main.go
make clean         # remove all build artefacts
```

---

## Contributing

Contributions are welcome. Please open an issue before starting significant work so we can discuss the approach.

- **Bug reports:** include the delta-rs version, Go version, and a minimal reproducer.
- **New RPC operations:** add to `proto/delta.proto` first, then implement in `service.rs`, then expose in `client.go`.
- **Arrow IPC streaming mode:** tracked in [#TODO] — a high-value contribution for bulk read performance.

---

## License

MIT. See [LICENSE](LICENSE).

delta-rs is licensed under Apache 2.0. Apache Arrow (Rust) and Apache DataFusion are licensed under Apache 2.0.
