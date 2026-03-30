# go-delta-rs — Claude Code instructions

## Project overview

Go bindings for Delta Lake via a Rust gRPC sidecar.
- **Go package** (`deltago/`) — pure Go client, no CGo
- **Rust binary** (`delta-server/`) — wraps delta-rs + tonic gRPC server
- **Proto contract** (`proto/delta.proto`) — source of truth for the API

## Build commands

```bash
# Build the Rust sidecar (required before running tests or examples)
cd delta-server && cargo build --release

# Build Go
go build ./...

# Run all Go tests (unit only — no sidecar required)
go test ./deltago/...

# Run integration tests (requires built sidecar binary)
go test ./deltago/... -tags integration

# Regenerate proto stubs after editing proto/delta.proto
make generate

# Build everything
make all
```

## Key files

| File | Purpose |
|---|---|
| `proto/delta.proto` | Canonical API definition — edit this first when adding RPCs |
| `delta-server/src/service.rs` | All gRPC handler implementations in Rust |
| `delta-server/src/main.rs` | Server entry point, reads `$DELTA_SERVER_PORT` |
| `deltago/client.go` | Go `DeltaClient` — public API surface |
| `deltago/sidecar.go` | `Sidecar` lifecycle manager |
| `deltago/types.go` | Shared types (`Column`, `Row`, `WriteMode`, etc.) |
| `gen/go/delta/` | **Generated** — do not edit by hand |

## Conventions

### Adding a new RPC

1. Add the message types and `rpc` entry to `proto/delta.proto`
2. Run `make generate` to regenerate Go stubs
3. Implement the handler in `delta-server/src/service.rs`
4. Expose it in `deltago/client.go` with an idiomatic Go wrapper
5. Add a unit test in `deltago/client_test.go` (mock) and an integration test in `deltago/integration_test.go`

### Go style

- All public functions take `context.Context` as the first argument
- Errors are returned, never panicked
- `deltago.Row` is `map[string]any` — keep JSON serialisation in `types.go`
- Avoid adding new dependencies to the Go module without discussion

### Rust style

- All handlers are in `service.rs`; keep `main.rs` minimal
- Use the `internal()` helper to convert `anyhow`/`deltalake` errors to `tonic::Status`
- Log at `info` for normal operations, `warn` for recoverable issues, `error` for handler failures

### Proto changes

- Always keep `go_package` option set to `go-delta-rs/gen/go/delta`
- Column type strings must stay consistent between `proto/delta.proto`, `service.rs` (both Arrow and delta-rs mappings), and the README type table

## Dependency versions

Pinned for compatibility — do not bump independently:

| Crate | Version | Reason |
|---|---|---|
| `deltalake` | `0.22` | Arrow 53 alignment |
| `arrow` / `arrow-json` | `53` | Must match deltalake's transitive dep |
| `tonic` | `0.12` | prost 0.13 alignment |

To upgrade delta-rs, bump `deltalake` first and then align `arrow*` to whatever version delta-rs re-exports.

## Running tests

### Unit tests (no sidecar needed)
```bash
go test ./deltago/ -run TestUnit -v
```

### Integration tests (sidecar binary required)
```bash
cd delta-server && cargo build --release && cd ..
go test ./deltago/ -tags integration -v
```

Integration tests write to `/tmp/go-delta-rs-test-*` and clean up after themselves.

## Common issues

**`delta-server` binary not found** — run `cd delta-server && cargo build --release` first.

**Port conflict** — `SidecarOptions.Port: 0` (the default) picks a free port automatically.

**Schema inference** — when `Write` is called without an explicit `schema`, all columns are typed as `string`. Pass a schema to get correct numeric types.

**DataFusion SQL** — the `Filter` field in `ReadOptions` is a SQL `WHERE` clause evaluated by DataFusion. Column names must match the Delta table schema exactly (case-sensitive).
