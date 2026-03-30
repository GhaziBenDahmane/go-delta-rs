PROTO_DIR  := proto
GEN_DIR    := gen/go/delta
BINARY     := delta-server/target/release/delta-server
GOPATH_BIN := $(shell go env GOPATH)/bin

.PHONY: all build-server build-go generate run-example clean

all: generate build-server build-go

## Regenerate Go protobuf/gRPC stubs from proto/delta.proto
generate:
	@which protoc-gen-go       >/dev/null 2>&1 || go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	@which protoc-gen-go-grpc  >/dev/null 2>&1 || go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	PATH="$$PATH:$(GOPATH_BIN)" protoc \
		--go_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_out=. \
		--go-grpc_opt=paths=source_relative \
		--proto_path=$(PROTO_DIR) \
		$(PROTO_DIR)/delta.proto
	mkdir -p $(GEN_DIR)
	mv delta.pb.go $(GEN_DIR)/
	mv delta_grpc.pb.go $(GEN_DIR)/

## Build the Rust delta-server binary (release mode)
build-server:
	cd delta-server && cargo build --release

## Build the Go packages
build-go:
	go build ./...

## Run unit tests (no sidecar required)
test-unit:
	go test ./deltago/ -v -race

## Run integration tests (requires built sidecar binary)
test-integration: build-server
	go test ./deltago/ -tags integration -v -timeout 120s

## Run all tests
test: test-unit test-integration

## Run the example against a local filesystem table
run-example: build-server build-go
	go run ./example/main.go file:///tmp/my-delta-table

## Remove build artefacts
clean:
	cd delta-server && cargo clean
	go clean ./...
