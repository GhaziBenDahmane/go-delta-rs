package deltago

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ── MarshalRows / UnmarshalRows ───────────────────────────────────────────────

func TestMarshalRows_empty(t *testing.T) {
	s, err := MarshalRows(nil)
	if err != nil {
		t.Fatal(err)
	}
	if s != "null" {
		t.Errorf("expected null, got %q", s)
	}
}

func TestMarshalRows_roundtrip(t *testing.T) {
	input := []Row{
		{"id": float64(1), "name": "alice", "active": true},
		{"id": float64(2), "name": "bob", "active": false},
	}
	s, err := MarshalRows(input)
	if err != nil {
		t.Fatal(err)
	}

	got, err := UnmarshalRows(s)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(input) {
		t.Fatalf("expected %d rows, got %d", len(input), len(got))
	}
	for i, row := range input {
		for k, v := range row {
			if got[i][k] != v {
				t.Errorf("row[%d][%q]: expected %v, got %v", i, k, v, got[i][k])
			}
		}
	}
}

func TestUnmarshalRows_invalidJSON(t *testing.T) {
	_, err := UnmarshalRows("not json")
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestUnmarshalRows_emptyArray(t *testing.T) {
	rows, err := UnmarshalRows("[]")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(rows))
	}
}

func TestUnmarshalRows_nullValues(t *testing.T) {
	rows, err := UnmarshalRows(`[{"id":1,"name":null}]`)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row")
	}
	if rows[0]["name"] != nil {
		t.Errorf("expected null name, got %v", rows[0]["name"])
	}
}

// ── toProtoCols / fromProtoCols ───────────────────────────────────────────────

func TestToProtoCols_nil(t *testing.T) {
	if toProtoCols(nil) != nil {
		t.Error("expected nil for nil input")
	}
}

func TestToProtoCols_roundtrip(t *testing.T) {
	cols := []Column{
		{Name: "id", Type: "int64", Nullable: false},
		{Name: "label", Type: "string", Nullable: true},
		{Name: "score", Type: "float64", Nullable: true},
	}
	proto := toProtoCols(cols)
	if len(proto) != len(cols) {
		t.Fatalf("expected %d cols, got %d", len(cols), len(proto))
	}
	back := fromProtoCols(proto)
	for i, c := range cols {
		if back[i].Name != c.Name || back[i].Type != c.Type || back[i].Nullable != c.Nullable {
			t.Errorf("col[%d] mismatch: want %+v, got %+v", i, c, back[i])
		}
	}
}

func TestFromProtoCols_empty(t *testing.T) {
	out := fromProtoCols(nil)
	if len(out) != 0 {
		t.Errorf("expected empty slice for nil input")
	}
}

// ── Column type coverage ──────────────────────────────────────────────────────

func TestToProtoCols_allTypes(t *testing.T) {
	types := []string{
		"string", "int32", "int64", "float32", "float64",
		"boolean", "timestamp", "date",
	}
	cols := make([]Column, len(types))
	for i, typ := range types {
		cols[i] = Column{Name: "col_" + typ, Type: typ, Nullable: true}
	}
	proto := toProtoCols(cols)
	if len(proto) != len(types) {
		t.Fatalf("expected %d proto cols", len(types))
	}
	for i, p := range proto {
		if p.DataType != types[i] {
			t.Errorf("col[%d]: expected type %q, got %q", i, types[i], p.DataType)
		}
	}
}

// ── WriteMode constants ───────────────────────────────────────────────────────

func TestWriteMode_values(t *testing.T) {
	if WriteAppend != "append" {
		t.Errorf("WriteAppend = %q, want \"append\"", WriteAppend)
	}
	if WriteOverwrite != "overwrite" {
		t.Errorf("WriteOverwrite = %q, want \"overwrite\"", WriteOverwrite)
	}
}

// ── Row JSON compatibility ────────────────────────────────────────────────────

func TestRow_jsonCompatibility(t *testing.T) {
	// Verify that Row (map[string]any) marshals to a JSON object, not array.
	row := Row{"key": "value", "num": 42}
	b, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	var check map[string]any
	if err := json.Unmarshal(b, &check); err != nil {
		t.Fatalf("row did not marshal to a JSON object: %v", err)
	}
}

func TestMarshalRows_producesArray(t *testing.T) {
	rows := []Row{{"x": 1}, {"x": 2}}
	s, err := MarshalRows(rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(s) == 0 || s[0] != '[' {
		t.Errorf("MarshalRows should produce a JSON array, got %q", s)
	}
}

// ── ReadOptions zero value ────────────────────────────────────────────────────

func TestReadOptions_zeroValue(t *testing.T) {
	// A zero-value ReadOptions should be safe to pass (no panics on field access).
	opts := &ReadOptions{}
	_ = opts.Version
	_ = opts.Filter
	_ = opts.Limit
}

// ── StorageConfig validation / env mapping ───────────────────────────────────

func TestStorageConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  StorageConfig
		wantErr bool
	}{
		{name: "empty config is valid"},
		{name: "etag conditional put is valid", config: StorageConfig{S3ConditionalPut: "etag"}},
		{name: "commit mode conditional put is valid", config: StorageConfig{S3CommitMode: S3CommitModeConditionalPutETag}},
		{name: "commit mode unsafe rename is valid", config: StorageConfig{S3CommitMode: S3CommitModeUnsafeRename}},
		{name: "commit mode dynamo is valid", config: StorageConfig{S3CommitMode: "dynamo:DeltaLocks"}},
		{name: "dynamo conditional put is valid", config: StorageConfig{S3ConditionalPut: "dynamo:delta-locks"}},
		{name: "multipart copy if not exists is valid", config: StorageConfig{S3CopyIfNotExists: "multipart"}},
		{name: "header copy if not exists is valid", config: StorageConfig{S3CopyIfNotExists: "header:if-none-match:*"}},
		{name: "sha256 checksum is valid", config: StorageConfig{S3ChecksumAlgorithm: "sha256"}},
		{name: "unsafe rename cannot combine with conditional put", config: StorageConfig{S3AllowUnsafeRename: true, S3ConditionalPut: "etag"}, wantErr: true},
		{name: "unsafe rename cannot combine with copy if not exists", config: StorageConfig{S3AllowUnsafeRename: true, S3CopyIfNotExists: "multipart"}, wantErr: true},
		{name: "invalid conditional put", config: StorageConfig{S3ConditionalPut: "if-none-match"}, wantErr: true},
		{name: "invalid copy if not exists", config: StorageConfig{S3CopyIfNotExists: "copy"}, wantErr: true},
		{name: "invalid checksum", config: StorageConfig{S3ChecksumAlgorithm: "md5"}, wantErr: true},
		{name: "commit mode cannot combine with explicit conditional put", config: StorageConfig{S3CommitMode: S3CommitModeConditionalPutETag, S3ConditionalPut: "etag"}, wantErr: true},
		{name: "invalid commit mode", config: StorageConfig{S3CommitMode: "magic"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr && err == nil {
				t.Fatal("expected validation error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected validation error: %v", err)
			}
		})
	}
}

func TestStorageEnv_includesConditionalCommitSettings(t *testing.T) {
	env := storageEnv(StorageConfig{
		S3CommitMode:        "dynamo:DeltaLocks",
		S3ChecksumAlgorithm: "sha256",
	})
	got := map[string]string{}
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			got[key] = value
		}
	}

	for key, want := range map[string]string{
		"AWS_CONDITIONAL_PUT":       "dynamo:DeltaLocks",
		"AWS_S3_CONDITIONAL_PUT":    "dynamo:DeltaLocks",
		"AWS_COPY_IF_NOT_EXISTS":    "dynamo:DeltaLocks",
		"AWS_S3_COPY_IF_NOT_EXISTS": "dynamo:DeltaLocks",
		"AWS_CHECKSUM_ALGORITHM":    "sha256",
	} {
		if got[key] != want {
			t.Fatalf("%s = %q, want %q", key, got[key], want)
		}
	}
}

func TestRuntimeEnv_includesMemorySettings(t *testing.T) {
	env := runtimeEnv(RuntimeConfig{
		Profile:       RuntimeProfileMinimumMemory,
		TableCacheTTL: 30 * time.Second,
	})
	got := map[string]string{}
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			got[key] = value
		}
	}

	for key, want := range map[string]string{
		"DELTA_TABLE_CACHE_MAX_ENTRIES":       "0",
		"DELTA_TABLE_CACHE_TTL_SECONDS":       "30",
		"DELTA_MEMORY_PURGE_INTERVAL_SECONDS": "30",
	} {
		if got[key] != want {
			t.Fatalf("%s = %q, want %q", key, got[key], want)
		}
	}
	mallocConf := got["MALLOC_CONF"]
	for _, fragment := range []string{
		"background_thread:true",
		"dirty_decay_ms:1000",
		"muzzy_decay_ms:1000",
	} {
		if !strings.Contains(mallocConf, fragment) {
			t.Fatalf("MALLOC_CONF = %q, missing %q", mallocConf, fragment)
		}
	}
}

// ── Row alignment / coercion ─────────────────────────────────────────────────

func TestAlignRowToSchema_dropsExtrasAndFillsMissing(t *testing.T) {
	row := Row{
		"id":     float64(42),
		"name":   123,
		"active": true,
		"extra":  "ignored",
	}
	schema := []Column{
		{Name: "id", Type: "int64", Nullable: false},
		{Name: "name", Type: "string", Nullable: true},
		{Name: "active", Type: "boolean", Nullable: true},
		{Name: "missing", Type: "string", Nullable: true},
	}

	got := AlignRowToSchema(row, schema)
	if len(got) != len(schema) {
		t.Fatalf("expected %d columns, got %d", len(schema), len(got))
	}
	if _, ok := got["extra"]; ok {
		t.Fatal("extra column should be dropped")
	}
	if got["missing"] != nil {
		t.Fatalf("missing column = %v, want nil", got["missing"])
	}
	if got["id"] != int64(42) {
		t.Fatalf("id = %[1]v (%[1]T), want int64(42)", got["id"])
	}
	if got["name"] != "123" {
		t.Fatalf("name = %[1]v (%[1]T), want string \"123\"", got["name"])
	}
	if got["active"] != true {
		t.Fatalf("active = %v, want true", got["active"])
	}
}

func TestAlignRowsToSchema_emptyReturnsNil(t *testing.T) {
	if got := AlignRowsToSchema(nil, []Column{{Name: "id", Type: "int64"}}); got != nil {
		t.Fatalf("expected nil, got %#v", got)
	}
}

// ── Structured Delta errors ──────────────────────────────────────────────────

func TestWrapDeltaError_parsesStructuredStatus(t *testing.T) {
	grpcErr := status.Error(
		codes.Internal,
		"delta_error phase=commit code=timeout retryable=true ambiguous=true table_uri=s3://bucket/table message=deadline exceeded",
	)

	wrapped := wrapDeltaError(grpcErr)
	deltaErr, ok := AsDeltaError(wrapped)
	if !ok {
		t.Fatalf("expected DeltaError, got %T", wrapped)
	}
	if deltaErr.Phase != "commit" {
		t.Fatalf("Phase = %q, want commit", deltaErr.Phase)
	}
	if deltaErr.Code != "timeout" {
		t.Fatalf("Code = %q, want timeout", deltaErr.Code)
	}
	if deltaErr.TableURI != "s3://bucket/table" {
		t.Fatalf("TableURI = %q", deltaErr.TableURI)
	}
	if deltaErr.Message != "deadline exceeded" {
		t.Fatalf("Message = %q", deltaErr.Message)
	}
	if !deltaErr.Retryable {
		t.Fatal("Retryable = false, want true")
	}
	if !deltaErr.AmbiguousCommit {
		t.Fatal("AmbiguousCommit = false, want true")
	}
	if !errors.Is(wrapped, grpcErr) {
		t.Fatal("wrapped error should unwrap to original grpc error")
	}
}

func TestWrapDeltaError_leavesUnstructuredError(t *testing.T) {
	grpcErr := status.Error(codes.Internal, "plain error")
	if got := wrapDeltaError(grpcErr); got != grpcErr {
		t.Fatalf("expected original error, got %T", got)
	}
}

func TestStorageCapabilitiesResultSupported(t *testing.T) {
	result := &StorageCapabilitiesResult{
		Checks: []CapabilityCheck{
			{Name: "put", Supported: true},
			{Name: "copy_if_not_exists", Supported: false},
		},
	}
	if !result.Supported("put") {
		t.Fatal("put should be supported")
	}
	if result.Supported("copy_if_not_exists") {
		t.Fatal("copy_if_not_exists should not be supported")
	}
	if result.Supported("missing") {
		t.Fatal("missing capability should not be supported")
	}
}
