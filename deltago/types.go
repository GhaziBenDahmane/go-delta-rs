// Package deltago provides a Go client for the delta-rs gRPC sidecar.
package deltago

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// WriteMode controls how rows are written to a Delta table.
type WriteMode string

const (
	WriteAppend    WriteMode = "append"
	WriteOverwrite WriteMode = "overwrite"
)

// Column describes a single field in a Delta table schema.
type Column struct {
	Name     string
	Type     string // "string","int32","int64","float32","float64","boolean","timestamp","date"
	Nullable bool
}

// TableInfo holds metadata about a Delta table.
type TableInfo struct {
	Version          int64
	Schema           []Column
	PartitionColumns []string
	NumFiles         int64
	CreatedTime      string
}

// CommitInfo holds information about a single Delta table commit.
type CommitInfo struct {
	Version             int64
	Timestamp           string
	Operation           string
	OperationParameters string
}

// Row is a single table row represented as a map of column name → value.
type Row = map[string]any

// WriteOptions controls optional write idempotency and diagnostics.
type WriteOptions struct {
	// BatchID is written to Delta commit metadata and echoed in write diagnostics.
	BatchID string
	// AppTransactionID enables Delta application transaction based idempotency.
	// Use a stable value for a logical batch.
	AppTransactionID string
	// AppTransactionVersion must be positive when AppTransactionID is set.
	AppTransactionVersion int64
	// CreateIfMissing creates the table before writing if it does not exist.
	// Use PartitionColumns to preserve partition metadata on first append.
	CreateIfMissing bool
	// PartitionColumns are used only when CreateIfMissing creates a new table.
	PartitionColumns []string
}

// WriteResult contains metadata returned by a write call.
type WriteResult struct {
	Version          int64
	RowsWritten      int64
	AlreadyCommitted bool
	BatchID          string
}

// DeleteOptions controls a Delta DELETE operation.
type DeleteOptions struct {
	// Predicate is a SQL predicate parsed by delta-rs/DataFusion.
	// Empty means full-table delete and requires AllowFullTableDelete.
	Predicate string
	// AllowFullTableDelete permits an empty Predicate.
	AllowFullTableDelete bool
}

// DeleteResult holds metrics returned by a Delta DELETE operation.
type DeleteResult struct {
	Version         int64
	FilesAdded      int64
	FilesRemoved    int64
	RowsDeleted     int64
	RowsCopied      int64
	ExecutionTimeMS int64
	ScanTimeMS      int64
	RewriteTimeMS   int64
}

// OptimizeOptions controls file compaction behaviour.
type OptimizeOptions struct {
	// TargetSizeBytes is the desired output file size. 0 = server default (256 MiB).
	TargetSizeBytes int64
	// PartitionFilter selects a single partition in "key=value" format,
	// e.g. "year_month=2026-02". Empty = optimize all partitions.
	PartitionFilter string
	// ZOrderColumns applies Z-ordering on the specified columns during OPTIMIZE,
	// co-locating related data within files to improve query performance.
	// Only non-partition columns are meaningful (partition columns are already
	// segregated into separate files).
	ZOrderColumns []string
}

// OptimizeResult holds the metrics returned by an Optimize call.
type OptimizeResult struct {
	FilesAdded          int64
	FilesRemoved        int64
	PartitionsOptimized int64
}

// RewriteCheckpointOptions controls checkpoint repartitioning.
type RewriteCheckpointOptions struct {
	// TargetPartSizeBytes is the desired maximum checkpoint part size. 0 = server default.
	TargetPartSizeBytes int64
	// TargetParts explicitly sets the number of checkpoint parts. When >0 it
	// takes precedence over TargetPartSizeBytes.
	TargetParts int32
	// DryRun previews the rewrite plan without writing objects.
	DryRun bool
}

// RewriteCheckpointResult contains the checkpoint rewrite metrics.
type RewriteCheckpointResult struct {
	Version          int64
	SourceParts      int32
	TargetParts      int32
	SourceSizeBytes  int64
	TargetSizeBytes  int64
	MaxPartSizeBytes int64
	Rows             int64
	Rewritten        bool
	BackupPrefix     string
	CheckpointFiles  []string
	Message          string
}

// CapabilityCheck reports support for one storage operation.
type CapabilityCheck struct {
	Name      string
	Supported bool
	Error     string
}

// StorageCapabilitiesResult contains the storage capability probe result.
type StorageCapabilitiesResult struct {
	TableURI string
	Checks   []CapabilityCheck
}

// Supported reports whether a named capability check succeeded.
func (r *StorageCapabilitiesResult) Supported(name string) bool {
	if r == nil {
		return false
	}
	for _, check := range r.Checks {
		if check.Name == name {
			return check.Supported
		}
	}
	return false
}

// MemoryStats reports jemalloc memory counters from the sidecar process.
// Values are zero when the sidecar allocator does not expose a counter.
type MemoryStats struct {
	AllocatedBytes uint64
	ActiveBytes    uint64
	ResidentBytes  uint64
	MappedBytes    uint64
	RetainedBytes  uint64
}

// RuntimeStats reports generic sidecar runtime state.
type RuntimeStats struct {
	Memory               MemoryStats
	TableCacheEntries    int64
	TableCacheMaxEntries int64
	TableCacheTTL        time.Duration
}

// ClearTableCacheResult reports how much table cache state was cleared.
type ClearTableCacheResult struct {
	TablesRemoved     int64
	TableCacheEntries int64
	MemoryBefore      MemoryStats
	MemoryAfter       MemoryStats
}

// ReleaseMemoryResult reports allocator counters before and after a purge.
type ReleaseMemoryResult struct {
	MemoryBefore MemoryStats
	MemoryAfter  MemoryStats
}

// MarshalRows serialises a slice of Row to a JSON array string.
func MarshalRows(rows []Row) (string, error) {
	b, err := json.Marshal(rows)
	return string(b), err
}

// UnmarshalRows deserialises a JSON array string to a slice of Row.
func UnmarshalRows(data string) ([]Row, error) {
	var rows []Row
	err := json.Unmarshal([]byte(data), &rows)
	return rows, err
}

// AlignRowToSchema returns a new row containing exactly the schema columns.
// Missing columns are set to nil and extra input columns are dropped.
func AlignRowToSchema(row Row, schema []Column) Row {
	aligned := make(Row, len(schema))
	for _, col := range schema {
		if value, ok := row[col.Name]; ok {
			aligned[col.Name] = CoerceValueForColumn(value, col)
		} else {
			aligned[col.Name] = nil
		}
	}
	return aligned
}

// AlignRowsToSchema applies AlignRowToSchema to a slice of rows.
func AlignRowsToSchema(rows []Row, schema []Column) []Row {
	if len(rows) == 0 {
		return nil
	}
	aligned := make([]Row, len(rows))
	for i, row := range rows {
		aligned[i] = AlignRowToSchema(row, schema)
	}
	return aligned
}

// CoerceValueForColumn performs conservative scalar coercions for common Delta
// primitive types. Unknown or incompatible values are returned unchanged so
// delta-rs remains the source of truth for final type validation.
func CoerceValueForColumn(value any, col Column) any {
	if isNilValue(value) {
		return nil
	}

	switch strings.ToLower(col.Type) {
	case "string", "str":
		return coerceString(value)
	case "int32", "integer", "int":
		return coerceInt32(value)
	case "int64", "long":
		return coerceInt64(value)
	case "float32", "float":
		return coerceFloat32(value)
	case "float64", "double":
		return coerceFloat64(value)
	case "boolean", "bool":
		return coerceBool(value)
	default:
		return value
	}
}

func isNilValue(value any) bool {
	if value == nil {
		return true
	}
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}

func coerceString(value any) any {
	switch v := value.(type) {
	case string, *string:
		return v
	case fmt.Stringer:
		return v.String()
	case *int:
		s := strconv.Itoa(*v)
		return &s
	case int:
		return strconv.Itoa(v)
	case *int32:
		s := strconv.FormatInt(int64(*v), 10)
		return &s
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case *int64:
		s := strconv.FormatInt(*v, 10)
		return &s
	case int64:
		return strconv.FormatInt(v, 10)
	case *float32:
		s := strconv.FormatFloat(float64(*v), 'f', -1, 32)
		return &s
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case *float64:
		s := strconv.FormatFloat(*v, 'f', -1, 64)
		return &s
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case *bool:
		s := strconv.FormatBool(*v)
		return &s
	case bool:
		return strconv.FormatBool(v)
	default:
		return value
	}
}

func coerceInt32(value any) any {
	switch v := value.(type) {
	case int32, *int32:
		return v
	case int:
		return int32(v)
	case *int:
		c := int32(*v)
		return &c
	case int64:
		return int32(v)
	case *int64:
		c := int32(*v)
		return &c
	case float64:
		return int32(v)
	case *float64:
		c := int32(*v)
		return &c
	default:
		return value
	}
}

func coerceInt64(value any) any {
	switch v := value.(type) {
	case int64, *int64:
		return v
	case int:
		return int64(v)
	case *int:
		c := int64(*v)
		return &c
	case int32:
		return int64(v)
	case *int32:
		c := int64(*v)
		return &c
	case float64:
		return int64(v)
	case *float64:
		c := int64(*v)
		return &c
	default:
		return value
	}
}

func coerceFloat32(value any) any {
	switch v := value.(type) {
	case float32, *float32:
		return v
	case float64:
		return float32(v)
	case *float64:
		c := float32(*v)
		return &c
	case int:
		return float32(v)
	case *int:
		c := float32(*v)
		return &c
	case int32:
		return float32(v)
	case *int32:
		c := float32(*v)
		return &c
	case int64:
		return float32(v)
	case *int64:
		c := float32(*v)
		return &c
	default:
		return value
	}
}

func coerceFloat64(value any) any {
	switch v := value.(type) {
	case float64, *float64:
		return v
	case float32:
		return float64(v)
	case *float32:
		c := float64(*v)
		return &c
	case int:
		return float64(v)
	case *int:
		c := float64(*v)
		return &c
	case int32:
		return float64(v)
	case *int32:
		c := float64(*v)
		return &c
	case int64:
		return float64(v)
	case *int64:
		c := float64(*v)
		return &c
	default:
		return value
	}
}

func coerceBool(value any) any {
	switch v := value.(type) {
	case bool, *bool:
		return v
	default:
		return value
	}
}
