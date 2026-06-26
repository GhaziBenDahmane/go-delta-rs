// Package deltago provides a Go client for the delta-rs gRPC sidecar.
package deltago

import "encoding/json"

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
}

// WriteResult contains metadata returned by a write call.
type WriteResult struct {
	Version          int64
	RowsWritten      int64
	AlreadyCommitted bool
	BatchID          string
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
