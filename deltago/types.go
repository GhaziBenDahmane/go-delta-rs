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
