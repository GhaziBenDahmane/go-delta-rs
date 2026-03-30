package deltago

import (
	"context"
	"fmt"
	"strconv"

	deltapb "github.com/ghazibendahmane/go-delta-rs/gen/go/delta"
)

// DeltaClient wraps the generated gRPC client with an ergonomic Go API.
type DeltaClient struct {
	client deltapb.DeltaServiceClient
}

// NewDeltaClient creates a DeltaClient that connects to an already-running
// delta-server at the given address (e.g. "127.0.0.1:50051").
// Use NewSidecar if you want go-delta-rs to manage the process lifecycle.
func NewDeltaClient(client deltapb.DeltaServiceClient) *DeltaClient {
	return &DeltaClient{client: client}
}

// Health pings the server and returns its version string.
func (c *DeltaClient) Health(ctx context.Context) (string, error) {
	resp, err := c.client.Health(ctx, &deltapb.HealthRequest{})
	if err != nil {
		return "", err
	}
	return resp.Version, nil
}

// CreateTable creates a new Delta table at tableURI with the given schema.
// partitionCols may be nil.
func (c *DeltaClient) CreateTable(ctx context.Context, tableURI string, schema []Column, partitionCols []string) error {
	cols := toProtoCols(schema)
	_, err := c.client.CreateTable(ctx, &deltapb.CreateTableRequest{
		TableUri:         tableURI,
		Schema:           cols,
		PartitionColumns: partitionCols,
	})
	return err
}

// Write appends or overwrites rows in the Delta table at tableURI.
// schema may be nil — if omitted the server infers all columns as strings.
func (c *DeltaClient) Write(ctx context.Context, tableURI string, mode WriteMode, rows []Row, schema []Column) error {
	jsonData, err := MarshalRows(rows)
	if err != nil {
		return fmt.Errorf("marshal rows: %w", err)
	}
	_, err = c.client.Write(ctx, &deltapb.WriteRequest{
		TableUri: tableURI,
		Mode:     string(mode),
		JsonData: jsonData,
		Schema:   toProtoCols(schema),
	})
	return err
}

// Read reads rows from the Delta table at tableURI.
// opts may be nil to use defaults (latest version, no filter, no limit).
func (c *DeltaClient) Read(ctx context.Context, tableURI string, opts *ReadOptions) ([]Row, error) {
	req := &deltapb.ReadRequest{TableUri: tableURI}
	if opts != nil {
		req.Version = opts.Version
		req.Filter = opts.Filter
		req.Limit = opts.Limit
	}
	resp, err := c.client.Read(ctx, req)
	if err != nil {
		return nil, err
	}
	return UnmarshalRows(resp.JsonData)
}

// GetTableInfo returns schema and metadata for the Delta table at tableURI.
func (c *DeltaClient) GetTableInfo(ctx context.Context, tableURI string) (*TableInfo, error) {
	resp, err := c.client.GetTableInfo(ctx, &deltapb.GetTableInfoRequest{TableUri: tableURI})
	if err != nil {
		return nil, err
	}
	return &TableInfo{
		Version:          resp.Version,
		Schema:           fromProtoCols(resp.Schema),
		PartitionColumns: resp.PartitionColumns,
		NumFiles:         resp.NumFiles,
		CreatedTime:      resp.CreatedTime,
	}, nil
}

// History returns the commit log for the Delta table at tableURI.
// limit=0 returns the full history.
func (c *DeltaClient) History(ctx context.Context, tableURI string, limit int) ([]CommitInfo, error) {
	resp, err := c.client.History(ctx, &deltapb.HistoryRequest{
		TableUri: tableURI,
		Limit:    int32(limit),
	})
	if err != nil {
		return nil, err
	}
	commits := make([]CommitInfo, len(resp.Commits))
	for i, c := range resp.Commits {
		commits[i] = CommitInfo{
			Version:             c.Version,
			Timestamp:           c.Timestamp,
			Operation:           c.Operation,
			OperationParameters: c.OperationParameters,
		}
	}
	return commits, nil
}

// Vacuum removes files no longer needed by the Delta table.
// retentionHours=0 uses the Delta default (168h).
// Set dryRun=true to preview without deleting.
func (c *DeltaClient) Vacuum(ctx context.Context, tableURI string, retentionHours float32, dryRun bool) ([]string, error) {
	resp, err := c.client.Vacuum(ctx, &deltapb.VacuumRequest{
		TableUri:       tableURI,
		RetentionHours: retentionHours,
		DryRun:         dryRun,
	})
	if err != nil {
		return nil, err
	}
	return resp.DeletedFiles, nil
}

// ReadOptions controls filtering and pagination for Read.
type ReadOptions struct {
	// Version to read. Empty string = latest.
	Version string
	// Filter is a SQL WHERE expression, e.g. "age > 30".
	Filter string
	// Limit is the maximum number of rows to return. 0 = no limit.
	Limit int64
}

// ReadAtVersion is a convenience helper for reading a specific table version.
func (c *DeltaClient) ReadAtVersion(ctx context.Context, tableURI string, version int64) ([]Row, error) {
	return c.Read(ctx, tableURI, &ReadOptions{Version: strconv.FormatInt(version, 10)})
}

// Optimize compacts small files in the Delta table into larger ones.
// targetSizeBytes=0 uses the server default (256 MiB).
// partitionFilter selects a single partition in "key=value" format,
// e.g. "year_month=2026-02". Leave empty to optimize all partitions.
func (c *DeltaClient) Optimize(ctx context.Context, tableURI string, opts *OptimizeOptions) (*OptimizeResult, error) {
	req := &deltapb.OptimizeRequest{TableUri: tableURI}
	if opts != nil {
		req.TargetSizeBytes = opts.TargetSizeBytes
		req.PartitionFilter = opts.PartitionFilter
	}
	resp, err := c.client.Optimize(ctx, req)
	if err != nil {
		return nil, err
	}
	return &OptimizeResult{
		FilesAdded:            resp.FilesAdded,
		FilesRemoved:          resp.FilesRemoved,
		PartitionsOptimized:   resp.PartitionsOptimized,
	}, nil
}

// ── proto conversion helpers ─────────────────────────────────────────────────

func toProtoCols(cols []Column) []*deltapb.ColumnDef {
	if len(cols) == 0 {
		return nil
	}
	out := make([]*deltapb.ColumnDef, len(cols))
	for i, c := range cols {
		out[i] = &deltapb.ColumnDef{
			Name:     c.Name,
			DataType: c.Type,
			Nullable: c.Nullable,
		}
	}
	return out
}

func fromProtoCols(cols []*deltapb.ColumnDef) []Column {
	out := make([]Column, len(cols))
	for i, c := range cols {
		out[i] = Column{Name: c.Name, Type: c.DataType, Nullable: c.Nullable}
	}
	return out
}
