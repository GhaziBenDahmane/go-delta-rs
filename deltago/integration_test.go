//go:build integration

package deltago_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/ghazibendahmane/go-delta-rs/deltago"
)

const binaryPath = "../delta-server/target/release/delta-server"

func newTestSidecar(t *testing.T) (*deltago.Sidecar, *deltago.DeltaClient) {
	t.Helper()
	if _, err := os.Stat(binaryPath); os.IsNotExist(err) {
		t.Skipf("delta-server binary not found at %s — run 'make build-server' first", binaryPath)
	}
	sidecar := deltago.NewSidecar(deltago.SidecarOptions{BinaryPath: binaryPath})
	if err := sidecar.Start(context.Background()); err != nil {
		t.Fatalf("start sidecar: %v", err)
	}
	t.Cleanup(func() { sidecar.Stop() })
	return sidecar, sidecar.Client()
}

func tempTableURI(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "go-delta-rs-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return "file://" + filepath.ToSlash(dir)
}

var testSchema = []deltago.Column{
	{Name: "id", Type: "int64", Nullable: false},
	{Name: "name", Type: "string", Nullable: true},
	{Name: "score", Type: "float64", Nullable: true},
}

// ── Health ────────────────────────────────────────────────────────────────────

func TestIntegration_Health(t *testing.T) {
	_, c := newTestSidecar(t)
	version, err := c.Health(context.Background())
	if err != nil {
		t.Fatalf("Health: %v", err)
	}
	if version == "" {
		t.Error("expected non-empty version")
	}
	t.Logf("server version: %s", version)
}

// ── CreateTable ───────────────────────────────────────────────────────────────

func TestIntegration_CreateTable(t *testing.T) {
	_, c := newTestSidecar(t)
	uri := tempTableURI(t)

	err := c.CreateTable(context.Background(), uri, testSchema, nil)
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
}

func TestIntegration_CreateTable_withPartition(t *testing.T) {
	_, c := newTestSidecar(t)
	uri := tempTableURI(t)

	schema := []deltago.Column{
		{Name: "date", Type: "string", Nullable: false},
		{Name: "value", Type: "int64", Nullable: true},
	}
	err := c.CreateTable(context.Background(), uri, schema, []string{"date"})
	if err != nil {
		t.Fatalf("CreateTable with partition: %v", err)
	}

	info, err := c.GetTableInfo(context.Background(), uri)
	if err != nil {
		t.Fatalf("GetTableInfo: %v", err)
	}
	if len(info.PartitionColumns) != 1 || info.PartitionColumns[0] != "date" {
		t.Errorf("expected partition column 'date', got %v", info.PartitionColumns)
	}
}

// ── Write / Read ──────────────────────────────────────────────────────────────

func TestIntegration_WriteAppend_Read(t *testing.T) {
	_, c := newTestSidecar(t)
	ctx := context.Background()
	uri := tempTableURI(t)

	if err := c.CreateTable(ctx, uri, testSchema, nil); err != nil {
		t.Fatal(err)
	}

	rows := []deltago.Row{
		{"id": 1, "name": "alice", "score": 9.5},
		{"id": 2, "name": "bob", "score": 7.2},
		{"id": 3, "name": "carol", "score": 8.8},
	}
	if err := c.Write(ctx, uri, deltago.WriteAppend, rows, testSchema); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := c.Read(ctx, uri, nil)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("expected 3 rows, got %d", len(got))
	}
}

func TestIntegration_WriteAppend_Accumulates(t *testing.T) {
	_, c := newTestSidecar(t)
	ctx := context.Background()
	uri := tempTableURI(t)

	if err := c.CreateTable(ctx, uri, testSchema, nil); err != nil {
		t.Fatal(err)
	}

	batch := []deltago.Row{{"id": 1, "name": "alice", "score": 9.5}}
	for i := 0; i < 3; i++ {
		if err := c.Write(ctx, uri, deltago.WriteAppend, batch, testSchema); err != nil {
			t.Fatalf("Write batch %d: %v", i, err)
		}
	}

	got, err := c.Read(ctx, uri, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Errorf("after 3 appends expected 3 rows, got %d", len(got))
	}
}

func TestIntegration_WriteOverwrite(t *testing.T) {
	_, c := newTestSidecar(t)
	ctx := context.Background()
	uri := tempTableURI(t)

	if err := c.CreateTable(ctx, uri, testSchema, nil); err != nil {
		t.Fatal(err)
	}

	initial := []deltago.Row{
		{"id": 1, "name": "alice", "score": 9.5},
		{"id": 2, "name": "bob", "score": 7.2},
	}
	if err := c.Write(ctx, uri, deltago.WriteAppend, initial, testSchema); err != nil {
		t.Fatal(err)
	}

	replacement := []deltago.Row{{"id": 99, "name": "zara", "score": 10.0}}
	if err := c.Write(ctx, uri, deltago.WriteOverwrite, replacement, testSchema); err != nil {
		t.Fatalf("Overwrite: %v", err)
	}

	got, err := c.Read(ctx, uri, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Errorf("after overwrite expected 1 row, got %d", len(got))
	}
}

// ── Read with options ─────────────────────────────────────────────────────────

func TestIntegration_Read_Filter(t *testing.T) {
	_, c := newTestSidecar(t)
	ctx := context.Background()
	uri := tempTableURI(t)

	if err := c.CreateTable(ctx, uri, testSchema, nil); err != nil {
		t.Fatal(err)
	}
	rows := []deltago.Row{
		{"id": 1, "name": "alice", "score": 9.5},
		{"id": 2, "name": "bob", "score": 5.0},
		{"id": 3, "name": "carol", "score": 8.8},
	}
	if err := c.Write(ctx, uri, deltago.WriteAppend, rows, testSchema); err != nil {
		t.Fatal(err)
	}

	got, err := c.Read(ctx, uri, &deltago.ReadOptions{Filter: "score > 8.0"})
	if err != nil {
		t.Fatalf("Read with filter: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("expected 2 rows with score > 8.0, got %d", len(got))
	}
}

func TestIntegration_Read_Limit(t *testing.T) {
	_, c := newTestSidecar(t)
	ctx := context.Background()
	uri := tempTableURI(t)

	if err := c.CreateTable(ctx, uri, testSchema, nil); err != nil {
		t.Fatal(err)
	}
	rows := make([]deltago.Row, 10)
	for i := range rows {
		rows[i] = deltago.Row{"id": i, "name": fmt.Sprintf("user%d", i), "score": float64(i)}
	}
	if err := c.Write(ctx, uri, deltago.WriteAppend, rows, testSchema); err != nil {
		t.Fatal(err)
	}

	got, err := c.Read(ctx, uri, &deltago.ReadOptions{Limit: 3})
	if err != nil {
		t.Fatalf("Read with limit: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("expected 3 rows (limit), got %d", len(got))
	}
}

// ── GetTableInfo ──────────────────────────────────────────────────────────────

func TestIntegration_GetTableInfo(t *testing.T) {
	_, c := newTestSidecar(t)
	ctx := context.Background()
	uri := tempTableURI(t)

	if err := c.CreateTable(ctx, uri, testSchema, nil); err != nil {
		t.Fatal(err)
	}

	info, err := c.GetTableInfo(ctx, uri)
	if err != nil {
		t.Fatalf("GetTableInfo: %v", err)
	}
	if info.Version < 0 {
		t.Errorf("unexpected version %d", info.Version)
	}
	if len(info.Schema) != len(testSchema) {
		t.Errorf("expected %d schema fields, got %d", len(testSchema), len(info.Schema))
	}
	for i, col := range testSchema {
		if info.Schema[i].Name != col.Name {
			t.Errorf("schema[%d].Name: expected %q, got %q", i, col.Name, info.Schema[i].Name)
		}
	}
}

// ── History ───────────────────────────────────────────────────────────────────

func TestIntegration_History(t *testing.T) {
	_, c := newTestSidecar(t)
	ctx := context.Background()
	uri := tempTableURI(t)

	if err := c.CreateTable(ctx, uri, testSchema, nil); err != nil {
		t.Fatal(err)
	}
	rows := []deltago.Row{{"id": 1, "name": "alice", "score": 9.5}}
	if err := c.Write(ctx, uri, deltago.WriteAppend, rows, testSchema); err != nil {
		t.Fatal(err)
	}

	commits, err := c.History(ctx, uri, 0)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	// We expect at least the create + write = 2 commits
	if len(commits) < 2 {
		t.Errorf("expected at least 2 commits, got %d", len(commits))
	}
	for _, commit := range commits {
		if commit.Operation == "" {
			t.Errorf("commit v%d has empty Operation", commit.Version)
		}
	}
}

// ── Time travel ───────────────────────────────────────────────────────────────

func TestIntegration_ReadAtVersion(t *testing.T) {
	_, c := newTestSidecar(t)
	ctx := context.Background()
	uri := tempTableURI(t)

	if err := c.CreateTable(ctx, uri, testSchema, nil); err != nil {
		t.Fatal(err)
	}

	// v1: write 1 row
	if err := c.Write(ctx, uri, deltago.WriteAppend, []deltago.Row{
		{"id": 1, "name": "alice", "score": 9.5},
	}, testSchema); err != nil {
		t.Fatal(err)
	}

	// v2: write another row
	if err := c.Write(ctx, uri, deltago.WriteAppend, []deltago.Row{
		{"id": 2, "name": "bob", "score": 7.0},
	}, testSchema); err != nil {
		t.Fatal(err)
	}

	// Reading v1 should return only 1 row
	rows, err := c.ReadAtVersion(ctx, uri, 1)
	if err != nil {
		t.Fatalf("ReadAtVersion(1): %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("expected 1 row at v1, got %d", len(rows))
	}

	// Latest should return 2 rows
	latest, err := c.Read(ctx, uri, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(latest) != 2 {
		t.Errorf("expected 2 rows at latest, got %d", len(latest))
	}
}

// ── Vacuum ────────────────────────────────────────────────────────────────────

func TestIntegration_Vacuum_DryRun(t *testing.T) {
	_, c := newTestSidecar(t)
	ctx := context.Background()
	uri := tempTableURI(t)

	if err := c.CreateTable(ctx, uri, testSchema, nil); err != nil {
		t.Fatal(err)
	}

	// Dry run should not error even on a fresh table with no expired files.
	_, err := c.Vacuum(ctx, uri, 0, true)
	if err != nil {
		t.Fatalf("Vacuum dry run: %v", err)
	}
}
