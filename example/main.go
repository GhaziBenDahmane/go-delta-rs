package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/ghazibendahmane/go-delta-rs/deltago"
)

func main() {
	ctx := context.Background()

	tableURI := "file:///tmp/my-delta-table"
	if len(os.Args) > 1 {
		tableURI = os.Args[1]
	}

	// Launch the sidecar. The binary must be built first:
	//   cd delta-server && cargo build --release
	sidecar := deltago.NewSidecar(deltago.SidecarOptions{
		BinaryPath: "./delta-server/target/release/delta-server",
	})
	if err := sidecar.Start(ctx); err != nil {
		log.Fatalf("start sidecar: %v", err)
	}
	defer sidecar.Stop()
	fmt.Printf("sidecar running on port %d\n", sidecar.Port())

	client := sidecar.Client()

	// 1. Create table
	schema := []deltago.Column{
		{Name: "id", Type: "int64", Nullable: false},
		{Name: "name", Type: "string", Nullable: true},
		{Name: "score", Type: "float64", Nullable: true},
	}
	if err := client.CreateTable(ctx, tableURI, schema, nil); err != nil {
		log.Fatalf("create table: %v", err)
	}
	fmt.Println("table created")

	// 2. Write rows
	rows := []deltago.Row{
		{"id": 1, "name": "alice", "score": 9.5},
		{"id": 2, "name": "bob", "score": 8.0},
		{"id": 3, "name": "carol", "score": 9.1},
	}
	if err := client.Write(ctx, tableURI, deltago.WriteAppend, rows, schema); err != nil {
		log.Fatalf("write: %v", err)
	}
	fmt.Printf("wrote %d rows\n", len(rows))

	// 3. Read all rows
	all, err := client.Read(ctx, tableURI, nil)
	if err != nil {
		log.Fatalf("read: %v", err)
	}
	fmt.Printf("read %d rows:\n", len(all))
	for _, r := range all {
		fmt.Printf("  %v\n", r)
	}

	// 4. Read with filter
	filtered, err := client.Read(ctx, tableURI, &deltago.ReadOptions{Filter: "score > 9.0"})
	if err != nil {
		log.Fatalf("read filtered: %v", err)
	}
	fmt.Printf("rows with score > 9.0: %d\n", len(filtered))

	// 5. Table info
	info, err := client.GetTableInfo(ctx, tableURI)
	if err != nil {
		log.Fatalf("get table info: %v", err)
	}
	fmt.Printf("table version=%d files=%d\n", info.Version, info.NumFiles)

	// 6. History
	commits, err := client.History(ctx, tableURI, 10)
	if err != nil {
		log.Fatalf("history: %v", err)
	}
	for _, c := range commits {
		fmt.Printf("  v%d  %s  %s\n", c.Version, c.Timestamp, c.Operation)
	}
}
