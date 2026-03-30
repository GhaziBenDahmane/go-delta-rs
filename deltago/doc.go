// Package deltago provides Go bindings for Delta Lake via a gRPC sidecar.
//
// It consists of two parts:
//
//   - A Rust binary (delta-server) that wraps delta-rs and exposes Delta Lake
//     operations over gRPC. Pre-built binaries for each platform are available
//     on the GitHub releases page.
//
//   - This Go package, which manages the sidecar process lifecycle and
//     exposes a clean API for creating, reading, and writing Delta tables.
//
// # Quick start
//
//	sidecar := deltago.NewSidecar(deltago.SidecarOptions{
//	    BinaryPath: "./delta-server",
//	})
//	if err := sidecar.Start(ctx); err != nil {
//	    log.Fatal(err)
//	}
//	defer sidecar.Stop()
//
//	c := sidecar.Client()
//
//	schema := []deltago.Column{
//	    {Name: "id",    Type: "int64",   Nullable: false},
//	    {Name: "email", Type: "string",  Nullable: true},
//	}
//	c.CreateTable(ctx, "s3://my-bucket/users", schema, nil)
//	c.Write(ctx, "s3://my-bucket/users", deltago.WriteAppend, rows, schema)
//	rows, _ := c.Read(ctx, "s3://my-bucket/users", nil)
//
// # Supported storage backends
//
// Table URIs follow the delta-rs conventions:
//   - file:///absolute/path  — local filesystem
//   - s3://bucket/prefix     — Amazon S3
//   - gs://bucket/prefix     — Google Cloud Storage
//   - az://container/path    — Azure Data Lake Storage
//
// Cloud credentials are read from the standard environment variables for each
// provider (AWS credential chain, GOOGLE_APPLICATION_CREDENTIALS, etc.).
//
// # Running the sidecar externally
//
// The sidecar can also run as a separate container or service. Connect to it
// directly using [NewDeltaClient] and a gRPC connection of your choice.
package deltago
