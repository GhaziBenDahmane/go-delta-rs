// Command maintenance runs compaction (Optimize) and/or Vacuum against a
// Delta table stored on S3-compatible storage.
//
// Configuration is read from environment variables:
//
//	DELTA_AWS_ACCESS_KEY_ID
//	DELTA_AWS_SECRET_ACCESS_KEY
//	DELTA_AWS_S3_ENDPOINT_URL
//	DELTA_S3_BUCKET
//	DELTA_S3_PREFIX
//	DELTA_SERVER_PATH   (optional — auto-downloaded if absent)
//
// Usage:
//
//	maintenance [flags]
//	  --dry-run                preview vacuum without deleting
//	  --vacuum-only            skip optimize
//	  --optimize-only          skip vacuum
//	  --partition-filter K=V   optimize a single partition, e.g. year_month=2026-03
//	                           (leave empty to optimize all partitions)
//	  --retention-hours N      vacuum retention in hours (default 168)
//	  --target-size-mb N       optimize target file size in MiB (default 256)
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/ghazibendahmane/go-delta-rs/deltago"
)

func main() {
	dryRun := flag.Bool("dry-run", false, "preview vacuum without deleting files")
	vacuumOnly := flag.Bool("vacuum-only", false, "skip optimize, run vacuum only")
	optimizeOnly := flag.Bool("optimize-only", false, "skip vacuum, run optimize only")
	partitionFilter := flag.String("partition-filter", "", "optimize a single partition in key=value format (empty = all partitions)")
	retentionHours := flag.Float64("retention-hours", 168, "vacuum retention in hours")
	targetSizeMB := flag.Int64("target-size-mb", 256, "optimize target file size in MiB")
	flag.Parse()

	// ── Config from env ──────────────────────────────────────────────────────
	accessKey := requireEnv("DELTA_AWS_ACCESS_KEY_ID")
	secretKey := requireEnv("DELTA_AWS_SECRET_ACCESS_KEY")
	endpoint := requireEnv("DELTA_AWS_S3_ENDPOINT_URL")
	// delta-rs requires a full URL with scheme; add https:// if absent.
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		endpoint = "https://" + endpoint
	}
	bucket := requireEnv("DELTA_S3_BUCKET")
	prefix := requireEnv("DELTA_S3_PREFIX")

	tableURI := fmt.Sprintf("s3://%s/%s", bucket, prefix)

	// ── Start sidecar ────────────────────────────────────────────────────────
	// S3-compatible stores require a region in signing headers.
	// Default to "us-east-1" when not explicitly set.
	region := os.Getenv("DELTA_AWS_S3_REGION")
	if region == "" {
		region = "us-east-1"
	}

	opts := deltago.SidecarOptions{
		BinaryPath: os.Getenv("DELTA_SERVER_PATH"), // empty → auto-download
		Storage: deltago.StorageConfig{
			S3Endpoint:          endpoint,
			S3AccessKeyID:       accessKey,
			S3SecretAccessKey:   secretKey,
			S3Region:            region,
			S3AllowUnsafeRename: true,
		},
		// AWS SDK v2026+ enables checksum validation by default. Some S3-compatible
		// stores do not return x-amz-checksum-* headers. Force both to
		// "when_required" for compatibility.
		Env: []string{
			"AWS_REQUEST_CHECKSUM_CALCULATION=when_required",
			"AWS_RESPONSE_CHECKSUM_VALIDATION=when_required",
		},
	}

	sidecar := deltago.NewSidecar(opts)
	ctx := context.Background()

	slog.Info("starting delta-server sidecar")
	if err := sidecar.Start(ctx); err != nil {
		slog.Error("failed to start sidecar", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := sidecar.Stop(); err != nil {
			slog.Warn("sidecar stop error", "error", err)
		}
	}()

	client := sidecar.Client()

	// ── Table info (before) ──────────────────────────────────────────────────
	printTableInfo(ctx, client, tableURI, "BEFORE")

	// ── Optimize ─────────────────────────────────────────────────────────────
	if !*vacuumOnly {
		label := "all partitions"
		if *partitionFilter != "" {
			label = "partition " + *partitionFilter
		}
		slog.Info("running optimize", "table", tableURI, "scope", label, "target_size_mb", *targetSizeMB)

		// Optimize reads parquet file data over S3, which can fail with transient
		// network errors on some S3-compatible stores. Retry up to 5 times with
		// linear back-off.
		var result *deltago.OptimizeResult
		for attempt := 1; attempt <= 5; attempt++ {
			var err error
			result, err = client.Optimize(ctx, tableURI, &deltago.OptimizeOptions{
				TargetSizeBytes: *targetSizeMB * 1024 * 1024,
				PartitionFilter: *partitionFilter,
			})
			if err == nil {
				break
			}
			if attempt == 5 {
				slog.Error("optimize failed after retries", "error", err)
				os.Exit(1)
			}
			slog.Warn("optimize attempt failed, retrying", "attempt", attempt, "error", err)
			time.Sleep(time.Duration(attempt*10) * time.Second)
		}
		slog.Info("optimize complete",
			"files_added", result.FilesAdded,
			"files_removed", result.FilesRemoved,
			"partitions_optimized", result.PartitionsOptimized,
		)
	}

	// ── Vacuum ───────────────────────────────────────────────────────────────
	if !*optimizeOnly {
		slog.Info("running vacuum",
			"table", tableURI,
			"retention_hours", *retentionHours,
			"dry_run", *dryRun,
		)

		deleted, err := client.Vacuum(ctx, tableURI, float32(*retentionHours), *dryRun)
		if err != nil {
			slog.Error("vacuum failed", "error", err)
			os.Exit(1)
		}

		if *dryRun {
			slog.Info("vacuum dry-run: files that would be deleted", "count", len(deleted))
			for _, f := range deleted {
				fmt.Println(" ", f)
			}
		} else {
			slog.Info("vacuum complete", "files_deleted", len(deleted))
		}
	}

	// ── Table info (after) ───────────────────────────────────────────────────
	if !*dryRun {
		printTableInfo(ctx, client, tableURI, "AFTER")
	}
}

func printTableInfo(ctx context.Context, client *deltago.DeltaClient, tableURI, label string) {
	info, err := client.GetTableInfo(ctx, tableURI)
	if err != nil {
		slog.Warn("could not fetch table info", "label", label, "error", err)
		return
	}
	slog.Info(fmt.Sprintf("table info [%s]", label),
		"version", info.Version,
		"num_files", info.NumFiles,
		"partition_columns", info.PartitionColumns,
	)
}

func requireEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("required environment variable not set", "var", key)
		os.Exit(1)
	}
	return v
}
