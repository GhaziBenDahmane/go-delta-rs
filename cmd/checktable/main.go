package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"

	"github.com/ghazibendahmane/go-delta-rs/deltago"
	pb "github.com/ghazibendahmane/go-delta-rs/gen/go/delta"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:50051", "delta-server gRPC address")
	tableURI := flag.String("table-uri", "", "Delta table URI, e.g. file:///tmp/events or s3://bucket/path")
	filter := flag.String("filter", "", "optional SQL filter for sample reads")
	limit := flag.Int64("limit", 5, "sample read limit; set to 0 to skip reading")
	capabilities := flag.Bool("capabilities", false, "probe object-store capabilities for the table URI")
	flag.Parse()

	if *tableURI == "" {
		log.Fatal("--table-uri is required")
	}

	conn, err := grpc.NewClient(
		*addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(256*1024*1024),
			grpc.MaxCallSendMsgSize(256*1024*1024),
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	ctx := context.Background()
	client := deltago.NewDeltaClient(pb.NewDeltaServiceClient(conn))

	info, err := client.GetTableInfo(ctx, *tableURI)
	if err != nil {
		log.Fatal("get table info:", err)
	}
	fmt.Printf("version=%d num_files=%d partition_columns=%v\n", info.Version, info.NumFiles, info.PartitionColumns)

	if *capabilities {
		result, err := client.CheckStorageCapabilities(ctx, *tableURI)
		if err != nil {
			log.Fatal("check storage capabilities:", err)
		}
		for _, check := range result.Checks {
			if check.Supported {
				fmt.Printf("capability %-32s supported\n", check.Name)
			} else {
				fmt.Printf("capability %-32s unsupported: %s\n", check.Name, check.Error)
			}
		}
	}

	if *limit <= 0 {
		return
	}

	rows, err := client.Read(ctx, *tableURI, &deltago.ReadOptions{
		Filter: *filter,
		Limit:  *limit,
	})
	if err != nil {
		log.Fatal("read sample:", err)
	}
	body, err := json.MarshalIndent(rows, "", "  ")
	if err != nil {
		log.Fatal("format sample rows:", err)
	}
	fmt.Printf("sample_rows=%d\n%s\n", len(rows), string(body))
}
