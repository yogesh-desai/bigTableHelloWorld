package main

import (
	"context"
	"flag"
	"log"

	"github.com/yogesh-desai/bigTableHelloWorld/lib"
	"github.com/yogesh-desai/bigTableHelloWorld/model"
	"go.opencensus.io/exporter/stackdriver"
	"go.opencensus.io/trace"
)

func main() {
	project := flag.String("project", "", "The Google Cloud Platform project ID. Required.")
	instance := flag.String("instance", "", "The Google Cloud Bigtable instance ID. Required.")
	flag.Parse()

	for _, f := range []string{"project", "instance"} {
		if flag.Lookup(f).Value.String() == "" {
			log.Fatalf("The %s flag is required.", f)
		}
	}

	ctx := context.Background()

	e, err := stackdriver.NewExporter(stackdriver.Options{ProjectID: *project})
	if err != nil {
		log.Fatalf("Could not create stackdriver exporter %v", err)
	}
	trace.RegisterExporter(trace.Exporter(e))
	trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})
	ctx, _ = trace.StartSpan(ctx, "test")
	model.Init(ctx, *project, *instance)

	defer func() {
		span := trace.FromContext(ctx)
		span.End()
		log.Println("closing connections!")
		if err := model.CloseConnections(ctx); err != nil {
			log.Fatalf("Could not close connections table %s: %v", lib.TABLE_NAME, err)
		}
	}()

	err = model.CreateIfNotExists(ctx)
	if err != nil {
		log.Fatalf("Could not create table %s: %v", lib.TABLE_NAME, err)
	}

	err = model.CreateColumnFamily(ctx)
	if err != nil {
		log.Fatalf("Could not create column family %s: %v", lib.COLUMN_FAMILY_NAME, err)
	}

	err = model.InsertAndDisplay(ctx, *project, *instance)
	if err != nil {
		log.Fatalf("Could not insert and display data %s: %v", lib.TABLE_NAME, err)
	}

	log.Printf("Deleting a row")
	if err = model.DeleteRow(ctx); err != nil {
		log.Fatalf("Could not delete table %s: %v", lib.TABLE_NAME, err)
	}

	keys, err := model.SampleRowKeys(ctx)
	if err != nil {
		log.Fatalf("Err in SampleRowKeys: %v", err)
	}
	log.Println("SampleRowKeys res: ", keys)

	log.Printf("Deleting the table")
	if err = model.DeleteTable(ctx); err != nil {
		log.Fatalf("Could not delete table %s: %v", lib.TABLE_NAME, err)
	}
}
