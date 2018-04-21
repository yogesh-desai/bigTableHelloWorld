package model

import (
	"context"
	"errors"
	"fmt"
	"log"

	"cloud.google.com/go/bigtable"

	"github.com/yogesh-desai/bigTableHelloWorld/lib"
)

var (
	greetings   = []string{"Hello World!", "Hello Cloud Bigtable!", "Hello golang!"}
	adminClient *bigtable.AdminClient
	client      *bigtable.Client
	tbl         *bigtable.Table
)

//functions specific to 1 table only

func Init(ctx context.Context, project, instance string) {
	aClient, err := GetAdminClient(ctx, project, instance)
	if err != nil {
		log.Fatalf("Could not create admin client: %v", err)
	}

	tblClient, err := bigtable.NewClient(ctx, project, instance)
	if err != nil {
		log.Fatalf("Could not create data operations client: %v", err)
	}

	adminClient = aClient
	client = tblClient
}

func TableExists(ctx context.Context) bool {
	tables, err := adminClient.Tables(ctx)
	if err != nil {
		log.Fatalf("Could not fetch table list: %v", err)
	}
	if !lib.SliceContains(tables, lib.TABLE_NAME) {
		return false
	}
	return true
}

func CreateIfNotExists(ctx context.Context) error {
	tableExists := TableExists(ctx)
	if !tableExists {
		log.Printf("Creating table %s", lib.TABLE_NAME)
		if err := adminClient.CreateTable(ctx, lib.TABLE_NAME); err != nil {
			log.Printf("Could not create table %s: %v", lib.TABLE_NAME, err)
			return err
		}
	}
	return nil
}

func CreateColumnFamily(ctx context.Context) error {
	tblInfo, err := adminClient.TableInfo(ctx, lib.TABLE_NAME)
	if err != nil {
		log.Printf("Could not read info for table %s: %v", lib.TABLE_NAME, err)
		return err
	}

	log.Printf("Table info%+v\n:", *tblInfo)

	if !lib.SliceContains(tblInfo.Families, lib.COLUMN_FAMILY_NAME) {
		if err := adminClient.CreateColumnFamily(ctx, lib.TABLE_NAME, lib.COLUMN_FAMILY_NAME); err != nil {
			log.Printf("Could not create column family %s: %v", lib.COLUMN_FAMILY_NAME, err)
			return err
		}
	}
	return nil
}

func DisplayRow(ctx context.Context, rowKey string) error {
	tbl := client.Open(lib.TABLE_NAME)

	row, err := tbl.ReadRow(ctx, rowKey, bigtable.RowFilter(bigtable.ColumnFilter(lib.COLUMN_NAME)))
	if err != nil {
		log.Printf("Could not read row with key %s: %v", rowKey, err)
		return err
	}
	log.Printf("\t%s = %s\n", rowKey, string(row[lib.COLUMN_FAMILY_NAME][0].Value))
	return nil
}

func DisplayAll(ctx context.Context) error {
	tbl := client.Open(lib.TABLE_NAME)

	err := tbl.ReadRows(ctx, bigtable.PrefixRange(lib.COLUMN_NAME), func(row bigtable.Row) bool {
		item := row[lib.COLUMN_FAMILY_NAME][0]
		log.Printf("\t%s = %s\n", item.Row, string(item.Value))
		return true
	}, bigtable.RowFilter(bigtable.ColumnFilter(lib.COLUMN_NAME)))
	if err != nil {
		return err
	}
	return nil
}

type RowRange struct {
	start string
	limit string
}

func SampleRowKeys(ctx context.Context) ([]string, error) {
	return tbl.SampleRowKeys(context.Background())
}

func InsertAndDisplay(ctx context.Context, project, instance string) error {
	tbl = client.Open(lib.TABLE_NAME)
	muts := make([]*bigtable.Mutation, len(greetings))
	rowKeys := make([]string, len(greetings))

	log.Printf("Writing greeting rows to table")
	for i, greeting := range greetings {
		muts[i] = bigtable.NewMutation()
		muts[i].Set(lib.COLUMN_FAMILY_NAME, lib.COLUMN_NAME, bigtable.Now(), []byte(greeting))

		rowKeys[i] = fmt.Sprintf("%s%d", lib.COLUMN_NAME, i)
	}

	rowErrs, err := tbl.ApplyBulk(ctx, rowKeys, muts)
	if err != nil {
		log.Printf("Could not apply bulk row mutation: %v", err)
		return err
	}
	if rowErrs != nil {
		for _, rowErr := range rowErrs {
			log.Printf("Error writing row: %v", rowErr)
		}
		log.Printf("Could not write some rows")
		return errors.New("Err writing some rows")
	}

	log.Printf("Getting a single greeting by row key:")
	err = DisplayRow(ctx, rowKeys[0])
	if err != nil {
		return err
	}

	log.Printf("Reading all greeting rows:")
	err = DisplayAll(ctx)
	if err != nil {
		return err
	}

	return nil
}

func DeleteRow(ctx context.Context) error {
	err := adminClient.DropRowRange(ctx, lib.TABLE_NAME, "testColumn0")
	if err != nil {
		log.Println("Err deleting row range: ", err)
	}

	log.Println("After deletion of single row:")
	err = DisplayAll(ctx)
	if err != nil {
		log.Printf("Err displaying data %s: %v", lib.TABLE_NAME, err)
		return err
	}

	return nil
}

func DeleteTable(ctx context.Context) error {
	if err := adminClient.DeleteTable(ctx, lib.TABLE_NAME); err != nil {
		log.Printf("Could not delete table %s: %v", lib.TABLE_NAME, err)
		return err
	}

	return nil
}
func CloseConnections(ctx context.Context) error {
	if err := client.Close(); err != nil {
		log.Printf("Could not close data operations client: %v", err)
		return err
	}

	if err := adminClient.Close(); err != nil {
		log.Printf("Could not close admin client: %v", err)
		return err
	}
	return nil
}
