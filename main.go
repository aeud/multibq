package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"io/ioutil"

	"flag"

	"cloud.google.com/go/bigquery"
)

var ch = make(chan time.Time)

var (
	dataset   *bigquery.Dataset
	q         string
	tableName *string
	datasetID *string
	projectID *string
)

func init() {

	tableName = flag.String("table", "rfm", "Table name")
	datasetID = flag.String("dataset", "rfm", "Dataset name to store the table to")
	projectID = flag.String("project", "rfm", "Google project ID")
	flag.Parse()

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, *projectID)
	if err != nil {
		panic(err)
	}
	dataset = client.Dataset(*datasetID)
	bs, err := ioutil.ReadFile(fmt.Sprintf("./queries/%v.sql", *tableName))
	if err != nil {
		panic(err)
	}
	q = string(bs)
}

func bq(t time.Time) error {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, *projectID)
	if err != nil {
		panic(err)
	}
	table := dataset.Table(fmt.Sprintf("%v_%v", *tableName, t.Format("20060102")))

	query := client.Query(fmt.Sprintf(q, t.Format("2006-01-02")))
	query.Dst = table
	query.CreateDisposition = bigquery.CreateIfNeeded
	query.WriteDisposition = bigquery.WriteTruncate
	query.UseStandardSQL = true

	job, err := query.Run(ctx)
	if err != nil {
		return err
	}

	for status, err := job.Status(ctx); !status.Done() && err == nil; status, err = job.Status(ctx) {
		// log.Println(status.State)
		if err != nil {
			return fmt.Errorf("API error: %v", status.Errors[0].Error())
		}
		if len(status.Errors) > 0 {
			return fmt.Errorf("Response error: %v", status.Errors[0].Error())
		}
		time.Sleep(5 * time.Second)
	}
	return nil
}

func main() {
	wg := new(sync.WaitGroup)
	for i := 0; i < 10; i++ {
		go func() {
			for {
				t := <-ch
				log.Println(t)
				if err := bq(t); err != nil {
					log.Println(err)
				}
				wg.Done()
			}
		}()
	}
	// var i int
	start, _ := time.Parse("2006-01-02", "2017-07-01")
	end, _ := time.Parse("2006-01-02", "2017-07-05")
	for end.Sub(start) >= 0 {
		wg.Add(1)
		ch <- start
		start = start.AddDate(0, 0, 1)
	}
	wg.Wait()
}
