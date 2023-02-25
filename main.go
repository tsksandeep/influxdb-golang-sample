package main

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/apache/arrow/go/v12/arrow/flight/flightsql"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

const (
	INFLUXDB_URL  = "https://us-east-1-1.aws.cloud2.influxdata.com"
	INFLUXDB_PORT = ":443"
)

var (
	ORGANISATION   = os.Getenv("ORGANISATION")
	BUCKET         = os.Getenv("BUCKET")
	INFLUXDB_TOKEN = os.Getenv("INFLUXDB_TOKEN")

	data = map[string]map[string]interface{}{
		"point1": {
			"location": "Klamath",
			"species":  "bees",
			"count":    23,
		},
		"point2": {
			"location": "Portland",
			"species":  "ants",
			"count":    30,
		},
		"point3": {
			"location": "Klamath",
			"species":  "bees",
			"count":    28,
		},
		"point4": {
			"location": "Portland",
			"species":  "ants",
			"count":    32,
		},
		"point5": {
			"location": "Klamath",
			"species":  "bees",
			"count":    29,
		},
		"point6": {
			"location": "Portland",
			"species":  "ants",
			"count":    40,
		},
	}
)

// Use influxdb2 for write
func dbWrite(ctx context.Context) error {
	writeClient := influxdb2.NewClient(INFLUXDB_URL, INFLUXDB_TOKEN)

	writeAPI := writeClient.WriteAPIBlocking(ORGANISATION, BUCKET)

	for key := range data {
		point := influxdb2.
			NewPointWithMeasurement("census").
			AddTag("location", data[key]["location"].(string)).
			AddField(data[key]["species"].(string), data[key]["count"])

		err := writeAPI.WritePoint(ctx, point)
		if err != nil {
			return fmt.Errorf("write API write point: %s", err)
		}

		time.Sleep(1 * time.Second)
	}

	return nil
}

// Use flightsql for query
func dbQuery(ctx context.Context) error {
	pool, err := x509.SystemCertPool()
	if err != nil {
		return fmt.Errorf("x509: %s", err)
	}

	transport := grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(pool, ""))
	opts := []grpc.DialOption{transport}

	url := strings.TrimPrefix(INFLUXDB_URL, "https://") + INFLUXDB_PORT

	client, err := flightsql.NewClient(url, nil, nil, opts...)
	if err != nil {
		return fmt.Errorf("flightsql: %s", err)
	}

	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+INFLUXDB_TOKEN)
	ctx = metadata.AppendToOutgoingContext(ctx, "bucket-name", BUCKET)

	query := `SELECT *
			  FROM 'census'
			  WHERE time >= now() - interval '1 hour'
			  AND ('bees' IS NOT NULL OR 'ants' IS NOT NULL)`

	info, err := client.Execute(ctx, query)
	if err != nil {
		return fmt.Errorf("flightsql flight info: %s", err)
	}

	reader, err := client.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		return fmt.Errorf("flightsql do get: %s", err)
	}

	for reader.Next() {
		b, err := json.Marshal(reader.Record())
		if err != nil {
			return err
		}

		fmt.Println(string(b))

		if err := reader.Err(); err != nil {
			return fmt.Errorf("flightsql reader: %s", err)
		}
	}

	return nil
}

func main() {
	err := dbWrite(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Write Successful...")

	err = dbQuery(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Read Successful...")
}
